package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// diagnosticDelayPattern extracts shared-client throttling delays without recompiling per line.
var diagnosticDelayPattern = regexp.MustCompile(`\bdelay="([^"]+)"`)

// diagnosticHTTPStatusPattern recognizes explicit HTTP failures without recompiling per line.
var diagnosticHTTPStatusPattern = regexp.MustCompile(`\bhttp[ /][45][0-9]{2}\b`)

// diagnosticARNPattern removes resource identifiers from selected log fields.
var diagnosticARNPattern = regexp.MustCompile(`(?i)\barn:[^\s"'<>]+`)

// diagnosticURLPattern finds URLs whose credentials must be removed.
var diagnosticURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)

// diagnosticURLSchemePattern recognizes escaped URLs after unescaping credential text.
var diagnosticURLSchemePattern = regexp.MustCompile(`(?i)https?://`)

// diagnosticCredentialPattern suppresses selected fields containing credential assignments.
var diagnosticCredentialPattern = regexp.MustCompile(`(?i)\b(authorization|token|password|passwd|secret|api[_-]?key|(?:access|refresh|id|session|auth|api)[_-]?token|client[_-]?secret|cookie|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key|(?:session|security)[_-]?token)|access[_-]?key[_-]?id|secret[_-]?access[_-]?key)["'\s]*[:=]|\b(bearer|basic)\s+\S+`)

// logRecord decodes only fields that can support a bounded diagnostic summary.
type logRecord struct {
	Message    string          `json:"msg"`
	Level      string          `json:"level"`
	Severity   string          `json:"severity"`
	Controller string          `json:"controller"`
	Error      string          `json:"error"`
	Job        string          `json:"job"`
	Cluster    string          `json:"cluster"`
	Delay      json.RawMessage `json:"delay"`
	Reason     string          `json:"reason"`
	Request    struct {
		Name string `json:"Name"`
	} `json:"request"`
	ProwJob struct {
		Name string `json:"name"`
	} `json:"ProwJob"`
}

// parseDiagnosticLogs scans all bounded lines but retains only narrowly classified evidence.
func parseDiagnosticLogs(ctx context.Context, data []byte, source, pod string, since, now time.Time, result *diagnosticSource, groups map[string]*diagnosticIssue) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), len(data)+1)
	for {
		if ctx.Err() != nil {
			result.canceled = true
			diagnosticLimit(result, "collection-deadline")
			return
		}
		if !scanner.Scan() {
			break
		}
		result.ScannedLines++
		timestamp, body, ok := timestampedLogLine(scanner.Text())
		if !ok || (!since.IsZero() && timestamp.Before(since)) || timestamp.After(now) {
			continue
		}
		if result.CoveredFrom == nil || timestamp.Before(*result.CoveredFrom) {
			value := timestamp
			result.CoveredFrom = &value
		}
		if result.CoveredTo == nil || timestamp.After(*result.CoveredTo) {
			value := timestamp
			result.CoveredTo = &value
		}
		issue, matched := classifyDiagnosticLine(source, body)
		if !matched {
			continue
		}
		result.MatchedLines++
		issue.Pod = redactLogText(pod, 253)
		issue.FirstAt, issue.LastAt, issue.Count = timestamp, timestamp, 1
		issue.ID = diagnosticFingerprint(source, issue)
		if existing, exists := groups[issue.ID]; exists {
			existing.Count++
			if timestamp.Before(existing.FirstAt) {
				existing.FirstAt = timestamp
			}
			if timestamp.After(existing.LastAt) {
				existing.LastAt = timestamp
			}
			if existing.ProwJob != issue.ProwJob {
				existing.ProwJob = ""
			}
			if issue.DelaySeconds != nil && (existing.DelaySeconds == nil || *issue.DelaySeconds > *existing.DelaySeconds) {
				existing.DelaySeconds = issue.DelaySeconds
			}
			continue
		}
		if len(groups) >= 100 {
			result.GroupsLimited = true
			diagnosticLimit(result, "group-limit")
			continue
		}
		groups[issue.ID] = &issue
	}
	if scanner.Err() != nil {
		diagnosticLimit(result, "read-incomplete")
	}
}

// timestampedLogLine trusts the Kubernetes outer timestamp rather than application clock fields.
func timestampedLogLine(line string) (time.Time, string, bool) {
	prefix, body, found := strings.Cut(line, " ")
	if !found {
		return time.Time{}, "", false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, prefix)
	if err != nil || timestamp.IsZero() {
		return time.Time{}, "", false
	}
	body = strings.TrimSpace(body)
	for _, marker := range []string{"stdout F ", "stderr F "} {
		body = strings.TrimPrefix(body, marker)
	}
	return timestamp.UTC(), body, true
}

// classifyDiagnosticLine excludes Plank, ordinary placements, and normal dispatcher information.
func classifyDiagnosticLine(source, body string) (diagnosticIssue, bool) {
	if source == "scheduler" && strings.HasPrefix(body, "I") && strings.Contains(body, "request.go:") && strings.Contains(body, `"Waited before sending request"`) && strings.Contains(body, `reason="client-side throttling, not priority and fairness"`) {
		match := diagnosticDelayPattern.FindStringSubmatch(body)
		if len(match) != 2 {
			return diagnosticIssue{}, false
		}
		delay, err := time.ParseDuration(match[1])
		if err != nil || delay <= 0 {
			return diagnosticIssue{}, false
		}
		seconds := delay.Seconds()
		return diagnosticIssue{Category: "api-throttling", Level: "info", Message: "Kubernetes client-side request throttling", Detail: "Shared-client log; attribution to the scheduler controller is not established. This is not evidence of dispatcher latency.", Component: "shared-client", DelaySeconds: &seconds}, true
	}
	var record logRecord
	if json.Unmarshal([]byte(body), &record) != nil {
		return diagnosticIssue{}, false
	}
	level := strings.ToLower(record.Level)
	if level == "" {
		level = strings.ToLower(record.Severity)
	}
	if level == "warn" {
		level = "warning"
	}
	if source == "scheduler" && (record.Controller != "scheduler" || (record.Message != "scheduling failed, using Spec.Cluster entry" && record.Message != "Waited before sending request" && (!diagnosticErrorLevel(level) || !explicitCommunication(record.Message+" "+record.Error)))) {
		return diagnosticIssue{}, false
	}
	if source != "scheduler" && (source != "dispatcher" || !diagnosticErrorLevel(level)) {
		return diagnosticIssue{}, false
	}
	issue := diagnosticIssue{Level: level, Message: redactLogText(record.Message, 512), Detail: redactLogText(record.Error, 768), Job: redactLogText(record.Job, 256), Cluster: redactLogText(record.Cluster, 128), ProwJob: redactLogText(record.Request.Name, 253)}
	if record.Job != "" {
		issue.Family = diagnosticJobFamily(record.Job)
	}
	if issue.ProwJob == "" {
		issue.ProwJob = redactLogText(record.ProwJob.Name, 253)
	}
	if source == "scheduler" {
		if record.Controller != "scheduler" {
			return diagnosticIssue{}, false
		}
		issue.Component = "scheduler"
		if record.Message == "scheduling failed, using Spec.Cluster entry" {
			issue.Category = "dispatcher-fallback"
			issue.Detail = "The scheduler selected Spec.Cluster after an external lookup failure. The underlying error was not logged; transport, HTTP rejection, and response decoding cannot be distinguished. Selection does not establish successful ProwJob persistence or execution."
			issue.Family = diagnosticJobFamily(record.Job)
			return issue, true
		}
		if record.Message == "Waited before sending request" && record.Reason == "client-side throttling, not priority and fairness" {
			value, err := strconv.ParseFloat(strings.Trim(string(record.Delay), `"`), 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
				return diagnosticIssue{}, false
			}
			seconds := value / float64(time.Second)
			issue.Category = "api-throttling"
			issue.DelaySeconds = &seconds
			issue.Detail = "Kubernetes client-side request throttling, not priority and fairness. This is not evidence of dispatcher latency."
			return issue, true
		}
		if !diagnosticErrorLevel(level) || !explicitCommunication(record.Message+" "+record.Error) {
			return diagnosticIssue{}, false
		}
		issue.Category = "communication"
		return issue, true
	}
	if source != "dispatcher" || !diagnosticErrorLevel(level) {
		return diagnosticIssue{}, false
	}
	issue.Category = "runtime-error"
	if explicitCommunication(record.Message + " " + record.Error) {
		issue.Category = "communication"
	}
	if issue.Detail == "" {
		issue.Detail = "No underlying error detail was recorded."
	}
	return issue, true
}

// diagnosticErrorLevel limits errors to explicitly elevated records.
func diagnosticErrorLevel(level string) bool {
	return level == "warning" || level == "error" || level == "fatal" || level == "panic"
}

// explicitCommunication requires explicit error text instead of guessing from a fallback warning.
func explicitCommunication(value string) bool {
	lower := strings.ToLower(value)
	for _, phrase := range []string{"no such host", "server misbehaving", "temporary failure in name resolution", "tls:", "x509:", "certificate signed by unknown authority", "certificate has expired", "connection refused", "connection reset", "broken pipe", "no route to host", "network is unreachable", "context deadline exceeded", "i/o timeout", "error sending request", "client.timeout exceeded", "unexpected status code", "http status", "status code:", "error decoding response", "looking for beginning of value"} {
		if strings.Contains(lower, phrase) {
			return true
		}
	}
	return diagnosticHTTPStatusPattern.MatchString(lower)
}

// diagnosticJobFamily labels name patterns, not authoritative ProwJob configuration types.
func diagnosticJobFamily(job string) string {
	for _, prefix := range []struct{ prefix, family string }{{"rehearse-", "rehearsal"}, {"aggregated-", "aggregated"}, {"ephemeralcluster-", "ephemeral"}} {
		if strings.HasPrefix(job, prefix.prefix) {
			return prefix.family
		}
	}
	return "regular"
}

// diagnosticFingerprint excludes timestamps and request identity so repetitions stay grouped.
func diagnosticFingerprint(source string, issue diagnosticIssue) string {
	data := strings.Join([]string{source, issue.Category, issue.Job, issue.Cluster, issue.Message, issue.Detail, issue.Pod}, "\x00")
	digest := sha256.Sum256([]byte(data))
	return hex.EncodeToString(digest[:12])
}

// redactLogText removes credentials before any selected field can reach a browser or fingerprint.
func redactLogText(value string, limit int) string {
	if value == "" {
		return ""
	}
	value = diagnosticARNPattern.ReplaceAllString(value, "[redacted ARN]")
	value = diagnosticURLPattern.ReplaceAllStringFunc(value, redactLogURL)
	// Error strings can contain multiply escaped JSON. Suppress the entire field
	// instead of guessing where a quoted credential ends and leaking a suffix.
	credentialText := strings.ReplaceAll(value, `\`, "")
	if credentialText != value && diagnosticURLSchemePattern.MatchString(credentialText) {
		return "[redacted credential-bearing text]"
	}
	if diagnosticCredentialPattern.MatchString(credentialText) {
		return "[redacted credential-bearing text]"
	}
	value = strings.TrimSpace(strings.Map(cleanLogRune, value))
	if len(value) > limit {
		value = value[:limit]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
		value += "…"
	}
	return value
}

// cleanLogRune prevents control characters from changing the presentation of diagnostic text.
func cleanLogRune(value rune) rune {
	if value < 32 || value == 127 {
		return ' '
	}
	return value
}

// redactLogURL removes all userinfo, query values, and fragments, including unfamiliar secret keys.
func redactLogURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return "[redacted URL]"
	}
	parsed.User = nil
	query := parsed.RawQuery != "" || parsed.ForceQuery
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	result := parsed.String()
	if query {
		result += "?[redacted]"
	}
	return result
}

// demoDiagnostics creates labeled examples through the real classifier without Kubernetes reads.
func demoDiagnostics(now time.Time, window time.Duration) diagnosticResponse {
	response := newDiagnosticResponse(now, window, defaultLogBytes)
	response.Enabled, response.Demo = true, true
	for _, source := range logSources() {
		result := emptyDiagnosticSource(source.id)
		result.Status = "available"
		result.ByteLimit = defaultLogBytes / int64(len(logSources()))
		ready, restarts := true, int32(0)
		result.Pods = []diagnosticPod{{Name: "demo-" + source.id, Ready: &ready, Restarts: &restarts, PreviousLogs: "not-needed"}}
		body := `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"rehearse-demo-example","cluster":"build02","request":{"Name":"demo-prowjob"}}`
		if source.id == "dispatcher" {
			body = `{"level":"error","msg":"failed to reconcile dispatcher runtime policy; retaining the last valid generation","error":"baseline and inventory are not loaded"}`
		}
		data := []byte(now.Add(-4*time.Minute).Format(time.RFC3339Nano) + " " + body + "\n" + now.Add(-2*time.Minute).Format(time.RFC3339Nano) + " " + body + "\n")
		if source.id == "scheduler" {
			data = append(data, []byte(now.Add(-time.Minute).Format(time.RFC3339Nano)+` {"controller":"scheduler","level":"info","msg":"Waited before sending request","delay":"1100000000","reason":"client-side throttling, not priority and fairness"}`+"\n")...)
			data = append(data, []byte(now.Add(-30*time.Second).Format(time.RFC3339Nano)+` {"controller":"scheduler","level":"error","msg":"Synthetic example: external scheduling request failed","error":"connection refused by dispatcher.example","job":"rehearse-demo-communication","cluster":"build01"}`+"\n")...)
		}
		groups := make(map[string]*diagnosticIssue)
		since := time.Time{}
		if window != 0 {
			since = now.Add(-window)
		}
		result.BytesRead = int64(len(data))
		parseDiagnosticLogs(context.Background(), data, source.id, "demo-"+source.id, since, now, &result, groups)
		for _, issue := range groups {
			result.Issues = append(result.Issues, *issue)
		}
		slices.SortFunc(result.Issues, compareDiagnosticIssues)
		response.Sources = append(response.Sources, result)
	}
	return response
}
