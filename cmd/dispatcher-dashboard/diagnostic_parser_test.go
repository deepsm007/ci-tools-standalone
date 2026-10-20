package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// TestDiagnosticClassification exercises synthetic selected evidence, never private production logs.
func TestDiagnosticClassification(t *testing.T) {
	for _, tc := range []struct {
		name, source, line, category, component, family string
		delay                                           float64
	}{
		{"fallback", "scheduler", `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"rehearse-example","cluster":"build01"}`, "dispatcher-fallback", "scheduler", "rehearsal", 0},
		{"aggregated fallback", "scheduler", `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"aggregated-example"}`, "dispatcher-fallback", "scheduler", "aggregated", 0},
		{"ephemeral fallback", "scheduler", `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"ephemeralcluster-example"}`, "dispatcher-fallback", "scheduler", "ephemeral", 0},
		{"regular fallback", "scheduler", `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"periodic-example"}`, "dispatcher-fallback", "scheduler", "regular", 0},
		{"scheduler throttle", "scheduler", `{"controller":"scheduler","level":"info","msg":"Waited before sending request","delay":"1100000000","reason":"client-side throttling, not priority and fairness","URL":"{\"User\":\"do-not-return\"}"}`, "api-throttling", "scheduler", "", 1.1},
		{"shared throttle", "scheduler", `I1020 05:01:26.343191 1 request.go:754] "Waited before sending request" delay="1.5s" reason="client-side throttling, not priority and fairness" verb="PATCH" URL="https://hidden.invalid?token=do-not-return"`, "api-throttling", "shared-client", "", 1.5},
		{"explicit DNS", "scheduler", `{"controller":"scheduler","level":"error","msg":"query failed","error":"lookup dispatcher: no such host"}`, "communication", "scheduler", "", 0},
		{"generated explicit communication", "scheduler", `{"controller":"scheduler","level":"error","msg":"query failed","error":"connection refused","job":"rehearse-example"}`, "communication", "scheduler", "rehearsal", 0},
		{"explicit TLS", "scheduler", `{"controller":"scheduler","level":"warning","msg":"request failed","error":"x509: certificate has expired"}`, "communication", "scheduler", "", 0},
		{"explicit HTTP", "scheduler", `{"controller":"scheduler","level":"error","msg":"unexpected status code: 503"}`, "communication", "scheduler", "", 0},
		{"explicit decode", "scheduler", `{"controller":"scheduler","level":"error","msg":"error decoding response"}`, "communication", "scheduler", "", 0},
		{"runtime reconcile", "dispatcher", `{"level":"error","msg":"failed to reconcile dispatcher runtime policy","error":"baseline and inventory are not loaded"}`, "runtime-error", "", "", 0},
		{"dispatcher transport", "dispatcher", `{"level":"fatal","msg":"request failed","error":"connection refused"}`, "communication", "", "", 0},
		{"plank excluded", "scheduler", `{"controller":"plank","level":"warning","msg":"scheduling failed, using Spec.Cluster entry"}`, "", "", "", 0},
		{"plank transport excluded", "scheduler", `{"controller":"plank","level":"error","msg":"connection refused"}`, "", "", "", 0},
		{"assignment excluded", "scheduler", `{"controller":"scheduler","level":"info","msg":"Cluster assigned"}`, "", "", "", 0},
		{"scheduler generic warning excluded", "scheduler", `{"controller":"scheduler","level":"warning","msg":"unknown scheduling event"}`, "", "", "", 0},
		{"dispatcher info excluded", "dispatcher", `{"level":"info","msg":"reconciling cluster config"}`, "", "", "", 0},
		{"invalid throttle excluded", "scheduler", `{"controller":"scheduler","level":"info","msg":"Waited before sending request","delay":"NaN","reason":"client-side throttling, not priority and fairness"}`, "", "", "", 0},
		{"unstructured error excluded", "scheduler", `connection refused`, "", "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, matched := classifyDiagnosticLine(tc.source, tc.line)
			if matched != (tc.category != "") || got.Category != tc.category || got.Component != tc.component || got.Family != tc.family {
				t.Fatalf("classification = %+v, matched %t", got, matched)
			}
			if tc.delay > 0 && (got.DelaySeconds == nil || *got.DelaySeconds != tc.delay) {
				t.Errorf("delay = %v, want %v", got.DelaySeconds, tc.delay)
			}
			if got.Category == "dispatcher-fallback" && !strings.Contains(got.Detail, "underlying error was not logged") {
				t.Error("fallback implied a known cause")
			}
			if strings.Contains(got.Detail, "do-not-return") {
				t.Error("discarded URL fields leaked")
			}
		})
	}
}

// TestDiagnosticRedaction keeps credential-bearing selected fields safe before publication.
func TestDiagnosticRedaction(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		secrets     []string
	}{
		{"authorization", "Authorization: Bearer secretbearer Basic c2VjcmV0", []string{"secretbearer", "c2VjcmV0"}},
		{"URL credentials", "error fetching https://alice:secretpass@dispatcher.invalid/path?token=secretquery#secretfragment", []string{"alice", "secretpass", "secretquery", "secretfragment"}},
		{"quoted credentials", `{"token":"secretjson","password":"secretpassword"} api_key=secretkey`, []string{"secretjson", "secretpassword", "secretkey"}},
		{"escaped JSON credentials", `request failed: {\"token\":\"escaped-secret\"}`, []string{"escaped-secret"}},
		{"escaped quote in credential", `request failed: {"password":"prefix\"suffix-secret"}`, []string{"prefix", "suffix-secret"}},
		{"multiply escaped authorization", `request failed: {\\\"Authorization\\\":\\\"Bearer escaped-bearer-secret\\\"}`, []string{"escaped-bearer-secret"}},
		{"escaped URL userinfo", `request failed: https:\/\/alice:escaped-url-secret@dispatcher.invalid/path`, []string{"alice", "escaped-url-secret"}},
		{"malformed URL", "https://user:secretvalue@host/%zz?token=value", []string{"secretvalue", "token=value"}},
		{"AWS access key ID", "AWS_ACCESS_KEY_ID=private-access", []string{"private-access"}},
		{"AWS secret access key", "AWS_SECRET_ACCESS_KEY=private-secret", []string{"private-secret"}},
		{"AWS session token", "AWS_SESSION_TOKEN=private-session", []string{"private-session"}},
		{"AWS camel case access key", `{"accessKeyId":"private-access"}`, []string{"private-access"}},
		{"AWS camel case secret", `{"secretAccessKey":"private-secret"}`, []string{"private-secret"}},
		{"ARN resource", "permission denied for arn:aws:iam::123456789012:role/private-role", []string{"arn:aws:", "123456789012", "private-role"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := redactLogText(tc.input, 512)
			for _, secret := range tc.secrets {
				if strings.Contains(got, secret) {
					t.Errorf("credential %q remained in %q", secret, got)
				}
			}
		})
	}
}

// TestDiagnosticGrouping verifies outer timestamps, coverage, repetition and bounded groups.
func TestDiagnosticGrouping(t *testing.T) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	body := `{"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"periodic-example","cluster":"build01","time":"1999-01-01T00:00:00Z","request":{"Name":"%s"}}`
	for _, tc := range []struct {
		name                                 string
		lines                                []string
		wantScanned, wantMatched, wantGroups int
		limited                              bool
	}{
		{"outer time and grouping", []string{now.Add(-2*time.Minute).Format(time.RFC3339Nano) + " " + fmt.Sprintf(body, "first"), now.Add(-time.Minute).Format(time.RFC3339Nano) + " " + fmt.Sprintf(body, "second"), now.Add(-time.Hour).Format(time.RFC3339Nano) + " " + fmt.Sprintf(body, "old"), "malformed line"}, 4, 2, 1, false},
		{"group cap", diagnosticGroupLines(now), 105, 105, 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := emptyDiagnosticSource("scheduler")
			groups := make(map[string]*diagnosticIssue)
			parseDiagnosticLogs(context.Background(), []byte(strings.Join(tc.lines, "\n")), "scheduler", "test-pod", now.Add(-15*time.Minute), now, &result, groups)
			if result.ScannedLines != tc.wantScanned || result.MatchedLines != tc.wantMatched || len(groups) != tc.wantGroups || result.GroupsLimited != tc.limited {
				t.Fatalf("unexpected result: %+v, %d groups", result, len(groups))
			}
			if !tc.limited {
				for _, issue := range groups {
					if issue.Count != 2 || !issue.FirstAt.Equal(now.Add(-2*time.Minute)) || !issue.LastAt.Equal(now.Add(-time.Minute)) || issue.ProwJob != "" {
						t.Errorf("incorrect repetition evidence: %+v", issue)
					}
				}
			}
			if result.CoveredFrom == nil || result.CoveredTo == nil {
				t.Error("missing actual log coverage")
			}
		})
	}
}

// diagnosticGroupLines generates synthetic distinct groups to test the hard memory bound.
func diagnosticGroupLines(now time.Time) []string {
	lines := []string{}
	for index := 0; index < 105; index++ {
		lines = append(lines, fmt.Sprintf(`%s {"controller":"scheduler","level":"warning","msg":"scheduling failed, using Spec.Cluster entry","job":"job-%d"}`, now.Format(time.RFC3339Nano), index))
	}
	return lines
}

// TestDiagnosticDistinctCauses prevents one runtime failure detail from hiding another.
func TestDiagnosticDistinctCauses(t *testing.T) {
	for _, tc := range []struct {
		name, first, second string
		same                bool
	}{
		{"same cause", "baseline not loaded", "baseline not loaded", true},
		{"different causes", "baseline not loaded", "inventory not loaded", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := diagnosticIssue{Message: "reconcile failed", Detail: tc.first}
			b := diagnosticIssue{Message: "reconcile failed", Detail: tc.second}
			if (diagnosticFingerprint("dispatcher", a) == diagnosticFingerprint("dispatcher", b)) != tc.same {
				t.Error("incorrect cause grouping")
			}
		})
	}
}

func TestDiagnosticAvailableAndUncappedLines(t *testing.T) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		since       time.Time
		lines, want int
	}{
		{name: "available includes older retained logs", lines: 1, want: 1},
		{name: "recent excludes older retained logs", since: now.Add(-30 * time.Minute), lines: 1, want: 0},
		{name: "more than two thousand lines", lines: 2501, want: 2501},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := now.Add(-48*time.Hour).Format(time.RFC3339Nano) + ` {"level":"error","msg":"connection refused"}` + "\n"
			result := emptyDiagnosticSource("dispatcher")
			groups := make(map[string]*diagnosticIssue)
			parseDiagnosticLogs(context.Background(), []byte(strings.Repeat(line, tc.lines)), "dispatcher", "pod", tc.since, now, &result, groups)
			if result.ScannedLines != tc.lines || result.MatchedLines != tc.want || result.Truncated {
				t.Errorf("unexpected parsing coverage: %+v", result)
			}
		})
	}
}

// diagnosticCancelContext cancels on a chosen parser checkpoint without scheduling or timers.
type diagnosticCancelContext struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

// Err makes cancellation deterministic while preserving the embedded context's error.
func (ctx *diagnosticCancelContext) Err() error {
	ctx.remaining--
	if ctx.remaining == 0 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

// TestDiagnosticParsingCancellation preserves collected evidence and stops scanning at cancellation.
func TestDiagnosticParsingCancellation(t *testing.T) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	line := now.Format(time.RFC3339Nano) + ` {"controller":"scheduler","level":"error","msg":"connection refused"}` + "\n"
	for _, tc := range []struct {
		name                     string
		data                     string
		cancelAfter              int
		wantScanned, wantMatched int
		wantCanceled             bool
	}{
		{name: "complete", data: strings.Repeat(line, 3), cancelAfter: -1, wantScanned: 3, wantMatched: 3},
		{name: "canceled before scan", data: strings.Repeat(line, 3), wantCanceled: true},
		{name: "canceled empty input", wantCanceled: true},
		{name: "canceled after matching line", data: strings.Repeat(line, 3), cancelAfter: 2, wantScanned: 1, wantMatched: 1, wantCanceled: true},
		{name: "canceled after ignored line", data: strings.Repeat("malformed line\n", 3), cancelAfter: 2, wantScanned: 1, wantCanceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelAfter == 0 {
				cancel()
			}
			parseContext := ctx
			if tc.cancelAfter > 0 {
				parseContext = &diagnosticCancelContext{Context: ctx, cancel: cancel, remaining: tc.cancelAfter}
			}
			result := emptyDiagnosticSource("scheduler")
			groups := make(map[string]*diagnosticIssue)
			parseDiagnosticLogs(parseContext, []byte(tc.data), "scheduler", "pod", time.Time{}, now, &result, groups)
			wantReasons := []string{}
			if tc.wantCanceled {
				wantReasons = append(wantReasons, "collection-deadline")
			}
			if result.ScannedLines != tc.wantScanned || result.MatchedLines != tc.wantMatched || result.Truncated != tc.wantCanceled || result.canceled != tc.wantCanceled {
				t.Fatalf("unexpected cancellation coverage: %+v", result)
			}
			if diff := cmp.Diff(wantReasons, result.TruncationReasons); diff != "" {
				t.Errorf("unexpected reasons (-want +got):\n%s", diff)
			}
			count := 0
			for _, issue := range groups {
				count += issue.Count
			}
			if count != tc.wantMatched {
				t.Errorf("retained issue count = %d, want %d", count, tc.wantMatched)
			}
		})
	}
}

// BenchmarkParseDiagnosticLogs measures a full source budget of repeated transport failures.
func BenchmarkParseDiagnosticLogs(b *testing.B) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	line := now.Format(time.RFC3339Nano) + ` {"controller":"scheduler","level":"error","msg":"request failed","error":"connection refused"}` + "\n"
	data := []byte(strings.Repeat(line, (4<<20)/len(line)))
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result := emptyDiagnosticSource("scheduler")
		groups := make(map[string]*diagnosticIssue)
		parseDiagnosticLogs(context.Background(), data, "scheduler", "pod", time.Time{}, now, &result, groups)
	}
}
