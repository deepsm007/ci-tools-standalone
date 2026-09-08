package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/yaml"
)

const silenceCreatedBy = "alert-proxy"

type Matcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

type Preset struct {
	Name         string
	Label        string
	Group        string
	Duration     time.Duration
	Default      bool
	UntilWeekday time.Weekday
	AtHour       int
	AtMinute     int
	OfferedFrom  time.Weekday
	Weekend      bool
}

type SilenceConfig struct {
	Default                   time.Duration
	MinDuration               time.Duration
	MaxDuration               time.Duration
	Presets                   []Preset
	NeverSilenceable          []*regexp.Regexp
	RequiredExactLabels       []string
	RequireOneExactLabelFrom  []string
	MinCurrentlyMatchedAlerts int
	MaxCurrentlyMatchedAlerts int
}

type rawSilenceConfig struct {
	Silence struct {
		Default     string `json:"default"`
		MinDuration string `json:"minDuration"`
		MaxDuration string `json:"maxDuration"`
		Presets     []struct {
			Name, Label, Group, Duration  string
			Default                       bool
			UntilWeekday, At, OfferedFrom string
		} `json:"presets"`
	} `json:"silence"`
	Safety struct {
		NeverSilenceable []map[string]string `json:"neverSilenceable"`
		Silence          struct {
			RequiredExactLabels        []string `json:"requiredExactLabels"`
			RequireOneExactLabelFrom   []string `json:"requireOneExactLabelFrom"`
			RequireNonEmptyExactValues bool     `json:"requireNonEmptyExactValues"`
			AllowedUserOperators       []string `json:"allowedUserOperators"`
			MinCurrentlyMatchedAlerts  int      `json:"minCurrentlyMatchedAlerts"`
			MaxCurrentlyMatchedAlerts  int      `json:"maxCurrentlyMatchedAlerts"`
		} `json:"silence"`
	} `json:"safety"`
}

type SilencePolicy struct{ Config SilenceConfig }

func LoadSilencePolicy(path string) (*SilencePolicy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read silence config: %w", err)
	}
	var raw rawSilenceConfig
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("decode silence config: %w", err)
	}
	parseDuration := func(name, value string) (time.Duration, error) {
		if value == "" {
			return 0, fmt.Errorf("%s is required", name)
		}
		d, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", name, err)
		}
		return d, nil
	}
	var cfg SilenceConfig
	if cfg.Default, err = parseDuration("silence.default", raw.Silence.Default); err != nil {
		return nil, err
	}
	if cfg.MinDuration, err = parseDuration("silence.minDuration", raw.Silence.MinDuration); err != nil {
		return nil, err
	}
	if cfg.MaxDuration, err = parseDuration("silence.maxDuration", raw.Silence.MaxDuration); err != nil {
		return nil, err
	}
	if cfg.MinDuration <= 0 || cfg.Default < cfg.MinDuration || cfg.Default > cfg.MaxDuration {
		return nil, errors.New("silence durations must satisfy 0 < minDuration <= default <= maxDuration")
	}
	defaultPresets := 0
	for i, p := range raw.Silence.Presets {
		preset := Preset{Name: p.Name, Label: p.Label, Group: p.Group, Default: p.Default}
		if preset.Default {
			defaultPresets++
		}
		if preset.Label == "" {
			return nil, fmt.Errorf("silence.presets[%d].label is required", i)
		}
		if preset.Name == "" {
			preset.Name = strings.ToLower(regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(preset.Label, "-"))
			preset.Name = strings.Trim(preset.Name, "-")
		}
		if p.Duration != "" {
			preset.Duration, err = time.ParseDuration(p.Duration)
			if err != nil {
				return nil, fmt.Errorf("invalid preset %q duration: %w", p.Label, err)
			}
		} else {
			preset.Weekend = true
			if strings.ToLower(p.UntilWeekday) != "monday" || strings.ToLower(p.OfferedFrom) != "thursday" {
				return nil, fmt.Errorf("preset %q supports only next Monday offered from Thursday", p.Label)
			}
			preset.UntilWeekday, preset.OfferedFrom = time.Monday, time.Thursday
			parts := strings.Split(p.At, ":")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid preset %q at", p.Label)
			}
			preset.AtHour, err = strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			preset.AtMinute, err = strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
		}
		if !preset.Weekend && (preset.Duration < cfg.MinDuration || preset.Duration > cfg.MaxDuration) {
			return nil, fmt.Errorf("preset %q is outside duration bounds", p.Label)
		}
		if preset.Default && (preset.Weekend || preset.Duration != cfg.Default) {
			return nil, fmt.Errorf("default preset %q must equal silence.default", p.Label)
		}
		cfg.Presets = append(cfg.Presets, preset)
	}
	if len(cfg.Presets) == 0 {
		return nil, errors.New("at least one silence preset is required")
	}
	if defaultPresets != 1 {
		return nil, errors.New("exactly one silence preset must be the default")
	}
	for _, rule := range raw.Safety.NeverSilenceable {
		pattern := rule["alertname"]
		if pattern == "" {
			return nil, errors.New("neverSilenceable rule requires alertname")
		}
		pattern = strings.TrimPrefix(pattern, "~")
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid neverSilenceable rule: %w", err)
		}
		cfg.NeverSilenceable = append(cfg.NeverSilenceable, re)
	}
	cfg.RequiredExactLabels = raw.Safety.Silence.RequiredExactLabels
	cfg.RequireOneExactLabelFrom = raw.Safety.Silence.RequireOneExactLabelFrom
	cfg.MinCurrentlyMatchedAlerts = raw.Safety.Silence.MinCurrentlyMatchedAlerts
	cfg.MaxCurrentlyMatchedAlerts = raw.Safety.Silence.MaxCurrentlyMatchedAlerts
	if !raw.Safety.Silence.RequireNonEmptyExactValues {
		return nil, errors.New("requireNonEmptyExactValues must be true")
	}
	if len(raw.Safety.Silence.AllowedUserOperators) != 1 || raw.Safety.Silence.AllowedUserOperators[0] != "=" {
		return nil, errors.New("allowedUserOperators must be exactly [=]")
	}
	if cfg.MinCurrentlyMatchedAlerts != 1 || cfg.MaxCurrentlyMatchedAlerts != 5 {
		return nil, errors.New("current-match safety bounds must be [1,5]")
	}
	return &SilencePolicy{Config: cfg}, nil
}

func (p *SilencePolicy) OfferedPresets(now time.Time) []Preset {
	out := []Preset{}
	for _, preset := range p.Config.Presets {
		if !preset.Weekend || now.UTC().Weekday() >= preset.OfferedFrom || now.UTC().Weekday() == time.Sunday {
			out = append(out, preset)
		}
	}
	return out
}

func (p *SilencePolicy) Preset(name string, now time.Time) (time.Time, error) {
	for _, preset := range p.OfferedPresets(now) {
		if preset.Name != name {
			continue
		}
		end := now.UTC().Add(preset.Duration)
		if preset.Weekend {
			days := (int(time.Monday) - int(now.UTC().Weekday()) + 7) % 7
			if days == 0 {
				days = 7
			}
			date := now.UTC().AddDate(0, 0, days)
			end = time.Date(date.Year(), date.Month(), date.Day(), preset.AtHour, preset.AtMinute, 0, 0, time.UTC)
		}
		return p.ClampExpiry(now, end), nil
	}
	return time.Time{}, fmt.Errorf("unknown or unavailable preset %q", name)
}

func (p *SilencePolicy) ClampExpiry(now, requested time.Time) time.Time {
	min, max := now.UTC().Add(p.Config.MinDuration), now.UTC().Add(p.Config.MaxDuration)
	if requested.Before(min) {
		return min
	}
	if requested.After(max) {
		return max
	}
	return requested.UTC()
}

func (p *SilencePolicy) MatcherCandidates(a AlertSnapshot) []Matcher {
	if a.Labels["alertname"] == "" {
		return nil
	}
	matchers := []Matcher{{Name: "alertname", Value: a.Labels["alertname"], IsEqual: true}}
	for _, narrow := range []string{"job_name", "namespace"} {
		if a.Labels[narrow] != "" {
			matchers = append(matchers, Matcher{Name: narrow, Value: a.Labels[narrow], IsEqual: true})
		}
	}
	if len(matchers) == 1 {
		return nil
	}
	if err := p.ValidateMatchers(matchers); err != nil {
		return nil
	}
	return matchers
}

func (p *SilencePolicy) CommonMatchers(batch []AlertSnapshot) ([]Matcher, error) {
	if len(batch) == 0 {
		return nil, errors.New("empty notification batch")
	}
	name := commonLabel(batch, "alertname")
	if name == "" {
		return nil, errors.New("alertname is not common to the notification")
	}
	matchers := []Matcher{{Name: "alertname", Value: name, IsEqual: true}}
	for _, key := range []string{"job_name", "namespace"} {
		if v := commonLabel(batch, key); v != "" {
			matchers = append(matchers, Matcher{Name: key, Value: v, IsEqual: true})
		}
	}
	if len(matchers) == 1 {
		return nil, errors.New("notification has no common job_name or namespace")
	}
	return matchers, p.ValidateMatchers(matchers)
}

func (p *SilencePolicy) ValidateMatchers(matchers []Matcher) error {
	values := map[string]string{}
	for _, m := range matchers {
		if m.Name == "" || m.Value == "" {
			return errors.New("matchers require non-empty names and values")
		}
		if m.IsRegex || !m.IsEqual {
			return errors.New("only exact equality matchers are allowed")
		}
		if old, ok := values[m.Name]; ok {
			if old != m.Value {
				return fmt.Errorf("conflicting matcher for %s", m.Name)
			}
			return fmt.Errorf("duplicate matcher for %s", m.Name)
		}
		values[m.Name] = m.Value
	}
	for _, required := range p.Config.RequiredExactLabels {
		if values[required] == "" {
			return fmt.Errorf("exact %s matcher is required", required)
		}
	}
	narrow := false
	for _, candidate := range p.Config.RequireOneExactLabelFrom {
		if values[candidate] != "" {
			narrow = true
		}
	}
	if !narrow {
		return fmt.Errorf("one exact narrowing matcher from %v is required", p.Config.RequireOneExactLabelFrom)
	}
	for _, rule := range p.Config.NeverSilenceable {
		if rule.MatchString(values["alertname"]) {
			return fmt.Errorf("alert %q is protected by neverSilenceable policy", values["alertname"])
		}
	}
	return nil
}

func canonicalMatchers(matchers []Matcher) []Matcher {
	out := make([]Matcher, 0, len(matchers))
	seen := map[Matcher]bool{}
	for _, matcher := range matchers {
		if !seen[matcher] {
			out = append(out, matcher)
			seen[matcher] = true
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Value < out[j].Value
		}
		return out[i].Name < out[j].Name
	})
	return out
}
func canonicalMatcherString(matchers []Matcher) string {
	parts := []string{}
	for _, m := range canonicalMatchers(matchers) {
		parts = append(parts, m.Name+"="+m.Value)
	}
	return strings.Join(parts, ", ")
}
func ownershipID(source string, matchers []Matcher) string {
	canonical, err := json.Marshal(struct {
		Source   string    `json:"source"`
		Matchers []Matcher `json:"matchers"`
	}{Source: source, Matchers: canonicalMatchers(matchers)})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
func labelsMatch(labels map[string]string, matchers []Matcher) bool {
	for _, m := range matchers {
		if m.IsRegex || !m.IsEqual || labels[m.Name] != m.Value {
			return false
		}
	}
	return true
}

type AMSilence struct {
	ID        string    `json:"id,omitempty"`
	Matchers  []Matcher `json:"matchers"`
	StartsAt  time.Time `json:"startsAt"`
	EndsAt    time.Time `json:"endsAt"`
	CreatedBy string    `json:"createdBy"`
	Comment   string    `json:"comment"`
	Status    struct {
		State string `json:"state"`
	} `json:"status,omitempty"`
}
type AMAlert struct {
	Labels   map[string]string `json:"labels"`
	StartsAt time.Time         `json:"startsAt"`
	EndsAt   time.Time         `json:"endsAt"`
	Status   struct {
		State string `json:"state"`
	} `json:"status"`
}

type AlertmanagerAPI interface {
	ListSilences(context.Context) ([]AMSilence, error)
	GetSilence(context.Context, string) (AMSilence, error)
	UpsertSilence(context.Context, AMSilence) (string, error)
	ExpireSilence(context.Context, string) error
	ListAlerts(context.Context) ([]AMAlert, error)
}

type HTTPAlertmanager struct {
	BaseURL, CAFile, TokenPath string
	Timeout                    time.Duration
	Client                     *http.Client

	clientMu        sync.Mutex
	cachedClient    *http.Client
	cachedTransport *http.Transport
	cachedCAHash    [sha256.Size]byte
}

func (a *HTTPAlertmanager) client() (*http.Client, error) {
	if a.Client != nil {
		return a.Client, nil
	}
	a.clientMu.Lock()
	defer a.clientMu.Unlock()
	return a.clientLocked()
}

func (a *HTTPAlertmanager) clientLocked() (*http.Client, error) {
	ca, err := os.ReadFile(a.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read Alertmanager CA: %w", err)
	}
	caHash := sha256.Sum256(ca)

	if a.cachedClient != nil && a.cachedCAHash == caHash {
		return a.cachedClient, nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	if pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("alertmanager CA contains no certificates")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}
	client := &http.Client{Timeout: a.Timeout, Transport: transport}
	oldTransport := a.cachedTransport
	a.cachedCAHash = caHash
	a.cachedTransport = transport
	a.cachedClient = client
	if oldTransport != nil {
		oldTransport.CloseIdleConnections()
	}
	return client, nil
}

func (a *HTTPAlertmanager) do(req *http.Request) (*http.Response, error) {
	if a.Client != nil {
		return a.Client.Do(req)
	}
	a.clientMu.Lock()
	defer a.clientMu.Unlock()
	client, err := a.clientLocked()
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (a *HTTPAlertmanager) request(ctx context.Context, method, path string, body any, out any) error {
	token, err := readSecret(a.TokenPath)
	if err != nil {
		return fmt.Errorf("read Alertmanager token: %w", err)
	}
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.BaseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &AMHTTPError{StatusCode: resp.StatusCode, Body: string(raw)}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode Alertmanager response: %w", err)
		}
	}
	return nil
}

type AMHTTPError struct {
	StatusCode int
	Body       string
}

func (e *AMHTTPError) Error() string {
	return fmt.Sprintf("Alertmanager returned %d: %s", e.StatusCode, e.Body)
}
func (a *HTTPAlertmanager) ListSilences(ctx context.Context) ([]AMSilence, error) {
	var out []AMSilence
	err := a.request(ctx, http.MethodGet, "/api/v2/silences", nil, &out)
	return out, err
}
func (a *HTTPAlertmanager) GetSilence(ctx context.Context, id string) (AMSilence, error) {
	var out AMSilence
	err := a.request(ctx, http.MethodGet, "/api/v2/silence/"+url.PathEscape(id), nil, &out)
	return out, err
}
func (a *HTTPAlertmanager) UpsertSilence(ctx context.Context, s AMSilence) (string, error) {
	var out struct {
		SilenceID string `json:"silenceID"`
	}
	err := a.request(ctx, http.MethodPost, "/api/v2/silences", s, &out)
	return out.SilenceID, err
}
func (a *HTTPAlertmanager) ExpireSilence(ctx context.Context, id string) error {
	return a.request(ctx, http.MethodDelete, "/api/v2/silence/"+url.PathEscape(id), nil, nil)
}
func (a *HTTPAlertmanager) ListAlerts(ctx context.Context) ([]AMAlert, error) {
	var out []AMAlert
	err := a.request(ctx, http.MethodGet, "/api/v2/alerts?active=true&silenced=true&inhibited=true&unprocessed=true", nil, &out)
	return out, err
}

func currentMatchCount(alerts []AMAlert, matchers []Matcher, now time.Time) int {
	count := 0
	for _, a := range alerts {
		if !a.EndsAt.IsZero() && !a.EndsAt.After(now) {
			continue
		}
		if labelsMatch(a.Labels, matchers) {
			count++
		}
	}
	return count
}

var markerRE = regexp.MustCompile(`\[alert-proxy ownership=([a-f0-9]{64}) operation=([a-f0-9]+) generation=([0-9]+)\]`)

func marker(ownership, operation string, generation int64) string {
	return fmt.Sprintf("[alert-proxy ownership=%s operation=%s generation=%d]", ownership, operation, generation)
}
func parseMarker(comment string) (ownership, operation string, generation int64, ok bool) {
	m := markerRE.FindStringSubmatch(comment)
	if len(m) != 4 {
		return
	}
	generation, err := strconv.ParseInt(m[3], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return m[1], m[2], generation, true
}

func readSecret(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) == 0 {
		return nil, errors.New("secret file is empty")
	}
	return b, nil
}
