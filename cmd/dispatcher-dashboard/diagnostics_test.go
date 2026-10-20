package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// diagnosticClock provides deterministic TTL checks without sleeps.
type diagnosticClock struct{ now time.Time }

// Now is read only while test calls are idle, matching the collector's serialized clock access.
func (clock *diagnosticClock) Now() time.Time { return clock.now }

// logReaderFake records the narrow read boundary and supplies synthetic current-container logs.
type logReaderFake struct {
	mu              sync.Mutex
	listError       error
	logError        error
	podCount        int
	restarts        int32
	previousError   error
	previousData    *string
	downloaded      int64
	data            string
	calls           []string
	opts            []*corev1.PodLogOptions
	deadlineMissing bool
}

// listPods validates fixed discovery and supplies enough metadata to exercise rollout limits.
func (fake *logReaderFake) listPods(ctx context.Context, namespace, selector string) (podListing, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, "list:"+namespace+":"+selector)
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 8*time.Second {
		fake.deadlineMissing = true
	}
	if fake.listError != nil {
		return podListing{}, fake.listError
	}
	listing := podListing{}
	for index := 0; index < fake.podCount; index++ {
		name := "scheduler-pod"
		container := "prow-controller-manager"
		if strings.Contains(selector, "prowjob-dispatcher") {
			name = "dispatcher-pod"
			container = "prowjob-dispatcher"
		}
		listing.pods = append(listing.pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + strings.Repeat("x", index), CreationTimestamp: metav1.NewTime(time.Unix(int64(index), 0))}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: container, Ready: true, RestartCount: fake.restarts}}}})
	}
	return listing, nil
}

// openLogs retains requested limits so tests can prove there are no arbitrary log targets.
func (fake *logReaderFake) openLogs(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls = append(fake.calls, "logs:"+namespace+":"+pod)
	fake.opts = append(fake.opts, opts)
	if opts.Previous && fake.previousError != nil {
		return nil, fake.previousError
	}
	if fake.logError != nil {
		return nil, fake.logError
	}
	data := fake.data
	if opts.Previous && fake.previousData != nil {
		data = *fake.previousData
	}
	return &countedLogStream{reader: strings.NewReader(data), fake: fake}, nil
}

// TestLogCollection covers authorization, absent pods, partial bounds and immutable log options.
func TestLogCollection(t *testing.T) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                        string
		pods                        int
		listErr, logErr             error
		data, status, errorContains string
		truncated                   bool
	}{
		{"available", 1, nil, nil, now.Add(-time.Minute).Format(time.RFC3339Nano) + ` {"controller":"scheduler","level":"info","msg":"Cluster assigned"}` + "\n", "available", "", false},
		{"missing pods", 0, nil, nil, "", "unavailable", "no matching pods", false},
		{"list forbidden", 1, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "hidden", errors.New("secret URL")), nil, "", "unavailable", "forbidden", false},
		{"logs forbidden", 1, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods/log"}, "hidden", errors.New("secret URL")), "", "unavailable", "forbidden", true},
		{"logs missing", 1, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods/log"}, "hidden"), "", "unavailable", "not found", true},
		{"pod limit", 3, nil, nil, "", "partial", "", true},
		{"byte limit", 1, nil, nil, strings.Repeat("x", int(defaultLogBytes/2)+1), "partial", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &logReaderFake{podCount: tc.pods, listError: tc.listErr, logError: tc.logErr, data: tc.data}
			clock := &diagnosticClock{now: now}
			collector := makeLogCollector(fake, "ci", clock.Now)
			response := collector.observe(context.Background(), 30*time.Minute)
			if !response.Enabled || len(response.Sources) != 2 {
				t.Fatalf("invalid response: %+v", response)
			}
			for _, source := range response.Sources {
				if source.Status != tc.status || source.Truncated != tc.truncated || !strings.Contains(source.Error, tc.errorContains) {
					t.Errorf("source = %+v", source)
				}
				if len(source.Pods) > logPodLimit {
					t.Error("too many pods read")
				}
				for _, pod := range source.Pods {
					if pod.Ready == nil || !*pod.Ready || pod.Restarts == nil || *pod.Restarts != 0 {
						t.Errorf("incorrect current pod metadata: %+v", pod)
					}
				}
				if strings.Contains(source.Error, "secret") || strings.Contains(source.Error, "hidden") {
					t.Error("raw API error leaked")
				}
			}
			if fake.deadlineMissing {
				t.Error("collection lacked its total deadline")
			}
			for _, opts := range fake.opts {
				if opts.Container != "prow-controller-manager" && opts.Container != "prowjob-dispatcher" {
					t.Error("unexpected container")
				}
				if opts.TailLines != nil || opts.LimitBytes == nil || *opts.LimitBytes <= 0 || *opts.LimitBytes > defaultLogBytes/2 || !opts.Timestamps || opts.Follow || opts.Previous || opts.SinceTime == nil || !opts.SinceTime.Time.Equal(now.Add(-30*time.Minute)) {
					t.Errorf("unbounded log options: %+v", opts)
				}
			}
		})
	}
}

// TestDiagnosticCache proves browser refreshes share reads and the two windows have bounded TTLs.
func TestDiagnosticCache(t *testing.T) {
	for _, tc := range []struct {
		name      string
		window    time.Duration
		elapsed   time.Duration
		wantCalls int
	}{
		{"same window cached", 30 * time.Minute, 29 * time.Second, 4},
		{"separate window collected", 0, 0, 8},
		{"exact TTL expires", 30 * time.Minute, 30 * time.Second, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			fake := &logReaderFake{podCount: 1}
			collector := makeLogCollector(fake, "ci", clock.Now)
			collector.observe(context.Background(), 30*time.Minute)
			clock.now = clock.now.Add(tc.elapsed)
			collector.observe(context.Background(), tc.window)
			if len(fake.calls) != tc.wantCalls {
				t.Errorf("reads = %d, want %d", len(fake.calls), tc.wantCalls)
			}
		})
	}
}

// TestConcurrentDiagnosticRequests verifies browsers share one collection without sleeping.
func TestConcurrentDiagnosticRequests(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requests  int
		canceled  bool
		wantReads int
	}{
		{"eight concurrent browsers", 8, false, 4},
		{"canceled request", 1, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			fake := &logReaderFake{podCount: 1}
			collector := makeLogCollector(fake, "ci", clock.Now)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			results := make(chan diagnosticResponse, tc.requests)
			for index := 0; index < tc.requests; index++ {
				go observeDiagnosticForTest(ctx, collector, results)
			}
			for index := 0; index < tc.requests; index++ {
				result := <-results
				if len(result.Sources) != 2 {
					t.Error("missing source coverage")
				}
			}
			if len(fake.calls) != tc.wantReads {
				t.Errorf("reads = %d, want %d", len(fake.calls), tc.wantReads)
			}
		})
	}
}

// observeDiagnosticForTest uses the real serialized cache in concurrent unit cases.
func observeDiagnosticForTest(ctx context.Context, collector *logCollector, results chan<- diagnosticResponse) {
	results <- collector.observe(ctx, 30*time.Minute)
}

// partialPodLogs exercises a rollout where only one of the two current containers can be read.
type partialPodLogs struct {
	logReaderFake
	fail bool
}

// openLogs fails the newer rollout pod without preventing the other pod's evidence from surviving.
func (fake *partialPodLogs) openLogs(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	if fake.fail && strings.HasSuffix(pod, "x") {
		return nil, errors.New("private Kubernetes endpoint")
	}
	return fake.logReaderFake.openLogs(ctx, namespace, pod, opts)
}

// TestPartialDiagnosticSource preserves coverage while distinguishing unavailable rollout logs.
func TestPartialDiagnosticSource(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fail        bool
		wantStatus  string
		wantScanned int
	}{
		{"both current pods readable", false, "available", 2},
		{"one current pod unreadable", true, "partial", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			fake := &partialPodLogs{logReaderFake: logReaderFake{podCount: 2, data: clock.now.Add(-time.Minute).Format(time.RFC3339Nano) + ` {"level":"info","msg":"ordinary information"}` + "\n"}, fail: tc.fail}
			collector := makeLogCollector(fake, "ci", clock.Now)
			response := collector.observe(context.Background(), 30*time.Minute)
			for _, source := range response.Sources {
				if source.Status != tc.wantStatus || source.ScannedLines != tc.wantScanned || source.CoveredFrom == nil {
					t.Errorf("incorrect partial evidence: %+v", source)
				}
				if strings.Contains(source.Error, "private") {
					t.Error("raw API error leaked")
				}
			}
		})
	}
}

// cancelingPodLogs simulates navigation canceling the first browser's in-flight collection.
type cancelingPodLogs struct {
	cancel    context.CancelFunc
	once      sync.Once
	completed sync.WaitGroup
}

// listPods cancels after collection starts and reports only the standard context failure.
func (fake *cancelingPodLogs) listPods(ctx context.Context, namespace, selector string) (podListing, error) {
	defer fake.completed.Done()
	fake.once.Do(fake.cancel)
	return podListing{}, ctx.Err()
}

// openLogs is never reached when pod discovery was canceled.
func (fake *cancelingPodLogs) openLogs(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	return nil, errors.New("unexpected log read")
}

// TestCanceledCollectionCache prevents one browser's navigation from poisoning shared results.
func TestCanceledCollectionCache(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{{"canceled recent window", 30 * time.Minute}, {"canceled available window", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fake := &cancelingPodLogs{cancel: cancel}
			fake.completed.Add(2)
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			collector := makeLogCollector(fake, "ci", clock.Now)
			collector.observe(ctx, tc.window)
			fake.completed.Wait()
			if len(collector.cache) != 0 {
				t.Fatal("canceled collection entered shared cache")
			}
			recovered := &logReaderFake{podCount: 1}
			collector.reader = recovered
			result := collector.observe(context.Background(), tc.window)
			if len(recovered.calls) != 4 {
				t.Fatalf("next browser did not collect fresh logs: %d calls", len(recovered.calls))
			}
			for _, source := range result.Sources {
				if source.Status != "available" {
					t.Errorf("recovery failed: %+v", source)
				}
			}
		})
	}
}

// TestDiagnosticRoutes verifies opt-in, explicit demo isolation, methods and selector rejection.
func TestDiagnosticRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, path, method string
		demo               bool
		wantStatus         int
		wantEnabled        bool
	}{
		{"disabled", "/api/v1/diagnostics", "GET", false, 200, false},
		{"demo", "/api/v1/diagnostics?window=30m", "GET", true, 200, true},
		{"available", "/api/v1/diagnostics?window=available", "GET", true, 200, true},
		{"six hours rejected", "/api/v1/diagnostics?window=6h", "GET", true, 400, false},
		{"twelve hours rejected", "/api/v1/diagnostics?window=12h", "GET", true, 400, false},
		{"custom rejected", "/api/v1/diagnostics?window=1m", "GET", true, 400, false},
		{"invalid window", "/api/v1/diagnostics?window=25h", "GET", true, 400, false},
		{"empty window", "/api/v1/diagnostics?window=", "GET", true, 400, false},
		{"duplicate window", "/api/v1/diagnostics?window=30m&window=6h", "GET", true, 400, false},
		{"invalid query encoding", "/api/v1/diagnostics?window=%zz", "GET", true, 400, false},
		{"arbitrary namespace", "/api/v1/diagnostics?namespace=private", "GET", true, 400, false},
		{"arbitrary pod", "/api/v1/diagnostics?pod=private", "GET", true, 400, false},
		{"read only", "/api/v1/diagnostics", "POST", true, 405, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := &dashboard{opts: options{demo: tc.demo}}
			response := httptest.NewRecorder()
			app.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if tc.wantStatus != 200 {
				return
			}
			var got diagnosticResponse
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.Enabled != tc.wantEnabled || got.Demo != tc.demo {
				t.Errorf("unexpected mode: %+v", got)
			}
			if got.RefreshIntervalSeconds != 0 || !got.RequestedTo.Equal(got.ObservedAt) {
				t.Errorf("incorrect manual window metadata: %+v", got)
			}
			if strings.Contains(tc.path, "window=available") && (got.Window != "available" || got.RequestedFrom != nil || got.WindowSeconds != 0) {
				t.Errorf("available mode has cutoff: %+v", got)
			}
			if tc.path == "/api/v1/diagnostics" && (got.WindowSeconds != 1800 || got.Window != "30m" || got.RequestedFrom == nil) {
				t.Error("default window is not 30 minutes")
			}
			if tc.demo {
				if len(got.Sources) != 2 || len(got.Sources[0].Issues) != 3 {
					t.Errorf("missing canned demo examples: %+v", got.Sources)
				}
			}
		})
	}
}

// TestDiagnosticModeValidation rejects live credential flags in explicit demo mode.
func TestDiagnosticModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts options
		want string
	}{
		{"demo live logs", options{demo: true, logs: logOptions{enabled: true}}, "--demo cannot be combined with live log diagnostics"},
		{"disabled kubeconfig", options{logs: logOptions{kubeconfig: "unread-secret-file"}}, "--log-kubeconfig requires"},
		{"demo kubeconfig", options{demo: true, logs: logOptions{kubeconfig: "unread-secret-file"}}, "--demo cannot be combined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newDashboard(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestDiagnosticWindow rejects all selectors and durations outside the two supported modes.
func TestDiagnosticWindow(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        time.Duration
		valid       bool
	}{
		{"default", "", 30 * time.Minute, true},
		{"recent", "window=30m", 30 * time.Minute, true},
		{"available", "window=available", 0, true},
		{"empty", "window=", 0, false},
		{"duplicate", "window=30m&window=available", 0, false},
		{"unsupported selector", "pod=private", 0, false},
		{"custom minute", "window=1m", 0, false},
		{"custom day", "window=24h", 0, false},
		{"six hours", "window=6h", 0, false},
		{"twelve hours", "window=12h", 0, false},
		{"equivalent seconds", "window=1800s", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := url.ParseQuery(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			got, err := diagnosticWindow(query)
			if (err == nil) != tc.valid || got != tc.want {
				t.Errorf("got %v, %v; want %v valid=%v", got, err, tc.want, tc.valid)
			}
		})
	}
}

// TestDiagnosticLimitReasons distinguishes conservative limit indicators without repeating parsing tests.
func TestDiagnosticLimitReasons(t *testing.T) {
	now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		pods   int
		data   string
		logErr error
		reason string
	}{
		{"pod cap", 3, "", nil, "pod-limit"},
		{"byte cap", 1, strings.Repeat("x", int(defaultLogBytes/2)), nil, "byte-limit"},
		{"group cap", 1, strings.Join(diagnosticGroupLines(now), "\n"), nil, "group-limit"},
		{"unreadable logs", 1, "", errors.New("private endpoint"), "read-incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := makeLogCollector(&logReaderFake{podCount: tc.pods, data: tc.data, logError: tc.logErr}, "ci", (&diagnosticClock{now: now}).Now)
			response := collector.observe(context.Background(), 30*time.Minute)
			source := response.Sources[1] // Sources are ordered dispatcher, scheduler.
			if source.ID != "scheduler" || !source.Truncated || !slices.Contains(source.TruncationReasons, tc.reason) {
				t.Errorf("missing limit indication: %+v", source)
			}
			count := len(source.TruncationReasons)
			diagnosticLimit(&source, tc.reason)
			if len(source.TruncationReasons) != count {
				t.Error("duplicate limit indicators")
			}
			if !response.RequestedTo.Equal(now) || (response.RequestedFrom == nil || !response.RequestedFrom.Equal(now.Add(-30*time.Minute))) || response.RefreshIntervalSeconds != 0 {
				t.Error("incorrect collection request metadata")
			}
		})
	}
}

// countedLogStream records bytes actually read even when the fake ignores Kubernetes LimitBytes.
type countedLogStream struct {
	reader io.Reader
	fake   *logReaderFake
}

// Read counts the transport bytes requested by the defensive reader.
func (stream *countedLogStream) Read(data []byte) (int, error) {
	count, err := stream.reader.Read(data)
	stream.fake.mu.Lock()
	stream.fake.downloaded += int64(count)
	stream.fake.mu.Unlock()
	return count, err
}

// Close has no external resource in the bounded fake stream.
func (*countedLogStream) Close() error { return nil }

func TestDiagnosticByteBudget(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pods        int
		restarts    int32
		budget      int64
		window      time.Duration
		wantStreams int
	}{
		{name: "current containers", pods: 1, budget: 1024, window: 30 * time.Minute, wantStreams: 2},
		{name: "rollout current and retained previous", pods: 2, restarts: 2, budget: 1024, window: 0, wantStreams: 8},
		{name: "odd total budget", pods: 2, restarts: 1, budget: 1025, window: 0, wantStreams: 8},
		{name: "tiny internal allowance skips zero requests", pods: 2, restarts: 1, budget: 2, window: 0, wantStreams: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			fake := &logReaderFake{podCount: tc.pods, restarts: tc.restarts, data: strings.Repeat("x", 2048)}
			collector := makeLogCollector(fake, "ci", clock.Now)
			collector.maxBytes = tc.budget
			response := collector.observe(context.Background(), tc.window)
			var reported int64
			for _, source := range response.Sources {
				reported += source.BytesRead
				if source.ByteLimit != tc.budget/2 || source.BytesRead > source.ByteLimit || !source.Truncated || !slices.Contains(source.TruncationReasons, "byte-limit") {
					t.Errorf("incorrect source budget: %+v", source)
				}
				for _, pod := range source.Pods {
					if tc.restarts > 0 && pod.PreviousLogs != "partial" && pod.PreviousLogs != "unavailable" {
						t.Errorf("missing prior coverage: %+v", pod)
					}
				}
			}
			if reported != fake.downloaded || fake.downloaded > tc.budget || response.ByteLimit != tc.budget {
				t.Errorf("downloaded=%d reported=%d totalLimit=%d responseLimit=%d", fake.downloaded, reported, tc.budget, response.ByteLimit)
			}
			if len(fake.opts) != tc.wantStreams {
				t.Errorf("got %d stream reads want %d", len(fake.opts), tc.wantStreams)
			}
			for _, opts := range fake.opts {
				if opts.TailLines != nil || opts.LimitBytes == nil || *opts.LimitBytes <= 0 || !opts.Timestamps || opts.Follow {
					t.Errorf("unbounded options: %+v", opts)
				}
				if (opts.SinceTime == nil) != (tc.window == 0) {
					t.Errorf("incorrect available lower bound: %+v", opts)
				}
			}
			if tc.window == 0 && (response.RequestedFrom != nil || response.Window != "available" || response.WindowSeconds != 0) {
				t.Errorf("incorrect available metadata: %+v", response)
			}
		})
	}
}

func TestPreviousLogCoverage(t *testing.T) {
	for _, tc := range []struct {
		name         string
		restarts     int32
		err          error
		want, status string
		wantStreams  int
	}{
		{name: "no restart", want: "not-needed", status: "available", wantStreams: 2},
		{name: "previous retained", restarts: 1, want: "collected", status: "available", wantStreams: 4},
		{name: "previous not found", restarts: 1, err: apierrors.NewNotFound(schema.GroupResource{Resource: "pods/log"}, "private"), want: "unavailable", status: "partial", wantStreams: 4},
		{name: "previous removed by runtime", restarts: 1, err: apierrors.NewBadRequest("previous terminated container private not found"), want: "unavailable", status: "partial", wantStreams: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}
			line := clock.now.Add(-time.Minute).Format(time.RFC3339Nano) + ` {"level":"error","msg":"connection refused"}` + "\n"
			fake := &logReaderFake{podCount: 1, restarts: tc.restarts, previousError: tc.err, data: line}
			response := makeLogCollector(fake, "ci", clock.Now).observe(context.Background(), 30*time.Minute)
			for _, source := range response.Sources {
				if source.Status != tc.status || len(source.Pods) != 1 || source.Pods[0].PreviousLogs != tc.want || source.CoveredFrom == nil {
					t.Errorf("incorrect retained coverage: %+v", source)
				}
				if tc.err != nil && (!slices.Contains(source.TruncationReasons, "previous-logs-unavailable") || strings.Contains(source.Error, "private")) {
					t.Errorf("incorrect previous-log error: %+v", source)
				}
				if source.BudgetNote == "" {
					t.Error("unused allowance has no explanation")
				}
			}
			if len(fake.opts) != tc.wantStreams {
				t.Errorf("got %d reads want %d", len(fake.opts), tc.wantStreams)
			}
		})
	}
}

func TestDiagnosticByteOptionValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes int64
		valid bool
	}{
		{"default", 0, true}, {"minimum", 1 << 20, true}, {"maximum", 64 << 20, true}, {"below", (1 << 20) - 1, false}, {"above", (64 << 20) + 1, false}, {"negative", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLogOptions(logOptions{maxBytes: tc.bytes}, false)
			if (err == nil) != tc.valid {
				t.Errorf("got %v valid=%v", err, tc.valid)
			}
		})
	}
}

func TestUnusedCurrentBudgetFundsPrevious(t *testing.T) {
	for _, tc := range []struct{ name, current string }{
		{name: "empty current"}, {name: "quiet current", current: "brief"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := strings.Repeat("x", 1024)
			fake := &logReaderFake{podCount: 1, restarts: 1, data: tc.current, previousData: &previous}
			collector := makeLogCollector(fake, "ci", (&diagnosticClock{now: time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)}).Now)
			collector.maxBytes = 1024
			response := collector.observe(context.Background(), 0)
			for _, source := range response.Sources {
				if source.BytesRead != 512 || source.Pods[0].PreviousLogs != "partial" {
					t.Errorf("unused allowance was not offered to previous logs: %+v", source)
				}
			}
			if fake.downloaded != 1024 {
				t.Errorf("downloaded %d want1024", fake.downloaded)
			}
		})
	}
}

// cancelingPreviousLogs fails a retained-log request after current evidence was parsed.
type cancelingPreviousLogs struct {
	logReaderFake
	cancel context.CancelFunc
}

// openLogs models a Kubernetes request that returns when its collection context ends.
func (fake *cancelingPreviousLogs) openLogs(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	if opts.Previous && strings.HasPrefix(pod, "scheduler-") {
		fake.cancel()
		return nil, ctx.Err()
	}
	return fake.logReaderFake.openLogs(ctx, namespace, pod, opts)
}

func TestCancellationPreservesCurrentEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{{"recent logs", 30 * time.Minute}, {"available logs", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
			line := now.Add(-time.Minute).Format(time.RFC3339Nano) + ` {"controller":"scheduler","level":"error","msg":"request failed","error":"connection refused"}` + "\n"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fake := &cancelingPreviousLogs{logReaderFake: logReaderFake{podCount: 1, restarts: 1, data: line}, cancel: cancel}
			collector := makeLogCollector(fake, "ci", (&diagnosticClock{now: now}).Now)
			response := collector.observe(ctx, tc.window)
			if len(response.Sources) != 2 {
				t.Fatalf("missing source coverage: %+v", response.Sources)
			}
			source := response.Sources[1]
			if source.ID != "scheduler" || source.Status != "partial" || source.BytesRead != int64(len(line)) || len(source.Issues) != 1 || source.Issues[0].Count != 1 || !slices.Contains(source.TruncationReasons, "collection-deadline") {
				t.Errorf("current evidence lost on cancellation: %+v", source)
			}
			if source.Error != "collection deadline exceeded or canceled" || len(collector.cache) != 0 {
				t.Errorf("canceled result error=%q, cache entries=%d", source.Error, len(collector.cache))
			}
		})
	}
}
