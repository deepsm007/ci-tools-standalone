package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// logOptions enables only trusted command-line Kubernetes configuration.
type logOptions struct {
	enabled    bool
	namespace  string
	kubeconfig string
	maxBytes   int64
}

// logPodLimit bounds metadata discovery during a rollout.
const logPodLimit = 2

// defaultLogBytes is the total download budget shared by both components.
const defaultLogBytes int64 = 8 << 20

// maximumLogBytes bounds the configured total download and parser memory.
const maximumLogBytes int64 = 64 << 20

// diagnosticResponse distinguishes disabled, demo, and live observations.
type diagnosticResponse struct {
	Enabled                bool               `json:"enabled"`
	Demo                   bool               `json:"demo,omitempty"`
	ObservedAt             time.Time          `json:"observedAt"`
	RequestedFrom          *time.Time         `json:"requestedFrom"`
	RequestedTo            time.Time          `json:"requestedTo"`
	Window                 string             `json:"window"`
	ByteLimit              int64              `json:"byteLimit"`
	WindowSeconds          int64              `json:"windowSeconds"`
	RefreshIntervalSeconds int                `json:"refreshIntervalSeconds"`
	Sources                []diagnosticSource `json:"sources"`
}

// diagnosticSource reports collection coverage independently of matching issues.
type diagnosticSource struct {
	canceled          bool
	ID                string            `json:"id"`
	BytesRead         int64             `json:"bytesRead"`
	ByteLimit         int64             `json:"byteLimit"`
	BudgetNote        string            `json:"budgetNote,omitempty"`
	Status            string            `json:"status"`
	Error             string            `json:"error,omitempty"`
	CoveredFrom       *time.Time        `json:"coveredFrom"`
	CoveredTo         *time.Time        `json:"coveredTo"`
	ScannedLines      int               `json:"scannedLines"`
	MatchedLines      int               `json:"matchedLines"`
	Truncated         bool              `json:"truncated"`
	TruncationReasons []string          `json:"truncationReasons"`
	GroupsLimited     bool              `json:"groupsLimited"`
	Pods              []diagnosticPod   `json:"pods"`
	Issues            []diagnosticIssue `json:"issues"`
}

// diagnosticPod reports main-container readiness, restarts and retained previous-log coverage.
type diagnosticPod struct {
	Name         string `json:"name"`
	PreviousLogs string `json:"previousLogs"`
	Ready        *bool  `json:"ready,omitempty"`
	Restarts     *int32 `json:"restarts,omitempty"`
}

// diagnosticIssue contains selected, redacted fields, never a raw log record.
type diagnosticIssue struct {
	ID           string    `json:"id"`
	Category     string    `json:"category"`
	Level        string    `json:"level"`
	Message      string    `json:"message"`
	Detail       string    `json:"detail,omitempty"`
	Job          string    `json:"job,omitempty"`
	Family       string    `json:"family,omitempty"`
	Cluster      string    `json:"cluster,omitempty"`
	ProwJob      string    `json:"prowJob,omitempty"`
	Component    string    `json:"component,omitempty"`
	Pod          string    `json:"pod"`
	Count        int       `json:"count"`
	FirstAt      time.Time `json:"firstAt"`
	LastAt       time.Time `json:"lastAt"`
	DelaySeconds *float64  `json:"delaySeconds,omitempty"`
}

// logSource fixes selectors and containers independently of browser input.
type logSource struct{ id, selector, container string }

// logSources returns fresh immutable definitions, avoiding configurable discovery targets.
func logSources() []logSource {
	return []logSource{{"scheduler", "component=prow-controller-manager", "prow-controller-manager"}, {"dispatcher", "component=prowjob-dispatcher", "prowjob-dispatcher"}}
}

// podListing records whether bounded metadata discovery omitted any matching pods.
type podListing struct {
	pods []corev1.Pod
	more bool
}

// podLogReader is the minimal read-only boundary needed by the collector.
type podLogReader interface {
	listPods(context.Context, string, string) (podListing, error)
	openLogs(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error)
}

// kubernetesPodLogs adapts the existing typed core client without importing every clientset.
type kubernetesPodLogs struct{ core coreclient.CoreV1Interface }

// listPods bounds discovery to one extra pod so omitted coverage can be disclosed.
func (reader kubernetesPodLogs) listPods(ctx context.Context, namespace, selector string) (podListing, error) {
	list, err := reader.core.Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: logPodLimit + 1})
	if err != nil {
		return podListing{}, err
	}
	return podListing{list.Items, list.Continue != ""}, nil
}

// openLogs can only issue GET pods/log with bounded, fixed container options.
func (reader kubernetesPodLogs) openLogs(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	return reader.core.Pods(namespace).GetLogs(pod, opts).Stream(ctx)
}

// cachedDiagnostics expires short-lived click-burst results independently for each duration.
type cachedDiagnostics struct {
	expiresAt time.Time
	response  diagnosticResponse
}

// logCollector shares bounded cached results across browsers and serializes cache misses.
type logCollector struct {
	reader    podLogReader
	namespace string
	gate      chan struct{}
	cache     map[time.Duration]cachedDiagnostics
	now       func() time.Time
	maxBytes  int64
}

// validateLogOptions prevents disabled or demo configurations from accidentally mixing live sources.
func validateLogOptions(opts logOptions, demo bool) error {
	if demo && (opts.enabled || opts.kubeconfig != "") {
		return errors.New("--demo cannot be combined with live log diagnostics")
	}
	if !opts.enabled && opts.kubeconfig != "" {
		return errors.New("--log-kubeconfig requires --enable-log-diagnostics")
	}
	if opts.maxBytes != 0 && (opts.maxBytes < 1<<20 || opts.maxBytes > maximumLogBytes) {
		return errors.New("--log-max-bytes must be between 1048576 and 67108864")
	}
	return nil
}

// newLogCollector creates only a read-only Kubernetes reader.
func newLogCollector(opts logOptions) (*logCollector, error) {
	if err := validateLogOptions(opts, false); err != nil {
		return nil, err
	}
	if opts.namespace == "" {
		opts.namespace = "ci"
	}
	if len(validation.IsDNS1123Label(opts.namespace)) != 0 {
		return nil, errors.New("--log-namespace must be a valid Kubernetes namespace")
	}
	reader, err := newKubernetesLogReader(opts)
	if err != nil {
		return nil, err
	}
	collector := makeLogCollector(reader, opts.namespace, time.Now)
	if opts.maxBytes != 0 {
		collector.maxBytes = opts.maxBytes
	}
	return collector, nil
}

// newKubernetesLogReader uses only trusted server-side credentials.
func newKubernetesLogReader(opts logOptions) (podLogReader, error) {
	var config *rest.Config
	var err error
	if opts.kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", opts.kubeconfig)
	}
	if opts.kubeconfig == "" {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, errors.New("could not load Kubernetes credentials for log diagnostics")
	}
	config.Timeout = 8 * time.Second
	client, err := coreclient.NewForConfig(config)
	if err != nil {
		return nil, errors.New("could not create Kubernetes log reader")
	}
	return kubernetesPodLogs{client}, nil
}

// makeLogCollector provides the same cache and limits to production and deterministic tests.
func makeLogCollector(reader podLogReader, namespace string, now func() time.Time) *logCollector {
	return &logCollector{reader: reader, namespace: namespace, maxBytes: defaultLogBytes, gate: make(chan struct{}, 1), cache: make(map[time.Duration]cachedDiagnostics), now: now}
}

// serveDiagnostics accepts only a bounded time window, never pod or credential selectors.
func (app *dashboard) serveDiagnostics(writer http.ResponseWriter, request *http.Request) {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid diagnostics query")
		return
	}
	window, err := diagnosticWindow(query)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	response := newDiagnosticResponse(now, window, defaultLogBytes)
	if app.opts.demo {
		writeJSON(writer, http.StatusOK, demoDiagnostics(now, window))
		return
	}
	if app.diagnostics == nil {
		writeJSON(writer, http.StatusOK, response)
		return
	}
	writeJSON(writer, http.StatusOK, app.diagnostics.observe(request.Context(), window))
}

// diagnosticWindow exposes exactly the two manual collection modes.
func diagnosticWindow(query url.Values) (time.Duration, error) {
	for key := range query {
		if key != "window" {
			return 0, errors.New("only window is supported")
		}
	}
	values, exists := query["window"]
	if !exists {
		return 30 * time.Minute, nil
	}
	if len(values) == 1 {
		switch values[0] {
		case "30m":
			return 30 * time.Minute, nil
		case "available":
			return 0, nil
		}
	}
	return 0, errors.New("window must be 30m or available")
}

// newDiagnosticResponse makes an absent lower time cutoff explicit for available mode.
func newDiagnosticResponse(now time.Time, window time.Duration, byteLimit int64) diagnosticResponse {
	response := diagnosticResponse{ObservedAt: now, RequestedTo: now, Window: "available", ByteLimit: byteLimit, Sources: []diagnosticSource{}}
	if window != 0 {
		since := now.Add(-window)
		response.RequestedFrom = &since
		response.Window = "30m"
		response.WindowSeconds = int64(window / time.Second)
	}
	return response
}

// observe bounds cache waits and Kubernetes collection together.
func (collector *logCollector) observe(ctx context.Context, window time.Duration) diagnosticResponse {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return unavailableDiagnostics(collector.now().UTC(), window, "collection canceled", collector.maxBytes)
	}
	select {
	case collector.gate <- struct{}{}:
		defer collector.unlock()
	case <-ctx.Done():
		return unavailableDiagnostics(collector.now().UTC(), window, "collection deadline exceeded", collector.maxBytes)
	}
	now := collector.now().UTC()
	for key, cached := range collector.cache {
		if !now.Before(cached.expiresAt) {
			delete(collector.cache, key)
		}
	}
	if cached, exists := collector.cache[window]; exists && now.Before(cached.expiresAt) {
		return cached.response
	}
	response := newDiagnosticResponse(now, window, collector.maxBytes)
	response.Enabled = true
	results := make(chan diagnosticSource, 2)
	for _, source := range logSources() {
		go collector.collectSource(ctx, source, now, window, results)
	}
	// Readers and parsing honor ctx; join workers before releasing the gate so
	// cancellation preserves their partial evidence and cannot overlap new work.
	for range logSources() {
		response.Sources = append(response.Sources, <-results)
	}

	slices.SortFunc(response.Sources, compareDiagnosticSources)
	canceled := ctx.Err() != nil
	for _, source := range response.Sources {
		canceled = canceled || source.canceled
	}
	if !canceled {

		collector.cache[window] = cachedDiagnostics{collector.now().UTC().Add(30 * time.Second), response}
	}
	return response
}

// unlock releases serialized cache access after collection and response assembly.
func (collector *logCollector) unlock() { <-collector.gate }

// compareDiagnosticSources gives browser responses a stable source order.
func compareDiagnosticSources(a, b diagnosticSource) int { return strings.Compare(a.ID, b.ID) }

// unavailableDiagnostics does not misrepresent a deadline as an issue-free observation.
func unavailableDiagnostics(now time.Time, window time.Duration, message string, maxBytes int64) diagnosticResponse {
	response := newDiagnosticResponse(now, window, maxBytes)
	response.Enabled = true
	for _, source := range logSources() {
		result := emptyDiagnosticSource(source.id)
		result.ByteLimit = maxBytes / int64(len(logSources()))
		result.Error = message
		response.Sources = append(response.Sources, result)
	}
	return response
}

// emptyDiagnosticSource keeps missing coverage explicit in JSON.
func emptyDiagnosticSource(id string) diagnosticSource {
	return diagnosticSource{ID: id, Status: "unavailable", TruncationReasons: []string{}, Pods: []diagnosticPod{}, Issues: []diagnosticIssue{}}
}

// diagnosticLimit records conservative limit indicators without claiming full historical coverage.
func diagnosticLimit(source *diagnosticSource, reason string) {
	source.Truncated = true
	if !slices.Contains(source.TruncationReasons, reason) {
		source.TruncationReasons = append(source.TruncationReasons, reason)
	}
}

// compareLogPods selects the newest discovered pods first during rolling deployments.
func compareLogPods(a, b corev1.Pod) int {
	if result := b.CreationTimestamp.Compare(a.CreationTimestamp.Time); result != 0 {
		return result
	}
	return strings.Compare(a.Name, b.Name)
}

// collectSource keeps component reads independent within their equal reserved budgets.
func (collector *logCollector) collectSource(ctx context.Context, source logSource, now time.Time, window time.Duration, results chan<- diagnosticSource) {
	result := collector.collectPods(ctx, source, now, window)
	result.canceled = ctx.Err() != nil
	results <- result
}

// logStream identifies one fixed main-container instance and its metadata row.
type logStream struct {
	pod      int
	previous bool
}

// collectPods prioritizes current containers before reading retained previous instances.
func (collector *logCollector) collectPods(ctx context.Context, source logSource, now time.Time, window time.Duration) diagnosticSource {
	result := emptyDiagnosticSource(source.id)
	result.ByteLimit = collector.maxBytes / int64(len(logSources()))
	listing, err := collector.reader.listPods(ctx, collector.namespace, source.selector)
	if err != nil {
		result.Error = logReadError("pod discovery", err)
		return result
	}
	if len(listing.pods) == 0 {
		result.Error = "no matching pods found"
		return result
	}
	slices.SortFunc(listing.pods, compareLogPods)
	if listing.more || len(listing.pods) > logPodLimit {
		diagnosticLimit(&result, "pod-limit")
	}
	if len(listing.pods) > logPodLimit {
		listing.pods = listing.pods[:logPodLimit]
	}
	streams := make([]logStream, 0, 2*len(listing.pods))
	for index, pod := range listing.pods {
		ready, restarts := false, int32(0)
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				ready = condition.Status == corev1.ConditionTrue
			}
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == source.container {
				restarts = container.RestartCount
			}
		}
		result.Pods = append(result.Pods, diagnosticPod{Name: pod.Name, Ready: &ready, Restarts: &restarts, PreviousLogs: "not-needed"})
		streams = append(streams, logStream{pod: index})
	}
	for index, pod := range result.Pods {
		if *pod.Restarts > 0 {
			result.Pods[index].PreviousLogs = "unavailable"
			streams = append(streams, logStream{pod: index, previous: true})
		}
	}
	successful := 0
	groups := make(map[string]*diagnosticIssue)
	for index, target := range streams {
		if ctx.Err() != nil {
			break
		}
		limit := (result.ByteLimit - result.BytesRead) / int64(len(streams)-index)
		if limit <= 0 {
			diagnosticLimit(&result, "byte-limit")
			continue
		}
		pod := &result.Pods[target.pod]
		opts := &corev1.PodLogOptions{Container: source.container, LimitBytes: &limit, Timestamps: true, Previous: target.previous}
		since := time.Time{}
		if window != 0 {
			since = now.Add(-window)
			value := metav1.NewTime(since)
			opts.SinceTime = &value
		}
		stream, err := collector.reader.openLogs(ctx, collector.namespace, pod.Name, opts)
		if err != nil {
			operation := "pod logs"
			reason := "read-incomplete"
			if target.previous {
				operation = "previous pod logs"
				reason = "previous-logs-unavailable"
			}
			result.Error = logReadError(operation, err)
			diagnosticLimit(&result, reason)
			continue
		}
		// Never probe one byte beyond the reserved stream allowance.
		data, readErr := io.ReadAll(io.LimitReader(stream, limit))
		closeErr := stream.Close()
		result.BytesRead += int64(len(data))
		incomplete := readErr != nil || closeErr != nil
		capped := int64(len(data)) >= limit
		if incomplete {
			result.Error = "pod logs could not be read completely"
			diagnosticLimit(&result, "read-incomplete")
		}
		if capped {
			diagnosticLimit(&result, "byte-limit")
		}
		if readErr == nil || len(data) > 0 {
			successful++
		}
		if target.previous {
			pod.PreviousLogs = "collected"
			if incomplete || capped {
				pod.PreviousLogs = "partial"
			}
		}
		parseDiagnosticLogs(ctx, data, source.id, pod.Name, since, now, &result, groups)
	}
	if ctx.Err() != nil {
		result.Error = "collection deadline exceeded or canceled"
		diagnosticLimit(&result, "collection-deadline")
	}
	if result.BytesRead < result.ByteLimit {
		result.BudgetNote = "The remaining byte allowance is divided fairly among unread current and previous streams. Earlier unused allowances fund later streams; Kubernetes retention, missing streams, and the collection deadline can leave budget unused."
	}
	if successful > 0 {
		result.Status = "available"
	}
	if successful > 0 && (successful < len(streams) || result.Error != "" || result.Truncated || result.GroupsLimited) {
		result.Status = "partial"
	}
	for _, issue := range groups {
		result.Issues = append(result.Issues, *issue)
	}
	slices.SortFunc(result.Issues, compareDiagnosticIssues)
	return result
}

// logReadError exposes authorization and availability classes without server URLs or credentials.
func logReadError(operation string, err error) string {
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		return operation + " forbidden by Kubernetes authorization"
	}
	if apierrors.IsNotFound(err) {
		return operation + " unavailable (pod or container not found)"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return operation + " deadline exceeded or canceled"
	}
	return operation + " unavailable"
}

// compareDiagnosticIssues keeps the latest matching evidence first with stable tie-breaking.
func compareDiagnosticIssues(a, b diagnosticIssue) int {
	if !a.LastAt.Equal(b.LastAt) {
		return b.LastAt.Compare(a.LastAt)
	}
	return strings.Compare(a.ID, b.ID)
}
