package main

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// TestParseMetrics distinguishes absent and uninitialized timestamps from actual observations.
func TestParseMetrics(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	reconciled, baseline, failures := time.Unix(100, 0).UTC(), time.Unix(90, 500000000).UTC(), float64(3)
	zero := float64(0)
	for _, tc := range []struct {
		name, metrics string
		want          dispatcherHealth
	}{
		{"known metrics", "# HELP ignored\nprowjob_dispatcher_last_successful_reconcile_timestamp_seconds 100\nprowjob_dispatcher_last_baseline_update_timestamp_seconds 90.5\nprowjob_dispatcher_reconcile_errors_total 3\n", dispatcherHealth{now, true, &reconciled, &baseline, &failures}},
		{"missing metrics", "unrelated_metric 5\n", dispatcherHealth{ObservedAt: now, MetricsAvailable: true}},
		{"zero timestamps", "prowjob_dispatcher_last_successful_reconcile_timestamp_seconds 0\nprowjob_dispatcher_last_baseline_update_timestamp_seconds 0\nprowjob_dispatcher_reconcile_errors_total 0\n", dispatcherHealth{ObservedAt: now, MetricsAvailable: true, ReconcileErrors: &zero}},
		{"invalid values", "prowjob_dispatcher_last_successful_reconcile_timestamp_seconds NaN\nprowjob_dispatcher_last_baseline_update_timestamp_seconds +Inf\nprowjob_dispatcher_reconcile_errors_total -1\n", dispatcherHealth{ObservedAt: now, MetricsAvailable: true}},
		{"unrecognized labels", "prowjob_dispatcher_last_successful_reconcile_timestamp_seconds{cluster=\"other\"} 100\nprowjob_dispatcher_reconcile_errors_total_extra 3\n", dispatcherHealth{ObservedAt: now, MetricsAvailable: true}},
		{"out of range timestamp", "prowjob_dispatcher_last_successful_reconcile_timestamp_seconds 1e100\n", dispatcherHealth{ObservedAt: now, MetricsAvailable: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseMetrics([]byte(tc.metrics), now)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected health (-want +got):\n%s", diff)
			}
		})
	}
}

// TestMetricsFailure verifies optional metrics cannot take down the main observation API.
func TestMetricsFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		stub upstreamStub
	}{
		{"unreachable", upstreamStub{err: errors.New("unreachable")}},
		{"not found", upstreamStub{status: 404, body: "missing"}},
		{"unavailable", upstreamStub{status: 503, body: "unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := testDashboard(t, &tc.stub)
			response := httptest.NewRecorder()
			app.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/dispatcher/health", nil))
			if response.Code != 200 {
				t.Fatalf("health status = %d", response.Code)
			}
			var health dispatcherHealth
			if err := json.Unmarshal(response.Body.Bytes(), &health); err != nil {
				t.Fatal(err)
			}
			if health.MetricsAvailable || health.LastSuccessfulReconcileAt != nil || health.LastBaselineUpdateAt != nil || health.ReconcileErrors != nil {
				t.Fatalf("unexpected health: %+v", health)
			}
			if len(tc.stub.requests) != 1 || tc.stub.requests[0].URL.Path != "/metrics" {
				t.Fatal("metrics request was not confined to /metrics")
			}
			tc.stub.err, tc.stub.status, tc.stub.body = nil, 200, `{"ready":true}`
			state := httptest.NewRecorder()
			app.ServeHTTP(state, httptest.NewRequest("GET", "/api/v1/dispatcher/state", nil))
			if state.Code != 200 || state.Body.String() != `{"ready":true}` {
				t.Fatalf("state failed after metrics error: %d %s", state.Code, state.Body.String())
			}
		})
	}
}
