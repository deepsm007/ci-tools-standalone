package main

import (
	"bufio"
	"bytes"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// dispatcherHealth keeps unavailable metrics distinct from a reported zero counter.
type dispatcherHealth struct {
	ObservedAt                time.Time  `json:"observedAt"`
	MetricsAvailable          bool       `json:"metricsAvailable"`
	LastSuccessfulReconcileAt *time.Time `json:"lastSuccessfulReconcileAt"`
	LastBaselineUpdateAt      *time.Time `json:"lastBaselineUpdateAt"`
	ReconcileErrors           *float64   `json:"reconcileErrors"`
}

// serveHealth isolates optional metrics failure from observation and readiness routes.
func (app *dashboard) serveHealth(writer http.ResponseWriter, request *http.Request) {
	now := time.Now().UTC()
	if app.opts.demo {
		reconciled, baseline, failures := app.sample.generatedAt, app.sample.generatedAt.Add(-8*time.Minute), float64(3)
		writeJSON(writer, http.StatusOK, dispatcherHealth{now, true, &reconciled, &baseline, &failures})
		return
	}
	health := dispatcherHealth{ObservedAt: now}
	data, status, err := app.fetch(request, "/metrics", "")
	if err == nil && status == http.StatusOK {
		health = parseMetrics(data, now)
	}
	writeJSON(writer, http.StatusOK, health)
}

// parseMetrics reads only the three unlabelled health metrics introduced with observation.
func parseMetrics(data []byte, now time.Time) dispatcherHealth {
	health := dispatcherHealth{ObservedAt: now, MetricsAvailable: true}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			continue
		}
		switch fields[0] {
		case "prowjob_dispatcher_last_successful_reconcile_timestamp_seconds":
			health.LastSuccessfulReconcileAt = metricTime(value)
		case "prowjob_dispatcher_last_baseline_update_timestamp_seconds":
			health.LastBaselineUpdateAt = metricTime(value)
		case "prowjob_dispatcher_reconcile_errors_total":
			health.ReconcileErrors = &value
		}
	}
	if scanner.Err() != nil {
		return dispatcherHealth{ObservedAt: now}
	}
	return health
}

// metricTime maps the Prometheus uninitialized zero timestamp to unavailable.
func metricTime(value float64) *time.Time {
	if value <= 0 || value > float64(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Unix()) {
		return nil
	}
	seconds, fraction := math.Modf(value)
	timestamp := time.Unix(int64(seconds), int64(fraction*1e9)).UTC()
	return &timestamp
}
