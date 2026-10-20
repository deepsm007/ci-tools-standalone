package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

// TestDemoJobs exercises the real upstream's generation-bound filter and pagination rules.
func TestDemoJobs(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	data := newDemoData(now)
	for _, tc := range []struct {
		name, query           string
		wantStatus, wantCount int
		wantNext              bool
	}{
		{"all", "", 200, 78, false},
		{"first page", "?limit=25", 200, 25, true},
		{"continuation", "?limit=500&after=" + data.jobs[24].Name + "&generation=1842", 200, 53, false},
		{"group exact", "?group=openshift%2Finstaller", 200, 13, false},
		{"group not substring", "?group=installer", 200, 0, false},
		{"name substring", "?name=machine-config-operator", 200, 13, false},
		{"effective drained cluster", "?cluster=build01", 200, 0, false},
		{"effective destination", "?cluster=build02", 200, 14, false},
		{"missing generation", "?after=anything", 400, 0, false},
		{"invalid generation", "?generation=abc", 400, 0, false},
		{"changed generation", "?generation=1841", 409, 0, false},
		{"zero limit", "?limit=0", 400, 0, false},
		{"large limit", "?limit=501", 400, 0, false},
		{"invalid limit", "?limit=many", 400, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/api/v1/dispatcher/jobs"+tc.query, nil)
			response := httptest.NewRecorder()
			data.serve(response, request, now)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if tc.wantStatus != 200 {
				return
			}
			var got struct {
				sampleMetadata
				Jobs      []sampleJob `json:"jobs"`
				NextAfter string      `json:"nextAfter"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Jobs) != tc.wantCount {
				t.Errorf("jobs = %d, want %d", len(got.Jobs), tc.wantCount)
			}
			if (got.NextAfter != "") != tc.wantNext {
				t.Errorf("nextAfter = %q, want continuation %t", got.NextAfter, tc.wantNext)
			}
			if got.Policy.Generation != demoGeneration || !got.Ready {
				t.Errorf("invalid metadata: %+v", got.sampleMetadata)
			}
		})
	}
}

// TestDemoConsistency checks aggregate load, serving counts and explicit expiry fallback.
func TestDemoConsistency(t *testing.T) {
	for _, tc := range []struct {
		name string
		now  time.Time
	}{
		{"fixed sample", time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := newDemoData(tc.now)
			counts, demand, serving := map[string]int{}, map[string]float64{}, map[string]int{}
			fallbacks := 0
			for _, job := range data.jobs {
				counts[job.Decision.Cluster]++
				demand[job.Decision.Cluster] += job.Demand
				if job.Decision.OverrideID != "" {
					serving[job.Decision.OverrideID]++
				}
				if job.Decision.Source == "expired-override-fallback" {
					fallbacks++
					if job.Decision.Cluster != job.BaselineCluster || job.Decision.OverrideID != "" || job.Decision.ValidUntil != nil {
						t.Errorf("invalid fallback: %+v", job)
					}
				}
			}
			if len(data.clusters) != 13 || len(data.jobs) != 78 || fallbacks == 0 {
				t.Fatalf("fixture size or fallback missing: %d clusters, %d jobs, %d fallbacks", len(data.clusters), len(data.jobs), fallbacks)
			}
			for _, cluster := range data.clusters {
				if cluster.Jobs != counts[cluster.Name] || cluster.Demand != demand[cluster.Name] {
					t.Errorf("inconsistent cluster: %+v", cluster)
				}
			}
			for _, override := range data.overrides {
				spec := override.Record["spec"].(map[string]any)
				id := spec["id"].(string)
				if override.ServingJobs != serving[id] {
					t.Errorf("inconsistent serving count for %s", id)
				}
				if len(override.Record["status"].(map[string]any)["history"].([]map[string]any)) < 2 {
					t.Errorf("missing audit history for %s", id)
				}
			}
		})
	}
}

// TestDemoExpiry verifies serving views fail back together at an override's exact expiry.
func TestDemoExpiry(t *testing.T) {
	started := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                    string
		elapsed                 time.Duration
		wantDrain, wantCapacity bool
	}{
		{"before expiry", 77 * time.Minute, true, true},
		{"at drain expiry", 78 * time.Minute, false, true},
		{"after capacity expiry", 109 * time.Minute, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := newDemoData(started)
			now := started.Add(tc.elapsed)
			counts, demand, serving := map[string]int{}, map[string]float64{}, map[string]int{}
			for _, job := range data.effectiveJobs(now) {
				counts[job.Decision.Cluster]++
				demand[job.Decision.Cluster] += job.Demand
				if job.Decision.OverrideID != "" {
					serving[job.Decision.OverrideID]++
				}
				if !tc.wantDrain && job.BaselineCluster == "build01" && (job.Decision.Cluster != "build01" || job.Decision.Source != "expired-override-fallback" || job.Decision.ValidUntil != nil) {
					t.Errorf("drain failed to expire: %+v", job)
				}
			}
			if (serving["drain-build01"] > 0) != tc.wantDrain || (serving["capacity-build05"] > 0) != tc.wantCapacity {
				t.Errorf("serving counts = %v", serving)
			}
			for _, cluster := range data.effectiveClusters(now) {
				if cluster.Jobs != counts[cluster.Name] || cluster.Demand != demand[cluster.Name] {
					t.Errorf("inconsistent expired cluster: %+v", cluster)
				}
			}
			for _, override := range data.effectiveOverrides(now) {
				id := override.Record["spec"].(map[string]any)["id"].(string)
				if override.ServingJobs != serving[id] {
					t.Errorf("inconsistent expired override %s", id)
				}
			}
			if data.jobs[0].Decision.PolicyGeneration != demoGeneration {
				t.Fatal("expiry changed immutable generation")
			}
			if data.effectiveJobs(started)[0].Decision != data.jobs[0].Decision {
				t.Fatal("expiry mutated original policy")
			}
		})
	}
}
