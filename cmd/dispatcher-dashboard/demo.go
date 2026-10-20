package main

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// demoGeneration is stable while a demo process serves its immutable sample policy.
const demoGeneration = uint64(1842)

// demoDigest identifies sample policy inputs without resembling a live secret.
const demoDigest = "8c5d3b6fa0e2d7904d1b83a764cca96117d22ba670156cb678f24676aa839ed1"

// samplePolicy mirrors the public policy metadata, independently of ci-tools internals.
type samplePolicy struct {
	Generation  uint64    `json:"generation"`
	Digest      string    `json:"digest"`
	Checksum    string    `json:"checksum"`
	GeneratedAt time.Time `json:"generatedAt"`
	OverrideIDs []string  `json:"overrideIDs"`
}

// sampleMetadata preserves the observation API's flattened metadata shape.
type sampleMetadata struct {
	Ready      bool         `json:"ready"`
	ObservedAt time.Time    `json:"observedAt"`
	Policy     samplePolicy `json:"policy"`
}

// sampleCluster describes configured capacity and estimated policy demand, not physical use.
type sampleCluster struct {
	Name               string   `json:"name"`
	InventoryKnown     bool     `json:"inventoryKnown"`
	Provider           string   `json:"provider"`
	Capabilities       []string `json:"capabilities"`
	ConfiguredCapacity int      `json:"configuredCapacity"`
	Blocked            bool     `json:"blocked"`
	Jobs               int      `json:"jobs"`
	Demand             float64  `json:"demand"`
}

// sampleDecision preserves the serving API's placement source and expiry explanation.
type sampleDecision struct {
	Cluster          string     `json:"cluster"`
	Source           string     `json:"source"`
	PolicyGeneration uint64     `json:"policyGeneration"`
	PolicyDigest     string     `json:"policyDigest"`
	OverrideID       string     `json:"overrideID,omitempty"`
	ValidUntil       *time.Time `json:"validUntil,omitempty"`
	Explanation      string     `json:"explanation"`
}

// sampleJob is the actual observation job contract used by demo filtering and aggregation.
type sampleJob struct {
	Name            string         `json:"name"`
	Group           string         `json:"group"`
	Capabilities    []string       `json:"capabilities"`
	Demand          float64        `json:"demand"`
	BaselineCluster string         `json:"baselineCluster"`
	Decision        sampleDecision `json:"decision"`
}

// sampleOverride pairs durable lifecycle history with publication and effective serving count.
type sampleOverride struct {
	Record      map[string]any `json:"record"`
	Published   bool           `json:"published"`
	ServingJobs int            `json:"servingJobs"`
}

// demoData holds an immutable realistic policy, keeping repeated page requests consistent.
type demoData struct {
	generatedAt time.Time
	jobs        []sampleJob
	clusters    []sampleCluster
	overrides   []sampleOverride
}

// newDemoData creates 78 jobs across 13 clusters with active and historical controls.
func newDemoData(now time.Time) *demoData {
	data := &demoData{generatedAt: now.Add(-12 * time.Minute)}
	providers := []string{"aws", "aws", "gcp", "gcp", "aws", "aws", "azure", "azure", "gcp", "gcp", "aws", "aws", "azure"}
	for index, provider := range providers {
		data.clusters = append(data.clusters, sampleCluster{Name: fmt.Sprintf("build%02d", index+1), InventoryKnown: true, Provider: provider, Capabilities: []string{"amd64", "privileged"}, ConfiguredCapacity: 100})
	}
	// A separate blocked inventory entry remains visible even with no effective jobs.
	data.clusters[12].Blocked = true
	groups := []string{"openshift/installer", "openshift/origin", "openshift/release", "openshift/cluster-api", "openshift/machine-config-operator", "openshift/console"}
	for index := 0; index < 78; index++ {
		clusterIndex := index % 12 // build13 is blocked and receives no baseline assignments.
		cluster := data.clusters[clusterIndex].Name
		group := groups[index%len(groups)]
		job := sampleJob{Name: fmt.Sprintf("periodic-ci-%s-main-e2e-%03d", strings.ReplaceAll(group, "/", "-"), index+1), Group: group, Capabilities: []string{"amd64", "privileged"}, Demand: float64(1 + index%5), BaselineCluster: cluster, Decision: sampleDecision{Cluster: cluster, Source: "baseline", PolicyGeneration: demoGeneration, PolicyDigest: demoDigest, Explanation: "using Git-materialized baseline assignment"}}
		switch clusterIndex {
		case 0:
			job.Decision.Cluster, job.Decision.Source, job.Decision.OverrideID = "build02", "runtime-override", "drain-build01"
			expires := now.Add(78 * time.Minute)
			job.Decision.ValidUntil, job.Decision.Explanation = &expires, "drain override drain-build01 moved workload from build01"
		case 4:
			if index%24 == 4 {
				job.Decision.Cluster, job.Decision.Source, job.Decision.OverrideID = "build06", "runtime-override", "capacity-build05"
				expires := now.Add(108 * time.Minute)
				job.Decision.ValidUntil, job.Decision.Explanation = &expires, "capacity override capacity-build05 moved workload from build05"
			}
		case 8:
			job.Decision.Source, job.Decision.Explanation = "expired-override-fallback", "runtime override expired; using Git baseline"
		}
		data.jobs = append(data.jobs, job)
		for clusterIndex := range data.clusters {
			if data.clusters[clusterIndex].Name == job.Decision.Cluster {
				data.clusters[clusterIndex].Jobs++
				data.clusters[clusterIndex].Demand += job.Demand
			}
		}
	}
	slices.SortFunc(data.jobs, compareSampleJobs)
	data.overrides = sampleOverrides(now, data.jobs)
	return data
}

// compareSampleJobs gives pagination the same lexical name order as the live API.
func compareSampleJobs(left, right sampleJob) int {
	return strings.Compare(left.Name, right.Name)
}

// metadata captures observation time while retaining the sample's immutable policy generation.
func (data *demoData) metadata(now time.Time) sampleMetadata {
	return sampleMetadata{true, now, samplePolicy{demoGeneration, demoDigest, "9a97dc51a23e807dfb4dd6798f13f671007c286cd01219d8931a90be53fea2c8", data.generatedAt, []string{"capacity-build05", "drain-build01", "expired-build09"}}}
}

// serve supplies only sample observation endpoints, with no implicit fallback from live mode.
func (data *demoData) serve(writer http.ResponseWriter, request *http.Request, now time.Time) {
	switch request.URL.Path {
	case "/api/v1/dispatcher/state":
		clusters := data.effectiveClusters(now)
		demand := float64(0)
		for _, cluster := range clusters {
			demand += cluster.Demand
		}
		writeJSON(writer, http.StatusOK, struct {
			sampleMetadata
			Jobs     int             `json:"jobs"`
			Demand   float64         `json:"demand"`
			Clusters []sampleCluster `json:"clusters"`
		}{data.metadata(now), len(data.jobs), demand, clusters})
	case "/api/v1/dispatcher/jobs":
		data.serveJobs(writer, request.URL.Query(), now)
	case "/api/v1/dispatcher/overrides":
		writeJSON(writer, http.StatusOK, struct {
			sampleMetadata
			Overrides []sampleOverride `json:"overrides"`
		}{data.metadata(now), data.effectiveOverrides(now)})
	}
}

// effectiveJobs mirrors expiry fallback without changing the immutable sample generation.
func (data *demoData) effectiveJobs(now time.Time) []sampleJob {
	jobs := slices.Clone(data.jobs)
	for index := range jobs {
		decision := &jobs[index].Decision
		if decision.OverrideID == "" || decision.ValidUntil == nil || now.Before(*decision.ValidUntil) {
			continue
		}
		decision.Cluster, decision.Source = jobs[index].BaselineCluster, "expired-override-fallback"
		decision.OverrideID, decision.ValidUntil = "", nil
		decision.Explanation = "runtime override expired; using Git baseline"
	}
	return jobs
}

// effectiveClusters aggregates the same time-dependent decisions exposed by the jobs route.
func (data *demoData) effectiveClusters(now time.Time) []sampleCluster {
	clusters := slices.Clone(data.clusters)
	for index := range clusters {
		clusters[index].Jobs, clusters[index].Demand = 0, 0
	}
	for _, job := range data.effectiveJobs(now) {
		for index := range clusters {
			if clusters[index].Name == job.Decision.Cluster {
				clusters[index].Jobs++
				clusters[index].Demand += job.Demand
			}
		}
	}
	return clusters
}

// effectiveOverrides leaves durable status untouched while refreshing actual serving counts.
func (data *demoData) effectiveOverrides(now time.Time) []sampleOverride {
	counts := make(map[string]int)
	for _, job := range data.effectiveJobs(now) {
		if job.Decision.OverrideID != "" {
			counts[job.Decision.OverrideID]++
		}
	}
	overrides := slices.Clone(data.overrides)
	for index := range overrides {
		spec := overrides[index].Record["spec"].(map[string]any)
		overrides[index].ServingJobs = counts[spec["id"].(string)]
	}
	return overrides
}

// serveJobs implements the upstream filter and pagination rules for explicit sample mode.
func (data *demoData) serveJobs(writer http.ResponseWriter, query url.Values, now time.Time) {
	limit := 100
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(writer, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		limit = parsed
	}
	if query.Get("after") != "" && query.Get("generation") == "" {
		writeError(writer, http.StatusBadRequest, "generation is required with after")
		return
	}
	if value := query.Get("generation"); value != "" {
		generation, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid generation")
			return
		}
		if generation != demoGeneration {
			writeError(writer, http.StatusConflict, "serving policy generation changed; restart pagination")
			return
		}
	}
	jobs, nextAfter := []sampleJob{}, ""
	for _, job := range data.effectiveJobs(now) {
		if job.Name <= query.Get("after") || !strings.Contains(job.Name, query.Get("name")) || query.Get("group") != "" && job.Group != query.Get("group") || query.Get("cluster") != "" && job.Decision.Cluster != query.Get("cluster") {
			continue
		}
		if len(jobs) == limit {
			nextAfter = jobs[len(jobs)-1].Name
			break
		}
		jobs = append(jobs, job)
	}
	writeJSON(writer, http.StatusOK, struct {
		sampleMetadata
		Jobs      []sampleJob `json:"jobs"`
		NextAfter string      `json:"nextAfter,omitempty"`
	}{data.metadata(now), jobs, nextAfter})
}

// sampleOverrides shows durable desired policy and its audit history separately from serving.
func sampleOverrides(now time.Time, jobs []sampleJob) []sampleOverride {
	definitions := []struct {
		id, kind, cluster, state, reason string
		published                        bool
		expires                          time.Duration
	}{
		{"capacity-build05", "Capacity", "build05", "Active", "Reduce configured capacity during storage maintenance", true, 108 * time.Minute},
		{"drain-build01", "Drain", "build01", "Active", "Drain for a planned control plane upgrade", true, 78 * time.Minute},
		{"expired-build09", "Drain", "build09", "Expired", "Temporary network investigation ended; expiry fallback uses baseline", true, -2 * time.Minute},
		{"failed-build07", "Drain", "build07", "Failed", "No eligible destination for a scoped workload", false, 50 * time.Minute},
		{"pending-build11", "Capacity", "build11", "Pending", "Awaiting a second approval for reduced capacity", false, 120 * time.Minute},
		{"revoked-build03", "Drain", "build03", "Revoked", "Maintenance canceled by incident owner", false, 40 * time.Minute},
	}
	result := make([]sampleOverride, 0, len(definitions))
	for _, definition := range definitions {
		startsAt, expiresAt := now.Add(-time.Hour), now.Add(definition.expires)
		approvals := []map[string]any{{"userID": "operator-alex", "at": startsAt.Add(-5 * time.Minute)}}
		if definition.state != "Pending" {
			approvals = append(approvals, map[string]any{"userID": "operator-sam", "at": startsAt.Add(-3 * time.Minute)})
		}
		spec := map[string]any{"id": definition.id, "planID": "plan-" + definition.id, "sourceGeneration": demoGeneration - 1, "policyInputDigest": demoDigest, "kind": definition.kind, "cluster": definition.cluster, "scope": map[string]string{}, "startsAt": startsAt, "expiresAt": expiresAt, "createdBy": "operator-alex", "sourceChannelID": "ci-infrastructure", "reason": definition.reason, "incidentURL": "https://issues.redhat.com/browse/DPTP-5481", "approvals": approvals, "requiredApprovals": 2, "idempotencyKey": "demo-" + definition.id}
		if definition.kind == "Capacity" {
			spec["capacity"] = 40
		}
		if definition.state == "Revoked" {
			spec["revokedAt"], spec["revokedBy"] = now.Add(-20*time.Minute), "operator-sam"
		}
		transitionAt := now.Add(-12 * time.Minute)
		if definition.state == "Expired" {
			transitionAt = expiresAt
		}
		if definition.state == "Revoked" {
			transitionAt = now.Add(-20 * time.Minute)
		}
		history := []map[string]any{{"at": startsAt.Add(-8 * time.Minute), "state": "Pending", "actor": "operator-alex", "message": "Plan confirmed; awaiting approvals"}, {"at": transitionAt, "state": definition.state, "actor": "dispatcher", "message": definition.reason, "generation": demoGeneration}}
		status := map[string]any{"state": definition.state, "observedGeneration": 1, "policyGeneration": demoGeneration, "message": definition.reason, "history": history}
		servingJobs := 0
		for _, job := range jobs {
			if job.Decision.OverrideID == definition.id {
				servingJobs++
			}
		}
		result = append(result, sampleOverride{map[string]any{"apiVersion": "ci.openshift.io/v1", "kind": "DispatchOverride", "metadata": map[string]any{"name": definition.id, "generation": 1}, "spec": spec, "status": status}, definition.published, servingJobs})
	}
	return result
}
