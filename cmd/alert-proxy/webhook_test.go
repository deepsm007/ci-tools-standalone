package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type updateErrorStore struct {
	StateStore
	err error
}

func (s *updateErrorStore) Update(context.Context, func(*State) error) error { return s.err }

func webhookBody(t *testing.T, status string, alerts ...map[string]any) []byte {
	t.Helper()
	p := map[string]any{"version": "4", "groupKey": "{}:{alertname=\"Example\",job=\"ci\"}", "status": status, "receiver": "slack-criticals", "groupLabels": map[string]string{"alertname": "Example", "job": "ci"}, "externalURL": "https://am.example", "alerts": alerts}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func alertPayload(fp, status string, started time.Time) map[string]any {
	return map[string]any{"status": status, "labels": map[string]string{"alertname": "Example", "job_name": "job"}, "annotations": map[string]string{"message": "boom"}, "startsAt": started.Format(time.RFC3339Nano), "endsAt": time.Time{}.Format(time.RFC3339Nano), "generatorURL": "https://prom.example/graph", "fingerprint": fp}
}

func TestWebhookLifecycleAndPersistentDeduplication(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Unix(10_000, 0).UTC()
	processor := &WebhookProcessor{store: store, channel: "C1", deliveryTTL: 8 * time.Minute, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }, renderer: testRenderer()}
	body := webhookBody(t, "firing", alertPayload("a", "firing", now.Add(-time.Hour)), alertPayload("b", "firing", now.Add(-time.Hour)))
	duplicate, err := processor.Process(ctx, "app-ci-uwm", body)
	if err != nil || duplicate {
		t.Fatalf("first delivery: duplicate=%v err=%v", duplicate, err)
	}
	state, _ := store.Read(ctx)
	g := state.Groups["{}:{alertname=\"Example\",job=\"ci\"}"]
	if g.NotificationCount != 1 || g.Parent == nil || len(state.Outbox) != 1 {
		t.Fatalf("unexpected initial state: %#v outbox=%d", g, len(state.Outbox))
	}
	duplicate, err = processor.Process(ctx, "app-ci-uwm", body)
	if err != nil || !duplicate {
		t.Fatalf("redelivery: duplicate=%v err=%v", duplicate, err)
	}
	state, _ = store.Read(ctx)
	if state.Groups[g.GroupKey].NotificationCount != 1 {
		t.Fatal("duplicate incremented notification count")
	}

	now = now.Add(time.Minute)
	resolvedB := webhookBody(t, "resolved", alertPayload("b", "resolved", now.Add(-time.Hour)))
	if _, err := processor.Process(ctx, "app-ci-uwm", resolvedB); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	g = state.Groups[g.GroupKey]
	if g.Status != "incomplete" || !g.ClosedAt.IsZero() {
		t.Fatalf("omitted firing member was falsely resolved: status=%s closed=%v", g.Status, g.ClosedAt)
	}
	now = now.Add(time.Minute)
	resolvedA := webhookBody(t, "resolved", alertPayload("a", "resolved", now.Add(-time.Hour)))
	if _, err := processor.Process(ctx, "app-ci-uwm", resolvedA); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	oldEpisode := state.Groups[g.GroupKey].EpisodeID
	if state.Groups[g.GroupKey].ClosedReason != "resolved" || state.Groups[g.GroupKey].Parent != nil {
		t.Fatalf("explicit settlement did not detach resolved episode: %#v", state.Groups[g.GroupKey])
	}

	now = now.Add(time.Minute)
	recurrence := webhookBody(t, "firing", alertPayload("a", "firing", now))
	if _, err := processor.Process(ctx, "app-ci-uwm", recurrence); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.Groups[g.GroupKey].EpisodeID == oldEpisode || state.Groups[g.GroupKey].Parent == nil {
		t.Fatal("recurrence did not start a new episode")
	}
}

func TestDeliveryHashIncludesLifecycleAndRenderedContent(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	a := AlertSnapshot{Fingerprint: "x", Status: "firing", StartsAt: start, Labels: map[string]string{"a": "b"}, Annotations: map[string]string{"message": "one"}}
	one, _ := deliveryHash("s", "g", map[string]string{"x": "y"}, []AlertSnapshot{a})
	a.Status = "resolved"
	two, _ := deliveryHash("s", "g", map[string]string{"x": "y"}, []AlertSnapshot{a})
	a.Status = "firing"
	a.StartsAt = start.Add(time.Hour)
	three, _ := deliveryHash("s", "g", map[string]string{"x": "y"}, []AlertSnapshot{a})
	a.StartsAt = start
	a.Annotations["message"] = "two"
	four, _ := deliveryHash("s", "g", map[string]string{"x": "y"}, []AlertSnapshot{a})
	if one == two || one == three || one == four {
		t.Fatalf("delivery hashes collided: %s %s %s %s", one, two, three, four)
	}
}

func TestWebhookValidationUsesPerAlertStatus(t *testing.T) {
	now := time.Now()
	in := alertmanagerWebhook{GroupKey: "g", Receiver: "r", Status: "resolved"}
	in.Alerts = append(in.Alerts, struct {
		Status       string            `json:"status"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     time.Time         `json:"startsAt"`
		EndsAt       time.Time         `json:"endsAt"`
		GeneratorURL string            `json:"generatorURL"`
		Fingerprint  string            `json:"fingerprint"`
	}{Status: "firing", Fingerprint: "x", StartsAt: now})
	if err := validateWebhook("source", &in); err == nil {
		t.Fatal("accepted top-level resolved status with firing member")
	}
}

func TestThreadWorkTargetsAlreadyPostedParentWithoutDeadDependency(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Unix(20_000, 0).UTC()
	processor := &WebhookProcessor{store: store, channel: "C1", deliveryTTL: 8 * time.Minute, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }, renderer: testRenderer()}
	if _, err := processor.Process(ctx, "app-ci-uwm", webhookBody(t, "firing", alertPayload("a", "firing", now))); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(s *State) error {
		g := s.Groups["{}:{alertname=\"Example\",job=\"ci\"}"]
		g.Parent.MessageTS = "1.2"
		delete(s.Outbox, "parent:"+g.Parent.PostID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	alerts := []map[string]any{}
	for i := 0; i < 11; i++ {
		alerts = append(alerts, alertPayload(string(rune('a'+i)), "firing", now))
	}
	if _, err := processor.Process(ctx, "app-ci-uwm", webhookBody(t, "firing", alerts...)); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	for id, work := range state.Outbox {
		if work.Object == "memberList" || strings.HasPrefix(id, "new-member:") {
			if work.Target.ThreadTS != "1.2" || work.DependsOnPostID != "" {
				t.Fatalf("thread work %s is deadlocked behind completed parent: %#v", id, work)
			}
		}
	}
}

func TestOrphanResolvedDeliveryDoesNotCreateEpisode(t *testing.T) {
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	processor := &WebhookProcessor{store: store, channel: "C1", deliveryTTL: 8 * time.Minute, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }, renderer: testRenderer()}
	if _, err := processor.Process(context.Background(), "app-ci-uwm", webhookBody(t, "resolved", alertPayload("a", "resolved", now.Add(-time.Hour)))); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(context.Background())
	if len(state.Groups) != 0 || len(state.Outbox) != 0 {
		t.Fatalf("orphan resolution created a misleading episode: groups=%#v outbox=%#v", state.Groups, state.Outbox)
	}
}

func TestWebhookResponseMetricsUseBoundedClassesAndCountOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	now := time.Now().UTC()
	store := NewMemoryStateStore()
	processor := &WebhookProcessor{store: store, channel: "C1", deliveryTTL: 8 * time.Minute, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }, renderer: testRenderer(), metrics: metrics}
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handler := WebhookHandler(processor, tokenPath, func() bool { return true })
	request := func(method, auth string, body []byte) int {
		r := httptest.NewRequest(method, "/webhook/alertmanager?source=app-ci-uwm", strings.NewReader(string(body)))
		r.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	valid := webhookBody(t, "firing", alertPayload("a", "firing", now.Add(-time.Hour)))
	if got := request(http.MethodGet, "Bearer secret", nil); got != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d", got)
	}
	if got := request(http.MethodPost, "Bearer wrong", valid); got != http.StatusUnauthorized {
		t.Fatalf("auth status=%d", got)
	}
	if got := request(http.MethodPost, "Bearer secret", valid); got != http.StatusOK {
		t.Fatalf("success status=%d", got)
	}
	failing := *processor
	failing.store = &updateErrorStore{StateStore: store, err: errors.New("invalid state mode from persistence")}
	failingHandler := WebhookHandler(&failing, tokenPath, func() bool { return true })
	r := httptest.NewRequest(http.MethodPost, "/webhook/alertmanager?source=app-ci-uwm", strings.NewReader(string(valid)))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	failingHandler.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("persistence error containing invalid became non-retryable: status=%d body=%q", w.Code, w.Body.String())
	}
	if got := metricCounterValue(metrics.webhookRequests.WithLabelValues("2xx")); got != 1 {
		t.Fatalf("2xx count=%v", got)
	}
	if got := metricCounterValue(metrics.webhookRequests.WithLabelValues("4xx")); got != 2 {
		t.Fatalf("4xx count=%v", got)
	}
	if got := metricCounterValue(metrics.webhookRequests.WithLabelValues("5xx")); got != 1 {
		t.Fatalf("5xx count=%v", got)
	}
}

func TestShrinkingBatchDurablyRetiresOverflowReply(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	processor := &WebhookProcessor{store: store, channel: "C1", deliveryTTL: 8 * time.Minute, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }, renderer: testRenderer()}
	large := make([]map[string]any, 0, maxRenderedMembers+1)
	for i := 0; i < maxRenderedMembers+1; i++ {
		large = append(large, alertPayload(fmt.Sprintf("fp-%d", i), "firing", now.Add(-time.Hour)))
	}
	if _, err := processor.Process(ctx, alertmanagerSource, webhookBody(t, "firing", large...)); err != nil {
		t.Fatal(err)
	}
	groupKey := "{}:{alertname=\"Example\",job=\"ci\"}"
	if err := store.Update(ctx, func(s *State) error {
		g := s.Groups[groupKey]
		g.Parent.MessageTS = "parent-ts"
		g.Parent.Phase = "posted"
		g.MemberList.MessageTS = "overflow-ts"
		g.MemberList.Phase = "posted"
		s.Outbox = map[string]*OutboxWork{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	small := []map[string]any{alertPayload("fp-0", "firing", now.Add(-time.Hour)), alertPayload("fp-1", "firing", now.Add(-time.Hour))}
	if _, err := processor.Process(ctx, alertmanagerSource, webhookBody(t, "firing", small...)); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	g := state.Groups[groupKey]
	retirementID := "member-list:" + g.MemberList.PostID
	retirement := state.Outbox[retirementID]
	if retirement == nil || retirement.Object != "memberListRetire" || retirement.Kind != "update" || retirement.Target.MessageTS != "overflow-ts" || retirement.ImmutablePayload == nil || !strings.Contains(retirement.ImmutablePayload.Text, "Overflow list retired") {
		t.Fatalf("stale overflow reply was not durably retired: %#v", retirement)
	}
	worker := NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil)
	for i := 0; i < 2; i++ {
		if did, err := worker.ProcessNext(ctx); err != nil || !did {
			t.Fatalf("delivery %d did=%v err=%v", i, did, err)
		}
	}
	state, _ = store.Read(ctx)
	if state.Groups[groupKey].MemberList != nil || state.Outbox[retirementID] != nil {
		t.Fatalf("retired overflow remained tracked: group=%#v outbox=%#v", state.Groups[groupKey], state.Outbox)
	}
}
