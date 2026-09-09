package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDrainAdoptsAndExpiresUnreferencedOwnedSilence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := NewMemoryStateStore()
	api := newFakeAM()
	owner := ownershipID("app-ci-uwm", validMatchers())
	silence := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 3), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour)}
	silence.Status.State = "active"
	api.silences[silence.ID] = silence
	operations := newTestOperationWorker(store, api, false)
	outbox := NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil)
	drain := &DrainController{store: store, api: api, operations: operations, outbox: outbox, renderer: testRenderer(), now: time.Now}
	if err := drain.Run(ctx); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	state := api.silences["live"].Status.State
	api.mu.Unlock()
	if state != "expired" {
		t.Fatalf("owned silence state=%s", state)
	}
	persisted, _ := store.Read(context.Background())
	if persisted.Mode != "draining" || len(persisted.Groups) != 0 || len(persisted.SilenceRefs) != 0 {
		t.Fatalf("drain did not finish cleanly: %#v", persisted)
	}
}

func TestLifecycleStalenessNeverClaimsResolution(t *testing.T) {
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	g := openTestGroup()
	g.LastSeen = now.Add(-7 * time.Hour)
	g.EpisodeStartedAt = g.LastSeen
	_ = store.Update(context.Background(), func(s *State) error { s.Groups["g"] = g; return nil })
	sweeper := &LifecycleSweeper{store: store, renderer: testRenderer(), repeatInterval: 2 * time.Hour, escalationThreshold: 24 * time.Hour, now: func() time.Time { return now }}
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(context.Background())
	if state.Groups["g"].ClosedReason != "stale" || state.Groups["g"].ClosedReason == "resolved" {
		t.Fatalf("stale group state=%#v", state.Groups["g"])
	}
}

func TestEscalationLatchedOnce(t *testing.T) {
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	g := openTestGroup()
	g.EpisodeStartedAt = now.Add(-25 * time.Hour)
	g.LastSeen = now
	g.Parent.MessageTS = "1"
	_ = store.Update(context.Background(), func(s *State) error { s.Groups["g"] = g; return nil })
	sweeper := &LifecycleSweeper{store: store, renderer: testRenderer(), repeatInterval: 2 * time.Hour, escalationThreshold: 24 * time.Hour, now: func() time.Time { return now }}
	_ = sweeper.Sweep(context.Background())
	state, _ := store.Read(context.Background())
	first := len(state.Outbox)
	_ = sweeper.Sweep(context.Background())
	state, _ = store.Read(context.Background())
	if first != 2 || len(state.Outbox) != first || state.Groups["g"].EscalatedAt.IsZero() {
		t.Fatalf("escalation was not once-per-episode: %d -> %d", first, len(state.Outbox))
	}
}

func TestDrainNeverSubmitsPreparedSuppressingOperation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	now := time.Now().UTC()
	if err := store.Update(ctx, func(s *State) error {
		s.SilenceOperations["prepared"] = &SilenceOperation{Action: "create", OwnershipID: ownershipID("app-ci-uwm", validMatchers()), OwnershipGeneration: 1, Actor: "UADMIN", Reason: "queued before drain", RequestedAt: now, DeadlineAt: now.Add(time.Minute), RequestedMatchers: validMatchers(), RequestedEndsAt: now.Add(4 * time.Hour), Source: "app-ci-uwm", Phase: "prepared"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	operations := newTestOperationWorker(store, api, true)
	drain := &DrainController{store: store, api: api, operations: operations, outbox: NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil), renderer: testRenderer(), now: func() time.Time { return now }}
	if err := drain.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	if did, err := operations.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if api.upserts != 0 || state.SilenceOperations["prepared"].Phase != "failed" {
		t.Fatalf("drain submitted suppression: upserts=%d op=%#v", api.upserts, state.SilenceOperations["prepared"])
	}
}

func TestDrainWaitsForSubmittedMarkerThenExpiresIt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	now := time.Now().UTC()
	owner := ownershipID("app-ci-uwm", validMatchers())
	if err := store.Update(ctx, func(s *State) error {
		s.Mode = "draining"
		s.SilenceOperations["deadbeef"] = &SilenceOperation{Action: "create", OwnershipID: owner, OwnershipGeneration: 1, Actor: "UADMIN", Reason: "queued before drain", RequestedAt: now, DeadlineAt: now.Add(time.Minute), RequestedMatchers: validMatchers(), RequestedEndsAt: now.Add(4 * time.Hour), Source: "app-ci-uwm", Phase: "submitted"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := newTestOperationWorker(store, api, true)
	worker.now = func() time.Time { return now }
	if did, err := worker.ProcessNext(ctx); err == nil || !did {
		t.Fatalf("first marker discovery did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.SilenceOperations["deadbeef"].Phase != "ambiguous" || api.upserts != 0 {
		t.Fatalf("zero-marker inventory abandoned/replayed operation: %#v upserts=%d", state.SilenceOperations["deadbeef"], api.upserts)
	}
	visible := AMSilence{ID: "late", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: now, EndsAt: now.Add(time.Hour)}
	visible.Status.State = "active"
	api.silences[visible.ID] = visible
	now = now.Add(2 * time.Second)
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("late marker recovery did=%v err=%v", did, err)
	}
	state, _ = store.Read(ctx)
	if state.SilenceOperations["deadbeef"].Phase != "failed" || state.SilenceRefs[owner] == nil || state.SilenceRefs[owner].LastObservedState != "expired" || api.silences[visible.ID].Status.State != "expired" {
		t.Fatalf("late marker was not expired and journaled: op=%#v ref=%#v silence=%#v", state.SilenceOperations["deadbeef"], state.SilenceRefs[owner], api.silences[visible.ID])
	}
}

func TestNaturalSilenceExpiryRetiresDurableControls(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	owner := ownershipID("app-ci-uwm", validMatchers())
	ref := &SilenceRef{SilenceID: "live", ShortID: "short", Channel: "C", ThreadTS: "parent", CanonicalMatchers: validMatchers(), Source: "app-ci-uwm", Generation: 1, Actor: "UADMIN", Reason: "repair", LastObservedState: "active", AuditMessageTS: "audit-ts"}
	_ = store.Update(ctx, func(s *State) error { s.SilenceRefs[owner] = ref; return nil })
	worker := newTestOperationWorker(store, newFakeAM(), true)
	expired := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: time.Now().Add(-time.Hour), EndsAt: time.Now()}
	expired.Status.State = "expired"
	if err := worker.RefreshOwned(ctx, []AMSilence{expired}); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.SilenceRefs[owner].LastObservedState != "expired" {
		t.Fatalf("ref not expired: %#v", state.SilenceRefs[owner])
	}
	var final *OutboxWork
	for _, work := range state.Outbox {
		if work.Object == "silenceAudit" && work.Kind == "update" {
			final = work
		}
	}
	if final == nil || final.Target.MessageTS != "audit-ts" || final.ImmutablePayload == nil || strings.Contains(string(final.ImmutablePayload.Blocks), "alert-proxy:unsilence") || strings.Contains(string(final.ImmutablePayload.Blocks), "alert-proxy:extend") {
		t.Fatalf("natural expiry left stale controls: %#v", final)
	}
}

func TestDrainTreatsAlreadyAbsentOwnedSilenceAsExpired(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	owner := ownershipID("app-ci-uwm", validMatchers())
	if err := store.Update(ctx, func(s *State) error {
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: "gone", ShortID: "short", CanonicalMatchers: validMatchers(), Source: "app-ci-uwm", Generation: 1, LastObservedState: "active"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	api := newFakeAM()
	worker := newTestOperationWorker(store, api, false)
	drain := &DrainController{store: store, api: api, operations: worker, outbox: NewOutboxWorker(store, &fakeSlack{}, testRenderer(), nil), renderer: testRenderer(), now: time.Now}
	if err := drain.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	if did, err := worker.ProcessNext(ctx); err != nil || !did {
		t.Fatalf("did=%v err=%v", did, err)
	}
	state, _ := store.Read(ctx)
	if state.SilenceRefs[owner].LastObservedState != "expired" || api.expires != 0 {
		t.Fatalf("absent silence did not converge safely: ref=%#v expires=%d", state.SilenceRefs[owner], api.expires)
	}
}

func TestLifecycleSweepPrunesDurableAuditStateWithoutWebhookTraffic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	if err := store.Update(ctx, func(s *State) error {
		g := openTestGroup()
		g.RecentDeliveries["old"] = DeliveryRecord{ProcessedAt: now.Add(-2 * time.Hour)}
		s.Groups[g.GroupKey] = g
		s.Requests["old"] = &RequestReservation{ExpiresAt: now.Add(-time.Hour)}
		s.SilenceOperations["old"] = &SilenceOperation{OwnershipID: "owner", Phase: "completed", CompletedAt: now.Add(-2 * time.Hour)}
		s.SilenceRefs["expired"] = &SilenceRef{LastObservedState: "expired", LastObservedAt: now.Add(-2 * time.Hour)}
		s.SilenceRefs["deleted"] = &SilenceRef{LastObservedState: "deleted", LastObservedAt: now.Add(-2 * time.Hour)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sweeper := &LifecycleSweeper{store: store, renderer: testRenderer(), repeatInterval: 2 * time.Hour, escalationThreshold: 24 * time.Hour, deliveryTTL: time.Hour, requestTTL: time.Hour, auditTTL: time.Hour, now: func() time.Time { return now }}
	if err := sweeper.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if len(state.Groups["g"].RecentDeliveries) != 0 || len(state.Requests) != 0 || len(state.SilenceOperations) != 0 || len(state.SilenceRefs) != 0 {
		t.Fatalf("hourly pruning did not bound state: %#v", state)
	}
}

func TestProbeGroupNeverEscalates(t *testing.T) {
	store := NewMemoryStateStore()
	now := time.Now().UTC()
	g := openTestGroup()
	g.GroupLabels = map[string]string{"alert_proxy_probe": "true"}
	g.EpisodeStartedAt = now.Add(-48 * time.Hour)
	g.LastSeen = now
	g.Parent.MessageTS = "1"
	_ = store.Update(context.Background(), func(s *State) error { s.Groups[g.GroupKey] = g; return nil })
	sweeper := &LifecycleSweeper{store: store, renderer: testRenderer(), repeatInterval: 2 * time.Hour, escalationThreshold: 24 * time.Hour, now: func() time.Time { return now }}
	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(context.Background())
	if !state.Groups[g.GroupKey].EscalatedAt.IsZero() || len(state.Outbox) != 0 {
		t.Fatalf("probe group escalated: group=%#v outbox=%#v", state.Groups[g.GroupKey], state.Outbox)
	}
}
