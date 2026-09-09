package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestReadinessAlertmanagerGate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	now := time.Now()
	enabled := false
	r := &Readiness{api: api, store: store, enabled: func() bool { return enabled }, now: func() time.Time { return now }}
	api.listErr = errors.New("network down")
	if err := r.Check(ctx); err != nil || !r.Ready() {
		t.Fatalf("clean disabled instance depended on Alertmanager: ready=%v err=%v", r.Ready(), err)
	}
	enabled = true
	api.listErr = nil
	if err := r.Check(ctx); err != nil || !r.Ready() {
		t.Fatalf("initial success: %v ready=%v", err, r.Ready())
	}
	api.listErr = errors.New("temporary")
	now = now.Add(30 * time.Second)
	if err := r.Check(ctx); err == nil || !r.Ready() {
		t.Fatalf("transient failure inside one minute should retain readiness: err=%v ready=%v", err, r.Ready())
	}
	now = now.Add(31 * time.Second)
	_ = r.Check(ctx)
	if r.Ready() {
		t.Fatal("stale Alertmanager success retained readiness past one minute")
	}
	api.listErr = &AMHTTPError{StatusCode: http.StatusForbidden}
	now = now.Add(time.Second)
	_ = r.Check(ctx)
	if r.Ready() {
		t.Fatal("authorization error did not fail readiness immediately")
	}
}

func TestPersistedLiveSilenceEnablesReadinessGateWhenFeatureOff(t *testing.T) {
	store := NewMemoryStateStore()
	_ = store.Update(context.Background(), func(s *State) error { s.SilenceRefs["x"] = &SilenceRef{LastObservedState: "active"}; return nil })
	api := newFakeAM()
	api.listErr = errors.New("down")
	r := &Readiness{api: api, store: store, enabled: func() bool { return false }, now: time.Now}
	if err := r.Check(context.Background()); err == nil || r.Ready() {
		t.Fatal("live owned silence did not gate readiness")
	}
}

func TestAlertmanagerCredentialFileFailureIsImmediate(t *testing.T) {
	if !isImmediateAMFailure(errors.New("read Alertmanager token: projected token missing")) || !isImmediateAMFailure(errors.New("read Alertmanager CA: service CA missing")) {
		t.Fatal("local Alertmanager authentication or TLS material failures must fail readiness immediately")
	}
}

func TestReadinessStaysFalseWhileSilenceRecoveryIsNonterminal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	_ = store.Update(ctx, func(s *State) error {
		s.SilenceOperations["recover"] = &SilenceOperation{Action: "create", OwnershipID: "owner", Phase: "ambiguous"}
		return nil
	})
	r := &Readiness{api: newFakeAM(), store: store, enabled: func() bool { return false }, now: time.Now}
	if err := r.Check(ctx); err == nil || r.Ready() {
		t.Fatalf("successful Alertmanager inventory restored readiness during recovery: ready=%v err=%v", r.Ready(), err)
	}
	_ = store.Update(ctx, func(s *State) error { s.SilenceOperations["recover"].Phase = "failed"; return nil })
	if err := r.Check(ctx); err != nil || !r.Ready() {
		t.Fatalf("terminal recovery did not restore readiness: ready=%v err=%v", r.Ready(), err)
	}
}

func TestReadinessReconcilesOwnedTransitionWithoutRepeatingStateUpdate(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStateStore()
	owner := ownershipID("app-ci-uwm", validMatchers())
	startsAt := time.Date(2026, time.September, 9, 10, 0, 0, 0, time.UTC)
	endsAt := startsAt.Add(4 * time.Hour)
	if err := base.Update(ctx, func(s *State) error {
		s.Groups["g"] = &GroupState{Source: "app-ci-uwm", GroupKey: "g", Channel: "C", EpisodeID: "e", ShortID: "short", Status: "firing", KnownMembers: map[string]KnownMember{}, RecentDeliveries: map[string]DeliveryRecord{}, Parent: &SlackObject{PostID: "parent", MessageTS: "1", DesiredRevision: 1}, ScheduledSilence: &SilencePresentation{Actor: "UADMIN", Matchers: validMatchers(), StartsAt: startsAt, EndsAt: endsAt}}
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: "live", CanonicalMatchers: validMatchers(), CanonicalStartsAt: startsAt, CanonicalEndsAt: endsAt, LastObservedState: "pending", Origin: &SilenceOrigin{GroupKey: "g", EpisodeID: "e"}, Actor: "UADMIN", Reason: "repair"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store := &updateCountingStore{StateStore: base}
	api := newFakeAM()
	active := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: startsAt, EndsAt: endsAt}
	active.Status.State = "active"
	api.silences[active.ID] = active
	worker := newTestOperationWorker(store, api, true)
	readiness := &Readiness{api: api, store: store, enabled: func() bool { return true }, operations: worker, now: time.Now}
	if err := readiness.Check(ctx); err != nil || !readiness.Ready() {
		t.Fatalf("first readiness check failed: ready=%v err=%v", readiness.Ready(), err)
	}
	state, err := store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.SilenceRefs[owner].LastObservedState != "active" || state.Groups["g"].ClosedReason != "silenced" {
		t.Fatalf("pending-to-active transition was not reconciled: ref=%#v group=%#v", state.SilenceRefs[owner], state.Groups["g"])
	}
	if got := store.updateCount(); got != 1 {
		t.Fatalf("first readiness reconciliation caused %d updates, want 1", got)
	}
	if err := readiness.Check(ctx); err != nil || !readiness.Ready() {
		t.Fatalf("repeated readiness check failed: ready=%v err=%v", readiness.Ready(), err)
	}
	if got := store.updateCount(); got != 1 {
		t.Fatalf("unchanged readiness reconciliation caused another state update: %d", got)
	}
}

func TestServingReadinessRejectsPersistedDrainMode(t *testing.T) {
	store := NewMemoryStateStore()
	_ = store.Update(context.Background(), func(s *State) error { s.Mode = "draining"; return nil })
	r := &Readiness{api: newFakeAM(), store: store, enabled: func() bool { return false }, now: time.Now}
	if err := r.Check(context.Background()); err == nil || r.Ready() {
		t.Fatalf("serving process became ready in drain mode: ready=%v err=%v", r.Ready(), err)
	}
}
