package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestOperationWorker(store StateStore, api *fakeAM, enabled bool) *OperationWorker {
	flag := enabled
	return NewOperationWorker(store, api, testAuthorizer("UADMIN"), testPolicy(), func() bool { return flag }, testRenderer(), nil)
}

type updateCountingStore struct {
	StateStore
	mu      sync.Mutex
	updates int
}

type readSignalingStore struct {
	StateStore
	reads chan struct{}
}

func (s *readSignalingStore) Read(ctx context.Context) (*State, error) {
	state, err := s.StateStore.Read(ctx)
	select {
	case s.reads <- struct{}{}:
	default:
	}
	return state, err
}

func (s *updateCountingStore) Update(ctx context.Context, mutate func(*State) error) error {
	s.mu.Lock()
	s.updates++
	s.mu.Unlock()
	return s.StateStore.Update(ctx, mutate)
}

func (s *updateCountingStore) updateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updates
}

func validMatchers() []Matcher {
	return []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}
}

func TestRefreshOwnedSkipsUnchangedActiveObservations(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStateStore()
	owner := ownershipID("app-ci-uwm", validMatchers())
	startsAt := time.Date(2026, time.September, 9, 10, 0, 0, 0, time.UTC)
	endsAt := startsAt.Add(4 * time.Hour)
	lastObservedAt := startsAt.Add(-time.Minute)
	if err := base.Update(ctx, func(s *State) error {
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: "live", CanonicalMatchers: validMatchers(), CanonicalStartsAt: startsAt, CanonicalEndsAt: endsAt, LastObservedState: "active", LastObservedAt: lastObservedAt}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store := &updateCountingStore{StateStore: base}
	worker := newTestOperationWorker(store, newFakeAM(), true)
	now := endsAt.Add(time.Minute)
	worker.now = func() time.Time { return now }
	observed := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: startsAt, EndsAt: endsAt}
	observed.Status.State = "active"
	for i := 0; i < 2; i++ {
		if err := worker.RefreshOwned(ctx, []AMSilence{observed}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(5 * time.Second)
	}
	if got := store.updateCount(); got != 0 {
		t.Fatalf("unchanged observations caused %d state updates", got)
	}
	state, _ := store.Read(ctx)
	if !state.SilenceRefs[owner].LastObservedAt.Equal(lastObservedAt) {
		t.Fatalf("unchanged observation advanced timestamp to %s", state.SilenceRefs[owner].LastObservedAt)
	}

	observed.EndsAt = endsAt.Add(time.Hour)
	if err := worker.RefreshOwned(ctx, []AMSilence{observed}); err != nil {
		t.Fatal(err)
	}
	if got := store.updateCount(); got != 1 {
		t.Fatalf("changed observation caused %d updates, want 1", got)
	}
	if err := worker.RefreshOwned(ctx, []AMSilence{observed}); err != nil {
		t.Fatal(err)
	}
	if got := store.updateCount(); got != 1 {
		t.Fatalf("repeated changed observation caused %d updates, want 1", got)
	}
	if err := worker.RefreshOwned(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := worker.RefreshOwned(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := store.updateCount(); got != 2 {
		t.Fatalf("deletion and its repeat caused %d updates, want 2", got)
	}
}

func TestRefreshOwnedWakesOutboxWhenClearingStaleScheduledPresentation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	owner := ownershipID("app-ci-uwm", validMatchers())
	startsAt := time.Date(2026, time.September, 9, 10, 0, 0, 0, time.UTC)
	endsAt := startsAt.Add(time.Hour)
	if err := store.Update(ctx, func(s *State) error {
		s.Groups["g"] = &GroupState{Source: "app-ci-uwm", GroupKey: "g", Channel: "C", EpisodeID: "e", ShortID: "short", Status: "firing", KnownMembers: map[string]KnownMember{}, RecentDeliveries: map[string]DeliveryRecord{}, Parent: &SlackObject{PostID: "parent", MessageTS: "1", DesiredRevision: 1}, ScheduledSilence: &SilencePresentation{Actor: "UADMIN", Matchers: validMatchers(), StartsAt: startsAt, EndsAt: endsAt}}
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: "live", CanonicalMatchers: validMatchers(), CanonicalStartsAt: startsAt, CanonicalEndsAt: endsAt, LastObservedState: "active", Origin: &SilenceOrigin{GroupKey: "g", EpisodeID: "e"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := newTestOperationWorker(store, newFakeAM(), true)
	woke := make(chan struct{}, 1)
	worker.outboxWake = func() { woke <- struct{}{} }
	observed := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: startsAt, EndsAt: endsAt}
	observed.Status.State = "active"
	if err := worker.RefreshOwned(ctx, []AMSilence{observed}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-woke:
	default:
		t.Fatal("clearing the stale scheduled presentation did not wake the outbox")
	}
	state, _ := store.Read(ctx)
	if state.Groups["g"].ScheduledSilence != nil || state.Outbox["parent:parent"] == nil {
		t.Fatalf("scheduled presentation was not rerendered: group=%#v outbox=%#v", state.Groups["g"], state.Outbox)
	}
}

func TestOperationRunDoesNotPeriodicallyReconcileLiveSilences(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := NewMemoryStateStore()
	if err := base.Update(ctx, func(s *State) error {
		s.SilenceRefs["owned"] = &SilenceRef{SilenceID: "live", LastObservedState: "active"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store := &readSignalingStore{StateStore: base, reads: make(chan struct{}, 10)}
	api := newFakeAM()
	listed := make(chan struct{}, 1)
	api.onListSilences = func() { listed <- struct{}{} }
	worker := newTestOperationWorker(store, api, true)
	go worker.Run(ctx)
	worker.Wake()
	select {
	case <-store.reads:
	case <-time.After(time.Second):
		t.Fatal("operation worker did not process its wake")
	}
	select {
	case <-listed:
		t.Fatal("operation worker performed periodic Alertmanager reconciliation")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestReconcileSnapshotCannotRaceCompletedOperation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.upsertState = "pending"
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	listed := make(chan struct{})
	releaseList := make(chan struct{})
	var listMu sync.Mutex
	listCalls := 0
	api.onListSilences = func() {
		listMu.Lock()
		listCalls++
		call := listCalls
		listMu.Unlock()
		if call != 1 {
			return
		}
		if worker.reconcileMu.TryLock() {
			worker.reconcileMu.Unlock()
			t.Error("reconciliation did not hold the snapshot mutex while listing")
		}
		close(listed)
		<-releaseList
	}
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- worker.Reconcile(ctx) }()
	<-listed
	processDone := make(chan error, 1)
	processAtReconcileLock := make(chan struct{})
	worker.beforeReconcileLock = func() { close(processAtReconcileLock) }
	go func() {
		_, err := worker.ProcessNext(ctx)
		processDone <- err
	}()
	<-processAtReconcileLock
	select {
	case err := <-processDone:
		t.Fatalf("operation crossed a stale reconciliation snapshot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseList)
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	if err := <-processDone; err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	op := state.SilenceOperations[id]
	ref := state.SilenceRefs[op.OwnershipID]
	if op.Phase != "completed" || ref == nil || ref.LastObservedState != "pending" {
		t.Fatalf("operation was corrupted by stale reconciliation: op=%#v ref=%#v", op, ref)
	}
}

func TestOperationRecoveryAdoptsTimedOutSubmissionAndExpiresDuplicate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	api.upsertErrAfterSideEffect = errors.New("timeout after write")
	worker := newTestOperationWorker(store, api, true)
	opID, err := worker.Prepare(ctx, PrepareSilenceRequest{RequestID: "trigger", RequestKind: "interaction", Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair", Preset: "4-hours"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err == nil {
		t.Fatal("expected ambiguous post error")
	}
	state, _ := store.Read(ctx)
	if state.SilenceOperations[opID].Phase != "ambiguous" {
		t.Fatalf("phase=%s", state.SilenceOperations[opID].Phase)
	}
	retryAt := state.SilenceOperations[opID].LastAttemptAt.Add(operationRetryDelay(state.SilenceOperations[opID].Attempt) + time.Nanosecond)
	worker.now = func() time.Time { return retryAt }
	// The side effect already happened. A later inventory guard must not turn
	// marker adoption into failure merely because the target alert resolved.
	api.mu.Lock()
	api.alerts = nil
	api.mu.Unlock()
	owner := ownershipID("app-ci-uwm", validMatchers())
	old := AMSilence{ID: "duplicate", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 0), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour)}
	old.Status.State = "active"
	api.mu.Lock()
	api.silences[old.ID] = old
	api.mu.Unlock()
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.SilenceOperations[opID].Phase != "completed" || state.SilenceRefs[owner] == nil {
		t.Fatalf("operation not recovered: %#v", state.SilenceOperations[opID])
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	active := 0
	for _, s := range api.silences {
		if s.Status.State == "active" || s.Status.State == "pending" {
			active++
		}
	}
	if active != 1 || api.upserts != 1 {
		t.Fatalf("active=%d upserts=%d silences=%#v", active, api.upserts, api.silences)
	}
}

func TestFinalInventoryClosesModalRaceAtZeroAndSix(t *testing.T) {
	for _, final := range []int{0, 6} {
		t.Run(string(rune('0'+final)), func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryStateStore()
			api := newFakeAM()
			api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
			worker := newTestOperationWorker(store, api, true)
			id, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair"})
			if err != nil {
				t.Fatal(err)
			}
			api.mu.Lock()
			api.alerts = nil
			for i := 0; i < final; i++ {
				api.alerts = append(api.alerts, activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"}))
			}
			api.mu.Unlock()
			if _, err := worker.ProcessNext(ctx); err != nil {
				t.Fatal(err)
			}
			state, _ := store.Read(ctx)
			if state.SilenceOperations[id].Phase != "failed" || api.upserts != 0 {
				t.Fatalf("final=%d phase=%s upserts=%d", final, state.SilenceOperations[id].Phase, api.upserts)
			}
		})
	}
}

func TestOneNonTerminalOperationPerOwnershipAcrossOrigins(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	worker := newTestOperationWorker(store, api, true)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := worker.Prepare(ctx, PrepareSilenceRequest{RequestID: string(rune('a' + i)), RequestKind: "interaction", Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair", Origin: &SilenceOrigin{GroupKey: string(rune('g' + i)), EpisodeID: "e"}})
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	success, failed := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else {
			failed++
		}
	}
	if success != 1 || failed != 1 {
		t.Fatalf("success=%d failed=%d", success, failed)
	}
	state, _ := store.Read(ctx)
	nonterminal := 0
	for _, op := range state.SilenceOperations {
		if op.Phase != "completed" && op.Phase != "failed" {
			nonterminal++
		}
	}
	if nonterminal != 1 {
		t.Fatalf("nonterminal=%d", nonterminal)
	}
}

func TestPendingReadbackDoesNotCloseUntilObservedActive(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.upsertState = "pending"
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	g := &GroupState{Source: "app-ci-uwm", GroupKey: "g", Channel: "C", EpisodeID: "e", ShortID: "short", Status: "firing", LatestBatch: []AlertSnapshot{{Fingerprint: "f", Status: "firing", Labels: map[string]string{"alertname": "Broken", "namespace": "ci"}}}, KnownMembers: map[string]KnownMember{"f": {LastExplicitStatus: "firing"}}, Parent: &SlackObject{PostID: "p", MessageTS: "1", DesiredRevision: 1}, RecentDeliveries: map[string]DeliveryRecord{}}
	_ = store.Update(ctx, func(s *State) error { s.Groups["g"] = g; return nil })
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair", Origin: &SilenceOrigin{GroupKey: "g", EpisodeID: "e"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if !state.Groups["g"].ClosedAt.IsZero() || state.Groups["g"].ScheduledSilence == nil {
		t.Fatal("pending silence closed episode or was not presented")
	}
	ref := state.SilenceRefs[state.SilenceOperations[id].OwnershipID]
	api.mu.Lock()
	s := api.silences[ref.SilenceID]
	s.Status.State = "active"
	api.silences[ref.SilenceID] = s
	all := []AMSilence{s}
	api.mu.Unlock()
	if err := worker.RefreshOwned(ctx, all); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if state.Groups["g"].ClosedReason != "silenced" || state.Groups["g"].Parent != nil {
		t.Fatal("active observation did not close and detach episode")
	}
}

func TestMatcherReplacementReservesOldAndNewOwnership(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci", "job_name": "job"})}
	worker := newTestOperationWorker(store, api, true)
	old := validMatchers()
	oldID := ownershipID("app-ci-uwm", old)
	next := []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "job_name", Value: "job", IsEqual: true}}
	if _, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: next, ReplacesOwnershipID: oldID, EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "replace"}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: old, EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "race"}); err == nil {
		t.Fatal("old ownership was not reserved by replacement")
	}
}

func TestMatcherReplacementEstablishesDestinationBeforeExpiringSource(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	oldMatchers := validMatchers()
	newMatchers := []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "job_name", Value: "job", IsEqual: true}}
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci", "job_name": "job"})}
	oldOwner := ownershipID("app-ci-uwm", oldMatchers)
	old := AMSilence{ID: "old", Matchers: oldMatchers, CreatedBy: silenceCreatedBy, Comment: marker(oldOwner, "deadbeef", 1), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour)}
	old.Status.State = "active"
	api.silences[old.ID] = old
	if err := store.Update(ctx, func(s *State) error {
		s.SilenceRefs[oldOwner] = &SilenceRef{SilenceID: old.ID, ShortID: "old-short", CanonicalMatchers: oldMatchers, Source: "app-ci-uwm", Generation: 1, LastObservedState: "active"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := newTestOperationWorker(store, api, true)
	if _, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: newMatchers, ReplacesOwnershipID: oldOwner, EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "narrow scope"}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	oldState := api.silences["old"].Status.State
	upserts := api.upserts
	api.mu.Unlock()
	state, _ := store.Read(ctx)
	if oldState != "expired" || upserts != 1 || state.SilenceRefs[oldOwner].LastObservedState != "expired" || state.SilenceRefs[ownershipID("app-ci-uwm", newMatchers)].LastObservedState != "active" {
		t.Fatalf("replacement did not move ownership safely: old=%s upserts=%d refs=%#v", oldState, upserts, state.SilenceRefs)
	}
}

func TestMentionOperationReportsCanonicalResultToOriginatingThread(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	worker := newTestOperationWorker(store, api, true)
	if _, err := worker.Prepare(ctx, PrepareSilenceRequest{RequestID: "event", RequestKind: "mention", Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair", ReplyTarget: &OutboxTarget{Channel: "C", ThreadTS: "12.3"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	ref := state.SilenceRefs[ownershipID("app-ci-uwm", validMatchers())]
	if ref == nil || ref.Channel != "C" || ref.ThreadTS != "12.3" {
		t.Fatalf("mention correlation was not persisted: %#v", ref)
	}
	found := false
	for id, work := range state.Outbox {
		if len(id) >= len("silence-audit:") && id[:len("silence-audit:")] == "silence-audit:" && work.Target.Channel == "C" && work.Target.ThreadTS == "12.3" {
			found = true
		}
	}
	if !found {
		t.Fatalf("canonical mention result was not queued to its thread: %#v", state.Outbox)
	}
}

func TestAmbiguousSubmissionIsExpiredWhenRecoveryAuthorizationIsLost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	api.upsertErrAfterSideEffect = errors.New("timeout after write")
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err == nil {
		t.Fatal("expected ambiguous submission")
	}
	state, _ := store.Read(ctx)
	worker.now = func() time.Time { return state.SilenceOperations[id].LastAttemptAt.Add(2 * time.Second) }
	worker.authz.mu.Lock()
	worker.authz.users = map[string]bool{}
	worker.authz.group = &fakeGroupReader{err: errors.New("group API unavailable")}
	worker.authz.mu.Unlock()
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	owner := state.SilenceOperations[id].OwnershipID
	if state.SilenceOperations[id].Phase != "failed" || state.SilenceRefs[owner] == nil || state.SilenceRefs[owner].LastObservedState != "expired" {
		t.Fatalf("unsafe recovery result: op=%#v ref=%#v", state.SilenceOperations[id], state.SilenceRefs[owner])
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	for _, silence := range api.silences {
		if silence.Status.State == "active" || silence.Status.State == "pending" {
			t.Fatalf("authorization loss stranded live owned silence: %#v", silence)
		}
	}
}

func TestFinalAuthorizationImmediatelyPrecedesSuppressingCall(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"})}
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.Prepare(ctx, PrepareSilenceRequest{Source: "app-ci-uwm", Matchers: validMatchers(), EndsAt: time.Now().Add(4 * time.Hour), Actor: "UADMIN", Reason: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	api.onListAlerts = func() {
		worker.authz.mu.Lock()
		worker.authz.users = map[string]bool{}
		worker.authz.group = &fakeGroupReader{err: errors.New("group API unavailable")}
		worker.authz.mu.Unlock()
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if api.upserts != 0 || state.SilenceOperations[id].Phase != "failed" {
		t.Fatalf("authorization revoked during inventory: upserts=%d op=%#v", api.upserts, state.SilenceOperations[id])
	}
}

func TestFinalAuthorizationImmediatelyPrecedesUserExpiry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	owner := ownershipID("app-ci-uwm", validMatchers())
	silence := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour)}
	silence.Status.State = "active"
	api.silences[silence.ID] = silence
	_ = store.Update(ctx, func(s *State) error {
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: silence.ID, ShortID: "short", CanonicalMatchers: validMatchers(), Source: "app-ci-uwm", Generation: 1, Actor: "UADMIN", LastObservedState: "active"}
		return nil
	})
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.PrepareExpire(ctx, "request", "interaction", silence.ID, "UADMIN", "restore paging")
	if err != nil {
		t.Fatal(err)
	}
	api.onListSilences = func() {
		worker.authz.mu.Lock()
		worker.authz.users = map[string]bool{}
		worker.authz.group = &fakeGroupReader{err: errors.New("group API unavailable")}
		worker.authz.mu.Unlock()
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if api.expires != 0 || state.SilenceOperations[id].Phase != "failed" {
		t.Fatalf("authorization revoked before expiry: expires=%d op=%#v", api.expires, state.SilenceOperations[id])
	}
}

func TestAmbiguousOperationDoesNotBlockUnrelatedOwnership(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	firstMatchers := validMatchers()
	secondMatchers := []Matcher{{Name: "alertname", Value: "Other", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}
	api.alerts = []AMAlert{activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"}), activeAlert(map[string]string{"alertname": "Other", "namespace": "ci"})}
	api.upsertErrAfterSideEffect = errors.New("timeout after write")
	now := time.Now().UTC()
	_ = store.Update(ctx, func(s *State) error {
		s.SilenceOperations["a"] = &SilenceOperation{Action: "create", OwnershipID: ownershipID("app-ci-uwm", firstMatchers), OwnershipGeneration: 1, Actor: "UADMIN", Reason: "first", RequestedAt: now, DeadlineAt: now.Add(time.Minute), RequestedMatchers: firstMatchers, RequestedEndsAt: now.Add(4 * time.Hour), Source: "app-ci-uwm", Phase: "prepared"}
		s.SilenceOperations["b"] = &SilenceOperation{Action: "create", OwnershipID: ownershipID("app-ci-uwm", secondMatchers), OwnershipGeneration: 1, Actor: "UADMIN", Reason: "second", RequestedAt: now, DeadlineAt: now.Add(time.Minute), RequestedMatchers: secondMatchers, RequestedEndsAt: now.Add(4 * time.Hour), Source: "app-ci-uwm", Phase: "prepared"}
		return nil
	})
	worker := newTestOperationWorker(store, api, true)
	worker.now = func() time.Time { return now }
	if _, err := worker.ProcessNext(ctx); err == nil {
		t.Fatal("expected first ownership to become ambiguous")
	}
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.SilenceOperations["a"].Phase != "ambiguous" || state.SilenceOperations["b"].Phase != "completed" || api.upserts != 2 {
		t.Fatalf("unrelated operation was head-of-line blocked: a=%#v b=%#v upserts=%d", state.SilenceOperations["a"], state.SilenceOperations["b"], api.upserts)
	}
}

func TestOperationDeadlineEndsMarkerDiscoveryWithoutResubmission(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	now := time.Now().UTC()
	_ = store.Update(ctx, func(s *State) error {
		s.SilenceOperations["expired"] = &SilenceOperation{Action: "create", OwnershipID: ownershipID("app-ci-uwm", validMatchers()), OwnershipGeneration: 1, Actor: "UADMIN", Reason: "old", RequestedAt: now.Add(-time.Hour), DeadlineAt: now.Add(-time.Minute), RequestedMatchers: validMatchers(), RequestedEndsAt: now.Add(time.Hour), Source: "app-ci-uwm", Phase: "ambiguous"}
		return nil
	})
	worker := newTestOperationWorker(store, api, true)
	worker.now = func() time.Time { return now }
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Read(ctx)
	if state.SilenceOperations["expired"].Phase != "failed" || api.upserts != 0 {
		t.Fatalf("expired operation was resubmitted: %#v upserts=%d", state.SilenceOperations["expired"], api.upserts)
	}
}

func TestAmbiguousExpiryRecoveryFinishesWithoutReauthorization(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	api := newFakeAM()
	owner := ownershipID("app-ci-uwm", validMatchers())
	silence := AMSilence{ID: "live", Matchers: validMatchers(), CreatedBy: silenceCreatedBy, Comment: marker(owner, "deadbeef", 1), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour)}
	silence.Status.State = "active"
	api.silences[silence.ID] = silence
	api.expireErrAfterSideEffect = errors.New("timeout after expiry")
	_ = store.Update(ctx, func(s *State) error {
		s.SilenceRefs[owner] = &SilenceRef{SilenceID: silence.ID, ShortID: "short", CanonicalMatchers: validMatchers(), Source: "app-ci-uwm", Generation: 1, Actor: "UADMIN", LastObservedState: "active"}
		return nil
	})
	worker := newTestOperationWorker(store, api, true)
	id, err := worker.PrepareExpire(ctx, "request", "interaction", silence.ID, "UADMIN", "restore paging")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessNext(ctx); err == nil {
		t.Fatal("expected ambiguous expiry")
	}
	state, _ := store.Read(ctx)
	worker.now = func() time.Time { return state.SilenceOperations[id].LastAttemptAt.Add(2 * time.Second) }
	worker.authz.mu.Lock()
	worker.authz.users = map[string]bool{}
	worker.authz.group = &fakeGroupReader{err: errors.New("group unavailable")}
	worker.authz.mu.Unlock()
	if _, err := worker.ProcessNext(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Read(ctx)
	if api.expires != 1 || state.SilenceOperations[id].Phase != "completed" || state.SilenceRefs[owner].LastObservedState != "expired" {
		t.Fatalf("expiry recovery was replayed/stranded: expires=%d op=%#v ref=%#v", api.expires, state.SilenceOperations[id], state.SilenceRefs[owner])
	}
}
