package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type PrepareSilenceRequest struct {
	RequestID           string
	RequestKind         string
	Source              string
	Matchers            []Matcher
	EndsAt              time.Time
	Actor               string
	Reason              string
	Preset              string
	Origin              *SilenceOrigin
	ReplyTarget         *OutboxTarget
	ReplacesOwnershipID string
}

type OperationWorker struct {
	store       StateStore
	api         AlertmanagerAPI
	authz       *Authorizer
	policy      *SilencePolicy
	enabled     func() bool
	renderer    *Renderer
	metrics     *Metrics
	now         func() time.Time
	wake        chan struct{}
	locksMu     sync.Mutex
	locks       map[string]*sync.Mutex
	reconcileMu sync.Mutex
	outboxWake  func()

	beforeReconcileLock func()
}

func NewOperationWorker(store StateStore, api AlertmanagerAPI, authz *Authorizer, policy *SilencePolicy, enabled func() bool, renderer *Renderer, metrics *Metrics) *OperationWorker {
	return &OperationWorker{store: store, api: api, authz: authz, policy: policy, enabled: enabled, renderer: renderer, metrics: metrics, now: time.Now, wake: make(chan struct{}, 1), locks: map[string]*sync.Mutex{}}
}
func (w *OperationWorker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *OperationWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
		attempted := map[string]bool{}
		for {
			id, did, _ := w.processNext(ctx, attempted)
			if !did {
				break
			}
			attempted[id] = true
		}
	}
}

func (w *OperationWorker) Prepare(ctx context.Context, req PrepareSilenceRequest) (string, error) {
	if req.RequestID != "" {
		state, err := w.store.Read(ctx)
		if err != nil {
			return "", err
		}
		if reservation := state.Requests[req.RequestID]; reservation != nil {
			return reservation.OperationID, nil
		}
	}
	if !w.enabled() {
		return "", errors.New("creating or increasing Alertmanager silences is disabled")
	}
	if err := w.authz.Authorize(ctx, req.Actor, "silence"); err != nil {
		return "", err
	}
	if strings.TrimSpace(req.Reason) == "" {
		return "", errors.New("a silence reason is required")
	}
	if len([]rune(req.Reason)) > 500 {
		return "", errors.New("silence reason exceeds 500 characters")
	}
	if err := w.policy.ValidateMatchers(req.Matchers); err != nil {
		return "", err
	}
	now := w.now().UTC()
	req.EndsAt = w.policy.ClampExpiry(now, req.EndsAt)
	if req.EndsAt.Sub(now) < w.policy.Config.MinDuration {
		return "", errors.New("silence duration is too short")
	}
	alerts, err := w.api.ListAlerts(ctx)
	if err != nil {
		return "", fmt.Errorf("inventory current alerts: %w", err)
	}
	count := currentMatchCount(alerts, req.Matchers, now)
	if count < w.policy.Config.MinCurrentlyMatchedAlerts || count > w.policy.Config.MaxCurrentlyMatchedAlerts {
		return "", fmt.Errorf("silence currently matches %d alerts; allowed range is [%d,%d]", count, w.policy.Config.MinCurrentlyMatchedAlerts, w.policy.Config.MaxCurrentlyMatchedAlerts)
	}
	ownership := ownershipID(req.Source, req.Matchers)
	operationID := randomID(16)
	duplicate := false
	action := "create"
	err = w.store.Update(ctx, func(s *State) error {
		duplicate, action = false, "create"
		if s.Mode == "draining" {
			return errors.New("proxy is draining")
		}
		if req.RequestID != "" {
			if old := s.Requests[req.RequestID]; old != nil {
				operationID = old.OperationID
				duplicate = true
				return nil
			}
		}
		if nonTerminalOperationFor(s, ownership, req.ReplacesOwnershipID) {
			return errors.New("another silence operation is already in progress for this matcher scope")
		}
		generation := int64(1)
		if ref := s.SilenceRefs[ownership]; ref != nil {
			generation = ref.Generation + 1
		}
		for _, old := range s.SilenceOperations {
			if old.OwnershipID == ownership && old.OwnershipGeneration >= generation {
				generation = old.OwnershipGeneration + 1
			}
		}
		if ref := s.SilenceRefs[ownership]; ref != nil && ref.SilenceID != "" && (ref.LastObservedState == "active" || ref.LastObservedState == "pending") {
			action = "update"
		}
		op := &SilenceOperation{Action: action, OwnershipID: ownership, OwnershipGeneration: generation, ReplacesOwnershipID: req.ReplacesOwnershipID, Actor: req.Actor, Reason: req.Reason, RequestedAt: now, DeadlineAt: now.Add(5 * time.Minute), RequestedMatchers: canonicalMatchers(req.Matchers), RequestedEndsAt: req.EndsAt, Source: req.Source, Origin: req.Origin, ReplyTarget: req.ReplyTarget, Preset: req.Preset, Phase: "prepared"}
		s.SilenceOperations[operationID] = op
		if req.RequestID != "" {
			s.Requests[req.RequestID] = &RequestReservation{Kind: req.RequestKind, OperationID: operationID, Phase: "processing", ReservedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if !duplicate {
		w.Wake()
	}
	if w.metrics != nil {
		result := "accepted"
		if duplicate {
			result = "duplicate"
		}
		w.metrics.silenceActions.WithLabelValues(action, req.Preset, result).Inc()
	}
	return operationID, nil
}

func (w *OperationWorker) PrepareExpire(ctx context.Context, requestID, kind, silenceID, actor, reason string) (string, error) {
	if requestID != "" {
		state, err := w.store.Read(ctx)
		if err != nil {
			return "", err
		}
		if reservation := state.Requests[requestID]; reservation != nil {
			return reservation.OperationID, nil
		}
	}
	if err := w.authz.Authorize(ctx, actor, "unsilence"); err != nil {
		return "", err
	}
	state, err := w.store.Read(ctx)
	if err != nil {
		return "", err
	}
	var ownership string
	var ref *SilenceRef
	for id, r := range state.SilenceRefs {
		if r.SilenceID == silenceID {
			ownership = id
			ref = r
			break
		}
	}
	if ref == nil {
		return "", errors.New("silence is not owned by alert-proxy")
	}
	if ref.LastObservedState != "active" && ref.LastObservedState != "pending" {
		return "", errors.New("proxy-owned silence is no longer active or pending")
	}
	now := w.now().UTC()
	operationID := randomID(16)
	duplicate := false
	err = w.store.Update(ctx, func(s *State) error {
		duplicate = false
		if s.Mode == "draining" {
			return errors.New("proxy is draining")
		}
		if requestID != "" {
			if old := s.Requests[requestID]; old != nil {
				operationID = old.OperationID
				duplicate = true
				return nil
			}
		}
		if nonTerminalOperationFor(s, ownership) {
			return errors.New("another silence operation is in progress")
		}
		generation := ref.Generation + 1
		var replyTarget *OutboxTarget
		if ref.Channel != "" {
			replyTarget = &OutboxTarget{Channel: ref.Channel, ThreadTS: ref.ThreadTS}
		}
		s.SilenceOperations[operationID] = &SilenceOperation{Action: "expire", OwnershipID: ownership, OwnershipGeneration: generation, Actor: actor, Reason: reason, RequestedAt: now, DeadlineAt: now.Add(5 * time.Minute), Source: ref.Source, Origin: ref.Origin, ReplyTarget: replyTarget, Phase: "prepared"}
		if requestID != "" {
			s.Requests[requestID] = &RequestReservation{Kind: kind, OperationID: operationID, Phase: "processing", ReservedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if !duplicate {
		w.Wake()
	}
	if w.metrics != nil {
		result := "accepted"
		if duplicate {
			result = "duplicate"
		}
		w.metrics.silenceActions.WithLabelValues("expire", "", result).Inc()
	}
	return operationID, nil
}

func (w *OperationWorker) ProcessNext(ctx context.Context) (bool, error) {
	_, did, err := w.processNext(ctx, nil)
	return did, err
}

func (w *OperationWorker) processNext(ctx context.Context, skip map[string]bool) (string, bool, error) {
	state, err := w.store.Read(ctx)
	if err != nil {
		return "", false, err
	}
	ids := make([]string, 0)
	now := w.now().UTC()
	for id, op := range state.SilenceOperations {
		if op.Phase == "completed" || op.Phase == "failed" || skip[id] {
			continue
		}
		if !op.LastAttemptAt.IsZero() && now.Before(op.LastAttemptAt.Add(operationRetryDelay(op.Attempt))) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return "", false, nil
	}
	id := ids[0]
	op := state.SilenceOperations[id]
	unlock := w.lockOwnerships(op.OwnershipID, op.ReplacesOwnershipID)
	defer unlock()
	// Another caller may have selected the same operation before waiting for its
	// ownership lock. Re-read under the lock so a completed command is never
	// submitted twice by concurrent recovery and worker loops.
	state, err = w.store.Read(ctx)
	if err != nil {
		return id, false, err
	}
	op = state.SilenceOperations[id]
	if op == nil || op.Phase == "completed" || op.Phase == "failed" {
		return id, true, nil
	}
	if w.beforeReconcileLock != nil {
		w.beforeReconcileLock()
	}
	w.reconcileMu.Lock()
	err = w.process(ctx, id, op)
	w.reconcileMu.Unlock()
	if err != nil {
		return id, true, err
	}
	return id, true, nil
}

func operationRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	return time.Second * time.Duration(1<<min(attempt-1, 6))
}

func (w *OperationWorker) lockOwnerships(ids ...string) func() {
	set := map[string]bool{}
	for _, id := range ids {
		if id != "" {
			set[id] = true
		}
	}
	ordered := sortedStrings(set)
	locks := make([]*sync.Mutex, 0, len(ordered))
	w.locksMu.Lock()
	for _, id := range ordered {
		m := w.locks[id]
		if m == nil {
			m = &sync.Mutex{}
			w.locks[id] = m
		}
		locks = append(locks, m)
	}
	w.locksMu.Unlock()
	for _, m := range locks {
		m.Lock()
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}
}

func (w *OperationWorker) process(ctx context.Context, id string, op *SilenceOperation) error {
	now := w.now().UTC()
	silences, err := w.api.ListSilences(ctx)
	if err != nil {
		return w.retryOperation(ctx, id, fmt.Errorf("list silences: %w", err))
	}
	if op.Action == "expire" {
		return w.processExpire(ctx, id, op, silences)
	}
	candidates := ownedSilences(silences, op.OwnershipID)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	var canonical *AMSilence
	for i := range candidates {
		_, operation, generation, ok := parseMarker(candidates[i].Comment)
		if ok && operation == id && generation == op.OwnershipGeneration {
			canonical = &candidates[i]
			break
		}
	}
	draining, err := w.draining(ctx)
	if err != nil {
		return err
	}
	if draining {
		deadline := op.DeadlineAt
		if deadline.IsZero() {
			deadline = op.RequestedAt.Add(5 * time.Minute)
		}
		if canonical == nil && op.Phase == "prepared" {
			return w.fail(ctx, id, errors.New("proxy entered drain before the prepared suppressing operation was submitted"))
		}
		if canonical == nil && now.Before(deadline) {
			for _, silence := range liveSilences(candidates) {
				if err := w.api.ExpireSilence(ctx, silence.ID); err != nil {
					return w.retryOperation(ctx, id, fmt.Errorf("expire existing ownership during marker discovery in drain %s: %w", silence.ID, err))
				}
				confirmed, err := w.api.GetSilence(ctx, silence.ID)
				if err != nil || confirmed.Status.State != "expired" {
					return w.retryOperation(ctx, id, fmt.Errorf("drain expiry for existing silence %s was not confirmed", silence.ID))
				}
			}
			return w.retryOperation(ctx, id, errors.New("drain is awaiting discovery of the submitted silence marker"))
		}
		return w.expireDrainRecovery(ctx, id, op, candidates, canonical)
	}
	if canonical == nil {
		if !op.DeadlineAt.IsZero() && !now.Before(op.DeadlineAt) {
			return w.fail(ctx, id, errors.New("operation deadline passed and authenticated marker inventory found no submitted silence"))
		}
		if op.Phase != "prepared" {
			// A submitted operation has crossed the side-effect boundary. Never
			// replay it blindly: keep discovering its unique marker until the
			// finite deadline proves that no Alertmanager object was created.
			return w.retryOperation(ctx, id, errors.New("submitted silence marker is not yet discoverable"))
		}
		if !w.enabled() {
			return w.fail(ctx, id, errors.New("suppressing silence mutation was disabled before any owned Alertmanager object was found"))
		}
		if err := w.authz.Authorize(ctx, op.Actor, "silence"); err != nil {
			if op.Phase == "prepared" {
				return w.fail(ctx, id, err)
			}
			return w.retryOperation(ctx, id, fmt.Errorf("awaiting marker discovery after authorization loss: %w", err))
		}
		if err := w.policy.ValidateMatchers(op.RequestedMatchers); err != nil {
			return w.fail(ctx, id, err)
		}
		alerts, err := w.api.ListAlerts(ctx)
		if err != nil {
			return w.retryOperation(ctx, id, fmt.Errorf("inventory alerts: %w", err))
		}
		count := currentMatchCount(alerts, op.RequestedMatchers, now)
		if count < w.policy.Config.MinCurrentlyMatchedAlerts || count > w.policy.Config.MaxCurrentlyMatchedAlerts {
			return w.fail(ctx, id, fmt.Errorf("final inventory matched %d alerts; allowed range is [%d,%d]", count, w.policy.Config.MinCurrentlyMatchedAlerts, w.policy.Config.MaxCurrentlyMatchedAlerts))
		}
		var updateID string
		live := liveSilences(candidates)
		if len(live) > 0 {
			sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })
			updateID = live[0].ID
		}
		request := AMSilence{ID: updateID, Matchers: canonicalMatchers(op.RequestedMatchers), StartsAt: now, EndsAt: op.RequestedEndsAt.UTC(), CreatedBy: silenceCreatedBy, Comment: marker(op.OwnershipID, id, op.OwnershipGeneration) + " " + op.Reason}
		if err := w.markSubmitted(ctx, id, now); err != nil {
			return err
		}
		op.Phase = "submitted"
		// Submission is durable before the final authorization so there is no
		// ConfigMap round trip between this check and the suppressing API call.
		if err := w.authz.Authorize(ctx, op.Actor, "silence"); err != nil {
			return w.fail(ctx, id, err)
		}
		silenceID, postErr := w.api.UpsertSilence(ctx, request)
		if postErr != nil {
			var httpErr *AMHTTPError
			if errors.As(postErr, &httpErr) && httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
				return w.fail(ctx, id, postErr)
			}
			return w.retryOperation(ctx, id, postErr)
		}
		observed, readErr := w.api.GetSilence(ctx, silenceID)
		if readErr != nil {
			return w.retryOperation(ctx, id, fmt.Errorf("read back submitted silence: %w", readErr))
		}
		canonical = &observed
		silences, err = w.api.ListSilences(ctx)
		if err != nil {
			return w.retryOperation(ctx, id, err)
		}
		candidates = ownedSilences(silences, op.OwnershipID)
	} else {
		if err := w.authz.Authorize(ctx, op.Actor, "silence"); err != nil {
			return w.expireUnauthorizedRecovery(ctx, id, op, candidates, *canonical, err)
		}
		if w.metrics != nil {
			w.metrics.silenceRecoveries.WithLabelValues("adopted").Inc()
		}
	}
	if canonical == nil {
		return w.retryOperation(ctx, id, errors.New("submitted silence was not discoverable by operation marker"))
	}
	for _, duplicate := range candidates {
		if duplicate.ID == canonical.ID || duplicate.Status.State == "expired" {
			continue
		}
		if err := w.api.ExpireSilence(ctx, duplicate.ID); err != nil {
			return w.retryOperation(ctx, id, fmt.Errorf("expire duplicate silence %s: %w", duplicate.ID, err))
		}
		read, err := w.api.GetSilence(ctx, duplicate.ID)
		if err != nil || read.Status.State != "expired" {
			return w.retryOperation(ctx, id, fmt.Errorf("duplicate silence %s expiry was not confirmed", duplicate.ID))
		}
		if w.metrics != nil {
			w.metrics.silenceRecoveries.WithLabelValues("duplicate-expired").Inc()
		}
	}
	readback, err := w.api.GetSilence(ctx, canonical.ID)
	if err != nil {
		return w.retryOperation(ctx, id, err)
	}
	if op.ReplacesOwnershipID != "" && op.ReplacesOwnershipID != op.OwnershipID {
		for _, source := range ownedSilences(silences, op.ReplacesOwnershipID) {
			if source.Status.State == "active" || source.Status.State == "pending" {
				if err := w.api.ExpireSilence(ctx, source.ID); err != nil {
					return w.retryOperation(ctx, id, err)
				}
				confirmed, err := w.api.GetSilence(ctx, source.ID)
				if err != nil || confirmed.Status.State != "expired" {
					return w.retryOperation(ctx, id, errors.New("replacement source expiry was not confirmed"))
				}
			}
		}
	}
	return w.complete(ctx, id, op, readback)
}

func (w *OperationWorker) processExpire(ctx context.Context, id string, op *SilenceOperation, silences []AMSilence) error {
	candidates := ownedSilences(silences, op.OwnershipID)
	live := liveSilences(candidates)
	if len(live) == 0 {
		return w.completeExpire(ctx, id, op)
	}
	if op.Actor != "alert-proxy-drain" && op.Phase == "prepared" {
		if err := w.markSubmitted(ctx, id, w.now().UTC()); err != nil {
			return err
		}
		op.Phase = "submitted"
		// Expiry recovery is safe after this point. Keep the final user
		// authorization immediately adjacent to the first expiry call.
		if err := w.authz.Authorize(ctx, op.Actor, "unsilence"); err != nil {
			return w.fail(ctx, id, err)
		}
	}
	for _, silence := range live {
		if err := w.api.ExpireSilence(ctx, silence.ID); err != nil {
			return w.retryOperation(ctx, id, err)
		}
		confirmed, err := w.api.GetSilence(ctx, silence.ID)
		if err != nil {
			return w.retryOperation(ctx, id, err)
		}
		if confirmed.Status.State != "expired" {
			return w.retryOperation(ctx, id, fmt.Errorf("silence %s is %s after expiry", silence.ID, confirmed.Status.State))
		}
	}
	return w.completeExpire(ctx, id, op)
}

func (w *OperationWorker) completeExpire(ctx context.Context, id string, op *SilenceOperation) error {
	recorded := false
	err := w.store.Update(ctx, func(s *State) error {
		recorded = false
		x := s.SilenceOperations[id]
		if x == nil {
			return nil
		}
		recorded = true
		x.Phase = "completed"
		x.CompletedAt = w.now().UTC()
		completeOperationRequests(s, id, x.CompletedAt)
		if ref := s.SilenceRefs[op.OwnershipID]; ref != nil {
			ref.LastObservedState = "expired"
			ref.LastObservedAt = x.CompletedAt
			ref.Generation = op.OwnershipGeneration
			ref.LastOperationID = id
		}
		message := fmt.Sprintf("<@%s> expired the Alertmanager silence for `%s`. Reason: %s", op.Actor, slackEscape(canonicalMatcherString(refMatchers(s, op.OwnershipID))), slackEscape(op.Reason))
		w.finalizeSilenceAudit(s, op, message)
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		w.metrics.silenceActions.WithLabelValues("expire", "", "success").Inc()
	}
	if err == nil && recorded {
		logSilenceOperation(id, op, "success", nil)
	}
	return err
}

func liveSilences(silences []AMSilence) []AMSilence {
	live := make([]AMSilence, 0, len(silences))
	for _, silence := range silences {
		if silence.Status.State == "active" || silence.Status.State == "pending" {
			live = append(live, silence)
		}
	}
	return live
}

func (w *OperationWorker) markSubmitted(ctx context.Context, id string, now time.Time) error {
	return w.store.Update(ctx, func(s *State) error {
		x := s.SilenceOperations[id]
		if x == nil || x.Phase == "completed" || x.Phase == "failed" {
			return errors.New("silence operation disappeared before submission")
		}
		x.Phase = "submitted"
		x.LastAttemptAt = now
		return nil
	})
}

func (w *OperationWorker) draining(ctx context.Context) (bool, error) {
	state, err := w.store.Read(ctx)
	if err != nil {
		return false, err
	}
	return state.Mode == "draining", nil
}

func (w *OperationWorker) expireDrainRecovery(ctx context.Context, id string, op *SilenceOperation, candidates []AMSilence, canonical *AMSilence) error {
	var observed *AMSilence
	for _, silence := range liveSilences(candidates) {
		if err := w.api.ExpireSilence(ctx, silence.ID); err != nil {
			return w.retryOperation(ctx, id, fmt.Errorf("expire suppressing operation during drain %s: %w", silence.ID, err))
		}
		confirmed, err := w.api.GetSilence(ctx, silence.ID)
		if err != nil || confirmed.Status.State != "expired" {
			return w.retryOperation(ctx, id, fmt.Errorf("drain expiry for silence %s was not confirmed", silence.ID))
		}
		if canonical != nil && silence.ID == canonical.ID {
			copy := confirmed
			observed = &copy
		}
	}
	if observed == nil && canonical != nil {
		copy := *canonical
		copy.Status.State = "expired"
		observed = &copy
	}
	now := w.now().UTC()
	return w.store.Update(ctx, func(s *State) error {
		x := s.SilenceOperations[id]
		if x == nil {
			return nil
		}
		x.Phase = "failed"
		x.CompletedAt = now
		x.LastError = "suppressing operation was cancelled and any discovered side effect was expired during rollback drain"
		completeOperationRequests(s, id, now)
		if observed != nil {
			x.ObservedSilenceIDs = []string{observed.ID}
			shortID, channel, threadTS, parentPostID := operationCorrelation(s, op)
			ref := &SilenceRef{SilenceID: observed.ID, ShortID: shortID, Channel: channel, ThreadTS: threadTS, ParentPostID: parentPostID, CanonicalMatchers: canonicalMatchers(observed.Matchers), Source: op.Source, Generation: op.OwnershipGeneration, Origin: op.Origin, Actor: op.Actor, Reason: op.Reason, CreatedAt: op.RequestedAt, CanonicalStartsAt: observed.StartsAt, CanonicalEndsAt: observed.EndsAt, LastObservedState: "expired", LastObservedAt: now, LastOperationID: id}
			s.SilenceRefs[op.OwnershipID] = ref
			w.enqueueAudit(s, op, fmt.Sprintf("Rollback drain recovered and expired the Alertmanager silence for `%s`; paging suppression was confirmed removed.", slackEscape(canonicalMatcherString(observed.Matchers))), false)
		} else if ref := s.SilenceRefs[op.OwnershipID]; ref != nil {
			ref.LastObservedState = "expired"
			ref.LastObservedAt = now
			w.finalizeSilenceAuditForRef(s, op, ref, "Rollback drain confirmed that this Alertmanager silence is no longer suppressing paging.")
		}
		return nil
	})
}

func (w *OperationWorker) expireUnauthorizedRecovery(ctx context.Context, id string, op *SilenceOperation, candidates []AMSilence, canonical AMSilence, authErr error) error {
	for _, silence := range liveSilences(candidates) {
		if err := w.api.ExpireSilence(ctx, silence.ID); err != nil {
			return w.retryOperation(ctx, id, fmt.Errorf("expire unauthorized recovered silence %s: %w", silence.ID, err))
		}
		confirmed, err := w.api.GetSilence(ctx, silence.ID)
		if err != nil || confirmed.Status.State != "expired" {
			return w.retryOperation(ctx, id, fmt.Errorf("unauthorized recovered silence %s expiry was not confirmed", silence.ID))
		}
		if silence.ID == canonical.ID {
			canonical = confirmed
		}
	}
	now := w.now().UTC()
	recorded := false
	err := w.store.Update(ctx, func(s *State) error {
		recorded = false
		x := s.SilenceOperations[id]
		if x == nil {
			return nil
		}
		recorded = true
		x.Phase = "failed"
		x.CompletedAt = now
		x.LastError = "authorization was lost during recovery; recovered suppression was expired: " + authErr.Error()
		x.ObservedSilenceIDs = []string{canonical.ID}
		completeOperationRequests(s, id, now)
		shortID, channel, threadTS, parentPostID := operationCorrelation(s, op)
		ref := &SilenceRef{SilenceID: canonical.ID, ShortID: shortID, Channel: channel, ThreadTS: threadTS, ParentPostID: parentPostID, CanonicalMatchers: canonicalMatchers(canonical.Matchers), Source: op.Source, Generation: op.OwnershipGeneration, Origin: op.Origin, Actor: op.Actor, Reason: op.Reason, CreatedAt: op.RequestedAt, CanonicalStartsAt: canonical.StartsAt, CanonicalEndsAt: canonical.EndsAt, LastObservedState: "expired", LastObservedAt: now, LastOperationID: id}
		s.SilenceRefs[op.OwnershipID] = ref
		message := fmt.Sprintf("Recovered an Alertmanager silence requested by <@%s>, but authorization was no longer valid; the proxy expired `%s` and confirmed paging suppression was removed.", slackEscape(op.Actor), slackEscape(canonicalMatcherString(canonical.Matchers)))
		w.enqueueAudit(s, op, message, false)
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		w.metrics.silenceRecoveries.WithLabelValues("failed").Inc()
	}
	if err == nil && recorded {
		logSilenceOperation(id, op, "recovered-expired", authErr)
	}
	return err
}

func (w *OperationWorker) complete(ctx context.Context, id string, op *SilenceOperation, canonical AMSilence) error {
	recorded := false
	err := w.store.Update(ctx, func(s *State) error {
		recorded = false
		x := s.SilenceOperations[id]
		if x == nil {
			return nil
		}
		recorded = true
		x.Phase = "completed"
		x.CompletedAt = w.now().UTC()
		completeOperationRequests(s, id, x.CompletedAt)
		x.ObservedSilenceIDs = []string{canonical.ID}
		if previous := s.SilenceRefs[op.OwnershipID]; previous != nil && (previous.LastObservedState == "active" || previous.LastObservedState == "pending") {
			w.finalizeSilenceAuditForRef(s, op, previous, fmt.Sprintf("<@%s> superseded this Alertmanager silence with a new requested expiry. Reason: %s", slackEscape(op.Actor), slackEscape(op.Reason)))
		}
		shortID, channel, threadTS, parentPostID := operationCorrelation(s, op)
		ref := &SilenceRef{SilenceID: canonical.ID, ShortID: shortID, Channel: channel, ThreadTS: threadTS, ParentPostID: parentPostID, CanonicalMatchers: canonicalMatchers(canonical.Matchers), Source: op.Source, Generation: op.OwnershipGeneration, Origin: op.Origin, Actor: op.Actor, Reason: op.Reason, CreatedAt: op.RequestedAt, CanonicalStartsAt: canonical.StartsAt, CanonicalEndsAt: canonical.EndsAt, LastObservedState: canonical.Status.State, LastObservedAt: x.CompletedAt, LastOperationID: id}
		s.SilenceRefs[op.OwnershipID] = ref
		if op.ReplacesOwnershipID != "" && op.ReplacesOwnershipID != op.OwnershipID {
			if replaced := s.SilenceRefs[op.ReplacesOwnershipID]; replaced != nil {
				replaced.LastObservedState = "expired"
				replaced.LastObservedAt = x.CompletedAt
				w.finalizeSilenceAuditForRef(s, op, replaced, fmt.Sprintf("<@%s> replaced and expired this Alertmanager silence while changing matcher scope. Reason: %s", slackEscape(op.Actor), slackEscape(op.Reason)))
			}
		}
		message := fmt.Sprintf("<@%s> requested an Alertmanager silence for `%s` until %s UTC. Canonical state: *%s*. Reason: %s", slackEscape(op.Actor), slackEscape(canonicalMatcherString(canonical.Matchers)), canonical.EndsAt.UTC().Format("2006-01-02 15:04"), slackEscape(canonical.Status.State), slackEscape(op.Reason))
		auditedByClose := false
		if op.Origin != nil {
			if g := s.Groups[op.Origin.GroupKey]; g != nil && g.EpisodeID == op.Origin.EpisodeID && g.ClosedAt.IsZero() {
				switch canonical.Status.State {
				case "active":
					g.ScheduledSilence = nil
					g.Status = "silenced"
					closeEpisode(s, g, x.CompletedAt, "silenced", w.renderer.RenderSilencedParent(g, op.Actor, canonical.Matchers, canonical.EndsAt), w.renderer.RenderSilenceAudit(message, shortID, w.enabled()))
					ref.AuditPostID = "close-reply:" + g.EpisodeID
					auditedByClose = true
				case "pending":
					g.ScheduledSilence = &SilencePresentation{Actor: op.Actor, Matchers: canonicalMatchers(canonical.Matchers), StartsAt: canonical.StartsAt, EndsAt: canonical.EndsAt, Operation: id}
					queueParentRender(s, g)
				}
			}
		}
		if !auditedByClose {
			w.enqueueAudit(s, op, message, canonical.Status.State == "active" || canonical.Status.State == "pending")
		}
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		w.metrics.silenceActions.WithLabelValues(op.Action, op.Preset, "success").Inc()
	}
	if err == nil && recorded {
		logSilenceOperation(id, op, "success", nil)
	}
	return err
}

func (w *OperationWorker) enqueueAudit(s *State, op *SilenceOperation, message string, controls bool) {
	ref := s.SilenceRefs[op.OwnershipID]
	if ref == nil {
		return
	}
	channel, threadTS, depends := ref.Channel, ref.ThreadTS, ref.ParentPostID
	if channel == "" {
		return
	}
	payload := w.renderer.RenderEventReply(message)
	if controls {
		payload = w.renderer.RenderSilenceAudit(message, ref.ShortID, w.enabled())
	}
	groupKey := ""
	if op.Origin != nil {
		groupKey = op.Origin.GroupKey
	}
	id := "silence-audit:" + randomID(12)
	ref.AuditPostID = id
	putOutbox(s, id, &OutboxWork{GroupKey: groupKey, Object: "silenceAudit", Kind: "post", DesiredRevision: 1, Target: OutboxTarget{Channel: channel, ThreadTS: threadTS}, DependsOnPostID: depends, ImmutablePayload: &payload, Phase: "pending"})
}

func operationCorrelation(s *State, op *SilenceOperation) (shortID, channel, threadTS, parentPostID string) {
	shortID = randomID(8)
	if previous := s.SilenceRefs[op.OwnershipID]; previous != nil {
		shortID, channel, threadTS, parentPostID = previous.ShortID, previous.Channel, previous.ThreadTS, previous.ParentPostID
	}
	if op.ReplyTarget != nil && op.ReplyTarget.Channel != "" {
		channel, threadTS, parentPostID = op.ReplyTarget.Channel, op.ReplyTarget.ThreadTS, ""
	}
	if shortID == "" {
		shortID = randomID(8)
	}
	if op.Origin != nil {
		if group := s.Groups[op.Origin.GroupKey]; group != nil {
			channel = group.Channel
			if group.Parent != nil {
				threadTS, parentPostID = group.Parent.MessageTS, group.Parent.PostID
			}
		}
	}
	return
}

func (w *OperationWorker) finalizeSilenceAudit(s *State, op *SilenceOperation, message string) {
	ref := s.SilenceRefs[op.OwnershipID]
	if ref == nil {
		w.enqueueAudit(s, op, message, false)
		return
	}
	w.finalizeSilenceAuditForRef(s, op, ref, message)
}

func (w *OperationWorker) finalizeSilenceAuditForRef(s *State, op *SilenceOperation, ref *SilenceRef, message string) {
	payload := w.renderer.RenderEventReply(message)
	if ref.AuditMessageTS != "" {
		putOutbox(s, "silence-audit-final:"+op.OwnershipID+":"+ref.AuditMessageTS, &OutboxWork{GroupKey: originGroupKey(op), Object: "silenceAudit", Kind: "update", DesiredRevision: 1, Target: OutboxTarget{Channel: ref.Channel, MessageTS: ref.AuditMessageTS}, ImmutablePayload: &payload, AuditFallback: &OutboxTarget{Channel: ref.Channel, ThreadTS: ref.ThreadTS}, AuditDependsOn: ref.ParentPostID, Phase: "pending"})
		return
	}
	if ref.AuditPostID != "" {
		if work := s.Outbox[ref.AuditPostID]; work != nil {
			work.ImmutablePayload = &payload
			work.DesiredRevision++
			if work.Phase != "inFlight" {
				work.Phase = "pending"
			}
			return
		}
	}
	// Older state may predate durable audit correlation. Preserve the expiry
	// report as a new control-free reply rather than silently losing it.
	w.enqueueAudit(s, op, message, false)
}
func (w *OperationWorker) fail(ctx context.Context, id string, err error) error {
	if w.metrics != nil {
		w.metrics.silenceRecoveries.WithLabelValues("failed").Inc()
	}
	var failed *SilenceOperation
	persistErr := w.store.Update(ctx, func(s *State) error {
		failed = nil
		if x := s.SilenceOperations[id]; x != nil {
			copy := *x
			failed = &copy
			x.Phase = "failed"
			x.LastError = err.Error()
			x.CompletedAt = w.now().UTC()
			completeOperationRequests(s, id, x.CompletedAt)
			if target, groupKey, depends := operationReplyTarget(s, x); target.Channel != "" {
				message := fmt.Sprintf("Alertmanager silence operation requested by <@%s> failed: %s", slackEscape(x.Actor), slackEscape(err.Error()))
				payload := w.renderer.RenderEventReply(message)
				putOutbox(s, "silence-failure:"+id, &OutboxWork{GroupKey: groupKey, Object: "eventReply", Kind: "post", Target: target, DependsOnPostID: depends, ImmutablePayload: &payload, Phase: "pending"})
			}
		}
		return nil
	})
	if persistErr == nil && failed != nil {
		if w.metrics != nil {
			w.metrics.silenceActions.WithLabelValues(failed.Action, failed.Preset, "failed").Inc()
		}
		logSilenceOperation(id, failed, "failed", err)
	}
	return persistErr
}

func logSilenceOperation(id string, op *SilenceOperation, result string, err error) {
	entry := logrus.WithFields(logrus.Fields{"component": "alert-proxy-silence-operation", "operation": id, "action": op.Action, "actor": op.Actor, "ownership": op.OwnershipID, "source": op.Source, "result": result})
	if err != nil {
		entry.WithError(err).Warn("processed Alertmanager silence operation")
		return
	}
	entry.Info("processed Alertmanager silence operation")
}

func operationReplyTarget(s *State, op *SilenceOperation) (OutboxTarget, string, string) {
	if op.ReplyTarget != nil && op.ReplyTarget.Channel != "" {
		return *op.ReplyTarget, "", ""
	}
	for _, ownership := range []string{op.OwnershipID, op.ReplacesOwnershipID} {
		if ref := s.SilenceRefs[ownership]; ref != nil && ref.Channel != "" {
			return OutboxTarget{Channel: ref.Channel, ThreadTS: ref.ThreadTS}, originGroupKey(op), ref.ParentPostID
		}
	}
	if op.Origin != nil {
		if group := s.Groups[op.Origin.GroupKey]; group != nil && group.EpisodeID == op.Origin.EpisodeID && group.Parent != nil {
			target := OutboxTarget{Channel: group.Channel, ThreadTS: group.Parent.MessageTS}
			depends := ""
			if target.ThreadTS == "" {
				depends = group.Parent.PostID
			}
			return target, op.Origin.GroupKey, depends
		}
	}
	return OutboxTarget{}, "", ""
}

func originGroupKey(op *SilenceOperation) string {
	if op.Origin != nil {
		return op.Origin.GroupKey
	}
	return ""
}

func completeOperationRequests(s *State, operationID string, now time.Time) {
	for _, request := range s.Requests {
		if request.OperationID == operationID {
			request.Phase = "processed"
			request.CompletedAt = now
		}
	}
}
func (w *OperationWorker) retryOperation(ctx context.Context, id string, err error) error {
	if w.metrics != nil {
		w.metrics.alertmanagerErrors.WithLabelValues("mutation", "alertmanager").Inc()
	}
	if persistErr := w.store.Update(ctx, func(s *State) error {
		if x := s.SilenceOperations[id]; x != nil {
			if x.Phase != "prepared" {
				x.Phase = "ambiguous"
			}
			x.LastError = err.Error()
			x.LastAttemptAt = w.now().UTC()
			x.Attempt++
		}
		return nil
	}); persistErr != nil {
		return persistErr
	}
	return err
}

func ownedSilences(all []AMSilence, ownership string) []AMSilence {
	out := []AMSilence{}
	for _, s := range all {
		owner, _, _, ok := parseMarker(s.Comment)
		if s.CreatedBy == silenceCreatedBy && ok && owner == ownership {
			out = append(out, s)
		}
	}
	return out
}
func refMatchers(s *State, ownership string) []Matcher {
	if ref := s.SilenceRefs[ownership]; ref != nil {
		return ref.CanonicalMatchers
	}
	return nil
}

func (w *OperationWorker) RecoverAll(ctx context.Context) error {
	for {
		did, err := w.ProcessNext(ctx)
		if err != nil {
			return err
		}
		if !did {
			return nil
		}
	}
}

func (w *OperationWorker) RefreshOwned(ctx context.Context, silences []AMSilence) error {
	w.reconcileMu.Lock()
	defer w.reconcileMu.Unlock()
	return w.refreshOwned(ctx, silences)
}

func (w *OperationWorker) Reconcile(ctx context.Context) error {
	w.reconcileMu.Lock()
	defer w.reconcileMu.Unlock()
	silences, err := w.api.ListSilences(ctx)
	if err != nil {
		return err
	}
	return w.refreshOwned(ctx, silences)
}

func (w *OperationWorker) refreshOwned(ctx context.Context, silences []AMSilence) error {
	observed := map[string]AMSilence{}
	for _, silence := range silences {
		ownership, _, generation, owned := parseMarker(silence.Comment)
		if !owned || silence.CreatedBy != silenceCreatedBy {
			continue
		}
		current, ok := observed[ownership]
		_, _, currentGeneration, _ := parseMarker(current.Comment)
		if !ok || generation > currentGeneration || generation == currentGeneration && silence.ID < current.ID {
			observed[ownership] = silence
		}
	}
	state, err := w.store.Read(ctx)
	if err != nil {
		return err
	}
	if !ownedRefreshNeeded(state, observed) {
		return nil
	}
	now := w.now().UTC()
	changed := false
	err = w.store.Update(ctx, func(s *State) error {
		changed = false
		for ownership, ref := range s.SilenceRefs {
			canonical, ok := observed[ownership]
			if !ok {
				if ref.LastObservedState == "active" || ref.LastObservedState == "pending" {
					oldState := ref.LastObservedState
					ref.LastObservedState = "deleted"
					ref.LastObservedAt = now
					changed = true
					op := auditOperationForRef(ownership, ref)
					w.finalizeSilenceAuditForRef(s, op, ref, fmt.Sprintf("Alertmanager no longer reports this previously *%s* silence for `%s`; its Extend and Unsilence controls have been retired.", oldState, slackEscape(canonicalMatcherString(ref.CanonicalMatchers))))
					if ref.Origin != nil {
						if g := s.Groups[ref.Origin.GroupKey]; g != nil && g.EpisodeID == ref.Origin.EpisodeID {
							if g.ScheduledSilence != nil {
								g.ScheduledSilence = nil
								queueParentRender(s, g)
							}
						}
					}
				}
				continue
			}
			oldState := ref.LastObservedState
			matchers := canonicalMatchers(canonical.Matchers)
			if !sameSilenceObservation(ref, canonical, matchers) {
				ref.SilenceID = canonical.ID
				ref.CanonicalMatchers = matchers
				ref.CanonicalStartsAt = canonical.StartsAt
				ref.CanonicalEndsAt = canonical.EndsAt
				ref.LastObservedState = canonical.Status.State
				ref.LastObservedAt = now
				changed = true
			}
			if (oldState == "active" || oldState == "pending") && canonical.Status.State != "active" && canonical.Status.State != "pending" {
				op := auditOperationForRef(ownership, ref)
				w.finalizeSilenceAuditForRef(s, op, ref, fmt.Sprintf("This Alertmanager silence for `%s` is now *%s*; its Extend and Unsilence controls have been retired.", slackEscape(canonicalMatcherString(canonical.Matchers)), slackEscape(canonical.Status.State)))
			}
			if canonical.Status.State != "pending" && ref.Origin != nil {
				if g := s.Groups[ref.Origin.GroupKey]; g != nil && g.EpisodeID == ref.Origin.EpisodeID {
					if g.ScheduledSilence != nil {
						g.ScheduledSilence = nil
						queueParentRender(s, g)
						changed = true
					}
				}
			}
			if oldState == "pending" && canonical.Status.State == "active" && ref.Origin != nil {
				if g := s.Groups[ref.Origin.GroupKey]; g != nil && g.EpisodeID == ref.Origin.EpisodeID && g.ClosedAt.IsZero() {
					g.ScheduledSilence = nil
					message := fmt.Sprintf("<@%s> created an Alertmanager silence for `%s` until %s UTC. Canonical state: *active*. Reason: %s", slackEscape(ref.Actor), slackEscape(canonicalMatcherString(canonical.Matchers)), canonical.EndsAt.UTC().Format("2006-01-02 15:04"), slackEscape(ref.Reason))
					g.Status = "silenced"
					closeEpisode(s, g, now, "silenced", w.renderer.RenderSilencedParent(g, ref.Actor, canonical.Matchers, canonical.EndsAt), w.renderer.RenderEventReply(message))
				}
			}
		}
		return nil
	})
	if err == nil && changed && w.outboxWake != nil {
		w.outboxWake()
	}
	return err
}

func ownedRefreshNeeded(s *State, observed map[string]AMSilence) bool {
	for ownership, ref := range s.SilenceRefs {
		canonical, ok := observed[ownership]
		if !ok {
			if ref.LastObservedState == "active" || ref.LastObservedState == "pending" {
				return true
			}
			continue
		}
		if !sameSilenceObservation(ref, canonical, canonicalMatchers(canonical.Matchers)) {
			return true
		}
		if canonical.Status.State != "pending" && ref.Origin != nil {
			if group := s.Groups[ref.Origin.GroupKey]; group != nil && group.EpisodeID == ref.Origin.EpisodeID && group.ScheduledSilence != nil {
				return true
			}
		}
	}
	return false
}

func sameSilenceObservation(ref *SilenceRef, canonical AMSilence, matchers []Matcher) bool {
	if ref.SilenceID != canonical.ID || ref.LastObservedState != canonical.Status.State || !ref.CanonicalStartsAt.Equal(canonical.StartsAt) || !ref.CanonicalEndsAt.Equal(canonical.EndsAt) || len(ref.CanonicalMatchers) != len(matchers) {
		return false
	}
	for i := range matchers {
		if ref.CanonicalMatchers[i] != matchers[i] {
			return false
		}
	}
	return true
}

func auditOperationForRef(ownership string, ref *SilenceRef) *SilenceOperation {
	return &SilenceOperation{OwnershipID: ownership, Actor: ref.Actor, Reason: ref.Reason, Source: ref.Source, Origin: ref.Origin, ReplyTarget: &OutboxTarget{Channel: ref.Channel, ThreadTS: ref.ThreadTS}}
}

func queueParentRender(s *State, g *GroupState) {
	if g == nil || g.Parent == nil || !g.ClosedAt.IsZero() {
		return
	}
	g.Parent.DesiredRevision++
	kind := "post"
	target := OutboxTarget{Channel: g.Channel}
	if g.Parent.MessageTS != "" {
		kind = "update"
		target.MessageTS = g.Parent.MessageTS
	}
	putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: kind, DesiredRevision: g.Parent.DesiredRevision, Target: target, ProbeDelivery: isProbeGroup(g), Phase: "pending"})
}
