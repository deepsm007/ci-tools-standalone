package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type LifecycleSweeper struct {
	store                               StateStore
	renderer                            *Renderer
	repeatInterval, escalationThreshold time.Duration
	deliveryTTL, requestTTL, auditTTL   time.Duration
	now                                 func() time.Time
	wakeOutbox                          func()
}

func (s *LifecycleSweeper) Run(ctx context.Context) {
	_ = s.Sweep(ctx)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.Sweep(ctx)
		}
	}
}
func (s *LifecycleSweeper) Sweep(ctx context.Context) error {
	now := s.now().UTC()
	changed := false
	err := s.store.Update(ctx, func(state *State) error {
		changed = false
		if s.deliveryTTL > 0 && s.requestTTL > 0 && s.auditTTL > 0 {
			pruneState(state, now, s.deliveryTTL, s.requestTTL, s.auditTTL)
		}
		for key, g := range state.Groups {
			if !g.ClosedAt.IsZero() {
				if now.Sub(g.ClosedAt) > time.Hour && !hasGroupOutbox(state, key) {
					delete(state.Groups, key)
					changed = true
				}
				continue
			}
			if now.Sub(g.LastSeen) > 3*s.repeatInterval {
				g.Status = "stale"
				final := *g
				final.ClosedReason = "stale"
				closeEpisode(state, g, now, "stale", s.renderer.RenderParent(&final, false), s.renderer.RenderEventReply("This Alertmanager notification group is no longer tracked; no explicit resolution was inferred."))
				changed = true
				continue
			}
			if !isProbeGroup(g) && g.Ack == nil && g.EscalatedAt.IsZero() && g.Status == "firing" && now.Sub(g.EpisodeStartedAt) >= s.escalationThreshold {
				g.EscalatedAt = now
				payload := s.renderer.RenderEventReply(fmt.Sprintf(":warning: This notification group has been firing for %s without acknowledgement.", humanDuration(now.Sub(g.EpisodeStartedAt))))
				target := OutboxTarget{Channel: g.Channel}
				depends := ""
				if g.Parent.MessageTS != "" {
					target.ThreadTS = g.Parent.MessageTS
				} else {
					depends = g.Parent.PostID
				}
				putOutbox(state, "escalation:"+g.EpisodeID, &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: target, DependsOnPostID: depends, ImmutablePayload: &payload, Phase: "pending"})
				reactionPayload := SlackPayload{Text: "warning"}
				reactionTarget := OutboxTarget{Channel: g.Channel, MessageTS: g.Parent.MessageTS}
				putOutbox(state, "escalation-reaction:"+g.EpisodeID, &OutboxWork{GroupKey: g.GroupKey, Object: "reaction", Kind: "reaction", Target: reactionTarget, DependsOnPostID: depends, ImmutablePayload: &reactionPayload, Phase: "pending"})
				changed = true
			}
		}
		return nil
	})
	if err == nil && changed && s.wakeOutbox != nil {
		s.wakeOutbox()
	}
	return err
}

func hasGroupOutbox(state *State, key string) bool {
	for _, work := range state.Outbox {
		if work.GroupKey == key && work.Phase != "failed" {
			return true
		}
	}
	return false
}

type DrainController struct {
	store      StateStore
	api        AlertmanagerAPI
	operations *OperationWorker
	outbox     *OutboxWorker
	renderer   *Renderer
	now        func() time.Time
}

func (d *DrainController) Begin(ctx context.Context) error {
	now := d.now().UTC()
	live, err := d.api.ListSilences(ctx)
	if err != nil {
		return err
	}
	return d.store.Update(ctx, func(s *State) error {
		s.Mode = "draining"
		for _, silence := range live {
			ownership, operation, generation, owned := parseMarker(silence.Comment)
			if !owned || silence.CreatedBy != silenceCreatedBy || (silence.Status.State != "active" && silence.Status.State != "pending") {
				continue
			}
			if s.SilenceRefs[ownership] == nil {
				s.SilenceRefs[ownership] = &SilenceRef{SilenceID: silence.ID, CanonicalMatchers: canonicalMatchers(silence.Matchers), Source: "unknown", Generation: generation, Actor: "unknown", Reason: "adopted during rollback drain", CreatedAt: now, CanonicalStartsAt: silence.StartsAt, CanonicalEndsAt: silence.EndsAt, LastObservedState: silence.Status.State, LastObservedAt: now, LastOperationID: operation}
			} else {
				ref := s.SilenceRefs[ownership]
				ref.SilenceID = silence.ID
				ref.CanonicalMatchers = canonicalMatchers(silence.Matchers)
				ref.CanonicalStartsAt = silence.StartsAt
				ref.CanonicalEndsAt = silence.EndsAt
				ref.LastObservedState = silence.Status.State
				ref.LastObservedAt = now
			}
		}
		for _, g := range s.Groups {
			if g.ClosedAt.IsZero() {
				g.Status = "stale"
				final := *g
				final.ClosedReason = "stale"
				closeEpisode(s, g, now, "stale", d.renderer.RenderParent(&final, false), d.renderer.RenderEventReply("alert-proxy is draining for rollback; controls have been disabled."))
			}
		}
		for ownership, ref := range s.SilenceRefs {
			if ref.LastObservedState != "active" && ref.LastObservedState != "pending" {
				continue
			}
			if nonTerminalOperationFor(s, ownership) {
				continue
			}
			operationID := randomID(16)
			s.SilenceOperations[operationID] = &SilenceOperation{Action: "expire", OwnershipID: ownership, OwnershipGeneration: ref.Generation + 1, Actor: "alert-proxy-drain", Reason: "alert-proxy rollback drain", RequestedAt: now, DeadlineAt: now.Add(5 * time.Minute), Source: ref.Source, Origin: ref.Origin, Phase: "prepared"}
		}
		return nil
	})
}

func (d *DrainController) Run(ctx context.Context) error {
	for {
		if err := d.Begin(ctx); err != nil {
			if err := waitForDrainRetry(ctx); err != nil {
				return err
			}
			continue
		}
		did, err := d.operations.ProcessNext(ctx)
		if err != nil {
			if err := waitForDrainRetry(ctx); err != nil {
				return err
			}
			continue
		}
		if did {
			continue
		}
		did, err = d.outbox.ProcessNext(ctx)
		if err != nil {
			return err
		}
		if did {
			continue
		}
		complete, err := d.Verify(ctx)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		if err := waitForDrainRetry(ctx); err != nil {
			return err
		}
	}
}

func waitForDrainRetry(ctx context.Context) error {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *DrainController) Verify(ctx context.Context) (bool, error) {
	silences, err := d.api.ListSilences(ctx)
	if err != nil {
		return false, err
	}
	for _, silence := range silences {
		_, _, _, owned := parseMarker(silence.Comment)
		if owned && silence.CreatedBy == silenceCreatedBy && (silence.Status.State == "active" || silence.Status.State == "pending") {
			return false, nil
		}
	}
	state, err := d.store.Read(ctx)
	if err != nil {
		return false, err
	}
	if len(state.Outbox) > 0 {
		for _, work := range state.Outbox {
			if work.Phase == "failed" {
				return false, errors.New("drain contains permanently failed Slack close-out work")
			}
		}
		return false, nil
	}
	for _, op := range state.SilenceOperations {
		if op.Phase != "completed" && op.Phase != "failed" {
			return false, nil
		}
		if op.Phase == "failed" && op.Action == "expire" {
			return false, errors.New("drain contains a failed silence operation")
		}
	}
	err = d.store.Update(ctx, func(s *State) error {
		s.Groups = map[string]*GroupState{}
		for id, ref := range s.SilenceRefs {
			if ref.LastObservedState == "expired" {
				delete(s.SilenceRefs, id)
			}
		}
		return nil
	})
	return err == nil, err
}
