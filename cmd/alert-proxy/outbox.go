package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type SlackAPI interface {
	PostMessage(context.Context, OutboxTarget, SlackPayload) (string, error)
	UpdateMessage(context.Context, OutboxTarget, SlackPayload) error
	PostEphemeral(context.Context, string, string, string) error
	OpenView(context.Context, string, any) error
	AddReaction(context.Context, string, string, string) error
	LookupUserByEmail(context.Context, string) (string, error)
}

type SlackHTTPClient struct {
	TokenPath, BaseURL string
	Client             *http.Client
}
type slackResponse struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	TS      string `json:"ts"`
	Message struct {
		TS string `json:"ts"`
	} `json:"message"`
	User struct {
		ID string `json:"id"`
	} `json:"user"`
}
type SlackError struct{ Code string }

func (e *SlackError) Error() string { return "slack API: " + e.Code }

func (s *SlackHTTPClient) call(ctx context.Context, method string, payload any, out *slackResponse) error {
	token, err := readSecret(s.TokenPath)
	if err != nil {
		return fmt.Errorf("read Slack token: %w", err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	base := s.BaseURL
	if base == "" {
		base = "https://slack.com/api/"
	}
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/"+method, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("slack HTTP %d: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return err
	}
	if !out.OK {
		return &SlackError{Code: out.Error}
	}
	return nil
}
func (s *SlackHTTPClient) PostMessage(ctx context.Context, t OutboxTarget, p SlackPayload) (string, error) {
	var out slackResponse
	err := s.call(ctx, "chat.postMessage", map[string]any{"channel": t.Channel, "thread_ts": t.ThreadTS, "text": p.Text, "blocks": p.Blocks}, &out)
	return out.TS, err
}
func (s *SlackHTTPClient) UpdateMessage(ctx context.Context, t OutboxTarget, p SlackPayload) error {
	var out slackResponse
	return s.call(ctx, "chat.update", map[string]any{"channel": t.Channel, "ts": t.MessageTS, "text": p.Text, "blocks": p.Blocks}, &out)
}
func (s *SlackHTTPClient) PostEphemeral(ctx context.Context, channel, user, text string) error {
	var out slackResponse
	return s.call(ctx, "chat.postEphemeral", map[string]any{"channel": channel, "user": user, "text": text}, &out)
}
func (s *SlackHTTPClient) OpenView(ctx context.Context, trigger string, view any) error {
	var out slackResponse
	return s.call(ctx, "views.open", map[string]any{"trigger_id": trigger, "view": view}, &out)
}
func (s *SlackHTTPClient) AddReaction(ctx context.Context, channel, ts, name string) error {
	var out slackResponse
	return s.call(ctx, "reactions.add", map[string]any{"channel": channel, "timestamp": ts, "name": name}, &out)
}
func (s *SlackHTTPClient) LookupUserByEmail(ctx context.Context, email string) (string, error) {
	var out slackResponse
	err := s.call(ctx, "users.lookupByEmail", map[string]any{"email": email}, &out)
	return out.User.ID, err
}

type OutboxWorker struct {
	store    StateStore
	slack    SlackAPI
	renderer *Renderer
	metrics  *Metrics
	now      func() time.Time
	wake     chan struct{}
	rate     time.Duration
}

func NewOutboxWorker(store StateStore, slack SlackAPI, renderer *Renderer, metrics *Metrics) *OutboxWorker {
	return &OutboxWorker{store: store, slack: slack, renderer: renderer, metrics: metrics, now: time.Now, wake: make(chan struct{}, 1), rate: time.Second}
}
func (w *OutboxWorker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *OutboxWorker) RecoverInFlight(ctx context.Context) error {
	return w.store.Update(ctx, func(s *State) error {
		for _, work := range s.Outbox {
			if work.Phase != "inFlight" {
				continue
			}
			if work.Kind == "post" && work.Target.MessageTS == "" {
				if w.metrics != nil {
					w.metrics.ambiguousPosts.WithLabelValues(work.Object).Inc()
				}
			}
			work.Phase = "pending"
		}
		return nil
	})
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.rate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
		for {
			did, err := w.ProcessNext(ctx)
			if err != nil || !did {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

func (w *OutboxWorker) ProcessNext(ctx context.Context) (bool, error) {
	state, err := w.store.Read(ctx)
	if err != nil {
		return false, err
	}
	now := w.now()
	ids := sortedOutboxIDs(state)
	var id string
	var work *OutboxWork
	blockedGroups := map[string]bool{}
	for _, candidate := range ids {
		x := state.Outbox[candidate]
		if x.GroupKey != "" && blockedGroups[x.GroupKey] {
			continue
		}
		if x.Phase == "failed" {
			// A permanent failure is retained for alerting, but cannot ever
			// succeed in sequence. Do not let it starve newer parent close-outs.
			continue
		}
		if x.Phase == "pending" && !x.LastAttemptAt.IsZero() {
			delay := time.Second * time.Duration(1<<min(x.Attempt, 6))
			if now.Sub(x.LastAttemptAt) < delay {
				if x.GroupKey != "" {
					blockedGroups[x.GroupKey] = true
				}
				continue
			}
		}
		if x.DependsOnPostID != "" && x.Target.ThreadTS == "" {
			// An unresolved dependent is not an ordering barrier for the
			// prerequisite itself (or for independent close-out work).
			continue
		}
		id = candidate
		work = x
		break
	}
	if work == nil {
		return false, nil
	}
	var payload SlackPayload
	if work.ImmutablePayload != nil {
		payload = *work.ImmutablePayload
	} else {
		g := state.Groups[work.GroupKey]
		if g == nil {
			return false, w.failWork(ctx, id, "group no longer exists", true)
		}
		switch work.Object {
		case "parent":
			payload = w.renderer.RenderParent(g, true)
		case "memberList":
			payload = w.renderer.RenderMemberList(g)
		default:
			return false, w.failWork(ctx, id, "work has no immutable payload", true)
		}
	}
	if err := w.store.Update(ctx, func(s *State) error {
		x := s.Outbox[id]
		if x == nil {
			return errors.New("outbox work disappeared")
		}
		x.Phase = "inFlight"
		x.Attempt++
		x.LastAttemptAt = now
		return nil
	}); err != nil {
		return false, err
	}
	switch work.Kind {
	case "reaction":
		err = w.slack.AddReaction(ctx, work.Target.Channel, work.Target.MessageTS, payload.Text)
	case "update":
		err = w.slack.UpdateMessage(ctx, work.Target, payload)
	default:
		var ts string
		ts, err = w.slack.PostMessage(ctx, work.Target, payload)
		if err == nil && strings.TrimSpace(ts) == "" {
			err = errors.New("slack accepted chat.postMessage without returning a message timestamp")
		}
		if err == nil {
			commitErr := w.completePost(ctx, id, work, ts)
			if commitErr != nil && w.metrics != nil {
				w.metrics.ambiguousPosts.WithLabelValues(work.Object).Inc()
				w.metrics.outboxAttempts.WithLabelValues("post", "error").Inc()
			}
			return true, commitErr
		}
	}
	if err != nil {
		var se *SlackError
		if errors.As(err, &se) && work.Kind == "reaction" && (se.Code == "message_not_found" || se.Code == "missing_scope") {
			return true, w.dropWork(ctx, id)
		}
		if errors.As(err, &se) && se.Code == "message_not_found" && work.Kind == "update" {
			if work.Object == "memberListRetire" {
				return true, w.completeMemberListRetirement(ctx, id, work)
			}
			return true, w.replaceDeletedMessage(ctx, id, work)
		}
		if errors.As(err, &se) && se.Code == "cant_update_message" {
			return true, w.failWork(ctx, id, err.Error(), true)
		}
		if errors.As(err, &se) && isPermanentSlackError(se.Code) {
			return true, w.failWork(ctx, id, err.Error(), true)
		}
		if work.Kind == "post" && w.metrics != nil {
			w.metrics.ambiguousPosts.WithLabelValues(work.Object).Inc()
		}
		if w.metrics != nil {
			w.metrics.outboxAttempts.WithLabelValues(work.Kind, "error").Inc()
		}
		return true, w.failWork(ctx, id, err.Error(), false)
	}
	if work.Kind == "reaction" {
		return true, w.completeOneShot(ctx, id, work)
	}
	return true, w.completeUpdate(ctx, id, work)
}

func (w *OutboxWorker) dropWork(ctx context.Context, id string) error {
	return w.store.Update(ctx, func(s *State) error {
		delete(s.Outbox, id)
		return nil
	})
}

func isPermanentSlackError(code string) bool {
	switch code {
	case "invalid_blocks", "invalid_arguments", "channel_not_found", "not_in_channel", "invalid_auth", "account_inactive", "token_revoked":
		return true
	default:
		return false
	}
}

func (w *OutboxWorker) completePost(ctx context.Context, id string, attempt *OutboxWork, ts string) error {
	probeDelivery, recorded := false, false
	err := w.store.Update(ctx, func(s *State) error {
		probeDelivery, recorded = false, false
		work := s.Outbox[id]
		if work == nil {
			return nil
		}
		recorded = true
		probeDelivery = attempt.ProbeDelivery
		if g := s.Groups[work.GroupKey]; g != nil {
			var object *SlackObject
			if g.Parent != nil && g.Parent.PostID == strings.TrimPrefix(id, "parent:") {
				object = g.Parent
			}
			if g.MemberList != nil && g.MemberList.PostID == strings.TrimPrefix(id, "member-list:") {
				object = g.MemberList
			}
			if object != nil {
				object.MessageTS = ts
				object.Phase = "posted"
				object.AppliedRevision = attempt.DesiredRevision
			}
		}
		for _, dependent := range s.Outbox {
			if dependent.DependsOnPostID != "" && ("parent:"+dependent.DependsOnPostID == id || "member-list:"+dependent.DependsOnPostID == id) {
				if dependent.Kind == "reaction" {
					dependent.Target.MessageTS = ts
				} else {
					dependent.Target.ThreadTS = ts
				}
				dependent.DependsOnPostID = ""
			}
		}
		for _, ref := range s.SilenceRefs {
			if ref.ParentPostID != "" && ("parent:"+ref.ParentPostID == id || "member-list:"+ref.ParentPostID == id) {
				ref.ThreadTS = ts
				ref.ParentPostID = ""
			}
			if ref.AuditPostID == id {
				ref.AuditMessageTS = ts
			}
		}
		if work.ImmutablePayload == nil && work.DesiredRevision > attempt.DesiredRevision {
			work.Kind = "update"
			work.Target.MessageTS = ts
			work.Phase = "pending"
		} else if work.ImmutablePayload != nil && work.DesiredRevision > attempt.DesiredRevision {
			work.Kind = "update"
			work.Target.MessageTS = ts
			work.Phase = "pending"
		} else {
			delete(s.Outbox, id)
		}
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		action := "post"
		if attempt.Target.ThreadTS != "" {
			action = "thread"
		}
		w.metrics.slackMessages.WithLabelValues(action).Inc()
		w.metrics.outboxAttempts.WithLabelValues("post", "success").Inc()
		if probeDelivery {
			w.metrics.endToEndDeliveries.Inc()
		}
	}
	return err
}

func (w *OutboxWorker) completeUpdate(ctx context.Context, id string, attempt *OutboxWork) error {
	probeDelivery, recorded := false, false
	err := w.store.Update(ctx, func(s *State) error {
		probeDelivery, recorded = false, false
		work := s.Outbox[id]
		if work == nil {
			return nil
		}
		recorded = true
		probeDelivery = attempt.ProbeDelivery
		if g := s.Groups[work.GroupKey]; g != nil {
			var object *SlackObject
			if g.Parent != nil && g.Parent.MessageTS == attempt.Target.MessageTS {
				object = g.Parent
			}
			if g.MemberList != nil && g.MemberList.MessageTS == attempt.Target.MessageTS {
				object = g.MemberList
			}
			if object != nil {
				object.AppliedRevision = attempt.DesiredRevision
				object.Phase = "posted"
			}
		}
		if work.DesiredRevision > attempt.DesiredRevision {
			work.Phase = "pending"
		} else {
			if attempt.Object == "memberListRetire" && work.Object == "memberListRetire" {
				if g := s.Groups[work.GroupKey]; g != nil && g.MemberList != nil && g.MemberList.MessageTS == attempt.Target.MessageTS {
					g.MemberList = nil
				}
			}
			delete(s.Outbox, id)
		}
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		w.metrics.slackMessages.WithLabelValues("update").Inc()
		w.metrics.outboxAttempts.WithLabelValues("update", "success").Inc()
		if probeDelivery {
			w.metrics.endToEndDeliveries.Inc()
		}
	}
	return err
}

func (w *OutboxWorker) completeMemberListRetirement(ctx context.Context, id string, attempt *OutboxWork) error {
	return w.store.Update(ctx, func(s *State) error {
		work := s.Outbox[id]
		if work == nil {
			return nil
		}
		if g := s.Groups[work.GroupKey]; g != nil && g.MemberList != nil && g.MemberList.MessageTS == attempt.Target.MessageTS {
			g.MemberList = nil
		}
		delete(s.Outbox, id)
		return nil
	})
}
func (w *OutboxWorker) completeOneShot(ctx context.Context, id string, attempt *OutboxWork) error {
	recorded := false
	err := w.store.Update(ctx, func(s *State) error {
		recorded = s.Outbox[id] != nil
		delete(s.Outbox, id)
		return nil
	})
	if err == nil && recorded && w.metrics != nil {
		action := attempt.Kind
		if attempt.Target.ThreadTS != "" {
			action = "thread"
		}
		w.metrics.slackMessages.WithLabelValues(action).Inc()
		w.metrics.outboxAttempts.WithLabelValues(attempt.Kind, "success").Inc()
	}
	return err
}
func (w *OutboxWorker) failWork(ctx context.Context, id, msg string, permanent bool) error {
	return w.store.Update(ctx, func(s *State) error {
		if x := s.Outbox[id]; x != nil {
			x.LastError = msg
			if permanent {
				x.Phase = "failed"
			} else {
				x.Phase = "pending"
			}
		}
		return nil
	})
}

func (w *OutboxWorker) replaceDeletedMessage(ctx context.Context, id string, work *OutboxWork) error {
	return w.store.Update(ctx, func(s *State) error {
		current := s.Outbox[id]
		if current == nil {
			return nil
		}
		if work.Object == "silenceAudit" {
			sequence := current.Sequence
			for _, ref := range s.SilenceRefs {
				if ref.AuditMessageTS != work.Target.MessageTS || ref.Channel != "" && ref.Channel != work.Target.Channel {
					continue
				}
				newID := "silence-audit:" + randomID(12)
				target := OutboxTarget{Channel: ref.Channel, ThreadTS: ref.ThreadTS}
				if target.Channel == "" {
					target.Channel = work.Target.Channel
				}
				delete(s.Outbox, id)
				putOutbox(s, newID, &OutboxWork{GroupKey: work.GroupKey, Object: "silenceAudit", Kind: "post", DesiredRevision: current.DesiredRevision, Target: target, DependsOnPostID: ref.ParentPostID, ImmutablePayload: current.ImmutablePayload, Phase: "pending"})
				s.Outbox[newID].Sequence = sequence
				ref.AuditMessageTS = ""
				ref.AuditPostID = newID
				return nil
			}
			if current.AuditFallback != nil && current.AuditFallback.Channel != "" {
				newID := "silence-audit:" + randomID(12)
				delete(s.Outbox, id)
				putOutbox(s, newID, &OutboxWork{GroupKey: work.GroupKey, Object: "silenceAudit", Kind: "post", DesiredRevision: current.DesiredRevision, Target: *current.AuditFallback, DependsOnPostID: current.AuditDependsOn, ImmutablePayload: current.ImmutablePayload, Phase: "pending"})
				s.Outbox[newID].Sequence = sequence
				return nil
			}
			current.Phase = "failed"
			current.LastError = "deleted silence audit had no durable reference for replacement"
			return nil
		}
		g := s.Groups[work.GroupKey]
		// A detached close-out is already over; a deleted message does not need replacement.
		if g == nil || g.Parent == nil {
			delete(s.Outbox, id)
			return nil
		}
		if work.Object == "memberList" && g.MemberList != nil && g.MemberList.MessageTS == work.Target.MessageTS {
			sequence := current.Sequence
			oldPost := g.MemberList.PostID
			newPost := randomID(18)
			g.MemberList = &SlackObject{PostID: newPost, DesiredRevision: g.MemberList.DesiredRevision + 1, Phase: "pending"}
			delete(s.Outbox, id)
			for _, dependent := range s.Outbox {
				if dependent.DependsOnPostID == oldPost {
					dependent.DependsOnPostID = newPost
				}
			}
			replacementID := "member-list:" + newPost
			putOutbox(s, replacementID, &OutboxWork{GroupKey: g.GroupKey, Object: "memberList", Kind: "post", DesiredRevision: g.MemberList.DesiredRevision, Target: OutboxTarget{Channel: g.Channel, ThreadTS: g.Parent.MessageTS}, DependsOnPostID: dependencyForParent(g.Parent), Phase: "pending"})
			// The replacement is the same logical prerequisite. Preserve its place
			// ahead of replies that were already waiting on the deleted message.
			s.Outbox[replacementID].Sequence = sequence
			return nil
		}
		if work.Object != "parent" {
			current.Phase = "failed"
			current.LastError = "deleted Slack message cannot be replaced for object " + work.Object
			return nil
		}
		sequence := current.Sequence
		oldPost := g.Parent.PostID
		newPost := randomID(18)
		g.Parent = &SlackObject{PostID: newPost, DesiredRevision: g.Parent.DesiredRevision + 1, Phase: "pending"}
		delete(s.Outbox, id)
		for _, dependent := range s.Outbox {
			if dependent.Target.ThreadTS == work.Target.MessageTS {
				dependent.Target.ThreadTS = ""
				dependent.DependsOnPostID = newPost
			} else if dependent.DependsOnPostID == oldPost {
				dependent.DependsOnPostID = newPost
			}
		}
		for _, ref := range s.SilenceRefs {
			if ref.ThreadTS == work.Target.MessageTS && (ref.Channel == "" || ref.Channel == work.Target.Channel) {
				ref.ThreadTS = ""
				ref.ParentPostID = newPost
			}
		}
		replacementID := "parent:" + newPost
		putOutbox(s, replacementID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: "post", DesiredRevision: g.Parent.DesiredRevision, Target: OutboxTarget{Channel: g.Channel}, ProbeDelivery: isProbeGroup(g), Phase: "pending"})
		s.Outbox[replacementID].Sequence = sequence
		return nil
	})
}

func dependencyForParent(parent *SlackObject) string {
	if parent == nil || parent.MessageTS != "" {
		return ""
	}
	return parent.PostID
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortedStrings(in map[string]bool) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
