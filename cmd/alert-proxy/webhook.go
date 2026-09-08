package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type alertmanagerWebhook struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []struct {
		Status       string            `json:"status"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     time.Time         `json:"startsAt"`
		EndsAt       time.Time         `json:"endsAt"`
		GeneratorURL string            `json:"generatorURL"`
		Fingerprint  string            `json:"fingerprint"`
	} `json:"alerts"`
}

const alertmanagerSource = "app-ci-uwm"

type WebhookProcessor struct {
	store       StateStore
	channel     string
	deliveryTTL time.Duration
	requestTTL  time.Duration
	auditTTL    time.Duration
	now         func() time.Time
	renderer    *Renderer
	metrics     *Metrics
	wake        func()
}

type webhookPayloadError struct{ err error }

func (e *webhookPayloadError) Error() string { return e.err.Error() }
func (e *webhookPayloadError) Unwrap() error { return e.err }

func (p *WebhookProcessor) Process(ctx context.Context, source string, body []byte) (bool, error) {
	var in alertmanagerWebhook
	if err := json.Unmarshal(body, &in); err != nil {
		return false, &webhookPayloadError{err: fmt.Errorf("decode Alertmanager webhook: %w", err)}
	}
	if err := validateWebhook(source, &in); err != nil {
		return false, &webhookPayloadError{err: err}
	}
	batch := make([]AlertSnapshot, 0, len(in.Alerts))
	for _, a := range in.Alerts {
		batch = append(batch, AlertSnapshot{Fingerprint: a.Fingerprint, Status: a.Status, Labels: cloneMap(a.Labels), Annotations: cloneMap(a.Annotations), StartsAt: a.StartsAt.UTC(), EndsAt: a.EndsAt.UTC(), GeneratorURL: a.GeneratorURL})
	}
	sort.Slice(batch, func(i, j int) bool { return batch[i].Fingerprint < batch[j].Fingerprint })
	deliveryID, err := deliveryHash(source, in.GroupKey, in.GroupLabels, batch)
	if err != nil {
		return false, err
	}
	now := p.now().UTC()
	duplicate := false
	repeat := false
	ignoredResolved := false
	err = p.store.Update(ctx, func(s *State) error {
		duplicate, repeat, ignoredResolved = false, false, false
		if s.Mode == "draining" {
			return errors.New("proxy is draining")
		}
		pruneState(s, now, p.deliveryTTL, p.requestTTL, p.auditTTL)
		old := s.Groups[in.GroupKey]
		repeat = old != nil && old.ClosedAt.IsZero()
		if old != nil {
			if record, ok := old.RecentDeliveries[deliveryID]; ok && now.Sub(record.ProcessedAt) <= p.deliveryTTL {
				duplicate = true
				return nil
			}
		}
		if (old == nil || !old.ClosedAt.IsZero()) && in.Status == "resolved" {
			ignoredResolved = true
			if old != nil {
				old.RecentDeliveries[deliveryID] = DeliveryRecord{ProcessedAt: now}
			}
			return nil
		}
		g := old
		if g == nil || !g.ClosedAt.IsZero() {
			var previous *EpisodeSummary
			recent := map[string]DeliveryRecord{}
			if old != nil {
				for k, v := range old.RecentDeliveries {
					recent[k] = v
				}
				previous = &EpisodeSummary{EpisodeID: old.EpisodeID, StartedAt: old.EpisodeStartedAt, ClosedAt: old.ClosedAt, ClosedReason: old.ClosedReason, NotificationCount: old.NotificationCount, Ack: old.Ack}
			}
			g = &GroupState{Source: source, Receiver: in.Receiver, GroupKey: in.GroupKey, GroupLabels: cloneMap(in.GroupLabels), ExternalURL: in.ExternalURL, Channel: p.channel, ShortID: randomID(10), AlertShortIDs: map[string]string{}, KnownMembers: map[string]KnownMember{}, EpisodeID: randomID(16), EpisodeStartedAt: now, FirstSeen: now, Status: in.Status, PreviousEpisode: previous, RecentDeliveries: recent}
			g.Parent = &SlackObject{PostID: randomID(18), DesiredRevision: 1, Phase: "pending"}
		}
		previousSet := stringSet(g.PreviousDeliveredFingerprints)
		newMembers := []AlertSnapshot{}
		for _, a := range batch {
			if g.AlertShortIDs == nil {
				g.AlertShortIDs = map[string]string{}
			}
			if g.AlertShortIDs[a.Fingerprint] == "" {
				g.AlertShortIDs[a.Fingerprint] = randomID(8)
			}
			if in.Status == "firing" && a.Status == "firing" && !previousSet[a.Fingerprint] && g.NotificationCount > 0 {
				newMembers = append(newMembers, a)
			}
			g.KnownMembers[a.Fingerprint] = KnownMember{LastExplicitStatus: a.Status, LastExplicitAt: now}
		}
		if len(newMembers) > 0 {
			g.Ack = nil
		}
		g.Source, g.Receiver, g.ExternalURL = source, in.Receiver, in.ExternalURL
		g.GroupLabels, g.LatestBatch = cloneMap(in.GroupLabels), batch
		g.PreviousDeliveredFingerprints = fingerprints(batch)
		g.LastSeen = now
		g.NotificationCount++
		g.Status = in.Status
		g.RecentDeliveries[deliveryID] = DeliveryRecord{ProcessedAt: now}
		if g.Parent == nil {
			return errors.New("open episode has no parent Slack object")
		}
		g.Parent.DesiredRevision++
		parentWorkID := "parent:" + g.Parent.PostID
		kind := "post"
		target := OutboxTarget{Channel: g.Channel}
		if g.Parent.MessageTS != "" {
			kind = "update"
			target.MessageTS = g.Parent.MessageTS
		}
		putOutbox(s, parentWorkID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: kind, DesiredRevision: g.Parent.DesiredRevision, Target: target, ProbeDelivery: isProbeGroup(g), Phase: "pending"})

		if len(batch) > maxRenderedMembers {
			if g.MemberList == nil {
				g.MemberList = &SlackObject{PostID: randomID(18), DesiredRevision: 1, Phase: "pending"}
			} else {
				g.MemberList.DesiredRevision++
			}
			kind := "post"
			target := OutboxTarget{Channel: g.Channel, ThreadTS: g.Parent.MessageTS}
			depends := ""
			if g.Parent.MessageTS == "" {
				depends = g.Parent.PostID
			}
			if g.MemberList.MessageTS != "" {
				kind = "update"
				target.MessageTS = g.MemberList.MessageTS
			}
			putOutbox(s, "member-list:"+g.MemberList.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "memberList", Kind: kind, DesiredRevision: g.MemberList.DesiredRevision, Target: target, DependsOnPostID: depends, Phase: "pending"})
		} else if g.MemberList != nil {
			retireMemberList(s, g, p.renderer)
		}
		for _, a := range newMembers {
			payload := p.renderer.RenderEventReply(fmt.Sprintf("New alert in this notification group: %s", slackEscape(alertDisplayName(a))))
			id := "new-member:" + g.EpisodeID + ":" + a.Fingerprint
			target := OutboxTarget{Channel: g.Channel, ThreadTS: g.Parent.MessageTS}
			depends := ""
			if g.Parent.MessageTS == "" {
				depends = g.Parent.PostID
			}
			putOutbox(s, id, &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: target, DependsOnPostID: depends, ImmutablePayload: &payload, Phase: "pending"})
		}

		if in.Status == "resolved" {
			knownFiring := 0
			for _, member := range g.KnownMembers {
				if member.LastExplicitStatus == "firing" {
					knownFiring++
				}
			}
			if knownFiring == 0 {
				g.Status = "resolved"
				closeEpisode(s, g, now, "resolved", p.renderer.RenderParent(g, false), p.renderer.RenderEventReply("This notification group has explicitly resolved in Alertmanager."))
			} else {
				g.Status = "incomplete"
			}
		}
		s.Groups[in.GroupKey] = g
		return nil
	})
	if err != nil {
		return false, err
	}
	if duplicate {
		if p.metrics != nil {
			p.metrics.duplicateDeliveries.Inc()
		}
		return true, nil
	}
	if p.metrics != nil {
		p.metrics.notificationsReceived.WithLabelValues(source, in.Status).Inc()
		if repeat {
			p.metrics.notificationsSuppressed.WithLabelValues("repeat").Inc()
		}
	}
	if p.wake != nil && !ignoredResolved {
		p.wake()
	}
	return false, nil
}

func retireMemberList(s *State, g *GroupState, renderer *Renderer) {
	member := g.MemberList
	if member == nil {
		return
	}
	id := "member-list:" + member.PostID
	work := s.Outbox[id]
	if member.MessageTS == "" && (work == nil || work.Phase != "inFlight") {
		// A post that has definitely not started can be cancelled without ever
		// creating the now-obsolete overflow reply.
		delete(s.Outbox, id)
		g.MemberList = nil
		return
	}
	member.DesiredRevision++
	payload := renderer.RenderEventReply(fmt.Sprintf("Overflow list retired: the latest notification has %d alerts, all shown in the parent message.", len(g.LatestBatch)))
	target := OutboxTarget{Channel: g.Channel, MessageTS: member.MessageTS}
	depends := ""
	kind := "update"
	if member.MessageTS == "" {
		// The original post is already in flight. Its completion will supply the
		// timestamp and turn this higher desired revision into an update.
		kind = "post"
		target = OutboxTarget{Channel: g.Channel, ThreadTS: g.Parent.MessageTS}
		depends = dependencyForParent(g.Parent)
	}
	putOutbox(s, id, &OutboxWork{GroupKey: g.GroupKey, Object: "memberListRetire", Kind: kind, DesiredRevision: member.DesiredRevision, Target: target, DependsOnPostID: depends, ImmutablePayload: &payload, Phase: "pending"})
}

func closeEpisode(s *State, g *GroupState, now time.Time, reason string, parentPayload, replyPayload SlackPayload) {
	g.ClosedAt, g.ClosedReason = now, reason
	if g.Parent != nil {
		postID := g.Parent.PostID
		if g.Parent.MessageTS != "" {
			delete(s.Outbox, "parent:"+postID)
			putOutbox(s, "close-parent:"+g.EpisodeID, &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "update", Target: OutboxTarget{Channel: g.Channel, MessageTS: g.Parent.MessageTS}, ImmutablePayload: &parentPayload, ProbeDelivery: isProbeGroup(g), Phase: "pending"})
			putOutbox(s, "close-reply:"+g.EpisodeID, &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: g.Channel, ThreadTS: g.Parent.MessageTS}, ImmutablePayload: &replyPayload, Phase: "pending"})
		} else {
			if w := s.Outbox["parent:"+postID]; w != nil {
				w.ImmutablePayload = &parentPayload
				w.DesiredRevision++
			}
			putOutbox(s, "close-reply:"+g.EpisodeID, &OutboxWork{GroupKey: g.GroupKey, Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: g.Channel}, DependsOnPostID: postID, ImmutablePayload: &replyPayload, Phase: "pending"})
		}
		g.Parent = nil
	}
	if g.MemberList != nil {
		if work := s.Outbox["member-list:"+g.MemberList.PostID]; work == nil || work.Phase != "inFlight" {
			delete(s.Outbox, "member-list:"+g.MemberList.PostID)
		}
		g.MemberList = nil
	}
	g.KnownMembers = map[string]KnownMember{}
	if reason == "silenced" {
		g.RecentDeliveries = map[string]DeliveryRecord{}
	}
}

func validateWebhook(source string, in *alertmanagerWebhook) error {
	if source != alertmanagerSource {
		return fmt.Errorf("source query parameter must be %q", alertmanagerSource)
	}
	if in.GroupKey == "" || in.Receiver == "" {
		return errors.New("groupKey and receiver are required")
	}
	if in.Status != "firing" && in.Status != "resolved" {
		return fmt.Errorf("invalid top-level status %q", in.Status)
	}
	if len(in.Alerts) == 0 {
		return errors.New("alerts must not be empty")
	}
	firing := 0
	seen := map[string]bool{}
	for i, a := range in.Alerts {
		if a.Status != "firing" && a.Status != "resolved" {
			return fmt.Errorf("alerts[%d] has invalid status %q", i, a.Status)
		}
		if a.Status == "firing" {
			firing++
		}
		if a.Fingerprint == "" {
			return fmt.Errorf("alerts[%d].fingerprint is required", i)
		}
		if seen[a.Fingerprint] {
			return fmt.Errorf("duplicate fingerprint %q", a.Fingerprint)
		}
		seen[a.Fingerprint] = true
		if a.StartsAt.IsZero() {
			return fmt.Errorf("alerts[%d].startsAt is required", i)
		}
	}
	want := "resolved"
	if firing > 0 {
		want = "firing"
	}
	if in.Status != want {
		return fmt.Errorf("top-level status %q does not match delivered batch status %q", in.Status, want)
	}
	return nil
}

func deliveryHash(source, groupKey string, groupLabels map[string]string, batch []AlertSnapshot) (string, error) {
	canonical := struct {
		Source, GroupKey string
		GroupLabels      map[string]string
		Alerts           []AlertSnapshot
	}{source, groupKey, groupLabels, batch}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func WebhookHandler(processor *WebhookProcessor, tokenPath string, ready func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeWebhookResponse(w, processor.metrics, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !ready() {
			writeWebhookResponse(w, processor.metrics, http.StatusServiceUnavailable, "not ready")
			return
		}
		token, err := readSecret(tokenPath)
		if err != nil {
			writeWebhookResponse(w, processor.metrics, http.StatusServiceUnavailable, "webhook credential unavailable")
			return
		}
		authorization := r.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(authorization, "Bearer ")), token) != 1 {
			writeWebhookResponse(w, processor.metrics, http.StatusUnauthorized, "unauthorized")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if err != nil {
			writeWebhookResponse(w, processor.metrics, http.StatusBadRequest, "invalid body")
			return
		}
		_, err = processor.Process(r.Context(), r.URL.Query().Get("source"), body)
		if err != nil {
			// Alertmanager never retries 4xx. Persistence and readiness failures are therefore 5xx.
			var payloadErr *webhookPayloadError
			if errors.As(err, &payloadErr) {
				writeWebhookResponse(w, processor.metrics, http.StatusBadRequest, err.Error())
			} else {
				writeWebhookResponse(w, processor.metrics, http.StatusServiceUnavailable, "temporarily unavailable")
			}
			return
		}
		writeWebhookResponse(w, processor.metrics, http.StatusOK, "")
	}
}

func writeWebhookResponse(w http.ResponseWriter, metrics *Metrics, status int, message string) {
	if metrics != nil {
		class := "other"
		if status >= 100 && status <= 599 {
			class = fmt.Sprintf("%dxx", status/100)
		}
		metrics.webhookRequests.WithLabelValues(class).Inc()
	}
	if message != "" {
		http.Error(w, message, status)
		return
	}
	w.WriteHeader(status)
}

func randomID(bytes int) string {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func fingerprints(batch []AlertSnapshot) []string {
	out := make([]string, len(batch))
	for i := range batch {
		out[i] = batch[i].Fingerprint
	}
	sort.Strings(out)
	return out
}
func stringSet(in []string) map[string]bool {
	out := map[string]bool{}
	for _, v := range in {
		out[v] = true
	}
	return out
}
