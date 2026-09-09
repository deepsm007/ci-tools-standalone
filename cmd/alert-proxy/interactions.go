package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
)

const forwardedReplayWindow = 5 * time.Minute

type InteractionHandler struct {
	store                  StateStore
	slack                  SlackAPI
	operations             *OperationWorker
	policy                 *SilencePolicy
	channel                string
	forwarderSecretPath    string
	slackSigningSecretPath string
	renderer               *Renderer
	metrics                *Metrics
	now                    func() time.Time
	wakeOutbox             func()
}

func (h *InteractionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := verifyForwarded(body, r.Header, h.forwarderSecretPath, h.now()); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := verifySlack(body, r.Header, h.slackSigningSecretPath, h.now()); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	raw := values.Get("payload")
	if raw == "" {
		http.Error(w, "missing payload", http.StatusBadRequest)
		return
	}
	var callback slack.InteractionCallback
	if err := json.Unmarshal([]byte(raw), &callback); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if callback.TriggerID == "" {
		http.Error(w, "missing interaction id", http.StatusBadRequest)
		return
	}
	if !h.interactionTargetsChannel(r.Context(), &callback) {
		http.Error(w, "forbidden channel", http.StatusForbidden)
		return
	}
	response, err := h.handle(r.Context(), &callback)
	if err != nil {
		logrus.WithFields(logrus.Fields{"component": "alert-proxy-interaction", "actor": callback.User.ID, "kind": string(callback.Type), "result": "error"}).WithError(err).Warn("processed Slack alert action")
		if h.metrics != nil {
			h.metrics.interactions.WithLabelValues(string(callback.Type), "error").Inc()
		}
		_ = h.slack.PostEphemeral(r.Context(), h.channel, callback.User.ID, "Alert action failed: "+err.Error())
		w.WriteHeader(http.StatusOK)
		return
	}
	logrus.WithFields(logrus.Fields{"component": "alert-proxy-interaction", "actor": callback.User.ID, "kind": string(callback.Type), "result": "accepted"}).Info("processed Slack alert action")
	if h.metrics != nil {
		h.metrics.interactions.WithLabelValues(string(callback.Type), "accepted").Inc()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if len(response) > 0 {
		_, _ = w.Write(response)
	}
}

func (h *InteractionHandler) interactionTargetsChannel(ctx context.Context, cb *slack.InteractionCallback) bool {
	channel := cb.Channel.ID
	if channel == "" {
		channel = cb.Container.ChannelID
	}
	if channel != "" {
		return channel == h.channel
	}
	if cb.Type != slack.InteractionTypeViewSubmission {
		return false
	}
	target, err := parseModalMetadata(cb.View.PrivateMetadata)
	if err != nil {
		return false
	}
	state, err := h.store.Read(ctx)
	if err != nil {
		return false
	}
	if strings.HasPrefix(target, "silence:") {
		ref := findSilenceByShortID(state, strings.TrimPrefix(target, "silence:"))
		return ref != nil && ref.Channel == h.channel
	}
	g, _ := findInteractionTarget(state, target)
	return g != nil && g.Channel == h.channel
}

func (h *InteractionHandler) handle(ctx context.Context, cb *slack.InteractionCallback) ([]byte, error) {
	switch cb.Type {
	case slack.InteractionTypeBlockActions:
		if len(cb.ActionCallback.BlockActions) != 1 {
			return nil, errors.New("exactly one block action is required")
		}
		action := cb.ActionCallback.BlockActions[0]
		if strings.HasPrefix(action.ActionID, "alert-proxy:ack:") {
			return nil, h.ack(ctx, cb.TriggerID, strings.TrimPrefix(action.ActionID, "alert-proxy:ack:"), cb.User.ID)
		}
		if strings.HasPrefix(action.ActionID, "alert-proxy:silence:") {
			preset := action.SelectedOption.Value
			if preset == "" {
				preset = action.Value
			}
			return nil, h.openSilenceModal(ctx, cb.TriggerID, strings.TrimPrefix(action.ActionID, "alert-proxy:silence:"), preset, cb.User.ID)
		}
		if action.ActionID == "alert-proxy:silence-alert" {
			target := strings.TrimPrefix(action.SelectedOption.Value, "alert-proxy:silence-alert:")
			return nil, h.openSilenceModal(ctx, cb.TriggerID, target, "", cb.User.ID)
		}
		if strings.HasPrefix(action.ActionID, "alert-proxy:extend:") {
			return nil, h.openSilenceModal(ctx, cb.TriggerID, "silence:"+strings.TrimPrefix(action.ActionID, "alert-proxy:extend:"), "", cb.User.ID)
		}
		if strings.HasPrefix(action.ActionID, "alert-proxy:unsilence:") {
			state, err := h.store.Read(ctx)
			if err != nil {
				return nil, err
			}
			ref := findSilenceByShortID(state, strings.TrimPrefix(action.ActionID, "alert-proxy:unsilence:"))
			if ref == nil {
				return nil, errors.New("proxy-owned silence no longer exists")
			}
			_, err = h.operations.PrepareExpire(ctx, cb.TriggerID, "interaction", ref.SilenceID, cb.User.ID, "requested from Slack interaction")
			return nil, err
		}
		return nil, errors.New("unknown alert-proxy action")
	case slack.InteractionTypeViewSubmission:
		return h.submitSilenceModal(ctx, cb)
	default:
		return nil, errors.New("unsupported interaction type")
	}
}

func (h *InteractionHandler) reserve(ctx context.Context, key, kind string, mutate func(*State) error) (bool, error) {
	duplicate := false
	now := h.now()
	err := h.store.Update(ctx, func(s *State) error {
		duplicate = false
		if old := s.Requests[key]; old != nil {
			duplicate = true
			return nil
		}
		if s.Mode == "draining" {
			return errors.New("proxy is draining")
		}
		if mutate != nil {
			if err := mutate(s); err != nil {
				return err
			}
		}
		s.Requests[key] = &RequestReservation{Kind: kind, Phase: "processed", ReservedAt: now, CompletedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
		return nil
	})
	return duplicate, err
}

func (h *InteractionHandler) ack(ctx context.Context, key, target, actor string) error {
	duplicate, err := h.reserve(ctx, key, "interaction", func(s *State) error {
		g := findGroupByShortID(s, target)
		if g == nil || !g.ClosedAt.IsZero() {
			return errors.New("alert episode is no longer open")
		}
		g.Ack = &AckState{Actor: actor, At: h.now().UTC()}
		g.Parent.DesiredRevision++
		kind := "post"
		target := OutboxTarget{Channel: g.Channel}
		if g.Parent.MessageTS != "" {
			kind = "update"
			target.MessageTS = g.Parent.MessageTS
		}
		putOutbox(s, "parent:"+g.Parent.PostID, &OutboxWork{GroupKey: g.GroupKey, Object: "parent", Kind: kind, DesiredRevision: g.Parent.DesiredRevision, Target: target, ProbeDelivery: isProbeGroup(g), Phase: "pending"})
		return nil
	})
	if err == nil && !duplicate && h.wakeOutbox != nil {
		h.wakeOutbox()
	}
	return err
}

func (h *InteractionHandler) openSilenceModal(ctx context.Context, key, target, preset, actor string) error {
	if err := h.operations.authz.Authorize(ctx, actor, "silence"); err != nil {
		return err
	}
	duplicate, err := h.reserve(ctx, key, "interaction", nil)
	if err != nil || duplicate {
		return err
	}
	state, err := h.store.Read(ctx)
	if err != nil {
		return err
	}
	var matchers []Matcher
	if strings.HasPrefix(target, "silence:") {
		ref := findSilenceByShortID(state, strings.TrimPrefix(target, "silence:"))
		if ref == nil || (ref.LastObservedState != "active" && ref.LastObservedState != "pending") {
			return errors.New("proxy-owned silence is no longer active or pending")
		}
		matchers = canonicalMatchers(ref.CanonicalMatchers)
	} else {
		g, alert := findInteractionTarget(state, target)
		if g == nil || !g.ClosedAt.IsZero() {
			return errors.New("alert episode is no longer open")
		}
		if alert != nil {
			matchers = h.policy.MatcherCandidates(*alert)
		} else {
			matchers, err = h.policy.CommonMatchers(g.LatestBatch)
		}
	}
	if err != nil || len(matchers) == 0 {
		return errors.New("this alert does not have a safe silence scope")
	}
	alerts, err := h.operations.api.ListAlerts(ctx)
	if err != nil {
		return fmt.Errorf("check current alerts: %w", err)
	}
	count := currentMatchCount(alerts, matchers, h.now())
	if count < 1 || count > 5 {
		return fmt.Errorf("silence currently matches %d alerts; allowed range is [1,5]", count)
	}
	if preset == "" {
		for _, p := range h.policy.Config.Presets {
			if p.Default {
				preset = p.Name
				break
			}
		}
	}
	if preset == "" {
		preset = h.policy.Config.Presets[0].Name
	}
	view := h.silenceModal(target, preset, matchers, count)
	if err := h.slack.OpenView(ctx, key, view); err != nil {
		return err
	}
	_ = actor
	return nil
}

func (h *InteractionHandler) silenceModal(target, preset string, matchers []Matcher, count int) any {
	options := []any{}
	var initial any
	for _, p := range h.policy.OfferedPresets(h.now()) {
		o := map[string]any{"text": map[string]any{"type": "plain_text", "text": p.Label}, "value": p.Name}
		options = append(options, o)
		if p.Name == preset {
			initial = o
		}
	}
	custom := map[string]any{"text": map[string]any{"type": "plain_text", "text": "Custom…"}, "value": "custom"}
	options = append(options, custom)
	if preset == "custom" {
		initial = custom
	}
	if initial == nil {
		initial = options[0]
	}
	scopeOptions := []any{}
	for _, matcher := range matchers {
		if matcher.Name == "alertname" {
			continue
		}
		scopeOptions = append(scopeOptions, map[string]any{"text": map[string]any{"type": "plain_text", "text": matcher.Name + "=" + matcher.Value}, "value": matcher.Name})
	}
	blocks := []any{
		map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": fmt.Sprintf("This exact scope currently matches *%d* firing alerts (including inhibited or already-silenced alerts):\n`%s`", count, canonicalMatcherString(matchers))}},
		map[string]any{"type": "input", "block_id": "duration", "label": map[string]any{"type": "plain_text", "text": "Duration"}, "element": map[string]any{"type": "static_select", "action_id": "value", "options": options, "initial_option": initial}},
		map[string]any{"type": "input", "block_id": "scope", "label": map[string]any{"type": "plain_text", "text": "Exact narrowing labels"}, "element": map[string]any{"type": "checkboxes", "action_id": "value", "options": scopeOptions, "initial_options": scopeOptions}},
		map[string]any{"type": "input", "block_id": "custom_date", "optional": true, "label": map[string]any{"type": "plain_text", "text": "Custom expiry date (UTC)"}, "element": map[string]any{"type": "datepicker", "action_id": "value", "initial_date": h.now().UTC().Add(24 * time.Hour).Format("2006-01-02")}},
		map[string]any{"type": "input", "block_id": "custom_time", "optional": true, "label": map[string]any{"type": "plain_text", "text": "Custom expiry time (UTC)"}, "element": map[string]any{"type": "timepicker", "action_id": "value", "initial_time": h.now().UTC().Format("15:04")}},
		map[string]any{"type": "input", "block_id": "reason", "label": map[string]any{"type": "plain_text", "text": "Reason"}, "element": map[string]any{"type": "plain_text_input", "action_id": "value", "multiline": true, "max_length": 500}},
	}
	return map[string]any{"type": "modal", "callback_id": "alert-proxy:silence-submit", "private_metadata": "alert-proxy:modal:" + target, "title": map[string]any{"type": "plain_text", "text": "Silence CI alert"}, "submit": map[string]any{"type": "plain_text", "text": "Silence"}, "close": map[string]any{"type": "plain_text", "text": "Cancel"}, "blocks": blocks}
}

func (h *InteractionHandler) submitSilenceModal(ctx context.Context, cb *slack.InteractionCallback) ([]byte, error) {
	target, err := parseModalMetadata(cb.View.PrivateMetadata)
	if err != nil {
		return modalErrors(map[string]string{"reason": err.Error()}), nil
	}
	state, err := h.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	var source string
	var origin *SilenceOrigin
	var baseMatchers []Matcher
	var replacesOwnershipID string
	if strings.HasPrefix(target, "silence:") {
		ref := findSilenceByShortID(state, strings.TrimPrefix(target, "silence:"))
		if ref == nil || (ref.LastObservedState != "active" && ref.LastObservedState != "pending") {
			return modalErrors(map[string]string{"reason": "This proxy-owned silence is no longer active or pending."}), nil
		}
		source, origin, baseMatchers = ref.Source, ref.Origin, canonicalMatchers(ref.CanonicalMatchers)
		replacesOwnershipID = ownershipID(ref.Source, ref.CanonicalMatchers)
	} else {
		g, alert := findInteractionTarget(state, target)
		if g == nil || !g.ClosedAt.IsZero() {
			return modalErrors(map[string]string{"reason": "This alert episode is no longer open."}), nil
		}
		source, origin = g.Source, &SilenceOrigin{GroupKey: g.GroupKey, EpisodeID: g.EpisodeID}
		if alert != nil {
			baseMatchers = h.policy.MatcherCandidates(*alert)
		} else {
			baseMatchers, err = h.policy.CommonMatchers(g.LatestBatch)
		}
	}
	if err != nil || len(baseMatchers) == 0 {
		return modalErrors(map[string]string{"reason": "This alert no longer has a safe matcher scope."}), nil
	}
	if cb.View.State == nil {
		return modalErrors(map[string]string{"reason": "Slack modal state is missing."}), nil
	}
	values := cb.View.State.Values
	reason := values["reason"]["value"].Value
	presetName := values["duration"]["value"].SelectedOption.Value
	if strings.TrimSpace(reason) == "" {
		return modalErrors(map[string]string{"reason": "A reason is required."}), nil
	}
	var ends time.Time
	if presetName == "custom" {
		date := values["custom_date"]["value"].SelectedDate
		clock := values["custom_time"]["value"].SelectedTime
		if date == "" || clock == "" {
			return modalErrors(map[string]string{"custom_date": "Choose a custom UTC date and time."}), nil
		}
		ends, err = time.ParseInLocation("2006-01-02 15:04", date+" "+clock, time.UTC)
		if err != nil {
			return modalErrors(map[string]string{"custom_date": "Invalid UTC date or time."}), nil
		}
		ends = h.policy.ClampExpiry(h.now(), ends)
	} else {
		ends, err = h.policy.Preset(presetName, h.now())
		if err != nil {
			return modalErrors(map[string]string{"duration": err.Error()}), nil
		}
	}
	selectedNames := map[string]bool{}
	for _, selected := range values["scope"]["value"].SelectedOptions {
		selectedNames[selected.Value] = true
	}
	matchers := []Matcher{}
	for _, matcher := range baseMatchers {
		if matcher.Name == "alertname" || selectedNames[matcher.Name] {
			matchers = append(matchers, matcher)
		}
	}
	if len(matchers) == 0 {
		return modalErrors(map[string]string{"reason": "This alert no longer has a safe matcher scope."}), nil
	}
	if replacesOwnershipID == ownershipID(source, matchers) {
		replacesOwnershipID = ""
	}
	_, err = h.operations.Prepare(ctx, PrepareSilenceRequest{RequestID: cb.TriggerID, RequestKind: "interaction", Source: source, Matchers: matchers, EndsAt: ends, Actor: cb.User.ID, Reason: reason, Preset: presetName, Origin: origin, ReplacesOwnershipID: replacesOwnershipID})
	if err != nil {
		return modalErrors(map[string]string{"reason": err.Error()}), nil
	}
	return nil, nil
}

func modalErrors(errors map[string]string) []byte {
	raw, _ := json.Marshal(map[string]any{"response_action": "errors", "errors": errors})
	return raw
}
func parseModalMetadata(value string) (string, error) {
	if !strings.HasPrefix(value, "alert-proxy:modal:") {
		return "", errors.New("invalid modal target")
	}
	target := strings.TrimPrefix(value, "alert-proxy:modal:")
	if target == "" {
		return "", errors.New("empty modal target")
	}
	return target, nil
}
func findGroupByShortID(s *State, id string) *GroupState {
	for _, g := range s.Groups {
		if g.ShortID == id {
			return g
		}
	}
	return nil
}
func findInteractionTarget(s *State, id string) (*GroupState, *AlertSnapshot) {
	for _, g := range s.Groups {
		if g.ShortID == id {
			return g, nil
		}
		for fp, short := range g.AlertShortIDs {
			if short == id {
				for i := range g.LatestBatch {
					if g.LatestBatch[i].Fingerprint == fp {
						return g, &g.LatestBatch[i]
					}
				}
			}
		}
	}
	return nil, nil
}

func findSilenceByShortID(s *State, id string) *SilenceRef {
	for _, ref := range s.SilenceRefs {
		if ref.ShortID == id {
			return ref
		}
	}
	return nil
}

type MentionEnvelope struct {
	EventID  string `json:"eventID"`
	IssuedAt int64  `json:"issuedAt"`
	Channel  string `json:"channel"`
	User     string `json:"user"`
	ThreadTS string `json:"threadTS"`
	Text     string `json:"text"`
}
type MentionHandler struct {
	store                        StateStore
	operations                   *OperationWorker
	policy                       *SilencePolicy
	channel, forwarderSecretPath string
	renderer                     *Renderer
	metrics                      *Metrics
	now                          func() time.Time
	wakeOutbox                   func()
}

func (h *MentionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := verifyForwarded(body, r.Header, h.forwarderSecretPath, h.now()); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var env MentionEnvelope
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		http.Error(w, "invalid envelope", http.StatusBadRequest)
		return
	}
	timestamp, _ := strconv.ParseInt(r.Header.Get("X-Alert-Proxy-Timestamp"), 10, 64)
	if env.IssuedAt != timestamp {
		http.Error(w, "issuedAt does not match signed timestamp", http.StatusUnauthorized)
		return
	}
	if env.EventID == "" || env.User == "" || env.Channel != h.channel {
		http.Error(w, "invalid mention envelope", http.StatusForbidden)
		return
	}
	commandKind := "unknown"
	if fields := strings.Fields(mentionCommand(env.Text)); len(fields) > 0 {
		commandKind = fields[0]
	}
	if err := h.handle(r.Context(), env); err != nil {
		logrus.WithFields(logrus.Fields{"component": "alert-proxy-mention", "actor": env.User, "kind": commandKind, "result": "error"}).WithError(err).Warn("processed ci-alerts command")
		_ = h.reply(r.Context(), env, "ci-alerts command failed: "+slackEscape(err.Error()), false)
	} else {
		logrus.WithFields(logrus.Fields{"component": "alert-proxy-mention", "actor": env.User, "kind": commandKind, "result": "accepted"}).Info("processed ci-alerts command")
	}
	w.WriteHeader(http.StatusOK)
}

func (h *MentionHandler) handle(ctx context.Context, env MentionEnvelope) error {
	current, err := h.store.Read(ctx)
	if err != nil {
		return err
	}
	if current.Mode == "draining" {
		return errors.New("alert-proxy is draining and refuses new Slack commands")
	}
	command := mentionCommand(env.Text)
	if command == "" {
		return errors.New("usage: ci-alerts list|silences|silence|unsilence")
	}
	fields := strings.Fields(command)
	switch fields[0] {
	case "list":
		state, err := h.store.Read(ctx)
		if err != nil {
			return err
		}
		lines := []string{"Tracked CI alert episodes:"}
		keys := make([]string, 0, len(state.Groups))
		for k, g := range state.Groups {
			if g.ClosedAt.IsZero() {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			g := state.Groups[k]
			ack := "unacknowledged"
			if g.Ack != nil {
				ack = "acknowledged by <@" + g.Ack.Actor + ">"
			}
			lines = append(lines, fmt.Sprintf("• %s — %d alerts in latest notification, %s", renderLabels(g.GroupLabels), len(g.LatestBatch), ack))
		}
		if len(keys) == 0 {
			lines = append(lines, "• none")
		}
		return h.reply(ctx, env, strings.Join(lines, "\n"), true)
	case "silences":
		state, err := h.store.Read(ctx)
		if err != nil {
			return err
		}
		lines := []string{"Proxy-owned Alertmanager silences:"}
		ids := make([]string, 0, len(state.SilenceRefs))
		for id := range state.SilenceRefs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			ref := state.SilenceRefs[id]
			lines = append(lines, fmt.Sprintf("• `%s` (`%s`) — %s until %s UTC, by <@%s>: %s", slackEscape(ref.SilenceID), slackEscape(canonicalMatcherString(ref.CanonicalMatchers)), slackEscape(ref.LastObservedState), ref.CanonicalEndsAt.UTC().Format("2006-01-02 15:04"), slackEscape(ref.Actor), slackEscape(ref.Reason)))
		}
		if len(ids) == 0 {
			lines = append(lines, "• none")
		}
		return h.reply(ctx, env, strings.Join(lines, "\n"), true)
	case "silence":
		if len(fields) < 4 {
			return errors.New("usage: ci-alerts silence <alertname>[,<label>=<value>] <duration> <reason>")
		}
		matchers, err := parseMentionMatchers(fields[1])
		if err != nil {
			return err
		}
		duration, err := time.ParseDuration(fields[2])
		if err != nil {
			return err
		}
		reason := strings.Join(fields[3:], " ")
		_, err = h.operations.Prepare(ctx, PrepareSilenceRequest{RequestID: env.EventID, RequestKind: "mention", Source: "app-ci-uwm", Matchers: matchers, EndsAt: h.now().Add(duration), Actor: env.User, Reason: reason, Preset: "mention", ReplyTarget: &OutboxTarget{Channel: env.Channel, ThreadTS: env.ThreadTS}})
		if err != nil {
			return err
		}
		return h.reply(ctx, env, "Alertmanager silence requested; the canonical result will be reported here.", false)
	case "unsilence":
		if len(fields) != 2 {
			return errors.New("usage: ci-alerts unsilence <id>")
		}
		_, err := h.operations.PrepareExpire(ctx, env.EventID, "mention", fields[1], env.User, "requested from ci-alerts mention")
		if err != nil {
			return err
		}
		return h.reply(ctx, env, "Alertmanager silence expiry requested.", false)
	default:
		return errors.New("unknown ci-alerts command")
	}
}

func (h *MentionHandler) reply(ctx context.Context, env MentionEnvelope, text string, reserve bool) error {
	now := h.now()
	payload := h.renderer.RenderEventReply(text)
	id := "mention-reply:" + env.EventID
	err := h.store.Update(ctx, func(s *State) error {
		if reserve {
			if s.Requests[env.EventID] != nil {
				return nil
			}
			s.Requests[env.EventID] = &RequestReservation{Kind: "mention", Phase: "processed", ReservedAt: now, CompletedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
		}
		if s.Outbox[id] == nil {
			putOutbox(s, id, &OutboxWork{Object: "eventReply", Kind: "post", Target: OutboxTarget{Channel: env.Channel, ThreadTS: env.ThreadTS}, ImmutablePayload: &payload, Phase: "pending"})
		}
		return nil
	})
	if err == nil && h.wakeOutbox != nil {
		h.wakeOutbox()
	}
	return err
}
func mentionCommand(text string) string {
	return strings.TrimSpace(text)
}
func parseMentionMatchers(spec string) ([]Matcher, error) {
	parts := strings.Split(spec, ",")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return nil, errors.New("alertname is required")
	}
	out := []Matcher{{Name: "alertname", Value: strings.TrimSpace(parts[0]), IsEqual: true}}
	nameRE := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	for _, part := range parts[1:] {
		if strings.ContainsAny(part, "!~") {
			return nil, errors.New("only exact equality matchers are allowed")
		}
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 || !nameRE.MatchString(pair[0]) || pair[1] == "" {
			return nil, fmt.Errorf("invalid exact matcher %q", part)
		}
		out = append(out, Matcher{Name: pair[0], Value: pair[1], IsEqual: true})
	}
	return out, nil
}

func verifyForwarded(body []byte, headers http.Header, secretPath string, now time.Time) error {
	timestamp := headers.Get("X-Alert-Proxy-Timestamp")
	signature := headers.Get("X-Alert-Proxy-Signature")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return err
	}
	issued := time.Unix(seconds, 0)
	if now.Sub(issued) > forwardedReplayWindow || issued.Sub(now) > forwardedReplayWindow {
		return errors.New("stale forwarded request")
	}
	secret, err := readSecret(secretPath)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("v1:" + timestamp + ":"))
	_, _ = mac.Write(body)
	expected := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return errors.New("invalid forwarded signature")
	}
	return nil
}
func verifySlack(body []byte, headers http.Header, secretPath string, now time.Time) error {
	timestamp := headers.Get("X-Slack-Request-Timestamp")
	signature := headers.Get("X-Slack-Signature")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return err
	}
	issued := time.Unix(seconds, 0)
	if now.Sub(issued) > forwardedReplayWindow || issued.Sub(now) > forwardedReplayWindow {
		return errors.New("stale Slack request")
	}
	secret, err := readSecret(secretPath)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return errors.New("invalid Slack signature")
	}
	return nil
}
