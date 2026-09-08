package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	maxRenderedMembers  = 10
	maxMemberListBlocks = 50
)

type Renderer struct {
	SilencesEnabled bool
	Policy          *SilencePolicy
	Now             func() time.Time
}

func (r *Renderer) RenderParent(g *GroupState, controls bool) SlackPayload {
	status := strings.ToUpper(g.Status)
	switch g.ClosedReason {
	case "silenced":
		status = "SILENCED IN ALERTMANAGER"
	case "stale":
		status = "NO LONGER TRACKED"
	case "resolved":
		status = "RESOLVED"
	}
	header := fmt.Sprintf("*[%s] %s*", status, renderLabels(g.GroupLabels))
	meta := fmt.Sprintf("%d alerts in this notification · firing for %s · notification %d · last seen %s UTC", len(g.LatestBatch), humanDuration(g.LastSeen.Sub(g.EpisodeStartedAt)), g.NotificationCount, g.LastSeen.UTC().Format("2006-01-02 15:04"))
	if isProbeGroup(g) {
		probeText := fmt.Sprintf("Alert proxy end-to-end probe · status %s · delivery %d · %s UTC", strings.ToLower(status), g.NotificationCount, g.LastSeen.UTC().Format("2006-01-02 15:04"))
		return payload(probeText, []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": probeText}}})
	}
	if g.Ack != nil {
		meta += fmt.Sprintf("\nAcknowledged by <@%s> at %s UTC", slackEscape(g.Ack.Actor), g.Ack.At.UTC().Format("2006-01-02 15:04"))
	}
	if g.Status == "incomplete" {
		missing := 0
		for fp, k := range g.KnownMembers {
			if k.LastExplicitStatus == "firing" && !containsFingerprint(g.LatestBatch, fp) {
				missing++
			}
		}
		meta += fmt.Sprintf("\n:warning: Alertmanager sent a resolved notification with no unsuppressed firing alert, but %d previously firing alerts were omitted; this episode remains open.", missing)
	}
	if g.ScheduledSilence != nil {
		meta += fmt.Sprintf("\n:clock3: Alertmanager silence scheduled by <@%s> for `%s`, starting %s UTC and ending %s UTC.", slackEscape(g.ScheduledSilence.Actor), slackEscape(canonicalMatcherString(g.ScheduledSilence.Matchers)), g.ScheduledSilence.StartsAt.UTC().Format("2006-01-02 15:04"), g.ScheduledSilence.EndsAt.UTC().Format("2006-01-02 15:04"))
	}
	blocks := []any{
		map[string]any{"type": "header", "text": map[string]any{"type": "plain_text", "text": plain(header, 150), "emoji": true}},
		map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": mrkdwn(meta)}},
	}
	limit := len(g.LatestBatch)
	if limit > maxRenderedMembers {
		limit = maxRenderedMembers
	}
	for i := 0; i < limit; i++ {
		a := g.LatestBatch[i]
		section := map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": mrkdwn(renderAlert(a))}}
		if controls && r.SilencesEnabled && r.Policy != nil && r.Policy.MatcherCandidates(a) != nil {
			if short := g.AlertShortIDs[a.Fingerprint]; short != "" {
				section["accessory"] = map[string]any{"type": "overflow", "action_id": "alert-proxy:silence-alert", "options": []any{map[string]any{"text": map[string]any{"type": "plain_text", "text": "Silence this alert…"}, "value": "alert-proxy:silence-alert:" + short}}}
			}
		}
		blocks = append(blocks, section)
	}
	if len(g.LatestBatch) > maxRenderedMembers {
		blocks = append(blocks, map[string]any{
			"type": "context",
			"elements": []any{
				map[string]any{"type": "mrkdwn", "text": fmt.Sprintf("…and %d more — see the maintained thread reply.", len(g.LatestBatch)-maxRenderedMembers)},
			},
		})
	}
	if controls && g.ClosedAt.IsZero() {
		elements := []any{map[string]any{"type": "button", "action_id": "alert-proxy:ack:" + g.ShortID, "text": map[string]any{"type": "plain_text", "text": "Ack"}, "value": g.ShortID}}
		if r.SilencesEnabled && r.Policy != nil {
			if matchers, err := r.Policy.CommonMatchers(g.LatestBatch); err == nil && len(matchers) > 0 {
				groups := map[string][]any{}
				order := []string{}
				for _, p := range r.Policy.OfferedPresets(r.now()) {
					if _, ok := groups[p.Group]; !ok {
						order = append(order, p.Group)
					}
					groups[p.Group] = append(groups[p.Group], map[string]any{"text": map[string]any{"type": "plain_text", "text": p.Label}, "value": p.Name})
				}
				optionGroups := []any{}
				for _, name := range order {
					optionGroups = append(optionGroups, map[string]any{"label": map[string]any{"type": "plain_text", "text": presetGroupLabel(name)}, "options": groups[name]})
				}
				optionGroups = append(optionGroups, map[string]any{"label": map[string]any{"type": "plain_text", "text": "Custom"}, "options": []any{map[string]any{"text": map[string]any{"type": "plain_text", "text": "Custom…"}, "value": "custom"}}})
				elements = append(elements, map[string]any{"type": "static_select", "action_id": "alert-proxy:silence:" + g.ShortID, "placeholder": map[string]any{"type": "plain_text", "text": "Silence this notification group…"}, "option_groups": optionGroups})
			}
		}
		blocks = append(blocks, map[string]any{"type": "actions", "block_id": "alert-proxy:controls:" + g.ShortID, "elements": elements})
	}
	return payload(header+"\n"+meta, blocks)
}

func (r *Renderer) RenderMemberList(g *GroupState) SlackPayload {
	lines := []string{fmt.Sprintf("*All %d alerts in the latest notification*", len(g.LatestBatch))}
	for _, a := range g.LatestBatch {
		lines = append(lines, "• "+renderAlert(a))
	}
	chunks := []string{}
	current := ""
	for _, line := range lines {
		if len(current)+len(line)+1 > 2900 && current != "" {
			chunks = append(chunks, current)
			current = ""
		}
		if current != "" {
			current += "\n"
		}
		current += line
	}
	if current != "" {
		chunks = append(chunks, current)
	}
	if len(chunks) > maxMemberListBlocks {
		chunks = append(chunks[:maxMemberListBlocks-1], ":warning: Additional alert details were omitted because this notification exceeds Slack's 50-block message limit. Use Alertmanager for the complete delivered batch.")
	}
	blocks := make([]any, 0, len(chunks))
	for _, chunk := range chunks {
		blocks = append(blocks, map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": mrkdwn(chunk)}})
	}
	return payload(strings.Join(lines, "\n"), blocks)
}

func (r *Renderer) RenderEventReply(text string) SlackPayload {
	return payload(text, []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": mrkdwn(text)}}})
}

func (r *Renderer) RenderSilenceAudit(text, shortID string, allowExtend bool) SlackPayload {
	elements := []any{}
	if allowExtend {
		elements = append(elements, map[string]any{"type": "button", "action_id": "alert-proxy:extend:" + shortID, "text": map[string]any{"type": "plain_text", "text": "Extend…"}, "value": shortID})
	}
	elements = append(elements, map[string]any{"type": "button", "style": "danger", "action_id": "alert-proxy:unsilence:" + shortID, "text": map[string]any{"type": "plain_text", "text": "Unsilence"}, "value": shortID})
	blocks := []any{map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": mrkdwn(text)}}, map[string]any{"type": "actions", "block_id": "alert-proxy:silence-controls:" + shortID, "elements": elements}}
	return payload(text, blocks)
}

func (r *Renderer) RenderSilencedParent(g *GroupState, actor string, matchers []Matcher, endsAt time.Time) SlackPayload {
	copy := *g
	copy.Status = "silenced"
	copy.ClosedReason = "silenced"
	p := r.RenderParent(&copy, false)
	note := fmt.Sprintf("\nSilenced in Alertmanager by <@%s> for `%s` until %s UTC.", slackEscape(actor), slackEscape(canonicalMatcherString(matchers)), endsAt.UTC().Format("2006-01-02 15:04"))
	p.Text += note
	var blocks []any
	_ = json.Unmarshal(p.Blocks, &blocks)
	blocks = append(blocks, map[string]any{
		"type": "context",
		"elements": []any{
			map[string]any{"type": "mrkdwn", "text": note},
		},
	})
	p.Blocks, _ = json.Marshal(blocks)
	return p
}

func payload(text string, blocks []any) SlackPayload {
	raw, _ := json.Marshal(blocks)
	return SlackPayload{Text: plain(text, 4000), Blocks: raw}
}

func renderAlert(a AlertSnapshot) string {
	name := alertDisplayName(a)
	icon := ":red_circle:"
	if a.Status == "resolved" {
		icon = ":white_check_mark:"
	}
	detail := firstNonEmpty(a.Annotations["message"], a.Annotations["summary"], a.Annotations["description"])
	line := fmt.Sprintf("%s *%s* — `%s`", icon, slackEscape(name), a.Status)
	if detail != "" {
		line += "\n" + slackEscape(detail)
	}
	if a.GeneratorURL != "" {
		line += fmt.Sprintf(" · <%s|source>", slackURL(a.GeneratorURL))
	}
	return line
}

func alertDisplayName(a AlertSnapshot) string {
	name := a.Labels["alertname"]
	if name == "" {
		name = "alert " + a.Fingerprint
	}
	qualifiers := []string{}
	for _, k := range []string{"job_name", "namespace", "job"} {
		if a.Labels[k] != "" {
			qualifiers = append(qualifiers, k+"="+a.Labels[k])
		}
	}
	if len(qualifiers) > 0 {
		name += " (" + strings.Join(qualifiers, ", ") + ")"
	}
	return name
}

func renderLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, slackEscape(labels[k]))
	}
	if len(parts) == 0 {
		return "Alertmanager notification"
	}
	return strings.Join(parts, " ")
}

func commonLabel(batch []AlertSnapshot, key string) string {
	if len(batch) == 0 {
		return ""
	}
	value := batch[0].Labels[key]
	if value == "" {
		return ""
	}
	for _, a := range batch[1:] {
		if a.Labels[key] != value {
			return ""
		}
	}
	return value
}
func containsFingerprint(batch []AlertSnapshot, fp string) bool {
	for _, a := range batch {
		if a.Fingerprint == fp {
			return true
		}
	}
	return false
}
func isProbeGroup(g *GroupState) bool {
	if g == nil {
		return false
	}
	if g.GroupLabels["alert_proxy_probe"] == "true" {
		return true
	}
	for _, alert := range g.LatestBatch {
		if alert.Labels["alert_proxy_probe"] == "true" {
			return true
		}
	}
	return false
}
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
func presetGroupLabel(group string) string {
	if group == "" {
		return "Durations"
	}
	label := strings.ReplaceAll(group, "-", " ")
	return strings.ToUpper(label[:1]) + label[1:]
}
func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
}
func slackEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	return strings.ReplaceAll(s, ">", "&gt;")
}
func slackURL(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "|", "%7C"), ">", "%3E")
}
func plain(s string, max int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "*", ""))
	runes := []rune(s)
	if len(runes) > max {
		return string(runes[:max-1]) + "…"
	}
	return s
}
func mrkdwn(s string) string {
	runes := []rune(s)
	if len(runes) > 3000 {
		return string(runes[:2999]) + "…"
	}
	return s
}
func (r *Renderer) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
