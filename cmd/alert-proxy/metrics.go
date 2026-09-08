package main

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	notificationsReceived   *prometheus.CounterVec
	webhookRequests         *prometheus.CounterVec
	slackMessages           *prometheus.CounterVec
	notificationsSuppressed *prometheus.CounterVec
	activeGroups            *prometheus.GaugeVec
	activeGroupMembers      *prometheus.GaugeVec
	silences                *prometheus.GaugeVec
	silenceActions          *prometheus.CounterVec
	interactions            *prometheus.CounterVec
	alertmanagerErrors      *prometheus.CounterVec
	alertmanagerReady       prometheus.Gauge
	silenceOperations       *prometheus.GaugeVec
	silenceRecoveries       *prometheus.CounterVec
	outboxDepth             *prometheus.GaugeVec
	outboxAttempts          *prometheus.CounterVec
	ambiguousPosts          *prometheus.CounterVec
	authzDecisions          *prometheus.CounterVec
	groupMembersUnresolved  prometheus.Gauge
	statePersistErrors      prometheus.Counter
	stateBytes              prometheus.Gauge
	duplicateDeliveries     prometheus.Counter
	authzCacheAge           prometheus.Gauge
	endToEndDeliveries      prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		notificationsReceived:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_notifications_received_total", Help: "Accepted Alertmanager notifications."}, []string{"source", "status"}),
		webhookRequests:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_webhook_requests_total", Help: "Alertmanager webhook HTTP requests by bounded response class (2xx, 4xx, or 5xx)."}, []string{"status"}),
		slackMessages:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_slack_messages_total", Help: "Successful Slack messages."}, []string{"action"}),
		notificationsSuppressed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_notifications_suppressed_total", Help: "Notifications collapsed by the proxy."}, []string{"reason"}),
		activeGroups:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "alert_proxy_active_groups", Help: "Open Slack alert episodes."}, []string{"severity"}),
		activeGroupMembers:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "alert_proxy_active_group_members", Help: "Firing alerts in latest delivered batches."}, []string{"severity"}),
		silences:                prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "alert_proxy_silences", Help: "Last canonical state of proxy-owned silences."}, []string{"state"}),
		silenceActions:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_silence_actions_total", Help: "Silence mutation requests."}, []string{"action", "preset", "result"}),
		interactions:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_interactions_total", Help: "Forwarded Slack actions."}, []string{"kind", "result"}),
		alertmanagerErrors:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_alertmanager_errors_total", Help: "Alertmanager API errors."}, []string{"operation", "source"}),
		alertmanagerReady:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "alert_proxy_alertmanager_ready", Help: "Whether the Alertmanager readiness dependency is healthy."}),
		silenceOperations:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "alert_proxy_silence_operations", Help: "Finite silence operations by phase."}, []string{"phase"}),
		silenceRecoveries:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_silence_recoveries_total", Help: "Recovered silence operations."}, []string{"result"}),
		outboxDepth:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "alert_proxy_slack_outbox_depth", Help: "Durable Slack work by phase."}, []string{"phase"}),
		outboxAttempts:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_slack_outbox_attempts_total", Help: "Slack outbox delivery attempts."}, []string{"kind", "result"}),
		ambiguousPosts:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_slack_ambiguous_posts_total", Help: "Ambiguous first Slack posts."}, []string{"kind"}),
		authzDecisions:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "alert_proxy_authz_decisions_total", Help: "Privileged Slack authorization decisions."}, []string{"action", "result"}),
		groupMembersUnresolved:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "alert_proxy_group_members_unresolved", Help: "OpenShift group members without a Slack identity."}),
		statePersistErrors:      prometheus.NewCounter(prometheus.CounterOpts{Name: "alert_proxy_state_persist_errors_total", Help: "Failed durable state writes."}),
		stateBytes:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "alert_proxy_state_bytes", Help: "Serialized ConfigMap state size."}),
		duplicateDeliveries:     prometheus.NewCounter(prometheus.CounterOpts{Name: "alert_proxy_duplicate_deliveries_total", Help: "Persistently deduplicated webhook deliveries."}),
		authzCacheAge:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "alert_proxy_authz_cache_age_seconds", Help: "Age of the current privileged-group cache."}),
		endToEndDeliveries:      prometheus.NewCounter(prometheus.CounterOpts{Name: "alert_proxy_end_to_end_deliveries_total", Help: "Successful Slack parent deliveries for end-to-end probe notifications."}),
	}
	reg.MustRegister(m.notificationsReceived, m.webhookRequests, m.slackMessages, m.notificationsSuppressed, m.activeGroups, m.activeGroupMembers, m.silences, m.silenceActions, m.interactions, m.alertmanagerErrors, m.alertmanagerReady, m.silenceOperations, m.silenceRecoveries, m.outboxDepth, m.outboxAttempts, m.ambiguousPosts, m.authzDecisions, m.groupMembersUnresolved, m.statePersistErrors, m.stateBytes, m.duplicateDeliveries, m.authzCacheAge, m.endToEndDeliveries)
	return m
}

func (m *Metrics) refreshState(s *State) {
	m.activeGroups.Reset()
	m.activeGroupMembers.Reset()
	m.silences.Reset()
	m.silenceOperations.Reset()
	m.outboxDepth.Reset()
	for _, g := range s.Groups {
		if !g.ClosedAt.IsZero() {
			continue
		}
		severity := commonLabel(g.LatestBatch, "severity")
		if severity == "" {
			severity = "unknown"
		}
		m.activeGroups.WithLabelValues(severity).Inc()
		for _, a := range g.LatestBatch {
			if a.Status == "firing" {
				m.activeGroupMembers.WithLabelValues(severity).Inc()
			}
		}
	}
	for _, ref := range s.SilenceRefs {
		if ref.LastObservedState == "pending" || ref.LastObservedState == "active" {
			m.silences.WithLabelValues(ref.LastObservedState).Inc()
		}
	}
	for _, op := range s.SilenceOperations {
		if op.Phase != "completed" && op.Phase != "failed" {
			m.silenceOperations.WithLabelValues(op.Phase).Inc()
		}
	}
	for _, work := range s.Outbox {
		m.outboxDepth.WithLabelValues(work.Phase).Inc()
	}
}
