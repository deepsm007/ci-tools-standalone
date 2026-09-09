package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"
)

func secretFile(t *testing.T, name, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func signHeader(secret, version, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(version + ":" + timestamp + ":"))
	_, _ = mac.Write(body)
	return version + "=" + hex.EncodeToString(mac.Sum(nil))
}
func signedRequest(path string, body []byte, now time.Time, forwardSecret, slackSecret string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	ts := strconv.FormatInt(now.Unix(), 10)
	r.Header.Set("X-Alert-Proxy-Timestamp", ts)
	r.Header.Set("X-Alert-Proxy-Signature", signHeader(forwardSecret, "v1", ts, body))
	if slackSecret != "" {
		r.Header.Set("X-Slack-Request-Timestamp", ts)
		r.Header.Set("X-Slack-Signature", signHeader(slackSecret, "v0", ts, body))
	}
	return r
}

func TestMentionEnvelopeExactHMACIssuedAtAndChannel(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	forwardPath := secretFile(t, "forward", "forward-secret")
	store := NewMemoryStateStore()
	handler := &MentionHandler{store: store, channel: "C1", forwarderSecretPath: forwardPath, renderer: testRenderer(), now: func() time.Time { return now }}
	env := MentionEnvelope{EventID: "Ev1", IssuedAt: now.Unix(), Channel: "C1", User: "U1", ThreadTS: "12.3", Text: "list"}
	body, _ := json.Marshal(env)
	req := signedRequest("/mentions", body, now, "forward-secret", "")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	state, _ := store.Read(req.Context())
	if state.Requests["Ev1"] == nil || state.Outbox["mention-reply:Ev1"] == nil {
		t.Fatalf("mention was not durably reserved with reply: %#v", state)
	}
	env.EventID = "Ev2"
	env.IssuedAt--
	body, _ = json.Marshal(env)
	req = signedRequest("/mentions", body, now, "forward-secret", "")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("issuedAt mismatch status=%d", rr.Code)
	}
	env.EventID = "Ev3"
	env.IssuedAt = now.Unix()
	env.Channel = "OTHER"
	body, _ = json.Marshal(env)
	req = signedRequest("/mentions", body, now, "forward-secret", "")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("wrong channel status=%d", rr.Code)
	}
}

func TestMentionCommandUsesForwardedRemainderVerbatim(t *testing.T) {
	if got := mentionCommand("  list  "); got != "list" {
		t.Fatalf("command remainder=%q", got)
	}
	if got := mentionCommand("please ci-alerts unsilence id"); got != "please ci-alerts unsilence id" {
		t.Fatalf("embedded prefix was reinterpreted: %q", got)
	}
}

func TestInteractionRequiresBothHMACsAndUpdatesAckTransactionally(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	forwardPath := secretFile(t, "forward", "forward-secret")
	slackPath := secretFile(t, "slack", "slack-secret")
	store := NewMemoryStateStore()
	g := openTestGroup()
	_ = store.Update(t.Context(), func(s *State) error { s.Groups["g"] = g; return nil })
	handler := &InteractionHandler{store: store, slack: &fakeSlack{}, channel: "C1", forwarderSecretPath: forwardPath, slackSigningSecretPath: slackPath, renderer: testRenderer(), now: func() time.Time { return now }}
	callback := slack.InteractionCallback{Type: slack.InteractionTypeBlockActions, TriggerID: "trigger-1", Channel: slack.Channel{GroupConversation: slack.GroupConversation{Conversation: slack.Conversation{ID: "C1"}}}, User: slack.User{ID: "U1"}, ActionCallback: slack.ActionCallbacks{BlockActions: []*slack.BlockAction{{ActionID: "alert-proxy:ack:short", Value: "short"}}}}
	raw, _ := json.Marshal(callback)
	form := url.Values{"payload": []string{string(raw)}}.Encode()
	body := []byte(form)
	req := signedRequest("/interactions", body, now, "forward-secret", "slack-secret")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d %s", rr.Code, rr.Body.String())
	}
	state, _ := store.Read(req.Context())
	if state.Groups["g"].Ack == nil || state.Requests["trigger-1"] == nil || state.Outbox["parent:post"] == nil {
		t.Fatalf("ack was not one durable transaction: %#v", state)
	}
	callback.TriggerID = "trigger-2"
	callback.Channel.ID = "OTHER"
	raw, _ = json.Marshal(callback)
	body = []byte(url.Values{"payload": []string{string(raw)}}.Encode())
	req = signedRequest("/interactions", body, now, "forward-secret", "slack-secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("wrong channel status=%d", rr.Code)
	}
	req = signedRequest("/interactions", body, now, "wrong-forward", "slack-secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong forwarder HMAC status=%d", rr.Code)
	}
}

func TestSilenceSubmissionRejectsGroupWithoutSafeCommonMatchers(t *testing.T) {
	store := NewMemoryStateStore()
	group := openTestGroup()
	group.LatestBatch = append(group.LatestBatch, AlertSnapshot{
		Fingerprint: "other",
		Status:      "firing",
		Labels:      map[string]string{"alertname": "Different", "namespace": "ci"},
	})
	if err := store.Update(t.Context(), func(s *State) error {
		s.Groups[group.GroupKey] = group
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handler := &InteractionHandler{store: store, policy: testPolicy()}
	callback := &slack.InteractionCallback{View: slack.View{PrivateMetadata: "alert-proxy:modal:" + group.ShortID}}
	response, err := handler.submitSilenceModal(t.Context(), callback)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response), "safe matcher scope") {
		t.Fatalf("unexpected modal response: %s", response)
	}
}

func TestEndpointCredentialsAreNotInterchangeable(t *testing.T) {
	now := time.Unix(3_000_000, 0)
	webhookPath := secretFile(t, "webhook", "webhook-token")
	forwardPath := secretFile(t, "forward", "forward-secret")
	processor := &WebhookProcessor{}
	webhook := WebhookHandler(processor, webhookPath, func() bool { return true })
	req := httptest.NewRequest(http.MethodPost, "/webhook/alertmanager?source=app-ci-uwm", bytes.NewReader(nil))
	req.Header.Set("Authorization", "Bearer forward-secret")
	rr := httptest.NewRecorder()
	webhook.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("forwarder secret accepted by webhook: %d", rr.Code)
	}
	body := []byte("payload={}")
	interaction := &InteractionHandler{forwarderSecretPath: forwardPath, slackSigningSecretPath: secretFile(t, "slack", "slack-secret"), now: func() time.Time { return now }}
	req = signedRequest("/interactions", body, now, "webhook-token", "slack-secret")
	rr = httptest.NewRecorder()
	interaction.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("webhook credential accepted as forwarder HMAC: %d", rr.Code)
	}
}

func TestForwardedReplayWindow(t *testing.T) {
	now := time.Unix(4_000_000, 0)
	path := secretFile(t, "forward", "secret")
	body := []byte("body")
	req := signedRequest("/mentions", body, now.Add(-forwardedReplayWindow-time.Second), "secret", "")
	if err := verifyForwarded(body, req.Header, path, now); err == nil {
		t.Fatal("stale captured request was accepted")
	}
}
