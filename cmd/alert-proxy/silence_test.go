package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSilenceMatcherSafety(t *testing.T) {
	p := testPolicy()
	tests := []struct {
		name     string
		matchers []Matcher
		wantErr  bool
	}{
		{"valid job", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "job_name", Value: "job", IsEqual: true}}, false},
		{"valid namespace", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}, false},
		{"regex", []Matcher{{Name: "alertname", Value: ".*", IsRegex: true, IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}, true},
		{"negative", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: false}, {Name: "namespace", Value: "ci", IsEqual: true}}, true},
		{"severity false narrowing", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "severity", Value: "critical", IsEqual: true}}, true},
		{"empty", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "namespace", Value: "", IsEqual: true}}, true},
		{"duplicate", []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}, true},
		{"watchdog", []Matcher{{Name: "alertname", Value: "alert-proxy-Singleton-Down", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := p.ValidateMatchers(tc.matchers)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestCanonicalMatchersNormalizeSetAndHumanReadableDisplay(t *testing.T) {
	matchers := []Matcher{
		{Name: "namespace", Value: "ci", IsEqual: true},
		{Name: "alertname", Value: "Broken", IsEqual: true},
		{Name: "namespace", Value: "ci", IsEqual: true},
	}
	canonical := canonicalMatchers(matchers)
	if len(canonical) != 2 {
		t.Fatalf("canonical matcher set retained duplicates: %#v", canonical)
	}
	if got := canonicalMatcherString(matchers); got != "alertname=Broken, namespace=ci" {
		t.Fatalf("unexpected matcher display %q", got)
	}
	if ownershipID("app-ci-uwm", matchers) != ownershipID("app-ci-uwm", canonical) {
		t.Fatal("equivalent matcher sets produced different ownership IDs")
	}
}

func TestCurrentMatchCountIncludesSuppressedAndBounds(t *testing.T) {
	matchers := []Matcher{{Name: "alertname", Value: "Broken", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}
	for _, count := range []int{0, 1, 5, 6} {
		alerts := []AMAlert{}
		for i := 0; i < count; i++ {
			alerts = append(alerts, activeAlert(map[string]string{"alertname": "Broken", "namespace": "ci"}))
		}
		got := currentMatchCount(alerts, matchers, time.Now())
		if got != count {
			t.Fatalf("count %d: got %d", count, got)
		}
	}
}

func TestLoadSilencePolicyAndWeekendPreset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "silence.yaml")
	content := `silence:
  default: 24h
  minDuration: 2h
  maxDuration: 168h
  presets:
  - {name: two-hours, label: "2 hours", group: short, duration: 2h}
  - {name: one-day, label: "1 day", group: multi-day, duration: 24h, default: true}
  - {name: weekend, label: "Until Monday 08:00 UTC", group: weekend, untilWeekday: monday, at: "08:00", offeredFrom: thursday}
safety:
  neverSilenceable:
  - alertname: "~^alert-proxy-.*$"
  silence:
    requiredExactLabels: [alertname]
    requireOneExactLabelFrom: [job_name, namespace]
    requireNonEmptyExactValues: true
    allowedUserOperators: ["="]
    minCurrentlyMatchedAlerts: 1
    maxCurrentlyMatchedAlerts: 5
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadSilencePolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateMatchers([]Matcher{{Name: "alertname", Value: "alert-proxy-Webhook4xx", IsEqual: true}, {Name: "namespace", Value: "ci", IsEqual: true}}); err == nil {
		t.Fatal("loaded policy allowed silencing a non-Down alert-proxy self-alert")
	}
	thursday := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	end, err := p.Preset("weekend", thursday)
	if err != nil {
		t.Fatal(err)
	}
	if end.Weekday() != time.Monday || end.Hour() != 8 || end.Sub(thursday) > p.Config.MaxDuration {
		t.Fatalf("unexpected weekend end %s", end)
	}
	if _, err := p.Preset("weekend", time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("weekend offered on Wednesday: %v", err)
	}
}

func TestMentionMatcherParserRejectsUserOperators(t *testing.T) {
	for _, spec := range []string{"Alert,namespace!=ci", "Alert,namespace=~ci", "Alert,namespace="} {
		if _, err := parseMentionMatchers(spec); err == nil {
			t.Fatalf("accepted %q", spec)
		}
	}
	m, err := parseMentionMatchers("Alert,namespace=ci")
	if err != nil || len(m) != 2 {
		t.Fatalf("valid matchers: %#v %v", m, err)
	}
}

func TestHTTPAlertmanagerAPIContract(t *testing.T) {
	var sawAlerts, sawUpsert, sawDelete bool
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("unauthorized")), Header: make(http.Header)}, nil
		}
		status, body := http.StatusOK, ""
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/alerts":
			sawAlerts = true
			if r.URL.RawQuery != "active=true&silenced=true&inhibited=true&unprocessed=true" {
				t.Errorf("alerts query=%q", r.URL.RawQuery)
			}
			body = `[]`
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/silences":
			sawUpsert = true
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("content-type=%q", r.Header.Get("Content-Type"))
			}
			var silence AMSilence
			if err := json.NewDecoder(r.Body).Decode(&silence); err != nil {
				t.Error(err)
			}
			if silence.ID != "existing" || silence.CreatedBy != silenceCreatedBy || silence.Comment != "marker reason" || len(silence.Matchers) != 2 {
				t.Errorf("upsert body=%#v", silence)
			}
			body = `{"silenceID":"canonical"}`
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v2/silence/id with space":
			sawDelete = true
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			status = http.StatusNotFound
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: r}, nil
	})
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	api := &HTTPAlertmanager{BaseURL: "https://alertmanager.example", TokenPath: tokenPath, Timeout: time.Second, Client: &http.Client{Transport: transport}}
	if _, err := api.ListAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	silence := AMSilence{ID: "existing", Matchers: validMatchers(), StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour), CreatedBy: silenceCreatedBy, Comment: "marker reason"}
	if id, err := api.UpsertSilence(context.Background(), silence); err != nil || id != "canonical" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	if err := api.ExpireSilence(context.Background(), "id with space"); err != nil {
		t.Fatal(err)
	}
	if !sawAlerts || !sawUpsert || !sawDelete {
		t.Fatalf("calls alerts=%v upsert=%v delete=%v", sawAlerts, sawUpsert, sawDelete)
	}
}

func TestHTTPAlertmanagerReusesClientAndReplacesItWhenCARotates(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, testCAPEM(t, 1), 0600); err != nil {
		t.Fatal(err)
	}
	api := &HTTPAlertmanager{CAFile: caPath, Timeout: time.Second}
	first, err := api.client()
	if err != nil {
		t.Fatal(err)
	}
	again, err := api.client()
	if err != nil {
		t.Fatal(err)
	}
	if again != first || again.Transport != first.Transport {
		t.Fatal("unchanged CA did not reuse the Alertmanager client and transport")
	}
	if err := os.WriteFile(caPath, testCAPEM(t, 2), 0600); err != nil {
		t.Fatal(err)
	}
	replaced, err := api.client()
	if err != nil {
		t.Fatal(err)
	}
	if replaced == first || replaced.Transport == first.Transport {
		t.Fatal("rotated CA did not replace the Alertmanager client and transport")
	}
	reusedReplacement, err := api.client()
	if err != nil {
		t.Fatal(err)
	}
	if reusedReplacement != replaced {
		t.Fatal("replacement client was not cached")
	}
	if err := os.WriteFile(caPath, testCAPEM(t, 3), 0600); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	clients := make(chan *http.Client, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := api.client()
			clients <- client
			errs <- err
		}()
	}
	wg.Wait()
	close(clients)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var concurrentReplacement *http.Client
	for client := range clients {
		if concurrentReplacement == nil {
			concurrentReplacement = client
		}
		if client != concurrentReplacement {
			t.Fatal("concurrent CA replacement constructed more than one client")
		}
	}
	if concurrentReplacement == replaced {
		t.Fatal("concurrent calls did not install the rotated CA client")
	}
}

func TestHTTPAlertmanagerReloadsTokenWithInjectedClient(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var authorizations []string
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`[]`)), Header: make(http.Header), Request: r}, nil
	})}
	api := &HTTPAlertmanager{BaseURL: "https://alertmanager.example", CAFile: filepath.Join(dir, "missing-ca"), TokenPath: tokenPath, Client: client}
	if _, err := api.ListAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ListAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(authorizations) != 2 || authorizations[0] != "Bearer first" || authorizations[1] != "Bearer second" {
		t.Fatalf("authorization headers=%v", authorizations)
	}
}

func TestHTTPAlertmanagerTLSLifecycleAcrossCARotation(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v2/silences" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	})
	firstConnections := newTLSConnectionTracker()
	secondConnections := newTLSConnectionTracker()
	firstServer, firstCA := startAlertmanagerTLSServer(t, 101, firstConnections, handler)
	secondServer, secondCA := startAlertmanagerTLSServer(t, 202, secondConnections, handler)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(caPath, firstCA, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	api := &HTTPAlertmanager{BaseURL: firstServer.URL, CAFile: caPath, TokenPath: tokenPath, Timeout: time.Second}
	if _, err := api.ListSilences(context.Background()); err != nil {
		t.Fatalf("first CA was not trusted: %v", err)
	}
	firstIdle := firstConnections.waitFor(t, http.StateIdle)
	if _, err := api.ListSilences(context.Background()); err != nil {
		t.Fatalf("cached client did not retain first CA trust: %v", err)
	}
	if reused := firstConnections.waitFor(t, http.StateIdle); reused != firstIdle {
		t.Fatal("cached client did not reuse its idle TLS connection")
	}

	if err := os.WriteFile(caPath, secondCA, 0600); err != nil {
		t.Fatal(err)
	}
	api.BaseURL = secondServer.URL
	if _, err := api.ListSilences(context.Background()); err != nil {
		t.Fatalf("rotated CA was not trusted: %v", err)
	}
	_ = secondConnections.waitFor(t, http.StateIdle)
	if closed := firstConnections.waitFor(t, http.StateClosed); closed != firstIdle {
		t.Fatal("CA replacement did not close the old transport's idle connection")
	}
	api.BaseURL = firstServer.URL
	if _, err := api.ListSilences(context.Background()); err == nil {
		t.Fatal("rotated client continued to trust the old TLS server")
	}
}

func TestHTTPAlertmanagerCARotationWaitsForSelectedClientRequest(t *testing.T) {
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseOld)
		}
	}()
	oldHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(oldStarted)
		<-releaseOld
		_, _ = io.WriteString(w, `[]`)
	})
	newStarted := make(chan struct{})
	newHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(newStarted)
		_, _ = io.WriteString(w, `[]`)
	})
	oldServer, oldCA := startAlertmanagerTLSServer(t, 303, nil, oldHandler)
	newServer, newCA := startAlertmanagerTLSServer(t, 404, nil, newHandler)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(caPath, oldCA, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	api := &HTTPAlertmanager{BaseURL: oldServer.URL, CAFile: caPath, TokenPath: tokenPath, Timeout: time.Second}
	oldDone := make(chan error, 1)
	go func() {
		_, err := api.ListSilences(context.Background())
		oldDone <- err
	}()
	<-oldStarted
	if api.clientMu.TryLock() {
		api.clientMu.Unlock()
		t.Fatal("request released the client mutex before Client.Do returned headers")
	}
	if err := os.WriteFile(caPath, newCA, 0600); err != nil {
		t.Fatal(err)
	}
	api.BaseURL = newServer.URL
	newDone := make(chan error, 1)
	go func() {
		_, err := api.ListSilences(context.Background())
		newDone <- err
	}()
	select {
	case <-newStarted:
		t.Fatal("CA rotation began a new RoundTrip while the old selected client was still in use")
	case err := <-newDone:
		t.Fatalf("CA rotation request returned before the old Client.Do window ended: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseOld)
	released = true
	if err := <-oldDone; err != nil {
		t.Fatalf("old request failed: %v", err)
	}
	select {
	case <-newStarted:
	case <-time.After(time.Second):
		t.Fatal("rotated request did not begin after the old Client.Do window ended")
	}
	if err := <-newDone; err != nil {
		t.Fatalf("rotated request failed: %v", err)
	}
}

func TestHTTPAlertmanagerRotatedCAFailureDoesNotUseCachedTrust(t *testing.T) {
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, `[]`)
	})
	server, ca := startAlertmanagerTLSServer(t, 505, nil, handler)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	api := &HTTPAlertmanager{BaseURL: server.URL, CAFile: caPath, TokenPath: tokenPath, Timeout: time.Second}
	if _, err := api.ListSilences(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ListSilences(context.Background()); err == nil {
		t.Fatal("malformed rotated CA reused the previously cached trust")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("malformed CA reached the old trusted server %d times, want 1", got)
	}
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ListSilences(context.Background()); err != nil {
		t.Fatalf("restored CA did not restore service: %v", err)
	}
	if err := os.Remove(caPath); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ListSilences(context.Background()); err == nil {
		t.Fatal("missing rotated CA reused the previously cached trust")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("missing CA reached the old trusted server %d times, want 2", got)
	}
}

func startAlertmanagerTLSServer(t *testing.T, serial int64, tracker *tlsConnectionTracker, handler http.Handler) (*httptest.Server, []byte) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	if tracker != nil {
		server.Config.ConnState = tracker.observe
	}
	certificate, ca := testTLSServerCertificate(t, serial)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, ca
}

type tlsConnectionTracker struct {
	states map[http.ConnState]chan net.Conn
}

func newTLSConnectionTracker() *tlsConnectionTracker {
	return &tlsConnectionTracker{states: map[http.ConnState]chan net.Conn{
		http.StateIdle:   make(chan net.Conn, 8),
		http.StateClosed: make(chan net.Conn, 8),
	}}
}

func (t *tlsConnectionTracker) observe(connection net.Conn, state http.ConnState) {
	if states := t.states[state]; states != nil {
		states <- connection
	}
}

func (t *tlsConnectionTracker) waitFor(testingT *testing.T, state http.ConnState) net.Conn {
	testingT.Helper()
	select {
	case connection := <-t.states[state]:
		return connection
	case <-time.After(time.Second):
		testingT.Fatalf("timed out waiting for TLS connection state %s", state)
		return nil
	}
}

func testTLSServerCertificate(t *testing.T, serial int64) (tls.Certificate, []byte) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "alertmanager-test-server"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func testCAPEM(t *testing.T, serial int64) []byte {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "alertmanager-test-ca"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(4102444800, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
