package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// upstreamStub supplies isolated HTTP transport responses without opening a listener.
type upstreamStub struct {
	status   int
	body     string
	err      error
	location string
	requests []*http.Request
}

// RoundTrip captures only test credentials and supplies a controlled upstream response.
func (stub *upstreamStub) RoundTrip(request *http.Request) (*http.Response, error) {
	stub.requests = append(stub.requests, request.Clone(request.Context()))
	if stub.err != nil {
		return nil, stub.err
	}
	header := http.Header{}
	if stub.location != "" {
		header.Set("Location", stub.location)
	}
	return &http.Response{StatusCode: stub.status, Header: header, Body: io.NopCloser(strings.NewReader(stub.body)), Request: request}, nil
}

// testDashboard creates a validated local fixture without network access.
func testDashboard(t *testing.T, stub *upstreamStub) *dashboard {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-observation-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "service-ca.crt")
	ca, err := os.ReadFile("testdata/service-ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, ca, 0600); err != nil {
		t.Fatal(err)
	}
	app, err := newDashboard(options{dispatcherURL: "https://dispatcher.example", tokenFile: tokenFile, caFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	app.client.Transport = stub
	return app
}

// TestObservationRoutes verifies exact allowlisting and preservation of upstream API errors.
func TestObservationRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, method, path  string
		upstreamStatus      int
		body                string
		wantStatus          int
		wantPath, wantQuery string
	}{
		{"state", "GET", "/api/v1/dispatcher/state?token=browser-secret", 200, `{"ready":true}`, 200, "/observe/v1/state", ""},
		{"jobs allowed filters", "GET", "/api/v1/dispatcher/jobs?name=foo&group=a%2Fb&cluster=build01&limit=2&after=bar&generation=8&url=http://evil&token=browser-secret", 200, `{"jobs":[]}`, 200, "/observe/v1/jobs", "after=bar&cluster=build01&generation=8&group=a%2Fb&limit=2&name=foo"},
		{"overrides", "GET", "/api/v1/dispatcher/overrides?limit=2", 503, `{"error":"unavailable"}`, 503, "/observe/v1/overrides", ""},
		{"bad filter", "GET", "/api/v1/dispatcher/jobs?limit=0", 400, `{"error":"limit must be between 1 and 500"}`, 400, "/observe/v1/jobs", "limit=0"},
		{"generation changed", "GET", "/api/v1/dispatcher/jobs?generation=1", 409, `{"error":"serving policy generation changed; restart pagination"}`, 409, "/observe/v1/jobs", "generation=1"},
		{"unauthorized", "GET", "/api/v1/dispatcher/state", 401, `{"error":"unauthorized"}`, 401, "/observe/v1/state", ""},
		{"mutation", "POST", "/api/v1/dispatcher/state", 200, `{}`, 405, "", ""},
		{"control route", "GET", "/api/v1/dispatcher/controls", 200, `{}`, 404, "", ""},
		{"arbitrary path", "GET", "/observe/v1/state", 200, `{}`, 404, "", ""},
		{"path extension", "GET", "/api/v1/dispatcher/state/anything", 200, `{}`, 404, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &upstreamStub{status: tc.upstreamStatus, body: tc.body}
			app := testDashboard(t, stub)
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tc.method, tc.path, nil)
			request.Header.Set("Authorization", "Bearer browser-secret")
			app.ServeHTTP(response, request)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.wantStatus, response.Body.String())
			}
			if tc.wantPath == "" {
				if len(stub.requests) != 0 {
					t.Fatalf("unexpected upstream requests: %d", len(stub.requests))
				}
				return
			}
			if len(stub.requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(stub.requests))
			}
			got := stub.requests[0]
			if diff := cmp.Diff([]string{tc.wantPath, tc.wantQuery, "Bearer test-observation-secret", "GET"}, []string{got.URL.Path, got.URL.RawQuery, got.Header.Get("Authorization"), got.Method}); diff != "" {
				t.Errorf("unexpected request (-want +got):\n%s", diff)
			}
			if response.Body.String() != tc.body {
				t.Errorf("body = %q, want %q", response.Body.String(), tc.body)
			}
		})
	}
}

// TestUpstreamFailures ensures redirects, invalid data and transport errors fail closed.
func TestUpstreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stub     upstreamStub
		wantBody string
	}{
		{"network error", upstreamStub{err: errors.New("sensitive upstream URL and credentials")}, "dispatcher request failed"},
		{"redirect", upstreamStub{status: 302, location: "http://untrusted.example", body: `{}`}, "dispatcher redirect refused"},
		{"invalid JSON", upstreamStub{status: 200, body: "upstream HTML"}, "dispatcher returned invalid JSON"},
		{"oversized body", upstreamStub{status: 200, body: strings.Repeat("x", maxResponseBytes+1)}, "dispatcher response could not be read within the size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := testDashboard(t, &tc.stub)
			response := httptest.NewRecorder()
			app.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/dispatcher/state", nil))
			if response.Code != 502 || !strings.Contains(response.Body.String(), tc.wantBody) {
				t.Errorf("response = %d %s", response.Code, response.Body.String())
			}
			if len(tc.stub.requests) != 1 {
				t.Errorf("requests = %d, want 1 (no redirects)", len(tc.stub.requests))
			}
			if strings.Contains(response.Body.String(), "sensitive") || strings.Contains(response.Body.String(), "untrusted.example") {
				t.Error("upstream details leaked")
			}
		})
	}
}

// TestTokenRotation verifies server-side rotation and browser-facing credential redaction.
func TestTokenRotation(t *testing.T) {
	for _, tc := range []struct {
		name, replacement string
		wantStatus        int
	}{
		{"rotated", "rotated-observation-secret", 200},
		{"empty", "\n", 502},
		{"multiple lines", "one\ntwo", 502},
		{"JSON escaping", "quoted\"token", 502},
		{"control character", "token\x00", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &upstreamStub{status: 200, body: `{"error":"test-observation-secret"}`}
			app := testDashboard(t, stub)
			first := httptest.NewRecorder()
			app.ServeHTTP(first, httptest.NewRequest("GET", "/api/v1/dispatcher/state", nil))
			if strings.Contains(first.Body.String(), "test-observation-secret") {
				t.Fatal("token leaked")
			}
			if err := os.WriteFile(app.opts.tokenFile, []byte(tc.replacement), 0600); err != nil {
				t.Fatal(err)
			}
			stub.body = `{}`
			second := httptest.NewRecorder()
			app.ServeHTTP(second, httptest.NewRequest("GET", "/api/v1/dispatcher/state", nil))
			if second.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", second.Code, tc.wantStatus)
			}
			if tc.wantStatus == 200 && stub.requests[1].Header.Get("Authorization") != "Bearer "+tc.replacement {
				t.Error("new token was not used")
			}
			if tc.wantStatus != 200 && len(stub.requests) != 1 {
				t.Error("invalid rotated token reached upstream")
			}
		})
	}
}

// TestConfiguration rejects ambiguous demo and live destinations before binding.
func TestConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		opts      options
		wantError string
	}{
		{"demo", options{demo: true}, ""},
		{"missing origin", options{}, "--dispatcher-url must be"},
		{"missing token", options{dispatcherURL: "https://dispatcher.example"}, "--dispatcher-token-file is required"},
		{"URL credentials", options{dispatcherURL: "https://user:secret@dispatcher.example"}, "--dispatcher-url must be"},
		{"URL path", options{dispatcherURL: "https://dispatcher.example/control"}, "--dispatcher-url must be"},
		{"URL query", options{dispatcherURL: "https://dispatcher.example?token=secret"}, "--dispatcher-url must be"},
		{"empty URL query", options{dispatcherURL: "https://dispatcher.example?"}, "--dispatcher-url must be"},
		{"HTTP origin", options{dispatcherURL: "http://dispatcher.example"}, "--dispatcher-url must be"},
		{"missing CA", options{dispatcherURL: "https://dispatcher.example", tokenFile: "not-read"}, "--dispatcher-ca-file is required"},
		{"missing CA file", options{dispatcherURL: "https://dispatcher.example", tokenFile: "not-read", caFile: "missing-file"}, "dispatcher CA file is unavailable"},
		{"non-http URL", options{dispatcherURL: "file:///tmp/data"}, "--dispatcher-url must be"},
		{"demo with live CA", options{demo: true, caFile: "testdata/service-ca.pem"}, "--demo cannot be combined"},
		{"demo with live token", options{demo: true, tokenFile: "not-read"}, "--demo cannot be combined"},
		{"demo with live URL", options{demo: true, dispatcherURL: "https://dispatcher.example"}, "--demo cannot be combined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newDashboard(tc.opts)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Errorf("error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

// TestLocalRoutes checks liveness, local readiness and the browser's non-secret config.
func TestLocalRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, path, method string
		removeToken        bool
		wantStatus         int
	}{
		{"liveness", "/healthz", "GET", true, 200},
		{"ready", "/readyz", "GET", false, 200},
		{"token missing", "/readyz", "GET", true, 503},
		{"config", "/api/v1/config", "GET", false, 200},
		{"method", "/api/v1/config", "DELETE", false, 405},
		{"static root", "/", "GET", false, 200},
		{"static script", "/assets/app.js", "GET", false, 200},
		{"favicon", "/favicon.ico", "GET", false, 204},
		{"static traversal", "/assets/../server.go", "GET", false, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &upstreamStub{}
			app := testDashboard(t, stub)
			if tc.removeToken {
				if err := os.Remove(app.opts.tokenFile); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			app.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", response.Code, tc.wantStatus)
			}
			if len(stub.requests) != 0 {
				t.Error("local route contacted upstream")
			}
			if response.Header().Get("Content-Security-Policy") == "" {
				t.Error("missing CSP")
			}
			if strings.Contains(response.Body.String(), "test-observation-secret") || strings.Contains(response.Body.String(), "dispatcher.example") {
				t.Error("configuration leaked")
			}
		})
	}
}

// TestServiceCAReadiness validates local trust mounts without contacting the dispatcher.
func TestServiceCAReadiness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remove bool
	}{
		{name: "malformed"},
		{name: "missing", remove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &upstreamStub{}
			app := testDashboard(t, stub)
			if err := os.WriteFile(app.opts.caFile, []byte("invalid service trust"), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.remove {
				if err := os.Remove(app.opts.caFile); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			app.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
			if response.Code != 503 || !strings.Contains(response.Body.String(), "dispatcher CA file is unavailable or invalid") {
				t.Errorf("response = %d %s", response.Code, response.Body.String())
			}
			if len(stub.requests) != 0 {
				t.Error("readiness contacted upstream")
			}
			_, err := newDashboard(app.opts)
			if err == nil || !strings.Contains(err.Error(), "dispatcher CA file") {
				t.Errorf("startup error = %v", err)
			}
		})
	}
}
