package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// assets keeps the dashboard self-contained in its binary.
//
//go:embed web/index.html web/styles.css web/app.js
var assets embed.FS

// maxResponseBytes bounds both observation JSON and optional metrics responses.
const maxResponseBytes = 8 << 20

// dashboard serves a fixed read-only surface backed by a single dispatcher.
type dashboard struct {
	opts        options
	client      *http.Client
	sample      *demoData
	diagnostics *logCollector
	trust       *dispatcherTransport
}

// newDashboard validates live credentials before starting a listener.
func newDashboard(opts options) (*dashboard, error) {
	if err := validateLogOptions(opts.logs, opts.demo); err != nil {
		return nil, err
	}
	if opts.demo && (opts.dispatcherURL != "" || opts.tokenFile != "" || opts.caFile != "") {
		return nil, errors.New("--demo cannot be combined with live dispatcher configuration")
	}
	app := &dashboard{opts: opts, client: &http.Client{Timeout: 8 * time.Second, CheckRedirect: rejectRedirect}}
	if opts.demo {
		app.sample = newDemoData(time.Now().UTC())
		return app, nil
	}
	upstream, err := url.Parse(opts.dispatcherURL)
	if err != nil || upstream.Host == "" || upstream.Scheme != "https" || upstream.User != nil || upstream.RawQuery != "" || upstream.ForceQuery || upstream.Fragment != "" || (upstream.Path != "" && upstream.Path != "/") || upstream.RawPath != "" || upstream.Opaque != "" {
		return nil, errors.New("--dispatcher-url must be an https origin without credentials, path, query, or fragment")
	}
	if opts.tokenFile == "" {
		return nil, errors.New("--dispatcher-token-file is required in live mode")
	}
	if opts.caFile == "" {
		return nil, errors.New("--dispatcher-ca-file is required in live mode")
	}
	app.trust = &dispatcherTransport{caFile: opts.caFile}
	if _, err := app.trust.transport(); err != nil {
		return nil, err
	}
	app.client.Transport = app.trust
	app.opts.dispatcherURL = strings.TrimRight(opts.dispatcherURL, "/")
	if _, err := app.readToken(); err != nil {
		return nil, err
	}
	if opts.logs.enabled {
		collector, err := newLogCollector(opts.logs)
		if err != nil {
			return nil, err
		}
		app.diagnostics = collector
	}
	return app, nil
}

// rejectRedirect prevents credentials from reaching any redirect destination.
func rejectRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// readToken reloads the file per request to support projected ServiceAccount token rotation.
func (app *dashboard) readToken() (string, error) {
	data, err := os.ReadFile(app.opts.tokenFile)
	if err != nil {
		return "", errors.New("dispatcher token file is unavailable")
	}
	token := strings.TrimSpace(string(data))
	if !validBearerToken(token) {
		return "", errors.New("dispatcher token file must contain one non-empty bearer token")
	}
	return token, nil
}

// validBearerToken prevents control characters and JSON escaping from hiding leaked credentials.
func validBearerToken(token string) bool {
	if token == "" {
		return false
	}
	for _, character := range token {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-._~+/=", character) {
			continue
		}
		return false
	}
	return true
}

// ServeHTTP provides only allowlisted GET routes and same-origin static assets.
func (app *dashboard) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("X-Frame-Options", "DENY")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeError(writer, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}
	switch request.URL.Path {
	case "/favicon.ico":
		writer.WriteHeader(http.StatusNoContent)
	case "/healthz":
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
	case "/readyz":
		if !app.opts.demo {
			if _, err := app.readToken(); err != nil {
				writeError(writer, http.StatusServiceUnavailable, "dispatcher token file is unavailable")
				return
			}
			if _, err := app.trust.transport(); err != nil {
				writeError(writer, http.StatusServiceUnavailable, "dispatcher CA file is unavailable or invalid")
				return
			}
		}
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
	case "/api/v1/config":
		writeJSON(writer, http.StatusOK, struct {
			Demo                   bool `json:"demo"`
			RefreshIntervalSeconds int  `json:"refreshIntervalSeconds"`
		}{app.opts.demo, 15})
	case "/api/v1/dispatcher/state":
		app.proxyObservation(writer, request, "/observe/v1/state")
	case "/api/v1/dispatcher/jobs":
		app.proxyObservation(writer, request, "/observe/v1/jobs")
	case "/api/v1/dispatcher/overrides":
		app.proxyObservation(writer, request, "/observe/v1/overrides")
	case "/api/v1/dispatcher/health":
		app.serveHealth(writer, request)
	case "/api/v1/diagnostics":
		app.serveDiagnostics(writer, request)
	case "/", "/assets/styles.css", "/assets/app.js":
		app.serveAsset(writer, request)
	default:
		writeError(writer, http.StatusNotFound, "unknown dashboard route")
	}
}

// serveAsset exposes only the three embedded frontend files.
func (app *dashboard) serveAsset(writer http.ResponseWriter, request *http.Request) {
	name, contentType := "web/index.html", "text/html; charset=utf-8"
	switch request.URL.Path {
	case "/assets/styles.css":
		name, contentType = "web/styles.css", "text/css; charset=utf-8"
	case "/assets/app.js":
		name, contentType = "web/app.js", "text/javascript; charset=utf-8"
	}
	data, err := assets.ReadFile(name)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "dashboard asset unavailable")
		return
	}
	writer.Header().Set("Content-Type", contentType)
	if _, err := writer.Write(data); err != nil {
		return // The client disconnected; response headers are already committed.
	}
}

// jobsQuery copies only documented observation filters, never a target URL or credentials.
func jobsQuery(query url.Values) string {
	allowed := url.Values{}
	for _, key := range []string{"name", "group", "cluster", "limit", "after", "generation"} {
		if values, exists := query[key]; exists {
			allowed[key] = values
		}
	}
	return allowed.Encode()
}

// fetch reads a fixed dispatcher path with a fresh server-only observation credential.
func (app *dashboard) fetch(request *http.Request, path, query string) ([]byte, int, error) {
	token, err := app.readToken()
	if err != nil {
		return nil, 0, err
	}
	upstream, err := http.NewRequestWithContext(request.Context(), http.MethodGet, app.opts.dispatcherURL+path, nil)
	if err != nil {
		return nil, 0, errors.New("could not prepare dispatcher request")
	}
	upstream.URL.RawQuery = query
	upstream.Header.Set("Authorization", "Bearer "+token)
	upstream.Header.Set("Accept", "application/json, text/plain")
	response, err := app.client.Do(upstream)
	if err != nil {
		return nil, 0, errors.New("dispatcher request failed")
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return nil, 0, errors.New("dispatcher redirect refused")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(data) > maxResponseBytes {
		return nil, 0, errors.New("dispatcher response could not be read within the size limit")
	}
	// An upstream error must not accidentally echo the secret into the browser.
	data = bytes.ReplaceAll(data, []byte(token), []byte("[redacted]"))
	return data, response.StatusCode, nil
}

// proxyObservation preserves observation JSON and status without forwarding upstream headers.
func (app *dashboard) proxyObservation(writer http.ResponseWriter, request *http.Request, path string) {
	if app.opts.demo {
		app.sample.serve(writer, request, time.Now().UTC())
		return
	}
	query := ""
	if path == "/observe/v1/jobs" {
		query = jobsQuery(request.URL.Query())
	}
	data, status, err := app.fetch(request, path, query)
	if err != nil {
		writeError(writer, http.StatusBadGateway, err.Error())
		return
	}
	if !json.Valid(data) {
		writeError(writer, http.StatusBadGateway, "dispatcher returned invalid JSON")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if _, err := writer.Write(data); err != nil {
		return // The client disconnected after the status was sent.
	}
}

// writeError uses the same error envelope as the observation API.
func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

// writeJSON serializes bounded, known response types before committing headers.
func writeJSON(writer http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		http.Error(writer, "could not encode dashboard response", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if _, err := fmt.Fprintln(writer, string(data)); err != nil {
		return // No further response is possible after a disconnected client.
	}
}
