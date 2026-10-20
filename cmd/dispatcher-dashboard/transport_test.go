package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestServiceCATrust verifies trust changes and invalid mounts without making network calls.
func TestServiceCATrust(t *testing.T) {
	for _, tc := range []struct {
		name, replacement, wantError string
		remove                       bool
	}{
		{name: "unchanged", replacement: "testdata/service-ca.pem"},
		{name: "rotated", replacement: "testdata/service-ca-rotated.pem"},
		{name: "invalid", replacement: "not a PEM certificate", wantError: "must contain PEM certificates"},
		{name: "empty", wantError: "must contain PEM certificates"},
		{name: "unavailable", remove: true, wantError: "CA file is unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original, err := os.ReadFile("testdata/service-ca.pem")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "ca.crt")
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			trust := &dispatcherTransport{caFile: path}
			first, err := trust.transport()
			if err != nil {
				t.Fatal(err)
			}
			defer first.CloseIdleConnections()
			if first.TLSClientConfig.InsecureSkipVerify || first.TLSClientConfig.MinVersion != tls.VersionTLS12 {
				t.Fatal("TLS verification or minimum version missing")
			}
			replacement := []byte(tc.replacement)
			if strings.HasPrefix(tc.replacement, "testdata/") {
				replacement, err = os.ReadFile(tc.replacement)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(path, replacement, 0600); err != nil {
				t.Fatal(err)
			}
			if tc.remove {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			second, err := trust.transport()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error = %v, want %q", err, tc.wantError)
				}
				response, err := trust.RoundTrip(httptest.NewRequest("GET", "https://dispatcher.invalid/observe/v1/state", nil))
				if response != nil && response.Body != nil {
					if closeErr := response.Body.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("request error = %v, want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer second.CloseIdleConnections()
			if (first == second) != (tc.name == "unchanged") {
				t.Error("transport was not reused or replaced with the CA bundle")
			}
			block, _ := pem.Decode(replacement)
			if block == nil {
				t.Fatal("invalid fixture")
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cert.Verify(x509.VerifyOptions{Roots: second.TLSClientConfig.RootCAs, CurrentTime: cert.NotBefore.Add(1)}); err != nil {
				t.Errorf("replacement CA not trusted: %v", err)
			}
			if diff := cmp.Diff(replacement, trust.bundle); diff != "" {
				t.Errorf("unexpected bundle (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTransportHTTPSOnly rejects plaintext before reading trust or sending any credentials.
func TestTransportHTTPSOnly(t *testing.T) {
	for _, tc := range []struct{ name, url string }{
		{"plaintext", "http://dispatcher.invalid/observe/v1/state"},
		{"unsupported scheme", "ftp://dispatcher.invalid/observe/v1/state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trust := &dispatcherTransport{caFile: "must-not-read"}
			request := httptest.NewRequest("GET", tc.url, nil)
			request.Header.Set("Authorization", "Bearer private-projected-token")
			response, err := trust.RoundTrip(request)
			if response != nil && response.Body != nil {
				if closeErr := response.Body.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if err == nil || err.Error() != "dispatcher requests require HTTPS" {
				t.Errorf("error = %v", err)
			}
		})
	}
}
