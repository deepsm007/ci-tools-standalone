package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// dispatcherTransport rotates pooled TLS connections when the injected service CA changes.
type dispatcherTransport struct {
	caFile string
	mu     sync.Mutex
	bundle []byte
	active *http.Transport
}

// transport loads trust before each request without mutating transports used concurrently.
func (trust *dispatcherTransport) transport() (*http.Transport, error) {
	bundle, err := os.ReadFile(trust.caFile)
	if err != nil {
		return nil, errors.New("dispatcher CA file is unavailable")
	}
	trust.mu.Lock()
	defer trust.mu.Unlock()
	if trust.active != nil && bytes.Equal(bundle, trust.bundle) {
		return trust.active, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.New("system certificate trust is unavailable")
	}
	if !roots.AppendCertsFromPEM(bundle) {
		return nil, errors.New("dispatcher CA file must contain PEM certificates")
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 8 * time.Second,
		IdleConnTimeout:       60 * time.Second,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   10,
		ForceAttemptHTTP2:     true,
	}
	if trust.active != nil {
		trust.active.CloseIdleConnections()
	}
	trust.bundle, trust.active = bundle, transport
	return transport, nil
}

// RoundTrip refuses to send the projected credential unless current service trust is usable.
func (trust *dispatcherTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" {
		return nil, errors.New("dispatcher requests require HTTPS")
	}
	transport, err := trust.transport()
	if err != nil {
		return nil, err
	}
	return transport.RoundTrip(request)
}
