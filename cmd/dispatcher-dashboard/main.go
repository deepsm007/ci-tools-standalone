// Command dispatcher-dashboard serves a read-only view of dispatcher observation data.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// options holds the local listener and one trusted upstream, never browser credentials.
type options struct {
	listen        string
	dispatcherURL string
	tokenFile     string
	caFile        string
	demo          bool
	logs          logOptions
}

// main configures the read-only server and shuts down in-flight requests on termination.
func main() {
	opts := options{}
	flag.StringVar(&opts.listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flag.StringVar(&opts.dispatcherURL, "dispatcher-url", "", "Dispatcher origin URL (required in live mode)")
	flag.StringVar(&opts.tokenFile, "dispatcher-token-file", "", "Projected ServiceAccount token with prowjob-dispatcher-observation audience (required in live mode)")
	flag.StringVar(&opts.caFile, "dispatcher-ca-file", "", "PEM service CA bundle for the dispatcher HTTPS endpoint (required in live mode)")
	flag.BoolVar(&opts.demo, "demo", false, "Serve explicitly labeled sample data without a dispatcher")
	flag.BoolVar(&opts.logs.enabled, "enable-log-diagnostics", false, "Enable manual diagnostic reads from Kubernetes pod logs")
	flag.StringVar(&opts.logs.namespace, "log-namespace", "ci", "Namespace containing the fixed diagnostic log sources")
	flag.StringVar(&opts.logs.kubeconfig, "log-kubeconfig", "", "Explicit local kubeconfig for diagnostics; otherwise use in-cluster credentials")
	flag.Int64Var(&opts.logs.maxBytes, "log-max-bytes", 8<<20, "Total diagnostic log byte budget across both components (1 MiB through 64 MiB)")
	flag.Parse()
	if err := run(context.Background(), opts); err != nil {
		logrus.WithError(err).Fatal("dispatcher dashboard stopped")
	}
}

// run binds a bounded HTTP server after validating configuration and credentials.
func run(ctx context.Context, opts options) error {
	app, err := newDashboard(opts)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: opts.listen, Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	failures := make(chan error, 1)
	go serve(server, failures)
	logrus.WithFields(logrus.Fields{"listen": opts.listen, "demo": opts.demo}).Info("serving read-only dispatcher dashboard")
	select {
	case err := <-failures:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down dashboard: %w", err)
	}
	return <-failures
}

// serve reports listener failures without exposing upstream request details.
func serve(server *http.Server, failures chan<- error) {
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	failures <- err
}
