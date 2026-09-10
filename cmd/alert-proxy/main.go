package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	userv1 "github.com/openshift/api/user/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/prow/pkg/logrusutil"
)

type options struct {
	port                                                                                                int
	channelID                                                                                           string
	slackTokenPath, slackSigningSecretPath, webhookTokenPath, forwarderSecretPath, slackIdentityMapPath string
	repeatInterval, groupInterval, deliveryLogTTL, escalationThreshold                                  time.Duration
	namespace, stateConfigMap, silenceConfigPath                                                        string
	enableSilences                                                                                      bool
	privilegedGroup, alertmanagerURL, alertmanagerCAFile, alertmanagerTokenPath                         string
	kubeconfig                                                                                          string
	gracePeriod                                                                                         time.Duration
}

const (
	alertmanagerServiceHost               = "alertmanager-user-workload.openshift-user-workload-monitoring.svc"
	alertmanagerServiceFullyQualifiedHost = "alertmanager-user-workload.openshift-user-workload-monitoring.svc.cluster.local"
	alertmanagerServicePort               = "9095"
)

func gatherOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("alert-proxy", flag.ContinueOnError)
	fs.IntVar(&o.port, "port", 8080, "HTTP server port.")
	fs.StringVar(&o.channelID, "channel-id", "", "Slack channel ID managed by the proxy.")
	fs.StringVar(&o.slackTokenPath, "slack-token-path", "/etc/slack/oauth_token", "Slack OAuth token file.")
	fs.StringVar(&o.slackSigningSecretPath, "slack-signing-secret-path", "/etc/slack/signing_secret", "Slack signing secret file.")
	fs.StringVar(&o.webhookTokenPath, "webhook-token-path", "/etc/webhook/token", "Alertmanager webhook bearer-token file.")
	fs.StringVar(&o.forwarderSecretPath, "forwarder-secret-path", "/etc/forwarder/hmac_secret", "slack-bot forwarder HMAC secret file.")
	fs.StringVar(&o.slackIdentityMapPath, "slack-identity-map-path", "/etc/alert-proxy/slack-identities.yaml", "Reviewed Kerberos-to-Slack identity overrides.")
	fs.DurationVar(&o.repeatInterval, "repeat-interval", 2*time.Hour, "Alertmanager route repeat_interval.")
	fs.DurationVar(&o.groupInterval, "group-interval", 5*time.Minute, "Alertmanager route group_interval.")
	fs.DurationVar(&o.deliveryLogTTL, "delivery-log-ttl", 8*time.Minute, "Persisted webhook delivery deduplication TTL.")
	fs.DurationVar(&o.escalationThreshold, "escalation-threshold", 24*time.Hour, "Unacknowledged firing duration before escalation.")
	fs.StringVar(&o.namespace, "namespace", "ci", "Namespace containing proxy state.")
	fs.StringVar(&o.stateConfigMap, "state-configmap", "alert-proxy-state", "State ConfigMap name.")
	fs.StringVar(&o.silenceConfigPath, "silence-config-path", "/etc/alert-proxy/silence.yaml", "Silence policy file.")
	fs.BoolVar(&o.enableSilences, "enable-silences", false, "Allow mutations that create or increase Alertmanager suppression.")
	fs.StringVar(&o.privilegedGroup, "privileged-group", "test-platform-ci-admins", "OpenShift Group allowed to mutate silences.")
	fs.StringVar(&o.alertmanagerURL, "alertmanager-url", "https://alertmanager-user-workload.openshift-user-workload-monitoring.svc:9095", "Alertmanager API URL.")
	fs.StringVar(&o.alertmanagerCAFile, "alertmanager-ca-file", "/etc/alertmanager-ca/service-ca.crt", "Alertmanager service CA file.")
	fs.StringVar(&o.alertmanagerTokenPath, "alertmanager-token-path", "/var/run/secrets/kubernetes.io/serviceaccount/token", "Alertmanager bearer-token file.")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Optional kubeconfig; in-cluster configuration is used by default.")
	fs.DurationVar(&o.gracePeriod, "grace-period", 30*time.Second, "HTTP shutdown grace period.")
	return o, fs.Parse(args)
}

func validateAlertmanagerURL(value string) error {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return errors.New("must be an absolute URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("must not contain user information, a query, or a fragment")
	}
	if parsed.Scheme != "https" {
		return errors.New("scheme must be HTTPS")
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host != alertmanagerServiceHost && host != alertmanagerServiceFullyQualifiedHost {
		return errors.New("must target the in-cluster Alertmanager service")
	}
	if parsed.Port() != alertmanagerServicePort {
		return fmt.Errorf("must target Alertmanager service port %s", alertmanagerServicePort)
	}
	if parsed.EscapedPath() != "" && parsed.EscapedPath() != "/" {
		return errors.New("must not contain an API path")
	}
	return nil
}

func (o options) validate() error {
	if o.channelID == "" {
		return errors.New("--channel-id is required")
	}
	if o.groupInterval >= o.deliveryLogTTL || o.deliveryLogTTL >= 2*o.groupInterval {
		return errors.New("delivery-log-ttl must satisfy group-interval < delivery-log-ttl < 2 * group-interval")
	}
	if o.repeatInterval <= 0 || o.escalationThreshold <= 0 {
		return errors.New("intervals must be positive")
	}
	for name, value := range map[string]string{"slack-token-path": o.slackTokenPath, "slack-signing-secret-path": o.slackSigningSecretPath, "webhook-token-path": o.webhookTokenPath, "forwarder-secret-path": o.forwarderSecretPath, "slack-identity-map-path": o.slackIdentityMapPath, "silence-config-path": o.silenceConfigPath, "alertmanager-url": o.alertmanagerURL, "alertmanager-ca-file": o.alertmanagerCAFile, "alertmanager-token-path": o.alertmanagerTokenPath} {
		if value == "" {
			return fmt.Errorf("--%s is required", name)
		}
	}
	if err := validateAlertmanagerURL(o.alertmanagerURL); err != nil {
		return fmt.Errorf("invalid --alertmanager-url: %w", err)
	}
	return nil
}

func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if env := os.Getenv(clientcmd.RecommendedConfigPathEnvVar); env != "" {
		return clientcmd.BuildConfigFromFlags("", env)
	}
	return rest.InClusterConfig()
}

type Readiness struct {
	mu               sync.RWMutex
	ready            bool
	required         bool
	lastSuccess      time.Time
	lastErr          error
	immediateFailure bool
	api              AlertmanagerAPI
	store            StateStore
	enabled          func() bool
	metrics          *Metrics
	operations       *OperationWorker
	now              func() time.Time
}

func (r *Readiness) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.ready {
		return false
	}
	if !r.required {
		return true
	}
	if r.immediateFailure {
		return false
	}
	return r.lastErr == nil || r.now().Sub(r.lastSuccess) <= time.Minute
}
func (r *Readiness) Check(ctx context.Context) error {
	state, err := r.store.Read(ctx)
	if err != nil {
		r.set(false, true, err, true)
		return err
	}
	if state.Mode == "draining" {
		err := errors.New("proxy state is in rollback drain mode")
		r.set(false, true, err, true)
		return err
	}
	required := r.enabled() || hasLiveSilenceWork(state)
	if !required {
		r.set(true, false, nil, false)
		return nil
	}
	if r.operations != nil {
		err = r.operations.Reconcile(ctx)
	} else {
		_, err = r.api.ListSilences(ctx)
	}
	if err == nil {
		current, stateErr := r.store.Read(ctx)
		if stateErr != nil {
			r.set(false, true, stateErr, true)
			return stateErr
		}
		if hasNonTerminalSilenceOperation(current) {
			err := errors.New("unfinished Alertmanager silence operation recovery")
			r.set(false, true, err, false)
			return err
		}
		r.mu.Lock()
		r.ready = true
		r.required = true
		r.lastSuccess = r.now()
		r.lastErr = nil
		r.immediateFailure = false
		r.mu.Unlock()
		if r.metrics != nil {
			r.metrics.alertmanagerReady.Set(1)
		}
		return nil
	}
	immediate := isImmediateAMFailure(err)
	r.mu.Lock()
	r.required = true
	r.lastErr = err
	r.immediateFailure = immediate
	if immediate || r.lastSuccess.IsZero() || r.now().Sub(r.lastSuccess) > time.Minute {
		r.ready = false
	}
	r.mu.Unlock()
	if r.metrics != nil {
		r.metrics.alertmanagerReady.Set(0)
	}
	return err
}
func (r *Readiness) set(ready, required bool, err error, immediate bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ready = ready
	r.required = required
	r.lastErr = err
	r.immediateFailure = immediate
	if r.metrics != nil {
		if ready {
			r.metrics.alertmanagerReady.Set(1)
		} else {
			r.metrics.alertmanagerReady.Set(0)
		}
	}
}
func (r *Readiness) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.Check(ctx)
		}
	}
}
func isImmediateAMFailure(err error) bool {
	var h *AMHTTPError
	if errors.As(err, &h) && (h.StatusCode == http.StatusUnauthorized || h.StatusCode == http.StatusForbidden) {
		return true
	}
	text := err.Error()
	return strings.Contains(text, "certificate") || strings.Contains(text, "x509") || strings.Contains(text, "Alertmanager CA") || strings.Contains(text, "Alertmanager token")
}

func main() {
	logrusutil.ComponentInit()
	args := os.Args[1:]
	drainMode := false
	if len(args) > 0 && args[0] == "drain" {
		drainMode = true
		args = args[1:]
	}
	o, err := gatherOptions(args)
	if err != nil {
		logrus.WithError(err).Fatal("parse options")
	}
	if err := o.validate(); err != nil {
		logrus.WithError(err).Fatal("invalid options")
	}
	policy, err := LoadSilencePolicy(o.silenceConfigPath)
	if err != nil {
		logrus.WithError(err).Fatal("load silence policy")
	}
	if policy.Config.MinDuration != o.repeatInterval {
		logrus.Fatalf("silence minDuration %s must equal repeat-interval %s", policy.Config.MinDuration, o.repeatInterval)
	}
	config, err := loadRESTConfig(o.kubeconfig)
	if err != nil {
		logrus.WithError(err).Fatal("load Kubernetes config")
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logrus.WithError(err).Fatal("create Kubernetes client")
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		logrus.WithError(err).Fatal("register Kubernetes API types")
	}
	if err := userv1.AddToScheme(scheme); err != nil {
		logrus.WithError(err).Fatal("register OpenShift user API types")
	}
	kubeObjectClient, err := ctrlruntimeclient.New(config, ctrlruntimeclient.Options{Scheme: scheme})
	if err != nil {
		logrus.WithError(err).Fatal("create Kubernetes object client")
	}
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	store := NewConfigMapStateStore(clientset, o.namespace, o.stateConfigMap, metrics)
	if err := store.Update(context.Background(), func(*State) error { return nil }); err != nil {
		logrus.WithError(err).Error("state is unavailable; process will remain unready until it can be read without overwriting it")
	}
	slackClient := &SlackHTTPClient{TokenPath: o.slackTokenPath}
	renderer := &Renderer{SilencesEnabled: o.enableSilences, Policy: policy, Now: time.Now}
	api := &HTTPAlertmanager{BaseURL: o.alertmanagerURL, CAFile: o.alertmanagerCAFile, TokenPath: o.alertmanagerTokenPath, Timeout: 10 * time.Second}
	authz := NewAuthorizer(&KubeGroupReader{Client: kubeObjectClient}, slackClient, o.privilegedGroup, o.slackIdentityMapPath, metrics, logrus.WithField("component", "authz"))
	if err := authz.Refresh(context.Background()); err != nil {
		logrus.WithError(err).Warn("initial authorization refresh failed; privileged actions fail closed")
	}
	outbox := NewOutboxWorker(store, slackClient, renderer, metrics)
	operations := NewOperationWorker(store, api, authz, policy, func() bool { return o.enableSilences }, renderer, metrics)
	operations.outboxWake = outbox.Wake
	if err := outbox.RecoverInFlight(context.Background()); err != nil {
		logrus.WithError(err).Warn("Slack outbox recovery is pending while state is unavailable")
	}
	if drainMode {
		logrus.Info("starting rollback drain as the replacement/only alert-proxy process; the serving replica must already be stopped and the state ConfigMap retained")
		controller := &DrainController{store: store, api: api, operations: operations, outbox: outbox, renderer: renderer, now: time.Now}
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := controller.Run(ctx); err != nil {
			logrus.WithError(err).Fatal("drain failed")
		}
		logrus.Info("drain completed: no live proxy-owned silences or pending Slack work remain")
		return
	}
	sweeper := &LifecycleSweeper{store: store, renderer: renderer, repeatInterval: o.repeatInterval, escalationThreshold: o.escalationThreshold, deliveryTTL: o.deliveryLogTTL, requestTTL: 24 * time.Hour, auditTTL: 30 * 24 * time.Hour, now: time.Now, wakeOutbox: outbox.Wake}
	if err := sweeper.Sweep(context.Background()); err != nil {
		logrus.WithError(err).Warn("startup lifecycle sweep is pending while state is unavailable")
	}
	readiness := &Readiness{api: api, store: store, enabled: func() bool { return o.enableSilences }, metrics: metrics, operations: operations, now: time.Now}
	if err := readiness.Check(context.Background()); err != nil {
		logrus.WithError(err).Warn("initial Alertmanager readiness check failed")
	}
	if readiness.required {
		if err := operations.RecoverAll(context.Background()); err != nil {
			readiness.set(false, true, err, isImmediateAMFailure(err))
			logrus.WithError(err).Warn("unfinished silence operation recovery did not complete")
		}
	}
	processor := &WebhookProcessor{store: store, channel: o.channelID, deliveryTTL: o.deliveryLogTTL, requestTTL: 24 * time.Hour, auditTTL: 30 * 24 * time.Hour, now: time.Now, renderer: renderer, metrics: metrics, wake: outbox.Wake}
	interactions := &InteractionHandler{store: store, slack: slackClient, operations: operations, policy: policy, channel: o.channelID, forwarderSecretPath: o.forwarderSecretPath, slackSigningSecretPath: o.slackSigningSecretPath, renderer: renderer, metrics: metrics, now: time.Now, wakeOutbox: outbox.Wake}
	mentions := &MentionHandler{store: store, operations: operations, policy: policy, channel: o.channelID, forwarderSecretPath: o.forwarderSecretPath, renderer: renderer, metrics: metrics, now: time.Now, wakeOutbox: outbox.Wake}
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook/alertmanager", WebhookHandler(processor, o.webhookTokenPath, readiness.Ready))
	mux.Handle("/interactions", interactions)
	mux.Handle("/mentions", mentions)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !readiness.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go authz.Run(ctx)
	go outbox.Run(ctx)
	go operations.Run(ctx)
	go sweeper.Run(ctx)
	go readiness.Run(ctx)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if state, err := store.Read(ctx); err == nil {
					metrics.refreshState(state)
				}
			}
		}
	}()
	server := &http.Server{Addr: ":" + strconv.Itoa(o.port), Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), o.gracePeriod)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logrus.WithField("port", o.port).Info("alert-proxy listening")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logrus.WithError(err).Fatal("HTTP server failed")
	}
}
