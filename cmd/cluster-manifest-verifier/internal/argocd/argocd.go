package argocd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	synccommon "github.com/argoproj/argo-cd/gitops-engine/pkg/sync/common"
	argocdclient "github.com/argoproj/argo-cd/v3/pkg/apiclient"
	"github.com/argoproj/argo-cd/v3/pkg/apiclient/application"
	"github.com/argoproj/argo-cd/v3/pkg/apiclient/applicationset"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/ci-tools-standalone/cmd/cluster-manifest-verifier/internal/syncplanner"
)

const (
	gitopsNamespace   = "openshift-gitops"
	syncPollInterval  = 2 * time.Second
	syncTimeout       = 5 * time.Minute
	tempAppNamePrefix = "cmv-pr-"
	generateRetries   = 5
	generateRetryWait = 2 * time.Second
)

type Config struct {
	Server     string
	Revision   string
	PullNumber int
}

type Client struct {
	revision     string
	pullNumber   int
	appsetClient applicationset.ApplicationSetServiceClient
	appClient    application.ApplicationServiceClient
	close        func() error
}

func New(cfg Config) (*Client, error) {
	authToken := strings.TrimSpace(os.Getenv("ARGOCD_AUTH_TOKEN"))
	if authToken == "" {
		return nil, errors.New("ARGOCD_AUTH_TOKEN is required")
	}

	client, err := argocdclient.NewClient(&argocdclient.ClientOptions{
		ServerAddr: cfg.Server,
		AuthToken:  authToken,
		GRPCWeb:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("create Argo CD client: %w", err)
	}

	appsetCloser, appsetClient, err := client.NewApplicationSetClient()
	if err != nil {
		return nil, fmt.Errorf("create ApplicationSet client: %w", err)
	}

	appCloser, appClient, err := client.NewApplicationClient()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create Application client: %w", err), appsetCloser.Close())
	}

	return &Client{
		revision:     cfg.Revision,
		pullNumber:   cfg.PullNumber,
		appsetClient: appsetClient,
		appClient:    appClient,
		close: func() error {
			return errors.Join(appsetCloser.Close(), appCloser.Close())
		},
	}, nil
}

func (c *Client) Close() error {
	if c.close == nil {
		return nil
	}
	return c.close()
}

func (c *Client) Revision() string {
	return c.revision
}

func (c *Client) GenerateApplications(ctx context.Context, applicationSets []argov1alpha1.ApplicationSet) ([]argov1alpha1.Application, error) {
	var generated []argov1alpha1.Application
	var genErr error
	for _, appset := range applicationSets {
		appset, err := c.applicationSetForGenerate(appset)
		if err != nil {
			genErr = errors.Join(genErr, fmt.Errorf("ApplicationSet %q: %w", appset.Name, err))
			continue
		}

		resp, err := c.generateWithRetry(ctx, &appset)
		if err != nil {
			genErr = errors.Join(genErr, fmt.Errorf("ApplicationSet %q: %w", appset.Name, err))
			continue
		}
		logrus.Infof("ApplicationSet %q: generated %d application(s)", appset.Name, len(resp.Applications))
		for _, app := range resp.Applications {
			if app != nil {
				generated = append(generated, *app)
			}
		}
	}
	return generated, genErr
}

// generateWithRetry calls ApplicationSet.Generate, retrying when Argo CD's shared
// git working tree hits a transient index.lock / HEAD.lock race.
func (c *Client) generateWithRetry(ctx context.Context, appset *argov1alpha1.ApplicationSet) (*applicationset.ApplicationSetGenerateResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= generateRetries; attempt++ {
		resp, err := c.appsetClient.Generate(ctx, &applicationset.ApplicationSetGenerateRequest{
			ApplicationSet: appset,
		})
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isTransientGitLockError(err) || attempt == generateRetries {
			return nil, err
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"applicationSet": appset.Name,
			"attempt":        attempt,
		}).Warn("ApplicationSet generate hit transient git lock; retrying")
		timer := time.NewTimer(generateRetryWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

// isTransientGitLockError reports whether err looks like a concurrent git
// checkout race on Argo CD's on-disk repository cache (index.lock / HEAD.lock).
// Permanent failures such as permission denied on a lock file are not retried.
func isTransientGitLockError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Git lock contention always includes "File exists"; do not treat other
	// .lock failures (for example permission denied) as transient.
	return strings.Contains(msg, "File exists") && strings.Contains(msg, ".lock")
}

// DryRunSync creates a temporary Application from source (PR revision, unique name),
// dry-run syncs it, then deletes it. The live Application is never touched.
func (c *Client) DryRunSync(ctx context.Context, source argov1alpha1.Application, plan *syncplanner.Plan) error {
	temp := c.temporaryApplication(source)

	if plan.FullSync {
		logrus.Infof("Application %q: dry-run syncing full application as %q (non-resource change in PR)", source.Name, temp.Name)
	} else {
		logrus.Infof("Application %q: dry-run syncing %d changed resource(s) as %q", source.Name, len(plan.Resources), temp.Name)
	}

	validate := true
	if _, err := c.appClient.Create(ctx, &application.ApplicationCreateRequest{
		Application: &temp,
		Validate:    &validate,
	}); err != nil {
		return fmt.Errorf("create temporary application %q: %w", temp.Name, err)
	}

	defer func() {
		cascade := false
		if _, err := c.appClient.Delete(ctx, &application.ApplicationDeleteRequest{
			Name:         &temp.Name,
			AppNamespace: new(gitopsNamespace),
			Cascade:      &cascade,
		}); err != nil {
			logrus.WithError(err).Warnf("delete temporary application %q", temp.Name)
		} else {
			logrus.Infof("deleted temporary application %q", temp.Name)
		}
	}()

	if _, err := c.appClient.Sync(ctx, c.syncRequest(&temp, plan)); err != nil {
		return fmt.Errorf("start dry-run sync: %w", err)
	}
	return c.waitForSyncOperation(ctx, temp.Name)
}

func (c *Client) temporaryApplication(source argov1alpha1.Application) argov1alpha1.Application {
	app := *source.DeepCopy()
	app.ObjectMeta = metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s%d-%s", tempAppNamePrefix, c.pullNumber, source.Name),
		Namespace: gitopsNamespace,
	}
	app.Status = argov1alpha1.ApplicationStatus{}
	app.Operation = nil
	app.Spec.SyncPolicy = nil

	if app.Spec.Source != nil {
		app.Spec.Source.TargetRevision = c.revision
	}
	for i := range app.Spec.Sources {
		app.Spec.Sources[i].TargetRevision = c.revision
	}
	return app
}

func (c *Client) applicationSetForGenerate(appset argov1alpha1.ApplicationSet) (argov1alpha1.ApplicationSet, error) {
	if appset.Namespace != gitopsNamespace {
		return appset, fmt.Errorf("metadata.namespace must be %q, got %q", gitopsNamespace, appset.Namespace)
	}
	for i := range appset.Spec.Generators {
		appset.Spec.Generators[i] = c.setGeneratorRevision(appset.Spec.Generators[i])
	}
	if appset.Spec.Template.Spec.Source != nil {
		appset.Spec.Template.Spec.Source.TargetRevision = c.revision
	}
	for i := range appset.Spec.Template.Spec.Sources {
		appset.Spec.Template.Spec.Sources[i].TargetRevision = c.revision
	}
	return appset, nil
}

func (c *Client) setGeneratorRevision(gen argov1alpha1.ApplicationSetGenerator) argov1alpha1.ApplicationSetGenerator {
	if gen.Git != nil {
		git := *gen.Git
		git.Revision = c.revision
		gen.Git = &git
	}
	if gen.Matrix != nil {
		for i := range gen.Matrix.Generators {
			gen.Matrix.Generators[i] = c.setNestedGeneratorRevision(gen.Matrix.Generators[i])
		}
	}
	if gen.Merge != nil {
		for i := range gen.Merge.Generators {
			gen.Merge.Generators[i] = c.setNestedGeneratorRevision(gen.Merge.Generators[i])
		}
	}
	return gen
}

func (c *Client) setNestedGeneratorRevision(gen argov1alpha1.ApplicationSetNestedGenerator) argov1alpha1.ApplicationSetNestedGenerator {
	if gen.Git == nil {
		return gen
	}
	git := *gen.Git
	git.Revision = c.revision
	gen.Git = &git
	return gen
}

func (c *Client) syncRequest(app *argov1alpha1.Application, plan *syncplanner.Plan) *application.ApplicationSyncRequest {
	dryRun := true
	req := &application.ApplicationSyncRequest{
		Name:         &app.Name,
		AppNamespace: new(gitopsNamespace),
		DryRun:       &dryRun,
		Revision:     &c.revision,
	}
	if !plan.FullSync {
		for i := range plan.Resources {
			req.Resources = append(req.Resources, &plan.Resources[i])
		}
	}
	return req
}

func (c *Client) waitForSyncOperation(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	ticker := time.NewTicker(syncPollInterval)
	defer ticker.Stop()

	for {
		app, err := c.appClient.Get(ctx, &application.ApplicationQuery{
			Name:         &name,
			AppNamespace: new(gitopsNamespace),
		})
		if err != nil {
			return fmt.Errorf("get application status: %w", err)
		}

		op := app.Status.OperationState
		if op != nil && op.Phase.Completed() {
			if op.Phase.Successful() {
				logrus.Infof("Application %q: dry-run sync succeeded", name)
				return nil
			}
			return fmt.Errorf("dry-run sync failed: phase=%s message=%q%s",
				op.Phase, op.Message, formatFailedSyncResources(op.SyncResult))
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for dry-run sync: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func formatFailedSyncResources(result *argov1alpha1.SyncOperationResult) string {
	if result == nil {
		return ""
	}
	var failed []string
	for _, resource := range result.Resources {
		if resource.Status == synccommon.ResultCodeSyncFailed {
			failed = append(failed, fmt.Sprintf("%s/%s in %s: %s",
				resource.Kind, resource.Name, resource.Namespace, resource.Message))
		}
	}
	if len(failed) == 0 {
		return ""
	}
	return "; failed resources: " + strings.Join(failed, "; ")
}
