package main

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	v1 "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"
	"sigs.k8s.io/prow/pkg/config"
	"sigs.k8s.io/prow/pkg/github"
	"sigs.k8s.io/prow/pkg/labels"
)

func TestTraditionalDispatchCheckModes(t *testing.T) {
	for _, mode := range []string{"auto", "lgtm", "manual"} {
		t.Run(mode, func(t *testing.T) {
			pj := makeTriggerPJ(strings.Repeat("a", 40))
			refs := pj.Spec.Refs
			first := config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-unit"},
				Reporter: config.Reporter{Context: "ci/unit"}, AlwaysRun: true}
			second := config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-e2e"},
				Reporter: config.Reporter{Context: "ci/e2e"}, RerunCommand: "/test e2e"}
			provider := &ConfigDataProvider{updatedPresubmits: map[string]presubmitTests{
				"openshift/myrepo": {alwaysRequired: []config.Presubmit{first}, protected: []config.Presubmit{second}},
			}}
			unit := makeProwJob(first.Name, refs.Pulls[0].SHA)
			unit.Status.State = v1.SuccessState
			reader := newFakePJLister(unit)
			gh := &fakeAgenticGitHub{pr: github.PullRequest{Number: 42, State: github.PullRequestStateOpen,
				Head: github.PullRequestBranch{SHA: refs.Pulls[0].SHA},
				Base: github.PullRequestBranch{Ref: "main", Repo: github.Repo{Name: refs.Repo, Owner: github.User{Login: refs.Org}}}}}
			checks := &dispatchChecks{gh: gh, appID: 101}
			enabled := mustEnabledConfig(t)
			enabled.Orgs[0].Repos[0].Mode.Trigger = mode
			main, lgtm := &watcher{config: enabled}, &watcher{}
			if mode == "lgtm" {
				main, lgtm = lgtm, main
			}
			logger := logrus.NewEntry(logrus.New())
			cw := &clientWrapper{ghc: gh, configDataProvider: provider, watcher: main, lgtmWatcher: lgtm, pjLister: reader, checks: checks}
			event := github.PullRequestEvent{Action: github.PullRequestActionOpened, Repo: gh.pr.Base.Repo, PullRequest: gh.pr}
			cw.handlePipelineContextCreation(logger, event)
			cw.handlePipelineContextCreation(logger, event)
			require.Len(t, gh.checks, 1)
			require.Equal(t, "queued", gh.checks[0].Status)
			gh.beforeComment = func(string) {
				require.Equal(t, "in_progress", gh.checks[0].Status)
				require.NotEqual(t, "success", gh.checks[0].Conclusion)
			}
			switch mode {
			case "auto":
				r := &reconciler{pjclientset: reader.(ctrlruntimeclient.Client), lister: reader, configDataProvider: provider,
					ghc: gh, logger: logger, watcher: main, lgtmWatcher: lgtm, checks: checks,
					closedPRsCache: closedPRsCache{prs: map[string]pullRequest{}, ghc: gh}}
				req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: unit.Namespace, Name: unit.Name}}
				require.NoError(t, r.reconcile(context.Background(), req))
				require.NoError(t, r.reconcile(context.Background(), req))
			case "lgtm":
				event.Action, event.Label = github.PullRequestActionLabeled, github.Label{Name: labels.LGTM}
				cw.handleLabelAddition(logger, event)
				cw.handleLabelAddition(logger, event)
			case "manual":
				cw.handleIssueComment(logger, github.IssueCommentEvent{Action: github.IssueCommentActionCreated, Repo: gh.pr.Base.Repo,
					Issue: github.Issue{Number: 42, PullRequest: &struct{}{}}, Comment: github.IssueComment{Body: "/pipeline required"}})
			}
			require.Len(t, gh.comments, 1)
			require.Contains(t, gh.comments[0].Body, "/test e2e")
			require.Equal(t, "completed", gh.checks[0].Status)
			require.Equal(t, "success", gh.checks[0].Conclusion)
			// Reopening events do not undo dispatch; a fresh push has its own check.
			event.Action = github.PullRequestActionReopened
			cw.handlePipelineContextCreation(logger, event)
			require.Equal(t, "success", gh.checks[0].Conclusion)
			event.Action, event.PullRequest.Head.SHA = github.PullRequestActionSynchronize, strings.Repeat("b", 40)
			cw.handlePipelineContextCreation(logger, event)
			require.Len(t, gh.checks, 2)
			require.Equal(t, "queued", gh.checks[1].Status)
			require.Empty(t, gh.checks[1].Conclusion)
		})
	}
}

func TestTraditionalDispatchFailuresAndRetry(t *testing.T) {
	for _, failure := range []string{"check", "planning", "context", "comment", "completion"} {
		t.Run(failure, func(t *testing.T) {
			pj := makeTriggerPJ(strings.Repeat("a", 40))
			job := config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-e2e"},
				Reporter: config.Reporter{Context: "ci/e2e"}, RerunCommand: "/test e2e"}
			presubmits := presubmitTests{protected: []config.Presubmit{job}}
			gh := &fakeAgenticGitHub{}
			checks := &dispatchChecks{gh: gh, appID: 101}
			reader := newFakePJLister()
			mode := modeDelta
			switch failure {
			case "check":
				gh.failCheck = true
			case "planning":
				reader = errorLister{}
			case "context", "comment":
				// A forced rerun must retire an old success before attempting dispatch.
				_, err := gh.CreateCheckRun("openshift", "myrepo", traditionalCheck(pj.Spec.Refs, "completed", "success", "previous dispatch"))
				require.NoError(t, err)
				mode = modeForce
				gh.failStatus, gh.failCommentAfterWrite = failure == "context", failure == "comment"
			case "completion":
				gh.afterCheckWrite = func(check github.CheckRun) {
					if check.Status == "in_progress" {
						gh.failCheck = true
					}
				}
			}
			gh.beforeComment = func(string) {
				require.Len(t, gh.checks, 1)
				require.NotEqual(t, "success", gh.checks[0].Conclusion)
			}
			released := false
			err := sendCommentWithMode(presubmits, pj, gh, func() { released = true }, reader, mode, false, checks)
			require.Error(t, err)
			require.True(t, released)
			for _, check := range gh.checks {
				require.NotEqual(t, "success", check.Conclusion)
			}
			if failure == "check" || failure == "planning" || failure == "context" {
				require.Empty(t, gh.comments)
			}
			if failure == "completion" {
				// Hook created a job before the failed success update. Retry closes
				// the same check without another /test comment or waiting for results.
				gh.failCheck, gh.afterCheckWrite = false, nil
				reader = newFakePJLister(makeProwJob(job.Name, pj.Spec.Refs.Pulls[0].SHA))
				ready, err := checkFirstStageComplete(context.Background(), reader, pj, presubmits)
				require.NoError(t, err)
				require.True(t, ready)
				require.NoError(t, sendCommentWithMode(presubmits, pj, gh, func() {}, reader, modeDelta, false, checks))
				require.Len(t, gh.comments, 1)
				require.Len(t, gh.checks, 1)
				require.Equal(t, "success", gh.checks[0].Conclusion)
				require.Equal(t, 1, gh.statusWrites, "an empty retry must not reset job results")
			}
		})
	}
}

func TestTraditionalDispatchPublishesOnlyRequestedContexts(t *testing.T) {
	for _, mode := range []scheduleMode{modeDelta, modeForce} {
		t.Run(map[scheduleMode]string{modeDelta: "initial-delta", modeForce: "rerun"}[mode], func(t *testing.T) {
			pj := makeTriggerPJ(strings.Repeat("a", 40))
			jobs := []config.Presubmit{
				{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-existing"}, Reporter: config.Reporter{Context: "ci/existing"}, RerunCommand: "/test existing"},
				{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-required"}, Reporter: config.Reporter{Context: "ci/required"}, RerunCommand: "/test required"},
				{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-changed", Annotations: map[string]string{"pipeline_run_if_changed": `\.go$`}}, Reporter: config.Reporter{Context: "ci/changed"}, RerunCommand: "/test changed"},
				{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-skipped", Annotations: map[string]string{"pipeline_run_if_changed": `\.md$`}}, Reporter: config.Reporter{Context: "ci/skipped"}, RerunCommand: "/test skipped"},
			}
			gh := &fakeAgenticGitHub{changes: []github.PullRequestChange{{Filename: "main.go"}}}
			checks := &dispatchChecks{gh: gh, appID: 101}
			if mode == modeForce {
				_, err := gh.CreateCheckRun(pj.Spec.Refs.Org, pj.Spec.Refs.Repo, traditionalCheck(pj.Spec.Refs, "completed", "success", "previous dispatch"))
				require.NoError(t, err)
			} else {
				require.NoError(t, checks.pending(pj.Spec.Refs))
			}
			want := []string{"ci/required", "ci/changed"}
			if mode == modeForce {
				want = append([]string{"ci/existing"}, want...)
			}
			gh.beforeComment = func(body string) {
				require.NotEqual(t, "success", gh.checks[0].Conclusion)
				var contexts []string
				for _, status := range gh.statusUpdates {
					require.Equal(t, "pending", status.State)
					contexts = append(contexts, status.Context)
				}
				require.Equal(t, want, contexts, "publish requested contexts before Hook receives /test")
				require.NotContains(t, body, "/test skipped")
			}
			presubmits := presubmitTests{protected: jobs[:2], pipelineConditionallyRequired: jobs[2:]}
			require.NoError(t, sendCommentWithMode(presubmits, pj, gh, func() {}, newFakePJLister(makeProwJob(jobs[0].Name, pj.Spec.Refs.Pulls[0].SHA)), mode, false, checks))
			require.Len(t, gh.comments, 1)
			require.Equal(t, "success", gh.checks[0].Conclusion)
		})
	}
}

func TestTraditionalEmptyDispatchAndCheckOwnership(t *testing.T) {
	pj := makeTriggerPJ(strings.Repeat("a", 40))
	foreign := traditionalCheck(pj.Spec.Refs, "completed", "success", "another app")
	foreign.ID, foreign.App.ID = 900, 202
	gh := &fakeAgenticGitHub{checks: []github.CheckRun{foreign}}
	checks := &dispatchChecks{gh: gh, appID: 101}
	results := make(chan error, 2)
	go func() { results <- checks.pending(pj.Spec.Refs) }()
	go func() {
		results <- sendCommentWithMode(presubmitTests{}, pj, gh, func() {}, nil, modeDelta, false, checks)
	}()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Empty(t, gh.comments)
	require.Len(t, gh.checks, 2)
	require.Equal(t, foreign, gh.checks[0])
	require.Equal(t, "success", gh.checks[1].Conclusion)
}

func TestTraditionalGreenCheckNoWork(t *testing.T) {
	for _, scenario := range []string{"empty", "already-dispatched", "planning-error", "ack-error"} {
		t.Run(scenario, func(t *testing.T) {
			pj := makeTriggerPJ(strings.Repeat("a", 40))
			gate := traditionalCheck(pj.Spec.Refs, "completed", "success", "previous dispatch")
			gate.ID, gate.App.ID = 1, 101
			gh := &fakeAgenticGitHub{checks: []github.CheckRun{gate}, failCheck: true}
			job := config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-openshift-myrepo-main-e2e"}, RerunCommand: "/test e2e"}
			presubmits := presubmitTests{}
			var reader ctrlruntimeclient.Reader
			switch scenario {
			case "already-dispatched":
				presubmits.protected = []config.Presubmit{job}
				reader = newFakePJLister(makeProwJob(job.Name, pj.Spec.Refs.Pulls[0].SHA))
			case "planning-error":
				presubmits.protected = []config.Presubmit{job}
				reader = errorLister{}
			case "ack-error":
				gh.failCommentAfterWrite = true
			}
			released := false
			// A fresh dispatcher models restart: no in-memory success is retained.
			err := sendCommentWithMode(presubmits, pj, gh, func() { released = true }, reader, modeDelta, scenario == "ack-error", &dispatchChecks{gh: gh, appID: 101})
			if strings.HasSuffix(scenario, "error") {
				require.Error(t, err)
				require.True(t, released)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, gate, gh.checks[0])
			require.Zero(t, gh.checkAttempts)
		})
	}
}

func TestMarkPipelineGateOwnership(t *testing.T) {
	for _, scenario := range []string{"missing", "running", "rerun", "other-app", "other-pr", "other-base", "other-head", "strict-missing"} {
		t.Run(scenario, func(t *testing.T) {
			refs := makeTriggerPJ(strings.Repeat("a", 40)).Spec.Refs
			gate := traditionalCheck(refs, "in_progress", "", "running dispatch")
			gate.ID, gate.App.ID = 1, 101
			switch scenario {
			case "rerun":
				gate.Output.Title = traditionalRerunCheckTitle
			case "other-app":
				gate.App.ID = 202
			case "other-pr":
				gate.ExternalID = "pipeline-controller:other/repo#9:" + refs.BaseRef
			case "other-base":
				gate.ExternalID += "-old"
			case "other-head":
				gate.HeadSHA = strings.Repeat("b", 40)
			}
			gh := &fakeAgenticGitHub{}
			if scenario != "missing" && scenario != "strict-missing" {
				gh.checks = []github.CheckRun{gate}
			}
			checks := &dispatchChecks{gh: gh, appID: 101}
			marked, err := checks.markGateCheck(refs, 500, "", scenario != "strict-missing")
			if scenario == "other-pr" || scenario == "other-base" || scenario == "strict-missing" {
				require.Error(t, err)
				require.Zero(t, gh.checkAttempts)
			} else {
				require.NoError(t, err)
				require.Equal(t, "completed", marked.Status)
				require.Equal(t, "success", marked.Conclusion)
				require.Equal(t, refs.Pulls[0].SHA, marked.HeadSHA)
				require.Equal(t, pipelineExternalID(refs), marked.ExternalID)
				require.Equal(t, int64(101), marked.App.ID)
				require.NotEmpty(t, marked.Output.Title)
				if scenario == "running" || scenario == "rerun" {
					require.Len(t, gh.checks, 1)
					require.Equal(t, gate.Output.Title, marked.Output.Title)
				}
				if scenario == "other-app" || scenario == "other-head" {
					require.Equal(t, gate, gh.checks[0], "leave other apps and revisions unchanged")
					require.Len(t, gh.checks, 2)
				}
			}
			require.Zero(t, gh.statusWrites)
			require.Empty(t, gh.comments)
		})
	}
}
