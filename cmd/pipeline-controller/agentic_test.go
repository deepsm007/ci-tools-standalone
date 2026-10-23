package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/prow/pkg/config"
	"sigs.k8s.io/prow/pkg/flagutil"
	"sigs.k8s.io/prow/pkg/github"
	"sigs.k8s.io/prow/pkg/labels"
)

type fakeAgenticGitHub struct {
	pr                    github.PullRequest
	comments              []github.IssueComment
	checks                []github.CheckRun
	changes               []github.PullRequestChange
	member                bool
	collaborator          bool
	statusWrites          int
	statusUpdates         []github.Status
	failStatus            bool
	failCheck             bool
	failCheckAfterCreate  bool
	failCheckAfterUpdate  bool
	failCommentAfterWrite bool
	failLabel             bool
	failLabelAfterWrite   bool
	checkAttempts         int
	getPullRequestCalls   int
	getPullRequestError   error
	beforeComment         func(string)
	afterCheckWrite       func(github.CheckRun)
}

func (f *fakeAgenticGitHub) GetPullRequest(_, _ string, _ int) (*github.PullRequest, error) {
	f.getPullRequestCalls++
	copy := f.pr
	copy.Labels = append([]github.Label{}, f.pr.Labels...)
	return &copy, f.getPullRequestError
}
func (f *fakeAgenticGitHub) CreateComment(_, _ string, _ int, body string) error {
	if f.beforeComment != nil {
		f.beforeComment(body)
	}
	f.comments = append(f.comments, github.IssueComment{Body: body})
	if f.failCommentAfterWrite {
		f.failCommentAfterWrite = false
		return fmt.Errorf("response lost: %w", io.ErrUnexpectedEOF)
	}
	return nil
}
func (f *fakeAgenticGitHub) GetPullRequestChanges(_, _ string, _ int) ([]github.PullRequestChange, error) {
	return f.changes, nil
}
func (f *fakeAgenticGitHub) CreateStatus(_, _, _ string, status github.Status) error {
	f.statusWrites++
	if f.failStatus {
		return io.ErrUnexpectedEOF
	}
	f.statusUpdates = append(f.statusUpdates, status)
	return nil
}
func (f *fakeAgenticGitHub) AddLabel(_, _ string, _ int, label string) error {
	if f.failLabel {
		return io.ErrUnexpectedEOF
	}
	f.pr.Labels = append(f.pr.Labels, github.Label{Name: label})
	if f.failLabelAfterWrite {
		f.failLabelAfterWrite = false
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (f *fakeAgenticGitHub) RemoveLabel(_, _ string, _ int, label string) error {
	var kept []github.Label
	for _, current := range f.pr.Labels {
		if current.Name != label {
			kept = append(kept, current)
		}
	}
	f.pr.Labels = kept
	return nil
}
func (f *fakeAgenticGitHub) GetIssueLabels(_, _ string, _ int) ([]github.Label, error) {
	return f.pr.Labels, nil
}
func (f *fakeAgenticGitHub) ListCheckRuns(_, _, sha string) (*github.CheckRunList, error) {
	result := &github.CheckRunList{}
	for _, check := range f.checks {
		if check.HeadSHA == sha {
			result.CheckRuns = append(result.CheckRuns, check)
		}
	}
	return result, nil
}
func (f *fakeAgenticGitHub) CreateCheckRun(_, _ string, check github.CheckRun) (int64, error) {
	f.checkAttempts++
	if f.failCheck {
		return 0, io.ErrUnexpectedEOF
	}
	check.ID, check.App.ID = int64(len(f.checks)+1), 101
	f.checks = append(f.checks, check)
	if f.afterCheckWrite != nil {
		f.afterCheckWrite(check)
	}
	if f.failCheckAfterCreate {
		f.failCheckAfterCreate = false
		return 0, io.ErrUnexpectedEOF
	}
	return check.ID, nil
}
func (f *fakeAgenticGitHub) UpdateCheckRun(_, _ string, id int64, check github.CheckRun) error {
	f.checkAttempts++
	if f.failCheck {
		return io.ErrUnexpectedEOF
	}
	for i := range f.checks {
		if f.checks[i].ID != id {
			continue
		}
		old := f.checks[i]
		if check.ExternalID != "" {
			old.ExternalID = check.ExternalID
		}
		if check.Name != "" {
			old.Name = check.Name
		}
		if check.Status != "" {
			old.Status = check.Status
		}
		if check.Conclusion != "" {
			old.Conclusion = check.Conclusion // PATCH omits empty conclusions.
		}
		old.Output = check.Output
		f.checks[i] = old
		if f.afterCheckWrite != nil {
			f.afterCheckWrite(old)
		}
		if f.failCheckAfterUpdate {
			f.failCheckAfterUpdate = false
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	return errors.New("unknown check")
}
func (f *fakeAgenticGitHub) IsMember(_, _ string) (bool, error) { return f.member, nil }
func (f *fakeAgenticGitHub) IsCollaborator(_, _, _ string) (bool, error) {
	return f.collaborator, nil
}

type agenticFixture struct {
	a    *agenticController
	gh   *fakeAgenticGitHub
	cfg  *config.Config
	now  time.Time
	work agenticWork
}

func newAgenticFixture(t *testing.T, mode string) *agenticFixture {
	t.Helper()
	f := &agenticFixture{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC), work: agenticWork{org: "org", repo: "repo", number: 42}}
	f.gh = &fakeAgenticGitHub{member: true, pr: github.PullRequest{Number: 42, State: github.PullRequestStateOpen,
		Head: github.PullRequestBranch{SHA: strings.Repeat("a", 40)},
		Base: github.PullRequestBranch{Ref: "main", Repo: github.Repo{Name: "repo", Owner: github.User{Login: "org"}}}}}
	static := []config.Presubmit{
		{JobBase: config.JobBase{Name: "pull-ci-org-repo-main-unit"}, AlwaysRun: true, Reporter: config.Reporter{Context: "ci/unit"}},
		{JobBase: config.JobBase{Name: "pull-ci-org-repo-main-e2e"}, Reporter: config.Reporter{Context: "ci/e2e"}, RerunCommand: "/test e2e"},
	}
	f.cfg = &config.Config{}
	require.NoError(t, f.cfg.SetPresubmits(map[string][]config.Presubmit{"org/repo": static}))
	var enrollment enabledConfig
	require.NoError(t, yaml.Unmarshal([]byte(fmt.Sprintf("orgs:\n- org: org\n  repos:\n  - name: repo\n    branches: [main]\n    mode: {trigger: %s, agentic: {mode: chai}}\n", mode)), &enrollment))
	w, lgtm := &watcher{config: enrollment}, &watcher{}
	if mode == "lgtm" {
		w, lgtm = lgtm, w
	}
	logger := logrus.NewEntry(logrus.New())
	f.a = &agenticController{gh: f.gh, reader: newFakePJLister(), watcher: w, lgtmWatcher: lgtm, logger: logger,
		configDataProvider: NewConfigDataProvider(func() *config.Config { return f.cfg }, func() []string { return []string{"org/repo"} }, logger),
		checks:             &dispatchChecks{gh: f.gh, appID: 101}, appID: 101, now: func() time.Time { return f.now },
		options: agenticOptions{timeout: defaultAgenticTimeout, trustedAuthors: flagutil.NewStrings("chai[bot]")}}
	return f
}

func (f *agenticFixture) passFirstStage() {
	pj := makeProwJob("pull-ci-org-repo-"+f.gh.pr.Base.Ref+"-unit", f.gh.pr.Head.SHA)
	pj.Spec.Refs = pullRefs(f.work.org, f.work.repo, &f.gh.pr)
	pj.Labels["prow.k8s.io/refs.org"], pj.Labels["prow.k8s.io/refs.repo"] = f.work.org, f.work.repo
	pj.Labels["prow.k8s.io/refs.base_ref"] = f.gh.pr.Base.Ref
	f.a.reader = newFakePJLister(pj)
}

func (f *agenticFixture) command(id int, body string) *github.IssueComment {
	return &github.IssueComment{ID: id, Body: "/pipeline " + body, User: github.User{Login: "maintainer"}, CreatedAt: f.now}
}

func (f *agenticFixture) reconcile(t *testing.T, comment *github.IssueComment) {
	t.Helper()
	f.a.mu.Lock()
	defer f.a.mu.Unlock()
	require.NoError(t, f.a.reconcilePull(context.Background(), f.work, comment))
}

func (f *agenticFixture) state() *agenticState { return f.a.states[f.work] }

// TestAgenticLGTMReadinessWaitsForApprovalAndFirstStage covers both arrival orders of the readiness prerequisites.
func TestAgenticLGTMReadinessWaitsForApprovalAndFirstStage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		approvalFirst bool
	}{
		{name: "first-stage-success-before-approval"},
		{name: "approval-before-first-stage-success", approvalFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgenticFixture(t, "lgtm")
			if tc.approvalFirst {
				f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			}
			if !tc.approvalFirst {
				f.passFirstStage()
			}
			f.reconcile(t, nil)
			f.now = f.now.Add(2 * f.a.options.timeout)
			f.reconcile(t, nil)
			if diff := cmp.Diff(map[string]any{
				"checks":               0,
				"comments":             0,
				"status writes":        0,
				"ready":                false,
				"timed out":            false,
				"no response deadline": true,
			}, map[string]any{
				"checks":               len(f.gh.checks),
				"comments":             len(f.gh.comments),
				"status writes":        f.gh.statusWrites,
				"ready":                f.state().ready,
				"timed out":            f.state().timedOut,
				"no response deadline": f.state().waitingUntil.IsZero(),
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if tc.approvalFirst {
				f.passFirstStage()
			}
			if !tc.approvalFirst {
				f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			}
			f.reconcile(t, nil)
			if diff := cmp.Diff(1, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(map[string]any{
				"gate title":        agenticCheckTitle,
				"gate status":       "queued",
				"response deadline": f.now.Add(f.a.options.timeout),
			}, map[string]any{
				"gate title":        f.gh.checks[0].Output.Title,
				"gate status":       f.gh.checks[0].Status,
				"response deadline": f.state().waitingUntil,
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			f.reconcile(t, nil)
			if diff := cmp.Diff(1, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
		})
	}
}

// TestAgenticLGTMLabelEventUsesCurrentApproval ensures queued events use live approval metadata.
func TestAgenticLGTMLabelEventUsesCurrentApproval(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		currentLGTM, eventLGTM bool
		wantChecks             int
	}{
		{name: "current-approval-wakes-readiness", currentLGTM: true, wantChecks: 1},
		{name: "absent-approval-waits"},
		{name: "delayed-label-event-cannot-replace-current-approval", eventLGTM: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgenticFixture(t, "lgtm")
			f.passFirstStage()
			f.a.queue = workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[agenticWork](time.Second, time.Minute))
			defer f.a.queue.ShutDown()
			event := github.PullRequestEvent{Action: github.PullRequestActionLabeled, Label: github.Label{Name: labels.LGTM},
				Repo: f.gh.pr.Base.Repo, PullRequest: f.gh.pr}
			if tc.eventLGTM {
				event.PullRequest.Labels = []github.Label{{Name: labels.LGTM}}
			}
			if tc.currentLGTM {
				f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			}
			f.a.handlePullRequest(f.a.logger, event)
			if diff := cmp.Diff(1, len(f.a.inputs)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(true, f.a.processNext(context.Background())); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(tc.wantChecks, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(tc.wantChecks == 0, f.state().waitingUntil.IsZero()); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
		})
	}
}

// TestAgenticLGTMAutoOverridePublishesReadiness covers command and persisted-label overrides before LGTM.
func TestAgenticLGTMAutoOverridePublishesReadiness(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command bool
	}{
		{name: "explicit-command", command: true},
		{name: "persisted-label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgenticFixture(t, "lgtm")
			f.passFirstStage()
			f.reconcile(t, nil)
			if diff := cmp.Diff(0, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			var command *github.IssueComment
			if tc.command {
				command = f.command(100, "auto")
			}
			if !tc.command {
				f.gh.pr.Labels = []github.Label{{Name: PipelineAutoLabel}}
			}
			f.reconcile(t, command)
			if diff := cmp.Diff(1, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(map[string]any{
				"auto override":        true,
				"LGTM approval":        false,
				"gate title":           agenticCheckTitle,
				"no response deadline": false,
				"comments":             0,
			}, map[string]any{
				"auto override":        hasAgenticLabel(&f.gh.pr, PipelineAutoLabel),
				"LGTM approval":        hasAgenticLabel(&f.gh.pr, labels.LGTM),
				"gate title":           f.gh.checks[0].Output.Title,
				"no response deadline": f.state().waitingUntil.IsZero(),
				"comments":             len(f.gh.comments),
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
		})
	}
}

// TestAgenticLGTMOptOutWaitsForApproval keeps opted-out pipelines from creating gates before LGTM.
func TestAgenticLGTMOptOutWaitsForApproval(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command bool
	}{
		{name: "explicit-command", command: true},
		{name: "persisted-label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgenticFixture(t, "lgtm")
			f.passFirstStage()
			var command *github.IssueComment
			if tc.command {
				command = f.command(100, "skip-agentic-mode")
			}
			if !tc.command {
				f.gh.pr.Labels = []github.Label{{Name: agenticSkipLabel}}
			}
			f.reconcile(t, command)
			f.now = f.now.Add(2 * f.a.options.timeout)
			f.reconcile(t, nil)
			if diff := cmp.Diff(map[string]any{
				"opt out":              true,
				"checks":               0,
				"status writes":        0,
				"no response deadline": true,
			}, map[string]any{
				"opt out":              f.state().optOut,
				"checks":               len(f.gh.checks),
				"status writes":        f.gh.statusWrites,
				"no response deadline": f.state().waitingUntil.IsZero(),
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			f.gh.pr.Labels = append(f.gh.pr.Labels, github.Label{Name: labels.LGTM})
			f.reconcile(t, nil)
			if diff := cmp.Diff(1, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(map[string]any{
				"gate conclusion": "success",
				"gate title":      traditionalCheckTitle,
			}, map[string]any{
				"gate conclusion": f.gh.checks[0].Conclusion,
				"gate title":      f.gh.checks[0].Output.Title,
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
		})
	}
}

// TestAgenticLGTMRemovalPreservesGateButNewRevisionWaits preserves current ownership while requiring fresh approval after a revision change.
func TestAgenticLGTMRemovalPreservesGateButNewRevisionWaits(t *testing.T) {
	for _, tc := range []struct {
		name, newHead, newBase string
	}{
		{name: "new-head-requires-current-approval", newHead: strings.Repeat("b", 40)},
		{name: "new-base-requires-current-approval", newBase: "release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAgenticFixture(t, "lgtm")
			f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			f.passFirstStage()
			f.reconcile(t, nil)
			gate, deadline := f.gh.checks[0], f.state().waitingUntil
			f.gh.pr.Labels = nil
			f.reconcile(t, nil)
			if diff := cmp.Diff(map[string]any{
				"gate":              gate,
				"response deadline": deadline,
			}, map[string]any{
				"gate":              f.gh.checks[0],
				"response deadline": f.state().waitingUntil,
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			mark := f.command(100, "mark-pipeline-gate "+f.gh.pr.Head.SHA)
			mark.User.Login = "chai[bot]"
			f.reconcile(t, mark)
			if diff := cmp.Diff("success", f.gh.checks[0].Conclusion); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			f.a.states = nil
			f.reconcile(t, nil)
			if diff := cmp.Diff(true, f.state().dispatched); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if tc.newBase != "" {
				f.gh.pr.Base.Ref = tc.newBase
				f.a.lgtmWatcher.config.Orgs[0].Repos[0].Branches = append(f.a.lgtmWatcher.config.Orgs[0].Repos[0].Branches, tc.newBase)
				presubmits := f.cfg.GetPresubmitsStatic("org/repo")
				for i := range presubmits {
					presubmits[i].Name = strings.Replace(presubmits[i].Name, "-main-", "-"+tc.newBase+"-", 1)
				}
				require.NoError(t, f.cfg.SetPresubmits(map[string][]config.Presubmit{"org/repo": presubmits}))
				f.a.configDataProvider.gatherData()
			}
			if tc.newHead != "" {
				f.gh.pr.Head.SHA = tc.newHead
			}
			f.passFirstStage()
			f.reconcile(t, nil)
			if diff := cmp.Diff(map[string]any{
				"checks":               1,
				"ready":                false,
				"no response deadline": true,
			}, map[string]any{
				"checks":               len(f.gh.checks),
				"ready":                f.state().ready,
				"no response deadline": f.state().waitingUntil.IsZero(),
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			f.reconcile(t, nil)
			if diff := cmp.Diff(2, len(f.gh.checks)); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(map[string]any{
				"new gate head": f.gh.pr.Head.SHA,
			}, map[string]any{
				"new gate head": f.gh.checks[1].HeadSHA,
			}); diff != "" {
				t.Errorf("unexpected result (-want +got):\n%s", diff)
				return
			}
		})
	}
}

func TestAgenticReadinessAndCompletion(t *testing.T) {
	for _, mode := range []string{"auto", "manual", "lgtm"} {
		t.Run(mode, func(t *testing.T) {
			f := newAgenticFixture(t, mode)
			if mode == "lgtm" {
				f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			}
			f.reconcile(t, nil)
			require.Empty(t, f.gh.checks, "first-stage success must precede CheckRun creation")
			f.passFirstStage()
			f.reconcile(t, &github.IssueComment{ID: 90, Body: "Chai has not seen readiness yet.",
				User: github.User{Login: "chai[bot]"}, CreatedAt: f.now})
			if mode == "manual" {
				f.reconcile(t, f.command(95, "required"))
			}
			require.Len(t, f.gh.checks, 1)
			require.Equal(t, pipelineGate, f.gh.checks[0].Name)
			require.Equal(t, "queued", f.gh.checks[0].Status)
			deadline := f.state().waitingUntil
			require.False(t, deadline.IsZero(), "a comment before readiness must not cancel the new timer")
			f.now = f.now.Add(time.Minute)
			f.reconcile(t, nil)
			require.Equal(t, deadline, f.state().waitingUntil)
			ack := &github.IssueComment{ID: 100, Body: "Chai is reviewing this PR.",
				User: github.User{Login: "chai[bot]"}, CreatedAt: f.now}
			f.reconcile(t, ack)
			require.True(t, f.state().waitingUntil.IsZero())
			require.Equal(t, "queued", f.gh.checks[0].Status)
			f.now = f.now.Add(time.Hour)
			f.reconcile(t, nil)
			require.Empty(t, f.gh.comments)
			require.Zero(t, f.gh.statusWrites, "agentic handoff must not publish job contexts")
			mark := f.command(200, "mark-pipeline-gate "+f.gh.pr.Head.SHA)
			mark.User = ack.User
			f.gh.member = false // Configured Chai need not be an organization member.
			f.reconcile(t, mark)
			require.Equal(t, "success", f.gh.checks[0].Conclusion)
			require.Len(t, f.gh.checks, 1)
			require.Empty(t, f.gh.comments)
		})
	}
}

func TestAgenticChaiCommentResponse(t *testing.T) {
	for _, scenario := range []string{"plain", "help", "human", "edited", "old", "late"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(1, "required"))
			deadline, gate, writes := f.state().waitingUntil, f.gh.checks[0], f.gh.checkAttempts
			f.now = f.now.Add(time.Minute)
			comment := github.IssueComment{ID: 100, Body: "Looking at this PR.", User: github.User{Login: "chai[bot]"}, CreatedAt: f.now}
			event := github.IssueCommentEvent{Action: github.IssueCommentActionCreated, Repo: f.gh.pr.Base.Repo,
				Issue: github.Issue{Number: f.work.number, PullRequest: &struct{}{}}, Comment: comment}
			switch scenario {
			case "help":
				event.Comment.Body = "/pipeline help"
			case "human":
				event.Comment.User.Login = "maintainer"
			case "edited":
				event.Action = github.IssueCommentActionEdited
			case "old":
				event.Comment.CreatedAt = f.state().publishedAt.Add(-time.Second)
			case "late":
				f.now = deadline.Add(time.Second)
				event.Comment.CreatedAt = f.now
			}
			f.a.queue = workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[agenticWork](time.Second, time.Minute))
			defer f.a.queue.ShutDown()
			cw := &clientWrapper{ghc: f.gh, agentic: f.a}
			cw.handleIssueComment(f.a.logger, event)
			if len(f.a.inputs) != 0 {
				require.True(t, f.a.processNext(context.Background()))
			}
			switch scenario {
			case "plain", "help":
				require.True(t, f.state().waitingUntil.IsZero())
			case "late":
				require.True(t, f.state().timedOut, "a late comment cannot cancel fallback even before timer delivery")
			default:
				require.Equal(t, deadline, f.state().waitingUntil)
			}
			require.Equal(t, gate, f.gh.checks[0], "a response must leave the check pending and need no GitHub write")
			require.Equal(t, writes, f.gh.checkAttempts)
			require.Zero(t, f.gh.statusWrites)
		})
	}
}

func TestAgenticResponseTimeoutUsesTraditionalTrigger(t *testing.T) {
	for _, mode := range []string{"auto", "manual", "lgtm", "lgtm-approved"} {
		t.Run(mode, func(t *testing.T) {
			trigger := strings.TrimSuffix(mode, "-approved")
			f := newAgenticFixture(t, trigger)
			if trigger == "lgtm" {
				f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
			}
			f.passFirstStage()
			if trigger == "manual" {
				f.reconcile(t, f.command(1, "required"))
			}
			f.reconcile(t, nil)
			f.now = f.state().waitingUntil
			if mode == "lgtm" {
				f.gh.pr.Labels = nil // Fallback must wait if approval was removed after handoff.
			}
			f.gh.beforeComment = func(body string) {
				if strings.Contains(body, "/test e2e") {
					require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
					require.Equal(t, []github.Status{{Context: "ci/e2e", State: "pending", Description: PipelinePendingMessage}}, f.gh.statusUpdates)
				}
			}
			f.reconcile(t, nil)
			require.True(t, f.state().timedOut)
			if mode == "auto" || mode == "lgtm-approved" {
				require.Len(t, f.gh.comments, 1)
				require.Contains(t, f.gh.comments[0].Body, "/test e2e")
				require.Equal(t, "success", f.gh.checks[0].Conclusion)
				f.reconcile(t, nil)
				require.Len(t, f.gh.comments, 1, "fallback must dispatch once")
			} else {
				require.Empty(t, f.gh.comments)
				require.Equal(t, "queued", f.gh.checks[0].Status)
				if mode == "lgtm" {
					f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
					f.reconcile(t, nil)
					require.Equal(t, "success", f.gh.checks[0].Conclusion)
				}
			}
			late := f.command(100, "mark-pipeline-gate "+f.gh.pr.Head.SHA)
			late.User = github.User{Login: "chai[bot]"}
			f.reconcile(t, late)
			require.True(t, f.state().timedOut, "late Chai messages cannot reclaim dispatch")
		})
	}
}

func TestAgenticModeSwitchAndIdempotence(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	deadline := f.state().waitingUntil
	f.reconcile(t, f.command(100, "agentic-mode"))
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, deadline, f.state().waitingUntil)
	f.reconcile(t, f.command(200, "skip-agentic-mode"))
	require.True(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
	require.True(t, f.state().optOut)
	require.True(t, f.state().waitingUntil.IsZero())
	f.reconcile(t, f.command(300, "mark-pipeline-gate"))
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	f.reconcile(t, f.command(400, "agentic-mode"))
	require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
	require.False(t, f.state().optOut)
	require.Len(t, f.gh.checks, 1, "completed dispatch must not emit another readiness signal")
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	deadline = f.state().waitingUntil
	f.now = f.now.Add(time.Minute)
	f.reconcile(t, f.command(500, "agentic-mode"))
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, deadline, f.state().waitingUntil)
}

func TestAgenticReenableCompletedDispatchPreservesRevision(t *testing.T) {
	for _, mode := range []string{"auto", "manual", "lgtm"} {
		for _, restart := range []bool{false, true} {
			for _, restore := range []string{"command", "label-removal"} {
				t.Run(fmt.Sprintf("%s/restart=%t/%s", mode, restart, restore), func(t *testing.T) {
					f := newAgenticFixture(t, mode)
					if mode == "lgtm" {
						f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
					}
					f.passFirstStage()
					f.reconcile(t, nil)
					f.reconcile(t, f.command(100, "skip-agentic-mode"))
					if !f.state().dispatched {
						f.reconcile(t, f.command(200, "remaining"))
					}
					require.Equal(t, "success", f.gh.checks[0].Conclusion)
					completed := f.gh.checks[0]
					attempts, statuses := f.gh.checkAttempts, f.gh.statusWrites
					if restart {
						f.a.states = nil
					}
					if restore == "command" {
						f.reconcile(t, f.command(300, "agentic-mode"))
						require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "next HEAD SHA")
					} else {
						f.gh.pr.Labels = nil
						f.reconcile(t, nil)
					}
					require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
					require.False(t, f.state().optOut)
					require.True(t, f.state().dispatched)
					require.True(t, f.state().ready)
					require.True(t, f.state().waitingUntil.IsZero())
					require.Equal(t, []github.CheckRun{completed}, f.gh.checks)
					require.Equal(t, attempts, f.gh.checkAttempts)
					require.Equal(t, statuses, f.gh.statusWrites)
					comments := len(f.gh.comments)
					f.a.states = nil
					f.now = f.now.Add(2 * f.a.options.timeout)
					f.reconcile(t, nil)
					require.Equal(t, []github.CheckRun{completed}, f.gh.checks, "restart must preserve the completed revision")
					require.Len(t, f.gh.comments, comments, "no redispatch or response timeout for this HEAD")
					if mode == "lgtm" {
						f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
					}
					f.gh.pr.Head.SHA = strings.Repeat("b", 40)
					f.a.reader = newFakePJLister()
					f.reconcile(t, nil)
					require.Len(t, f.gh.checks, 1, "next SHA still waits for first-stage success")
					f.passFirstStage()
					if mode == "manual" {
						f.reconcile(t, f.command(400, "required"))
					}
					f.reconcile(t, nil)
					require.Len(t, f.gh.checks, 2)
					require.Equal(t, completed, f.gh.checks[0])
					require.Equal(t, f.gh.pr.Head.SHA, f.gh.checks[1].HeadSHA)
					require.Equal(t, "queued", f.gh.checks[1].Status)
					require.Equal(t, agenticCheckTitle, f.gh.checks[1].Output.Title)
				})
			}
		}
	}
}

func TestAgenticReenableCompletedDispatchReplyRetry(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.passFirstStage()
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	completed := f.gh.checks[0]
	f.gh.failCommentAfterWrite = true
	command := f.command(200, "agentic-mode")
	require.NoError(t, f.a.reconcilePull(context.Background(), f.work, command), "confirmation is best-effort after a completed mutation")
	f.reconcile(t, command)
	require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
	require.Equal(t, []github.CheckRun{completed}, f.gh.checks)
	require.True(t, f.state().waitingUntil.IsZero())
}

func TestAgenticDeferredRestoreRetainsTraditionalCommands(t *testing.T) {
	for _, mode := range []string{"auto", "manual", "lgtm"} {
		for _, origin := range []string{"skip", "timeout", "timeout-mark"} {
			for _, restart := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/restart=%t", mode, origin, restart), func(t *testing.T) {
					f := newAgenticFixture(t, mode)
					if mode == "lgtm" {
						f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}}
					}
					f.passFirstStage()
					if origin == "skip" {
						f.reconcile(t, f.command(100, "skip-agentic-mode"))
					} else {
						if mode == "manual" {
							f.reconcile(t, f.command(1, "required"))
						}
						f.reconcile(t, nil)
						f.now = f.state().waitingUntil
						f.reconcile(t, nil)
					}
					if !f.state().dispatched {
						command := "remaining"
						if origin == "timeout-mark" {
							command = "mark-pipeline-gate"
						}
						f.reconcile(t, f.command(200, command))
					}
					completed := f.gh.checks[0]
					require.Equal(t, traditionalCheckTitle, completed.Output.Title, "traditional ownership must survive a controller restart")
					f.reconcile(t, f.command(300, "agentic-mode"))
					require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "next HEAD SHA")
					if restart {
						f.a.states = nil
					}
					f.reconcile(t, nil)
					require.True(t, f.state().traditionalMode())
					require.False(t, f.state().optOut)
					f.reconcile(t, f.command(400, "agentic-mode"))
					require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "next HEAD SHA")
					require.Equal(t, []github.CheckRun{completed}, f.gh.checks)
					statuses := f.gh.statusWrites
					f.reconcile(t, f.command(500, "remaining"))
					require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "already been scheduled")
					require.Equal(t, statuses, f.gh.statusWrites)
					chai := f.command(600, "mark-pipeline-gate "+f.gh.pr.Head.SHA)
					chai.User.Login = "chai[bot]"
					f.reconcile(t, chai)
					require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "traditional scheduling owns this revision")
					require.Equal(t, []github.CheckRun{completed}, f.gh.checks)
					f.reconcile(t, f.command(700, "required"))
					require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "/test e2e")
					require.Equal(t, statuses+1, f.gh.statusWrites)
					f.gh.pr.Head.SHA = strings.Repeat("b", 40)
					f.a.reader = newFakePJLister()
					f.reconcile(t, f.command(800, "required"))
					require.False(t, f.state().traditionalMode(), "the next revision returns to agentic ownership")
					require.Len(t, f.gh.checks, 2)
					require.Equal(t, agenticCheckTitle, f.gh.checks[1].Output.Title)
					require.False(t, f.state().waitingUntil.IsZero(), "an explicit command starts the next revision immediately")
				})
			}
		}
	}
}

func TestAgenticConfirmationFailureDoesNotReplayModeChange(t *testing.T) {
	for _, push := range []bool{false, true} {
		t.Run(fmt.Sprintf("push=%t", push), func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(1, "required"))
			skip := f.command(100, "skip-agentic-mode")
			f.gh.failCommentAfterWrite = true
			f.reconcile(t, skip)
			require.True(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
			require.Equal(t, skip.ID, f.state().lastCommand)
			f.gh.pr.Labels = nil
			if push {
				f.gh.pr.Head.SHA = strings.Repeat("b", 40)
				f.passFirstStage()
			}
			f.reconcile(t, skip)
			require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel), "a handled skip cannot overwrite a later manual label removal")
			require.False(t, f.state().traditionalMode())
			require.Len(t, f.gh.checks, 1, "manual restoration must wait for a new command")
			f.reconcile(t, f.command(200, "required"))
			require.Equal(t, agenticCheckTitle, f.gh.checks[len(f.gh.checks)-1].Output.Title)
		})
	}
}

func TestAgenticDeferredRestoreRerunFailurePreservesOwnershipAfterRestart(t *testing.T) {
	for _, failure := range []string{"comment", "completion"} {
		t.Run(failure, func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(100, "skip-agentic-mode"))
			f.reconcile(t, f.command(200, "remaining"))
			f.reconcile(t, f.command(300, "agentic-mode"))
			if failure == "comment" {
				f.gh.failCommentAfterWrite = true
			} else {
				f.gh.afterCheckWrite = func(check github.CheckRun) {
					if check.Status == "in_progress" {
						f.gh.failCheck = true
					}
				}
			}
			require.Error(t, f.a.reconcilePull(t.Context(), f.work, f.command(400, "required")))
			require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
			require.Equal(t, traditionalRerunCheckTitle, f.gh.checks[0].Output.Title)
			f.gh.failCheck, f.gh.afterCheckWrite = false, nil
			f.a.states = nil
			f.reconcile(t, nil)
			require.True(t, f.state().traditionalMode())
			require.Len(t, f.gh.checks, 1, "a rerun failure must not signal fresh agentic readiness for the same HEAD")
			require.True(t, f.state().waitingUntil.IsZero())
			f.reconcile(t, f.command(500, "required"))
			require.Equal(t, "success", f.gh.checks[0].Conclusion)
			require.Equal(t, traditionalRerunCheckTitle, f.gh.checks[0].Output.Title)
		})
	}
}

func TestAgenticIncompleteOptOutRemovalAcrossRestart(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	require.Equal(t, "queued", f.gh.checks[0].Status)
	require.Equal(t, traditionalCheckTitle, f.gh.checks[0].Output.Title)
	f.gh.pr.Labels = nil
	f.a.states = nil
	f.reconcile(t, nil)
	require.False(t, f.state().traditionalMode(), "removing an incomplete opt-out must restore agentic readiness, even after restart")
	require.Len(t, f.gh.checks, 1, "manual restoration must not create readiness automatically")
	require.True(t, f.state().waitingUntil.IsZero())
	f.reconcile(t, f.command(200, "required"))
	require.Len(t, f.gh.checks, 2)
	require.Equal(t, agenticCheckTitle, f.gh.checks[1].Output.Title)
	require.False(t, f.state().waitingUntil.IsZero())
}

func TestAgenticRestoreAfterTraditionalDispatchFailure(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, restore := range []string{"command", "label-removal"} {
			for _, failure := range []string{"comment", "completion"} {
				for _, restart := range []bool{false, true} {
					t.Run(fmt.Sprintf("historical=%t/%s/%s/restart=%t", historical, restore, failure, restart), func(t *testing.T) {
						f := newAgenticFixture(t, "manual")
						f.passFirstStage()
						f.reconcile(t, f.command(100, "skip-agentic-mode"))
						if historical {
							f.reconcile(t, f.command(200, "remaining"))
							if restore == "command" {
								f.reconcile(t, f.command(300, "agentic-mode"))
							}
						}
						if failure == "comment" {
							f.gh.failCommentAfterWrite = true
						} else {
							f.gh.afterCheckWrite = func(check github.CheckRun) {
								if check.Status == "in_progress" {
									f.gh.failCheck = true
								}
							}
						}
						require.Error(t, f.a.reconcilePull(t.Context(), f.work, f.command(400, "required")))
						gate := f.gh.checks[0]
						f.gh.failCheck, f.gh.afterCheckWrite = false, nil
						if restart {
							f.a.states = nil
						}
						if restore == "command" {
							f.reconcile(t, f.command(500, "agentic-mode"))
						} else {
							f.gh.pr.Labels = nil
							f.reconcile(t, nil)
						}
						if !historical {
							require.False(t, f.state().traditionalMode(), "an initial failed dispatch can restore agentic mode immediately")
							require.Len(t, f.gh.checks, 1, "restoring manual mode must wait for a handoff command")
							f.reconcile(t, f.command(600, "required"))
							require.Len(t, f.gh.checks, 2)
							require.Equal(t, agenticCheckTitle, f.gh.checks[1].Output.Title)
							return
						}
						require.True(t, f.state().traditionalMode(), "a failed rerun cannot erase earlier completed dispatch ownership")
						require.False(t, f.state().dispatched, "historical completion is not success of the latest rerun")
						require.False(t, f.state().ready)
						require.Equal(t, []github.CheckRun{gate}, f.gh.checks)
						require.Equal(t, traditionalRerunCheckTitle, gate.Output.Title)
						if restore == "command" {
							require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "rerun gate is unchanged")
							require.NotContains(t, f.gh.comments[len(f.gh.comments)-1].Body, "dispatch is already complete")
						}
						f.reconcile(t, f.command(600, "mark-pipeline-gate"))
						require.Equal(t, traditionalRerunCheckTitle, f.gh.checks[0].Output.Title, "human acknowledgment retains historical ownership")
						f.a.states = nil
						f.reconcile(t, nil)
						require.True(t, f.state().traditionalMode())
						f.gh.pr.Head.SHA = strings.Repeat("b", 40)
						f.a.reader = newFakePJLister()
						f.reconcile(t, nil)
						require.False(t, f.state().traditionalMode(), "historical ownership does not carry over to the next revision")
					})
				}
			}
		}
	}
}

func TestAgenticManualOptOutKeepsDurableCompletedOwner(t *testing.T) {
	for _, traditionalWrite := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("traditional-write=%t/restart=%t", traditionalWrite, restart), func(t *testing.T) {
				f := newAgenticFixture(t, "manual")
				f.passFirstStage()
				f.reconcile(t, f.command(1, "required"))
				f.reconcile(t, f.command(100, "mark-pipeline-gate"))
				completed := f.gh.checks[0]
				f.gh.pr.Labels = []github.Label{{Name: agenticSkipLabel}}
				f.reconcile(t, nil)
				if traditionalWrite {
					f.reconcile(t, f.command(200, "required"))
					completed = f.gh.checks[0]
				}
				f.gh.pr.Labels = nil
				if restart {
					f.a.states = nil
				}
				f.reconcile(t, nil)
				require.Equal(t, traditionalWrite, f.state().traditionalMode(), "removal restores the check's actual durable owner, not transient label history")
				require.True(t, f.state().dispatched)
				require.True(t, f.state().ready)
				require.Equal(t, []github.CheckRun{completed}, f.gh.checks, "removal cannot reset a completed gate or emit new readiness")
				f.a.states = nil
				f.reconcile(t, nil)
				require.Equal(t, traditionalWrite, f.state().traditionalMode())
			})
		}
	}
}

func TestAgenticHistoricalRerunRetargetClearsOwnership(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.a.watcher.config.Orgs[0].Repos[0].Branches = []string{"main", "release"}
			presubmits := f.a.configDataProvider.updatedPresubmits["org/repo"]
			presubmits.alwaysRequired = append(presubmits.alwaysRequired, config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-org-repo-release-unit"}, AlwaysRun: true})
			f.a.configDataProvider.updatedPresubmits["org/repo"] = presubmits
			f.passFirstStage()
			f.reconcile(t, f.command(100, "skip-agentic-mode"))
			f.reconcile(t, f.command(200, "remaining"))
			f.reconcile(t, f.command(300, "agentic-mode"))
			f.reconcile(t, f.command(400, "required"))
			require.Equal(t, traditionalRerunCheckTitle, f.gh.checks[0].Output.Title)
			f.gh.pr.Base.Ref = "release"
			f.a.reader = newFakePJLister()
			if restart {
				f.a.states = nil
			}
			f.reconcile(t, nil)
			require.False(t, f.state().traditionalMode(), "historical ownership cannot survive a change of target branch")
			require.Len(t, f.gh.checks, 1)
			require.Equal(t, "failure", f.gh.checks[0].Conclusion)
			f.passFirstStage()
			f.reconcile(t, nil)
			require.Len(t, f.gh.checks, 1, "retargeted manual mode still requires a command")
			f.reconcile(t, f.command(500, "required"))
			require.Len(t, f.gh.checks, 2)
			require.Equal(t, agenticCheckTitle, f.gh.checks[1].Output.Title)
			require.False(t, f.state().traditionalMode())
		})
	}
}

func TestAgenticMutationFailureRemainsRetryable(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	f.reconcile(t, f.command(100, "mark-pipeline-gate"))
	f.gh.failLabel = true
	skip := f.command(200, "skip-agentic-mode")
	require.Error(t, f.a.reconcilePull(context.Background(), f.work, skip))
	require.Equal(t, 100, f.state().lastCommand)
	require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
	require.NotEqual(t, "success", f.gh.checks[0].Conclusion, "failed ownership mutation must stay fail-closed")
	f.gh.failLabel = false
	f.reconcile(t, skip)
	require.True(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
	require.Equal(t, skip.ID, f.state().lastCommand)
}

func TestAgenticSharedSHAOwnershipFailsClosed(t *testing.T) {
	for _, mode := range []string{"pending", "agentic-complete", "traditional-complete"} {
		t.Run(mode, func(t *testing.T) {
			f := newAgenticFixture(t, "auto")
			f.passFirstStage()
			f.reconcile(t, nil)
			switch mode {
			case "agentic-complete":
				f.reconcile(t, f.command(100, "mark-pipeline-gate"))
			case "traditional-complete":
				f.reconcile(t, f.command(100, "skip-agentic-mode"))
			}
			gate, attempts, statuses := f.gh.checks[0], f.gh.checkAttempts, f.gh.statusWrites
			f.gh.pr.Number, f.work.number = 43, 43
			f.gh.pr.Labels = nil
			f.passFirstStage()
			err := f.a.reconcilePull(t.Context(), f.work, nil)
			require.ErrorContains(t, err, "different PR", "commit-scoped gates cannot safely be reused for another PR")
			require.Equal(t, []github.CheckRun{gate}, f.gh.checks)
			require.Equal(t, attempts, f.gh.checkAttempts)
			require.Equal(t, statuses, f.gh.statusWrites)
		})
	}
}

func TestAgenticSkipInvalidatesCompletionBeforeTraditionalDispatch(t *testing.T) {
	for _, scenario := range []string{"manual", "failed-plan", "lost-label-response"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(1, "required"))
			f.reconcile(t, f.command(100, "mark-pipeline-gate"))
			f.gh.failLabelAfterWrite = scenario == "lost-label-response"
			skip := f.command(200, "skip-agentic-mode")
			if f.gh.failLabelAfterWrite {
				require.Error(t, f.a.reconcilePull(context.Background(), f.work, skip))
				require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
			}
			f.reconcile(t, skip)
			require.True(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
			require.Len(t, f.gh.checks, 1, "skip must PATCH, not create a Chai readiness signal")
			require.Equal(t, "queued", f.gh.checks[0].Status)
			require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
			require.Zero(t, f.gh.statusWrites, "manual fallback waits for a traditional command")
			if scenario == "failed-plan" {
				f.a.reader = errorLister{}
				require.Error(t, f.a.reconcilePull(context.Background(), f.work, f.command(300, "remaining")))
				require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
				return
			}
			f.reconcile(t, f.command(300, "required"))
			require.Equal(t, "success", f.gh.checks[0].Conclusion)
			require.Contains(t, f.gh.comments[len(f.gh.comments)-1].Body, "/test e2e")
		})
	}
}

func TestAgenticTraditionalCompletionSurvivesSkipAndRestart(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.passFirstStage()
	f.reconcile(t, nil)
	f.now = f.state().waitingUntil
	f.reconcile(t, nil)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	attempts, statuses := f.gh.checkAttempts, f.gh.statusWrites
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	f.a.states = nil
	f.reconcile(t, nil)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	require.Equal(t, attempts, f.gh.checkAttempts, "already-traditional completion must not be reset")
	require.Equal(t, statuses, f.gh.statusWrites, "restart must reuse native dispatch completion")
}

func TestAgenticFallbackHumanMarkDoesNotDispatch(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	f.passFirstStage()
	f.reconcile(t, f.command(200, "mark-pipeline-gate"))
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	require.Zero(t, f.gh.statusWrites, "marking completion must not start tests")
	f.a.states = nil
	f.reconcile(t, nil)
	require.Zero(t, f.gh.statusWrites, "native completion survives memory loss")
}

func TestAgenticCommandRevisionAndTrust(t *testing.T) {
	for _, scenario := range []string{"foreign-head", "untrusted", "early", "optional-head", "explicit-head"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAgenticFixture(t, "auto")
			if scenario != "early" {
				f.passFirstStage()
			}
			f.reconcile(t, nil)
			command := f.command(100, "mark-pipeline-gate")
			switch scenario {
			case "foreign-head":
				command.Body += " " + strings.Repeat("b", 40)
			case "untrusted":
				f.gh.member = false
			case "explicit-head":
				command.Body += " " + f.gh.pr.Head.SHA
			}
			f.reconcile(t, command)
			switch scenario {
			case "early", "optional-head", "explicit-head":
				require.Equal(t, "success", f.gh.checks[0].Conclusion)
			default:
				require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
				require.False(t, f.state().waitingUntil.IsZero())
			}
			require.Zero(t, f.gh.statusWrites)
		})
	}
}

func TestAgenticPushAndRestart(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.passFirstStage()
	f.reconcile(t, nil)
	f.reconcile(t, f.command(100, "mark-pipeline-gate"))
	f.a.states = nil // Restart: completion remains GitHub's native check.
	f.reconcile(t, nil)
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	require.True(t, f.state().waitingUntil.IsZero())
	oldHead := f.gh.pr.Head.SHA
	f.gh.pr.Head.SHA = strings.Repeat("b", 40)
	f.reconcile(t, nil)
	require.Len(t, f.gh.checks, 1)
	f.reconcile(t, f.command(200, "mark-pipeline-gate "+oldHead))
	f.passFirstStage()
	f.reconcile(t, nil)
	require.Len(t, f.gh.checks, 2)
	require.Equal(t, "queued", f.gh.checks[1].Status)
	f.reconcile(t, f.command(300, "mark-pipeline-gate"))
	require.Equal(t, "success", f.gh.checks[1].Conclusion)
}

func TestAgenticFailedPublicationAndFallbackRetry(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.passFirstStage()
	f.gh.failCheck = true
	require.Error(t, f.a.reconcilePull(context.Background(), f.work, nil))
	require.Empty(t, f.gh.checks)
	require.True(t, f.state().waitingUntil.IsZero(), "timeout starts only after successful publication")
	f.gh.failCheck = false
	f.reconcile(t, nil)
	f.now = f.state().waitingUntil
	f.gh.failStatus = true
	require.Error(t, f.a.reconcilePull(context.Background(), f.work, nil))
	require.Empty(t, f.gh.comments)
	require.NotEqual(t, "success", f.gh.checks[0].Conclusion)
	f.gh.failStatus = false
	f.reconcile(t, nil)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
}

// TestAgenticManualCommandsPublishReadinessOnce covers command-only manual handoff and reuse.
func TestAgenticManualCommandsPublishReadinessOnce(t *testing.T) {
	for _, command := range []string{"required", "remaining"} {
		t.Run(command, func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.gh.pr.Labels = []github.Label{{Name: labels.LGTM}, {Name: PipelineAutoLabel}}
			f.reconcile(t, nil)
			require.Empty(t, f.gh.checks, "first-stage success and approval must not start manual handoff")
			require.True(t, f.state().waitingUntil.IsZero())

			f.gh.pr.Labels = nil
			f.a.reader = errorLister{}
			f.reconcile(t, f.command(100, command))
			require.Len(t, f.gh.checks, 1)
			gate, deadline := f.gh.checks[0], f.state().waitingUntil
			require.Equal(t, "queued", gate.Status)
			require.Equal(t, agenticCheckTitle, gate.Output.Title)
			require.False(t, deadline.IsZero())
			require.Empty(t, f.gh.comments)

			f.now = f.now.Add(time.Minute)
			f.reconcile(t, f.command(200, command))
			f.passFirstStage()
			f.reconcile(t, nil)
			require.Equal(t, []github.CheckRun{gate}, f.gh.checks)
			require.Equal(t, deadline, f.state().waitingUntil)
			require.Equal(t, 1, f.gh.checkAttempts)
			require.Len(t, f.gh.comments, 1)
			require.Contains(t, f.gh.comments[0].Body, "only once per HEAD SHA")
			require.Contains(t, f.gh.comments[0].Body, f.gh.pr.Head.SHA)
			require.Zero(t, f.gh.statusWrites)
		})
	}
}

func TestAgenticOptOutPersistsAcrossPush(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	f.a.states = nil
	f.gh.pr.Head.SHA = strings.Repeat("b", 40)
	f.passFirstStage()
	f.reconcile(t, nil)
	require.Len(t, f.gh.comments, 2)
	require.Contains(t, f.gh.comments[1].Body, "/test e2e")
	require.Equal(t, "success", f.gh.checks[len(f.gh.checks)-1].Conclusion)
}

func TestAgenticReconcilerIgnoresSecondStageUpdates(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	pj := makeProwJob("pull-ci-org-repo-main-e2e", f.gh.pr.Head.SHA)
	pj.Spec.Refs = pullRefs(f.work.org, f.work.repo, &f.gh.pr)
	reader := newFakePJLister(pj)
	r := &reconciler{pjclientset: reader.(ctrlruntimeclient.Client), configDataProvider: f.a.configDataProvider, agentic: f.a}
	require.NoError(t, r.reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: pj.Namespace, Name: pj.Name}}))
	require.Empty(t, f.a.inputs)
	require.Zero(t, f.gh.getPullRequestCalls)
}

func TestAgenticRetargetInvalidatesCompletionBeforeReadiness(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.a.watcher.config.Orgs[0].Repos[0].Branches = []string{"main", "release"}
	presubmits := f.a.configDataProvider.updatedPresubmits["org/repo"]
	presubmits.alwaysRequired = append(presubmits.alwaysRequired, config.Presubmit{JobBase: config.JobBase{Name: "pull-ci-org-repo-release-unit"}, AlwaysRun: true})
	f.a.configDataProvider.updatedPresubmits["org/repo"] = presubmits
	f.passFirstStage()
	f.reconcile(t, nil)
	f.reconcile(t, f.command(100, "mark-pipeline-gate"))
	f.gh.pr.Base.Ref = "release"
	f.now = f.now.Add(time.Minute)
	f.a.states = nil // Retarget validation must not depend on surviving process memory.
	f.reconcile(t, nil)
	require.Len(t, f.gh.checks, 1, "the next check must wait for target-branch first-stage success")
	require.Equal(t, "failure", f.gh.checks[0].Conclusion)
	f.passFirstStage()
	f.reconcile(t, nil)
	require.Len(t, f.gh.checks, 2)
	require.Equal(t, "queued", f.gh.checks[1].Status)
}

func TestAgenticFallbackManualCommandAndReenable(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	f.now = f.state().waitingUntil
	f.reconcile(t, nil)
	f.reconcile(t, f.command(100, "remaining"))
	require.Len(t, f.gh.comments, 1)
	require.Contains(t, f.gh.comments[0].Body, "/test e2e")
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	f.reconcile(t, f.command(200, "remaining"))
	require.Len(t, f.gh.statusUpdates, 1, "duplicate deltas must not reset contexts or repost /test")
	f.reconcile(t, f.command(300, "agentic-mode"))
	require.False(t, f.state().timedOut)
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	require.True(t, f.state().waitingUntil.IsZero())
}

func TestAgenticFreshReadinessRetryDoesNotDuplicateCreation(t *testing.T) {
	for _, reply := range []bool{false, true} {
		t.Run(fmt.Sprintf("reply=%t", reply), func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(1, "required"))
			f.reconcile(t, f.command(100, "skip-agentic-mode"))
			f.reconcile(t, f.command(200, "agentic-mode"))
			f.gh.failCheckAfterCreate = true
			command := f.command(250, "required")
			require.Error(t, f.a.reconcilePull(context.Background(), f.work, command))
			require.Len(t, f.gh.checks, 2, "GitHub accepted the fresh readiness despite a lost response")
			f.a.reader = errorLister{}
			var comment *github.IssueComment
			if reply {
				comment = &github.IssueComment{ID: 300, Body: "Chai is reviewing this PR.", User: github.User{Login: "chai[bot]"}, CreatedAt: f.now}
			}
			f.reconcile(t, comment)
			require.Len(t, f.gh.checks, 2, "retry must reuse the queued check, not start Chai twice")
			require.Equal(t, reply, f.state().waitingUntil.IsZero(), "a reply during the lost response must cancel the timer")
		})
	}
}

func TestAgenticPendingReadinessSurvivesRestartWithoutTimerRecovery(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	f.a.states, f.a.reader = nil, errorLister{}
	f.now = f.now.Add(time.Hour)
	f.reconcile(t, nil)
	require.True(t, f.state().waitingUntil.IsZero())
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, "queued", f.gh.checks[0].Status)
	f.reconcile(t, f.command(100, "mark-pipeline-gate"))
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
}

func TestAgenticReenablePreservesCompletedDispatchWithoutFirstStageJobs(t *testing.T) {
	f := newAgenticFixture(t, "auto")
	f.reconcile(t, f.command(100, "skip-agentic-mode"))
	f.passFirstStage()
	f.reconcile(t, nil)
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	f.a.reader = newFakePJLister() // First-stage jobs were cleaned before re-enable.
	f.reconcile(t, f.command(200, "agentic-mode"))
	require.Equal(t, "success", f.gh.checks[0].Conclusion)
	require.Len(t, f.gh.checks, 1, "completed dispatch must not need first-stage jobs again")
	require.True(t, f.state().ready)
	require.True(t, f.state().waitingUntil.IsZero())
}

func TestAgenticRetiredGateCannotAuthorizeChaiReadinessAfterRestart(t *testing.T) {
	f := newAgenticFixture(t, "manual")
	f.passFirstStage()
	f.reconcile(t, f.command(1, "required"))
	f.reconcile(t, f.command(100, "mark-pipeline-gate"))
	f.reconcile(t, f.command(200, "skip-agentic-mode"))
	f.a.reader = newFakePJLister()
	f.reconcile(t, f.command(300, "agentic-mode"))
	require.Equal(t, "failure", f.gh.checks[0].Conclusion)
	f.a.states = nil
	mark := f.command(400, "mark-pipeline-gate")
	mark.User.Login = "chai[bot]"
	f.reconcile(t, mark)
	ack := &github.IssueComment{ID: 500, Body: "Chai is reviewing this PR.",
		User: github.User{Login: "chai[bot]"}, CreatedAt: f.now}
	f.reconcile(t, ack)
	require.Len(t, f.gh.checks, 1)
	require.Equal(t, "failure", f.gh.checks[0].Conclusion)
	require.False(t, f.state().ready)
}

func TestAgenticHumanMarkMissingOrRunningGate(t *testing.T) {
	for _, mode := range []string{"auto", "manual", "lgtm"} {
		for _, scenario := range []string{"missing-member", "missing-collaborator", "running", "missing-traditional", "lost-create-response"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				f := newAgenticFixture(t, mode)
				if scenario == "missing-collaborator" {
					f.gh.member, f.gh.collaborator = false, true
				}
				if scenario == "running" {
					gate := traditionalCheck(pullRefs(f.work.org, f.work.repo, &f.gh.pr), "in_progress", "", "dispatching")
					gate.Output.Title = agenticCheckTitle
					_, err := f.gh.CreateCheckRun(f.work.org, f.work.repo, gate)
					require.NoError(t, err)
				}
				if scenario == "missing-traditional" {
					f.gh.pr.Labels = []github.Label{{Name: agenticSkipLabel}}
				}
				command := f.command(100, "mark-pipeline-gate "+f.gh.pr.Head.SHA)
				if scenario == "lost-create-response" {
					f.gh.failCheckAfterCreate = true
					require.Error(t, f.a.reconcilePull(context.Background(), f.work, command))
				}
				f.reconcile(t, command)
				require.Len(t, f.gh.checks, 1)
				gate := f.gh.checks[0]
				require.Equal(t, "completed", gate.Status)
				require.Equal(t, "success", gate.Conclusion)
				require.Equal(t, pipelineExternalID(pullRefs(f.work.org, f.work.repo, &f.gh.pr)), gate.ExternalID)
				wantTitle := agenticCheckTitle
				if scenario == "missing-traditional" {
					wantTitle = traditionalCheckTitle
				}
				require.Equal(t, wantTitle, gate.Output.Title)
				require.True(t, f.state().dispatched)
				require.True(t, f.state().waitingUntil.IsZero())
				f.passFirstStage()
				f.now = f.now.Add(time.Hour)
				f.reconcile(t, nil)
				f.a.states = nil
				f.reconcile(t, nil)
				require.Equal(t, []github.CheckRun{gate}, f.gh.checks, "completion must not publish fresh readiness after reconciliation or restart")
				require.True(t, f.state().dispatched)
				require.True(t, f.state().waitingUntil.IsZero())
				require.Zero(t, f.gh.statusWrites)
				require.Empty(t, f.gh.comments, "marking must not dispatch tests or trigger fallback")
			})
		}
	}
}

func TestAgenticMissingGateMarkRequiresTrustedHuman(t *testing.T) {
	for _, scenario := range []string{"untrusted", "stale-head", "chai", "chai-stale-memory"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAgenticFixture(t, "auto")
			f.reconcile(t, nil)
			command := f.command(100, "mark-pipeline-gate")
			switch scenario {
			case "untrusted":
				f.gh.member = false
			case "stale-head":
				command.Body += " " + strings.Repeat("b", 40)
			case "chai", "chai-stale-memory":
				command.User.Login, f.gh.member = "chai[bot]", false
				if scenario == "chai-stale-memory" {
					f.state().ready = true
				}
			}
			err := f.a.reconcilePull(context.Background(), f.work, command)
			if scenario == "chai-stale-memory" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, f.gh.checks)
			require.Zero(t, f.gh.checkAttempts)
			require.Zero(t, f.gh.statusWrites)
			for _, comment := range f.gh.comments {
				require.NotContains(t, comment.Body, "/test ")
			}
		})
	}
}

func TestAgenticChaiCannotMarkChangedGateWithStaleReadiness(t *testing.T) {
	for _, scenario := range []string{"retired", "traditional", "wrong-base", "wrong-pr"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAgenticFixture(t, "manual")
			f.passFirstStage()
			f.reconcile(t, f.command(1, "required"))
			switch scenario {
			case "retired":
				f.gh.checks[0].Status, f.gh.checks[0].Conclusion = "completed", "failure"
			case "traditional":
				f.gh.checks[0].Status, f.gh.checks[0].Conclusion = "completed", "success"
				f.gh.checks[0].Output.Title = traditionalCheckTitle
			case "wrong-base":
				f.gh.checks[0].ExternalID += "-old"
			case "wrong-pr":
				f.gh.checks[0].ExternalID = "pipeline-controller:org/repo#99:main"
			}
			gate := f.gh.checks[0]
			attempts := f.gh.checkAttempts
			mark := f.command(100, "mark-pipeline-gate")
			mark.User.Login, f.gh.member = "chai[bot]", false
			require.Error(t, f.a.reconcilePull(context.Background(), f.work, mark))
			require.Equal(t, attempts, f.gh.checkAttempts)
			require.Equal(t, []github.CheckRun{gate}, f.gh.checks)
			require.Zero(t, f.gh.statusWrites)
			require.Empty(t, f.gh.comments)
		})
	}
}

func TestAgenticMarkUpdateFailurePreservesPersistedCompletionOrResponseTimeout(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%t", accepted), func(t *testing.T) {
			f := newAgenticFixture(t, "auto")
			f.passFirstStage()
			f.reconcile(t, nil)
			deadline := f.state().waitingUntil
			f.gh.failCheck, f.gh.failCheckAfterUpdate = !accepted, accepted
			require.Error(t, f.a.reconcilePull(context.Background(), f.work, f.command(100, "mark-pipeline-gate")))
			require.Equal(t, deadline, f.state().waitingUntil, "an unconfirmed mark must preserve the response timeout")
			require.False(t, f.state().dispatched)
			persisted := f.gh.checks[0]
			if accepted {
				require.Equal(t, "success", persisted.Conclusion)
			} else {
				require.Equal(t, "queued", persisted.Status)
			}
			// No command retry arrives: the ordinary timeout reconciliation must
			// honor the actual check rather than assume the update was rejected.
			f.gh.failCheck = false
			f.now = deadline.Add(time.Minute)
			f.reconcile(t, nil)
			if accepted {
				require.Equal(t, []github.CheckRun{persisted}, f.gh.checks)
				require.False(t, f.state().timedOut)
				require.Zero(t, f.gh.statusWrites)
				require.Empty(t, f.gh.comments)
				f.a.states = nil
				f.reconcile(t, nil)
				require.Equal(t, []github.CheckRun{persisted}, f.gh.checks)
				require.Empty(t, f.gh.comments)
			} else {
				require.True(t, f.state().timedOut)
				require.Equal(t, traditionalCheckTitle, f.gh.checks[0].Output.Title)
				require.Positive(t, f.gh.statusWrites)
				require.Len(t, f.gh.comments, 1)
				require.Contains(t, f.gh.comments[0].Body, "/test e2e")
			}
			require.True(t, f.state().dispatched)
			require.True(t, f.state().waitingUntil.IsZero())
		})
	}
}
