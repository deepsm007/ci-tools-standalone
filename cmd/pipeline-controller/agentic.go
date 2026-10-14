package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	v1 "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"
	"sigs.k8s.io/prow/pkg/github"
	"sigs.k8s.io/prow/pkg/labels"
)

const agenticSkipLabel = "pipeline-skip-agentic-mode"
const agenticCheckTitle = "Agentic pipeline gate"

func nativeAgenticReadiness(gate *github.CheckRun) bool {
	return gate.ID != 0 && ((gate.Status == "queued" && gate.Output.Title == agenticCheckTitle) || (gate.Status == "completed" && gate.Conclusion == "success"))
}

type agenticGitHubClient interface {
	minimalGhClient
	pipelineCheckClient
	pipelineCommandClient
	RemoveLabel(org, repo string, number int, label string) error
}

// Only handoff state is retained in memory. GitHub owns the gate and the opt-out
// label; restarts deliberately do not recover response timers or missed events.
type agenticState struct {
	head, base   string
	gate         github.CheckRun
	ready        bool
	fresh        bool
	timedOut     bool
	optOut       bool
	traditional  bool // Current revision ownership, independent of the next-push opt-out label.
	dispatched   bool
	lastCommand  int
	waitingUntil time.Time
	publishedAt  time.Time
}

func (s *agenticState) traditionalMode() bool {
	return s.optOut || s.timedOut || s.traditional
}

type agenticController struct {
	gh                 agenticGitHubClient
	reader             ctrlruntimeclient.Reader
	configDataProvider *ConfigDataProvider
	watcher            *watcher
	lgtmWatcher        *watcher
	checks             *dispatchChecks
	appID              int64
	dryRun             bool
	now                func() time.Time
	logger             *logrus.Entry
	options            agenticOptions
	mu                 sync.Mutex
	states             map[agenticWork]*agenticState
	queueMu            sync.Mutex
	queue              workqueue.TypedRateLimitingInterface[agenticWork]
	inputs             map[agenticWork]*agenticInput
}

func (a *agenticController) repoConfig(org, repo, branch string) (RepoConfig, bool) {
	if a == nil {
		return RepoConfig{}, false
	}
	for i, w := range []*watcher{a.watcher, a.lgtmWatcher} {
		if w == nil {
			continue
		}
		if cfg, ok := w.getConfig()[org][repo]; ok && isBranchEnabled(cfg.Branches, branch) {
			if i == 1 {
				cfg.Trigger = "lgtm"
			}
			return cfg, cfg.Agentic.enabled()
		}
	}
	return RepoConfig{}, false
}

func (a *agenticController) hasRepo(org, repo string) bool {
	if a != nil {
		for _, w := range []*watcher{a.watcher, a.lgtmWatcher} {
			if w != nil && w.getConfig()[org][repo].Agentic.enabled() {
				return true
			}
		}
	}
	return false
}

func (a *agenticController) hasAgenticEnrollment() bool {
	for _, w := range []*watcher{a.watcher, a.lgtmWatcher} {
		if w == nil {
			continue
		}
		for _, repos := range w.getConfig() {
			for _, cfg := range repos {
				if cfg.Agentic.enabled() {
					return true
				}
			}
		}
	}
	return false
}

func (a *agenticController) currentTime() time.Time {
	if a.now != nil {
		return a.now().UTC()
	}
	return time.Now().UTC()
}

func hasAgenticLabel(pr *github.PullRequest, name string) bool {
	for _, label := range pr.Labels {
		if label.Name == name {
			return true
		}
	}
	return false
}

func (a *agenticController) trustedChai(login string) bool {
	for _, author := range a.options.trustedAuthors.Strings() {
		if strings.EqualFold(login, author) {
			return true
		}
	}
	return false
}

func pullRefs(org, repo string, pr *github.PullRequest) *v1.Refs {
	return &v1.Refs{Org: org, Repo: repo, BaseRef: pr.Base.Ref,
		Pulls: []v1.Pull{{Number: pr.Number, SHA: pr.Head.SHA}}}
}

func (a *agenticController) cancelResponseTimeout(s *agenticState, comment *github.IssueComment) {
	if comment != nil && a.trustedChai(comment.User.Login) && comment.ID > 0 && s.ready && !s.traditionalMode() &&
		!s.waitingUntil.IsZero() && !comment.CreatedAt.Before(s.publishedAt) && !comment.CreatedAt.After(s.waitingUntil) {
		s.waitingUntil = time.Time{} // Any new Chai comment is a response, not dispatch completion.
	}
}

// Caller holds a.mu. All reads and dispatch decisions use live PR metadata.
func (a *agenticController) reconcilePull(ctx context.Context, work agenticWork, comment *github.IssueComment) error {
	pr, err := a.gh.GetPullRequest(work.org, work.repo, work.number)
	if err != nil {
		return err
	}
	cfg, enabled := a.repoConfig(work.org, work.repo, pr.Base.Ref)
	if !enabled || pr.State != github.PullRequestStateOpen || pr.Draft {
		delete(a.states, work)
		return nil
	}
	if a.dryRun {
		return nil
	}
	if err := a.validateEnrollment(); err != nil {
		return err
	}
	if pr.Head.SHA == "" || a.reader == nil || a.checks == nil {
		return fmt.Errorf("agentic readiness requires a HEAD SHA, ProwJob cache and GitHub App checks")
	}
	if a.states == nil {
		a.states = map[agenticWork]*agenticState{}
	}
	s := a.states[work]
	if s == nil || s.head != pr.Head.SHA || s.base != pr.Base.Ref {
		fresh := s != nil
		gate, err := findPipelineCheck(a.gh, a.appID, work.org, work.repo, pr.Head.SHA)
		if err != nil {
			return err
		}
		refs := pullRefs(work.org, work.repo, pr)
		if gate.ID != 0 && !strings.HasPrefix(gate.ExternalID, pipelinePRIdentity(work.org, work.repo, pr.Number)+":") {
			return fmt.Errorf("pipeline gate belongs to a different PR")
		}
		if gate.ID != 0 && (fresh || gate.ExternalID != pipelineExternalID(refs)) {
			if err := a.retireCheck(work, *gate); err != nil {
				return err
			}
			fresh = true
		}
		lastCommand := 0
		if s != nil {
			lastCommand = s.lastCommand // A completed command is not replayed on a later push.
		}
		s = &agenticState{head: pr.Head.SHA, base: pr.Base.Ref, fresh: fresh, gate: *gate, lastCommand: lastCommand}
		if !fresh && traditionalRevisionOwner(gate) {
			s.traditional = true // Ownership survives a failed or in-progress traditional rerun too.
		}
		if !fresh && nativeAgenticReadiness(gate) {
			s.ready = true // Native readiness/completion survives a lost in-memory timer.
			s.dispatched = gate.Status == "completed" && gate.Conclusion == "success"
		}
		a.states[work] = s
	}
	optOut := hasAgenticLabel(pr, agenticSkipLabel)
	if s.optOut && !optOut {
		if _, err := a.restoreAgenticMode(work, s, pr, false); err != nil {
			return err
		}
	}
	s.optOut = optOut
	a.cancelResponseTimeout(s, comment)
	if comment != nil {
		if err := a.applyCommand(work, s, pr, cfg, *comment); err != nil {
			return err
		}
	}
	if s.traditionalMode() {
		s.waitingUntil = time.Time{}
		return a.dispatchTraditional(ctx, work, s, pr, cfg)
	}
	if !s.ready {
		pj := &v1.ProwJob{Spec: v1.ProwJobSpec{Refs: pullRefs(work.org, work.repo, pr)}}
		complete, err := checkFirstStageComplete(ctx, a.reader, pj, a.configDataProvider.GetPresubmits(work.org+"/"+work.repo))
		if err != nil || !complete {
			return err
		}
		created, err := a.publishReadiness(work, s, pr)
		if err != nil {
			return err
		}
		if !created {
			a.cancelResponseTimeout(s, comment) // The reply may confirm a creation whose response was lost.
		}
	}
	if !s.waitingUntil.IsZero() && !a.currentTime().Before(s.waitingUntil) {
		// A completion update may have reached GitHub even when its response
		// and subsequent command retries were lost. Confirm persisted ownership
		// before a response timeout can overwrite that success and dispatch tests.
		gate, err := findPipelineCheck(a.gh, a.appID, work.org, work.repo, s.head)
		if err != nil {
			return err
		}
		if gate.HeadSHA == s.head && gate.ExternalID == pipelineExternalID(pullRefs(work.org, work.repo, pr)) &&
			gate.Status == "completed" && gate.Conclusion == "success" && !traditionalRevisionOwner(gate) {
			s.gate, s.ready, s.dispatched, s.fresh = *gate, true, true, false
			s.waitingUntil = time.Time{}
			return nil
		}
		s.timedOut, s.waitingUntil = true, time.Time{}
		return a.dispatchTraditional(ctx, work, s, pr, cfg)
	}
	return nil
}

// A completed dispatch belongs to the current revision regardless of its mode.
// Removing opt-out must not signal Chai again or invalidate that completion.
func (a *agenticController) restoreAgenticMode(work agenticWork, s *agenticState, pr *github.PullRequest, removeLabel bool) (bool, error) {
	gate, err := findPipelineCheck(a.gh, a.appID, work.org, work.repo, pr.Head.SHA)
	if err != nil {
		return false, err
	}
	if gate.ID != 0 && !strings.HasPrefix(gate.ExternalID, pipelinePRIdentity(work.org, work.repo, pr.Number)+":") {
		return false, fmt.Errorf("no active owned pipeline gate for this PR/revision")
	}
	current := gate.ID != 0 && gate.ExternalID == pipelineExternalID(pullRefs(work.org, work.repo, pr))
	completed := current &&
		gate.Status == "completed" && gate.Conclusion == "success"
	deferred := current && (completed || traditionalRevisionOwner(gate))
	if !deferred && gate.ID != 0 {
		if err := a.retireCheck(work, *gate); err != nil {
			return false, err
		}
	}
	if removeLabel && s.optOut {
		if err := a.gh.RemoveLabel(work.org, work.repo, work.number, agenticSkipLabel); err != nil {
			return false, err
		}
	}
	s.optOut, s.timedOut, s.dispatched = false, false, completed
	s.traditional = current && traditionalRevisionOwner(gate)
	s.ready, s.fresh, s.waitingUntil = completed, !deferred, time.Time{}
	if deferred {
		s.gate, s.publishedAt = *gate, time.Time{}
	}
	return deferred, nil
}

func (a *agenticController) retireCheck(work agenticWork, gate github.CheckRun) error {
	return a.gh.UpdateCheckRun(work.org, work.repo, gate.ID, github.CheckRun{Status: "completed", Conclusion: "failure",
		Output: github.CheckRunOutput{Title: agenticCheckTitle, Summary: "Previous dispatch completion was invalidated; a new pipeline cycle is required."}})
}

// The boolean reports new creation, so an earlier comment cannot cancel its timer.
func (a *agenticController) publishReadiness(work agenticWork, s *agenticState, pr *github.PullRequest) (bool, error) {
	a.checks.mu.Lock()
	defer a.checks.mu.Unlock()
	gate, err := findPipelineCheck(a.gh, a.appID, work.org, work.repo, s.head)
	if err != nil {
		return false, err
	}
	refs := pullRefs(work.org, work.repo, pr)
	if gate.ID != 0 && gate.ExternalID != pipelineExternalID(refs) {
		if !strings.HasPrefix(gate.ExternalID, pipelinePRIdentity(work.org, work.repo, pr.Number)+":") {
			return false, fmt.Errorf("pipeline gate belongs to a different PR")
		}
		s.fresh = true // Same HEAD, different target branch.
	}
	if !s.fresh && nativeAgenticReadiness(gate) {
		s.gate, s.ready = *gate, true
		if !s.publishedAt.IsZero() {
			s.waitingUntil = a.currentTime().Add(a.options.timeout) // A lost creation response was confirmed by this retry.
		}
		return false, nil // Restarts do not recover response timers.
	}
	if gate.ID != 0 {
		if err := a.retireCheck(work, *gate); err != nil {
			return false, err
		}
	}
	next := traditionalCheck(refs, "queued", "", fmt.Sprintf("First-stage tests passed. Any new Chai comment within %s cancels traditional fallback. The gate stays pending until `/pipeline mark-pipeline-gate %s`. Individual test contexts gate results.", a.options.timeout, s.head))
	next.Output.Title = agenticCheckTitle
	s.fresh, s.publishedAt = false, a.currentTime().Truncate(time.Second)
	id, err := a.gh.CreateCheckRun(work.org, work.repo, next)
	if err != nil {
		return false, err
	}
	next.ID, next.App.ID = id, a.appID
	s.gate, s.ready, s.fresh = next, true, false
	s.waitingUntil = a.currentTime().Add(a.options.timeout)
	return true, nil
}

// This is the ordinary selector and /test dispatcher, not an agentic scheduler.
func (a *agenticController) dispatchTraditional(ctx context.Context, work agenticWork, s *agenticState, pr *github.PullRequest, cfg RepoConfig) error {
	if s.dispatched {
		return nil
	}
	if err := a.checks.pending(pullRefs(work.org, work.repo, pr)); err != nil {
		return err
	}
	shouldDispatch := cfg.Trigger == "auto" || (cfg.Trigger == "lgtm" && (hasAgenticLabel(pr, PipelineAutoLabel) || hasAgenticLabel(pr, labels.LGTM)))
	if !shouldDispatch {
		return nil
	}
	pj := &v1.ProwJob{Spec: v1.ProwJobSpec{Refs: pullRefs(work.org, work.repo, pr)}}
	presubmits := a.configDataProvider.GetPresubmits(work.org + "/" + work.repo)
	complete, err := checkFirstStageComplete(ctx, a.reader, pj, presubmits)
	if err != nil || !complete {
		return err
	}
	if err := sendCommentWithMode(presubmits, pj, a.gh, func() {}, a.reader, modeDelta, false, a.checks); err != nil {
		return err
	}
	s.dispatched = true
	return nil
}
