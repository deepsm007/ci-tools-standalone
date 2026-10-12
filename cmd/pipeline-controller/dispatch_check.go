package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	v1 "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"
	"sigs.k8s.io/prow/pkg/github"
)

const pipelineGate = "ci/pipeline-gate"
const traditionalCheckTitle = "Pipeline dispatch"
const traditionalRerunCheckTitle = "Traditional pipeline rerun"
const dispatchSuccessSummary = "Applicable second-stage tests were dispatched, already had runs, or were not required. Individual job contexts report test results."

func dispatchOverrideSummary(commentID int) string {
	return fmt.Sprintf("Dispatch completion acknowledged with `/pipeline mark-pipeline-gate` (comment %d). No tests were started or test results overridden.", commentID)
}

func pipelinePRIdentity(org, repo string, number int) string {
	return fmt.Sprintf("pipeline-controller:%s/%s#%d", org, repo, number)
}

func pipelineExternalID(refs *v1.Refs) string {
	return pipelinePRIdentity(refs.Org, refs.Repo, refs.Pulls[0].Number) + ":" + refs.BaseRef
}

type pipelineCheckClient interface {
	ListCheckRuns(org, repo, ref string) (*github.CheckRunList, error)
	CreateCheckRun(org, repo string, check github.CheckRun) (int64, error)
	UpdateCheckRun(org, repo string, id int64, check github.CheckRun) error
}

func findPipelineCheck(gh pipelineCheckClient, appID int64, org, repo, sha string) (*github.CheckRun, error) {
	checks, err := gh.ListCheckRuns(org, repo, sha)
	if err != nil {
		return nil, err
	}
	if checks == nil {
		return nil, fmt.Errorf("empty check-run response")
	}
	gate := &github.CheckRun{}
	for _, check := range checks.CheckRuns {
		if check.Name == pipelineGate && check.App.ID == appID && check.ID > gate.ID {
			copy := check
			gate = &copy
		}
	}
	return gate, nil
}

// A rerun may fail after an earlier successful dispatch. Its title retains that
// historical ownership without claiming that the current rerun succeeded.
func traditionalRevisionOwner(gate *github.CheckRun) bool {
	return gate.ID != 0 && (gate.Output.Title == traditionalRerunCheckTitle ||
		(gate.Output.Title == traditionalCheckTitle && gate.Status == "completed" && gate.Conclusion == "success"))
}

func writePipelineCheck(gh pipelineCheckClient, appID int64, org, repo string, gate *github.CheckRun, next github.CheckRun, reopen bool) error {
	if gate.ID != 0 && gate.ExternalID == next.ExternalID && next.Output.Title == traditionalCheckTitle &&
		(gate.Output.Title == traditionalRerunCheckTitle || (traditionalRevisionOwner(gate) && next.Status != "completed")) {
		next.Output.Title = traditionalRerunCheckTitle
	}
	if gate.ID == 0 {
		id, err := gh.CreateCheckRun(org, repo, next)
		if err != nil {
			return err
		}
		next.ID = id
	} else {
		// PATCH cannot clear conclusion:null with this client. Retire an old
		// success before reopening the same run, so Tide cannot prefer it.
		if reopen && next.Status != "completed" {
			if err := gh.UpdateCheckRun(org, repo, gate.ID, github.CheckRun{Status: "completed", Conclusion: "failure", Output: next.Output}); err != nil {
				return err
			}
		}
		if err := gh.UpdateCheckRun(org, repo, gate.ID, next); err != nil {
			return err
		}
		next.ID = gate.ID
	}
	next.App.ID = appID
	*gate = next
	return nil
}

// Traditional dispatch needs no persistent state: reuse the owned check on the
// current commit. The same instance serializes PR events and automatic dispatch.
type dispatchChecks struct {
	gh interface {
		pipelineCheckClient
		pipelineCommandClient
	}
	appID int64
	mu    sync.Mutex
}

func traditionalCheck(refs *v1.Refs, status, conclusion, summary string) github.CheckRun {
	return github.CheckRun{Name: pipelineGate, HeadSHA: refs.Pulls[0].SHA,
		ExternalID: pipelineExternalID(refs), Status: status, Conclusion: conclusion,
		Output: github.CheckRunOutput{Title: traditionalCheckTitle, Summary: fmt.Sprintf("Pipeline for `%s` → `%s`.\n\n%s", refs.Pulls[0].SHA, refs.BaseRef, summary)}}
}

func (c *dispatchChecks) pending(refs *v1.Refs) error {
	return c.pendingWithReset(refs, false)
}

func (c *dispatchChecks) pendingWithReset(refs *v1.Refs, reset bool) error {
	if c == nil {
		return nil // Normal token-authenticated deployments retain their workflow.
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	gate, err := findPipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, refs.Pulls[0].SHA)
	if err != nil {
		return err
	}
	if reset && gate.ID == 0 {
		return nil // A mode switch must not create a readiness signal.
	}
	if !reset && gate.ID != 0 && gate.ExternalID == pipelineExternalID(refs) {
		return nil // Repeated PR events must not reset a successful dispatch.
	}
	if gate.ID != 0 && !strings.HasPrefix(gate.ExternalID, pipelinePRIdentity(refs.Org, refs.Repo, refs.Pulls[0].Number)+":") {
		return fmt.Errorf("pipeline gate belongs to a different PR")
	}
	return writePipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, gate,
		traditionalCheck(refs, "queued", "", "Waiting for the configured trigger to dispatch second-stage tests."), gate.Conclusion == "success")
}

// The caller holds c.mu across planning and dispatch, when checks are enabled.
func (c *dispatchChecks) dispatch(refs *v1.Refs, hasJobs bool, post func() error) error {
	if c == nil {
		return post()
	}
	if refs == nil || len(refs.Pulls) != 1 || refs.Pulls[0].SHA == "" {
		return fmt.Errorf("pipeline dispatch check requires one PR with a HEAD SHA")
	}
	gate, err := findPipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, refs.Pulls[0].SHA)
	if err != nil {
		return err
	}
	if gate.ID != 0 && !strings.HasPrefix(gate.ExternalID, pipelinePRIdentity(refs.Org, refs.Repo, refs.Pulls[0].Number)+":") {
		return fmt.Errorf("pipeline gate belongs to a different PR")
	}
	if !hasJobs && gate.ExternalID == pipelineExternalID(refs) && gate.Status == "completed" && gate.Conclusion == "success" {
		return post() // An empty retry or acknowledgment cannot undo dispatch.
	}
	if hasJobs {
		next := traditionalCheck(refs, "in_progress", "", "Dispatching applicable second-stage tests.")
		if err := writePipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, gate, next, gate.Conclusion == "success"); err != nil {
			return err
		}
	}
	if err := post(); err != nil {
		next := traditionalCheck(refs, "completed", "failure", "Second-stage dispatch failed: "+err.Error())
		return errors.Join(err, writePipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, gate, next, false))
	}
	next := traditionalCheck(refs, "completed", "success", dispatchSuccessSummary)
	return writePipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, gate, next, false)
}

func (c *dispatchChecks) markGate(refs *v1.Refs, commentID int) error {
	return c.markGateWithTitle(refs, commentID, "")
}

func (c *dispatchChecks) markGateWithTitle(refs *v1.Refs, commentID int, title string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	gate, err := findPipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, refs.Pulls[0].SHA)
	if err != nil {
		return err
	}
	if gate.ID == 0 || gate.ExternalID != pipelineExternalID(refs) {
		return fmt.Errorf("no active owned pipeline gate for the current HEAD/base")
	}
	next := traditionalCheck(refs, "completed", "success", dispatchOverrideSummary(commentID))
	if title == "" {
		title = gate.Output.Title
	}
	next.Output.Title = title
	return writePipelineCheck(c.gh, c.appID, refs.Org, refs.Repo, gate, next, false)
}
