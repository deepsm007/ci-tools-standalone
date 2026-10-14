package main

import (
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/prow/pkg/github"
)

func TestPipelineHelpDoesNotReconcile(t *testing.T) {
	for _, body := range []string{"/pipeline help", "\r\n/pipeline help\r\n\r\n"} {
		for _, action := range []github.IssueCommentEventAction{github.IssueCommentActionCreated, github.IssueCommentActionEdited} {
			f := newAgenticFixture(t, "auto")
			cw := &clientWrapper{ghc: f.gh, agentic: f.a}
			cw.handleIssueComment(f.a.logger, github.IssueCommentEvent{Action: action, Repo: f.gh.pr.Base.Repo,
				Issue: github.Issue{Number: 42, PullRequest: &struct{}{}}, Comment: github.IssueComment{Body: body}})
			if action == github.IssueCommentActionCreated {
				require.Len(t, f.gh.comments, 1)
				for _, command := range []string{"help", "required", "remaining", "auto", "agentic-mode", "skip-agentic-mode", "mark-pipeline-gate"} {
					require.Contains(t, f.gh.comments[0].Body, "/pipeline "+command)
				}
			} else {
				require.Empty(t, f.gh.comments)
			}
			require.Zero(t, f.gh.getPullRequestCalls)
			require.Empty(t, f.gh.checks)
		}
	}
}

func TestParsePipelineCommandLineEndings(t *testing.T) {
	for _, tc := range []struct {
		body string
		name string
	}{
		{"/pipeline agentic-mode", "agentic-mode"},
		// The two restore comment bodies originally ignored on PR #5462.
		{"\r\n/pipeline agentic-mode\r\n\r\n", "agentic-mode"},
		{"/pipeline agentic-mode\r\n\r\n", "agentic-mode"},
		{"Explanation\r\n/pipeline skip-agentic-mode\r\n", "skip-agentic-mode"},
		{"/pipeline mark-pipeline-gate " + strings.Repeat("a", 40) + "\r\n", "mark-pipeline-gate"},
		{"/pipeline agentic-mode\r", ""},
		{"/pipeline agentic-mode extra\r\n", ""},
		{"/pipeline agentic-mode " + strings.Repeat("a", 40) + "\r\n", ""},
		{"/pipeline mark-pipeline-gate short\r\n", ""},
		{"/pipeline agentic-mode\r\n/pipeline skip-agentic-mode\r\n", ""},
		{"/pipeline required\n/pipeline remaining\r\n", ""},
		{"/pipeline help\r\n", ""},
	} {
		t.Run(tc.body, func(t *testing.T) {
			command, ok := parsePipelineCommand(tc.body)
			require.Equal(t, tc.name != "", ok)
			require.Equal(t, tc.name, command.name)
		})
	}
}

func TestAgenticModeCRLFCommentEvents(t *testing.T) {
	for _, body := range []string{"\r\n/pipeline agentic-mode\r\n\r\n", "/pipeline agentic-mode\r\n\r\n"} {
		for _, action := range []github.IssueCommentEventAction{github.IssueCommentActionCreated, github.IssueCommentActionEdited} {
			t.Run(string(action)+"/"+body, func(t *testing.T) {
				f := newAgenticFixture(t, "manual")
				f.reconcile(t, f.command(100, "skip-agentic-mode"))
				f.a.queue = workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[agenticWork](0, 0))
				defer f.a.queue.ShutDown()
				cw := &clientWrapper{ghc: f.gh, agentic: f.a}
				comment := f.command(200, "agentic-mode")
				comment.Body = body
				cw.handleIssueComment(f.a.logger, github.IssueCommentEvent{Action: action, Repo: f.gh.pr.Base.Repo,
					Issue: github.Issue{Number: f.work.number, PullRequest: &struct{}{}}, Comment: *comment})
				if action == github.IssueCommentActionCreated {
					require.True(t, f.a.processNext(t.Context()))
					require.False(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
				} else {
					require.Empty(t, f.a.inputs, "edited comments must not execute a mode change")
					require.True(t, hasAgenticLabel(&f.gh.pr, agenticSkipLabel))
				}
			})
		}
	}
}

func TestTraditionalMarkPipelineGate(t *testing.T) {
	for _, scenario := range []string{"member", "collaborator", "untrusted", "stale-head", "explicit-head", "missing-gate", "disabled", "edited", "suffix", "multiple", "no-app", "write-error"} {
		t.Run(scenario, func(t *testing.T) {
			refs := makeTriggerPJ(strings.Repeat("a", 40)).Spec.Refs
			foreign := traditionalCheck(refs, "completed", "success", "another app")
			foreign.ID, foreign.App.ID = 900, 202
			gh := &fakeAgenticGitHub{member: true, checks: []github.CheckRun{foreign},
				pr: github.PullRequest{Number: refs.Pulls[0].Number, State: github.PullRequestStateOpen,
					Head: github.PullRequestBranch{SHA: refs.Pulls[0].SHA},
					Base: github.PullRequestBranch{Ref: refs.BaseRef, Repo: github.Repo{Name: refs.Repo, Owner: github.User{Login: refs.Org}}}}}
			checks := &dispatchChecks{gh: gh, appID: 101}
			if scenario != "missing-gate" {
				require.NoError(t, checks.pending(refs))
			}
			cw := &clientWrapper{ghc: gh, checks: checks, watcher: &watcher{config: mustEnabledConfig(t)}, lgtmWatcher: &watcher{},
				configDataProvider: &ConfigDataProvider{updatedPresubmits: map[string]presubmitTests{}}}
			event := github.IssueCommentEvent{Action: github.IssueCommentActionCreated, Repo: gh.pr.Base.Repo,
				Issue:   github.Issue{Number: gh.pr.Number, PullRequest: &struct{}{}},
				Comment: github.IssueComment{ID: 500, Body: "/pipeline mark-pipeline-gate", User: github.User{Login: "maintainer"}}}
			switch scenario {
			case "collaborator":
				gh.member, gh.collaborator = false, true
			case "untrusted":
				gh.member = false
			case "stale-head":
				event.Comment.Body += " " + strings.Repeat("b", 40)
			case "explicit-head":
				event.Comment.Body += " " + refs.Pulls[0].SHA
			case "disabled":
				cw.watcher.config.Orgs[0].Repos[0].Branches = []string{"release"}
			case "edited":
				event.Action = github.IssueCommentActionEdited
			case "suffix":
				event.Comment.Body += " please"
			case "multiple":
				event.Comment.Body += "\n/pipeline skip-agentic-mode"
			case "no-app":
				cw.checks = nil
			case "write-error":
				gh.failCheck = true
			}
			cw.handleIssueComment(logrus.NewEntry(logrus.New()), event)
			require.Equal(t, foreign, gh.checks[0])
			switch scenario {
			case "member", "collaborator", "explicit-head", "missing-gate":
				require.Len(t, gh.checks, 2)
				require.Equal(t, traditionalCheckTitle, gh.checks[1].Output.Title)
				require.Equal(t, pipelineExternalID(refs), gh.checks[1].ExternalID)
				require.Equal(t, refs.Pulls[0].SHA, gh.checks[1].HeadSHA)
				require.Equal(t, "completed", gh.checks[1].Status)
				require.Equal(t, "success", gh.checks[1].Conclusion)
				require.Contains(t, gh.checks[1].Output.Summary, "comment 500")
			default:
				require.NotEqual(t, "success", gh.checks[1].Conclusion)
			}
			require.Zero(t, gh.statusWrites)
		})
	}
}
