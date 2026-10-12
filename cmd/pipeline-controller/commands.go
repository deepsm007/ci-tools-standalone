package main

import (
	"regexp"
	"strings"
)

const pipelineHelp = "Pipeline commands:\n\n" +
	"- `/pipeline help` — list commands.\n" +
	"- `/pipeline required` — rerun the traditional second-stage set.\n" +
	"- `/pipeline remaining` — run missing second-stage tests.\n" +
	"- `/pipeline auto` — enable automatic dispatch in LGTM mode.\n" +
	"- `/pipeline agentic-mode` — restore configured agentic mode; completed dispatch and traditional rerun commands stay unchanged until the next HEAD SHA.\n" +
	"- `/pipeline skip-agentic-mode` — use traditional scheduling for this PR, including future pushes.\n" +
	"- `/pipeline mark-pipeline-gate [<HEAD>]` — mark `ci/pipeline-gate` green; omitted HEAD means current HEAD. Does not run tests or override results."

var (
	pipelineHelpRE    = regexp.MustCompile(`(?im)^/pipeline[\t ]+help[\t ]*$`)
	pipelineCommandRE = regexp.MustCompile(`(?im)^/pipeline[\t ]+(required|remaining|auto|agentic-mode|skip-agentic-mode|mark-pipeline-gate)(?:[\t ]+([0-9a-f]{40}))?[\t ]*$`)
)

type pipelineCommand struct{ name, head string }

func normalizePipelineComment(body string) string {
	return strings.ReplaceAll(body, "\r\n", "\n")
}

func parsePipelineCommand(body string) (pipelineCommand, bool) {
	matches := pipelineCommandRE.FindAllStringSubmatch(normalizePipelineComment(body), -1)
	if len(matches) != 1 {
		return pipelineCommand{}, false
	}
	command := pipelineCommand{name: strings.ToLower(matches[0][1]), head: strings.ToLower(matches[0][2])}
	if command.head != "" && command.name != "mark-pipeline-gate" {
		return pipelineCommand{}, false
	}
	return command, true
}

type pipelineCommandClient interface {
	IsMember(org, user string) (bool, error)
	IsCollaborator(org, repo, user string) (bool, error)
}

func trustedPipelineCommandAuthor(gh pipelineCommandClient, org, repo, login string) (bool, error) {
	if login == "" {
		return false, nil
	}
	member, err := gh.IsMember(org, login)
	if err != nil || member {
		return member, err
	}
	return gh.IsCollaborator(org, repo, login)
}
