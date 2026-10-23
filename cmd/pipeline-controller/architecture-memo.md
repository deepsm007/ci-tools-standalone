# Agentic dispatch handoff

Agentic Handoff is a closed beta available to a limited set of enrolled repositories.

Proposal: pipeline-controller owns first-stage readiness and the merge gate in
agentic mode. Chai owns second-stage execution unless a response timeout or
`/pipeline skip-agentic-mode` hands dispatch back to the traditional workflow.

## Workflow

- After applicable first-stage tests pass, create `ci/pipeline-gate`
  pending (`queued`). In LGTM mode, also require the current `lgtm` label or
  `pipeline-auto` override before creating readiness or starting its response timeout.
  Either condition may arrive first. A trusted user's `/pipeline required` or
  `/pipeline remaining` creates readiness immediately, even before first-stage success
  and without LGTM. Both commands hand off to Chai; traditional mode keeps its existing
  force-set and missing-tests dispatch behavior.
  Repeated manual commands, first-stage completion, later approval and automatic events
  reuse the same native gate for this PR HEAD SHA, including after completion or restart.
  They do not reopen completed gates or refresh response timeouts. New SHAs can start a
  new handoff under the same manual or automatic rules; mode-switch and retarget ownership
  rules still apply. Removing approval preserves an existing gate and its timer.
  Successful readiness dispatch starts a 20-minute wait for Chai.
- Any new comment from a configured Chai author during the wait cancels the response timeout,
  but leaves the gate pending. No message within 20 minutes hands second-stage
  dispatch to the traditional workflow; the gate turns green only after dispatch succeeds.
  No acknowledgement command or HEAD is needed. Old comments do not cancel a new wait;
  late comments do not reclaim dispatch after timeout fallback or opt-out.
- Chai owns second-stage selection and dispatch. It ensures selected job
  contexts block merging before marking the gate; success does not mean
  tests passed. Pipeline-controller neither reads plans nor verifies job lists.
- `/pipeline mark-pipeline-gate [<HEAD>]` completes the active check, including
  running checks. Trusted organization members and repository collaborators may
  also create a missing completed/success gate for older enrolled PRs. Agentic
  completion persists with agentic ownership and suppresses fresh readiness or
  timeout fallback for that revision, including after restart. Marking neither
  dispatches tests nor writes job contexts. Configured Chai must still have an
  existing active readiness gate and satisfy timing and ownership checks; it
  cannot backfill missing gates. Reject a
  supplied HEAD that differs from the current PR HEAD; omitted HEAD targets
  current HEAD when processed. Chai should supply HEAD to avoid repush races.
  New HEAD/base requires fresh readiness and completion.
- `/pipeline skip-agentic-mode` opts this PR into the existing traditional
  auto/manual/LGTM workflow. `/pipeline agentic-mode` restores agentic review
  only when repository/branch configuration permits it. Completed dispatch for
  the current revision is preserved; agentic readiness resumes on the next
  HEAD/base. An incomplete dispatch gets fresh readiness once first-stage tests pass
  and the LGTM trigger, if configured, is satisfied.
  Traditional rerun commands retain current-revision ownership across this deferred
  restore and restart; the successful native check records that ownership.
  Reopening that check retains historical ownership in its rerun title, even if the
  latest rerun fails; current status still reports failure, not historical success.
  Already-agentic PRs receive confirmation only: no gate reset, timer restart or new analysis.
- Persist opt-out as a PR label across pushes/restarts. Mode changes do not
  cancel running jobs; restoring agentic mode preserves completed dispatch.
  Traditional dispatch still completes the renamed check automatically.
- Keep successful command mutations separate from best-effort confirmation replies;
  failed confirmation must not replay an old opt-out over a newer label choice.
- Check runs are commit-scoped, not PR-scoped. Shared-SHA ownership conflicts stay
  fail-closed until merge-gating policy can safely account for all affected PRs.

## Remove

- Chai plan parsing, job-list validation, frozen selections and review-request correlation.
- Agentic second-stage scheduling, context publication, rerun/delta planning and dispatch-comment deduplication.
- Per-PR JSON journals, schema handling, filesystem locks, retention cleanup,
  disk-backed witnesses, PVC dependency and state-directory/TTL flags.
- Old check/command names, obsolete agentic command handlers and their tests.

## Keep and rollout

Reuse first-stage/ProwJob-cache logic, traditional scheduling, owned-check helpers,
command authorization, trusted-Chai response detection, response timeout and
bounded API retries. Recover through ordinary events and commands; no startup
PR scan or polling. Timers and timeout fallback may be lost on restart (accepted for now).
Owned completion checks and opt-out labels remain; missed events may require another event/rerun.

Update Chai for the new pending-check creation signal and mode handoff. Require
the new check in Tide/branch protection on enrolled branches; retire the old
`ci/tests-dispatched` requirement and pending checks. Update help and deployment flags together.
Remove state-directory/TTL deployment arguments and the unused PVC mount; there are no flag aliases.
Keep one active controller replica with `Recreate`; no leader election is added.
