# PostgreSQL queued activity owner crash recovery — 2026-10-30

## Scope

Supplemental acceptance-harness change based on frozen production candidate
`caa32882f9a3fd1da4450c8b216607c053996893`. This patch is on
`t3/matching-queued-activity-crash-recovery`; it does not move or amend the
frozen candidate. It closes the gap recorded in
`matching-harness-acceptance-audit-20261030.md`: the PostgreSQL two-instance
SIGKILL scenario covered a queued decision task, while the separate activity
backlog restart probe did not crash the activity's owner.

## Added scenario and assertions

The existing PostgreSQL production failure-recovery path in
`tests/matching-service-compat.cjs` now adds a PostgreSQL-only activity case
after its decision-task owner crash/recovery case. It keeps the existing Go
FrontEnd, History, and Worker services and uses two Rust Matching members; the
baseline still starts no Go Matching.

The seed phase runs the existing Go SDK. Its worker polls a dedicated workflow
decision list and runs `ActivityCrashRecoveryWorkflow`, which schedules
`EchoActivity` onto a separate activity task list. No worker polls that activity
list during seeding. The harness waits (bounded at 45 seconds) until PostgreSQL
reports a durable `task_type=1` row for the activity list, so the crash point is
based on persisted queue state rather than a delay.

Before killing a member, the harness asks both Rust Matching endpoints for
`ListTaskListPartitions` and requires their activity partition owner to agree
and identify one of those two processes. It SIGKILLs that owner and waits up to
45 seconds for the surviving Rust member to report ownership of the activity
partition. The recovery phase starts existing Go SDK workers on the workflow
and activity lists through the configured Go Frontend TChannel endpoint. It
asserts the SDK result is `activity-crash-recovered`, exactly one activity
completion event and one workflow completion event are in history, and
PostgreSQL reports zero queued activity rows within 30 seconds. It then
restarts the killed Rust member and requires the two Rust activity ownership
views to reconverge and a Go Frontend activity-list describe to succeed within
30 seconds.

The seed process is terminated as a process group on normal completion and
failure paths. The seed queue observation, owner convergence, SDK recovery,
post-recovery queue observation, and member restart all have bounded waits.
Existing scenarios are unchanged; this adds a distinct PostgreSQL-only case.

## Validation status and required evidence

No local Cargo, Go, formatter, runtime, or CI validation was run, per task
constraints. This is source work, not passing mixed-cluster evidence.

After review/integration, run the existing immutable PostgreSQL production
validation lane against a fresh Matching binary built from the integrated
source. Review its artifact for the new
`production-activity-crash-recovery.json`, both SDK logs, nested gate exits,
the binary SHA256, and the baseline topology fields showing `goMatchingStarted:
false` and Go services `frontend,history,worker`. Record the run URL, head SHA,
binary SHA256, and conclusion here. A passing adapter test or Rust CI run alone
does not validate this scenario.
