# PostgreSQL queued activity owner crash recovery — 2026-10-30

## Scope

Supplemental acceptance-harness change based on frozen production candidate
`caa32882f9a3fd1da4450c8b216607c053996893`. This patch is on
`t3/matching-queued-activity-crash-recovery`; it does not move or amend the
frozen candidate. It closes the gap recorded in
`matching-harness-acceptance-audit-20261030.md`: the PostgreSQL two-instance
SIGKILL scenario covered a queued decision task, while the separate activity
backlog restart probe did not crash the activity's owner.

Supplemental review patch: `t3/matching-pg-observer-fix`, commit
`f07ef0a37fa391cc1492d4e2563df9ef95e50068`, based on original harness commit
`3a1da7b06bdbec4ddaa5858f003f4f986870540c`.

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

Review correction on the supplemental branch: Go's persistence enum is
`TaskListTypeDecision=0`, `TaskListTypeActivity=1` (`go/common/persistence/data_manager_interfaces.go`),
matching Rust's persistence `task_type` values. The PostgreSQL observer in
`durableTaskCount` already interpolates its `taskType` argument into both the
durable-row and task-list-range queries; the activity caller passes `1`, while
legacy calls default to decision type `0`. That reported PostgreSQL hardcode was
not present in commit `3a1da7b06bdbec4ddaa5858f003f4f986870540c`.

The cross-backend observer did have a Cassandra gap: it discarded the optional
type and the Go CQL helper queried decision rows unconditionally. The observer
now forwards the type; the helper accepts `0`/`1`, defaults older invocations
to decision type `0`, and uses the selected type for both row and range reads.
The observer self-check asserts the activity argument and decision default.
The activity-owner restart path checks both Rust endpoints' activity partition
views converge and confirms a Go Frontend activity-list describe after either
primary or secondary owner restart. A single common nonempty owner for one key
does not establish that the restarted process rejoined the owner set.

## Independent supplemental review

Reviewed `3a1da7b06bdbec4ddaa5858f003f4f986870540c` and supplemental head
`01bf00dd04d72f131e9d1a527d83e2904783bf4b` (branch
`t3/matching-pg-observer-fix`). Confirmed the reported PostgreSQL task-type
hardcode was not present: the PostgreSQL `durableTaskCount` path already binds
its `taskType` into both task row and range row queries. The Cassandra helper
did discard the activity type; supplemental fix forwards it to both CQL
queries and preserves decision type 0 as the legacy default.

Review found and fixed two assertion gaps in this branch: the activity probe
now requires a positive activity-list range ID before owner selection/kill,
and the recovered history requires exactly one scheduled and started activity,
zero activity timeout/failure events, and exactly one activity/workflow
completion. This prevents an activity timeout/retry followed by eventual
success from satisfying the recovery assertion. These are source changes only;
they have not been run locally or in CI.

The JS observer self-check does **not** execute the Go Cassandra observer or
exercise either CQL query (including the range query). It only asserts that the
JS adapter forwards types 0 and 1. The production mixed gate must therefore
show the activity pre-kill `range_id > 0` and one observed durable activity row;
that live assertion exercises the Cassandra range read for the activity type.

The supplemental follow-up replaces that weak check with bounded activity
partition ownership evidence. After restart it probes 64 deterministic
activity task-list names through both direct Rust `ListTaskListPartitions`
clients, requiring every owner to be nonempty and equal across the endpoints,
and requiring the restarted endpoint address itself to own at least one of the
64 partitions. It also retains the Go Frontend activity `DescribeTaskList`
check. The production evidence JSON records each key and both observed owners,
the restarted address, and the assertion description. This establishes that
the restarted endpoint participates as a published activity partition owner
in both views; it is not a dump of the full SWIM member set and must not be
reported as exhaustive membership proof.

Forced cleanup preserves the original probe failure: the outer `probeFailure`
is rethrown after shutdown, and a shutdown timeout logs the forced SIGKILL
without replacing that original failure. The activity seed worker is also
asserted to have remained alive until intentional process-group SIGKILL after
the recovery SDK succeeds.

Required CI: run the active production PostgreSQL mixed gate only after its
owner finishes scheduling, against the committed supplemental source. Inspect
its activity row/range before kill, actual SIGKILL signal, survivor owner,
activity history event counts (including zero timeout/failure), one SDK result,
one activity/workflow completion, post-recovery queue drain, and binary SHA256.
The owner-convergence assertion source fix has not been executed. Inspect the
64 recorded post-restart keys, each observed from both Rust endpoints, and
require at least one owner pair to equal the restarted endpoint address. The
gate should fail if only the survivor remains an activity partition owner.
Do not claim full Ringpop membership from these partition samples. No local
Cargo, Go, formatter, runtime, or CI action was taken by this review agent.

No local Cargo, Go, formatter, runtime, or CI validation was run, per task
constraints. Existing workspace evidence is reported as **1,829 passed, 28
ignored**; it is prior workspace validation and does not validate this source
change. Active PostgreSQL production gate **41034380907** remains **pending** for
this supplemental source review; it targets the separate frozen candidate
`caa32882f9a3fd1da4450c8b216607c053996893`, not this branch.

After this patch is integrated and the active lane is finished, run the
existing PostgreSQL production validation lane against a fresh Matching binary
built from the integrated source. Review its artifact for the new
`production-activity-crash-recovery.json`, both SDK logs, nested gate exits,
the binary SHA256, and the baseline topology fields showing `goMatchingStarted:
false` and Go services `frontend,history,worker`. Record the run URL, head SHA,
binary SHA256, and conclusion here. A passing observer fixture or Rust CI run alone
does not validate this scenario.
