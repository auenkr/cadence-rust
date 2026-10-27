# Matching continuation checkpoint, 2026-10-27

Matching remains incomplete. Continue Matching before any History migration.
This checkpoint supersedes unsupported retry-timing claims in worker notes.

## Verified state

- Master was clean at `052281dcc`; origin/master matched.
- Immutable candidate `matching-acceptance-20261027-v3` points to
  `ff3b638b49190f7653c2de3b062232d7ccc57f07`.
- Fresh uncached workspace CI run `40467005617` passed on that candidate;
  compact proof is in `memory/evidence/matching-candidate-v3-40467005617/`.
- PostgreSQL mixed run `40468419712` is in progress on v3. Build completed;
  runtime validation was active at the checkpoint. Inspect artifacts and binary
  SHA256 before crediting it. No second mixed gate may run concurrently.
- Adaptive diagnostic run `40465991711` failed after the isolated retry's
  approximately 60-second budget. Its retained history has four durable events.
  This does not establish the failing delivery boundary or a harness defect.
- Cassandra normalization `e2f2bd5cd` remains HELD, despite focused CI and the
  initial review: the Go cache uses raw string keys after persistence lookup.
  Await the original-Go caller oracle before changing cache behavior.

## Critical retry correction

`go/service/history/execution/mutable_state_task_generator.go:363` assigns
`getNextDecisionTimeout(...)` to `startToCloseTimeout`. The one-minute initial
retry value is an already-started decision attempt's timeout, not a delay before
scheduling its retry. Do not infer that History intentionally waits a minute
before re-enqueueing from this constant. Trace failure, scheduling, transfer,
Matching Add, and Poll separately. Preserve the existing SDK completion budget
and retry assertions until actual evidence supports a change.

The retry worker was sent this correction. Its current instrumentation branch
`t3/c5d6474c` head `7d072c25422c165c92e6912d4210de7b8bb741a4` has focused CI
`40468304743` in progress. Its evidence note's prior scheduling-delay diagnosis
must be corrected before integration. Instrumentation alone is not a fix.

## Existing worker pool: do not duplicate

Three normal-speed gpt-6-luna, medium-reasoning workers already exist:

1. Retry boundary: delegated task `b277d41e-aa96-4ae2-928b-1178299e68dd`,
   worktree `t3-c5d6474c`, branch `t3/c5d6474c`.
2. Go domain-cache caller oracle: `3247c82b-f3e4-436a-8b5d-9cf610f68ac3`,
   worktree `t3-567c2eeb`, branch `t3/go-domain-cache-oracle`.
3. Compact evidence collector: `9153bc17-545e-4ce7-9003-e2ef5fff071e`,
   worktree `t3-4c27d162`, branch `t3/matching-evidence-collector`.

The latter two had little recorded activity after their initial work, and both
received checkpoint requests at 07:40 UTC. Check actual activity and preserve WIP
if the 60-minute inactivity threshold is reached; do not leave stuck workers
running indefinitely. Do not poll delegated task status; completion wakes parent.

## Remaining acceptance

Resolve retry diagnosis and cache parity evidence, integrate only justified
changes, then freeze the final source revision. The final acceptance ledger
requires fresh CI and ten serial mixed lanes with SDK completion, backend,
transport, discovery, restart/drain/crash-recovery evidence and binary SHA256.
Older successful runs support development but do not establish final-pin proof.
Provisional user-facing estimate: 6–12 hours if no additional defects emerge;
1–3 days if retry diagnosis requires fixes and repeated validation. This is an
estimate, not a completion promise or a defensible percentage of code remaining.

No local Cargo or Go validation is allowed. Use only public-repo standard Linux
Actions runners and keep evidence compact. Resource check at resume: load 0.54,
approximately 4.1 GiB available RAM; no local heavy workload was started.

## Follow-up at 07:50 UTC

PostgreSQL run40468419712 succeeded; inspected runtime results, compact proof in
`memory/evidence/matching-postgres-v3-40468419712/`. Its recorded preliminary and
acceptanceEligible:false labels are preserved. This closes one candidate lane,
not full service acceptance.

Diagnostic tag `matching-retry-trace-20261027-v1` pins instrumentation source
`7d072c25422c165c92e6912d4210de7b8bb741a4`. Focused CI40468304743 succeeded.
Adaptive retry-only mixed diagnostic40469125507 is now running on that tag.
The worker completed at34346568b with memory-only follow-up corrections; source
instrumentation matches the tested pin. The retry-timing error was corrected.

The cache caller-oracle draft was checkpointed/pushed as1a91d94f3 on
t3/go-domain-cache-oracle, explicitly unverified. The stalled cache and collector
tasks are being cancelled/replaced; do not treat their old running labels as
active useful work or modify their preserved branches speculatively.
