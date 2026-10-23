# Cassandra lane: `DescribeTaskList must report the reported backing rate` (100000 !== 1)

Diagnosis only (code reading; no cargo, no docker, no gate runs). Branch `matching-cass-rate-diag-20261024`
from `origin/matching-gate-final3-20261024` (41b680a0f). No production or test code changed.

Failing assertion: `tests/matching-service-compat.cjs:1168`
(`assert.equal(pacedDescribe.taskListStatus.ratePerSecond, 1, ...)`), run in
`memory/evidence/matching-gate-final3-20261024/4-cassandra/` (`run.exit` = 1, `evidence/probe.log`).

## Verdict

**Not a Rust divergence in how the rate is stored or reported.** The describe was answered by a
different in-memory owner of the `paced-*` task list than the one that applied the poll's rate.
The second owner loaded the list fresh, so its limiter had the default 100000. The probe assumes a
single owner and never checks that the ring agrees before or after the poll/describe pair.

- Classification: harness/topology precondition (ring ownership of a new list moved or disagreed between
  the poll and the describe), not a Rust-vs-Go semantic gap in the rate path.
- Confidence: high on the mechanism (two in-memory owners, fresh one answered the describe). Moderate on
  the trigger: the ring change itself is not explained by any log in the evidence.
- Why only Cassandra: **not established.** The Cassandra and MySQL two-node lanes run the same probe code and
  the same flags, so the lane-specific difference is timing. This is a hypothesis, not a finding.
- No reproducer test added: no Rust-vs-Go divergence was shown, so the conditional in the brief does not apply.

## What the probe does (the failing section)

- `tests/matching-service-compat.cjs:1142-1152`: start a workflow on `paced-${id}`, poll the decision, schedule 3 activities.
- `:1156-1158`: three activity polls, each via `WorkerAPI` (Go Frontend), each with `taskListMetadata.maxTasksPerSecond = 1`.
- `:1166`: cumulative latency of polls 2..3 must be >= 500 ms. **This passed**, so the dispatching node's limiter had rate 1.
- `:1167`: `DescribeTaskList(includeTaskListStatus)` via `WorkflowAPI` (Go Frontend).
- `:1168`: `ratePerSecond` must equal 1.
- Grep of `1142-1168` for `directPartitionOwner` returns 0: no ring-agreement check for the paced name.
  Compare the checks that do exist at `:595-615` (ringpop convergence, 64 names), `:1063-1080`
  (rejoin, 16 names, secondary-restart branch only) and `:1041` (drain owner).

## Rust path (rust/crates/cadence-matching/src)

- Poll applies the worker rate on the serving node: `grpc_matching.rs:1954-1975` reads
  `max_tasks_per_second`, calls `poll_rate` (`:1070-1127`), which calls `queue.limiter.report_limit(rate)`.
  The override runs first, then the worker rate, else `TASK_DISPATCH_RPS` (`:1127`).
- Report rule: `cadence-common/src/matching_rate.rs:39-70`. A lower value always wins. With the default
  100000 this accepts 1. Matches Go `task_list_limiter.go:66-90`.
- Describe reads that same limiter: `grpc_matching.rs:3343-3400` (describe entry, `durable_queue` at `:3380-3383`),
  then `:3459` `rate_per_second: queue.limiter.limit()`.
  Go equivalent: `task_list_manager.go:785-796` (`RatePerSecond: c.limiter.Limit()`).
- Default for a freshly created owner: `matching_limiter.rs:21` `TASK_DISPATCH_RPS = 100000.0`.
- Queue lookup: `grpc_matching.rs:1280-1310` (`durable_queue`) calls `check_task_list_ownership`, then
  `queues.get` (`matching_registry.rs:967+`). Registry `get` calls `check_task_list_ownership`
  (`matching_registry.rs:1008`). In ringpop mode (no ownership policy) that is
  `membership.check_task_list` **unconditionally** (`matching_registry.rs:777-790`), with no cache shortcut.
  `check_task_list` is strict (`matching_membership.rs:228-240`): a non-owner gets `ABORTED`
  (`task_list_not_owned_by`, `:241-255`).
- Consequence: a describe succeeds on node B only if B's ring says B owns the list **and** B holds no queue
  for it, which forces a fresh load and a default limiter. The same holds for poll on node A.

## Go path (go/service/matching, go/client/matching)

- Poll routing: `client/matching/client.go:133-152`. Uses `loadBalancer.PickReadPartition`, then
  `peerResolver.FromTaskList(partitionName)`.
- Describe routing: `client/matching/client.go:281-291`. Uses `peerResolver.FromTaskList(rootName)`.
  These land on the same host when the read-partition count is 1 (the lane sets no partition config; see below).
- Owner side: `handler/engine.go:1133-1156` (describe), `:1150` `getOrCreateTaskListManager`,
  `:315-331` (cached shortcut for excluded names, which skips the ring check), `:1600-1660`
  (`errIfShardOwnershipLost`, the ring check for non-owners).
- Go's `DescribeTaskList` also reports `limiter.Limit()` of the manager it loaded. A freshly loaded
  manager reports 100000 too, so Go would fail this assertion under the same split.

## Evidence for the two-owner split

- `4-cassandra/evidence/rust-server-logs/rust-server-secondary.log:7`:
  `Matching task list lost ownership during shutdown: ... name: paced-20968af5-..., type: 1, rangeID: 1, columns: (range_id=2)`.
  The secondary still held the paced queue at range 1 when it shut down. The DB was at range 2, so another node
  loaded the same list and bumped the range.
- Primary-side logs (`rust-server-before-restart.log`, `after-graceful-drain.log`, `after-restart.log`,
  `after-semaphore-release-recovery.log`) contain no `paced-` line. Consistent with the primary loading the
  list fresh (range 2) and releasing it cleanly.
- The poll's rate was applied on the node that dispatched (`:1166` passed). The describe was answered by the
  node that loaded the list second (range 2, default limiter). Range 1 was the first load, so the second
  load happened after the poll, most likely at the describe.
- The secondary never unloaded the paced queue before shutdown, so its membership watcher did not drop it.

Inferred timeline (consistent with all of the above; timestamps are not in the evidence):

1. Node A (secondary) loads `paced-<id>` at range 1 and serves the three rate-1 activity polls (`:1156-1166`).
2. Between the last poll (`:1158`) and the describe (`:1167`), ring ownership of the list moves to, or is
   seen by, node B: B's ring says B owns it, and A's cached queue is not unloaded.
3. B loads the list fresh (range 2, limiter 100000) and answers the describe (`grpc_matching.rs:3459`).
4. Go Frontend routes the describe to B, so Frontend's ring view also changed between the two calls, or the
   two calls resolved differently.

What would change the picture: a ring change in the window, or a Frontend/Rust ring disagreement (ringpop
gossip). Neither is recorded in the evidence. Both the probe and the MySQL run share this structure. The
trigger is therefore an unverified timing/membership event, and the leading candidate is the Cassandra lane's
resource load (Cassandra container plus Go services on the 3-core cap) delaying ringpop state. This is a
hypothesis, not a finding.

## Hypotheses checked against the brief

| Hypothesis | Result |
|---|---|
| Describe answered by a different node than the poll; rate per node | **Confirmed as the mechanism** (two in-memory owners, evidence above). Caused by ring change or disagreement, not by the rate code. |
| Go keeps rate in the task-list manager on the owner | True in both: `task_list_manager.go:722/794`, `grpc_matching.rs:1967/3459`. Parity. |
| Poll rate stored only for non-forwarded polls | **Ruled out.** `poll_rate` (`grpc_matching.rs:1967`) runs before the `forwarded` flag is read (`:1981`). |
| Cassandra lane routes differently | **Not supported.** Same wrapper and harness, same Go client and Frontend code. Only the DB backend and two dynamic-config values differ (below). |
| Partition or owner hashing differs | **Ruled out for the lane config.** Both routes use the same name when read partitions = 1. The lane sets no partition overrides (`run-go-service-baseline.py:496` applies only with `--probe-list-partitions`). The Rust default for read partitions was not re-read; treat as an assumption. |
| Idle-unload drops the queue between poll and describe | **Unlikely.** Default idle interval 300 s (`grpc_matching.rs:687`, `matching_dyncfg_defaults_tests.rs:49`). Window is milliseconds. |

## Lane config differences (MySQL two-node vs Cassandra)

From `command.txt` and `config-redacted/` in `gate-final2/2-mysql-two-node` vs `gate-final3/4-cassandra`:

- MySQL two-node ran with `--matching-idle-check-seconds 3` (`idleTasklistCheckInterval: 3s`).
  Cassandra ran with the default 300 s. A shorter idle sweep would evict stale cached owners sooner,
  so this may narrow the split window. Unverified.
- Cassandra dynamic config sets `system.enableDistributedSemaphore: true`. MySQL does not.
- Both lanes run `--matching-restart --matching-failure-recovery` (Cassandra sets these implicitly,
  `run-go-service-baseline.py:364-367`), and both use `--history-transport grpc`.

Neither difference is a direct cause of a routing change, so these are context only.

## Harness gap (the actionable part)

The paced section starts with the ring unsettled in one respect. The drain branch restarts the primary
(`compat.cjs:1057-1058`, `waitForRustReady` only). The rejoin ring check exists only in the secondary
branch (`:1063-1080`). The probe then runs `go run` (the drain SDK, up to 120 s) and the semaphore and idle
sections before the paced section. It never checks that both Rust nodes agree on the owner of
`pacedList.name` before the poll, and never re-checks after the describe. A split is therefore
indistinguishable from a limiter bug.

## Proposed fix (do not apply; harness only)

1. Before the paced poll (`:1142`), wait until both Rust nodes report the same owner for `pacedList.name`,
   using the existing `directPartitionOwner` pattern (`:595-615`, `:1041`, `:1069`). Record the owner.
2. Re-check owner equality immediately before the describe (`:1167`). On mismatch, fail with
   "paced list owner changed during the rate window" and dump both nodes' owner and rangeID. Do not fail
   with the rate assertion.
3. Optionally assert the limiter on the node that served the poll (direct `MatchingAPI` describe to that
   node's gRPC port), and keep the Frontend assertion only when owners agree.
4. Add per-node owner and rangeID dumps for the paced list to the evidence directory.

Open items (need a gate run, not reading):

- Timestamped membership events for the paced window, from both Rust nodes, to confirm the ring change.
- Go Frontend's ring view at the same moments, to rule out a Frontend/Rust disagreement.

## Side note (not causal here)

- Go skips the ring check for a cached, excluded (UUID-named) manager: `engine.go:323-325`.
  Rust's registry always ring-checks in ringpop mode (`matching_registry.rs:777-790`). This is a possible
  parity difference (Rust stricter). It would matter only if a stale cached manager served a request.
  It does not explain this describe. Config for `ExcludeShortLivedTaskListsFromShardManager` was not checked.

## Ruled out or not verified

- Not verified: Rust default for `matching.numTasklistReadPartitions` (`grpc_matching.rs:438/1181`); assumed 1.
- Not verified: whether `ExcludeShortLivedTaskListsFromShardManager` is set in the lane (affects the side note).
- Not run: cargo, docker, gate. No code changed; this file is the only change in this commit.
