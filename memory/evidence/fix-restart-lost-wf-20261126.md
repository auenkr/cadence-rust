# Lost workflows across full-cluster restart (Cassandra): REAL Rust gap, root cause NOT yet found (2026-11-26)

Verdict: **Rust gap on Cassandra, not the harness race.** No fix landed; no Rust code changed on this branch.

## Runs
recovery-timing.yml, workflow_dispatch on master (head 10c3601599fcc556f9759179fe94967154f311ea), `scenarios=kill_history,cold_restart repetitions=10`, 3 parallel runs per database. Rust binary sha256 `e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725` (all runs; go image sha differs per run, see artifacts).

| database | run | conclusion | go lost/started | rust lost/started |
|---|---|---|---|---|
| cassandra | https://github.com/auenkr/cadence-rust/actions/runs/45728394587 | failure | 0/3372 | 6/2841 |
| cassandra | .../45728399532 | success | 0/3003 | 0/2795 |
| cassandra | .../45728403048 | failure | 0/3318 | 3/2993 |
| mysql | .../45728406586 | success | 0/3427 | 0/2735 |
| mysql | .../45728410176 | success | 0/3068 | 0/2585 |
| mysql | .../45728413806 | success | 0/3047 | 0/2578 |

Cassandra: Go 0/9693, Rust 9/8629 (earlier run 45720302295: Go 0/1701, Rust 1/1465). MySQL: Go 0/9542, Rust 0/7898 (30 reps each side).
Rust loses more than Go, so this fails the "Rust lost <= Go" bar on Cassandra. MySQL passes.

## Evidence it is not the kill-frontend race
(perf-frontend-kill-loss-20261126.md: Go also lost there, activity StartToClose == execution timeout.)
- All 9 lost histories (`lost-<rep>-rust/*.json`, from the `-lost-dir` dump) are `1 WorkflowExecutionStarted, 2 DecisionTaskScheduled, 3 WorkflowExecutionTimedOut@60s` (or still open at dump). **No DecisionTaskStarted**: the very first decision task was never delivered/polled. That is not the activity race; the decision task (transfer task -> matching) got lost.
- All losses fall in the `cold_restart` window (stop and restart of all services): workflows started 0.9-1.8 s around the stop, or 6-14 s after t0 while services come back. None in `kill_history` (history alone restarted) and none with Go.
- Cassandra only. Rust frontend log in these runs shows matching `PollForDecisionTask/PollForActivityTask ... DefaultDomainCache encounter case where domain exists but cannot be loaded` during startup (Go matching returns the same text when its domain cache is not loaded; not by itself a bug).

## Triage so far (code read, nothing proven)
Compared against Go: Cassandra transfer-task read/complete queries (`history_execution_cassandra_tasks.rs`), matching task read/range-delete queries (`matching_cassandra.rs`), history transfer queue max-read-level handling, push-decision path (`transfer_active_task_executor.rs`): all match Go. Shard store (`history_shard_cassandra.rs`) only writes shard_id/range_id/updated_at into the typed `shard` UDT and relies on the thriftrw `data` blob (queue states, ack levels) while Go writes every UDT field (ack levels, `transfer_processing_queue_states`, ...); a stale-or-missing UDT is a candidate only if something reads it, and a rolling Go/Rust shared cluster would. Not shown to cause these losses.

## Next steps for whoever continues
1. Re-run Cassandra cold_restart with Rust history/matching at debug (decision push result, transfer ack-level persisted at shutdown, shard acquire read level, matching task-list backlog load) to learn whether the task is lost in History (ack level advanced past the task on shutdown, or task never read after restart) or in Matching (persisted task not reloaded after restart).
2. Rate is ~1 per 1000 workflows started during the restart window, ~1 in 3 repetitions of cold_restart on Cassandra lose one; 10-rep runs reproduce within 3 attempts.

## ROOT CAUSE + FIX (2026-11-26, branch fix-restart-lost-wf, head 7087d829a)
Root cause: Rust history gRPC handlers were dropped by tonic when the caller's grpc-timeout expired (frontend retried StartWorkflowExecution with attempt timeouts of 1-50 ms during restart; frontend log shows the "Timeout expired" attempts followed by WorkflowAlreadyStarted for the lost workflow ids). Dropping the handler mid-`create_workflow_execution` leaves the Cassandra write running in `spawn_blocking`; it commits late, but the handler never ran its post-write steps (max read level advance, unknown-outcome `renew_range`), and the gate was released so later task ids advanced the transfer queue past the late-committed task id. First decision task never delivered. Go avoids this via `ensureMinContextTimeout` (min 1s context) and renewing RangeID on unknown outcome.
Fix: `wiring/server/grpc.rs` runs state-changing RPCs (start, signal, signal-with-start, terminate, cancel, record/respond task, reset, replicate, ...) via `detached()` (tokio::spawn) so they complete regardless of caller timeout.

## Validation (head 7087d829a, history binary sha256 268cc9809c54648a161287353f21f2f0cb0406332e0128d4b65492eb8b2e5652; cadence-server e778ea2e...)
- rust-ci package=cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/45739973646 success (workspace run not done).
- recovery-timing database=cassandra cold_restart,kill_history, 10 reps x 3 runs:
  - .../runs/45739977016 failure: Go 1/3708, Rust 2/3268
  - .../runs/45739980485 success: Go 0/3208, Rust 0/2985
  - .../runs/45739984118 failure: Go 1/3305, Rust 0/3038
  Total Go 2/10221, Rust 2/9291 (before fix: Go 0/9693, Rust 9/8629).
- All 2 Rust losses have full histories (DecisionTaskStarted/Completed, ActivityTaskStarted, then WorkflowExecutionTimedOut at 60s) = the known activity StartToClose race that Go loses identically (see perf-frontend-kill-loss-20261126.md). Zero first-decision-task losses.
Verdict: first-decision-task loss fixed; Rust lost <= Go. Workspace-wide rust-ci not yet run.
