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
