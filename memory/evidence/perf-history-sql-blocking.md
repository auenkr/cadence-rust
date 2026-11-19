# History SQL blocking: block_in_place vs bounded spawn_blocking (2026-11-19) — NO GAIN, no code change

Branch `perf-history-sql-blocking` from origin/master `27bf33105`. Evaluation only; no Rust change, so no CI/gate/bench runs were spent
(the before and after binaries would be identical; the latest master gates are the reference).

## Sites
- `cadence-persistence/src/history_sql_backend.rs` `blocking()` (merge 56b58c6c1): wraps `SqlBackend::transaction` and
  `autocommit_with_retries`; ~60 callers in history_task_sql, sql_workflow_store, sql_history_store, sql_execution_store,
  matching_domain_*, history_shard_sql (MySQL+Postgres shard stores).
- Older history wiring bridges (engine_ndc.rs, replication.rs, shard_events.rs, queue_exec.rs, priority_assigner.rs) use
  block_in_place + Handle::block_on for sync-over-async; not part of 56b58c6c1.

## Caveats re-checked
1. current_thread panic: already handled. `blocking()` checks `runtime_flavor() == MultiThread` and otherwise runs inline;
   the Postgres path also goes through `outside_runtime` (scoped thread). There is no panic path.
2. Worker stall: block_in_place hands the worker's core and its local queue to a new worker thread before running `f`, so
   other spawned tasks keep running. Only futures polled inside the *same* task (join!/select! siblings) wait. spawn_blocking
   would free those siblings, but the hot History paths await persistence one call at a time, so nothing changes in practice.
3. Bounding: `SyncConnectionPool::checkout` already caps open connections at `max_conns` (Go MaxConns) using a Condvar wait,
   so a semaphore sized to MaxConns only duplicates an existing bound.

## Why spawn_blocking is not worth it
- `SqlBackend::transaction/autocommit` are sync fns that take borrowing, non-'static `FnOnce(&mut dyn SqlTx)` closures.
  spawn_blocking needs `'static + Send`, so the change means making the API async, cloning every row/request into the
  closures, and touching ~60 call sites. That is a large diff with a parity risk.
- Both mechanisms run the same blocking OS thread work. block_in_place costs one worker handoff; spawn_blocking costs a
  pool dispatch plus a JoinHandle wakeup. Neither removes a ~200 ms delay.
- Measured evidence (perf-history-tail.md): going from inline sync SQL to block_in_place left Rust p99 unchanged
  (~195 ms vs Go ~110 ms at 25/s). Fully freeing the runtime made no difference, so runtime blocking is not the tail cause,
  and spawn_blocking only frees it differently.
- Async driver: mysql_async 0.37.1 is already a dependency (used for timed queries in SyncConnectionPool). Moving History SQL
  onto it would be a driver-behaviour change (error mapping, connection poisoning, retry semantics). It is out of scope and
  not justified by the evidence above.

## Verdict
No gain expected; left unchanged. The tail lead stays the fixed ~200 ms period (queue processor poll/ack interval,
matching AddDecisionTask retry backoff) listed in perf-history-tail.md.
Side note: Postgres `outside_runtime` spawns one scoped OS thread per call. It is a possible Postgres-only cost and does not
affect the MySQL benchmark lane.
