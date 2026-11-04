# History migration plan (started 2026-11-04)

Starting point: `rust/crates/cadence-history` is a stub (`not_migrated`). Go source:
`go/service/history` (~62k non-test lines). Rust Matching + Rust Frontend are
done and must be used in mixed gates (Go Worker stays, per skip-rust-worker).

Workers: up to 2 Codex Luna (simple = low/mid, hard = high, hard only to unblock).
All Cargo on CI (`gh workflow run rust-ci.yml --ref <branch> -f package=cadence-history`).

## Phases (each = branch `history-<phase>`, merged to master after CI green)
- H1 persistence-sql: shard store + execution store (create/get/update/conflict
  resolve/delete current+concrete, transfer/timer/replication/cross-cluster task
  CRUD + range-ack) for MySQL/Postgres, ported from go/common/persistence/sql +
  persistence/execution_manager*. History-v2 append/read/fork/delete.
- H2 skeleton: binary wiring (config, ringpop membership, Thrift+gRPC servers
  with every HistoryService RPC registered), shard controller (rangeID steal,
  ownership, shard close), history client routing by workflowID->shard.
- H3 mutable state + events: go/service/history/execution (mutable_state_builder,
  history_builder, state_builder), events cache, workflow cache, version histories.
- H4 engine API: decision/handler RPCs (Start, Signal, SignalWithStart,
  RecordDecision/ActivityStarted, RespondDecision/Activity*, Terminate, Cancel,
  Reset, Query, Describe, Poll mutable state, RemoveSignal, ResetSticky...)
  incl. decision checker/task handler (go/service/history/decision).
- H5 queues: transfer active/standby, timer active/standby, queuev2, task
  executors (matching push, child workflows, visibility, close execution), DLQ.
- H6 replication + NDC + failover + archival hooks + cassandra persistence.
- H7 mixed gate: Go Worker + Rust Matching + Rust Frontend + Rust History,
  Go SDK workflows (sample suite), restart/recovery, MySQL/Postgres/Cassandra;
  parity audit doc; then remove Go dependency.

## Status
- [ ] H1  - [~] H2 (skeleton merged; TChannel+shard recovery pending)  - [ ] H3  - [ ] H4  - [ ] H5  - [ ] H6  - [ ] H7

## Go package sizes (non-test lines) for task breakdown
execution 14167, task 8318, queuev2 5752, queue 5731, engine 5558, ndc 3952,
replication 3196, shard 3157, decision 3147, reset 732, events 631,
failover 575, query 372, workflowcache 320.

## Next slices (simple-worker sized, dispatch after H1/H2 merge)
- H3a execution/mutable_state_builder* (state maps, add*/replicate* events)
- H3b execution/history_builder + state_builder + events cache + workflowcache
- H4a decision/ (checker, task handler, attr validator)
- H4b engine/engineimpl start/signal/terminate/describe/query/reset
- H5a queue/ transfer+timer active executors; H5b standby + queuev2

## Progress 2026-11-04
- Merged: H2 skeleton (41787078112), H2b TChannel+shards (41790419418), H1a SQL shard stores (41789544521),
  H1 typed contracts + SQL impl guide (41793573331, memory/history-h1-sql-impl-guide.md), H3a mutable-state foundation (41791690758).
- Running: H1 SQL slice 1 (history-h1-sql-s1), H3a2 decision methods + CI fix (history-h3a2-decision-activity).
- Next: SQL slices 2-4, H3a3 activity, H3a4 timer/child/signal/cancel/completed, H3b builders/caches, H4, H5, H6, H7.
- Lesson: workers return early with partial work; require green CI on final head and narrow per-file scope.

## Paused 2026-11-04 (user request)
- Also merged: sqlblobs Go-compatible codecs (41798998970), H3a2 decision replicate transitions (41799001510),
  ActivityInfo sqlblobs adapter (41804173705).
- Cancelled when paused: decision Add*Event worker (branch history-h3a3-decision-x never pushed, no code saved; re-dispatch with same explicit brief);
  `history-h1-sql-s1b`, `history-h3a3-decision` (no code, can be deleted).
- Resume order: remaining 5 sqlblobs adapters (ChildExecution, Signal, RequestCancel, Timer, WorkflowExecutionInfo),
  then SQL slice 1-4 (memory/history-h1-sql-impl-guide.md), decision add-events (finish h3a3-decision-x), activity,
  timer/child/signal/cancel, builders/caches, H4-H7.
- Worker lesson: simple workers need explicit per-function step lists (see h3a3-decision-x brief style).

## STRATEGY CHANGE 2026-11-04 (user): full line-by-line port first, test at the end
Port all of go/service/history (~75k lines) to compilable Rust package by package; no per-slice integration testing.
Hard workers (Claude Sonnet 5.5 low / Codex 6.1 sol low), max 3 parallel. CI = clippy/compile green only until port complete; then gates (H7).
Work units (each a worker, own branch history-port-<unit>, own Rust module dir under rust/crates/cadence-history/src/<pkg>/):
 U1 execution (14k; mutable_state_builder*, history_builder, state_builder, workflow ctx, checksum, retry/cron) -- continues luna branches decision-y/activity
 U2 task (8.3k) + taskdlq(0.7k)  U3 queue (5.7k)  U4 queuev2 (5.7k)  U5 engine/engineimpl (5.3k) + engine + workflow + query + reset
 U6 ndc (4k) + events + workflowcache  U7 replication (3.2k) + failover  U8 shard (3.2k, partly done) + config + resource + constants + lookup
 U9 decision (3.1k)  U10 handler (2.4k) + wrappers (grpc/thrift/ratelimited) + main wiring
 Plus persistence: history_store/execution store slices 2-4 + cassandra (separate worker after s1d).
In flight (Luna, let finish): history-h1-sql-s1d, history-h3a3-decision-y, history-h3a4-activity.

## Worker update 2026-11-04 (user): hard workers only
Luna workers cancelled; relaunched on the same branches as Codex gpt-6.1-sol (low, default speed): history-port-persist, history-port-exec-a,
history-port-decision. Claude Sonnet 5.5 low: history-port-task (task+taskdlq), history-port-engine (engine+handler+wrappers). 5 total.
Unassigned: queue, queuev2, ndc, replication, failover, shard remainder, main wiring. Scheduled continuation 03:45 local daily (T3 scheduled task).
