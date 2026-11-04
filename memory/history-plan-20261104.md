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
- [ ] H1  - [ ] H2  - [ ] H3  - [ ] H4  - [ ] H5  - [ ] H6  - [ ] H7
