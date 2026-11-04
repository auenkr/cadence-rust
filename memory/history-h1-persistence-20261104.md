# History H1 SQL persistence checkpoint (2026-11-04)

Branch: `history-h1-persistence`, based on `master` at `797ff0f15fde9be16a4cafd33850b2cf9c70122d`.

## Implemented

- Added `rust/crates/cadence-persistence/src/history_sql.rs` and exported it in `lib.rs`.
- Defined async `ShardStore`, `ExecutionStore`, and `HistoryStore` contracts, serialized shard/execution/history node row types, and explicit persistence error categories for the required workflow and ownership failures.
- Existing branch token codec and branch read helpers were inspected and are reusable: `history_branch.rs` and `history_reader.rs`.

This is a contract-only checkpoint. No MySQL or PostgreSQL implementation, SQL execution, CRUD behavior, task-store methods, or History-v2 writes have been ported. Therefore H1 is **not complete**, and these contracts are not yet usable by the History service.

## Go references and parity work remaining

- Shards: `go/common/persistence/sql/sql_shard_store.go` (`CreateShard`, `GetShard`, conditional `UpdateShard`, `lockShard`). Preserve shard row encoding, current-cluster ack defaults, transaction lock and `ShardOwnershipLost` behavior.
- Workflow executions: `go/common/persistence/sql/sql_execution_store.go` and `sql_execution_store_util.go`; also the `go/common/persistence/serialization` parser. Port all create modes, mutable state maps/buffered events/version histories, update/conflict resolution, concrete/current deletion, current execution reads/lists, existence checks, and exact condition errors.
- Task rows: `go/common/persistence/sql/sql_task_store.go` and `go/common/persistence/sql/sql_history_dlq_task_store.go`. Transfer, timer, replication, cross-cluster, and replication DLQ range reads/acks are not implemented.
- History v2: `go/common/persistence/sql/sql_history_store.go`; append/read/fork/delete and tree/branch listing are not implemented. Reuse `history_branch.rs` and `history_reader.rs` where applicable.
- Schemas inspected: `go/schema/mysql/v8/cadence/schema.sql`, `go/schema/postgres/cadence/schema.sql`, and corresponding `versioned/v0.1/base.sql`. `rust-ci.yml` does not provision either database; add sqlite-free behavior tests and ignored database integration coverage unless a separate CI database job is added.
- The Rust persistence crate currently has native MySQL/Postgres driver dependencies, not `sqlx`; no persistence backend was introduced in this checkpoint.

## Validation

- Cargo was not run locally, per repository instructions.
- Requested CI dispatched with `gh workflow run rust-ci.yml --ref history-h1-persistence -f package=cadence-persistence`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41785167891
- Head SHA: `124bf52d7724eeb31a011321ce026c41062d1c4c`
- At checkpoint: `in_progress`; final conclusion pending.

## Next work

Implement concrete MySQL and PostgreSQL shard, execution, and task stores first (including tests/error mapping), then History-v2; dispatch CI after each coherent update. Do not treat this checkpoint as H1 completion or proceed to H2.
