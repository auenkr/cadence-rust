# Matching RPC ignored-test disposition (2026-10-25)

Audited at `6af476941`.

## Non-durable DescribeTaskList status

`matching_rpc_non_durable_describe_reports_task_id_block_and_empty` in `rust/crates/cadence-matching/src/matching_rpc_tests.rs` is ignored with a claim that Go derives `task_id_block` and `empty` without an ack manager. The Go source confirms the caller-visible contract: `go/service/matching/tasklist/task_list_manager.go:767-805` constructs a `TaskIDBlock` from `db.RangeID()` and configured range size and sets `Empty` from `GetAckLevel() == taskWriter.GetMaxReadLevel()` whenever status is requested.

Rust's durable Matching RPC path (`grpc_matching.rs:3699-3808`) obtains all status fields from `queue.queue.status()` and emits `task_id_block` from the live queue's range. The launcher chooses this durable runtime for configured stores and embedded SQLite (`launcher.rs:1300-1354`). This is the service path used for normal persisted Matching and is not missing the behavior.

The ignored test deliberately constructs `Matching::new` with `queues: None`. That fallback delegates `DescribeTaskList` to the service-core HTTP handler; `cadence-service-core/src/handlers.rs:2141-2255` computes backlog and poller metrics but leaves `TaskListStatus`'s read/ack levels, task-id block, and empty at protobuf defaults. No task-list ack/range manager exists in that fallback from which to derive Go's values. `MatchingRuntime::in_memory` and the launcher's explicit `InMemory` task-store source also select this backend; therefore this is a real discrepancy for the in-memory/development runtime, not a general durable-production gap. No safe task-id block can be synthesized from the available in-memory execution store (the Go value depends on DB range ID and configured range size). The ignored test remains valid for that mode; it must not be cited as evidence that durable Matching fails parity. No code change made because inventing a range would be observably wrong.

## Semaphore bucket creation during shutdown

`matching_rpc_semaphore_rejects_new_buckets_while_shutting_down` at `matching_rpc_tests.rs:2680` is **active**, not ignored. It enables the feature and checks that after shutdown starts a cache-miss bucket returns `Aborted` with owner `not known`. Production reachability is present: `MatchingRuntime::drain_with` invokes `begin_shutdown()` (`matching_runtime.rs:655`) while the listener remains live; `Matching::check_semaphore_ownership` checks that flag before membership lookup or manager creation (`grpc_matching.rs:1127-1147`). The Go analogue is `engine.go:1664-1689`: on a cache miss `AcquireSemaphore` calls `errIfSemaphoreOwnershipLost` before `GetOrCreate` (`engine.go:462+`), and shutdown returns `SemaphoreNotOwnedByHostError` with owner `not known`. The Rust active RPC regression covers the caller-visible status/message and prevents a new manager. This claim is already closed in code and does not warrant a second test.

## Validation boundary

No code changed, so no Actions run was dispatched. No local Cargo/Go/fmt commands or mixed-cluster gates were run. The in-memory `DescribeTaskList` status discrepancy remains scoped to the explicit no-durable/in-memory runtime; durable queue status and shutdown rejection are source-backed as above. The ignored test does not establish or refute other status compatibility dimensions.
