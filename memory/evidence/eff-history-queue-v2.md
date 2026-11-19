# eff-history-queue-v2 (2026-11-19): PARTIAL, queue v2 not wired yet

Branch `eff-history-queue-v2` (base origin/master 54d16d617).

## Step 1: which queue version Go uses by default
- `go/common/dynamicconfig/dynamicproperties/constants.go:2395-2406`: `history.enableTransferQueueV2` and `history.enableTimerQueueV2` both default **true** (ShardID filter).
- `constants.go:2787-2801`: `history.{transfer,timer}ProcessorCachedQueueReaderMode` default **"enabled"**.
- `go/config/dynamicconfig/development.yaml:103-108` also sets both v2 flags true. Gate and benchmark configs do not override them, so the Go baseline runs v2 with the cached reader.
- Rust `config.rs:355,395,419` already reads the same keys with the same defaults. Nothing consumes them: `wiring/assembly/queues.rs::build_queue_processors` always builds v1.

## Step 2a: v2 queue-state persistence data loss
Already fixed on master in 1e1a5e8f8. Review item 12 (history-crossreview-opus-20261113) is stale:
- `shard_adapters.rs:356-410` maps every virtual queue id, slice and predicate in both directions.
- `wiring/persistence/shard_events.rs` (`shard_data_to_record` / `shard_row_to_data`) persists them.
- `sqlblobs.rs` thrift encoding: `shard_queue_state_roundtrip_is_lossless` covers it.
This branch adds `shard_events::queue_state_roundtrip_tests::shard_row_keeps_full_queue_v2_state`. It checks the whole row round trip (ShardInfoData -> ShardRow -> ShardInfoData) for transfer and timer states, several virtual queues, and all predicate kinds.

## Step 2b: v2 wiring is not done. Why it does not fit in one pass
`queuev2/` is its own type set. Wiring it needs these pieces, none of which exist yet:
1. **Notify payload (blocker for the cached reader).** Go `NotifyTaskInfo.Tasks` carries the full `persistence.Task`. The cached reader puts those tasks in memory and executes them without a DB read. Rust `engine::notify_tasks::NotifiedTask` keeps only id/type/version/timestamp/kind. The executors need the decoded blob (`queue_exec::TaskInfoAdapter` -> `task::deps::*Task`). For example, a transfer task needs its target domain, task list and schedule id. So every engine call site that notifies tasks must pass the full task (or the `SqlHistoryTask` blob) through. If the cached reader were enabled with today's payload, executors would get incomplete tasks.
2. A `v2::QueueEnv` implementation: `new_history_task` (wrap `task::history_task::new_history_task` plus `task::executor_wrapper`), `new_rescheduler` (`task/rescheduler.rs`), the rate limiter (`queue_exec::DynamicRateLimiter`) and the timer gate.
3. Adapters between the v2 task, processor and executor types (`deps.rs:428-453`; `TaskExecutor` is an empty marker trait) and `task::interface::{Task, Processor}`. PTask must carry the full SQL task so the executors can downcast it. Check that `wiring/persistence/queues.rs:207` (`SqlExecutionManager` v2 impl) does this.
4. A `v2::Queue` -> `engine::runtime::QueueProcessor` adapter. `Action`/`ActionResult` are placeholders, so GetState/Reset need a port.
5. `QueueFactoryDeps::create_queue_v1`, so the v1 path is kept when the flag is false. Branch in `queue_factory.rs:51` on `enable_*_queue_v2(shard_id)`.
6. Gate coverage for v2 plus Rust<->Go shard handover (Go reads the `queue_states` that Rust writes).

Suggested split for workers: (A) a full-task notify payload in the engine (this is the prerequisite). (B) the QueueEnv and task/processor adapters. (C) the factory switch and Queue adapter. (D) gates and benchmark.

## Validation (this branch)
| check | run | result |
|---|---|---|
| rust-ci cadence-history (head 17ba6ee8f) | https://github.com/auenkr/cadence-rust/actions/runs/44493897357 | success (clippy + tests) |

No gates or benchmark were run: queue behaviour is unchanged (test-only diff).
