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

## Piece A: full-task notify payload (branch queue-v2-a-notify, 2026-11-19)
- `engine/notify_tasks.rs`: `NotifiedTask.full: Option<FullPersistedTask>` (`Transfer(pc::TransferTask)` / `Timer(pc::TimerTask)`), filled by `NotifiedTask::from_transfer/from_timer`. This matches Go, where `NotifyTaskInfo.Tasks` carries the full `persistence.Task`. `None` only for the synthetic failover-callback notify tasks, which Go also builds with no payload.
- All call sites go through `wiring/assembly/engine_cache.rs::notify_tasks_with`, which is used for successful writes, ambiguous-error writes and the NDC context hooks. It now passes the full tasks. The v1 processors ignore the new field.
- Unit tests `notify_tasks::full_task_tests::{transfer,timer}_notification_carries_full_persisted_task`.

| check | run | result |
|---|---|---|
| rust-ci cadence-history (b03e0db22) | https://github.com/auenkr/cadence-rust/actions/runs/44495411634 | success |
| History mixed gate MySQL | https://github.com/auenkr/cadence-rust/actions/runs/44495416974 | success, `GATE SUMMARY failed: []` |
| History mixed gate Postgres | https://github.com/auenkr/cadence-rust/actions/runs/44495422534 | success, `GATE SUMMARY failed: []` |

## Piece B (queue-v2-b-adapters, 2026-11-19)
QueueEnv + task/processor/rescheduler/executor adapters (wiring/assembly/queue_v2_adapters.rs), notify full tasks -> cached reader; factory unchanged (v1 active).
Tests: transfer/timer end-to-end via adapters, cached reader serves notified tasks w/o DB and falls back on eviction/miss, timer gate.
- rust-ci (cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/44498733522 success (head 8b18d3d07)
- History MySQL gate: https://github.com/auenkr/cadence-rust/actions/runs/44498744144 success, `GATE SUMMARY failed: []`

## Piece C/D (queue-v2-c-switch, 2026-11-19)
Piece C: new module `wiring/assembly/queue_v2_wiring.rs`:
- `queue_v2_config(&Config)` builds the v2 config.
- `TracingV2Logger` is the v2 logger.
- `QueueV2Exec` holds the v2 inputs. Its executor wrapper is active plus a standby for the current cluster, as Go `createQueuev2` builds it.
- `is_queue_v2_enabled` / `build_queue_v2` call `queuev2::{transfer,timer}_queue_factory`. The cached reader is used per `*CachedQueueReaderMode`.
- `EngineQueueV2` adapts v2 `Queue` to the engine `QueueProcessor`:
  - start runs on a spawned task, and stop waits for it before stopping.
  - notify passes the full tasks to the cached reader.
  - failover is forwarded.
  - handle_action returns no states, the same as Go v2 `HandleAction` returning (nil, nil).
  - lock/unlock are no-ops, as in Go.
  - Queue state is persisted through `RealQueueV2ShardContext` (shard `queue_states`).
- `queues::build_queue_processors` picks per category and per shard on `history.enable{Transfer,Timer}QueueV2` (default true). When the flag is false it builds v1.
- `queue_provider` builds the v2 inputs.
- Gate harness: `history-mixed-gate.yml` input `queue_v2` is now a choice: false (config as is; development.yaml turns v2 on), true, or off (forces v1 in the Rust dynamic config).

| check | run | result |
|---|---|---|
| rust-ci cadence-history (283b331ee) | https://github.com/auenkr/cadence-rust/actions/runs/44501314413 | success |
| History gate MySQL (v2, 38b320752) | https://github.com/auenkr/cadence-rust/actions/runs/44502702719 | success, `GATE SUMMARY failed: []` |
| History gate Postgres (v2) | https://github.com/auenkr/cadence-rust/actions/runs/44502708348 | success, `GATE SUMMARY failed: []` |
| History gate Cassandra (v2) | https://github.com/auenkr/cadence-rust/actions/runs/44502713556 | success, `GATE SUMMARY failed: []` |
| History gate MySQL queue_v2=off (v1) | https://github.com/auenkr/cadence-rust/actions/runs/44502719529 | success, `GATE SUMMARY failed: []` |
| restartgate mysql, branch | https://github.com/auenkr/cadence-rust/actions/runs/44502725563 , https://github.com/auenkr/cadence-rust/actions/runs/44513227729 | failure: soak-driver-exit, zero-failed-workflows, all-started-completed ("no matching hosts in the membership ring") |
| restartgate mysql, master 0e5c84050 | https://github.com/auenkr/cadence-rust/actions/runs/44513215209 | same failure, so it predates this branch (last restartgate run before this, 2026-11-14 43543900094, also failed) |

The two-history kill/restart phase inside the mixed gates (two_history=true by default) passed on all three databases, so the v2 queue survived shard movement.

Benchmark settings: 25 wf/s, 60 s, mysql_tmpfs, one pair each. Before = master 0e5c84050, after = 38b320752. Completion latency is in ms. CPU is History container CPU-seconds.

| benchmark | run | Go p50/p99, CPU | Rust p50/p99, CPU |
|---|---|---|---|
| history-only, before | https://github.com/auenkr/cadence-rust/actions/runs/44507631170 | 94.6/1533.9, 36.8 | 52.5/210.8, 23.1 |
| history-only, after | https://github.com/auenkr/cadence-rust/actions/runs/44507603232 | 34.7/111.0, 19.2 | 29.0/99.5, 13.1 |
| all services, before | https://github.com/auenkr/cadence-rust/actions/runs/44507636498 | 77.6/253.3, 34.3 | 36.1/197.8, 18.1 |
| all services, after | https://github.com/auenkr/cadence-rust/actions/runs/44507608741 | 78.8/255.6, 34.9 | 36.5/105.8, 18.4 |

SELECT counts per table (mysql_digests): transfer_tasks = 13 and timer_tasks = 13 in every arm, both before and after, for Go and Rust. The digest window shows no difference in queue reads, so I can't claim a read reduction. The history-only "before" pair was noisy: its Go arm was also slow. The all-services Rust p99 dropped from 197.8 to 105.8 ms, with p50 and CPU flat. This is one pair only.

Verdict: the v2 switch defaults to on, as in Go. CI and all mixed gates pass on MySQL, Postgres and Cassandra, and the v1 fallback passes too. The restart gate fails on master as well, so that failure is not from this change.
