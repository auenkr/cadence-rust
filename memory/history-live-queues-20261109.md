# History live queue assembly — 2026-11-09

## Result

This bounded queue wiring slice is **not complete**. The startup provider is still not a real `QueueExecProvider`, and History must not be considered runnable with live queues based on this change. `launcher.rs` now installs `UnassembledExecutorsProvider`; shard creation fails explicitly instead of logging an error and returning an empty processor map. This prevents a shard from appearing healthy while transfer and timer work is unprocessed.

## Startup path traced

Go `historyEngineFactory.CreateEngine` creates transfer and timer queue processors for each shard. Queue processor construction owns the active queue and creates standby queues for every enabled remote cluster. The Rust path is `ShardRegistry::create_entry` -> `QueueFactory::build_queue_processors` -> `queues::build_queue_processors` -> `queue_exec::{build_transfer_queue_processor,build_timer_queue_processor}`. Queue executors are started and stopped through the engine's queue processor lifecycle.

The Rust repository already has task executor dependency adapters in `queue_deps.rs`, `queue_deps_transfer_active.rs`, `queue_deps_timer_active.rs`, and `queue_deps_standby.rs`, plus task executor constructors in `task/`. Matching task dispatch resolves through `HistoryEnv::matching_resolver`; child workflow and history interactions use the local `RegistryHistoryClient`, so no Go service counterpart is involved.

## Exact remaining blockers

- There is no per-shard provider assembly for `QueueExecCommon`: `QueueShardContext`, task `ShardContext`, task processor, task allocator, queue metrics/config, redispatcher factory, and critical retry property must be connected together.
- `RealQueueShardContext::new` requires a queue `ProcessorConfig`, `QueueConfig`, domain cache adapter, and active-cluster manager. There is no assembly/helper that builds these from `HistoryEnv`'s dynamic config and shard context. The processor config/queue config must be populated from the existing dynamic properties, not guessed constants.
- Queue allocator traits (`queue::task_allocator::{DomainCache,ActiveClusterManager}`) need concrete adapters over `HistoryDomainCache` and domain active-cluster config.
- Active executors need construction with `TransferDepsImpl`, `TransferActiveDepsImpl`, `TimerActiveDepsImpl`, and `CleanupDepsImpl`; standby executors need per-remote-cluster `TransferStandbyDepsImpl` / `TimerStandbyDepsImpl` and `HistoryResenderImpl`, wrapped with the task executor's active/standby routing policy. The provider must bind the active transfer deps to the `ShardRegistry` before tasks can invoke engine callbacks.
- Timer standby currently returns an explicit unsupported error because timer sequence/user timer state and passive mutable-state update are absent in `EngineMutableState` (`queue_deps_standby.rs`). This must remain an error; it cannot be replaced by a successful no-op.
- There is no queue runtime lifecycle validation yet. Correctness requires checking queue processor start/stop on shard ownership change and process shutdown, plus failure/restart/recovery with Rust Matching and Frontend.

## Change made

- `queue_factory.rs`: renamed the misleading `MissingExecutorsProvider` to `UnassembledExecutorsProvider`; it returns a contextual error. `ExecQueueFactory` also errors if a provider returns no processors, rather than treating that as a valid empty queue set.
- `launcher.rs`: uses this explicit failing provider. This is a guard against silent task loss, **not** the requested live executor wiring.

## Evidence

- Starting parent: `c42480cdc7f7e106a95e13660567a105d7fa95fc`.
- No local Cargo, rustfmt, Go build/test, or mixed-cluster gate was run, per current host/CI rules.
- Implementation commit/SHA and CI run URL/head/conclusion: to be filled after push and workflow dispatch.
