# History queue executor assembly (2026-11-11)

Branch: `history-queueexec-assembly` (from master efbc0ea40).

## Done
- `rust/crates/cadence-history/src/wiring/assembly/queue_provider.rs`: `HistoryQueueExecProvider` (real `QueueExecProvider`).
  - Shared service-level task processor (Go `handler.Start`): priority assigner -> flat WRR scheduler -> `ParallelTaskProcessor`, wrapped by `new_rate_limited_processor`. Started lazily on the first shard build and stopped by `provider.stop()`.
  - Per shard: `TaskShardContext`, `RealQueueShardContext` (SQL queue shard), task allocator, redispatcher factory (active/standby intervals), active and per-remote-cluster standby transfer and timer executors, and the `TransferActiveDepsImpl` registry binding.
  - Shared `HistoryResenderImpl` over `ReplicationClientBean`.
- `SchedTask` / `SchedulerProcessor` / `SchedulerCfg` adapters; `CommonTask::as_any` default hook in `task/parallel_processor.rs`.
- `launcher.rs`: `MissingExecutorsProvider` replaced; the provider is bound to the registry and stopped after `service.stop()`.
- Unit tests: dynamic weight config, unbound registry controller, scheduler construction and start/stop.

## Gaps
- Hierarchical WRR scheduler (Go default) is not ported; the flat WRR is used, with a warning.
- `ReinjectorSlot` is unbound, so reinjection returns an explicit error.
- `TimerStandbyDepsImpl::load_workflow`, timer retry_workflow / continue-as-new stay explicit TODO errors.
- Rate limiter shard count comes from the registry (0 before bind).
- No end-to-end per-shard `queue_exec` test (needs a full `HistoryEnv`); mixed Go/Rust validation not run.
- Shard release stops the per-shard queue processors, but the shared processor stops only at service shutdown.

## CI
- Run: https://github.com/auenkr/cadence-rust/actions/runs/42946632177 (package=cadence-history)
- Head SHA: c472cd63da578552478b46959d6068690b1b9f83 (code head)
- Conclusion: success
