# history-port-shard checklist (2026-11-05)

Branch `history-port-shard`. Validation: CI only (`gh workflow run rust-ci.yml --ref history-port-shard -f package=cadence-history`).

## Ported
| Go | Rust |
|---|---|
| history/config/config.go (+constants) | `config.rs` (generated `Config::new` via `memory/tools/gen_history_config.py`), `config_collection.rs` (dynamicconfig Collection) |
| history/constants/{constants,test_constants}.go | `constants.rs` (domain-cache test entries deferred) |
| shard/context.go: GenerateTaskID(s), renewRange, max read levels, ack levels, cluster ack levels, queue states, processing queue states, failover levels, domain notification version, current time, failover markers (validate/add core), persistShardInfo, allocateTransfer/TimerIDs, closeShard/closedError, acquireShard (post-load), shardedHistoryTaskDLQWriter dedup | `shard_context.rs` |
| lookup/lookup.go | `shard_context::history_server_key_by_shard_id` |
| shard/controller.go (core: ownership, acquire pump) | existing `shard.rs` (not yet extended with items/engine factory) |

## TODO
- context.go: Create/Update/ConflictResolve workflow execution write paths, AppendHistoryV2Events, ReplicateFailoverMarkers, ReinjectHistoryTasks, emitShardInfoMetricsLogs, events cache wiring (need persistence ExecutionManager / domain cache ports)
- notifying_execution_manager.go, task_notifier.go
- controller.go: historyShardsItem lifecycle + EngineFactory, doShutdown, membership event pump
- resource/resource.go; main.rs/service wiring
- domain cache test constants

## CI
Last green run: none yet. Latest dispatched: https://github.com/auenkr/cadence-rust/actions/runs/41900828938
