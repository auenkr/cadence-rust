# Matching configuration reconciliation (2026-10-27)

Static source audit only. Compared `go/service/matching/config/config.go` `NewConfig`, property definitions/defaults in `go/common/dynamicconfig/dynamicproperties/constants.go`, current Matching production readers/wiring in Rust, and Go production callsites. No Cargo or Go command was run. The 2026-10-21 audit is stale for user/worker quotas, persistence QPS, throttled logging, task-info logging, exclusion policy, and partition rollout; those now have production source paths.

## Conclusion

No confirmed current configuration behavior mismatch was established. Production Rust readers exist for behavior-bearing NewConfig fields, including the settings previously marked missing. Some Go constructor fields have no production Go consumer and should not be promoted into compatibility requirements. Two architectural differences should remain explicit: Go's 100,000 range allocation is not a single Matching constructor parameter in Rust (range allocation is implemented at the registry/persistence boundary), and several operational settings are split across Rust modules rather than represented by a `Config` struct.

The ignored Rust test `go_new_config_every_field_has_a_rust_reader` in `rust/crates/cadence-matching/tests/matching_partial_upgrade_a.rs:84-87` is not a confirmed product gap. Its placeholder expects a centralized Rust `Config` and blanket reader for every Go struct field, including fixed/injected fields and unused Go settings. Go's `config_test.go::TestNewConfig` is a constructor/table test, not evidence every field affects runtime. Close this case by replacing the ignored test with auditable assertions grouped by actual behavior-bearing Rust reader/module and marking constructor-only values (hostname, RPC binding, injected callbacks, fixed constants) at their real wiring sites; omit Go fields proven unused. Keep focused defaults/filter/lifetime tests at reader modules and the dynamic-config defaults audit. No single synthetic `NewConfig` table is required.

## Field-by-field table

Defaults below refer to Go dynamic-property metadata and Rust production source where applicable. `TF` means Go task-list-info filters (domain name, task list name, task type); `D` means domain-name filter; `DID` means domain-ID filter; `plain` means unfiltered. `fixed/injected` fields have no dynamic property.

| Go NewConfig field (property; filter) | Go runtime consumer / lifetime | Current Rust production reader / comparison |
|---|---|---|
| `PersistenceMaxQPS` (`matching.persistenceMaxQPS`; plain) | `service.go` resource/store setup; per-host persistence quota | `matching_persistence_limiter.rs`, launcher creates one limiter and wraps Matching stores; default 3000. Present. |
| `PersistenceGlobalMaxQPS` (`matching.persistenceGlobalMaxQPS`; plain) | resource/store setup; cluster quota | Same shared limiter; default 0 means Go-compatible host fallback. Present. |
| `EnableSyncMatch` (`matching.enableSyncMatch`; TF) | copied into `TaskListConfig`; no production read of resulting field found | `matching_config::enable_sync_match` exists as overlay reader but no behavior consumer. Correctly treated as unused compatibility surface. |
| `UserRPS` (`matching.rps`; plain) | handler user RPC limiter; refreshed dynamically | `matching_config::{user_rps,...}` and Matching RPC limiter production path. Present. |
| `WorkerRPS` (`matching.workerrps`; plain) | handler worker RPC limiter; refreshed dynamically | Same RPC limiter production path. Present. |
| `DomainUserRPS` (`matching.domainrps`; D) | domain fallback quota | `matching_config::domain_user_rps`, RPC limiter. Present. |
| `DomainWorkerRPS` (`matching.domainworkerrps`; D) | worker domain fallback quota | `matching_config::domain_worker_rps`, RPC limiter. Present. |
| `ShutdownDrainDuration` (`matching.shutdownDrainDuration`; plain) | service shutdown sampling | `matching_runtime.rs` reads at shutdown. Present. |
| `RangeSize` (fixed 100000) | task ID allocation in task-list manager | Rust task range reservation is owned by registry/persistence (`matching_registry.rs`), not a Go-shaped service Config. No behavior mismatch established; verify configured value at registry construction before interpreting this as a gap. |
| `ReadRangeSize` (`matching.readRangeSize`; plain) | reader allocation/read range | `grpc_matching.rs` provider and `matching_reader.rs` live config. Present. |
| `EnableReturnAllTaskListKinds` (`matching.matchingReturnAllTaskListKinds`; plain) | `GetTaskListsByDomain` behavior | `grpc_matching.rs` getter. Present. |
| `GetTasksBatchSize` (`matching.getTasksBatchSize`; TF) | reader batch; invalid values fall back | `grpc_matching.rs` reader provider, `matching_reader.rs`. Present. |
| `UpdateAckInterval` (`matching.updateAckInterval`; TF) | reader ack cadence | `grpc_matching.rs` and registry scheduling. Present. |
| `IdleTasklistCheckInterval` (`matching.idleTasklistCheckInterval`; TF) | task-list liveness/sweep ticker | `grpc_matching.rs`, registry. Present. |
| `MaxTasklistIdleTime` (`matching.maxTasklistIdleTime`; TF) | copied into `TaskListConfig`; no production read found | `matching_config` reader only; no behavior consumer. Do not count absent runtime action as mismatch. |
| `NumTasklistWritePartitions` (`matching.numTasklistWritePartitions`; TF) | task-list partition setup | `grpc_matching.rs` partition provider/registry. Present. |
| `NumTasklistReadPartitions` (`matching.numTasklistReadPartitions`; TF) | task-list partition setup | Same. Present. |
| `ForwarderMaxOutstandingPolls` (`matching.forwarderMaxOutstandingPolls`; TF) | forwarder admission | `grpc_matching.rs`/forwarder providers. Present. |
| `ForwarderMaxOutstandingTasks` (`matching.forwarderMaxOutstandingTasks`; TF) | forwarder admission | Same. Present. |
| `ForwarderMaxRatePerSecond` (`matching.forwarderMaxRatePerSecond`; TF) | forwarding rate cap | Same. Present. |
| `ForwarderMaxChildrenPerNode` (`matching.forwarderMaxChildrenPerNode`; TF) | partition tree width | Same. Present. |
| `AppendTaskTimeout` (`matching.appendTaskTimeout`; TF) | persistence append timeout | `grpc_matching.rs` task writer provider. Present. |
| `AsyncTaskDispatchTimeout` (`matching.asyncTaskDispatchTimeout`; TF) | async match dispatch timeout | `grpc_matching.rs` provider/reader flow. Present. |
| `LocalPollWaitTime` (`matching.localPollWaitTime`; TF) | local poll grace before forwarding | `grpc_matching.rs` local waits. Present. |
| `LocalTaskWaitTime` (`matching.localTaskWaitTime`; TF) | local task grace before forwarding | Same. Present. |
| `TaskIsolationDuration` (`matching.taskIsolationDuration`; TF) | isolation eligibility | `grpc_matching.rs` isolation provider/registry. Present. |
| `TaskIsolationPollerWindow` (`matching.taskIsolationPollerWindow`; TF) | isolation poller recency | Same. Present. |
| `EnableGetNumberOfPartitionsFromCache` (`matching.enableGetNumberOfPartitionsFromCache`; TF) | partition config cache | `grpc_matching.rs`. Present. |
| `MinTaskListWritePartitions` (`matching.taskListMinimumWritePartitions`; TF) | adaptive scaler minimum | scaler config in `grpc_matching.rs`; registry/scaler. Present. |
| `PartitionUpscaleRPS` (`matching.partitionUpscaleRPS`; TF) | adaptive scaler threshold | scaler provider and scaler module. Present. |
| `PartitionDownscaleFactor` (`matching.partitionDownscaleFactor`; TF) | adaptive scaler hysteresis | same. Present. |
| `PartitionUpscaleSustainedDuration` (`matching.partitionUpscaleSustainedDuration`; TF) | scaler duration | same. Present. |
| `PartitionDownscaleSustainedDuration` (`matching.partitionDownscaleSustainedDuration`; TF) | scaler duration | same. Present. |
| `AdaptiveScalerUpdateInterval` (`matching.adaptiveScalerUpdateInterval`; TF) | scaler loop cadence | same. Present. |
| `EnableAdaptiveScaler` (`matching.enableAdaptiveScaler`; TF) | scaler gate | same. Present. |
| `EnablePartitionEmptyCheck` (`matching.enablePartitionEmptyCheck`; TF) | empty partition decisions | same. Present. |
| `EnableStandbyTaskCompletion` (`matching.enableStandbyTaskCompletion`; TF) | standby completion behavior | `grpc_matching.rs`, standby module. Present. |
| `EnableClientAutoConfig` (`matching.enableClientAutoConfig`; TF) | worker auto-config response hint | `grpc_matching.rs` response path. Present. |
| `QPSTrackerInterval` (`matching.qpsTrackerInterval`; TF) | task-list QPS sampling cadence (restart semantics per property docs) | `grpc_matching.rs` QPS provider/registry. Present. |
| `OverrideTaskListRPS` (`matching.overrideTaskListRps`; TF) | task-list advertised RPS override | `grpc_matching.rs`. Present. |
| `EnablePartitionIsolationGroupAssignment` (`system.enablePartitionIsolationGroupAssignment`; TF) | isolation group assignment | scaler/registry provider. Present. |
| `IsolationGroupUpscaleSustainedDuration` (`matching.isolationGroupUpscaleSustainedDuration`; TF) | assignment scaler | scaler provider. Present. |
| `IsolationGroupDownscaleSustainedDuration` (`matching.isolationGroupDownscaleSustainedDuration`; TF) | assignment scaler | scaler provider. Present. |
| `IsolationGroupHasPollersSustainedDuration` (`matching.isolationGroupHasPollersSustainedDuration`; TF) | assignment active threshold | scaler provider. Present. |
| `IsolationGroupNoPollersSustainedDuration` (`matching.isolationGroupNoPollersSustainedDuration`; TF) | assignment inactive threshold | scaler provider. Present. |
| `IsolationGroupsPerPartition` (`matching.isolationGroupsPerPartition`; TF) | assignment target | scaler provider. Present. |
| `LongPollExpirationInterval` (`matching.longPollExpirationInterval`; TF) | poll hold timeout | `grpc_matching.rs` poll path. Present. |
| `MinTaskThrottlingBurstSize` (`matching.minTaskThrottlingBurstSize`; TF) | task-list throttle bucket | `grpc_matching.rs` throttle provider. Present. |
| `MaxTaskDeleteBatchSize` (`matching.maxTaskDeleteBatchSize`; TF) | range deletion batch | task deletion provider in `grpc_matching.rs`. Present. |
| `OutstandingTaskAppendsThreshold` (`matching.outstandingTaskAppendsThreshold`; TF) | writer backlog threshold | writer provider in `grpc_matching.rs`. Present. |
| `MaxTaskBatchSize` (`matching.maxTaskBatchSize`; TF) | writer append batch | writer provider in `grpc_matching.rs`. Present. |
| `ThrottledLogRPS` (`matching.throttledLogRPS`; plain) | shared handler throttled logger; dynamic refresh | `matching_throttled_logger.rs`, instantiated on gRPC handler, used by both poll methods; default 20. Present. |
| `EnableDebugMode` (`system.enableDebugMode`; plain, read once) | engine logging mode | launcher reads once and installs task info logger. Present. |
| `EnableTaskInfoLogByDomainID` (`matching.enableTaskInfoLogByDomainID`; DID) | exact Go consumer `handler/engine.go:1593`; Info only when debug mode and domain filter true | `matching_task_info_log.rs`, launcher installation and `grpc_matching.rs` emits. Present; Rust documents that unavailable Debug level events are dropped. |
| `ActivityTaskSyncMatchWaitTime` (`matching.activityTaskSyncMatchWaitTime`; D) | copied to `TaskListConfig`; no production read found | `matching_config` reader only; no behavioral consumer. Do not count as mismatch. |
| `EnableTasklistIsolation` (`system.enableTasklistIsolation`; D) | isolation gate | `grpc_matching.rs` and registry. Present. |
| `EnableDistributedSemaphore` (`system.enableDistributedSemaphore`; D) | Go `handler/engine.go:458`; only checked when creating a manager, after a cached-manager fast path | `grpc_matching.rs:3356` checks the same domain-name filtered flag only when creating a manager; cached-manager fast path at 3345 mirrors Go. Present. |
| `SemaphoreIdleTime` (`matching.semaphoreIdleTime`; D) | bucket expiry | `grpc_matching.rs` reads live, semaphore manager. Present. |
| `AllIsolationGroups` (injected callback) | injected group-name provider | Rust reads `system.allIsolationGroups` in isolation providers. Present as split runtime provider. |
| `HostName` (injected) | service identity / membership | launcher derives host identity and membership config. Present as startup wiring, not a dynamic reader. |
| `RPCConfig` (injected) | listener/bind configuration | launcher/listener config. Present as startup wiring. |
| `TaskDispatchRPS` (fixed 100000) | dispatch limiter default | `matching_limiter.rs` documents and implements Go value. Present. |
| `TaskDispatchRPSTTL` (fixed 1m) | dispatch limiter refresh TTL | same. Present. |
| `MaxTimeBetweenTaskDeletes` (fixed 1s) | task GC threshold/cadence | Go `task_gc.go:92`; Rust GC runs in registry/reader lifecycle. Check exact timer interpretation; no proven discrepancy from field mapping alone. |
| `ExcludeShortLivedTaskListsFromShardManager` (`matching.excludeShortLivedTaskListsFromShardManager`; operational config, plain) | Go `handler/engine.go:1704` shard distributor eligibility | `launcher.rs` builds membership policy from same key. Present. |
| `RecordTaskStartedTimeout` (`matching.recordTaskStartedTimeout`; D) | Go `handler/engine.go:1490,1520`, per-domain History RPC timeout | `grpc_matching.rs` provider and `matching_history.rs` live timeout path. Present. |

## Evidence and remaining closure

- Current Rust production wiring: `launcher.rs:761-800` installs task-info logging and persistence limiter; `grpc_matching.rs:363-417` constructs shared throttled logger; poll callsites at 1940 and 2210; task-info events at 3333, 4290, 4337; persistence hooks at 1047, 1052, 1247, 1272, 1282.
- Exact Go task-info production use: `go/service/matching/handler/engine.go:1593`. Short-lived shard-manager setting: `engine.go:1704`. Record-start timeouts: `engine.go:1490,1520`. Persistence quotas: `service.go:62-75`. Throttled logger setup: `service.go:69-73`, handler use in `handler.go` poll paths.
- Go fields `EnableSyncMatch`, `MaxTasklistIdleTime`, `ActivityTaskSyncMatchWaitTime` are assigned into task-list config but their resulting fields have no production reads in Matching; constructor table assertions do not make them behavior requirements.
- `EnableDistributedSemaphore` branch structure was checked against Go: the existing-manager fast path bypasses the gate in both implementations; no mismatch identified.
- `RangeSize`, fixed GC interval, and injected startup dependencies are architectural wiring rows. Test their effective runtime wiring, not presence in a synthetic Rust config struct.
- The ignored test's `go_new_config_every_field_has_a_rust_reader` should not remain phrased as parity contract. Replace with consumer-driven checks and let the package's focused module tests plus `matching_dyncfg_defaults_tests.rs` guard defaults and property keys. Test file is `rust/crates/cadence-matching/tests/matching_partial_upgrade_a.rs`.

