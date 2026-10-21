# Matching config parity audit (2026-10-21, source only)

Scope: every field constructed by pinned Go `go/service/matching/config/config.go`
(`NewConfig`, lines 34-119 and 188-256), its actual Matching production consumer,
and Rust production provider/read point. This is a source audit, not a runtime
test. No Go, Cargo, build, service, or container command was run. User and
worker RPC quota findings below were implemented in source after this audit;
see [`matching-rpc-quotas-20261021.md`](matching-rpc-quotas-20261021.md) for
the exact contract and queued validation. Persistence QPS and throttled logging
remain separate gaps.

## Findings that affect behavior

| Go config field / property | Go consumer and semantics | Rust production path | Finding |
|---|---|---|---|
| `UserRPS` / `matching.userRPS`; `DomainUserRPS` / `matching.domainUserRPS` | `go/service/matching/handler/handler.go:67-74`: user RPC quota has a host-wide dynamic limiter and a domain-filtered fallback limiter. Collection/fallback checks use request domain. | No Matching user quota construction/read in `rust/crates/cadence-server/src`; the only Matching limiter modules are task dispatch (`matching_limiter.rs`) and forwarding (`matching_forward.rs`). | **Missing behavior.** Config values accepted by the shared overlay do not throttle user RPCs.
| `WorkerRPS` / `matching.workerRPS`; `DomainWorkerRPS` / `matching.domainWorkerRPS` | `handler/handler.go:76-83`: separate worker RPC host and domain fallback limiters. | No corresponding worker quota in Rust Matching production source. | **Missing behavior.** Worker RPC quotas are not enforced.
| `PersistenceMaxQPS` / `matching.persistenceMaxQPS`; `PersistenceGlobalMaxQPS` / `matching.persistenceGlobalMaxQPS` | `go/service/matching/service.go:62-75` passes both to `resource.New`, where persistence clients are configured with per-host and global limits. | No corresponding keys or quota wiring under Rust Matching/persistence production source. | **Missing behavior.** Rust Matching persistence operations do not apply Go's configured service/global QPS limits.
| `ThrottledLogRPS` / `matching.throttledLogRPS` | `service.go:69-73` passes the value to `resource.New`; handler receives the resulting throttled logger (`handler.go:54-64`). | No production read/provider for the property in Rust Matching. | Missing operational behavior: Matching throttled log emission is not governed by this setting.
| `EnableTaskInfoLogByDomainID` / `matching.enableTaskInfoLogByDomainID` | Constructed domain-ID-filtered at `config.go:218`; inspect all Go handler callsites before claiming whether it changes production task logging. | No Rust provider/read. | No behavior claim made here; production effect requires callsite verification.

Persistence QPS and throttled logging remain real resource gaps and require
separate work. RPC quota behavior is implemented in source; its metrics
exporter connection and live validation remain pending as described in the
linked note.

## Property-by-property inventory

`Go field` gives the source line where `NewConfig` assigns the field. Unless a
row says otherwise, task-list filters are Go's `DomainName`, `TaskListName`,
and `TaskType`; domain filters use only `DomainName`, plain properties have no
filters. Property defaults are defined next to each key in
`go/common/dynamicconfig/dynamicproperties/constants.go`. Rust defaults below
refer to the production constructor/read site, not a test fixture.

| Go field and assignment | Go read/lifetime | Rust production provider/read and comparison |
|---|---|---|
| `PersistenceMaxQPS`, `PersistenceGlobalMaxQPS` (`config.go:190-191`) | Service resource construction; host and cluster persistence quotas, `service.go:62-75`. | **Missing**, see findings above. |
| `EnableSyncMatch` (`192`) | Task-list-scoped function placed in `TaskListConfig` (`task_list_manager.go:1059-1061`); no production call of the field was found in Go `go/service/matching` after construction. | No reader. Not classified as behavior gap without a Go consumer. |
| `UserRPS`, `WorkerRPS`, `DomainUserRPS`, `DomainWorkerRPS` (`193-196`) | Handler constructs separate dynamic user and worker limiters; `handler.go:67-83`. | **Missing**, see findings above. |
| `RangeSize` (`197`) | Fixed Go value 100000, consumed when task-list manager allocates task ID ranges. | Rust startup passes range size to `QueueRegistry` (`main.rs` Matching constructor, `grpc_matching.rs:320+`); compare `matching_runtime` range configuration. |
| `ReadRangeSize` (`198`) | Plain property consumed by task reader; sampled for reader batch/range allocation. | `grpc_matching.rs:483-530` reader provider, `matching_reader.rs` defaults and read path; present. |
| `GetTasksBatchSize` (`199`) | Task-list scoped reader batch limit; invalid value falls back in Go reader. | `grpc_matching.rs:483-530`, `matching_reader.rs`; present with reader fallback. |
| `UpdateAckInterval` (`200`) | Task-list scoped ack update cadence; manager/reader lifecycle. | `grpc_matching.rs:567-578` provider and registry ack scheduling; present. |
| `IdleTasklistCheckInterval`, `MaxTasklistIdleTime` (`201-202`) | Go task-list config exposes both (`task_list_manager.go:1047-1052`); actual liveness ticker is `IdleTasklistCheckInterval` (`task_list_manager.go:217-219`). `MaxTasklistIdleTime` has no consumer found in production Go Matching. | Rust uses `matching.idleTasklistCheckInterval` provider at `grpc_matching.rs:581-591`, owner liveness and idle sweep in `matching_registry.rs:966-974, 1750-1788`; present. No Rust read for the Go field with no consumer. |
| `NumTasklistWritePartitions`, `NumTasklistReadPartitions` (`210-211`) | Task-list filtered partition config used by manager and adaptive partition setup. | `grpc_matching.rs:323-360` provider, registry partition initialization; present. |
| `ForwarderMaxOutstandingPolls`, `ForwarderMaxOutstandingTasks`, `ForwarderMaxRatePerSecond`, `ForwarderMaxChildrenPerNode` (`212-215`) | Task-list config used by forwarder admission/limits. | `grpc_matching.rs:428-481` providers and `matching_registry.rs`/`matching_forward.rs`; present. |
| `EnableGetNumberOfPartitionsFromCache` (`215`) | Task-list scoped partition config cache toggle. | `grpc_matching.rs:389-403, 630-638`; present. |
| `ShutdownDrainDuration` (`216`) | Plain property sampled by service shutdown (`service.go:147`). | `matching_runtime.rs:652-673` samples `matching.shutdownDrainDuration` on shutdown; present. |
| `EnableDebugMode` (`217`) | Plain value sampled once at startup. | No equivalent dynamic Matching behavior identified; debug-mode integration is outside the Matching property reader. Review as startup/config integration item, not claimed implemented. |
| `EnableTaskInfoLogByDomainID` (`218`) | Domain-ID-filtered logging control. | No Rust production provider/read; effect not asserted until exact Go callsite is verified. |
| `ActivityTaskSyncMatchWaitTime` (`219`) | Domain-filtered property put into `TaskListConfig` (`task_list_manager.go:1040`); no use of the config field found in the task-list-manager sync path examined. | Rust sync-match wait uses task-list `matching.localTaskWaitTime` in `grpc_matching.rs:360-380` and registry; no separate activity/domain provider. Verify Go consumer before calling this an omission. |
| `EnableTasklistIsolation`, `EnableDistributedSemaphore`, `SemaphoreIdleTime` (`220-222`) | Domain filtered. Isolation gates matching behavior; distributed semaphore gate and idle timeout govern semaphore managers. | Isolation provider in `grpc_matching.rs:687-778`; semaphore config/read in `matching_semaphore.rs` and `grpc_matching.rs` references; compare distributed semaphore enable wiring separately from provider existence. |
| `AppendTaskTimeout`, `AsyncTaskDispatchTimeout`, `LocalPollWaitTime`, `LocalTaskWaitTime` (`223-226`) | Task-list scoped writer/reader and local forwarding waits. | Writer provider at `grpc_matching.rs:36-73`; reader dispatch provider `:483-530`; local waits `:360-388`; registry consumes them; present. |
| `PartitionUpscaleRPS`, `PartitionDownscaleFactor`, upscale/downscale sustained durations, `AdaptiveScalerUpdateInterval`, `EnableAdaptiveScaler`, `EnablePartitionEmptyCheck` (`227-233`) | Task-list scoped adaptive scaler controls. | `grpc_matching.rs:598-681` ScalerConfigProvider and `matching_scaler.rs`; present. |
| `QPSTrackerInterval`, `OverrideTaskListRPS` (`234-235`) | Task-list scoped QPS tracker cadence and poller advertised rate override. | QPS provider `grpc_matching.rs:405-426`; override reader `:974-982`; registry QPS tracker; present. |
| `EnablePartitionIsolationGroupAssignment`, isolation group scaling intervals, `IsolationGroupsPerPartition` (`236-241`) | Task-list scoped partition isolation assignment/scaler controls. | `grpc_matching.rs:655-681` and `matching_scaler.rs`; present. |
| `TaskIsolationDuration`, `TaskIsolationPollerWindow` (`242-243`) | Task-list scoped isolation task age and poller recency controls. | isolation provider `grpc_matching.rs:687-778`, poller history in `matching_registry.rs`; present. |
| `HostName`, `RPCConfig` (`244`) | Startup identity/listener configuration, not dynamic properties. | Production main passes host and RPC bind/listener config into Matching startup; present in startup wiring. |
| `TaskDispatchRPS`, `TaskDispatchRPSTTL` (`246-247`) | Fixed defaults 100000 and one minute; task dispatch/poller rate limiter settings. | `matching_limiter.rs` constants and task dispatch use in `grpc_matching.rs`; present. |
| `MaxTimeBetweenTaskDeletes` (`248`) | Fixed 1 second GC cadence. | Reader/queue GC scheduling in `matching_registry.rs`; present, verify exact interval at runtime. |
| `AllIsolationGroups` (`249`) | Startup callback returning all configured isolation group names. | `system.allIsolationGroups` read in `grpc_matching.rs:677, 699+`; present. |
| `EnableStandbyTaskCompletion` (`250`) | Task-list scoped standby completion gate. | `grpc_matching.rs:778-792` provider and `matching_standby.rs`; present. |
| `EnableClientAutoConfig` (`251`) | Task-list scoped poller response hint; Go matcher uses it at `tasklist/matcher.go:457`; also engine poll response callsites. | `grpc_matching.rs:1000-1025` auto-config hint; present. |
| `EnableReturnAllTaskListKinds` (`252`) | Plain false default used by `GetTaskListsByDomain`. | `grpc_matching.rs:2058-2082` reads the exact Go key `matching.matchingReturnAllTaskListKinds`; present. |
| `ExcludeShortLivedTaskListsFromShardManager` (`253`) | Operational dynamic config; controls Matching shard distributor exclusion (`handler/engine.go:1704`). | Membership/shard routing uses Matching eligibility policy; verify it reads operational config in startup provider before acceptance. |
| `RecordTaskStartedTimeout` (`254`) | Domain-filtered timeout for History Record*TaskStarted operations. | `grpc_matching.rs:532-550` provider, passed into History calls; present. |
| `MinTaskListWritePartitions` (`255`) | Task-list scoped adaptive minimum. | `grpc_matching.rs:655` scaler config; present. |
| `MaxTaskDeleteBatchSize` (`205`) | Task-list scoped task deletion batch. | `task_delete_batch_size_provider` in `grpc_matching.rs:30-49`; present. |
| `OutstandingTaskAppendsThreshold`, `MaxTaskBatchSize` (`206-207`) | Task-list scoped writer buffering and append batch. | `task_writer_live_provider` in `grpc_matching.rs:51-76`, `matching_writer.rs`; present. |
| `ThrottledLogRPS` (`208`) | Passed to resource throttled logger, `service.go:69-73`. | **Missing**, see findings above. |
| `LongPollExpirationInterval`, `MinTaskThrottlingBurstSize` | Assigned at `config.go:203-204` (long poll), `:204` (burst). | `grpc_matching.rs:1043-1052` long poll and `:550-563` burst; present. |
| `EnablePartitionIsolationGroupAssignment` and isolation timings | Task-list filtered at `config.go:236-241`. | Scaler provider `grpc_matching.rs:655-681`; present. |

## Queued validation and implementation

Source regressions now cover RPC classification, filter/fallback behavior,
dynamic refresh, reservation refund, rejection type, and injected metric
events. Remaining focused work covers:

1. per-host and cluster-wide persistence QPS enforcement across Matching
   operations, including limiter sharing between stores/clients;
2. throttled logger max rate.

After the telemetry lane is released, run the focused Rust Matching tests and
the mixed Go/Rust Matching SDK/recovery gate. Capture effective configured
rates, blocked/released RPC class and domain, persisted task outcomes, and
process restart evidence. This audit does not establish Matching completion.

## RPC quota implementation update (2026-10-21)

The earlier user/worker rows above describe the audit-time state and are
superseded by [`matching-rpc-quotas-20261021.md`](matching-rpc-quotas-20261021.md).
Production Go keys are `matching.rps`, `matching.workerrps`,
`matching.domainrps`, and `matching.domainworkerrps`; method classification,
5-second sampling, token reservation/refund order, count-only methods,
ServiceBusy mapping, source regression coverage, and queued tests are recorded
there. A production tagged metrics exporter remains unavailable in the Rust
server; quota code has an injectable sink, with production sink wiring still
pending.
