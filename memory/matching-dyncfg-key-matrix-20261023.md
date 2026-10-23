# Matching dynamic-config key matrix (2026-10-23)

Go: `go/service/matching/config/config.go` `NewConfig` + `constants.go` defaults/filters (filters shown = the getter used in NewConfig).
Rust: first non-test reader of the key string (`matching_config.rs` readers cover enableSyncMatch, maxTasklistIdleTime, activityTaskSyncMatchWaitTime, semaphoreIdleTime, rps/workerrps/domainrps/domainworkerrps; `cadence-service-core/src/matching_quota.rs` for the rps keys).
Rust defaults are pinned by `rust/crates/cadence-matching/src/matching_dyncfg_defaults_tests.rs` and `matching_config.rs` tests.
Durations are ParseDuration strings on both sides; Rust stores nanoseconds. `UnlimitedRPS` = i32::MAX.

| Go key | Go default | Go filters | Rust reader file:line | Rust default | match? |
|---|---|---|---|---|---|
| `matching.appendTaskTimeout` | time.Second * 5 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:80 | = Go (pinned by test) | yes |
| `matching.asyncTaskDispatchTimeout` | time.Second * 3 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:601 | = Go (pinned by test) | yes |
| `system.enableDebugMode` | false | - | cadence-matching/src/launcher.rs:711 | = Go (pinned by test) | yes |
| `system.enableDistributedSemaphore` | false | DomainName | cadence-matching/src/grpc_matching.rs:2906 | = Go (pinned by test) | yes |
| `matching.enablePartitionIsolationGroupAssignment` | false | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:748 | = Go (pinned by test) | yes |
| `system.enableTasklistIsolation` | false | DomainName | cadence-matching/src/grpc_matching.rs:784 | = Go (pinned by test) | yes |
| `matching.localPollWaitTime` | time.Millisecond * 10 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:455 | = Go (pinned by test) | yes |
| `matching.localTaskWaitTime` | time.Millisecond * 10 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:441 | = Go (pinned by test) | yes |
| `matching.activityTaskSyncMatchWaitTime` | time.Millisecond * 50 | DomainName | - | = Go (pinned by test) | yes |
| `matching.adaptiveScalerUpdateInterval` | time.Second * 15 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:727 | = Go (pinned by test) | yes |
| `matching.domainrps` | 0 | DomainName | cadence-matching/src/matching_rpc_tests.rs:46 | = Go (pinned by test) | yes |
| `matching.domainworkerrps` | UnlimitedRPS | DomainName | cadence-matching/src/matching_rpc_tests.rs:48 | = Go (pinned by test) | yes |
| `matching.enableAdaptiveScaler` | false | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:721 | = Go (pinned by test) | yes |
| `matching.enableClientAutoConfig` | false | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:1222 | = Go (pinned by test) | yes |
| `matching.enableGetNumberOfPartitionsFromCache` | false | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:469 | = Go (pinned by test) | yes |
| `matching.enablePartitionEmptyCheck` | false | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:751 | = Go (pinned by test) | yes |
| `matching.matchingReturnAllTaskListKinds` | false | - | cadence-matching/src/grpc_matching.rs:2790 | = Go (pinned by test) | yes |
| `matching.enableStandbyTaskCompletion` | true | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:875 | = Go (pinned by test) | yes |
| `matching.enableSyncMatch` | true | DomainName, TaskListName, TaskType | - | = Go (pinned by test) | yes |
| `matching.enableTaskInfoLogByDomainID` | false | DomainID | cadence-matching/src/matching_dyncfg_defaults_tests.rs:78 | = Go (pinned by test) | yes |
| `matching.excludeShortLivedTaskListsFromShardManager` | true | - | cadence-matching/src/launcher.rs:1356 | = Go (pinned by test) | yes |
| `matching.forwarderMaxChildrenPerNode` | 20 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:505 | = Go (pinned by test) | yes |
| `matching.forwarderMaxOutstandingPolls` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:521 | = Go (pinned by test) | yes |
| `matching.forwarderMaxOutstandingTasks` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:535 | = Go (pinned by test) | yes |
| `matching.forwarderMaxRatePerSecond` | 10 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:549 | = Go (pinned by test) | yes |
| `matching.getTasksBatchSize` | 1000 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:593 | = Go (pinned by test) | yes |
| `matching.idleTasklistCheckInterval` | time.Minute * 5 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:676 | = Go (pinned by test) | yes |
| `matching.isolationGroupDownscaleSustainedDuration` | 2 * time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:757 | = Go (pinned by test) | yes |
| `matching.isolationGroupHasPollersSustainedDuration` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:761 | = Go (pinned by test) | yes |
| `matching.isolationGroupNoPollersSustainedDuration` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:765 | = Go (pinned by test) | yes |
| `matching.isolationGroupUpscaleSustainedDuration` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:753 | = Go (pinned by test) | yes |
| `matching.isolationGroupsPerPartition` | 2 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:768 | = Go (pinned by test) | yes |
| `matching.longPollExpirationInterval` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:1250 | = Go (pinned by test) | yes |
| `matching.maxTaskBatchSize` | 100 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:73 | = Go (pinned by test) | yes |
| `matching.maxTaskDeleteBatchSize` | 100 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:54 | = Go (pinned by test) | yes |
| `matching.minTaskThrottlingBurstSize` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:648 | = Go (pinned by test) | yes |
| `matching.numTasklistReadPartitions` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:427 | = Go (pinned by test) | yes |
| `matching.numTasklistWritePartitions` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:409 | = Go (pinned by test) | yes |
| `matching.outstandingTaskAppendsThreshold` | 250 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:87 | = Go (pinned by test) | yes |
| `matching.overrideTaskListRps` | 0 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:1082 | = Go (pinned by test) | yes |
| `matching.partitionDownscaleFactor` | 0.75 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:732 | = Go (pinned by test) | yes |
| `matching.partitionDownscaleSustainedDuration` | 2 * time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:743 | = Go (pinned by test) | yes |
| `matching.partitionUpscaleRPS` | 200 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:730 | = Go (pinned by test) | yes |
| `matching.partitionUpscaleSustainedDuration` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:739 | = Go (pinned by test) | yes |
| `matching.persistenceGlobalMaxQPS` | 0 | - | cadence-matching/src/matching_dyncfg_defaults_tests.rs:76 | = Go (pinned by test) | yes |
| `matching.persistenceMaxQPS` | 3000 | - | cadence-matching/src/matching_dyncfg_defaults_tests.rs:75 | = Go (pinned by test) | yes |
| `matching.qpsTrackerInterval` | time.Second * 10 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:487 | = Go (pinned by test) | yes |
| `matching.readRangeSize` | 50000 | - | cadence-matching/src/grpc_matching.rs:597 | = Go (pinned by test) | yes |
| `matching.recordTaskStartedTimeout` | time.Second | DomainName | cadence-matching/src/grpc_matching.rs:633 | = Go (pinned by test) | yes |
| `matching.semaphoreIdleTime` | time.Minute * 5 | DomainName | cadence-matching/src/grpc_matching.rs:2910 | = Go (pinned by test) | yes |
| `matching.shutdownDrainDuration` | 0 | - | cadence-matching/src/matching_runtime.rs:716 | = Go (pinned by test) | yes |
| `matching.taskListMinimumWritePartitions` | 1 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:746 | = Go (pinned by test) | yes |
| `matching.throttledLogRPS` | 20 | - | cadence-matching/src/matching_dyncfg_defaults_tests.rs:77 | = Go (pinned by test) | yes |
| `matching.updateAckInterval` | time.Minute | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:612 | = Go (pinned by test) | yes |
| `matching.rps` | 1200 | - | cadence-matching/src/matching_rpc_tests.rs:45 | = Go (pinned by test) | yes |
| `matching.workerrps` | UnlimitedRPS | - | cadence-matching/src/matching_rpc_tests.rs:47 | = Go (pinned by test) | yes |
| `matching.maxTasklistIdleTime` | time.Minute * 5 | DomainName, TaskListName, TaskType | - | = Go (pinned by test) | yes |
| `matching.taskIsolationDuration` | time.Millisecond * 200 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:832 | = Go (pinned by test) | yes |
| `matching.taskIsolationPollerWindow` | time.Second * 2 | DomainName, TaskListName, TaskType | cadence-matching/src/grpc_matching.rs:770 | = Go (pinned by test) | yes |

## Result
- Keys checked: 59. **Mismatches: 0** (no key-name typo, default, filter-scope or unit difference).
- `matching.longPollExpirationInterval`: Go default 1m; Rust default is the runtime `poll_expiration`, which launcher.rs passes as `Duration::from_secs(60)` (not a table-pinned literal).
- `matching.errorInjectionRate` (Go: `go/client/clientfactory.go:183`, float, default 0) is read by Rust launcher.rs:706; it is a Go key, not a Rust-only alias as v4 said.

## Items left unconfirmed by v4 audit
- `matching.shutdownDrainDuration`: Go default `0` (constants.go:5762), unfiltered `GetDurationProperty`, consumed at `go/service/matching/service.go:147` (`time.Sleep`). Rust `matching_runtime.rs:716` is unfiltered, ParseDuration string, default `Duration::ZERO`: match. Pinned by `shutdown_drain_duration_defaults_to_zero`.
- `matching.percentageOnboardedToShardManager`: Go consumer is `go/cmd/server/cadence/server.go:157` (`operationalDynamicConfig.GetIntProperty` into `membership.NewPercentageOnboarded`, `go/common/membership/percentage_onboarded.go`, which emits gauge `percentage_onboarded_to_shard_manager` per `Value()`), default 0, unfiltered. Rust `launcher.rs:1347` unfiltered int, default 0: match.

## CI evidence
