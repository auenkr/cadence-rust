# Dynamic config key parity audit (Frontend / Matching / History)

Method: parsed all 506 `DynamicConfigs` entries in `go/common/dynamicconfig/dynamicproperties/constants.go`;
366 are referenced by non-test code in `go/service/{frontend,matching,history}`. Rust side: key string
occurrences outside the admin key registry (`cadence-persistence/src/dynamic_config_keys.rs`), history
`config.rs` getters compared mechanically (key, helper filter, default) against Go `config.go`, and each Rust
config field checked for consumers outside `config.rs`. The Go `Filters:` list in constants.go is documentation;
the real filter is the getter used in each service `config.go` (e.g. replicator batch keys list ClusterName but Go
reads them `FilteredByShardID`, as Rust does).

Classes: (a) same default+filters, (b) missing, (c) default/filter differs, (d) read but no effect.

## Summary
| Service | keys | (a) | (b) | (c) | (d) |
|---|---|---|---|---|---|
| History (config.go, 251 getters) | ~250 | all key/filter/default identical (mechanical compare, only normalisation noise) | 0 | 0 | see below |
| Matching | ~70 | all read in runtime (earlier audits memory/matching-config-property-audit-20261021.md, defaults tests `matching_dyncfg_defaults_tests.rs`) | 0 | 0 | 0 |
| Frontend | ~95 | read in workflow_backend/request_validator/launcher/handlers | see below | 0 | see below |

## Keys not (fully) honoured
| Key | Status | Fix / note |
|---|---|---|
| `history.enableCrossClusterOperations` (DomainName, false) | (d) read, `Validation::cross_cluster_operations_enabled` hardcoded `false` | **FIXED** `engine_decision_ops.rs` now calls the config getter with the source domain name (Go `decision/checker.go:929`). Default unchanged (false). |
| `history.acquireShardInterval`, `history.acquireShardConcurrency` | (d) | GAP: Rust `ShardRegistry` creates shards lazily and only reconciles on membership change; no periodic acquire pass (Go `controller.go:407,450`). Architectural. |
| `history.persistenceMaxQPS`, `history.persistenceGlobalMaxQPS`, `frontend.persistenceMaxQPS`, `frontend.persistenceGlobalMaxQPS` | (d) | GAP: no persistence-level rate limiter wrapper for Frontend/History stores (matching has its own). Needs a store wrapper. |
| `history.historyVisibilityOpenMaxQPS/ClosedMaxQPS`, `frontend.esVisibilityListMaxQPS` | (d) | GAP, same family (visibility store limiter). |
| `history.throttledLogRPS`, `frontend.throttledLogRPS` | (d) | GAP: Rust uses plain tracing, no throttled logger (log-only). |
| `system.enableVisibilitySampling` (history/frontend) | (d) | GAP: no DB visibility sampling wrapper. |
| History cache keys (`history.cacheInitialSize`, `executionCacheMaxByteSize`, `enableSizeBasedHistory*Cache`, `eventsCache*`) | (d) | GAP: Rust caches have their own sizing; behaviour-neutral except memory bounds. |
| `history.taskSchedulerType`, `taskSchedulerRoundRobinWeight`, `standbyTaskReReplicationContextTimeout`, `timerProcessor/transferProcessorFailoverMaxPollRPS`, `timerProcessorGetFailureRetryCount`, `replicatorTaskBatchSize`, `replicatorUpperLatency`, `replicationBudgetManager*` | (d) | GAP: read into Config but not consumed by Rust queue/replication code; each needs per-consumer wiring. |
| `limit.historySize.warn`, `limit.historyCount.warn`, `limit.pendingActivityCount.warn`, `system.sampleLoggingRate`, `system.enableShardIDMetrics`, `system.largeShard*MetricThreshold`, `history.emitShardDiffLog`, `history.enableTimerDebugLogByDomainID` | (d) | GAP: log/metric only. |
| `limit.identityLength/signalNameLength/requestIDLength` in History Config | n/a | Go History does not consume these either (Frontend validator does, Rust does). |
| `system.enableLogCustomerQueryParameter`, `system.writeVisibilityStoreName`, `frontend.pinotOptimizedQueryColumns` | (d)/(b) | Frontend persistence-factory/Pinot/ES-only concerns; Rust has no Pinot store. GAP. |

All other frontend/matching/history keys: (a).

## CI evidence
Gated SHA a006778059e89e09ab2cfffe13bbe6c20fe0e245 (only History touched; Frontend/Matching untouched so their gates not rerun).
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44813805446 success
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44813818023 success
- history-mixed-gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/44813831981 success, summary.json failedSteps [] (428 steps)
