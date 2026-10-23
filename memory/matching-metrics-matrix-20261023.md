# Matching metrics matrix — 2026-10-23

Base: origin/matching-candidate-20261023 (4f5fb6722). Source audit, not a runtime parity claim.
Read audits: origin/audit/matching-metrics-20261021:memory/matching-metrics-parity-20261021.md
and origin/audit/matching-parity-v2-20261022:memory/matching-go-parity-audit-v2-20261022.md.

The table includes all Matching definitions and direct metric calls in Matching,
membership, sharddistributor, and metered persistence wrappers. Shared persistence rows
include operations beyond Matching; only task/domain/semaphore consumers are in scope.
Names present in Rust are not proof of live emissions. Paths identify definitions and
Go call sites; full dynamic tag inheritance still needs review for rows marked partial.

## Tag and bucket contracts

Go NewPerTaskListScope uses `domain` and `tasklist` (never `task_list`).
Manager scopes add `operation=TaskListMgr`, `tasklistType=decision|activity`.
Scaler scopes use `operation=adaptivescaler` with those same dimensions.
Handler scopes use the RPC method as operation and caller/transport context tags;
AcquireSemaphore uses `semaphore`, not `semaphore_name`.
AddTask uses `tasklistType=decision_task|activity_task` and `matching_host`.
Sticky/ephemeral labels are `__sticky__`/`__ephemeral__`. Tasklist values are sanitized.
Leak counters add `isolation_group,leak_cause`; isolation match adds
`isolation_group,poller_isolation_group`; mismatch adds `isolation_enabled`;
partition config adds `tasklist_root_partition`.

Low1ms100s: Go scale 1, start 1ms, end 17m28.576s, 40 exponential
interior boundaries plus 0 and final bound. Mid1To16k: scale 2,
start 8, end 524288, 64 interior boundaries plus 0 and final bound.
Default1ms100s: scale 2, start 1ms, end 17m28.576s, 80 interior boundaries.
Go exponential histograms add `histogram_start,histogram_end,histogram_scale`.
Timers have reporter-configured distributions, no exponential tags.
Rollup names in defs.go emit at the root scope according to Go migration config;
Rust rollup/migration configuration remains a gap.

## Metric inventory

| Go definition / emitted name | Type | Tags | Buckets | Go references | Rust name definition | Status |
|---|---|---|---|---|---|---|
| `CadenceRequests` / `cadence_requests` | Counter | scope-specific; see source sites | — | go/service/matching/tasklist/adaptive_scaler.go:209 | rust/crates/cadence-server/src/matching_metrics.rs | implemented name; see branch notes and limitations |
| `CadenceFailures` / `cadence_errors` | Counter | scope-specific; see source sites | — | go/service/matching/tasklist/adaptive_scaler.go:213 | rust/crates/cadence-server/src/matching_metrics.rs | implemented name; see branch notes and limitations |
| `PersistenceRequests` / `persistence_requests` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:193<br>go/common/persistence/wrappers/metered/base.go:221 | rust/crates/cadence-server/src/matching_metrics.rs<br>rust/crates/cadence-server/src/matching_persistence_limiter.rs | name present; emission/tag audit required |
| `PersistenceFailures` / `persistence_errors` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:158<br>go/common/persistence/wrappers/metered/base.go:161<br>go/common/persistence/wrappers/metered/base.go:164<br>go/common/persistence/wrappers/metered/base.go:168 | rust/crates/cadence-server/src/matching_metrics.rs<br>rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceLatency` / `persistence_latency` | Timer | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:202<br>go/common/persistence/wrappers/metered/base.go:225 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceLatencyHistogram` / `persistence_latency_ns` | Histogram | scope-specific; see source sites | Default1ms100s | go/common/persistence/wrappers/metered/base.go:203<br>go/common/persistence/wrappers/metered/base.go:226<br>go/common/persistence/wrappers/metered/base.go:251 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceLatencyManualHistogram` / `persistence_latency_histogram` | Histogram | scope-specific; see source sites | PersistenceLatencyBuckets | go/common/persistence/wrappers/metered/base.go:180 | — | MISSING |
| `PersistenceLatencyHistogramPerHost` / `persistence_latency_histogram_per_host` | Histogram | scope-specific; see source sites | PersistenceLatencyBuckets | go/common/persistence/wrappers/metered/base.go:175 | — | MISSING |
| `PersistenceErrShardExistsCounter` / `persistence_errors_shard_exists` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:149 | — | MISSING |
| `PersistenceErrShardOwnershipLostCounter` / `persistence_errors_shard_ownership_lost` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:151 | — | MISSING |
| `PersistenceErrConditionFailedCounter` / `persistence_errors_condition_failed` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:145 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrCurrentWorkflowConditionFailedCounter` / `persistence_errors_current_workflow_condition_failed` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:147 | — | MISSING |
| `PersistenceErrTimeoutCounter` / `persistence_errors_timeout` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:157 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrBusyCounter` / `persistence_errors_busy` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:160 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PersistenceErrEntityNotExistsCounter` / `persistence_errors_entity_not_exists` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:153 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrExecutionAlreadyStartedCounter` / `persistence_errors_execution_already_started` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:143 | — | MISSING |
| `PersistenceErrDomainAlreadyExistsCounter` / `persistence_errors_domain_already_exists` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:139 | — | MISSING |
| `PersistenceErrBadRequestCounter` / `persistence_errors_bad_request` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:141 | — | MISSING |
| `PersistenceErrDuplicateRequestCounter` / `persistence_errors_duplicate_request` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:155 | — | MISSING |
| `PersistenceErrDBUnavailableCounter` / `persistence_errors_db_unavailable` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:163 | — | MISSING |
| `PersistenceEmptyResponseCounter` / `persistence_empty_response` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:303 | — | MISSING |
| `PersistenceResponseRowSize` / `persistence_response_row_size` | Histogram | scope-specific; see source sites | ResponseRowSizeBuckets | go/common/persistence/wrappers/metered/base.go:305 | — | MISSING |
| `PersistenceResponsePayloadSize` / `persistence_response_payload_size` | Histogram | scope-specific; see source sites | ResponsePayloadSizeBuckets | go/common/persistence/wrappers/metered/base.go:321 | — | MISSING |
| `PersistenceRequestsPerDomain` / `persistence_requests_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:191<br>go/common/persistence/wrappers/metered/base.go:241 | rust/crates/cadence-server/src/matching_metrics.rs<br>rust/crates/cadence-server/src/matching_persistence_limiter.rs | name present; emission/tag audit required |
| `PersistenceRequestsPerShard` / `persistence_requests_per_shard` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:242<br>go/common/persistence/wrappers/metered/base.go:243 | — | MISSING |
| `PersistenceFailuresPerDomain` / `persistence_errors_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:120<br>go/common/persistence/wrappers/metered/base.go:123<br>go/common/persistence/wrappers/metered/base.go:126<br>go/common/persistence/wrappers/metered/base.go:130 | rust/crates/cadence-server/src/matching_metrics.rs | implemented name; see branch notes and limitations |
| `PersistenceLatencyPerDomain` / `persistence_latency_per_domain` | Timer | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:199<br>go/common/persistence/wrappers/metered/base.go:249 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceLatencyPerDomainHistogram` / `persistence_latency_per_domain_ns` | Histogram | scope-specific; see source sites | Default1ms100s | go/common/persistence/wrappers/metered/base.go:200<br>go/common/persistence/wrappers/metered/base.go:250 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceLatencyPerShard` / `persistence_latency_per_shard` | Timer | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:252<br>go/common/persistence/wrappers/metered/base.go:255 | — | MISSING |
| `PersistenceLatencyPerShardHistogram` / `persistence_latency_per_shard_ns` | Histogram | scope-specific; see source sites | Low1ms100s | go/common/persistence/wrappers/metered/base.go:253<br>go/common/persistence/wrappers/metered/base.go:256 | — | MISSING |
| `PersistenceErrShardExistsCounterPerDomain` / `persistence_errors_shard_exists_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:111 | — | MISSING |
| `PersistenceErrShardOwnershipLostCounterPerDomain` / `persistence_errors_shard_ownership_lost_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:113 | — | MISSING |
| `PersistenceErrConditionFailedCounterPerDomain` / `persistence_errors_condition_failed_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:107 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrCurrentWorkflowConditionFailedCounterPerDomain` / `persistence_errors_current_workflow_condition_failed_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:109 | — | MISSING |
| `PersistenceErrTimeoutCounterPerDomain` / `persistence_errors_timeout_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:119 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrBusyCounterPerDomain` / `persistence_errors_busy_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:122 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PersistenceErrEntityNotExistsCounterPerDomain` / `persistence_errors_entity_not_exists_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:115 | rust/crates/cadence-server/src/matching_persistence_limiter.rs | implemented name; see branch notes and limitations |
| `PersistenceErrExecutionAlreadyStartedCounterPerDomain` / `persistence_errors_execution_already_started_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:105 | — | MISSING |
| `PersistenceErrDomainAlreadyExistsCounterPerDomain` / `persistence_errors_domain_already_exists_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:101 | — | MISSING |
| `PersistenceErrBadRequestCounterPerDomain` / `persistence_errors_bad_request_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:103 | — | MISSING |
| `PersistenceErrDuplicateRequestCounterPerDomain` / `persistence_errors_duplicate_request_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:117 | — | MISSING |
| `PersistenceErrDBUnavailableCounterPerDomain` / `persistence_errors_db_unavailable_per_domain` | Counter | scope-specific; see source sites | — | go/common/persistence/wrappers/metered/base.go:125 | — | MISSING |
| `CadenceTasklistRequests` / `cadence_tasklist_request` | Counter | scope-specific; see source sites | — | go/service/matching/handler/engine.go:549<br>go/service/matching/handler/engine.go:632 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `HashringViewIdentifier` / `hashring_view_identifier` | Counter | scope-specific; see source sites | — | go/common/membership/hashring.go:394 | — | MISSING |
| `TaskListPartitionConfigVersionGauge` / `task_list_partition_config_version` | Gauge | scope-specific; see source sites | — | go/service/matching/tasklist/task_list_manager.go:381 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPartitionConfigNumReadGauge` / `task_list_partition_config_num_read` | Gauge | scope-specific; see source sites | — | go/service/matching/tasklist/task_list_manager.go:372<br>go/service/matching/tasklist/task_list_manager.go:379 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPartitionConfigNumWriteGauge` / `task_list_partition_config_num_write` | Gauge | scope-specific; see source sites | — | go/service/matching/tasklist/task_list_manager.go:373<br>go/service/matching/tasklist/task_list_manager.go:380 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ShardDistributorResolverLookups` / `shard_distributor_resolver_lookups` | Counter | scope-specific; see source sites | — | go/common/membership/sharddistributorresolver.go:172 | rust/crates/cadence-server/src/matching_metrics.rs<br>rust/crates/cadence-server/src/matching_shard_routing.rs | implemented name; see branch notes and limitations |
| `RingResolverError` / `ring_resolver_error` | Counter | scope-specific; see source sites | — | go/common/membership/resolver.go:237 | — | MISSING |
| `PollSuccessPerTaskListCounter` / `poll_success_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:529<br>go/service/matching/tasklist/matcher.go:550<br>go/service/matching/tasklist/matcher.go:567<br>go/service/matching/tasklist/matcher.go:619<br>go/service/matching/tasklist/matcher.go:640<br>go/service/matching/tasklist/matcher.go:657<br>go/service/matching/tasklist/matcher.go:682<br>go/service/matching/tasklist/matcher.go:701<br>go/service/matching/tasklist/matcher.go:718<br>go/service/matching/tasklist/matcher.go:748<br>go/service/matching/tasklist/matcher.go:767<br>go/service/matching/tasklist/matcher.go:784 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollTimeoutPerTaskListCounter` / `poll_timeouts_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:570<br>go/service/matching/tasklist/matcher.go:660 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollSuccessWithSyncPerTaskListCounter` / `poll_success_sync_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:525<br>go/service/matching/tasklist/matcher.go:546<br>go/service/matching/tasklist/matcher.go:566<br>go/service/matching/tasklist/matcher.go:615<br>go/service/matching/tasklist/matcher.go:636<br>go/service/matching/tasklist/matcher.go:656<br>go/service/matching/tasklist/matcher.go:680<br>go/service/matching/tasklist/matcher.go:699<br>go/service/matching/tasklist/matcher.go:717<br>go/service/matching/tasklist/matcher.go:746<br>go/service/matching/tasklist/matcher.go:765<br>go/service/matching/tasklist/matcher.go:783 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `LeaseRequestPerTaskListCounter` / `lease_requests_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_writer.go:228 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `LeaseFailurePerTaskListCounter` / `lease_failures_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_writer.go:231 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ConditionFailedErrorPerTaskListCounter` / `condition_failed_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:358 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `RespondQueryTaskFailedPerTaskListCounter` / `respond_query_failed_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1094 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncThrottlePerTaskListCounter` / `sync_throttle_count_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:155<br>go/service/matching/tasklist/matcher.go:256 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `BufferThrottlePerTaskListCounter` / `buffer_throttle_count_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:544 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `BufferUnknownTaskDispatchError` / `buffer_unknown_task_dispatch_error_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:568 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `BufferIsolationGroupRedirectCounter` / `buffer_isolation_group_redirected_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `BufferIsolationGroupRedirectFailureCounter` / `buffer_isolation_group_redirect_failure_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `BufferIsolationGroupMisconfiguredCounter` / `buffer_isolation_group_misconfigured_failure_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `ExpiredTasksPerTaskListCounter` / `tasks_expired_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:365<br>go/service/matching/tasklist/task_reader.go:490 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardedPerTaskListCounter` / `forwarded_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/handler.go:167<br>go/service/matching/handler/handler.go:200<br>go/service/matching/handler/handler.go:233<br>go/service/matching/handler/handler.go:273<br>go/service/matching/handler/handler.go:314 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardTaskCallsPerTaskList` / `forward_task_calls_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `ForwardTaskErrorsPerTaskList` / `forward_task_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `SyncMatchForwardTaskThrottleErrorPerTasklist` / `sync_forward_task_throttle_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:231 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardTaskThrottleErrorPerTasklist` / `async_forward_task_throttle_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:380 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardQueryCallsPerTaskList` / `forward_query_calls_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `ForwardQueryErrorsPerTaskList` / `forward_query_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `ForwardPollCallsPerTaskList` / `forward_poll_calls_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `ForwardPollErrorsPerTaskList` / `forward_poll_errors_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | — | — | Go catalog only; no direct emission in scanned paths |
| `SyncMatchLatencyPerTaskList` / `syncmatch_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:586<br>go/service/matching/handler/engine.go:662 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncMatchLatencyPerTaskListHistogram` / `syncmatch_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/handler/engine.go:587<br>go/service/matching/handler/engine.go:663 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLatencyPerTaskList` / `asyncmatch_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:503<br>go/service/matching/handler/engine.go:1398<br>go/service/matching/handler/engine.go:1432 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLatencyPerTaskListHistogram` / `asyncmatch_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/task_reader.go:506<br>go/service/matching/handler/engine.go:1399<br>go/service/matching/handler/engine.go:1433 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchDispatchTimeoutCounterPerTaskList` / `asyncmatch_dispatch_timeouts_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:537 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardTaskLatencyPerTaskList` / `forward_task_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/forwarder.go:134 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardTaskLatencyPerTaskListHistogram` / `forward_task_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/forwarder.go:137 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardQueryLatencyPerTaskList` / `forward_query_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/forwarder.go:192 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardQueryLatencyPerTaskListHistogram` / `forward_query_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/forwarder.go:195 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardPollLatencyPerTaskList` / `forward_poll_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/forwarder.go:222 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `ForwardPollLatencyPerTaskListHistogram` / `forward_poll_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/forwarder.go:225 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `LocalToLocalMatchPerTaskListCounter` / `local_to_local_matches_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1567 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `LocalToRemoteMatchPerTaskListCounter` / `local_to_remote_matches_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1565 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `RemoteToLocalMatchPerTaskListCounter` / `remote_to_local_matches_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1563 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `RemoteToRemoteMatchPerTaskListCounter` / `remote_to_remote_matches_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1561 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationTaskMatchPerTaskListCounter` / `isolation_task_matches_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1581 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationSuccessPerTaskListCounter` / `isolation_success_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:1583 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollerPerTaskListCounter` / `poller_count_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:213 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollerInvalidIsolationGroupCounter` / `poller_invalid_isolation_group_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:444 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPartitionUpdateFailedCounter` / `tasklist_partition_update_failed_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:492 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListManagersGauge` / `tasklist_managers` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_registry.go:143 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskLagPerTaskListGauge` / `task_lag_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:395 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskBacklogPerTaskListGauge` / `task_backlog_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:231 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskCountPerTaskListGauge` / `task_count_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_reader.go:273 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `RateLimitPerTaskListGauge` / `rate_limit_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_limiter.go:103<br>go/service/matching/tasklist/task_list_limiter.go:112 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncMatchLocalPollLatencyPerTaskList` / `syncmatch_local_poll_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:175<br>go/service/matching/tasklist/matcher.go:203<br>go/service/matching/tasklist/matcher.go:265 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncMatchLocalPollLatencyPerTaskListHistogram` / `syncmatch_local_poll_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:176<br>go/service/matching/tasklist/matcher.go:204<br>go/service/matching/tasklist/matcher.go:266 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncMatchForwardPollLatencyPerTaskList` / `syncmatch_forward_poll_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:226 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `SyncMatchForwardPollLatencyPerTaskListHistogram` / `syncmatch_forward_poll_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:227 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollCounterPerTaskList` / `asyncmatch_local_poll_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:339<br>go/service/matching/tasklist/matcher.go:366 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAttemptPerTaskList` / `asyncmatch_local_poll_attempt_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:367 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAttemptPerTaskListHistogram` / `asyncmatch_local_poll_attempt_per_tl_counts` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Mid1To16k | go/service/matching/tasklist/matcher.go:368 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollLatencyPerTaskList` / `asyncmatch_local_poll_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:340<br>go/service/matching/tasklist/matcher.go:369 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollLatencyPerTaskListHistogram` / `asyncmatch_local_poll_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:341<br>go/service/matching/tasklist/matcher.go:370 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardPollCounterPerTaskList` / `asyncmatch_forward_poll_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:415 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardPollAttemptPerTaskList` / `asyncmatch_forward_poll_attempt_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:416 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardPollAttemptPerTaskListHistogram` / `asyncmatch_forward_poll_attempt_per_tl_counts` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Mid1To16k | go/service/matching/tasklist/matcher.go:417 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardPollLatencyPerTaskList` / `asyncmatch_forward_poll_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:418 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchForwardPollLatencyPerTaskListHistogram` / `asyncmatch_forward_poll_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:419 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAfterForwardFailedCounterPerTaskList` / `asyncmatch_local_poll_after_forward_failed_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:399 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAfterForwardFailedAttemptPerTaskList` / `asyncmatch_local_poll_after_forward_failed_attempt_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:400 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAfterForwardFailedAttemptPerTaskListHistogram` / `asyncmatch_local_poll_after_forward_failed_attempt_per_tl_counts` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Mid1To16k | go/service/matching/tasklist/matcher.go:401 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAfterForwardFailedLatencyPerTaskList` / `asyncmatch_local_poll_after_forward_failed_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:402 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `AsyncMatchLocalPollAfterForwardFailedLatencyPerTaskListHistogram` / `asyncmatch_local_poll_after_forward_failed_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:403 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollLocalMatchLatencyPerTaskList` / `poll_local_match_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:465<br>go/service/matching/tasklist/matcher.go:494<br>go/service/matching/tasklist/matcher.go:527<br>go/service/matching/tasklist/matcher.go:548 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollLocalMatchLatencyPerTaskListHistogram` / `poll_local_match_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:466<br>go/service/matching/tasklist/matcher.go:495<br>go/service/matching/tasklist/matcher.go:528<br>go/service/matching/tasklist/matcher.go:549 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollForwardMatchLatencyPerTaskList` / `poll_forward_match_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:590 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollForwardMatchLatencyPerTaskListHistogram` / `poll_forward_match_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:591 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollLocalMatchAfterForwardFailedLatencyPerTaskList` / `poll_local_match_after_forward_failed_latency_per_tl` | Timer | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/matcher.go:617<br>go/service/matching/tasklist/matcher.go:638 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollLocalMatchAfterForwardFailedLatencyPerTaskListHistogram` / `poll_local_match_after_forward_failed_latency_per_tl_ns` | Histogram | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | Low1ms100s | go/service/matching/tasklist/matcher.go:618<br>go/service/matching/tasklist/matcher.go:639 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollDecisionTaskAlreadyStartedCounterPerTaskList` / `poll_decision_task_already_started_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:835 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PollActivityTaskAlreadyStartedCounterPerTaskList` / `poll_activity_task_already_started_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/handler/engine.go:988 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListReadWritePartitionMismatchGauge` / `tasklist_read_write_partition_mismatch` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:960 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPollerPartitionMismatchGauge` / `tasklist_poller_partition_mismatch` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:964 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `EstimatedAddTaskQPSGauge` / `estimated_add_task_qps_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:567<br>go/service/matching/tasklist/adaptive_scaler.go:238 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPartitionUpscaleThresholdGauge` / `tasklist_partition_upscale_threshold` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:239 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskListPartitionDownscaleThresholdGauge` / `tasklist_partition_downscale_threshold` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:240 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `StandbyClusterTasksCompletedCounterPerTaskList` / `standby_cluster_tasks_completed_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_completer.go:128<br>go/service/matching/tasklist/task_completer.go:150 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `StandbyClusterTasksNotStartedCounterPerTaskList` / `standby_cluster_tasks_not_started_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_completer.go:155 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `StandbyClusterTasksCompletionFailurePerTaskList` / `standby_cluster_tasks_completion_failure_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_completer.go:163 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `TaskIsolationLeakPerTaskList` / `task_isolation_leak_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/task_list_manager.go:917<br>go/service/matching/tasklist/task_list_manager.go:921<br>go/service/matching/tasklist/task_list_manager.go:925<br>go/service/matching/tasklist/task_list_manager.go:929<br>go/service/matching/tasklist/task_list_manager.go:934<br>go/service/matching/tasklist/task_list_manager.go:946 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PartitionUpscale` / `partition_upscale_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:248 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PartitionDownscale` / `partition_downscale_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:253 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `PartitionDrained` / `partition_drained_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:302 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationRebalance` / `isolation_rebalance_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/adaptive_scaler.go:170 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationGroupStartedPolling` / `ig_started_polling_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/isolation_balancer.go:152 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationGroupStoppedPolling` / `ig_stopped_polling_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/isolation_balancer.go:146 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationGroupUpscale` / `ig_upscale_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/isolation_balancer.go:190 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationGroupDownscale` / `ig_downscale_per_tl` | Counter | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/isolation_balancer.go:192 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |
| `IsolationGroupPartitionsGauge` / `ig_partitions_per_tl` | Gauge | manager: operation=TaskListMgr, domain, tasklist, tasklistType; see exceptions below | — | go/service/matching/tasklist/isolation_balancer.go:166<br>go/service/matching/tasklist/isolation_balancer.go:200 | rust/crates/cadence-server/src/matching_metrics.rs | name present; emission/tag audit required |

## Scope inventory

```go
PersistenceGetTransferTasksScope:                         {operation: "GetTransferTasks"},
PersistenceCompleteTransferTaskScope:                     {operation: "CompleteTransferTask"},
PersistenceGetCrossClusterTasksScope:                     {operation: "GetCrossClusterTasks"},
PersistenceCompleteCrossClusterTaskScope:                 {operation: "GetCrossClusterTasks"},
PersistenceGetReplicationTasksScope:                      {operation: "GetReplicationTasks"},
PersistenceCompleteReplicationTaskScope:                  {operation: "CompleteReplicationTask"},
PersistencePutReplicationTaskToDLQScope:                  {operation: "PutReplicationTaskToDLQ"},
PersistenceGetReplicationTasksFromDLQScope:               {operation: "GetReplicationTasksFromDLQ"},
PersistenceDeleteReplicationTaskFromDLQScope:             {operation: "DeleteReplicationTaskFromDLQ"},
PersistenceRangeDeleteReplicationTaskFromDLQScope:        {operation: "RangeDeleteReplicationTaskFromDLQ"},
PersistenceCreateFailoverMarkerTasksScope:                {operation: "CreateFailoverMarkerTasks"},
PersistenceCreateHistoryTasksScope:                       {operation: "CreateHistoryTasks"},
PersistenceGetTimerIndexTasksScope:                       {operation: "GetTimerIndexTasks"},
PersistenceCompleteTimerTaskScope:                        {operation: "CompleteTimerTask"},
PersistenceGetHistoryTasksScope:                          {operation: "GetHistoryTasks"},
PersistenceCompleteHistoryTaskScope:                      {operation: "CompleteHistoryTask"},
PersistenceRangeCompleteHistoryTaskScope:                 {operation: "RangeCompleteHistoryTask"},
PersistenceCreateTasksScope:                              {operation: "CreateTask"},
PersistenceGetTasksScope:                                 {operation: "GetTasks"},
PersistenceCompleteTaskScope:                             {operation: "CompleteTask"},
PersistenceCompleteTasksLessThanScope:                    {operation: "CompleteTasksLessThan"},
PersistenceGetOrphanTasksScope:                           {operation: "GetOrphanTasks"},
PersistenceLeaseTaskListScope:                            {operation: "LeaseTaskList"},
PersistenceGetTaskListScope:                              {operation: "GetTaskList"},
PersistenceUpdateTaskListScope:                           {operation: "UpdateTaskList"},
PersistenceListTaskListScope:                             {operation: "ListTaskList"},
PersistenceDeleteTaskListScope:                           {operation: "DeleteTaskList"},
PersistenceGetTaskListSizeScope:                          {operation: "GetTaskListSize"},
PersistenceCreateDomainScope:                             {operation: "CreateDomain"},
PersistenceGetDomainScope:                                {operation: "GetDomain"},
PersistenceUpdateDomainScope:                             {operation: "UpdateDomain"},
PersistenceDeleteDomainScope:                             {operation: "DeleteDomain"},
PersistenceDeleteDomainByNameScope:                       {operation: "DeleteDomainByName"},
PersistenceListDomainsScope:                              {operation: "ListDomain"},
PersistenceGetMetadataScope:                              {operation: "GetMetadata"},
PersistenceFetchWorkflowTimerTasksForCleanupScope:        {operation: "FetchWorkflowTimerTasksForCleanup"},
ResolverHostNotFoundScope:                                {operation: "ResolverHostNotFound"},
MatchingClientPollForDecisionTaskScope:            {operation: "MatchingClientPollForDecisionTask", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientPollForActivityTaskScope:            {operation: "MatchingClientPollForActivityTask", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientAddActivityTaskScope:                {operation: "MatchingClientAddActivityTask", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientAddDecisionTaskScope:                {operation: "MatchingClientAddDecisionTask", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientQueryWorkflowScope:                  {operation: "MatchingClientQueryWorkflow", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientRespondQueryTaskCompletedScope:      {operation: "MatchingClientRespondQueryTaskCompleted", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientCancelOutstandingPollScope:          {operation: "MatchingClientCancelOutstandingPoll", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientDescribeTaskListScope:               {operation: "MatchingClientDescribeTaskList", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientListTaskListPartitionsScope:         {operation: "MatchingClientListTaskListPartitions", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientGetTaskListsByDomainScope:           {operation: "MatchingClientGetTaskListsByDomain", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientUpdateTaskListPartitionConfigScope:  {operation: "MatchingClientUpdateTaskListPartitionConfig", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientRefreshTaskListPartitionConfigScope: {operation: "MatchingClientRefreshTaskListPartitionConfig", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
MatchingClientAcquireSemaphoreScope:               {operation: "MatchingClientAcquireSemaphore", tags: map[string]string{CadenceRoleTagName: MatchingClientRoleTagValue}},
HashringScope:                 {operation: "Hashring"},
ShardManagerOnboardingScope:   {operation: "ShardManagerOnboarding"},
ShardDistributorResolverScope: {operation: "ShardDistributorResolver"},
PersistenceCreateHistoryDLQTaskScope:                {operation: "CreateHistoryDLQTask"},
PersistenceGetHistoryDLQTasksScope:                  {operation: "GetHistoryDLQTasks"},
PersistenceDeleteHistoryDLQTasksScope:               {operation: "DeleteHistoryDLQTasks"},
MatchingPollForDecisionTaskScope:            {operation: "PollForDecisionTask"},
MatchingPollForActivityTaskScope:            {operation: "PollForActivityTask"},
MatchingAddActivityTaskScope:                {operation: "AddActivityTask"},
MatchingAddDecisionTaskScope:                {operation: "AddDecisionTask"},
MatchingAddTaskScope:                        {operation: "AddTask"},
MatchingTaskListMgrScope:                    {operation: "TaskListMgr"},
MatchingAdaptiveScalerScope:                 {operation: "adaptivescaler"},
MatchingQueryWorkflowScope:                  {operation: "QueryWorkflow"},
MatchingRespondQueryTaskCompletedScope:      {operation: "RespondQueryTaskCompleted"},
MatchingCancelOutstandingPollScope:          {operation: "CancelOutstandingPoll"},
MatchingDescribeTaskListScope:               {operation: "DescribeTaskList"},
MatchingListTaskListPartitionsScope:         {operation: "ListTaskListPartitions"},
MatchingGetTaskListsByDomainScope:           {operation: "GetTaskListsByDomain"},
MatchingUpdateTaskListPartitionConfigScope:  {operation: "UpdateTaskListPartitionConfig"},
MatchingRefreshTaskListPartitionConfigScope: {operation: "RefreshTaskListPartitionConfig"},
MatchingAcquireSemaphoreScope:               {operation: "AcquireSemaphore"},
```

## Remaining gaps

- Backlog forwarding emissions require the separate backlog behavior work; do not fabricate them on local dispatch.
- Shared persistence domain/semaphore success/latency/error emissions and membership view/error wiring need completion. Task-store success/typed errors and shard-routing emissions are now wired.
- Engine counters now inherit task-local RPC tags; background poll completion falls back to the poll RPC scope. Full mixed-cluster metric comparison remains required.
- Go rollups, migration selection, domain-all timer duplication and optional persistence histograms need implementation.
- Matrix statuses are conservative: a constant/helper is not proof of an emit site.

## Implemented in this branch

- Canonical keys: `tasklist` and `semaphore`; no aliases emitted.
- Manager counter/gauge/timer/histogram helpers add `operation=TaskListMgr`
  and `tasklistType=activity|decision`. Registry manager-count gauge remains
  operation-only, matching Go's registry scope.
- Scaler metrics use `operation=adaptivescaler`; update attempts/failures
  emit `cadence_requests` / `cadence_errors`. AddTask arrival QPS is emitted
  independently of scaler enablement (non-forwarded requests only).
- Engine forwarded/match/isolation/already-started/query-response counters
  inherit the current handler request scope (including caller/transport).
- Matching duration and integer histograms carry Go metadata tags with
  scale 1 / 2 respectively. Their existing bucket builders are retained.
- Task-store calls emit the Go metered request, timer, full-resolution
  Default1ms100s histogram, and classified error counters. GetTaskList is
  overall (`task_category=none`); the other task calls are per-domain,
  without task_category. `is_retry=false` reflects the current adapter boundary;
  retry context propagation is not implemented. A weak reference to the existing
  domain cache supplies names using cached snapshots only.
- Persistence failure names corrected to `persistence_errors` and
  `persistence_errors_per_domain`. ConditionFailed/NotFound do not also
  count as generic failures; Timeout/Throttled do.
- All three exporters support persistence Default1ms100s (scale 2,
  80 interior boundaries, 0 and final bound).
- Shard router resolve emits `percentage_onboarded_to_shard_manager`
  (`operation=ShardManagerOnboarding`) and
  `shard_distributor_resolver_lookups` (`operation=ShardDistributorResolver`,
  `routing_path=hash_ring|shard_distributor`) before lookup, including failures.
  LookupN deliberately does not emit the single-lookup routing counter.

## Unresolved parity dependencies / limitations

This branch is **partial**, not a declaration of Matching metrics completion.
The ten backlog forwarding series have helpers but no behavior path at the base;
they must be wired by the backlog-forwarding implementation, not fabricated on
local handoff. Membership `hashring_view_identifier` (Go UpdateGauge despite
Counter catalog type) and `ring_resolver_error` still lack emit sites. External
shard-manager client/executor/spectator metrics are outside the direct Go
`common/metrics/defs.go` inventory and need a separate upstream-client audit.
Domain metadata / semaphore metering, persistence retry tagging, row-count and
payload-size metrics, optional per-host/manual persistence histograms, and
rollup/migration-config handling remain incomplete. QPS rejections have Go
request/error counters but still lack latency observations.

## Validation

See memory/evidence/matching-metrics-parity-20261023/validation.md for actual
command exit codes. Name-present rows above are source inventory findings,
not blanket parity claims; tests only establish the explicitly asserted paths.
