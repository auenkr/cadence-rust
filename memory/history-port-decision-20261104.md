# History decision package port — 2026-11-04

Branch: `history-port-decision`; worktree: `/mnt/storage/service/wt-port-decision`.

Scope: all 11 non-test, non-mock Go files in decision, events, query, workflowcache and reset. No local Cargo or Go validation. No integration tests or mixed-cluster gate run for this port-first task.

## Function inventory

The entries below map Go functions to Rust implementations. Constructors and private helpers sometimes map to Rust associated methods or inline branches. Dependency traits marked `TODO(link)` intentionally await the independent execution, persistence, config and transport ports; this is package-source work, not a running History service parity claim.

### `go/service/history/decision/handler.go`

Rust: `rust/crates/cadence-history/src/decision/handler.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `NewHandler` | `RpcHandler::new` |
| `HandleDecisionTaskScheduled` | `RpcHandler::handle_decision_task_scheduled / handle_decision_task_scheduled` |
| `HandleDecisionTaskStarted` | `RpcHandler::handle_decision_task_started / handle_decision_task_started` |
| `HandleDecisionTaskFailed` | `RpcHandler::handle_decision_task_failed / handle_decision_task_failed` |
| `HandleDecisionTaskCompleted` | `RpcHandler::handle_decision_task_completed / handle_decision_task_completed` |
| `createRecordDecisionTaskStartedResponse` | `create_record_decision_task_started_response` |
| `handleBufferedQueries` | `handle_buffered_queries` |
| `failDecisionHelper` | `handle_decision_task_completed failure/reload block` |
| `getActiveDomainByWorkflow` | `get_active_domain_by_workflow` |
| `getDecisionInfoAttempt` | `get_decision_info_attempt` |
| `getDecisionInfoStartedID` | `get_decision_info_started_id` |

### `go/service/history/decision/checker.go`

Rust: `rust/crates/cadence-history/src/decision/checker.rs + decision/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `newAttrValidator` | `checker::new_attr_validator` |
| `newWorkflowSizeChecker` | `checker::new_workflow_size_checker` |
| `failWorkflowIfBlobSizeExceedsLimit` | `fail_workflow_if_blob_size_exceeds_limit` |
| `failWorkflowSizeExceedsLimit` | `fail_workflow_size_exceeds_limit` |
| `validateActivityScheduleAttributes` | `checker::validate ScheduleActivityTask branch` |
| `validateTimerScheduleAttributes` | `checker::validate StartTimer branch` |
| `validateActivityCancelAttributes` | `validate_activity_cancel_attributes` |
| `validateTimerCancelAttributes` | `validate_timer_cancel_attributes` |
| `validateRecordMarkerAttributes` | `validate_record_marker_attributes` |
| `validateCompleteWorkflowExecutionAttributes` | `validate_complete_workflow_execution_attributes` |
| `validateFailWorkflowExecutionAttributes` | `validate_fail_workflow_execution_attributes` |
| `validateCancelWorkflowExecutionAttributes` | `validate_cancel_workflow_execution_attributes` |
| `validateCancelExternalWorkflowExecutionAttributes` | `validate_cancel_external_workflow_execution_attributes` |
| `validateSignalExternalWorkflowExecutionAttributes` | `validate_signal_external_workflow_execution_attributes` |
| `validateUpsertWorkflowSearchAttributes` | `validate_upsert_workflow_search_attributes` |
| `validateAcquireSemaphoreAttributes` | `validate_acquire_semaphore_attributes` |
| `validateContinueAsNewWorkflowExecutionAttributes` | `validate_continue_as_new_workflow_execution_attributes` |
| `validateStartChildExecutionAttributes` | `validate_start_child_execution_attributes` |
| `validatedTaskList` | `validated_task_list` |
| `validateCrossDomainCall` | `validate_cross_domain_call` |
| `createCrossDomainCallError` | `validate_cross_domain_call invalid-error closure` |

### `go/service/history/decision/task_handler.go`

Rust: `rust/crates/cadence-history/src/decision/task_handler.rs + decision/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `newDecisionTaskHandler` | `TaskHandler::new` |
| `handleDecisions` | `handle_decisions` |
| `handleDecisionWithResult` | `handle_decision_with_result` |
| `handleDecision` | `TaskHandler::handle_decision_with_result dispatch` |
| `handleDecisionScheduleActivity` | `TaskHandler::handle_decision_with_result ScheduleActivityTask branch` |
| `handleFailWorkflowError` | `handle_fail_workflow_error` |
| `handleDecisionRequestCancelActivity` | `TaskHandler::handle_decision_with_result RequestCancelActivityTask branch` |
| `handleDecisionStartTimer` | `TaskHandler::handle_decision_with_result StartTimer branch` |
| `handleDecisionCompleteWorkflow` | `TaskHandler::handle_decision_with_result CompleteWorkflowExecution branch` |
| `handleDecisionFailWorkflow` | `TaskHandler::handle_decision_with_result FailWorkflowExecution branch` |
| `handleWorkflowFailure` | `handle_workflow_failure` |
| `handleDecisionCancelTimer` | `TaskHandler::handle_decision_with_result CancelTimer branch` |
| `handleDecisionCancelWorkflow` | `TaskHandler::handle_decision_with_result CancelWorkflowExecution branch` |
| `handleDecisionRequestCancelExternalWorkflow` | `TaskHandler::handle_decision_with_result RequestCancelExternalWorkflowExecution branch` |
| `handleDecisionRecordMarker` | `TaskHandler::handle_decision_with_result RecordMarker branch` |
| `handleDecisionContinueAsNewWorkflow` | `TaskHandler::handle_decision_with_result ContinueAsNewWorkflowExecution branch` |
| `handleDecisionStartChildWorkflow` | `TaskHandler::handle_decision_with_result StartChildWorkflowExecution branch` |
| `handleDecisionSignalExternalWorkflow` | `TaskHandler::handle_decision_with_result SignalExternalWorkflowExecution branch` |
| `handleDecisionUpsertWorkflowSearchAttributes` | `TaskHandler::handle_decision_with_result UpsertWorkflowSearchAttributes branch` |
| `handleDecisionAcquireSemaphore` | `handle_decision_acquire_semaphore` |
| `failWorkflowSemaphoreNotFound` | `TaskHandler::handle_decision_acquire_semaphore NotFound branch` |
| `acquireWaitTimeoutSeconds` | `acquire_wait_timeout_seconds` |
| `convertSearchAttributesToByteArray` | `convert_search_attributes_to_byte_array` |
| `retryCronContinueAsNew` | `retry_cron_continue_as_new_attributes + Operations::retry_cron_continue_as_new` |
| `validateDecisionAttr` | `validate_decision_attr` |
| `handlerFailDecision` | `handler_fail_decision` |

### `go/service/history/events/blob.go`

Rust: `rust/crates/cadence-history/src/events/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `Find` | `find` |

### `go/service/history/events/cache.go`

Rust: `rust/crates/cadence-history/src/events/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `NewGlobalCache` | `EventCache::new_with_ttl / with_policy / with_observer (global adapter)` |
| `NewCache` | `EventCache::new_with_ttl / with_policy / with_observer (shard adapter)` |
| `newCacheWithOption` | `EventCache::new_with_ttl / disabled` |
| `cacheMetricsScope` | `EventCacheObserver / HistoryBranchStore metric callbacks` |
| `newEventKey` | `EventKey construction in get_event / put_event` |
| `GetEvent` | `get_event` |
| `PutEvent` | `put_event` |
| `getHistoryEventFromStore` | `StoreEventReader::read_event` |

### `go/service/history/events/notifier.go`

Rust: `rust/crates/cadence-history/src/events/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `NewNotification` | `new_notification` |
| `NewNotifier` | `Notifier::new / new_with_observer` |
| `WatchHistoryEvent` | `watch_history_event` |
| `UnwatchHistoryEvent` | `unwatch_history_event` |
| `dispatchHistoryEventNotification` | `dispatch_history_event_notification` |
| `enqueueHistoryEventNotification` | `enqueue_history_event_notification` |
| `dequeueHistoryEventNotifications` | `Notifier::run / process` |
| `Start` | `start` |
| `Stop` | `stop` |
| `NotifyNewHistoryEvent` | `notify_new_history_event` |

### `go/service/history/query/query.go`

Rust: `rust/crates/cadence-history/src/query/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `newQuery` | `Query::new` |
| `getQueryID` | `Query::id` |
| `getQueryTermCh` | `Query::term.subscribe (watch receiver)` |
| `getQueryInput` | `Query::input` |
| `getTerminationState` | `Query::termination` |
| `setTerminationState` | `Query::set_termination` |
| `validateTerminationState` | `validate_termination_state` |

### `go/service/history/query/registry.go`

Rust: `rust/crates/cadence-history/src/query/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `NewRegistry` | `Registry::new` |
| `HasBufferedQuery` | `has_buffered_query` |
| `GetBufferedIDs` | `get_buffered_i_ds` |
| `HasCompletedQuery` | `has_completed_query` |
| `GetCompletedIDs` | `get_completed_i_ds` |
| `HasUnblockedQuery` | `has_unblocked_query` |
| `GetUnblockedIDs` | `get_unblocked_i_ds` |
| `HasFailedQuery` | `has_failed_query` |
| `GetFailedIDs` | `get_failed_i_ds` |
| `GetQueryTermCh` | `get_query_term_ch` |
| `GetQueryInput` | `get_query_input` |
| `GetTerminationState` | `get_termination_state` |
| `BufferQuery` | `buffer_query` |
| `SetTerminationState` | `set_termination_state` |
| `RemoveQuery` | `remove_query` |
| `getQueryNoLock` | `Registry::get` |
| `getIDs` | `ids` |

### `go/service/history/workflowcache/cache.go`

Rust: `rust/crates/cadence-history/src/workflowcache/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `New` | `WorkflowCache::new / with_observer` |
| `allow` | `allow` |
| `emitRateLimitMetrics` | `WorkflowCacheObserver::rate_limited callback in allow` |
| `AllowExternal` | `allow_external` |
| `AllowInternal` | `allow_internal` |
| `getCacheItem` | `WorkflowCache::allow cache lookup/insert block` |
| `logError` | `WorkflowCacheObserver::unexpected_error callback in allow` |

### `go/service/history/workflowcache/metrics.go`

Rust: `rust/crates/cadence-history/src/workflowcache/mod.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `reset` | `reset` |
| `updatePerDomainMaxWFRequestCount` | `WorkflowIdCountMetric::update + WorkflowCacheObserver::per_workflow_request_count` |

### `go/service/history/reset/resetter.go`

Rust: `rust/crates/cadence-history/src/reset/mod.rs + reset/history.rs`.

| Go function | Rust function or branch |
| --- | --- |
| `NewWorkflowResetter` | `WorkflowResetter::new` |
| `ResetWorkflow` | `WorkflowResetter::reset_workflow / history::reset_workflow` |
| `prepareResetWorkflow` | `prepare_reset_workflow` |
| `persistToDB` | `persist_to_db` |
| `replayResetWorkflow` | `replay_reset_workflow` |
| `failInflightActivity` | `fail_inflight_activity` |
| `forkAndGenerateBranchToken` | `fork_and_generate_branch_token` |
| `terminateWorkflow` | `terminate_workflow` |
| `reapplyResetAndContinueAsNewWorkflowEvents` | `reapply_reset_and_continue_as_new_workflow_events` |
| `reapplyWorkflowEvents` | `reapply_workflow_events` |
| `reapplyEvents` | `reapply_events` |
| `getPaginationFn` | `ResetHistory::paginate_history and reapply_workflow_events paging loop` |
| `closePendingDecisionTask` | `close_pending_decision_task` |

## Checkpoint and validation

- Source inventory: 11 files; 123 Go functions.
- Inherited head: `a273559ae`; preserved its uncommitted five-line workflow-size checker before progressing.
- Pushed checkpoints: `d609971a2`, `93a062ffb`, `f2da23c45`, `6b0bf6d9a`, `0e3df19c3`, `640e8c635`.
- CI `41885127774`: failed formatting parse (event-cache insertion semicolon); fixed.
- CI `41886201384`: failed formatting parse (inherited reset trait declarations missing semicolons); fixed.
- CI https://github.com/auenkr/cadence-rust/actions/runs/41886856650 at `0e3df19c3`: Rust type-check reached Clippy; five lint errors, fixed in `640e8c635` (legacy reset facade removed rather than suppressing its errors).
- Current run: https://github.com/auenkr/cadence-rust/actions/runs/41888005722 at `640e8c635`; pending final result. Formatting is performed on the runner and its patch will be imported for scoped files only.

## Linkage notes

- No files belonging to execution/task/engine/persistence workers were edited.
- `decision/checker.rs` provides domain-aware normalization/validation; `decision/task_handler.rs` provides mutation control flow over primitive Operations; `decision/handler.rs` provides RPC/token/domain and completion-loop control flow over execution/cache primitives.
- `reset/history.rs` contains replay, prepare, signal-chain traversal (including pre-mutation current-run lock avoidance), one-batch persistence validation and termination. The public WorkflowResetter calls this path.
- Query uses a watch channel so termination remains observable to late waiters. Local query results preserve nil versus present-empty answers; conversion from the current shared API necessarily infers presence from result type because that shared API stores bytes in Vec.
- `PortDecision` preserves semaphore attributes omitted from current shared API. Local active-cluster-selection structs preserve the other omitted Go field until shared API wiring lands.
- Metrics, configuration, domain cache, token serialization and mutable-state/store calls remain explicit dependency traits with TODO(link). These need concrete adapters when assembling the independently ported packages.

Final acceptance still requires green CI on the final source head and importing runner formatting; no service-completion or SDK compatibility claim is made here.

## Last green CI

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41895734486 (rust-ci, package=cadence-history, clippy + tests green)
- Head SHA: 9ab1ef6187171c18af7c3dfce79c26c1f64b1715
- Note: the 6 Go files in decision/ (3 non-test) map to Rust per tables above; remaining `TODO(link)` seams await execution/persistence/config ports.
