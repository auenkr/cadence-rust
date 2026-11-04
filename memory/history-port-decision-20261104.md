# History port decision checklist (2026-11-04)

Branch: `history-port-decision` (`/mnt/storage/service/wt-port-decision`). Scope includes every non-test, non-mock Go source file requested by the task. This ledger is an implementation checklist; `partial` means the Rust counterpart is present but still needs full Go behavior parity.

## decision

### `go/service/history/decision/checker.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `newAttrValidator` -> `new_attr_validator()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `newWorkflowSizeChecker` -> `new_workflow_size_checker()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `failWorkflowIfBlobSizeExceedsLimit` -> `fail_workflow_if_blob_size_exceeds_limit()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `failWorkflowSizeExceedsLimit` -> `fail_workflow_size_exceeds_limit()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `validateActivityScheduleAttributes` -> `validate_activity_schedule_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateTimerScheduleAttributes` -> `validate_timer_schedule_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateActivityCancelAttributes` -> `validate_activity_cancel_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateTimerCancelAttributes` -> `validate_timer_cancel_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateRecordMarkerAttributes` -> `validate_record_marker_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateCompleteWorkflowExecutionAttributes` -> `validate_complete_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateFailWorkflowExecutionAttributes` -> `validate_fail_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateCancelWorkflowExecutionAttributes` -> `validate_cancel_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateCancelExternalWorkflowExecutionAttributes` -> `validate_cancel_external_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `validateSignalExternalWorkflowExecutionAttributes` -> `validate_signal_external_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `validateUpsertWorkflowSearchAttributes` -> `validate_upsert_workflow_search_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateAcquireSemaphoreAttributes` -> `validate_acquire_semaphore_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `validateContinueAsNewWorkflowExecutionAttributes` -> `validate_continue_as_new_workflow_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateStartChildExecutionAttributes` -> `validate_start_child_execution_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validatedTaskList` -> `validated_task_list()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateCrossDomainCall` -> `validate_cross_domain_call()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `createCrossDomainCallError` -> `create_cross_domain_call_error()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)

### `go/service/history/decision/handler.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `NewHandler` -> `new_handler()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `HandleDecisionTaskScheduled` -> `handle_decision_task_scheduled()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `HandleDecisionTaskStarted` -> `handle_decision_task_started()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `HandleDecisionTaskFailed` -> `handle_decision_task_failed()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `HandleDecisionTaskCompleted` -> `handle_decision_task_completed()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `createRecordDecisionTaskStartedResponse` -> `create_record_decision_task_started_response()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleBufferedQueries` -> `handle_buffered_queries()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `failDecisionHelper` -> `fail_decision_helper()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getActiveDomainByWorkflow` -> `get_active_domain_by_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getDecisionInfoAttempt` -> `get_decision_info_attempt()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `getDecisionInfoStartedID` -> `get_decision_info_started_id()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)

### `go/service/history/decision/task_handler.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `newDecisionTaskHandler` -> `new_decision_task_handler()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisions` -> `handle_decisions()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handleDecisionWithResult` -> `handle_decision_with_result()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handleDecision` -> `handle_decision()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handleDecisionScheduleActivity` -> `handle_decision_schedule_activity()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleFailWorkflowError` -> `handle_fail_workflow_error()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handleDecisionRequestCancelActivity` -> `handle_decision_request_cancel_activity()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionStartTimer` -> `handle_decision_start_timer()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionCompleteWorkflow` -> `handle_decision_complete_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionFailWorkflow` -> `handle_decision_fail_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleWorkflowFailure` -> `handle_workflow_failure()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handleDecisionCancelTimer` -> `handle_decision_cancel_timer()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionCancelWorkflow` -> `handle_decision_cancel_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionRequestCancelExternalWorkflow` -> `handle_decision_request_cancel_external_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionRecordMarker` -> `handle_decision_record_marker()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionContinueAsNewWorkflow` -> `handle_decision_continue_as_new_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionStartChildWorkflow` -> `handle_decision_start_child_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionSignalExternalWorkflow` -> `handle_decision_signal_external_workflow()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionUpsertWorkflowSearchAttributes` -> `handle_decision_upsert_workflow_search_attributes()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `handleDecisionAcquireSemaphore` -> `handle_decision_acquire_semaphore()` in `rust/crates/cadence-history/src/decision/mod.rs` (missing; Rust entry point not implemented)
- [ ] `failWorkflowSemaphoreNotFound` -> `fail_workflow_semaphore_not_found()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `acquireWaitTimeoutSeconds` -> `acquire_wait_timeout_seconds()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `convertSearchAttributesToByteArray` -> `convert_search_attributes_to_byte_array()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `retryCronContinueAsNew` -> `retry_cron_continue_as_new()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `validateDecisionAttr` -> `validate_decision_attr()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)
- [ ] `handlerFailDecision` -> `handler_fail_decision()` in `rust/crates/cadence-history/src/decision/mod.rs` (partial; parity pending)

## events

### `go/service/history/events/blob.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `Find` -> `find()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)

### `go/service/history/events/cache.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `NewGlobalCache` -> `new_global_cache()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `NewCache` -> `new_cache()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `newCacheWithOption` -> `new_cache_with_option()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `cacheMetricsScope` -> `cache_metrics_scope()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `newEventKey` -> `new_event_key()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `GetEvent` -> `get_event()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `PutEvent` -> `put_event()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `getHistoryEventFromStore` -> `get_history_event_from_store()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)

### `go/service/history/events/notifier.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `NewNotification` -> `new_notification()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `NewNotifier` -> `new_notifier()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `WatchHistoryEvent` -> `watch_history_event()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `UnwatchHistoryEvent` -> `unwatch_history_event()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `dispatchHistoryEventNotification` -> `dispatch_history_event_notification()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `enqueueHistoryEventNotification` -> `enqueue_history_event_notification()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `dequeueHistoryEventNotifications` -> `dequeue_history_event_notifications()` in `rust/crates/cadence-history/src/events/mod.rs` (missing; Rust entry point not implemented)
- [ ] `Start` -> `start()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `Stop` -> `stop()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)
- [ ] `NotifyNewHistoryEvent` -> `notify_new_history_event()` in `rust/crates/cadence-history/src/events/mod.rs` (partial; parity pending)

## query

### `go/service/history/query/query.go` -> `rust/crates/cadence-history/src/query/mod.rs`

- [ ] `newQuery` -> `new_query()` in `rust/crates/cadence-history/src/query/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getQueryID` -> `get_query_id()` in `rust/crates/cadence-history/src/query/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getQueryTermCh` -> `get_query_term_ch()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `getQueryInput` -> `get_query_input()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `getTerminationState` -> `get_termination_state()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `setTerminationState` -> `set_termination_state()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `validateTerminationState` -> `validate_termination_state()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)

### `go/service/history/query/registry.go` -> `rust/crates/cadence-history/src/query/mod.rs`

- [ ] `NewRegistry` -> `new_registry()` in `rust/crates/cadence-history/src/query/mod.rs` (missing; Rust entry point not implemented)
- [ ] `HasBufferedQuery` -> `has_buffered_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetBufferedIDs` -> `get_buffered_ids()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `HasCompletedQuery` -> `has_completed_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetCompletedIDs` -> `get_completed_ids()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `HasUnblockedQuery` -> `has_unblocked_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetUnblockedIDs` -> `get_unblocked_ids()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `HasFailedQuery` -> `has_failed_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetFailedIDs` -> `get_failed_ids()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetQueryTermCh` -> `get_query_term_ch()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetQueryInput` -> `get_query_input()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `GetTerminationState` -> `get_termination_state()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `BufferQuery` -> `buffer_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `SetTerminationState` -> `set_termination_state()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `RemoveQuery` -> `remove_query()` in `rust/crates/cadence-history/src/query/mod.rs` (partial; parity pending)
- [ ] `getQueryNoLock` -> `get_query_no_lock()` in `rust/crates/cadence-history/src/query/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getIDs` -> `get_ids()` in `rust/crates/cadence-history/src/query/mod.rs` (missing; Rust entry point not implemented)

## workflowcache

### `go/service/history/workflowcache/cache.go` -> `rust/crates/cadence-history/src/workflowcache/mod.rs`

- [ ] `New` -> `new()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; parity pending)
- [ ] `allow` -> `allow()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; parity pending)
- [ ] `emitRateLimitMetrics` -> `emit_rate_limit_metrics()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (missing; Rust entry point not implemented)
- [ ] `AllowExternal` -> `allow_external()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; parity pending)
- [ ] `AllowInternal` -> `allow_internal()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; parity pending)
- [ ] `getCacheItem` -> `get_cache_item()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (missing; Rust entry point not implemented)
- [ ] `logError` -> `log_error()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (missing; Rust entry point not implemented)

### `go/service/history/workflowcache/metrics.go` -> `rust/crates/cadence-history/src/workflowcache/mod.rs`

- [ ] `reset` -> `reset()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; parity pending)
- [ ] `updatePerDomainMaxWFRequestCount` -> `update_per_domain_max_wfrequest_count()` in `rust/crates/cadence-history/src/workflowcache/mod.rs` (missing; Rust entry point not implemented)

## reset

### `go/service/history/reset/resetter.go` -> `rust/crates/cadence-history/src/reset/mod.rs`

- [ ] `NewWorkflowResetter` -> `new_workflow_resetter()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `ResetWorkflow` -> `reset_workflow()` in `rust/crates/cadence-history/src/reset/mod.rs` (partial; parity pending)
- [ ] `prepareResetWorkflow` -> `prepare_reset_workflow()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `persistToDB` -> `persist_to_db()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `replayResetWorkflow` -> `replay_reset_workflow()` in `rust/crates/cadence-history/src/reset/mod.rs` (partial; parity pending)
- [ ] `failInflightActivity` -> `fail_inflight_activity()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `forkAndGenerateBranchToken` -> `fork_and_generate_branch_token()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `terminateWorkflow` -> `terminate_workflow()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `reapplyResetAndContinueAsNewWorkflowEvents` -> `reapply_reset_and_continue_as_new_workflow_events()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `reapplyWorkflowEvents` -> `reapply_workflow_events()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `reapplyEvents` -> `reapply_events()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `getPaginationFn` -> `get_pagination_fn()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)
- [ ] `closePendingDecisionTask` -> `close_pending_decision_task()` in `rust/crates/cadence-history/src/reset/mod.rs` (missing; Rust entry point not implemented)

## Current implementation and validation status

- Source inventory: 11 requested Go files, 5,202 source lines, excluding tests and mocks.
- Rust modules exist for all five packages and provide query state management, event blob/cache/notifier primitives, workflow cache/rate limit primitives, decision attribute/timeout helpers and dispatch interfaces, and a reset orchestration/backend seam.
- The checklist entries remain unchecked because substantial behavior is still delegated to backend traits or lacks a full translation. Specifically, decision RPC/decision side effects, several validator cases (search attributes, semaphore, child/cross-domain setup), reset replay/reapply/persistence internals, and metrics/logging parity still require implementation. This branch is not a completion/parity claim.
- No Cargo command was run locally. A package CI run was dispatched at `https://github.com/auenkr/cadence-rust/actions/runs/41878622967` for head `04f6c010ad1ec6a60634a595a0d0599fb3f3e773`; it remained queued at the last check. This run predates subsequent unpushed changes and cannot establish validation of the final head.
- CI compile diagnostic at `a80e5011b9570556f1fada35b57492de2ad64562`: `E0716` from the temporary async receiver lock in `events::Notifier::run`. Fixed by holding a named receiver guard across `tokio::select!` and dropping it after selection. The failure log was retrieved from run `41880200309`; rerun needed on the new head.
