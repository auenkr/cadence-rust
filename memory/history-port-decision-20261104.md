# History port decision checklist (2026-11-04)

Branch: `history-port-decision` (`/mnt/storage/service/wt-port-decision`). Scope includes every non-test, non-mock Go source file requested by the task. This ledger is an implementation checklist; `partial` means the Rust counterpart is present but still needs full Go behavior parity.

## decision

### `go/service/history/decision/checker.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `newAttrValidator` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `newWorkflowSizeChecker` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `failWorkflowIfBlobSizeExceedsLimit` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `failWorkflowSizeExceedsLimit` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateActivityScheduleAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateTimerScheduleAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateActivityCancelAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateTimerCancelAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateRecordMarkerAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateCompleteWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateFailWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateCancelWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateCancelExternalWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateSignalExternalWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateUpsertWorkflowSearchAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateAcquireSemaphoreAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateContinueAsNewWorkflowExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateStartChildExecutionAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validatedTaskList` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateCrossDomainCall` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `createCrossDomainCallError` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/decision/handler.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `NewHandler` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `HandleDecisionTaskScheduled` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `HandleDecisionTaskStarted` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `HandleDecisionTaskFailed` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `HandleDecisionTaskCompleted` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `createRecordDecisionTaskStartedResponse` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleBufferedQueries` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `failDecisionHelper` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `getActiveDomainByWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `getDecisionInfoAttempt` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `getDecisionInfoStartedID` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/decision/task_handler.go` -> `rust/crates/cadence-history/src/decision/mod.rs`

- [ ] `newDecisionTaskHandler` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisions` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionWithResult` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecision` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionScheduleActivity` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleFailWorkflowError` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionRequestCancelActivity` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionStartTimer` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionCompleteWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionFailWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleWorkflowFailure` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionCancelTimer` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionCancelWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionRequestCancelExternalWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionRecordMarker` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionContinueAsNewWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionStartChildWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionSignalExternalWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionUpsertWorkflowSearchAttributes` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handleDecisionAcquireSemaphore` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `failWorkflowSemaphoreNotFound` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `acquireWaitTimeoutSeconds` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `convertSearchAttributesToByteArray` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `retryCronContinueAsNew` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateDecisionAttr` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)
- [ ] `handlerFailDecision` — Rust counterpart in `rust/crates/cadence-history/src/decision/mod.rs` (partial; behavior parity audit pending)

## events

### `go/service/history/events/blob.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `Find` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/events/cache.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `NewGlobalCache` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `NewCache` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `newCacheWithOption` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `cacheMetricsScope` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `newEventKey` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetEvent` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `PutEvent` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `getHistoryEventFromStore` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/events/notifier.go` -> `rust/crates/cadence-history/src/events/mod.rs`

- [ ] `NewNotification` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `NewNotifier` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `WatchHistoryEvent` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `UnwatchHistoryEvent` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `dispatchHistoryEventNotification` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `enqueueHistoryEventNotification` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `dequeueHistoryEventNotifications` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `Start` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `Stop` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)
- [ ] `NotifyNewHistoryEvent` — Rust counterpart in `rust/crates/cadence-history/src/events/mod.rs` (partial; behavior parity audit pending)

## query

### `go/service/history/query/query.go` -> `rust/crates/cadence-history/src/query/mod.rs`

- [ ] `newQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getQueryID` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getQueryTermCh` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getQueryInput` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getTerminationState` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `setTerminationState` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `validateTerminationState` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/query/registry.go` -> `rust/crates/cadence-history/src/query/mod.rs`

- [ ] `NewRegistry` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `HasBufferedQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetBufferedIDs` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `HasCompletedQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetCompletedIDs` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `HasUnblockedQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetUnblockedIDs` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `HasFailedQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetFailedIDs` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetQueryTermCh` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetQueryInput` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `GetTerminationState` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `BufferQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `SetTerminationState` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `RemoveQuery` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getQueryNoLock` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)
- [ ] `getIDs` — Rust counterpart in `rust/crates/cadence-history/src/query/mod.rs` (partial; behavior parity audit pending)

## workflowcache

### `go/service/history/workflowcache/cache.go` -> `rust/crates/cadence-history/src/workflowcache/mod.rs`

- [ ] `New` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `allow` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `emitRateLimitMetrics` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `AllowExternal` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `AllowInternal` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `getCacheItem` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `logError` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)

### `go/service/history/workflowcache/metrics.go` -> `rust/crates/cadence-history/src/workflowcache/mod.rs`

- [ ] `reset` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)
- [ ] `updatePerDomainMaxWFRequestCount` — Rust counterpart in `rust/crates/cadence-history/src/workflowcache/mod.rs` (partial; behavior parity audit pending)

## reset

### `go/service/history/reset/resetter.go` -> `rust/crates/cadence-history/src/reset/mod.rs`

- [ ] `NewWorkflowResetter` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `ResetWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `prepareResetWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `persistToDB` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `replayResetWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `failInflightActivity` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `forkAndGenerateBranchToken` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `terminateWorkflow` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `reapplyResetAndContinueAsNewWorkflowEvents` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `reapplyWorkflowEvents` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `reapplyEvents` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `getPaginationFn` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
- [ ] `closePendingDecisionTask` — Rust counterpart in `rust/crates/cadence-history/src/reset/mod.rs` (partial; behavior parity audit pending)
