# History gap audit: ndc / replication / failover / taskdlq / workflowcache (2026-11-06)

Branch `history-gap-ndcrepl`. Method: read Go (non-test) and Rust side by side, function by function.
Status: `identical` = faithful port, `differs` = behavioural gap (fixed in this branch where noted), `missing`.
Idiomatic collapses (env traits instead of function fields, ownership-move instead of deferred releaseFn,
`guarded`/`finish_guarded` instead of recover) are counted as identical.

## ndc/ (all identical)
| Go file | Rust file | Status |
|---|---|---|
| activity_replicator.go | ndc/activity_replicator.rs | identical |
| branch_manager.go | ndc/branch_manager.rs | identical |
| conflict_resolver.go | ndc/conflict_resolver.rs | identical |
| events_reapplier.go | ndc/events_reapplier.rs | identical |
| existing_workflow_transaction_manager.go | ndc/existing_workflow_transaction_manager.rs | identical |
| new_workflow_transaction_manager.go | ndc/new_workflow_transaction_manager.rs | identical |
| transaction_manager.go | ndc/transaction_manager.rs | identical |
| history_replicator.go | ndc/history_replicator.rs | identical |
| replication_task.go | ndc/replication_task.rs | identical |
| workflow_resetter.go | ndc/workflow_resetter.rs | identical |
Only omissions: debug-level logs; `rebuild` clones version history.

## replication/
| Go file | Rust file | Status |
|---|---|---|
| task_processor.go (all 15 funcs: processorLoop, cleanupReplicationTaskLoop, cleanupAckedReplicationTasks, processResponse, syncShardStatusLoop, handleSyncShardStatus, processSingleTask, processTaskOnce, putReplicationTaskToDLQ, generateDLQRequest, triggerDataInconsistencyScan, isTransientRetryableError, shouldRetryDLQ, updateFailureMetric, taskProcessingStartWait, isShuttingDown) | task_processor.rs | identical |
| dlq_handler.go | dlq_handler.rs | pending audit |
| dynamic_task_batch_sizer.go | dynamic_task_batch_sizer.rs | pending audit |
| metrics_emitter.go | metrics_emitter.rs | pending audit |
| task_ack_manager.go | task_ack_manager.rs | pending audit |
| task_executor.go | task_executor.rs | pending audit |
| task_fetcher.go | task_fetcher.rs | pending audit |
| task_hydrator.go | task_hydrator.rs | pending audit |
| task_reader.go | task_reader.rs | pending audit |
| task_store.go | task_store.rs | pending audit |

## failover/
| Go func | Status |
|---|---|
| coordinator: NewCoordinator, Start, NotifyFailoverMarkers, ReceiveFailoverMarkers, handleFailoverMarkers, cleanupInvalidMarkers, notifyRemoteCoordinator, aggregateNotificationRequests, receive/notify loops | identical |
| coordinator: Stop | differs -> fixed: Rust used `Notify` (notify_waiters + 2x notify_one, only one permit stored) so a busy loop could miss shutdown; now a `CancellationToken` like Go's `close(shutdownChan)` |
| coordinator: GetFailoverInfo | identical except `PendingShards` is an empty Vec instead of nil when all shards complete (type limitation, wire-level empty list vs omitted) |
| marker_notifier.go (NewMarkerNotifier, Start, Stop, notifyPendingFailoverMarker) | identical |

## taskdlq/
| Go func | Status |
|---|---|
| NewProcessor, NewShardMaxReadLevelFn, NewProcessorFromShard, Start, Stop, processLoop, sweepInterval, runSweep, processPendingFailovers, ProcessShard, domainName, ProcessPartition, FailoverPartitions, processAckLevels, processAckLevel, advanceAckLevel, Partition.key | identical |
| host_limiter.go, interface.go, constants.go | pending audit (small) |

## workflowcache/
| Go func | Status |
|---|---|
| New, AllowExternal, AllowInternal, allow, getCacheItem | identical (LRU is a HashMap with TTL-from-creation and oldest-access eviction) |
| emitRateLimitMetrics, logError, workflowIDCountMetric.updatePerDomainMaxWFRequestCount | partial: metrics go through `WorkflowCacheObserver` hooks; the int exponential histogram emit is not forwarded |
