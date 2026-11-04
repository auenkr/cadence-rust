# History SQL persistence port — 2026-11-04

Branch: `history-port-persist`, based on `origin/master` after fetch. Worktree: `/home/auenkr/.t3/worktrees/cadence-rust/t3-cee2bc69`.

## Current status

**Incomplete.** This is a working progress record, not Slice 2/3/4 completion evidence. No MySQL or PostgreSQL store implementation currently implements `GoExecutionStore` or `GoHistoryV2Store`; the Rust changes so far establish missing contract/query/helper groundwork only. No database integration gate has run.

## Changes committed and pushed

- `883bccce2`: added execution row lock/update SQL and History-v2 node/tree SQL statements to `history_sql_queries.rs` for MySQL and PostgreSQL.
- `2d172bf93`: added conditional execution row update methods. They lock and compare `next_event_id` before updating the row in a transaction. Added transfer, timer, replication and cross-cluster SQL query templates.
- `4ca25f248`: added keyed blobs to workflow state map contracts; changed History-v2 tree results to represent branch metadata; added Go-compatible `HistoryTreeInfo` sqlblobs codec helpers.
- `0d133fba3`: added the Go little-endian int64 History node page token codec and row-order/transaction validation, including duplicate-transaction selection and corruption errors.
- `7ab12e019`, `2a92b9d4d`: added branch fork ancestry, shared-ancestor reference maxima and deletion-range helpers.

## CI

- `rust-ci` run https://github.com/auenkr/cadence-rust/actions/runs/41876876946 was dispatched at the earlier `883bccce2` head; it was superseded before validation and is not evidence.
- Push-triggered run https://github.com/auenkr/cadence-rust/actions/runs/41878446427 targets `7ab12e01913ca2f654eb2cb1026cffeb8c605f04`; it was queued when last checked. Later head `2a92b9d4d` still needs a fresh final run.
- The old run https://github.com/auenkr/cadence-rust/actions/runs/41876865312 remained reported `in_progress` despite all visible steps ending at 17:23 UTC; its clippy and test steps were skipped after cancellation, so it is not validation evidence.
- No successful CI head or run URL yet.

## Function parity checklist

Legend: **partial** = supporting contract/query/helper exists but no equivalent complete persistence method is wired; **missing** = no Rust implementation.

### `sql_execution_store.go`

| Go function(s) | Rust mapping/status |
|---|---|
| `resolveShardID`, `effectiveShardID`, `txExecuteShardLocked` | Missing; shard row CRUD exists in `history_shard_sql.rs`, but no shard-aware execution transaction dispatcher. |
| `CreateWorkflowExecution`, `createWorkflowExecutionTx` | Partial row helper `MySqlExecutionRows::create` / `PostgresExecutionRows::create`; mutable-state maps/tasks/workflow requests and full trait wiring missing. |
| `getExecutions`, `GetWorkflowExecution`, `populateWorkflowMutableState` | Partial row/blob helpers in `sql_execution_store.rs`; mutable map, requested-signal and buffered-event reads missing. |
| `UpdateWorkflowExecution`, `updateWorkflowExecutionTx` | Partial `update_conditioned`; mutation helpers, maps/tasks/current transitions are missing. |
| `ConflictResolveWorkflowExecution`, `conflictResolveWorkflowExecutionTx` | Missing. |
| `DeleteWorkflowExecution`, `DeleteCurrentWorkflowExecution`, `GetCurrentExecution`, `IsWorkflowExecutionExists` | Partial inherent row methods in `MySqlExecutionRows` / `PostgresExecutionRows`; trait wiring absent. |
| `ListCurrentExecutions`, `ListConcreteExecutions`, `populateInternalListConcreteExecutions` | Missing; Go gob pagination compatibility absent. |
| `CreateFailoverMarkerTasks`, `CreateHistoryTasks` | Missing. |
| `GetReplicationTasksFromDLQ`, `GetReplicationDLQSize`, `DeleteReplicationTaskFromDLQ`, `RangeDeleteReplicationTaskFromDLQ`, `PutReplicationTaskToDLQ` | Missing. |
| `GetHistoryTasks`, `getImmediateHistoryTasks`, `getScheduledHistoryTasks`, `CompleteHistoryTask`, `completeScheduledHistoryTask`, `completeImmediateHistoryTask`, `RangeCompleteHistoryTask`, `rangeCompleteScheduledHistoryTask`, `rangeCompleteImmediateHistoryTask` | Missing. |
| `SelectWorkflowTimerTasks` | Missing. |
| `GetActiveClusterSelectionPolicy`, `DeleteActiveClusterSelectionPolicy` | Partial query/catalog groundwork only; no operations. |

### `sql_execution_store_util.go`

| Go function(s) | Rust mapping/status |
|---|---|
| `applyWorkflowMutationTx`, `applyWorkflowSnapshotTxAsReset`, `applyWorkflowSnapshotTxAsNew`, `applyTasks` | Missing. |
| `lockCurrentExecutionIfExists`, `createOrUpdateCurrentExecution`, `lockAndCheckNextEventID`, `lockNextEventID` | Partial `update_conditioned` for next-event lock/check only; current transition helpers missing. |
| `createImmediateTasks`, `createScheduledTasks`, `createTransferTasks`, `createReplicationTasks`, `createTimerTasks` | Missing; query statements only for queue families. |
| `assertNotCurrentExecution`, `assertRunIDAndUpdateCurrentExecution`, `assertCurrentExecution`, `assertRunIDMismatch`, `updateCurrentExecution` | Missing. |
| `buildExecutionRow`, `createExecution`, `updateExecution`, `insertActiveClusterSelectionPolicy` | Partial `snapshot_to_execution_row`, `create`, and `update_conditioned`; active policy and complete execution write helpers missing. |

### `workflow_state_maps.go` / `workflow_state_non_maps.go`

| Go functions | Rust mapping/status |
|---|---|
| `updateActivityInfos`, `getActivityInfoMap`, `deleteActivityInfoMap` | Partial: `KeyedBlob<i64>` contract values; no SQL CRUD/codecs wired. |
| `updateTimerInfos`, `getTimerInfoMap`, `deleteTimerInfoMap` | Partial: `KeyedBlob<String>` contract values; no SQL CRUD/codecs wired. |
| `updateChildExecutionInfos`, `getChildExecutionInfoMap`, `deleteChildExecutionInfoMap` | Partial keyed contract; no SQL CRUD/codecs wired. |
| `updateRequestCancelInfos`, `getRequestCancelInfoMap`, `deleteRequestCancelInfoMap` | Partial keyed contract; no SQL CRUD/codecs wired. |
| `updateSignalInfos`, `getSignalInfoMap`, `deleteSignalInfoMap` | Partial keyed contract; no SQL CRUD/codecs wired. |
| `updateSignalsRequested`, `getSignalsRequested`, `deleteSignalsRequestedSet` | Missing. |
| `updateBufferedEvents`, `getBufferedEvents`, `deleteBufferedEvents` | Missing. |

### `sql_history_store.go`

| Go function(s) | Rust mapping/status |
|---|---|
| `AppendHistoryNodes` | Partial: SQL statements and tree metadata codec; no transactional backend operation or duplicate-error conversion. |
| `ReadHistoryBranch` | Partial: page token and ordered row validation helper; no SQL backend operation. |
| `ForkHistoryBranch` | Partial: fork ancestor helper and insert query; no backend operation/tree write. |
| `DeleteHistoryBranch` | Partial: shared-reference and bottom-up deletion range helpers plus batch SQL statements; no backend operation. |
| `GetHistoryTree` | Partial: query and branch-result contract/codec; no backend operation. |
| `GetAllHistoryTreeBranches` and Go gob token encode/decode | Missing. |

## Next work

1. Implement all workflow-state map and non-map SQL CRUD for both backends, preserving transaction semantics and serializer payloads.
2. Implement mutation/reset/conflict-resolution transaction paths, then task tables/DLQ and History-v2 backend methods; implement Go gob page-token compatibility.
3. Implement and wire every `GoExecutionStore` and `GoHistoryV2Store` method.
4. Dispatch one final `rust-ci.yml` run on the final pushed head, fix failures using CI logs, and update this file with the successful run URL, exact head SHA and conclusion.
5. Only after the SQL store parity is complete, add/reuse the requested mixed Go/Rust database and SDK compatibility validation evidence.
