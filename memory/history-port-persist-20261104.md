# History SQL persistence port — 2026-11-04 (continued 2026-11-05)

Branch: `history-port-persist`. Worktree: `/home/auenkr/.t3/worktrees/cadence-rust/t3-cee2bc69`.

## Scope and status

Implementation is now wired into `GoExecutionStore` (`SqlWorkflowStore`) and `GoHistoryV2Store` (`SqlHistoryV2Store`) for MySQL and PostgreSQL via `SqlBackend`. This supersedes the earlier incomplete helper-only checklist. This is an implementation/compilation port, **not service parity acceptance**. No integration or mixed-cluster tests were added/run in this task, as requested. History service lifecycle, deployment wiring and SDK compatibility acceptance remain outside this worker's validation.

Transaction writes use a single connection and commit, shared shard ownership locks, current row locks, execution condition locks, then the same mutation/reset/new ordering. State maps remain separate Go-schema rows; map payloads are Go sqlblobs supplied as keyed data/encoding blobs. Activity heartbeats retain separate SQL columns. Task tables retain the plugin-specific bounds (including the existing PostgreSQL replication unbounded-delete quirk), timer lookahead/JSON pagination, little-endian immediate/DLQ/history-node cursors and gob tree/execution cursors. Cross-cluster task rows preserve target cluster. Visibility/other task insertion stays a Go no-op; CreateHistoryTasks, list-current, exists and workflow-timer selection retain original SQL unimplemented/TODO behavior.

New trait methods expose full scheduled task keys/pages, failover range IDs, DLQ pages and explicit DLQ delete bounds while retaining existing convenience methods. History-v2 preserves negated SQL transaction IDs, duplicate history condition failures, corruption checks, ancestor fork selection and bottom-up batched deletion. Execution serialization uses Go event-store version/default initiated ID and preserves activity retry FailureOptions.

## Validation

- https://github.com/auenkr/cadence-rust/actions/runs/41887514632 — `26c42b7acbb08703d5c59608ad6a5f322eb5bf53`, failed Clippy (`items_after_test_module`), fixed by moving the codec test module.
- https://github.com/auenkr/cadence-rust/actions/runs/41888820179 — `3c81a2386ecf4df44a1fdcf9836842ad8ba32119`, **success**, cadence-persistence clippy then tests; CI formatting artifact applied only to this port's files.
- Final fresh-target run: https://github.com/auenkr/cadence-rust/actions/runs/41911404098, head `374ce4a1707ed336fb993554373cc24f42dd01d4`, conclusion **failure**: Clippy passed; 424 tests passed and one stale zombie collision assertion failed (updated to Go `ConditionFailed`). Final rerun pending.
- Static SQL catalog table-name check against `go/schema/mysql/v8/cadence/schema.sql`: no unknown table names; `git diff --check`: pass.
- Final audit corrections include `signals_requested_sets` cleanup, SQL timeout/throttling categories, nullable heartbeat payload reads, omission of deprecated `LastWriteEventID` from NDC writes, and the matching regression assertion.
- All Cargo commands ran on GitHub Actions; no local Cargo or Go builds/tests. No integration test evidence is claimed.

## Go function to Rust mapping

Each source function in the requested five Go files is listed below. Constructors and shard resolution are represented by required Rust fields and typed constructors; there is one configured backend pool per store.

### `sql_execution_store.go`

| Go function | Rust implementation |
|---|---|
| `NewSQLExecutionStore` | `SqlWorkflowStore::new` / explicit required `i32` shard fields / `lock_shard` inside `SqlBackend::transaction` |
| `resolveShardID` | `SqlWorkflowStore::new` / explicit required `i32` shard fields / `lock_shard` inside `SqlBackend::transaction` |
| `effectiveShardID` | `SqlWorkflowStore::new` / explicit required `i32` shard fields / `lock_shard` inside `SqlBackend::transaction` |
| `txExecuteShardLocked` | `SqlWorkflowStore::new` / explicit required `i32` shard fields / `lock_shard` inside `SqlBackend::transaction` |
| `CreateWorkflowExecution` | `SqlWorkflowStore::create` + `validate_create_transition` |
| `createWorkflowExecutionTx` | `SqlWorkflowStore::create` + `validate_create_transition` |
| `getExecutions` | `SqlWorkflowStore::get` / trait `get_workflow_execution`; maps first, execution/checksum last, discard checksum on range change |
| `GetWorkflowExecution` | `SqlWorkflowStore::get` / trait `get_workflow_execution`; maps first, execution/checksum last, discard checksum on range change |
| `UpdateWorkflowExecution` | `SqlWorkflowStore::update` / `validate_update` (all three modes) |
| `updateWorkflowExecutionTx` | `SqlWorkflowStore::update` / `validate_update` (all three modes) |
| `ConflictResolveWorkflowExecution` | `SqlWorkflowStore::conflict_resolve` / `validate_conflict` (both modes, optional current/new writes) |
| `conflictResolveWorkflowExecutionTx` | `SqlWorkflowStore::conflict_resolve` / `validate_conflict` (both modes, optional current/new writes) |
| `DeleteWorkflowExecution` | corresponding `GoExecutionStore` methods in `sql_workflow_store.rs` |
| `DeleteCurrentWorkflowExecution` | corresponding `GoExecutionStore` methods in `sql_workflow_store.rs` |
| `GetCurrentExecution` | corresponding `GoExecutionStore` methods in `sql_workflow_store.rs` |
| `ListCurrentExecutions` | corresponding trait methods retain Go SQL `Not yet implemented` error |
| `IsWorkflowExecutionExists` | corresponding trait methods retain Go SQL `Not yet implemented` error |
| `ListConcreteExecutions` | trait `list_concrete_executions`, `decode_execution_info`, `history_tree_token::{encode_execution,decode_execution}` |
| `getReadLevels` | `SqlHistoryTasks::get_dlq_page` / `get_dlq`, trait `get_replication_dlq_page`; retain max(read+batch,max), little-endian cursor |
| `GetReplicationTasksFromDLQ` | `SqlHistoryTasks::get_dlq_page` / `get_dlq`, trait `get_replication_dlq_page`; retain max(read+batch,max), little-endian cursor |
| `GetReplicationDLQSize` | `SqlHistoryTasks::{dlq_size,delete_dlq,range_delete_dlq,put_dlq}`; trait full-range `range_delete_replication_dlq_page` |
| `DeleteReplicationTaskFromDLQ` | `SqlHistoryTasks::{dlq_size,delete_dlq,range_delete_dlq,put_dlq}`; trait full-range `range_delete_replication_dlq_page` |
| `RangeDeleteReplicationTaskFromDLQ` | `SqlHistoryTasks::{dlq_size,delete_dlq,range_delete_dlq,put_dlq}`; trait full-range `range_delete_replication_dlq_page` |
| `CreateFailoverMarkerTasks` | `SqlWorkflowStore::create_failover_markers`, trait `create_failover_markers_with_range`; shard lock and sentinel metadata |
| `CreateHistoryTasks` | `SqlHistoryTasks::create_history_tasks`: retain Go SQL unsupported error |
| `serialize` | `TimerCursor` serde JSON (`TaskID`, RFC3339 `Timestamp`) |
| `deserialize` | `TimerCursor` serde JSON (`TaskID`, RFC3339 `Timestamp`) |
| `PutReplicationTaskToDLQ` | `SqlHistoryTasks::{dlq_size,delete_dlq,range_delete_dlq,put_dlq}`; trait full-range `range_delete_replication_dlq_page` |
| `populateWorkflowMutableState` | `SqlWorkflowStore::get` / trait `get_workflow_execution`; maps first, execution/checksum last, discard checksum on range change |
| `populateInternalListConcreteExecutions` | trait `list_concrete_executions`, `decode_execution_info`, `history_tree_token::{encode_execution,decode_execution}` |
| `GetHistoryTasks` | `SqlHistoryTasks::get_page`, trait `get_history_task_page`; immediate cursor and timer lookahead |
| `getImmediateHistoryTasks` | `SqlHistoryTasks::get_page`, trait `get_history_task_page`; immediate cursor and timer lookahead |
| `getScheduledHistoryTasks` | `SqlHistoryTasks::get_page`, trait `get_history_task_page`; immediate cursor and timer lookahead |
| `CompleteHistoryTask` | `SqlWorkflowStore::complete_history_task_keys`, trait `complete_history_task_batch`, `SqlHistoryTasks::complete` |
| `completeScheduledHistoryTask` | `SqlWorkflowStore::complete_history_task_keys`, trait `complete_history_task_batch`, `SqlHistoryTasks::complete` |
| `completeImmediateHistoryTask` | `SqlWorkflowStore::complete_history_task_keys`, trait `complete_history_task_batch`, `SqlHistoryTasks::complete` |
| `RangeCompleteHistoryTask` | `SqlHistoryTasks::range_complete`, trait `range_complete_history_task_page`; bounded and unbounded backend-specific deletes |
| `rangeCompleteScheduledHistoryTask` | `SqlHistoryTasks::range_complete`, trait `range_complete_history_task_page`; bounded and unbounded backend-specific deletes |
| `rangeCompleteImmediateHistoryTask` | `SqlHistoryTasks::range_complete`, trait `range_complete_history_task_page`; bounded and unbounded backend-specific deletes |
| `GetActiveClusterSelectionPolicy` | corresponding trait methods in `sql_workflow_store.rs` |
| `DeleteActiveClusterSelectionPolicy` | corresponding trait methods in `sql_workflow_store.rs` |
| `SelectWorkflowTimerTasks` | trait `select_workflow_timer_tasks`: empty result, matching Go SQL TODO |

### `sql_execution_store_util.go`

| Go function | Rust implementation |
|---|---|
| `applyWorkflowMutationTx` | `SqlWorkflowStore::apply_mutation_tx` |
| `applyWorkflowSnapshotTxAsReset` | `SqlWorkflowStore::apply_snapshot_tx(reset=true/false)` |
| `applyWorkflowSnapshotTxAsNew` | `SqlWorkflowStore::apply_snapshot_tx(reset=true/false)` |
| `applyTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `lockCurrentExecutionIfExists` | `SqlWorkflowStore::{current_tx,current_write,create}` |
| `createOrUpdateCurrentExecution` | `SqlWorkflowStore::{current_tx,current_write,create}` |
| `lockAndCheckNextEventID` | `SqlWorkflowStore::lock_condition` |
| `lockNextEventID` | `SqlWorkflowStore::lock_condition` |
| `createImmediateTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `createScheduledTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `createTransferTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `createReplicationTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `createTimerTasks` | `SqlWorkflowStore::tasks` / `SqlHistoryTasks::insert_tx`; unsupported category insertion remains Go no-op |
| `assertNotCurrentExecution` | `SqlWorkflowStore::assert_current` and `validate_create_transition` |
| `assertRunIDAndUpdateCurrentExecution` | `SqlWorkflowStore::assert_current` and `validate_create_transition` |
| `assertCurrentExecution` | `SqlWorkflowStore::assert_current` and `validate_create_transition` |
| `assertRunIDMismatch` | `SqlWorkflowStore::assert_current` and `validate_create_transition` |
| `updateCurrentExecution` | `SqlWorkflowStore::{current_tx,current_write,create}` |
| `buildExecutionRow` | `snapshot_to_execution_row`, `SqlWorkflowStore::{execution_write,apply_snapshot_tx}` |
| `createExecution` | `snapshot_to_execution_row`, `SqlWorkflowStore::{execution_write,apply_snapshot_tx}` |
| `updateExecution` | `snapshot_to_execution_row`, `SqlWorkflowStore::{execution_write,apply_snapshot_tx}` |
| `insertActiveClusterSelectionPolicy` | `snapshot_to_execution_row`, `SqlWorkflowStore::{execution_write,apply_snapshot_tx}` |

### `workflow_state_maps.go`

| Go function | Rust implementation |
|---|---|
| `updateActivityInfos` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `getActivityInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `deleteActivityInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `updateTimerInfos` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `getTimerInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `deleteTimerInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `updateChildExecutionInfos` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `getChildExecutionInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `deleteChildExecutionInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `updateRequestCancelInfos` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `getRequestCancelInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `deleteRequestCancelInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `updateSignalInfos` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `getSignalInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |
| `deleteSignalInfoMap` | `SqlWorkflowStore::{maps,get}` / `workflow_state_map_statement`; standalone backend helpers in `history_state_maps_sql.rs`; keyed Go sqlblobs retained |

### `workflow_state_non_maps.go`

| Go function | Rust implementation |
|---|---|
| `updateSignalsRequested` | `SqlWorkflowStore::{signals,get}`, standalone helpers in `history_state_non_maps_sql.rs` |
| `getSignalsRequested` | `SqlWorkflowStore::{signals,get}`, standalone helpers in `history_state_non_maps_sql.rs` |
| `deleteSignalsRequestedSet` | `SqlWorkflowStore::{signals,get}`, standalone helpers in `history_state_non_maps_sql.rs` |
| `updateBufferedEvents` | `SqlWorkflowStore::{apply_mutation_tx,apply_snapshot_tx,get}`, standalone helpers in `history_state_non_maps_sql.rs` |
| `getBufferedEvents` | `SqlWorkflowStore::{apply_mutation_tx,apply_snapshot_tx,get}`, standalone helpers in `history_state_non_maps_sql.rs` |
| `deleteBufferedEvents` | `SqlWorkflowStore::{apply_mutation_tx,apply_snapshot_tx,get}`, standalone helpers in `history_state_non_maps_sql.rs` |

### `sql_history_store.go`

| Go function | Rust implementation |
|---|---|
| `NewHistoryV2Persistence` | `SqlHistoryV2Store::new` |
| `AppendHistoryNodes` | `SqlHistoryV2Store::append_history_nodes` (`GoHistoryV2Store` implementation) |
| `ReadHistoryBranch` | `SqlHistoryV2Store::read_history_branch` (`GoHistoryV2Store` implementation) |
| `ForkHistoryBranch` | `SqlHistoryV2Store::fork_history_branch` (`GoHistoryV2Store` implementation) |
| `DeleteHistoryBranch` | `SqlHistoryV2Store::delete_history_branch` (`GoHistoryV2Store` implementation) |
| `GetAllHistoryTreeBranches` | `SqlHistoryV2Store::get_all_history_tree_branches` (`GoHistoryV2Store` implementation) |
| `GetHistoryTree` | `SqlHistoryV2Store::get_history_tree` (`GoHistoryV2Store` implementation) |
