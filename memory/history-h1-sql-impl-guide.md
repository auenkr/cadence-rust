# History H1 SQL execution-store implementation guide

This guide is the implementation handoff after `history_contract.rs`. The contract takes typed snapshot/mutation inputs, including mode, prior run/version, condition, tasks by category, timestamps, and complete list pagination. Implementations must keep each operation's SQL writes in the same transaction as the shard range lock and current-execution transition. Do not serialize the workflow state into one Rust-only execution blob: the Go SQL schema stores the execution row and mutable-state maps separately.

## Contracts and exact Go behavior

Reference `go/common/persistence/data_store_interfaces.go` (`ExecutionStore`, request structs, `InternalWorkflowSnapshot`, `InternalWorkflowMutation`, mutable-state info records, HistoryStore) and `data_manager_interfaces.go` (mode values, task key/data types and manager requests). Rust target: `rust/crates/cadence-persistence/src/history_contract.rs`. It intentionally keeps variable typed Go info payloads as `DataBlob` at the contract boundary; the SQL worker must add typed serializers/parsers before CRUD and must not invent a new wire encoding. Go's `InternalWorkflowExecutionInfo` maps the row columns `next_event_id`, `last_write_version`, `data`, `data_encoding`; activity/timer/child/cancel/signal/requested-signal/buffered-event state maps to their respective tables.

Use SQL plugin implementations in `go/common/persistence/sql/sqlplugin/{mysql,postgres}` as the exact statement source. Both plugins implement identical logical predicates/order/ranges; MySQL uses `?` in positional queries and `:named_column` for named inserts/updates; PostgreSQL uses `$1...` positional queries and `:named_column` named inserts/updates. Preserve plugin-specific lock syntax: MySQL shared read lock `LOCK IN SHARE MODE`, PostgreSQL `FOR SHARE`; both use `FOR UPDATE`. The execution row statement templates are `execution.go` in each plugin (same table/columns): INSERT `executions(shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding)`; UPDATE the latter four values by four-part primary key; SELECT/DELETE by `(shard_id,domain_id,workflow_id,run_id)`. Current rows use `current_executions` columns `(shard_id,domain_id,workflow_id,run_id,create_request_id,state,close_status,start_version,last_write_version)`.

## Slice 1 — execution create/read/current/list/delete

Map `sql_execution_store.go`: `resolveShardID`, `effectiveShardID`, `txExecuteShardLocked`, `CreateWorkflowExecution`, `createWorkflowExecutionTx`, `getExecutions`, `GetWorkflowExecution`, `DeleteWorkflowExecution`, `DeleteCurrentWorkflowExecution`, `GetCurrentExecution`, `ListCurrentExecutions`, `IsWorkflowExecutionExists`, `ListConcreteExecutions`, `populateWorkflowMutableState`, `populateInternalListConcreteExecutions`.

Required SQL/plugin calls:

| Operation | Go SQL plugin calls / exact predicates | Rust target |
|---|---|---|
| shard ownership | `SelectFromShards` / lock shard row by shard id; compare `range_id` in transaction | `ShardStore` transaction helper, then `GoExecutionStore` |
| current read/lock | `SelectFromCurrentExecutions` by shard/domain/workflow; `LockCurrentExecutions` same key `FOR UPDATE`; join variant selects `current_executions ce` + `executions e` and locks | `get_current_execution`, create transition helper |
| create row | `InsertIntoExecutions` with row columns listed above | snapshot serializer + insert |
| execution read | `SelectFromExecutions` by shard/domain/workflow/run | reconstruct `WorkflowSnapshot`; do concurrent map reads first, read execution/checksum last |
| current transition | `InsertIntoCurrentExecutions` for absent current; `UpdateCurrentExecutions` by shard/domain/workflow; conditional delete by four key columns | `create_or_update_current_execution` enforcing BrandNew, WorkflowIdReuse, ContinueAsNew, Zombie semantics |
| delete concrete | transaction deletes `executions`, `activity_info_maps`, `timer_info_maps`, `child_execution_info_maps`, `request_cancel_info_maps`, `signal_info_maps`, `signals_requested`, `buffered_events` for shard/domain/workflow/run | `delete_workflow_execution` |
| exists/list | `SelectFromExecutions` exact key for exists; `SelectFromExecutions` list by shard, workflow cursor, ordered `workflow_id`, page limit; current scan uses current rows and joined execution state where required | preserve continuation-token encoding and ordering |

Core templates are defined in both `sqlplugin/mysql/execution.go` and `sqlplugin/postgres/execution.go`:

```sql
INSERT INTO executions(shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding)
VALUES(:shard_id,:domain_id,:workflow_id,:run_id,:next_event_id,:last_write_version,:data,:data_encoding);
UPDATE executions SET next_event_id=:next_event_id,last_write_version=:last_write_version,data=:data,data_encoding=:data_encoding
 WHERE shard_id=:shard_id AND domain_id=:domain_id AND workflow_id=:workflow_id AND run_id=:run_id;
SELECT shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding FROM executions
 WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?;
DELETE FROM executions WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?;
SELECT next_event_id FROM executions WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=? FOR UPDATE;
UPDATE current_executions SET run_id=:run_id,create_request_id=:create_request_id,state=:state,close_status=:close_status,
 start_version=:start_version,last_write_version=:last_write_version
 WHERE shard_id=:shard_id AND domain_id=:domain_id AND workflow_id=:workflow_id;
```

Use `?` positional placeholders for MySQL and `$1...` for PostgreSQL. Both use named binds for the shown inserts/updates. Shared read locking is `LOCK IN SHARE MODE` on MySQL and `FOR SHARE` on PostgreSQL; both use `FOR UPDATE`. Plugin files carry the exact bound query arguments; do not mechanically translate batch-delete syntax.

Mode rules are in `createWorkflowExecutionTx` and `createOrUpdateCurrentExecution`: BrandNew fails if current exists; WorkflowIdReuse checks previous last-write version, completed state and previous run; Zombie requires different current run and does not replace current; ContinueAsNew checks previous run. `WorkflowExecutionAlreadyStartedError` must retain start request, current run/state/close status/version. Missing execution -> EntityNotExists; current predicate mismatch -> CurrentWorkflowConditionFailed.

## Slice 2 — mutation, reset/conflict resolution and workflow-state maps

Map `sql_execution_store.go`: `UpdateWorkflowExecution`, `updateWorkflowExecutionTx`, `ConflictResolveWorkflowExecution`, `conflictResolveWorkflowExecutionTx`; and `sql_execution_store_util.go`: `applyWorkflowMutationTx`, `applyWorkflowSnapshotTxAsReset`, `applyWorkflowSnapshotTxAsNew`, `applyTasks`, `lockAndCheckNextEventID`, `lockNextEventID`, `assertNotCurrentExecution`, `assertRunIDAndUpdateCurrentExecution`, `assertCurrentExecution`, `assertRunIDMismatch`, `updateCurrentExecution`, `buildExecutionRow`, `createExecution`, `updateExecution`, `insertActiveClusterSelectionPolicy`.

Mutation transaction order: lock/check `executions.next_event_id == mutation.condition`; update execution blob/version/checksum; insert categorized tasks; upsert/delete each info map; update signal requested ids; clear buffered events when requested, then insert new buffered-event batches. Snapshot-as-new inserts execution then all info maps, buffered events/signals, tasks, workflow requests and optional active-cluster policy. Snapshot-as-reset updates execution and replaces all state maps/tasks according to Go reset helper. Keep mode semantics: UpdateCurrent performs conditional current-run transition (including new run snapshot for continue-as-new); BypassCurrent asserts current is not this run; IgnoreCurrent skips current row. Conflict resolve similarly handles reset snapshot, optional current mutation and optional new snapshot atomically. Never weaken request `condition` or shard range checks.

Map `workflow_state_maps.go` helpers one-to-one: `updateActivityInfos/getActivityInfoMap/deleteActivityInfoMap` -> `activity_info_maps`; `updateTimerInfos/getTimerInfoMap/deleteTimerInfoMap` -> `timer_info_maps`; child execution helpers -> `child_execution_info_maps`; request cancel -> `request_cancel_info_maps`; signal -> `signal_info_maps`. Each uses insert/upsert, select by workflow key, delete by workflow key plus map key. `workflow_state_non_maps.go`: requested signal set uses `signals_requested` insert/delete/select; buffered events use `buffered_events` append/select/delete. Preserve blob `data` and `data_encoding`, keyed map identity (activity schedule id, timer id, child initiated id, cancel initiated id, signal initiated id), sentinel behavior, and SQL uniqueness conflict conversions.

## Slice 3 — transfer, timer, replication, cross-cluster tasks and DLQ

Map `sql_execution_store.go`: `CreateFailoverMarkerTasks`, `GetReplicationTasksFromDLQ`, `GetReplicationDLQSize`, `DeleteReplicationTaskFromDLQ`, `RangeDeleteReplicationTaskFromDLQ`, `PutReplicationTaskToDLQ`, `CreateHistoryTasks`, `GetHistoryTasks`, `getImmediateHistoryTasks`, `getScheduledHistoryTasks`, `CompleteHistoryTask`, `completeScheduledHistoryTask`, `completeImmediateHistoryTask`, `RangeCompleteHistoryTask`, `rangeCompleteScheduledHistoryTask`, `rangeCompleteImmediateHistoryTask`, `SelectWorkflowTimerTasks`. Map snapshot/mutation task insertion helpers `createImmediateTasks`, `createScheduledTasks`, `createTransferTasks`, `createReplicationTasks`, `createTimerTasks` in `sql_execution_store_util.go`. Task templates are in both plugins' `execution.go` and have category-specific ranges:

| Table | MySQL filter and ordering | PostgreSQL filter and ordering |
|---|---|---|
| transfer | `shard_id=? AND task_id>=? AND task_id<? ORDER BY shard_id,task_id LIMIT ?` | `shard_id=$1 AND task_id >= $2 AND task_id < $3 ORDER BY shard_id,task_id LIMIT $4` |
| cross-cluster | `target_cluster=? AND shard_id=? AND task_id>? AND task_id<=? ORDER BY task_id LIMIT ?` | same predicates with `$1..$5` |
| timer | `shard_id=? AND ((visibility_timestamp>=? AND task_id>=?) OR visibility_timestamp>?) AND visibility_timestamp<? ORDER BY visibility_timestamp,task_id LIMIT ?` | `shard_id=$1 AND (visibility_timestamp,task_id)>=($2,$3) AND visibility_timestamp<$4 ORDER BY visibility_timestamp,task_id LIMIT $5` |

Insert templates: transfer `INSERT INTO transfer_tasks(shard_id,task_id,data,data_encoding) VALUES(:shard_id,:task_id,:data,:data_encoding)`; cross-cluster `INSERT INTO cross_cluster_tasks(target_cluster,shard_id,task_id,data,data_encoding) VALUES(:target_cluster,:shard_id,:task_id,:data,:data_encoding)`; timer `INSERT INTO timer_tasks(shard_id,visibility_timestamp,task_id,data,data_encoding) VALUES(:shard_id,:visibility_timestamp,:task_id,:data,:data_encoding)`; replication `INSERT INTO replication_tasks(shard_id,task_id,data,data_encoding) VALUES(:shard_id,:task_id,:data,:data_encoding)`. Exact deletes use transfer `(shard_id,task_id)`, timer `(shard_id,visibility_timestamp,task_id)`, cross-cluster `(target_cluster,shard_id,task_id)`. MySQL range delete uses `DELETE ... WHERE ... ORDER BY ... LIMIT ?`; PostgreSQL uses a key `IN (SELECT key FROM same_table WHERE ... ORDER BY ... LIMIT $n)`. Replication/DLQ keys and predicates must follow the plugin filters, not an assumed negative-task encoding. `sql_task_store.go` is Matching's task-list persistence and is not a History execution task store.

History task insertion is explicitly unsupported by current SQL Go implementation (`errHistoryTasksNotImplemented`) where invoked; retain that behavior instead of silently dropping tasks. For replication DLQ retain source-cluster mapping, ack/read levels, size, and range-delete semantics; return Go-compatible `RangeDelete...Response` count and error classes. Store task serializer outputs unchanged as `data`/`data_encoding`; deserialize based on task category when reading. Include cross-cluster task CRUD from plugin methods/schema even though Go `ExecutionStore` interface exposes those through related task APIs and manager requests.

## Slice 4 — History-v2 store and parity verification

Map `sql_history_store.go`: `AppendHistoryNodes`, `ReadHistoryBranch`, `ForkHistoryBranch`, `DeleteHistoryBranch`, `GetHistoryTree`, `GetAllHistoryTreeBranches`, page-token encode/decode and cleanup batching helpers. Locate exact plugin statements with `rg 'HistoryNode|HistoryTree' go/common/persistence/sql/sqlplugin/{mysql,postgres}` (these methods may be colocated with execution/task table methods, not a standalone `history.go`). `history_node` key is `(shard_id,tree_id,branch_id,node_id,txn_id)`; append writes node data+encoding and on new branch writes `history_tree` metadata in the same transaction. Read filters by tree+branch, node lower/upper bounds, ordered node id and transaction id with page limit; continuation token advances min node id. Fork reads/updates tree metadata ancestors and branch information. Delete branch removes node rows in bounded batches and then its tree metadata row. Get tree reads one tree/branch metadata record; get-all uses stable shard/tree/branch pagination and serialized page token. Keep Go duplicate append as ConditionFailed, invalid ancestor append as InvalidPersistenceRequest, and corrupted node/transaction ordering as InternalDataInconsistency.

Rust reuse: `history_branch.rs` for branch ancestry/token representation, `history_reader.rs` for read request/page reconstruction, and `history_sql.rs` / `history_shard_sql.rs` for SQL transaction interfaces. `GoHistoryV2Store` in `history_contract.rs` is the typed method contract; refine request variants where Go's operation-specific fields differ before implementation.

## Hand-off completion checks

Each slice should be an independent PR/commit: (1) execution row/current transitions and CRUD, (2) full mutable-state maps/mutations/conflict reset, (3) task queues and DLQ, (4) History-v2. For each slice add MySQL and PostgreSQL integration tests against Go schema, then dispatch `rust-ci.yml` for `cadence-persistence` (fresh CI target, clippy before tests); record run URL, tested head, conclusion, and database fixtures here. Before declaring H1 complete test the Rust History service with Go Frontend/Matching and without Go History, including SDK workflow start/task progression, continue-as-new, signals, timers, replication, restart/recovery, ownership loss and database restart. This guide itself is not evidence of SQL backend completion.
