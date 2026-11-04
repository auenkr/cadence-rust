//! SQL statement catalog for the H1 History shard and execution stores.
//!
//! These statements mirror the existing Go SQL plugins. Keeping placeholder
//! styles explicit lets backend implementations share the same transaction
//! semantics without interpolating workflow identifiers into SQL.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    MySql,
    Postgres,
}

impl Dialect {
    pub const fn statement(self, statement: Statement) -> &'static str {
        match (self, statement) {
            (Self::MySql, Statement::CreateShard) => "INSERT INTO shards(shard_id, range_id, data, data_encoding) VALUES(?, ?, ?, ?)",
            (Self::Postgres, Statement::CreateShard) => "INSERT INTO shards(shard_id, range_id, data, data_encoding) VALUES($1, $2, $3, $4)",
            (Self::MySql, Statement::GetShard) => "SELECT shard_id, range_id, data, data_encoding FROM shards WHERE shard_id = ?",
            (Self::Postgres, Statement::GetShard) => "SELECT shard_id, range_id, data, data_encoding FROM shards WHERE shard_id = $1",
            (Self::MySql, Statement::LockShard) => "SELECT range_id FROM shards WHERE shard_id = ? FOR UPDATE",
            (Self::Postgres, Statement::LockShard) => "SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE",
            (Self::MySql, Statement::UpdateShard) => "UPDATE shards SET range_id = ?, data = ?, data_encoding = ? WHERE shard_id = ?",
            (Self::Postgres, Statement::UpdateShard) => "UPDATE shards SET range_id = $1, data = $2, data_encoding = $3 WHERE shard_id = $4",
            (Self::MySql, Statement::GetExecution) => "SELECT shard_id, domain_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding FROM executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?",
            (Self::Postgres, Statement::GetExecution) => "SELECT shard_id, domain_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding FROM executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3 AND run_id = $4",
            (Self::MySql, Statement::LockExecution) => "SELECT next_event_id FROM executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ? FOR UPDATE",
            (Self::Postgres, Statement::LockExecution) => "SELECT next_event_id FROM executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3 AND run_id = $4 FOR UPDATE",
            (Self::MySql, Statement::UpdateExecution) => "UPDATE executions SET next_event_id = ?, last_write_version = ?, data = ?, data_encoding = ? WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?",
            (Self::Postgres, Statement::UpdateExecution) => "UPDATE executions SET next_event_id = $1, last_write_version = $2, data = $3, data_encoding = $4 WHERE shard_id = $5 AND domain_id = $6 AND workflow_id = $7 AND run_id = $8",
            (Self::MySql, Statement::DeleteExecution) => "DELETE FROM executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?",
            (Self::Postgres, Statement::DeleteExecution) => "DELETE FROM executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3 AND run_id = $4",
            (Self::MySql, Statement::GetCurrentExecution) => "SELECT run_id, create_request_id, state, close_status, start_version, last_write_version FROM current_executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ?",
            (Self::Postgres, Statement::GetCurrentExecution) => "SELECT run_id, create_request_id, state, close_status, start_version, last_write_version FROM current_executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3",
            (Self::MySql, Statement::DeleteCurrentExecution) => "DELETE FROM current_executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?",
            (Self::Postgres, Statement::DeleteCurrentExecution) => "DELETE FROM current_executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3 AND run_id = $4",
            (Self::MySql, Statement::InsertExecution) => "INSERT INTO executions(shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding) VALUES(?,?,?,?,?,?,?,?)",
            (Self::Postgres, Statement::InsertExecution) => "INSERT INTO executions(shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
            (Self::MySql, Statement::InsertCurrentExecution) => "INSERT INTO current_executions(shard_id,domain_id,workflow_id,run_id,create_request_id,state,close_status,start_version,last_write_version) VALUES(?,?,?,?,?,?,?,?,?)",
            (Self::Postgres, Statement::InsertCurrentExecution) => "INSERT INTO current_executions(shard_id,domain_id,workflow_id,run_id,create_request_id,state,close_status,start_version,last_write_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            (Self::MySql, Statement::LockCurrentExecution) => "SELECT shard_id,domain_id,workflow_id,run_id,create_request_id,state,close_status,start_version,last_write_version FROM current_executions WHERE shard_id=? AND domain_id=? AND workflow_id=? FOR UPDATE",
            (Self::Postgres, Statement::LockCurrentExecution) => "SELECT shard_id,domain_id,workflow_id,run_id,create_request_id,state,close_status,start_version,last_write_version FROM current_executions WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 FOR UPDATE",
            (Self::MySql, Statement::UpdateCurrentExecution) => "UPDATE current_executions SET run_id=?,create_request_id=?,state=?,close_status=?,start_version=?,last_write_version=? WHERE shard_id=? AND domain_id=? AND workflow_id=?",
            (Self::Postgres, Statement::UpdateCurrentExecution) => "UPDATE current_executions SET run_id=$1,create_request_id=$2,state=$3,close_status=$4,start_version=$5,last_write_version=$6 WHERE shard_id=$7 AND domain_id=$8 AND workflow_id=$9",
            (Self::MySql, Statement::DeleteActivityInfoMaps) => "DELETE FROM activity_info_maps WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteActivityInfoMaps) => "DELETE FROM activity_info_maps WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteTimerInfoMaps) => "DELETE FROM timer_info_maps WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteTimerInfoMaps) => "DELETE FROM timer_info_maps WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteChildExecutionInfoMaps) => "DELETE FROM child_execution_info_maps WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteChildExecutionInfoMaps) => "DELETE FROM child_execution_info_maps WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteRequestCancelInfoMaps) => "DELETE FROM request_cancel_info_maps WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteRequestCancelInfoMaps) => "DELETE FROM request_cancel_info_maps WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteSignalInfoMaps) => "DELETE FROM signal_info_maps WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteSignalInfoMaps) => "DELETE FROM signal_info_maps WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteSignalsRequested) => "DELETE FROM signals_requested WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteSignalsRequested) => "DELETE FROM signals_requested WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::DeleteBufferedEvents) => "DELETE FROM buffered_events WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
            (Self::Postgres, Statement::DeleteBufferedEvents) => "DELETE FROM buffered_events WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
            (Self::MySql, Statement::InsertHistoryNode) => "INSERT INTO history_node(shard_id,tree_id,branch_id,node_id,txn_id,data,data_encoding) VALUES(?,?,?,?,?,?,?)",
            (Self::Postgres, Statement::InsertHistoryNode) => "INSERT INTO history_node(shard_id,tree_id,branch_id,node_id,txn_id,data,data_encoding) VALUES($1,$2,$3,$4,$5,$6,$7)",
            (Self::MySql, Statement::ReadHistoryNodes) => "SELECT node_id,txn_id,data,data_encoding FROM history_node WHERE shard_id=? AND tree_id=? AND branch_id=? AND node_id>=? AND node_id<? ORDER BY shard_id,tree_id,branch_id,node_id,txn_id LIMIT ?",
            (Self::Postgres, Statement::ReadHistoryNodes) => "SELECT node_id,txn_id,data,data_encoding FROM history_node WHERE shard_id=$1 AND tree_id=$2 AND branch_id=$3 AND node_id >= $4 AND node_id < $5 ORDER BY shard_id,tree_id,branch_id,node_id,txn_id LIMIT $6",
            (Self::MySql, Statement::DeleteHistoryNodes) => "DELETE FROM history_node WHERE shard_id=? AND tree_id=? AND branch_id=? AND node_id>=?",
            (Self::Postgres, Statement::DeleteHistoryNodes) => "DELETE FROM history_node WHERE shard_id=$1 AND tree_id=$2 AND branch_id=$3 AND node_id >= $4",
            (Self::MySql, Statement::RangeDeleteHistoryNodes) => "DELETE FROM history_node WHERE shard_id=? AND tree_id=? AND branch_id=? AND node_id>=? ORDER BY shard_id,tree_id,branch_id,node_id,txn_id LIMIT ?",
            (Self::Postgres, Statement::RangeDeleteHistoryNodes) => "DELETE FROM history_node WHERE shard_id=$1 AND tree_id=$2 AND branch_id=$3 AND (node_id,txn_id) IN (SELECT node_id,txn_id FROM history_node WHERE shard_id=$1 AND tree_id=$2 AND branch_id=$3 AND node_id >= $4 LIMIT $5)",
            (Self::MySql, Statement::InsertHistoryTree) => "INSERT INTO history_tree(shard_id,tree_id,branch_id,data,data_encoding) VALUES(?,?,?,?,?)",
            (Self::Postgres, Statement::InsertHistoryTree) => "INSERT INTO history_tree(shard_id,tree_id,branch_id,data,data_encoding) VALUES($1,$2,$3,$4,$5)",
            (Self::MySql, Statement::GetHistoryTree) => "SELECT branch_id,data,data_encoding FROM history_tree WHERE shard_id=? AND tree_id=?",
            (Self::Postgres, Statement::GetHistoryTree) => "SELECT branch_id,data,data_encoding FROM history_tree WHERE shard_id=$1 AND tree_id=$2",
            (Self::MySql, Statement::GetAllHistoryTreeBranches) => "SELECT shard_id,tree_id,branch_id,data,data_encoding FROM history_tree WHERE (shard_id=? AND tree_id=? AND branch_id>?) OR (shard_id=? AND tree_id>?) OR (shard_id>?) ORDER BY shard_id,tree_id,branch_id LIMIT ?",
            (Self::Postgres, Statement::GetAllHistoryTreeBranches) => "SELECT shard_id,tree_id,branch_id,data,data_encoding FROM history_tree WHERE (shard_id=$1 AND tree_id=$2 AND branch_id>$3) OR (shard_id=$4 AND tree_id>$5) OR (shard_id>$6) ORDER BY shard_id,tree_id,branch_id LIMIT $7",
            (Self::MySql, Statement::DeleteHistoryTree) => "DELETE FROM history_tree WHERE shard_id=? AND tree_id=? AND branch_id=?",
            (Self::Postgres, Statement::DeleteHistoryTree) => "DELETE FROM history_tree WHERE shard_id=$1 AND tree_id=$2 AND branch_id=$3",
            (Self::MySql, Statement::InsertTransferTask) => "INSERT INTO transfer_tasks(shard_id,task_id,data,data_encoding) VALUES(?,?,?,?)",
            (Self::Postgres, Statement::InsertTransferTask) => "INSERT INTO transfer_tasks(shard_id,task_id,data,data_encoding) VALUES($1,$2,$3,$4)",
            (Self::MySql, Statement::SelectTransferTasks) => "SELECT task_id,data,data_encoding FROM transfer_tasks WHERE shard_id=? AND task_id>=? AND task_id<? ORDER BY shard_id,task_id LIMIT ?",
            (Self::Postgres, Statement::SelectTransferTasks) => "SELECT task_id,data,data_encoding FROM transfer_tasks WHERE shard_id=$1 AND task_id >= $2 AND task_id < $3 ORDER BY shard_id,task_id LIMIT $4",
            (Self::MySql, Statement::DeleteTransferTask) => "DELETE FROM transfer_tasks WHERE shard_id=? AND task_id=?",
            (Self::Postgres, Statement::DeleteTransferTask) => "DELETE FROM transfer_tasks WHERE shard_id=$1 AND task_id=$2",
            (Self::MySql, Statement::InsertReplicationTask) => "INSERT INTO replication_tasks(shard_id,task_id,data,data_encoding) VALUES(?,?,?,?)",
            (Self::Postgres, Statement::InsertReplicationTask) => "INSERT INTO replication_tasks(shard_id,task_id,data,data_encoding) VALUES($1,$2,$3,$4)",
            (Self::MySql, Statement::SelectReplicationTasks) => "SELECT task_id,data,data_encoding FROM replication_tasks WHERE shard_id=? AND task_id>=? AND task_id<? ORDER BY task_id LIMIT ?",
            (Self::Postgres, Statement::SelectReplicationTasks) => "SELECT task_id,data,data_encoding FROM replication_tasks WHERE shard_id=$1 AND task_id >= $2 AND task_id < $3 ORDER BY task_id LIMIT $4",
            (Self::MySql, Statement::DeleteReplicationTask) => "DELETE FROM replication_tasks WHERE shard_id=? AND task_id=?",
            (Self::Postgres, Statement::DeleteReplicationTask) => "DELETE FROM replication_tasks WHERE shard_id=$1 AND task_id=$2",
            (Self::MySql, Statement::InsertCrossClusterTask) => "INSERT INTO cross_cluster_tasks(target_cluster,shard_id,task_id,data,data_encoding) VALUES(?,?,?,?,?)",
            (Self::Postgres, Statement::InsertCrossClusterTask) => "INSERT INTO cross_cluster_tasks(target_cluster,shard_id,task_id,data,data_encoding) VALUES($1,$2,$3,$4,$5)",
            (Self::MySql, Statement::SelectCrossClusterTasks) => "SELECT task_id,data,data_encoding FROM cross_cluster_tasks WHERE target_cluster=? AND shard_id=? AND task_id>? AND task_id<=? ORDER BY task_id LIMIT ?",
            (Self::Postgres, Statement::SelectCrossClusterTasks) => "SELECT task_id,data,data_encoding FROM cross_cluster_tasks WHERE target_cluster=$1 AND shard_id=$2 AND task_id > $3 AND task_id <= $4 ORDER BY task_id LIMIT $5",
            (Self::MySql, Statement::DeleteCrossClusterTask) => "DELETE FROM cross_cluster_tasks WHERE target_cluster=? AND shard_id=? AND task_id=?",
            (Self::Postgres, Statement::DeleteCrossClusterTask) => "DELETE FROM cross_cluster_tasks WHERE target_cluster=$1 AND shard_id=$2 AND task_id=$3",
            (Self::MySql, Statement::InsertTimerTask) => "INSERT INTO timer_tasks(shard_id,visibility_timestamp,task_id,data,data_encoding) VALUES(?,?,?,?,?)",
            (Self::Postgres, Statement::InsertTimerTask) => "INSERT INTO timer_tasks(shard_id,visibility_timestamp,task_id,data,data_encoding) VALUES($1,$2,$3,$4,$5)",
            (Self::MySql, Statement::SelectTimerTasks) => "SELECT visibility_timestamp,task_id,data,data_encoding FROM timer_tasks WHERE shard_id=? AND ((visibility_timestamp>=? AND task_id>=?) OR visibility_timestamp>?) AND visibility_timestamp<? ORDER BY visibility_timestamp,task_id LIMIT ?",
            (Self::Postgres, Statement::SelectTimerTasks) => "SELECT visibility_timestamp,task_id,data,data_encoding FROM timer_tasks WHERE shard_id=$1 AND (visibility_timestamp,task_id)>=($2,$3) AND visibility_timestamp<$4 ORDER BY visibility_timestamp,task_id LIMIT $5",
            (Self::MySql, Statement::DeleteTimerTask) => "DELETE FROM timer_tasks WHERE shard_id=? AND visibility_timestamp=? AND task_id=?",
            (Self::Postgres, Statement::DeleteTimerTask) => "DELETE FROM timer_tasks WHERE shard_id=$1 AND visibility_timestamp=$2 AND task_id=$3",
            (Self::MySql, Statement::InsertReplicationTaskDLQ) => "INSERT INTO replication_tasks_dlq(source_cluster_name,shard_id,task_id,data,data_encoding) VALUES(?,?,?,?,?)",
            (Self::Postgres, Statement::InsertReplicationTaskDLQ) => "INSERT INTO replication_tasks_dlq(source_cluster_name,shard_id,task_id,data,data_encoding) VALUES($1,$2,$3,$4,$5)",
            (Self::MySql, Statement::SelectReplicationTasksDLQ) => "SELECT task_id,data,data_encoding FROM replication_tasks_dlq WHERE source_cluster_name=? AND shard_id=? AND task_id>=? AND task_id<? ORDER BY task_id LIMIT ?",
            (Self::Postgres, Statement::SelectReplicationTasksDLQ) => "SELECT task_id,data,data_encoding FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2 AND task_id >= $3 AND task_id < $4 ORDER BY task_id LIMIT $5",
            (Self::MySql, Statement::CountReplicationTasksDLQ) => "SELECT count(1) FROM replication_tasks_dlq WHERE source_cluster_name=? AND shard_id=?",
            (Self::Postgres, Statement::CountReplicationTasksDLQ) => "SELECT count(1) FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2",
            (Self::MySql, Statement::DeleteReplicationTaskDLQ) => "DELETE FROM replication_tasks_dlq WHERE source_cluster_name=? AND shard_id=? AND task_id=?",
            (Self::Postgres, Statement::DeleteReplicationTaskDLQ) => "DELETE FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2 AND task_id=$3",
            (Self::MySql, Statement::RangeDeleteReplicationTasksDLQ) => "DELETE FROM replication_tasks_dlq WHERE source_cluster_name=? AND shard_id=? AND task_id>=? AND task_id<? ORDER BY task_id LIMIT ?",
            (Self::Postgres, Statement::RangeDeleteReplicationTasksDLQ) => "DELETE FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2 AND task_id IN (SELECT task_id FROM replication_tasks_dlq WHERE source_cluster_name=$1 AND shard_id=$2 AND task_id >= $3 AND task_id < $4 ORDER BY task_id LIMIT $5)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Statement {
    CreateShard,
    GetShard,
    LockShard,
    UpdateShard,
    GetExecution,
    LockExecution,
    UpdateExecution,
    DeleteExecution,
    GetCurrentExecution,
    DeleteCurrentExecution,
    InsertExecution,
    InsertCurrentExecution,
    LockCurrentExecution,
    UpdateCurrentExecution,
    DeleteActivityInfoMaps,
    DeleteTimerInfoMaps,
    DeleteChildExecutionInfoMaps,
    DeleteRequestCancelInfoMaps,
    DeleteSignalInfoMaps,
    DeleteSignalsRequested,
    DeleteBufferedEvents,
    InsertHistoryNode,
    ReadHistoryNodes,
    DeleteHistoryNodes,
    RangeDeleteHistoryNodes,
    InsertHistoryTree,
    GetHistoryTree,
    GetAllHistoryTreeBranches,
    DeleteHistoryTree,
    InsertTransferTask,
    SelectTransferTasks,
    DeleteTransferTask,
    InsertReplicationTask,
    SelectReplicationTasks,
    DeleteReplicationTask,
    InsertCrossClusterTask,
    SelectCrossClusterTasks,
    DeleteCrossClusterTask,
    InsertTimerTask,
    SelectTimerTasks,
    DeleteTimerTask,
    InsertReplicationTaskDLQ,
    SelectReplicationTasksDLQ,
    CountReplicationTasksDLQ,
    DeleteReplicationTaskDLQ,
    RangeDeleteReplicationTasksDLQ,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStateMap { Activity, Timer, ChildExecution, RequestCancel, Signal }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowMapOperation { Upsert, Select, DeleteAll, DeleteKeys }

/// Build one of the fixed Go `execution_maps.go` templates. `key_count` is
/// only used to expand the variable length `IN (...)` deletion predicate; all
/// table and column identifiers come from this closed enum.
pub fn workflow_state_map_statement(
    dialect: Dialect,
    map: WorkflowStateMap,
    operation: WorkflowMapOperation,
    key_count: usize,
) -> String {
    let (table, key, extra) = match map {
        WorkflowStateMap::Activity => ("activity_info_maps", "schedule_id", vec!["last_heartbeat_details", "last_heartbeat_updated_time"]),
        WorkflowStateMap::Timer => ("timer_info_maps", "timer_id", vec![]),
        WorkflowStateMap::ChildExecution => ("child_execution_info_maps", "initiated_id", vec![]),
        WorkflowStateMap::RequestCancel => ("request_cancel_info_maps", "initiated_id", vec![]),
        WorkflowStateMap::Signal => ("signal_info_maps", "initiated_id", vec![]),
    };
    let mut value_columns = vec!["data", "data_encoding"];
    value_columns.extend(extra);
    let table_key_columns = format!("shard_id,domain_id,workflow_id,run_id,{key}");
    let columns = value_columns.join(",");
    let insert_columns = format!("{table_key_columns},{columns}");
    let count = insert_columns.split(',').count();
    let markers = (1..=count).map(|n| bind_marker(dialect, n)).collect::<Vec<_>>().join(",");
    let base = "shard_id,domain_id,workflow_id,run_id";
    match operation {
        WorkflowMapOperation::Upsert => match dialect {
            Dialect::MySql => format!("REPLACE INTO {table}({insert_columns}) VALUES({markers})"),
            Dialect::Postgres => {
                let key_columns = format!("{base},{key}");
                format!("INSERT INTO {table}({insert_columns}) VALUES({markers}) ON CONFLICT ({key_columns}) DO UPDATE SET ({columns}) = ({})", value_columns.iter().map(|column| format!("excluded.{column}")).collect::<Vec<_>>().join(","))
            }
        },
        WorkflowMapOperation::Select => format!("SELECT {key},{columns} FROM {table} WHERE {}", where_workflow_key(dialect)),
        WorkflowMapOperation::DeleteAll => format!("DELETE FROM {table} WHERE {}", where_workflow_key(dialect)),
        WorkflowMapOperation::DeleteKeys => {
            let markers = (0..key_count).map(|index| match dialect {
                Dialect::MySql => "?".to_string(),
                Dialect::Postgres => format!("${}", index + 5),
            }).collect::<Vec<_>>();
            let in_list = if markers.is_empty() { "NULL".to_string() } else { markers.join(",") };
            format!("DELETE FROM {table} WHERE {} AND {key} IN ({in_list})", where_workflow_key(dialect))
        }
    }
}

fn bind_marker(dialect: Dialect, index: usize) -> String {
    match dialect { Dialect::MySql => "?".into(), Dialect::Postgres => format!("${index}") }
}

fn where_workflow_key(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::MySql => "shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?",
        Dialect::Postgres => "shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_update_locks_and_conditions_on_range_id() {
        assert!(Dialect::MySql.statement(Statement::LockShard).ends_with("FOR UPDATE"));
        assert!(Dialect::Postgres.statement(Statement::LockShard).ends_with("FOR UPDATE"));
        assert!(Dialect::MySql.statement(Statement::UpdateShard).contains("WHERE shard_id = ?"));
        assert!(Dialect::Postgres.statement(Statement::UpdateShard).contains("WHERE shard_id = $4"));
    }

    #[test]
    fn mysql_and_postgres_bind_markers_match_their_drivers() {
        assert!(Dialect::MySql.statement(Statement::GetExecution).contains("?"));
        assert!(Dialect::Postgres.statement(Statement::GetExecution).contains("$4"));
    }

    #[test]
    fn execution_mutation_and_history_v2_statements_keep_go_predicates() {
        for dialect in [Dialect::MySql, Dialect::Postgres] {
            assert!(dialect.statement(Statement::LockExecution).ends_with("FOR UPDATE"));
            assert!(dialect.statement(Statement::UpdateExecution).contains("WHERE"));
            let read = dialect.statement(Statement::ReadHistoryNodes);
            assert!(read.contains("node_id") && read.contains("txn_id") && read.contains("ORDER BY") && read.contains("LIMIT"));
            assert!(dialect.statement(Statement::DeleteHistoryNodes).contains("node_id"));
            assert!(dialect.statement(Statement::GetHistoryTree).contains("tree_id"));
            assert!(dialect.statement(Statement::GetAllHistoryTreeBranches).contains("ORDER BY shard_id,tree_id,branch_id"));
        }
        assert!(Dialect::MySql.statement(Statement::RangeDeleteHistoryNodes).contains("ORDER BY shard_id,tree_id,branch_id,node_id,txn_id LIMIT ?"));
        assert!(Dialect::Postgres.statement(Statement::RangeDeleteHistoryNodes).contains("(node_id,txn_id) IN (SELECT"));
    }

    #[test]
    fn workflow_state_map_queries_keep_go_upsert_keys_and_values() {
        let mysql = workflow_state_map_statement(Dialect::MySql, WorkflowStateMap::Activity, WorkflowMapOperation::Upsert, 0);
        assert!(mysql.starts_with("REPLACE INTO activity_info_maps"));
        assert!(mysql.contains("schedule_id,data,data_encoding,last_heartbeat_details,last_heartbeat_updated_time"));
        let postgres = workflow_state_map_statement(Dialect::Postgres, WorkflowStateMap::Timer, WorkflowMapOperation::Upsert, 0);
        assert!(postgres.contains("ON CONFLICT (shard_id,domain_id,workflow_id,run_id,timer_id) DO UPDATE"));
        let delete = workflow_state_map_statement(Dialect::Postgres, WorkflowStateMap::Signal, WorkflowMapOperation::DeleteKeys, 2);
        assert!(delete.ends_with("initiated_id IN ($5,$6)"));
    }
}
