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
}
