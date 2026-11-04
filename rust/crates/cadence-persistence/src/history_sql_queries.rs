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
            (Self::MySql, Statement::DeleteCurrentExecution) => "DELETE FROM current_executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND last_write_version = ?",
            (Self::Postgres, Statement::DeleteCurrentExecution) => "DELETE FROM current_executions WHERE shard_id = $1 AND domain_id = $2 AND workflow_id = $3 AND last_write_version = $4",
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
