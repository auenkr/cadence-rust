//! History SQL execution row statements and create-mode transition checks.
//!
//! SQL transactions must lock and compare the shard row before invoking these
//! statements. The workflow info payload itself is deliberately left at the
//! `DataBlob` boundary until its Go `WorkflowExecutionInfo` codec is available.

use crate::history_contract::{
    CurrentExecution, CreateWorkflowMode, PersistenceError, WorkflowExecutionInfo,
};
use crate::history_sql_queries::{Dialect, Statement};
use crate::history_sql::{ExecutionRow, WorkflowKey};
use crate::matching_mysql::{MySqlTaskStore, SyncConnectionPool};
use crate::matching_postgres::{PostgresConnectionPool, PostgresTaskStore};
use mysql::prelude::Queryable;
use std::sync::Arc;

type ExecutionDbRow = (i32, Vec<u8>, String, Vec<u8>, i64, i64, Vec<u8>, String);
type CurrentExecutionDbRow = (Vec<u8>, String, i32, i32, i64, i64);

pub struct CreateExecutionRowRequest<'a> {
    pub row: &'a ExecutionRow,
    pub info: &'a WorkflowExecutionInfo,
    pub start_version: i64,
    pub range_id: i64,
    pub mode: CreateWorkflowMode,
    pub previous_run_id: &'a str,
    pub previous_last_write_version: i64,
}

/// Statement source for the execution/current rows and concrete-execution
/// cleanup. Placeholder spelling matches the Go SQL plugins.
pub fn statement(dialect: Dialect, query: Statement) -> &'static str {
    dialect.statement(query)
}

/// MySQL execution-row operations share Matching's existing SQL pool.
#[derive(Clone)]
pub struct MySqlExecutionRows { pool: Arc<SyncConnectionPool> }

impl MySqlExecutionRows {
    pub fn from_task_store(store: &MySqlTaskStore) -> Self { Self { pool: store.history_pool() } }

    pub fn get(&self, key: &WorkflowKey) -> Result<ExecutionRow, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row: Option<ExecutionDbRow> = conn.exec_first(
            statement(Dialect::MySql, Statement::GetExecution),
            (key.shard_id, key.domain_id.as_slice(), key.workflow_id.as_str(), key.run_id.as_slice()),
        ).map_err(internal)?;
        let Some((shard_id, domain_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding)) = row else {
            return Err(PersistenceError::EntityNotExists(format!("Workflow execution not found. WorkflowId: {}, RunId: {}", key.workflow_id, uuid::Uuid::from_bytes(key.run_id))));
        };
        let domain_id: [u8; 16] = domain_id.try_into().map_err(|_| PersistenceError::InternalDataInconsistency("execution domain_id is not 16 bytes".into()))?;
        let run_id: [u8; 16] = run_id.try_into().map_err(|_| PersistenceError::InternalDataInconsistency("execution run_id is not 16 bytes".into()))?;
        Ok(ExecutionRow { key: WorkflowKey { shard_id, domain_id, workflow_id, run_id }, next_event_id, last_write_version, data, data_encoding })
    }

    pub fn get_current(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str) -> Result<CurrentExecution, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row: Option<CurrentExecutionDbRow> = conn.exec_first(
            statement(Dialect::MySql, Statement::GetCurrentExecution), (shard_id, domain_id.as_slice(), workflow_id),
        ).map_err(internal)?;
        row.map(|(run_id, create_request_id, state, close_status, start_version, last_write_version)| {
            let run_id = uuid::Uuid::from_slice(&run_id).map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?.to_string();
            Ok(CurrentExecution { run_id, create_request_id, state, close_status, start_version, last_write_version })
        }).transpose()?.ok_or_else(|| PersistenceError::EntityNotExists(format!("current workflow execution not found: {workflow_id}")))
    }

    pub fn exists(&self, key: &WorkflowKey) -> Result<bool, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let found: Option<i32> = conn.exec_first("SELECT 1 FROM executions WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?", (key.shard_id, key.domain_id.as_slice(), key.workflow_id.as_str(), key.run_id.as_slice())).map_err(internal)?;
        Ok(found.is_some())
    }

    pub fn delete(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.start_transaction(mysql::TxOpts::default()).map_err(internal)?;
        for query in [Statement::DeleteExecution, Statement::DeleteActivityInfoMaps, Statement::DeleteTimerInfoMaps, Statement::DeleteChildExecutionInfoMaps, Statement::DeleteRequestCancelInfoMaps, Statement::DeleteSignalInfoMaps, Statement::DeleteSignalsRequested, Statement::DeleteBufferedEvents] {
            tx.exec_drop(statement(Dialect::MySql, query), (key.shard_id, key.domain_id.as_slice(), key.workflow_id.as_str(), key.run_id.as_slice())).map_err(internal)?;
        }
        tx.commit().map_err(internal)
    }

    pub fn delete_current(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(statement(Dialect::MySql, Statement::DeleteCurrentExecution), (key.shard_id, key.domain_id.as_slice(), key.workflow_id.as_str(), key.run_id.as_slice())).map_err(internal)?;
        Ok(())
    }

    /// Persist the execution row and current-execution transition atomically.
    /// The caller supplies the Go-encoded row blob; SQL never invents a codec.
    pub fn create(&self, request: &CreateExecutionRowRequest<'_>) -> Result<(), PersistenceError> {
        let CreateExecutionRowRequest { row, info, start_version, range_id, mode, previous_run_id, previous_last_write_version } = request;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.start_transaction(mysql::TxOpts::default()).map_err(internal)?;
        let found: Option<i64> = tx.exec_first(statement(Dialect::MySql, Statement::LockShard), (row.key.shard_id,)).map_err(internal)?;
        let Some(found) = found else { return Err(PersistenceError::Internal(format!("shard {} does not exist", row.key.shard_id))); };
        if found != *range_id { return Err(PersistenceError::ShardOwnershipLost { shard_id: row.key.shard_id, expected_range_id: *range_id, actual_range_id: found }); }
        let current: Option<CurrentExecutionDbRow> = tx.exec_first(statement(Dialect::MySql, Statement::LockCurrentExecution), (row.key.shard_id, row.key.domain_id.as_slice(), row.key.workflow_id.as_str())).map_err(internal)?;
        let current = current.map(|(run_id, create_request_id, state, close_status, start_version, last_write_version)| {
            let run_id = uuid::Uuid::from_slice(&run_id).map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?.to_string();
            Ok(CurrentExecution { run_id, create_request_id, state, close_status, start_version, last_write_version })
        }).transpose()?;
        let replace_current = validate_create_transition(*mode, info, previous_run_id, *previous_last_write_version, current.as_ref())?;
        if replace_current {
            match mode {
                CreateWorkflowMode::BrandNew => { tx.exec_drop(statement(Dialect::MySql, Statement::InsertCurrentExecution), (row.key.shard_id, row.key.domain_id.as_slice(), row.key.workflow_id.as_str(), row.key.run_id.as_slice(), info.create_request_id.as_str(), info.state, info.close_status, *start_version, info.last_write_version)).map_err(internal)?; }
                CreateWorkflowMode::WorkflowIdReuse | CreateWorkflowMode::ContinueAsNew => { tx.exec_drop(statement(Dialect::MySql, Statement::UpdateCurrentExecution), (row.key.run_id.as_slice(), info.create_request_id.as_str(), info.state, info.close_status, *start_version, info.last_write_version, row.key.shard_id, row.key.domain_id.as_slice(), row.key.workflow_id.as_str())).map_err(internal)?; }
                CreateWorkflowMode::Zombie => unreachable!("zombie create must not replace current"),
            }
        }
        tx.exec_drop(statement(Dialect::MySql, Statement::InsertExecution), (row.key.shard_id, row.key.domain_id.as_slice(), row.key.workflow_id.as_str(), row.key.run_id.as_slice(), row.next_event_id, row.last_write_version, row.data.as_slice(), row.data_encoding.as_str())).map_err(internal)?;
        tx.commit().map_err(internal)
    }
}

/// PostgreSQL execution-row operations share Matching's existing SQL pool.
#[derive(Clone)]
pub struct PostgresExecutionRows { pool: Arc<PostgresConnectionPool> }

impl PostgresExecutionRows {
    pub fn from_task_store(store: &PostgresTaskStore) -> Self { Self { pool: store.history_pool() } }

    pub fn get(&self, key: &WorkflowKey) -> Result<ExecutionRow, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row = conn.query_opt(statement(Dialect::Postgres, Statement::GetExecution), &[&key.shard_id, &&key.domain_id[..], &key.workflow_id, &&key.run_id[..]]).map_err(internal)?;
        let row = row.ok_or_else(|| PersistenceError::EntityNotExists(format!("Workflow execution not found. WorkflowId: {}, RunId: {}", key.workflow_id, uuid::Uuid::from_bytes(key.run_id))))?;
        let domain_id: Vec<u8> = row.get(1); let run_id: Vec<u8> = row.get(3);
        let domain_id: [u8; 16] = domain_id.try_into().map_err(|_| PersistenceError::InternalDataInconsistency("execution domain_id is not 16 bytes".into()))?;
        let run_id: [u8; 16] = run_id.try_into().map_err(|_| PersistenceError::InternalDataInconsistency("execution run_id is not 16 bytes".into()))?;
        Ok(ExecutionRow { key: WorkflowKey { shard_id: row.get(0), domain_id, workflow_id: row.get(2), run_id }, next_event_id: row.get(4), last_write_version: row.get(5), data: row.get(6), data_encoding: row.get(7) })
    }

    pub fn get_current(&self, shard_id: i32, domain_id: &[u8; 16], workflow_id: &str) -> Result<CurrentExecution, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row = conn.query_opt(statement(Dialect::Postgres, Statement::GetCurrentExecution), &[&shard_id, &&domain_id[..], &workflow_id]).map_err(internal)?.ok_or_else(|| PersistenceError::EntityNotExists(format!("current workflow execution not found: {workflow_id}")))?;
        let run_id: Vec<u8> = row.get(0);
        let run_id = uuid::Uuid::from_slice(&run_id).map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?.to_string();
        Ok(CurrentExecution { run_id, create_request_id: row.get(1), state: row.get(2), close_status: row.get(3), start_version: row.get(4), last_write_version: row.get(5) })
    }

    pub fn exists(&self, key: &WorkflowKey) -> Result<bool, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        Ok(conn.query_opt("SELECT 1 FROM executions WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4", &[&key.shard_id, &&key.domain_id[..], &key.workflow_id, &&key.run_id[..]]).map_err(internal)?.is_some())
    }

    pub fn delete(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.transaction().map_err(internal)?;
        for query in [Statement::DeleteExecution, Statement::DeleteActivityInfoMaps, Statement::DeleteTimerInfoMaps, Statement::DeleteChildExecutionInfoMaps, Statement::DeleteRequestCancelInfoMaps, Statement::DeleteSignalInfoMaps, Statement::DeleteSignalsRequested, Statement::DeleteBufferedEvents] {
            tx.execute(statement(Dialect::Postgres, query), &[&key.shard_id, &&key.domain_id[..], &key.workflow_id, &&key.run_id[..]]).map_err(internal)?;
        }
        tx.commit().map_err(internal)
    }

    pub fn delete_current(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.execute(statement(Dialect::Postgres, Statement::DeleteCurrentExecution), &[&key.shard_id, &&key.domain_id[..], &key.workflow_id, &&key.run_id[..]]).map_err(internal)?;
        Ok(())
    }

    /// Persist the execution row and current-execution transition atomically.
    /// The caller supplies the Go-encoded row blob; SQL never invents a codec.
    pub fn create(&self, request: &CreateExecutionRowRequest<'_>) -> Result<(), PersistenceError> {
        let CreateExecutionRowRequest { row, info, start_version, range_id, mode, previous_run_id, previous_last_write_version } = request;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.transaction().map_err(internal)?;
        let found = tx.query_opt(statement(Dialect::Postgres, Statement::LockShard), &[&row.key.shard_id]).map_err(internal)?.map(|r| r.get::<_, i64>(0));
        let Some(found) = found else { return Err(PersistenceError::Internal(format!("shard {} does not exist", row.key.shard_id))); };
        if found != *range_id { return Err(PersistenceError::ShardOwnershipLost { shard_id: row.key.shard_id, expected_range_id: *range_id, actual_range_id: found }); }
        let current = tx.query_opt(statement(Dialect::Postgres, Statement::LockCurrentExecution), &[&row.key.shard_id, &&row.key.domain_id[..], &row.key.workflow_id]).map_err(internal)?.map(|r| {
            let run_id: Vec<u8> = r.get(3);
            let run_id = uuid::Uuid::from_slice(&run_id).map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?.to_string();
            Ok(CurrentExecution { run_id, create_request_id: r.get(4), state: r.get(5), close_status: r.get(6), start_version: r.get(7), last_write_version: r.get(8) })
        }).transpose()?;
        let replace_current = validate_create_transition(*mode, info, previous_run_id, *previous_last_write_version, current.as_ref())?;
        if replace_current {
            match mode {
                CreateWorkflowMode::BrandNew => { tx.execute(statement(Dialect::Postgres, Statement::InsertCurrentExecution), &[&row.key.shard_id, &&row.key.domain_id[..], &row.key.workflow_id, &&row.key.run_id[..], &info.create_request_id, &info.state, &info.close_status, start_version, &info.last_write_version]).map_err(internal)?; }
                CreateWorkflowMode::WorkflowIdReuse | CreateWorkflowMode::ContinueAsNew => { tx.execute(statement(Dialect::Postgres, Statement::UpdateCurrentExecution), &[&&row.key.run_id[..], &info.create_request_id, &info.state, &info.close_status, start_version, &info.last_write_version, &row.key.shard_id, &&row.key.domain_id[..], &row.key.workflow_id]).map_err(internal)?; }
                CreateWorkflowMode::Zombie => unreachable!("zombie create must not replace current"),
            }
        }
        tx.execute(statement(Dialect::Postgres, Statement::InsertExecution), &[&row.key.shard_id, &&row.key.domain_id[..], &row.key.workflow_id, &&row.key.run_id[..], &row.next_event_id, &row.last_write_version, &row.data, &row.data_encoding]).map_err(internal)?;
        tx.commit().map_err(internal)
    }
}

fn internal(error: impl std::fmt::Display) -> PersistenceError { PersistenceError::Internal(error.to_string()) }

/// Check Go `createWorkflowExecutionTx`'s current-row predicates. The bool
/// reports whether the current row is to be replaced; Zombie creates a
/// concrete execution without becoming current.
pub fn validate_create_transition(
    mode: CreateWorkflowMode,
    new: &WorkflowExecutionInfo,
    previous_run_id: &str,
    previous_last_write_version: i64,
    current: Option<&CurrentExecution>,
) -> Result<bool, PersistenceError> {
    let Some(current) = current else {
        return match mode {
            CreateWorkflowMode::BrandNew => Ok(true),
            CreateWorkflowMode::Zombie => Ok(false),
            CreateWorkflowMode::WorkflowIdReuse | CreateWorkflowMode::ContinueAsNew => Err(PersistenceError::CurrentWorkflowConditionFailed(format!("Workflow execution creation condition failed. WorkflowId: {}, current execution does not exist", new.workflow_id))),
        };
    };
    match mode {
        CreateWorkflowMode::BrandNew => Err(PersistenceError::WorkflowAlreadyStarted {
            message: format!("Workflow execution already running. WorkflowId: {}", new.workflow_id),
            start_request_id: current.create_request_id.clone(),
            run_id: current.run_id.clone(),
            state: current.state,
            close_status: current.close_status,
            last_write_version: current.last_write_version,
        }),
        CreateWorkflowMode::WorkflowIdReuse => {
            let fail = |condition: &str| PersistenceError::CurrentWorkflowConditionFailed(format!(
                "Workflow execution creation condition failed. WorkflowId: {}, {condition}", new.workflow_id
            ));
            if previous_last_write_version != current.last_write_version {
                return Err(fail(&format!("LastWriteVersion: {}, PreviousLastWriteVersion: {previous_last_write_version}", current.last_write_version)));
            }
            if current.state != 2 {
                return Err(fail(&format!("State: {}, Expected: 2", current.state)));
            }
            if current.run_id != previous_run_id {
                return Err(fail(&format!("RunID: {}, PreviousRunID: {previous_run_id}", current.run_id)));
            }
            Ok(true)
        }
        CreateWorkflowMode::ContinueAsNew => {
            if current.run_id != previous_run_id {
                return Err(PersistenceError::CurrentWorkflowConditionFailed(format!(
                    "Workflow execution creation condition failed. WorkflowId: {}, RunID: {}, PreviousRunID: {previous_run_id}",
                    new.workflow_id, current.run_id
                )));
            }
            Ok(true)
        }
        CreateWorkflowMode::Zombie => {
            if current.run_id == new.run_id {
                return Err(PersistenceError::CurrentWorkflowConditionFailed(format!(
                    "Workflow execution run ID must mismatch current run ID. WorkflowId: {}, RunID: {}",
                    new.workflow_id, current.run_id
                )));
            }
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(run_id: &str) -> WorkflowExecutionInfo {
        WorkflowExecutionInfo { workflow_id: "wf".into(), run_id: run_id.into(), ..Default::default() }
    }
    fn current() -> CurrentExecution {
        CurrentExecution { run_id: "old".into(), create_request_id: "request".into(), state: 2, close_status: 1, start_version: 4, last_write_version: 7 }
    }

    #[test]
    fn brand_new_preserves_already_started_fields() {
        let error = validate_create_transition(CreateWorkflowMode::BrandNew, &info("new"), "", 0, Some(&current())).unwrap_err();
        assert_eq!(error, PersistenceError::WorkflowAlreadyStarted { message: "Workflow execution already running. WorkflowId: wf".into(), start_request_id: "request".into(), run_id: "old".into(), state: 2, close_status: 1, last_write_version: 7 });
    }

    #[test]
    fn reuse_and_continue_check_prior_current_row() {
        assert!(validate_create_transition(CreateWorkflowMode::WorkflowIdReuse, &info("new"), "old", 7, Some(&current())).unwrap());
        assert!(matches!(validate_create_transition(CreateWorkflowMode::WorkflowIdReuse, &info("new"), "old", 8, Some(&current())), Err(PersistenceError::CurrentWorkflowConditionFailed(_))));
        assert!(validate_create_transition(CreateWorkflowMode::ContinueAsNew, &info("new"), "old", 0, Some(&current())).unwrap());
        assert!(matches!(validate_create_transition(CreateWorkflowMode::ContinueAsNew, &info("new"), "other", 0, Some(&current())), Err(PersistenceError::CurrentWorkflowConditionFailed(_))));
    }

    #[test]
    fn zombie_does_not_replace_current_and_must_have_different_run() {
        assert!(!validate_create_transition(CreateWorkflowMode::Zombie, &info("new"), "", 0, Some(&current())).unwrap());
        assert!(matches!(validate_create_transition(CreateWorkflowMode::Zombie, &info("old"), "", 0, Some(&current())), Err(PersistenceError::CurrentWorkflowConditionFailed(_))));
    }

    #[test]
    fn absent_current_only_accepts_brand_new_and_zombie_modes() {
        assert!(validate_create_transition(CreateWorkflowMode::BrandNew, &info("new"), "", 0, None).unwrap());
        assert!(!validate_create_transition(CreateWorkflowMode::Zombie, &info("new"), "", 0, None).unwrap());
        assert!(matches!(validate_create_transition(CreateWorkflowMode::WorkflowIdReuse, &info("new"), "", 0, None), Err(PersistenceError::CurrentWorkflowConditionFailed(_))));
        assert!(matches!(validate_create_transition(CreateWorkflowMode::ContinueAsNew, &info("new"), "", 0, None), Err(PersistenceError::CurrentWorkflowConditionFailed(_))));
    }

    #[test]
    fn statements_preserve_both_dialect_predicates() {
        for dialect in [Dialect::MySql, Dialect::Postgres] {
            assert!(statement(dialect, Statement::LockCurrentExecution).ends_with("FOR UPDATE"));
            assert!(statement(dialect, Statement::DeleteCurrentExecution).contains("run_id"));
            for query in [Statement::DeleteActivityInfoMaps, Statement::DeleteTimerInfoMaps, Statement::DeleteChildExecutionInfoMaps, Statement::DeleteRequestCancelInfoMaps, Statement::DeleteSignalInfoMaps, Statement::DeleteSignalsRequested, Statement::DeleteBufferedEvents] {
                let sql = statement(dialect, query);
                assert!(sql.starts_with("DELETE FROM"));
                assert!(sql.contains("shard_id"));
                assert!(sql.contains("run_id"));
            }
        }
        assert!(statement(Dialect::MySql, Statement::LockCurrentExecution).contains("=?"));
        assert!(statement(Dialect::Postgres, Statement::LockCurrentExecution).contains("=$3"));
    }
}
