//! History SQL execution row statements and create-mode transition checks.

use crate::history_contract::{
    CreateWorkflowMode, CurrentExecution, DataBlob, PersistenceError, SqlWorkflowExecutionInfo,
    WorkflowExecutionInfo,
};
use crate::history_sql::{ExecutionRow, WorkflowKey};
use crate::history_sql_queries::{Dialect, Statement};
use crate::matching_mysql::{MySqlTaskStore, SyncConnectionPool};
use crate::matching_postgres::{PostgresConnectionPool, PostgresTaskStore};
use mysql::prelude::Queryable;
use std::sync::Arc;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Convert the persistence contract to the exact sqlblobs struct used by Go.
/// UUID fields are stored as their 16 byte SQL representation; variable
/// payloads keep their original bytes and encoding labels.
pub fn execution_info_to_sql(
    info: &WorkflowExecutionInfo,
    start_version: i64,
) -> Result<SqlWorkflowExecutionInfo, PersistenceError> {
    let uuid_bytes = |value: &str, name: &str| -> Result<Vec<u8>, PersistenceError> {
        if value.is_empty() {
            return Ok(Vec::new());
        }
        uuid::Uuid::parse_str(value)
            .map(|v| v.as_bytes().to_vec())
            .map_err(|e| PersistenceError::InvalidArgument(format!("{name}: {e}")))
    };
    let seconds = |nanos: i64| {
        i32::try_from(nanos / NANOS_PER_SECOND).map_err(|_| {
            PersistenceError::InvalidArgument("workflow timeout seconds out of range".into())
        })
    };
    Ok(SqlWorkflowExecutionInfo {
        parent_domain_id: uuid_bytes(&info.parent_domain_id, "parent_domain_id")?,
        parent_workflow_id: info.parent_workflow_id.clone(),
        parent_run_id: uuid_bytes(&info.parent_run_id, "parent_run_id")?,
        initiated_id: if info.parent_domain_id.is_empty() {
            -23
        } else {
            info.initiated_id
        },
        completion_event_batch_id: info.completion_event_batch_id,
        completion_event: info
            .completion_event
            .as_ref()
            .map_or_else(Vec::new, |v| v.data.clone()),
        completion_event_encoding: info
            .completion_event
            .as_ref()
            .map_or_else(String::new, |v| v.encoding.clone()),
        task_list: info.task_list.clone(),
        task_list_kind: info.task_list_kind,
        workflow_type_name: info.workflow_type_name.clone(),
        workflow_timeout_seconds: seconds(info.workflow_timeout_nanos)?,
        decision_task_timeout_seconds: seconds(info.decision_start_to_close_timeout_nanos)?,
        execution_context: info.execution_context.clone(),
        state: info.state,
        close_status: info.close_status,
        start_version,
        last_write_event_id: 0,
        last_event_task_id: info.last_event_task_id,
        last_first_event_id: info.last_first_event_id,
        last_processed_event: info.last_processed_event,
        start_time_unix_nanos: info.start_time_unix_nanos,
        last_updated_time_unix_nanos: info.last_updated_time_unix_nanos,
        decision_version: info.decision_version,
        decision_schedule_id: info.decision_schedule_id,
        decision_started_id: info.decision_started_id,
        decision_timeout_seconds: seconds(info.decision_timeout_nanos)?,
        decision_attempt: info.decision_attempt,
        decision_started_timestamp_unix_nanos: info.decision_started_time_unix_nanos,
        decision_scheduled_timestamp_unix_nanos: info.decision_scheduled_time_unix_nanos,
        cancel_requested: info.cancel_requested,
        decision_original_scheduled_timestamp_unix_nanos: info
            .decision_original_scheduled_time_unix_nanos,
        create_request_id: info.create_request_id.clone(),
        decision_request_id: info.decision_request_id.clone(),
        cancel_request_id: if info.cancel_requested {
            info.cancel_request_id.clone()
        } else {
            String::new()
        },
        sticky_task_list: info.sticky_task_list.clone(),
        sticky_schedule_to_start_timeout_seconds: info.sticky_schedule_to_start_timeout_nanos
            / NANOS_PER_SECOND,
        retry_attempt: i64::from(info.attempt),
        retry_initial_interval_seconds: seconds(info.initial_interval_nanos)?,
        retry_maximum_interval_seconds: seconds(info.maximum_interval_nanos)?,
        retry_maximum_attempts: info.maximum_attempts,
        retry_expiration_seconds: seconds(info.expiration_interval_nanos)?,
        retry_backoff_coefficient: info.backoff_coefficient,
        retry_jitter_coefficient: info.jitter_coefficient,
        retry_expiration_time_unix_nanos: info.expiration_time_unix_nanos,
        retry_non_retryable_errors: info.non_retriable_errors.clone(),
        has_retry_policy: info.has_retry_policy,
        cron_schedule: info.cron_schedule.clone(),
        event_store_version: 2,
        event_branch_token: info.branch_token.clone(),
        signal_count: i64::from(info.signal_count),
        history_size: info.history_size,
        client_library_version: info.client_library_version.clone(),
        client_feature_version: info.client_feature_version.clone(),
        client_impl: info.client_impl.clone(),
        auto_reset_points: info
            .auto_reset_points
            .as_ref()
            .map_or_else(Vec::new, |v| v.data.clone()),
        auto_reset_points_encoding: info
            .auto_reset_points
            .as_ref()
            .map_or_else(String::new, |v| v.encoding.clone()),
        search_attributes: info.search_attributes.clone(),
        memo: info.memo.clone(),
        version_histories: Vec::new(),
        version_histories_encoding: String::new(),
        first_execution_run_id: uuid_bytes(&info.first_execution_run_id, "first_execution_run_id")?,
        partition_config: info.partition_config.clone(),
        checksum: Vec::new(),
        checksum_encoding: String::new(),
        cron_overlap_policy: info.cron_overlap_policy,
        active_cluster_selection_policy: info
            .active_cluster_selection_policy
            .as_ref()
            .map_or_else(Vec::new, |v| v.data.clone()),
        active_cluster_selection_policy_encoding: info
            .active_cluster_selection_policy
            .as_ref()
            .map_or_else(String::new, |v| v.encoding.clone()),
    })
}

/// Decode Go's SQL struct into the public execution info contract. Metadata
/// held in SQL columns (workflow/run/domain IDs, next event and last write
/// version) is supplied separately by the caller.
pub fn execution_info_from_sql(
    sql: SqlWorkflowExecutionInfo,
    key: &WorkflowKey,
    next_event_id: i64,
    last_write_version: i64,
) -> Result<
    (
        WorkflowExecutionInfo,
        i64,
        Option<DataBlob>,
        Option<DataBlob>,
    ),
    PersistenceError,
> {
    let uuid_string = |value: Vec<u8>, name: &str| -> Result<String, PersistenceError> {
        if value.is_empty() {
            return Ok(String::new());
        }
        uuid::Uuid::from_slice(&value)
            .map(|v| v.to_string())
            .map_err(|e| PersistenceError::InternalDataInconsistency(format!("{name}: {e}")))
    };
    let blob = |data: Vec<u8>, encoding: String| {
        (!data.is_empty() || !encoding.is_empty()).then_some(DataBlob { data, encoding })
    };
    let start_version = sql.start_version;
    let version_histories = blob(sql.version_histories, sql.version_histories_encoding);
    let checksum = blob(sql.checksum, sql.checksum_encoding);
    let is_cron = !sql.cron_schedule.is_empty();
    Ok((
        WorkflowExecutionInfo {
            domain_id: uuid::Uuid::from_bytes(key.domain_id).to_string(),
            workflow_id: key.workflow_id.clone(),
            run_id: uuid::Uuid::from_bytes(key.run_id).to_string(),
            first_execution_run_id: uuid_string(
                sql.first_execution_run_id,
                "first_execution_run_id",
            )?,
            parent_domain_id: uuid_string(sql.parent_domain_id, "parent_domain_id")?,
            parent_workflow_id: sql.parent_workflow_id,
            parent_run_id: uuid_string(sql.parent_run_id, "parent_run_id")?,
            initiated_id: sql.initiated_id,
            completion_event_batch_id: sql.completion_event_batch_id,
            create_request_id: sql.create_request_id,
            state: sql.state,
            close_status: sql.close_status,
            next_event_id,
            last_first_event_id: sql.last_first_event_id,
            last_event_task_id: sql.last_event_task_id,
            last_processed_event: sql.last_processed_event,
            last_write_version,
            start_time_unix_nanos: sql.start_time_unix_nanos,
            last_updated_time_unix_nanos: sql.last_updated_time_unix_nanos,
            workflow_timeout_nanos: i64::from(sql.workflow_timeout_seconds) * NANOS_PER_SECOND,
            decision_start_to_close_timeout_nanos: i64::from(sql.decision_task_timeout_seconds)
                * NANOS_PER_SECOND,
            execution_context: sql.execution_context,
            signal_count: sql.signal_count as i32,
            decision_version: sql.decision_version,
            decision_schedule_id: sql.decision_schedule_id,
            decision_started_id: sql.decision_started_id,
            decision_request_id: sql.decision_request_id,
            decision_timeout_nanos: i64::from(sql.decision_timeout_seconds) * NANOS_PER_SECOND,
            decision_attempt: sql.decision_attempt,
            decision_started_time_unix_nanos: sql.decision_started_timestamp_unix_nanos,
            decision_scheduled_time_unix_nanos: sql.decision_scheduled_timestamp_unix_nanos,
            decision_original_scheduled_time_unix_nanos: sql
                .decision_original_scheduled_timestamp_unix_nanos,
            cancel_requested: sql.cancel_requested,
            cancel_request_id: sql.cancel_request_id,
            task_list: sql.task_list,
            task_list_kind: sql.task_list_kind,
            workflow_type_name: sql.workflow_type_name,
            sticky_task_list: sql.sticky_task_list,
            sticky_schedule_to_start_timeout_nanos: sql.sticky_schedule_to_start_timeout_seconds
                * NANOS_PER_SECOND,
            client_library_version: sql.client_library_version,
            client_feature_version: sql.client_feature_version,
            client_impl: sql.client_impl,
            cron_schedule: sql.cron_schedule,
            cron_overlap_policy: sql.cron_overlap_policy,
            expiration_interval_nanos: i64::from(sql.retry_expiration_seconds) * NANOS_PER_SECOND,
            completion_event: blob(sql.completion_event, sql.completion_event_encoding),
            auto_reset_points: blob(sql.auto_reset_points, sql.auto_reset_points_encoding),
            branch_token: sql.event_branch_token,
            attempt: sql.retry_attempt as i32,
            has_retry_policy: sql.has_retry_policy,
            initial_interval_nanos: i64::from(sql.retry_initial_interval_seconds)
                * NANOS_PER_SECOND,
            backoff_coefficient: sql.retry_backoff_coefficient,
            jitter_coefficient: sql.retry_jitter_coefficient,
            maximum_interval_nanos: i64::from(sql.retry_maximum_interval_seconds)
                * NANOS_PER_SECOND,
            expiration_time_unix_nanos: sql.retry_expiration_time_unix_nanos,
            maximum_attempts: sql.retry_maximum_attempts,
            non_retriable_errors: sql.retry_non_retryable_errors,
            memo: sql.memo,
            search_attributes: sql.search_attributes,
            partition_config: sql.partition_config,
            active_cluster_selection_policy: blob(
                sql.active_cluster_selection_policy,
                sql.active_cluster_selection_policy_encoding,
            ),
            history_size: sql.history_size,
            is_cron,
        },
        start_version,
        version_histories,
        checksum,
    ))
}

/// Build the execution SQL row payload using the caller's configured Go
/// serialization encoding. No default encoding is selected here.
pub fn encode_execution_info(
    info: &WorkflowExecutionInfo,
    start_version: i64,
    encoding: crate::sqlblobs::Encoding,
) -> Result<DataBlob, PersistenceError> {
    let sql = execution_info_to_sql(info, start_version)?;
    // Go leaves the deprecated LastWriteEventID pointer nil for NDC writes.
    // Emitting even zero would make Go reconstruct a legacy ReplicationState.
    let mut value = crate::sqlblobs::sql_workflow_execution_info_to_struct(&sql);
    value.fields.retain(|(id, _)| *id != 44);
    let data = encoding
        .encode(&value)
        .map_err(|e| PersistenceError::InvalidArgument(e.to_string()))?;
    Ok(DataBlob {
        data,
        encoding: encoding.as_str().into(),
    })
}

/// Convert a persistence snapshot into the Go-compatible execution row.
/// `Encoding` is explicit because Go selects it from serializer configuration.
pub fn snapshot_to_execution_row(
    snapshot: &crate::history_contract::WorkflowSnapshot,
    shard_id: i32,
    encoding: crate::sqlblobs::Encoding,
) -> Result<ExecutionRow, PersistenceError> {
    let domain_id = uuid::Uuid::parse_str(&snapshot.execution_info.domain_id)
        .map_err(|e| PersistenceError::InvalidArgument(format!("domain_id: {e}")))?;
    let run_id = uuid::Uuid::parse_str(&snapshot.execution_info.run_id)
        .map_err(|e| PersistenceError::InvalidArgument(format!("run_id: {e}")))?;
    let mut sql = execution_info_to_sql(&snapshot.execution_info, snapshot.start_version)?;
    if let Some(blob) = &snapshot.version_histories {
        sql.version_histories.clone_from(&blob.data);
        sql.version_histories_encoding.clone_from(&blob.encoding);
    }
    if let Some(blob) = &snapshot.checksum {
        sql.checksum.clone_from(&blob.data);
        sql.checksum_encoding.clone_from(&blob.encoding);
    }
    // Go leaves the deprecated LastWriteEventID pointer nil for NDC writes.
    // Emitting even zero would make Go reconstruct a legacy ReplicationState.
    let mut value = crate::sqlblobs::sql_workflow_execution_info_to_struct(&sql);
    value.fields.retain(|(id, _)| *id != 44);
    let data = encoding
        .encode(&value)
        .map_err(|e| PersistenceError::InvalidArgument(e.to_string()))?;
    Ok(ExecutionRow {
        key: WorkflowKey {
            shard_id,
            domain_id: *domain_id.as_bytes(),
            workflow_id: snapshot.execution_info.workflow_id.clone(),
            run_id: *run_id.as_bytes(),
        },
        next_event_id: snapshot.execution_info.next_event_id,
        last_write_version: snapshot.last_write_version,
        data,
        data_encoding: encoding.as_str().into(),
    })
}

pub fn decode_execution_info(
    row: &ExecutionRow,
) -> Result<
    (
        WorkflowExecutionInfo,
        i64,
        Option<DataBlob>,
        Option<DataBlob>,
    ),
    PersistenceError,
> {
    let encoding = crate::sqlblobs::Encoding::parse(&row.data_encoding)
        .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?;
    let value = encoding
        .decode(&row.data)
        .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?;
    let sql = crate::sqlblobs::sql_workflow_execution_info_from_struct(&value)
        .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?;
    execution_info_from_sql(sql, &row.key, row.next_event_id, row.last_write_version)
}

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

/// Conditional execution-row update used by Go's `lockAndCheckNextEventID`
/// followed by `updateExecution`. Higher-level mutation/map/task writes must
/// run in this same transaction when they are supplied by the caller.
pub struct ConditionedExecutionUpdate<'a> {
    pub row: &'a ExecutionRow,
    pub condition: i64,
}

/// Statement source for the execution/current rows and concrete-execution
/// cleanup. Placeholder spelling matches the Go SQL plugins.
pub fn statement(dialect: Dialect, query: Statement) -> &'static str {
    dialect.statement(query)
}

/// MySQL execution-row operations share Matching's existing SQL pool.
#[derive(Clone)]
pub struct MySqlExecutionRows {
    pool: Arc<SyncConnectionPool>,
}

impl MySqlExecutionRows {
    pub fn from_task_store(store: &MySqlTaskStore) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }

    pub fn get(&self, key: &WorkflowKey) -> Result<ExecutionRow, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row: Option<ExecutionDbRow> = conn
            .exec_first(
                statement(Dialect::MySql, Statement::GetExecution),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id.as_str(),
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        let Some((
            shard_id,
            domain_id,
            workflow_id,
            run_id,
            next_event_id,
            last_write_version,
            data,
            data_encoding,
        )) = row
        else {
            return Err(PersistenceError::EntityNotExists(format!(
                "Workflow execution not found. WorkflowId: {}, RunId: {}",
                key.workflow_id,
                uuid::Uuid::from_bytes(key.run_id)
            )));
        };
        let domain_id: [u8; 16] = domain_id.try_into().map_err(|_| {
            PersistenceError::InternalDataInconsistency(
                "execution domain_id is not 16 bytes".into(),
            )
        })?;
        let run_id: [u8; 16] = run_id.try_into().map_err(|_| {
            PersistenceError::InternalDataInconsistency("execution run_id is not 16 bytes".into())
        })?;
        Ok(ExecutionRow {
            key: WorkflowKey {
                shard_id,
                domain_id,
                workflow_id,
                run_id,
            },
            next_event_id,
            last_write_version,
            data,
            data_encoding,
        })
    }

    pub fn get_current(
        &self,
        shard_id: i32,
        domain_id: &[u8; 16],
        workflow_id: &str,
    ) -> Result<CurrentExecution, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row: Option<CurrentExecutionDbRow> = conn
            .exec_first(
                statement(Dialect::MySql, Statement::GetCurrentExecution),
                (shard_id, domain_id.as_slice(), workflow_id),
            )
            .map_err(internal)?;
        row.map(
            |(
                run_id,
                create_request_id,
                state,
                close_status,
                start_version,
                last_write_version,
            )| {
                let run_id = uuid::Uuid::from_slice(&run_id)
                    .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?
                    .to_string();
                Ok(CurrentExecution {
                    run_id,
                    create_request_id,
                    state,
                    close_status,
                    start_version,
                    last_write_version,
                })
            },
        )
        .transpose()?
        .ok_or_else(|| {
            PersistenceError::EntityNotExists(format!(
                "current workflow execution not found: {workflow_id}"
            ))
        })
    }

    pub fn exists(&self, key: &WorkflowKey) -> Result<bool, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let found: Option<i32> = conn.exec_first("SELECT 1 FROM executions WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?", (key.shard_id, key.domain_id.as_slice(), key.workflow_id.as_str(), key.run_id.as_slice())).map_err(internal)?;
        Ok(found.is_some())
    }

    pub fn delete(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(internal)?;
        for query in [
            Statement::DeleteExecution,
            Statement::DeleteActivityInfoMaps,
            Statement::DeleteTimerInfoMaps,
            Statement::DeleteChildExecutionInfoMaps,
            Statement::DeleteRequestCancelInfoMaps,
            Statement::DeleteSignalInfoMaps,
            Statement::DeleteSignalsRequested,
            Statement::DeleteBufferedEvents,
        ] {
            tx.exec_drop(
                statement(Dialect::MySql, query),
                (
                    key.shard_id,
                    key.domain_id.as_slice(),
                    key.workflow_id.as_str(),
                    key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        }
        tx.commit().map_err(internal)
    }

    pub fn delete_current(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.exec_drop(
            statement(Dialect::MySql, Statement::DeleteCurrentExecution),
            (
                key.shard_id,
                key.domain_id.as_slice(),
                key.workflow_id.as_str(),
                key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        Ok(())
    }

    /// Persist the execution row and current-execution transition atomically.
    /// The caller supplies the Go-encoded row blob; SQL never invents a codec.
    pub fn create(&self, request: &CreateExecutionRowRequest<'_>) -> Result<(), PersistenceError> {
        let CreateExecutionRowRequest {
            row,
            info,
            start_version,
            range_id,
            mode,
            previous_run_id,
            previous_last_write_version,
        } = request;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(internal)?;
        let found: Option<i64> = tx
            .exec_first(
                statement(Dialect::MySql, Statement::LockShard),
                (row.key.shard_id,),
            )
            .map_err(internal)?;
        let Some(found) = found else {
            return Err(PersistenceError::Internal(format!(
                "shard {} does not exist",
                row.key.shard_id
            )));
        };
        if found != *range_id {
            return Err(PersistenceError::ShardOwnershipLost {
                shard_id: row.key.shard_id,
                expected_range_id: *range_id,
                actual_range_id: found,
            });
        }
        let current: Option<CurrentExecutionDbRow> = tx
            .exec_first(
                statement(Dialect::MySql, Statement::LockCurrentExecution),
                (
                    row.key.shard_id,
                    row.key.domain_id.as_slice(),
                    row.key.workflow_id.as_str(),
                ),
            )
            .map_err(internal)?;
        let current = current
            .map(
                |(
                    run_id,
                    create_request_id,
                    state,
                    close_status,
                    start_version,
                    last_write_version,
                )| {
                    let run_id = uuid::Uuid::from_slice(&run_id)
                        .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?
                        .to_string();
                    Ok(CurrentExecution {
                        run_id,
                        create_request_id,
                        state,
                        close_status,
                        start_version,
                        last_write_version,
                    })
                },
            )
            .transpose()?;
        let replace_current = validate_create_transition(
            *mode,
            info,
            previous_run_id,
            *previous_last_write_version,
            current.as_ref(),
        )?;
        if replace_current {
            match mode {
                CreateWorkflowMode::BrandNew => {
                    tx.exec_drop(
                        statement(Dialect::MySql, Statement::InsertCurrentExecution),
                        (
                            row.key.shard_id,
                            row.key.domain_id.as_slice(),
                            row.key.workflow_id.as_str(),
                            row.key.run_id.as_slice(),
                            info.create_request_id.as_str(),
                            info.state,
                            info.close_status,
                            *start_version,
                            info.last_write_version,
                        ),
                    )
                    .map_err(internal)?;
                }
                CreateWorkflowMode::WorkflowIdReuse | CreateWorkflowMode::ContinueAsNew => {
                    tx.exec_drop(
                        statement(Dialect::MySql, Statement::UpdateCurrentExecution),
                        (
                            row.key.run_id.as_slice(),
                            info.create_request_id.as_str(),
                            info.state,
                            info.close_status,
                            *start_version,
                            info.last_write_version,
                            row.key.shard_id,
                            row.key.domain_id.as_slice(),
                            row.key.workflow_id.as_str(),
                        ),
                    )
                    .map_err(internal)?;
                }
                CreateWorkflowMode::Zombie => {
                    unreachable!("zombie create must not replace current")
                }
            }
        }
        tx.exec_drop(
            statement(Dialect::MySql, Statement::InsertExecution),
            (
                row.key.shard_id,
                row.key.domain_id.as_slice(),
                row.key.workflow_id.as_str(),
                row.key.run_id.as_slice(),
                row.next_event_id,
                row.last_write_version,
                row.data.as_slice(),
                row.data_encoding.as_str(),
            ),
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)
    }

    pub fn update_conditioned(
        &self,
        request: &ConditionedExecutionUpdate<'_>,
    ) -> Result<(), PersistenceError> {
        let row = request.row;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(internal)?;
        let current: Option<i64> = tx
            .exec_first(
                statement(Dialect::MySql, Statement::LockExecution),
                (
                    row.key.shard_id,
                    row.key.domain_id.as_slice(),
                    row.key.workflow_id.as_str(),
                    row.key.run_id.as_slice(),
                ),
            )
            .map_err(internal)?;
        let Some(current) = current else {
            return Err(PersistenceError::EntityNotExists(format!(
                "Workflow execution not found. WorkflowId: {}, RunId: {}",
                row.key.workflow_id,
                uuid::Uuid::from_bytes(row.key.run_id)
            )));
        };
        if current != request.condition {
            return Err(PersistenceError::ConditionFailed(format!(
                "NextEventID condition failed. Expected: {}, Actual: {}",
                request.condition, current
            )));
        }
        tx.exec_drop(
            statement(Dialect::MySql, Statement::UpdateExecution),
            (
                row.next_event_id,
                row.last_write_version,
                row.data.as_slice(),
                row.data_encoding.as_str(),
                row.key.shard_id,
                row.key.domain_id.as_slice(),
                row.key.workflow_id.as_str(),
                row.key.run_id.as_slice(),
            ),
        )
        .map_err(internal)?;
        if tx.affected_rows() != 1 {
            return Err(PersistenceError::ConditionFailed(
                "execution row update affected no rows".into(),
            ));
        }
        tx.commit().map_err(internal)
    }
}

/// PostgreSQL execution-row operations share Matching's existing SQL pool.
#[derive(Clone)]
pub struct PostgresExecutionRows {
    pool: Arc<PostgresConnectionPool>,
}

impl PostgresExecutionRows {
    pub fn from_task_store(store: &PostgresTaskStore) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }

    pub fn get(&self, key: &WorkflowKey) -> Result<ExecutionRow, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row = conn
            .query_opt(
                statement(Dialect::Postgres, Statement::GetExecution),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        let row = row.ok_or_else(|| {
            PersistenceError::EntityNotExists(format!(
                "Workflow execution not found. WorkflowId: {}, RunId: {}",
                key.workflow_id,
                uuid::Uuid::from_bytes(key.run_id)
            ))
        })?;
        let domain_id: Vec<u8> = row.get(1);
        let run_id: Vec<u8> = row.get(3);
        let domain_id: [u8; 16] = domain_id.try_into().map_err(|_| {
            PersistenceError::InternalDataInconsistency(
                "execution domain_id is not 16 bytes".into(),
            )
        })?;
        let run_id: [u8; 16] = run_id.try_into().map_err(|_| {
            PersistenceError::InternalDataInconsistency("execution run_id is not 16 bytes".into())
        })?;
        Ok(ExecutionRow {
            key: WorkflowKey {
                shard_id: row.get(0),
                domain_id,
                workflow_id: row.get(2),
                run_id,
            },
            next_event_id: row.get(4),
            last_write_version: row.get(5),
            data: row.get(6),
            data_encoding: row.get(7),
        })
    }

    pub fn get_current(
        &self,
        shard_id: i32,
        domain_id: &[u8; 16],
        workflow_id: &str,
    ) -> Result<CurrentExecution, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let row = conn
            .query_opt(
                statement(Dialect::Postgres, Statement::GetCurrentExecution),
                &[&shard_id, &&domain_id[..], &workflow_id],
            )
            .map_err(internal)?
            .ok_or_else(|| {
                PersistenceError::EntityNotExists(format!(
                    "current workflow execution not found: {workflow_id}"
                ))
            })?;
        let run_id: Vec<u8> = row.get(0);
        let run_id = uuid::Uuid::from_slice(&run_id)
            .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?
            .to_string();
        Ok(CurrentExecution {
            run_id,
            create_request_id: row.get(1),
            state: row.get(2),
            close_status: row.get(3),
            start_version: row.get(4),
            last_write_version: row.get(5),
        })
    }

    pub fn exists(&self, key: &WorkflowKey) -> Result<bool, PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        Ok(conn.query_opt("SELECT 1 FROM executions WHERE shard_id=$1 AND domain_id=$2 AND workflow_id=$3 AND run_id=$4", &[&key.shard_id, &&key.domain_id[..], &key.workflow_id, &&key.run_id[..]]).map_err(internal)?.is_some())
    }

    pub fn delete(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.transaction().map_err(internal)?;
        for query in [
            Statement::DeleteExecution,
            Statement::DeleteActivityInfoMaps,
            Statement::DeleteTimerInfoMaps,
            Statement::DeleteChildExecutionInfoMaps,
            Statement::DeleteRequestCancelInfoMaps,
            Statement::DeleteSignalInfoMaps,
            Statement::DeleteSignalsRequested,
            Statement::DeleteBufferedEvents,
        ] {
            tx.execute(
                statement(Dialect::Postgres, query),
                &[
                    &key.shard_id,
                    &&key.domain_id[..],
                    &key.workflow_id,
                    &&key.run_id[..],
                ],
            )
            .map_err(internal)?;
        }
        tx.commit().map_err(internal)
    }

    pub fn delete_current(&self, key: &WorkflowKey) -> Result<(), PersistenceError> {
        let mut conn = self.pool.checkout().map_err(internal)?;
        conn.execute(
            statement(Dialect::Postgres, Statement::DeleteCurrentExecution),
            &[
                &key.shard_id,
                &&key.domain_id[..],
                &key.workflow_id,
                &&key.run_id[..],
            ],
        )
        .map_err(internal)?;
        Ok(())
    }

    /// Persist the execution row and current-execution transition atomically.
    /// The caller supplies the Go-encoded row blob; SQL never invents a codec.
    pub fn create(&self, request: &CreateExecutionRowRequest<'_>) -> Result<(), PersistenceError> {
        let CreateExecutionRowRequest {
            row,
            info,
            start_version,
            range_id,
            mode,
            previous_run_id,
            previous_last_write_version,
        } = request;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.transaction().map_err(internal)?;
        let found = tx
            .query_opt(
                statement(Dialect::Postgres, Statement::LockShard),
                &[&row.key.shard_id],
            )
            .map_err(internal)?
            .map(|r| r.get::<_, i64>(0));
        let Some(found) = found else {
            return Err(PersistenceError::Internal(format!(
                "shard {} does not exist",
                row.key.shard_id
            )));
        };
        if found != *range_id {
            return Err(PersistenceError::ShardOwnershipLost {
                shard_id: row.key.shard_id,
                expected_range_id: *range_id,
                actual_range_id: found,
            });
        }
        let current = tx
            .query_opt(
                statement(Dialect::Postgres, Statement::LockCurrentExecution),
                &[
                    &row.key.shard_id,
                    &&row.key.domain_id[..],
                    &row.key.workflow_id,
                ],
            )
            .map_err(internal)?
            .map(|r| {
                let run_id: Vec<u8> = r.get(3);
                let run_id = uuid::Uuid::from_slice(&run_id)
                    .map_err(|e| PersistenceError::InternalDataInconsistency(e.to_string()))?
                    .to_string();
                Ok(CurrentExecution {
                    run_id,
                    create_request_id: r.get(4),
                    state: r.get(5),
                    close_status: r.get(6),
                    start_version: r.get(7),
                    last_write_version: r.get(8),
                })
            })
            .transpose()?;
        let replace_current = validate_create_transition(
            *mode,
            info,
            previous_run_id,
            *previous_last_write_version,
            current.as_ref(),
        )?;
        if replace_current {
            match mode {
                CreateWorkflowMode::BrandNew => {
                    tx.execute(
                        statement(Dialect::Postgres, Statement::InsertCurrentExecution),
                        &[
                            &row.key.shard_id,
                            &&row.key.domain_id[..],
                            &row.key.workflow_id,
                            &&row.key.run_id[..],
                            &info.create_request_id,
                            &info.state,
                            &info.close_status,
                            start_version,
                            &info.last_write_version,
                        ],
                    )
                    .map_err(internal)?;
                }
                CreateWorkflowMode::WorkflowIdReuse | CreateWorkflowMode::ContinueAsNew => {
                    tx.execute(
                        statement(Dialect::Postgres, Statement::UpdateCurrentExecution),
                        &[
                            &&row.key.run_id[..],
                            &info.create_request_id,
                            &info.state,
                            &info.close_status,
                            start_version,
                            &info.last_write_version,
                            &row.key.shard_id,
                            &&row.key.domain_id[..],
                            &row.key.workflow_id,
                        ],
                    )
                    .map_err(internal)?;
                }
                CreateWorkflowMode::Zombie => {
                    unreachable!("zombie create must not replace current")
                }
            }
        }
        tx.execute(
            statement(Dialect::Postgres, Statement::InsertExecution),
            &[
                &row.key.shard_id,
                &&row.key.domain_id[..],
                &row.key.workflow_id,
                &&row.key.run_id[..],
                &row.next_event_id,
                &row.last_write_version,
                &row.data,
                &row.data_encoding,
            ],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)
    }

    pub fn update_conditioned(
        &self,
        request: &ConditionedExecutionUpdate<'_>,
    ) -> Result<(), PersistenceError> {
        let row = request.row;
        let mut conn = self.pool.checkout().map_err(internal)?;
        let mut tx = conn.transaction().map_err(internal)?;
        let current = tx
            .query_opt(
                statement(Dialect::Postgres, Statement::LockExecution),
                &[
                    &row.key.shard_id,
                    &&row.key.domain_id[..],
                    &row.key.workflow_id,
                    &&row.key.run_id[..],
                ],
            )
            .map_err(internal)?
            .map(|r| r.get::<_, i64>(0));
        let Some(current) = current else {
            return Err(PersistenceError::EntityNotExists(format!(
                "Workflow execution not found. WorkflowId: {}, RunId: {}",
                row.key.workflow_id,
                uuid::Uuid::from_bytes(row.key.run_id)
            )));
        };
        if current != request.condition {
            return Err(PersistenceError::ConditionFailed(format!(
                "NextEventID condition failed. Expected: {}, Actual: {}",
                request.condition, current
            )));
        }
        let affected = tx
            .execute(
                statement(Dialect::Postgres, Statement::UpdateExecution),
                &[
                    &row.next_event_id,
                    &row.last_write_version,
                    &row.data,
                    &row.data_encoding,
                    &row.key.shard_id,
                    &&row.key.domain_id[..],
                    &row.key.workflow_id,
                    &&row.key.run_id[..],
                ],
            )
            .map_err(internal)?;
        if affected != 1 {
            return Err(PersistenceError::ConditionFailed(
                "execution row update affected no rows".into(),
            ));
        }
        tx.commit().map_err(internal)
    }
}

fn internal(error: impl std::fmt::Display) -> PersistenceError {
    PersistenceError::Internal(error.to_string())
}

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
            message: format!(
                "Workflow execution already running. WorkflowId: {}",
                new.workflow_id
            ),
            start_request_id: current.create_request_id.clone(),
            run_id: current.run_id.clone(),
            state: current.state,
            close_status: current.close_status,
            last_write_version: current.last_write_version,
        }),
        CreateWorkflowMode::WorkflowIdReuse => {
            let fail = |condition: &str| {
                PersistenceError::CurrentWorkflowConditionFailed(format!(
                    "Workflow execution creation condition failed. WorkflowId: {}, {condition}",
                    new.workflow_id
                ))
            };
            if previous_last_write_version != current.last_write_version {
                return Err(fail(&format!(
                    "LastWriteVersion: {}, PreviousLastWriteVersion: {previous_last_write_version}",
                    current.last_write_version
                )));
            }
            if current.state != 2 {
                return Err(fail(&format!("State: {}, Expected: 2", current.state)));
            }
            if current.run_id != previous_run_id {
                return Err(fail(&format!(
                    "RunID: {}, PreviousRunID: {previous_run_id}",
                    current.run_id
                )));
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
                return Err(PersistenceError::ConditionFailed(format!(
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
    use crate::history_contract::WorkflowSnapshot;

    fn info(run_id: &str) -> WorkflowExecutionInfo {
        WorkflowExecutionInfo {
            workflow_id: "wf".into(),
            run_id: run_id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_execution_row_round_trips_go_sqlblobs_payload() {
        let mut execution_info = info("00000000-0000-0000-0000-000000000002");
        execution_info.domain_id = "00000000-0000-0000-0000-000000000001".into();
        execution_info.next_event_id = 42;
        execution_info.workflow_timeout_nanos = NANOS_PER_SECOND * 17;
        execution_info.create_request_id = "request".into();
        let snapshot = WorkflowSnapshot {
            execution_info,
            start_version: 7,
            last_write_version: 9,
            version_histories: Some(DataBlob {
                encoding: "thriftrw".into(),
                data: vec![1, 2],
            }),
            checksum: Some(DataBlob {
                encoding: "thriftrw".into(),
                data: vec![3, 4],
            }),
            ..Default::default()
        };
        let row =
            snapshot_to_execution_row(&snapshot, 5, crate::sqlblobs::Encoding::ThriftRW).unwrap();
        let wire = crate::sqlblobs::Encoding::ThriftRW.decode(&row.data).unwrap();
        assert!(wire.get(44).is_none(), "NDC writes must not activate legacy replication state");
        assert_eq!(wire.get(102), Some(&cadence_thrift::Value::I32(2)));
        let (decoded, start_version, histories, checksum) = decode_execution_info(&row).unwrap();
        assert_eq!(decoded.workflow_id, snapshot.execution_info.workflow_id);
        assert_eq!(decoded.run_id, snapshot.execution_info.run_id);
        assert_eq!(decoded.domain_id, snapshot.execution_info.domain_id);
        assert_eq!(decoded.next_event_id, 42);
        assert_eq!(decoded.workflow_timeout_nanos, NANOS_PER_SECOND * 17);
        assert_eq!(decoded.create_request_id, "request");
        assert_eq!(start_version, 7);
        assert_eq!(histories, snapshot.version_histories);
        assert_eq!(checksum, snapshot.checksum);
    }
    fn current() -> CurrentExecution {
        CurrentExecution {
            run_id: "old".into(),
            create_request_id: "request".into(),
            state: 2,
            close_status: 1,
            start_version: 4,
            last_write_version: 7,
        }
    }

    #[test]
    fn brand_new_preserves_already_started_fields() {
        let error = validate_create_transition(
            CreateWorkflowMode::BrandNew,
            &info("new"),
            "",
            0,
            Some(&current()),
        )
        .unwrap_err();
        assert_eq!(
            error,
            PersistenceError::WorkflowAlreadyStarted {
                message: "Workflow execution already running. WorkflowId: wf".into(),
                start_request_id: "request".into(),
                run_id: "old".into(),
                state: 2,
                close_status: 1,
                last_write_version: 7
            }
        );
    }

    #[test]
    fn reuse_and_continue_check_prior_current_row() {
        assert!(validate_create_transition(
            CreateWorkflowMode::WorkflowIdReuse,
            &info("new"),
            "old",
            7,
            Some(&current())
        )
        .unwrap());
        assert!(matches!(
            validate_create_transition(
                CreateWorkflowMode::WorkflowIdReuse,
                &info("new"),
                "old",
                8,
                Some(&current())
            ),
            Err(PersistenceError::CurrentWorkflowConditionFailed(_))
        ));
        assert!(validate_create_transition(
            CreateWorkflowMode::ContinueAsNew,
            &info("new"),
            "old",
            0,
            Some(&current())
        )
        .unwrap());
        assert!(matches!(
            validate_create_transition(
                CreateWorkflowMode::ContinueAsNew,
                &info("new"),
                "other",
                0,
                Some(&current())
            ),
            Err(PersistenceError::CurrentWorkflowConditionFailed(_))
        ));
    }

    #[test]
    fn zombie_does_not_replace_current_and_must_have_different_run() {
        assert!(!validate_create_transition(
            CreateWorkflowMode::Zombie,
            &info("new"),
            "",
            0,
            Some(&current())
        )
        .unwrap());
        assert!(matches!(
            validate_create_transition(
                CreateWorkflowMode::Zombie,
                &info("old"),
                "",
                0,
                Some(&current())
            ),
            Err(PersistenceError::CurrentWorkflowConditionFailed(_))
        ));
    }

    #[test]
    fn absent_current_only_accepts_brand_new_and_zombie_modes() {
        assert!(validate_create_transition(
            CreateWorkflowMode::BrandNew,
            &info("new"),
            "",
            0,
            None
        )
        .unwrap());
        assert!(
            !validate_create_transition(CreateWorkflowMode::Zombie, &info("new"), "", 0, None)
                .unwrap()
        );
        assert!(matches!(
            validate_create_transition(
                CreateWorkflowMode::WorkflowIdReuse,
                &info("new"),
                "",
                0,
                None
            ),
            Err(PersistenceError::CurrentWorkflowConditionFailed(_))
        ));
        assert!(matches!(
            validate_create_transition(
                CreateWorkflowMode::ContinueAsNew,
                &info("new"),
                "",
                0,
                None
            ),
            Err(PersistenceError::CurrentWorkflowConditionFailed(_))
        ));
    }

    #[test]
    fn statements_preserve_both_dialect_predicates() {
        for dialect in [Dialect::MySql, Dialect::Postgres] {
            assert!(statement(dialect, Statement::LockCurrentExecution).ends_with("FOR UPDATE"));
            assert!(statement(dialect, Statement::DeleteCurrentExecution).contains("run_id"));
            for query in [
                Statement::DeleteActivityInfoMaps,
                Statement::DeleteTimerInfoMaps,
                Statement::DeleteChildExecutionInfoMaps,
                Statement::DeleteRequestCancelInfoMaps,
                Statement::DeleteSignalInfoMaps,
                Statement::DeleteSignalsRequested,
                Statement::DeleteBufferedEvents,
            ] {
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
