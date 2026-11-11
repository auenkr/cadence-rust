//! Cassandra implementation of History's `GoExecutionStore`
//! (Go `nosqlplugin/cassandra/workflow*.go`, `tasks*.go`,
//! `history_task_dlq*.go`). Ported in increments; methods not yet ported
//! return `Internal("... not yet ported")`.
use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::SerializeValue;
use std::sync::Arc;
use uuid::Uuid;

/// Go constants.go row types / ids.
pub(crate) const ROW_TYPE_EXECUTION: i32 = 1;
pub(crate) const EXECUTION_TASK_ID: i64 = -10;
pub(crate) const PERMANENT_RUN_ID: Uuid = Uuid::from_u128(0x30000000_0000_f000_f000_000000000001);
pub(crate) const DEFAULT_VISIBILITY_MS: i64 = 946_684_800_000;

const GET_CURRENT: &str = "SELECT current_run_id, execution, workflow_last_write_version FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?";
const IS_EXISTS: &str = "SELECT shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?";
const DELETE_ROW: &str = "DELETE FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ? ";
const DELETE_CURRENT: &str = "DELETE FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?  if current_run_id = ? ";

/// Subset of the `workflow_execution` UDT used by the current-run row.
#[derive(Debug, Clone, Default, scylla::DeserializeValue, SerializeValue)]
struct CurrentUdt {
    run_id: Option<Uuid>,
    create_request_id: Option<String>,
    state: Option<i32>,
    close_status: Option<i32>,
}

pub(crate) fn vis_ts() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(DEFAULT_VISIBILITY_MS).expect("valid default visibility timestamp")
}

pub(crate) fn internal(error: impl std::fmt::Display) -> PersistenceError {
    PersistenceError::Internal(format!("Cassandra execution: {error}"))
}

pub(crate) fn parse_uuid(value: &str) -> Result<Uuid, PersistenceError> {
    Uuid::parse_str(value).map_err(|e| PersistenceError::InvalidArgument(format!("invalid uuid {value}: {e}")))
}

fn not_ported<T>(what: &str) -> Result<T, PersistenceError> {
    Err(PersistenceError::Internal(format!("Cassandra execution store: {what} not yet ported")))
}

#[derive(Clone)]
pub struct CassandraExecutionStore {
    driver: Arc<CassandraBlockingSession>,
}

impl CassandraExecutionStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
    }

    fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(Arc<CassandraBlockingSession>) -> Result<T, PersistenceError> + Send + 'static,
    ) -> impl std::future::Future<Output = Result<T, PersistenceError>> {
        let driver = self.driver.clone();
        async move {
            tokio::task::spawn_blocking(move || f(driver))
                .await
                .map_err(|e| internal(format!("blocking task: {e}")))?
        }
    }
}

#[async_trait]
impl GoExecutionStore for CassandraExecutionStore {
    async fn create_workflow_execution(&self, r: CreateWorkflowExecutionRequest) -> Result<CurrentExecution, PersistenceError> {
        self.blocking(move |d| crate::history_execution_cassandra_write::create_workflow_execution(&d, r)).await
    }
    async fn get_workflow_execution(&self, s: i32, d: String, w: String, r: String, _range: i64) -> Result<WorkflowSnapshot, PersistenceError> {
        self.blocking(move |dr| crate::history_execution_cassandra_write::get_workflow_execution(&dr, s, &d, &w, &r)).await
    }
    async fn update_workflow_execution(&self, r: UpdateWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        self.blocking(move |d| crate::history_execution_cassandra_write::update_workflow_execution(&d, r)).await
    }
    async fn conflict_resolve_workflow_execution(&self, r: ConflictResolveWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        self.blocking(move |d| crate::history_execution_cassandra_write::conflict_resolve_workflow_execution(&d, r)).await
    }
    async fn delete_workflow_execution(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.blocking(move |driver| {
            let session = driver.session();
            driver
                .run(session.query_unpaged(
                    DELETE_ROW,
                    (shard_id, ROW_TYPE_EXECUTION, parse_uuid(&domain_id)?, workflow_id, parse_uuid(&run_id)?, vis_ts(), EXECUTION_TASK_ID),
                ))
                .map_err(internal)?;
            Ok(())
        })
        .await
    }
    async fn delete_current_workflow_execution(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.blocking(move |driver| {
            let session = driver.session();
            driver
                .run(session.query_unpaged(
                    DELETE_CURRENT,
                    (shard_id, ROW_TYPE_EXECUTION, parse_uuid(&domain_id)?, workflow_id, PERMANENT_RUN_ID, vis_ts(), EXECUTION_TASK_ID, parse_uuid(&run_id)?),
                ))
                .map_err(internal)?;
            Ok(())
        })
        .await
    }
    async fn get_current_execution(&self, shard_id: i32, domain_id: String, workflow_id: String) -> Result<CurrentExecution, PersistenceError> {
        self.blocking(move |driver| {
            let session = driver.session();
            let result = driver
                .run(session.query_unpaged(
                    GET_CURRENT,
                    (shard_id, ROW_TYPE_EXECUTION, parse_uuid(&domain_id)?, workflow_id.clone(), PERMANENT_RUN_ID, vis_ts(), EXECUTION_TASK_ID),
                ))
                .map_err(internal)?;
            let rows = result.into_rows_result().map_err(internal)?;
            let (run, udt, version) = rows
                .maybe_first_row::<(Option<Uuid>, Option<CurrentUdt>, Option<i64>)>()
                .map_err(internal)?
                .ok_or_else(|| {
                    PersistenceError::EntityNotExists(format!("Workflow execution not found.  WorkflowId: {workflow_id}"))
                })?;
            let udt = udt.unwrap_or_default();
            // Go: lastWriteVersion defaults to constants.EmptyVersion (-24).
            let last_write_version = version.unwrap_or(-24);
            Ok(CurrentExecution {
                run_id: run.map(|u| u.to_string()).unwrap_or_default(),
                create_request_id: udt.create_request_id.unwrap_or_default(),
                state: udt.state.unwrap_or_default(),
                close_status: udt.close_status.unwrap_or_default(),
                start_version: last_write_version,
                last_write_version,
            })
        })
        .await
    }
    async fn is_workflow_execution_exists(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<bool, PersistenceError> {
        self.blocking(move |driver| {
            let session = driver.session();
            let result = driver
                .run(session.query_unpaged(
                    IS_EXISTS,
                    (shard_id, ROW_TYPE_EXECUTION, parse_uuid(&domain_id)?, workflow_id, parse_uuid(&run_id)?, vis_ts(), EXECUTION_TASK_ID),
                ))
                .map_err(internal)?;
            let rows = result.into_rows_result().map_err(internal)?;
            Ok(rows.rows_num() > 0)
        })
        .await
    }
    async fn list_concrete_executions(&self, _f: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        not_ported("list_concrete_executions")
    }
    async fn list_current_executions(&self, _f: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        not_ported("list_current_executions")
    }
    async fn create_transfer_tasks(&self, _s: i32, _t: Vec<TransferTask>) -> Result<(), PersistenceError> {
        not_ported("create_transfer_tasks")
    }
    async fn get_transfer_tasks(&self, _s: i32, _a: i64, _b: i64, _c: i32) -> Result<Vec<TransferTask>, PersistenceError> {
        not_ported("get_transfer_tasks")
    }
    async fn complete_transfer_task(&self, _s: i32, _t: i64) -> Result<(), PersistenceError> {
        not_ported("complete_transfer_task")
    }
    async fn create_failover_marker_tasks(&self, _s: i32, _t: Vec<Task>) -> Result<(), PersistenceError> {
        not_ported("create_failover_marker_tasks")
    }
    async fn create_history_tasks(&self, _s: i32, _t: Vec<Task>) -> Result<(), PersistenceError> {
        not_ported("create_history_tasks")
    }
    async fn create_timer_tasks(&self, _s: i32, _t: Vec<TimerTask>) -> Result<(), PersistenceError> {
        not_ported("create_timer_tasks")
    }
    async fn select_timer_tasks(&self, _s: i32, _a: i64, _b: i64, _c: i32) -> Result<Vec<TimerTask>, PersistenceError> {
        not_ported("select_timer_tasks")
    }
    async fn complete_timer_task(&self, _s: i32, _v: i64, _t: i64) -> Result<(), PersistenceError> {
        not_ported("complete_timer_task")
    }
    async fn select_workflow_timer_tasks(&self, _r: TimerTaskRange) -> Result<Vec<HistoryTaskKey>, PersistenceError> {
        not_ported("select_workflow_timer_tasks")
    }
    async fn create_replication_tasks(&self, _s: i32, _t: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        not_ported("create_replication_tasks")
    }
    async fn get_replication_tasks(&self, _s: i32, _a: i64, _b: i64, _c: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        not_ported("get_replication_tasks")
    }
    async fn complete_replication_task(&self, _s: i32, _t: i64) -> Result<(), PersistenceError> {
        not_ported("complete_replication_task")
    }
    async fn get_replication_dlq_size(&self, _c: String) -> Result<i64, PersistenceError> {
        not_ported("get_replication_dlq_size")
    }
    async fn delete_replication_task_from_dlq(&self, _c: String, _t: i64) -> Result<(), PersistenceError> {
        not_ported("delete_replication_task_from_dlq")
    }
    async fn put_replication_task_to_dlq(&self, _c: String, _t: ReplicationTask) -> Result<(), PersistenceError> {
        not_ported("put_replication_task_to_dlq")
    }
    async fn get_replication_tasks_from_dlq(&self, _c: String, _a: i64, _b: i64, _s: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        not_ported("get_replication_tasks_from_dlq")
    }
    async fn range_delete_replication_tasks_from_dlq(&self, _c: String, _m: i64, _p: i32) -> Result<u64, PersistenceError> {
        not_ported("range_delete_replication_tasks_from_dlq")
    }
    async fn create_cross_cluster_tasks(&self, _s: i32, _t: Vec<CrossClusterTask>) -> Result<(), PersistenceError> {
        not_ported("create_cross_cluster_tasks")
    }
    async fn get_cross_cluster_tasks(&self, _s: i32, _a: i64, _b: i64, _c: i32) -> Result<Vec<CrossClusterTask>, PersistenceError> {
        not_ported("get_cross_cluster_tasks")
    }
    async fn complete_cross_cluster_task(&self, _s: i32, _t: i64) -> Result<(), PersistenceError> {
        not_ported("complete_cross_cluster_task")
    }
    async fn create_replication_tasks_from_dlq(&self, _c: String, _t: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        not_ported("create_replication_tasks_from_dlq")
    }
    async fn get_history_tasks(&self, _r: TaskRange, _c: HistoryTaskCategory) -> Result<Vec<Task>, PersistenceError> {
        not_ported("get_history_tasks")
    }
    async fn complete_history_task(&self, _s: i32, _c: HistoryTaskCategory, _t: i64) -> Result<(), PersistenceError> {
        not_ported("complete_history_task")
    }
    async fn range_complete_history_tasks(&self, _r: TaskRange, _c: HistoryTaskCategory) -> Result<u64, PersistenceError> {
        not_ported("range_complete_history_tasks")
    }
    async fn get_replication_dlq_page(&self, _r: crate::history_task_sql::ReplicationDlqQuery) -> Result<crate::history_task_sql::ReplicationDlqPage, PersistenceError> {
        not_ported("get_replication_dlq_page")
    }
    async fn range_delete_replication_dlq_page(&self, _s: i32, _c: String, _a: i64, _b: i64, _p: i32) -> Result<u64, PersistenceError> {
        not_ported("range_delete_replication_dlq_page")
    }
    async fn get_history_task_page(&self, _r: crate::history_task_sql::HistoryTaskQuery) -> Result<crate::history_task_sql::HistoryTaskPage, PersistenceError> {
        not_ported("get_history_task_page")
    }
    async fn complete_history_task_batch(&self, _s: i32, _c: HistoryTaskCategory, _k: Vec<(i64, i64)>) -> Result<(), PersistenceError> {
        not_ported("complete_history_task_batch")
    }
    async fn range_complete_history_task_page(&self, _r: crate::history_task_sql::HistoryTaskQuery) -> Result<u64, PersistenceError> {
        not_ported("range_complete_history_task_page")
    }
    async fn create_failover_markers_with_range(&self, _s: i32, _r: i64, _m: Vec<Task>) -> Result<(), PersistenceError> {
        not_ported("create_failover_markers_with_range")
    }
    async fn get_active_cluster_selection_policy(&self, _s: i32, _d: String, _w: String, _r: String) -> Result<Option<DataBlob>, PersistenceError> {
        not_ported("get_active_cluster_selection_policy")
    }
    async fn delete_active_cluster_selection_policy(&self, _s: i32, _d: String, _w: String, _r: String) -> Result<(), PersistenceError> {
        not_ported("delete_active_cluster_selection_policy")
    }
}
