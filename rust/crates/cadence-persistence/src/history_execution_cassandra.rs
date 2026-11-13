//! Cassandra implementation of History's `GoExecutionStore`
//! (Go `nosqlplugin/cassandra/workflow*.go`, `tasks*.go`,
//! `history_task_dlq*.go`). Ported in fully.
use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::SerializeValue;
use std::sync::Arc;
use crate::history_execution_cassandra_tasks as t;
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
    create_request_id: Option<Uuid>,
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

#[derive(Clone)]
pub struct CassandraExecutionStore {
    driver: Arc<CassandraBlockingSession>,
    shard_id: i32,
    target_cluster: String,
}

impl CassandraExecutionStore {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver, shard_id: 0, target_cluster: String::new() }
    }

    /// Shard served by this store (used by the DLQ methods, whose trait signatures omit it).
    pub fn with_shard_id(mut self, shard_id: i32) -> Self {
        self.shard_id = shard_id;
        self
    }

    /// Target cluster for cross-cluster task reads/completions.
    pub fn with_target_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.target_cluster = cluster.into();
        self
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
                create_request_id: udt.create_request_id.map(|u| u.to_string()).unwrap_or_default(),
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
    async fn list_concrete_executions(&self, f: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        self.blocking(move |d| t::list_concrete_executions(&d, f)).await
    }
    async fn list_current_executions(&self, f: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        self.blocking(move |d| t::list_current_executions(&d, f)).await
    }
    async fn create_transfer_tasks(&self, s: i32, v: Vec<TransferTask>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_transfer_tasks(&d, s, v)).await
    }
    async fn get_transfer_tasks(&self, s: i32, a: i64, b: i64, c: i32) -> Result<Vec<TransferTask>, PersistenceError> {
        self.blocking(move |d| t::get_transfer_tasks(&d, s, a, b, c)).await
    }
    async fn complete_transfer_task(&self, s: i32, id: i64) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::complete_transfer_task(&d, s, id)).await
    }
    async fn create_failover_marker_tasks(&self, s: i32, v: Vec<Task>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_failover_markers(&d, s, None, v)).await
    }
    async fn create_history_tasks(&self, r: CreateHistoryTasksRequest) -> Result<(), PersistenceError> {
        self.blocking(move |d| crate::history_execution_cassandra_write::create_history_tasks(&d, r)).await
    }
    async fn create_timer_tasks(&self, s: i32, v: Vec<TimerTask>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_timer_tasks(&d, s, v)).await
    }
    async fn select_timer_tasks(&self, s: i32, a: i64, b: i64, c: i32) -> Result<Vec<TimerTask>, PersistenceError> {
        self.blocking(move |d| t::select_timer_tasks(&d, s, a, b, c)).await
    }
    async fn complete_timer_task(&self, s: i32, vis: i64, id: i64) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::complete_timer_task(&d, s, vis, id)).await
    }
    async fn select_workflow_timer_tasks(&self, r: SelectWorkflowTimerTasksRequest) -> Result<Vec<HistoryTaskKey>, PersistenceError> {
        self.blocking(move |d| crate::history_execution_cassandra_write::select_workflow_timer_tasks(&d, r)).await
    }
    async fn create_replication_tasks(&self, s: i32, v: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_replication_tasks(&d, s, v)).await
    }
    async fn get_replication_tasks(&self, s: i32, a: i64, b: i64, c: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        self.blocking(move |d| t::get_replication_tasks(&d, s, a, b, c)).await
    }
    async fn complete_replication_task(&self, s: i32, id: i64) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::complete_replication_task(&d, s, id)).await
    }
    async fn get_replication_dlq_size(&self, s: i32, c: String) -> Result<i64, PersistenceError> {
        self.blocking(move |d| t::get_replication_dlq_size(&d, s, &c)).await
    }
    async fn delete_replication_task_from_dlq(&self, s: i32, c: String, id: i64) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::delete_replication_task_from_dlq(&d, s, &c, id)).await
    }
    async fn put_replication_task_to_dlq(&self, s: i32, c: String, task: ReplicationTask) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::put_replication_task_to_dlq(&d, s, &c, task)).await
    }
    async fn get_replication_tasks_from_dlq(&self, c: String, a: i64, b: i64, size: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        let s = self.shard_id;
        self.blocking(move |d| t::get_replication_tasks_from_dlq(&d, s, &c, a, b, size)).await
    }
    async fn range_delete_replication_tasks_from_dlq(&self, c: String, max: i64, _page: i32) -> Result<u64, PersistenceError> {
        let s = self.shard_id;
        self.blocking(move |d| t::range_delete_replication_dlq(&d, s, &c, i64::MIN, max)).await
    }
    async fn create_cross_cluster_tasks(&self, s: i32, v: Vec<CrossClusterTask>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_cross_cluster_tasks(&d, s, v)).await
    }
    async fn get_cross_cluster_tasks(&self, s: i32, a: i64, b: i64, c: i32) -> Result<Vec<CrossClusterTask>, PersistenceError> {
        let target = self.target_cluster.clone();
        self.blocking(move |d| t::get_cross_cluster_tasks(&d, s, &target, a, b, c)).await
    }
    async fn complete_cross_cluster_task(&self, s: i32, id: i64) -> Result<(), PersistenceError> {
        let target = self.target_cluster.clone();
        self.blocking(move |d| t::complete_cross_cluster_task(&d, s, &target, id)).await
    }
    async fn create_replication_tasks_from_dlq(&self, _c: String, v: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        let s = self.shard_id;
        self.blocking(move |d| t::create_replication_tasks(&d, s, v)).await
    }
    async fn get_history_tasks(&self, r: TaskRange, c: HistoryTaskCategory) -> Result<Vec<Task>, PersistenceError> {
        self.blocking(move |d| t::get_history_tasks(&d, r, c)).await
    }
    async fn complete_history_task(&self, s: i32, c: HistoryTaskCategory, id: i64) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::complete_history_task(&d, s, c, id)).await
    }
    async fn range_complete_history_tasks(&self, r: TaskRange, c: HistoryTaskCategory) -> Result<u64, PersistenceError> {
        self.blocking(move |d| t::range_complete_history_tasks(&d, r, c)).await
    }
    async fn get_replication_dlq_page(&self, r: crate::history_task_sql::ReplicationDlqQuery) -> Result<crate::history_task_sql::ReplicationDlqPage, PersistenceError> {
        self.blocking(move |d| t::get_replication_dlq_page(&d, r)).await
    }
    async fn range_delete_replication_dlq_page(&self, s: i32, c: String, a: i64, b: i64, _p: i32) -> Result<u64, PersistenceError> {
        self.blocking(move |d| t::range_delete_replication_dlq(&d, s, &c, a, b)).await
    }
    async fn get_history_task_page(&self, r: crate::history_task_sql::HistoryTaskQuery) -> Result<crate::history_task_sql::HistoryTaskPage, PersistenceError> {
        self.blocking(move |d| t::get_history_task_page(&d, r)).await
    }
    async fn complete_history_task_batch(&self, s: i32, c: HistoryTaskCategory, k: Vec<(i64, i64)>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::complete_history_task_batch(&d, s, c, k)).await
    }
    async fn range_complete_history_task_page(&self, r: crate::history_task_sql::HistoryTaskQuery) -> Result<u64, PersistenceError> {
        self.blocking(move |d| t::range_complete_history_task_page(&d, r)).await
    }
    async fn create_failover_markers_with_range(&self, s: i32, r: i64, m: Vec<Task>) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::create_failover_markers(&d, s, Some(r), m)).await
    }
    async fn get_active_cluster_selection_policy(&self, s: i32, dm: String, w: String, r: String) -> Result<Option<DataBlob>, PersistenceError> {
        self.blocking(move |d| t::get_active_cluster_selection_policy(&d, s, &dm, &w, &r)).await
    }
    async fn delete_active_cluster_selection_policy(&self, s: i32, dm: String, w: String, r: String) -> Result<(), PersistenceError> {
        self.blocking(move |d| t::delete_active_cluster_selection_policy(&d, s, &dm, &w, &r)).await
    }
}
