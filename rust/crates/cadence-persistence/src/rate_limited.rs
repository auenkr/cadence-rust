//! Go `common/persistence/wrappers/ratelimited`: every persistence manager is
//! wrapped with one per-datastore dynamic limiter (`client/factory.go`
//! `buildRatelimiters`: created only when `maxQPS() > 0` at construction) whose
//! rate is `permember.PerMember(service, globalMaxQPS, hostMaxQPS, resolver)`.
//! A rejected call returns `ServiceBusyError("Persistence Max QPS Reached.")`
//! without reaching the store. (Go's `system.rateLimiterBypassCallerTypes` is
//! context based; Rust store calls carry no caller type.)
use crate::history_contract::*;
use crate::history_reader::{HistoryNodeRow, HistoryNodeStore, HistoryReadError};
use crate::history_sql::{HistoryPersistenceError, ShardRow, ShardStore};
use async_trait::async_trait;
use cadence_api::types::DataBlob;
use cadence_common::dynamic_rate_limiter::DynamicRateLimiter;
use std::sync::Arc;

type ReadResult<T> = Result<T, HistoryReadError>;

/// Go `ratelimited.ErrPersistenceLimitExceeded`.
pub const PERSISTENCE_LIMIT_EXCEEDED: &str = "Persistence Max QPS Reached.";

/// Go `permember.PerMember`.
pub fn per_member(global: f64, instance: f64, member_count: Result<usize, ()>) -> f64 {
    if global <= 0.0 {
        return instance;
    }
    match member_count {
        Ok(n) if n >= 1 => (global / n as f64).max(1.0).min(instance),
        _ => instance,
    }
}

/// Shared per-datastore limiter handed to every wrapper.
pub struct PersistenceLimiter(DynamicRateLimiter);

impl PersistenceLimiter {
    /// Go `buildRatelimiters`: `None` when `host_qps() <= 0` at construction.
    pub fn build(
        host_qps: Arc<dyn Fn() -> i64 + Send + Sync>,
        global_qps: Arc<dyn Fn() -> i64 + Send + Sync>,
        member_count: Arc<dyn Fn() -> Result<usize, ()> + Send + Sync>,
    ) -> Option<Arc<Self>> {
        if host_qps() <= 0 {
            return None;
        }
        Some(Arc::new(Self(DynamicRateLimiter::new(Box::new(move || {
            per_member(global_qps() as f64, host_qps() as f64, member_count())
        })))))
    }

    pub fn allow(&self) -> bool {
        self.0.allow()
    }
}

macro_rules! guard {
    ($self:ident, $err:expr) => {
        if !$self.limiter.allow() {
            return Err($err(PERSISTENCE_LIMIT_EXCEEDED.to_owned()));
        }
    };
}

pub struct RateLimitedExecutionStore {
    inner: Arc<dyn GoExecutionStore>,
    limiter: Arc<PersistenceLimiter>,
}
impl RateLimitedExecutionStore {
    pub fn wrap(
        inner: Arc<dyn GoExecutionStore>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn GoExecutionStore> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), PersistenceError> {
        guard!(self, PersistenceError::ServiceBusy);
        Ok(())
    }
}

pub struct RateLimitedHistoryV2Store {
    inner: Arc<dyn GoHistoryV2Store>,
    limiter: Arc<PersistenceLimiter>,
}
impl RateLimitedHistoryV2Store {
    pub fn wrap(
        inner: Arc<dyn GoHistoryV2Store>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn GoHistoryV2Store> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), PersistenceError> {
        guard!(self, PersistenceError::ServiceBusy);
        Ok(())
    }
}

#[async_trait]
impl GoExecutionStore for RateLimitedExecutionStore {
    async fn create_workflow_execution(&self, request: CreateWorkflowExecutionRequest) -> Result<CurrentExecution, PersistenceError> {
        self.check()?;
        self.inner.create_workflow_execution(request).await
    }
    async fn get_workflow_execution(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String, range_id: i64) -> Result<WorkflowSnapshot, PersistenceError> {
        self.check()?;
        self.inner.get_workflow_execution(shard_id, domain_id, workflow_id, run_id, range_id).await
    }
    async fn update_workflow_execution(&self, request: UpdateWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.update_workflow_execution(request).await
    }
    async fn conflict_resolve_workflow_execution(&self, request: ConflictResolveWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.conflict_resolve_workflow_execution(request).await
    }
    async fn delete_workflow_execution(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.delete_workflow_execution(shard_id, domain_id, workflow_id, run_id).await
    }
    async fn delete_current_workflow_execution(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.delete_current_workflow_execution(shard_id, domain_id, workflow_id, run_id).await
    }
    async fn get_current_execution(&self, shard_id: i32, domain_id: String, workflow_id: String) -> Result<CurrentExecution, PersistenceError> {
        self.check()?;
        self.inner.get_current_execution(shard_id, domain_id, workflow_id).await
    }
    async fn is_workflow_execution_exists(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<bool, PersistenceError> {
        self.check()?;
        self.inner.is_workflow_execution_exists(shard_id, domain_id, workflow_id, run_id).await
    }
    async fn list_concrete_executions(&self, filter: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        self.check()?;
        self.inner.list_concrete_executions(filter).await
    }
    async fn list_current_executions(&self, filter: ExecutionListFilter) -> Result<ExecutionPage, PersistenceError> {
        self.check()?;
        self.inner.list_current_executions(filter).await
    }
    async fn create_transfer_tasks(&self, shard_id: i32, tasks: Vec<TransferTask>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_transfer_tasks(shard_id, tasks).await
    }
    async fn get_transfer_tasks(&self, shard_id: i32, min_id: i64, max_id: i64, batch_size: i32) -> Result<Vec<TransferTask>, PersistenceError> {
        self.check()?;
        self.inner.get_transfer_tasks(shard_id, min_id, max_id, batch_size).await
    }
    async fn complete_transfer_task(&self, shard_id: i32, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_transfer_task(shard_id, task_id).await
    }
    async fn create_failover_marker_tasks(&self, shard_id: i32, tasks: Vec<Task>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_failover_marker_tasks(shard_id, tasks).await
    }
    async fn create_history_tasks(&self, request: CreateHistoryTasksRequest) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_history_tasks(request).await
    }
    async fn create_timer_tasks(&self, shard_id: i32, tasks: Vec<TimerTask>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_timer_tasks(shard_id, tasks).await
    }
    async fn select_timer_tasks(&self, shard_id: i32, min_timestamp: i64, max_timestamp: i64, batch_size: i32) -> Result<Vec<TimerTask>, PersistenceError> {
        self.check()?;
        self.inner.select_timer_tasks(shard_id, min_timestamp, max_timestamp, batch_size).await
    }
    async fn complete_timer_task(&self, shard_id: i32, visibility_timestamp: i64, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_timer_task(shard_id, visibility_timestamp, task_id).await
    }
    async fn select_workflow_timer_tasks(&self, request: SelectWorkflowTimerTasksRequest) -> Result<Vec<HistoryTaskKey>, PersistenceError> {
        self.check()?;
        self.inner.select_workflow_timer_tasks(request).await
    }
    async fn create_replication_tasks(&self, shard_id: i32, tasks: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_replication_tasks(shard_id, tasks).await
    }
    async fn get_replication_tasks(&self, shard_id: i32, min_id: i64, max_id: i64, batch_size: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        self.check()?;
        self.inner.get_replication_tasks(shard_id, min_id, max_id, batch_size).await
    }
    async fn complete_replication_task(&self, shard_id: i32, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_replication_task(shard_id, task_id).await
    }
    async fn get_replication_dlq_size(&self, shard_id: i32, source_cluster: String) -> Result<i64, PersistenceError> {
        self.check()?;
        self.inner.get_replication_dlq_size(shard_id, source_cluster).await
    }
    async fn delete_replication_task_from_dlq(&self, shard_id: i32, source_cluster: String, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.delete_replication_task_from_dlq(shard_id, source_cluster, task_id).await
    }
    async fn put_replication_task_to_dlq(&self, shard_id: i32, source_cluster: String, task: ReplicationTask) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.put_replication_task_to_dlq(shard_id, source_cluster, task).await
    }
    async fn get_replication_tasks_from_dlq(&self, source_cluster: String, min_id: i64, max_id: i64, batch_size: i32) -> Result<Vec<ReplicationTask>, PersistenceError> {
        self.check()?;
        self.inner.get_replication_tasks_from_dlq(source_cluster, min_id, max_id, batch_size).await
    }
    async fn range_delete_replication_tasks_from_dlq(&self, source_cluster: String, inclusive_max_id: i64, page_size: i32) -> Result<u64, PersistenceError> {
        self.check()?;
        self.inner.range_delete_replication_tasks_from_dlq(source_cluster, inclusive_max_id, page_size).await
    }
    async fn create_cross_cluster_tasks(&self, shard_id: i32, tasks: Vec<CrossClusterTask>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_cross_cluster_tasks(shard_id, tasks).await
    }
    async fn get_cross_cluster_tasks(&self, shard_id: i32, min_id: i64, max_id: i64, batch_size: i32) -> Result<Vec<CrossClusterTask>, PersistenceError> {
        self.check()?;
        self.inner.get_cross_cluster_tasks(shard_id, min_id, max_id, batch_size).await
    }
    async fn complete_cross_cluster_task(&self, shard_id: i32, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_cross_cluster_task(shard_id, task_id).await
    }
    async fn create_replication_tasks_from_dlq(&self, source_cluster: String, tasks: Vec<ReplicationTask>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_replication_tasks_from_dlq(source_cluster, tasks).await
    }
    async fn get_history_tasks(&self, range: TaskRange, category: HistoryTaskCategory) -> Result<Vec<Task>, PersistenceError> {
        self.check()?;
        self.inner.get_history_tasks(range, category).await
    }
    async fn complete_history_task(&self, shard_id: i32, category: HistoryTaskCategory, task_id: i64) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_history_task(shard_id, category, task_id).await
    }
    async fn range_complete_history_tasks(&self, range: TaskRange, category: HistoryTaskCategory) -> Result<u64, PersistenceError> {
        self.check()?;
        self.inner.range_complete_history_tasks(range, category).await
    }
    async fn get_replication_dlq_page(&self, request: crate::history_task_sql::ReplicationDlqQuery) -> Result<crate::history_task_sql::ReplicationDlqPage, PersistenceError> {
        self.check()?;
        self.inner.get_replication_dlq_page(request).await
    }
    async fn range_delete_replication_dlq_page(&self, shard_id: i32, source_cluster: String, inclusive_min_id: i64, exclusive_max_id: i64, page_size: i32) -> Result<u64, PersistenceError> {
        self.check()?;
        self.inner.range_delete_replication_dlq_page(shard_id, source_cluster, inclusive_min_id, exclusive_max_id, page_size).await
    }
    async fn get_history_task_page(&self, request: crate::history_task_sql::HistoryTaskQuery) -> Result<crate::history_task_sql::HistoryTaskPage, PersistenceError> {
        self.check()?;
        self.inner.get_history_task_page(request).await
    }
    async fn complete_history_task_batch(&self, shard_id: i32, category: HistoryTaskCategory, keys: Vec<(i64, i64)>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.complete_history_task_batch(shard_id, category, keys).await
    }
    async fn range_complete_history_task_page(&self, request: crate::history_task_sql::HistoryTaskQuery) -> Result<u64, PersistenceError> {
        self.check()?;
        self.inner.range_complete_history_task_page(request).await
    }
    async fn create_failover_markers_with_range(&self, shard_id: i32, range_id: i64, markers: Vec<Task>) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.create_failover_markers_with_range(shard_id, range_id, markers).await
    }
    async fn get_active_cluster_selection_policy(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<Option<DataBlob>, PersistenceError> {
        self.check()?;
        self.inner.get_active_cluster_selection_policy(shard_id, domain_id, workflow_id, run_id).await
    }
    async fn delete_active_cluster_selection_policy(&self, shard_id: i32, domain_id: String, workflow_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.delete_active_cluster_selection_policy(shard_id, domain_id, workflow_id, run_id).await
    }
}

#[async_trait]
impl GoHistoryV2Store for RateLimitedHistoryV2Store {
    async fn append_history_nodes(&self, request: HistoryBranchRequest) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.append_history_nodes(request).await
    }
    async fn read_history_branch(&self, request: HistoryBranchRequest) -> Result<HistoryBranchPage, PersistenceError> {
        self.check()?;
        self.inner.read_history_branch(request).await
    }
    async fn fork_history_branch(&self, request: HistoryBranchRequest, fork_node_id: i64, new_branch_id: String) -> Result<HistoryBranchRequest, PersistenceError> {
        self.check()?;
        self.inner.fork_history_branch(request, fork_node_id, new_branch_id).await
    }
    async fn delete_history_branch(&self, request: HistoryBranchRequest) -> Result<(), PersistenceError> {
        self.check()?;
        self.inner.delete_history_branch(request).await
    }
    async fn get_history_tree(&self, request: HistoryBranchRequest) -> Result<Vec<HistoryTreeBranch>, PersistenceError> {
        self.check()?;
        self.inner.get_history_tree(request).await
    }
    async fn get_all_history_tree_branches(&self, shard_id: i32, page_size: i32, next_page_token: Vec<u8>) -> Result<HistoryTreePage, PersistenceError> {
        self.check()?;
        self.inner.get_all_history_tree_branches(shard_id, page_size, next_page_token).await
    }
}

#[derive(Debug)]
pub struct RateLimitedNodeStore {
    inner: Arc<dyn HistoryNodeStore>,
    limiter: Arc<PersistenceLimiter>,
}
impl std::fmt::Debug for PersistenceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PersistenceLimiter")
    }
}
impl RateLimitedNodeStore {
    pub fn wrap(
        inner: Arc<dyn HistoryNodeStore>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn HistoryNodeStore> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), HistoryReadError> {
        guard!(self, HistoryReadError::ServiceBusy);
        Ok(())
    }
}
impl HistoryNodeStore for RateLimitedNodeStore {
    fn select_page(
        &self,
        shard_id: i32,
        tree_id: [u8; 16],
        branch_id: [u8; 16],
        min_node: i64,
        max_node: i64,
        limit: i64,
        token: Option<&[u8]>,
    ) -> ReadResult<(Vec<HistoryNodeRow>, Option<Vec<u8>>)> {
        self.check()?;
        self.inner.select_page(shard_id, tree_id, branch_id, min_node, max_node, limit, token)
    }
    fn select_nodes(
        &self,
        shard_id: i32,
        tree_id: [u8; 16],
        branch_id: [u8; 16],
        min_node: i64,
        max_node: i64,
        limit: i64,
    ) -> ReadResult<Vec<HistoryNodeRow>> {
        self.check()?;
        self.inner.select_nodes(shard_id, tree_id, branch_id, min_node, max_node, limit)
    }
    fn select_current_execution(
        &self,
        shard_id: i32,
        domain_id: [u8; 16],
        workflow_id: &str,
    ) -> ReadResult<Option<crate::history_reader::CurrentExecutionRow>> {
        self.check()?;
        self.inner.select_current_execution(shard_id, domain_id, workflow_id)
    }
    fn select_active_cluster_policy(
        &self,
        shard_id: i32,
        domain_id: [u8; 16],
        workflow_id: &str,
        run_id: [u8; 16],
    ) -> ReadResult<Option<(Vec<u8>, String)>> {
        self.check()?;
        self.inner.select_active_cluster_policy(shard_id, domain_id, workflow_id, run_id)
    }
}

pub struct RateLimitedShardStore {
    inner: Arc<dyn ShardStore>,
    limiter: Arc<PersistenceLimiter>,
}
impl RateLimitedShardStore {
    pub fn wrap(
        inner: Arc<dyn ShardStore>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn ShardStore> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), HistoryPersistenceError> {
        guard!(self, HistoryPersistenceError::ServiceBusy);
        Ok(())
    }
}
#[async_trait]
impl ShardStore for RateLimitedShardStore {
    async fn create_shard(&self, row: ShardRow) -> Result<(), HistoryPersistenceError> {
        self.check()?;
        self.inner.create_shard(row).await
    }
    async fn get_shard(&self, shard_id: i32) -> Result<ShardRow, HistoryPersistenceError> {
        self.check()?;
        self.inner.get_shard(shard_id).await
    }
    async fn update_shard(
        &self,
        row: ShardRow,
        previous_range_id: i64,
    ) -> Result<(), HistoryPersistenceError> {
        self.check()?;
        self.inner.update_shard(row, previous_range_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[derive(Debug)]
    struct Nodes;
    impl HistoryNodeStore for Nodes {
        fn select_nodes(
            &self, _: i32, _: [u8; 16], _: [u8; 16], _: i64, _: i64, _: i64,
        ) -> ReadResult<Vec<HistoryNodeRow>> {
            Ok(Vec::new())
        }
    }

    fn limiter(host: i64, global: i64, members: usize) -> Option<Arc<PersistenceLimiter>> {
        PersistenceLimiter::build(
            Arc::new(move || host),
            Arc::new(move || global),
            Arc::new(move || Ok(members)),
        )
    }

    #[test]
    fn per_member_matches_go() {
        assert_eq!(per_member(0.0, 100.0, Ok(4)), 100.0);
        assert_eq!(per_member(100.0, 100.0, Err(())), 100.0);
        assert_eq!(per_member(100.0, 100.0, Ok(0)), 100.0);
        assert_eq!(per_member(100.0, 100.0, Ok(4)), 25.0);
        assert_eq!(per_member(100.0, 10.0, Ok(4)), 10.0);
        assert_eq!(per_member(2.0, 100.0, Ok(4)), 1.0);
    }

    #[test]
    fn no_limiter_when_host_qps_not_positive() {
        assert!(limiter(0, 0, 1).is_none());
    }

    #[test]
    fn rejects_with_go_service_busy_text_once_burst_is_spent() {
        let store = RateLimitedNodeStore::wrap(Arc::new(Nodes), limiter(2, 0, 1));
        let call = || store.select_nodes(1, [0; 16], [0; 16], 0, 10, 10);
        assert!(call().is_ok());
        assert!(call().is_ok());
        match call() {
            Err(HistoryReadError::ServiceBusy(m)) => assert_eq!(m, "Persistence Max QPS Reached."),
            other => panic!("expected ServiceBusy, got {other:?}"),
        }
    }

    #[test]
    fn host_rate_is_re_read_dynamically_at_build_only_for_enablement() {
        let host = Arc::new(AtomicI64::new(1));
        let h = host.clone();
        let l = PersistenceLimiter::build(
            Arc::new(move || h.load(Ordering::SeqCst)),
            Arc::new(|| 0),
            Arc::new(|| Ok(1)),
        )
        .unwrap();
        assert!(l.allow());
        assert!(!l.allow());
    }
}
