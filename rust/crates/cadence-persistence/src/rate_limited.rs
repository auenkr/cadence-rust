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

    #[derive(Debug, Default)]
    struct NativeVis {
        native: bool,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl VisibilityStore for NativeVis {
        fn select(&self, _: &VisibilityFilter) -> Result<Vec<VisibilityRow>, VisibilityError> {
            Ok(Vec::new())
        }
        fn get_closed(&self, _: &str, _: &str) -> Result<Option<VisibilityRow>, VisibilityError> {
            Ok(None)
        }
        fn supports_native(&self) -> bool {
            self.native
        }
        fn list_native(&self, _: &VisibilityFilter, _: &[u8], _: i64, _: i64) -> Option<Result<ListPage, VisibilityError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.native.then(|| Ok(ListPage::default()))
        }
    }

    #[test]
    fn list_native_checks_limiter_before_inner_call() {
        let inner = Arc::new(NativeVis { native: true, ..Default::default() });
        let wrapped = RateLimitedVisibilityStore::wrap(inner.clone(), limiter(1, 0, 1));
        let filter = VisibilityFilter::default();
        let mut busy = 0;
        for _ in 0..50 {
            if matches!(wrapped.list_native(&filter, &[], 0, 0), Some(Err(VisibilityError::ServiceBusy(_)))) {
                busy += 1;
            }
        }
        assert!(busy > 0);
        assert_eq!(inner.calls.load(Ordering::SeqCst) + busy, 50);
    }

    #[test]
    fn list_native_none_for_non_native_store_without_charging() {
        let inner = Arc::new(NativeVis::default());
        let wrapped = RateLimitedVisibilityStore::wrap(inner.clone(), limiter(1, 0, 1));
        for _ in 0..50 {
            assert!(wrapped.list_native(&VisibilityFilter::default(), &[], 0, 0).is_none());
        }
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
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

// ---- Go ratelimited.NewDomainManager / NewQueueManager / NewVisibilityManager ----

use crate::matching_domain_metadata::{
    DomainMetadataPersistence, DomainReadError, DomainReplicationDlqMessage,
    DomainReplicationQueueMessage, MatchingDomainMetadata, MatchingDomainPage,
};
use crate::visibility_reader::{ListPage, VisibilityError, VisibilityFilter, VisibilityRow, VisibilityStore};
use std::collections::HashMap;

/// Domain + domain-replication-queue manager (these String-error APIs carry
/// Go's ServiceBusy text; the busy message is returned verbatim).
#[derive(Debug)]
pub struct RateLimitedDomainMetadata {
    inner: Arc<dyn DomainMetadataPersistence>,
    limiter: Arc<PersistenceLimiter>,
}
impl RateLimitedDomainMetadata {
    pub fn wrap(
        inner: Arc<dyn DomainMetadataPersistence>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn DomainMetadataPersistence> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), String> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(PERSISTENCE_LIMIT_EXCEEDED.to_owned())
        }
    }
}
impl DomainMetadataPersistence for RateLimitedDomainMetadata {
    fn metadata_version(&self) -> Result<i64, String> {
        self.check()?;
        self.inner.metadata_version()
    }
    fn get_domain(&self, name: Option<&str>, id: Option<&str>) -> Result<MatchingDomainMetadata, String> {
        self.check()?;
        self.inner.get_domain(name, id)
    }
    fn get_domain_typed(&self, name: Option<&str>, id: Option<&str>) -> Result<MatchingDomainMetadata, DomainReadError> {
        self.check().map_err(DomainReadError::Other)?;
        self.inner.get_domain_typed(name, id)
    }
    fn list_domains(&self, page_size: usize, page_token: Option<&[u8]>) -> Result<MatchingDomainPage, String> {
        self.check()?;
        self.inner.list_domains(page_size, page_token)
    }
    fn create_domain(&self, metadata: &MatchingDomainMetadata, expected_version: i64) -> Result<(), String> {
        self.check()?;
        self.inner.create_domain(metadata, expected_version)
    }
    fn update_domain(&self, metadata: &MatchingDomainMetadata, expected_version: i64) -> Result<(), String> {
        self.check()?;
        self.inner.update_domain(metadata, expected_version)
    }
    fn delete_domain(&self, id: &str) -> Result<(), String> {
        self.check()?;
        self.inner.delete_domain(id)
    }
    fn enqueue_domain_replication_message(&self, payload: Vec<u8>) -> Result<i64, String> {
        self.check()?;
        self.inner.enqueue_domain_replication_message(payload)
    }
    fn read_domain_replication_messages(&self, last_message_id: i64, max_count: usize) -> Result<Vec<DomainReplicationQueueMessage>, String> {
        self.check()?;
        self.inner.read_domain_replication_messages(last_message_id, max_count)
    }
    fn get_domain_replication_ack_levels(&self) -> Result<HashMap<String, i64>, String> {
        self.check()?;
        self.inner.get_domain_replication_ack_levels()
    }
    fn update_domain_replication_ack_level(&self, cluster_name: String, message_id: i64) -> Result<(), String> {
        self.check()?;
        self.inner.update_domain_replication_ack_level(cluster_name, message_id)
    }
    fn read_domain_replication_dlq(&self, first_message_id: i64, last_message_id: i64, max_count: usize) -> Result<Vec<DomainReplicationDlqMessage>, String> {
        self.check()?;
        self.inner.read_domain_replication_dlq(first_message_id, last_message_id, max_count)
    }
    fn delete_domain_replication_dlq_range(&self, first_message_id: i64, last_message_id: i64) -> Result<(), String> {
        self.check()?;
        self.inner.delete_domain_replication_dlq_range(first_message_id, last_message_id)
    }
    fn domain_replication_dlq_size(&self) -> Result<i64, String> {
        self.check()?;
        self.inner.domain_replication_dlq_size()
    }
    fn get_domain_replication_dlq_ack_levels(&self) -> Result<HashMap<String, i64>, String> {
        self.check()?;
        self.inner.get_domain_replication_dlq_ack_levels()
    }
    fn update_domain_replication_dlq_ack_level(&self, cluster_name: String, message_id: i64) -> Result<(), String> {
        self.check()?;
        self.inner.update_domain_replication_dlq_ack_level(cluster_name, message_id)
    }
    fn supports_domain_audit_log(&self) -> bool {
        self.inner.supports_domain_audit_log()
    }
    fn create_domain_audit_log(&self, log: &crate::domain_audit::DomainAuditLog, ttl_seconds: i32) -> Result<(), String> {
        self.check()?;
        self.inner.create_domain_audit_log(log, ttl_seconds)
    }
    fn get_domain_audit_logs(&self, domain_id: &str, operation_type: i32, page_size: usize, page_token: Option<&[u8]>) -> Result<crate::domain_audit::DomainAuditLogPage, String> {
        self.check()?;
        self.inner.get_domain_audit_logs(domain_id, operation_type, page_size, page_token)
    }
}

/// Go `ratelimited.NewVisibilityManager` (SQL/Cassandra visibility uses the
/// datastore limiter).
#[derive(Debug)]
pub struct RateLimitedVisibilityStore {
    inner: Arc<dyn VisibilityStore>,
    limiter: Arc<PersistenceLimiter>,
}
impl RateLimitedVisibilityStore {
    pub fn wrap(
        inner: Arc<dyn VisibilityStore>,
        limiter: Option<Arc<PersistenceLimiter>>,
    ) -> Arc<dyn VisibilityStore> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }
    fn check(&self) -> Result<(), VisibilityError> {
        guard!(self, VisibilityError::ServiceBusy);
        Ok(())
    }
}
impl VisibilityStore for RateLimitedVisibilityStore {
    fn admin_delete(&self, domain_id: &str, run_id: &str) -> Result<(), VisibilityError> {
        self.check()?;
        self.inner.admin_delete(domain_id, run_id)
    }
    fn select(&self, filter: &VisibilityFilter) -> Result<Vec<VisibilityRow>, VisibilityError> {
        self.check()?;
        self.inner.select(filter)
    }
    fn get_closed(&self, domain_id: &str, run_id: &str) -> Result<Option<VisibilityRow>, VisibilityError> {
        self.check()?;
        self.inner.get_closed(domain_id, run_id)
    }
    fn get_closed_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<Option<VisibilityRow>, VisibilityError> {
        self.check()?;
        self.inner.get_closed_by_workflow(domain_id, workflow_id, run_id)
    }
    fn list_native(&self, filter: &VisibilityFilter, page_token: &[u8], earliest_time_us: i64, latest_time_us: i64) -> Option<Result<ListPage, VisibilityError>> {
        // Go checks the limiter before every call; `None` stays "no native paging".
        if !self.inner.supports_native() {
            return None;
        }
        if let Err(error) = self.check() {
            return Some(Err(error));
        }
        self.inner.list_native(filter, page_token, earliest_time_us, latest_time_us)
    }
    fn supports_native(&self) -> bool {
        self.inner.supports_native()
    }
}
