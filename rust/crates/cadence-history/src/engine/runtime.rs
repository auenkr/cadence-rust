//! TODO(link): adapters for packages being ported concurrently. No engine policy lives here.
use std::{collections::HashMap, sync::Arc};
use async_trait::async_trait;
use cadence_api::{admin::*, types::*};
use super::{deps::*, types::*};

#[derive(Clone, Copy, Default)]
pub struct UpdateAction { pub create_decision: bool, pub noop: bool }
pub type UpdateFn<'a> = Box<dyn Fn(ExecutionContextRef, MutableStateRef) -> Fut<'a, UpdateAction> + Send + Sync + 'a>;
#[async_trait]
pub trait WorkflowUpdater: Send + Sync {
    async fn update<'a>(&self, domain_id: &str, execution: &WorkflowExecution, current: bool, now: i64, action: UpdateFn<'a>) -> HResult<()>;
}
#[async_trait]
pub trait ActiveClusterManager: Send + Sync {
    async fn active_cluster_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<String>;
}
#[derive(Clone, Copy)]
pub enum QueueAction { Reset, GetState }
#[async_trait]
pub trait QueueProcessor: Send + Sync {
    async fn handle_action(&self, cluster: &str, action: QueueAction) -> HResult<Vec<String>>;
    fn notify_new_task(&self, cluster: &str, tasks: &[Task]);
}
#[derive(Clone, Debug)]
pub enum Task {
    History { info: ReplicationTaskInfo, branch_token: Vec<u8>, new_run_branch_token: Vec<u8> },
    SyncActivity(ReplicationTaskInfo),
    FailoverMarker(ReplicationTaskInfo),
}
impl Task {
    pub fn info(&self) -> &ReplicationTaskInfo {
        match self { Self::History { info, .. } | Self::SyncActivity(info) | Self::FailoverMarker(info) => info }
    }
}
#[async_trait]
pub trait ReplicationHydrator: Send + Sync {
    async fn hydrate(&self, task: Task) -> HResult<Option<ReplicationTask>>;
}
#[async_trait]
pub trait ReplicationAckManager: Send + Sync {
    async fn get_tasks(&self, cluster: &str, last_read_id: i64) -> HResult<ReplicationMessages>;
}
pub type DLQRead = (Vec<Option<ReplicationTask>>, Vec<ReplicationTaskInfo>, Vec<u8>);
#[async_trait]
pub trait ReplicationDLQHandler: Send + Sync {
    async fn get_message_count(&self, force_fetch: bool) -> HResult<HashMap<String, i64>>;
    async fn read_messages(&self, cluster: &str, end: i64, page_size: i32, token: &[u8]) -> HResult<DLQRead>;
    async fn purge_messages(&self, cluster: &str, end: i64) -> HResult<()>;
    async fn merge_messages(&self, cluster: &str, end: i64, page_size: i32, token: &[u8]) -> HResult<Vec<u8>>;
}
#[async_trait]
pub trait Replicator: Send + Sync {
    async fn apply_events(&self, request: &ReplicateEventsV2Request) -> HResult<()>;
    async fn sync_activity(&self, request: &SyncActivityRequest) -> HResult<()>;
}
#[async_trait]
pub trait DecisionHandler: Send + Sync {
    async fn handle_decision_task_scheduled(&self, request: &ScheduleDecisionTaskRequest) -> HResult<()>;
}
pub trait ShardClock: Send + Sync { fn set_current_time(&self, cluster: &str, timestamp: i64); }
pub trait EngineMetrics: Send + Sync {
    fn stale_mutable_state(&self, operation: &str);
    fn activity_e2e_latency(&self, operation: &str, domain: &str, workflow_type: &str, activity_type: &str, task_list: &str, started_time: i64);
    fn replication_latency(&self, operation: &str, elapsed: std::time::Duration);
}
/// Components are supplied by the shard factory. TODO(link): factory wiring in history_engine.go.
pub struct HistoryEngine {
    pub current_cluster_name: String,
    pub domain_cache: Arc<dyn DomainCache>,
    pub cluster_metadata: Arc<dyn ClusterMetadata>,
    pub active_cluster_manager: Arc<dyn ActiveClusterManager>,
    pub time_source: Arc<dyn TimeSource>,
    pub updater: Arc<dyn WorkflowUpdater>,
    pub queue_processors: HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>>,
    pub replication_dlq_handler: Arc<dyn ReplicationDLQHandler>,
    pub replication_ack_manager: Arc<dyn ReplicationAckManager>,
    pub replication_hydrator: Arc<dyn ReplicationHydrator>,
    pub replicator: Arc<dyn Replicator>,
    pub decision_handler: Arc<dyn DecisionHandler>,
    pub shard_clock: Arc<dyn ShardClock>,
    pub metrics: Arc<dyn EngineMetrics>,
}
