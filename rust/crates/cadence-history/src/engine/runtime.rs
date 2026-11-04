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
    async fn handle_decision_task_started(&self, request: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse>;
    async fn handle_decision_task_completed(&self, request: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse>;
    async fn handle_decision_task_failed(&self, request: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()>;
    async fn handle_decision_task_scheduled(&self, request: &ScheduleDecisionTaskRequest) -> HResult<()>;
}
pub trait EngineConfig: Send + Sync {
    fn enable_consistent_query(&self) -> bool;
    fn enable_consistent_query_by_domain(&self, domain: &str) -> bool;
    fn enable_sticky_query(&self, domain: &str) -> bool;
    fn max_buffered_query_count(&self) -> usize;
    fn long_poll_expiration_interval(&self, domain: &str) -> std::time::Duration;
    fn maximum_signals_per_execution(&self, domain: &str) -> i32;
    fn enable_history_corruption_check(&self, domain: &str) -> bool;
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
    pub config: Arc<dyn EngineConfig>,
    pub execution_cache: Arc<dyn ExecutionCache>,
    pub matching_client: Arc<dyn QueryMatchingClient>,
    pub raw_matching_client: Arc<dyn QueryMatchingClient>,
    pub client_checker: Arc<dyn ClientChecker>,
    pub workflow_resetter: Arc<dyn WorkflowResetter>,
    pub history_reader: Arc<dyn HistoryReader>,
    pub current_execution_reader: Arc<dyn CurrentExecutionReader>,
    pub history_event_notifier: Arc<dyn HistoryEventNotifier>,
    pub task_refresher: Arc<dyn MutableStateTaskRefresher>,
}

#[async_trait]
pub trait MutableStateTaskRefresher: Send + Sync {
    async fn refresh_tasks(&self, start_timestamp: i64, state: MutableStateRef) -> HResult<()>;
}

#[derive(Clone)]
pub struct HistoryNotification {
    /// Go `events.Notification.ID` (`WorkflowIdentifier`).
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub last_first_event_id: i64,
    pub next_event_id: i64,
    pub previous_started_event_id: i64,
    pub workflow_state: i32,
    pub workflow_close_state: i32,
    pub version_histories: VersionHistories,
}
pub trait HistoryEventNotifier: Send + Sync {
    fn watch_history_event(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)>;
    fn unwatch_history_event(&self, domain_id: &str, execution: &WorkflowExecution, subscriber: &str);
    fn notify_new_history_event(&self, event: HistoryNotification);
}

#[async_trait]
pub trait CurrentExecutionReader: Send + Sync {
    async fn get_current_execution(&self, domain_id: &str, workflow_id: &str, domain_name: &str) -> HResult<String>;
}
pub struct ResetWorkflow {
    pub domain_id: String, pub workflow_id: String, pub base_run_id: String,
    pub base_branch_token: Vec<u8>, pub rebuild_last_event_id: i64, pub rebuild_last_event_version: i64,
    pub base_next_event_id: i64, pub reset_run_id: String, pub request_id: String,
    pub current_context: ExecutionContextRef, pub current_state: MutableStateRef,
    pub current_release: Option<ReleaseFunc>, pub reason: String,
    pub events: Vec<cadence_api::service_types::HistoryEvent>, pub skip_signal_reapply: bool,
}
#[async_trait]
pub trait WorkflowResetter: Send + Sync { async fn reset_workflow(&self, request: ResetWorkflow) -> HResult<()>; }
#[async_trait]
pub trait HistoryReader: Send + Sync {
    async fn paginate_history(&self, first: i64, next: i64, branch: &[u8], token: &[u8], page_size: i32, domain: &str) -> HResult<(Vec<Vec<cadence_api::service_types::HistoryEvent>>, Vec<u8>)>;
}

pub enum QueryTermination { Completed(cadence_api::tasks::WorkflowQueryResult), Unblocked, Failed(HistoryError), Invalid }
pub trait QueryRegistry: Send + Sync {
    fn buffered_count(&self) -> usize;
    fn buffer_query(&self, query: Option<&cadence_api::tasks::WorkflowQuery>) -> (String, tokio::sync::oneshot::Receiver<()>);
    fn remove_query(&self, id: &str);
    fn get_termination_state(&self, id: &str) -> HResult<QueryTermination>;
}
#[async_trait]
pub trait QueryMatchingClient: Send + Sync {
    async fn query_workflow(&self, domain_id: &str, request: &cadence_api::tasks::QueryWorkflowRequest, task_list: Option<&TaskList>) -> HResult<cadence_api::tasks::QueryWorkflowResponse>;
}
pub trait ClientChecker: Send + Sync { fn supports_sticky_query(&self, implementation: &str, feature_version: &str) -> bool; }
