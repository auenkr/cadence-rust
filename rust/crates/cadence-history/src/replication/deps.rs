//! Interfaces for collaborators of the replication package that live in other
//! Go packages (`shard`, `engine`, `execution`, `ndc`, `persistence`,
//! `client`, `metrics`, `config`, `backoff`). They are intentionally small so
//! the real implementations can be plugged in when those modules are merged.
use async_trait::async_trait;
use cadence_api::admin::{
    GetWorkflowExecutionRawHistoryV2Request, GetWorkflowExecutionRawHistoryV2Response,
    GetDLQReplicationMessagesRequest, GetReplicationMessagesRequest, GetReplicationMessagesResponse,
    ReplicationTaskInfo,
};
use cadence_api::errors::RetryTaskV2Error;
use cadence_api::types::{
    DataBlob, FailoverMarkerAttributes, ReplicationTask, VersionHistories, VersionHistoryItem,
    WorkflowExecution,
};
use cadence_common::backoff::ExponentialRetryPolicy;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::mutable_state_activity::ActivityInfo;

/// `constants.EmptyMessageID`.
pub const EMPTY_MESSAGE_ID: i64 = -1;
/// `constants.EmptyEventID`.
pub const EMPTY_EVENT_ID: i64 = -23;
/// `constants.FirstEventID`.
pub const FIRST_EVENT_ID: i64 = 1;

/// Union of the Go error types the replication package inspects.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ReplicationError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("service busy: {0}")]
    ServiceBusy(String),
    #[error("entity not exists: {0}")]
    EntityNotExists(String),
    #[error("internal data inconsistency: {0}")]
    InternalDataInconsistency(String),
    #[error("shard ownership lost: {0}")]
    ShardOwnershipLost(String),
    #[error("domain not active: {0}")]
    DomainNotActive(String),
    #[error("workflow execution already started: {0}")]
    WorkflowExecutionAlreadyStarted(String),
    #[error("workflow execution already completed: {0}")]
    WorkflowExecutionAlreadyCompleted(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("{0}")]
    RetryTaskV2(RetryTaskV2Error),
    /// yarpc `CodeDeadlineExceeded`.
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),
    /// persistence transient error (`persistence.IsTransientError`).
    #[error("transient persistence error: {0}")]
    Transient(String),
    /// `ndc.ErrSkipTask`.
    #[error("skip task")]
    SkipTask,
    /// `ErrUnknownCluster`.
    #[error("unknown cluster")]
    UnknownCluster,
    /// `execution.ErrMissingVersionHistories` (a `BadRequestError` in Go).
    #[error("versionHistories is empty, which is required for NDC feature. It's probably from deprecated 2dc workflows")]
    MissingVersionHistories,
    #[error("{0}")]
    Other(String),
}

impl ReplicationError {
    /// `ErrUnknownReplicationTask`.
    pub fn unknown_replication_task() -> Self {
        Self::BadRequest("unknown replication task".into())
    }
    /// `ErrEmptyFailoverMarkerAttributes`.
    pub fn empty_failover_marker_attributes() -> Self {
        Self::BadRequest("empty failover marker attributes".into())
    }
    /// `common.IsServiceBusyError`.
    pub fn is_service_busy(&self) -> bool {
        matches!(self, Self::ServiceBusy(_))
    }
    /// `common.IsEntityNotExistsError`.
    pub fn is_entity_not_exists(&self) -> bool {
        matches!(self, Self::EntityNotExists(_))
    }
    /// `persistence.IsTransientError` (internal service error / busy / timeout / db unavailable).
    pub fn is_persistence_transient(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::ServiceBusy(_))
    }
}

pub type Result<T> = std::result::Result<T, ReplicationError>;

// ---------------------------------------------------------------- metrics

pub type Tags<'a> = &'a [(&'a str, &'a str)];

/// Minimal `metrics.Client` / `metrics.Scope` surface. `scope` and `name` are
/// the Go metric scope / metric names.
pub trait ReplicationMetrics: Send + Sync {
    fn inc_counter(&self, _scope: &str, _name: &str, _tags: Tags<'_>) {}
    fn add_counter(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _delta: i64) {}
    fn update_gauge(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _value: f64) {}
    fn record_timer(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _d: Duration) {}
    fn record_histogram_value(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _value: f64) {}
    fn record_histogram_duration(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _d: Duration) {}
    fn exponential_histogram(&self, _scope: &str, _name: &str, _tags: Tags<'_>, _d: Duration) {}
}

/// `metrics.NoopClient` equivalent.
pub struct NoopMetrics;
impl ReplicationMetrics for NoopMetrics {}

// ---------------------------------------------------------------- clock / limiter

/// `clock.TimeSource`.
pub trait TimeSource: Send + Sync {
    fn now_nanos(&self) -> i64;
}

pub struct RealTimeSource;
impl TimeSource for RealTimeSource {
    fn now_nanos(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
}

/// `quotas.Limiter` (only `Wait` is used by this package).
#[async_trait]
pub trait Limiter: Send + Sync {
    async fn wait(&self);
}

// ---------------------------------------------------------------- config

/// Subset of `service/history/config.Config` used by replication.
pub trait ReplicationConfig: Send + Sync {
    fn number_of_shards(&self) -> usize;
    fn max_response_size(&self) -> usize;
    fn replication_task_processor_error_retry_wait(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_error_retry_max_attempts(&self, shard_id: i32) -> usize;
    fn replication_task_processor_error_second_retry_wait(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_error_second_retry_max_wait(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_error_second_retry_expiration(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_no_task_retry_wait(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_cleanup_interval(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_cleanup_jitter_coefficient(&self, shard_id: i32) -> f64;
    fn replication_task_processor_start_wait(&self, shard_id: i32) -> Duration;
    fn replication_task_processor_start_wait_jitter_coefficient(&self, shard_id: i32) -> f64;
    fn replication_task_processor_latency_log_threshold(&self) -> Duration;
    fn replication_task_processor_shard_qps(&self) -> f64;
    fn replication_task_processor_host_qps(&self) -> f64;
    fn replicator_task_delete_batch_size(&self) -> usize;
    fn shard_sync_min_interval(&self) -> Duration;
    fn shard_sync_timer_jitter_coefficient(&self) -> f64;
    fn replication_task_fetcher_parallelism(&self) -> usize;
    fn replication_task_fetcher_aggregation_interval(&self) -> Duration;
    fn replication_task_fetcher_timer_jitter_coefficient(&self) -> f64;
    fn replication_task_fetcher_error_retry_wait(&self) -> Duration;
    fn replication_task_fetcher_service_busy_wait(&self) -> Duration;
    fn replicator_cache_capacity(&self) -> usize;
    fn replicator_cache_max_size(&self) -> usize;
    fn replicator_read_task_max_retry_count(&self) -> usize;
    fn replication_task_generation_qps(&self) -> f64;
    fn replicator_processor_min_task_batch_size(&self, shard_id: i32) -> i32;
    fn replicator_processor_max_task_batch_size(&self, shard_id: i32) -> i32;
    fn replicator_processor_batch_size_step_count(&self, shard_id: i32) -> usize;
}

// ---------------------------------------------------------------- retry

/// `backoff.RetryPolicy`.
pub trait RetryPolicy: Send + Sync {
    /// `None` means `done`.
    fn compute_next_delay(&self, elapsed: Duration, num_attempts: usize) -> Option<Duration>;
}

impl RetryPolicy for ExponentialRetryPolicy {
    fn compute_next_delay(&self, elapsed: Duration, num_attempts: usize) -> Option<Duration> {
        ExponentialRetryPolicy::compute_next_delay(self, &mut rand::thread_rng(), elapsed, num_attempts)
    }
}

/// `backoff.MultiPhasesRetryPolicy`; each phase carries its maximum attempts
/// (0 for the final, unbounded phase).
pub struct MultiPhasesRetryPolicy {
    phases: Vec<(ExponentialRetryPolicy, usize)>,
}

impl MultiPhasesRetryPolicy {
    pub fn new(phases: Vec<(ExponentialRetryPolicy, usize)>) -> Self {
        let n = phases.len();
        for (i, (_, max)) in phases.iter().enumerate() {
            if i + 1 < n && *max == 0 {
                panic!("Non final retry policy in MultiPhasesRetryPolicy need to set maximum attempts");
            }
        }
        Self { phases }
    }
}

impl RetryPolicy for MultiPhasesRetryPolicy {
    fn compute_next_delay(&self, elapsed: Duration, num_attempts: usize) -> Option<Duration> {
        let mut previous = 0usize;
        for (policy, max) in &self.phases {
            if num_attempts >= previous {
                if let Some(d) = ExponentialRetryPolicy::compute_next_delay(
                    policy,
                    &mut rand::thread_rng(),
                    elapsed,
                    num_attempts - previous,
                ) {
                    return Some(d);
                }
            }
            previous += *max;
        }
        None
    }
}

/// `backoff.ThrottleRetry.Do` (without throttle policy / operation timeout,
/// which the replication package does not configure).
pub async fn throttle_retry<T, F, Fut>(
    policy: &dyn RetryPolicy,
    is_retryable: impl Fn(&ReplicationError) -> bool,
    mut op: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let start = std::time::Instant::now();
    let mut attempts = 0usize;
    let mut prev_err: Option<ReplicationError> = None;
    loop {
        let err = match op().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if !is_retryable(&err) {
            return Err(prev_err.unwrap_or(err));
        }
        let next = match policy.compute_next_delay(start.elapsed(), attempts) {
            Some(n) => n,
            None => return Err(prev_err.unwrap_or(err)),
        };
        attempts += 1;
        prev_err = Some(err);
        tokio::time::sleep(next).await;
    }
}

// ---------------------------------------------------------------- persistence view

/// `persistence.HistoryTaskKey` immediate key is just a task id here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskInfo {
    pub task_id: i64,
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub version: i64,
    pub visibility_timestamp_nanos: i64,
}

/// `persistence.Task` variants read from the replication queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryTask {
    SyncActivity { info: TaskInfo, scheduled_id: i64 },
    HistoryReplication {
        info: TaskInfo,
        first_event_id: i64,
        next_event_id: i64,
        branch_token: Vec<u8>,
        new_run_branch_token: Vec<u8>,
    },
    FailoverMarker { info: TaskInfo },
    /// Any other `persistence.Task` implementation (hydrates to `errUnknownReplicationTask`).
    Unknown { info: TaskInfo },
}

impl HistoryTask {
    pub fn info(&self) -> &TaskInfo {
        match self {
            Self::SyncActivity { info, .. }
            | Self::HistoryReplication { info, .. }
            | Self::FailoverMarker { info }
            | Self::Unknown { info } => info,
        }
    }
    pub fn task_id(&self) -> i64 {
        self.info().task_id
    }
}

/// `persistence.ReplicationTaskInfo` (DLQ record).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqTaskInfo {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub task_id: i64,
    pub task_type: i16,
    pub version: i64,
    pub first_event_id: i64,
    pub next_event_id: i64,
    pub scheduled_id: i64,
}

/// `persistence.ReplicationTaskTypeHistory` / `SyncActivity` wire values.
pub const REPLICATION_TASK_TYPE_HISTORY: i16 = 0;
pub const REPLICATION_TASK_TYPE_SYNC_ACTIVITY: i16 = 1;

#[derive(Debug, Clone)]
pub struct PutReplicationTaskToDlqRequest {
    pub source_cluster_name: String,
    pub task_info: DlqTaskInfo,
    pub task: ReplicationTask,
    pub domain_name: String,
    pub shard_id: i32,
}

#[derive(Debug, Clone)]
pub struct ReplicationDlqTask {
    pub info: Option<DlqTaskInfo>,
    pub task: Option<ReplicationTask>,
}

#[derive(Debug, Clone, Default)]
pub struct GetReplicationTasksFromDlqResponse {
    pub tasks: Vec<ReplicationDlqTask>,
    pub next_page_token: Vec<u8>,
}

/// The `persistence.ExecutionManager` calls used by replication.
#[async_trait]
pub trait ExecutionManager: Send + Sync {
    /// `GetHistoryTasks` for the replication category; returns tasks and whether a next page exists.
    async fn get_replication_tasks(
        &self,
        shard_id: i32,
        inclusive_min_task_id: i64,
        exclusive_max_task_id: i64,
        page_size: usize,
    ) -> Result<(Vec<HistoryTask>, bool)>;
    /// `RangeCompleteHistoryTask`; returns `TasksCompleted`.
    async fn range_complete_replication_task(
        &self,
        shard_id: i32,
        exclusive_max_task_id: i64,
        page_size: usize,
    ) -> Result<usize>;
    async fn put_replication_task_to_dlq(&self, req: PutReplicationTaskToDlqRequest) -> Result<()>;
    async fn get_replication_dlq_size(&self, shard_id: i32, source_cluster: &str) -> Result<i64>;
    async fn get_replication_tasks_from_dlq(
        &self,
        shard_id: i32,
        source_cluster: &str,
        read_level: i64,
        max_read_level: i64,
        batch_size: usize,
        next_page_token: Vec<u8>,
    ) -> Result<GetReplicationTasksFromDlqResponse>;
    async fn range_delete_replication_task_from_dlq(
        &self,
        shard_id: i32,
        source_cluster: &str,
        inclusive_begin_task_id: i64,
        exclusive_end_task_id: i64,
    ) -> Result<()>;
}

/// `persistence.HistoryManager.ReadRawHistoryBranch` for one batch.
#[async_trait]
pub trait HistoryManager: Send + Sync {
    async fn read_raw_history_branch(
        &self,
        shard_id: i32,
        domain_name: &str,
        branch_token: &[u8],
        min_event_id: i64,
        max_event_id: i64,
        page_size: usize,
    ) -> Result<Vec<DataBlob>>;
}

/// `persistence.PayloadSerializer.DeserializeBatchEvents` reduced to what the
/// DLQ request generation reads: `(first event id, last event id, first event version)`.
pub trait PayloadSerializer: Send + Sync {
    /// Returns `(id, version)` of each event in the batch.
    fn deserialize_batch_events(&self, blob: &DataBlob) -> Result<Vec<(i64, i64)>>;
}

// ---------------------------------------------------------------- domain cache

/// `cache.DomainCacheEntry` fields used here.
#[derive(Debug, Clone, Default)]
pub struct DomainEntry {
    pub name: String,
    pub replication_clusters: Vec<String>,
}

impl DomainEntry {
    /// `HasReplicationCluster`.
    pub fn has_replication_cluster(&self, cluster: &str) -> bool {
        self.replication_clusters.iter().any(|c| c == cluster)
    }
}

pub trait DomainCache: Send + Sync {
    fn get_domain_by_id(&self, id: &str) -> Result<DomainEntry>;
    fn get_domain_name(&self, id: &str) -> Result<String>;
}

// ---------------------------------------------------------------- shard / engine / cluster

/// Subset of `shard.Context` used by replication.
#[async_trait]
pub trait ShardContext: Send + Sync {
    fn shard_id(&self) -> i32;
    fn current_cluster_name(&self) -> String;
    fn remote_cluster_names(&self) -> Vec<String>;
    fn time_source(&self) -> Arc<dyn TimeSource>;
    fn domain_cache(&self) -> Arc<dyn DomainCache>;
    fn execution_manager(&self) -> Arc<dyn ExecutionManager>;
    fn client_bean(&self) -> Arc<dyn ClientBean>;
    /// `ClusterNameForFailoverVersion`.
    fn cluster_name_for_failover_version(&self, version: i64) -> Result<String>;
    /// `GetQueueClusterAckLevel(Replication, cluster).GetTaskID()`.
    fn get_queue_cluster_ack_level(&self, cluster: &str) -> i64;
    /// `UpdateIfNeededAndGetQueueMaxReadLevel(Replication, cluster).GetTaskID()`.
    fn update_if_needed_and_get_queue_max_read_level(&self, cluster: &str) -> i64;
    fn update_queue_cluster_ack_level(&self, cluster: &str, ack_level: i64) -> Result<()>;
    /// `AddingPendingFailoverMarker`.
    fn adding_pending_failover_marker(&self, marker: FailoverMarkerAttributes) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct SyncActivityRequest {
    pub task: cadence_api::types::SyncActivityTaskAttributes,
}

#[derive(Debug, Clone, Default)]
pub struct ReplicateEventsV2Request {
    pub domain_uuid: Option<String>,
    pub workflow_execution: WorkflowExecution,
    pub version_history_items: Option<Vec<VersionHistoryItem>>,
    pub events: Option<DataBlob>,
    pub new_run_events: Option<DataBlob>,
}

#[derive(Debug, Clone)]
pub struct SyncShardStatusRequest {
    pub source_cluster: String,
    pub shard_id: i64,
    pub timestamp: Option<i64>,
}

/// `engine.Engine` methods used by replication.
#[async_trait]
pub trait Engine: Send + Sync {
    async fn sync_shard_status(&self, req: SyncShardStatusRequest, timeout: Duration) -> Result<()>;
    async fn sync_activity(&self, req: SyncActivityRequest, timeout: Duration) -> Result<()>;
    async fn replicate_events_v2(&self, req: ReplicateEventsV2Request, timeout: Duration) -> Result<()>;
}

/// Cross-shard history client (`client.Bean.GetHistoryClient`).
#[async_trait]
pub trait HistoryClient: Send + Sync {
    async fn sync_activity(&self, req: SyncActivityRequest, timeout: Duration) -> Result<()>;
    async fn replicate_events_v2(&self, req: ReplicateEventsV2Request, timeout: Duration) -> Result<()>;
}

/// `admin.Client` calls used against a remote cluster.
#[async_trait]
pub trait AdminClient: Send + Sync {
    async fn get_replication_messages(
        &self,
        req: GetReplicationMessagesRequest,
        timeout: Duration,
    ) -> Result<GetReplicationMessagesResponse>;
    async fn get_dlq_replication_messages(
        &self,
        req: GetDLQReplicationMessagesRequest,
    ) -> Result<Vec<ReplicationTask>>;
    /// `admin.Client.ReapplyEvents` (NDC reapply to the remote active cluster).
    async fn reapply_events(&self, _req: cadence_api::admin::ReapplyEventsRequest) -> Result<()> {
        Err(ReplicationError::Other("ReapplyEvents is not supported by this admin client".into()))
    }
    /// `admin.Client.GetWorkflowExecutionRawHistoryV2` (used by the history resender).
    async fn get_workflow_execution_raw_history_v2(
        &self,
        _req: GetWorkflowExecutionRawHistoryV2Request,
        _timeout: Duration,
    ) -> Result<GetWorkflowExecutionRawHistoryV2Response> {
        Err(ReplicationError::Other("GetWorkflowExecutionRawHistoryV2 is not supported by this admin client".into()))
    }
}

/// `frontend.Client` call used by `triggerDataInconsistencyScan`.
#[async_trait]
pub trait FrontendClient: Send + Sync {
    /// Sends the `SignalWithStartWorkflowExecution` described by the Go code;
    /// `signal_input` is the JSON-marshalled `entity.Execution`.
    async fn start_data_corruption_scan(&self, signal_input: Vec<u8>) -> Result<()>;
}

/// `client.Bean`.
pub trait ClientBean: Send + Sync {
    fn history_client(&self) -> Arc<dyn HistoryClient>;
    fn remote_admin_client(&self, cluster: &str) -> Result<Arc<dyn AdminClient>>;
    fn remote_frontend_client(&self, cluster: &str) -> Result<Arc<dyn FrontendClient>>;
}

/// `ndc.HistoryResender`.
#[async_trait]
pub trait HistoryResender: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn send_single_workflow_history(
        &self,
        remote_cluster: &str,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        start_event_id: Option<i64>,
        start_event_version: Option<i64>,
        end_event_id: Option<i64>,
        end_event_version: Option<i64>,
    ) -> Result<()>;
}

// ---------------------------------------------------------------- mutable state

/// Mutable-state view used by the hydrator.
pub trait MutableStateView: Send + Sync {
    fn is_workflow_execution_running(&self) -> bool;
    fn get_activity_info(&self, id: i64) -> Option<ActivityInfo>;
    fn get_version_histories(&self) -> Option<VersionHistories>;
}

/// `execution.ReleaseFunc`; invoked at most once.
pub type ReleaseFn = Box<dyn FnOnce(Option<&ReplicationError>) + Send>;

#[async_trait]
pub trait MutableStateProvider: Send + Sync {
    async fn get_mutable_state(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<(Arc<dyn MutableStateView>, ReleaseFn)>;
}

/// `types.ReplicationMessagesSizeFn`.
pub type ReplicationMessagesSizeFn =
    Arc<dyn Fn(&cadence_api::admin::ReplicationMessages) -> usize + Send + Sync>;

/// Convenience used by several files.
pub type TaskMap = HashMap<i64, ReplicationTask>;

/// Convert `ReplicationTaskInfo` (admin type) helper not needed elsewhere.
pub fn replication_task_info_from_dlq(info: &DlqTaskInfo) -> ReplicationTaskInfo {
    ReplicationTaskInfo {
        domain_id: info.domain_id.clone(),
        workflow_id: info.workflow_id.clone(),
        run_id: info.run_id.clone(),
        task_type: info.task_type,
        task_id: info.task_id,
        version: info.version,
        first_event_id: info.first_event_id,
        next_event_id: info.next_event_id,
        scheduled_id: info.scheduled_id,
    }
}

/// `common.Daemon` (Start/Stop lifecycle).
pub trait Daemon: Send + Sync {
    fn start(&self);
    fn stop(&self);
}

/// Minimal `quotas.NewDynamicRateLimiter` stand-in: spaces `wait()` calls
/// at `1/qps` seconds (a qps <= 0 or non-finite value means unlimited).
pub struct SimpleRateLimiter {
    interval: Option<Duration>,
    next: tokio::sync::Mutex<tokio::time::Instant>,
}

impl SimpleRateLimiter {
    pub fn new(qps: f64) -> Self {
        let interval = if qps.is_finite() && qps > 0.0 {
            Some(Duration::from_secs_f64(1.0 / qps))
        } else {
            None
        };
        Self { interval, next: tokio::sync::Mutex::new(tokio::time::Instant::now()) }
    }
}

#[async_trait]
impl Limiter for SimpleRateLimiter {
    async fn wait(&self) {
        let interval = match self.interval {
            Some(i) => i,
            None => return,
        };
        let at = {
            let mut next = self.next.lock().await;
            let now = tokio::time::Instant::now();
            let at = if *next > now { *next } else { now };
            *next = at + interval;
            at
        };
        tokio::time::sleep_until(at).await;
    }
}
