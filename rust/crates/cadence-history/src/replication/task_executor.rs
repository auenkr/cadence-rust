//! Port of `service/history/replication/task_executor.go`.
use async_trait::async_trait;
use cadence_api::enums::ReplicationTaskType;
use cadence_api::errors::RetryTaskV2Error;
use cadence_api::types::{ReplicationTask, WorkflowExecution};
use std::sync::Arc;
use std::time::Instant;

use super::deps::{
    DomainCache, Engine, HistoryResender, ReplicateEventsV2Request, ReplicationError,
    ReplicationMetrics, Result, ShardContext, SyncActivityRequest,
};
use super::REPLICATION_TIMEOUT;

/// Metric scope names used by the executor (`metrics.*Scope`).
pub const SYNC_ACTIVITY_TASK_SCOPE: &str = "SyncActivityTask";
pub const HISTORY_REPLICATION_V2_TASK_SCOPE: &str = "HistoryReplicationV2Task";
pub const FAILOVER_MARKER_SCOPE: &str = "FailoverMarker";
pub const REPLICATOR_SCOPE: &str = "Replicator";
const REREPLICATION_BY_ACTIVITY_SCOPE: &str = "HistoryRereplicationByActivityReplication";
const REREPLICATION_BY_HISTORY_SCOPE: &str = "HistoryRereplicationByHistoryReplication";

/// `TaskExecutor`.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Returns the metric scope together with the execution result.
    async fn execute(
        &self,
        replication_task: &ReplicationTask,
        force_apply: bool,
    ) -> (&'static str, Result<()>);
}

/// `taskExecutorImpl`.
pub struct TaskExecutorImpl {
    current_cluster: String,
    source_cluster: String,
    shard: Arc<dyn ShardContext>,
    num_shards: i32,
    domain_cache: Arc<dyn DomainCache>,
    history_resender: Arc<dyn HistoryResender>,
    history_engine: Arc<dyn Engine>,
    metrics: Arc<dyn ReplicationMetrics>,
}

impl TaskExecutorImpl {
    /// `NewTaskExecutor`. `num_shards` is `shard.GetConfig().NumberOfShards`.
    pub fn new(
        source_cluster: String,
        shard: Arc<dyn ShardContext>,
        num_shards: i32,
        domain_cache: Arc<dyn DomainCache>,
        history_resender: Arc<dyn HistoryResender>,
        history_engine: Arc<dyn Engine>,
        metrics: Arc<dyn ReplicationMetrics>,
    ) -> Self {
        Self {
            current_cluster: shard.current_cluster_name(),
            source_cluster,
            shard,
            num_shards,
            domain_cache,
            history_resender,
            history_engine,
            metrics,
        }
    }

    fn is_local_shard(&self, workflow_id: &str) -> bool {
        let n = self.num_shards.max(1) as u32;
        let target = (cadence_common::membership::fingerprint32(workflow_id.as_bytes()) % n) as i32;
        self.shard.shard_id() == target
    }

    async fn sync_activity_action(&self, local: bool, req: &SyncActivityRequest) -> Result<()> {
        if !local {
            self.shard
                .client_bean()
                .history_client()
                .sync_activity(req.clone(), REPLICATION_TIMEOUT)
                .await
        } else {
            self.history_engine.sync_activity(req.clone(), REPLICATION_TIMEOUT).await
        }
    }

    async fn history_replication_action(&self, local: bool, req: &ReplicateEventsV2Request) -> Result<()> {
        if !local {
            self.shard
                .client_bean()
                .history_client()
                .replicate_events_v2(req.clone(), REPLICATION_TIMEOUT)
                .await
        } else {
            self.history_engine
                .replicate_events_v2(req.clone(), REPLICATION_TIMEOUT)
                .await
        }
    }

    /// `handleActivityTask`.
    async fn handle_activity_task(&self, task: &ReplicationTask, force_apply: bool) -> Result<()> {
        let attr = task.sync_activity_task_attributes.clone().unwrap_or_default();
        let workflow_id = attr.workflow_id.clone().unwrap_or_default();
        if !self
            .filter_task(attr.domain_id.as_deref().unwrap_or_default(), force_apply)?
        {
            return Ok(());
        }

        let start = Instant::now();
        let result = self.handle_activity_task_inner(attr, &workflow_id).await;
        self.metrics
            .record_timer(SYNC_ACTIVITY_TASK_SCOPE, "cadence_latency", &[], start.elapsed());
        self.metrics.exponential_histogram(
            SYNC_ACTIVITY_TASK_SCOPE,
            "cadence_latency_histogram",
            &[],
            start.elapsed(),
        );
        result
    }

    async fn handle_activity_task_inner(
        &self,
        attr: cadence_api::types::SyncActivityTaskAttributes,
        workflow_id: &str,
    ) -> Result<()> {
        let request = SyncActivityRequest { task: attr };
        let local = self.is_local_shard(workflow_id);

        let err = match self.sync_activity_action(local, &request).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let retry_err = match to_retry_task_v2_error(&err) {
            Some(r) => r.clone(),
            None => return Err(err),
        };
        self.metrics
            .inc_counter(REREPLICATION_BY_ACTIVITY_SCOPE, "cadence_client_requests", &[]);
        let start = Instant::now();
        let resend = self.resend(&retry_err).await;
        self.metrics.record_timer(
            REREPLICATION_BY_ACTIVITY_SCOPE,
            "cadence_client_latency",
            &[],
            start.elapsed(),
        );
        self.metrics.exponential_histogram(
            REREPLICATION_BY_ACTIVITY_SCOPE,
            "cadence_client_latency_histogram",
            &[],
            start.elapsed(),
        );
        match resend {
            Ok(()) => {}
            Err(ReplicationError::SkipTask) => {
                tracing::error!(
                    domain_id = %retry_err.domain_id,
                    workflow_id = %retry_err.workflow_id,
                    run_id = %retry_err.run_id,
                    "skip replication sync activity task"
                );
                return Ok(());
            }
            Err(resend_err) => {
                tracing::error!(
                    domain_id = %retry_err.domain_id,
                    workflow_id = %retry_err.workflow_id,
                    run_id = %retry_err.run_id,
                    error = %resend_err,
                    "error resend history for sync activity"
                );
                return Err(err);
            }
        }
        self.sync_activity_action(local, &request).await
    }

    async fn resend(&self, retry_err: &RetryTaskV2Error) -> Result<()> {
        self.history_resender
            .send_single_workflow_history(
                &self.source_cluster,
                &retry_err.domain_id,
                &retry_err.workflow_id,
                &retry_err.run_id,
                retry_err.start_event_id,
                retry_err.start_event_version,
                retry_err.end_event_id,
                retry_err.end_event_version,
            )
            .await
    }

    /// `handleHistoryReplicationTaskV2`.
    async fn handle_history_replication_task_v2(
        &self,
        task: &ReplicationTask,
        force_apply: bool,
    ) -> Result<()> {
        let attr = task.history_task_v2_attributes.clone().unwrap_or_default();
        if !self
            .filter_task(attr.domain_id.as_deref().unwrap_or_default(), force_apply)?
        {
            return Ok(());
        }
        let start = Instant::now();
        let result = self.handle_history_replication_inner(attr).await;
        self.metrics.record_timer(
            HISTORY_REPLICATION_V2_TASK_SCOPE,
            "cadence_latency",
            &[],
            start.elapsed(),
        );
        self.metrics.exponential_histogram(
            HISTORY_REPLICATION_V2_TASK_SCOPE,
            "cadence_latency",
            &[],
            start.elapsed(),
        );
        result
    }

    async fn handle_history_replication_inner(
        &self,
        attr: cadence_api::types::HistoryTaskV2Attributes,
    ) -> Result<()> {
        let workflow_id = attr.workflow_id.clone().unwrap_or_default();
        let request = ReplicateEventsV2Request {
            domain_uuid: attr.domain_id.clone(),
            workflow_execution: WorkflowExecution {
                workflow_id: attr.workflow_id.clone().unwrap_or_default(),
                run_id: attr.run_id.clone().unwrap_or_default(),
            },
            version_history_items: attr.version_history_items.clone(),
            events: attr.events.clone(),
            new_run_events: attr.new_run_events.clone(),
        };
        let local = self.is_local_shard(&workflow_id);

        let err = match self.history_replication_action(local, &request).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let retry_err = match to_retry_task_v2_error(&err) {
            Some(r) => r.clone(),
            None => return Err(err),
        };
        self.metrics
            .inc_counter(REREPLICATION_BY_HISTORY_SCOPE, "cadence_client_requests", &[]);
        let start = Instant::now();
        let resend = self.resend(&retry_err).await;
        self.metrics.record_timer(
            REREPLICATION_BY_HISTORY_SCOPE,
            "cadence_client_latency",
            &[],
            start.elapsed(),
        );
        self.metrics.exponential_histogram(
            REREPLICATION_BY_HISTORY_SCOPE,
            "cadence_client_latency_histogram",
            &[],
            start.elapsed(),
        );
        match resend {
            Ok(()) => {}
            Err(ReplicationError::SkipTask) => {
                tracing::error!(
                    domain_id = %retry_err.domain_id,
                    workflow_id = %retry_err.workflow_id,
                    run_id = %retry_err.run_id,
                    "skip replication history task"
                );
                return Ok(());
            }
            Err(resend_err) => {
                tracing::error!(
                    domain_id = %retry_err.domain_id,
                    workflow_id = %retry_err.workflow_id,
                    run_id = %retry_err.run_id,
                    first_event_id = ?retry_err.start_event_id,
                    first_event_version = ?retry_err.start_event_version,
                    last_event_id = ?retry_err.end_event_id,
                    last_event_version = ?retry_err.end_event_version,
                    error = %resend_err,
                    "error resend history for history event v2"
                );
                return Err(err);
            }
        }
        self.history_replication_action(local, &request).await
    }

    /// `handleFailoverReplicationTask`.
    fn handle_failover_replication_task(&self, task: &ReplicationTask) -> Result<()> {
        let mut attrs = match &task.failover_marker_attributes {
            Some(a) => a.clone(),
            None => {
                tracing::error!("FailoverMarker replication task with nil attributes");
                return Err(ReplicationError::empty_failover_marker_attributes());
            }
        };
        attrs.creation_time = task.creation_time;
        self.shard.adding_pending_failover_marker(attrs)
    }

    /// `filterTask`.
    fn filter_task(&self, domain_id: &str, force_apply: bool) -> Result<bool> {
        if force_apply {
            return Ok(true);
        }
        let entry = self.domain_cache.get_domain_by_id(domain_id)?;
        Ok(entry.has_replication_cluster(&self.current_cluster))
    }
}

#[async_trait]
impl TaskExecutor for TaskExecutorImpl {
    async fn execute(
        &self,
        replication_task: &ReplicationTask,
        force_apply: bool,
    ) -> (&'static str, Result<()>) {
        match replication_task.task_type {
            Some(ReplicationTaskType::SyncActivity) => (
                SYNC_ACTIVITY_TASK_SCOPE,
                self.handle_activity_task(replication_task, force_apply).await,
            ),
            Some(ReplicationTaskType::HistoryV2) => (
                HISTORY_REPLICATION_V2_TASK_SCOPE,
                self.handle_history_replication_task_v2(replication_task, force_apply)
                    .await,
            ),
            Some(ReplicationTaskType::FailoverMarker) => (
                FAILOVER_MARKER_SCOPE,
                self.handle_failover_replication_task(replication_task),
            ),
            _ => {
                tracing::error!("Unknown task type.");
                (REPLICATOR_SCOPE, Err(ReplicationError::unknown_replication_task()))
            }
        }
    }
}

/// `toRetryTaskV2Error`.
pub fn to_retry_task_v2_error(err: &ReplicationError) -> Option<&RetryTaskV2Error> {
    match err {
        ReplicationError::RetryTaskV2(e) => Some(e),
        _ => None,
    }
}
