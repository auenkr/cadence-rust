//! Port of `service/history/replication/task_processor.go`.
use cadence_api::admin::{ReplicationMessages, ReplicationToken, SyncShardStatus};
use cadence_api::enums::ReplicationTaskType;
use cadence_api::types::ReplicationTask;
use cadence_common::backoff::{ExponentialRetryPolicy, Retrier, NO_INTERVAL};
use cadence_common::jitter::jit_duration;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::deps::{
    throttle_retry, Daemon, DlqTaskInfo, Engine, Limiter, MultiPhasesRetryPolicy,
    PayloadSerializer, PutReplicationTaskToDlqRequest, ReplicationConfig, ReplicationError,
    ReplicationMetrics, Result, RetryPolicy, ShardContext, SimpleRateLimiter,
    SyncShardStatusRequest, EMPTY_MESSAGE_ID, REPLICATION_TASK_TYPE_HISTORY,
    REPLICATION_TASK_TYPE_SYNC_ACTIVITY,
};
use super::task_executor::TaskExecutor;
use super::task_fetcher::{Request, TaskFetcher};
use super::REPLICATION_TIMEOUT;

const DROP_SYNC_SHARD_TASK_TIME_THRESHOLD: Duration = Duration::from_secs(10 * 60);
const DLQ_ERROR_RETRY_WAIT: Duration = Duration::from_secs(1);
#[allow(dead_code)]
const DLQ_METRICS_EMIT_TIMER_INTERVAL: Duration = Duration::from_secs(5 * 60);
#[allow(dead_code)]
const DLQ_METRICS_EMIT_TIMER_COEFFICIENT: f64 = 0.05;

const STATUS_INITIALIZED: u8 = 0;
const STATUS_STARTED: u8 = 1;
const STATUS_STOPPED: u8 = 2;

const FETCHER_SCOPE: &str = "ReplicationTaskFetcher";

/// `TaskProcessor`.
pub trait TaskProcessor: Daemon {}

/// `common.CreateReplicationServiceBusyRetryPolicy`.
fn create_replication_service_busy_retry_policy() -> ExponentialRetryPolicy {
    ExponentialRetryPolicy::new(Duration::from_secs(2))
        .with_maximum_interval(Duration::from_secs(10))
        .with_expiration_interval(Duration::from_secs(5 * 60))
}

/// `isTransientRetryableError`.
pub fn is_transient_retryable_error(err: &ReplicationError) -> bool {
    !matches!(
        err,
        ReplicationError::BadRequest(_) | ReplicationError::ServiceBusy(_)
    )
}

/// `taskProcessorImpl`.
pub struct TaskProcessorImpl {
    me: std::sync::Weak<TaskProcessorImpl>,
    current_cluster: String,
    source_cluster: String,
    status: AtomicU8,
    shard: Arc<dyn ShardContext>,
    history_engine: Arc<dyn Engine>,
    history_serializer: Arc<dyn PayloadSerializer>,
    config: Arc<dyn ReplicationConfig>,
    metrics: Arc<dyn ReplicationMetrics>,
    task_executor: Arc<dyn TaskExecutor>,
    host_rate_limiter: Arc<dyn Limiter>,
    shard_rate_limiter: Arc<dyn Limiter>,

    task_retry_policy: MultiPhasesRetryPolicy,
    dlq_retry_policy: ExponentialRetryPolicy,
    no_task_retrier: Mutex<Retrier>,

    last_processed_message_id: AtomicI64,
    last_retrieved_message_id: AtomicI64,

    request_chan: mpsc::Sender<Request>,
    sync_shard_tx: mpsc::Sender<Option<SyncShardStatus>>,
    sync_shard_rx: Mutex<Option<mpsc::Receiver<Option<SyncShardStatus>>>>,
    done: CancellationToken,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TaskProcessorImpl {
    /// `NewTaskProcessor`.
    pub fn new(
        shard: Arc<dyn ShardContext>,
        history_engine: Arc<dyn Engine>,
        config: Arc<dyn ReplicationConfig>,
        metrics: Arc<dyn ReplicationMetrics>,
        task_fetcher: &dyn TaskFetcher,
        task_executor: Arc<dyn TaskExecutor>,
        history_serializer: Arc<dyn PayloadSerializer>,
    ) -> Arc<Self> {
        let shard_id = shard.shard_id();
        let source_cluster = task_fetcher.get_source_cluster().to_string();
        let first_retry_policy = ExponentialRetryPolicy::new(
            config.replication_task_processor_error_retry_wait(shard_id),
        )
        .with_maximum_attempts(config.replication_task_processor_error_retry_max_attempts(shard_id));
        let second_retry_policy = ExponentialRetryPolicy::new(
            config.replication_task_processor_error_second_retry_wait(shard_id),
        )
        .with_maximum_interval(config.replication_task_processor_error_second_retry_max_wait(shard_id))
        .with_expiration_interval(
            config.replication_task_processor_error_second_retry_expiration(shard_id),
        );
        let first_max = config.replication_task_processor_error_retry_max_attempts(shard_id);
        let task_retry_policy =
            MultiPhasesRetryPolicy::new(vec![(first_retry_policy, first_max), (second_retry_policy, 0)]);

        let dlq_retry_policy =
            ExponentialRetryPolicy::new(DLQ_ERROR_RETRY_WAIT).with_expiration_interval(NO_INTERVAL);

        let no_task_backoff_policy =
            ExponentialRetryPolicy::new(config.replication_task_processor_no_task_retry_wait(shard_id))
                .with_backoff_coefficient(1.0)
                .with_expiration_interval(NO_INTERVAL);
        let no_task_retrier = Retrier::new(no_task_backoff_policy);

        let (sync_shard_tx, sync_shard_rx) = mpsc::channel(1);
        let shard_rate_limiter: Arc<dyn Limiter> =
            Arc::new(SimpleRateLimiter::new(config.replication_task_processor_shard_qps()));
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            current_cluster: shard.current_cluster_name(),
            source_cluster,
            status: AtomicU8::new(STATUS_INITIALIZED),
            shard,
            history_engine,
            history_serializer,
            config,
            metrics,
            task_executor,
            host_rate_limiter: task_fetcher.get_rate_limiter(),
            shard_rate_limiter,
            task_retry_policy,
            dlq_retry_policy,
            no_task_retrier: Mutex::new(no_task_retrier),
            last_processed_message_id: AtomicI64::new(EMPTY_MESSAGE_ID),
            last_retrieved_message_id: AtomicI64::new(EMPTY_MESSAGE_ID),
            request_chan: task_fetcher.get_request_chan(shard_id as usize),
            sync_shard_tx,
            sync_shard_rx: Mutex::new(Some(sync_shard_rx)),
            done: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
        })
    }

    fn shard_id(&self) -> i32 {
        self.shard.shard_id()
    }

    fn is_shutting_down(&self) -> bool {
        self.done.is_cancelled()
    }

    /// `processorLoop`.
    async fn processor_loop(self: Arc<Self>) {
        loop {
            if self.is_shutting_down() {
                break;
            }
            let (resp_tx, resp_rx) = oneshot::channel::<ReplicationMessages>();
            let request = Request {
                token: ReplicationToken {
                    shard_id: self.shard_id(),
                    last_retrieved_message_id: self.last_retrieved_message_id.load(Ordering::SeqCst),
                    last_processed_message_id: self.last_processed_message_id.load(Ordering::SeqCst),
                },
                resp_chan: resp_tx,
            };
            tokio::select! {
                _ = self.done.cancelled() => break,
                r = self.request_chan.send(request) => {
                    if r.is_err() {
                        break;
                    }
                }
            }

            tokio::select! {
                response = resp_rx => {
                    let response = match response {
                        Ok(r) => r,
                        Err(_) => {
                            tracing::debug!("Fetch replication messages chan closed.");
                            continue;
                        }
                    };
                    tracing::debug!(
                        read_level = response.last_retrieved_message_id,
                        has_more = response.has_more,
                        count = response.replication_tasks.len(),
                        "Got fetch replication messages response."
                    );
                    self.task_processing_start_wait(&response.replication_tasks).await;
                    self.process_response(response).await;
                }
                _ = self.done.cancelled() => break,
            }
        }
        tracing::debug!(
            read_level = self.last_retrieved_message_id.load(Ordering::SeqCst),
            "Closing replication task processor."
        );
    }

    /// `cleanupReplicationTaskLoop`.
    async fn cleanup_replication_task_loop(self: Arc<Self>) {
        let shard_id = self.shard_id();
        let next = |s: &Self| {
            jit_duration(
                &mut rand::thread_rng(),
                s.config.replication_task_processor_cleanup_interval(shard_id),
                s.config.replication_task_processor_cleanup_jitter_coefficient(shard_id),
            )
        };
        let timer = tokio::time::sleep(next(&self));
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = self.done.cancelled() => return,
                _ = &mut timer => {
                    if let Err(e) = self.cleanup_acked_replication_tasks().await {
                        tracing::error!(error = %e, "Failed to clean up replication messages.");
                        self.metrics.inc_counter("ReplicationTaskCleanup", "replication_task_cleanup_failure", &[]);
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + next(&self));
                }
            }
        }
    }

    /// `cleanupAckedReplicationTasks`.
    async fn cleanup_acked_replication_tasks(&self) -> Result<()> {
        let mut min_ack_level = i64::MAX;
        for cluster_name in self.shard.remote_cluster_names() {
            let ack_level = self.shard.get_queue_cluster_ack_level(&cluster_name);
            if ack_level < min_ack_level {
                min_ack_level = ack_level;
            }
        }
        tracing::debug!(read_level = min_ack_level, "Cleaning up replication task queue.");
        self.metrics.inc_counter("ReplicationTaskCleanup", "replication_task_cleanup_count", &[]);
        let max_read_level = self
            .shard
            .update_if_needed_and_get_queue_max_read_level(&self.current_cluster);
        let lag_count = max_read_level.wrapping_sub(min_ack_level);
        let tags = [("target_cluster", self.current_cluster.as_str())];
        self.metrics.record_timer(
            FETCHER_SCOPE,
            "replication_tasks_lag",
            &tags,
            Duration::from_nanos(lag_count.max(0) as u64),
        );
        self.metrics
            .record_histogram_value(FETCHER_SCOPE, "replication_tasks_lag_histogram", &tags, lag_count as f64);
        self.metrics
            .update_gauge(FETCHER_SCOPE, "replication_tasks_lag_gauge", &tags, lag_count as f64);
        loop {
            let page_size = self.config.replicator_task_delete_batch_size();
            let completed = self
                .shard
                .execution_manager()
                .range_complete_replication_task(self.shard_id(), min_ack_level.wrapping_add(1), page_size)
                .await?;
            // `persistence.HasMoreRowsToDelete`
            let has_more = page_size > 0 && completed >= page_size;
            if !has_more {
                break;
            }
        }
        Ok(())
    }

    /// `processResponse`.
    async fn process_response(&self, response: ReplicationMessages) {
        // non-blocking hand-off of the sync shard status (drops it if the loop is busy)
        let _ = self.sync_shard_tx.try_send(response.sync_shard_status.clone());

        let tags = [("target_cluster", self.source_cluster.as_str())];
        let batch_request_start = Instant::now();
        for replication_task in &response.replication_tasks {
            let is_failover_marker =
                replication_task.task_type == Some(ReplicationTaskType::FailoverMarker);
            if !is_failover_marker {
                self.host_rate_limiter.wait().await;
                self.shard_rate_limiter.wait().await;
            }
            if self.process_single_task(replication_task).await.is_err() {
                return;
            }
        }

        if response.replication_tasks.is_empty() {
            let backoff = self
                .no_task_retrier
                .lock()
                .unwrap()
                .next_backoff(&mut rand::thread_rng())
                .unwrap_or_default();
            tokio::time::sleep(backoff).await;
        } else {
            let applied_latency = batch_request_start.elapsed();
            self.metrics
                .record_timer(FETCHER_SCOPE, "replication_tasks_applied_latency", &tags, applied_latency);
            self.metrics.exponential_histogram(
                FETCHER_SCOPE,
                "replication_tasks_applied_latency_histogram",
                &tags,
                applied_latency,
            );
        }

        if self.is_shutting_down() {
            return;
        }

        self.last_processed_message_id
            .store(response.last_retrieved_message_id, Ordering::SeqCst);
        self.last_retrieved_message_id
            .store(response.last_retrieved_message_id, Ordering::SeqCst);
        self.metrics.update_gauge(
            FETCHER_SCOPE,
            "last_retrieved_message_id",
            &tags,
            response.last_retrieved_message_id as f64,
        );
        self.no_task_retrier.lock().unwrap().reset();
    }

    /// `syncShardStatusLoop`.
    async fn sync_shard_status_loop(self: Arc<Self>, mut rx: mpsc::Receiver<Option<SyncShardStatus>>) {
        let next = |s: &Self| {
            jit_duration(
                &mut rand::thread_rng(),
                s.config.shard_sync_min_interval(),
                s.config.shard_sync_timer_jitter_coefficient(),
            )
        };
        let timer = tokio::time::sleep(next(&self));
        tokio::pin!(timer);
        let mut sync_shard_task: Option<SyncShardStatus> = None;
        loop {
            tokio::select! {
                Some(req) = rx.recv() => {
                    sync_shard_task = req;
                }
                _ = &mut timer => {
                    if let Err(e) = self.handle_sync_shard_status(sync_shard_task.as_ref()).await {
                        tracing::error!(error = %e, "failed to sync shard status");
                        self.metrics.inc_counter("HistorySyncShardStatus", "sync_shard_from_remote_failure", &[]);
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + next(&self));
                }
                _ = self.done.cancelled() => return,
            }
        }
    }

    /// `handleSyncShardStatus`.
    async fn handle_sync_shard_status(&self, status: Option<&SyncShardStatus>) -> Result<()> {
        let status = match status {
            Some(s) => s,
            None => return Ok(()),
        };
        let now = self.shard.time_source().now_nanos();
        let age = now.saturating_sub(status.timestamp.unwrap_or_default());
        if age > DROP_SYNC_SHARD_TASK_TIME_THRESHOLD.as_nanos() as i64 {
            return Ok(());
        }
        self.metrics.inc_counter("HistorySyncShardStatus", "sync_shard_from_remote_counter", &[]);
        self.history_engine
            .sync_shard_status(
                SyncShardStatusRequest {
                    source_cluster: self.source_cluster.clone(),
                    shard_id: self.shard_id() as i64,
                    timestamp: status.timestamp,
                },
                REPLICATION_TIMEOUT,
            )
            .await
    }

    /// `processSingleTask`.
    async fn process_single_task(&self, replication_task: &ReplicationTask) -> Result<()> {
        let busy_policy = create_replication_service_busy_retry_policy();
        let result = throttle_retry(
            &busy_policy as &dyn RetryPolicy,
            |e| e.is_service_busy(),
            || async {
                throttle_retry(
                    &self.task_retry_policy as &dyn RetryPolicy,
                    is_transient_retryable_error,
                    || async {
                        if self.is_shutting_down() {
                            Ok(())
                        } else {
                            self.process_task_once(replication_task).await
                        }
                    },
                )
                .await
            },
        )
        .await;

        let err = match result {
            Ok(()) => return Ok(()),
            Err(e) if e.is_service_busy() => return Err(e),
            Err(ReplicationError::MissingVersionHistories) => {
                tracing::warn!("Encounter workflow without version histories");
                return Ok(());
            }
            Err(e) => e,
        };

        if self.is_shutting_down() {
            tracing::warn!(error = %err, "Skip adding new messages to DLQ.");
            return Err(err);
        }
        let request = match self.generate_dlq_request(replication_task) {
            Ok(r) => r,
            Err(err2) => {
                tracing::error!(error = %err2, "Failed to generate DLQ replication task.");
                return Ok(());
            }
        };
        tracing::error!(
            domain_id = %request.task_info.domain_id,
            workflow_id = %request.task_info.workflow_id,
            run_id = %request.task_info.run_id,
            task_id = request.task_info.task_id,
            task_type = request.task_info.task_type,
            error = %err,
            "Failed to apply replication task after retry. Putting task into DLQ."
        );
        self.put_replication_task_to_dlq(request).await
    }

    /// `processTaskOnce`.
    async fn process_task_once(&self, replication_task: &ReplicationTask) -> Result<()> {
        let ts = self.shard.time_source();
        let start_time = ts.now_nanos();
        let (scope, result) = self.task_executor.execute(replication_task, false).await;

        if let Err(err) = &result {
            self.update_failure_metric(scope, err, self.shard_id());
            return result;
        }

        let now = ts.now_nanos();
        let domain_id = match replication_task.task_type {
            Some(ReplicationTaskType::HistoryV2) => replication_task
                .history_task_v2_attributes
                .as_ref()
                .and_then(|a| a.domain_id.clone()),
            Some(ReplicationTaskType::SyncActivity) => replication_task
                .sync_activity_task_attributes
                .as_ref()
                .and_then(|a| a.domain_id.clone()),
            Some(ReplicationTaskType::FailoverMarker) => replication_task
                .failover_marker_attributes
                .as_ref()
                .and_then(|a| a.domain_id.clone()),
            _ => None,
        }
        .unwrap_or_default();
        let domain_name = if !domain_id.is_empty() {
            self.shard.domain_cache().get_domain_name(&domain_id)?
        } else {
            String::new()
        };
        let tags = [
            ("target_cluster", self.source_cluster.as_str()),
            ("domain", domain_name.as_str()),
        ];
        let processing = Duration::from_nanos(now.saturating_sub(start_time).max(0) as u64);
        let e2e = Duration::from_nanos(
            now.saturating_sub(replication_task.creation_time.unwrap_or_default()).max(0) as u64,
        );
        self.metrics.exponential_histogram(scope, "task_processing_latency_histogram", &tags, processing);
        self.metrics.exponential_histogram(scope, "exponential_replication_task_latency", &tags, e2e);
        self.metrics.record_timer(scope, "task_processing_latency", &tags, processing);
        self.metrics.record_timer(scope, "replication_task_latency", &tags, e2e);

        let threshold = self.config.replication_task_processor_latency_log_threshold();
        if !threshold.is_zero() && e2e >= threshold {
            let (workflow_id, run_id) = match &replication_task.history_task_v2_attributes {
                Some(a) => (
                    a.workflow_id.clone().unwrap_or_default(),
                    a.run_id.clone().unwrap_or_default(),
                ),
                None => (String::new(), String::new()),
            };
            tracing::warn!(
                domain_id = %domain_id,
                workflow_id = %workflow_id,
                run_id = %run_id,
                shard_id = self.shard_id(),
                source_cluster = %self.source_cluster,
                "high replication latency"
            );
        }

        self.metrics.inc_counter(scope, "replication_tasks_applied_per_domain", &tags);
        let shard = self.shard_id().to_string();
        let shard_tags = [
            ("target_cluster", self.source_cluster.as_str()),
            ("instance", shard.as_str()),
        ];
        self.metrics.inc_counter(scope, "replication_tasks_applied", &shard_tags);
        Ok(())
    }

    /// `putReplicationTaskToDLQ`.
    async fn put_replication_task_to_dlq(&self, request: PutReplicationTaskToDlqRequest) -> Result<()> {
        let shard = self.shard_id().to_string();
        self.metrics.update_gauge(
            "ReplicationDLQStats",
            "replication_dlq_max_level_gauge",
            &[("target_cluster", self.source_cluster.as_str()), ("instance", shard.as_str())],
            request.task_info.task_id as f64,
        );
        throttle_retry(
            &self.dlq_retry_policy as &dyn RetryPolicy,
            |e| self.should_retry_dlq(e),
            || async {
                let res = self
                    .shard
                    .execution_manager()
                    .put_replication_task_to_dlq(request.clone())
                    .await;
                if let Err(e) = &res {
                    tracing::error!(error = %e, "Failed to put replication task to DLQ.");
                    self.metrics.inc_counter(FETCHER_SCOPE, "replication_dlq_failed", &[]);
                }
                res
            },
        )
        .await
    }

    /// `generateDLQRequest`.
    fn generate_dlq_request(
        &self,
        replication_task: &ReplicationTask,
    ) -> Result<PutReplicationTaskToDlqRequest> {
        match replication_task.task_type {
            Some(ReplicationTaskType::SyncActivity) => {
                let attrs = replication_task
                    .sync_activity_task_attributes
                    .clone()
                    .unwrap_or_default();
                let domain_id = attrs.domain_id.clone().unwrap_or_default();
                let domain_name = self.shard.domain_cache().get_domain_name(&domain_id)?;
                Ok(PutReplicationTaskToDlqRequest {
                    source_cluster_name: self.source_cluster.clone(),
                    task_info: DlqTaskInfo {
                        domain_id,
                        workflow_id: attrs.workflow_id.clone().unwrap_or_default(),
                        run_id: attrs.run_id.clone().unwrap_or_default(),
                        task_id: replication_task.source_task_id.unwrap_or_default(),
                        task_type: REPLICATION_TASK_TYPE_SYNC_ACTIVITY,
                        scheduled_id: attrs.scheduled_id.unwrap_or_default(),
                        ..Default::default()
                    },
                    task: replication_task.clone(),
                    domain_name,
                    shard_id: self.shard_id(),
                })
            }
            Some(ReplicationTaskType::HistoryV2) => {
                let attrs = replication_task
                    .history_task_v2_attributes
                    .clone()
                    .unwrap_or_default();
                let domain_id = attrs.domain_id.clone().unwrap_or_default();
                let domain_name = self.shard.domain_cache().get_domain_name(&domain_id)?;
                let blob = attrs.events.clone().unwrap_or_default();
                let events = self.history_serializer.deserialize_batch_events(&blob)?;
                if events.is_empty() {
                    tracing::error!("Empty events in a batch");
                    return Err(ReplicationError::Other(
                        "corrupted history event batch, empty events".into(),
                    ));
                }
                let (first_id, first_version) = events[0];
                let (last_id, _) = events[events.len() - 1];
                Ok(PutReplicationTaskToDlqRequest {
                    source_cluster_name: self.source_cluster.clone(),
                    task_info: DlqTaskInfo {
                        domain_id,
                        workflow_id: attrs.workflow_id.clone().unwrap_or_default(),
                        run_id: attrs.run_id.clone().unwrap_or_default(),
                        task_id: replication_task.source_task_id.unwrap_or_default(),
                        task_type: REPLICATION_TASK_TYPE_HISTORY,
                        first_event_id: first_id,
                        next_event_id: last_id + 1,
                        version: first_version,
                        ..Default::default()
                    },
                    task: replication_task.clone(),
                    domain_name,
                    shard_id: self.shard_id(),
                })
            }
            _ => Err(ReplicationError::Other("unknown replication task type".into())),
        }
    }

    /// `triggerDataInconsistencyScan`.
    #[allow(dead_code)]
    async fn trigger_data_inconsistency_scan(&self, replication_task: &ReplicationTask) -> Result<()> {
        let (failover_version, domain_id, workflow_id, run_id) =
            if let Some(attr) = &replication_task.history_task_v2_attributes {
                let items = attr.version_history_items.clone().unwrap_or_default();
                if items.is_empty() {
                    return Err(ReplicationError::Other(
                        "failed to trigger data scan due to invalid version history".into(),
                    ));
                }
                (
                    items[0].version,
                    attr.domain_id.clone().unwrap_or_default(),
                    attr.workflow_id.clone().unwrap_or_default(),
                    attr.run_id.clone().unwrap_or_default(),
                )
            } else if let Some(attr) = &replication_task.sync_activity_task_attributes {
                (
                    attr.version.unwrap_or_default(),
                    attr.domain_id.clone().unwrap_or_default(),
                    attr.workflow_id.clone().unwrap_or_default(),
                    attr.run_id.clone().unwrap_or_default(),
                )
            } else {
                return Ok(());
            };
        let cluster_name = self.shard.cluster_name_for_failover_version(failover_version)?;
        let client = self.shard.client_bean().remote_frontend_client(&cluster_name)?;
        let fix_execution = serde_json::json!({
            "DomainID": domain_id,
            "WorkflowID": workflow_id,
            "RunID": run_id,
            "ShardID": self.shard_id(),
        });
        let input = serde_json::to_vec(&fix_execution)
            .map_err(|e| ReplicationError::Other(e.to_string()))?;
        client.start_data_corruption_scan(input).await
    }

    /// `shouldRetryDLQ`.
    fn should_retry_dlq(&self, _err: &ReplicationError) -> bool {
        if self.is_shutting_down() {
            tracing::debug!("ReplicationTaskProcessor shutting down.");
            return false;
        }
        true
    }

    /// `updateFailureMetric`.
    fn update_failure_metric(&self, scope: &str, err: &ReplicationError, shard_id: i32) {
        let shard = shard_id.to_string();
        let tags = [("instance", shard.as_str())];
        self.metrics.inc_counter(scope, "replicator_failures", &tags);
        let name = match err {
            ReplicationError::ShardOwnershipLost(_) => "cadence_err_shard_ownership_lost_counter",
            ReplicationError::BadRequest(_) => "cadence_err_bad_request_counter",
            ReplicationError::DomainNotActive(_) => "cadence_err_domain_not_active_counter",
            ReplicationError::WorkflowExecutionAlreadyStarted(_) => {
                "cadence_err_execution_already_started_counter"
            }
            ReplicationError::EntityNotExists(_) => "cadence_err_entity_not_exists_counter",
            ReplicationError::WorkflowExecutionAlreadyCompleted(_) => {
                "cadence_err_workflow_execution_already_completed_counter"
            }
            ReplicationError::LimitExceeded(_) => "cadence_err_limit_exceeded_counter",
            ReplicationError::DeadlineExceeded(_) => "cadence_err_context_timeout_counter",
            _ => return,
        };
        self.metrics.inc_counter(scope, name, &tags);
    }

    /// `taskProcessingStartWait`.
    async fn task_processing_start_wait(&self, tasks: &[ReplicationTask]) {
        let shard_id = self.shard_id();
        let wait = self.config.replication_task_processor_start_wait(shard_id);
        if wait.is_zero() {
            return;
        }
        if tasks
            .iter()
            .any(|t| t.task_type == Some(ReplicationTaskType::FailoverMarker))
        {
            return;
        }
        let jittered = jit_duration(
            &mut rand::thread_rng(),
            wait,
            self.config.replication_task_processor_start_wait_jitter_coefficient(shard_id),
        );
        tokio::time::sleep(jittered).await;
    }
}

impl Daemon for TaskProcessorImpl {
    fn start(&self) {
        if self
            .status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let me = match self.me.upgrade() {
            Some(m) => m,
            None => return,
        };
        let mut tasks = self.tasks.lock().unwrap();
        tasks.push(tokio::spawn(me.clone().processor_loop()));
        if let Some(rx) = self.sync_shard_rx.lock().unwrap().take() {
            tasks.push(tokio::spawn(me.clone().sync_shard_status_loop(rx)));
        }
        tasks.push(tokio::spawn(me.cleanup_replication_task_loop()));
        tracing::info!(shard_id = self.shard_id(), source_cluster = %self.source_cluster, "ReplicationTaskProcessor started.");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::debug!("ReplicationTaskProcessor shutting down.");
        self.done.cancel();
        let handles: Vec<_> = self.tasks.lock().unwrap().drain(..).collect();
        tokio::spawn(async move {
            let all = futures_util::future::join_all(handles);
            if tokio::time::timeout(Duration::from_secs(10), all).await.is_err() {
                tracing::warn!("ReplicationTaskProcessor timed out on shutdown.");
            } else {
                tracing::info!("ReplicationTaskProcessor shutdown completed");
            }
        });
    }
}

impl TaskProcessor for TaskProcessorImpl {}
