//! Port of `service/history/replication/dlq_handler.go`.
use async_trait::async_trait;
use cadence_api::admin::{GetDLQReplicationMessagesRequest, ReplicationTaskInfo};
use cadence_api::types::ReplicationTask;
use cadence_common::jitter::jit_duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::deps::{
    replication_task_info_from_dlq, Daemon, ReplicationError, ReplicationMetrics, Result,
    ShardContext,
};
use super::task_executor::TaskExecutor;

const DEFAULT_BEGINNING_MESSAGE_ID: i64 = -1;
const DLQ_METRICS_EMIT_TIMER_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DLQ_METRICS_EMIT_TIMER_COEFFICIENT: f64 = 0.05;
const DLQ_STATS_SCOPE: &str = "ReplicationDLQStats";

const STATUS_INITIALIZED: u8 = 0;
const STATUS_STARTED: u8 = 1;
const STATUS_STOPPED: u8 = 2;

/// `errInvalidCluster`.
fn err_invalid_cluster() -> ReplicationError {
    ReplicationError::BadRequest("Invalid target cluster name.".into())
}

/// `DLQHandler`.
#[async_trait]
pub trait DlqHandler: Daemon {
    async fn get_message_count(&self, force_fetch: bool) -> Result<HashMap<String, i64>>;
    async fn read_messages(
        &self,
        source_cluster: &str,
        last_message_id: i64,
        page_size: usize,
        page_token: Vec<u8>,
    ) -> Result<(Vec<Option<ReplicationTask>>, Vec<ReplicationTaskInfo>, Vec<u8>)>;
    async fn purge_messages(&self, source_cluster: &str, last_message_id: i64) -> Result<()>;
    async fn merge_messages(
        &self,
        source_cluster: &str,
        last_message_id: i64,
        page_size: usize,
        page_token: Vec<u8>,
    ) -> Result<Vec<u8>>;
}

/// `dlqHandlerImpl`.
pub struct DlqHandlerImpl {
    me: std::sync::Weak<DlqHandlerImpl>,
    task_executors: HashMap<String, Arc<dyn TaskExecutor>>,
    shard: Arc<dyn ShardContext>,
    metrics: Arc<dyn ReplicationMetrics>,
    done: CancellationToken,
    status: AtomicU8,
    latest_counts: Mutex<Option<HashMap<String, i64>>>,
}

impl DlqHandlerImpl {
    /// `NewDLQHandler`.
    pub fn new(
        shard: Arc<dyn ShardContext>,
        metrics: Arc<dyn ReplicationMetrics>,
        task_executors: HashMap<String, Arc<dyn TaskExecutor>>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            task_executors,
            shard,
            metrics,
            done: CancellationToken::new(),
            status: AtomicU8::new(STATUS_INITIALIZED),
            latest_counts: Mutex::new(None),
        })
    }

    async fn fetch_and_emit_message_count(&self) -> Result<()> {
        let shard_id = self.shard.shard_id();
        let shard_tag = shard_id.to_string();
        let mut result = HashMap::new();
        for source_cluster in self.task_executors.keys() {
            let size = match self
                .shard
                .execution_manager()
                .get_replication_dlq_size(shard_id, source_cluster)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(source_cluster = %source_cluster, error = %e, "failed to get replication DLQ size");
                    self.metrics
                        .inc_counter(DLQ_STATS_SCOPE, "replication_dlq_probe_failed", &[]);
                    return Err(e);
                }
            };
            self.metrics.update_gauge(
                DLQ_STATS_SCOPE,
                "replication_dlq_size",
                &[("source_cluster", source_cluster.as_str()), ("instance", shard_tag.as_str())],
                size as f64,
            );
            if size > 0 {
                result.insert(source_cluster.clone(), size);
            }
        }
        *self.latest_counts.lock().unwrap() = Some(result);
        Ok(())
    }

    async fn emit_dlq_size_metrics_loop(self: Arc<Self>) {
        let next = || {
            jit_duration(
                &mut rand::thread_rng(),
                DLQ_METRICS_EMIT_TIMER_INTERVAL,
                DLQ_METRICS_EMIT_TIMER_COEFFICIENT,
            )
        };
        let timer = tokio::time::sleep(next());
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = &mut timer => {
                    let _ = self.fetch_and_emit_message_count().await;
                    timer.as_mut().reset(tokio::time::Instant::now() + next());
                }
                _ = self.done.cancelled() => return,
            }
        }
    }

    /// `readMessagesWithAckLevel`.
    async fn read_messages_with_ack_level(
        &self,
        source_cluster: &str,
        last_message_id: i64,
        page_size: usize,
        page_token: Vec<u8>,
    ) -> Result<(Vec<Option<ReplicationTask>>, Vec<ReplicationTaskInfo>, Vec<u8>)> {
        let resp = self
            .shard
            .execution_manager()
            .get_replication_tasks_from_dlq(
                self.shard.shard_id(),
                source_cluster,
                DEFAULT_BEGINNING_MESSAGE_ID + 1,
                last_message_id + 1,
                page_size,
                page_token,
            )
            .await?;
        let (tasks, infos) = self.hydrate_dlq_tasks(source_cluster, resp.tasks).await?;
        Ok((tasks, infos, resp.next_page_token))
    }

    /// `hydrateDLQTasks`. Both returned vectors have equal length; an entry is
    /// `None` when the task could not be resolved.
    async fn hydrate_dlq_tasks(
        &self,
        source_cluster: &str,
        raw_tasks: Vec<super::deps::ReplicationDlqTask>,
    ) -> Result<(Vec<Option<ReplicationTask>>, Vec<ReplicationTaskInfo>)> {
        let mut hydrated: HashMap<i64, ReplicationTask> = HashMap::with_capacity(raw_tasks.len());
        let mut task_infos: Vec<ReplicationTaskInfo> = Vec::with_capacity(raw_tasks.len());
        let mut need_hydration: Vec<ReplicationTaskInfo> = Vec::new();

        for task in raw_tasks {
            let info = match task.info {
                Some(i) => i,
                None => {
                    return Err(ReplicationError::Other("nil task info in DLQ response".into()))
                }
            };
            let ti = replication_task_info_from_dlq(&info);
            task_infos.push(ti.clone());
            match task.task {
                Some(t) => {
                    hydrated.insert(info.task_id, t);
                }
                None => need_hydration.push(ti),
            }
        }

        if !need_hydration.is_empty() {
            let remote = self.shard.client_bean().remote_admin_client(source_cluster)?;
            let tasks = remote
                .get_dlq_replication_messages(GetDLQReplicationMessagesRequest {
                    task_infos: need_hydration,
                })
                .await?;
            for task in tasks {
                hydrated.insert(task.source_task_id.unwrap_or_default(), task);
            }
        }

        let mut replication_tasks = Vec::with_capacity(task_infos.len());
        for ti in &task_infos {
            let rt = hydrated.remove(&ti.task_id);
            if rt.is_none() {
                tracing::warn!(
                    domain_id = %ti.domain_id,
                    workflow_id = %ti.workflow_id,
                    run_id = %ti.run_id,
                    task_id = ti.task_id,
                    "replication task not found after hydration"
                );
            }
            replication_tasks.push(rt);
        }
        Ok((replication_tasks, task_infos))
    }
}

impl Daemon for DlqHandlerImpl {
    fn start(&self) {
        if self
            .status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if let Some(me) = self.me.upgrade() {
            tokio::spawn(me.emit_dlq_size_metrics_loop());
        }
        tracing::info!("DLQ handler started.");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::debug!("DLQ handler shutting down.");
        self.done.cancel();
    }
}

#[async_trait]
impl DlqHandler for DlqHandlerImpl {
    async fn get_message_count(&self, force_fetch: bool) -> Result<HashMap<String, i64>> {
        let missing = self.latest_counts.lock().unwrap().is_none();
        if force_fetch || missing {
            self.fetch_and_emit_message_count().await?;
        }
        Ok(self.latest_counts.lock().unwrap().clone().unwrap_or_default())
    }

    async fn read_messages(
        &self,
        source_cluster: &str,
        last_message_id: i64,
        page_size: usize,
        page_token: Vec<u8>,
    ) -> Result<(Vec<Option<ReplicationTask>>, Vec<ReplicationTaskInfo>, Vec<u8>)> {
        self.read_messages_with_ack_level(source_cluster, last_message_id, page_size, page_token)
            .await
    }

    async fn purge_messages(&self, source_cluster: &str, last_message_id: i64) -> Result<()> {
        self.shard
            .execution_manager()
            .range_delete_replication_task_from_dlq(
                self.shard.shard_id(),
                source_cluster,
                DEFAULT_BEGINNING_MESSAGE_ID + 1,
                last_message_id + 1,
            )
            .await
    }

    async fn merge_messages(
        &self,
        source_cluster: &str,
        last_message_id: i64,
        page_size: usize,
        page_token: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let executor = match self.task_executors.get(source_cluster) {
            Some(e) => e.clone(),
            None => return Err(err_invalid_cluster()),
        };
        let (tasks, raw_tasks, token) = self
            .read_messages_with_ack_level(source_cluster, last_message_id, page_size, page_token)
            .await?;

        let mut replication_tasks: HashMap<i64, ReplicationTask> = HashMap::new();
        for task in tasks.into_iter().flatten() {
            replication_tasks.insert(task.source_task_id.unwrap_or_default(), task);
        }

        let mut last_message_id = DEFAULT_BEGINNING_MESSAGE_ID;
        for raw in &raw_tasks {
            if let Some(task) = replication_tasks.get(&raw.task_id) {
                let (_, res) = executor.execute(task, true).await;
                res?;
            }
            // If hydrated replication task does not exist in remote cluster - continue merging
            // Record lastMessageID with raw task id, so that they can be purged after.
            if last_message_id < raw.task_id {
                last_message_id = raw.task_id;
            }
        }

        self.shard
            .execution_manager()
            .range_delete_replication_task_from_dlq(
                self.shard.shard_id(),
                source_cluster,
                DEFAULT_BEGINNING_MESSAGE_ID + 1,
                last_message_id + 1,
            )
            .await?;
        Ok(token)
    }
}
