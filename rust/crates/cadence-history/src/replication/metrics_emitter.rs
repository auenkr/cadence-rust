//! Port of `service/history/replication/metrics_emitter.go`.
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::deps::{Daemon, ReplicationMetrics, Result, ShardContext};
use super::task_reader::TaskReaderApi;

const METRICS_EMISSION_INTERVAL: Duration = Duration::from_secs(60);
const SCOPE: &str = "ReplicationMetricEmitter";

const STATUS_INITIALIZED: u8 = 0;
const STATUS_STARTED: u8 = 1;
const STATUS_STOPPED: u8 = 2;

/// `MetricsEmitterImpl`: emits source side replication metrics occasionally.
pub struct MetricsEmitterImpl {
    me: Weak<MetricsEmitterImpl>,
    shard_id: i32,
    current_cluster: String,
    remote_clusters: Vec<String>,
    shard: Arc<dyn ShardContext>,
    reader: Arc<dyn TaskReaderApi>,
    metrics: Arc<dyn ReplicationMetrics>,
    status: AtomicU8,
    interval: Duration,
    cancel: CancellationToken,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MetricsEmitterImpl {
    /// `NewMetricsEmitter`.
    pub fn new(
        shard_id: i32,
        shard: Arc<dyn ShardContext>,
        reader: Arc<dyn TaskReaderApi>,
        metrics: Arc<dyn ReplicationMetrics>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            shard_id,
            current_cluster: shard.current_cluster_name(),
            remote_clusters: shard.remote_cluster_names(),
            shard,
            reader,
            metrics,
            status: AtomicU8::new(STATUS_INITIALIZED),
            interval: METRICS_EMISSION_INTERVAL,
            cancel: CancellationToken::new(),
            handle: Mutex::new(None),
        })
    }

    async fn emit_metrics_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.interval,
            self.interval,
        );
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = ticker.tick() => self.emit_metrics().await,
            }
        }
    }

    /// `emitMetrics`.
    async fn emit_metrics(&self) {
        let instance = self.shard_id.to_string();
        for remote in &self.remote_clusters {
            let latency = match self.determine_replication_latency(remote).await {
                Ok(l) => l,
                Err(_) => return,
            };
            self.metrics.update_gauge(
                SCOPE,
                "replication_latency",
                &[
                    ("active_cluster", self.current_cluster.as_str()),
                    ("instance", instance.as_str()),
                    ("target_cluster", remote.as_str()),
                ],
                latency.as_nanos() as f64,
            );
            tracing::debug!(
                remote_cluster = %remote,
                "ReplicationLatency metric emitted: {}",
                latency.as_nanos() as f64
            );
        }
    }

    /// `determineReplicationLatency`.
    pub(super) async fn determine_replication_latency(&self, remote_cluster: &str) -> Result<Duration> {
        let last_read_task_id = self.shard.get_queue_cluster_ack_level(remote_cluster);
        let (tasks, _) = match self
            .reader
            .read(last_read_task_id, last_read_task_id + 1, 1)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    remote_cluster = %remote_cluster,
                    error = %e,
                    "Error reading when determining replication latency, lastReadTaskID={}",
                    last_read_task_id
                );
                return Err(e);
            }
        };
        tracing::debug!(remote_cluster = %remote_cluster, number = tasks.len(), "Number of tasks retrieved");
        let mut latency = Duration::ZERO;
        if let Some(first) = tasks.first() {
            let creation = first.info().visibility_timestamp_nanos;
            let now = self.shard.time_source().now_nanos();
            latency = Duration::from_nanos(now.saturating_sub(creation).max(0) as u64);
        }
        Ok(latency)
    }
}

impl Daemon for MetricsEmitterImpl {
    fn start(&self) {
        if self
            .status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if let Some(me) = self.me.upgrade() {
            *self.handle.lock().unwrap() = Some(tokio::spawn(me.emit_metrics_loop()));
        }
        tracing::info!(shard_id = self.shard_id, "ReplicationMetricsEmitter started.");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tracing::info!("ReplicationMetricsEmitter shutting down.");
        self.cancel.cancel();
        if let Some(h) = self.handle.lock().unwrap().take() {
            tokio::spawn(async move {
                if tokio::time::timeout(Duration::from_secs(5), h).await.is_err() {
                    tracing::warn!("ReplicationMetricsEmitter timed out on shutdown.");
                }
            });
        }
    }
}
