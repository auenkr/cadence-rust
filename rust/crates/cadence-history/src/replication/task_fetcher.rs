//! Port of `service/history/replication/task_fetcher.go`.
use cadence_api::admin::{GetReplicationMessagesRequest, ReplicationMessages, ReplicationToken};
use cadence_common::jitter::jit_duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::deps::{
    AdminClient, ClientBean, Daemon, Limiter, ReplicationConfig, ReplicationError,
    ReplicationMetrics, Result, SimpleRateLimiter,
};

const FETCH_TASK_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_CHAN_BUFFER_SIZE: usize = 1000;
const SCOPE: &str = "ReplicationTaskFetcher";

const STATUS_INITIALIZED: u8 = 0;
const STATUS_STARTED: u8 = 1;
const STATUS_STOPPED: u8 = 2;

/// `request`: a shard's token plus the channel its answer is delivered on.
/// Dropping the sender closes the response channel without a value.
pub struct Request {
    pub token: ReplicationToken,
    pub resp_chan: oneshot::Sender<ReplicationMessages>,
}

/// `TaskFetcher`.
pub trait TaskFetcher: Daemon {
    fn get_source_cluster(&self) -> &str;
    fn get_request_chan(&self, shard_id: usize) -> mpsc::Sender<Request>;
    fn get_rate_limiter(&self) -> Arc<dyn Limiter>;
}

/// `TaskFetchers`.
pub trait TaskFetchers: Daemon {
    fn get_fetchers(&self) -> Vec<Arc<dyn TaskFetcher>>;
}

/// `taskFetchersImpl`.
pub struct TaskFetchersImpl {
    status: AtomicU8,
    fetchers: Vec<Arc<dyn TaskFetcher>>,
}

impl TaskFetchersImpl {
    /// `NewTaskFetchers`.
    pub fn new(
        config: Arc<dyn ReplicationConfig>,
        current_cluster: &str,
        remote_clusters: Vec<String>,
        client_bean: &dyn ClientBean,
        metrics: Arc<dyn ReplicationMetrics>,
    ) -> Result<Self> {
        let mut fetchers: Vec<Arc<dyn TaskFetcher>> = Vec::new();
        for cluster_name in remote_clusters {
            let remote = client_bean.remote_admin_client(&cluster_name)?;
            fetchers.push(TaskFetcherImpl::new(
                cluster_name,
                current_cluster.to_string(),
                config.clone(),
                remote,
                metrics.clone(),
            ));
        }
        Ok(Self { status: AtomicU8::new(STATUS_INITIALIZED), fetchers })
    }
}

impl Daemon for TaskFetchersImpl {
    fn start(&self) {
        if self
            .status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        for f in &self.fetchers {
            f.start();
        }
        tracing::info!(count = self.fetchers.len(), "Replication task fetchers started.");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        for f in &self.fetchers {
            f.stop();
        }
        tracing::info!(count = self.fetchers.len(), "Replication task fetchers stopped.");
    }
}

impl TaskFetchers for TaskFetchersImpl {
    fn get_fetchers(&self) -> Vec<Arc<dyn TaskFetcher>> {
        self.fetchers.clone()
    }
}

/// `taskFetcherImpl`.
pub struct TaskFetcherImpl {
    me: Weak<TaskFetcherImpl>,
    status: AtomicU8,
    current_cluster: String,
    source_cluster: String,
    config: Arc<dyn ReplicationConfig>,
    metrics: Arc<dyn ReplicationMetrics>,
    remote_peer: Arc<dyn AdminClient>,
    rate_limiter: Arc<dyn Limiter>,
    request_tx: Vec<mpsc::Sender<Request>>,
    request_rx: Mutex<Vec<Option<mpsc::Receiver<Request>>>>,
    cancel: CancellationToken,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TaskFetcherImpl {
    /// `newReplicationTaskFetcher`.
    pub fn new(
        source_cluster: String,
        current_cluster: String,
        config: Arc<dyn ReplicationConfig>,
        source_frontend: Arc<dyn AdminClient>,
        metrics: Arc<dyn ReplicationMetrics>,
    ) -> Arc<Self> {
        let parallelism = config.replication_task_fetcher_parallelism().max(1);
        let mut request_tx = Vec::with_capacity(parallelism);
        let mut request_rx = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let (tx, rx) = mpsc::channel(REQUEST_CHAN_BUFFER_SIZE);
            request_tx.push(tx);
            request_rx.push(Some(rx));
        }
        let rate_limiter: Arc<dyn Limiter> =
            Arc::new(SimpleRateLimiter::new(config.replication_task_processor_host_qps()));
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            status: AtomicU8::new(STATUS_INITIALIZED),
            current_cluster,
            source_cluster,
            config,
            metrics,
            remote_peer: source_frontend,
            rate_limiter,
            request_tx,
            request_rx: Mutex::new(request_rx),
            cancel: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
        })
    }

    /// `fetchTasks`.
    async fn fetch_tasks(self: Arc<Self>, mut rx: mpsc::Receiver<Request>) {
        let jitter = |d: Duration| {
            jit_duration(
                &mut rand::thread_rng(),
                d,
                self.config.replication_task_fetcher_timer_jitter_coefficient(),
            )
        };
        let timer = tokio::time::sleep(jitter(self.config.replication_task_fetcher_aggregation_interval()));
        tokio::pin!(timer);

        let mut request_by_shard: HashMap<i32, Request> = HashMap::new();
        loop {
            tokio::select! {
                Some(request) = rx.recv() => {
                    let shard_id = request.token.shard_id;
                    if request_by_shard.contains_key(&shard_id) {
                        tracing::info!(shard_id, "Get replication task request already exist for shard.");
                        // dropping the old request closes its response channel.
                    }
                    request_by_shard.insert(shard_id, request);
                }
                _ = &mut timer => {
                    let next = match self.fetch_and_distribute_tasks(&mut request_by_shard).await {
                        Err(e) if e.is_service_busy() => self.config.replication_task_fetcher_service_busy_wait(),
                        Err(_) => jitter(self.config.replication_task_fetcher_error_retry_wait()),
                        Ok(()) => jitter(self.config.replication_task_fetcher_aggregation_interval()),
                    };
                    timer.as_mut().reset(tokio::time::Instant::now() + next);
                }
                _ = self.cancel.cancelled() => return,
            }
        }
    }

    /// `fetchAndDistributeTasks`.
    pub async fn fetch_and_distribute_tasks(
        &self,
        request_by_shard: &mut HashMap<i32, Request>,
    ) -> Result<()> {
        let start = Instant::now();
        let result = self.fetch_and_distribute_inner(request_by_shard).await;
        self.metrics.exponential_histogram(
            SCOPE,
            "exponential_replication_task_fetch_latency",
            &[("target_cluster", self.source_cluster.as_str())],
            start.elapsed(),
        );
        result
    }

    async fn fetch_and_distribute_inner(
        &self,
        request_by_shard: &mut HashMap<i32, Request>,
    ) -> Result<()> {
        if request_by_shard.is_empty() {
            tracing::debug!("Skip fetching as no processor is asking for tasks.");
            return Ok(());
        }
        let messages_by_shard = match self.get_messages(request_by_shard).await {
            Ok(m) => m,
            Err(err) => {
                if !err.is_service_busy() {
                    tracing::error!(error = %err, "Failed to get replication tasks");
                } else {
                    tracing::debug!("Failed to get replication tasks because service busy");
                }
                return Err(err);
            }
        };
        let total_tasks: usize = messages_by_shard.values().map(|m| m.replication_tasks.len()).sum();
        self.metrics.record_histogram_value(
            SCOPE,
            "replication_tasks_fetched_size",
            &[("target_cluster", self.source_cluster.as_str())],
            total_tasks as f64,
        );
        tracing::debug!(count = messages_by_shard.len(), "Successfully fetched replication tasks.");
        for (shard_id, tasks) in messages_by_shard {
            if let Some(request) = request_by_shard.remove(&shard_id) {
                // send then close (dropping the sender); a gone receiver is ignored.
                let _ = request.resp_chan.send(tasks);
            }
        }
        Ok(())
    }

    /// `getMessages`.
    pub(super) async fn get_messages(
        &self,
        request_by_shard: &HashMap<i32, Request>,
    ) -> Result<HashMap<i32, ReplicationMessages>> {
        let tokens: Vec<ReplicationToken> =
            request_by_shard.values().map(|r| r.token.clone()).collect();
        let request = GetReplicationMessagesRequest {
            tokens,
            cluster_name: self.current_cluster.clone(),
        };
        let response = tokio::select! {
            r = self.remote_peer.get_replication_messages(request, FETCH_TASK_REQUEST_TIMEOUT) => r?,
            _ = self.cancel.cancelled() => {
                return Err(ReplicationError::Other("context canceled".into()))
            }
        };
        Ok(response.messages_by_shard.unwrap_or_default())
    }
}

impl Daemon for TaskFetcherImpl {
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
        let mut handles = self.tasks.lock().unwrap();
        let mut rxs = self.request_rx.lock().unwrap();
        for rx in rxs.iter_mut() {
            if let Some(rx) = rx.take() {
                handles.push(tokio::spawn(me.clone().fetch_tasks(rx)));
            }
        }
        tracing::info!(
            cluster = %self.source_cluster,
            count = self.request_tx.len(),
            "Replication task fetcher started."
        );
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.cancel.cancel();
        // Go waits up to 10s for the goroutines; the tasks exit on cancellation.
        let handles: Vec<_> = self.tasks.lock().unwrap().drain(..).collect();
        tokio::spawn(async move {
            let all = futures_util::future::join_all(handles);
            if tokio::time::timeout(Duration::from_secs(10), all).await.is_err() {
                tracing::warn!("Replication task fetcher timed out on shutdown.");
            } else {
                tracing::info!("Replication task fetcher graceful shutdown completed.");
            }
        });
    }
}

impl TaskFetcher for TaskFetcherImpl {
    fn get_source_cluster(&self) -> &str {
        &self.source_cluster
    }

    fn get_request_chan(&self, shard_id: usize) -> mpsc::Sender<Request> {
        self.request_tx[shard_id % self.request_tx.len()].clone()
    }

    fn get_rate_limiter(&self) -> Arc<dyn Limiter> {
        self.rate_limiter.clone()
    }
}
