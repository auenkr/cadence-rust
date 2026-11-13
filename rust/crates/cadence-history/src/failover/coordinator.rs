//! Port of `go/service/history/failover/coordinator.go`.
use super::*;
use cadence_common::jitter::jit_duration;
use cadence_api::types::FailoverMarkerAttributes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const NOTIFICATION_CHAN_BUFFER_SIZE: usize = 1000;
pub const RECEIVE_CHAN_BUFFER_SIZE: usize = 1000;
pub const CLEANUP_MARKER_INTERVAL: Duration = Duration::from_secs(30 * 60);
pub const INVALID_MARKER_DURATION: Duration = Duration::from_secs(60 * 60);
pub const UPDATE_DOMAIN_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(50);
pub const UPDATE_DOMAIN_RETRY_COEFFICIENT: f64 = 2.0;
pub const UPDATE_DOMAIN_MAX_RETRY: usize = 2;
pub const NOTIFY_REMOTE_COORDINATOR_TIMEOUT: Duration = Duration::from_secs(5);

pub const DAEMON_STATUS_INITIALIZED: i32 = 0;
pub const DAEMON_STATUS_STARTED: i32 = 1;
pub const DAEMON_STATUS_STOPPED: i32 = 2;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum FailoverError {
    /// `errRecordNotFound` (`types.EntityNotExistsError`).
    #[error("Graceful failover record not found in shard coordinator")]
    EntityNotExists,
}

/// `failover.Coordinator`.
pub trait Coordinator: Send + Sync {
    fn start(self: Arc<Self>);
    fn stop(&self);
    fn notify_failover_markers(&self, shard_id: i32, markers: Vec<FailoverMarkerAttributes>);
    fn receive_failover_markers(&self, shard_ids: Vec<i32>, marker: FailoverMarkerAttributes);
    fn get_failover_info(&self, domain_id: &str) -> Result<GetFailoverInfoResponse, FailoverError>;
}

#[derive(Debug, Clone)]
struct NotificationRequest {
    shard_id: i32,
    markers: Vec<FailoverMarkerAttributes>,
}

#[derive(Debug, Clone)]
pub struct ReceiveRequest {
    pub shard_ids: Vec<i32>,
    pub marker: FailoverMarkerAttributes,
}

#[derive(Debug, Clone)]
struct FailoverRecord {
    failover_version: i64,
    shards: HashSet<i32>,
    last_updated_time: i64,
    first_seen_time: i64,
    first_marker_creation_time: i64,
}

pub struct NewCoordinatorParams {
    pub domain_manager: Arc<dyn DomainManager>,
    pub history_client: Arc<dyn HistoryClient>,
    pub time_source: Arc<dyn TimeSource>,
    pub domain_cache: Arc<dyn DomainCache>,
    pub config: Arc<dyn FailoverConfig>,
    pub metrics: Arc<dyn FailoverMetrics>,
}

pub struct CoordinatorImpl {
    status: AtomicI32,
    notification_tx: mpsc::Sender<NotificationRequest>,
    notification_rx: Mutex<Option<mpsc::Receiver<NotificationRequest>>>,
    receive_tx: mpsc::Sender<ReceiveRequest>,
    receive_rx: Mutex<Option<mpsc::Receiver<ReceiveRequest>>>,
    /// Go `shutdownChan` (closed on Stop; wakes every loop, including busy ones).
    shutdown: CancellationToken,
    recorder: tokio::sync::Mutex<HashMap<String, FailoverRecord>>,
    p: NewCoordinatorParams,
}

impl CoordinatorImpl {
    /// `NewCoordinator`. The Go retry policy (50ms, x2, max 2 attempts) is
    /// represented by the `UPDATE_DOMAIN_*` constants and applied by the
    /// `DomainManager::clean_pending_active_state` implementation.
    pub fn new(p: NewCoordinatorParams) -> Arc<Self> {
        let (ntx, nrx) = mpsc::channel(NOTIFICATION_CHAN_BUFFER_SIZE);
        let (rtx, rrx) = mpsc::channel(RECEIVE_CHAN_BUFFER_SIZE);
        Arc::new(Self {
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            notification_tx: ntx,
            notification_rx: Mutex::new(Some(nrx)),
            receive_tx: rtx,
            receive_rx: Mutex::new(Some(rrx)),
            shutdown: CancellationToken::new(),
            recorder: tokio::sync::Mutex::new(HashMap::new()),
            p,
        })
    }

    fn now(&self) -> i64 {
        self.p.time_source.now_nanos()
    }

    async fn receive_failover_markers_loop(self: Arc<Self>, mut rx: mpsc::Receiver<ReceiveRequest>) {
        let mut ticker = tokio::time::interval(CLEANUP_MARKER_INTERVAL);
        ticker.tick().await; // Go's ticker first fires after one interval.
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = ticker.tick() => self.cleanup_invalid_markers().await,
                Some(request) = rx.recv() => self.handle_failover_markers(request).await,
            }
        }
    }

    fn next_notify_interval(&self) -> Duration {
        jit_duration(
            &mut rand::thread_rng(),
            self.p.config.notify_failover_marker_interval(),
            self.p.config.notify_failover_marker_timer_jitter_coefficient(),
        )
    }

    async fn notify_failover_marker_loop(self: Arc<Self>, mut rx: mpsc::Receiver<NotificationRequest>) {
        let sleep = tokio::time::sleep(self.next_notify_interval());
        tokio::pin!(sleep);
        // Keyed by (domain_id, failover_version) like Go's FailoverMarkerAttributes mask.
        let mut request_by_marker: HashMap<(Option<String>, Option<i64>), ReceiveRequest> = HashMap::new();
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                Some(req) = rx.recv() => {
                    // duplicated shard IDs are fine; the receiver de-dups (handle_failover_markers).
                    aggregate_notification_requests(&req, &mut request_by_marker);
                }
                _ = &mut sleep => {
                    if self.notify_remote_coordinator(&request_by_marker).await.is_ok() {
                        request_by_marker = HashMap::new();
                    }
                    sleep.as_mut().reset(tokio::time::Instant::now() + self.next_notify_interval());
                }
            }
        }
    }

    pub async fn handle_failover_markers(&self, request: ReceiveRequest) {
        let mut recorder = self.recorder.lock().await;
        let marker = &request.marker;
        let domain_id = marker.domain_id.clone().unwrap_or_default();
        let marker_version = marker.failover_version.unwrap_or_default();
        if let Some(record) = recorder.get(&domain_id) {
            // local version older than marker: another failover happened, invalidate.
            if record.failover_version < marker_version {
                recorder.remove(&domain_id);
            } else if record.failover_version > marker_version {
                tracing::info!(
                    domain_id = %domain_id, local = record.failover_version, marker = marker_version,
                    "Dropped failover marker: local failover version is greater than marker version"
                );
                self.p.metrics.inc_counter(FailoverMetric::DroppedRegressedDomain, None);
                return;
            }
        }

        let now = self.now();
        let record = recorder.entry(domain_id.clone()).or_insert_with(|| FailoverRecord {
            failover_version: marker_version,
            shards: HashSet::new(),
            last_updated_time: 0,
            first_seen_time: now,
            first_marker_creation_time: 0,
        });
        record.last_updated_time = self.now();
        // skip non-positive values so a single bad marker can't poison the minimum
        let ct = marker.creation_time.unwrap_or_default();
        if ct > 0 && (record.first_marker_creation_time == 0 || ct < record.first_marker_creation_time) {
            record.first_marker_creation_time = ct;
        }
        for shard_id in &request.shard_ids {
            record.shards.insert(*shard_id);
        }

        let domain_name = match self.p.domain_cache.get_domain_name(&domain_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(domain_id = %domain_id, error = %e,
                    "Coordinator failed to get domain name while recording failover markers from request");
                self.p.metrics.inc_counter(FailoverMetric::CadenceFailures, Some(""));
                return;
            }
        };

        if record.shards.len() == self.p.config.number_of_shards() {
            let first_seen_time = record.first_seen_time;
            let first_marker_creation_time = record.first_marker_creation_time;
            let failover_version = record.failover_version;
            let clean_start = self.now();
            let result = self.p.domain_manager.clean_pending_active_state(&domain_id, failover_version).await;
            let clean_duration = nanos(self.now() - clean_start);
            let updated = match result {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(domain_id = %domain_id, error = %e,
                        "Coordinator failed to update domain after receiving failover markers from all shards");
                    self.p.metrics.inc_counter(FailoverMetric::CadenceFailures, None);
                    return;
                }
            };
            recorder.remove(&domain_id);
            // reset the gauge so it does not linger at the last partial count
            self.p.metrics.update_gauge(FailoverMetric::MarkerCount, Some(&domain_name), 0.0);
            if !updated {
                // another path already cleared pending-active; avoid misleading log/latency
                return;
            }
            let now = self.now();
            // earliest marker creation time across all shards for the full failover duration
            let failover_duration = nanos(now - first_marker_creation_time);
            self.p.metrics.record_timer(FailoverMetric::GracefulFailoverLatency, Some(&domain_name), failover_duration);
            self.p.metrics.record_timer(FailoverMetric::GracefulFailoverLatencyHistogram, Some(&domain_name), failover_duration);
            tracing::info!(
                domain_name = %domain_name, failover_version = ?marker.failover_version,
                failover_duration_seconds = failover_duration.as_secs_f64(),
                marker_pipeline_duration_seconds = nanos(now - first_seen_time).as_secs_f64(),
                clean_pending_active_duration_seconds = clean_duration.as_secs_f64(),
                "Updated domain from pending-active to active"
            );
        } else {
            self.p.metrics.update_gauge(
                FailoverMetric::MarkerCount, Some(&domain_name), record.shards.len() as f64);
        }
    }

    pub async fn cleanup_invalid_markers(&self) {
        let mut recorder = self.recorder.lock().await;
        let now = self.now();
        recorder.retain(|_, r| nanos(now - r.last_updated_time) <= INVALID_MARKER_DURATION);
    }

    async fn notify_remote_coordinator(
        &self,
        request_by_marker: &HashMap<(Option<String>, Option<i64>), ReceiveRequest>,
    ) -> Result<(), String> {
        if request_by_marker.is_empty() {
            return Ok(());
        }
        let tokens: Vec<FailoverMarkerToken> = request_by_marker
            .values()
            .map(|r| FailoverMarkerToken { shard_ids: r.shard_ids.clone(), failover_marker: r.marker.clone() })
            .collect();
        if let Err(e) = self.p.history_client.notify_failover_markers(tokens, NOTIFY_REMOTE_COORDINATOR_TIMEOUT).await {
            self.p.metrics.inc_counter(FailoverMetric::NotificationFailure, None);
            tracing::error!(error = %e, "Failed to notify failover markers");
            return Err(e);
        }
        Ok(())
    }
}

fn nanos(n: i64) -> Duration {
    Duration::from_nanos(n.max(0) as u64)
}

fn aggregate_notification_requests(
    request: &NotificationRequest,
    request_by_marker: &mut HashMap<(Option<String>, Option<i64>), ReceiveRequest>,
) {
    for marker in &request.markers {
        let mask = (marker.domain_id.clone(), marker.failover_version);
        let req = request_by_marker
            .entry(mask)
            .or_insert_with(|| ReceiveRequest { shard_ids: vec![], marker: marker.clone() });
        req.shard_ids.push(request.shard_id);
    }
}

impl Coordinator for CoordinatorImpl {
    fn start(self: Arc<Self>) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let rrx = self.receive_rx.lock().unwrap().take();
        let nrx = self.notification_rx.lock().unwrap().take();
        if let Some(rx) = rrx {
            tokio::spawn(self.clone().receive_failover_markers_loop(rx));
        }
        if let Some(rx) = nrx {
            tokio::spawn(self.clone().notify_failover_marker_loop(rx));
        }
        tracing::info!("Coordinator state changed: started");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        // close(shutdownChan) wakes both loops.
        self.shutdown.cancel();
        tracing::info!("Coordinator state changed: stopped");
    }

    fn notify_failover_markers(&self, shard_id: i32, markers: Vec<FailoverMarkerAttributes>) {
        tracing::info!(shard_id, markers = markers.len(), "failover-marker-coordinator-notify");
        // Go blocks on a full channel; here we await via a spawned send.
        let tx = self.notification_tx.clone();
        let req = NotificationRequest { shard_id, markers };
        if let Err(mpsc::error::TrySendError::Full(req)) = tx.try_send(req) {
            tokio::spawn(async move {
                let _ = tx.send(req).await;
            });
        }
    }

    fn receive_failover_markers(&self, shard_ids: Vec<i32>, marker: FailoverMarkerAttributes) {
        tracing::info!(?shard_ids, domain_id = ?marker.domain_id, "failover-marker-coordinator-receive");
        let tx = self.receive_tx.clone();
        let req = ReceiveRequest { shard_ids, marker };
        if let Err(mpsc::error::TrySendError::Full(req)) = tx.try_send(req) {
            tokio::spawn(async move {
                let _ = tx.send(req).await;
            });
        }
    }

    fn get_failover_info(&self, domain_id: &str) -> Result<GetFailoverInfoResponse, FailoverError> {
        // Blocking lock is fine: critical sections never await while holding it except
        // `handle_failover_markers`, which uses the async lock; use try_lock fallback.
        let recorder = loop {
            if let Ok(g) = self.recorder.try_lock() {
                break g;
            }
            std::thread::yield_now();
        };
        let record = recorder.get(domain_id).ok_or(FailoverError::EntityNotExists)?;
        let pending_shards = (0..self.p.config.number_of_shards() as i32)
            .filter(|i| !record.shards.contains(i))
            .collect();
        Ok(GetFailoverInfoResponse {
            completed_shard_count: record.shards.len() as i32,
            pending_shards,
        })
    }
}
