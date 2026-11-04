//! Port of `go/service/history/failover/marker_notifier.go`.
use super::coordinator::{Coordinator, DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};
use super::*;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// `failover.MarkerNotifier` (a `common.Daemon`).
pub trait MarkerNotifier: Send + Sync {
    fn start(self: Arc<Self>);
    fn stop(&self);
}

pub struct MarkerNotifierImpl {
    status: AtomicI32,
    shutdown: Notify,
    shard: Arc<dyn ShardContext>,
    config: Arc<dyn FailoverConfig>,
    failover_coordinator: Arc<dyn Coordinator>,
    metrics: Arc<dyn FailoverMetrics>,
}

impl MarkerNotifierImpl {
    /// `NewMarkerNotifier`.
    pub fn new(
        shard: Arc<dyn ShardContext>,
        config: Arc<dyn FailoverConfig>,
        failover_coordinator: Arc<dyn Coordinator>,
        metrics: Arc<dyn FailoverMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
            shutdown: Notify::new(),
            shard,
            config,
            failover_coordinator,
            metrics,
        })
    }

    async fn notify_pending_failover_marker(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.config.notify_failover_marker_interval());
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = self.shutdown.notified() => return,
                _ = ticker.tick() => {
                    let markers = match self.shard.validate_and_update_failover_markers() {
                        Ok(m) => m,
                        Err(e) => {
                            self.metrics.inc_counter(FailoverMetric::UpdateShardFailure, None);
                            tracing::error!(error = %e, "Failed to update pending failover markers in shard info.");
                            Vec::new()
                        }
                    };
                    if !markers.is_empty() {
                        self.failover_coordinator.notify_failover_markers(self.shard.shard_id(), markers);
                    }
                }
            }
        }
    }
}

impl MarkerNotifier for MarkerNotifierImpl {
    fn start(self: Arc<Self>) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        tokio::spawn(self.clone().notify_pending_failover_marker());
        tracing::info!("Marker notifier state changed: started");
    }

    fn stop(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.shutdown.notify_one();
        tracing::info!("Marker notifier state changed: stopped");
    }
}
