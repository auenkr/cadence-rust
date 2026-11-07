//! Port of `Start` / `Stop` of `go/service/history/engine/engineimpl/history_engine.go`
//! and of the `common.Daemon` part of `go/service/history/engine/interface.go`.
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{deps::*, runtime::*};

/// The components `Start`/`Stop` drive besides the queue processors:
/// `dlqProcessor`, `replicationDLQHandler`, `replicationMetricsEmitter`,
/// `replicationTaskProcessors` and `failoverMarkerNotifier`.
#[async_trait]
pub trait EngineLifecycleHost: Send + Sync {
    /// `dlqProcessor.Start(ctx)`
    async fn start_dlq_processor(&self, ctx: CancellationToken) -> HResult<()>;
    /// `dlqProcessor.Stop(ctx)`
    async fn stop_dlq_processor(&self, ctx: CancellationToken) -> HResult<()>;
    /// `replicationDLQHandler.Start()`
    fn start_replication_dlq_handler(&self);
    /// `replicationDLQHandler.Stop()`
    fn stop_replication_dlq_handler(&self);
    /// `replicationMetricsEmitter.Start()`
    fn start_replication_metrics_emitter(&self);
    /// `replicationMetricsEmitter.Stop()`
    fn stop_replication_metrics_emitter(&self);
    /// `replicationTaskProcessor.Start()` for every processor
    fn start_replication_task_processors(&self);
    /// `replicationTaskProcessor.Stop()` for every processor
    fn stop_replication_task_processors(&self);
    /// `config.EnableGracefulFailover()`
    fn enable_graceful_failover(&self) -> bool;
    /// `failoverMarkerNotifier.Start()`
    fn start_failover_marker_notifier(&self);
    /// `failoverMarkerNotifier.Stop()`
    fn stop_failover_marker_notifier(&self);
}

impl HistoryEngine {
    /// `Start`: spins up all the components needed to serve this shard.
    pub async fn start(self: &Arc<Self>) {
        tracing::info!("History engine state changed: starting");
        for processor in self.queue_processors.values() {
            processor.start();
        }
        let ctx = CancellationToken::new();
        *self.lifecycle_cancel.lock().unwrap() = Some(ctx.clone());
        if let Err(e) = self.lifecycle_host.start_dlq_processor(ctx).await {
            tracing::error!(error = %e, "failed to start history task DLQ processor");
        }
        self.lifecycle_host.start_replication_dlq_handler();
        self.lifecycle_host.start_replication_metrics_emitter();

        // The failover callback may create a failover queue processor, which needs the
        // queue processors started first (see the Go comment).
        self.register_domain_failover_callback();

        self.lifecycle_host.start_replication_task_processors();
        if self.lifecycle_host.enable_graceful_failover() {
            self.lifecycle_host.start_failover_marker_notifier();
        }
        tracing::info!("History engine state changed: started");
    }

    /// `Stop`
    pub async fn stop(&self) {
        tracing::info!("History engine state changed: stopping");
        for processor in self.queue_processors.values() {
            processor.stop().await;
        }
        if let Some(cancel) = self.lifecycle_cancel.lock().unwrap().take() {
            cancel.cancel();
        }
        if let Err(e) = self.lifecycle_host.stop_dlq_processor(CancellationToken::new()).await {
            tracing::error!(error = %e, "failed to stop history task DLQ processor");
        }
        self.lifecycle_host.stop_replication_dlq_handler();
        self.lifecycle_host.stop_replication_metrics_emitter();
        self.lifecycle_host.stop_replication_task_processors();
        self.lifecycle_host.stop_failover_marker_notifier();

        // unset the failover callback
        self.domain_cache.unregister_domain_change_callback(&super::workflow_mutations::create_shard_name_from_shard_id(self.start_host.shard_id()));
        tracing::info!("History engine state changed: stopped");
    }
}
