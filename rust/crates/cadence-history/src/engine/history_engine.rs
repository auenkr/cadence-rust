//! Engine-owned component lifecycle from engineimpl/history_engine.go.
//! TODO(link): shard factory builds these components using the package ports.
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use super::{deps::HResult, register_domain_failover_callback::{DomainChangeRegistrar, FailoverCallbacks, create_shard_name_from_shard_id}, runtime::HistoryEngine};

pub trait Daemon: Send + Sync { fn start(&self); fn stop(&self); }
#[async_trait]
pub trait TaskDLQProcessor: Send + Sync {
    async fn start(&self, canceled: tokio::sync::watch::Receiver<bool>) -> HResult<()>;
    async fn stop(&self) -> HResult<()>;
}
pub trait DomainCallbackUnregister: DomainChangeRegistrar { fn unregister(&self, name: &str); }
pub struct EngineLifecycle {
    pub dlq: Arc<dyn TaskDLQProcessor>,
    pub replication_dlq: Arc<dyn Daemon>,
    pub replication_metrics: Arc<dyn Daemon>,
    pub replication_processors: Vec<Arc<dyn Daemon>>,
    pub failover_marker_notifier: Arc<dyn Daemon>,
    pub failover_callbacks: Arc<FailoverCallbacks>,
    pub domain_callbacks: Arc<dyn DomainCallbackUnregister>,
    pub graceful_failover: bool,
    pub shard_id: i32,
    pub cancel: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
}
impl HistoryEngine {
    pub async fn start(&self) {
        tracing::info!(lifecycle = "Starting", "History engine state changed");
        for processor in self.queue_processors.values() { processor.start(); }
        let (cancel, canceled) = tokio::sync::watch::channel(false);
        *self.lifecycle.cancel.lock().expect("engine lifecycle lock") = Some(cancel);
        if let Err(error) = self.lifecycle.dlq.start(canceled).await {
            tracing::error!(error = %error, "failed to start history task DLQ processor");
        }
        self.lifecycle.replication_dlq.start(); self.lifecycle.replication_metrics.start();
        // Queue scan bounds are available only after processors have started.
        self.lifecycle.failover_callbacks.register_domain_failover_callback(&*self.lifecycle.domain_callbacks);
        for processor in &self.lifecycle.replication_processors { processor.start(); }
        if self.lifecycle.graceful_failover { self.lifecycle.failover_marker_notifier.start(); }
        tracing::info!(lifecycle = "Started", "History engine state changed");
    }
    pub async fn stop(&self) {
        tracing::info!(lifecycle = "Stopping", "History engine state changed");
        for processor in self.queue_processors.values() { processor.stop(); }
        if let Some(cancel) = self.lifecycle.cancel.lock().expect("engine lifecycle lock").as_ref() { let _ = cancel.send(true); }
        if let Err(error) = self.lifecycle.dlq.stop().await {
            tracing::error!(error = %error, "failed to stop history task DLQ processor");
        }
        self.lifecycle.replication_dlq.stop(); self.lifecycle.replication_metrics.stop();
        for processor in &self.lifecycle.replication_processors { processor.stop(); }
        self.lifecycle.failover_marker_notifier.stop();
        self.lifecycle.domain_callbacks.unregister(&create_shard_name_from_shard_id(self.lifecycle.shard_id));
        tracing::info!(lifecycle = "Stopped", "History engine state changed");
    }
}
