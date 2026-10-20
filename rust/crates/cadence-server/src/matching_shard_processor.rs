//! Matching's registry-backed adapter for shard-manager executor callbacks.
//!
//! A shard processor is a view over existing QueueRegistry owners. It does not
//! construct queue state: Matching RPCs acquire managers through the same
//! registry as before, and assignment loss stops those owners by task-list name.

use crate::{
    matching_registry::QueueRegistry,
    matching_shard_distributor::{BoxFuture, ShardProcessor, ShardProcessorFactory, ShardReport},
};
use cadence_proto::sharddistributor as sd;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

/// Creates callback objects over the live Matching queue registry.
///
/// `report_ttl_nanos` is signed to preserve Go's `time.Duration` semantics,
/// including a negative configured TTL (which makes the initial report expire
/// immediately). The caller supplies `ShardDistributorMatchingConfig::report_ttl_nanos()`.
pub struct QueueRegistryShardProcessorFactory {
    registry: QueueRegistry,
    report_ttl_nanos: i64,
}

impl QueueRegistryShardProcessorFactory {
    #[must_use]
    pub fn new(registry: QueueRegistry, report_ttl_nanos: i64) -> Self {
        Self {
            registry,
            report_ttl_nanos,
        }
    }
}

impl ShardProcessorFactory for QueueRegistryShardProcessorFactory {
    fn create(&self, shard_key: String) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
        if shard_key.is_empty() {
            return Box::pin(async { Err("ShardID must be specified".to_string()) });
        }
        let processor: Arc<dyn ShardProcessor> = Arc::new(QueueRegistryShardProcessor {
            registry: self.registry.clone(),
            shard_key,
            report_ttl_nanos: self.report_ttl_nanos,
            created_at: Instant::now(),
            report: Mutex::new(ShardReport {
                status: sd::ShardStatus::Ready,
                load: 0.0,
            }),
        });
        Box::pin(async move { Ok(processor) })
    }
}

struct QueueRegistryShardProcessor {
    registry: QueueRegistry,
    shard_key: String,
    report_ttl_nanos: i64,
    /// Go initializes `reportTime` at processor construction and never updates
    /// it in GetShardReport. Keep that behavior: load remains zero until this
    /// original deadline, then it is recomputed on every report.
    created_at: Instant,
    report: Mutex<ShardReport>,
}

impl ShardProcessor for QueueRegistryShardProcessor {
    /// Go's Matching shard processor Start is deliberately a no-op. Queue
    /// owners continue to be created by the normal Matching registry path.
    fn start(&self) -> BoxFuture<Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    /// Go calls Stop on every registered manager with this exact name. The
    /// registry detaches them and waits for reader shutdown and persistence
    /// cleanup before this callback completes, allowing executor drain/stop to
    /// send its DRAINING heartbeat after queue teardown.
    fn stop(&self) -> BoxFuture<()> {
        let registry = self.registry.clone();
        let shard_key = self.shard_key.clone();
        Box::pin(async move {
            if let Err(error) = registry.stop_task_list_name(&shard_key).await {
                eprintln!("Matching shard processor cleanup failed for {shard_key}: {error}");
            }
        })
    }

    /// Preserve Go's initial zero load and strict TTL check. After expiry, sum
    /// QPS across current loaded owners of the exact task-list name.
    fn report(&self) -> ShardReport {
        let mut report = self.report.lock().expect("Matching shard report lock");
        let elapsed_nanos = self.created_at.elapsed().as_nanos().min(i64::MAX as u128) as i64;
        if elapsed_nanos > self.report_ttl_nanos {
            report.load = self.registry.qps_for_task_list_name(&self.shard_key);
        }
        *report
    }

    fn set_status(&self, status: sd::ShardStatus) {
        self.report
            .lock()
            .expect("Matching shard report lock")
            .status = status;
    }
}
