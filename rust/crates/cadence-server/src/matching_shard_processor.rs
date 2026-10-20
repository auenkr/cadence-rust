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
    clock_nanos: Arc<dyn Fn() -> u128 + Send + Sync>,
}

impl QueueRegistryShardProcessorFactory {
    #[must_use]
    pub fn new(registry: QueueRegistry, report_ttl_nanos: i64) -> Self {
        let origin = Instant::now();
        Self::with_clock(
            registry,
            report_ttl_nanos,
            Arc::new(move || origin.elapsed().as_nanos()),
        )
    }

    fn with_clock(
        registry: QueueRegistry,
        report_ttl_nanos: i64,
        clock_nanos: Arc<dyn Fn() -> u128 + Send + Sync>,
    ) -> Self {
        Self {
            registry,
            report_ttl_nanos,
            clock_nanos,
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
            created_at_nanos: (self.clock_nanos)(),
            clock_nanos: self.clock_nanos.clone(),
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
    created_at_nanos: u128,
    clock_nanos: Arc<dyn Fn() -> u128 + Send + Sync>,
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
        let elapsed_nanos = (self.clock_nanos)()
            .saturating_sub(self.created_at_nanos)
            .min(i64::MAX as u128) as i64;
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

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::BlobEncoding,
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };
    use std::{
        num::NonZeroU32,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };
    use uuid::Uuid;

    fn fixture() -> (tempfile::TempDir, Arc<SqliteTaskStore>, QueueRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let history = crate::matching_history::MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap())
            .with_qps_interval(Arc::new(|_, _, _| 1_000_000));
        (dir, store, registry)
    }

    fn key(domain: u128, name: &str, task_type: i32) -> TaskListKey {
        TaskListKey {
            domain_id: Uuid::from_u128(domain),
            name: name.into(),
            task_type,
        }
    }

    #[tokio::test]
    async fn report_has_strict_ttl_and_recomputes_qps_after_original_deadline() {
        let (_dir, _store, registry) = fixture();
        let now = Arc::new(AtomicU64::new(0));
        let clock = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::SeqCst) as u128)
        };
        let factory = QueueRegistryShardProcessorFactory::with_clock(registry.clone(), 10, clock);
        let processor = factory.create("queue".into()).await.unwrap();
        assert_eq!(processor.report().load, 0.0);
        now.store(10, Ordering::SeqCst);
        assert_eq!(
            processor.report().load,
            0.0,
            "expiry is strict: elapsed == TTL is still fresh"
        );
        now.store(11, Ordering::SeqCst);
        assert_eq!(processor.report().load, 0.0);
        let queue = registry.get(key(1, "queue", 0), 0, 1).await.unwrap();
        let data = cadence_persistence::matching_blob::TaskData {
            partition_config: Some(std::collections::HashMap::from([(
                "isolation-group".into(),
                "blue".into(),
            )])),
            ..Default::default()
        };
        crate::grpc_matching::report_arrival(&queue, &data);
        std::thread::sleep(Duration::from_millis(3));
        assert!(queue.qps.group_qps("blue") > 0.0);
        assert!(queue.qps.qps() > 0.0);
        now.store(12, Ordering::SeqCst);
        assert!(processor.report().load > 0.0);
    }

    #[tokio::test]
    async fn start_is_noop_and_stop_is_exact_name_across_domain_and_type() {
        let (_dir, _store, registry) = fixture();
        let same_name_decision = registry.get(key(1, "queue/1", 0), 0, 1).await.unwrap();
        let same_name_activity = registry.get(key(2, "queue/1", 1), 0, 1).await.unwrap();
        let root = registry.get(key(1, "queue", 0), 0, 1).await.unwrap();
        let other_partition = registry.get(key(1, "queue/2", 0), 0, 1).await.unwrap();
        let processor = QueueRegistryShardProcessorFactory::new(registry.clone(), 0)
            .create("queue/1".into())
            .await
            .unwrap();
        processor.start().await.unwrap();
        assert_eq!(
            registry.loaded_owner_count(),
            4,
            "start must not create owners"
        );
        processor.stop().await;
        assert!(same_name_decision.queue.is_stopped());
        assert!(same_name_activity.queue.is_stopped());
        assert!(
            !root.queue.is_stopped(),
            "root name is a distinct exact key"
        );
        assert!(
            !other_partition.queue.is_stopped(),
            "partition name is a distinct exact key"
        );
        assert_eq!(registry.loaded_owner_count(), 2);
        // Normal registry access after assignment returns can rebuild from the
        // same SQLite store; no second queue owner is created by the factory.
        let reassigned = registry.get(key(1, "queue/1", 0), 0, 2).await.unwrap();
        assert!(!reassigned.queue.is_stopped());
        assert_eq!(registry.loaded_owner_count(), 3);
    }
}
