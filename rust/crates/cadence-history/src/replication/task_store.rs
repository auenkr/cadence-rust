//! Port of `service/history/replication/task_store.go` (plus the
//! `common/cache.BoundedAckCache` it relies on).
use async_trait::async_trait;
use cadence_api::types::{DataBlob, ReplicationTask};
use cadence_common::backoff::ExponentialRetryPolicy;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::deps::{
    throttle_retry, DomainCache, HistoryTask, Limiter, ReplicationConfig, ReplicationError,
    ReplicationMetrics, Result, TimeSource,
};

/// `cache.ErrAckCacheFull` / `cache.ErrAlreadyAcked`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AckCacheError {
    #[error("ack cache is full")]
    Full,
    #[error("already acked")]
    AlreadyAcked,
}

struct AckCacheInner<T> {
    items: BTreeMap<i64, (T, u64)>,
    last_ack: i64,
    curr_size: u64,
}

/// `cache.BoundedAckCache`: keeps items by monotonically increasing sequence
/// number bounded by item count and total byte size.
pub struct BoundedAckCache<T: Clone> {
    max_count: usize,
    max_size: u64,
    inner: Mutex<AckCacheInner<T>>,
}

impl<T: Clone> BoundedAckCache<T> {
    pub fn new(max_count: usize, max_size: u64) -> Self {
        Self {
            max_count,
            max_size,
            inner: Mutex::new(AckCacheInner {
                items: BTreeMap::new(),
                last_ack: i64::MIN,
                curr_size: 0,
            }),
        }
    }

    /// `Put`.
    pub fn put(&self, seq: i64, item: T, size: u64) -> std::result::Result<(), AckCacheError> {
        let mut g = self.inner.lock().unwrap();
        if g.last_ack >= seq {
            return Err(AckCacheError::AlreadyAcked);
        }
        if g.items.contains_key(&seq) {
            return Ok(());
        }
        if g.items.len() >= self.max_count {
            return Err(AckCacheError::Full);
        }
        match g.curr_size.checked_add(size) {
            Some(total) if total < self.max_size => {
                g.curr_size = total;
            }
            _ => return Err(AckCacheError::Full),
        }
        g.items.insert(seq, (item, size));
        Ok(())
    }

    /// `Get`.
    pub fn get(&self, seq: i64) -> Option<T> {
        self.inner.lock().unwrap().items.get(&seq).map(|(t, _)| t.clone())
    }

    /// `Ack`: removes items up to and including `level`; returns (freed size, count).
    pub fn ack(&self, level: i64) -> (u64, usize) {
        let mut g = self.inner.lock().unwrap();
        let keep = g.items.split_off(&(level.saturating_add(1)));
        let removed = std::mem::replace(&mut g.items, keep);
        let freed: u64 = removed.values().map(|(_, s)| *s).sum();
        g.curr_size = g.curr_size.saturating_sub(freed);
        if level > g.last_ack {
            g.last_ack = level;
        }
        (freed, g.items.len())
    }

    /// `Size`.
    pub fn size(&self) -> u64 {
        self.inner.lock().unwrap().curr_size
    }

    /// `Count`.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().items.len()
    }
}

fn blob_size(b: &Option<DataBlob>) -> u64 {
    match b {
        Some(b) => (std::mem::size_of::<DataBlob>() + b.data.len()) as u64,
        None => 0,
    }
}

/// `ReplicationTask.ByteSize()`. Rust struct sizes differ from Go's
/// `unsafe.Sizeof`, so this is an approximation with the same shape
/// (struct size + string/blob lengths). TODO-verify: only used to bound cache memory.
pub fn replication_task_byte_size(t: &ReplicationTask) -> u64 {
    let mut size = std::mem::size_of::<ReplicationTask>() as u64;
    if let Some(a) = &t.history_task_v2_attributes {
        size += std::mem::size_of_val(a) as u64;
        size += a.domain_id.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.workflow_id.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.run_id.as_ref().map_or(0, |s| s.len()) as u64;
        if let Some(items) = &a.version_history_items {
            size += (items.len() * (8 + std::mem::size_of::<cadence_api::types::VersionHistoryItem>())) as u64;
        }
        size += blob_size(&a.events);
        size += blob_size(&a.new_run_events);
    }
    if let Some(a) = &t.sync_activity_task_attributes {
        size += std::mem::size_of_val(a) as u64;
        size += a.domain_id.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.workflow_id.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.run_id.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.details.as_ref().map_or(0, |d| d.len()) as u64;
        size += a.last_failure_reason.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.last_worker_identity.as_ref().map_or(0, |s| s.len()) as u64;
        size += a.last_failure_details.as_ref().map_or(0, |d| d.len()) as u64;
        if let Some(vh) = &a.version_history {
            size += vh.branch_token.len() as u64;
            size += (vh.items.len() * std::mem::size_of::<cadence_api::types::VersionHistoryItem>()) as u64;
        }
    }
    if let Some(a) = &t.failover_marker_attributes {
        size += std::mem::size_of_val(a) as u64;
        size += a.domain_id.as_ref().map_or(0, |s| s.len()) as u64;
    }
    if let Some(a) = &t.sync_shard_status_task_attributes {
        size += std::mem::size_of_val(a) as u64;
        size += a.source_cluster.as_ref().map_or(0, |s| s.len()) as u64;
    }
    size
}

/// `taskHydrator` interface.
#[async_trait]
pub trait TaskHydratorApi: Send + Sync {
    async fn hydrate(&self, task: &HistoryTask) -> Result<Option<ReplicationTask>>;
}

/// `TaskStore`.
pub struct TaskStore {
    clusters: HashMap<String, BoundedAckCache<Arc<ReplicationTask>>>,
    domains: Arc<dyn DomainCache>,
    hydrator: Arc<dyn TaskHydratorApi>,
    rate_limiter: Arc<dyn Limiter>,
    retry_policy: ExponentialRetryPolicy,
    time_source: Arc<dyn TimeSource>,
    metrics: Arc<dyn ReplicationMetrics>,
    shard_id: i32,
    last_log_time: Mutex<Option<Instant>>,
}

const SCOPE: &str = "ReplicatorCacheManager";

impl TaskStore {
    /// `NewTaskStore`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &dyn ReplicationConfig,
        remote_clusters: Vec<String>,
        domains: Arc<dyn DomainCache>,
        metrics: Arc<dyn ReplicationMetrics>,
        hydrator: Arc<dyn TaskHydratorApi>,
        rate_limiter: Arc<dyn Limiter>,
        shard_id: i32,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        let mut clusters = HashMap::new();
        for name in remote_clusters {
            clusters.insert(
                name,
                BoundedAckCache::new(config.replicator_cache_capacity(), config.replicator_cache_max_size() as u64),
            );
        }
        let retry_policy = ExponentialRetryPolicy::new(Duration::from_millis(100))
            .with_maximum_attempts(config.replicator_read_task_max_retry_count())
            .with_backoff_coefficient(1.0);
        Self {
            clusters,
            domains,
            hydrator,
            rate_limiter,
            retry_policy,
            time_source,
            metrics,
            shard_id,
            last_log_time: Mutex::new(None),
        }
    }

    fn record_cache_size(&self, cluster: &str, count: usize) {
        let shard = self.shard_id.to_string();
        let tags = [("shard_id", shard.as_str()), ("cluster", cluster), ("cache_type", "replication")];
        self.metrics.record_timer(SCOPE, "cache_size", &tags, Duration::from_nanos(count as u64));
        self.metrics.record_histogram_value(SCOPE, "cache_size_histogram", &tags, count as f64);
        self.metrics.update_gauge(SCOPE, "cache_size_gauge", &tags, count as f64);
    }

    /// `Get`. `Ok(None)` means the task must be ignored.
    pub async fn get(&self, cluster: &str, info: &HistoryTask) -> Result<Option<Arc<ReplicationTask>>> {
        let cache = self.clusters.get(cluster).ok_or(ReplicationError::UnknownCluster)?;
        let domain = self
            .domains
            .get_domain_by_id(&info.info().domain_id)
            .map_err(|e| ReplicationError::Other(format!("resolving domain: {e}")))?;
        if !domain.has_replication_cluster(cluster) {
            return Ok(None);
        }
        let shard = self.shard_id.to_string();
        let tags = [("shard_id", shard.as_str()), ("cluster", cluster), ("cache_type", "replication")];
        self.metrics.inc_counter(SCOPE, "cache_requests", &tags);
        let start = self.time_source.now_nanos();
        let result = self.get_inner(cluster, cache, info, &tags).await;
        let elapsed = (self.time_source.now_nanos() - start).max(0) as u64;
        self.metrics.exponential_histogram(SCOPE, "exponential_cache_latency", &tags, Duration::from_nanos(elapsed));
        result
    }

    async fn get_inner(
        &self,
        _cluster: &str,
        cache: &BoundedAckCache<Arc<ReplicationTask>>,
        info: &HistoryTask,
        tags: &[(&str, &str)],
    ) -> Result<Option<Arc<ReplicationTask>>> {
        if let Some(task) = cache.get(info.task_id()) {
            self.metrics.inc_counter(SCOPE, "cache_hit", tags);
            return Ok(Some(task));
        }
        self.metrics.inc_counter(SCOPE, "cache_miss", tags);
        self.rate_limiter.wait().await;
        let hydrated = throttle_retry(
            &self.retry_policy,
            |e| e.is_persistence_transient(),
            || self.hydrator.hydrate(info),
        )
        .await;
        match hydrated {
            Err(e) => {
                self.metrics.inc_counter(SCOPE, "cache_failures", tags);
                Err(e)
            }
            Ok(task) => {
                let task = task.map(Arc::new);
                self.put(task.clone());
                Ok(task)
            }
        }
    }

    /// `Put`.
    pub fn put(&self, task: Option<Arc<ReplicationTask>>) {
        let task = match task {
            Some(t) => t,
            None => return,
        };
        let domain = match self.get_domain(&task) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = %e, "failed to resolve domain");
                return;
            }
        };
        let size = replication_task_byte_size(&task);
        let task_id = task.source_task_id.unwrap_or_default();
        for (target, cache) in &self.clusters {
            if let Some(d) = &domain {
                if !d.has_replication_cluster(target) {
                    continue;
                }
            }
            let shard = self.shard_id.to_string();
            let tags = [("shard_id", shard.as_str()), ("cluster", target.as_str()), ("cache_type", "replication")];
            match cache.put(task_id, task.clone(), size) {
                Err(AckCacheError::Full) => {
                    self.metrics.inc_counter(SCOPE, "cache_full", &tags);
                    let mut last = self.last_log_time.lock().unwrap();
                    if last.is_none_or(|t| t.elapsed() > Duration::from_secs(60)) {
                        tracing::warn!(shard_id = self.shard_id, "Replication cache is full");
                        *last = Some(Instant::now());
                    }
                }
                Err(AckCacheError::AlreadyAcked) | Ok(()) => {}
            }
            self.record_cache_size(target, cache.count());
        }
    }

    /// `Ack`.
    pub fn ack(&self, cluster: &str, last_task_id: i64) -> Result<()> {
        let cache = self.clusters.get(cluster).ok_or(ReplicationError::UnknownCluster)?;
        let _ = cache.ack(last_task_id);
        self.record_cache_size(cluster, cache.count());
        Ok(())
    }

    fn get_domain(&self, task: &ReplicationTask) -> Result<Option<super::deps::DomainEntry>> {
        let ids = [
            task.history_task_v2_attributes.as_ref().and_then(|a| a.domain_id.clone()),
            task.sync_activity_task_attributes.as_ref().and_then(|a| a.domain_id.clone()),
            task.failover_marker_attributes.as_ref().and_then(|a| a.domain_id.clone()),
        ];
        for id in ids.into_iter().flatten() {
            if !id.is_empty() {
                return self.domains.get_domain_by_id(&id).map(Some);
            }
        }
        Ok(None)
    }
}
