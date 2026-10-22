//! Production owner for Matching's handler, durable queue registry and
//! background maintenance task.
//!
//! Persistence, History and dynamic configuration enter through explicit
//! dependencies. The production domain cache can be attached through the
//! cache-aware constructors; membership discovery/eviction remains a separate
//! adapter, so this owner does not represent complete Go Matching parity.
use crate::{admin::DynamicConfigOverlay, grpc_matching::Matching};
use cadence_persistence::matching_sqlite::SqliteTaskStore;
use std::{
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

pub type DrainDuration = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type EvictFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
pub type EvictSelf = Arc<dyn Fn() -> EvictFuture + Send + Sync>;

async fn drain_service(
    evict_self: &mut Option<EvictSelf>,
    drain_duration: &DrainDuration,
) -> anyhow::Result<()> {
    let evict_result = if let Some(evict) = evict_self.take() {
        evict().await
    } else {
        Ok(())
    };
    let drain = drain_duration();
    if !drain.is_zero() {
        tokio::time::sleep(drain).await;
    }
    evict_result
}

/// Owns one production Matching handler and all queue lifecycle tasks it
/// starts. shutdown is required on listener success and failure paths.
pub struct MatchingRuntime {
    matching: Matching,
    registry: Option<crate::matching_registry::QueueRegistry>,
    maintenance_stop: Option<oneshot::Sender<()>>,
    maintenance: Option<JoinHandle<Result<(), tonic::Status>>>,
    domain_cache: Option<Arc<crate::matching_domain_cache::MatchingDomainCache>>,
    drain_duration: DrainDuration,
    evict_self: Option<EvictSelf>,
    drained: bool,
}

impl MatchingRuntime {
    pub fn in_memory(state: crate::handlers::AppState, drain_duration: DrainDuration) -> Self {
        Self {
            matching: Matching::new(state),
            registry: None,
            maintenance_stop: None,
            maintenance: None,
            domain_cache: None,
            drain_duration,
            evict_self: None,
            drained: false,
        }
    }

    /// Construct the currently supported durable SQLite runtime. History is
    /// injected so the owner never assumes a loopback address implicitly.
    pub fn with_durable_queues(
        state: crate::handlers::AppState,
        store: Arc<SqliteTaskStore>,
        history: crate::matching_history::MatchingHistoryClient,
        task_id_range: NonZeroU32,
        poll_expiration: Duration,
        drain_duration: DrainDuration,
    ) -> Self {
        let registry = crate::matching_registry::QueueRegistry::new(store, history, task_id_range);
        let matching = Matching::with_durable_queues(state, registry.clone(), poll_expiration);
        let (stop, stopped) = oneshot::channel();
        let maintenance_registry = registry.clone();
        let maintenance = tokio::spawn(async move {
            maintenance_registry
                .maintain_until(Duration::from_secs(10), async {
                    let _ = stopped.await;
                })
                .await
        });
        Self {
            matching,
            registry: Some(registry),
            maintenance_stop: Some(stop),
            maintenance: Some(maintenance),
            domain_cache: None,
            drain_duration,
            evict_self: None,
            drained: false,
        }
    }

    pub fn matching(&self) -> &Matching {
        &self.matching
    }

    /// Start and connect the persistence-backed domain cache before constructing
    /// Matching. Its synchronous initial refresh therefore gates readiness;
    /// subsequent notifications update the same domain view consumed by Matching.
    pub async fn in_memory_with_domain_cache(
        state: crate::handlers::AppState,
        drain_duration: DrainDuration,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<Self> {
        Self::start_domain_cache(state.domains.clone(), cache.clone()).await?;
        let mut runtime = Self::in_memory(state, drain_duration);
        runtime.matching = runtime.matching.with_domain_cache(cache.clone());
        runtime.matching = runtime.matching.with_domain_cache(cache.clone());
        runtime.domain_cache = Some(cache);
        Ok(runtime)
    }

    pub async fn with_durable_queues_and_domain_cache(
        state: crate::handlers::AppState,
        store: Arc<SqliteTaskStore>,
        history: crate::matching_history::MatchingHistoryClient,
        task_id_range: NonZeroU32,
        poll_expiration: Duration,
        drain_duration: DrainDuration,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<Self> {
        Self::start_domain_cache(state.domains.clone(), cache.clone()).await?;
        let mut runtime = Self::with_durable_queues(
            state,
            store,
            history,
            task_id_range,
            poll_expiration,
            drain_duration,
        );
        runtime.matching = runtime.matching.with_domain_cache(cache.clone());
        runtime.domain_cache = Some(cache);
        Ok(runtime)
    }

    async fn start_domain_cache(
        domains: cadence_persistence::InMemoryDomainStore,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(move || {
            cache
                .start()
                .map_err(|error| anyhow::anyhow!("starting Matching domain cache: {error}"))?;
            let changed: crate::matching_domain_cache::DomainChangeCallback =
                Arc::new(move |entries| domains.sync_matching_metadata(entries));
            let catch_up: crate::matching_domain_cache::CatchUpCallback =
                Arc::new(|entries, prepare, changed| {
                    prepare();
                    changed(entries);
                });
            cache.register_callback("matching", catch_up, Arc::new(|| {}), changed);
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("joining Matching domain cache startup: {error}"))?
    }

    /// Supply Go's membership eviction action when production membership
    /// lifecycle is wired. It remains absent in the current main assembly.
    pub fn with_membership_evictor(mut self, evict_self: EvictSelf) -> Self {
        self.evict_self = Some(evict_self);
        self
    }

    /// Begin Go's shutdown drain while listeners remain live. The caller
    /// drops listeners only after this future completes.
    pub async fn drain(&mut self) -> anyhow::Result<()> {
        if self.drained {
            return Ok(());
        }
        let result = drain_service(&mut self.evict_self, &self.drain_duration).await;
        self.drained = true;
        result
    }

    /// Finish shutdown after draining traffic: stop the owner and join
    /// maintenance so final acknowledgements and cleanup complete.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        let drain_result = self.drain().await;
        if let Some(stop) = self.maintenance_stop.take() {
            let _ = stop.send(());
        }
        let maintenance_result = if let Some(maintenance) = self.maintenance.take() {
            maintenance
                .await
                .map_err(|error| anyhow::anyhow!("joining Matching maintenance: {error}"))
                .and_then(|result| {
                    result
                        .map_err(|error| anyhow::anyhow!("stopping Matching maintenance: {error}"))
                })
        } else if let Some(registry) = &self.registry {
            registry
                .shutdown(crate::grpc_matching::matching_now())
                .await
                .map_err(|error| anyhow::anyhow!("stopping Matching registry: {error}"))
        } else {
            Ok(())
        };
        if let Some(cache) = self.domain_cache.take() {
            cache.unregister_callback("matching");
            cache.stop();
        }
        maintenance_result?;
        drain_result?;
        Ok(())
    }
}

impl Drop for MatchingRuntime {
    fn drop(&mut self) {
        // Normal shutdown awaits the task. This is a leak guard for abandoned
        // owners, where persistence cannot safely be awaited.
        if let Some(stop) = self.maintenance_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.maintenance.take() {
            task.abort();
        }
    }
}

/// Read matching.shutdownDrainDuration at call time. Go defines this property
/// without filters and defaults it to zero.
pub fn live_shutdown_drain_duration(overlay: Arc<Mutex<DynamicConfigOverlay>>) -> DrainDuration {
    Arc::new(move || {
        let values = overlay
            .lock()
            .expect("admin dynamic config lock")
            .property_values("matching.shutdownDrainDuration")
            .to_vec();
        let mut selected = None;
        for value in values.iter().filter(|value| value.filters.is_empty()) {
            let Some(blob) = value.value.as_ref() else {
                continue;
            };
            if blob.encoding_type != Some(cadence_api::enums::EncodingType::JSON) {
                continue;
            }
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&blob.data) {
                selected = Some(value);
            }
        }
        let nanos = selected
            .and_then(|value| {
                value
                    .as_str()
                    .and_then(cadence_common::duration::parse_nanos)
            })
            .unwrap_or_default();
        Duration::from_nanos(nanos.max(0) as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn shutdown_drain_provider_reads_live_unfiltered_value() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let provider = live_shutdown_drain_duration(overlay.clone());
        assert_eq!(provider(), Duration::ZERO);
        overlay.lock().unwrap().update(
            false,
            "matching.shutdownDrainDuration",
            vec![
                cadence_api::admin::DynamicConfigValue {
                    filters: vec![cadence_api::admin::DynamicConfigFilter {
                        name: "taskListName".into(),
                        value: Some(cadence_api::types::DataBlob {
                            encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                            data: br#""99ms""#.to_vec(),
                        }),
                    }],
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""99ms""#.to_vec(),
                    }),
                },
                cadence_api::admin::DynamicConfigValue {
                    filters: vec![],
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""25ms""#.to_vec(),
                    }),
                },
            ],
        );
        assert_eq!(provider(), Duration::from_millis(25));
    }

    #[tokio::test]
    async fn shutdown_drain_runs_membership_evictor_before_live_drain_read() {
        let order = Arc::new(AtomicUsize::new(0));
        let evict_order = order.clone();
        let evict: EvictSelf = Arc::new(move || {
            let evict_order = evict_order.clone();
            Box::pin(async move {
                assert_eq!(evict_order.fetch_add(1, Ordering::SeqCst), 0);
                Ok(())
            })
        });
        let drain_order = order.clone();
        let duration: DrainDuration = Arc::new(move || {
            assert_eq!(drain_order.fetch_add(1, Ordering::SeqCst), 1);
            Duration::ZERO
        });
        let mut evictor = Some(evict);
        drain_service(&mut evictor, &duration).await.unwrap();
        assert_eq!(order.load(Ordering::SeqCst), 2);
        assert!(evictor.is_none());
    }

    #[tokio::test]
    async fn durable_runtime_joins_maintenance_and_flushes_on_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(
                directory.path().join("matching.db"),
                cadence_persistence::matching_blob::BlobEncoding::ThriftRWSnappy,
            )
            .unwrap(),
        );
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let history = crate::matching_history::MatchingHistoryClient::new(
            channel,
            Duration::from_millis(10),
            1024,
        );
        let mut runtime = MatchingRuntime::with_durable_queues(
            crate::handlers::AppState::default(),
            store,
            history,
            NonZeroU32::new(100).unwrap(),
            Duration::from_secs(1),
            Arc::new(|| Duration::ZERO),
        );
        runtime.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}
