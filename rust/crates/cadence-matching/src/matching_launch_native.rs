//! Assembly of a native Matching host over configured persistence.
//!
//! The caller must install the real routing, forwarding, scaler, partition
//! refresh and metric providers on the registry before passing it here.
//! This module does not supply permissive defaults for those providers.
use std::sync::Arc;

use cadence_common::go_string::GoString;
use cadence_persistence::semaphores_native::SemaphorePersistenceBytes;
use tonic::Status;

use crate::{
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_domain_uuid::DomainIdParse,
    matching_engine_context_native::PollObserverFactory,
    matching_host_native::NativeMatchingHost,
    matching_membership::Membership,
    matching_quota::MatchingRpcQuota,
    matching_registry_native::NativeQueueRegistry,
    matching_runtime::{DrainDuration, MatchingRuntime},
    matching_semaphore_manager_native::Registry,
    matching_semaphore_service_native::{ContextTimeoutMetric, NativeSemaphoreService},
    matching_shard_routing::MatchingShardRouter,
};

/// Inputs already opened and limited by the configured production launcher.
/// Both stores must use the same persistence limiter as the domain cache.
pub struct ConfiguredNativeAssembly {
    pub task_store: Arc<dyn cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes>,
    pub semaphore_store: Arc<dyn SemaphorePersistenceBytes>,
    pub history: crate::matching_history::MatchingHistoryClient,
    pub cache: Arc<MatchingDomainCache>,
    pub membership: Membership,
    pub quota: Arc<MatchingRpcQuota>,
    pub domain_parse: DomainIdParse,
    pub drain: DrainDuration,
    pub fatal: crate::matching_fatal::MatchingFatalSender,
    pub metrics: crate::matching_metrics::MatchingMetrics,
    pub poll_observer: PollObserverFactory,
}

/// Construct the production registry and host without opening a legacy queue
/// owner. The weak local service slot is published before admission opens.
pub async fn assemble_configured(
    state: AppState,
    deps: ConfiguredNativeAssembly,
) -> anyhow::Result<MatchingRuntime> {
    let registry = crate::matching_runtime_native::native_queue_registry(
        deps.task_store,
        deps.history,
        deps.cache.clone(),
        &state,
        state.current_cluster_name.clone(),
        std::num::NonZeroU32::new(100_000).expect("task id range"),
        crate::matching_fatal::native_fatal_sinks(deps.fatal),
    );
    let (registry, slot) = crate::matching_runtime_native::attach_live_native_registry(
        registry, &state, deps.cache.clone(), deps.membership.clone(), None,
        deps.metrics.clone(),
    );
    let timeout_metric = semaphore_timeout_metric(deps.cache.clone(), deps.metrics.clone());
    assemble(
        state, deps.cache, registry, deps.semaphore_store, deps.membership,
        None, deps.quota, deps.domain_parse, deps.drain, deps.metrics,
        timeout_metric, Some(deps.poll_observer), Some(slot),
    ).await
}

fn semaphore_timeout_metric(
    cache: Arc<MatchingDomainCache>,
    metrics: crate::matching_metrics::MatchingMetrics,
) -> ContextTimeoutMetric {
    Arc::new(move |domain, semaphore, metadata| {
        let name = domain.as_utf8().ok()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .and_then(|id| cache.get_domain_by_id(&id.to_string()).ok())
            .map(|domain| domain.info.name);
        let semaphore = String::from_utf8_lossy(semaphore.as_bytes());
        metrics.semaphore_context_timeout(
            name.as_deref(), &semaphore,
            Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(metadata, "grpc")),
        );
    })
}

/// Assemble and attach a native service to a runtime with no durable legacy
/// owner. The store must be the configured, shared-limiter byte store.
#[allow(clippy::too_many_arguments)]
pub async fn assemble(
    state: AppState,
    cache: Arc<MatchingDomainCache>,
    registry: NativeQueueRegistry,
    semaphore_store: Arc<dyn SemaphorePersistenceBytes>,
    membership: Membership,
    shard_router: Option<Arc<MatchingShardRouter>>,
    quota: Arc<MatchingRpcQuota>,
    domain_parse: DomainIdParse,
    drain: DrainDuration,
    metrics: crate::matching_metrics::MatchingMetrics,
    timeout_metric: ContextTimeoutMetric,
    observer: Option<PollObserverFactory>,
    slot: Option<crate::matching_registry_routing_native::NativeServiceSlot>,
) -> anyhow::Result<MatchingRuntime> {
    let runtime = MatchingRuntime::in_memory_with_domain_cache(state.clone(), drain, cache.clone())
        .await?
        .with_membership(membership.clone());
    let shutdown_handle = runtime.matching().clone();
    let domain_name = {
        let cache = cache.clone();
        Arc::new(move |id: &GoString| {
            let raw = std::str::from_utf8(id.as_bytes())
                .map_err(|_| Status::invalid_argument("invalid domain UUID"))?;
            let id = uuid::Uuid::parse_str(raw)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let domain = cache.get_domain_by_id(&id.to_string())
                .map_err(crate::matching_domain_cache::DomainCacheError::into_status)?;
            Ok(GoString::from(domain.info.name))
        })
    };
    let overlay = state.admin.dynamic_config.clone();
    let config = Arc::new(move |domain: &GoString| {
        // Domain cache names originate in the legacy metadata store as UTF-8.
        let name = std::str::from_utf8(domain.as_bytes()).unwrap_or("");
        let overlay = overlay.lock().expect("admin lock");
        (
            cadence_persistence::dynamic_config::boolean(
                overlay.property_values("system.enableDistributedSemaphore"), Some(name), false,
            ),
            crate::matching_config::semaphore_idle_time(
                overlay.property_values("matching.semaphoreIdleTime"), name,
            ),
        )
    });
    let semaphore = NativeSemaphoreService::new(
        Registry::new(semaphore_store), membership.clone(), domain_name, config,
        Arc::new(move || shutdown_handle.is_shutting_down()), timeout_metric,
    );
    let host = Arc::new(NativeMatchingHost::assemble(
        state, cache, registry.clone(), membership.clone(), shard_router, quota,
        domain_parse, semaphore.callback(), metrics, observer,
    ));
    if let Some(slot) = slot {
        let service: Arc<dyn cadence_proto::native::NativeMatchingApi> = host.clone();
        slot.publish(&service);
    }
    runtime.with_native_host(registry, host, semaphore, membership)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

    struct EmptySource;
    impl crate::matching_domain_cache::DomainMetadataSource for EmptySource {
        fn metadata_version(&self, _: Duration) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> { Ok(0) }
        fn list_domains(&self, _: usize, _: Option<&[u8]>, _: Duration) -> Result<crate::matching_domain_cache::DomainMetadataPage, crate::matching_domain_cache::DomainMetadataSourceError> {
            Ok(crate::matching_domain_cache::DomainMetadataPage { domains: vec![], next_page_token: None })
        }
        fn get_domain(&self, _: Option<&str>, _: Option<&str>, _: Duration) -> Result<MatchingDomainMetadata, crate::matching_domain_cache::DomainMetadataSourceError> {
            Err(crate::matching_domain_cache::DomainMetadataSourceError::permanent("missing"))
        }
    }

    #[tokio::test]
    async fn configured_service_gates_admission_and_stops() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(cadence_persistence::matching_sqlite::SqliteTaskStore::create(
            directory.path().join("native.db"), cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
        ).unwrap());
        let state = AppState::default();
        let cache = Arc::new(MatchingDomainCache::new(Arc::new(EmptySource)));
        let metrics = crate::matching_metrics::MatchingMetrics::default();
        let quota = Arc::new(MatchingRpcQuota::with_metric_sink(state.admin.dynamic_config.clone(), Arc::new(|_| {})));
        let (fatal, _fatal_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = assemble_configured(state, ConfiguredNativeAssembly {
            task_store: store,
            semaphore_store: Arc::new(cadence_persistence::semaphores_native::InMemorySemaphorePersistenceBytes::default()),
            history: crate::matching_history::MatchingHistoryClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(), Duration::from_secs(1), 32 * 1024 * 1024,
            ),
            cache: cache.clone(),
            membership: Membership::new("local", Vec::new()),
            quota,
            domain_parse: DomainIdParse::Lenient,
            drain: Arc::new(|| Duration::ZERO),
            fatal,
            poll_observer: crate::matching_engine_context_native::metrics_poll_observer_factory(metrics.clone(), cache),
            metrics,
        }).await.unwrap();
        let service = runtime.matching().native_service().unwrap().clone();
        let request = || tonic::Request::new(cadence_proto::native::matching::GetTaskListsByDomainRequest::default());
        assert!(tokio::time::timeout(Duration::from_millis(20), service.get_task_lists_by_domain(request())).await.is_err());
        runtime.mark_started();
        let result = tokio::time::timeout(Duration::from_secs(1), service.get_task_lists_by_domain(request())).await.unwrap();
        assert!(result.is_err()); // Missing domain proves the request passed startup and quota admission.
        runtime.shutdown().await.unwrap();
        let error = service.get_task_lists_by_domain(request()).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }
}
