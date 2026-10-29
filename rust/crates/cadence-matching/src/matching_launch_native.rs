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
    pub task_store:
        Arc<dyn cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes>,
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
        registry,
        &state,
        deps.cache.clone(),
        deps.membership.clone(),
        None,
        deps.metrics.clone(),
    );
    let timeout_metric = semaphore_timeout_metric(deps.cache.clone(), deps.metrics.clone());
    assemble(
        state,
        deps.cache,
        registry,
        deps.semaphore_store,
        deps.membership,
        None,
        deps.quota,
        deps.domain_parse,
        deps.drain,
        deps.metrics,
        timeout_metric,
        Some(deps.poll_observer),
        Some(slot),
    )
    .await
}

fn semaphore_timeout_metric(
    cache: Arc<MatchingDomainCache>,
    metrics: crate::matching_metrics::MatchingMetrics,
) -> ContextTimeoutMetric {
    Arc::new(move |domain, semaphore, metadata| {
        let name = domain
            .as_utf8()
            .ok()
            .and_then(|id| cache.get_domain_by_id(id).ok())
            .map(|domain| domain.info.name);
        let semaphore = semaphore.go_rune_string();
        metrics.semaphore_context_timeout(
            name.as_deref(),
            &semaphore,
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
            let domain = cache
                .get_domain_by_id(raw)
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
                overlay.property_values("system.enableDistributedSemaphore"),
                Some(name),
                false,
            ),
            crate::matching_config::semaphore_idle_time(
                overlay.property_values("matching.semaphoreIdleTime"),
                name,
            ),
        )
    });
    let semaphore = NativeSemaphoreService::new(
        Registry::new(semaphore_store),
        membership.clone(),
        domain_name,
        config,
        Arc::new(move || shutdown_handle.is_shutting_down()),
        timeout_metric,
    );
    let host = Arc::new(NativeMatchingHost::assemble(
        state,
        cache,
        registry.clone(),
        membership.clone(),
        shard_router,
        quota,
        domain_parse,
        semaphore.callback(),
        metrics,
        observer,
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
    use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
    use std::time::Duration;

    struct EmptySource;
    impl crate::matching_domain_cache::DomainMetadataSource for EmptySource {
        fn metadata_version(
            &self,
            _: Duration,
        ) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> {
            Ok(0)
        }
        fn list_domains(
            &self,
            _: usize,
            _: Option<&[u8]>,
            _: Duration,
        ) -> Result<
            crate::matching_domain_cache::DomainMetadataPage,
            crate::matching_domain_cache::DomainMetadataSourceError,
        > {
            Ok(crate::matching_domain_cache::DomainMetadataPage {
                domains: vec![],
                next_page_token: None,
            })
        }
        fn get_domain(
            &self,
            _: Option<&str>,
            _: Option<&str>,
            _: Duration,
        ) -> Result<MatchingDomainMetadata, crate::matching_domain_cache::DomainMetadataSourceError>
        {
            Err(crate::matching_domain_cache::DomainMetadataSourceError::permanent("missing"))
        }
    }

    #[tokio::test]
    async fn configured_service_gates_admission_and_stops() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            cadence_persistence::matching_sqlite::SqliteTaskStore::create(
                directory.path().join("native.db"),
                cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
            )
            .unwrap(),
        );
        let state = AppState::default();
        let cache = Arc::new(MatchingDomainCache::new(Arc::new(EmptySource)));
        let metrics = crate::matching_metrics::MatchingMetrics::default();
        let quota = Arc::new(MatchingRpcQuota::with_metric_sink(
            state.admin.dynamic_config.clone(),
            Arc::new(|_| {}),
        ));
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
        let request = || {
            tonic::Request::new(
                cadence_proto::native::matching::GetTaskListsByDomainRequest::default(),
            )
        };
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            service.get_task_lists_by_domain(request())
        )
        .await
        .is_err());
        runtime.mark_started();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            service.get_task_lists_by_domain(request()),
        )
        .await
        .unwrap();
        assert!(result.is_err()); // Missing domain proves the request passed startup and quota admission.
        runtime.shutdown().await.unwrap();
        let error = service
            .get_task_lists_by_domain(request())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    // Exercises the configured host through the same TCP gRPC server wrapper used
    // by the launcher. The store parks only the first scan of each phase.
    #[tokio::test]
    async fn configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry() {
        use cadence_common::membership::Host;
        use cadence_proto::native::NativeMatchingApi;
        use cadence_persistence::semaphores_native::{
            InMemorySemaphorePersistenceBytes, SemaphorePersistenceBytes,
        };
        use crate::matching_semaphore_manager_native::tests::ParkGrant;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio::sync::Notify;

        const DOMAIN: &str = "00000000-0000-0000-0000-000000000001";
        struct Source;
        impl crate::matching_domain_cache::DomainMetadataSource for Source {
            fn metadata_version(&self, _: Duration) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> { Ok(0) }
            fn list_domains(&self, _: usize, _: Option<&[u8]>, _: Duration) -> Result<crate::matching_domain_cache::DomainMetadataPage, crate::matching_domain_cache::DomainMetadataSourceError> {
                Ok(crate::matching_domain_cache::DomainMetadataPage { domains: vec![], next_page_token: None })
            }
            fn get_domain(&self, _: Option<&str>, _: Option<&str>, _: Duration) -> Result<MatchingDomainMetadata, crate::matching_domain_cache::DomainMetadataSourceError> {
                Ok(MatchingDomainMetadata {
                    info: cadence_api::types::DomainInfo { uuid: DOMAIN.into(), name: "domain".into(), ..Default::default() },
                    configuration: Default::default(), replication: Default::default(), is_global_domain: false,
                    archival_bucket: String::new(), status_raw: 0, archival_status_raw: 0,
                    history_archival_status_raw: 0, visibility_archival_status_raw: 0,
                    config_version: 0, notification_version: 0, failover_notification_version: 0,
                    failover_version: 0, previous_failover_version: -1, failover_end_time_nanos: None,
                    last_updated_time_nanos: 0, bad_binaries: None, isolation_groups: None,
                    async_workflow_config: None, active_clusters_config: None, decoded_bad_binaries: None,
                    decoded_isolation_groups: None, decoded_async_workflow_config: None, config_decode_error: None,
                })
            }
        }
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop { fn drop(&mut self) { self.0.abort(); } }

        tokio::time::timeout(Duration::from_secs(45), async {
            let directory = tempfile::tempdir().unwrap();
            let task_store = Arc::new(cadence_persistence::matching_sqlite::SqliteTaskStore::create(
                directory.path().join("native.db"), cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
            ).unwrap());
            let store = Arc::new(ParkGrant {
                inner: InMemorySemaphorePersistenceBytes::default(), entered: Notify::new(), release: Notify::new(),
                scan_pages: AtomicUsize::new(0), park_scan: AtomicBool::new(true), fail_scan: AtomicBool::new(false),
                scan_entered: Notify::new(), scan_release: Notify::new(),
            });
            let domain = GoString::from(DOMAIN);
            for name in ["sem", "deadline"] {
                store.inner.create_semaphore(&domain, &GoString::from(name), 1, 1).await.unwrap();
            }
            let state = AppState::default();
            state.admin.dynamic_config.lock().unwrap().update(false, "system.enableDistributedSemaphore", vec![
                cadence_api::admin::DynamicConfigValue {
                    value: Some(cadence_api::types::DataBlob::new(cadence_api::enums::EncodingType::JSON, b"true".to_vec())),
                    filters: vec![],
                }
            ]);
            let cache = Arc::new(MatchingDomainCache::new(Arc::new(Source)));
            let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
            let metrics = crate::matching_metrics::MatchingMetrics::new(sink.clone());
            let quota = Arc::new(MatchingRpcQuota::with_metric_sink(state.admin.dynamic_config.clone(), Arc::new(|_| {})));
            let (fatal, _fatal_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut runtime = assemble_configured(state, ConfiguredNativeAssembly {
                task_store, semaphore_store: store.clone(),
                history: crate::matching_history::MatchingHistoryClient::new(
                    tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(), Duration::from_secs(1), 32 * 1024 * 1024),
                cache: cache.clone(), membership: Membership::new("local", vec![Host {
                    address: "local".into(), identity: "local".into(), grpc_address: String::new(),
                }]), quota, domain_parse: DomainIdParse::Lenient, drain: Arc::new(|| Duration::ZERO), fatal,
                poll_observer: crate::matching_engine_context_native::metrics_poll_observer_factory(metrics.clone(), cache), metrics,
            }).await.unwrap();
            runtime.mark_started();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(crate::grpc_matching::serve_with_listener(listener, runtime.matching().clone()));
            let _abort = AbortOnDrop(server.abort_handle());
            let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap().connect().await.unwrap();
            let client = cadence_proto::native_service::matching_api_client::MatchingApiClient::new(channel);
            let transport = crate::matching_transport_native::SharedNativeMatching(
                runtime.matching().native_service().unwrap().clone(),
            );
            let request = |name: &str, owner: &str, timeout: Option<Duration>| {
                let mut request = tonic::Request::new(cadence_proto::native::matching::AcquireSemaphoreRequest {
                    domain_id: GoString::from(DOMAIN), semaphore_name: GoString::from(name), bucket: 0,
                    owner_id: GoString::from(owner),
                });
                if let Some(timeout) = timeout { request.set_timeout(timeout); }
                request
            };
            let count = || sink.events().iter().filter(|event| event.name == crate::matching_metrics::SEMAPHORE_CONTEXT_TIMEOUT).count();

            // A live gRPC initiating caller gets the independent 30s load error.
            // The in-process transport adapter gives the waiter a caller deadline.
            let mut first_client = client.clone();
            let first_request = request("sem", "1:first:r:1", None);
            let first = tokio::spawn(async move { first_client.acquire_semaphore(first_request).await });
            tokio::time::timeout(Duration::from_secs(2), store.scan_entered.notified()).await.unwrap();
            let error = tokio::time::timeout(Duration::from_secs(3), transport.acquire_semaphore(
                request("sem", "1:waiter:r:2", Some(Duration::from_millis(200))),
            )).await.unwrap().unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(error.message(), "context deadline exceeded");
            assert_eq!(count(), 1);
            assert_eq!(store.scan_pages.load(Ordering::SeqCst), 1);
            let error = tokio::time::timeout(Duration::from_secs(33), first).await.unwrap().unwrap().unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(error.message(), format!("load semaphore bucket {DOMAIN}/sem/0: context deadline exceeded"));
            assert_eq!(count(), 1);
            // A new scan and successful grant prove the stopped manager was removed.
            store.release.notify_one();
            let mut retry_client = client.clone();
            let retry = tokio::time::timeout(Duration::from_secs(3), retry_client.acquire_semaphore(
                request("sem", "1:retry:r:3", Some(Duration::from_secs(2))),
            )).await.unwrap().unwrap().into_inner();
            assert_eq!(retry.outcome, 1);
            assert_eq!(store.scan_pages.load(Ordering::SeqCst), 2);

            // Through the in-process transport adapter, an initiating caller's deadline expires while its independent scan
            // is parked; after the scan returns Go substitutes the context error.
            store.park_scan.store(true, Ordering::SeqCst);
            let deadline_transport = transport.clone();
            let deadline_request = request("deadline", "1:first:r:4", Some(Duration::from_millis(200)));
            let deadline = tokio::spawn(async move { deadline_transport.acquire_semaphore(deadline_request).await });
            tokio::time::timeout(Duration::from_secs(2), store.scan_entered.notified()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(350)).await;
            assert!(!deadline.is_finished(), "initiating scan must outlive its caller deadline");
            store.scan_release.notify_one();
            let error = tokio::time::timeout(Duration::from_secs(2), deadline).await.unwrap().unwrap().unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(error.message(), "context deadline exceeded");
            assert_eq!(count(), 2);
            store.release.notify_one();
            let mut retry_client = client;
            let retry = tokio::time::timeout(Duration::from_secs(3), retry_client.acquire_semaphore(
                request("deadline", "1:retry:r:5", Some(Duration::from_secs(2))),
            )).await.unwrap().unwrap().into_inner();
            assert_eq!(retry.outcome, 1);
            runtime.shutdown().await.unwrap();
            server.abort();
        }).await.expect("configured transport scenario timed out");
    }

}
