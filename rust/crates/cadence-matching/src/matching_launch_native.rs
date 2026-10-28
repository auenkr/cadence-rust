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
    matching_host_native::{NativeFailureMetric, NativeMatchingHost, NativeRequestMetric},
    matching_membership::Membership,
    matching_quota::MatchingRpcQuota,
    matching_registry_native::NativeQueueRegistry,
    matching_runtime::{DrainDuration, MatchingRuntime},
    matching_semaphore_manager_native::Registry,
    matching_semaphore_service_native::{ContextTimeoutMetric, NativeSemaphoreService},
    matching_shard_routing::MatchingShardRouter,
};

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
    request_metric: NativeRequestMetric,
    failure_metric: NativeFailureMetric,
    timeout_metric: ContextTimeoutMetric,
    observer: Option<PollObserverFactory>,
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
                .map_err(|error| Status::not_found(error.to_string()))?;
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
        domain_parse, semaphore.callback(), request_metric, failure_metric, observer,
    ));
    runtime.with_native_host(registry, host, semaphore, membership)
}
