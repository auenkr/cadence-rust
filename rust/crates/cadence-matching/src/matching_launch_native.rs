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
    matching_registry_native::{NativeLocalPollServiceBuilder, NativeQueueRegistry},
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
    pub scaler_metrics: crate::matching_scaler_native::ScalerMetrics,
    pub local_service: NativeLocalPollServiceBuilder,
}

/// Construct the production registry and host without opening a legacy queue
/// owner. This is an integration seam, not a production switch: the local
/// service builder must be acyclic and lifecycle and ownership must be wired
/// before the launcher exposes the resulting runtime.
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
    )
    .with_metrics(deps.metrics.clone())
    .with_poll_observer(registry_poll_observer(deps.metrics.clone()))
    .with_live_routing(
        deps.membership.clone(),
        None,
        deps.local_service,
        crate::matching_registry_routing_native::partition_refresher(deps.membership.clone()),
    );
    let registry = crate::matching_scaler_runtime_native::attach_native_scaler(
        registry,
        &state,
        deps.cache.clone(),
        deps.membership.clone(),
        None,
        deps.scaler_metrics,
    );
    let timeout_metric = semaphore_timeout_metric(deps.cache.clone(), deps.metrics.clone());
    assemble(
        state, deps.cache, registry, deps.semaphore_store, deps.membership,
        None, deps.quota, deps.domain_parse, deps.drain, deps.metrics,
        timeout_metric, Some(deps.poll_observer),
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

fn registry_poll_observer(
    metrics: crate::matching_metrics::MatchingMetrics,
) -> crate::matching_registry_native::NativePollObserverProvider {
    Arc::new(move |context| {
        Arc::new(crate::matching_host_native::NativeMetricsPollObserver::new(
            metrics.clone(), context.clone(),
        ))
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
        domain_parse, semaphore.callback(), metrics, observer,
    ));
    runtime.with_native_host(registry, host, semaphore, membership)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_metrics::{MatchingMetrics, PollMetricContext, RecordingMetricSink, POLLER_PER_TASK_LIST};

    #[test]
    fn registry_observer_reports_on_the_supplied_sink_and_context() {
        let sink = Arc::new(RecordingMetricSink::default());
        let provider = registry_poll_observer(MatchingMetrics::new(sink.clone()));
        let context = PollMetricContext {
            domain: "example".into(),
            task_list: "queue".into(),
            task_list_kind: 0,
            task_type: "decision_task",
            poller_isolation_group: String::new(),
        };
        provider(&context).poller_count(3);
        let events = sink.events();
        assert!(events.iter().any(|event| event.name == POLLER_PER_TASK_LIST
            && event.value == crate::matching_metrics::MetricValue::Number(3.0)
            && event.tags.get("domain").is_some_and(|domain| domain == "example")));
    }
}
