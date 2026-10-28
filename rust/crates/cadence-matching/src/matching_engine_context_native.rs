//! Concrete dependencies for the native Matching engine's add, poll and query paths.
//!
//! Poll registration is intentionally injected: the service owner must supply the
//! real cancellation-map guard used by its RPC lifecycle.
use crate::{
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_dispatch_native::{NativeFuture, NativeMatchingApi, NativePollObserver},
    matching_engine_native::{DecisionAddContext, NativeControlContext, NativePollContext, NativePollRegistration, NativeQueryContext, NativeQueryParent},
    matching_forward_native::RemoteNativeMatching,
    matching_membership::Membership,
    matching_registry_native::NativeQueueHandle,
    matching_shard_routing::MatchingShardRouter,
};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use std::{sync::Arc, time::Duration};
use tonic::Status;

pub type PollRegistrar = Arc<dyn Fn(&GoString, &GoString, i32, &GoString) -> NativePollRegistration + Send + Sync>;
pub type PollObserverFactory = Arc<dyn Fn(&GoString, &GoString, i32) -> Option<Arc<dyn NativePollObserver>> + Send + Sync>;

/// Native engine context assembled from the same state as the gRPC Matching
/// service. Raw names stay `GoString` through config and ownership lookups.
pub struct NativeMatchingEngineContext {
    control: Arc<dyn NativeControlContext>,
    cache: Arc<MatchingDomainCache>,
    domains: cadence_persistence::InMemoryDomainStore,
    cluster: String,
    membership: Membership,
    shard_router: Option<Arc<MatchingShardRouter>>,
    routing_registry: Option<crate::matching_registry_native::NativeQueueRegistry>,
    overlay: Arc<std::sync::Mutex<crate::admin::DynamicConfigOverlay>>,
    register: PollRegistrar,
    observer: Option<PollObserverFactory>,
}

impl NativeMatchingEngineContext {
    /// Constructor for the service assembler, which already owns the concrete
    /// control context and its registry.
    pub fn with_control(
        state: AppState,
        cache: Arc<MatchingDomainCache>,
        control: Arc<dyn NativeControlContext>,
        membership: Membership,
        shard_router: Option<Arc<MatchingShardRouter>>,
        register: PollRegistrar,
        observer: Option<PollObserverFactory>,
    ) -> Self {
        Self { control, cache, domains: state.domains.clone(), cluster: state.current_cluster_name.clone(), overlay: state.admin.dynamic_config.clone(), membership, shard_router, routing_registry: None, register, observer }
    }

    /// Share the runtime's live router rather than freezing its startup value.
    pub fn with_routing_registry(mut self, registry: crate::matching_registry_native::NativeQueueRegistry) -> Self {
        if let Some(router) = self.shard_router.take() { registry.set_shard_router(router); }
        self.routing_registry = Some(registry);
        self
    }
    fn current_router(&self) -> Option<Arc<MatchingShardRouter>> {
        self.routing_registry.as_ref().and_then(|registry| registry.shard_router()).or_else(|| self.shard_router.clone())
    }
    fn active(&self, id: &GoString) -> bool {
        std::str::from_utf8(id.as_bytes()).ok()
            .and_then(|id| self.domains.is_active_in(id, &self.cluster).ok())
            .unwrap_or(true)
    }

    fn dynamic_duration(&self, key: &str, domain: &GoString, list: &GoString, kind: i32, default: i64) -> Duration {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        Duration::from_nanos(cadence_persistence::dynamic_config::task_list_duration_bytes(
            config.property_values(key), domain_name.as_deref(), list.as_bytes(), kind, default,
        ).max(0) as u64)
    }

    fn dynamic_integer(&self, key: &str, domain: &GoString, list: &GoString, kind: i32, default: i64) -> i64 {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values(key), domain_name.as_deref(), list.as_bytes(), kind, default,
        )
    }

    fn dynamic_boolean(&self, key: &str, domain: &GoString, list: &GoString, kind: i32, default: bool) -> bool {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        cadence_persistence::dynamic_config::task_list_boolean_bytes(
            config.property_values(key), domain_name.as_deref(), list.as_bytes(), kind, default,
        )
    }

    fn cached_domain_name(&self, id: &GoString) -> Option<String> {
        let id = std::str::from_utf8(id.as_bytes()).ok()?;
        let uuid = uuid::Uuid::parse_str(id).ok()?;
        self.cache.get_domain_by_id(&uuid.to_string()).ok().map(|domain| domain.info.name)
    }

    fn state_overlay(&self) -> Arc<std::sync::Mutex<crate::admin::DynamicConfigOverlay>> { self.overlay.clone() }
}


impl DecisionAddContext for NativeMatchingEngineContext {
    fn domain_active(&self, domain: uuid::Uuid) -> Result<bool, Status> {
        let id = GoString::from(domain.to_string());
        Ok(self.active(&id))
    }
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString, task_type: i32) -> i64 {
        self.dynamic_integer("matching.forwarderMaxChildrenPerNode", &GoString::from(domain.to_string()), root, task_type, 20).max(1)
    }
    fn check_write_partition(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { self.control.check_queue(key, queue) }
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
        let router = self.current_router();
        let membership = self.membership.clone();
        Box::pin(async move {
            let channel = if let Some(router) = router {
                router.remote_channel_bytes(name.as_bytes()).await.map(|(channel, _)| channel).map_err(|error| Status::unavailable(error.to_string()))?
            } else { membership.remote_channel_bytes(name.as_bytes())? };
            Ok(channel.map(|channel| Arc::new(RemoteNativeMatching(channel)) as Arc<dyn NativeMatchingApi>))
        })
    }
}

impl NativePollContext for NativeMatchingEngineContext {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> { self.control.domain_name(id) }
    fn domain_active(&self, id: &GoString) -> Result<bool, Status> { Ok(self.active(id)) }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { self.control.check_queue(key, queue) }
    fn register_poll(&self, domain: &GoString, list: &GoString, task_type: i32, poller: &GoString) -> NativePollRegistration { (self.register)(domain, list, task_type, poller) }
    fn failover(&self, domain: &GoString) -> NativeFuture<()> {
        let Ok(raw) = std::str::from_utf8(domain.as_bytes()) else { return Box::pin(std::future::pending()) };
        let raw = raw.to_owned();
        let mut subscription = self.domains.subscribe_failover(&raw);
        Box::pin(async move { let _ = subscription.changed().await; })
    }
    fn expiration(&self, domain: &GoString, list: &GoString, task_type: i32) -> Duration {
        self.dynamic_duration("matching.longPollExpirationInterval", domain, list, task_type, 60_000_000_000)
    }
    fn override_rate(&self, domain: &GoString, list: &GoString, task_type: i32) -> f64 {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_float_bytes(config.property_values("matching.overrideTaskListRps"), self.cached_domain_name(domain).as_deref(), list.as_bytes(), task_type, 0.0)
    }
    fn auto_config_enabled(&self, domain: &GoString, list: &GoString, task_type: i32) -> bool { self.dynamic_boolean("matching.enableClientAutoConfig", domain, list, task_type, false) }
    fn observer(&self, domain: &GoString, list: &GoString, task_type: i32) -> Option<Arc<dyn NativePollObserver>> { self.observer.as_ref().and_then(|factory| factory(domain, list, task_type)) }
}

impl NativeQueryContext for NativeMatchingEngineContext {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> { self.control.domain_name(id) }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> { self.control.check_queue(key, queue) }
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString) -> i64 { self.dynamic_integer("matching.forwarderMaxChildrenPerNode", &GoString::from(domain.to_string()), root, 0, 20).max(1) }
    fn remote_parent(&self, name: GoString) -> NativeFuture<Result<Option<Arc<dyn NativeQueryParent>>, Status>> {
        let router = self.current_router();
        let membership = self.membership.clone();
        Box::pin(async move {
            let channel = if let Some(router) = router {
                router.remote_channel_bytes(name.as_bytes()).await.map(|(channel, _)| channel).map_err(|error| Status::unavailable(error.to_string()))?
            } else { membership.remote_channel_bytes(name.as_bytes())? };
            Ok(channel.map(|channel| Arc::new(RemoteNativeMatching(channel)) as Arc<dyn NativeQueryParent>))
        })
    }
    fn default_deadline(&self) -> tokio::time::Instant { tokio::time::Instant::now() + Duration::from_secs(10) }
}

#[cfg(test)]
mod tests {
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};

    #[test]
    fn task_list_config_filter_keeps_invalid_utf8_distinct() {
        let configured = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON), data: b"17".to_vec(),
            }),
            filters: vec![DynamicConfigFilter {
                name: "taskListName".into(),
                value: Some(cadence_api::types::DataBlob {
                    encoding_type: Some(cadence_api::enums::EncodingType::JSON), data: br#""\ufffd""#.to_vec(),
                }),
            }],
        };
        let values = cadence_persistence::dynamic_config::PropertyValues::snapshot(vec![configured]);
        assert_eq!(cadence_persistence::dynamic_config::task_list_integer_bytes(values.clone(), None, b"\xff", 0, 1), 1);
        assert_eq!(cadence_persistence::dynamic_config::task_list_integer_bytes(values, None, "\u{fffd}".as_bytes(), 0, 1), 17);
    }
}
