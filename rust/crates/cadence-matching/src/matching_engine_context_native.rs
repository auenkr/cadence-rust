//! Concrete dependencies for the native Matching engine's add, poll and query paths.
//!
//! Poll registration is intentionally injected: the service owner must supply the
//! real cancellation-map guard used by its RPC lifecycle.
use crate::{
    handlers::AppState,
    matching_dispatch_native::{NativeFuture, NativeMatchingApi, NativePollObserver},
    matching_domain_cache::MatchingDomainCache,
    matching_engine_native::{
        DecisionAddContext, NativeControlContext, NativePollContext, NativePollRegistration,
        NativeQueryContext, NativeQueryParent,
    },
    matching_forward_native::RemoteNativeMatching,
    matching_membership::Membership,
    matching_registry_native::NativeQueueHandle,
    matching_shard_routing::MatchingShardRouter,
};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use std::{sync::Arc, time::Duration};
use tonic::Status;

pub type PollRegistrar =
    Arc<dyn Fn(&GoString, &GoString, i32, &GoString) -> NativePollRegistration + Send + Sync>;
pub type PollObserverFactory = Arc<
    dyn Fn(&GoString, &GoString, i32, i32, &GoString) -> Option<Arc<dyn NativePollObserver>>
        + Send
        + Sync,
>;

/// Create observers with the live poll request's persistence kind and isolation group.
pub fn metrics_poll_observer_factory(
    metrics: crate::matching_metrics::MatchingMetrics,
    cache: Arc<MatchingDomainCache>,
) -> PollObserverFactory {
    metrics_poll_observer_factory_with_names(
        metrics,
        Arc::new(move |id| cache.get_domain_by_id(id).ok().map(|d| d.info.name)),
    )
}

type MetricDomainNameProvider = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

fn metrics_poll_observer_factory_with_names(
    metrics: crate::matching_metrics::MatchingMetrics,
    names: MetricDomainNameProvider,
) -> PollObserverFactory {
    Arc::new(
        move |domain, list, task_type, task_list_kind, isolation_group| {
            let domain = domain
                .as_utf8()
                .ok()
                .and_then(|id| names(id))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| "_unknown_".to_owned());
            let context = crate::matching_metrics::PollMetricContext {
                domain,
                task_list: list.go_rune_string(),
                task_list_kind,
                task_type: if task_type == 1 {
                    "activity_task"
                } else {
                    "decision_task"
                },
                poller_isolation_group: isolation_group.go_rune_string(),
            };
            Some(Arc::new(
                crate::matching_host_native::NativeMetricsPollObserver::new(
                    metrics.clone(),
                    context,
                )
                .with_raw_isolation_group(isolation_group.clone()),
            ) as Arc<dyn NativePollObserver>)
        },
    )
}

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
        Self {
            control,
            cache,
            domains: state.domains.clone(),
            cluster: state.current_cluster_name.clone(),
            overlay: state.admin.dynamic_config.clone(),
            membership,
            shard_router,
            routing_registry: None,
            register,
            observer,
        }
    }

    /// Share the runtime's live router rather than freezing its startup value.
    pub fn with_routing_registry(
        mut self,
        registry: crate::matching_registry_native::NativeQueueRegistry,
    ) -> Self {
        if let Some(router) = self.shard_router.take() {
            registry.set_shard_router(router);
        }
        self.routing_registry = Some(registry);
        self
    }
    fn current_router(&self) -> Option<Arc<MatchingShardRouter>> {
        self.routing_registry
            .as_ref()
            .and_then(|registry| registry.shard_router())
            .or_else(|| self.shard_router.clone())
    }
    fn active(&self, id: &GoString) -> Result<bool, Status> {
        cached_domain_active(&self.cache, id, &self.cluster)
    }

    fn dynamic_duration(
        &self,
        key: &str,
        domain: &GoString,
        list: &GoString,
        kind: i32,
        default: i64,
    ) -> Duration {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        Duration::from_nanos(
            cadence_persistence::dynamic_config::task_list_duration_bytes(
                config.property_values(key),
                domain_name.as_deref(),
                list.as_bytes(),
                kind,
                default,
            )
            .max(0) as u64,
        )
    }

    fn dynamic_integer(
        &self,
        key: &str,
        domain: &GoString,
        list: &GoString,
        kind: i32,
        default: i64,
    ) -> i64 {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values(key),
            domain_name.as_deref(),
            list.as_bytes(),
            kind,
            default,
        )
    }

    fn dynamic_boolean(
        &self,
        key: &str,
        domain: &GoString,
        list: &GoString,
        kind: i32,
        default: bool,
    ) -> bool {
        let overlay = self.state_overlay();
        let config = overlay.lock().expect("admin lock");
        let domain_name = self.cached_domain_name(domain);
        cadence_persistence::dynamic_config::task_list_boolean_bytes(
            config.property_values(key),
            domain_name.as_deref(),
            list.as_bytes(),
            kind,
            default,
        )
    }

    fn cached_domain_name(&self, id: &GoString) -> Option<String> {
        let id = std::str::from_utf8(id.as_bytes()).ok()?;
        self.cache
            .get_domain_by_id(id)
            .ok()
            .map(|domain| domain.info.name)
    }

    fn state_overlay(&self) -> Arc<std::sync::Mutex<crate::admin::DynamicConfigOverlay>> {
        self.overlay.clone()
    }
}

/// Match Go DomainCacheEntry.IsActiveIn using the exact requested cache key.
/// A source cache miss must fail AddTask, rather than dispatch work as active.
pub(crate) fn cached_domain_active(
    cache: &MatchingDomainCache,
    id: &GoString,
    cluster: &str,
) -> Result<bool, Status> {
    let raw = id.as_utf8().map_err(|_| Status::invalid_argument("invalid domain UUID"))?;
    let domain = cache.get_domain_by_id(raw)
        .map_err(crate::matching_domain_cache::DomainCacheError::into_status)?;
    if !domain.is_global_domain {
        return Ok(true);
    }
    if domain.failover_end_time_nanos.is_some() {
        return Ok(false);
    }
    Ok(domain.replication.active_cluster_name == cluster || domain.replication.active_clusters
        .as_ref()
        .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
        .is_some_and(|scopes| scopes.values().any(|scope| {
            scope.cluster_attributes.as_ref().is_some_and(|attributes| {
                attributes.values().any(|attribute| attribute.active_cluster_name == cluster)
            })
        })))
}

impl DecisionAddContext for NativeMatchingEngineContext {
    fn domain_active(&self, domain: &GoString) -> Result<bool, Status> {
        self.active(domain)
    }
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString, task_type: i32) -> i64 {
        self.dynamic_integer(
            "matching.forwarderMaxChildrenPerNode",
            &GoString::from(domain.to_string()),
            root,
            task_type,
            20,
        )
        .max(1)
    }
    fn check_write_partition(
        &self,
        key: &TaskListKeyBytes,
        queue: &NativeQueueHandle,
    ) -> Result<(), Status> {
        self.control.check_queue(key, queue)
    }
    fn remote_parent(
        &self,
        name: GoString,
    ) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>> {
        let router = self.current_router();
        let membership = self.membership.clone();
        Box::pin(async move {
            let channel = if let Some(router) = router {
                router
                    .remote_channel_bytes(name.as_bytes())
                    .await
                    .map(|(channel, _)| channel)
                    .map_err(|error| Status::unavailable(error.to_string()))?
            } else {
                membership.remote_channel_bytes(name.as_bytes())?
            };
            Ok(channel.map(|channel| {
                Arc::new(RemoteNativeMatching(channel)) as Arc<dyn NativeMatchingApi>
            }))
        })
    }
}

impl NativePollContext for NativeMatchingEngineContext {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> {
        self.control.domain_name(id)
    }
    fn domain_active(&self, id: &GoString) -> Result<bool, Status> {
        self.active(id)
    }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> {
        self.control.check_queue(key, queue)
    }
    fn register_poll(
        &self,
        domain: &GoString,
        list: &GoString,
        task_type: i32,
        poller: &GoString,
    ) -> NativePollRegistration {
        (self.register)(domain, list, task_type, poller)
    }
    fn failover(&self, domain: &GoString) -> NativeFuture<()> {
        let Ok(raw) = std::str::from_utf8(domain.as_bytes()) else {
            return Box::pin(std::future::pending());
        };
        let raw = raw.to_owned();
        let mut subscription = self.domains.subscribe_failover(&raw);
        Box::pin(async move {
            let _ = subscription.changed().await;
        })
    }
    fn expiration(&self, domain: &GoString, list: &GoString, task_type: i32) -> Duration {
        self.dynamic_duration(
            "matching.longPollExpirationInterval",
            domain,
            list,
            task_type,
            60_000_000_000,
        )
    }
    fn override_rate(&self, domain: &GoString, list: &GoString, task_type: i32) -> f64 {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_float_bytes(
            config.property_values("matching.overrideTaskListRps"),
            self.cached_domain_name(domain).as_deref(),
            list.as_bytes(),
            task_type,
            0.0,
        )
    }
    fn auto_config_enabled(&self, domain: &GoString, list: &GoString, task_type: i32) -> bool {
        self.dynamic_boolean(
            "matching.enableClientAutoConfig",
            domain,
            list,
            task_type,
            false,
        )
    }
    fn observer_with_context(
        &self,
        domain: &GoString,
        list: &GoString,
        task_type: i32,
        task_list_kind: i32,
        isolation_group: &GoString,
    ) -> Option<Arc<dyn NativePollObserver>> {
        self.observer
            .as_ref()
            .and_then(|factory| factory(domain, list, task_type, task_list_kind, isolation_group))
    }
}

impl NativeQueryContext for NativeMatchingEngineContext {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> {
        self.control.domain_name(id)
    }
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status> {
        self.control.check_queue(key, queue)
    }
    fn forward_degree(&self, domain: uuid::Uuid, root: &GoString) -> i64 {
        self.dynamic_integer(
            "matching.forwarderMaxChildrenPerNode",
            &GoString::from(domain.to_string()),
            root,
            0,
            20,
        )
        .max(1)
    }
    fn remote_parent(
        &self,
        name: GoString,
    ) -> NativeFuture<Result<Option<Arc<dyn NativeQueryParent>>, Status>> {
        let router = self.current_router();
        let membership = self.membership.clone();
        Box::pin(async move {
            let channel = if let Some(router) = router {
                router
                    .remote_channel_bytes(name.as_bytes())
                    .await
                    .map(|(channel, _)| channel)
                    .map_err(|error| Status::unavailable(error.to_string()))?
            } else {
                membership.remote_channel_bytes(name.as_bytes())?
            };
            Ok(channel.map(|channel| {
                Arc::new(RemoteNativeMatching(channel)) as Arc<dyn NativeQueryParent>
            }))
        })
    }
    fn default_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};

    #[test]
    fn isolation_success_compares_raw_names_before_label_sanitization() {
        use crate::matching_metrics::{
            MatchingMetrics, RecordingMetricSink, ISOLATION_SUCCESS, ISOLATION_TASK_MATCH,
        };
        use std::collections::BTreeMap;
        let domain = GoString::from(uuid::Uuid::nil().to_string());
        let task_group = GoString::from(b"zone-\xff".as_slice());
        for (poller, matched) in [
            (task_group.clone(), true),
            (GoString::from(b"zone-\xfe".as_slice()), false),
            (GoString::from("zone-\u{fffd}"), false),
        ] {
            let sink = Arc::new(RecordingMetricSink::default());
            let factory = metrics_poll_observer_factory_with_names(
                MatchingMetrics::new(sink.clone()),
                Arc::new(|_| Some("domain-a".into())),
            );
            let observer = factory(&domain, &GoString::from("list"), 0, 0, &poller).unwrap();
            let partition =
                BTreeMap::from([(GoString::from("isolation-group"), task_group.clone())]);
            observer.poll_match(false, false, Some(&partition));
            let events = sink.events();
            assert!(events
                .iter()
                .any(|event| event.name == ISOLATION_TASK_MATCH));
            assert_eq!(
                events.iter().any(|event| event.name == ISOLATION_SUCCESS),
                matched
            );
        }
    }

    #[test]
    fn poll_observers_keep_normal_and_sticky_tags_distinct() {
        use crate::matching_metrics::{MatchingMetrics, RecordingMetricSink, POLL_SUCCESS};
        let sink = Arc::new(RecordingMetricSink::default());
        let factory = metrics_poll_observer_factory_with_names(
            MatchingMetrics::new(sink.clone()),
            Arc::new(|_| Some("domain-a".into())),
        );
        let domain = GoString::from(uuid::Uuid::nil().to_string());
        for (list, kind, group) in [("normal-list", 0, "group-a"), ("sticky-list", 1, "group-b")] {
            factory(
                &domain,
                &GoString::from(list),
                0,
                kind,
                &GoString::from(group),
            )
            .unwrap()
            .poll_success(false);
        }
        let events = sink.events();
        for list in ["normal_list", "__sticky__"] {
            assert!(events.iter().any(|e| e.name == POLL_SUCCESS
                && e.tags.get("tasklist").is_some_and(|v| v == list)
                && e.tags.get("domain").is_some_and(|v| v == "domain-a")));
        }
    }

    #[test]
    fn task_list_config_filter_keeps_invalid_utf8_distinct() {
        let configured = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: b"17".to_vec(),
            }),
            filters: vec![DynamicConfigFilter {
                name: "taskListName".into(),
                value: Some(cadence_api::types::DataBlob {
                    encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                    data: br#""\ufffd""#.to_vec(),
                }),
            }],
        };
        let values =
            cadence_persistence::dynamic_config::PropertyValues::snapshot(vec![configured]);
        assert_eq!(
            cadence_persistence::dynamic_config::task_list_integer_bytes(
                values.clone(),
                None,
                b"\xff",
                0,
                1
            ),
            1
        );
        assert_eq!(
            cadence_persistence::dynamic_config::task_list_integer_bytes(
                values,
                None,
                "\u{fffd}".as_bytes(),
                0,
                1
            ),
            17
        );
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cadence_api::types::{DomainInfo, DomainConfiguration, DomainReplicationConfiguration};
    use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
    use crate::matching_domain_cache::{DomainMetadataSource, DomainMetadataSourceError, DomainMetadataPage, DomainErrorClass};

    pub(crate) fn record(id: usize, notification_version: i64) -> MatchingDomainMetadata {
        let name = format!("domain-{id}");
        MatchingDomainMetadata {
            info: DomainInfo {
                name,
                uuid: format!("00000000-0000-0000-0000-{id:012x}"),
                ..DomainInfo::default()
            },
            configuration: DomainConfiguration::default(),
            replication: DomainReplicationConfiguration::default(),
            is_global_domain: false,
            archival_bucket: String::new(),
            status_raw: 0,
            archival_status_raw: 0,
            history_archival_status_raw: 0,
            visibility_archival_status_raw: 0,
            config_version: notification_version,
            notification_version,
            failover_notification_version: notification_version,
            failover_version: notification_version,
            previous_failover_version: -1,
            failover_end_time_nanos: None,
            last_updated_time_nanos: 0,
            bad_binaries: None,
            isolation_groups: None,
            async_workflow_config: None,
            active_clusters_config: None,
            decoded_bad_binaries: None,
            decoded_isolation_groups: None,
            decoded_async_workflow_config: None,
            config_decode_error: None,
        }
    }

    struct Source(Vec<MatchingDomainMetadata>);
    impl DomainMetadataSource for Source {
        fn metadata_version(&self, _: Duration) -> Result<i64, DomainMetadataSourceError> { Ok(1) }
        fn list_domains(&self, _: usize, _: Option<&[u8]>, _: Duration) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
            Ok(DomainMetadataPage { domains: self.0.clone(), next_page_token: None })
        }
        fn get_domain(&self, _: Option<&str>, id: Option<&str>, _: Duration) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
            let id = id.unwrap_or_default();
            let parsed = uuid::Uuid::parse_str(id).map_err(|_| DomainMetadataSourceError::permanent("UUID is invalid.").with_class(DomainErrorClass::BadRequest))?;
            self.0.iter().find(|d| uuid::Uuid::parse_str(&d.info.uuid).ok() == Some(parsed)).cloned()
                .ok_or_else(|| DomainMetadataSourceError::permanent("source domain missing").with_class(DomainErrorClass::NotFound))
        }
    }
    pub(crate) fn cache(records: Vec<MatchingDomainMetadata>) -> Arc<MatchingDomainCache> {
        let cache = Arc::new(MatchingDomainCache::new(Arc::new(Source(records))));
        cache.refresh_domains().unwrap();
        cache
    }

    #[test]
    fn activity_uses_exact_live_cache_entry_and_go_failover_rules() {
        let mut domain = record(0xabc, 0);
        let id = GoString::from(domain.info.uuid.clone());
        assert!(cached_domain_active(&cache(vec![domain.clone()]), &id, "other").unwrap());
        domain.is_global_domain = true;
        domain.replication.active_cluster_name = "active".into();
        assert!(cached_domain_active(&cache(vec![domain.clone()]), &id, "active").unwrap());
        assert!(!cached_domain_active(&cache(vec![domain.clone()]), &id, "other").unwrap());
        domain.replication.active_clusters = Some(serde_json::from_value(serde_json::json!({
            "activeClustersByClusterAttribute": {"region": {"clusterAttributes": {
                "r": {"activeClusterName": "other"}
            }}}
        })).unwrap());
        assert!(cached_domain_active(&cache(vec![domain.clone()]), &id, "other").unwrap());
        domain.failover_end_time_nanos = Some(0);
        for cluster in ["active", "other"] {
            assert!(!cached_domain_active(&cache(vec![domain.clone()]), &id, cluster).unwrap());
        }
        let cached = cache(vec![domain]);
        for alias in [id.as_utf8().unwrap().to_uppercase(), id.as_utf8().unwrap().replace('-', "")] {
            let error = cached_domain_active(&cached, &alias.into(), "active").unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(error.message(), "DefaultDomainCache encounter case where domain exists but cannot be loaded");
        }
        let error = cached_domain_active(&cached, &uuid::Uuid::from_u128(99).to_string().into(), "active").unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
        assert_eq!(error.message(), "source domain missing");
    }
}
