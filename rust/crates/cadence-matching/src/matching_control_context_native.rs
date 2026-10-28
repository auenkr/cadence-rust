//! Production control context for the byte-preserving native Matching engine.
//!
//! The adapter shares the service's domain cache and live dynamic-config
//! overlay. Ownership checks are synchronous because the control engine's
//! admission seam is synchronous; shard-distributor owner lookups remain
//! asynchronous and are used only for partition metadata.

use crate::{
    admin::DynamicConfigOverlay,
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_engine_native::NativeControlContext as NativeControlContextApi,
    matching_membership::Membership,
    matching_registry_native::{NativeQueueHandle, NativeQueueRegistry},
    matching_shard_routing::MatchingShardRouter,
};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use std::sync::{Arc, Mutex};
use tonic::{Code, Status};

/// Production implementation of the native Matching control dependencies.
///
/// Construct this alongside the native queue registry and pass it to the
/// control RPC entry points. The registry is retained so this context cannot
/// accidentally be assembled for a different Matching instance; queue
/// admission is checked against the same live membership ring used by its
/// owner for task-list routing.
pub struct NativeMatchingControlContext {
    cache: Arc<MatchingDomainCache>,
    domains: cadence_persistence::InMemoryDomainStore,
    registry: NativeQueueRegistry,
    membership: Membership,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
}

impl NativeMatchingControlContext {
    pub fn new(
        state: AppState,
        cache: Arc<MatchingDomainCache>,
        registry: NativeQueueRegistry,
        membership: Membership,
        shard_router: Option<Arc<MatchingShardRouter>>,
    ) -> Self {
        let overlay = state.admin.dynamic_config.clone();
        if let Some(router) = shard_router {
            registry.set_shard_router(router);
        }
        Self {
            cache,
            domains: state.domains.clone(),
            registry,
            membership,
            overlay,
        }
    }

    fn cached_domain_name(&self, id: &GoString) -> Option<String> {
        let id = std::str::from_utf8(id.as_bytes()).ok()?;
        let uuid = uuid::Uuid::parse_str(id).ok()?;
        self.cache
            .get_domain_by_id(&uuid.to_string())
            .ok()
            .map(|domain| domain.info.name)
    }

    fn task_list_boolean(
        &self,
        property: &str,
        domain: &GoString,
        list: &GoString,
        task_type: i32,
        default: bool,
    ) -> bool {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_boolean_bytes(
            config.property_values(property),
            self.cached_domain_name(domain).as_deref(),
            list.as_bytes(),
            task_type,
            default,
        )
    }
}

impl NativeControlContextApi for NativeMatchingControlContext {
    fn domain_id(&self, name: &GoString) -> Result<uuid::Uuid, Status> {
        let name = std::str::from_utf8(name.as_bytes())
            .map_err(|_| Status::invalid_argument("Domain name is not valid UTF-8."))?;
        if name.is_empty() {
            return Err(Status::invalid_argument("Domain name is not valid."));
        }
        let domain = self
            .cache
            .get_domain(name)
            .map_err(crate::matching_domain_cache::DomainCacheError::into_status)?;
        self.domains.insert_matching_metadata_if_missing(&domain);
        uuid::Uuid::parse_str(&domain.info.uuid)
            .map_err(|_| Status::internal("Domain cache contains an invalid domain UUID."))
    }

    fn domain_name(&self, id: &GoString) -> Result<GoString, Status> {
        let raw = std::str::from_utf8(id.as_bytes())
            .map_err(|_| Status::invalid_argument("invalid domain UUID"))?;
        let uuid = uuid::Uuid::parse_str(raw)
            .map_err(|error| Status::new(Code::InvalidArgument, error.to_string()))?;
        let domain = self
            .cache
            .get_domain_by_id(&uuid.to_string())
            .map_err(crate::matching_domain_cache::DomainCacheError::into_status)?;
        self.domains.insert_matching_metadata_if_missing(&domain);
        Ok(GoString::from(domain.info.name))
    }

    fn check_queue(
        &self,
        key: &TaskListKeyBytes,
        _queue: &NativeQueueHandle,
    ) -> Result<(), Status> {
        // Keep the registry as part of this concrete context's construction
        // contract, and route by the exact raw name used as the registry key.
        self.registry.check_task_list_ownership(&key.name, true)
    }

    fn return_all_kinds(&self) -> bool {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::boolean(
            config.property_values("matching.matchingReturnAllTaskListKinds"),
            None,
            false,
        )
    }

    fn scaler_enabled(&self, domain: &GoString, list: &GoString, task_type: i32) -> bool {
        self.task_list_boolean(
            "matching.enableAdaptiveScaler",
            domain,
            list,
            task_type,
            false,
        )
    }

    fn isolation_groups(&self) -> Vec<GoString> {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::string_list(
            config.property_values("system.allIsolationGroups"),
        )
        .into_iter()
        .map(GoString::from)
        .collect()
    }

    fn poller_window_nanos(&self, domain: &GoString, list: &GoString, task_type: i32) -> i64 {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_duration_bytes(
            config.property_values("matching.taskIsolationPollerWindow"),
            self.cached_domain_name(domain).as_deref(),
            list.as_bytes(),
            task_type,
            2_000_000_000,
        )
    }

    fn write_partition_count(&self, domain: &GoString, root: &GoString, task_type: i32) -> i64 {
        let config = self.overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values("matching.numTasklistWritePartitions"),
            self.cached_domain_name(domain).as_deref(),
            root.as_bytes(),
            task_type,
            1,
        )
        .max(1)
    }

    fn owner_host(
        &self,
        partition: GoString,
    ) -> crate::matching_dispatch_native::NativeFuture<Result<GoString, Status>> {
        let router = self.registry.shard_router();
        let membership = self.membership.clone();
        Box::pin(
            async move { resolve_owner_host(&membership, router.as_deref(), &partition).await },
        )
    }
}

async fn resolve_owner_host(
    membership: &Membership,
    router: Option<&MatchingShardRouter>,
    partition: &GoString,
) -> Result<GoString, Status> {
    let address = if let Some(router) = router {
        router
            .resolve_bytes(partition.as_bytes())
            .await
            .map(|(host, _)| host.address)
            .map_err(|error| Status::unavailable(error.to_string()))?
    } else {
        membership.owner_bytes(partition.as_bytes())?.address
    };
    Ok(GoString::from(address))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
    use cadence_common::membership::Host;

    #[tokio::test]
    async fn owner_fallback_routes_using_exact_raw_partition_bytes() {
        let membership = Membership::new(
            "local",
            vec![Host {
                address: "matching.internal:7934".into(),
                identity: "local".into(),
                grpc_address: "127.0.0.1:7934".into(),
            }],
        );
        let raw = GoString::from(&b"root-\xff#0"[..]);
        let owner = resolve_owner_host(&membership, None, &raw).await.unwrap();
        assert_eq!(owner.as_bytes(), b"matching.internal:7934");
    }

    #[test]
    fn byte_filter_does_not_match_replacement_character() {
        let value = DynamicConfigValue {
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
        let values = cadence_persistence::dynamic_config::PropertyValues::snapshot(vec![value]);
        assert_eq!(
            cadence_persistence::dynamic_config::task_list_integer_bytes(
                values.clone(),
                None,
                b"\xff",
                0,
                1,
            ),
            1
        );
        assert_eq!(
            cadence_persistence::dynamic_config::task_list_integer_bytes(
                values,
                None,
                "\u{fffd}".as_bytes(),
                0,
                1,
            ),
            17
        );
    }

    #[tokio::test]
    async fn cold_lookup_publishes_standby_domain_metadata() {
        use crate::matching_domain_cache::{
            DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError,
        };
        use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
        use std::time::Duration;
        struct Source(MatchingDomainMetadata);
        impl DomainMetadataSource for Source {
            fn metadata_version(&self, _: Duration) -> Result<i64, DomainMetadataSourceError> {
                Ok(2)
            }
            fn list_domains(
                &self,
                _: usize,
                _: Option<&[u8]>,
                _: Duration,
            ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
                Ok(DomainMetadataPage {
                    domains: vec![self.0.clone()],
                    next_page_token: None,
                })
            }
            fn get_domain(
                &self,
                _: Option<&str>,
                _: Option<&str>,
                _: Duration,
            ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
                Ok(self.0.clone())
            }
        }
        let metadata = MatchingDomainMetadata {
            info: cadence_api::types::DomainInfo {
                name: "cold-standby".into(),
                uuid: uuid::Uuid::from_u128(1).to_string(),
                ..Default::default()
            },
            configuration: Default::default(),
            replication: cadence_api::types::DomainReplicationConfiguration {
                active_cluster_name: "remote".into(),
                ..Default::default()
            },
            is_global_domain: true,
            archival_bucket: String::new(),
            status_raw: 0,
            archival_status_raw: 0,
            history_archival_status_raw: 0,
            visibility_archival_status_raw: 0,
            config_version: 1,
            notification_version: 1,
            failover_notification_version: 1,
            failover_version: 1,
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
        };
        for by_name in [false, true] {
            let state = AppState::default();
            let domains = state.domains.clone();
            let id = metadata.info.uuid.clone();
            assert!(domains.is_active_in(&id, "local").is_err());
            let cache = Arc::new(MatchingDomainCache::new(Arc::new(Source(metadata.clone()))));
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                cadence_persistence::matching_sqlite::SqliteTaskStore::create(
                    directory.path().join("cold.db"),
                    cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
                )
                .unwrap(),
            );
            let history = crate::matching_history::MatchingHistoryClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
                Duration::from_secs(1),
                1024,
            );
            let registry =
                NativeQueueRegistry::new(store, history, std::num::NonZeroU32::new(100).unwrap());
            let context = NativeMatchingControlContext::new(
                state,
                cache,
                registry,
                Membership::new("local", vec![]),
                None,
            );
            if by_name {
                context.domain_id(&GoString::from("cold-standby")).unwrap();
            } else {
                context.domain_name(&GoString::from(id.as_str())).unwrap();
            }
            assert!(!domains.is_active_in(&id, "local").unwrap());
            assert!(domains.is_active_in(&id, "remote").unwrap());
        }
    }

    #[tokio::test]
    async fn cache_lookup_preserves_typed_failures() {
        use crate::matching_domain_cache::DomainErrorClass;
        struct EmptySource(DomainErrorClass);
        impl crate::matching_domain_cache::DomainMetadataSource for EmptySource {
            fn metadata_version(
                &self,
                _: std::time::Duration,
            ) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> {
                Ok(0)
            }
            fn list_domains(
                &self,
                _: usize,
                _: Option<&[u8]>,
                _: std::time::Duration,
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
                _: std::time::Duration,
            ) -> Result<
                cadence_persistence::matching_domain_metadata::MatchingDomainMetadata,
                crate::matching_domain_cache::DomainMetadataSourceError,
            > {
                Err(
                    crate::matching_domain_cache::DomainMetadataSourceError::permanent(
                        "lookup failure",
                    )
                    .with_class(self.0),
                )
            }
        }
        for (class, expected) in [
            (DomainErrorClass::NotFound, tonic::Code::NotFound),
            (DomainErrorClass::Internal, tonic::Code::Internal),
            (DomainErrorClass::BadRequest, tonic::Code::InvalidArgument),
            (DomainErrorClass::DbUnavailable, tonic::Code::Internal),
        ] {
            let cache = Arc::new(MatchingDomainCache::new(Arc::new(EmptySource(class))));
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                cadence_persistence::matching_sqlite::SqliteTaskStore::create(
                    directory.path().join("cache.db"),
                    cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
                )
                .unwrap(),
            );
            let history = crate::matching_history::MatchingHistoryClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
                std::time::Duration::from_secs(1),
                1024,
            );
            let registry =
                NativeQueueRegistry::new(store, history, std::num::NonZeroU32::new(100).unwrap());
            let context = NativeMatchingControlContext::new(
                AppState::default(),
                cache,
                registry,
                Membership::new("local", vec![]),
                None,
            );
            for status in [
                context.domain_id(&GoString::from("absent")).unwrap_err(),
                context
                    .domain_name(&GoString::from("00000000-0000-0000-0000-000000000001"))
                    .unwrap_err(),
            ] {
                assert_eq!(status.code(), expected);
                assert_eq!(status.message(), "lookup failure");
                if class == DomainErrorClass::NotFound {
                    assert!(
                        !status.details().is_empty(),
                        "missing Cadence EntityNotExistsError details"
                    );
                }
            }
        }
    }
}
