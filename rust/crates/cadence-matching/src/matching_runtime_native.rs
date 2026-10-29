//! Production adapters for the byte-preserving Matching registry.
//!
//! Queue identity and task-list dynamic-config filters remain raw bytes,
//! including invalid UTF-8. The launcher owns registration and listeners.

use crate::{
    admin::DynamicConfigOverlay,
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_history::MatchingHistoryClient,
    matching_reader::ReaderLive,
    matching_reader_native::NativeIsolationConfig,
    matching_registry_native::{NativeDomainHooks, NativeFatalSinks, NativeQueueRegistry},
};
use cadence_common::go_string::GoString;
use cadence_persistence::{
    matching_domain_metadata::MatchingDomainMetadata,
    matching_native_task_store::MatchingTaskStoreBytes,
};
use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
};
use tonic::Status;

/// Assemble native registry providers from the same domain/config state used
/// by the legacy Matching builder.
pub fn native_queue_registry(
    store: Arc<dyn MatchingTaskStoreBytes>,
    history: MatchingHistoryClient,
    cache: Arc<MatchingDomainCache>,
    state: &AppState,
    cluster: impl Into<String>,
    range_size: NonZeroU32,
    fatal: NativeFatalSinks,
) -> NativeQueueRegistry {
    let overlay = state.admin.dynamic_config.clone();
    let history = history.with_record_started_timeout_bytes(record_started_timeout_provider(
        cache.clone(),
        overlay.clone(),
    ));
    let cluster = cluster.into();

    let hooks = NativeDomainHooks {
        is_active: {
            let cache = cache.clone();
            let cluster = cluster.clone();
            // Go taskReader.newDispatchContext assumes active on lookup error
            // only for its timeout. Dispatch still resolves source metadata.
            Arc::new(move |id| domain_is_active(&cache, id, &cluster))
        },
        domain_name: {
            let cache = cache.clone();
            Arc::new(move |id| domain_by_raw_id(&cache, id).ok().map(|d| d.info.name))
        },
        standby_domains: {
            let cache = cache.clone();
            let cluster = cluster.clone();
            Arc::new(move |id| {
                let domain = domain_by_raw_id(&cache, id)?;
                Ok(crate::matching_standby_native::StandbyDomainNative {
                    active_in_cluster:
                        crate::matching_engine_context_native::metadata_domain_active(
                            &domain, &cluster,
                        ),
                    active_active: domain.replication.active_clusters.is_some(),
                    name: GoString::from(domain.info.name),
                })
            })
        },
    };

    let standby = standby_provider(cache.clone(), overlay.clone());
    let partition_count = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.numTasklistReadPartitions",
        1,
    );
    let cache_enabled = boolean_provider(
        cache.clone(),
        overlay.clone(),
        "matching.enableGetNumberOfPartitionsFromCache",
        false,
    );
    let sync_wait = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.localTaskWaitTime",
        10_000_000,
    );
    let local_poll_wait = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.localPollWaitTime",
        10_000_000,
    );
    let forward_degree = {
        let value = integer_provider(
            cache.clone(),
            overlay.clone(),
            "matching.forwarderMaxChildrenPerNode",
            20,
        );
        Arc::new(move |domain: &GoString, task: &GoString, task_type: i32| {
            value(domain, task, task_type).max(1)
        })
    };
    let forward_polls = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.forwarderMaxOutstandingPolls",
        1,
    );
    let forward_tasks = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.forwarderMaxOutstandingTasks",
        1,
    );
    let forward_rate = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.forwarderMaxRatePerSecond",
        10,
    );
    let qps_interval = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.qpsTrackerInterval",
        crate::matching_registry_native::DEFAULT_QPS_TRACKER_INTERVAL_NANOS,
    );
    let min_burst = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.minTaskThrottlingBurstSize",
        crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE,
    );
    let update_ack = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.updateAckInterval",
        60_000_000_000,
    );
    let idle_check = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.idleTasklistCheckInterval",
        300_000_000_000,
    );
    let delete_batch = integer_provider(
        cache.clone(),
        overlay.clone(),
        "matching.maxTaskDeleteBatchSize",
        100,
    );
    let writer_live = writer_live_provider(cache.clone(), overlay.clone());
    let reader_live = reader_properties_provider(cache.clone(), overlay.clone());
    let isolation = {
        let cache = cache.clone();
        let overlay = overlay.clone();
        let domains = state.domains.clone();
        Arc::new(move |domain: &GoString, task: &GoString, task_type: i32, kind: i16,
                       pollers: Arc<crate::matching_dispatch_native::NativePollerManager>,
                       partitions: Arc<crate::matching_partition_native::NativePartitionCache>| {
            let name = cached_domain_name(&cache, domain);
            let config = overlay.lock().expect("admin lock");
            let enabled = cadence_persistence::dynamic_config::boolean(
                config.property_values("system.enableTasklistIsolation"), name.as_deref(), false);
            let raw_groups = cadence_persistence::dynamic_config::string_list(
                config.property_values("system.allIsolationGroups"));
            let groups: Vec<GoString> = if enabled && kind != 1 {
                raw_groups.into_iter().map(GoString::from).collect()
            } else { Vec::new() };
            let known = groups.clone();
            let reload_name = name.clone();
            let reload_overlay = overlay.clone();
            let selector_overlay = overlay.clone();
            let selector_domains = domains.clone();
            let selector_task = task.clone();
            let selector_name = name.clone();
            NativeIsolationConfig {
                groups,
                should_reload: Arc::new(move || {
                    let config = reload_overlay.lock().expect("admin lock");
                    cadence_persistence::dynamic_config::boolean(config.property_values("system.enableTasklistIsolation"), reload_name.as_deref(), false) != enabled
                }),
                select: Arc::new(move |data| {
                    let requested = data.partition_config.as_ref().and_then(|map| map.get(b"isolation-group".as_slice())).cloned().unwrap_or_default();
                    let now = crate::grpc_matching::matching_now();
                    let (window, total_duration) = {
                        let config = selector_overlay.lock().expect("admin lock");
                        let duration = |key, default| cadence_persistence::dynamic_config::task_list_duration_bytes(
                            config.property_values(key), selector_name.as_deref(), selector_task.as_bytes(), task_type, default,
                        );
                        (duration("matching.taskIsolationPollerWindow", 2_000_000_000),
                         duration("matching.taskIsolationDuration", 200_000_000))
                    };
                    let drained = selector_name.as_deref().ok_or(()).and_then(|name| selector_domains.domain_isolation_groups(name).map_err(|_| ())).map(|domain_config| {
                        domain_config.into_iter().flat_map(|c| c.isolation_groups).any(|g| g.name.as_bytes() == requested.as_bytes() && g.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                        || selector_domains.global_isolation_groups().into_iter().flat_map(|c| c.isolation_groups).any(|g| g.name.as_bytes() == requested.as_bytes() && g.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                    });
                    let partition_id = cadence_common::task_list::TaskListNameBytes::parse(selector_task.as_bytes()).map(|n| n.partition() as i32).unwrap_or_default();
                    let partition_groups = partitions.get().and_then(|c| c.read_partitions.get(&partition_id).cloned());
                    let recent = pollers.lock().expect("poller history lock").has_group_after(requested.as_bytes(), now.saturating_sub(window));
                    let (group, duration, cause) = select_isolation_bytes(
                        enabled, kind == 1, &requested, known.iter().any(|g| g == &requested), drained, recent,
                        partition_groups.as_deref(), total_duration, now.saturating_sub(data.created_nanos),
                    );
                    (group, duration, cause)
                }),
            }
        }) as crate::matching_registry_native::NativeIsolationProvider
    };

    NativeQueueRegistry::new(store, history, range_size)
        .with_domain_hooks(hooks)
        .with_standby_enabled(standby)
        .with_partition_count(partition_count)
        .with_partition_cache_enabled(cache_enabled)
        .with_sync_wait(sync_wait)
        .with_local_poll_wait(local_poll_wait)
        .with_forward_degree(forward_degree)
        .with_forward_polls(forward_polls)
        .with_forward_tasks(forward_tasks, forward_rate)
        .with_qps_interval(qps_interval)
        .with_reader_live(reader_live)
        .with_min_burst(min_burst)
        .with_update_ack_interval(update_ack)
        .with_idle_tasklist_check_interval(idle_check)
        .with_max_task_delete_batch_size(delete_batch)
        .with_writer_live(writer_live)
        .with_isolation(isolation)
        .with_fatal_sinks(fatal)
}

/// Complete the registry's live attachments before constructing the host.
/// Publish the resulting host as `Arc<dyn cadence_proto::native::NativeMatchingApi>`
/// through the returned slot before opening admission. The runtime must retain
/// the strong host; the slot and the registry retain only weak service references.
#[allow(clippy::too_many_arguments)]
pub fn attach_live_native_registry(
    registry: NativeQueueRegistry,
    state: &AppState,
    cache: Arc<MatchingDomainCache>,
    membership: crate::matching_membership::Membership,
    router: Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>,
    metrics: crate::matching_metrics::MatchingMetrics,
) -> (
    NativeQueueRegistry,
    crate::matching_registry_routing_native::NativeServiceSlot,
) {
    let slot = crate::matching_registry_routing_native::NativeServiceSlot::default();
    let scaler_metrics = crate::matching_scaler_runtime_native::scoped_scaler_metrics(
        metrics.clone(),
        cache.clone(),
    );
    let poll_metrics = metrics.clone();
    let registry = registry
        .with_metrics(metrics)
        .with_poll_observer(Arc::new(move |context| {
            Arc::new(crate::matching_host_native::NativeMetricsPollObserver::new(
                poll_metrics.clone(),
                context.clone(),
            ))
        }));
    let registry = registry.with_live_routing_factory(
        membership.clone(),
        router.clone(),
        slot.factory(),
        crate::matching_registry_routing_native::partition_refresher(membership.clone()),
    );
    let registry = crate::matching_scaler_runtime_native::attach_scoped_native_scaler(
        registry,
        state,
        cache,
        membership,
        router,
        scaler_metrics,
    );
    (registry, slot)
}

/// Byte-preserving port of `TaskIsolation::select_with_cause`; configured
/// metadata names are UTF-8 bytes, while task group names stay arbitrary Go bytes.
#[allow(clippy::too_many_arguments)]
fn select_isolation_bytes(
    enabled: bool,
    sticky: bool,
    group: &GoString,
    known: bool,
    drained: Result<bool, ()>,
    recent_poller: bool,
    partition_groups: Option<&[GoString]>,
    total_duration: i64,
    task_latency: i64,
) -> (GoString, i64, Option<&'static str>) {
    if !enabled || sticky || group.is_empty() {
        return (GoString::default(), 0, None);
    }
    if drained.is_err() {
        return (GoString::default(), 0, Some("error"));
    }
    if drained == Ok(true) {
        return (GoString::default(), 0, Some("group_drained"));
    }
    if !known {
        return (GoString::default(), 0, Some("group_unknown"));
    }
    if !recent_poller {
        return (GoString::default(), 0, Some("no_recent_pollers"));
    }
    if !partition_groups
        .is_some_and(|groups| groups.is_empty() || groups.iter().any(|g| g == group))
    {
        return (GoString::default(), 0, Some("partition_change"));
    }
    if total_duration == 0 {
        return (group.clone(), 0, None);
    }
    let remaining = total_duration.saturating_sub(task_latency);
    if remaining > cadence_common::task_isolation::MINIMUM_ISOLATION_NANOS {
        (group.clone(), remaining, None)
    } else {
        (GoString::default(), 0, Some("expired"))
    }
}

fn reader_properties_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
) -> crate::matching_registry_native::NativeReaderPropertiesProvider {
    let dispatch_timeout = duration_provider(
        cache.clone(),
        overlay.clone(),
        "matching.asyncTaskDispatchTimeout",
        crate::matching_reader::DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS,
    );
    Arc::new(move |domain: &GoString, task: &GoString, task_type| {
        let name = cached_domain_name(&cache, domain);
        let scoped = |key: &'static str, default| {
            let overlay = overlay.clone();
            let name = name.clone();
            let task = task.clone();
            Arc::new(move || {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer_bytes(
                    config.property_values(key),
                    name.as_deref(),
                    task.as_bytes(),
                    task_type,
                    default,
                )
            }) as crate::matching_reader::LiveInt
        };
        let global = |key: &'static str, default| {
            let overlay = overlay.clone();
            Arc::new(move || {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::integer(
                    config.property_values(key),
                    None,
                    default,
                )
            }) as crate::matching_reader::LiveInt
        };
        let ack_overlay = overlay.clone();
        let ack_name = name.clone();
        let ack_task = task.clone();
        ReaderLive {
            batch_size: scoped(
                "matching.getTasksBatchSize",
                crate::matching_reader::DEFAULT_TASKS_BATCH_SIZE,
            ),
            read_range_size: global(
                "matching.readRangeSize",
                crate::matching_reader::DEFAULT_READ_RANGE_SIZE,
            ),
            dispatch_timeout: {
                let provider = dispatch_timeout.clone();
                let domain = domain.clone();
                let task = task.clone();
                Arc::new(move || provider(&domain, &task, task_type))
            },
            update_ack_interval: Arc::new(move || {
                let config = ack_overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration_bytes(
                    config.property_values("matching.updateAckInterval"),
                    ack_name.as_deref(),
                    ack_task.as_bytes(),
                    task_type,
                    crate::matching_reader::DEFAULT_UPDATE_ACK_INTERVAL_NANOS,
                )
            }),
        }
    })
}

fn standby_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
) -> crate::matching_standby_native::EnabledNative {
    Arc::new(move |domain_id, task_list, task_type| {
        let name = cached_domain_name(&cache, domain_id);
        let config = overlay.lock().expect("admin lock");
        task_list_boolean(
            &config,
            "matching.enableStandbyTaskCompletion",
            name.as_deref(),
            task_list,
            task_type,
            true,
        )
    })
}

fn writer_live_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
) -> crate::matching_registry_native::NativeWriterLiveProvider {
    Arc::new(move |domain: &GoString, task: &GoString, task_type: i32| {
        let name = cached_domain_name(&cache, domain);
        let config = overlay.lock().expect("admin lock");
        let batch = cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values("matching.maxTaskBatchSize"),
            name.as_deref(),
            task.as_bytes(),
            task_type,
            100,
        );
        let timeout = cadence_persistence::dynamic_config::task_list_duration_bytes(
            config.property_values("matching.appendTaskTimeout"),
            name.as_deref(),
            task.as_bytes(),
            task_type,
            5_000_000_000,
        );
        let capacity = cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values("matching.outstandingTaskAppendsThreshold"),
            name.as_deref(),
            task.as_bytes(),
            task_type,
            250,
        );
        (batch, timeout, capacity)
    })
}

fn task_list_boolean(
    overlay: &DynamicConfigOverlay,
    key: &str,
    name: Option<&str>,
    task: &GoString,
    task_type: i32,
    default: bool,
) -> bool {
    cadence_persistence::dynamic_config::task_list_boolean_bytes(
        overlay.property_values(key),
        name,
        task.as_bytes(),
        task_type,
        default,
    )
}

fn integer_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str,
    default: i64,
) -> crate::matching_registry_native::NativeProvider<i64> {
    Arc::new(move |domain, task, task_type| {
        let name = cached_domain_name(&cache, domain);
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values(key),
            name.as_deref(),
            task.as_bytes(),
            task_type,
            default,
        )
    })
}

fn duration_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str,
    default: i64,
) -> crate::matching_registry_native::NativeProvider<i64> {
    Arc::new(move |domain, task, task_type| {
        let name = cached_domain_name(&cache, domain);
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_duration_bytes(
            config.property_values(key),
            name.as_deref(),
            task.as_bytes(),
            task_type,
            default,
        )
    })
}

fn boolean_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str,
    default: bool,
) -> crate::matching_registry_native::NativeProvider<bool> {
    Arc::new(move |domain, task, task_type| {
        let name = cached_domain_name(&cache, domain);
        let config = overlay.lock().expect("admin lock");
        task_list_boolean(&config, key, name.as_deref(), task, task_type, default)
    })
}

fn record_started_timeout_provider(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
) -> crate::matching_history::RecordStartedTimeoutBytes {
    Arc::new(move |domain| {
        // Resolve before taking the overlay lock: cold metadata lookup may block.
        let name = cached_domain_name(&cache, &GoString::from(domain));
        let config = overlay.lock().expect("admin lock");
        let nanos = cadence_persistence::dynamic_config::duration(
            config.property_values("matching.recordTaskStartedTimeout"),
            name.as_deref(),
            crate::matching_history::DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS,
        );
        std::time::Duration::from_nanos(nanos.max(0) as u64)
    })
}

fn domain_is_active(cache: &MatchingDomainCache, id: &GoString, cluster: &str) -> bool {
    crate::matching_engine_context_native::cached_domain_active(cache, id, cluster).unwrap_or(true)
}

fn cached_domain_name(cache: &MatchingDomainCache, id: &GoString) -> Option<String> {
    domain_by_raw_id(cache, id)
        .ok()
        .map(|domain| domain.info.name)
}

fn domain_by_raw_id(
    cache: &MatchingDomainCache,
    id: &GoString,
) -> Result<MatchingDomainMetadata, Status> {
    let raw = std::str::from_utf8(id.as_bytes())
        .map_err(|_| Status::invalid_argument("invalid domain UUID"))?;
    cache
        .get_domain_by_id(raw)
        .map_err(crate::matching_domain_cache::DomainCacheError::into_status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
    use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

    fn empty_cache() -> MatchingDomainCache {
        struct EmptySource;
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
                MatchingDomainMetadata,
                crate::matching_domain_cache::DomainMetadataSourceError,
            > {
                Err(crate::matching_domain_cache::DomainMetadataSourceError::permanent("missing"))
            }
        }
        MatchingDomainCache::new(Arc::new(EmptySource))
    }

    #[test]
    fn cold_lookup_refreshes_limiter_before_locking_config() {
        use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
        use std::time::Duration;
        const ID: &str = "00000000-0000-0000-0000-000000000001";
        const ENV: &str = "CADENCE_COLD_LOOKUP_CASE";
        let Ok(case) = std::env::var(ENV) else {
            for case in ["integer", "duration", "boolean", "writer", "standby"] {
                let status = std::process::Command::new("timeout")
                    .arg("10s")
                    .arg(std::env::current_exe().unwrap())
                    .args(["--exact", "matching_runtime_native::tests::cold_lookup_refreshes_limiter_before_locking_config", "--nocapture"])
                    .env(ENV, case)
                    .status()
                    .unwrap();
                assert!(
                    status.success(),
                    "cold lookup case {case} failed or timed out: {status}"
                );
            }
            return;
        };
        struct Source(MatchingDomainMetadata);
        impl crate::matching_domain_cache::DomainMetadataSource for Source {
            fn metadata_version(
                &self,
                _: Duration,
            ) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> {
                Ok(2)
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
                    domains: vec![self.0.clone()],
                    next_page_token: None,
                })
            }
            fn get_domain(
                &self,
                _: Option<&str>,
                id: Option<&str>,
                _: Duration,
            ) -> Result<
                MatchingDomainMetadata,
                crate::matching_domain_cache::DomainMetadataSourceError,
            > {
                assert_eq!(id, Some(ID));
                Ok(self.0.clone())
            }
        }
        let record = MatchingDomainMetadata {
            info: cadence_api::types::DomainInfo {
                name: "cold-domain".into(),
                uuid: ID.into(),
                ..Default::default()
            },
            configuration: Default::default(),
            replication: Default::default(),
            is_global_domain: false,
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
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let value = |data: &[u8], filtered: bool| DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: data.to_vec(),
            }),
            filters: if filtered {
                vec![DynamicConfigFilter {
                    name: "domainName".into(),
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""cold-domain""#.to_vec(),
                    }),
                }]
            } else {
                vec![]
            },
        };
        {
            let mut config = overlay.lock().unwrap();
            config.update(
                false,
                "matching.persistenceMaxQPS",
                vec![value(b"3000", false)],
            );
            for (key, data) in [
                ("test.integer", b"17".as_slice()),
                ("test.duration", br#""250ms""#.as_slice()),
                ("test.boolean", b"false".as_slice()),
                ("matching.enableStandbyTaskCompletion", b"false".as_slice()),
                ("matching.maxTaskBatchSize", b"17".as_slice()),
                ("matching.appendTaskTimeout", br#""250ms""#.as_slice()),
                ("matching.outstandingTaskAppendsThreshold", b"19".as_slice()),
            ] {
                config.update(false, key, vec![value(data, true)]);
            }
        }
        let clock = Arc::new(AtomicI64::new(0));
        let now = clock.clone();
        let limiter = crate::matching_persistence_limiter::MatchingPersistenceLimiter::with_clock(
            overlay.clone(),
            Arc::new(AtomicUsize::new(1)),
            Arc::new(move || now.load(Ordering::SeqCst)),
        );
        let source =
            crate::matching_persistence::limit_domain_source(Arc::new(Source(record)), limiter);
        let cache = Arc::new(MatchingDomainCache::new(source));
        clock.store(5_000_000_000, Ordering::SeqCst);
        let domain = GoString::from(ID);
        let task = GoString::from(&b"queue-\xff"[..]);
        match case.as_str() {
            "integer" => assert_eq!(
                integer_provider(cache, overlay, "test.integer", 1)(&domain, &task, 0),
                17
            ),
            "duration" => assert_eq!(
                duration_provider(cache, overlay, "test.duration", 1)(&domain, &task, 0),
                250_000_000
            ),
            "boolean" => assert!(!boolean_provider(cache, overlay, "test.boolean", true)(
                &domain, &task, 0
            )),
            "writer" => assert_eq!(
                writer_live_provider(cache, overlay)(&domain, &task, 0),
                (17, 250_000_000, 19)
            ),
            "standby" => assert!(!standby_provider(cache, overlay)(&domain, &task, 0)),
            _ => panic!("unknown case"),
        }
    }

    #[test]
    fn native_config_filter_preserves_invalid_task_list_bytes() {
        let value = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: b"false".to_vec(),
            }),
            filters: vec![DynamicConfigFilter {
                name: "taskListName".into(),
                value: Some(cadence_api::types::DataBlob {
                    encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                    data: br#""\ufffd""#.to_vec(),
                }),
            }],
        };
        let mut overlay = DynamicConfigOverlay::default();
        overlay.update(false, "native.boolean", vec![value]);
        let cache = empty_cache();
        let invalid = task_list_boolean(
            &overlay,
            "native.boolean",
            cached_domain_name(&cache, &GoString::from("unknown")).as_deref(),
            &GoString::from(&b"\xff"[..]),
            0,
            true,
        );
        let replacement = task_list_boolean(
            &overlay,
            "native.boolean",
            cached_domain_name(&cache, &GoString::from("unknown")).as_deref(),
            &GoString::from("\u{fffd}"),
            0,
            true,
        );
        assert!(invalid);
        assert!(!replacement);
    }

    #[test]
    fn reader_dispatch_timeout_reads_live_durations_and_exact_task_filters() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let provider = reader_properties_provider(Arc::new(empty_cache()), overlay.clone());
        let domain = GoString::from("00000000-0000-0000-0000-000000000001");
        let raw = provider(&domain, &GoString::from(&b"queue-\xff"[..]), 0);
        let valid = provider(&domain, &GoString::from("queue-\u{fffd}"), 0);
        let activity = provider(&domain, &GoString::from("queue-\u{fffd}"), 1);
        assert_eq!((raw.dispatch_timeout)(), 3_000_000_000);
        let value = |json: &[u8], filters| DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: json.to_vec(),
            }),
            filters,
        };
        let filter = |name: &str, json: &[u8]| DynamicConfigFilter {
            name: name.into(),
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: json.to_vec(),
            }),
        };
        for (json, expected) in [
            (br#""250ms""#.as_slice(), 250_000_000),
            (br#""0s""#.as_slice(), 0),
            (br#""invalid""#.as_slice(), 3_000_000_000),
        ] {
            overlay.lock().unwrap().update(
                false,
                "matching.asyncTaskDispatchTimeout",
                vec![
                    value(json, vec![]),
                    value(
                        br#""75ms""#,
                        vec![
                            filter("taskListName", br#""queue-\ufffd""#),
                            filter("taskType", b"0"),
                        ],
                    ),
                ],
            );
            assert_eq!(
                (raw.dispatch_timeout)(),
                expected,
                "invalid bytes must not alias U+FFFD"
            );
            assert_eq!((valid.dispatch_timeout)(), 75_000_000, "decision filter");
            assert_eq!((activity.dispatch_timeout)(), expected, "task type filter");
        }
        // Numeric reader fields retain integer property semantics.
        overlay.lock().unwrap().update(
            false,
            "matching.getTasksBatchSize",
            vec![value(b"17", vec![])],
        );
        overlay
            .lock()
            .unwrap()
            .update(false, "matching.readRangeSize", vec![value(b"42", vec![])]);
        assert_eq!((raw.batch_size)(), 17);
        assert_eq!((raw.read_range_size)(), 42);
    }

    #[test]
    fn record_started_provider_observes_live_config_and_zero_timeout() {
        use std::time::Duration;
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let provider = record_started_timeout_provider(Arc::new(empty_cache()), overlay.clone());
        let id = b"00000000-0000-0000-0000-000000000001";
        assert_eq!(provider(id), Duration::from_secs(1));
        for (value, expected) in [
            (br#""250ms""#.as_slice(), Duration::from_millis(250)),
            (br#""0s""#.as_slice(), Duration::ZERO),
        ] {
            overlay.lock().unwrap().update(
                false,
                "matching.recordTaskStartedTimeout",
                vec![DynamicConfigValue {
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: value.to_vec(),
                    }),
                    filters: vec![],
                }],
            );
            assert_eq!(provider(id), expected);
            assert_eq!(
                provider(b"\xff"),
                expected,
                "invalid domain bytes use the global property"
            );
        }
    }

    #[test]
    fn byte_isolation_selector_uses_live_poller_and_partition_fixtures() {
        use crate::matching_partition_native::{NativePartitionCache, NativeStoredPartitionConfig};
        use cadence_common::poller_bytes::{Info, Manager};
        let task_group = GoString::from("zone-\u{fffd}");
        let invalid_group = GoString::from(&b"zone-\xff"[..]);
        let configured_value = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: br#"["zone-\uFFFD"]"#.to_vec(),
            }),
            filters: Vec::new(),
        };
        let mut dynamic = DynamicConfigOverlay::default();
        dynamic.update(false, "system.allIsolationGroups", vec![configured_value]);
        let configured = cadence_persistence::dynamic_config::string_list(
            dynamic.property_values("system.allIsolationGroups"),
        );
        let known = configured
            .iter()
            .map(|group| GoString::from(group.as_str()))
            .collect::<Vec<_>>();
        let mut poller_manager = Manager::default();
        poller_manager.start(
            b"poll",
            Info {
                identity: "worker".into(),
                rate_per_second: 1.0,
                isolation_group: task_group.clone(),
            },
            10,
        );
        let pollers = Arc::new(Mutex::new(poller_manager));
        let config = NativeStoredPartitionConfig {
            read_partitions: std::collections::HashMap::from([(0, vec![task_group.clone()])]),
            ..Default::default()
        };
        let cache = NativePartitionCache::default();
        cache.push(&config);
        let actual_groups = cache.get().unwrap().read_partitions[&0].clone();
        let recent = pollers
            .lock()
            .unwrap()
            .has_group_after(task_group.as_bytes(), 9);
        assert_eq!(
            select_isolation_bytes(
                true,
                false,
                &task_group,
                known.iter().any(|g| g == &task_group),
                Ok(false),
                recent,
                Some(&actual_groups),
                0,
                0
            ),
            (task_group.clone(), 0, None)
        );
        assert_eq!(
            select_isolation_bytes(
                true,
                false,
                &invalid_group,
                known.iter().any(|g| g == &invalid_group),
                Ok(false),
                recent,
                Some(&actual_groups),
                0,
                0
            )
            .2,
            Some("group_unknown")
        );
        assert_eq!(
            select_isolation_bytes(
                true,
                false,
                &task_group,
                known.iter().any(|g| g == &task_group),
                Ok(true),
                recent,
                Some(&actual_groups),
                0,
                0
            )
            .2,
            Some("group_drained")
        );
        assert!(select_isolation_bytes(
            true,
            true,
            &task_group,
            true,
            Ok(false),
            recent,
            Some(&actual_groups),
            0,
            0
        )
        .0
        .is_empty());
        assert_eq!(
            select_isolation_bytes(
                true,
                false,
                &task_group,
                true,
                Ok(false),
                recent,
                Some(&actual_groups),
                200_000_000,
                160_000_000
            )
            .2,
            Some("expired")
        );
    }

    #[test]
    fn reader_timeout_activity_uses_durable_source_cache() {
        use crate::matching_engine_context_native::source_domain_tests::{cache, record};
        let mut domain = record(0xabc, 0);
        let id = GoString::from(domain.info.uuid.clone());
        domain.is_global_domain = true;
        domain.replication.active_cluster_name = "other".into();
        assert!(!domain_is_active(
            &cache(vec![domain.clone()]),
            &id,
            "active"
        ));
        domain.replication.active_cluster_name = "active".into();
        assert!(domain_is_active(
            &cache(vec![domain.clone()]),
            &id,
            "active"
        ));
        domain.failover_end_time_nanos = Some(0);
        assert!(!domain_is_active(&cache(vec![domain]), &id, "active"));
    }

    #[test]
    fn missing_or_non_utf8_domain_preserves_legacy_active_default() {
        let cache = empty_cache();
        assert!(domain_is_active(
            &cache,
            &GoString::from("missing"),
            "cluster-a"
        ));
        assert!(domain_is_active(
            &cache,
            &GoString::from(&b"\xff"[..]),
            "cluster-a"
        ));
    }

    #[test]
    fn native_fatal_adapter_retains_sql_cause_and_reader_text() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sinks = crate::matching_fatal::native_fatal_sinks(sender);
        let cause = cadence_persistence::matching_blob::SqlRunIdBytesError {
            raw: GoString::from(&b"r\xff"[..]),
            message: "parse".into(),
        };
        sinks.writer.unwrap()(cause.clone());
        assert!(
            matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::SqlRunIdBytes(actual)) if actual == cause)
        );
        sinks.reader.unwrap()("ack invariant".into());
        assert!(
            matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::ReaderInvariant(s)) if s == "ack invariant")
        );
    }
}
