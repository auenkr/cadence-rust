//! Production adapters for the byte-preserving Matching registry.
//!
//! Queue identity and task-list dynamic-config filters remain raw bytes,
//! including invalid UTF-8. The launcher owns registration and listeners.

use crate::{
    admin::DynamicConfigOverlay,
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_history::MatchingHistoryClient,
    matching_registry_native::{NativeDomainHooks, NativeFatalSinks, NativeQueueRegistry},
    matching_reader::ReaderLive,
};
use cadence_common::go_string::GoString;
use cadence_persistence::{
    matching_domain_metadata::MatchingDomainMetadata,
    matching_native_task_store::MatchingTaskStoreBytes,
};
use std::{num::NonZeroU32, sync::{Arc, Mutex}};
use tonic::{Code, Status};

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
    let domains = state.domains.clone();
    let cluster = cluster.into();

    let hooks = NativeDomainHooks {
        is_active: {
            let domains = domains.clone();
            let cluster = cluster.clone();
            Arc::new(move |id| domain_is_active(&domains, id, &cluster))
        },
        domain_name: {
            let cache = cache.clone();
            Arc::new(move |id| domain_by_raw_id(&cache, id).ok().map(|d| d.info.name))
        },
        standby_domains: {
            let cache = cache.clone();
            let domains = domains.clone();
            let cluster = cluster.clone();
            Arc::new(move |id| {
                let domain = domain_by_raw_id(&cache, id)?;
                Ok(crate::matching_standby_native::StandbyDomainNative {
                    active_in_cluster: domain_is_active(&domains, id, &cluster),
                    active_active: domain.replication.active_clusters.is_some(),
                    name: GoString::from(domain.info.name),
                })
            })
        },
    };

    let standby: crate::matching_standby_native::EnabledNative = {
        let overlay = overlay.clone();
        let cache = cache.clone();
        Arc::new(move |domain_id, task_list, task_type| {
            let config = overlay.lock().expect("admin lock");
            task_list_boolean(
                &config, "matching.enableStandbyTaskCompletion", &cache,
                domain_id, task_list, task_type, true,
            )
        })
    };
    let partition_count = integer_provider(cache.clone(), overlay.clone(), "matching.numTasklistReadPartitions", 1);
    let cache_enabled = boolean_provider(cache.clone(), overlay.clone(), "matching.enableGetNumberOfPartitionsFromCache", false);
    let sync_wait = duration_provider(cache.clone(), overlay.clone(), "matching.localTaskWaitTime", 10_000_000);
    let local_poll_wait = duration_provider(cache.clone(), overlay.clone(), "matching.localPollWaitTime", 10_000_000);
    let forward_degree = {
        let value = integer_provider(cache.clone(), overlay.clone(), "matching.forwarderMaxChildrenPerNode", 20);
        Arc::new(move |domain, task, task_type| value(domain, task, task_type).max(1))
    };
    let forward_polls = integer_provider(cache.clone(), overlay.clone(), "matching.forwarderMaxOutstandingPolls", 1);
    let forward_tasks = integer_provider(cache.clone(), overlay.clone(), "matching.forwarderMaxOutstandingTasks", 1);
    let forward_rate = integer_provider(cache.clone(), overlay.clone(), "matching.forwarderMaxRatePerSecond", 10);
    let qps_interval = duration_provider(
        cache.clone(), overlay.clone(), "matching.qpsTrackerInterval",
        crate::matching_registry_native::DEFAULT_QPS_TRACKER_INTERVAL_NANOS,
    );
    let min_burst = integer_provider(
        cache.clone(), overlay.clone(), "matching.minTaskThrottlingBurstSize",
        crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE,
    );
    let update_ack = duration_provider(cache.clone(), overlay.clone(), "matching.updateAckInterval", 60_000_000_000);
    let idle_check = duration_provider(cache.clone(), overlay.clone(), "matching.idleTasklistCheckInterval", 300_000_000_000);
    let delete_batch = integer_provider(cache.clone(), overlay.clone(), "matching.maxTaskDeleteBatchSize", 100);
    let writer_live = {
        let cache = cache.clone();
        let overlay = overlay.clone();
        Arc::new(move |domain, task, task_type| {
            let config = overlay.lock().expect("admin lock");
            let name = cached_domain_name(&cache, domain);
            let batch = cadence_persistence::dynamic_config::task_list_integer_bytes(
                config.property_values("matching.maxTaskBatchSize"), name.as_deref(),
                task.as_bytes(), task_type, 100,
            );
            let timeout = cadence_persistence::dynamic_config::task_list_duration_bytes(
                config.property_values("matching.appendTaskTimeout"), name.as_deref(),
                task.as_bytes(), task_type, 5_000_000_000,
            );
            let capacity = cadence_persistence::dynamic_config::task_list_integer_bytes(
                config.property_values("matching.outstandingTaskAppendsThreshold"), name.as_deref(),
                task.as_bytes(), task_type, 250,
            );
            (batch, timeout, capacity)
        })
    };
    let reader_live = {
        let cache = cache.clone();
        let overlay = overlay.clone();
        Arc::new(move |domain: &GoString, task: &GoString, task_type| {
            let name = cached_domain_name(&cache, domain);
            let scoped = |key: &'static str, default| {
                let overlay = overlay.clone();
                let name = name.clone();
                let task = task.clone();
                Arc::new(move || {
                    let config = overlay.lock().expect("admin lock");
                    cadence_persistence::dynamic_config::task_list_integer_bytes(
                        config.property_values(key), name.as_deref(), task.as_bytes(), task_type, default,
                    )
                }) as crate::matching_reader::LiveInt
            };
            let global = |key: &'static str, default| {
                let overlay = overlay.clone();
                Arc::new(move || {
                    let config = overlay.lock().expect("admin lock");
                    cadence_persistence::dynamic_config::integer(config.property_values(key), None, default)
                }) as crate::matching_reader::LiveInt
            };
            let ack_overlay = overlay.clone();
            let ack_name = name.clone();
            let ack_task = task.clone();
            ReaderLive {
                batch_size: scoped("matching.getTasksBatchSize", crate::matching_reader::DEFAULT_TASKS_BATCH_SIZE),
                read_range_size: global("matching.readRangeSize", crate::matching_reader::DEFAULT_READ_RANGE_SIZE),
                dispatch_timeout: scoped("matching.asyncTaskDispatchTimeout", crate::matching_reader::DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS),
                update_ack_interval: Arc::new(move || {
                    let config = ack_overlay.lock().expect("admin lock");
                    cadence_persistence::dynamic_config::task_list_duration_bytes(
                        config.property_values("matching.updateAckInterval"), ack_name.as_deref(),
                        ack_task.as_bytes(), task_type, crate::matching_reader::DEFAULT_UPDATE_ACK_INTERVAL_NANOS,
                    )
                }),
            }
        })
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
        .with_fatal_sinks(fatal)
}

fn task_list_boolean(
    overlay: &DynamicConfigOverlay, key: &str, cache: &MatchingDomainCache,
    domain: &GoString, task: &GoString, task_type: i32, default: bool,
) -> bool {
    cadence_persistence::dynamic_config::task_list_boolean_bytes(
        overlay.property_values(key), cached_domain_name(cache, domain).as_deref(),
        task.as_bytes(), task_type, default,
    )
}

fn integer_provider(
    cache: Arc<MatchingDomainCache>, overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str, default: i64,
) -> crate::matching_registry_native::NativeProvider<i64> {
    Arc::new(move |domain, task, task_type| {
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values(key), cached_domain_name(&cache, domain).as_deref(),
            task.as_bytes(), task_type, default,
        )
    })
}

fn duration_provider(
    cache: Arc<MatchingDomainCache>, overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str, default: i64,
) -> crate::matching_registry_native::NativeProvider<i64> {
    Arc::new(move |domain, task, task_type| {
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_duration_bytes(
            config.property_values(key), cached_domain_name(&cache, domain).as_deref(),
            task.as_bytes(), task_type, default,
        )
    })
}

fn boolean_provider(
    cache: Arc<MatchingDomainCache>, overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str, default: bool,
) -> crate::matching_registry_native::NativeProvider<bool> {
    Arc::new(move |domain, task, task_type| {
        let config = overlay.lock().expect("admin lock");
        task_list_boolean(&config, key, &cache, domain, task, task_type, default)
    })
}

fn domain_is_active(domains: &cadence_persistence::InMemoryDomainStore, id: &GoString, cluster: &str) -> bool {
    let Ok(id) = std::str::from_utf8(id.as_bytes()) else { return true };
    domains.is_active_in(id, cluster).unwrap_or(true)
}

fn cached_domain_name(cache: &MatchingDomainCache, id: &GoString) -> Option<String> {
    domain_by_raw_id(cache, id).ok().map(|domain| domain.info.name)
}

fn domain_by_raw_id(cache: &MatchingDomainCache, id: &GoString) -> Result<MatchingDomainMetadata, Status> {
    let raw = std::str::from_utf8(id.as_bytes())
        .map_err(|_| Status::new(Code::InvalidArgument, "invalid domain UUID"))?;
    let uuid = uuid::Uuid::parse_str(raw).map_err(|error| Status::new(Code::InvalidArgument, error.to_string()))?;
    cache.get_domain_by_id(&uuid.to_string()).map_err(|error| Status::not_found(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
    use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

    fn empty_cache() -> MatchingDomainCache {
        struct EmptySource;
        impl crate::matching_domain_cache::DomainMetadataSource for EmptySource {
            fn metadata_version(&self, _: std::time::Duration) -> Result<i64, crate::matching_domain_cache::DomainMetadataSourceError> { Ok(0) }
            fn list_domains(&self, _: usize, _: Option<&[u8]>, _: std::time::Duration) -> Result<crate::matching_domain_cache::DomainMetadataPage, crate::matching_domain_cache::DomainMetadataSourceError> {
                Ok(crate::matching_domain_cache::DomainMetadataPage { domains: vec![], next_page_token: None })
            }
            fn get_domain(&self, _: Option<&str>, _: Option<&str>, _: std::time::Duration) -> Result<MatchingDomainMetadata, crate::matching_domain_cache::DomainMetadataSourceError> {
                Err(crate::matching_domain_cache::DomainMetadataSourceError::permanent("missing"))
            }
        }
        MatchingDomainCache::new(Arc::new(EmptySource))
    }

    #[test]
    fn native_config_filter_preserves_invalid_task_list_bytes() {
        let value = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob { encoding_type: None, data: b"false".to_vec() }),
            filters: vec![DynamicConfigFilter {
                name: "taskListName".into(),
                value: Some(cadence_api::types::DataBlob { encoding_type: None, data: br#""\ufffd""#.to_vec() }),
            }],
        };
        let mut overlay = DynamicConfigOverlay::default();
        overlay.update(false, "native.boolean", vec![value]);
        let cache = empty_cache();
        let invalid = task_list_boolean(&overlay, "native.boolean", &cache, &GoString::from("unknown"), &GoString::from(&b"\xff"[..]), 0, true);
        let replacement = task_list_boolean(&overlay, "native.boolean", &cache, &GoString::from("unknown"), &GoString::from("\u{fffd}"), 0, true);
        assert!(invalid);
        assert!(!replacement);
    }

    #[test]
    fn missing_or_non_utf8_domain_preserves_legacy_active_default() {
        let domains = cadence_persistence::InMemoryDomainStore::new();
        assert!(domain_is_active(&domains, &GoString::from("missing"), "cluster-a"));
        assert!(domain_is_active(&domains, &GoString::from(&b"\xff"[..]), "cluster-a"));
    }

    #[test]
    fn native_fatal_adapter_retains_sql_cause_and_reader_text() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sinks = crate::matching_fatal::native_fatal_sinks(sender);
        let cause = cadence_persistence::matching_blob::SqlRunIdBytesError {
            raw: GoString::from(&b"r\xff"[..]), message: "parse".into(),
        };
        sinks.writer.unwrap()(cause.clone());
        assert!(matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::SqlRunIdBytes(actual)) if actual == cause));
        sinks.reader.unwrap()("ack invariant".into());
        assert!(matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::ReaderInvariant(s)) if s == "ack invariant"));
    }
}
