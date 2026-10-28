//! Production adapters for the byte-preserving Matching registry.
//!
//! The launcher can pass its actual byte store/history client, cache, AppState
//! and fatal channel here; service-specific membership and transport routing
//! are attached by the launcher after construction.

use crate::{
    admin::DynamicConfigOverlay,
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_history::MatchingHistoryClient,
    matching_registry_native::{NativeDomainHooks, NativeFatalSinks, NativeQueueRegistry},
};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes;
use std::{num::NonZeroU32, sync::{Arc, Mutex}};
use tonic::{Code, Status};

/// Assemble the production native registry providers. Queue identity and every
/// task-list dynamic-config filter remain raw bytes, including invalid UTF-8.
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
    let cluster = cluster.into();

    let hooks = NativeDomainHooks {
        is_active: {
            let cache = cache.clone();
            let cluster = cluster.clone();
            Arc::new(move |id| domain_by_raw_id(&cache, id).is_ok_and(|domain| {
                domain.replication.active_cluster_name == cluster
            }))
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
                    active_in_cluster: domain.replication.active_cluster_name == cluster,
                    active_active: domain.replication.active_clusters.is_some(),
                    name: GoString::from(domain.info.name),
                })
            })
        },
    };

    let standby: crate::matching_standby_native::EnabledNative = {
        let overlay = overlay.clone();
        let domains = state.domains.clone();
        Arc::new(move |domain_id, task_list, task_type| {
            let name = std::str::from_utf8(domain_id.as_bytes()).ok()
                .and_then(|id| domains.get_domain_by_id(id).ok().map(|d| d.info.name));
            let config = overlay.lock().expect("admin lock");
            cadence_persistence::dynamic_config::task_list_boolean_bytes(
                config.property_values("matching.enableStandbyTaskCompletion"),
                name.as_deref(), task_list.as_bytes(), task_type, true,
            )
        })
    };

    let partition_count = config_provider(cache.clone(), overlay.clone(), "matching.numTasklistReadPartitions", 1i64);
    let cache_enabled: crate::matching_registry_native::NativeProvider<bool> = {
        let overlay = overlay.clone();
        let cache = cache.clone();
        Arc::new(move |domain, task, task_type| {
            let config = overlay.lock().expect("admin lock");
            cadence_persistence::dynamic_config::task_list_boolean_bytes(
                config.property_values("matching.enableGetNumberOfPartitionsFromCache"),
                cached_domain_name(&cache, domain).as_deref(), task.as_bytes(), task_type, false,
            )
        })
    };

    NativeQueueRegistry::new(store, history, range_size)
        .with_domain_hooks(hooks)
        .with_standby_enabled(standby)
        .with_partition_count(partition_count)
        .with_partition_cache_enabled(cache_enabled)
        .with_fatal_sinks(fatal)
}

fn cached_domain_name(cache: &MatchingDomainCache, id: &GoString) -> Option<String> {
    domain_by_raw_id(cache, id).ok().map(|domain| domain.info.name)
}

fn domain_by_raw_id(
    cache: &MatchingDomainCache,
    id: &GoString,
) -> Result<crate::matching_domain_cache::MatchingDomainMetadata, Status> {
    let raw = std::str::from_utf8(id.as_bytes())
        .map_err(|_| Status::new(Code::InvalidArgument, "invalid domain UUID"))?;
    let uuid = uuid::Uuid::parse_str(raw)
        .map_err(|error| Status::new(Code::InvalidArgument, error.to_string()))?;
    cache.get_domain_by_id(&uuid.to_string())
        .map_err(|error| Status::not_found(error.to_string()))
}

fn config_provider<T: Send + Sync + 'static>(
    cache: Arc<MatchingDomainCache>,
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    key: &'static str,
    default: T,
) -> Arc<dyn Fn(&GoString, &GoString, i32) -> T + Send + Sync>
where
    T: Copy + From<i64>,
{
    Arc::new(move |domain, task, task_type| {
        let config = overlay.lock().expect("admin lock");
        T::from(cadence_persistence::dynamic_config::task_list_integer_bytes(
            config.property_values(key), cached_domain_name(&cache, domain).as_deref(), task.as_bytes(), task_type,
            i64::from(default),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
    use cadence_persistence::dynamic_config::{task_list_boolean_bytes, PropertyValues};

    #[test]
    fn byte_filter_distinguishes_invalid_name_from_replacement_character() {
        let value = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob { encoding_type: None, data: b"false".to_vec() }),
            filters: vec![DynamicConfigFilter {
                name: "taskListName".into(),
                value: Some(cadence_api::types::DataBlob { encoding_type: None, data: br#""\ufffd""#.to_vec() }),
            }],
        };
        assert!(task_list_boolean_bytes(&PropertyValues::snapshot(vec![value]), None, b"\xff", 0, true));
    }

    #[test]
    fn native_fatal_adapter_preserves_writer_cause_and_reader_text() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sinks = crate::matching_fatal::native_fatal_sinks(sender);
        sinks.writer.unwrap()(cadence_persistence::matching_blob::SqlRunIdBytesError {
            raw: GoString::from(&b"r\xff"[..]), message: "parse".into(),
        });
        assert!(matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::SqlRunIdBytes(_))));
        sinks.reader.unwrap()("ack invariant".into());
        assert!(matches!(receiver.try_recv(), Ok(crate::matching_fatal::MatchingFatal::ReaderInvariant(s)) if s == "ack invariant"));
    }
}
