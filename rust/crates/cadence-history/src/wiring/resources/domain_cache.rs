//! `cache.DomainCache` for History (go/common/cache/domainCache.go), shared by
//! the engine, task, replication and failover modules. It wraps the Go-parity
//! `MatchingDomainCache` (periodic refresh, read-through on miss, change
//! callbacks) and converts `MatchingDomainMetadata` into each module's entry type.
//! The cache is constructed (and `start()`ed) by the assembly unit with a
//! `DomainMetadataSource` over the configured persistence.
use std::sync::Arc;

use cadence_api::enums::{ArchivalStatus as ApiArchival, DomainStatus};
use cadence_matching::matching_domain_cache::{
    DomainCacheError, DomainErrorClass, MatchingDomainCache,
};
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

use crate::engine::deps as eng;
use crate::replication::deps as repl;
use crate::task::deps as task;

#[derive(Clone)]
pub struct HistoryDomainCache {
    inner: Arc<MatchingDomainCache>,
}

impl HistoryDomainCache {
    pub fn new(inner: Arc<MatchingDomainCache>) -> Self {
        Self { inner }
    }
    pub fn inner(&self) -> &Arc<MatchingDomainCache> {
        &self.inner
    }
}

fn is_not_found(e: &DomainCacheError) -> bool {
    matches!(
        e,
        DomainCacheError::SourceClassified { class: DomainErrorClass::NotFound, .. }
    )
}

fn status_i32(m: &MatchingDomainMetadata) -> i32 {
    match m.info.status {
        Some(DomainStatus::Registered) => eng::DOMAIN_STATUS_REGISTERED,
        Some(DomainStatus::Deprecated) => eng::DOMAIN_STATUS_DEPRECATED,
        None => m.status_raw,
        #[allow(unreachable_patterns)]
        Some(_) => m.status_raw,
    }
}

fn engine_entry(m: &MatchingDomainMetadata) -> eng::DomainCacheEntry {
    eng::DomainCacheEntry {
        info: eng::DomainInfo {
            id: m.info.uuid.clone(),
            name: m.info.name.clone(),
            status: status_i32(m),
        },
        is_global_domain: m.is_global_domain,
        active_clusters: vec![(None, m.replication.active_cluster_name.clone())],
    }
}

fn archival(s: &Option<ApiArchival>) -> task::ArchivalStatus {
    match s {
        Some(ApiArchival::Enabled) => task::ArchivalStatus::Enabled,
        _ => task::ArchivalStatus::Disabled,
    }
}

fn task_entry(m: &MatchingDomainMetadata) -> task::DomainCacheEntry {
    task::DomainCacheEntry {
        info: task::DomainInfo { id: m.info.uuid.clone(), name: m.info.name.clone() },
        config: task::DomainConfig {
            history_archival_status: archival(&m.configuration.history_archival_status),
            history_archival_uri: m.configuration.history_archival_uri.clone(),
            visibility_archival_status: archival(&m.configuration.visibility_archival_status),
            visibility_archival_uri: m.configuration.visibility_archival_uri.clone(),
        },
        replication_config: task::DomainReplicationConfig {
            active_cluster_name: m.replication.active_cluster_name.clone(),
            active_active: m.replication.active_clusters.is_some(),
            clusters: m.replication.clusters.iter().map(|c| c.cluster_name.clone()).collect(),
        },
        is_global_domain: m.is_global_domain,
        failover_version: m.failover_version,
        retention_days: m.configuration.workflow_execution_retention_period_in_days,
        sampled_for_longer_retention_enabled: false,
        sampled_for_longer_retention: false,
    }
}

impl eng::DomainCache for HistoryDomainCache {
    fn get_domain_by_id(&self, id: &str) -> eng::HResult<Arc<eng::DomainCacheEntry>> {
        match self.inner.get_domain_by_id(id) {
            Ok(m) => Ok(Arc::new(engine_entry(&m))),
            Err(e) if is_not_found(&e) => Err(eng::HistoryError::entity_not_exists(e.to_string())),
            Err(e) => Err(eng::HistoryError::internal(e.to_string())),
        }
    }
    fn get_domain(&self, name: &str) -> eng::HResult<Arc<eng::DomainCacheEntry>> {
        match self.inner.get_domain(name) {
            Ok(m) => Ok(Arc::new(engine_entry(&m))),
            Err(e) if is_not_found(&e) => Err(eng::HistoryError::entity_not_exists(e.to_string())),
            Err(e) => Err(eng::HistoryError::internal(e.to_string())),
        }
    }
    fn get_domain_name(&self, id: &str) -> eng::HResult<String> {
        match self.inner.get_domain_name(id) {
            Ok(n) => Ok(n),
            Err(e) if is_not_found(&e) => Err(eng::HistoryError::entity_not_exists(e.to_string())),
            Err(e) => Err(eng::HistoryError::internal(e.to_string())),
        }
    }
    fn unregister_domain_change_callback(&self, shard_name: &str) {
        self.inner.unregister_callback(shard_name);
    }
}

fn task_err(e: DomainCacheError) -> task::TaskError {
    if is_not_found(&e) {
        task::TaskError::EntityNotExists(e.to_string())
    } else {
        task::TaskError::InternalService(e.to_string())
    }
}

impl task::DomainCache for HistoryDomainCache {
    fn get_domain_by_id(&self, id: &str) -> task::Result<Arc<task::DomainCacheEntry>> {
        self.inner.get_domain_by_id(id).map(|m| Arc::new(task_entry(&m))).map_err(task_err)
    }
    fn get_domain(&self, name: &str) -> task::Result<Arc<task::DomainCacheEntry>> {
        self.inner.get_domain(name).map(|m| Arc::new(task_entry(&m))).map_err(task_err)
    }
    fn get_domain_name(&self, id: &str) -> task::Result<String> {
        self.inner.get_domain_name(id).map_err(task_err)
    }
}

impl repl::DomainCache for HistoryDomainCache {
    fn get_domain_by_id(&self, id: &str) -> repl::Result<repl::DomainEntry> {
        self.inner
            .get_domain_by_id(id)
            .map(|m| repl::DomainEntry {
                name: m.info.name.clone(),
                replication_clusters: m
                    .replication
                    .clusters
                    .iter()
                    .map(|c| c.cluster_name.clone())
                    .collect(),
            })
            .map_err(|e| {
                if is_not_found(&e) {
                    repl::ReplicationError::EntityNotExists(e.to_string())
                } else {
                    repl::ReplicationError::Other(e.to_string())
                }
            })
    }
    fn get_domain_name(&self, id: &str) -> repl::Result<String> {
        self.inner.get_domain_name(id).map_err(|e| {
            if is_not_found(&e) {
                repl::ReplicationError::EntityNotExists(e.to_string())
            } else {
                repl::ReplicationError::Other(e.to_string())
            }
        })
    }
}

impl crate::failover::DomainCache for HistoryDomainCache {
    fn get_domain_name(&self, domain_id: &str) -> Result<String, String> {
        self.inner.get_domain_name(domain_id).map_err(|e| e.to_string())
    }
}
