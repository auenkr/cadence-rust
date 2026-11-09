//! Domain and active-cluster adapters for the History queue task allocator.
//! This module is construction support only; queue startup remains owned by the launcher.
use std::sync::Arc;

use crate::queue::deps::QueueError;
use crate::queue::task_allocator::{self as allocator, ActiveClusterManager, DomainCache, DomainEntry};
use crate::task::deps::TaskError;
use crate::wiring::assembly::shard_factory::DomainActiveClusterManager;
use crate::wiring::resources::domain_cache::HistoryDomainCache;
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

fn project_domain(metadata: &MatchingDomainMetadata) -> DomainEntry {
    let replication = &metadata.replication;
    DomainEntry {
        is_global_domain: metadata.is_global_domain,
        has_failover_end_time: metadata.failover_end_time_nanos.is_some(),
        is_active_active: replication.active_clusters.is_some(),
        active_cluster_name: replication.active_cluster_name.clone(),
        is_deprecated_or_deleted: matches!(metadata.info.status, Some(cadence_api::enums::DomainStatus::Deprecated | cadence_api::enums::DomainStatus::Deleted)),
        has_info: true,
        replication_clusters: replication.clusters.iter().map(|cluster| cluster.cluster_name.clone()).collect(),
    }
}

/// Projects Matching metadata into precisely the allocator fields used by Go.
pub struct HistoryQueueDomainCache {
    domains: Arc<HistoryDomainCache>,
}

impl HistoryQueueDomainCache {
    pub fn new(domains: Arc<HistoryDomainCache>) -> Self { Self { domains } }
}

#[async_trait::async_trait]
impl DomainCache for HistoryQueueDomainCache {
    fn get_domain_by_id(&self, domain_id: &str) -> Result<DomainEntry, QueueError> {
        let metadata = self.domains.inner().get_domain_by_id(domain_id).map_err(|error| {
            use cadence_matching::matching_domain_cache::{DomainCacheError, DomainErrorClass};
            match error {
                DomainCacheError::SourceClassified { class: DomainErrorClass::NotFound, message } => QueueError::EntityNotExists(message),
                other => QueueError::Transient(other.to_string()),
            }
        })?;
        Ok(project_domain(&metadata))
    }
}

/// Delegates workflow active-cluster selection to History's existing implementation.
pub struct HistoryQueueActiveClusterManager(Arc<DomainActiveClusterManager>);

impl HistoryQueueActiveClusterManager {
    pub fn new(domains: Arc<HistoryDomainCache>) -> Self {
        Self(Arc::new(DomainActiveClusterManager::new(domains)))
    }
}

#[async_trait::async_trait]
impl ActiveClusterManager for HistoryQueueActiveClusterManager {
    async fn get_active_cluster_name_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<String, QueueError> {
        use crate::task::deps::ActiveClusterManager as _;
        self.0.get_active_cluster_info_by_workflow(domain_id, workflow_id, run_id).await
            .map(|info| info.active_cluster_name)
            .map_err(|error| match error {
                TaskError::EntityNotExists(message) => QueueError::EntityNotExists(message),
                TaskError::InternalService(message) => QueueError::Transient(message),
                other => QueueError::Other(other.to_string()),
            })
    }
}

/// Creates an allocator with real History cache and active-cluster projections.
pub fn new_history_task_allocator(current_cluster: String, domains: Arc<HistoryDomainCache>) -> Arc<allocator::TaskAllocatorImpl> {
    allocator::new_task_allocator(current_cluster, Arc::new(HistoryQueueDomainCache::new(domains.clone())), Arc::new(HistoryQueueActiveClusterManager::new(domains)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{ActiveClusters, ClusterReplicationConfiguration, DomainConfiguration, DomainInfo, DomainReplicationConfiguration};

    fn metadata() -> MatchingDomainMetadata {
        MatchingDomainMetadata {
            info: DomainInfo { status: Some(cadence_api::enums::DomainStatus::Deprecated), ..DomainInfo::default() },
            replication: DomainReplicationConfiguration {
                active_cluster_name: "west".into(),
                clusters: vec![ClusterReplicationConfiguration { cluster_name: "west".into() }, ClusterReplicationConfiguration { cluster_name: "east".into() }],
                active_clusters: Some(ActiveClusters::default()),
            },
            is_global_domain: true,
            failover_end_time_nanos: Some(123),
            configuration: DomainConfiguration::default(), archival_bucket: String::new(), status_raw: 0,
            archival_status_raw: 0, history_archival_status_raw: 0, visibility_archival_status_raw: 0,
            config_version: 0, notification_version: 0, failover_notification_version: 0, failover_version: 0,
            previous_failover_version: 0, last_updated_time_nanos: 0, bad_binaries: None, isolation_groups: None,
            async_workflow_config: None, active_clusters_config: None, decoded_bad_binaries: None,
            decoded_isolation_groups: None, decoded_async_workflow_config: None, config_decode_error: None,
        }
    }

    #[test]
    fn projects_allocator_domain_semantics_and_replication_membership() {
        let entry = project_domain(&metadata());
        assert!(entry.is_global_domain);
        assert!(entry.has_failover_end_time);
        assert!(entry.is_active_active);
        assert_eq!(entry.active_cluster_name, "west");
        assert!(entry.is_deprecated_or_deleted);
        assert!(entry.has_info);
        assert!(entry.has_replication_cluster("east"));
        assert!(!entry.has_replication_cluster("north"));
    }

    #[test]
    fn absent_failover_and_active_active_metadata_stay_absent() {
        let mut metadata = metadata();
        metadata.failover_end_time_nanos = None;
        metadata.replication.active_clusters = None;
        let entry = project_domain(&metadata);
        assert!(!entry.has_failover_end_time);
        assert!(!entry.is_active_active);
    }
}
