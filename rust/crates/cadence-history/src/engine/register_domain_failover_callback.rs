//! Port of engineimpl/register_domain_failover_callback.go.
//! TODO(link): shard/domain-cache/queue adapters supply the contracts below.
use super::deps::{ClusterMetadata, HResult};
use async_trait::async_trait;
use cadence_api::types::ClusterAttribute;
use std::{collections::HashSet, sync::Arc};

#[derive(Clone, Default)]
pub struct FailoverDomain {
    pub id: String,
    pub name: String,
    pub notification_version: i64,
    pub failover_notification_version: i64,
    pub global: bool,
    pub active_active: bool,
    pub active_cluster: String,
    pub previous_failover_version: i64,
    pub failover_version: i64,
    /// Flattened ActiveClusters.AttributeScopes[scope].ClusterAttributes[name].
    pub attributes: Vec<(ClusterAttribute, String)>,
    pub active_in_clusters: HashSet<String>,
}
#[derive(Clone)]
pub struct FailoverPartition {
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
}
#[derive(Clone)]
pub struct FailoverMarker {
    pub domain_id: String,
    pub version: i64,
}

pub trait FailoverQueue: Send + Sync {
    fn lock_task_processing(&self);
    fn unlock_task_processing(&self);
    fn failover_domain(&self, domains: &HashSet<String>);
    /// NotifyNewTask with a synthetic decision task, or decision timeout at now.
    fn notify_scan(&self, cluster: &str, timer_timestamp: Option<i64>);
}
#[async_trait]
pub trait FailoverShard: Send + Sync {
    fn domain_notification_version(&self) -> i64;
    fn shard_id(&self) -> i32;
    fn now(&self) -> i64;
    async fn replicate_failover_markers(&self, markers: Vec<FailoverMarker>) -> HResult<()>;
    fn update_domain_notification_version(&self, version: i64) -> HResult<()>;
}
pub trait FailoverDLQ: Send + Sync {
    fn failover_partitions(&self, partitions: Vec<FailoverPartition>);
}
pub trait FailoverMetrics: Send + Sync {
    fn callback_count(&self);
    fn marker_callback_count(&self);
    fn marker_insert_failure(&self);
}
pub trait DomainChangeRegistrar: Send + Sync {
    /// The cache must run prepare before publishing changes, followed by callback.
    /// Catch-up uses the same pair and the captured initial shard version.
    fn register_domain_change_callback(
        &self,
        name: String,
        initial_version: i64,
        callbacks: Arc<FailoverCallbacks>,
    );
}
pub struct FailoverCallbacks {
    pub current_cluster: String,
    pub shard: Arc<dyn FailoverShard>,
    pub metadata: Arc<dyn ClusterMetadata>,
    pub queues: Vec<Arc<dyn FailoverQueue>>,
    pub transfer: Option<Arc<dyn FailoverQueue>>,
    pub timer: Option<Arc<dyn FailoverQueue>>,
    pub dlq: Option<Arc<dyn FailoverDLQ>>,
    pub metrics: Arc<dyn FailoverMetrics>,
}
impl FailoverCallbacks {
    pub fn register_domain_failover_callback(self: &Arc<Self>, cache: &dyn DomainChangeRegistrar) {
        let initial = self.shard.domain_notification_version();
        cache.register_domain_change_callback(
            create_shard_name_from_shard_id(self.shard.shard_id()),
            initial,
            self.clone(),
        );
    }

    pub async fn catch_up(&self, initial_version: i64, mut domains: Vec<FailoverDomain>) {
        domains.sort_by_key(|domain| domain.notification_version);
        domains.retain(|domain| domain.notification_version >= initial_version);
        if !domains.is_empty() {
            tracing::info!(domain_names = ?domains.iter().map(|d| &d.name).collect::<Vec<_>>(),
                "catchUpFn calling prepareCallback and callback");
            self.lock_task_processing_for_domain_update();
            self.domain_change_cb(&domains).await;
        }
    }

    pub fn lock_task_processing_for_domain_update(&self) {
        tracing::debug!("Locking processing for domain update");
        for queue in &self.queues {
            queue.lock_task_processing();
        }
    }
    pub fn unlock_processing_for_domain_update(&self) {
        tracing::debug!("Unlocking processing for failover");
        for queue in &self.queues {
            queue.unlock_task_processing();
        }
    }

    /// Called only after prepare has locked all queues. Guard also unlocks on error/cancellation.
    pub async fn domain_change_cb(&self, domains: &[FailoverDomain]) {
        struct Unlock<'a>(&'a FailoverCallbacks);
        impl Drop for Unlock<'_> {
            fn drop(&mut self) {
                self.0.unlock_processing_for_domain_update();
            }
        }
        let _unlock = Unlock(self);
        if domains.is_empty() {
            return;
        }
        let version = self.shard.domain_notification_version();
        let mut passive = HashSet::new();
        let mut active = HashSet::new();
        let mut partitions = Vec::new();
        for domain in domains {
            if !domain.global {
                continue;
            }
            if !domain.active_active
                && domain.failover_notification_version >= version
                && domain.active_cluster == self.current_cluster
            {
                passive.insert(domain.id.clone());
                partitions.push(FailoverPartition {
                    domain_id: domain.id.clone(),
                    cluster_attribute_scope: String::new(),
                    cluster_attribute_name: String::new(),
                });
            }
            if domain.active_active
                && domain.failover_notification_version >= version
                && domain.active_in_clusters.contains(&self.current_cluster)
            {
                active.insert(domain.id.clone());
                for attr in active_cluster_attributes_in(domain, &self.current_cluster) {
                    partitions.push(FailoverPartition {
                        domain_id: domain.id.clone(),
                        cluster_attribute_scope: attr.scope,
                        cluster_attribute_name: attr.name,
                    });
                }
            }
        }
        if !passive.is_empty() {
            tracing::debug!(workflow_domain_ids = ?passive, "Domain Failover Start.");
            for queue in &self.queues {
                queue.failover_domain(&passive);
            }
        }
        if !active.is_empty() {
            tracing::debug!(workflow_domain_ids = ?active, "Active-Active Domain updated");
        }
        if let Some(dlq) = &self.dlq {
            if !partitions.is_empty() {
                dlq.failover_partitions(partitions);
            }
        }
        if !passive.is_empty() || !active.is_empty() {
            self.notify_queues();
        }
        let markers =
            self.generate_graceful_failover_tasks_for_domain_update_callback(version, domains);
        self.metrics.callback_count();
        if !markers.is_empty() {
            if let Err(error) = self.shard.replicate_failover_markers(markers).await {
                tracing::error!(error = %error, "Failed to insert failover marker to replication queue.");
                self.metrics.marker_insert_failure();
                return; // preserve version so the next cache refresh retries
            }
        }
        let _ = self.shard.update_domain_notification_version(
            domains
                .last()
                .expect("nonempty domains")
                .notification_version
                .wrapping_add(1),
        );
    }

    pub fn notify_queues(&self) {
        let now = self.shard.now();
        let Some(transfer) = &self.transfer else {
            tracing::error!("transfer processor not found");
            return;
        };
        transfer.notify_scan(&self.current_cluster, None);
        let Some(timer) = &self.timer else {
            tracing::error!("timer processor not found");
            return;
        };
        timer.notify_scan(&self.current_cluster, Some(now));
    }

    pub fn generate_graceful_failover_tasks_for_domain_update_callback(
        &self,
        version: i64,
        domains: &[FailoverDomain],
    ) -> Vec<FailoverMarker> {
        let mut markers = Vec::new();
        for domain in domains {
            if domain.active_active {
                continue;
            }
            let previous_cluster = match self
                .metadata
                .cluster_name_for_failover_version(domain.previous_failover_version)
            {
                Ok(cluster) => cluster,
                Err(error) if domain.previous_failover_version != -1 => {
                    tracing::error!(workflow_domain_id = domain.id, error = %error, "Failed to handle graceful failover");
                    continue;
                }
                Err(_) => String::new(),
            };
            if domain.global
                && domain.failover_notification_version >= version
                && domain.active_cluster != self.current_cluster
                && domain.previous_failover_version != -1
                && previous_cluster == self.current_cluster
            {
                markers.push(FailoverMarker {
                    domain_id: domain.id.clone(),
                    version: domain.failover_version,
                });
                self.metrics.marker_callback_count();
            }
        }
        markers
    }

    pub fn failover_predicate(&self, version: i64, domain: &FailoverDomain, action: impl FnOnce()) {
        if domain.global
            && !domain.active_active
            && domain.failover_notification_version >= version
            && domain.active_cluster == self.current_cluster
        {
            action();
        }
    }
}
pub fn active_cluster_attributes_in(
    domain: &FailoverDomain,
    cluster: &str,
) -> Vec<ClusterAttribute> {
    domain
        .attributes
        .iter()
        .filter(|(_, active)| active == cluster)
        .map(|(attr, _)| attr.clone())
        .collect()
}
pub fn create_shard_name_from_shard_id(shard_id: i32) -> String {
    format!("history-engine-{shard_id}")
}
