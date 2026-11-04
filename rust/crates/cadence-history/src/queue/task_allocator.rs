//! Port of `go/service/history/queue/task_allocator.go`.
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::deps::QueueError;

/// Subset of go `cache.DomainCacheEntry` used by the allocator.
/// TODO(link): replace by cache::DomainCacheEntry
#[derive(Clone, Debug, Default)]
pub struct DomainEntry {
    pub is_global_domain: bool,
    pub has_failover_end_time: bool,
    pub is_active_active: bool,
    pub active_cluster_name: String,
    /// `GetInfo().Status` is deprecated or deleted
    pub is_deprecated_or_deleted: bool,
    pub has_info: bool,
    /// `GetReplicationConfig().Clusters` names (for `HasReplicationCluster`)
    pub replication_clusters: Vec<String>,
}
impl DomainEntry {
    pub fn has_replication_cluster(&self, cluster: &str) -> bool {
        self.replication_clusters.iter().any(|c| c == cluster)
    }
}

/// TODO(link): replace by cache::DomainCache
#[async_trait]
pub trait DomainCache: Send + Sync {
    /// Err(QueueError::EntityNotExists) mirrors `*types.EntityNotExistsError`.
    fn get_domain_by_id(&self, domain_id: &str) -> Result<DomainEntry, QueueError>;
}

/// TODO(link): replace by activecluster::Manager
#[async_trait]
pub trait ActiveClusterManager: Send + Sync {
    /// Returns `ActiveClusterName` of `GetActiveClusterInfoByWorkflow`.
    async fn get_active_cluster_name_by_workflow(
        &self,
        domain_id: &str,
        wf_id: &str,
        run_id: &str,
    ) -> Result<String, QueueError>;
}

/// Go `htask.ErrTaskPendingActive`; carried as `QueueError::Other` with this message.
pub const ERR_TASK_PENDING_ACTIVE: &str = "redispatch the task while the domain is pending-active";

/// TaskAllocator verifies if a task should be processed or not
#[async_trait]
pub trait TaskAllocator: Send + Sync {
    async fn verify_active_task(&self, domain_id: &str, wf_id: &str, r_id: &str) -> Result<bool, QueueError>;
    async fn verify_failover_active_task(
        &self,
        target_domain_ids: &HashSet<String>,
        domain_id: &str,
        wf_id: &str,
        r_id: &str,
    ) -> Result<bool, QueueError>;
    async fn verify_standby_task(
        &self,
        standby_cluster: &str,
        domain_id: &str,
        wf_id: &str,
        r_id: &str,
    ) -> Result<bool, QueueError>;
    /// Block all task allocation (Go `Lock`); paired with `unlock`.
    async fn lock(&self);
    /// Resume the task allocator (Go `Unlock`).
    fn unlock(&self);
}

pub struct TaskAllocatorImpl {
    current_cluster_name: String,
    domain_cache: Arc<dyn DomainCache>,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
    locker: Arc<RwLock<()>>,
    /// write guard held between `lock` and `unlock`
    held: std::sync::Mutex<Option<tokio::sync::OwnedRwLockWriteGuard<()>>>,
}

/// Go `NewTaskAllocator`.
pub fn new_task_allocator(
    current_cluster_name: String,
    domain_cache: Arc<dyn DomainCache>,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
) -> Arc<TaskAllocatorImpl> {
    Arc::new(TaskAllocatorImpl { current_cluster_name, domain_cache, active_cluster_mgr, locker: Arc::new(RwLock::new(())),
        held: std::sync::Mutex::new(None),
    })
}

impl TaskAllocatorImpl {
    async fn verify_task_activeness(
        &self,
        cluster: &str,
        domain_id: &str,
        wf_id: &str,
        r_id: &str,
        skip_local_domain: bool,
    ) -> Result<bool, QueueError> {
        let _g = self.locker.read().await;

        let entry = match self.domain_cache.get_domain_by_id(domain_id) {
            Ok(e) => e,
            Err(QueueError::EntityNotExists(_)) => {
                // the domain may be deleted: treat as not active
                tracing::warn!(domain_id, "Cannot find domain, default to not process task.");
                return Ok(false);
            }
            Err(e) => {
                tracing::warn!(domain_id, error = %e, "Failed to get domain from cache");
                return Err(e);
            }
        };

        // handle local domain
        if !entry.is_global_domain {
            return Ok(!skip_local_domain && self.current_cluster_name == cluster);
        }

        // return error for pending active domain so the task can be retried
        if entry.is_global_domain && entry.has_failover_end_time {
            return Err(QueueError::Other(ERR_TASK_PENDING_ACTIVE.to_string()));
        }

        if cluster.is_empty() {
            // failover queue task
            return Ok(true);
        }

        // handle active-active domain
        if entry.is_active_active {
            let active = self
                .active_cluster_mgr
                .get_active_cluster_name_by_workflow(domain_id, wf_id, r_id)
                .await
                .map_err(|e| {
                    tracing::warn!(domain_id, wf_id, r_id, error = %e, "Failed to lookup active cluster");
                    e
                })?;
            return Ok(active == cluster);
        }

        // handle active-passive domain
        Ok(entry.active_cluster_name == cluster)
    }
}

#[async_trait]
impl TaskAllocator for TaskAllocatorImpl {
    async fn verify_active_task(&self, domain_id: &str, wf_id: &str, r_id: &str) -> Result<bool, QueueError> {
        self.verify_task_activeness(&self.current_cluster_name, domain_id, wf_id, r_id, false).await
    }

    async fn verify_failover_active_task(
        &self,
        target_domain_ids: &HashSet<String>,
        domain_id: &str,
        wf_id: &str,
        r_id: &str,
    ) -> Result<bool, QueueError> {
        if !target_domain_ids.contains(domain_id) {
            return Ok(false);
        }
        self.verify_task_activeness("", domain_id, wf_id, r_id, true).await
    }

    async fn verify_standby_task(
        &self,
        standby_cluster: &str,
        domain_id: &str,
        wf_id: &str,
        r_id: &str,
    ) -> Result<bool, QueueError> {
        self.verify_task_activeness(standby_cluster, domain_id, wf_id, r_id, true).await
    }

    async fn lock(&self) {
        let g = self.locker.clone().write_owned().await;
        *self.held.lock().unwrap() = Some(g);
    }

    fn unlock(&self) {
        self.held.lock().unwrap().take();
    }
}

/// Go `isDomainNotRegistered`: domain missing → Err; deprecated/deleted → true.
pub fn is_domain_not_registered(cache: &dyn DomainCache, domain_id: &str) -> Result<bool, QueueError> {
    let e = cache.get_domain_by_id(domain_id)?;
    if !e.has_info {
        return Err(QueueError::Other("domain info is nil in cache".to_string()));
    }
    Ok(e.is_deprecated_or_deleted)
}
