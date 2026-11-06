//! Port of engineimpl/register_domain_failover_callback.go.
//!
//! The domain cache, the shard context and the DLQ processor are reached through
//! [`DomainFailoverHost`]; the queue processors come from `HistoryEngine::queue_processors`.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::BoxFuture;

use super::notify_tasks::{NotifiedTask, NotifiedTaskKind, NotifyTaskInfo};
use super::{deps::*, runtime::*, workflow_mutations::create_shard_name_from_shard_id};

/// `constants.InitialPreviousFailoverVersion`
pub const INITIAL_PREVIOUS_FAILOVER_VERSION: i64 = -1;

/// `cache.DomainCacheEntry` fields read by the failover callback.
#[derive(Debug, Clone, Default)]
pub struct FailoverDomainEntry {
    pub id: String,
    pub name: String,
    pub is_global_domain: bool,
    /// `IsDomainPendingActive`
    pub is_pending_active: bool,
    pub notification_version: i64,
    pub failover_notification_version: i64,
    pub failover_version: i64,
    pub previous_failover_version: i64,
    /// `GetReplicationConfig().ActiveClusterName`
    pub active_cluster_name: String,
    /// `GetReplicationConfig().ActiveClusters.AttributeScopes`: scope -> attribute name -> active cluster.
    /// `None` when `ActiveClusters` is nil.
    pub active_cluster_scopes: Option<BTreeMap<String, BTreeMap<String, String>>>,
}

impl FailoverDomainEntry {
    /// `DomainReplicationConfig.IsActiveActive`
    pub fn is_active_active(&self) -> bool {
        self.active_cluster_scopes.as_ref().is_some_and(|s| !s.is_empty())
    }

    /// `DomainCacheEntry.IsActiveIn`
    pub fn is_active_in(&self, current_cluster: &str) -> bool {
        if !self.is_global_domain {
            return true;
        }
        if self.is_pending_active {
            return false;
        }
        if current_cluster == self.active_cluster_name {
            return true;
        }
        self.active_cluster_scopes
            .iter()
            .flat_map(|scopes| scopes.values())
            .any(|attrs| attrs.values().any(|c| c == current_cluster))
    }
}

/// `taskdlq.Partition`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FailoverDlqPartition {
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
}

/// `persistence.FailoverMarkerTask` (visibility timestamp is set by the shard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverMarkerTask {
    pub version: i64,
    pub domain_id: String,
}

/// Callbacks registered on the domain cache.
pub type DomainChangedFn = Arc<dyn Fn(Vec<FailoverDomainEntry>) -> BoxFuture<'static, ()> + Send + Sync>;
/// `lockTaskProcessingForDomainUpdate` as a prepare callback.
pub type DomainLockFn = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[async_trait]
pub trait DomainFailoverHost: Send + Sync {
    /// `shard.GetDomainNotificationVersion`
    fn domain_notification_version(&self) -> i64;
    /// `shard.UpdateDomainNotificationVersion`
    fn update_domain_notification_version(&self, version: i64) -> HResult<()>;
    /// `shard.ReplicateFailoverMarkers`
    async fn replicate_failover_markers(&self, markers: Vec<FailoverMarkerTask>) -> HResult<()>;
    /// `e.dlqProcessor != nil`
    fn has_dlq_processor(&self) -> bool;
    /// `dlqProcessor.FailoverPartitions`
    fn failover_dlq_partitions(&self, partitions: Vec<FailoverDlqPartition>);
    /// `shard.GetDomainCache().RegisterDomainChangeCallback(shardName, catchUpFn, lock, changed)`.
    /// The host owns the catch-up function: it lists all domains, calls [`catch_up_entries`]
    /// with `initial_notification_version` and, when the result is non-empty, invokes `lock`
    /// followed by `changed(entries)`.
    fn register_domain_change_callback(&self, shard_name: &str, initial_notification_version: i64, lock: DomainLockFn, changed: DomainChangedFn);
}

/// The ordering and filtering part of `catchUpFn`: sort by notification version and keep the
/// domains with `NotificationVersion >= initialNotificationVersion`.
pub fn catch_up_entries(mut domains: Vec<FailoverDomainEntry>, initial_notification_version: i64) -> Vec<FailoverDomainEntry> {
    domains.sort_by_key(|d| d.notification_version);
    domains.into_iter().filter(|d| d.notification_version >= initial_notification_version).collect()
}

/// `activeClusterAttributesIn`: (scope, name) pairs of an active-active domain that are active in `cluster`.
pub fn active_cluster_attributes_in(entry: &FailoverDomainEntry, cluster: &str) -> Vec<(String, String)> {
    let Some(scopes) = &entry.active_cluster_scopes else { return Vec::new() };
    let mut out = Vec::new();
    for (scope, attrs) in scopes {
        for (name, active) in attrs {
            if active == cluster {
                out.push((scope.clone(), name.clone()));
            }
        }
    }
    out
}

impl HistoryEngine {
    /// `registerDomainFailoverCallback`
    pub fn register_domain_failover_callback(self: &Arc<Self>) {
        let initial = self.failover_host.domain_notification_version();
        let lock_engine = Arc::clone(self);
        let changed_engine = Arc::clone(self);
        self.failover_host.register_domain_change_callback(
            &create_shard_name_from_shard_id(self.start_host.shard_id()),
            initial,
            Arc::new(move || {
                let engine = Arc::clone(&lock_engine);
                Box::pin(async move { engine.lock_task_processing_for_domain_update().await })
            }),
            Arc::new(move |next| {
                let engine = Arc::clone(&changed_engine);
                Box::pin(async move { engine.domain_change_cb(&next).await })
            }),
        );
    }

    /// `domainChangeCB`
    pub async fn domain_change_cb(&self, next_domains: &[FailoverDomainEntry]) {
        self.domain_change_cb_inner(next_domains).await;
        // defer e.unlockProcessingForDomainUpdate()
        self.unlock_processing_for_domain_update();
    }

    async fn domain_change_cb_inner(&self, next_domains: &[FailoverDomainEntry]) {
        let host = &self.failover_host;
        if next_domains.is_empty() {
            return;
        }
        let shard_notification_version = host.domain_notification_version();
        let mut failover_active_passive: HashSet<String> = HashSet::new();
        let mut failover_active_active: HashSet<String> = HashSet::new();
        // dlq partitions collect the domain and cluster-attribute pairs that were failed over.
        let mut dlq_partitions: Vec<FailoverDlqPartition> = Vec::new();

        for next in next_domains {
            if !next.is_global_domain {
                continue;
            }
            let domain_failover_notification_version = next.failover_notification_version;
            if !next.is_active_active()
                && domain_failover_notification_version >= shard_notification_version
                && next.active_cluster_name == self.current_cluster_name
            {
                failover_active_passive.insert(next.id.clone());
                dlq_partitions.push(FailoverDlqPartition { domain_id: next.id.clone(), cluster_attribute_scope: String::new(), cluster_attribute_name: String::new() });
            }
            if next.is_active_active() && domain_failover_notification_version >= shard_notification_version && next.is_active_in(&self.current_cluster_name) {
                failover_active_active.insert(next.id.clone());
                for (scope, name) in active_cluster_attributes_in(next, &self.current_cluster_name) {
                    dlq_partitions.push(FailoverDlqPartition { domain_id: next.id.clone(), cluster_attribute_scope: scope, cluster_attribute_name: name });
                }
            }
        }

        if !failover_active_passive.is_empty() {
            tracing::debug!(domain_ids = ?failover_active_passive, "Domain Failover Start.");
            // Failover queues are not created for active-active domains.
            for processor in self.queue_processors.values() {
                processor.failover_domain(&failover_active_passive).await;
            }
        }
        if !failover_active_active.is_empty() {
            tracing::debug!(domain_ids = ?failover_active_active, "Active-Active Domain updated");
        }
        // Notify the DLQ processor to reprocess all the failed-over domains, including cluster attribute failovers.
        if host.has_dlq_processor() && !dlq_partitions.is_empty() {
            host.failover_dlq_partitions(dlq_partitions);
        }
        // Notify queues for any domain update (active-passive and active-active)
        if !failover_active_passive.is_empty() || !failover_active_active.is_empty() {
            self.notify_queues();
        }

        let failover_marker_tasks = self.generate_graceful_failover_tasks_for_domain_update_callback(shard_notification_version, next_domains);
        if !failover_marker_tasks.is_empty() {
            if let Err(e) = host.replicate_failover_markers(failover_marker_tasks).await {
                tracing::error!(error = %e, "Failed to insert failover marker to replication queue.");
                // fail this failover callback and it retries on next domain cache refresh
                return;
            }
        }
        let _ = host.update_domain_notification_version(next_domains[next_domains.len() - 1].notification_version + 1);
    }

    /// `notifyQueues`
    pub fn notify_queues(&self) {
        let now = self.time_source.now();
        // the fake tasks will not be actually used, we just need to make sure
        // its length > 0 and has correct timestamp, to trigger a db scan
        let fake_decision_task = NotifyTaskInfo {
            tasks: vec![NotifiedTask { version: 0, task_id: 0, task_type: 0, visibility_timestamp: 0, kind: NotifiedTaskKind::Generic }],
            ..Default::default()
        };
        let fake_decision_timeout_task = NotifyTaskInfo {
            tasks: vec![NotifiedTask { version: 0, task_id: 0, task_type: 0, visibility_timestamp: now, kind: NotifiedTaskKind::Generic }],
            ..Default::default()
        };
        let Some(transfer) = self.queue_processors.get(&HistoryTaskCategory::Transfer) else {
            tracing::error!("transfer processor not found");
            return;
        };
        transfer.notify_new_task(&self.current_cluster_name, &fake_decision_task);
        let Some(timer) = self.queue_processors.get(&HistoryTaskCategory::Timer) else {
            tracing::error!("timer processor not found");
            return;
        };
        timer.notify_new_task(&self.current_cluster_name, &fake_decision_timeout_task);
    }

    /// `generateGracefulFailoverTasksForDomainUpdateCallback`
    pub fn generate_graceful_failover_tasks_for_domain_update_callback(&self, shard_notification_version: i64, next_domains: &[FailoverDomainEntry]) -> Vec<FailoverMarkerTask> {
        // handle graceful failover on active to passive
        // make sure task processor failover the domain before inserting the failover marker
        let mut tasks = Vec::new();
        for next in next_domains {
            if next.is_active_active() {
                // Graceful failover is not used for active-active domains.
                continue;
            }
            let previous_failover_version = next.previous_failover_version;
            let previous_cluster_name = match self.cluster_metadata.cluster_name_for_failover_version(previous_failover_version) {
                Ok(name) => name,
                Err(e) if previous_failover_version != INITIAL_PREVIOUS_FAILOVER_VERSION => {
                    tracing::error!(domain_id = %next.id, error = %e, "Failed to handle graceful failover");
                    continue;
                }
                Err(_) => String::new(),
            };
            if next.is_global_domain
                && next.failover_notification_version >= shard_notification_version
                && next.active_cluster_name != self.current_cluster_name
                && previous_failover_version != INITIAL_PREVIOUS_FAILOVER_VERSION
                && previous_cluster_name == self.current_cluster_name
            {
                // the visibility timestamp will be set in shard context
                tasks.push(FailoverMarkerTask { version: next.failover_version, domain_id: next.id.clone() });
            }
        }
        tasks
    }

    /// `lockTaskProcessingForDomainUpdate`
    pub async fn lock_task_processing_for_domain_update(&self) {
        tracing::debug!("Locking processing for domain update");
        for processor in self.queue_processors.values() {
            processor.lock_task_processing().await;
        }
    }

    /// `unlockProcessingForDomainUpdate`
    pub fn unlock_processing_for_domain_update(&self) {
        tracing::debug!("Unlocking processing for failover");
        for processor in self.queue_processors.values() {
            processor.unlock_task_processing();
        }
    }

    /// `failoverPredicate`
    pub fn failover_predicate(&self, shard_notification_version: i64, next: &FailoverDomainEntry, action: impl FnOnce()) {
        if next.is_global_domain
            && !next.is_active_active()
            && next.failover_notification_version >= shard_notification_version
            && next.active_cluster_name == self.current_cluster_name
        {
            action();
        }
    }
}
