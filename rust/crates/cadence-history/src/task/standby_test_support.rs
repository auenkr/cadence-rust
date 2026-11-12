//! Shared fakes for the standby executor tests (ports of
//! `transfer_standby_task_executor_test.go` / `timer_standby_task_executor_test.go`).
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::deps::*;
use super::deps_exec::*;
use super::standby_task_util::TaskDLQWriter;

pub const NOW: i64 = 1_000_000_000_000;
pub const SEC: i64 = 1_000_000_000;
pub const RESEND_DELAY_SECS: i64 = 10;
pub const DISCARD_DELAY_SECS: i64 = 60;
/// Task/workflow failover version of the global (standby) domains.
pub const VERSION: i64 = 5;
pub const CURRENT_CLUSTER: &str = "current-cluster";
pub const STANDBY_CLUSTER: &str = "standby-cluster";
pub const ALT_CLUSTER: &str = "alt-cluster";
pub const GLOBAL_DOMAIN: &str = "global-domain-id";
pub const LOCAL_DOMAIN: &str = "local-domain-id";
pub const AA_DOMAIN: &str = "active-active-domain-id";

/// Between resend and discard delay: standby fetches history from the remote cluster.
pub fn fetch_time() -> i64 {
    NOW + (RESEND_DELAY_SECS + (DISCARD_DELAY_SECS - RESEND_DELAY_SECS) / 2) * SEC
}
/// Past the discard delay.
pub fn discard_time() -> i64 {
    NOW + DISCARD_DELAY_SECS * 2 * SEC
}

pub struct FakeTime(pub AtomicI64);
impl TimeSource for FakeTime {
    fn now(&self) -> Time {
        Time(self.0.load(Ordering::SeqCst))
    }
}

pub struct Domains;
impl DomainCache for Domains {
    fn get_domain_by_id(&self, id: &str) -> Result<Arc<DomainCacheEntry>> {
        Ok(Arc::new(DomainCacheEntry {
            info: DomainInfo { id: id.into(), name: format!("name-of-{id}") },
            is_global_domain: id != LOCAL_DOMAIN,
            failover_version: VERSION,
            replication_config: DomainReplicationConfig {
                active_cluster_name: ALT_CLUSTER.into(),
                active_active: id == AA_DOMAIN,
                clusters: vec![],
            },
            ..Default::default()
        }))
    }
    fn get_domain(&self, name: &str) -> Result<Arc<DomainCacheEntry>> {
        self.get_domain_by_id(name.trim_start_matches("name-of-"))
    }
    fn get_domain_name(&self, id: &str) -> Result<String> {
        Ok(format!("name-of-{id}"))
    }
}

pub struct ClusterMeta;
impl ClusterMetadata for ClusterMeta {
    fn get_current_cluster_name(&self) -> String {
        CURRENT_CLUSTER.into()
    }
}

/// Active-active workflows are active in `ALT_CLUSTER`.
pub struct ActiveMgr;
#[async_trait]
impl ActiveClusterManager for ActiveMgr {
    async fn get_active_cluster_info_by_workflow(&self, _: &str, _: &str, _: &str) -> Result<ActiveClusterInfo> {
        Ok(ActiveClusterInfo { active_cluster_name: ALT_CLUSTER.into(), failover_version: VERSION })
    }
    async fn get_active_cluster_selection_policy_for_workflow(&self, _: &str, _: &str, _: &str) -> Result<Option<ActiveClusterSelectionPolicy>> {
        Ok(Some(ActiveClusterSelectionPolicy {
            cluster_attribute: Some(ClusterAttribute { scope: "scope".into(), name: "name".into() }),
        }))
    }
}

pub struct Metrics;
impl MetricsClient for Metrics {
    fn scope(&self, _: ScopeIdx, _: Vec<Tag>) -> Arc<dyn MetricsScope> {
        noop_scope()
    }
}

pub struct ScopeCache;
impl DomainMetricsScopeCache for ScopeCache {
    fn get(&self, _: &str, _: ScopeIdx) -> Option<Arc<dyn MetricsScope>> {
        None
    }
    fn put(&self, _: &str, _: ScopeIdx, _: Arc<dyn MetricsScope>) {}
}

pub struct Svc;
impl Service for Svc {
    fn get_domain_metrics_scope_cache(&self) -> Arc<dyn DomainMetricsScopeCache> {
        Arc::new(ScopeCache)
    }
    fn get_history_task_dlq_manager(&self) -> Arc<dyn crate::taskdlq::processor::HistoryTaskDLQManager> {
        unimplemented!()
    }
}

pub struct Shard {
    pub time: Arc<FakeTime>,
}
impl ShardContext for Shard {
    fn get_shard_id(&self) -> i32 {
        1
    }
    fn get_time_source(&self) -> Arc<dyn TimeSource> {
        self.time.clone()
    }
    fn get_config(&self) -> Arc<dyn HistoryConfig> {
        unimplemented!()
    }
    fn get_domain_cache(&self) -> Arc<dyn DomainCache> {
        Arc::new(Domains)
    }
    fn get_cluster_metadata(&self) -> Arc<dyn ClusterMetadata> {
        Arc::new(ClusterMeta)
    }
    fn get_active_cluster_manager(&self) -> Arc<dyn ActiveClusterManager> {
        Arc::new(ActiveMgr)
    }
    fn get_metrics_client(&self) -> Arc<dyn MetricsClient> {
        Arc::new(Metrics)
    }
    fn get_service(&self) -> Arc<dyn Service> {
        Arc::new(Svc)
    }
    fn get_logger(&self) -> Logger {
        Logger::new()
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, _: HistoryTaskCategory, _: &str) -> HistoryTaskKey {
        unimplemented!()
    }
    fn as_task_reinjector(&self) -> Arc<dyn crate::taskdlq::interface::TaskReinjector> {
        unimplemented!()
    }
}

pub fn shard() -> Arc<dyn ShardContext> {
    Arc::new(Shard { time: Arc::new(FakeTime(AtomicI64::new(NOW))) })
}

/// Records `create_history_dlq_task` / `create_history_dlq_ack_level_if_not_exists` calls.
#[derive(Default)]
pub struct Dlq {
    pub calls: Mutex<Vec<String>>,
}
#[async_trait]
impl TaskDLQWriter for Dlq {
    async fn create_history_dlq_task(&self, r: CreateHistoryDLQTaskRequest) -> Result<()> {
        self.calls.lock().unwrap().push(format!(
            "task:{}:{}:{}:{}",
            r.domain_id, r.cluster_attribute_scope, r.cluster_attribute_name, r.task.get_task_id()
        ));
        Ok(())
    }
    async fn create_history_dlq_ack_level_if_not_exists(&self, r: CreateHistoryDLQAckLevelRequest) -> Result<()> {
        self.calls.lock().unwrap().push(format!("ack:{}", r.domain_id));
        Ok(())
    }
}

pub fn dlq_mode_enabled() -> StringPropertyFnWithDomainFilter {
    Arc::new(|_| HISTORY_TASK_DLQ_MODE_ENABLED.to_string())
}
