//! Adapters from the real `shard_context::ShardContext` to the shard traits the
//! queue packages declare (`queue::shard_deps::QueueShardContext` and
//! `queuev2::deps::ShardContext`).
//!
//! Known lossy spots (the real shard context stores a reduced form):
//! * processing queue states keep only the domain filter's `reverse_match` flag;
//!   the adapter remembers the full state it last wrote and returns it while the
//!   shard's copy still matches, otherwise domain ids are empty;
//! * failover levels carry no domain ids in the shard; the adapter keeps them
//!   in-process (a restart drops failover levels in Go too: they are not persisted);
//! * queuev2 queue states are reduced to `virtual_slice_inclusive_mins`; reading
//!   rebuilds a single virtual queue (id 0) with universal slices.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::queue::config::QueueConfig;
use crate::queue::deps::{self as q, GetHistoryTasksRequest, GetHistoryTasksResponse, QueueError, Time};
use crate::queue::shard_deps::{
    FailoverLevel as QFailoverLevel, HistoryKey, ProcessorConfig, QueueShardContext,
    RangeCompleteHistoryTaskRequest,
};
use crate::queue::task_allocator::{ActiveClusterManager, DomainCache};
use crate::queue::util::{PersistenceDomainFilter, PersistenceProcessingQueueState};
use crate::queuev2::deps as v2;
use crate::shard_context as sc;
use crate::wiring::persistence::queues::SqlQueueShardContext;

fn qcat(c: q::HistoryTaskCategory) -> sc::HistoryTaskCategory {
    match c {
        q::HistoryTaskCategory::Transfer => sc::HistoryTaskCategory::Transfer,
        q::HistoryTaskCategory::Timer => sc::HistoryTaskCategory::Timer,
    }
}

fn to_sc_key(k: HistoryKey) -> sc::HistoryTaskKey {
    sc::HistoryTaskKey { scheduled_time_nanos: k.scheduled_time.unix_nano(), task_id: k.task_id }
}

fn from_sc_key(k: sc::HistoryTaskKey) -> HistoryKey {
    HistoryKey { scheduled_time: Time(k.scheduled_time_nanos), task_id: k.task_id }
}

/// Maps shard errors onto the queue error kinds (`ErrShardClosed` and ownership
/// loss close the shard; persistence failures are background-transient).
pub fn map_shard_err(e: sc::ShardContextError) -> QueueError {
    match e {
        sc::ShardContextError::ShardClosed { .. } => QueueError::ShardClosed(e.to_string()),
        sc::ShardContextError::ShardOwnershipLost(m) => QueueError::ShardClosed(m),
        sc::ShardContextError::Persistence(m) => QueueError::Transient(m),
        other => QueueError::Other(other.to_string()),
    }
}

type StatesKey = (bool, String);

/// Everything the queue v1 processors read from `shard.Context`.
pub struct RealQueueShardContext {
    shard: Arc<sc::ShardContext>,
    tasks: Arc<SqlQueueShardContext>,
    processor_config: ProcessorConfig,
    queue_config: Arc<QueueConfig>,
    domain_cache: Arc<dyn DomainCache>,
    active_cluster_manager: Arc<dyn ActiveClusterManager>,
    cluster: Arc<dyn sc::ClusterInfo>,
    failover_domains: Mutex<HashMap<(sc::HistoryTaskCategory, String), HashSet<String>>>,
    last_states: Mutex<HashMap<StatesKey, Vec<PersistenceProcessingQueueState>>>,
}

impl RealQueueShardContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard: Arc<sc::ShardContext>,
        tasks: Arc<SqlQueueShardContext>,
        cluster: Arc<dyn sc::ClusterInfo>,
        processor_config: ProcessorConfig,
        queue_config: Arc<QueueConfig>,
        domain_cache: Arc<dyn DomainCache>,
        active_cluster_manager: Arc<dyn ActiveClusterManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard,
            tasks,
            processor_config,
            queue_config,
            domain_cache,
            active_cluster_manager,
            cluster,
            failover_domains: Mutex::new(HashMap::new()),
            last_states: Mutex::new(HashMap::new()),
        })
    }

    fn states_to_persistence(
        &self,
        transfer: bool,
        cluster: &str,
        shard_states: Vec<sc::ProcessingQueueState>,
    ) -> Vec<PersistenceProcessingQueueState> {
        if let Some(c) = self.last_states.lock().unwrap().get(&(transfer, cluster.to_string())) {
            let same = c.len() == shard_states.len()
                && c.iter().zip(&shard_states).all(|(a, b)| {
                    a.level == b.level
                        && a.ack_level == b.ack_level
                        && a.max_level == b.max_level
                        && a.domain_filter.reverse_match == b.domain_filter_reverse_match
                });
            if same {
                return c.clone();
            }
        }
        shard_states
            .into_iter()
            .map(|s| PersistenceProcessingQueueState {
                level: s.level,
                ack_level: s.ack_level,
                max_level: s.max_level,
                domain_filter: PersistenceDomainFilter {
                    domain_ids: Vec::new(),
                    reverse_match: s.domain_filter_reverse_match,
                },
            })
            .collect()
    }

    fn states_from_persistence(
        &self,
        transfer: bool,
        cluster: &str,
        states: Vec<PersistenceProcessingQueueState>,
    ) -> Vec<sc::ProcessingQueueState> {
        let out = states
            .iter()
            .map(|s| sc::ProcessingQueueState {
                level: s.level,
                ack_level: s.ack_level,
                max_level: s.max_level,
                domain_filter_reverse_match: s.domain_filter.reverse_match,
            })
            .collect();
        self.last_states.lock().unwrap().insert((transfer, cluster.to_string()), states);
        out
    }
}

#[async_trait]
impl q::ShardContext for RealQueueShardContext {
    fn get_shard_id(&self) -> i32 {
        self.shard.shard_id()
    }
    fn now(&self) -> Time {
        q::ShardContext::now(self.tasks.as_ref())
    }
    fn range_size_bits(&self) -> u32 {
        q::ShardContext::range_size_bits(self.tasks.as_ref())
    }
    fn enable_debug_mode(&self) -> bool {
        q::ShardContext::enable_debug_mode(self.tasks.as_ref())
    }
    async fn get_history_tasks(&self, req: GetHistoryTasksRequest) -> Result<GetHistoryTasksResponse, QueueError> {
        if let Some(e) = self.shard.closed_error() {
            return Err(map_shard_err(e));
        }
        q::ShardContext::get_history_tasks(self.tasks.as_ref(), req).await
    }
}

#[async_trait]
impl QueueShardContext for RealQueueShardContext {
    fn current_cluster_name(&self) -> String {
        self.cluster.current_cluster_name()
    }
    fn remote_cluster_names(&self) -> Vec<String> {
        let current = self.cluster.current_cluster_name();
        self.cluster.enabled_cluster_names().into_iter().filter(|c| *c != current).collect()
    }
    fn enabled_cluster_names(&self) -> Vec<String> {
        self.cluster.enabled_cluster_names()
    }
    fn processor_config(&self) -> &ProcessorConfig {
        &self.processor_config
    }
    fn queue_config(&self) -> Arc<QueueConfig> {
        self.queue_config.clone()
    }
    fn domain_cache(&self) -> Arc<dyn DomainCache> {
        self.domain_cache.clone()
    }
    fn active_cluster_manager(&self) -> Arc<dyn ActiveClusterManager> {
        self.active_cluster_manager.clone()
    }

    fn get_queue_ack_level(&self, category: q::HistoryTaskCategory) -> HistoryKey {
        from_sc_key(self.shard.get_queue_ack_level(qcat(category)))
    }
    fn get_queue_cluster_ack_level(&self, category: q::HistoryTaskCategory, cluster: &str) -> HistoryKey {
        from_sc_key(self.shard.get_queue_cluster_ack_level(qcat(category), cluster))
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, category: q::HistoryTaskCategory, cluster: &str) -> HistoryKey {
        from_sc_key(self.shard.update_if_needed_and_get_queue_max_read_level(qcat(category), cluster))
    }
    async fn update_queue_ack_level(&self, category: q::HistoryTaskCategory, key: HistoryKey) -> Result<(), QueueError> {
        self.shard.update_queue_ack_level(qcat(category), to_sc_key(key)).map_err(map_shard_err)
    }
    async fn update_queue_cluster_ack_level(
        &self,
        category: q::HistoryTaskCategory,
        cluster: &str,
        key: HistoryKey,
    ) -> Result<(), QueueError> {
        self.shard
            .update_queue_cluster_ack_level(qcat(category), cluster, to_sc_key(key))
            .map_err(map_shard_err)
    }

    fn get_transfer_processing_queue_states(&self, cluster: &str) -> Vec<PersistenceProcessingQueueState> {
        self.states_to_persistence(true, cluster, self.shard.get_transfer_processing_queue_states(cluster))
    }
    async fn update_transfer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<PersistenceProcessingQueueState>,
    ) -> Result<(), QueueError> {
        let s = self.states_from_persistence(true, cluster, states);
        self.shard.update_transfer_processing_queue_states(cluster, s).map_err(map_shard_err)
    }
    fn get_timer_processing_queue_states(&self, cluster: &str) -> Vec<PersistenceProcessingQueueState> {
        self.states_to_persistence(false, cluster, self.shard.get_timer_processing_queue_states(cluster))
    }
    async fn update_timer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<PersistenceProcessingQueueState>,
    ) -> Result<(), QueueError> {
        let s = self.states_from_persistence(false, cluster, states);
        self.shard.update_timer_processing_queue_states(cluster, s).map_err(map_shard_err)
    }

    fn get_all_failover_levels(&self, category: q::HistoryTaskCategory) -> Vec<QFailoverLevel> {
        let cat = qcat(category);
        let domains = self.failover_domains.lock().unwrap();
        self.shard
            .get_all_failover_levels(cat)
            .into_iter()
            .map(|(id, l)| QFailoverLevel {
                start_time: Time(l.start_time_nanos),
                min_level: from_sc_key(l.min_level),
                current_level: from_sc_key(l.current_level),
                max_level: from_sc_key(l.max_level),
                domain_ids: domains.get(&(cat, id)).cloned().unwrap_or_default(),
            })
            .collect()
    }
    async fn update_failover_level(
        &self,
        category: q::HistoryTaskCategory,
        failover_id: &str,
        level: QFailoverLevel,
    ) -> Result<(), QueueError> {
        let cat = qcat(category);
        self.failover_domains.lock().unwrap().insert((cat, failover_id.to_string()), level.domain_ids.clone());
        self.shard.update_failover_level(
            cat,
            failover_id,
            sc::FailoverLevel {
                start_time_nanos: level.start_time.unix_nano(),
                min_level: to_sc_key(level.min_level),
                current_level: to_sc_key(level.current_level),
                max_level: to_sc_key(level.max_level),
            },
        );
        Ok(())
    }
    async fn delete_failover_level(&self, category: q::HistoryTaskCategory, failover_id: &str) -> Result<(), QueueError> {
        let cat = qcat(category);
        self.failover_domains.lock().unwrap().remove(&(cat, failover_id.to_string()));
        self.shard.delete_failover_level(cat, failover_id);
        Ok(())
    }

    async fn range_complete_history_task(&self, req: RangeCompleteHistoryTaskRequest) -> Result<i64, QueueError> {
        self.tasks.range_complete_history_task(req).await
    }
}

// ------------------------------------------------------------------ queuev2

fn v2_cat(c: v2::HistoryTaskCategory) -> sc::HistoryTaskCategory {
    if c == v2::HistoryTaskCategory::TIMER {
        sc::HistoryTaskCategory::Timer
    } else {
        sc::HistoryTaskCategory::Transfer
    }
}

fn sc_to_v2_key(c: sc::HistoryTaskCategory, k: sc::HistoryTaskKey) -> v2::HistoryTaskKey {
    match c {
        sc::HistoryTaskCategory::Timer => {
            v2::HistoryTaskKey::new(v2::unix_nano_time(k.scheduled_time_nanos), k.task_id)
        }
        _ => v2::HistoryTaskKey::new_immediate(k.task_id),
    }
}

/// Everything queuev2 reads from `shard.Context`.
pub struct RealQueueV2ShardContext {
    shard: Arc<sc::ShardContext>,
    exec: Arc<dyn v2::ExecutionManager>,
    time_source: Arc<dyn v2::TimeSource>,
    config: Arc<v2::QueueConfig>,
    logger: Arc<dyn v2::Logger>,
    metrics: Arc<dyn v2::MetricsClient>,
    cluster: Arc<dyn sc::ClusterInfo>,
}

impl RealQueueV2ShardContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard: Arc<sc::ShardContext>,
        exec: Arc<dyn v2::ExecutionManager>,
        time_source: Arc<dyn v2::TimeSource>,
        config: Arc<v2::QueueConfig>,
        logger: Arc<dyn v2::Logger>,
        metrics: Arc<dyn v2::MetricsClient>,
        cluster: Arc<dyn sc::ClusterInfo>,
    ) -> Arc<Self> {
        Arc::new(Self { shard, exec, time_source, config, logger, metrics, cluster })
    }
}

impl v2::ShardContext for RealQueueV2ShardContext {
    fn get_shard_id(&self) -> i32 {
        self.shard.shard_id()
    }
    fn get_range_id(&self) -> i64 {
        self.shard.range_id()
    }
    fn get_time_source(&self) -> Arc<dyn v2::TimeSource> {
        self.time_source.clone()
    }
    fn get_config(&self) -> Arc<v2::QueueConfig> {
        self.config.clone()
    }
    fn get_logger(&self) -> Arc<dyn v2::Logger> {
        self.logger.clone()
    }
    fn get_metrics_client(&self) -> Arc<dyn v2::MetricsClient> {
        self.metrics.clone()
    }
    fn get_execution_manager(&self) -> Arc<dyn v2::ExecutionManager> {
        self.exec.clone()
    }
    fn get_queue_state(&self, c: v2::HistoryTaskCategory) -> Result<v2::TypesQueueState, v2::DepError> {
        let s = self.shard.get_queue_state(v2_cat(c)).map_err(|e| e.to_string())?;
        let max = s.exclusive_max_read_level;
        let mut vqs = BTreeMap::new();
        if !s.virtual_slice_inclusive_mins.is_empty() {
            let slices = s
                .virtual_slice_inclusive_mins
                .iter()
                .map(|m| v2::TypesVirtualSliceState {
                    task_range: v2::TypesTaskRange {
                        inclusive_min: v2::TypesTaskKey { task_id: m.task_id, scheduled_time_nano: m.scheduled_time_nano },
                        exclusive_max: v2::TypesTaskKey { task_id: max.task_id, scheduled_time_nano: max.scheduled_time_nano },
                    },
                    predicate: Some(v2::TypesPredicate::Universal),
                })
                .collect();
            vqs.insert(0, v2::TypesVirtualQueueState { virtual_slice_states: slices });
        }
        Ok(v2::TypesQueueState {
            virtual_queue_states: vqs,
            exclusive_max_read_level: v2::TypesTaskKey { task_id: max.task_id, scheduled_time_nano: max.scheduled_time_nano },
        })
    }
    fn update_queue_state(&self, c: v2::HistoryTaskCategory, s: v2::TypesQueueState) -> Result<(), v2::DepError> {
        let mins = s
            .virtual_queue_states
            .values()
            .flat_map(|vq| vq.virtual_slice_states.iter())
            .map(|sl| sc::TaskKey {
                task_id: sl.task_range.inclusive_min.task_id,
                scheduled_time_nano: sl.task_range.inclusive_min.scheduled_time_nano,
            })
            .collect();
        self.shard
            .update_queue_state(
                v2_cat(c),
                sc::QueueState {
                    exclusive_max_read_level: sc::TaskKey {
                        task_id: s.exclusive_max_read_level.task_id,
                        scheduled_time_nano: s.exclusive_max_read_level.scheduled_time_nano,
                    },
                    virtual_slice_inclusive_mins: mins,
                },
            )
            .map_err(|e| e.to_string())
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, c: v2::HistoryTaskCategory, cluster: &str) -> v2::HistoryTaskKey {
        let cat = v2_cat(c);
        sc_to_v2_key(cat, self.shard.update_if_needed_and_get_queue_max_read_level(cat, cluster))
    }
    fn get_current_cluster_name(&self) -> String {
        self.cluster.current_cluster_name()
    }
}
