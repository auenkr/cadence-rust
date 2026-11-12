//! Port of the self-contained logic of `go/service/history/shard/context.go`
//! (`contextImpl`): task ID allocation, range renewal, ack levels, queue
//! states, failover levels, current-time maps, shard-info persistence and the
//! sharded DLQ ack-level dedup writer, plus `go/service/history/lookup`.
//!
//! Dependencies that are not ported yet (execution manager, domain cache, event
//! cache, engine, metrics) are reached through the small traits below; the
//! workflow-write paths (Create/Update/ConflictResolve) are added together with
//! the persistence ExecutionManager port. Times are unix nanoseconds (`i64`),
//! the Rust equivalent of Go `time.Time` for these code paths.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// `persistence.DBTimestampMinPrecision` (1ms) in nanoseconds.
pub const DB_TIMESTAMP_MIN_PRECISION_NANOS: i64 = 1_000_000;

/// `persistence.HistoryTaskCategory` (IDs match Go: transfer=1, timer=2, replication=3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HistoryTaskCategory {
    Transfer,
    Timer,
    Replication,
}

impl HistoryTaskCategory {
    pub fn id(self) -> i32 {
        match self {
            Self::Transfer => 1,
            Self::Timer => 2,
            Self::Replication => 3,
        }
    }
}

/// `persistence.HistoryTaskKey`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryTaskKey {
    pub scheduled_time_nanos: i64,
    pub task_id: i64,
}

impl HistoryTaskKey {
    pub fn immediate(task_id: i64) -> Self {
        Self { scheduled_time_nanos: 0, task_id }
    }
    pub fn scheduled(scheduled_time_nanos: i64, task_id: i64) -> Self {
        Self { scheduled_time_nanos, task_id }
    }
}

/// `types.TaskKey`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaskKey {
    pub task_id: i64,
    pub scheduled_time_nano: i64,
}

/// `types.QueueState`; each virtual slice is reduced to its inclusive-min key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueState {
    pub exclusive_max_read_level: TaskKey,
    pub virtual_slice_inclusive_mins: Vec<TaskKey>,
}

/// `types.ProcessingQueueState` (reverse-match domain filter only is used by the shard).
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessingQueueState {
    pub level: i32,
    pub ack_level: i64,
    pub max_level: i64,
    pub domain_filter_reverse_match: bool,
}

impl ProcessingQueueState {
    fn default_for(ack_level: i64) -> Self {
        Self { level: 0, ack_level, max_level: i64::MAX, domain_filter_reverse_match: true }
    }
}

/// `persistence.FailoverLevel`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FailoverLevel {
    pub start_time_nanos: i64,
    pub min_level: HistoryTaskKey,
    pub current_level: HistoryTaskKey,
    pub max_level: HistoryTaskKey,
}

/// `types.FailoverMarkerAttributes` (identity fields).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailoverMarker {
    pub domain_id: String,
    pub failover_version: i64,
}

/// `persistence.ShardInfo`.
#[derive(Clone, Debug, Default)]
pub struct ShardInfoData {
    pub shard_id: i32,
    pub owner: String,
    pub range_id: i64,
    pub stolen_since_renew: i32,
    pub transfer_ack_level: i64,
    pub timer_ack_level: i64,
    pub replication_ack_level: i64,
    pub domain_notification_version: i64,
    pub cluster_transfer_ack_level: HashMap<String, i64>,
    pub cluster_timer_ack_level: HashMap<String, i64>,
    pub cluster_replication_level: HashMap<String, i64>,
    pub queue_states: HashMap<i32, QueueState>,
    pub transfer_processing_queue_states: HashMap<String, Vec<ProcessingQueueState>>,
    pub timer_processing_queue_states: HashMap<String, Vec<ProcessingQueueState>>,
    pub pending_failover_markers: Vec<FailoverMarker>,
}

/// Errors surfaced by the shard context (`ErrShardClosed` and persistence errors).
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ShardContextError {
    /// Go `ErrShardClosed{Msg: "shard closed"}`.
    #[error("shard closed")]
    ShardClosed { closed_at_nanos: i64 },
    #[error("Shard ownership lost: {0}")]
    ShardOwnershipLost(String),
    #[error("{0}")]
    Persistence(String),
    #[error("unknown history task category: {0}")]
    UnknownCategory(String),
    #[error("{0}")]
    Other(String),
}

/// `ShardManager.UpdateShard` boundary.
pub trait ShardPersistence: Send + Sync {
    fn update_shard(&self, info: &ShardInfoData, previous_range_id: i64) -> Result<(), ShardContextError>;
}

/// The subset of `cluster.Metadata` used by the shard context.
pub trait ClusterInfo: Send + Sync {
    fn current_cluster_name(&self) -> String;
    fn enabled_cluster_names(&self) -> Vec<String>;
}

/// Wall clock in unix nanos (`clock.TimeSource.Now`).
pub type TimeSource = Arc<dyn Fn() -> i64 + Send + Sync>;

struct Inner {
    shard_info: ShardInfoData,
    task_sequence_number: i64,
    max_task_sequence_number: i64,
    immediate_task_max_read_level: i64,
    scheduled_task_max_read_level: HashMap<String, i64>,
    remote_cluster_current_time: HashMap<String, i64>,
    failover_levels: HashMap<HistoryTaskCategory, HashMap<String, FailoverLevel>>,
    last_updated: i64,
}

/// Port of `contextImpl`.
pub struct ShardContext {
    shard_id: i32,
    range_size_bits: u32,
    shard_update_min_interval_nanos: i64,
    timer_processor_max_time_shift_nanos: i64,
    persistence: Arc<dyn ShardPersistence>,
    cluster: Arc<dyn ClusterInfo>,
    time_source: TimeSource,
    close_callback: Arc<dyn Fn(i32) + Send + Sync>,
    range_id: AtomicI64,
    closed_at: Mutex<Option<i64>>,
    closed: AtomicBool,
    /// Serialises task-id allocation -> DB write -> max-read-level update (Go holds the shard lock across all three).
    task_write_gate: Arc<tokio::sync::Mutex<()>>,
    inner: Mutex<Inner>,
    previous_shard_owner_was_different: bool,
}

impl ShardContext {
    /// Port of `acquireShard` minus the persistence retry loop (done by the
    /// caller) and engine wiring: builds the context from a loaded shard row,
    /// then renews the range with `isStealing = true`.
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        shard_info: ShardInfoData,
        identity: &str,
        range_size_bits: u32,
        shard_update_min_interval_nanos: i64,
        timer_processor_max_time_shift_nanos: i64,
        persistence: Arc<dyn ShardPersistence>,
        cluster: Arc<dyn ClusterInfo>,
        time_source: TimeSource,
        close_callback: Arc<dyn Fn(i32) + Send + Sync>,
    ) -> Result<Arc<Self>, ShardContextError> {
        let mut updated = shard_info.clone();
        let ownership_changed = shard_info.owner != identity;
        updated.owner = identity.to_string();

        let current = cluster.current_cluster_name();
        let mut remote = HashMap::new();
        let mut max_read = HashMap::new();
        for name in cluster.enabled_cluster_names() {
            let level = if name != current {
                let t = shard_info
                    .cluster_timer_ack_level
                    .get(&name)
                    .copied()
                    .unwrap_or(shard_info.timer_ack_level);
                remote.insert(name.clone(), t);
                t
            } else {
                shard_info.timer_ack_level
            };
            max_read.insert(name, truncate(level, DB_TIMESTAMP_MIN_PRECISION_NANOS));
        }

        let ctx = Arc::new(Self {
            shard_id: updated.shard_id,
            range_size_bits,
            shard_update_min_interval_nanos,
            timer_processor_max_time_shift_nanos,
            persistence,
            cluster,
            time_source,
            close_callback,
            range_id: AtomicI64::new(updated.range_id),
            closed_at: Mutex::new(None),
            closed: AtomicBool::new(false),
            task_write_gate: Arc::new(tokio::sync::Mutex::new(())),
            inner: Mutex::new(Inner {
                shard_info: updated,
                task_sequence_number: 0,
                max_task_sequence_number: 0,
                immediate_task_max_read_level: 0,
                scheduled_task_max_read_level: max_read,
                remote_cluster_current_time: remote,
                failover_levels: HashMap::new(),
                last_updated: 0,
            }),
            previous_shard_owner_was_different: ownership_changed,
        });
        {
            let mut g = ctx.inner.lock().unwrap();
            ctx.renew_range_locked(&mut g, true)?;
        }
        Ok(ctx)
    }

    pub fn shard_id(&self) -> i32 {
        self.shard_id
    }
    pub fn previous_shard_owner_was_different(&self) -> bool {
        self.previous_shard_owner_was_different
    }
    /// Go `GetRangeID` (atomic read, synced with `shardInfo.RangeID`).
    pub fn range_id(&self) -> i64 {
        self.range_id.load(Ordering::SeqCst)
    }
    pub fn shard_info_snapshot(&self) -> ShardInfoData {
        self.inner.lock().unwrap().shard_info.clone()
    }
    pub fn last_updated_time(&self) -> i64 {
        self.inner.lock().unwrap().last_updated
    }

    pub fn now(&self) -> i64 {
        (self.time_source)()
    }

    /// Go `closedError`.
    pub fn closed_error(&self) -> Option<ShardContextError> {
        self.closed_at
            .lock()
            .unwrap()
            .map(|t| ShardContextError::ShardClosed { closed_at_nanos: t })
    }

    /// Go `closeShard`: first caller wins, fails later writes by setting RangeID=-1
    /// and fires the close callback asynchronously.
    pub fn close_shard(&self, inner: Option<&mut ShardInfoData>) {
        {
            let mut c = self.closed_at.lock().unwrap();
            if c.is_some() {
                return;
            }
            *c = Some(self.now());
        }
        self.closed.store(true, Ordering::SeqCst);
        let cb = self.close_callback.clone();
        let id = self.shard_id;
        std::thread::spawn(move || cb(id));
        if let Some(info) = inner {
            info.range_id = -1;
        }
        self.range_id.store(-1, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    // ---- task IDs --------------------------------------------------------

    pub fn generate_task_id(&self) -> Result<i64, ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        self.generate_task_id_locked(&mut g)
    }

    pub fn generate_task_ids(&self, number: usize) -> Result<Vec<i64>, ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        let mut out = Vec::with_capacity(number);
        for _ in 0..number {
            out.push(self.generate_task_id_locked(&mut g)?);
        }
        Ok(out)
    }

    fn generate_task_id_locked(&self, g: &mut Inner) -> Result<i64, ShardContextError> {
        // updateRangeIfNeededLocked
        if g.task_sequence_number >= g.max_task_sequence_number {
            self.renew_range_locked(g, false)?;
        }
        let id = g.task_sequence_number;
        g.task_sequence_number += 1;
        Ok(id)
    }

    fn renew_range_locked(&self, g: &mut Inner, is_stealing: bool) -> Result<(), ShardContextError> {
        let mut updated = g.shard_info.clone();
        updated.range_id += 1;
        if is_stealing {
            updated.stolen_since_renew += 1;
        }
        if let Some(e) = self.closed_error() {
            return Err(e);
        }
        let res = self.persistence.update_shard(&updated, g.shard_info.range_id);
        if let Err(e) = res {
            if matches!(e, ShardContextError::ShardOwnershipLost(_)) {
                // Shard is stolen, trigger history engine shutdown
                self.close_shard(Some(&mut g.shard_info));
            }
            return Err(e);
        }
        g.task_sequence_number = updated.range_id << self.range_size_bits;
        g.max_task_sequence_number = (updated.range_id + 1) << self.range_size_bits;
        g.immediate_task_max_read_level = g.task_sequence_number - 1;
        self.range_id.store(updated.range_id, Ordering::SeqCst);
        g.shard_info = updated;
        Ok(())
    }

    /// Held by a writer from transfer-id allocation until the write result and read-level update are done, so a higher
    /// read level never overtakes a lower, not yet committed task id (Go does this under the shard lock).
    pub async fn lock_task_writes(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.task_write_gate.clone().lock_owned().await
    }

    /// Go `updateMaxReadLevelLocked`, called after a successful write.
    pub fn update_max_read_level(&self, rl: i64) {
        let mut g = self.inner.lock().unwrap();
        if rl > g.immediate_task_max_read_level {
            g.immediate_task_max_read_level = rl;
        }
    }

    // ---- max read levels --------------------------------------------------

    pub fn update_if_needed_and_get_queue_max_read_level(
        &self,
        category: HistoryTaskCategory,
        cluster: &str,
    ) -> HistoryTaskKey {
        match category {
            HistoryTaskCategory::Timer => self.update_scheduled_task_max_read_level(cluster),
            _ => HistoryTaskKey::immediate(self.inner.lock().unwrap().immediate_task_max_read_level),
        }
    }

    fn update_scheduled_task_max_read_level(&self, cluster: &str) -> HistoryTaskKey {
        let mut g = self.inner.lock().unwrap();
        let mut current = self.now();
        if !cluster.is_empty() && cluster != self.cluster.current_cluster_name() {
            current = g.remote_cluster_current_time.get(cluster).copied().unwrap_or(0);
        }
        let new_level = truncate(
            current + self.timer_processor_max_time_shift_nanos,
            DB_TIMESTAMP_MIN_PRECISION_NANOS,
        );
        let entry = g.scheduled_task_max_read_level.entry(cluster.to_string()).or_insert(0);
        if new_level > *entry {
            *entry = new_level;
        }
        HistoryTaskKey::scheduled(*entry, 0)
    }

    // ---- ack levels -------------------------------------------------------

    pub fn get_queue_ack_level(&self, category: HistoryTaskCategory) -> HistoryTaskKey {
        let g = self.inner.lock().unwrap();
        match category {
            HistoryTaskCategory::Transfer => HistoryTaskKey::immediate(g.shard_info.transfer_ack_level),
            HistoryTaskCategory::Timer => HistoryTaskKey::scheduled(g.shard_info.timer_ack_level, 0),
            HistoryTaskCategory::Replication => HistoryTaskKey::immediate(g.shard_info.replication_ack_level),
        }
    }

    pub fn update_queue_ack_level(
        &self,
        category: HistoryTaskCategory,
        ack_level: HistoryTaskKey,
    ) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        match category {
            HistoryTaskCategory::Transfer => {
                g.shard_info.transfer_ack_level = ack_level.task_id;
                g.shard_info.queue_states.insert(
                    HistoryTaskCategory::Transfer.id(),
                    QueueState {
                        exclusive_max_read_level: TaskKey { task_id: ack_level.task_id + 1, scheduled_time_nano: 0 },
                        virtual_slice_inclusive_mins: vec![],
                    },
                );
            }
            HistoryTaskCategory::Timer => {
                g.shard_info.timer_ack_level = ack_level.scheduled_time_nanos;
                g.shard_info.queue_states.insert(
                    HistoryTaskCategory::Timer.id(),
                    QueueState {
                        exclusive_max_read_level: TaskKey {
                            task_id: 0,
                            scheduled_time_nano: ack_level.scheduled_time_nanos,
                        },
                        virtual_slice_inclusive_mins: vec![],
                    },
                );
            }
            HistoryTaskCategory::Replication => g.shard_info.replication_ack_level = ack_level.task_id,
        }
        g.shard_info.stolen_since_renew = 0;
        self.persist_shard_info_locked(&mut g, false)
    }

    pub fn get_queue_cluster_ack_level(&self, category: HistoryTaskCategory, cluster: &str) -> HistoryTaskKey {
        let g = self.inner.lock().unwrap();
        let si = &g.shard_info;
        match category {
            HistoryTaskCategory::Transfer => HistoryTaskKey::immediate(
                si.cluster_transfer_ack_level.get(cluster).copied().unwrap_or(si.transfer_ack_level),
            ),
            HistoryTaskCategory::Timer => HistoryTaskKey::scheduled(
                si.cluster_timer_ack_level.get(cluster).copied().unwrap_or(si.timer_ack_level),
                0,
            ),
            // New cluster always starts from -1
            HistoryTaskCategory::Replication => {
                HistoryTaskKey::immediate(si.cluster_replication_level.get(cluster).copied().unwrap_or(-1))
            }
        }
    }

    pub fn update_queue_cluster_ack_level(
        &self,
        category: HistoryTaskCategory,
        cluster: &str,
        ack_level: HistoryTaskKey,
    ) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        match category {
            HistoryTaskCategory::Transfer => {
                g.shard_info.cluster_transfer_ack_level.insert(cluster.to_string(), ack_level.task_id);
            }
            HistoryTaskCategory::Timer => {
                g.shard_info.cluster_timer_ack_level.insert(cluster.to_string(), ack_level.scheduled_time_nanos);
            }
            HistoryTaskCategory::Replication => {
                g.shard_info.cluster_replication_level.insert(cluster.to_string(), ack_level.task_id);
            }
        }
        g.shard_info.stolen_since_renew = 0;
        self.persist_shard_info_locked(&mut g, false)
    }

    // ---- queue states -----------------------------------------------------

    pub fn get_queue_state(&self, category: HistoryTaskCategory) -> Result<QueueState, ShardContextError> {
        let g = self.inner.lock().unwrap();
        if let Some(s) = g.shard_info.queue_states.get(&category.id()) {
            return Ok(s.clone());
        }
        match category {
            HistoryTaskCategory::Transfer => Ok(QueueState {
                exclusive_max_read_level: TaskKey { task_id: g.shard_info.transfer_ack_level + 1, scheduled_time_nano: 0 },
                virtual_slice_inclusive_mins: vec![],
            }),
            HistoryTaskCategory::Timer => Ok(QueueState {
                exclusive_max_read_level: TaskKey { task_id: 0, scheduled_time_nano: g.shard_info.timer_ack_level },
                virtual_slice_inclusive_mins: vec![],
            }),
            HistoryTaskCategory::Replication => {
                Err(ShardContextError::UnknownCategory(format!("{:?}", category)))
            }
        }
    }

    pub fn update_queue_state(
        &self,
        category: HistoryTaskCategory,
        state: QueueState,
    ) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        g.shard_info.queue_states.insert(category.id(), state.clone());
        let clusters = self.cluster.enabled_cluster_names();
        match category {
            HistoryTaskCategory::Transfer => {
                let mut ack = state.exclusive_max_read_level.task_id;
                for m in &state.virtual_slice_inclusive_mins {
                    ack = ack.min(m.task_id);
                }
                ack -= 1; // exclusive to inclusive
                g.shard_info.transfer_ack_level = ack;
                for c in clusters {
                    g.shard_info.cluster_transfer_ack_level.insert(c.clone(), ack);
                    g.shard_info
                        .transfer_processing_queue_states
                        .insert(c, vec![ProcessingQueueState::default_for(ack)]);
                }
            }
            HistoryTaskCategory::Timer => {
                let mut ack = state.exclusive_max_read_level.scheduled_time_nano;
                for m in &state.virtual_slice_inclusive_mins {
                    ack = ack.min(m.scheduled_time_nano);
                }
                g.shard_info.timer_ack_level = ack;
                for c in clusters {
                    g.shard_info.cluster_timer_ack_level.insert(c.clone(), ack);
                    g.shard_info
                        .timer_processing_queue_states
                        .insert(c, vec![ProcessingQueueState::default_for(ack)]);
                }
            }
            HistoryTaskCategory::Replication => {
                // Go: fmt.Errorf("replication queue state is not supported")
                return Err(ShardContextError::Other("replication queue state is not supported".into()));
            }
        }
        g.shard_info.stolen_since_renew = 0;
        self.persist_shard_info_locked(&mut g, false)
    }

    pub fn get_transfer_processing_queue_states(&self, cluster: &str) -> Vec<ProcessingQueueState> {
        let g = self.inner.lock().unwrap();
        if let Some(s) = g.shard_info.transfer_processing_queue_states.get(cluster) {
            return s.clone();
        }
        let ack = g
            .shard_info
            .cluster_transfer_ack_level
            .get(cluster)
            .copied()
            .unwrap_or(g.shard_info.transfer_ack_level);
        vec![ProcessingQueueState::default_for(ack)]
    }

    pub fn update_transfer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<ProcessingQueueState>,
    ) -> Result<(), ShardContextError> {
        if states.is_empty() {
            return Err(ShardContextError::Other("empty transfer processing queue states".into()));
        }
        let mut g = self.inner.lock().unwrap();
        let ack = states.iter().map(|s| s.ack_level).min().unwrap_or(0);
        g.shard_info.transfer_processing_queue_states.insert(cluster.to_string(), states);
        g.shard_info.cluster_transfer_ack_level.insert(cluster.to_string(), ack);
        g.shard_info.stolen_since_renew = 0;
        self.persist_shard_info_locked(&mut g, false)
    }

    pub fn get_timer_processing_queue_states(&self, cluster: &str) -> Vec<ProcessingQueueState> {
        let g = self.inner.lock().unwrap();
        if let Some(s) = g.shard_info.timer_processing_queue_states.get(cluster) {
            return s.clone();
        }
        let ack = g
            .shard_info
            .cluster_timer_ack_level
            .get(cluster)
            .copied()
            .unwrap_or(g.shard_info.timer_ack_level);
        vec![ProcessingQueueState::default_for(ack)]
    }

    pub fn update_timer_processing_queue_states(
        &self,
        cluster: &str,
        states: Vec<ProcessingQueueState>,
    ) -> Result<(), ShardContextError> {
        if states.is_empty() {
            // Go reuses the "transfer" message here.
            return Err(ShardContextError::Other("empty transfer processing queue states".into()));
        }
        let mut g = self.inner.lock().unwrap();
        let ack = states.iter().map(|s| s.ack_level).min().unwrap_or(0);
        g.shard_info.timer_processing_queue_states.insert(cluster.to_string(), states);
        g.shard_info.cluster_timer_ack_level.insert(cluster.to_string(), ack);
        g.shard_info.stolen_since_renew = 0;
        self.persist_shard_info_locked(&mut g, false)
    }

    // ---- failover levels --------------------------------------------------

    pub fn update_failover_level(&self, category: HistoryTaskCategory, failover_id: &str, level: FailoverLevel) {
        let mut g = self.inner.lock().unwrap();
        g.failover_levels.entry(category).or_default().insert(failover_id.to_string(), level);
    }

    /// Go `DeleteFailoverLevel`; returns the failover latency (nanos) that Go
    /// records as a timer metric when the level existed.
    pub fn delete_failover_level(&self, category: HistoryTaskCategory, failover_id: &str) -> Option<i64> {
        let mut g = self.inner.lock().unwrap();
        let level = g.failover_levels.get_mut(&category)?.remove(failover_id)?;
        Some(self.now() - level.start_time_nanos)
    }

    pub fn get_all_failover_levels(&self, category: HistoryTaskCategory) -> HashMap<String, FailoverLevel> {
        let g = self.inner.lock().unwrap();
        g.failover_levels.get(&category).cloned().unwrap_or_default()
    }

    // ---- domain notification version, current time ------------------------

    pub fn get_domain_notification_version(&self) -> i64 {
        self.inner.lock().unwrap().shard_info.domain_notification_version
    }

    pub fn update_domain_notification_version(&self, v: i64) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        g.shard_info.domain_notification_version = v;
        self.persist_shard_info_locked(&mut g, false)
    }

    /// Go `SetCurrentTime`; panics for the current cluster like Go.
    pub fn set_current_time(&self, cluster: &str, current_time: i64) {
        let mut g = self.inner.lock().unwrap();
        if cluster == self.cluster.current_cluster_name() {
            panic!("Cannot set current time for current cluster");
        }
        let prev = g.remote_cluster_current_time.get(cluster).copied().unwrap_or(0);
        if prev < current_time {
            g.remote_cluster_current_time.insert(cluster.to_string(), current_time);
        }
    }

    pub fn get_current_time(&self, cluster: &str) -> i64 {
        let g = self.inner.lock().unwrap();
        if cluster != self.cluster.current_cluster_name() {
            return g.remote_cluster_current_time.get(cluster).copied().unwrap_or(0);
        }
        self.now()
    }

    // ---- failover markers -------------------------------------------------

    /// Go `ValidateAndUpdateFailoverMarkers` core: `is_stale(marker)` is the
    /// domain-cache based drop decision (deprecated / active / newer version /
    /// completed, or domain not found). Returns the remaining markers.
    pub fn validate_and_update_failover_markers(
        &self,
        mut is_stale: impl FnMut(&FailoverMarker) -> Result<bool, ShardContextError>,
    ) -> Result<Vec<FailoverMarker>, ShardContextError> {
        let pending = self.inner.lock().unwrap().shard_info.pending_failover_markers.clone();
        let mut completed: HashSet<usize> = HashSet::new();
        for (i, m) in pending.iter().enumerate() {
            if is_stale(m)? {
                completed.insert(i);
            }
        }
        if completed.is_empty() {
            return Ok(pending);
        }
        let mut g = self.inner.lock().unwrap();
        let remaining: Vec<FailoverMarker> = g
            .shard_info
            .pending_failover_markers
            .iter()
            .filter(|m| !pending.iter().enumerate().any(|(i, p)| completed.contains(&i) && p == *m))
            .cloned()
            .collect();
        g.shard_info.pending_failover_markers = remaining;
        self.persist_shard_info_locked(&mut g, false)?;
        Ok(g.shard_info.pending_failover_markers.clone())
    }

    /// Go `AddingPendingFailoverMarker` after the domain-cache skip check.
    pub fn add_pending_failover_marker(&self, marker: FailoverMarker) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        g.shard_info.pending_failover_markers.push(marker);
        self.persist_shard_info_locked(&mut g, true)
    }

    // ---- shard info persistence -------------------------------------------

    fn persist_shard_info_locked(&self, g: &mut Inner, forced: bool) -> Result<(), ShardContextError> {
        if let Some(e) = self.closed_error() {
            return Err(e);
        }
        let now = self.now();
        if !forced && g.last_updated + self.shard_update_min_interval_nanos > now {
            return Ok(());
        }
        let updated = g.shard_info.clone();
        match self.persistence.update_shard(&updated, g.shard_info.range_id) {
            Err(e) => {
                if matches!(e, ShardContextError::ShardOwnershipLost(_)) {
                    self.close_shard(Some(&mut g.shard_info));
                }
                Err(e)
            }
            Ok(()) => {
                g.last_updated = now;
                Ok(())
            }
        }
    }

    // ---- task ID assignment (allocateTransferIDsLocked / allocateTimerIDsLocked)

    /// Go `allocateTransferIDsLocked`: assigns IDs and returns the max read
    /// level to apply after the DB write succeeds. `tasks` are
    /// (task_id, visibility_timestamp) slots; a zero visibility is set to now.
    pub fn allocate_transfer_ids(&self, tasks: &mut [(i64, i64)], max_read_level: &mut i64) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        let now = self.now();
        for t in tasks.iter_mut() {
            let id = self.generate_task_id_locked(&mut g)?;
            t.0 = id;
            if t.1 == 0 {
                t.1 = now;
            }
            *max_read_level = id;
        }
        Ok(())
    }

    /// Go `allocateTimerIDsLocked` for `cluster` (already resolved by the caller
    /// from the domain entry / failover version).
    pub fn allocate_timer_ids(&self, cluster: &str, tasks: &mut [(i64, i64)]) -> Result<(), ShardContextError> {
        let mut g = self.inner.lock().unwrap();
        let now = truncate(self.now(), DB_TIMESTAMP_MIN_PRECISION_NANOS);
        for t in tasks.iter_mut() {
            let mut ts = truncate(t.1, DB_TIMESTAMP_MIN_PRECISION_NANOS);
            let read_cursor = g.scheduled_task_max_read_level.get(cluster).copied().unwrap_or(0);
            if ts < read_cursor {
                ts = read_cursor + DB_TIMESTAMP_MIN_PRECISION_NANOS;
            }
            if ts < now {
                ts = now + DB_TIMESTAMP_MIN_PRECISION_NANOS;
            }
            t.1 = ts;
            t.0 = self.generate_task_id_locked(&mut g)?;
        }
        Ok(())
    }
}

fn truncate(v: i64, unit: i64) -> i64 {
    v - v.rem_euclid(unit)
}

/// Go `lookup.HistoryServerByShardID`: the resolver key is `string(rune(shardID))`.
pub fn history_server_key_by_shard_id(shard_id: i32) -> String {
    char::from_u32(shard_id as u32).unwrap_or('\u{FFFD}').to_string()
}

/// `dlqAckLevelKey`.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DlqAckLevelKey {
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category_id: i32,
}

/// `shardedHistoryTaskDLQWriter`: dedups the idempotent ack-level write per
/// partition/task-category for the life of the shard.
#[derive(Default)]
pub struct ShardedDlqAckLevelCache {
    created: Mutex<HashSet<DlqAckLevelKey>>,
}

impl ShardedDlqAckLevelCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs `write` unless the key was already created; caches on success.
    pub fn create_if_not_exists<E>(
        &self,
        key: DlqAckLevelKey,
        write: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), E> {
        if self.created.lock().unwrap().contains(&key) {
            return Ok(());
        }
        write()?;
        self.created.lock().unwrap().insert(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct Store(Mutex<Vec<(i64, i64)>>, bool);
    impl ShardPersistence for Store {
        fn update_shard(&self, info: &ShardInfoData, prev: i64) -> Result<(), ShardContextError> {
            if self.1 {
                return Err(ShardContextError::ShardOwnershipLost("x".into()));
            }
            self.0.lock().unwrap().push((info.range_id, prev));
            Ok(())
        }
    }
    struct Cl;
    impl ClusterInfo for Cl {
        fn current_cluster_name(&self) -> String {
            "a".into()
        }
        fn enabled_cluster_names(&self) -> Vec<String> {
            vec!["a".into(), "b".into()]
        }
    }

    fn ctx(store: Arc<Store>, closed: Arc<AtomicUsize>) -> Result<Arc<ShardContext>, ShardContextError> {
        ShardContext::acquire(
            ShardInfoData { shard_id: 3, range_id: 4, ..Default::default() },
            "host1",
            4,
            0,
            1_000_000_000,
            store,
            Arc::new(Cl),
            Arc::new(|| 5_000_000_000),
            Arc::new(move |_| {
                closed.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    #[test]
    fn update_queue_state_replication_not_supported_like_go() {
        let store = Arc::new(Store(Mutex::new(vec![]), false));
        let c = ctx(store.clone(), Arc::new(AtomicUsize::new(0))).unwrap();
        let before = store.0.lock().unwrap().len();
        let err = c
            .update_queue_state(HistoryTaskCategory::Replication, QueueState::default())
            .unwrap_err();
        assert_eq!(err.to_string(), "replication queue state is not supported");
        // Go returns before updateShardInfoLocked: nothing persisted.
        assert_eq!(store.0.lock().unwrap().len(), before);
    }

    #[tokio::test]
    async fn task_write_gate_keeps_read_level_behind_uncommitted_ids() {
        let store = Arc::new(Store(Mutex::new(vec![]), false));
        let c = Arc::new(ctx(store, Arc::new(AtomicUsize::new(0))).unwrap());
        let read = |c: &ShardContext| c.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, "");
        // Writer A allocates id 80 and has not committed yet.
        let gate_a = c.lock_task_writes().await;
        let mut slots_a = vec![(0, 0)];
        let mut level_a = 0;
        c.allocate_transfer_ids(&mut slots_a, &mut level_a).unwrap();
        // Writer B must not be able to allocate (and so cannot raise the read level to 81) while A is in flight.
        let c2 = c.clone();
        let b = tokio::spawn(async move {
            let _gate_b = c2.lock_task_writes().await;
            let mut slots_b = vec![(0, 0)];
            let mut level_b = 0;
            c2.allocate_transfer_ids(&mut slots_b, &mut level_b).unwrap();
            c2.update_max_read_level(level_b);
            slots_b[0].0
        });
        tokio::task::yield_now().await;
        assert!(!b.is_finished(), "second writer ran before the first committed");
        assert_eq!(read(&c), HistoryTaskKey::immediate(79));
        c.update_max_read_level(level_a);
        drop(gate_a);
        assert_eq!(b.await.unwrap(), 81);
        assert_eq!(read(&c), HistoryTaskKey::immediate(81));
        assert_eq!(slots_a[0].0, 80);
    }

    #[test]
    fn acquire_renews_range_and_allocates_ids() {
        let store = Arc::new(Store(Mutex::new(vec![]), false));
        let c = ctx(store.clone(), Arc::new(AtomicUsize::new(0))).unwrap();
        assert_eq!(c.range_id(), 5);
        assert_eq!(store.0.lock().unwrap()[0], (5, 4));
        assert!(c.previous_shard_owner_was_different());
        // range 5, 4 size bits => ids start at 80 and the read level is 79
        assert_eq!(c.generate_task_id().unwrap(), 80);
        assert_eq!(c.generate_task_ids(2).unwrap(), vec![81, 82]);
        assert_eq!(
            c.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, ""),
            HistoryTaskKey::immediate(79)
        );
        // exhausting the range renews it
        for _ in 0..13 {
            c.generate_task_id().unwrap();
        }
        assert_eq!(c.generate_task_id().unwrap(), 96);
        assert_eq!(c.range_id(), 6);
    }

    #[test]
    fn stolen_shard_closes_and_fails_writes() {
        let store = Arc::new(Store(Mutex::new(vec![]), true));
        let closed = Arc::new(AtomicUsize::new(0));
        assert!(ctx(store, closed).is_err());
    }

    #[test]
    fn ack_levels_and_queue_state_backward_compat() {
        let store = Arc::new(Store(Mutex::new(vec![]), false));
        let c = ctx(store, Arc::new(AtomicUsize::new(0))).unwrap();
        c.update_queue_ack_level(HistoryTaskCategory::Transfer, HistoryTaskKey::immediate(9)).unwrap();
        assert_eq!(c.get_queue_ack_level(HistoryTaskCategory::Transfer).task_id, 9);
        assert_eq!(c.get_queue_state(HistoryTaskCategory::Transfer).unwrap().exclusive_max_read_level.task_id, 10);
        assert_eq!(c.get_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b").task_id, -1);
        c.update_queue_state(
            HistoryTaskCategory::Transfer,
            QueueState {
                exclusive_max_read_level: TaskKey { task_id: 20, scheduled_time_nano: 0 },
                virtual_slice_inclusive_mins: vec![TaskKey { task_id: 12, scheduled_time_nano: 0 }],
            },
        )
        .unwrap();
        assert_eq!(c.get_queue_ack_level(HistoryTaskCategory::Transfer).task_id, 11);
        assert_eq!(c.get_transfer_processing_queue_states("b")[0].ack_level, 11);
        assert!(c.update_queue_state(HistoryTaskCategory::Replication, QueueState::default()).is_err());
    }

    #[test]
    fn timer_ids_are_pushed_past_read_level_and_now() {
        let store = Arc::new(Store(Mutex::new(vec![]), false));
        let c = ctx(store, Arc::new(AtomicUsize::new(0))).unwrap();
        let mut tasks = [(0i64, 1_000_000i64)];
        c.allocate_timer_ids("a", &mut tasks).unwrap();
        assert_eq!(tasks[0].1, 5_000_000_000 + DB_TIMESTAMP_MIN_PRECISION_NANOS);
        assert_eq!(tasks[0].0, 80);
    }

    #[test]
    fn lookup_key_and_dlq_dedup() {
        assert_eq!(history_server_key_by_shard_id(65), "A");
        let cache = ShardedDlqAckLevelCache::new();
        let n = AtomicUsize::new(0);
        let key = DlqAckLevelKey {
            domain_id: "d".into(),
            cluster_attribute_scope: "s".into(),
            cluster_attribute_name: "n".into(),
            task_category_id: 1,
        };
        for _ in 0..3 {
            cache
                .create_if_not_exists::<()>(key.clone(), || {
                    n.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
}
