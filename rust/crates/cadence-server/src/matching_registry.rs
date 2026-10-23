//! Matching-owned queue lifecycle, independent of workflow execution state.
use crate::{matching_dispatch::TaskDispatcher, matching_history::MatchingHistoryClient};
use cadence_persistence::{
    matching_blob::TaskData,
    matching_queue::DurableTaskQueue,
    matching_task_store::{AllocatedTask, MatchingTaskStore, TaskListKey},
};
use cadence_proto::matching as m;
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex, RwLock, Weak},
};
use tonic::Status;
use uuid::Uuid;

fn registry_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
type Key = (Uuid, String, i32);
type StartupGates = HashMap<Key, Weak<Mutex<()>>>;
type LocalPollFactory = Arc<dyn Fn(QueueRegistry) -> crate::grpc_matching::Matching + Send + Sync>;

/// One partition's DescribeTaskList state as the adaptive scaler consumes it
/// (Go `toPartitionMetrics` inputs). `groups` holds each isolation group's
/// arrival QPS and whether it has pollers.
struct ScaledPartition {
    qps: f64,
    empty: bool,
    has_write: bool,
    groups: Vec<(String, f64, bool)>,
}

/// Go `toPartitionMetrics` for a partition described over RPC. Like Go, the
/// owner's partition config decides `has_write`, defaulting to true when the
/// response carries no config.
fn scaled_partition_from_response(
    id: i32,
    response: &m::DescribeTaskListResponse,
    empty_check: bool,
) -> ScaledPartition {
    let status = response.task_list_status.clone().unwrap_or_default();
    ScaledPartition {
        qps: status.new_tasks_per_second,
        empty: if empty_check {
            status.empty
        } else {
            status.backlog_count_hint == 0
        },
        has_write: response
            .partition_config
            .as_ref()
            .is_none_or(|config| config.write_partitions.contains_key(&id)),
        groups: status
            .isolation_group_metrics
            .iter()
            .map(|(group, metrics)| {
                (
                    group.clone(),
                    metrics.new_tasks_per_second,
                    metrics.poller_count > 0,
                )
            })
            .collect(),
    }
}

/// Synchronous snapshot policy used while the queue registry lock is held.
/// The RPC path refreshes assignment asynchronously before calling `get`;
/// this policy closes races with assignment drain and startup publication.
#[derive(Clone)]
pub struct QueueOwnershipPolicy {
    is_shard_distributor_eligible: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    has_local_processor: Arc<dyn Fn(&str) -> bool + Send + Sync>,
}

impl QueueOwnershipPolicy {
    #[must_use]
    pub fn new(
        is_shard_distributor_eligible: Arc<dyn Fn(&str) -> bool + Send + Sync>,
        has_local_processor: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            is_shard_distributor_eligible,
            has_local_processor,
        }
    }
}
struct Owner {
    queue: Arc<DurableTaskQueue>,
    dispatcher: TaskDispatcher,
    writer: crate::matching_writer::TaskWriter,
    reader: crate::matching_reader::TaskReader,
    limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    queries: Arc<crate::matching_query::QueryChannel>,
    qps: Arc<cadence_common::qps::EmaQpsTracker>,
    partitions: Arc<crate::matching_partition::PartitionCache>,
    sync_matcher: Arc<crate::matching_sync::SyncMatcher>,
    isolation_select: crate::matching_reader::IsolationSelector,
    sync_wait: Arc<dyn Fn() -> std::time::Duration + Send + Sync>,
    forward_tasks: Arc<crate::matching_forward::RequestTokens>,
    forward_rate: Arc<crate::matching_forward::ForwardRate>,
    /// Go `adaptiveScalerImpl` daemon handle. Aborted on unload/shutdown/drop
    /// so a stale loop can neither resurrect the owner through `get` nor pin
    /// the registry (and its task store) past its lifetime.
    scaler: Mutex<Option<tokio::task::AbortHandle>>,
    /// Go `taskListManagerImpl.createTime`: when the manager was created, used
    /// to withhold `LoadBalancerHints` for a task list's first 10s.
    created: i64,
    idle_ttl_nanos: i64,
    last_update: Mutex<tokio::time::Instant>,
    next_ack_update: Mutex<Option<tokio::time::Instant>>,
    next_idle_check: Mutex<tokio::time::Instant>,
    liveness: Arc<cadence_common::liveness::Liveness>,
}
impl Owner {
    fn abort_scaler(&self) {
        if let Some(handle) = self.scaler.lock().expect("Matching scaler lock").take() {
            handle.abort();
        }
    }
    fn stop(&self) {
        self.writer.stop();
        self.reader.stop();
        self.queue.stop();
        self.dispatcher.notify_new_tasks();
    }
    fn finish(
        &self,
        now: i64,
        readers: &mut Vec<crate::matching_reader::TaskReader>,
    ) -> Result<(), Status> {
        self.abort_scaler();
        self.stop();
        readers.push(self.reader.clone());
        let ack = self.queue.persist_final_ack(now);
        let garbage = self.queue.run_garbage_collection(true);
        ack.and(garbage.map(|_| ()))
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.abort_scaler();
    }
}
#[derive(Default)]
struct State {
    owners: HashMap<Key, Arc<Owner>>,
    stopped: bool,
}
/// Queue creation and insertion run in the same blocking worker, so cancelling
/// an RPC while its lease is acquired cannot orphan or duplicate that lease.
#[derive(Clone)]
pub struct QueueRegistry {
    store: Arc<dyn MatchingTaskStore>,
    history: MatchingHistoryClient,
    range_size: NonZeroU32,
    state: Arc<Mutex<State>>,
    changed: Arc<tokio::sync::Notify>,
    startups: Arc<Mutex<StartupGates>>,
    ownership_policy: Arc<RwLock<Option<QueueOwnershipPolicy>>>,
    shard_router: Arc<RwLock<Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>>>,
    standby_enabled: Option<crate::matching_standby::Enabled>,
    membership: Option<crate::matching_membership::Membership>,
    local_poll_service: Option<LocalPollFactory>,
    domain_metadata: Option<(cadence_persistence::InMemoryDomainStore, String)>,
    /// Go `cfg.NumTasklistReadPartitions(domain, taskList, type)`, clamped to
    /// >= 1 by the same caller. `None` keeps the single-partition default.
    partition_count: Option<PartitionCountProvider>,
    cache_enabled: Option<PartitionCacheEnabledProvider>,
    sync_wait: Option<SyncWaitProvider>,
    local_poll_wait: Option<SyncWaitProvider>,
    /// Go `cfg.ForwarderMaxChildrenPerNode(domain, taskList, type)`
    /// (`matching.forwarderMaxChildrenPerNode`, default 20). `None` uses the default.
    forward_degree: Option<ForwardDegreeProvider>,
    /// Go `cfg.ForwarderMaxOutstandingPolls(domain, taskList, type)`
    /// (`matching.forwarderMaxOutstandingPolls`, default 1). `None` uses the default.
    forward_polls: Option<ForwardPollsProvider>,
    forward_tasks: Option<ForwardPollsProvider>,
    forward_rate: Option<ForwardPollsProvider>,
    scaler_config: Option<ScalerConfigProvider>,
    /// Go `cfg.QPSTrackerInterval(domain, taskList, type)`. `None` uses the
    /// dynamic-property default (10s).
    qps_interval: Option<QpsIntervalProvider>,
    isolation: Option<IsolationProvider>,
    /// Live reader configuration (Go's `TaskListConfig.GetTasksBatchSize`,
    /// `ReadRangeSize` and `AsyncTaskDispatchTimeout`). `None` keeps the Go
    /// defaults.
    reader_live: Option<ReaderPropertiesProvider>,
    /// Go `cfg.MinTaskThrottlingBurstSize(domain, taskList, type)`, read once
    /// per limiter construction. `None` uses the property default (1).
    min_burst: Option<SyncWaitProvider>,
    /// Go `cfg.UpdateAckInterval(domain, taskList, type)` in nanoseconds,
    /// read per maintenance update. `None` uses the caller-supplied interval.
    update_ack_interval: Option<SyncWaitProvider>,
    /// Go `cfg.IdleTasklistCheckInterval(domain, taskList, type)` in
    /// nanoseconds, read once per owner for liveness TTL and idle checks.
    idle_tasklist_check_interval: Option<SyncWaitProvider>,
    /// Go's signed, task-list-filtered `MaxTaskDeleteBatchSize` provider.
    max_task_delete_batch_size: Option<TaskDeleteBatchSizeProvider>,
    writer_live: Option<WriterLiveProvider>,
    metrics: crate::matching_metrics::MatchingMetrics,
}
/// Live adaptive-scaler configuration per domain/task-list/type, mirroring
/// Go's `TaskListConfig` scaler reads. `None` keeps the scaler parked.
pub type ScalerConfigProvider =
    Arc<dyn Fn(&str, &str, i32) -> crate::matching_scaler::ScalerConfig + Send + Sync>;
pub type IsolationProvider = Arc<
    dyn Fn(
            &str,
            &str,
            i32,
            i16,
            Arc<Mutex<cadence_common::poller::Manager>>,
            Arc<crate::matching_partition::PartitionCache>,
        ) -> crate::matching_reader::IsolationConfig
        + Send
        + Sync,
>;
/// Live read-partition-count provider used to size the dispatch limiter and to
/// divide dispatch rate, mirroring Go's `NumTasklistReadPartitions`.
pub type SyncWaitProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
/// Go's signed `MaxTaskDeleteBatchSize` value, filtered by domain/list/type.
pub type TaskDeleteBatchSizeProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
pub type WriterLiveProvider = Arc<dyn Fn(&str, &str, i32) -> (i64, i64, i64) + Send + Sync>;
pub type WriterLiveConfig = Arc<dyn Fn() -> (i64, i64, i64) + Send + Sync>;
fn scoped_writer_live(
    provider: WriterLiveProvider,
    domain: String,
    task_list: String,
    task_type: i32,
) -> WriterLiveConfig {
    Arc::new(move || provider(&domain, &task_list, task_type))
}
/// Partition-tree degree used to resolve a child partition's parent for
/// add/poll forwarding, mirroring Go's `ForwarderMaxChildrenPerNode`.
pub type ForwardDegreeProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
/// Bound on concurrent forwarded polls per child partition, mirroring Go's
/// `ForwarderMaxOutstandingPolls`.
pub type ForwardPollsProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
pub type PartitionCacheEnabledProvider = Arc<dyn Fn(&str, &str, i32) -> bool + Send + Sync>;
pub type PartitionCountProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
/// Live QPS-tracker window per task list, mirroring Go's
/// `cfg.QPSTrackerInterval(domain, taskList, type)` (`matching.qpsTrackerInterval`,
/// default 10s). Returned in nanoseconds.
pub type QpsIntervalProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
/// Live reader configuration for one owner, mirroring the `TaskListConfig`
/// reads Go's `taskReader` performs (batch size, read range, dispatch timeout).
pub type ReaderPropertiesProvider =
    Arc<dyn Fn(&str, &str, i32) -> crate::matching_reader::ReaderLive + Send + Sync>;
/// Go `config.QPSTrackerInterval` default.
pub const DEFAULT_QPS_TRACKER_INTERVAL_NANOS: i64 = 10_000_000_000;
/// Go `taskListManagerImpl` constructs its tracker with `exp = 0.5`.
const QPS_TRACKER_EXP: f64 = 0.5;
#[derive(Clone)]
pub struct QueueHandle {
    pub queue: Arc<DurableTaskQueue>,
    pub dispatcher: TaskDispatcher,
    pub pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    pub limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    /// Go `matcher.queryTaskC` plus the engine's `lockableQueryTaskMap`.
    pub queries: Arc<crate::matching_query::QueryChannel>,
    /// Go `taskListManagerImpl.qpsTracker`: arrival QPS reported through
    /// `DescribeTaskList.newTasksPerSecond` and the isolation group metrics.
    pub qps: Arc<cadence_common::qps::EmaQpsTracker>,
    pub partitions: Arc<crate::matching_partition::PartitionCache>,
    sync_matcher: Arc<crate::matching_sync::SyncMatcher>,
    isolation_select: crate::matching_reader::IsolationSelector,
    sync_wait: Arc<dyn Fn() -> std::time::Duration + Send + Sync>,
    created: i64,
    liveness: Arc<cadence_common::liveness::Liveness>,
    writer: crate::matching_writer::TaskWriter,
    forward_tasks: Arc<crate::matching_forward::RequestTokens>,
    forward_rate: Arc<crate::matching_forward::ForwardRate>,
}
pub enum ForwardTaskPermitOutcome {
    Acquired(tokio::sync::OwnedSemaphorePermit),
    NoToken,
    RateLimited,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMatchOutcome {
    Matched,
    Unmatched,
    Throttled,
}
impl QueueHandle {
    pub fn try_forward_task(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match self.try_forward_task_observed() {
            ForwardTaskPermitOutcome::Acquired(permit) => Some(permit),
            ForwardTaskPermitOutcome::NoToken | ForwardTaskPermitOutcome::RateLimited => None,
        }
    }
    pub fn try_forward_task_observed(&self) -> ForwardTaskPermitOutcome {
        let Some(permit) = self.forward_tasks.try_acquire() else {
            return ForwardTaskPermitOutcome::NoToken;
        };
        if self.forward_rate.allow(registry_now()) {
            ForwardTaskPermitOutcome::Acquired(permit)
        } else {
            ForwardTaskPermitOutcome::RateLimited
        }
    }
    /// Go `OfferQuery` takes a forward-task token but `ForwardQueryTask`
    /// applies no rate limit, unlike `ForwardTask`.
    pub fn try_forward_query(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.forward_tasks.try_acquire()
    }
    /// Go AddTask/GetTask stop their owner when the domain isolation-enable
    /// snapshot changes. Existing polls are released by the normal stop path.
    pub fn stop_if_isolation_changed(&self) -> bool {
        if self.dispatcher.reader_should_reload() {
            self.queue.stop();
            true
        } else {
            false
        }
    }
    pub fn mark_alive(&self) {
        self.liveness.mark_alive(registry_now());
    }
    pub async fn try_sync_match(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
    ) -> bool {
        self.offer_sync(data, source_domain, deadline, true, false).await
    }
    pub async fn try_sync_match_observed(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
    ) -> SyncMatchOutcome {
        self.offer_sync_observed(data, source_domain, deadline, true, false).await
    }
    /// Go `TaskMatcher.Offer` skips the dispatch rate limiter for tasks
    /// already forwarded from a child partition; the entry partition limited
    /// them before forwarding.
    pub async fn try_sync_match_forwarded(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
    ) -> bool {
        self.offer_sync(data, source_domain, deadline, false, true).await
    }
    pub async fn offer_forwarded_backlog(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
    ) -> bool {
        let group = (self.isolation_select)(data).0;
        self.sync_matcher
            .offer(
                data.clone(),
                &group,
                &source_domain.to_string(),
                true,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                deadline,
            )
            .await
    }
    async fn offer_sync(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
        limit: bool,
        forwarded: bool,
    ) -> bool {
        matches!(
            self.offer_sync_observed(data, source_domain, deadline, limit, forwarded).await,
            SyncMatchOutcome::Matched
        )
    }
    async fn offer_sync_observed(
        &self,
        data: &TaskData,
        source_domain: Uuid,
        deadline: tokio::time::Instant,
        limit: bool,
        forwarded: bool,
    ) -> SyncMatchOutcome {
        use crate::matching_limiter::WaitOutcome;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() || self.queue.is_stopped() {
            return SyncMatchOutcome::Unmatched;
        }
        if limit {
            let limiter_deadline = self
                .limiter
                .now()
                .saturating_add(remaining.as_nanos().min(i64::MAX as u128) as i64);
            let stop = self.sync_matcher.stop_signal();
            let acquired = tokio::time::timeout_at(
                deadline,
                self.limiter.wait_until(stop, Some(limiter_deadline)),
            )
            .await;
            match acquired {
                Ok(WaitOutcome::Acquired) => (),
                Ok(WaitOutcome::Throttled) => return SyncMatchOutcome::Throttled,
                Ok(WaitOutcome::Stopped) | Err(_) => return SyncMatchOutcome::Unmatched,
            }
        }
        let group = (self.isolation_select)(data).0;
        if self.sync_matcher
            .offer(
                data.clone(),
                &group,
                &source_domain.to_string(),
                forwarded,
                (self.sync_wait)(),
                deadline,
            )
            .await
        {
            SyncMatchOutcome::Matched
        } else {
            SyncMatchOutcome::Unmatched
        }
    }
    pub async fn enqueue(
        &self,
        data: TaskData,
        source_domain: Uuid,
        schedule_to_start_seconds: i32,
    ) -> Result<AllocatedTask, Status> {
        self.liveness.mark_alive(registry_now());
        self.writer
            .append(crate::matching_writer::WriteTask {
                data,
                source_domain,
                schedule_to_start_seconds,
            })
            .await
    }
    /// Notification remains in the persistence worker if the AddTask RPC is
    /// cancelled while SQLite is writing. History may safely retry the request.
    pub async fn append(
        &self,
        tasks: Vec<TaskData>,
        now: i64,
    ) -> Result<Vec<AllocatedTask>, Status> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            handle.liveness.mark_alive(registry_now());
            let tasks = handle
                .queue
                .append(&tasks, now)
                .map_err(|error| Status::unavailable(error.to_string()))?;
            handle.dispatcher.notify_new_tasks();
            Ok(tasks)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    pub async fn append_in_domain(
        &self,
        tasks: Vec<TaskData>,
        source_domain: Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTask>, Status> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            handle.liveness.mark_alive(registry_now());
            let tasks = handle
                .queue
                .append_in_domain(&tasks, source_domain, now)
                .map_err(|error| Status::unavailable(error.to_string()))?;
            handle.dispatcher.notify_new_tasks();
            Ok(tasks)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    /// Go `taskListManagerImpl.LoadBalancerHints`: a task list reports hints
    /// only once it is 10s old, as the delivered-but-unacked backlog plus the
    /// arrival QPS.
    #[must_use]
    pub fn load_balancer_hints(&self, now: i64) -> Option<m::LoadBalancerHints> {
        if now.saturating_sub(self.created) < 10_000_000_000 {
            return None;
        }
        let (_, _, backlog, ..) = self.queue.status();
        Some(m::LoadBalancerHints {
            backlog_count: backlog,
            rate_per_second: self.qps.qps(),
        })
    }
    /// Go `taskListManager.HasPollerAfter`: a poller is currently waiting, or
    /// finished its last poll after `after`. Used by the sticky availability
    /// check (`_stickyPollerUnavailableWindow`).
    pub fn has_poller_after(&self, after: i64) -> bool {
        self.pollers
            .lock()
            .expect("poller history lock")
            .has_after(after)
    }
}
// SQL cleanup runs on the blocking pool; joining async loops must release that
// worker first, so simultaneous owner unloads cannot starve their own readers.
async fn owner_work<T: Send + 'static>(
    operation: impl FnOnce(&mut Vec<crate::matching_reader::TaskReader>) -> Result<T, Status>
        + Send
        + 'static,
) -> Result<T, Status> {
    let (result, readers) = tokio::task::spawn_blocking(move || {
        let mut readers = Vec::new();
        let result = operation(&mut readers);
        (result, readers)
    })
    .await
    .map_err(|error| Status::internal(error.to_string()))?;
    for reader in readers {
        reader.wait_stopped().await;
    }
    result
}
impl QueueRegistry {
    /// Report whether this exact queue manager is currently reusable without
    /// loading or mutating registry state. This is advisory: callers must
    /// still perform their normal ownership check and call `get`, which
    /// rechecks the owner under the startup gate.
    pub fn contains_cached(&self, key: &TaskListKey) -> bool {
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let state = self.state.lock().expect("Matching registry lock");
        state
            .owners
            .get(&lookup)
            .is_some_and(|owner| !owner.queue.is_stopped())
    }

    /// Go's domain failover callback releases blocked normal decision/activity
    /// pollers and sticky decision pollers. Owners and their manager state stay
    /// installed so later polls can immediately reuse the same queues.
    pub fn release_blocked_pollers_for_domain(&self, domain: Uuid) -> usize {
        let state = self.state.lock().expect("Matching registry lock");
        let mut released = 0;
        for ((id, _, task_type), owner) in &state.owners {
            if *id != domain {
                continue;
            }
            let kind = owner.queue.task_list_kind();
            let eligible =
                (kind == 0 && matches!(*task_type, 0 | 1)) || (kind == 1 && *task_type == 0);
            if eligible {
                owner.dispatcher.release_blocked_pollers();
                released += 1;
            }
        }
        released
    }

    /// Mirror Go `getTaskListsByDomainAndKind` followed by
    /// `disconnectTaskListPollersAfterDomainFailover`: derive unique root
    /// identifiers from managers already loaded for this domain, then load
    /// the root manager and release its blocked polls. Go's method is backed
    /// by `ManagersByDomainID`; it does not enumerate persisted task-list rows.
    pub async fn release_domain_failover_polls(&self, domain: Uuid) {
        let roots = {
            let state = self.state.lock().expect("Matching registry lock");
            let mut roots = std::collections::BTreeSet::new();
            for ((id, name, task_type), owner) in &state.owners {
                if *id != domain {
                    continue;
                }
                let kind = owner.queue.task_list_kind();
                let eligible =
                    (kind == 0 && matches!(*task_type, 0 | 1)) || (kind == 1 && *task_type == 0);
                if !eligible {
                    continue;
                }
                if let Ok(parsed) = cadence_common::task_list::TaskListName::parse(name) {
                    roots.insert((parsed.root().to_owned(), *task_type, kind));
                }
            }
            roots
        };
        for (name, task_type, kind) in roots {
            let key = TaskListKey {
                domain_id: domain,
                name,
                task_type,
            };
            match self.get(key, kind, registry_now()).await {
                Ok(owner) => owner.dispatcher.release_blocked_pollers(),
                Err(error) => eprintln!("warning: loading Matching manager after domain failover failed: {error}"),
            }
        }
    }

    /// Sum current QPS for loaded owners with this exact task-list name.
    pub(crate) fn qps_for_task_list_name(&self, name: &str) -> f64 {
        let owners = {
            let state = self.state.lock().expect("Matching registry lock");
            state.owners.iter()
                .filter(|((_, task_list, _), owner)| task_list == name && !owner.queue.is_stopped())
                .map(|(_, owner)| owner.qps.clone()).collect::<Vec<_>>()
        };
        owners.iter().map(|qps| qps.qps()).sum()
    }

    #[cfg(test)]
    pub(crate) fn loaded_owner_count(&self) -> usize {
        self.state.lock().expect("Matching registry lock").owners.len()
    }

    /// Stop all loaded owners for one exact task-list name so a later Get rebuilds them.
    pub(crate) async fn stop_task_list_name(&self, name: &str) -> Result<(), Status> {
        let registry = self.clone();
        let name = name.to_owned();
        owner_work(move |readers| {
            let owners = {
                let mut state = registry.state.lock().expect("Matching registry lock");
                let keys = state.owners.keys().filter(|(_, task_list, _)| task_list == &name).cloned().collect::<Vec<_>>();
                keys.into_iter().filter_map(|key| state.owners.remove(&key)).collect::<Vec<_>>()
            };
            let now = registry_now();
            let mut first_error = None;
            for owner in owners {
                if let Err(error) = owner.finish(now, readers) { first_error.get_or_insert(error); }
            }
            first_error.map_or(Ok(()), Err)
        }).await
    }

    /// Snapshot loaded managers only, like Go ManagersByDomainID. This must
    /// neither acquire new leases nor consult frontend workflow state.
    pub fn describe_by_domain(
        &self,
        domain: Uuid,
        all_kinds: bool,
        now: i64,
    ) -> m::GetTaskListsByDomainResponse {
        let state = self.state.lock().expect("Matching registry lock");
        let mut response = m::GetTaskListsByDomainResponse::default();
        for ((id, name, task_type), owner) in &state.owners {
            if *id != domain || (!all_kinds && owner.queue.task_list_kind() != 0) {
                continue;
            }
            let Ok(parsed) = cadence_common::task_list::TaskListName::parse(name) else {
                continue;
            };
            let pollers = owner
                .pollers
                .lock()
                .expect("poller history lock")
                .list(now)
                .into_iter()
                .map(|poller| cadence_proto::api::PollerInfo {
                identity: poller.identity,
                rate_per_second: poller.rate_per_second,
                last_access_time: Some(prost_types::Timestamp {
                    seconds: poller.last_access_time.div_euclid(1_000_000_000),
                    nanos: poller.last_access_time.rem_euclid(1_000_000_000) as i32,
                }),
                })
                .collect();
            let description = m::DescribeTaskListResponse {
                pollers,
                partition_config: owner
                    .partitions
                    .get()
                    .map(crate::grpc_matching::partition_config_to_proto),
                task_list: Some(cadence_proto::api::TaskList {
                    name: name.clone(),
                    kind: i32::from(owner.queue.task_list_kind()) + 1,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let target = if *task_type == 0 {
                &mut response.decision_task_list_map
            } else {
                &mut response.activity_task_list_map
            };
            target.insert(parsed.root().to_owned(), description);
        }
        response
    }
    pub fn new(
        store: Arc<dyn MatchingTaskStore>,
        history: MatchingHistoryClient,
        range_size: NonZeroU32,
    ) -> Self {
        Self {
            store,
            history,
            range_size,
            state: Arc::new(Mutex::new(State::default())),
            changed: Arc::new(tokio::sync::Notify::new()),
            startups: Arc::new(Mutex::new(HashMap::new())),
            ownership_policy: Arc::new(RwLock::new(None)),
            shard_router: Arc::new(RwLock::new(None)),
            domain_metadata: None,
            standby_enabled: None,
            membership: None,
            local_poll_service: None,
            partition_count: None,
            cache_enabled: None,
            sync_wait: None,
            local_poll_wait: None,
            forward_degree: None,
            forward_polls: None,
            forward_tasks: None,
            forward_rate: None,
            scaler_config: None,
            qps_interval: None,
            isolation: None,
            reader_live: None,
            min_burst: None,
            update_ack_interval: None,
            idle_tasklist_check_interval: None,
            max_task_delete_batch_size: None,
            writer_live: None,
            metrics: crate::matching_metrics::MatchingMetrics::default(),
        }
    }
    pub(crate) fn with_metrics(
        mut self,
        metrics: crate::matching_metrics::MatchingMetrics,
    ) -> Self {
        self.metrics = metrics;
        self
    }
    pub(crate) fn with_local_poll_service(mut self, factory: LocalPollFactory) -> Self {
        self.local_poll_service = Some(factory);
        self
    }
    pub fn with_membership(mut self, membership: crate::matching_membership::Membership) -> Self {
        self.membership = Some(membership);
        self
    }
    /// Bind the shard-distributor ownership policy after executor construction.
    /// Clones share this slot, so runtime wiring also affects internal readers.
    pub fn set_ownership_policy(&self, policy: QueueOwnershipPolicy) {
        *self
            .ownership_policy
            .write()
            .expect("Matching ownership policy lock") = Some(policy);
    }

    pub fn set_shard_router(
        &self,
        router: Arc<crate::matching_shard_routing::MatchingShardRouter>,
    ) {
        *self
            .shard_router
            .write()
            .expect("Matching shard router lock") = Some(router);
    }

    fn check_task_list_ownership(&self, name: &str, cached: bool) -> Result<(), Status> {
        let policy = self
            .ownership_policy
            .read()
            .expect("Matching ownership policy lock")
            .clone();
        let Some(policy) = policy else {
            return self.membership.as_ref().map_or(Ok(()), |membership| {
                membership.check_task_list(name)
            });
        };
        if (policy.is_shard_distributor_eligible)(name) {
            if (policy.has_local_processor)(name) {
                return Ok(());
            }
            return Err(self.membership.as_ref().map_or_else(
                || Status::internal("Matching membership is not configured."),
                |membership| membership.task_list_not_owned_by(name, "not known"),
            ));
        }
        if cached {
            return Ok(());
        }
        self.membership.as_ref().map_or(Ok(()), |membership| {
            membership.check_task_list(name)
        })
    }

    /// Like Go's membership watcher, detach obsolete owners before doing I/O.
    /// Unknown placement leaves existing readers intact until membership recovers.
    pub async fn unload_non_owned(&self, now: i64) -> Result<(), Status> {
        let Some(membership) = self.membership.clone() else {
            return Ok(());
        };
        let registry = self.clone();
        owner_work(move |readers| {
            let candidates = {
                let state = registry.state.lock().expect("Matching registry lock");
                let policy = registry
                    .ownership_policy
                    .read()
                    .expect("Matching ownership policy lock")
                    .clone();
                let mut candidates = Vec::new();
                for (key, owner) in &state.owners {
                    let owned = policy
                        .as_ref()
                        .filter(|policy| (policy.is_shard_distributor_eligible)(&key.1))
                        .map_or_else(
                            || membership.owns(&key.1),
                            |policy| Ok((policy.has_local_processor)(&key.1)),
                        )?;
                    if !owned {
                        candidates.push((key.clone(), owner.clone()));
                    }
                }
                candidates
            };
            let mut failure = None;
            for (key, owner) in candidates {
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
    pub fn with_standby_enabled(mut self, enabled: crate::matching_standby::Enabled) -> Self {
        self.standby_enabled = Some(enabled);
        self
    }
    /// Install Go's `NumTasklistReadPartitions` provider. The provider reads
    /// `matching.numTasklistReadPartitions`; the registry clamps to >= 1.
    pub fn with_partition_count(mut self, provider: PartitionCountProvider) -> Self {
        self.partition_count = Some(provider);
        self
    }
    pub fn with_sync_wait(mut self, provider: SyncWaitProvider) -> Self {
        self.sync_wait = Some(provider);
        self
    }
    pub fn with_local_poll_wait(mut self, provider: SyncWaitProvider) -> Self {
        self.local_poll_wait = Some(provider);
        self
    }
    pub fn with_forward_degree(mut self, provider: ForwardDegreeProvider) -> Self {
        self.forward_degree = Some(provider);
        self
    }
    pub fn with_forward_polls(mut self, provider: ForwardPollsProvider) -> Self {
        self.forward_polls = Some(provider);
        self
    }
    pub fn with_forward_tasks(
        mut self,
        tasks: ForwardPollsProvider,
        rate: ForwardPollsProvider,
    ) -> Self {
        self.forward_tasks = Some(tasks);
        self.forward_rate = Some(rate);
        self
    }
    /// Install the adaptive-scaler configuration provider. The scaler loop
    /// starts on root normal owners and rereads the provider every tick.
    pub fn with_scaler_config(mut self, provider: ScalerConfigProvider) -> Self {
        self.scaler_config = Some(provider);
        self
    }
    pub fn with_partition_cache_enabled(mut self, provider: PartitionCacheEnabledProvider) -> Self {
        self.cache_enabled = Some(provider);
        self
    }
    /// Install Go's `QPSTrackerInterval` provider. The provider reads
    /// `matching.qpsTrackerInterval` (default 10s) in nanoseconds.
    pub fn with_qps_interval(mut self, provider: QpsIntervalProvider) -> Self {
        self.qps_interval = Some(provider);
        self
    }
    pub fn with_isolation(mut self, provider: IsolationProvider) -> Self {
        self.isolation = Some(provider);
        self
    }
    /// Install the live reader-configuration provider (Go's
    /// `GetTasksBatchSize`, `ReadRangeSize` and `AsyncTaskDispatchTimeout`).
    pub fn with_reader_live(mut self, provider: ReaderPropertiesProvider) -> Self {
        self.reader_live = Some(provider);
        self
    }
    /// Install Go's `MinTaskThrottlingBurstSize` provider, read once when a
    /// queue owner's dispatch limiter is built.
    pub fn with_min_burst(mut self, provider: SyncWaitProvider) -> Self {
        self.min_burst = Some(provider);
        self
    }
    /// Install Go's live `matching.recordTaskStartedTimeout` provider on the
    /// Matching -> History client used for task-start acknowledgements.
    pub fn with_record_started_timeout(
        mut self,
        provider: crate::matching_history::RecordStartedTimeout,
    ) -> Self {
        self.history = self.history.clone().with_record_started_timeout(provider);
        self
    }
    /// Install Go's `UpdateAckInterval` provider, read per maintenance update.
    pub fn with_update_ack_interval(mut self, provider: SyncWaitProvider) -> Self {
        self.update_ack_interval = Some(provider);
        self
    }
    /// Install Go's `IdleTasklistCheckInterval` provider, read once per owner.
    pub fn with_idle_tasklist_check_interval(mut self, provider: SyncWaitProvider) -> Self {
        self.idle_tasklist_check_interval = Some(provider);
        self
    }
    pub fn with_max_task_delete_batch_size(
        mut self,
        provider: TaskDeleteBatchSizeProvider,
    ) -> Self {
        self.max_task_delete_batch_size = Some(provider);
        self
    }
    pub fn with_writer_live(mut self, provider: WriterLiveProvider) -> Self {
        self.writer_live = Some(provider);
        self
    }
    #[cfg(test)]
    pub(crate) fn isolation_provider_for_test(&self) -> IsolationProvider {
        self.isolation
            .clone()
            .expect("isolation provider installed")
    }
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn record_started_timeout_for_test(
        &self,
        domain_id: &str,
    ) -> Option<std::time::Duration> {
        self.history.record_started_timeout(domain_id)
    }
    pub fn with_domain_metadata(
        mut self,
        domains: cadence_persistence::InMemoryDomainStore,
        cluster: String,
    ) -> Self {
        self.domain_metadata = Some((domains, cluster));
        self
    }
    /// task_type is the persistence enum (decision 0, activity 1).
    pub fn get(
        &self,
        key: TaskListKey,
        kind: i16,
        now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueueHandle, Status>> + Send + '_>>
    {
        Box::pin(async move {
        if !(0..=1).contains(&key.task_type) {
            return Err(Status::invalid_argument("Unknown persistence task type."));
        }
        let registry = self.clone();
        owner_work(move |readers| {
            let lookup = (key.domain_id, key.name.clone(), key.task_type);
            // Go registers a manager before Start performs lease I/O. Serialize
            // startup by queue identity while keeping unrelated owners usable.
            // Weak entries cannot retain every task-list name ever encountered.
            let startup = {
                let mut gates = registry
                    .startups
                    .lock()
                    .expect("Matching startup gates lock");
                gates.retain(|_, gate| gate.strong_count() > 0);
                match gates.get(&lookup).and_then(Weak::upgrade) {
                    Some(gate) => gate,
                    None => {
                        let gate = Arc::new(Mutex::new(()));
                        gates.insert(lookup.clone(), Arc::downgrade(&gate));
                        gate
                    }
                }
            };
            let _starting = startup.lock().expect("Matching queue startup lock");
            let mut state = registry.state.lock().expect("Matching registry lock");
            if state.stopped {
                return Err(Status::unavailable("Matching registry stopped."));
            }
            let cached = state
                .owners
                .get(&lookup)
                .is_some_and(|owner| !owner.queue.is_stopped());
            registry.check_task_list_ownership(&key.name, cached)?;
            if state
                .owners
                .get(&lookup)
                .is_some_and(|owner| owner.queue.is_stopped())
            {
                if let Some(owner) = state.owners.remove(&lookup) {
                    registry.metrics.task_list_managers_gauge(state.owners.len());
                    drop(state);
                    if let Err(error) = owner.finish(now, readers) {
                        eprintln!("Matching stopped owner cleanup failed: {error}");
                    }
                    state = registry.state.lock().expect("Matching registry lock");
                    if state.stopped {
                        return Err(Status::unavailable("Matching registry stopped."));
                    }
                }
            }
            let owner = match state.owners.get(&lookup) {
                Some(owner) => owner.clone(),
                None => {
                    drop(state);
                    let domain = key.domain_id.to_string();
                    let metric_context = crate::matching_metrics::PollMetricContext {
                        domain: registry
                            .domain_metadata
                            .as_ref()
                            .and_then(|(domains, _)| {
                                domains
                                    .describe(&cadence_api::service_types::DescribeDomainRequest {
                                        name: None,
                                        uuid: Some(domain.clone()),
                                    })
                                    .ok()
                                    .and_then(|response| response.domain_info.map(|info| info.name))
                            })
                            .filter(|name| !name.is_empty())
                            .unwrap_or_else(|| "_unknown_".to_owned()),
                        task_list: lookup.1.clone(),
                        task_list_kind: i32::from(kind),
                        task_type: if lookup.2 == 1 { "activity_task" } else { "decision_task" },
                        poller_isolation_group: String::new(),
                    };
                    registry.metrics.lease_request(&metric_context);
                    let opened = DurableTaskQueue::open(
                            registry.store.clone(),
                            key.clone(),
                            kind,
                            registry.range_size,
                            now,
                        );
                    let queue = match opened {
                        Ok(queue) => Arc::new(queue),
                        Err(error) => {
                            registry.metrics.lease_failure(&metric_context);
                            return Err(Status::unavailable(error.to_string()));
                        }
                    };
                    let lease_metrics = registry.metrics.clone();
                    let lease_context = metric_context.clone();
                    queue.set_lease_observer(Arc::new(move |success| {
                        lease_metrics.lease_attempt(&lease_context, !success);
                    }));
                    let condition_metrics = registry.metrics.clone();
                    let condition_context = metric_context.clone();
                    queue.set_condition_failed_observer(Arc::new(move || {
                        condition_metrics.condition_failed(&condition_context);
                    }));
                    {
                        let provider = registry.max_task_delete_batch_size.clone();
                        let domain = domain.clone();
                        let name = lookup.1.clone();
                        let task_type = lookup.2;
                        queue.set_gc_batch_size_provider(Arc::new(move || {
                            provider.as_ref().map(|p| p(&domain, &name, task_type)).unwrap_or(100)
                        }));
                    }
                    let partitions = match crate::matching_partition::PartitionCache::load(
                        registry.store.as_ref(),
                            &queue,
                            &key,
                            kind,
                        ) {
                        Ok(cache) => Arc::new(cache),
                            Err(error) => {
                                queue.stop();
                                return Err(error);
                            }
                    };
                    let count_partitions: crate::matching_limiter::PartitionCount = {
                        let provider = registry.partition_count.clone();
                        let enabled = registry.cache_enabled.clone();
                        let cache = partitions.clone();
                        let domain_id = key.domain_id.to_string();
                        let task_list = key.name.clone();
                        let task_type = key.task_type;
                        Arc::new(move || {
                                let root =
                                    cadence_common::task_list::TaskListName::parse(&task_list)
                                        .expect("validated queue name");
                                if enabled.as_ref().is_some_and(|enabled| {
                                    enabled(&domain_id, root.root(), task_type)
                                }) {
                                    return cache
                                        .get()
                                        .map(|config| config.read_partitions.len() as i64)
                                        .unwrap_or(1);
                            }
                            provider
                                .as_ref()
                                .map(|provider| provider(&domain_id, &task_list, task_type))
                                .unwrap_or(1)
                                .max(1)
                        })
                    };
                        let idle_ttl_nanos = registry
                            .idle_tasklist_check_interval
                            .as_ref()
                            .map(|provider| provider(&domain, &lookup.1, lookup.2))
                            .unwrap_or(300_000_000_000)
                            .max(1);
                    let liveness = Arc::new(cadence_common::liveness::Liveness::new(
                        registry_now(),
                            idle_ttl_nanos,
                    ));
                        let next_ack_update =
                            registry.update_ack_interval.as_ref().map(|provider| {
                                let nanos = provider(&domain, &lookup.1, lookup.2).max(1) as u64;
                                tokio::time::Instant::now() + std::time::Duration::from_nanos(nanos)
                            });
                    // Go `stats.NewEmaFixedWindowQPSTracker(timeSource, 0.5,
                    // cfg.QPSTrackerInterval(domain, taskList, type), ...)`.
                    let qps = Arc::new(cadence_common::qps::EmaQpsTracker::new(
                        QPS_TRACKER_EXP,
                        std::time::Duration::from_nanos(
                            registry
                                .qps_interval
                                .as_ref()
                                .map(|provider| {
                                        provider(&lookup.0.to_string(), &lookup.1, lookup.2)
                                })
                                .unwrap_or(DEFAULT_QPS_TRACKER_INTERVAL_NANOS)
                                .max(1) as u64,
                        ),
                        cadence_common::qps::system_clock(),
                    ));
                    let activity_domain = domain.clone();
                    let metadata = registry.domain_metadata.clone();
                    let activity = Arc::new(move || {
                        metadata.as_ref().is_none_or(|(domains, cluster)| {
                            domains
                                .is_active_in(&activity_domain, cluster)
                                .unwrap_or(true)
                        })
                    });
                        let dispatcher = TaskDispatcher::new(
                            queue.clone(),
                            registry.history.clone(),
                            domain.clone(),
                        )
                            .with_liveness(liveness.clone());
                    let writer_live = registry.writer_live.as_ref().map(|provider| {
                        scoped_writer_live(provider.clone(), lookup.0.to_string(), lookup.1.clone(), lookup.2)
                    });
                    let writer = crate::matching_writer::TaskWriter::new_with_live_config(
                        queue.clone(),
                        dispatcher.task_notification(),
                        crate::matching_writer::WriterConfig::default(),
                        writer_live,
                    );
                    let min_burst = registry
                        .min_burst
                        .as_ref()
                        .map(|provider| provider(&domain, &lookup.1, lookup.2))
                        .unwrap_or(crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE);
                    let limiter = Arc::new(
                            crate::matching_limiter::TaskListLimiter::with_partitions_and_burst(
                                count_partitions,
                                min_burst,
                            ),
                    );
                        let pollers =
                            Arc::new(Mutex::new(cadence_common::poller::Manager::default()));
                        let isolation = registry
                            .isolation
                            .as_ref()
                            .map(|provider| {
                                provider(
                                    &lookup.0.to_string(),
                                    &lookup.1,
                                    lookup.2,
                                    kind,
                                    pollers.clone(),
                                    partitions.clone(),
                                )
                            })
                            .unwrap_or_default();
                    let sync_groups = isolation.groups.clone();
                    let isolation_select = isolation.select.clone();
                    let sync_wait: Arc<dyn Fn() -> std::time::Duration + Send + Sync> = {
                        let provider = registry.sync_wait.clone();
                        let domain = lookup.0.to_string();
                        let name = lookup.1.clone();
                        let task_type = lookup.2;
                            Arc::new(move || {
                                std::time::Duration::from_nanos(
                                    provider
                                        .as_ref()
                                        .map(|provider| provider(&domain, &name, task_type))
                                        .unwrap_or(10_000_000)
                                        .max(0) as u64,
                                )
                            })
                    };
                    let reader_config = crate::matching_reader::ReaderConfig {
                        live: registry
                            .reader_live
                            .as_ref()
                            .map(|provider| Arc::new(provider(&domain, &lookup.1, lookup.2))),
                        ..crate::matching_reader::ReaderConfig::default()
                    };
                    let reader = crate::matching_reader::TaskReader::with_dispatch_check_metrics(
                        queue.clone(),
                        dispatcher.task_notification(),
                        reader_config,
                        activity,
                        limiter.clone(),
                        isolation,
                            registry.domain_metadata.as_ref().map(|(domains, cluster)| {
                                crate::matching_standby::checker(
                                    domains.clone(),
                                    cluster.clone(),
                                    registry.history.clone(),
                                    key.clone(),
                                    registry.standby_enabled.clone(),
                                    registry.metrics.clone(),
                                    crate::matching_metrics::PollMetricContext {
                                        domain: domain.clone(),
                                        task_list: lookup.1.clone(),
                                        task_list_kind: i32::from(kind),
                                        task_type: if lookup.2 == 1 { "activity_task" } else { "decision_task" },
                                        poller_isolation_group: String::new(),
                                    },
                                )
                            }),
                        Some((
                            registry.metrics.clone(),
                            crate::matching_metrics::PollMetricContext {
                                domain: domain.clone(),
                                task_list: lookup.1.clone(),
                                task_list_kind: i32::from(kind),
                                task_type: if lookup.2 == 1 {
                                    "activity_task"
                                } else {
                                    "decision_task"
                                },
                                poller_isolation_group: String::new(),
                            },
                        )),
                    );
                    let read_stop = reader.stop_signal();
                        let sync_matcher = Arc::new(crate::matching_sync::SyncMatcher::new(
                            &sync_groups,
                            read_stop.subscribe(),
                        ));
                    let forward = {
                        // Go `forwarder` refuses sticky task lists; only a
                        // real child partition forwards polls to its parent.
                        // Sticky names never carry a partition suffix, so the
                        // root check subsumes the kind refusal below.
                            let name = cadence_common::task_list::TaskListName::parse(&lookup.1)
                                .map_err(Status::invalid_argument)?;
                        if kind == 1 || name.is_root() {
                            None
                        } else {
                            let max_outstanding: crate::matching_forward::Count = {
                                let provider = registry.forward_polls.clone();
                                let domain = domain.clone();
                                let name = lookup.1.clone();
                                let task_type = lookup.2;
                                    Arc::new(move || {
                                        provider
                                            .as_ref()
                                            .map(|provider| provider(&domain, &name, task_type))
                                            .unwrap_or(1)
                                    })
                            };
                            let parent_key = TaskListKey {
                                domain_id: key.domain_id,
                                name: lookup.1.clone(),
                                task_type: key.task_type,
                            };
                            let resolve_domain = domain.clone();
                            // Owners live in registry.state; retaining that
                            // same Arc here would form an owner/registry cycle.
                            let resolve_state = Arc::downgrade(&registry.state);
                            let mut resolve_registry = registry.clone();
                            resolve_registry.state = Arc::new(Mutex::new(State::default()));
                            Some(crate::matching_dispatch::ForwardState::new(
                                move || {
                                    let mut resolve_registry = resolve_registry.clone();
                                    let state = resolve_state.upgrade();
                                    let parent_key = parent_key.clone();
                                    let domain = resolve_domain.clone();
                                    Box::pin(async move {
                                        resolve_registry.state = state?;
                                            let degree = resolve_registry
                                                .forward_degree
                                                .as_ref()
                                                .map(|p| {
                                                    p(
                                                        &domain,
                                                        &parent_key.name,
                                                        parent_key.task_type,
                                                    )
                                                })
                                                .unwrap_or(20);
                                            let parent =
                                                cadence_common::task_list::TaskListName::parse(
                                                    &parent_key.name,
                                                )
                                                .ok()?
                                                .parent(degree);
                                            if parent.is_empty() {
                                                return None;
                                            }
                                            let parent_key = TaskListKey {
                                                name: parent.clone(),
                                                ..parent_key
                                            };
                                            resolve_parent_dispatcher(
                                                resolve_registry,
                                                parent_key,
                                                kind,
                                            )
                                            .await
                                            .map(|dispatcher| (dispatcher, parent))
                                    })
                                        as std::pin::Pin<
                                            Box<
                                                dyn std::future::Future<
                                                            Output = Option<(
                                                                crate::matching_dispatch::Parent,
                                                                String,
                                                            )>,
                                                    > + Send,
                                            >,
                                        >
                                },
                                max_outstanding,
                                lookup.1.clone(),
                                {
                                    let provider = registry.local_poll_wait.clone();
                                    let domain = domain.clone();
                                    let name = lookup.1.clone();
                                    let task_type = lookup.2;
                                        Arc::new(move || {
                                            provider
                                                .as_ref()
                                                .map(|provider| provider(&domain, &name, task_type))
                                                .unwrap_or(10_000_000)
                                        })
                                },
                            ))
                        }
                    };
                    let wake = registry.changed.clone();
                    let stopped = writer.stop_signal();
                    queue.set_stop_waker(Arc::new(move || {
                        let _ = stopped.send(true);
                        read_stop.send_replace(true);
                        wake.notify_waiters();
                    }));
                    let mut dispatcher = dispatcher
                        .with_writer(writer.clone())
                        .with_reader(reader.clone())
                        .with_sync_matcher(sync_matcher.clone())
                        .with_pollers(pollers.clone());
                    if let Some(forward) = forward {
                        dispatcher = dispatcher.with_forward(forward);
                    }
                    let owner = Arc::new(Owner {
                        forward_tasks: Arc::new(crate::matching_forward::RequestTokens::new({
                            let provider = registry.forward_tasks.clone();
                            let domain = domain.clone();
                            let name = lookup.1.clone();
                            let task_type = lookup.2;
                                Arc::new(move || {
                                    provider
                                        .as_ref()
                                        .map(|p| p(&domain, &name, task_type))
                                        .unwrap_or(1)
                                })
                        })),
                            forward_rate: Arc::new(crate::matching_forward::ForwardRate::new(
                                {
                            let provider = registry.forward_rate.clone();
                            let domain = domain.clone();
                            let name = lookup.1.clone();
                            let task_type = lookup.2;
                                    Arc::new(move || {
                                        provider
                                            .as_ref()
                                            .map(|p| p(&domain, &name, task_type))
                                            .unwrap_or(10)
                                    })
                                },
                                now,
                            )),
                        dispatcher,
                        writer,
                        reader,
                        limiter,
                        queue,
                        pollers,
                        queries: Arc::new(crate::matching_query::QueryChannel::new()),
                        qps,
                        partitions,
                        sync_matcher,
                        isolation_select,
                        sync_wait,
                        created: now,
                            idle_ttl_nanos,
                        last_update: Mutex::new(tokio::time::Instant::now()),
                            next_ack_update: Mutex::new(next_ack_update),
                        next_idle_check: Mutex::new(
                                tokio::time::Instant::now()
                                    + std::time::Duration::from_nanos(
                                        (idle_ttl_nanos / 2).max(1) as u64
                                    ),
                        ),
                        liveness,
                        scaler: Mutex::new(None),
                    });
                    state = registry.state.lock().expect("Matching registry lock");
                    if state.stopped {
                        drop(state);
                        if let Err(error) = owner.finish(now, readers) {
                            eprintln!("Matching startup shutdown cleanup failed: {error}");
                        }
                        return Err(Status::unavailable("Matching registry stopped."));
                    }
                    if let Err(error) = registry.check_task_list_ownership(&key.name, false) {
                        drop(state);
                        let _ = owner.finish(now, readers);
                        return Err(error);
                    }
                    // Reconcile publication with concurrent root updates. A
                    // nonroot startup may have read SQL before the root wrote
                    // and published its notification.
                    if kind == 0 {
                            let name = cadence_common::task_list::TaskListName::parse(&lookup.1)
                                .map_err(Status::invalid_argument)?;
                        if name.is_root() {
                            if let Some(config) = owner.partitions.get() {
                                for ((domain, task_list, task_type), other) in &state.owners {
                                        if *domain == lookup.0
                                            && *task_type == lookup.2
                                            && cadence_common::task_list::TaskListName::parse(
                                                task_list,
                                            )
                                            .is_ok_and(|other| other.root() == name.root())
                                        {
                                        other.partitions.push(&config);
                                    }
                                }
                            }
                            } else if let Some(root) =
                                state.owners.get(&(lookup.0, name.root().into(), lookup.2))
                            {
                                if let Some(config) = root.partitions.get() {
                                    owner.partitions.push(&config);
                                }
                        }
                    }
                    // Go `taskListManagerImpl.Start` pushes the loaded root config
                    // to every non-root read partition in the background, bounded
                    // by `notifyPartitionConfigTimeout` (5s).
                    if kind == 0 {
                        if let (Some(config), Ok(parsed)) = (
                            owner.partitions.get(),
                            cadence_common::task_list::TaskListName::parse(&key.name),
                        ) {
                            if parsed.is_root() {
                                let notifier = registry.clone();
                                let root_name = parsed.root().to_owned();
                                let read_partitions = config.read_partitions.keys().copied().collect();
                                let deadline = tokio::time::Instant::now()
                                    + std::time::Duration::from_secs(5);
                                tokio::spawn(async move {
                                    notifier
                                        .notify_read_partitions(
                                            key.domain_id,
                                            &root_name,
                                            kind,
                                            key.task_type,
                                            read_partitions,
                                            config,
                                            Some(deadline),
                                        )
                                        .await;
                                });
                            }
                        }
                    }
                    // Go starts the adaptive scaler on root normal managers;
                    // the loop self-terminates when the owner stops.
                    if kind == 0 && registry.scaler_config.is_some() {
                        if let Ok(parsed) =
                            cadence_common::task_list::TaskListName::parse(&key.name)
                        {
                            if parsed.is_root() {
                                // Like the forward resolver, retain only a
                                // weak handle on the owner state: the scaler
                                // must not keep the registry (and its task
                                // store) alive after every other handle is
                                // dropped. The loop exits once the upgrade
                                // fails.
                                let scaler_state = Arc::downgrade(&registry.state);
                                let mut scaler_registry = registry.clone();
                                scaler_registry.state = Arc::new(Mutex::new(State::default()));
                                let root = TaskListKey {
                                    domain_id: key.domain_id,
                                    name: key.name.clone(),
                                    task_type: key.task_type,
                                };
                                let task = tokio::spawn(async move {
                                    let mut scaler_registry = scaler_registry;
                                    scaler_registry.scaler_loop(root, scaler_state).await;
                                });
                                    *owner.scaler.lock().expect("Matching scaler lock") =
                                        Some(task.abort_handle());
                            }
                        }
                    }
                    state.owners.insert(lookup, owner.clone());
                    registry
                        .metrics
                        .task_list_managers_gauge(state.owners.len());
                    registry.changed.notify_waiters();
                    owner
                }
            };
            Ok(QueueHandle {
                queue: owner.queue.clone(),
                dispatcher: owner.dispatcher.clone(),
                pollers: owner.pollers.clone(),
                limiter: owner.limiter.clone(),
                queries: owner.queries.clone(),
                qps: owner.qps.clone(),
                partitions: owner.partitions.clone(),
                sync_matcher: owner.sync_matcher.clone(),
                isolation_select: owner.isolation_select.clone(),
                sync_wait: owner.sync_wait.clone(),
                created: owner.created,
                liveness: owner.liveness.clone(),
                writer: owner.writer.clone(),
                forward_tasks: owner.forward_tasks.clone(),
                forward_rate: owner.forward_rate.clone(),
            })
        })
        .await
        })
    }
    /// Go `adaptiveScalerImpl` periodic loop, running only on root normal
    /// owners. Live config may enable the scaler at any tick; the loop ends
    /// when the owner stops, the registry shuts down, or every other registry
    /// handle is gone (the weak state can no longer be upgraded).
    async fn scaler_loop(&mut self, root: TaskListKey, state: std::sync::Weak<Mutex<State>>) {
        let Some(provider) = self.scaler_config.clone() else {
            return;
        };
        let metric_domain = self
            .domain_metadata
            .as_ref()
            .and_then(|(domains, _)| {
                domains
                    .describe(&cadence_api::service_types::DescribeDomainRequest {
                        name: None,
                        uuid: Some(root.domain_id.to_string()),
                    })
                    .ok()
                    .and_then(|response| response.domain_info.map(|info| info.name))
            })
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "_unknown_".to_owned());
        let mut scaler = crate::matching_scaler::Scaler::new().with_metrics(
            self.metrics.clone(),
            crate::matching_metrics::PollMetricContext {
                domain: metric_domain,
                task_list: root.name.clone(),
                task_list_kind: 0,
                task_type: if root.task_type == 1 { "activity_task" } else { "decision_task" },
                poller_isolation_group: String::new(),
            },
        );
        loop {
            let name = match cadence_common::task_list::TaskListName::parse(&root.name) {
                Ok(name) => name,
                Err(_) => return,
            };
            let config = provider(&root.domain_id.to_string(), name.root(), root.task_type);
            tokio::time::sleep(std::time::Duration::from_nanos(
                config.update_interval_nanos.max(0) as u64,
            ))
            .await;
            let Some(live) = state.upgrade() else {
                return;
            };
            self.state = live;
            match self.scaler_update(&root, &config, &mut scaler).await {
                Ok(true) => (),
                Ok(false) => return,
                Err(error) => eprintln!("Matching adaptive scaler failed: {error}"),
            }
        }
    }
    /// Go `adaptiveScalerImpl.describePartition`: the root is described from
    /// this host, every other partition through a DescribeTaskList RPC to its
    /// owner. Without a membership ring all partitions are local.
    async fn describe_scaled_partition(
        &self,
        root: &TaskListKey,
        name: &cadence_common::task_list::TaskListName,
        id: i32,
        config: &crate::matching_scaler::ScalerConfig,
        current_write: &HashMap<i32, Vec<String>>,
        now: i64,
    ) -> Result<ScaledPartition, Status> {
        if id != 0 && self.membership.is_some() {
            let response = self.describe_partition_rpc(root, name, id).await?;
            return Ok(scaled_partition_from_response(id, &response, config.empty_check));
        }
        let sub = TaskListKey {
            name: name.partition_name(id as i64),
            ..root.clone()
        };
        let other = self.get(sub, 0, now).await?;
        let qps = other.qps.qps();
        let (_, _, backlog_hint, _, _, empty) = other.queue.status();
        let counts = other
            .pollers
            .lock()
            .expect("poller history lock")
            .count_by_group(now, now.saturating_sub(config.poller_window_nanos));
        Ok(ScaledPartition {
            qps,
            empty: if config.empty_check {
                empty
            } else {
                backlog_hint == 0
            },
            has_write: current_write.contains_key(&id),
            groups: config
                .isolation_groups
                .iter()
                .map(|group| {
                    (
                        group.clone(),
                        other.qps.group_qps(group),
                        counts.get(group).copied().unwrap_or(0) > 0,
                    )
                })
                .collect(),
        })
    }
    async fn describe_partition_rpc(
        &self,
        root: &TaskListKey,
        name: &cadence_common::task_list::TaskListName,
        id: i32,
    ) -> Result<m::DescribeTaskListResponse, Status> {
        let Some(membership) = self.membership.as_ref() else {
            return Err(Status::unavailable("Matching membership is not configured"));
        };
        let partition = name.partition_name(id as i64);
        let (_owner, channel) = membership.channel_for_task_list_owner(&partition)?;
        let request = m::DescribeTaskListRequest {
            domain_id: root.domain_id.to_string(),
            request: Some(cadence_proto::api::DescribeTaskListRequest {
                task_list: Some(cadence_proto::api::TaskList {
                    name: partition,
                    // Go `TaskListKindNormal` on the wire.
                    kind: 1,
                    ..Default::default()
                }),
                task_list_type: root.task_type + 1,
                include_task_list_status: true,
                ..Default::default()
            }),
        };
        let mut client = m::matching_api_client::MatchingApiClient::new(channel);
        client
            .describe_task_list(tonic::Request::new(request))
            .await
            .map(|response| response.into_inner())
    }
    /// One Go `run()` tick. Returns false when the loop should stop. A
    /// metrics-collection failure resets the sustains like Go's error branch.
    async fn scaler_update(
        &self,
        root: &TaskListKey,
        config: &crate::matching_scaler::ScalerConfig,
        scaler: &mut crate::matching_scaler::Scaler,
    ) -> Result<bool, Status> {
        use crate::matching_scaler::{AggregateMetrics, PartitionMetrics};
        let now = registry_now();
        let handle = match self.get(root.clone(), 0, now).await {
            Ok(handle) => handle,
            Err(_) => return Ok(false),
        };
        if handle.queue.is_stopped() {
            return Ok(false);
        }
        if !config.enabled || !config.cache_enabled {
            scaler.reset();
            return Ok(true);
        }
        let current =
            handle
                .partitions
                .get()
                .unwrap_or(cadence_persistence::StoredPartitionConfig {
            version: 0,
            read_partitions: HashMap::from([(0, Vec::new())]),
            write_partitions: HashMap::from([(0, Vec::new())]),
        });
        let metrics = if config.isolation_assignment {
            let mut metrics = AggregateMetrics {
                isolation_enabled: true,
                ..AggregateMetrics::default()
            };
            let name = cadence_common::task_list::TaskListName::parse(&root.name)
                .map_err(Status::invalid_argument)?;
            let mut ids: Vec<i32> = current.read_partitions.keys().copied().collect();
            ids.sort();
            for id in ids {
                let partition = match self
                    .describe_scaled_partition(root, &name, id, config, &current.write_partitions, now)
                    .await
                {
                    Ok(partition) => partition,
                    Err(_) => {
                        // Go `run()` resets the sustains when metrics collection fails.
                        scaler.reset();
                        return Ok(true);
                    }
                };
                metrics.total_qps += partition.qps;
                metrics.by_partition.insert(
                    id,
                    PartitionMetrics {
                        qps: partition.qps,
                        read_only: !partition.has_write,
                        empty: partition.empty,
                    },
                );
                for (group, qps, has_pollers) in partition.groups {
                    *metrics.qps_by_group.entry(group.clone()).or_insert(0.0) += qps;
                    *metrics.has_pollers_by_group.entry(group).or_insert(false) |= has_pollers;
                }
            }
            metrics
        } else {
            AggregateMetrics {
                // Go `assumeEvenQPS`: root arrival QPS times write count.
                total_qps: handle.qps.qps() * current.write_partitions.len() as f64,
                ..AggregateMetrics::default()
            }
        };
        match scaler.tick(
            config,
            now,
            &current.read_partitions,
            &current.write_partitions,
            &metrics,
            |_| None,
        ) {
            Some((read_partitions, write_partitions)) => {
                scaler.record_update_counter(false);
                let result = self.update_partition_config(
                    root.clone(),
                    handle,
                    cadence_persistence::StoredPartitionConfig {
                        version: 0,
                        read_partitions,
                        write_partitions,
                    },
                    now,
                )
                .await;
                if result.is_err() {
                    scaler.record_update_counter(true);
                }
                result.map(|_| true)
            },
            None => Ok(true),
        }
    }
    pub async fn update_partition_config(
        &self,
        key: TaskListKey,
        handle: QueueHandle,
        config: cadence_persistence::StoredPartitionConfig,
        now: i64,
    ) -> Result<(), Status> {
        self.update_partition_config_with_deadline(key, handle, config, now, None)
            .await
    }

    pub async fn update_partition_config_with_deadline(
        &self,
        key: TaskListKey,
        handle: QueueHandle,
        config: cadence_persistence::StoredPartitionConfig,
        now: i64,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Status> {
        let registry = self.clone();
        let domain_id = key.domain_id;
        let task_type = key.task_type;
        let task_list_kind = handle.queue.task_list_kind();
        let notification = tokio::task::spawn_blocking(move || {
            let Some((old, new)) = handle.partitions.update(&handle.queue, config, now)? else {
                return Ok::<_, Status>(None);
            };
            // Pinned Go taskListManager.notifyPartitionConfig notifies the
            // union of old/new non-root READ partitions only. Write partitions
            // are not independently refreshed by this path.
            let mut notify = std::collections::BTreeSet::new();
            for config in old.iter().chain(std::iter::once(&new)) {
                notify.extend(config.read_partitions.keys().copied());
            }
            notify.remove(&0);
            let name = cadence_common::task_list::TaskListName::parse(&key.name)
                .map_err(Status::invalid_argument)?;
            let state = registry.state.lock().expect("Matching registry lock");
            for ((domain, task_list, task_type), owner) in &state.owners {
                if *domain != key.domain_id || *task_type != key.task_type {
                    continue;
                }
                if let Ok(other) = cadence_common::task_list::TaskListName::parse(task_list) {
                    if other.root() == name.root()
                        && !other.is_root()
                        && notify.contains(&(other.partition() as i32))
                    {
                        owner.partitions.push(&new);
                    }
                }
            }
            Ok(Some((new, name.root().to_owned(), notify.into_iter().collect::<Vec<_>>())))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))??;

        let Some((new_config, root_name, read_partitions)) = notification else {
            return Ok(());
        };
        self.notify_read_partitions(
            domain_id,
            &root_name,
            task_list_kind,
            task_type,
            read_partitions,
            new_config,
            caller_deadline,
        )
        .await;
        Ok(())
    }
    /// Go `taskListManagerImpl.notifyPartitionConfig`: refresh every non-root
    /// read partition through its live owner, concurrently. Failures are
    /// reported but never fail the caller, as in Go.
    #[allow(clippy::too_many_arguments)]
    async fn notify_read_partitions(
        &self,
        domain_id: Uuid,
        root_name: &str,
        task_list_kind: i16,
        task_type: i32,
        read_partitions: Vec<i32>,
        config: cadence_persistence::StoredPartitionConfig,
        caller_deadline: Option<tokio::time::Instant>,
    ) {
        let Some(membership) = self.membership.clone() else {
            return;
        };
        let partition_config = crate::grpc_matching::partition_config_to_proto(config);
        let mut notifications = tokio::task::JoinSet::new();
        for partition in read_partitions.into_iter().filter(|partition| *partition != 0) {
            let task_list_name = format!("/__cadence_sys/{root_name}/{partition}");
            let (owner, channel) = match membership.channel_for_task_list_owner(&task_list_name) {
                Ok(resolved) => resolved,
                Err(error) => {
                    eprintln!("warning: resolving Matching partition refresh owner for {task_list_name}: {error}");
                    continue;
                }
            };
            let request = cadence_proto::matching::RefreshTaskListPartitionConfigRequest {
                domain_id: domain_id.to_string(),
                task_list: Some(cadence_proto::api::TaskList {
                    name: task_list_name.clone(),
                    kind: i32::from(task_list_kind) + 1,
                    ..Default::default()
                }),
                task_list_type: task_type + 1,
                partition_config: Some(partition_config.clone()),
            };
            let owner_identity = owner.identity().to_owned();
            notifications.spawn(async move {
                let mut request = tonic::Request::new(request);
                if let Some(deadline) = caller_deadline {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    request.set_timeout(remaining);
                }
                let mut client = cadence_proto::matching::matching_api_client::MatchingApiClient::new(channel);
                let call = client.refresh_task_list_partition_config(request);
                // The gRPC timeout header does not bound a peer that never
                // completes the transport handshake, so enforce the caller's
                // context deadline locally as Go's context cancellation does.
                let result = match caller_deadline {
                    Some(deadline) => tokio::time::timeout_at(deadline, call)
                        .await
                        .unwrap_or_else(|_| Err(Status::deadline_exceeded("context deadline exceeded"))),
                    None => call.await,
                };
                result
                    .map(|_| ())
                    .map_err(|error| (task_list_name, owner_identity, error))
            });
        }
        while let Some(result) = notifications.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err((task_list_name, owner, error))) => {
                    // Go logs partition notification failures but does not fail
                    // the root update RPC. Keep that error behavior while allowing
                    // a later refresh or owner reload to converge.
                    eprintln!("warning: refreshing Matching partition {task_list_name} on {owner} failed: {error}");
                }
                Err(error) => {
                    eprintln!("warning: Matching partition notification task failed: {error}")
                }
            }
        }
    }
    pub async fn refresh_partition_config(
        &self,
        mut key: TaskListKey,
        handle: QueueHandle,
        config: Option<cadence_persistence::StoredPartitionConfig>,
    ) -> Result<(), Status> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(config) = config {
                handle.partitions.push(&config);
                return Ok(());
            }
            key.name = cadence_common::task_list::TaskListName::parse(&key.name)
                .map_err(Status::invalid_argument)?
                .root()
                .into();
            handle.partitions.refresh(store.as_ref(), &key)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    /// An old manager's teardown must never unregister its replacement.
    pub async fn unload(
        &self,
        key: TaskListKey,
        handle: QueueHandle,
        now: i64,
    ) -> Result<bool, Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let mut state = registry.state.lock().expect("Matching registry lock");
            let lookup = (key.domain_id, key.name, key.task_type);
            let Some(owner) = state.owners.get(&lookup) else {
                return Ok(false);
            };
            if !Arc::ptr_eq(&owner.queue, &handle.queue) {
                return Ok(false);
            }
            let owner = state.owners.remove(&lookup).expect("checked owner");
            registry.metrics.task_list_managers_gauge(state.owners.len());
            drop(state);
            owner.finish(now, readers)?;
            Ok(true)
        })
        .await
    }
    /// Called by the service's configured ack timer. Fenced owners stop in the
    /// queue layer; report errors rather than silently accepting stale ownership.
    pub async fn persist_acks(&self, now: i64) -> Result<(), Status> {
        self.flush(now, false, None).await
    }
    /// All handles become unusable after shutdown. Persist every owner even if
    /// another owner fails, and return the first failure for service diagnostics.
    pub async fn shutdown(&self, now: i64) -> Result<(), Status> {
        self.flush(now, true, None).await
    }
    /// Fixed-interval controller. Reset after each completed update, as Go's
    /// task reader does; a transient persistence error does not kill the pump.
    /// The caller supplies the configured interval and owns the shutdown signal.
    pub async fn maintain_until<F>(
        &self,
        interval: std::time::Duration,
        shutdown: F,
    ) -> Result<(), Status>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.reap_stopped(registry_now()).await {
                eprintln!("Matching fatal-owner cleanup failed: {error}");
            }
            let deadline = self
                .state
                .lock()
                .expect("Matching registry lock")
                .owners
                .values()
                .map(|owner| {
                    (*owner
                        .next_ack_update
                        .lock()
                        .expect("Matching ack timer lock"))
                    .unwrap_or_else(|| {
                        *owner.last_update.lock().expect("Matching ack timer lock") + interval
                        })
                        .min(
                        *owner
                            .next_idle_check
                            .lock()
                            .expect("Matching idle timer lock"),
                    )
                })
                .min()
                .unwrap_or_else(|| tokio::time::Instant::now() + interval);
            tokio::select! {
                biased;
                _ = &mut shutdown => return self.shutdown(registry_now()).await,
                _ = &mut changed => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Err(error) = self.sweep_idle(registry_now(), true).await { eprintln!("Matching idle cleanup failed: {error}"); }
                    if let Err(error) = self.flush(registry_now(), false, Some(interval)).await {
                        eprintln!("Matching acknowledgement update failed: {error}");
                    }
                }
            }
        }
    }
    async fn reap_stopped(&self, now: i64) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let candidates = registry
                .state
                .lock()
                .expect("Matching registry lock")
                .owners
                .iter()
                .filter(|(_, owner)| owner.queue.is_stopped())
                .map(|(key, owner)| (key.clone(), owner.clone()))
                .collect::<Vec<_>>();
            let mut failure = None;
            for (key, owner) in candidates {
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
    /// Check idleness using the caller's wall-clock time source. Scheduled
    /// ticks use due_only; an explicit controller scan may check every owner.
    pub async fn sweep_idle(&self, now: i64, due_only: bool) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let candidates = {
                let state = registry.state.lock().expect("Matching registry lock");
                state
                    .owners
                    .iter()
                    .filter_map(|(key, owner)| {
                        let mut next = owner
                            .next_idle_check
                            .lock()
                            .expect("Matching idle timer lock");
                        if due_only && *next > tokio::time::Instant::now() {
                            return None;
                        }
                        // Go uses a ticker at TTL/2; retain its cadence after delayed ticks.
                        let ttl = owner.idle_ttl_nanos.max(2);
                        let interval = std::time::Duration::from_nanos((ttl / 2) as u64);
                        let lag = tokio::time::Instant::now().saturating_duration_since(*next);
                        *next += interval * ((lag.as_nanos() / interval.as_nanos() + 1) as u32);
                        (!owner.liveness.is_alive(now)).then(|| (key.clone(), owner.clone()))
                    })
                    .collect::<Vec<_>>()
            };
            let mut failure = None;
            for (key, owner) in candidates {
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
    fn detach(&self, key: &Key, expected: &Arc<Owner>) -> bool {
        let mut state = self.state.lock().expect("Matching registry lock");
        if state
            .owners
            .get(key)
            .is_some_and(|owner| Arc::ptr_eq(owner, expected))
        {
            state.owners.remove(key);
            self.metrics.task_list_managers_gauge(state.owners.len());
            true
        } else {
            false
        }
    }
    async fn flush(
        &self,
        now: i64,
        stop: bool,
        due: Option<std::time::Duration>,
    ) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let owners = {
                let mut state = registry.state.lock().expect("Matching registry lock");
                if state.stopped {
                    return Ok(());
                }
                if stop {
                    state.stopped = true;
                    let owners = state.owners.drain().collect::<Vec<_>>();
                    for count in (0..owners.len()).rev() {
                        registry.metrics.task_list_managers_gauge(count);
                    }
                    owners
                } else {
                    state
                        .owners
                        .iter()
                        .map(|(key, owner)| (key.clone(), owner.clone()))
                        .collect()
                }
            };
            // Unregister all owners and stop admission before doing any database
            // cleanup. A slow owner must not hold the registry lock or leave
            // other shutdown handles accepting new work.
            if stop {
                for (_, owner) in &owners {
                    owner.stop();
                }
            }
            let mut failure = None;
            for (key, owner) in owners {
                if stop {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                    continue;
                }
                let live_deadline = *owner
                    .next_ack_update
                    .lock()
                    .expect("Matching ack timer lock");
                let ack_due = live_deadline.map_or_else(
                    || {
                        due.is_none_or(|interval| {
                        owner
                            .last_update
                            .lock()
                            .expect("Matching ack timer lock")
                            .elapsed()
                                >= interval
                    })
                    },
                    |deadline| tokio::time::Instant::now() >= deadline,
                );
                if !owner.queue.is_stopped() && !ack_due {
                    continue;
                }
                if let Ok(task_count) = owner.queue.task_list_size() {
                    owner.reader.record_task_count(task_count);
                }
                let (_, ack_level, _) = owner.queue.levels();
                if ack_level >= 0 {
                    owner.reader.record_task_lag(owner.queue.task_lag());
                }
                let update = owner.queue.persist_ack(now);
                if let Err(error) = update {
                    failure.get_or_insert_with(|| Status::unavailable(error.to_string()));
                }
                *owner.last_update.lock().expect("Matching ack timer lock") =
                    tokio::time::Instant::now();
                *owner
                    .next_ack_update
                    .lock()
                    .expect("Matching ack timer lock") =
                    registry.update_ack_interval.as_ref().map(|provider| {
                        let nanos = provider(&key.0.to_string(), &key.1, key.2).max(1) as u64;
                        tokio::time::Instant::now() + std::time::Duration::from_nanos(nanos)
                    });
                if owner.queue.is_stopped() {
                    let detached = registry.detach(&key, &owner);
                    if detached {
                        if let Err(error) = owner.finish(now, readers) {
                            failure.get_or_insert(error);
                        }
                    }
                }
                owner.dispatcher.notify_new_tasks();
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
}

/// Resolve the current parent owner for poll forwarding.
async fn resolve_parent_dispatcher(
    registry: QueueRegistry,
    key: TaskListKey,
    kind: i16,
) -> Option<crate::matching_dispatch::Parent> {
    let router = {
        registry
            .shard_router
            .read()
            .expect("Matching shard router lock")
            .clone()
    };
    if let Some(router) = router {
        match router.remote_channel(&key.name).await {
            Ok((Some(channel), _path)) => {
                return Some(crate::matching_dispatch::Parent::Remote(channel));
            }
            Ok((None, _path)) => {}
            Err(_) => return None,
        }
    } else if let Some(membership) = &registry.membership {
        if let Some(channel) = membership.remote_channel(&key.name).ok()? {
            return Some(crate::matching_dispatch::Parent::Remote(channel));
        }
    }
    if let Some(factory) = registry.local_poll_service.clone() {
        return Some(crate::matching_dispatch::Parent::Service(Box::new(
            factory(registry),
        )));
    }
    let handle = registry.get(key, kind, registry_now()).await.ok()?;
    if handle.stop_if_isolation_changed() {
        return None;
    }
    Some(crate::matching_dispatch::Parent::Dispatcher(Box::new(
        handle.dispatcher,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_blob::BlobEncoding;
    use cadence_persistence::matching_sqlite::SqliteTaskStore;
    use std::time::Duration;

    #[test]
    fn writer_owner_closure_keeps_registry_key_and_observes_provider_updates() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let live_value = Arc::new(std::sync::atomic::AtomicI64::new(1));
        let provider: WriterLiveProvider = {
            let observed = observed.clone();
            let live_value = live_value.clone();
            Arc::new(move |domain, list, kind| {
                observed.lock().unwrap().push((domain.to_owned(), list.to_owned(), kind));
                (live_value.load(std::sync::atomic::Ordering::SeqCst), 5_000_000_000, 250)
            })
        };
        let writer = scoped_writer_live(provider, "domain-uuid".into(), "physical-queue".into(), 1);
        assert_eq!(writer(), (1, 5_000_000_000, 250));
        live_value.store(7, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(writer(), (7, 5_000_000_000, 250), "the owner closure is retained while provider values change");
        assert_eq!(*observed.lock().unwrap(), vec![
            ("domain-uuid".into(), "physical-queue".into(), 1),
            ("domain-uuid".into(), "physical-queue".into(), 1),
        ]);
    }
    #[test]
    fn scaled_partition_from_response_reads_owner_describe_like_go() {
        use cadence_proto::api as p;
        let response = m::DescribeTaskListResponse {
            task_list_status: Some(p::TaskListStatus {
                new_tasks_per_second: 3.0,
                backlog_count_hint: 0,
                empty: false,
                isolation_group_metrics: HashMap::from([
                    (
                        "zone-a".to_owned(),
                        p::IsolationGroupMetrics {
                            new_tasks_per_second: 2.0,
                            poller_count: 1,
                        },
                    ),
                    (
                        "zone-b".to_owned(),
                        p::IsolationGroupMetrics {
                            new_tasks_per_second: 0.0,
                            poller_count: 0,
                        },
                    ),
                ]),
                ..Default::default()
            }),
            partition_config: Some(crate::grpc_matching::partition_config_to_proto(
                cadence_persistence::StoredPartitionConfig {
                    version: 1,
                    read_partitions: HashMap::from([(0, Vec::new()), (2, Vec::new())]),
                    write_partitions: HashMap::from([(0, Vec::new())]),
                },
            )),
            ..Default::default()
        };
        let partition = scaled_partition_from_response(2, &response, false);
        assert_eq!(partition.qps, 3.0);
        // Go `toPartitionMetrics`: the backlog hint decides emptiness unless the
        // partition-empty check is enabled, which reads the status flag instead.
        assert!(partition.empty);
        assert!(!scaled_partition_from_response(2, &response, true).empty);
        // `hasWritePartition` comes from the owner's config.
        assert!(!partition.has_write);
        assert!(scaled_partition_from_response(0, &response, false).has_write);
        let mut groups = partition.groups;
        groups.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            groups,
            vec![("zone-a".into(), 2.0, true), ("zone-b".into(), 0.0, false)]
        );
        // A response without a partition config leaves the partition writable.
        let bare = m::DescribeTaskListResponse::default();
        assert!(scaled_partition_from_response(2, &bare, false).has_write);
    }
    async fn take(registry: &QueueRegistry, handle: &QueueHandle) -> AllocatedTask {
        let reader = registry
            .state
            .lock()
            .unwrap()
            .owners
            .values()
            .find(|owner| Arc::ptr_eq(&owner.queue, &handle.queue))
            .unwrap()
            .reader
            .clone();
        tokio::time::timeout(Duration::from_secs(2), reader.poll())
            .await
            .unwrap()
            .unwrap()
    }
    fn fixture() -> (
        tempfile::TempDir,
        Arc<SqliteTaskStore>,
        QueueRegistry,
        TaskListKey,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        };
        (dir, store, registry, key)
    }
    #[tokio::test]
    async fn task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls() {
        use cadence_common::membership::Host;
        use crate::matching_metrics::{MatchingMetrics, MetricValue, RecordingMetricSink, TASKLIST_MANAGERS};

        let (_dir, _store, registry, key) = fixture();
        let host = |identity: &str| Host {
            address: format!("{identity}:7935"),
            identity: identity.into(),
            grpc_address: format!("{identity}:7835"),
        };
        let membership = crate::matching_membership::Membership::new("self", vec![host("self")]);
        let sink = Arc::new(RecordingMetricSink::default());
        let registry = registry
            .with_membership(membership.clone())
            .with_metrics(MatchingMetrics::new(sink.clone()));

        let first = registry.get(key.clone(), 0, 1).await.unwrap();
        let reused = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(Arc::ptr_eq(&first.queue, &reused.queue));
        assert_eq!(
            sink.events().iter().filter(|event| event.name == TASKLIST_MANAGERS).count(),
            1,
            "lookup reuse does not register another manager"
        );

        first.queue.stop();
        let replacement = registry.get(key.clone(), 0, 3).await.unwrap();
        assert!(!Arc::ptr_eq(&first.queue, &replacement.queue));
        assert!(!registry
            .unload(key.clone(), first.clone(), 4)
            .await
            .unwrap());

        membership.replace(vec![host("other")]);
        registry.unload_non_owned(5).await.unwrap();
        assert!(!registry
            .unload(key.clone(), replacement.clone(), 6)
            .await
            .unwrap());
        membership.replace(vec![host("self")]);
        let reassigned = registry.get(key, 0, 7).await.unwrap();
        registry.shutdown(8).await.unwrap();

        let gauges: Vec<_> = sink
            .events()
            .into_iter()
            .filter(|event| event.name == TASKLIST_MANAGERS)
            .map(|event| event.value)
            .collect();
        assert_eq!(
            gauges,
            vec![
                MetricValue::Number(1.0),
                MetricValue::Number(0.0),
                MetricValue::Number(1.0),
                MetricValue::Number(0.0),
                MetricValue::Number(1.0),
                MetricValue::Number(0.0),
            ]
        );
        assert!(sink
            .events()
            .into_iter()
            .filter(|event| event.name == TASKLIST_MANAGERS)
            .all(|event| event.tags.get("operation").map(String::as_str) == Some("TaskListMgr")));
        assert!(reassigned.queue.is_stopped());
    }
    #[tokio::test]
    async fn matching_membership_guards_creation_and_unloads_moved_owners() {
        use cadence_common::membership::Host;
        let (_dir, _store, registry, key) = fixture();
        let host = |identity: &str| Host {
            address: format!("{identity}:7935"),
            identity: identity.into(),
            grpc_address: format!("{identity}:7835"),
        };
        let membership = crate::matching_membership::Membership::new("self", vec![host("other")]);
        let registry = registry.with_membership(membership.clone());
        let error = registry.get(key.clone(), 0, 1).await.err().unwrap();
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert!(registry.state.lock().unwrap().owners.is_empty());
        membership.replace(vec![host("self")]);
        let old = registry.get(key.clone(), 0, 2).await.unwrap();
        membership.replace(Vec::new());
        assert!(registry.unload_non_owned(3).await.is_err());
        assert!(
            !old.queue.is_stopped(),
            "unknown placement must retain the owner"
        );
        membership.replace(vec![host("other")]);
        registry.unload_non_owned(4).await.unwrap();
        assert!(old.queue.is_stopped());
        assert!(registry.state.lock().unwrap().owners.is_empty());
        assert_eq!(
            registry.get(key.clone(), 0, 5).await.err().unwrap().code(),
            tonic::Code::Aborted
        );
        membership.replace(vec![host("self")]);
        let replacement = registry.get(key.clone(), 0, 6).await.unwrap();
        assert!(!replacement.queue.is_stopped());
        assert!(!Arc::ptr_eq(&old.queue, &replacement.queue));
        registry.shutdown(7).await.unwrap();
    }

    #[tokio::test]
    async fn shard_policy_accepts_local_assignment_and_rechecks_before_publish() {
        use cadence_common::membership::Host;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (_dir, _store, registry, key) = fixture();
        let host = Host {
            address: "other:7935".into(),
            identity: "other".into(),
            grpc_address: "other:7835".into(),
        };
        let registry = registry.with_membership(
            crate::matching_membership::Membership::new("self", vec![host]),
        );
        let checks = Arc::new(AtomicUsize::new(0));
        let policy = QueueOwnershipPolicy::new(
            Arc::new(|_| true),
            {
                let checks = checks.clone();
                Arc::new(move |_| checks.fetch_add(1, Ordering::SeqCst) == 0)
            },
        );
        registry.set_ownership_policy(policy);

        let error = registry.get(key, 0, 1).await.err().unwrap();
        assert_eq!(error.code(), tonic::Code::Aborted);
        assert_eq!(checks.load(Ordering::SeqCst), 2);
        assert!(registry.state.lock().unwrap().owners.is_empty());
    }

    #[tokio::test]
    async fn excluded_cached_owner_skips_ring_check_but_new_owner_does_not() {
        use cadence_common::membership::Host;

        let (_dir, _store, registry, key) = fixture();
        let host = |identity: &str| Host {
            address: format!("{identity}:7935"),
            identity: identity.into(),
            grpc_address: format!("{identity}:7835"),
        };
        let membership = crate::matching_membership::Membership::new("self", vec![host("self")]);
        let registry = registry.with_membership(membership.clone());
        registry.set_ownership_policy(QueueOwnershipPolicy::new(
            Arc::new(|_| false),
            Arc::new(|_| false),
        ));
        let cached = registry.get(key.clone(), 0, 1).await.unwrap();

        membership.replace(vec![host("other")]);
        let reused = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(Arc::ptr_eq(&cached.queue, &reused.queue));

        let mut uncached_key = key;
        uncached_key.name = "uncached-excluded".into();
        assert_eq!(
            registry
                .get(uncached_key, 0, 3)
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::Aborted
        );
    }

    #[tokio::test]
    async fn assigned_eligible_owner_unloads_when_local_processor_snapshot_is_lost() {
        use cadence_common::membership::Host;
        use std::sync::atomic::{AtomicBool, Ordering};

        let (_dir, _store, registry, key) = fixture();
        let membership = crate::matching_membership::Membership::new(
            "self",
            vec![Host {
                address: "other:7935".into(),
                identity: "other".into(),
                grpc_address: "other:7835".into(),
            }],
        );
        let registry = registry.with_membership(membership);
        let assigned = Arc::new(AtomicBool::new(true));
        registry.set_ownership_policy(QueueOwnershipPolicy::new(
            Arc::new(|_| true),
            {
                let assigned = assigned.clone();
                Arc::new(move |_| assigned.load(Ordering::SeqCst))
            },
        ));
        let handle = registry.get(key, 0, 1).await.unwrap();
        assigned.store(false, Ordering::SeqCst);
        assert_eq!(
            registry
                .get(
                    TaskListKey {
                        domain_id: Uuid::from_u128(1),
                        name: "queue".into(),
                        task_type: 0,
                    },
                    0,
                    2,
                )
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::Aborted
        );
        registry.unload_non_owned(3).await.unwrap();
        assert!(handle.queue.is_stopped());
    }

    #[tokio::test]
    async fn slow_startup_does_not_block_loaded_owner_or_shutdown() {
        let (directory, _store, registry, key) = fixture();
        let loaded = registry.get(key.clone(), 0, 1).await.unwrap();
        let connection = rusqlite::Connection::open(directory.path().join("tasks.db")).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut new_key = key.clone();
        new_key.name = "slow-startup".into();
        let lookup = (new_key.domain_id, new_key.name.clone(), new_key.task_type);
        let worker = registry.clone();
        let loading = tokio::spawn(async move { worker.get(new_key, 0, 2).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let gate = registry
                    .startups
                    .lock()
                    .unwrap()
                    .get(&lookup)
                    .and_then(Weak::upgrade);
                if gate.is_some_and(|gate| gate.try_lock().is_err()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Let the lease call enter SQLite's busy wait behind our transaction.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let again = tokio::time::timeout(Duration::from_millis(500), registry.get(key, 0, 3))
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&loaded.queue, &again.queue));
        // Shutdown stops admission without waiting for a lease that has not yet
        // been registered. The loader must fence its eventual publication.
        let shutdown = registry.clone();
        let stopped = tokio::spawn(async move { shutdown.shutdown(4).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if registry.state.lock().unwrap().stopped {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            loading.await.unwrap().err().unwrap().code(),
            tonic::Code::Unavailable
        );
        stopped.await.unwrap().unwrap();
        assert!(registry.state.lock().unwrap().owners.is_empty());
    }

    #[tokio::test]
    async fn concurrent_load_acquires_one_lease_and_isolates_task_types() {
        let (_dir, store, registry, key) = fixture();
        let mut requests = Vec::new();
        for _ in 0..20 {
            let registry = registry.clone();
            let key = key.clone();
            requests.push(tokio::spawn(async move {
                registry.get(key, 0, 1).await.unwrap()
            }));
        }
        let first = requests.remove(0).await.unwrap();
        for request in requests {
            assert!(Arc::ptr_eq(&first.queue, &request.await.unwrap().queue));
        }
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 1);
        let mut activity = key.clone();
        activity.task_type = 1;
        let other = registry.get(activity, 0, 1).await.unwrap();
        assert!(!Arc::ptr_eq(&first.queue, &other.queue));
    }
    #[test]
    fn shutdown_joins_readers_with_one_blocking_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_directory, _store, registry, key) = fixture();
            let mut handles = Vec::new();
            for index in 0..10 {
                let mut key = key.clone();
                key.name = format!("shutdown-{index}");
                let handle = registry.get(key, 0, 1).await.unwrap();
                handle
                    .append(vec![TaskData::default(); 3], 1)
                    .await
                    .unwrap();
                handles.push(handle);
            }
            tokio::time::timeout(Duration::from_secs(2), registry.shutdown(2))
                .await
                .unwrap()
                .unwrap();
            assert!(handles.iter().all(|handle| handle.queue.is_stopped()));
        });
    }
    #[tokio::test]
    async fn shutdown_persists_ack_and_stops_previously_returned_handles() {
        let (_dir, store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &handle).await;
        handle.queue.complete(task.task_id).unwrap();
        registry.shutdown(2).await.unwrap();
        assert_eq!(
            store.get_task_list(&key).unwrap().data.ack_level,
            task.task_id
        );
        assert!(handle.queue.append(&[TaskData::default()], 3).is_err());
        assert!(registry.get(key, 0, 3).await.is_err());
        registry.shutdown(3).await.unwrap();
    }
    #[tokio::test]
    async fn fenced_owner_does_not_prevent_other_queues_from_flushing() {
        let (_dir, store, registry, key) = fixture();
        let stale = registry.get(key.clone(), 0, 1).await.unwrap();
        let mut other_key = key.clone();
        other_key.name = "other".into();
        let other = registry.get(other_key.clone(), 0, 1).await.unwrap();
        other.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &other).await;
        other.queue.complete(task.task_id).unwrap();
        store.lease(&key, 0, 0, 2).unwrap();
        assert!(registry.persist_acks(3).await.is_err());
        assert!(stale.append(vec![TaskData::default()], 3).await.is_err());
        assert_eq!(
            store.get_task_list(&other_key).unwrap().data.ack_level,
            task.task_id
        );
        assert!(!registry.state.lock().unwrap().owners.contains_key(&(
            key.domain_id,
            key.name.clone(),
            key.task_type
        )));
        registry.shutdown(4).await.unwrap();
        assert!(other.append(vec![TaskData::default()], 4).await.is_err());
    }
    #[tokio::test]
    async fn stale_teardown_cannot_remove_replacement_owner() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        assert!(registry.unload(key.clone(), old.clone(), 2).await.unwrap());
        let new = registry.get(key.clone(), 0, 3).await.unwrap();
        assert!(!registry.unload(key.clone(), old, 4).await.unwrap());
        let same = registry.get(key.clone(), 0, 4).await.unwrap();
        assert!(Arc::ptr_eq(&new.queue, &same.queue));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        let recovered = take(&registry, &new).await;
        assert_eq!(recovered.task_id, 1);
    }
    #[tokio::test]
    async fn maintenance_persists_live_ack_and_shutdown_stops_handles() {
        let (_dir, store, mut registry, key) = fixture();
        registry.update_ack_interval = Some(Arc::new(|_, _, _| 20_000_000));
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &handle).await;
        handle.queue.complete(task.task_id).unwrap();
        assert_eq!(store.get_task_list(&key).unwrap().data.ack_level, 0);
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(60), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.get_task_list(&key).unwrap().data.ack_level != task.task_id {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(store.get_task_list(&key).unwrap().data.last_updated_nanos > 1);
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
        assert!(handle.queue.is_stopped());
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert!(handle.append(vec![TaskData::default()], 2).await.is_err());
    }
    #[tokio::test]
    async fn maintenance_uses_live_tasklist_ack_interval_provider() {
        let (_dir, store, mut registry, key) = fixture();
        registry.update_ack_interval = Some(Arc::new(|_, _, _| 60_000_000_000));
        registry.get(key.clone(), 0, 1).await.unwrap();
        {
            let state = registry.state.lock().unwrap();
            *state.owners[&(key.domain_id, key.name.clone(), key.task_type)]
                .last_update
                .lock()
                .unwrap() = tokio::time::Instant::now() - Duration::from_secs(120);
            *state.owners[&(key.domain_id, key.name.clone(), key.task_type)]
                .next_ack_update
                .lock()
                .unwrap() = Some(tokio::time::Instant::now() - Duration::from_secs(60));
        }
        registry.flush(100, false, None).await.unwrap();
        assert_eq!(
            store.get_task_list(&key).unwrap().data.last_updated_nanos,
            100
        );
        registry.shutdown(101).await.unwrap();
    }

    #[tokio::test]
    async fn matching_idle_interval_is_sampled_for_each_owner_only_at_creation() {
        let (_dir, _store, mut registry, first) = fixture();
        let configured = Arc::new(std::sync::atomic::AtomicI64::new(10_000_000_000));
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        registry.idle_tasklist_check_interval = {
            let configured = configured.clone();
            let reads = reads.clone();
            Some(Arc::new(move |_, _, _| {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                configured.load(std::sync::atomic::Ordering::SeqCst)
            }))
        };
        registry.get(first.clone(), 0, 1).await.unwrap();
        configured.store(20_000_000_000, std::sync::atomic::Ordering::SeqCst);
        let mut second = first.clone();
        second.name = "new-owner".into();
        registry.get(second.clone(), 0, 1).await.unwrap();
        registry.sweep_idle(registry_now(), false).await.unwrap();
        let state = registry.state.lock().unwrap();
        assert_eq!(
            state.owners[&(first.domain_id, first.name, first.task_type)].idle_ttl_nanos,
            10_000_000_000
        );
        assert_eq!(
            state.owners[&(second.domain_id, second.name, second.task_type)].idle_ttl_nanos,
            20_000_000_000
        );
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn stopped_writer_is_replaced_without_waiting_for_ack_timer() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &old).await;
        old.queue.complete(task.task_id).unwrap();
        old.queue.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(!Arc::ptr_eq(&old.queue, &new.queue));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        assert_eq!(
            store.get_task_list(&key).unwrap().data.ack_level,
            task.task_id
        );
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert!(!registry.unload(key, old, 3).await.unwrap());
    }
    #[tokio::test]
    async fn shutdown_wakes_blocked_dispatcher() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key, 0, 1).await.unwrap();
        let poll = tokio::spawn(async move {
            handle
                .dispatcher
                .poll_decision(
                    cadence_proto::api::PollForDecisionTaskRequest::default(),
                    Duration::from_secs(10),
                )
                .await
        });
        tokio::task::yield_now().await;
        registry.shutdown(2).await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(response.task_token.is_empty());
    }
    #[tokio::test]
    async fn maintenance_deadlines_are_per_owner_and_reset_after_update() {
        let (_dir, store, registry, first) = fixture();
        registry.get(first.clone(), 0, 1).await.unwrap();
        let mut second = first.clone();
        second.name = "later".into();
        registry.get(second.clone(), 0, 1).await.unwrap();
        {
            let state = registry.state.lock().unwrap();
            let owner = &state.owners[&(first.domain_id, first.name.clone(), first.task_type)];
            *owner.last_update.lock().unwrap() =
                tokio::time::Instant::now() - Duration::from_secs(120);
        }
        registry
            .flush(100, false, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(
            store.get_task_list(&first).unwrap().data.last_updated_nanos,
            100
        );
        assert_eq!(
            store
                .get_task_list(&second)
                .unwrap()
                .data
                .last_updated_nanos,
            1
        );
        registry
            .flush(101, false, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(
            store.get_task_list(&first).unwrap().data.last_updated_nanos,
            100
        );
        assert_eq!(
            store
                .get_task_list(&second)
                .unwrap()
                .data
                .last_updated_nanos,
            1
        );
        registry.shutdown(102).await.unwrap();
    }
    #[tokio::test]
    async fn replacement_can_load_before_old_cleanup_and_keeps_its_lease() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &old).await;
        old.queue.complete(task.task_id).unwrap();
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let owner = registry.state.lock().unwrap().owners[&lookup].clone();
        assert!(registry.detach(&lookup, &owner));
        owner.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        let added = new.append(vec![TaskData::default()], 2).await.unwrap();
        assert_eq!(added[0].task_id, 101);
        let finishing = owner.clone();
        assert!(
            owner_work(move |readers| finishing.finish(3, readers))
                .await
                .is_err(),
            "old final ack must be fenced"
        );
        assert!(!registry.detach(&lookup, &owner));
        assert!(Arc::ptr_eq(
            &new.queue,
            &registry.get(key.clone(), 0, 4).await.unwrap().queue
        ));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        assert_eq!(store.get_task_list(&key).unwrap().data.ack_level, 0);
        assert_eq!(
            store
                .get_tasks(&key, 0, 200, 10)
                .unwrap()
                .iter()
                .map(|task| task.task_id)
                .collect::<Vec<_>>(),
            vec![101]
        );
    }
    #[tokio::test]
    async fn idle_unload_retains_unacknowledged_tasks_for_recovery() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        old.liveness.mark_alive(0);
        registry.sweep_idle(300_000_000_000, false).await.unwrap();
        assert!(old.queue.is_stopped());
        assert!(registry.state.lock().unwrap().owners.is_empty());
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 1);
        let new = registry.get(key.clone(), 0, 300_000_000_001).await.unwrap();
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        let recovered = take(&registry, &new).await;
        assert_eq!(recovered.task_id, 1);
    }
    #[tokio::test]
    async fn describe_lookup_does_not_refresh_liveness_but_append_does() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.liveness.mark_alive(0);
        let described = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(!described.liveness.is_alive(registry_now()));
        handle.append(vec![TaskData::default()], 2).await.unwrap();
        assert!(described.liveness.is_alive(registry_now()));
        registry.sweep_idle(registry_now(), false).await.unwrap();
        assert!(!handle.queue.is_stopped());
    }
    #[tokio::test]
    async fn idle_tick_runs_without_waiting_for_ack_deadline() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.liveness.mark_alive(0);
        {
            let state = registry.state.lock().unwrap();
            *state.owners[&(key.domain_id, key.name.clone(), key.task_type)]
                .next_idle_check
                .lock()
                .unwrap() = tokio::time::Instant::now();
        }
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(60), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.queue.is_stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn fatal_owner_wakeup_does_not_wait_for_ack_interval() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(3600), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        handle.queue.stop();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !registry.state.lock().unwrap().owners.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn registration_notification_reschedules_empty_controller() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let owner = registry
            .state
            .lock()
            .unwrap()
            .owners
            .remove(&lookup)
            .unwrap();
        owner.liveness.mark_alive(0);
        *owner.next_idle_check.lock().unwrap() = tokio::time::Instant::now();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(3600), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Controlled registration after the pump has begun waiting with no owners.
        registry.state.lock().unwrap().owners.insert(lookup, owner);
        registry.changed.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.queue.is_stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
}
