//! Byte-preserving native twin of `matching_registry.rs`: Matching-owned queue
//! lifecycle over `MatchingTaskStoreBytes`/`DurableTaskQueueBytes`, the native
//! writer, reader, sync matcher, dispatcher, partition cache, byte-keyed poller
//! history and QPS tracker. Queue identity is `(Uuid, GoString, i32)`, so raw
//! `\xff`, `\xfe` and U+FFFD task lists are distinct owners, leases and keys.
//!
//! Lifecycle is copied from the legacy registry (cancellation-safe blocking
//! startup under per-identity gates, ownership checks before and after I/O,
//! replacement-safe detach, ack/idle timers, GC, shutdown, fatal wiring, live
//! config providers, partition update/refresh cancellation). Lossy text exists
//! only in diagnostics and metric tags (`diag`).
//!
//! Typed native seams replace pieces that cannot be ported without String
//! identity (see `memory/matching-native-registry-20261028.md`):
//! `NativeMembership`, `NativeParentRouter`, `NativeLocalPollFactory`,
//! `NativePartitionRefresher`, `NativeScalerStarter`, `NativeDomainHooks`.
//! UNVERIFIED: not compiled or tested by the author (no local Cargo).
use crate::{
    matching_dispatch_native::{
        NativeFuture, NativeMatchingApi, NativeParent, NativePollObserver, NativePollerManager,
        NativeTaskDispatcher,
    },
    matching_history::MatchingHistoryClient,
    matching_metrics,
    matching_partition_native::{NativePartitionCache, NativeStoredPartitionConfig},
    matching_query_native::{QueryChannel, QueryIndex, QueryOutcome},
    matching_reader_native::{
        NativeBacklogForward, NativeDispatchCheck, NativeIsolationConfig, NativeIsolationSelector,
        NativeTaskReader, ReaderConfig, ReaderFatalSink, ReaderLive,
    },
    matching_standby_native::{checker_native, DomainResolverNative, EnabledNative},
    matching_sync_native::NativeSyncMatcher,
    matching_writer_native::{NativeFatalSink, NativeTaskWriter},
};
use cadence_common::{go_string::GoString, task_list::TaskListNameBytes};
use cadence_persistence::{
    matching_blob::TaskDataBytes,
    matching_native_queue::{DurableTaskQueueBytes, NativeQueueError, PendingTaskBytes},
    matching_native_task_store::{
        AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as StoreError,
        TaskListKeyBytes,
    },
    matching_task_store::MatchingTaskStoreError as LegacyStoreError,
};
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex, RwLock, Weak},
    time::Duration,
};
use tonic::Status;
use uuid::Uuid;

fn registry_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

/// Diagnostic text only. Never use for selection, routing or keys.
fn diag(bytes: &[u8]) -> String {
    const LIMIT: usize = 256;
    let mut text = String::from_utf8_lossy(&bytes[..bytes.len().min(LIMIT)]).into_owned();
    if bytes.len() > LIMIT {
        text.push_str("...");
    }
    text
}

fn gs(domain: Uuid) -> GoString {
    GoString::from(domain.to_string())
}

fn invalid_name(message: GoString) -> Status {
    crate::matching_native_status::raw_status(
        tonic::Code::InvalidArgument,
        &message,
        tonic::codegen::Bytes::new(),
    )
}

type Key = (Uuid, GoString, i32);
type StartupGates = HashMap<Key, Weak<Mutex<()>>>;

/// Dropping an RPC future cancels its Go context even when no deadline was set.
/// Blocking persistence cannot be interrupted mid-call; its next retry wait observes this flag.
struct PartitionUpdateCancellation(Arc<std::sync::atomic::AtomicBool>);
impl Drop for PartitionUpdateCancellation {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// `(domain id bytes, task list bytes, persistence task type)` live property read.
pub type NativeProvider<T> = Arc<dyn Fn(&GoString, &GoString, i32) -> T + Send + Sync>;
pub type NativeScalerConfigProvider = NativeProvider<crate::matching_scaler::ScalerConfig>;
pub type NativeIsolationProvider = Arc<
    dyn Fn(
            &GoString,
            &GoString,
            i32,
            i16,
            Arc<NativePollerManager>,
            Arc<NativePartitionCache>,
        ) -> NativeIsolationConfig
        + Send
        + Sync,
>;
pub type NativeWriterLiveProvider = NativeProvider<(i64, i64, i64)>;
pub type NativeWriterLiveConfig = Arc<dyn Fn() -> (i64, i64, i64) + Send + Sync>;
pub type NativeReaderPropertiesProvider = NativeProvider<ReaderLive>;
pub type NativePollObserverProvider =
    Arc<dyn Fn(&matching_metrics::PollMetricContext) -> Arc<dyn NativePollObserver> + Send + Sync>;
pub const DEFAULT_QPS_TRACKER_INTERVAL_NANOS: i64 = 10_000_000_000;
const QPS_TRACKER_EXP: f64 = 0.5;

/// Typed ring-membership seam (Go ringpop `Lookup`/`WhoAmI`). The legacy
/// `Membership` hashes `&str`; a raw-byte ring must hash the `GoString` bytes.
pub type NativeOwnershipLookup<T> = Arc<dyn Fn(&GoString) -> Result<T, Status> + Send + Sync>;
pub type NativeOwnershipError = Arc<dyn Fn(&GoString, &str) -> Status + Send + Sync>;
pub type NativeDomainName = Arc<dyn Fn(&GoString) -> Option<String> + Send + Sync>;
#[derive(Clone)]
pub struct NativeMembership {
    pub check_task_list: NativeOwnershipLookup<()>,
    pub owns: NativeOwnershipLookup<bool>,
    /// Go `TaskListNotOwnedByHostError` for a shard-distributor miss; `&str` is the reason.
    pub task_list_not_owned_by: NativeOwnershipError,
}

/// Synchronous snapshot policy used while the registry lock is held.
#[derive(Clone)]
pub struct NativeQueueOwnershipPolicy {
    pub is_shard_distributor_eligible: Arc<dyn Fn(&GoString) -> bool + Send + Sync>,
    pub has_local_processor: Arc<dyn Fn(&GoString) -> bool + Send + Sync>,
}

/// Domain-cache seams (Go `domainCache`). Names are diagnostic tags only.
#[derive(Clone)]
pub struct NativeDomainHooks {
    /// Go `IsActiveIn(cluster)` by raw domain id; unknown domains must answer true.
    pub is_active: Arc<dyn Fn(&GoString) -> bool + Send + Sync>,
    /// Metric tag only.
    pub domain_name: NativeDomainName,
    pub standby_domains: DomainResolverNative,
}

/// Parent resolution for poll/add forwarding when the parent partition is not
/// local: `Ok(Some(api))` forwards remotely, `Ok(None)` resolves locally, `Err`
/// abandons forwarding (legacy shard router / membership `remote_channel`).
pub type NativeParentRouter = Arc<
    dyn Fn(&GoString) -> NativeFuture<Result<Option<Arc<dyn NativeMatchingApi>>, Status>>
        + Send
        + Sync,
>;
/// In-process service for local parents (legacy `LocalPollFactory`).
pub type NativeLocalPollFactory =
    Arc<dyn Fn(NativeLocalPollHost) -> Option<Arc<dyn NativeMatchingApi>> + Send + Sync>;

/// Weak registry capability passed only for the duration of local-parent
/// service construction. The callback must not retain this registry in an
/// owner resolver; its result is the per-call service adapter.
#[derive(Clone)]
pub struct NativeLocalPollHost {
    detached: NativeQueueRegistry,
    state: Weak<Mutex<State>>,
}
impl NativeLocalPollHost {
    #[must_use]
    pub fn registry(&self) -> Option<NativeQueueRegistry> {
        let mut registry = self.detached.clone();
        registry.state = self.state.upgrade()?;
        Some(registry)
    }
}
pub type NativeLocalPollServiceBuilder =
    Arc<dyn Fn(NativeQueueRegistry) -> Arc<dyn NativeMatchingApi> + Send + Sync>;

/// One `RefreshTaskListPartitionConfig` notification to a non-root read
/// partition's owner (Go `notifyPartitionConfig`).
#[derive(Clone, Debug)]
pub struct NativePartitionRefresh {
    pub domain_id: Uuid,
    /// `/__cadence_sys/{root}/{partition}` as raw bytes.
    pub task_list: GoString,
    pub kind: i16,
    /// Persistence task type (0 decision, 1 activity).
    pub task_type: i32,
    pub config: NativeStoredPartitionConfig,
    pub deadline: Option<tokio::time::Instant>,
}
/// Resolves the partition owner and sends the refresh. Owner-resolution
/// failures are logged by the seam and return `Ok(())`, like the legacy path.
pub type NativePartitionRefresher =
    Arc<dyn Fn(NativePartitionRefresh) -> NativeFuture<Result<(), Status>> + Send + Sync>;

/// Handed to the adaptive scaler seam. Holds the registry weakly so the loop
/// can neither pin the registry nor resurrect it (legacy `scaler_loop`).
#[derive(Clone)]
pub struct NativeScalerHost {
    detached: NativeQueueRegistry,
    state: Weak<Mutex<State>>,
    pub root: TaskListKeyBytes,
}
impl NativeScalerHost {
    /// `None` once every other registry handle is gone.
    #[must_use]
    pub fn registry(&self) -> Option<NativeQueueRegistry> {
        let mut registry = self.detached.clone();
        registry.state = self.state.upgrade()?;
        Some(registry)
    }
}
/// Scaler loop factory. The future ends when the scaler should stop; the owner
/// aborts it on unload/shutdown/drop. Go `adaptiveScalerImpl` uses String-keyed
/// `matching_scaler` types, so the loop itself is injected (see memory note).
pub type NativeScalerStarter = Arc<dyn Fn(NativeScalerHost) -> NativeFuture<()> + Send + Sync>;

/// A loaded task list as `GetTaskListsByDomain` reports it.
#[derive(Clone, Debug)]
pub struct NativeLoadedTaskList {
    /// Full partition name.
    pub name: GoString,
    pub root: GoString,
    pub task_type: i32,
    pub kind: i16,
    pub pollers: Vec<cadence_common::poller_bytes::HistoricalInfo>,
    pub partition_config: Option<NativeStoredPartitionConfig>,
}
#[derive(Clone, Debug, Default)]
pub struct NativeTaskListsByDomain {
    pub decision: Vec<NativeLoadedTaskList>,
    pub activity: Vec<NativeLoadedTaskList>,
}

/// Fatal conditions the host must terminate for (Go `logger.Fatal`). The
/// legacy `MatchingFatal` carries String RunIDs, so native sinks are typed.
#[derive(Clone, Default)]
pub struct NativeFatalSinks {
    pub writer: Option<NativeFatalSink>,
    pub reader: Option<ReaderFatalSink>,
}

struct Owner {
    queue: Arc<DurableTaskQueueBytes>,
    dispatcher: NativeTaskDispatcher,
    writer: NativeTaskWriter,
    reader: NativeTaskReader,
    limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    pollers: Arc<NativePollerManager>,
    queries: Arc<QueryChannel>,
    qps: Arc<cadence_common::qps_bytes::EmaQpsBytesTracker>,
    partitions: Arc<NativePartitionCache>,
    sync_matcher: Arc<NativeSyncMatcher>,
    isolation_select: NativeIsolationSelector,
    sync_wait: Arc<dyn Fn() -> Duration + Send + Sync>,
    forward_tasks: Arc<crate::matching_forward::RequestTokens>,
    forward_rate: Arc<crate::matching_forward::ForwardRate>,
    scaler: crate::matching_scaler::ScalerDaemon,
    created: i64,
    idle_ttl_nanos: i64,
    last_update: Mutex<tokio::time::Instant>,
    next_ack_update: Mutex<Option<tokio::time::Instant>>,
    next_idle_check: Mutex<tokio::time::Instant>,
    liveness: Arc<cadence_common::liveness::Liveness>,
}
impl Owner {
    fn stop(&self) {
        self.writer.stop();
        self.reader.stop();
        self.queue.stop();
        self.dispatcher.notify_new_tasks();
    }
    fn finish(&self, now: i64, readers: &mut Vec<NativeTaskReader>) -> Result<(), Status> {
        self.scaler.stop();
        self.stop();
        readers.push(self.reader.clone());
        let ack = self.queue.persist_final_ack(now);
        let garbage = self.queue.run_garbage_collection(true);
        tolerate_lost_ownership(ack.and(garbage.map(|_| ())))
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}
/// Go's task list Stop logs and ignores a final-update failure. Losing the
/// lease to a peer (ConditionFailed) during shutdown is expected, not fatal.
fn tolerate_lost_ownership(result: Result<(), NativeQueueError>) -> Result<(), NativeQueueError> {
    match result {
        Err(NativeQueueError::Storage(StoreError::ConditionFailed { message })) => {
            eprintln!(
                "Matching task list lost ownership during shutdown: {}",
                diag(message.as_bytes())
            );
            Ok(())
        }
        Err(NativeQueueError::Storage(StoreError::Legacy(LegacyStoreError::ConditionFailed(
            reason,
        )))) => {
            eprintln!("Matching task list lost ownership during shutdown: {reason}");
            Ok(())
        }
        other => other,
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.scaler.stop();
    }
}
#[derive(Default)]
struct State {
    owners: HashMap<Key, Arc<Owner>>,
    stopped: bool,
}

type NativeShardRouterSlot =
    Arc<RwLock<Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>>>;

/// Queue creation and insertion run in the same blocking worker, so cancelling
/// an RPC while its lease is acquired cannot orphan or duplicate that lease.
#[derive(Clone)]
pub struct NativeQueueRegistry {
    store: Arc<dyn MatchingTaskStoreBytes>,
    history: MatchingHistoryClient,
    range_size: NonZeroU32,
    state: Arc<Mutex<State>>,
    query_index: Arc<QueryIndex>,
    changed: Arc<tokio::sync::Notify>,
    startups: Arc<Mutex<StartupGates>>,
    ownership_policy: Arc<RwLock<Option<NativeQueueOwnershipPolicy>>>,
    admission: Arc<RwLock<Option<crate::matching_runtime::NativeShardOwnershipCheck>>>,
    parent_router: Option<NativeParentRouter>,
    shard_router: NativeShardRouterSlot,
    standby_enabled: Option<EnabledNative>,
    membership: Option<NativeMembership>,
    local_poll_service: Option<NativeLocalPollFactory>,
    partition_refresher: Option<NativePartitionRefresher>,
    scaler_starter: Option<NativeScalerStarter>,
    domain_hooks: Option<NativeDomainHooks>,
    poll_observer: Option<NativePollObserverProvider>,
    partition_count: Option<NativeProvider<i64>>,
    cache_enabled: Option<NativeProvider<bool>>,
    sync_wait: Option<NativeProvider<i64>>,
    local_poll_wait: Option<NativeProvider<i64>>,
    forward_degree: Option<NativeProvider<i64>>,
    forward_polls: Option<NativeProvider<i64>>,
    forward_tasks: Option<NativeProvider<i64>>,
    forward_rate: Option<NativeProvider<i64>>,
    qps_interval: Option<NativeProvider<i64>>,
    qps_clock: cadence_common::qps::Clock,
    isolation: Option<NativeIsolationProvider>,
    reader_live: Option<NativeReaderPropertiesProvider>,
    min_burst: Option<NativeProvider<i64>>,
    update_ack_interval: Option<NativeProvider<i64>>,
    idle_tasklist_check_interval: Option<NativeProvider<i64>>,
    max_task_delete_batch_size: Option<NativeProvider<i64>>,
    writer_live: Option<NativeWriterLiveProvider>,
    fatal: NativeFatalSinks,
    writer_tracker: Arc<crate::matching_writer::WriterTracker>,
    metrics: matching_metrics::MatchingMetrics,
}

#[derive(Clone)]
pub struct NativeQueueHandle {
    pub queue: Arc<DurableTaskQueueBytes>,
    pub dispatcher: NativeTaskDispatcher,
    pub pollers: Arc<NativePollerManager>,
    pub limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    /// Go `matcher.queryTaskC` plus the engine's `lockableQueryTaskMap`.
    pub queries: Arc<QueryChannel>,
    /// Go `taskListManagerImpl.qpsTracker`.
    pub qps: Arc<cadence_common::qps_bytes::EmaQpsBytesTracker>,
    pub partitions: Arc<NativePartitionCache>,
    sync_matcher: Arc<NativeSyncMatcher>,
    isolation_select: NativeIsolationSelector,
    sync_wait: Arc<dyn Fn() -> Duration + Send + Sync>,
    created: i64,
    liveness: Arc<cadence_common::liveness::Liveness>,
    writer: NativeTaskWriter,
    forward_tasks: Arc<crate::matching_forward::RequestTokens>,
    forward_rate: Arc<crate::matching_forward::ForwardRate>,
}
pub use crate::matching_registry::{ForwardTaskPermitOutcome, SyncMatchOutcome};

impl NativeQueueHandle {
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
    /// Go `OfferQuery` takes a forward-task token but applies no rate limit.
    pub fn try_forward_query(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.forward_tasks.try_acquire()
    }
    /// Wait for a query forwarding token without applying the add-task rate limit.
    pub async fn wait_forward_query(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.forward_tasks.acquire().await
    }
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
        data: &AllocatedTaskBytes,
        source_domain: &GoString,
        deadline: tokio::time::Instant,
    ) -> bool {
        matches!(
            self.offer_sync_observed(data, source_domain, deadline, true, false)
                .await,
            SyncMatchOutcome::Matched
        )
    }
    pub async fn try_sync_match_observed(
        &self,
        data: &AllocatedTaskBytes,
        source_domain: &GoString,
        deadline: tokio::time::Instant,
    ) -> SyncMatchOutcome {
        self.offer_sync_observed(data, source_domain, deadline, true, false)
            .await
    }
    /// Go `TaskMatcher.Offer` skips the dispatch rate limiter for tasks
    /// already forwarded from a child partition.
    pub async fn try_sync_match_forwarded(
        &self,
        data: &AllocatedTaskBytes,
        source_domain: &GoString,
        deadline: tokio::time::Instant,
    ) -> bool {
        matches!(
            self.offer_sync_observed(data, source_domain, deadline, false, true)
                .await,
            SyncMatchOutcome::Matched
        )
    }
    pub async fn offer_forwarded_backlog(
        &self,
        data: &AllocatedTaskBytes,
        source_domain: &GoString,
        deadline: tokio::time::Instant,
    ) -> bool {
        let group = (self.isolation_select)(&data.data).0;
        self.sync_matcher
            .offer(
                data.clone(),
                &group,
                source_domain,
                true,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                deadline,
            )
            .await
    }
    async fn offer_sync_observed(
        &self,
        data: &AllocatedTaskBytes,
        source_domain: &GoString,
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
        let group = (self.isolation_select)(&data.data).0;
        if self
            .sync_matcher
            .offer(
                data.clone(),
                &group,
                source_domain,
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
        data: TaskDataBytes,
        source_domain: Uuid,
        schedule_to_start_seconds: i32,
    ) -> Result<AllocatedTaskBytes, Status> {
        self.liveness.mark_alive(registry_now());
        self.writer
            .append(PendingTaskBytes {
                data,
                source_domain,
                schedule_to_start_seconds,
            })
            .await
    }
    pub async fn append(
        &self,
        tasks: Vec<TaskDataBytes>,
        now: i64,
    ) -> Result<Vec<AllocatedTaskBytes>, Status> {
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
        tasks: Vec<TaskDataBytes>,
        source_domain: Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTaskBytes>, Status> {
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
    /// Go `LoadBalancerHints`: `(backlog_count, rate_per_second)` once 10s old.
    #[must_use]
    pub fn load_balancer_hints(&self, now: i64) -> Option<(i64, f64)> {
        if now.saturating_sub(self.created) < 10_000_000_000 {
            return None;
        }
        let (_, _, backlog, ..) = self.queue.status();
        Some((backlog, self.qps.qps()))
    }
    /// Go `HasPollerAfter`.
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
    operation: impl FnOnce(&mut Vec<NativeTaskReader>) -> Result<T, Status> + Send + 'static,
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

fn task_type_tag(task_type: i32) -> &'static str {
    if task_type == 1 {
        "activity_task"
    } else {
        "decision_task"
    }
}

impl NativeQueueRegistry {
    pub fn new(
        store: Arc<dyn MatchingTaskStoreBytes>,
        history: MatchingHistoryClient,
        range_size: NonZeroU32,
    ) -> Self {
        Self {
            store,
            history,
            range_size,
            state: Arc::new(Mutex::new(State::default())),
            query_index: Arc::default(),
            changed: Arc::new(tokio::sync::Notify::new()),
            startups: Arc::new(Mutex::new(HashMap::new())),
            ownership_policy: Arc::new(RwLock::new(None)),
            admission: Arc::new(RwLock::new(None)),
            parent_router: None,
            shard_router: Arc::default(),
            standby_enabled: None,
            membership: None,
            local_poll_service: None,
            partition_refresher: None,
            scaler_starter: None,
            domain_hooks: None,
            poll_observer: None,
            partition_count: None,
            cache_enabled: None,
            sync_wait: None,
            local_poll_wait: None,
            forward_degree: None,
            forward_polls: None,
            forward_tasks: None,
            forward_rate: None,
            qps_interval: None,
            qps_clock: cadence_common::qps::system_clock(),
            isolation: None,
            reader_live: None,
            min_burst: None,
            update_ack_interval: None,
            idle_tasklist_check_interval: None,
            max_task_delete_batch_size: None,
            writer_live: None,
            fatal: NativeFatalSinks::default(),
            writer_tracker: Arc::new(crate::matching_writer::WriterTracker::default()),
            metrics: matching_metrics::MatchingMetrics::default(),
        }
    }
    #[allow(dead_code)] // Used when the native registry is wired into the service.
    pub(crate) fn with_metrics(mut self, metrics: matching_metrics::MatchingMetrics) -> Self {
        self.metrics = metrics;
        self
    }
    pub fn with_fatal_sinks(mut self, fatal: NativeFatalSinks) -> Self {
        self.fatal = fatal;
        self
    }
    pub async fn wait_for_writers(&self) {
        self.writer_tracker.wait_idle().await;
    }
    /// Shared by all registry clones and native engine contexts. Publish the
    /// discovered router before admitting requests; readers release the lock
    /// before any asynchronous owner lookup.
    pub fn set_shard_router(
        &self,
        router: Arc<crate::matching_shard_routing::MatchingShardRouter>,
    ) {
        *self.shard_router.write().expect("native shard router lock") = Some(router);
    }
    pub fn shard_router(&self) -> Option<Arc<crate::matching_shard_routing::MatchingShardRouter>> {
        self.shard_router
            .read()
            .expect("native shard router lock")
            .clone()
    }
    pub fn with_local_poll_service(mut self, factory: NativeLocalPollFactory) -> Self {
        self.local_poll_service = Some(factory);
        self
    }
    /// Attach the current ring/router and a local poll-service constructor
    /// without making registry owners retain the service (and its registry).
    /// `build_local` is called per forwarded RPC after weak state upgrade.
    pub fn with_live_routing(
        self,
        membership: crate::matching_membership::Membership,
        router: Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>,
        build_local: NativeLocalPollServiceBuilder,
        refresh: NativePartitionRefresher,
    ) -> Self {
        let factory: NativeLocalPollFactory =
            Arc::new(move |host| host.registry().map(|registry| build_local(registry)));
        self.with_live_routing_factory(membership, router, factory, refresh)
    }

    /// Install routing with a fallible local factory, for services published
    /// after host construction. The factory receives a weak registry capability.
    pub fn with_live_routing_factory(
        self,
        membership: crate::matching_membership::Membership,
        router: Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>,
        factory: NativeLocalPollFactory,
        refresh: NativePartitionRefresher,
    ) -> Self {
        if let Some(router) = router {
            self.set_shard_router(router);
        }
        let parent_membership = membership.clone();
        let parent_router = self.shard_router.clone();
        let parent: NativeParentRouter = Arc::new(move |name| {
            let membership = parent_membership.clone();
            let name = name.clone();
            let router = parent_router
                .read()
                .expect("native shard router lock")
                .clone();
            Box::pin(async move {
                let channel = if let Some(router) = router {
                    router
                        .remote_channel_bytes(name.as_bytes())
                        .await
                        .map_err(|error| Status::unavailable(error.to_string()))?
                        .0
                } else {
                    membership.remote_channel_bytes(name.as_bytes())?
                };
                Ok(channel.map(|channel| {
                    Arc::new(crate::matching_forward_native::RemoteNativeMatching(
                        channel,
                    )) as Arc<dyn NativeMatchingApi>
                }))
            })
        });
        let local_membership = membership.native_membership();
        self.with_membership(local_membership)
            .with_parent_router(parent)
            .with_local_poll_service(factory)
            .with_partition_refresher(refresh)
    }
    pub fn with_parent_router(mut self, router: NativeParentRouter) -> Self {
        self.parent_router = Some(router);
        self
    }
    pub fn with_membership(mut self, membership: NativeMembership) -> Self {
        self.membership = Some(membership);
        self
    }
    #[must_use]
    pub fn has_membership(&self) -> bool {
        self.membership.is_some()
    }
    pub fn with_partition_refresher(mut self, refresher: NativePartitionRefresher) -> Self {
        self.partition_refresher = Some(refresher);
        self
    }
    pub fn with_scaler_starter(mut self, starter: NativeScalerStarter) -> Self {
        self.scaler_starter = Some(starter);
        self
    }
    pub fn with_domain_hooks(mut self, hooks: NativeDomainHooks) -> Self {
        self.domain_hooks = Some(hooks);
        self
    }
    pub fn with_poll_observer(mut self, provider: NativePollObserverProvider) -> Self {
        self.poll_observer = Some(provider);
        self
    }
    pub fn with_standby_enabled(mut self, enabled: EnabledNative) -> Self {
        self.standby_enabled = Some(enabled);
        self
    }
    /// Bind the shard-distributor ownership policy after executor construction.
    /// Clones share this slot, so runtime wiring also affects internal readers.
    pub fn set_ownership_policy(&self, policy: NativeQueueOwnershipPolicy) {
        *self
            .ownership_policy
            .write()
            .expect("Matching ownership policy lock") = Some(policy);
    }
    /// Install async shard admission on every clone before opening listeners.
    pub fn set_shard_admission(&self, check: crate::matching_runtime::NativeShardOwnershipCheck) {
        *self.admission.write().expect("Matching admission lock") = Some(check);
    }
    pub fn with_partition_count(mut self, provider: NativeProvider<i64>) -> Self {
        self.partition_count = Some(provider);
        self
    }
    pub fn with_partition_cache_enabled(mut self, provider: NativeProvider<bool>) -> Self {
        self.cache_enabled = Some(provider);
        self
    }
    pub fn with_sync_wait(mut self, provider: NativeProvider<i64>) -> Self {
        self.sync_wait = Some(provider);
        self
    }
    pub fn with_local_poll_wait(mut self, provider: NativeProvider<i64>) -> Self {
        self.local_poll_wait = Some(provider);
        self
    }
    pub fn with_forward_degree(mut self, provider: NativeProvider<i64>) -> Self {
        self.forward_degree = Some(provider);
        self
    }
    pub fn with_forward_polls(mut self, provider: NativeProvider<i64>) -> Self {
        self.forward_polls = Some(provider);
        self
    }
    pub fn with_forward_tasks(
        mut self,
        tasks: NativeProvider<i64>,
        rate: NativeProvider<i64>,
    ) -> Self {
        self.forward_tasks = Some(tasks);
        self.forward_rate = Some(rate);
        self
    }
    pub fn with_qps_interval(mut self, provider: NativeProvider<i64>) -> Self {
        self.qps_interval = Some(provider);
        self
    }
    pub fn with_isolation(mut self, provider: NativeIsolationProvider) -> Self {
        self.isolation = Some(provider);
        self
    }
    pub fn with_reader_live(mut self, provider: NativeReaderPropertiesProvider) -> Self {
        self.reader_live = Some(provider);
        self
    }
    pub fn with_min_burst(mut self, provider: NativeProvider<i64>) -> Self {
        self.min_burst = Some(provider);
        self
    }
    pub fn with_record_started_timeout(
        mut self,
        provider: crate::matching_history::RecordStartedTimeout,
    ) -> Self {
        self.history = self.history.clone().with_record_started_timeout(provider);
        self
    }
    pub fn with_update_ack_interval(mut self, provider: NativeProvider<i64>) -> Self {
        self.update_ack_interval = Some(provider);
        self
    }
    pub fn with_idle_tasklist_check_interval(mut self, provider: NativeProvider<i64>) -> Self {
        self.idle_tasklist_check_interval = Some(provider);
        self
    }
    pub fn with_max_task_delete_batch_size(mut self, provider: NativeProvider<i64>) -> Self {
        self.max_task_delete_batch_size = Some(provider);
        self
    }
    pub fn with_writer_live(mut self, provider: NativeWriterLiveProvider) -> Self {
        self.writer_live = Some(provider);
        self
    }

    /// Clone of the registry with an empty state plus a weak handle on the real
    /// state: owners live in `state`, so retaining that `Arc` from inside an
    /// owner would form an owner/registry cycle.
    fn detached(&self) -> (Self, Weak<Mutex<State>>) {
        let mut detached = self.clone();
        detached.state = Arc::new(Mutex::new(State::default()));
        (detached, Arc::downgrade(&self.state))
    }

    fn metric_domain(&self, domain: Uuid) -> String {
        self.domain_hooks
            .as_ref()
            .and_then(|hooks| (hooks.domain_name)(&gs(domain)))
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "_unknown_".to_owned())
    }
    fn metric_context(&self, key: &Key, kind: i16) -> matching_metrics::PollMetricContext {
        matching_metrics::PollMetricContext {
            domain: self.metric_domain(key.0),
            task_list: key.1.go_rune_string(),
            task_list_kind: i32::from(kind),
            task_type: task_type_tag(key.2),
            poller_isolation_group: String::new(),
        }
    }

    /// Advisory: this exact queue is currently reusable. Callers must still
    /// perform their normal ownership check and call `get`.
    pub fn contains_cached(&self, key: &TaskListKeyBytes) -> bool {
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let state = self.state.lock().expect("Matching registry lock");
        state
            .owners
            .get(&lookup)
            .is_some_and(|owner| !owner.queue.is_stopped())
    }

    /// Go `deliverQueryResult`: keyed by task ID alone and still works after
    /// the owning queue was unloaded or replaced.
    pub fn deliver_query_result(
        &self,
        task_id: &GoString,
        outcome: QueryOutcome,
    ) -> Result<(), Status> {
        self.query_index.deliver(task_id, outcome)
    }

    fn failover_eligible(kind: i16, task_type: i32) -> bool {
        (kind == 0 && matches!(task_type, 0 | 1)) || (kind == 1 && task_type == 0)
    }

    /// Go's domain failover callback releases blocked normal decision/activity
    /// pollers and sticky decision pollers. Owners stay installed.
    pub fn release_blocked_pollers_for_domain(&self, domain: Uuid) -> usize {
        let state = self.state.lock().expect("Matching registry lock");
        let mut released = 0;
        for ((id, _, task_type), owner) in &state.owners {
            if *id == domain && Self::failover_eligible(owner.queue.task_list_kind(), *task_type) {
                owner.dispatcher.release_blocked_pollers();
                released += 1;
            }
        }
        released
    }

    /// Go `getTaskListsByDomainAndKind` + `disconnectTaskListPollersAfterDomainFailover`.
    pub async fn release_domain_failover_polls(&self, domain: Uuid) {
        let roots = {
            let state = self.state.lock().expect("Matching registry lock");
            let mut roots = std::collections::BTreeSet::new();
            for ((id, name, task_type), owner) in &state.owners {
                if *id != domain {
                    continue;
                }
                let kind = owner.queue.task_list_kind();
                if !Self::failover_eligible(kind, *task_type) {
                    continue;
                }
                if let Ok(parsed) = TaskListNameBytes::parse(name.as_bytes()) {
                    roots.insert((parsed.root().clone(), *task_type, kind));
                }
            }
            roots
        };
        for (name, task_type, kind) in roots {
            let key = TaskListKeyBytes {
                domain_id: domain,
                name,
                task_type,
            };
            match self.get(key, kind, registry_now()).await {
                Ok(owner) => owner.dispatcher.release_blocked_pollers(),
                Err(error) => eprintln!(
                    "warning: loading Matching manager after domain failover failed: {error}"
                ),
            }
        }
    }

    /// Sum current QPS for loaded owners with this exact task-list name bytes.
    pub fn qps_for_task_list_name(&self, name: &GoString) -> f64 {
        let owners = {
            let state = self.state.lock().expect("Matching registry lock");
            state
                .owners
                .iter()
                .filter(|((_, task_list, _), owner)| task_list == name && !owner.queue.is_stopped())
                .map(|(_, owner)| owner.qps.clone())
                .collect::<Vec<_>>()
        };
        owners.iter().map(|qps| qps.qps()).sum()
    }

    #[cfg(test)]
    pub(crate) fn loaded_owner_count(&self) -> usize {
        self.state
            .lock()
            .expect("Matching registry lock")
            .owners
            .len()
    }

    /// Stop all loaded owners for one exact task-list name so a later Get rebuilds them.
    pub async fn stop_task_list_name(&self, name: &GoString) -> Result<(), Status> {
        let registry = self.clone();
        let name = name.clone();
        owner_work(move |readers| {
            let owners = {
                let mut state = registry.state.lock().expect("Matching registry lock");
                let keys = state
                    .owners
                    .keys()
                    .filter(|(_, task_list, _)| task_list == &name)
                    .cloned()
                    .collect::<Vec<_>>();
                keys.into_iter()
                    .filter_map(|key| state.owners.remove(&key))
                    .collect::<Vec<_>>()
            };
            let now = registry_now();
            let mut first_error = None;
            for owner in owners {
                if let Err(error) = owner.finish(now, readers) {
                    first_error.get_or_insert(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        })
        .await
    }

    /// Snapshot loaded managers only, like Go ManagersByDomainID. Neither
    /// acquires leases nor consults frontend workflow state.
    pub fn describe_by_domain(
        &self,
        domain: Uuid,
        all_kinds: bool,
        now: i64,
    ) -> NativeTaskListsByDomain {
        let state = self.state.lock().expect("Matching registry lock");
        let mut response = NativeTaskListsByDomain::default();
        for ((id, name, task_type), owner) in &state.owners {
            let kind = owner.queue.task_list_kind();
            if *id != domain || (!all_kinds && kind != 0) {
                continue;
            }
            let Ok(parsed) = TaskListNameBytes::parse(name.as_bytes()) else {
                continue;
            };
            let described = NativeLoadedTaskList {
                name: name.clone(),
                root: parsed.root().clone(),
                task_type: *task_type,
                kind,
                pollers: owner.pollers.lock().expect("poller history lock").list(now),
                partition_config: owner.partitions.get(),
            };
            if *task_type == 0 {
                response.decision.push(described);
            } else {
                response.activity.push(described);
            }
        }
        response
    }

    pub(crate) fn check_task_list_ownership(
        &self,
        name: &GoString,
        cached: bool,
    ) -> Result<(), Status> {
        let policy = self
            .ownership_policy
            .read()
            .expect("Matching ownership policy lock")
            .clone();
        let Some(policy) = policy else {
            return self
                .membership
                .as_ref()
                .map_or(Ok(()), |membership| (membership.check_task_list)(name));
        };
        if (policy.is_shard_distributor_eligible)(name) {
            if (policy.has_local_processor)(name) {
                return Ok(());
            }
            return Err(self.membership.as_ref().map_or_else(
                || Status::internal("Matching membership is not configured."),
                |membership| (membership.task_list_not_owned_by)(name, "not known"),
            ));
        }
        if cached {
            return Ok(());
        }
        self.membership
            .as_ref()
            .map_or(Ok(()), |membership| (membership.check_task_list)(name))
    }

    /// Like Go's membership watcher, detach obsolete owners before doing I/O.
    /// Unknown placement leaves existing readers intact until membership recovers.
    /// Returns the task-list names Go's `getNonOwnedTasklistsLocked` would return.
    pub async fn unload_non_owned(&self, now: i64) -> Result<Vec<GoString>, Status> {
        let Some(membership) = self.membership.clone() else {
            return Ok(Vec::new());
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
                            || (membership.owns)(&key.1),
                            |policy| Ok((policy.has_local_processor)(&key.1)),
                        )?;
                    if !owned {
                        candidates.push((key.clone(), owner.clone()));
                    }
                }
                candidates
            };
            let mut failure = None;
            let mut names = Vec::with_capacity(candidates.len());
            for (key, owner) in candidates {
                names.push(key.1.clone());
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(names), Err)
        })
        .await
    }

    /// task_type is the persistence enum (decision 0, activity 1).
    pub fn get(
        &self,
        key: TaskListKeyBytes,
        kind: i16,
        now: i64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<NativeQueueHandle, Status>> + Send + '_>,
    > {
        self.get_with_deadline(key, kind, now, None)
    }

    /// Bound caller admission and the wait for startup without cancelling an
    /// already dispatched manager startup, which uses a detached Go context.
    pub fn get_with_deadline(
        &self,
        key: TaskListKeyBytes,
        kind: i16,
        now: i64,
        deadline: Option<tokio::time::Instant>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<NativeQueueHandle, Status>> + Send + '_>,
    > {
        Box::pin(async move {
            let lookup = async move {
                if !(0..=1).contains(&key.task_type) {
                    return Err(Status::invalid_argument("Unknown persistence task type."));
                }
                let check = self
                    .admission
                    .read()
                    .expect("Matching admission lock")
                    .clone();
                if let Some(check) = check {
                    let cached = self.contains_cached(&key);
                    check(key.name.clone(), cached).await?;
                }
                // Do not dispatch startup if admission completed after expiry.
                if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                    return Err(Status::internal("context deadline exceeded"));
                }
                let registry = self.clone();
                owner_work(move |readers| registry.get_blocking(key, kind, now, readers)).await
            };
            match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, lookup)
                    .await
                    .unwrap_or_else(|_| Err(Status::internal("context deadline exceeded"))),
                None => lookup.await,
            }
        })
    }

    fn handle_of(owner: &Owner) -> NativeQueueHandle {
        NativeQueueHandle {
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
        }
    }

    fn get_blocking(
        &self,
        key: TaskListKeyBytes,
        kind: i16,
        now: i64,
        readers: &mut Vec<NativeTaskReader>,
    ) -> Result<NativeQueueHandle, Status> {
        let registry = self;
        let lookup: Key = (key.domain_id, key.name.clone(), key.task_type);
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
                registry
                    .metrics
                    .task_list_managers_gauge(state.owners.len());
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
        if let Some(owner) = state.owners.get(&lookup) {
            return Ok(Self::handle_of(owner));
        }
        drop(state);
        let owner = registry.build_owner(&key, &lookup, kind, now)?;

        let mut state = registry.state.lock().expect("Matching registry lock");
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
        // Reconcile publication with concurrent root updates. A nonroot
        // startup may have read SQL before the root wrote and published.
        if kind == 0 {
            let name = TaskListNameBytes::parse(lookup.1.as_bytes()).map_err(invalid_name)?;
            if name.is_root() {
                if let Some(config) = owner.partitions.get() {
                    for ((domain, task_list, task_type), other) in &state.owners {
                        if *domain == lookup.0
                            && *task_type == lookup.2
                            && TaskListNameBytes::parse(task_list.as_bytes())
                                .is_ok_and(|other| other.root() == name.root())
                        {
                            other.partitions.push(&config);
                        }
                    }
                }
            } else if let Some(root) = state.owners.get(&(lookup.0, name.root().clone(), lookup.2))
            {
                if let Some(config) = root.partitions.get() {
                    owner.partitions.push(&config);
                }
            }
        }
        // Go `taskListManagerImpl.Start` pushes the loaded root config to every
        // non-root read partition in the background, bounded by
        // `notifyPartitionConfigTimeout` (5s).
        if kind == 0 {
            if let (Some(config), Ok(parsed)) = (
                owner.partitions.get(),
                TaskListNameBytes::parse(key.name.as_bytes()),
            ) {
                if parsed.is_root() {
                    let notifier = registry.clone();
                    let root_name = parsed.root().clone();
                    let read_partitions = config.read_partitions.keys().copied().collect();
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                    let domain_id = key.domain_id;
                    let task_type = key.task_type;
                    tokio::spawn(async move {
                        notifier
                            .notify_read_partitions(
                                domain_id,
                                &root_name,
                                kind,
                                task_type,
                                read_partitions,
                                config,
                                Some(deadline),
                            )
                            .await;
                    });
                }
            }
        }
        // Go starts the adaptive scaler on root normal managers; the loop
        // self-terminates when the owner stops.
        if kind == 0 {
            if let Some(starter) = registry.scaler_starter.clone() {
                if TaskListNameBytes::parse(key.name.as_bytes())
                    .is_ok_and(|parsed| parsed.is_root())
                {
                    let (detached, weak) = registry.detached();
                    let host = NativeScalerHost {
                        detached,
                        state: weak,
                        root: key.clone(),
                    };
                    owner.scaler.start(starter(host));
                }
            }
        }
        state.owners.insert(lookup, owner.clone());
        registry
            .metrics
            .task_list_managers_gauge(state.owners.len());
        registry.changed.notify_waiters();
        Ok(Self::handle_of(&owner))
    }

    fn provider<T: Default + 'static>(
        provider: &Option<NativeProvider<T>>,
        domain: &GoString,
        name: &GoString,
        task_type: i32,
        default: T,
    ) -> T {
        provider
            .as_ref()
            .map_or(default, |p| p(domain, name, task_type))
    }

    fn scoped_count(
        provider: &Option<NativeProvider<i64>>,
        domain: GoString,
        name: GoString,
        task_type: i32,
        default: i64,
    ) -> crate::matching_forward::Count {
        let provider = provider.clone();
        Arc::new(move || {
            provider
                .as_ref()
                .map_or(default, |p| p(&domain, &name, task_type))
        })
    }

    /// Everything Go's `NewManager` + `Start` construct before publication.
    fn build_owner(
        &self,
        key: &TaskListKeyBytes,
        lookup: &Key,
        kind: i16,
        now: i64,
    ) -> Result<Arc<Owner>, Status> {
        let registry = self;
        let domain = gs(key.domain_id);
        let metric_context = registry.metric_context(lookup, kind);
        registry.metrics.lease_request(&metric_context);
        let queue = match DurableTaskQueueBytes::open(
            registry.store.clone(),
            key.clone(),
            kind,
            registry.range_size,
            now,
        ) {
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
                provider
                    .as_ref()
                    .map_or(100, |p| p(&domain, &name, task_type))
            }));
        }
        let partitions =
            match NativePartitionCache::load(registry.store.as_ref(), &queue, key, kind) {
                Ok(cache) => Arc::new(cache),
                Err(error) => {
                    queue.stop();
                    return Err(error.to_native_status());
                }
            };
        let count_partitions: crate::matching_limiter::PartitionCount = {
            let provider = registry.partition_count.clone();
            let enabled = registry.cache_enabled.clone();
            let cache = partitions.clone();
            let domain = domain.clone();
            let task_list = key.name.clone();
            let task_type = key.task_type;
            Arc::new(move || {
                let root =
                    TaskListNameBytes::parse(task_list.as_bytes()).expect("validated queue name");
                if enabled
                    .as_ref()
                    .is_some_and(|enabled| enabled(&domain, root.root(), task_type))
                {
                    return cache
                        .get()
                        .map(|config| config.read_partitions.len() as i64)
                        .unwrap_or(1);
                }
                provider
                    .as_ref()
                    .map(|provider| provider(&domain, &task_list, task_type))
                    .unwrap_or(1)
                    .max(1)
            })
        };
        let idle_ttl_nanos = Self::provider(
            &registry.idle_tasklist_check_interval,
            &domain,
            &lookup.1,
            lookup.2,
            300_000_000_000,
        )
        .max(1);
        let liveness = Arc::new(cadence_common::liveness::Liveness::new(
            registry_now(),
            idle_ttl_nanos,
        ));
        let next_ack_update = registry.update_ack_interval.as_ref().map(|provider| {
            let nanos = provider(&domain, &lookup.1, lookup.2).max(1) as u64;
            tokio::time::Instant::now() + Duration::from_nanos(nanos)
        });
        // Go `stats.NewEmaFixedWindowQPSTracker(timeSource, 0.5,
        // cfg.QPSTrackerInterval(domain, taskList, type), ...)`.
        let qps = Arc::new(cadence_common::qps_bytes::EmaQpsBytesTracker::new(
            QPS_TRACKER_EXP,
            Duration::from_nanos(
                Self::provider(
                    &registry.qps_interval,
                    &domain,
                    &lookup.1,
                    lookup.2,
                    DEFAULT_QPS_TRACKER_INTERVAL_NANOS,
                )
                .max(1) as u64,
            ),
            registry.qps_clock.clone(),
        ));
        let activity: Arc<dyn Fn() -> bool + Send + Sync> = {
            let hooks = registry.domain_hooks.clone();
            let domain = domain.clone();
            Arc::new(move || {
                hooks
                    .as_ref()
                    .is_none_or(|hooks| (hooks.is_active)(&domain))
            })
        };
        let dispatcher =
            NativeTaskDispatcher::new(queue.clone(), registry.history.clone(), domain.clone())
                .with_liveness(liveness.clone());
        let writer_live: Option<NativeWriterLiveConfig> =
            registry.writer_live.as_ref().map(|provider| {
                let provider = provider.clone();
                let (domain, task_list, task_type) = (domain.clone(), lookup.1.clone(), lookup.2);
                Arc::new(move || provider(&domain, &task_list, task_type)) as NativeWriterLiveConfig
            });
        let writer = NativeTaskWriter::new_with_fatal_sink(
            queue.clone(),
            dispatcher.task_notification(),
            crate::matching_writer::WriterConfig::default(),
            writer_live,
            registry.fatal.writer.clone(),
            Some(registry.writer_tracker.clone()),
        );
        let min_burst = Self::provider(
            &registry.min_burst,
            &domain,
            &lookup.1,
            lookup.2,
            crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE,
        );
        let limiter = Arc::new(
            crate::matching_limiter::TaskListLimiter::with_partitions_and_burst(
                count_partitions,
                min_burst,
            ),
        );
        let pollers: Arc<NativePollerManager> = Arc::default();
        let isolation = registry
            .isolation
            .as_ref()
            .map(|provider| {
                provider(
                    &domain,
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
        let sync_wait: Arc<dyn Fn() -> Duration + Send + Sync> = {
            let provider = registry.sync_wait.clone();
            let (domain, name, task_type) = (domain.clone(), lookup.1.clone(), lookup.2);
            Arc::new(move || {
                Duration::from_nanos(
                    provider
                        .as_ref()
                        .map_or(10_000_000, |provider| provider(&domain, &name, task_type))
                        .max(0) as u64,
                )
            })
        };
        let reader_config = ReaderConfig {
            live: registry
                .reader_live
                .as_ref()
                .map(|provider| Arc::new(provider(&domain, &lookup.1, lookup.2))),
            ..ReaderConfig::default()
        };
        let forward_tasks = Arc::new(crate::matching_forward::RequestTokens::new(
            Self::scoped_count(
                &registry.forward_tasks,
                domain.clone(),
                lookup.1.clone(),
                lookup.2,
                1,
            ),
        ));
        let forward_rate = Arc::new(crate::matching_forward::ForwardRate::new(
            Self::scoped_count(
                &registry.forward_rate,
                domain.clone(),
                lookup.1.clone(),
                lookup.2,
                10,
            ),
            now,
        ));
        // Go `forwarder` refuses sticky task lists; only a real child partition
        // forwards polls to its parent.
        let name = TaskListNameBytes::parse(lookup.1.as_bytes()).map_err(invalid_name)?;
        let forward = if kind == 1 || name.is_root() {
            None
        } else {
            Some(registry.forward_state(
                lookup,
                kind,
                &domain,
                forward_tasks.clone(),
                forward_rate.clone(),
            ))
        };
        let backlog_forward = forward.clone().map(|forward_state| {
            let forward_domain = domain.clone();
            let (forward_task_type, forward_kind) = (lookup.2, kind);
            let local_wait = Self::scoped_count(
                &registry.local_poll_wait,
                domain.clone(),
                lookup.1.clone(),
                lookup.2,
                10_000_000,
            );
            NativeBacklogForward {
                tokens: forward_state.add_tokens(),
                forward: Arc::new(move |task| {
                    let forward_state = forward_state.clone();
                    let domain = forward_domain.clone();
                    Box::pin(async move {
                        forward_state
                            .forward_backlog_task(
                                task,
                                &domain,
                                &domain,
                                forward_task_type,
                                forward_kind,
                            )
                            .await
                    })
                }),
                local_wait: Arc::new(move || Duration::from_nanos(local_wait().max(0) as u64)),
            }
        });
        let reader_fatal = registry.fatal.reader.clone();
        let dispatch_check: Option<NativeDispatchCheck> =
            registry.domain_hooks.as_ref().map(|hooks| {
                checker_native(
                    hooks.standby_domains.clone(),
                    registry.history.clone(),
                    key.clone(),
                    registry.standby_enabled.clone(),
                    registry.metrics.clone(),
                    registry.metric_context(lookup, kind),
                )
            });
        let reader = NativeTaskReader::with_fatal_invariant_sink(
            queue.clone(),
            dispatcher.task_notification(),
            reader_config,
            activity,
            limiter.clone(),
            isolation,
            dispatch_check,
            Some((
                registry.metrics.clone(),
                registry.metric_context(lookup, kind),
            )),
            backlog_forward,
            reader_fatal,
        );
        let read_stop = reader.stop_signal();
        let sync_matcher = Arc::new(NativeSyncMatcher::new(&sync_groups, read_stop.subscribe()));
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
        let idle_half = Duration::from_nanos((idle_ttl_nanos / 2).max(1) as u64);
        Ok(Arc::new(Owner {
            forward_tasks,
            forward_rate,
            dispatcher,
            writer,
            reader,
            limiter,
            queue,
            pollers,
            queries: Arc::new(QueryChannel::with_index(registry.query_index.clone())),
            qps,
            partitions,
            sync_matcher,
            isolation_select,
            sync_wait,
            created: now,
            idle_ttl_nanos,
            last_update: Mutex::new(tokio::time::Instant::now()),
            next_ack_update: Mutex::new(next_ack_update),
            next_idle_check: Mutex::new(tokio::time::Instant::now() + idle_half),
            liveness,
            scaler: Default::default(),
        }))
    }

    fn forward_state(
        &self,
        lookup: &Key,
        kind: i16,
        domain: &GoString,
        forward_tasks: Arc<crate::matching_forward::RequestTokens>,
        forward_rate: Arc<crate::matching_forward::ForwardRate>,
    ) -> crate::matching_dispatch_native::NativeForwardState {
        let max_outstanding = Self::scoped_count(
            &self.forward_polls,
            domain.clone(),
            lookup.1.clone(),
            lookup.2,
            1,
        );
        let local_wait = Self::scoped_count(
            &self.local_poll_wait,
            domain.clone(),
            lookup.1.clone(),
            lookup.2,
            10_000_000,
        );
        let child = lookup.1.clone();
        let task_type = lookup.2;
        let domain_id = lookup.0;
        let degree = Self::scoped_count(
            &self.forward_degree,
            domain.clone(),
            child.clone(),
            task_type,
            20,
        );
        // Owners live in registry.state; retaining that same Arc here would
        // form an owner/registry cycle.
        let (detached, weak) = self.detached();
        let resolve_child = child.clone();
        let mut state = crate::matching_dispatch_native::NativeForwardState::new(
            move || {
                let mut resolve_registry = detached.clone();
                let live = weak.upgrade();
                let child = resolve_child.clone();
                let degree = degree();
                Box::pin(async move {
                    resolve_registry.state = live?;
                    let parent = TaskListNameBytes::parse(child.as_bytes())
                        .ok()?
                        .parent(degree);
                    if parent.is_empty() {
                        return None;
                    }
                    let parent_key = TaskListKeyBytes {
                        domain_id,
                        name: parent.clone(),
                        task_type,
                    };
                    resolve_parent_dispatcher(resolve_registry, parent_key, kind)
                        .await
                        .map(|dispatcher| (dispatcher, parent))
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = Option<(NativeParent, GoString)>>
                                + Send,
                        >,
                    >
            },
            max_outstanding,
            forward_tasks,
            forward_rate,
            child,
            local_wait,
        );
        if let Some(provider) = &self.poll_observer {
            state = state.with_observer(provider(&self.metric_context(lookup, kind)));
        }
        state
    }

    pub async fn update_partition_config(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: NativeStoredPartitionConfig,
        now: i64,
    ) -> Result<(), Status> {
        self.update_partition_config_with_deadline(key, handle, config, now, None)
            .await
    }
    pub async fn update_partition_config_with_deadline(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: NativeStoredPartitionConfig,
        now: i64,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Status> {
        self.update_partition_config_inner(key, handle, config, now, caller_deadline, false, None)
            .await
    }
    /// Like `_with_deadline`, with the metrics domain name already resolved by the RPC layer.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_partition_config_for_domain(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: NativeStoredPartitionConfig,
        now: i64,
        caller_deadline: Option<tokio::time::Instant>,
        metric_domain: String,
    ) -> Result<(), Status> {
        self.update_partition_config_inner(
            key,
            handle,
            config,
            now,
            caller_deadline,
            false,
            Some(metric_domain),
        )
        .await
    }
    /// Scaler entry point: Go's scaler passes its own `a.ctx`, which ends when the manager stops.
    pub async fn update_partition_config_as_scaler(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: NativeStoredPartitionConfig,
        now: i64,
    ) -> Result<(), Status> {
        self.update_partition_config_inner(key, handle, config, now, None, true, None)
            .await
    }

    /// `scaler_ctx`: the caller context is the manager's own, cancelled when
    /// the queue stops; manual updates observe deadline and caller cancellation.
    #[allow(clippy::too_many_arguments)]
    async fn update_partition_config_inner(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: NativeStoredPartitionConfig,
        now: i64,
        caller_deadline: Option<tokio::time::Instant>,
        scaler_ctx: bool,
        metric_domain: Option<String>,
    ) -> Result<(), Status> {
        let registry = self.clone();
        let domain_id = key.domain_id;
        let task_type = key.task_type;
        let task_list_kind = handle.queue.task_list_kind();
        let caller_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _cancel_on_drop = PartitionUpdateCancellation(caller_cancelled.clone());
        let notification = tokio::task::spawn_blocking(move || {
            // The closure outlives a dropped caller future, so retry cut-off and
            // the failure counter happen here, like Go's `throttleRetry.Do(ctx, ...)`.
            let deadline = caller_deadline.map(tokio::time::Instant::into_std);
            let cancelled = || {
                caller_cancelled.load(std::sync::atomic::Ordering::Acquire)
                    || deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
                    || (scaler_ctx && handle.queue.is_stopped())
            };
            let on_failure = || {
                let mut context = registry.metric_context(
                    &(key.domain_id, key.name.clone(), key.task_type),
                    task_list_kind,
                );
                if let Some(domain) = metric_domain.clone() {
                    context.domain = domain;
                }
                registry
                    .metrics
                    .task_list_counter_event(matching_metrics::PARTITION_UPDATE_FAILED, &context);
            };
            let Some((old, new)) = handle
                .partitions
                .update_until(&handle.queue, config, now, cancelled, on_failure)
                .map_err(|error| error.to_native_status())?
            else {
                return Ok::<_, Status>(None);
            };
            // Pinned Go notifyPartitionConfig notifies the union of old/new
            // non-root READ partitions only.
            let mut notify = std::collections::BTreeSet::new();
            for config in old.iter().chain(std::iter::once(&new)) {
                notify.extend(config.read_partitions.keys().copied());
            }
            notify.remove(&0);
            let name = TaskListNameBytes::parse(key.name.as_bytes()).map_err(invalid_name)?;
            let state = registry.state.lock().expect("Matching registry lock");
            for ((domain, task_list, task_type), owner) in &state.owners {
                if *domain != key.domain_id || *task_type != key.task_type {
                    continue;
                }
                if let Ok(other) = TaskListNameBytes::parse(task_list.as_bytes()) {
                    if other.root() == name.root()
                        && !other.is_root()
                        && notify.contains(&(other.partition() as i32))
                    {
                        owner.partitions.push(&new);
                    }
                }
            }
            Ok(Some((
                new,
                name.root().clone(),
                notify.into_iter().collect::<Vec<_>>(),
            )))
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

    /// Go `notifyPartitionConfig`: refresh every non-root read partition
    /// through its live owner, concurrently. Failures are reported but never
    /// fail the caller, as in Go.
    #[allow(clippy::too_many_arguments)]
    async fn notify_read_partitions(
        &self,
        domain_id: Uuid,
        root_name: &GoString,
        task_list_kind: i16,
        task_type: i32,
        read_partitions: Vec<i32>,
        config: NativeStoredPartitionConfig,
        caller_deadline: Option<tokio::time::Instant>,
    ) {
        let Some(refresher) = self.partition_refresher.clone() else {
            return;
        };
        let mut notifications = tokio::task::JoinSet::new();
        for partition in read_partitions
            .into_iter()
            .filter(|partition| *partition != 0)
        {
            let mut task_list = b"/__cadence_sys/".to_vec();
            task_list.extend_from_slice(root_name.as_bytes());
            task_list.push(b'/');
            task_list.extend_from_slice(partition.to_string().as_bytes());
            let request = NativePartitionRefresh {
                domain_id,
                task_list: GoString::from(task_list),
                kind: task_list_kind,
                task_type,
                config: config.clone(),
                deadline: caller_deadline,
            };
            let refresher = refresher.clone();
            // Go runs each notification in an errgroup goroutine whose
            // `CapturePanic` turns a panic into that goroutine's error.
            notifications.spawn(async move {
                let name = request.task_list.clone();
                crate::matching_panic::capture("notify_partition_config", async move {
                    let call = refresher(request.clone());
                    let result = match request.deadline {
                        Some(deadline) => tokio::time::timeout_at(deadline, call)
                            .await
                            .unwrap_or_else(|_| {
                                Err(Status::deadline_exceeded("context deadline exceeded"))
                            }),
                        None => call.await,
                    };
                    result.map_err(|error| (request.task_list, error))
                })
                .await
                .map_err(|message| (name, message))
            });
        }
        while let Some(result) = notifications.join_next().await {
            match result {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err((name, error)))) => eprintln!(
                    "warning: refreshing Matching partition {} failed: {error}",
                    diag(name.as_bytes())
                ),
                Ok(Err((name, message))) => eprintln!(
                    "warning: notifying Matching partition {} failed: {message}",
                    diag(name.as_bytes())
                ),
                Err(error) => {
                    eprintln!("warning: Matching partition notification task failed: {error}")
                }
            }
        }
    }

    pub async fn refresh_partition_config(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: Option<NativeStoredPartitionConfig>,
    ) -> Result<(), Status> {
        self.refresh_partition_config_with_deadline(key, handle, config, None)
            .await
    }
    pub async fn refresh_partition_config_with_deadline(
        &self,
        mut key: TaskListKeyBytes,
        handle: NativeQueueHandle,
        config: Option<NativeStoredPartitionConfig>,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Status> {
        let store = self.store.clone();
        let caller_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _cancel_on_drop = PartitionUpdateCancellation(caller_cancelled.clone());
        tokio::task::spawn_blocking(move || {
            if let Some(config) = config {
                handle.partitions.push(&config);
                return Ok(());
            }
            key.name = TaskListNameBytes::parse(key.name.as_bytes())
                .map_err(invalid_name)?
                .root()
                .clone();
            let deadline = caller_deadline.map(tokio::time::Instant::into_std);
            handle
                .partitions
                .refresh_until(store.as_ref(), &key, || {
                    caller_cancelled.load(std::sync::atomic::Ordering::Acquire)
                        || deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
                })
                .map_err(|error| error.to_native_status())
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }

    /// An old manager's teardown must never unregister its replacement.
    pub async fn unload(
        &self,
        key: TaskListKeyBytes,
        handle: NativeQueueHandle,
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
            registry
                .metrics
                .task_list_managers_gauge(state.owners.len());
            drop(state);
            owner.finish(now, readers)?;
            Ok(true)
        })
        .await
    }
    /// Called by the service's configured ack timer.
    pub async fn persist_acks(&self, now: i64) -> Result<(), Status> {
        self.flush(now, false, None).await
    }
    /// All handles become unusable after shutdown. Persist every owner even if
    /// another owner fails, and return the first failure.
    pub async fn shutdown(&self, now: i64) -> Result<(), Status> {
        self.flush(now, true, None).await
    }
    /// Fixed-interval controller. Reset after each completed update, as Go's
    /// task reader does; a transient persistence error does not kill the pump.
    pub async fn maintain_until<F>(&self, interval: Duration, shutdown: F) -> Result<(), Status>
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
                        let interval = Duration::from_nanos((ttl / 2) as u64);
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
    async fn flush(&self, now: i64, stop: bool, due: Option<Duration>) -> Result<(), Status> {
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
                if let Err(error) = owner.queue.persist_ack(now) {
                    failure.get_or_insert_with(|| Status::unavailable(error.to_string()));
                }
                *owner.last_update.lock().expect("Matching ack timer lock") =
                    tokio::time::Instant::now();
                *owner
                    .next_ack_update
                    .lock()
                    .expect("Matching ack timer lock") =
                    registry.update_ack_interval.as_ref().map(|provider| {
                        let nanos = provider(&gs(key.0), &key.1, key.2).max(1) as u64;
                        tokio::time::Instant::now() + Duration::from_nanos(nanos)
                    });
                if owner.queue.is_stopped() && registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
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
    registry: NativeQueueRegistry,
    key: TaskListKeyBytes,
    kind: i16,
) -> Option<NativeParent> {
    if let Some(router) = registry.parent_router.clone() {
        match router(&key.name).await {
            Ok(Some(api)) => return Some(NativeParent::Service(api)),
            Ok(None) => {}
            Err(_) => return None,
        }
    }
    if let Some(factory) = registry.local_poll_service.clone() {
        let host = NativeLocalPollHost {
            detached: {
                let mut detached = registry.clone();
                detached.state = Arc::new(Mutex::new(State::default()));
                detached
            },
            state: Arc::downgrade(&registry.state),
        };
        return factory(host).map(NativeParent::Service);
    }
    let handle = registry.get(key, kind, registry_now()).await.ok()?;
    if handle.stop_if_isolation_changed() {
        return None;
    }
    Some(NativeParent::Dispatcher(Box::new(handle.dispatcher)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};

    #[derive(Default)]
    struct SlotPeer(std::sync::Mutex<Vec<u8>>);
    macro_rules! slot_peer {
        ($($method:ident: $request:ident => $response:ident),* $(,)?) => {
            #[tonic::async_trait]
            impl cadence_proto::native::NativeMatchingApi for SlotPeer {
                async fn add_decision_task(&self, request: tonic::Request<cadence_proto::native::matching::AddDecisionTaskRequest>) -> Result<tonic::Response<cadence_proto::native::matching::AddDecisionTaskResponse>, Status> {
                    *self.0.lock().unwrap() = request.into_inner().task_list.unwrap().name.as_bytes().to_vec();
                    Ok(tonic::Response::new(Default::default()))
                }
                $(async fn $method(&self, _: tonic::Request<cadence_proto::native::matching::$request>) -> Result<tonic::Response<cadence_proto::native::matching::$response>, Status> {
                    Ok(tonic::Response::new(Default::default()))
                })*
            }
        };
    }
    slot_peer! {
        poll_for_decision_task: PollForDecisionTaskRequest => PollForDecisionTaskResponse,
        poll_for_activity_task: PollForActivityTaskRequest => PollForActivityTaskResponse,
        add_activity_task: AddActivityTaskRequest => AddActivityTaskResponse,
        query_workflow: QueryWorkflowRequest => QueryWorkflowResponse,
        respond_query_task_completed: RespondQueryTaskCompletedRequest => RespondQueryTaskCompletedResponse,
        cancel_outstanding_poll: CancelOutstandingPollRequest => CancelOutstandingPollResponse,
        describe_task_list: DescribeTaskListRequest => DescribeTaskListResponse,
        list_task_list_partitions: ListTaskListPartitionsRequest => ListTaskListPartitionsResponse,
        get_task_lists_by_domain: GetTaskListsByDomainRequest => GetTaskListsByDomainResponse,
        update_task_list_partition_config: UpdateTaskListPartitionConfigRequest => UpdateTaskListPartitionConfigResponse,
        refresh_task_list_partition_config: RefreshTaskListPartitionConfigRequest => RefreshTaskListPartitionConfigResponse,
        acquire_semaphore: AcquireSemaphoreRequest => AcquireSemaphoreResponse,
    }

    #[tokio::test]
    async fn weak_service_slot_forwards_raw_local_parent_and_expires() {
        let (_dir, registry) = fixture();
        let slot = crate::matching_registry_routing_native::NativeServiceSlot::default();
        let factory = slot.factory();
        let host = NativeLocalPollHost {
            detached: {
                let mut detached = registry.clone();
                detached.state = Arc::new(Mutex::new(State::default()));
                detached
            },
            state: Arc::downgrade(&registry.state),
        };
        assert!(factory(host.clone()).is_none());
        let peer = Arc::new(SlotPeer::default());
        let service: Arc<dyn cadence_proto::native::NativeMatchingApi> = peer.clone();
        slot.publish(&service);
        let api = factory(host.clone()).unwrap();
        let raw = b"parent-\xff";
        api.add_decision_task(
            cadence_proto::native::matching::AddDecisionTaskRequest {
                task_list: Some(cadence_proto::native::api::TaskList {
                    name: GoString::from(&raw[..]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(&*peer.0.lock().unwrap(), raw);
        drop(api);
        drop(service);
        drop(peer);
        assert!(factory(host.clone()).is_none());
        let service: Arc<dyn cadence_proto::native::NativeMatchingApi> =
            Arc::new(SlotPeer::default());
        slot.publish(&service);
        drop(registry);
        assert!(factory(host).is_none());
    }

    fn fixture() -> (tempfile::TempDir, NativeQueueRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let registry = NativeQueueRegistry::new(store, history, NonZeroU32::new(100).unwrap());
        (dir, registry)
    }
    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn task(workflow: &[u8]) -> TaskDataBytes {
        TaskDataBytes {
            workflow_id: GoString::from(workflow),
            run_id: None,
            raw_run_id: None,
            schedule_id: 1,
            expiry_nanos: 0,
            created_nanos: 0,
            partition_config: None,
        }
    }

    #[tokio::test]
    async fn metric_context_keeps_full_name_and_go_invalid_byte_runes() {
        let (_directory, registry) = fixture();
        let mut raw = vec![b'x'; 300];
        raw.extend_from_slice(b"-\xe2\x82");
        let name = GoString::from(raw.clone());
        let context = registry.metric_context(&(Uuid::nil(), name.clone(), 0), 0);
        assert_eq!(context.task_list, format!("{}-��", "x".repeat(300)));
        assert_eq!(name.as_bytes(), raw);
    }

    #[test]
    fn invalid_partition_name_status_preserves_all_message_bytes() {
        let mut name = b"/__cadence_sys/".to_vec();
        name.extend_from_slice(&[b'x'; 300]);
        name.extend_from_slice(b"\xff/invalid");
        let message = TaskListNameBytes::parse(&name).unwrap_err();
        let status = invalid_name(message.clone());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.details().is_empty());
        let raw = status
            .metadata()
            .get_bin(crate::matching_native_status::RAW_MESSAGE_MARKER)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert_eq!(raw.as_ref(), message.as_bytes());
    }

    #[tokio::test]
    async fn registry_clones_observe_late_router_attachment_and_replacement() {
        let (_dir, registry) = fixture();
        let retained = registry.clone();
        assert!(retained.shard_router().is_none());
        let membership = crate::matching_membership::Membership::new("local", vec![]);
        let router = Arc::new(crate::matching_shard_routing::MatchingShardRouter::new(
            membership.clone(),
            None,
            Arc::new(|| 0),
            Arc::new(|| false),
        ));
        registry.set_shard_router(router.clone());
        assert!(Arc::ptr_eq(&retained.shard_router().unwrap(), &router));
        let replacement = Arc::new(crate::matching_shard_routing::MatchingShardRouter::new(
            membership,
            None,
            Arc::new(|| 100),
            Arc::new(|| false),
        ));
        retained.set_shard_router(replacement.clone());
        assert!(Arc::ptr_eq(&registry.shard_router().unwrap(), &replacement));
        registry.shutdown(registry_now()).await.unwrap();
    }

    #[tokio::test]
    async fn raw_ff_fe_and_replacement_queues_are_distinct_owners() {
        let (_dir, registry) = fixture();
        let now = registry_now();
        for name in [&b"\xff"[..], b"\xfe", "\u{fffd}".as_bytes()] {
            registry.get(key(name), 0, now).await.unwrap();
        }
        assert_eq!(registry.loaded_owner_count(), 3);
        let first = registry.get(key(b"\xff"), 0, now).await.unwrap();
        let again = registry.get(key(b"\xff"), 0, now).await.unwrap();
        assert!(Arc::ptr_eq(&first.queue, &again.queue));
        first.append(vec![task(b"\xff")], now).await.unwrap();
        let other = registry.get(key(b"\xfe"), 0, now).await.unwrap();
        assert_eq!(other.queue.task_list_size().unwrap(), 0);
        registry.shutdown(registry_now()).await.unwrap();
        assert!(registry.get(key(b"\xff"), 0, now).await.is_err());
    }

    #[tokio::test]
    async fn unload_cannot_remove_replacement_owner() {
        let (_dir, registry) = fixture();
        let now = registry_now();
        let old = registry.get(key(b"\xff"), 0, now).await.unwrap();
        old.queue.stop();
        let replacement = registry.get(key(b"\xff"), 0, now).await.unwrap();
        assert!(!Arc::ptr_eq(&old.queue, &replacement.queue));
        assert!(!registry.unload(key(b"\xff"), old, now).await.unwrap());
        assert!(registry.contains_cached(&key(b"\xff")));
        registry.shutdown(registry_now()).await.unwrap();
    }

    #[tokio::test]
    async fn async_admission_runs_for_raw_cached_and_stopped_queues() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_dir, registry) = fixture();
        let assigned = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let assignment = assigned.clone();
        let lookup: crate::matching_runtime::NativeShardLookup = Arc::new(move |name| {
            seen.lock().unwrap().push(name);
            assignment.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        });
        let (drain, receiver) = tokio::sync::watch::channel(false);
        let check = crate::matching_runtime::native_shard_ownership_check(
            crate::matching_membership::Membership::new("local", vec![]),
            Arc::new(|| 100),
            Arc::new(|| false),
            receiver,
            lookup,
        );
        registry.set_ownership_policy(NativeQueueOwnershipPolicy {
            is_shard_distributor_eligible: Arc::new(|_| true),
            has_local_processor: Arc::new(move |_| assigned.load(Ordering::SeqCst)),
        });
        let clone = registry.clone();
        registry.set_shard_admission(check);
        let raw = key(b"queue-\xff");
        let first = clone.get(raw.clone(), 0, registry_now()).await.unwrap();
        let second = clone.get(raw.clone(), 0, registry_now()).await.unwrap();
        assert!(Arc::ptr_eq(&first.queue, &second.queue));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![b"queue-\xff".to_vec(), b"queue-\xff".to_vec()]
        );
        drain.send(true).unwrap();
        assert!(clone.get(raw, 0, registry_now()).await.is_err());
        assert_eq!(calls.lock().unwrap().len(), 2);
        registry.shutdown(registry_now()).await.unwrap();
    }

    #[tokio::test]
    async fn admission_observes_live_cache_and_stopped_owner() {
        let (_dir, registry) = fixture();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        registry.set_shard_admission(Arc::new(move |_, cached| {
            seen.lock().unwrap().push(cached);
            Box::pin(async { Ok(()) })
        }));
        let raw = key(b"cached-\xff");
        let first = registry.get(raw.clone(), 0, registry_now()).await.unwrap();
        registry.get(raw.clone(), 0, registry_now()).await.unwrap();
        first.queue.stop();
        registry.get(raw, 0, registry_now()).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![false, true, false]);
        registry.shutdown(registry_now()).await.unwrap();
    }

    #[tokio::test]
    async fn admission_failure_cancellation_and_shutdown_never_publish() {
        let (_dir, registry) = fixture();
        registry.set_shard_admission(Arc::new(|_, _| {
            Box::pin(async { Err(Status::unavailable("heartbeat failed")) })
        }));
        assert_eq!(
            registry
                .get(key(b"failed"), 0, registry_now())
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(registry.loaded_owner_count(), 0);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let started = entered.clone();
        let unblock = release.clone();
        registry.set_shard_admission(Arc::new(move |_, _| {
            let started = started.clone();
            let unblock = unblock.clone();
            Box::pin(async move {
                started.notify_one();
                unblock.notified().await;
                Ok(())
            })
        }));
        let pending_registry = registry.clone();
        let pending =
            tokio::spawn(
                async move { pending_registry.get(key(b"race"), 0, registry_now()).await },
            );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        registry.shutdown(registry_now()).await.unwrap();
        release.notify_one();
        assert!(tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(registry.loaded_owner_count(), 0);
        let cancelled_registry = registry.clone();
        let cancelled = tokio::spawn(async move {
            cancelled_registry
                .get(key(b"cancel"), 0, registry_now())
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(registry.loaded_owner_count(), 0);
    }

    #[tokio::test]
    async fn unknown_task_type_is_rejected() {
        let (_dir, registry) = fixture();
        let mut bad = key(b"q");
        bad.task_type = 2;
        assert!(registry.get(bad, 0, registry_now()).await.is_err());
    }
}
