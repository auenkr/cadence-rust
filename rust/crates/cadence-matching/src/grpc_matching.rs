//! Native internal MatchingAPI adapters and outstanding-poll cancellation.
//! Durable queues dispatch through History RPCs; the in-memory mode uses the
//! local workflow engine. Both modes preserve the original MatchingAPI.
use crate::{grpc::Frontend, grpc_internal::History};
use cadence_common::task_list::TaskListName;
use cadence_proto::{api as p, matching as m, shared};

pub(crate) fn matching_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
/// Resolve a domain UUID to its name the way Go's domain cache does before
/// applying a dynamic-property filter. Unknown or unnamed domains fall back to
/// unfiltered values.
fn domain_name_for(
    domains: &cadence_persistence::InMemoryDomainStore,
    domain_id: &str,
) -> Option<String> {
    domains
        .describe(&cadence_api::service_types::DescribeDomainRequest {
            name: None,
            uuid: Some(domain_id.to_owned()),
        })
        .ok()
        .and_then(|response| response.domain_info.map(|info| info.name))
        .filter(|name| !name.is_empty())
}
pub(crate) fn matching_host_name() -> &'static str {
    static HOSTNAME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    HOSTNAME.get_or_init(|| {
        let mut hostname = [0 as libc::c_char; 256];
        // SAFETY: the array is writable for its full length and is explicitly
        // terminated before converting the result to a C string.
        let result = unsafe { libc::gethostname(hostname.as_mut_ptr(), hostname.len()) };
        if result != 0 {
            return String::new();
        }
        hostname[hostname.len() - 1] = 0;
        // SAFETY: the buffer was zero-initialized and its final byte is NUL.
        unsafe { std::ffi::CStr::from_ptr(hostname.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    })
}
fn task_delete_batch_size_provider(
    domains: cadence_persistence::InMemoryDomainStore,
    overlay: Arc<Mutex<crate::admin::DynamicConfigOverlay>>,
) -> crate::matching_registry::TaskDeleteBatchSizeProvider {
    Arc::new(move |domain_id, task_list, task_type| {
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_integer(
            config.property_values("matching.maxTaskDeleteBatchSize"),
            domain_name_for(&domains, domain_id).as_deref(),
            task_list,
            task_type,
            100,
        )
    })
}
/// Resolve both live task-writer settings using the physical task-list owner.
/// Keep this provider shared by task-list creation and tests so dynamic updates
/// are observed by existing writer actors.
pub(crate) fn task_writer_live_provider(
    domains: cadence_persistence::InMemoryDomainStore,
    overlay: Arc<Mutex<crate::admin::DynamicConfigOverlay>>,
) -> crate::matching_registry::WriterLiveProvider {
    Arc::new(move |domain_id, task_list, task_type| {
        let domain = domain_name_for(&domains, domain_id);
        let config = overlay.lock().expect("admin lock");
        let batch = cadence_persistence::dynamic_config::task_list_integer(
            config.property_values("matching.maxTaskBatchSize"),
            domain.as_deref(),
            task_list,
            task_type,
            100,
        );
        let timeout = cadence_persistence::dynamic_config::task_list_duration(
            config.property_values("matching.appendTaskTimeout"),
            domain.as_deref(),
            task_list,
            task_type,
            5_000_000_000,
        );
        let capacity = cadence_persistence::dynamic_config::task_list_integer(
            config.property_values("matching.outstandingTaskAppendsThreshold"),
            domain.as_deref(),
            task_list,
            task_type,
            250,
        );
        (batch, timeout, capacity)
    })
}
/// Go `taskListManagerImpl.AddTask` QPS reporting: an arrival counts toward the
/// root, and toward its isolation group when the task carries one.
pub(crate) fn report_arrival(
    queue: &crate::matching_registry::QueueHandle,
    data: &cadence_persistence::matching_blob::TaskData,
) {
    queue.mark_alive();
    match data
        .partition_config
        .as_ref()
        .and_then(|config| config.get(ISOLATION_GROUP_KEY))
    {
        Some(group) => queue.qps.report_group(group, 1),
        None => queue.qps.report_counter(1),
    }
}
/// Go `isolationgroup.GroupKey`: the task partition-config key carrying the
/// task's isolation group.
const ISOLATION_GROUP_KEY: &str = "isolation-group";
/// Go `common.IsValidContext` (contextExpireThreshold = 10ms): a poll whose
/// remaining deadline is below the threshold fails with the raw
/// `context.DeadlineExceeded`, which `handleErr` turns into InternalServiceError.
pub(crate) fn check_poll_context(deadline: Option<tokio::time::Instant>) -> Result<(), Status> {
    match deadline {
        Some(deadline)
            if deadline.saturating_duration_since(tokio::time::Instant::now())
                < std::time::Duration::from_millis(10) =>
        {
            Err(Status::internal("context deadline exceeded"))
        }
        _ => Ok(()),
    }
}
/// Go `tasklist.NewIdentifier` returns an untyped `fmt.Errorf`, which the handler's
/// `handleErr` turns into an `InternalServiceError` (never BadRequest).
fn task_list_name(name: &str) -> Result<TaskListName, Status> {
    TaskListName::parse(name).map_err(Status::internal)
}

/// Go `PollForDecisionTask` (engine.go:704) wraps the identifier error as
/// `couldn't create new decision tasklist %w`; `PollForActivityTask` (engine.go:906) returns it bare.
fn poll_task_list_name(kind: &str, name: &str) -> Result<TaskListName, Status> {
    if kind == "activity" {
        return task_list_name(name);
    }
    TaskListName::parse(name)
        .map_err(|e| Status::internal(format!("couldn't create new {kind} tasklist {e}")))
}
/// Go `_stickyPollerUnavailableWindow`.
const STICKY_POLLER_UNAVAILABLE_WINDOW: i64 = 10_000_000_000;
/// Go `taskListKind == types.TaskListKindSticky`.
fn sticky_task_list(list: &p::TaskList) -> bool {
    list.kind == 2
}
/// Go `_stickyPollerUnavailableError`: a sticky task list is only usable while
/// a sticky worker polled it recently, so the caller can fall back to the
/// non-sticky list instead of leaving the task stranded.
fn sticky_unavailable() -> Status {
    const MESSAGE: &str = "sticky worker is unavailable, please use non-sticky task list.";
    crate::grpc::detailed_status(
        tonic::Code::Unavailable,
        MESSAGE,
        "StickyWorkerUnavailableError",
        p::StickyWorkerUnavailableError {},
    )
}
/// Go matching's sticky pre-check in `AddDecisionTask` and `QueryWorkflow`.
fn check_sticky_poller(
    queue: &crate::matching_registry::QueueHandle,
    list: &p::TaskList,
) -> Result<(), Status> {
    if sticky_task_list(list)
        && !queue.has_poller_after(matching_now().saturating_sub(STICKY_POLLER_UNAVAILABLE_WINDOW))
    {
        return Err(sticky_unavailable());
    }
    Ok(())
}
/// Wall-clock nanoseconds for the semaphore manager's idle liveness.
fn semaphore_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
/// Go `ctx.Err()` for a semaphore caller: cancellation or an expired deadline, with
/// the error text Go reports. Cancellation wins when both have ended.
fn caller_ended(
    deadline: Option<tokio::time::Instant>,
    cancelled: Option<&tokio::sync::watch::Receiver<bool>>,
) -> Option<&'static str> {
    if cancelled.is_some_and(|cancelled| *cancelled.borrow()) {
        return Some("context canceled");
    }
    deadline
        .filter(|deadline| tokio::time::Instant::now() >= *deadline)
        .map(|_| "context deadline exceeded")
}

/// Caller cancellation for the semaphore handler. tonic never tells a handler that
/// its caller went away (the future is dropped), so a cancelled Go context is
/// supplied in-process as a request extension that no wire client can set.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct CallerCancel(pub(crate) tokio::sync::watch::Receiver<bool>);

/// Map a semaphore manager error to the Go-compatible gRPC status.
fn semaphore_status(error: crate::matching_semaphore::ManagerError) -> Status {
    use crate::matching_semaphore::ManagerError;
    match error {
        ManagerError::BadRequest(message) => Status::invalid_argument(message),
        ManagerError::Disabled => {
            Status::invalid_argument("distributed semaphore is not enabled for this domain")
        }
        ManagerError::NotFound(message) => Status::not_found(message),
        ManagerError::Deadline => Status::deadline_exceeded("semaphore acquire deadline exceeded"),
        ManagerError::NotReady => crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            "semaphore manager is not ready",
            "ServiceBusyError",
            cadence_proto::api::ServiceBusyError {
                reason: String::new(),
            },
        ),
        ManagerError::ServiceBusy(message) => crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            message,
            "ServiceBusyError",
            cadence_proto::api::ServiceBusyError {
                reason: String::new(),
            },
        ),
        ManagerError::Internal(message) => Status::internal(message),
    }
}
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[allow(deprecated)]
fn partition_config_from_proto(
    config: p::TaskListPartitionConfig,
) -> cadence_persistence::StoredPartitionConfig {
    let convert = |count, map| {
        cadence_common::task_list::partition_map::<p::TaskListPartition>(count, map)
            .into_iter()
            .map(|(id, partition)| (id, partition.isolation_groups))
            .collect()
    };
    cadence_persistence::StoredPartitionConfig {
        version: config.version,
        read_partitions: convert(config.num_read_partitions, config.read_partitions),
        write_partitions: convert(config.num_write_partitions, config.write_partitions),
    }
}

#[allow(deprecated)]
pub(crate) fn partition_config_to_proto(
    c: cadence_persistence::StoredPartitionConfig,
) -> p::TaskListPartitionConfig {
    let partitions = |map: std::collections::HashMap<i32, Vec<String>>| {
        map.into_iter()
            .map(|(id, groups)| {
                (
                    id,
                    p::TaskListPartition {
                        isolation_groups: groups,
                    },
                )
            })
            .collect()
    };
    p::TaskListPartitionConfig {
        version: c.version,
        num_read_partitions: c.read_partitions.len() as i32,
        num_write_partitions: c.write_partitions.len() as i32,
        read_partitions: partitions(c.read_partitions),
        write_partitions: partitions(c.write_partitions),
    }
}

#[allow(deprecated)]
fn partition_config_to_internal(
    c: cadence_persistence::StoredPartitionConfig,
) -> m::TaskListPartitionConfig {
    let partitions = |map: std::collections::HashMap<i32, Vec<String>>| {
        map.into_iter()
            .map(|(id, groups)| {
                (
                    id,
                    m::TaskListPartition {
                        isolation_groups: groups,
                    },
                )
            })
            .collect()
    };
    m::TaskListPartitionConfig {
        version: c.version,
        num_read_partitions: c.read_partitions.len() as i32,
        num_write_partitions: c.write_partitions.len() as i32,
        read_partitions: partitions(c.read_partitions),
        write_partitions: partitions(c.write_partitions),
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct PollKey {
    domain: String,
    task_list: String,
    task_type: i32,
    poller: String,
}
type Polls = Arc<Mutex<HashMap<PollKey, Arc<Notify>>>>;
struct PollGuard {
    polls: Polls,
    key: PollKey,
    cancel: Arc<Notify>,
}
/// Parameters for the sync-match partition walk shared by decision and
/// activity adds.
struct ForwardedAdd<'a> {
    domain_id: &'a str,
    list: &'a p::TaskList,
    task_type: i32,
    data: &'a cadence_persistence::matching_blob::TaskData,
    source: uuid::Uuid,
    entry: &'a crate::matching_registry::QueueHandle,
    deadline: tokio::time::Instant,
    forwarded: bool,
    backlog: bool,
    task_source: i32,
    schedule_to_start_timeout: Option<prost_types::Duration>,
}
impl Drop for PollGuard {
    fn drop(&mut self) {
        // Parity with Go poller.Manager.EndPoll: the entry is removed
        // unconditionally, so a finishing poll can drop a newer poll's entry
        // when poller IDs collide. Deliberate; do not add a ptr_eq guard.
        self.polls
            .lock()
            .expect("poll cancellation lock")
            .remove(&self.key);
    }
}
#[derive(Clone)]
pub struct Matching {
    pub frontend: Frontend,
    polls: Polls,
    started: tokio::sync::watch::Sender<bool>,
    queues: Option<crate::matching_registry::QueueRegistry>,
    /// Go `cfg.NumTasklistWritePartitions`, shared with the registry limiter.
    partition_count: Option<crate::matching_registry::PartitionCountProvider>,
    /// Go `cfg.EnableAdaptiveScaler`, guarding manual partition updates.
    scaler_config: Option<crate::matching_registry::ScalerConfigProvider>,
    /// Go `semaphoreRegistry`: one manager per semaphore bucket, in-memory.
    semaphores: crate::matching_semaphore::Registry,
    /// Go `matchingEngineImpl.shutdown`, shared by every clone of this handler.
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    membership: Option<crate::matching_membership::Membership>,
    shard_router: Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>,
    task_list_ownership: Option<TaskListOwnershipCheck>,
    rpc_quota: Arc<crate::matching_quota::MatchingRpcQuota>,
    throttled_logger: Arc<crate::matching_throttled_logger::MatchingThrottledLogger>,
    pub(crate) poll_expiration: std::time::Duration,
    metrics: crate::matching_metrics::MatchingMetrics,
    domain_name_resolver: Option<DomainNameResolver>,
    domain_cache: Option<Arc<crate::matching_domain_cache::MatchingDomainCache>>,
    domain_id_parse: crate::matching_domain_uuid::DomainIdParse,
    /// Test-only engine failure seam: `(rpc name, message)` makes that RPC's engine
    /// call fail with `message`. See `injected_engine_fault`.
    #[cfg(test)]
    pub(crate) engine_fault: Arc<std::sync::Mutex<Option<(&'static str, String)>>>,
}
type DomainNameResolver = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;
/// Runtime-provided ownership check. The cached flag is advisory and only
/// allows the excluded-task-list Ring fast path; eligible names must still
/// validate their local shard processor on every request.
pub type TaskListOwnershipCheck = Arc<
    dyn Fn(
            String,
            bool,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), tonic::Status>> + Send>>
        + Send
        + Sync,
>;
impl Matching {
    pub fn new(state: crate::handlers::AppState) -> Self {
        // Go builds the semaphore registry on the configured default datastore.
        let semaphore_store: Arc<dyn cadence_persistence::SemaphorePersistence> =
            match &state.semaphore_persistence {
                Some(store) => store.clone(),
                None => Arc::new(state.executions.clone()),
            };
        let semaphores = crate::matching_semaphore::Registry::new(semaphore_store);
        let rpc_quota = Arc::new(crate::matching_quota::MatchingRpcQuota::with_metric_sink(
            state.admin.dynamic_config.clone(),
            Arc::new(|_| {}),
        ));
        let throttled_logger = Arc::new(
            crate::matching_throttled_logger::MatchingThrottledLogger::new(
                state.admin.dynamic_config.clone(),
            ),
        );
        Self {
            frontend: Frontend { state },
            polls: Arc::new(Mutex::new(HashMap::new())),
            started: tokio::sync::watch::channel(true).0,
            queues: None,
            partition_count: None,
            scaler_config: None,
            semaphores,
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            membership: None,
            shard_router: None,
            task_list_ownership: None,
            rpc_quota,
            throttled_logger,
            poll_expiration: std::time::Duration::from_secs(60),
            metrics: crate::matching_metrics::MatchingMetrics::default(),
            domain_name_resolver: None,
            domain_cache: None,
            domain_id_parse: Default::default(),
            #[cfg(test)]
            engine_fault: Default::default(),
        }
    }
    /// Test-only: returns the injected engine error for `rpc`, mirroring a Go engine
    /// call that returns `errors.New(msg)`. Go's `handleErr` default case maps that to
    /// `InternalServiceError{Message: msg}`, which is `Status::internal(msg)` on the wire.
    #[cfg(test)]
    fn injected_engine_fault(&self, rpc: &str) -> Result<(), Status> {
        match &*self.engine_fault.lock().expect("engine fault lock") {
            Some((name, message)) if *name == rpc => Err(Status::internal(message.clone())),
            _ => Ok(()),
        }
    }
    /// Install an optional inbound Matching metrics sink.
    pub fn with_metrics(mut self, sink: Arc<dyn crate::matching_metrics::MetricSink>) -> Self {
        self.metrics = crate::matching_metrics::MatchingMetrics::new(sink);
        let quota_metrics = self.metrics.clone();
        self.rpc_quota.set_metric_sink(Arc::new(move |rejection| {
            quota_metrics.quota_rejection(rejection)
        }));
        if let Some(queues) = self.queues.take() {
            self.queues = Some(queues.with_metrics(self.metrics.clone()));
        }
        self
    }
    pub(crate) fn with_fatal_writer(
        mut self,
        fatal: tokio::sync::mpsc::UnboundedSender<
            cadence_persistence::matching_blob::SqlRunIdError,
        >,
    ) -> Self {
        if let Some(queues) = self.queues.take() {
            self.queues = Some(queues.with_fatal_writer(fatal));
        }
        self
    }
    pub(crate) fn inbound_metrics(&self) -> &crate::matching_metrics::MatchingMetrics {
        &self.metrics
    }
    pub(crate) fn with_domain_cache(
        mut self,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> Self {
        self.domain_id_parse = cache.domain_id_parse();
        self.domain_cache = Some(cache.clone());
        let domains = self.frontend.state.domains.clone();
        self.domain_name_resolver = Some(Arc::new(move |id| {
            let domain = cache.get_domain_by_id(id).ok()?;
            // Go consumers read the cache directly. Rust's quota and workflow
            // consumers use this metadata view; publish cold discoveries before
            // they run. Go's change callback intentionally omits new entries.
            domains.insert_matching_metadata_if_missing(&domain);
            Some(domain.info.name)
        }));
        self
    }
    #[cfg(test)]
    pub(crate) fn with_domain_id_parse(
        mut self,
        mode: crate::matching_domain_uuid::DomainIdParse,
    ) -> Self {
        self.domain_id_parse = mode;
        self
    }
    /// Go handler `h.domainName` (metrics lookup) runs first and, on SQL, panics for a
    /// malformed ID. Ordinary lookup errors stay swallowed here; the real validation in
    /// `domain_name` below keeps returning its typed errors.
    fn domain_lookup_guard(&self, domain_id: &str, operation: &'static str) -> Result<(), Status> {
        crate::matching_domain_uuid::domain_lookup_guard(self.domain_id_parse, domain_id, operation)
    }
    async fn metric_domain_name(&self, id: &str) -> Option<String> {
        if let Some(resolve) = self.domain_name_resolver.as_ref().cloned() {
            let id = id.to_owned();
            return match tokio::task::spawn_blocking(move || resolve(&id)).await {
                Ok(domain_name) => domain_name,
                Err(error) if error.is_panic() => {
                    // Keep resolver panics inside matching_panic::capture, as they
                    // were before crossing the blocking-task boundary.
                    std::panic::resume_unwind(error.into_panic())
                }
                Err(_) => None,
            };
        }
        domain_name_for(&self.frontend.state.domains, id)
    }
    /// Resolve a domain name the way Go's Matching domain cache does. The
    /// persistence read is blocking, and a cold result must also reach the
    /// in-memory view used by workflow and quota handlers.
    async fn domain_id_by_name(&self, name: &str) -> Result<String, Status> {
        if let Some(cache) = self.domain_cache.clone() {
            let domains = self.frontend.state.domains.clone();
            let name = name.to_owned();
            return match tokio::task::spawn_blocking(move || {
                let domain = cache.get_domain(&name)?;
                domains.insert_matching_metadata_if_missing(&domain);
                Ok::<_, crate::matching_domain_cache::DomainCacheError>(domain.info.uuid)
            })
            .await
            {
                Ok(Ok(id)) => Ok(id),
                Ok(Err(error)) => Err(match error {
                    crate::matching_domain_cache::DomainCacheError::EmptyName => {
                        Status::invalid_argument("Domain name is empty")
                    }
                    crate::matching_domain_cache::DomainCacheError::EmptyId => {
                        Status::invalid_argument("DomainID is empty.")
                    }
                    crate::matching_domain_cache::DomainCacheError::SourceClassified {
                        message,
                        class: crate::matching_domain_cache::DomainErrorClass::NotFound,
                    } => crate::grpc::domain_not_found(message),
                    crate::matching_domain_cache::DomainCacheError::SourceClassified {
                        message,
                        class: crate::matching_domain_cache::DomainErrorClass::BadRequest,
                    } => Status::invalid_argument(message),
                    crate::matching_domain_cache::DomainCacheError::SourceClassified { message, .. }
                    | crate::matching_domain_cache::DomainCacheError::Source(message) => {
                        Status::internal(message)
                    }
                    crate::matching_domain_cache::DomainCacheError::ExistsButNotLoaded(_) => {
                        Status::internal("DefaultDomainCache encounter case where domain exists but cannot be loaded")
                    }
                    crate::matching_domain_cache::DomainCacheError::Stopped => {
                        Status::internal("domain cache is stopped")
                    }
                }),
                Err(error) if error.is_panic() => {
                    std::panic::resume_unwind(error.into_panic())
                }
                Err(error) => Err(Status::internal(error.to_string())),
            };
        }
        self.frontend
            .state
            .domains
            .domain_id(name)
            .map_err(|error| crate::grpc::domain_not_found(error.message))
    }
    /// Incremental durable delivery backend. Domain metadata and the remaining
    /// administrative/query methods still use the supplied application state.
    pub fn with_durable_queues(
        state: crate::handlers::AppState,
        queues: crate::matching_registry::QueueRegistry,
        poll_expiration: std::time::Duration,
    ) -> Self {
        // Go `cfg.NumTasklistWritePartitions(domain, taskList, type)` reads the
        // dynamic properties `matching.numTasklistWritePartitions` (default 1)
        // and clamps to >= 1. The overlay is the same source the frontend reads
        // its own dynamic properties from.
        let domains = state.domains.clone();
        let overlay = state.admin.dynamic_config.clone();
        let provider: crate::matching_registry::PartitionCountProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(
                move |domain_id: &str, task_list: &str, task_type: i32| -> i64 {
                    let values = overlay
                        .lock()
                        .expect("admin lock")
                        .property_values("matching.numTasklistWritePartitions");
                    cadence_persistence::dynamic_config::task_list_integer(
                        &values,
                        domain_name_for(&domains, domain_id).as_deref(),
                        task_list,
                        task_type,
                        1,
                    )
                },
            )
        };
        let read_provider: crate::matching_registry::PartitionCountProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.numTasklistReadPartitions"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    1,
                )
            })
        };
        let sync_wait: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration(
                    config.property_values("matching.localTaskWaitTime"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    10_000_000,
                )
            })
        };
        let local_poll_wait: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration(
                    config.property_values("matching.localPollWaitTime"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    10_000_000,
                )
            })
        };
        let cache_enabled: crate::matching_registry::PartitionCacheEnabledProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_boolean(
                    config.property_values("matching.enableGetNumberOfPartitionsFromCache"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    false,
                )
            })
        };
        // Go `cfg.QPSTrackerInterval(domain, taskList, type)`
        // (`matching.qpsTrackerInterval`, default 10s in nanoseconds).
        let qps_interval: crate::matching_registry::QpsIntervalProvider = {
            let overlay = overlay.clone();
            let domains = domains.clone();
            Arc::new(
                move |domain_id: &str, task_list: &str, task_type: i32| -> i64 {
                    let values = overlay
                        .lock()
                        .expect("admin lock")
                        .property_values("matching.qpsTrackerInterval");
                    cadence_persistence::dynamic_config::task_list_duration(
                        &values,
                        domain_name_for(&domains, domain_id).as_deref(),
                        task_list,
                        task_type,
                        crate::matching_registry::DEFAULT_QPS_TRACKER_INTERVAL_NANOS,
                    )
                },
            )
        };
        let forward_degree: crate::matching_registry::ForwardDegreeProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.forwarderMaxChildrenPerNode"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    20,
                )
                // Go task_list_manager clamps ForwarderMaxChildrenPerNode to >= 1.
                .max(1)
            })
        };
        let forward_polls: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.forwarderMaxOutstandingPolls"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    1,
                )
            })
        };
        let forward_tasks: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.forwarderMaxOutstandingTasks"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    1,
                )
            })
        };
        let forward_rate: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.forwarderMaxRatePerSecond"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    10,
                )
            })
        };
        // Go's reader `TaskListConfig` reads: `GetTasksBatchSize` (default 1000,
        // invalid values fall back to it), `ReadRangeSize` (default 50000, a
        // plain unfiltered integer) and `AsyncTaskDispatchTimeout` (default 3s).
        let reader_live: crate::matching_registry::ReaderPropertiesProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id: &str, task_list: &str, task_type: i32| {
                let scoped = |key: &'static str, default: i64| -> crate::matching_reader::LiveInt {
                    let domains = domains.clone();
                    let overlay = overlay.clone();
                    let domain_id = domain_id.to_string();
                    let task_list = task_list.to_string();
                    Arc::new(move || {
                        let config = overlay.lock().expect("admin lock");
                        cadence_persistence::dynamic_config::task_list_integer(
                            config.property_values(key),
                            domain_name_for(&domains, &domain_id).as_deref(),
                            &task_list,
                            task_type,
                            default,
                        )
                    })
                };
                let global = |key: &'static str, default: i64| -> crate::matching_reader::LiveInt {
                    let overlay = overlay.clone();
                    Arc::new(move || {
                        let config = overlay.lock().expect("admin lock");
                        cadence_persistence::dynamic_config::integer(
                            config.property_values(key),
                            None,
                            default,
                        )
                    })
                };
                crate::matching_reader::ReaderLive {
                    batch_size: scoped(
                        "matching.getTasksBatchSize",
                        crate::matching_reader::DEFAULT_TASKS_BATCH_SIZE,
                    ),
                    read_range_size: global(
                        "matching.readRangeSize",
                        crate::matching_reader::DEFAULT_READ_RANGE_SIZE,
                    ),
                    dispatch_timeout: scoped(
                        "matching.asyncTaskDispatchTimeout",
                        crate::matching_reader::DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS,
                    ),
                    update_ack_interval: {
                        let domains = domains.clone();
                        let overlay = overlay.clone();
                        let domain_id = domain_id.to_string();
                        let task_list = task_list.to_string();
                        Arc::new(move || {
                            let config = overlay.lock().expect("admin lock");
                            cadence_persistence::dynamic_config::task_list_duration(
                                config.property_values("matching.updateAckInterval"),
                                domain_name_for(&domains, &domain_id).as_deref(),
                                &task_list,
                                task_type,
                                crate::matching_reader::DEFAULT_UPDATE_ACK_INTERVAL_NANOS,
                            )
                        })
                    },
                }
            })
        };
        // Go `cfg.RecordTaskStartedTimeout(domain)` (`matching.recordTaskStartedTimeout`,
        // Domain filter only, default 1s): the operation timeout of each
        // Record*TaskStarted attempt.
        let record_started_timeout: crate::matching_history::RecordStartedTimeout = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id: &str| {
                let nanos = {
                    let config = overlay.lock().expect("admin lock");
                    cadence_persistence::dynamic_config::duration(
                        config.property_values("matching.recordTaskStartedTimeout"),
                        domain_name_for(&domains, domain_id).as_deref(),
                        crate::matching_history::DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS,
                    )
                };
                std::time::Duration::from_nanos(nanos.max(0) as u64)
            })
        };
        // Go `cfg.MinTaskThrottlingBurstSize(domain, taskList, type)`, default 1.
        let min_burst: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.minTaskThrottlingBurstSize"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE,
                )
            })
        };
        let update_ack_interval: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration(
                    config.property_values("matching.updateAckInterval"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    60_000_000_000,
                )
            })
        };
        let idle_tasklist_check_interval: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration(
                    config.property_values("matching.idleTasklistCheckInterval"),
                    domain_name_for(&domains, domain_id).as_deref(),
                    task_list,
                    task_type,
                    300_000_000_000,
                )
            })
        };
        let max_task_delete_batch_size =
            task_delete_batch_size_provider(domains.clone(), overlay.clone());
        let writer_live = task_writer_live_provider(domains.clone(), overlay.clone());
        let scaler_config: crate::matching_registry::ScalerConfigProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let name = domain_name_for(&domains, domain_id);
                let config = overlay.lock().expect("admin lock");
                let integer = |key: &str, default: i64| {
                    cadence_persistence::dynamic_config::task_list_integer(
                        config.property_values(key),
                        name.as_deref(),
                        task_list,
                        task_type,
                        default,
                    )
                };
                let duration = |key: &str, default: i64| {
                    cadence_persistence::dynamic_config::task_list_duration(
                        config.property_values(key),
                        name.as_deref(),
                        task_list,
                        task_type,
                        default,
                    )
                };
                let boolean = |key: &str, default: bool| {
                    cadence_persistence::dynamic_config::task_list_boolean(
                        config.property_values(key),
                        name.as_deref(),
                        task_list,
                        task_type,
                        default,
                    )
                };
                crate::matching_scaler::ScalerConfig {
                    enabled: boolean("matching.enableAdaptiveScaler", false),
                    // Read from the same locked snapshot: the standalone
                    // cache-enabled provider re-locks the store, which would
                    // self-deadlock on a non-reentrant mutex.
                    cache_enabled: boolean("matching.enableGetNumberOfPartitionsFromCache", false),
                    update_interval_nanos: duration(
                        "matching.adaptiveScalerUpdateInterval",
                        15_000_000_000,
                    ),
                    upscale_rps: integer("matching.partitionUpscaleRPS", 200) as f64,
                    downscale_factor: cadence_persistence::dynamic_config::task_list_float(
                        config.property_values("matching.partitionDownscaleFactor"),
                        name.as_deref(),
                        task_list,
                        task_type,
                        0.75,
                    ),
                    upscale_sustain_nanos: duration(
                        "matching.partitionUpscaleSustainedDuration",
                        60_000_000_000,
                    ),
                    downscale_sustain_nanos: duration(
                        "matching.partitionDownscaleSustainedDuration",
                        120_000_000_000,
                    ),
                    min_write_partitions: integer("matching.taskListMinimumWritePartitions", 1),
                    isolation_assignment: boolean(
                        "matching.enablePartitionIsolationGroupAssignment",
                        false,
                    ),
                    empty_check: boolean("matching.enablePartitionEmptyCheck", false),
                    group_upscale_sustain_nanos: duration(
                        "matching.isolationGroupUpscaleSustainedDuration",
                        60_000_000_000,
                    ),
                    group_downscale_sustain_nanos: duration(
                        "matching.isolationGroupDownscaleSustainedDuration",
                        120_000_000_000,
                    ),
                    group_has_pollers_sustain_nanos: duration(
                        "matching.isolationGroupHasPollersSustainedDuration",
                        60_000_000_000,
                    ),
                    group_no_pollers_sustain_nanos: duration(
                        "matching.isolationGroupNoPollersSustainedDuration",
                        60_000_000_000,
                    ),
                    groups_per_partition: integer("matching.isolationGroupsPerPartition", 2),
                    poller_window_nanos: duration(
                        "matching.taskIsolationPollerWindow",
                        2_000_000_000,
                    ),
                    isolation_groups: cadence_persistence::dynamic_config::string_list(
                        config.property_values("system.allIsolationGroups"),
                    ),
                }
            })
        };
        let isolation: crate::matching_registry::IsolationProvider = Arc::new(
            move |domain_id, task_list, task_type, kind, pollers, partitions| {
                let name = domain_name_for(&domains, domain_id);
                let config = overlay.lock().expect("admin lock");
                let enabled = cadence_persistence::dynamic_config::boolean(
                    config.property_values("system.enableTasklistIsolation"),
                    name.as_deref(),
                    false,
                );
                let groups = if enabled && kind != 1 {
                    cadence_persistence::dynamic_config::string_list(
                        config.property_values("system.allIsolationGroups"),
                    )
                } else {
                    Vec::new()
                };
                drop(config);
                let known = groups.clone();
                let overlay = overlay.clone();
                let domains = domains.clone();
                let task_list = task_list.to_owned();
                let reload_overlay = overlay.clone();
                let reload_name = name.clone();
                crate::matching_reader::IsolationConfig {
                    groups,
                    should_reload: Arc::new(move || {
                        let config = reload_overlay.lock().expect("admin lock");
                        cadence_persistence::dynamic_config::boolean(
                            config.property_values("system.enableTasklistIsolation"),
                            reload_name.as_deref(),
                            false,
                        ) != enabled
                    }),
                    select: Arc::new(move |task| {
                        let group = task
                            .partition_config
                            .as_ref()
                            .and_then(|map| map.get(ISOLATION_GROUP_KEY))
                            .map(String::as_str)
                            .unwrap_or_default();
                        let now = matching_now();
                        let config = overlay.lock().expect("admin lock");
                        let duration = |key, default| {
                            cadence_persistence::dynamic_config::task_list_duration(
                                config.property_values(key),
                                name.as_deref(),
                                &task_list,
                                task_type,
                                default,
                            )
                        };
                        let window = duration("matching.taskIsolationPollerWindow", 2_000_000_000);
                        let total_duration =
                            duration("matching.taskIsolationDuration", 200_000_000);
                        drop(config);
                        let drained = name.as_deref().ok_or(()).and_then(|name| domains.domain_isolation_groups(name).map_err(|_| ())).map(|config| {
                        config.into_iter().flat_map(|config| config.isolation_groups).any(|partition| partition.name == group && partition.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                        || domains.global_isolation_groups().into_iter().flat_map(|config| config.isolation_groups).any(|partition| partition.name == group && partition.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                    });
                        let partition = partitions.get();
                        let partition_id = TaskListName::parse(&task_list)
                            .map(|name| name.partition() as i32)
                            .unwrap_or_default();
                        let partition_groups = partition
                            .as_ref()
                            .map(|config| config.read_partitions.get(&partition_id));
                        let empty = Vec::new();
                        let policy = cadence_common::task_isolation::TaskIsolation {
                            enabled,
                            sticky: kind == 1,
                            group,
                            known: known.iter().any(|known| known == group),
                            drained,
                            recent_poller: pollers
                                .lock()
                                .expect("poller history lock")
                                .has_group_after(group, now.saturating_sub(window)),
                            partition_groups: match partition_groups {
                                None => Some(empty.as_slice()),
                                Some(groups) => groups.map(Vec::as_slice),
                            },
                            total_duration,
                            task_latency: now.saturating_sub(task.created_nanos),
                        };
                        let (group, duration, leak_cause) = policy.select_with_cause();
                        (group.to_owned(), duration, leak_cause)
                    }),
                }
            },
        );
        let standby_enabled: crate::matching_standby::Enabled = {
            let domains = state.domains.clone();
            let overlay = state.admin.dynamic_config.clone();
            Arc::new(move |domain, name, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_boolean(
                    config.property_values("matching.enableStandbyTaskCompletion"),
                    domain_name_for(&domains, domain).as_deref(),
                    name,
                    task_type,
                    true,
                )
            })
        };
        let queues = queues
            .with_standby_enabled(standby_enabled)
            .with_partition_count(read_provider)
            .with_partition_cache_enabled(cache_enabled)
            .with_sync_wait(sync_wait)
            .with_local_poll_wait(local_poll_wait)
            .with_forward_degree(forward_degree)
            .with_forward_polls(forward_polls)
            .with_forward_tasks(forward_tasks, forward_rate)
            .with_scaler_config(scaler_config.clone())
            .with_qps_interval(qps_interval)
            .with_reader_live(reader_live)
            .with_min_burst(min_burst)
            .with_record_started_timeout(record_started_timeout)
            .with_update_ack_interval(update_ack_interval)
            .with_idle_tasklist_check_interval(idle_tasklist_check_interval)
            .with_max_task_delete_batch_size(max_task_delete_batch_size)
            .with_writer_live(writer_live)
            .with_isolation(isolation)
            .with_domain_metadata(state.domains.clone(), state.current_cluster_name.clone());
        let mut matching = Self::new(state);
        matching.queues = Some(queues);
        matching.partition_count = Some(provider);
        matching.scaler_config = Some(scaler_config);
        matching.poll_expiration = poll_expiration;
        matching.bind_local_poll_service()
    }
    /// Production publishes the handler before membership bootstrap so Ringpop
    /// can join; Meta health must wait until engine startup completes.
    pub fn with_start_pending(mut self) -> Self {
        self.started = tokio::sync::watch::channel(false).0;
        self
    }
    pub fn mark_started(&self) {
        self.started.send_replace(true);
    }
    pub(crate) async fn wait_started(&self) {
        let mut started = self.started.subscribe();
        started
            .wait_for(|ready| *ready)
            .await
            .expect("Matching startup sender lives with handler");
    }
    pub fn with_membership(mut self, membership: crate::matching_membership::Membership) -> Self {
        self.queues = self
            .queues
            .map(|queues| queues.with_membership(membership.clone()));
        self.membership = Some(membership);
        self.bind_local_poll_service()
    }
    pub fn with_task_list_ownership(mut self, check: TaskListOwnershipCheck) -> Self {
        self.task_list_ownership = Some(check);
        self
    }
    pub fn with_shard_router(
        mut self,
        router: crate::matching_shard_routing::MatchingShardRouter,
    ) -> Self {
        let router = Arc::new(router);
        if let Some(queues) = &self.queues {
            queues.set_shard_router(router.clone());
        }
        self.shard_router = Some(router);
        self
    }
    async fn remote_channel_for_task_list(
        &self,
        name: &str,
    ) -> Result<Option<tonic::transport::Channel>, Status> {
        if let Some(router) = &self.shard_router {
            return router
                .remote_channel(name)
                .await
                .map(|(channel, _path)| channel)
                .map_err(|error| Status::unavailable(error.to_string()));
        }
        self.membership
            .as_ref()
            .map_or(Ok(None), |membership| membership.remote_channel(name))
    }
    async fn owner_address_for_task_list(&self, name: &str) -> Result<Option<String>, Status> {
        if let Some(router) = &self.shard_router {
            return router
                .resolve(name)
                .await
                .map(|(host, _path)| Some(host.address))
                .map_err(|error| Status::unavailable(error.to_string()));
        }
        self.membership.as_ref().map_or(Ok(None), |membership| {
            membership.owner(name).map(|host| Some(host.address))
        })
    }
    async fn task_list_partition_metadata(
        &self,
        domain_id: &str,
        name: &TaskListName,
        task_type: i32,
    ) -> Result<Vec<p::TaskListPartitionMetadata>, Status> {
        let count = self
            .partition_count
            .as_ref()
            .map(|provider| provider(domain_id, name.root(), task_type))
            .unwrap_or(1)
            .max(1);
        let mut result = Vec::with_capacity(count as usize);
        for partition in 0..count {
            let key = name.partition_name(partition);
            let owner_host_name = self
                .owner_address_for_task_list(&key)
                // Go listTaskListPartitions deliberately discards getHostInfo errors.
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            result.push(p::TaskListPartitionMetadata {
                key,
                owner_host_name,
            });
        }
        Ok(result)
    }
    async fn check_task_list_ownership(&self, name: &str, cached: bool) -> Result<(), Status> {
        if let Some(check) = &self.task_list_ownership {
            check(name.to_owned(), cached).await
        } else {
            Ok(())
        }
    }
    fn bind_local_poll_service(mut self) -> Self {
        // The factory must not own its registry: owners retain forwarding
        // resolvers. Retain only handler state, and borrow the live registry
        // for each forwarded RPC after its weak owner-state upgrade.
        if let Some(queues) = self.queues.take() {
            let base = self.clone();
            self.queues = Some(queues.with_local_poll_service(Arc::new(move |queues| {
                let mut service = base.clone();
                service.queues = Some(queues);
                service
            })));
        }
        self
    }
    /// Go `Stop` closes the engine's shutdown channel. Set by the runtime once the drain has run.
    pub(crate) fn begin_shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Go `Stop` stops every semaphore bucket. Each stopped manager unregisters itself.
    pub(crate) fn stop_semaphores(&self) {
        for manager in self.semaphores.all_managers() {
            manager.stop();
        }
    }

    /// Go `errIfSemaphoreOwnershipLost` for a bucket that has no manager yet. Once the
    /// engine shuts down, the bucket is refused before any load.
    fn check_semaphore_ownership(
        &self,
        id: &crate::matching_semaphore::Identifier,
    ) -> Result<(), Status> {
        if self.is_shutting_down() {
            let my_identity = self
                .membership
                .as_ref()
                .map_or("", |membership| membership.identity());
            return Err(crate::matching_membership::semaphore_not_owned_status(
                "not known",
                my_identity,
                id.to_log_string(),
            ));
        }
        match &self.membership {
            Some(membership) => membership.check_semaphore(id),
            None => Ok(()),
        }
    }

    /// The service owns this future and cancels it during shutdown. Watch
    /// coalescing is safe: cleanup always consults the latest complete ring.
    pub async fn maintain_membership_until<F>(&self, shutdown: F) -> Result<(), Status>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        crate::matching_panic::capture::<(), Status>("membership change loop", async move {
            let Some(membership) = &self.membership else {
                return Ok(());
            };
            let mut changes = membership.subscribe();
            tokio::pin!(shutdown);
            loop {
                let queues = match &self.queues {
                    Some(queues) => queues.unload_non_owned(matching_now()).await.map(|_| ()),
                    None => Ok(()),
                };
                // Do not let a SQL cleanup failure prevent semaphore cleanup.
                for manager in self.semaphores.all_managers() {
                    match membership.owns(&manager.identifier().ring_key()) {
                        Ok(false) => {
                            let stopped = crate::matching_panic::capture_sync(
                                "semaphore manager stop",
                                || manager.stop(),
                            );
                            self.semaphores.unregister(&manager);
                            if let Err(message) = stopped {
                                eprintln!("Matching semaphore unload failed: {message}");
                            }
                        }
                        Ok(true) => {}
                        Err(error) => {
                            eprintln!("Matching semaphore membership lookup failed: {error}");
                            break;
                        }
                    }
                }
                if let Err(error) = queues {
                    eprintln!("Matching membership cleanup failed: {error}");
                }
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    result = changes.changed() => if result.is_err() { return Ok(()); },
                }
            }
        })
        .await
        .unwrap_or(Ok(()))
    }
    fn poll_rate(
        &self,
        domain: &str,
        list: &str,
        task_list_kind: i32,
        task_type: i32,
        worker_rate: Option<f64>,
        limiter: &crate::matching_limiter::TaskListLimiter,
    ) -> f64 {
        // The override read must release the admin lock before reporting: the
        // limiter's live partition-count provider reads the same overlay, so
        // reporting under the lock would re-enter this mutex and hang.
        let override_rate = {
            let config = self
                .frontend
                .state
                .admin
                .dynamic_config
                .lock()
                .expect("admin lock");
            cadence_persistence::dynamic_config::task_list_float(
                config.property_values("matching.overrideTaskListRps"),
                domain_name_for(&self.frontend.state.domains, domain).as_deref(),
                list,
                task_type,
                0.0,
            )
        };
        let rate = if override_rate > 0.0 {
            Some(override_rate)
        } else {
            worker_rate
        };
        match rate.filter(|rate| *rate >= 0.0) {
            Some(rate) => {
                if let Some(reported) = limiter.report_limit(rate) {
                    self.metrics.task_list_gauge(
                        crate::matching_metrics::RATE_LIMIT_PER_TASK_LIST,
                        &crate::matching_metrics::PollMetricContext {
                            domain: domain_name_for(&self.frontend.state.domains, domain)
                                .unwrap_or_else(|| domain.to_owned()),
                            task_list: list.to_owned(),
                            task_list_kind,
                            task_type: if task_type == 0 {
                                "decision_task"
                            } else {
                                "activity_task"
                            },
                            poller_isolation_group: String::new(),
                        },
                        reported,
                    );
                }
                rate
            }
            None => crate::matching_limiter::TASK_DISPATCH_RPS,
        }
    }
    fn emit_partition_runtime_metrics(
        &self,
        domain_id: &str,
        list: &p::TaskList,
        task_type: i32,
        queue: &crate::matching_registry::QueueHandle,
    ) -> Result<(), Status> {
        if list.kind != 0 {
            return Ok(());
        }
        let parsed = task_list_name(&list.name)?;
        let domain = domain_name_for(&self.frontend.state.domains, domain_id)
            .unwrap_or_else(|| domain_id.to_owned());
        let context = crate::matching_metrics::PollMetricContext {
            domain: domain.clone(),
            task_list: list.name.clone(),
            task_list_kind: list.kind,
            task_type: if task_type == 0 {
                "decision_task"
            } else {
                "activity_task"
            },
            poller_isolation_group: String::new(),
        };
        let config = queue.partitions.get();
        self.metrics.partition_config_gauges(
            &context,
            parsed.root(),
            config
                .as_ref()
                .map(|value| value.read_partitions.len())
                .unwrap_or(1),
            config
                .as_ref()
                .map(|value| value.write_partitions.len())
                .unwrap_or(1),
            config.as_ref().map(|value| value.version),
        );
        if !parsed.is_root() {
            return Ok(());
        }
        let (read_partitions, write_partitions, isolation_enabled) = {
            let config = self
                .frontend
                .state
                .admin
                .dynamic_config
                .lock()
                .expect("admin lock");
            let root = parsed.root();
            let read = cadence_persistence::dynamic_config::task_list_integer(
                config.property_values("matching.numTasklistReadPartitions"),
                Some(&domain),
                root,
                task_type,
                1,
            )
            .max(1) as usize;
            let write = cadence_persistence::dynamic_config::task_list_integer(
                config.property_values("matching.numTasklistWritePartitions"),
                Some(&domain),
                root,
                task_type,
                1,
            )
            .max(1) as usize;
            let enabled = cadence_persistence::dynamic_config::boolean(
                config.property_values("system.enableTasklistIsolation"),
                Some(&domain),
                false,
            );
            (read, write, enabled)
        };
        let poller_count = queue.pollers.lock().expect("poller history lock").count();
        self.metrics.partition_mismatch(
            &context,
            read_partitions != write_partitions,
            poller_count < read_partitions || poller_count < write_partitions,
            isolation_enabled,
        );
        Ok(())
    }
    /// Go `types.AutoConfigHint`. `taskMatcher.Poll` stamps the task it returns
    /// with `cfg.EnableClientAutoConfig()` bound to the manager's own task-list
    /// type plus the elapsed poll wait, and the engine stamps the same hint on
    /// an empty poll. For the empty branch Go passes the decision task type even
    /// for activity polls, so the caller selects the type accordingly.
    fn auto_config_hint(
        &self,
        domain: &str,
        list: &str,
        task_type: i32,
        started: std::time::Instant,
    ) -> p::AutoConfigHint {
        let enabled = {
            let config = self
                .frontend
                .state
                .admin
                .dynamic_config
                .lock()
                .expect("admin lock");
            cadence_persistence::dynamic_config::task_list_boolean(
                config.property_values("matching.enableClientAutoConfig"),
                domain_name_for(&self.frontend.state.domains, domain).as_deref(),
                list,
                task_type,
                false,
            )
        };
        p::AutoConfigHint {
            enable_auto_config: enabled,
            poller_wait_time_in_ms: i64::try_from(started.elapsed().as_millis())
                .unwrap_or(i64::MAX),
        }
    }
    fn poll_timeout(
        &self,
        domain: &str,
        list: &str,
        task_type: i32,
        deadline: Option<tokio::time::Instant>,
    ) -> std::time::Duration {
        let config = self
            .frontend
            .state
            .admin
            .dynamic_config
            .lock()
            .expect("admin lock");
        let nanos = cadence_persistence::dynamic_config::task_list_duration(
            config.property_values("matching.longPollExpirationInterval"),
            domain_name_for(&self.frontend.state.domains, domain).as_deref(),
            list,
            task_type,
            self.poll_expiration.as_nanos().min(i64::MAX as u128) as i64,
        );
        let timeout = std::time::Duration::from_nanos(nanos.max(0) as u64);
        // Go taskListManager.newChildContext reserves one second to encode
        // the empty response before its caller's context expires.
        deadline
            .map(|deadline| {
                timeout.min(
                    deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .saturating_sub(std::time::Duration::from_secs(1)),
                )
            })
            .unwrap_or(timeout)
    }
    async fn durable_queue(
        &self,
        domain: &str,
        list: &p::TaskList,
        task_type: i32,
    ) -> Result<crate::matching_registry::QueueHandle, Status> {
        task_list_name(&list.name)?;
        let domain_id = uuid::Uuid::parse_str(domain)
            .map_err(|_| Status::invalid_argument("Domain UUID is not valid."))?;
        let kind = match list.kind {
            2 => 1,
            3 => 2,
            _ => 0,
        };
        let key = cadence_persistence::matching_sqlite::TaskListKey {
            domain_id,
            name: list.name.clone(),
            task_type,
        };
        let cached = self
            .queues
            .as_ref()
            .expect("durable backend selected")
            .contains_cached(&key);
        self.check_task_list_ownership(&list.name, cached).await?;
        self.queues
            .as_ref()
            .expect("durable backend selected")
            .get(key, kind, matching_now())
            .await
    }
    fn check_write_partition(
        &self,
        domain_id: &str,
        list: &p::TaskList,
        task_type: i32,
        queue: &crate::matching_registry::QueueHandle,
    ) -> Result<(), Status> {
        let name = task_list_name(&list.name)?;
        let config = self
            .frontend
            .state
            .admin
            .dynamic_config
            .lock()
            .expect("admin lock");
        let enabled = cadence_persistence::dynamic_config::task_list_boolean(
            config.property_values("matching.enableGetNumberOfPartitionsFromCache"),
            domain_name_for(&self.frontend.state.domains, domain_id).as_deref(),
            name.root(),
            task_type,
            false,
        );
        drop(config);
        if enabled
            && queue.partitions.get().is_some_and(|config| {
                !config
                    .write_partitions
                    .contains_key(&(name.partition() as i32))
            })
        {
            return Err(crate::grpc::detailed_status(
                tonic::Code::Aborted,
                "Current partition is drained.",
                "ReadOnlyPartitionError",
                p::ReadOnlyPartitionError {},
            ));
        }
        Ok(())
    }
    /// Go `types/mapper/proto.fromRemoteSyncMatchedErr`: `CodeUnavailable`
    /// carrying an empty `uber.cadence.shared.v1.RemoteSyncMatchedError`
    /// detail with the engine message. A child partition receiving it persists
    /// the task itself instead of retrying.
    fn remote_sync_failed() -> Status {
        crate::grpc::detailed_status_type(
            tonic::Code::Unavailable,
            "remote sync match failed",
            "type.googleapis.com/uber.cadence.shared.v1.RemoteSyncMatchedError".to_string(),
            shared::RemoteSyncMatchedError {},
        )
    }
    /// Go `taskListManagerImpl.AddTask` sync flow with in-process partition
    /// forwarding. The entry partition tries its own pollers first (dispatch
    /// limited unless the add already arrived forwarded), then each ancestor
    /// up to the root tries its pollers with forwarded semantics, mirroring
    /// `matcher.Offer` chaining `ForwardTask` to the parent. Returns the
    /// entry owner's partition config when some level sync-matches. The outer
    /// option records success independently of optional partition metadata.
    /// A drained ancestor aborts the chain like Go's parent `AddTask`
    /// rejecting the forward; the caller then falls back to the entry queue.
    async fn sync_match_chain(
        &self,
        add: ForwardedAdd<'_>,
    ) -> Result<Option<Option<m::TaskListPartitionConfig>>, Status> {
        let ForwardedAdd {
            domain_id,
            list,
            task_type,
            data,
            source,
            entry,
            deadline,
            forwarded,
            backlog,
            task_source,
            schedule_to_start_timeout,
        } = add;
        let metric_context = crate::matching_metrics::PollMetricContext {
            domain: domain_name_for(&self.frontend.state.domains, domain_id)
                .unwrap_or_else(|| domain_id.to_owned()),
            task_list: list.name.clone(),
            task_list_kind: list.kind,
            task_type: if task_type == 0 {
                "decision_task"
            } else {
                "activity_task"
            },
            poller_isolation_group: String::new(),
        };
        let match_started = tokio::time::Instant::now();
        let trace_identity = format!(
            "workflow={} run={} schedule_id={}",
            data.workflow_id,
            data.run_id_string(),
            data.schedule_id
        );
        let name = task_list_name(&list.name)?;
        let degree = {
            let config = self
                .frontend
                .state
                .admin
                .dynamic_config
                .lock()
                .expect("admin lock");
            cadence_persistence::dynamic_config::task_list_integer(
                config.property_values("matching.forwarderMaxChildrenPerNode"),
                domain_name_for(&self.frontend.state.domains, domain_id).as_deref(),
                name.root(),
                task_type,
                20,
            )
            // Go task_list_manager clamps ForwarderMaxChildrenPerNode to >= 1.
            .max(1)
        };
        let mut list = list.clone();
        let mut handle = entry.clone();
        let mut ancestor = false;
        let mut permits = Vec::new();
        let mut in_process_forward_metrics: Vec<(
            tokio::time::Instant,
            crate::matching_metrics::PollMetricContext,
        )> = Vec::new();
        loop {
            let outcome = if ancestor || forwarded {
                if handle
                    .try_sync_match_forwarded(data, source, deadline)
                    .await
                {
                    crate::matching_registry::SyncMatchOutcome::Matched
                } else {
                    crate::matching_registry::SyncMatchOutcome::Unmatched
                }
            } else {
                handle.try_sync_match_observed(data, source, deadline).await
            };
            if outcome == crate::matching_registry::SyncMatchOutcome::Throttled {
                let mut context = metric_context.clone();
                context.task_list = list.name.clone();
                self.metrics.sync_throttled(&context);
            }
            if outcome == crate::matching_registry::SyncMatchOutcome::Matched {
                adaptive_scaler_trace(
                    &list.name,
                    "sync_match",
                    &format!(
                        "{trace_identity} outcome=matched elapsed_ms={}",
                        match_started.elapsed().as_millis()
                    ),
                );
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                self.metrics.sync_match_latency(
                    &metric_context,
                    ancestor || forwarded,
                    match_started.elapsed(),
                );
                return Ok(Some(
                    entry.partitions.get().map(partition_config_to_internal),
                ));
            }
            let name = TaskListName::parse(&list.name)
                .map_err(|_| Status::invalid_argument("TaskList is not valid."))?;
            if backlog && (ancestor || forwarded) && name.is_root() {
                let accepted = handle.offer_forwarded_backlog(data, source, deadline).await;
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                return Ok(
                    accepted.then(|| entry.partitions.get().map(partition_config_to_internal))
                );
            }
            if name.is_root() || list.kind == 2 || tokio::time::Instant::now() >= deadline {
                adaptive_scaler_trace(
                    &list.name,
                    "sync_match",
                    &format!(
                        "{trace_identity} outcome=unmatched elapsed_ms={}",
                        match_started.elapsed().as_millis()
                    ),
                );
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                return Ok(None);
            }
            let parent = name.parent(degree);
            if parent.is_empty() {
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                return Ok(None);
            }
            let mut forward_context = metric_context.clone();
            forward_context.task_list = list.name.clone();
            let permit = match handle.try_forward_task_observed() {
                crate::matching_registry::ForwardTaskPermitOutcome::Acquired(permit) => permit,
                crate::matching_registry::ForwardTaskPermitOutcome::NoToken => {
                    for (started, context) in in_process_forward_metrics.drain(..) {
                        self.metrics
                            .forward_task_latency(&context, started.elapsed());
                    }
                    return Ok(None);
                }
                crate::matching_registry::ForwardTaskPermitOutcome::RateLimited => {
                    self.metrics.sync_forward_throttled(&forward_context);
                    for (started, context) in in_process_forward_metrics.drain(..) {
                        self.metrics
                            .forward_task_latency(&context, started.elapsed());
                    }
                    return Ok(None);
                }
            };
            permits.push(permit);
            let forwarded_from = list.name.clone();
            list.name = parent.clone();
            if let Some(channel) = self.remote_channel_for_task_list(&list.name).await? {
                let execution = Some(p::WorkflowExecution {
                    workflow_id: data.workflow_id.clone(),
                    run_id: data.run_id_string(),
                });
                let partition_config = data
                    .partition_config
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                let mut client = m::matching_api_client::MatchingApiClient::new(channel);
                let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
                let forward_started = tokio::time::Instant::now();
                let result = if task_type == 0 {
                    let mut request = Request::new(m::AddDecisionTaskRequest {
                        domain_id: source.to_string(),
                        workflow_execution: execution,
                        task_list: Some(list.clone()),
                        schedule_id: data.schedule_id,
                        schedule_to_start_timeout,
                        source: task_source,
                        forwarded_from,
                        partition_config,
                    });
                    request.set_timeout(timeout);
                    crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
                    client
                        .add_decision_task(request)
                        .await
                        .map(|response| response.into_inner().partition_config)
                } else {
                    let mut request = Request::new(m::AddActivityTaskRequest {
                        domain_id: domain_id.into(),
                        source_domain_id: source.to_string(),
                        workflow_execution: execution,
                        task_list: Some(list.clone()),
                        schedule_id: data.schedule_id,
                        schedule_to_start_timeout,
                        source: task_source,
                        forwarded_from,
                        partition_config,
                        activity_task_dispatch_info: None,
                    });
                    request.set_timeout(timeout);
                    crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
                    client
                        .add_activity_task(request)
                        .await
                        .map(|response| response.into_inner().partition_config)
                };
                self.metrics
                    .forward_task_latency(&forward_context, forward_started.elapsed());
                adaptive_scaler_trace(
                    &list.name,
                    "forward_attempt",
                    &format!(
                        "{trace_identity} elapsed_ms={} status={:?} ack={}",
                        forward_started.elapsed().as_millis(),
                        result.as_ref().err().map(|status| status.code()),
                        result.is_ok()
                    ),
                );
                // Go matcher.Offer counts a parent ServiceBusy (ErrForwarderSlowDown) on the child.
                if let Err(status) = &result {
                    if matches!(
                        crate::matching_forward::ForwardError::classify(status.clone()),
                        crate::matching_forward::ForwardError::SlowDown
                    ) {
                        self.metrics.sync_forward_throttled(&forward_context);
                    }
                }
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                return Ok(result
                    .ok()
                    .map(|_| entry.partitions.get().map(partition_config_to_internal)));
            }
            in_process_forward_metrics.push((tokio::time::Instant::now(), forward_context));
            handle = match self.durable_queue(domain_id, &list, task_type).await {
                Ok(handle) => handle,
                Err(_) => {
                    for (started, context) in in_process_forward_metrics.drain(..) {
                        self.metrics
                            .forward_task_latency(&context, started.elapsed());
                    }
                    return Ok(None);
                }
            };
            if handle.stop_if_isolation_changed()
                || self
                    .check_write_partition(domain_id, &list, task_type, &handle)
                    .is_err()
            {
                for (started, context) in in_process_forward_metrics.drain(..) {
                    self.metrics
                        .forward_task_latency(&context, started.elapsed());
                }
                return Ok(None);
            }
            ancestor = true;
        }
    }
    fn domain_name(&self, id: &str) -> Result<String, Status> {
        // Go cache miss -> sql/nosql_domain_store checkDomainExists: typed BadRequest, two spaces.
        if id.is_empty() {
            return Err(Status::invalid_argument(
                "GetDomain operation failed.  Both ID and Name are empty.",
            ));
        }
        History {
            frontend: self.frontend.clone(),
        }
        .domain_name(id)
    }
    /// Thrift argument that Go's `Args.FromWire` left nil for a method whose engine dereferences
    /// it. The handler still runs startup wait, metrics scope, domain-name lookup of the empty ID
    /// and the host quota before the engine's nil
    /// dereference (a TChannel call always carries a deadline, so the poll's timeout-set check passes and
    /// the engine's own context check runs only after the dereference), which `CapturePanic` returns as a plain error (no failure counter: the panic
    /// skips `handleErr`).
    pub(crate) async fn thrift_nil_request(
        &self,
        method: &'static str,
        rpc: crate::matching_quota::MatchingRpc,
    ) -> Status {
        self.wait_started().await;
        let domain = self.metric_domain_name("").await;
        let outcome = self
            .metrics
            .matching_request(method, domain.as_deref(), None, None, async {
                self.check_rpc_quota(rpc, Some(""), None)?;
                Ok(())
            })
            .await;
        match outcome {
            Err(status) => status,
            Ok(()) => Status::unknown(crate::matching_panic::NIL_DEREFERENCE_MESSAGE),
        }
    }
    fn check_rpc_quota(
        &self,
        method: crate::matching_quota::MatchingRpc,
        domain_id: Option<&str>,
        domain_name: Option<&str>,
    ) -> Result<(), Status> {
        let resolved = domain_name
            .map(str::to_owned)
            .or_else(|| domain_id.map(|id| self.domain_name(id).unwrap_or_default()));
        if self.rpc_quota.allow(method, resolved.as_deref()) {
            Ok(())
        } else {
            Err(crate::grpc::detailed_status(
                tonic::Code::ResourceExhausted,
                "Matching host rps exceeded",
                "ServiceBusyError",
                cadence_proto::api::ServiceBusyError {
                    reason: String::new(),
                },
            ))
        }
    }
    fn register_poll(&self, key: PollKey) -> PollGuard {
        let cancel = Arc::new(Notify::new());
        // Parity with Go poller.Manager.StartPoll: a repeated poller ID replaces
        // the map entry and does not cancel the earlier poll.
        self.polls
            .lock()
            .expect("poll cancellation lock")
            .insert(key.clone(), cancel.clone());
        PollGuard {
            polls: self.polls.clone(),
            key,
            cancel,
        }
    }
}
#[tonic::async_trait]
impl m::matching_api_server::MatchingApi for Matching {
    async fn poll_for_decision_task(
        &self,
        request: Request<m::PollForDecisionTaskRequest>,
    ) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
        crate::matching_panic::capture::<Response<m::PollForDecisionTaskResponse>, Status>("poll_for_decision_task", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "poll_for_decision_task")?;
        self.metrics.matching_request(
            "PollForDecisionTask",
            self.metric_domain_name(request.get_ref().domain_id.as_str()).await.as_deref(),
            request.get_ref().request.as_ref().and_then(|r| r.task_list.as_ref()).map(|t| (t.name.as_str(), Some(t.kind))),
            Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(request.metadata(), "grpc")),
            async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::PollForDecisionTask, Some(&request.get_ref().domain_id), None)?;
        let incoming_deadline = crate::matching_sync::request_deadline(&request);
        if incoming_deadline.is_none() {
            self.throttled_logger.error("PollForDecisionTask", "Context timeout not set for long poll API.");
            return Err(Status::invalid_argument("Context timeout is not set."));
        }
        let r = request.get_ref().clone();
        let received_at = std::time::Instant::now();
        let mut payload = r.request.unwrap_or_default();
        // Go getters are nil-safe: an absent TaskList is the empty-named list, and the
        // identifier is validated before any domain lookup.
        poll_task_list_name("decision", payload.task_list.as_ref().map_or("", |t| t.name.as_str()))?;
        payload.task_list.get_or_insert_with(Default::default);
        let mut failover = self.frontend.state.domains.subscribe_failover(&r.domain_id);
        payload.domain = self.domain_name(&r.domain_id)?;
        check_poll_context(incoming_deadline)?;
        if !r.forwarded_from.is_empty() {
            self.metrics.forwarded_per_task_list(&crate::matching_metrics::PollMetricContext {
                domain: payload.domain.clone(),
                task_list: payload.task_list.as_ref().map(|task_list| task_list.name.clone()).unwrap_or_default(),
                task_list_kind: payload.task_list.as_ref().map(|task_list| task_list.kind).unwrap_or_default(),
                task_type: "decision_task",
                poller_isolation_group: r.isolation_group.clone(),
            });
        }
        let task_list = payload.task_list.as_ref().expect("defaulted").name.clone();
        adaptive_scaler_trace(
            &task_list,
            "poll_received",
            &format!("poller_id={} request_id=unavailable", r.poller_id),
        );
        let guard = self.register_poll(PollKey {
            domain: r.domain_id.clone(),
            task_list,
            task_type: 1,
            poller: r.poller_id,
        });
        if self.queues.is_some() {
            let queue = self
                .durable_queue(
                    &r.domain_id,
                    payload.task_list.as_ref().expect("validated task list"),
                    0,
                )
                .await?;
            self.emit_partition_runtime_metrics(
                &r.domain_id,
                payload.task_list.as_ref().expect("validated task list"),
                0,
                &queue,
            )?;
            let poll_started = std::time::Instant::now();
            adaptive_scaler_trace(&guard.key.task_list, "poll_wait_start", &format!("poller_id={} elapsed_ms={}", guard.key.poller, received_at.elapsed().as_millis()));
            let expiration =
                self.poll_timeout(&r.domain_id, &guard.key.task_list, 0, incoming_deadline);
            if queue.stop_if_isolation_changed() {
                return Ok(Response::new(m::PollForDecisionTaskResponse {
                    auto_config_hint: Some(self.auto_config_hint(
                        &r.domain_id,
                        &guard.key.task_list,
                        0,
                        poll_started,
                    )),
                    ..Default::default()
                }));
            }
            let rate = self.poll_rate(
                &r.domain_id,
                &guard.key.task_list,
                payload.task_list.as_ref().map(|task_list| task_list.kind).unwrap_or_default(),
                0,
                None,
                &queue.limiter,
            );
            let lifecycle = crate::matching_dispatch::PollLifecycle {
                manager: queue.pollers.clone(),
                id: guard.key.poller.clone(),
                cancel: Some(guard.cancel.clone()),
                info: cadence_common::poller::Info {
                    identity: payload.identity.clone(),
                    rate_per_second: rate,
                    isolation_group: r.isolation_group.clone(),
                },
                forwarded: !r.forwarded_from.is_empty(),
                metric_context: Some(crate::matching_metrics::PollMetricContext {
                    domain: payload.domain.clone(),
                    task_list: guard.key.task_list.clone(),
                    task_list_kind: payload
                        .task_list
                        .as_ref()
                        .map(|list| list.kind)
                        .unwrap_or_default(),
                    task_type: "decision_task",
                    poller_isolation_group: r.isolation_group.clone(),
                }),
                metrics: self.metrics.clone(),
            };
            // Go's matcher serves `queryTaskC` on the same long poll as the
            // decision backlog; a ready query is handed to this poller instead
            // of waiting for a durable task. Racing is safe: dropping the
            // dispatch future makes its worker rewrite any claimed task.
            let active = self
                .frontend
                .state
                .domains
                .is_active_in(&r.domain_id, &self.frontend.state.current_cluster_name)
                .map_err(|error| crate::grpc::domain_not_found(error.message))?;
            let _standby_wait = (!active).then(|| lifecycle.begin());
            let query_lifecycle = lifecycle.clone();
            return tokio::select! {
                biased;
                query=queue.queries.next()=>{
                    let _wait=query_lifecycle.begin();
                    // Go treats a query delivered from queryTaskC as a
                    // successful synchronous poll. It is a local match and
                    // does not carry the task-isolation partition metadata.
                    if let Some(context) = query_lifecycle.metric_context.as_ref() {
                        query_lifecycle.metrics.poll_success(context, true);
                        query_lifecycle.metrics.poll_match(context, false, false, None);
                    }
                    // Go builds the response from `TaskList.Name` / `Query.Request.Query`;
                    // a nil field panics the poller with a plain runtime error. The
                    // query is consumed, never completed, and its waiter keeps waiting.
                    if !query.shape.is_valid() {
                        return Err(Status::unknown(crate::matching_panic::NIL_DEREFERENCE_MESSAGE));
                    }
                    let execution=query.request.workflow_execution.clone().unwrap_or_default();
                    let query_state = queue
                        .dispatcher
                        .query_mutable_state_with_deadline(&execution, incoming_deadline);
                    let query_state = if let Some(deadline) = incoming_deadline {
                        tokio::time::timeout_at(deadline, query_state)
                            .await
                            .unwrap_or_else(|_| Err(Status::deadline_exceeded("Matching poll deadline exceeded.")))
                    } else {
                        query_state.await
                    };
                    match query_state {
                        Ok(state)=>{
                            let mut response=crate::matching_query::query_response(query, &state);
                            response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                            response.auto_config_hint=Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started));
                            Ok(Response::new(response))
                        }
                        // Go fails the waiting query and returns an empty poll.
                        Err(error)=>{
                            let _=queue.queries.complete(&query.task_id, crate::matching_query::QueryOutcome::Internal(error));
                            Ok(Response::new(m::PollForDecisionTaskResponse { auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)), load_balancer_hints: queue.load_balancer_hints(matching_now()), partition_config: queue.partitions.get().map(partition_config_to_internal), ..Default::default() }))
                        }
                    }
                }
                result=async {
                    if active {
                        queue.dispatcher.poll_decision_with_poller(payload, expiration, lifecycle, incoming_deadline).await
                    } else {
                        // Go standby getTask polls only queryTaskC, never the
                        // durable backlog or forwarded workflow tasks.
                        // The query poll is forwarded to the parent (PollForQuery).
                        queue.dispatcher.forward_query_poll(payload, expiration, lifecycle).await
                    }
                }=>
                    result.map(|mut response| {
                        adaptive_scaler_trace(
                            &guard.key.task_list,
                            "poll_result",
                            &format!(
                                "poller_id={} request_id=unavailable elapsed_ms={} token_present={} workflow={} run={} started_event_id={}",
                                guard.key.poller,
                                poll_started.elapsed().as_millis(),
                                !response.task_token.is_empty(),
                                response.workflow_execution.as_ref().map_or("", |execution| execution.workflow_id.as_str()),
                                response.workflow_execution.as_ref().map_or("", |execution| execution.run_id.as_str()),
                                response.started_event_id
                            ),
                        );
                        response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                        response.auto_config_hint=Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started));
                        Response::new(response)
                    }).inspect_err(|error| {
                        adaptive_scaler_trace(&guard.key.task_list, "poll_error", &format!("poller_id={} elapsed_ms={} code={:?}", guard.key.poller, poll_started.elapsed().as_millis(), error.code()));
                    }),
                _=failover.changed()=>{
                    if let (Some(registry), Ok(domain)) = (&self.queues, uuid::Uuid::parse_str(&r.domain_id)) {
                        registry.release_blocked_pollers_for_domain(domain);
                    }
                    Ok(Response::new(m::PollForDecisionTaskResponse {
                    auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                    load_balancer_hints: queue.load_balancer_hints(matching_now()),
                    partition_config: queue.partitions.get().map(partition_config_to_internal),
                    ..Default::default()
                    }))
                },
                // Go returns an empty poll response (nil error) when a poll is cancelled.
                _=guard.cancel.notified()=>Ok(Response::new(m::PollForDecisionTaskResponse {
                    // Cancellation returns the empty response expected by Go long polls.
                    // Keep this bounded trace to distinguish it from a token handoff.
                    //
                    // The trace contains no task token or payload.
                    auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                    load_balancer_hints: queue.load_balancer_hints(matching_now()),
                    partition_config: queue.partitions.get().map(partition_config_to_internal),
                    ..Default::default()
                })).inspect(|_| {
                    adaptive_scaler_trace(&guard.key.task_list, "poll_cancelled", &format!("poller_id={} elapsed_ms={}", guard.key.poller, poll_started.elapsed().as_millis()));
                }),
            };
        }
        let expiration =
            self.poll_timeout(&r.domain_id, &guard.key.task_list, 0, incoming_deadline);
        let poll_started = std::time::Instant::now();
        let v = tokio::select! {
            result=tokio::time::timeout(expiration, p::worker_api_server::WorkerApi::poll_for_decision_task(&self.frontend,crate::grpc::with_isolation_group(payload, &r.isolation_group)))=>match result { Ok(result)=>result?.into_inner(), Err(_)=>p::PollForDecisionTaskResponse::default() },
            // Go returns an empty poll response (nil error) when a poll is cancelled.
            _=guard.cancel.notified()=>return Ok(Response::new(m::PollForDecisionTaskResponse {
                auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                ..Default::default()
            })),
        };
        let sticky_execution_enabled = !v.task_token.is_empty()
            && v.history
                .as_ref()
                .and_then(|history| history.events.first())
                .is_some_and(|event| event.event_id > 1);
        // The embedded engine plays Matching's role, so it stamps the same
        // auto-config hint Matching's matcher would (decision type either way).
        let auto_config_hint = v.auto_config_hint.or_else(|| {
            Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started))
        });
        Ok(Response::new(m::PollForDecisionTaskResponse {
            sticky_execution_enabled,
            task_token: v.task_token,
            workflow_execution: v.workflow_execution,
            workflow_type: v.workflow_type,
            previous_started_event_id: v.previous_started_event_id,
            started_event_id: v.started_event_id,
            attempt: v
                .attempt
                .try_into()
                .map_err(|_| Status::internal("Decision attempt overflow."))?,
            next_event_id: v.next_event_id,
            backlog_count_hint: v.backlog_count_hint,
            query: v.query,
            workflow_execution_task_list: v.workflow_execution_task_list,
            scheduled_time: v.scheduled_time,
            started_time: v.started_time,
            queries: v.queries,
            total_history_bytes: v.total_history_bytes,
            auto_config_hint,
            ..Default::default()
        }))
            }).await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn poll_for_activity_task(
        &self,
        request: Request<m::PollForActivityTaskRequest>,
    ) -> Result<Response<m::PollForActivityTaskResponse>, Status> {
        crate::matching_panic::capture::<Response<m::PollForActivityTaskResponse>, Status>("poll_for_activity_task", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "poll_for_activity_task")?;
        self.metrics.matching_request(
            "PollForActivityTask",
            self.metric_domain_name(request.get_ref().domain_id.as_str()).await.as_deref(),
            request.get_ref().request.as_ref().and_then(|r| r.task_list.as_ref()).map(|t| (t.name.as_str(), Some(t.kind))),
            Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(request.metadata(), "grpc")),
            async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::PollForActivityTask, Some(&request.get_ref().domain_id), None)?;
        let incoming_deadline = crate::matching_sync::request_deadline(&request);
        if incoming_deadline.is_none() {
            self.throttled_logger.error("PollForActivityTask", "Context timeout not set for long poll API.");
            return Err(Status::invalid_argument("Context timeout is not set."));
        }
        let r = request.get_ref().clone();
        let Some(mut payload) = r.request else {
            // Go PollForActivityTask reads `request.GetTaskList()` nil-safely, validates the empty
            // identifier, then dereferences `request.TaskListMetadata` (engine.go:915) before
            // `getOrCreateTaskListManager` (engine.go:928), so a domain lookup never runs first. The
            // runtime panic is captured by the handler as a plain error (Go oracle run
            // 40175488464: `runtime error: invalid memory address or nil pointer dereference`).
            check_poll_context(incoming_deadline)?;
            return Err(Status::unknown(crate::matching_panic::NIL_DEREFERENCE_MESSAGE));
        };
        // Go getters are nil-safe: an absent TaskList is the empty-named list, and the
        // identifier is validated before any domain lookup.
        poll_task_list_name("activity", payload.task_list.as_ref().map_or("", |t| t.name.as_str()))?;
        payload.task_list.get_or_insert_with(Default::default);
        let mut failover = self.frontend.state.domains.subscribe_failover(&r.domain_id);
        payload.domain = self.domain_name(&r.domain_id)?;
        check_poll_context(incoming_deadline)?;
        if !r.forwarded_from.is_empty() {
            self.metrics.forwarded_per_task_list(&crate::matching_metrics::PollMetricContext {
                domain: payload.domain.clone(),
                task_list: payload.task_list.as_ref().map(|task_list| task_list.name.clone()).unwrap_or_default(),
                task_list_kind: payload.task_list.as_ref().map(|task_list| task_list.kind).unwrap_or_default(),
                task_type: "activity_task",
                poller_isolation_group: r.isolation_group.clone(),
            });
        }
        let task_list = payload.task_list.as_ref().expect("defaulted").name.clone();
        let guard = self.register_poll(PollKey {
            domain: r.domain_id.clone(),
            task_list,
            task_type: 2,
            poller: r.poller_id,
        });
        if self.queues.is_some() {
            let queue = self
                .durable_queue(
                    &r.domain_id,
                    payload.task_list.as_ref().expect("validated task list"),
                    1,
                )
                .await?;
            self.emit_partition_runtime_metrics(
                &r.domain_id,
                payload.task_list.as_ref().expect("validated task list"),
                1,
                &queue,
            )?;
            let poll_started = std::time::Instant::now();
            let expiration =
                self.poll_timeout(&r.domain_id, &guard.key.task_list, 1, incoming_deadline);
            if queue.stop_if_isolation_changed() {
                return Ok(Response::new(m::PollForActivityTaskResponse {
                    auto_config_hint: Some(self.auto_config_hint(
                        &r.domain_id,
                        &guard.key.task_list,
                        0,
                        poll_started,
                    )),
                    ..Default::default()
                }));
            }
            // Go `getTask`: the worker-reported rate is applied to the shared
            // dispatch limiter before the poll is recorded; a missing or
            // negative metadata rate keeps the configured default for poller
            // history without resetting the shared limiter.
            let max_dispatch = payload
                .task_list_metadata
                .as_ref()
                .and_then(|metadata| metadata.max_tasks_per_second);
            let rate = self.poll_rate(
                &r.domain_id,
                &guard.key.task_list,
                payload.task_list.as_ref().map(|task_list| task_list.kind).unwrap_or_default(),
                1,
                max_dispatch,
                &queue.limiter,
            );
            let lifecycle = crate::matching_dispatch::PollLifecycle {
                manager: queue.pollers.clone(),
                id: guard.key.poller.clone(),
                cancel: Some(guard.cancel.clone()),
                info: cadence_common::poller::Info {
                    identity: payload.identity.clone(),
                    rate_per_second: rate,
                    isolation_group: r.isolation_group.clone(),
                },
                forwarded: !r.forwarded_from.is_empty(),
                metric_context: Some(crate::matching_metrics::PollMetricContext {
                    domain: payload.domain.clone(),
                    task_list: guard.key.task_list.clone(),
                    task_list_kind: payload
                        .task_list
                        .as_ref()
                        .map(|list| list.kind)
                        .unwrap_or_default(),
                    task_type: "activity_task",
                    poller_isolation_group: r.isolation_group.clone(),
                }),
                metrics: self.metrics.clone(),
            };
            return tokio::select! {
                result=async {
                    let active = self.frontend.state.domains.is_active_in(&r.domain_id, &self.frontend.state.current_cluster_name)
                        .map_err(|error| crate::grpc::domain_not_found(error.message))?;
                    if active {
                        queue.dispatcher.poll_activity_with_poller(payload, expiration, lifecycle, incoming_deadline).await
                    } else {
                        let _wait = lifecycle.begin();
                        tokio::time::sleep(expiration).await;
                        Ok(m::PollForActivityTaskResponse::default())
                    }
                }=>
                    result.map(|mut response| {
                        response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                        // Go's activity-delivered hint uses the manager's own
                        // activity type; its empty-poll branch passes the
                        // decision type even here.
                        let task_type = if response.task_token.is_empty() { 0 } else { 1 };
                        response.auto_config_hint=Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, task_type, poll_started));
                        Response::new(response)
                    }),
                _=async {
                    if queue.queue.task_list_kind() == 0 {
                        failover.changed().await
                    } else {
                        std::future::pending().await
                    }
                }=>{
                    if let (Some(registry), Ok(domain)) = (&self.queues, uuid::Uuid::parse_str(&r.domain_id)) {
                        registry.release_blocked_pollers_for_domain(domain);
                    }
                    Ok(Response::new(m::PollForActivityTaskResponse {
                    auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                    load_balancer_hints: queue.load_balancer_hints(matching_now()),
                    partition_config: queue.partitions.get().map(partition_config_to_internal),
                    ..Default::default()
                    }))
                },
                // Go returns an empty poll response (nil error) when a poll is cancelled.
                _=guard.cancel.notified()=>Ok(Response::new(m::PollForActivityTaskResponse {
                    auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                    load_balancer_hints: queue.load_balancer_hints(matching_now()),
                    partition_config: queue.partitions.get().map(partition_config_to_internal),
                    ..Default::default()
                })),
            };
        }
        let expiration =
            self.poll_timeout(&r.domain_id, &guard.key.task_list, 1, incoming_deadline);
        let poll_started = std::time::Instant::now();
        let v = tokio::select! {
            result=tokio::time::timeout(expiration, p::worker_api_server::WorkerApi::poll_for_activity_task(&self.frontend,crate::grpc::with_isolation_group(payload, &r.isolation_group)))=>match result { Ok(result)=>result?.into_inner(), Err(_)=>p::PollForActivityTaskResponse::default() },
            // Go returns an empty poll response (nil error) when a poll is cancelled.
            _=guard.cancel.notified()=>return Ok(Response::new(m::PollForActivityTaskResponse {
                auto_config_hint: Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started)),
                ..Default::default()
            })),
        };
        // Matching stamps a delivered activity with its own type, but an empty
        // poll with the decision type; the embedded engine mirrors that.
        let auto_config_task_type = if v.task_token.is_empty() { 0 } else { 1 };
        let auto_config_hint = v.auto_config_hint.or_else(|| {
            Some(self.auto_config_hint(
                &r.domain_id,
                &guard.key.task_list,
                auto_config_task_type,
                poll_started,
            ))
        });
        Ok(Response::new(m::PollForActivityTaskResponse {
            task_token: v.task_token,
            workflow_execution: v.workflow_execution,
            activity_id: v.activity_id,
            activity_type: v.activity_type,
            input: v.input,
            scheduled_time: v.scheduled_time,
            started_time: v.started_time,
            schedule_to_close_timeout: v.schedule_to_close_timeout,
            start_to_close_timeout: v.start_to_close_timeout,
            heartbeat_timeout: v.heartbeat_timeout,
            attempt: v.attempt,
            scheduled_time_of_this_attempt: v.scheduled_time_of_this_attempt,
            heartbeat_details: v.heartbeat_details,
            workflow_type: v.workflow_type,
            workflow_domain: v.workflow_domain,
            header: v.header,
            auto_config_hint,
            ..Default::default()
        }))
            }).await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn add_decision_task(
        &self,
        request: Request<m::AddDecisionTaskRequest>,
    ) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        crate::matching_panic::capture::<Response<m::AddDecisionTaskResponse>, Status>("add_decision_task", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "add_decision_task")?;
        self.metrics
            .matching_request(
                "AddDecisionTask",
                self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                    .as_deref(),
                request
                    .get_ref()
                    .task_list
                    .as_ref()
                    .map(|t| (t.name.as_str(), Some(t.kind))),
                Some(
                    crate::matching_metrics::MatchingMetrics::context_from_metadata(
                        request.metadata(),
                        "grpc",
                    ),
                ),
                async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::AddDecisionTask, Some(&request.get_ref().domain_id), None)?;
                    let add_started = tokio::time::Instant::now();
                    let parent_deadline = crate::matching_sync::request_deadline(&request);
                    let r = request.get_ref().clone();
                    let trace_ids = format!(
                        "workflow={} run={} schedule_id={}",
                        r.workflow_execution.as_ref().map_or("", |execution| execution.workflow_id.as_str()),
                        r.workflow_execution.as_ref().map_or("", |execution| execution.run_id.as_str()),
                        r.schedule_id,
                    );
                    let trace_task_list = r.task_list.as_ref().map_or("", |list| list.name.as_str());
                    adaptive_scaler_trace(
                        trace_task_list,
                        "decision_add_received",
                        &format!("{trace_ids} request_id=unavailable"),
                    );
                    log_received_add_task("Received AddDecisionTask", &r.domain_id, r.task_list.as_ref(), r.workflow_execution.as_ref(), 0, r.schedule_id);
                    task_list_name(
                        r.task_list
                            .as_ref()
                            .map(|t| t.name.as_str())
                            .unwrap_or_default(),
                    )?;
                    let domain = self.domain_name(&r.domain_id)?;
                    if !r.forwarded_from.is_empty() {
                        self.metrics.forwarded_per_task_list(
                            &crate::matching_metrics::PollMetricContext {
                                domain: domain.clone(),
                                task_list: r
                                    .task_list
                                    .as_ref()
                                    .map(|task_list| task_list.name.clone())
                                    .unwrap_or_default(),
                                task_list_kind: r
                                    .task_list
                                    .as_ref()
                                    .map(|task_list| task_list.kind)
                                    .unwrap_or_default(),
                                task_type: "decision_task",
                                poller_isolation_group: String::new(),
                            },
                        );
                    }
                    self.metrics.add_task_request(
                        &domain,
                        r.task_list
                            .as_ref()
                            .map(|task_list| task_list.name.as_str())
                            .unwrap_or_default(),
                        r.task_list
                            .as_ref()
                            .map(|task_list| task_list.kind)
                            .unwrap_or_default(),
                        !r.forwarded_from.is_empty(),
                        "decision_task",
                        matching_host_name(),
                    );
                    if r.task_list.as_ref().map_or(0, |t| t.kind) <= 1 && r.forwarded_from.is_empty() {
                        log_tasklist_counter("Emitting tasklist counter on decision task", &r.domain_id, r.task_list.as_ref());
                    }
                    if self.queues.is_some() {
                        // Go Add* use nil-safe getters: an absent TaskList or Execution is
                        // empty and still accepted (Go oracle run 40175488464).
                        let default_list = p::TaskList::default();
                        let list = r.task_list.as_ref().unwrap_or(&default_list);
                        let default_execution = p::WorkflowExecution::default();
                        let execution = r.workflow_execution.as_ref().unwrap_or(&default_execution);
                        let source = uuid::Uuid::parse_str(&r.domain_id).expect("domain validated");
                        let queue = self.durable_queue(&r.domain_id, list, 0).await?;
                        if queue.stop_if_isolation_changed() {
                            return Err(Status::internal("task list shutting down"));
                        }
                        self.check_write_partition(&r.domain_id, list, 0, &queue)?;
                        check_sticky_poller(&queue, list)?;
                        let data = cadence_persistence::matching_blob::TaskData {
                            workflow_id: execution.workflow_id.clone(),
                            schedule_id: r.schedule_id,
                            partition_config: if r.partition_config.is_empty() {
                                None
                            } else {
                                Some(r.partition_config.into_iter().collect())
                            },
                            ..Default::default()
                        }
                        .with_request_run_id(&execution.run_id)
                        .at_creation(matching_now(), 0);
                        let forwarded = !r.forwarded_from.is_empty();
                        // Go `taskListManagerImpl.AddTask`: only a non-forwarded add marks
                        // liveness and reports arrival QPS, grouped by the task's isolation
                        // group when it has one.
                        if !forwarded {
                            report_arrival(&queue, &data);
                            self.metrics.task_list_gauge(
                                crate::matching_metrics::ESTIMATED_ADD_TASK_QPS,
                                &crate::matching_metrics::PollMetricContext {
                                    domain: domain.clone(), task_list: list.name.clone(),
                                    task_list_kind: list.kind, task_type: "decision_task",
                                    poller_isolation_group: String::new(),
                                }, queue.qps.qps());
                        }
                        // Go `trySyncMatch`: a non-forwarded add spends at most 200ms
                        // keeping 1s of tailroom, while a forwarded add trusts the incoming
                        // context as is.
                        let deadline = if forwarded {
                            parent_deadline.unwrap_or_else(|| {
                                tokio::time::Instant::now() + std::time::Duration::from_millis(200)
                            })
                        } else {
                            let remaining = parent_deadline.map(|deadline| {
                                deadline.saturating_duration_since(tokio::time::Instant::now())
                            });
                            tokio::time::Instant::now()
                                + crate::matching_sync::sync_match_budget(remaining)
                        };
                        let active = self
                            .frontend
                            .state
                            .domains
                            .is_active_in(
                                &source.to_string(),
                                &self.frontend.state.current_cluster_name,
                            )
                            .map_err(|error| crate::grpc::domain_not_found(error.message))?;
                        adaptive_scaler_trace(
                            &list.name,
                            "decision_add_ready_for_sync",
                            &format!("{trace_ids} elapsed_ms={}", add_started.elapsed().as_millis()),
                        );
                        if active {
                            adaptive_scaler_trace(&list.name, "decision_add_sync_start", &trace_ids);
                            let matched = self
                                .sync_match_chain(ForwardedAdd {
                                    domain_id: &r.domain_id,
                                    list,
                                    task_type: 0,
                                    data: &data,
                                    source,
                                    entry: &queue,
                                    deadline,
                                    forwarded,
                                    backlog: r.source == 2,
                                    task_source: r.source,
                                    schedule_to_start_timeout: r.schedule_to_start_timeout,
                                })
                                .await?;
                            adaptive_scaler_trace(&list.name, "decision_add_sync_result", &format!("{trace_ids} matched={} elapsed_ms={}", matched.is_some(), add_started.elapsed().as_millis()));
                            if let Some(partition_config) = matched {
                                // Go records the handler-level latency only when AddTask sync matched.
                                self.metrics.handler_sync_match_latency(
                                    "AddDecisionTask",
                                    Some(domain.as_str()),
                                    r.task_list.as_ref().map(|t| (t.name.as_str(), Some(t.kind))),
                                    Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                        request.metadata(),
                                        "grpc",
                                    )),
                                    add_started.elapsed(),
                                );
                                return Ok(Response::new(m::AddDecisionTaskResponse {
                                    partition_config,
                                }));
                            }
                        }
                        // Go `errRemoteSyncMatchFailed`: a forwarded add never persists;
                        // the child partition keeps the task when sync matching fails.
                        if forwarded {
                            return Err(Self::remote_sync_failed());
                        }
                        let enqueue_started = std::time::Instant::now();
                        let enqueue_result = queue
                            .enqueue(
                                data,
                                source,
                                r.schedule_to_start_timeout
                                    .map(|timeout| timeout.seconds as i32)
                                    .unwrap_or_default(),
                            )
                            .await;
                        adaptive_scaler_trace(&list.name, "decision_add_enqueue", &format!("{trace_ids} elapsed_ms={} status={}", enqueue_started.elapsed().as_millis(), if enqueue_result.is_ok() { "ok" } else { "error" }));
                        enqueue_result?;
                        return Ok(Response::new(m::AddDecisionTaskResponse {
                            partition_config: queue
                                .partitions
                                .get()
                                .map(partition_config_to_internal),
                        }));
                    }
                    let execution = r.workflow_execution.unwrap_or_default();
                    let store = self
                        .frontend
                        .state
                        .execution_store(&domain)
                        .map_err(|e| crate::grpc::domain_not_found(e.message))?;
                    match store.requeue_decision_task(
                        &domain,
                        &execution.workflow_id,
                        &execution.run_id,
                        r.schedule_id,
                    ) {
                        Ok(()) => Ok(Response::new(m::AddDecisionTaskResponse {
                            partition_config: None,
                        })),
                        Err(
                            cadence_persistence::TaskError::InvalidToken(e)
                            | cadence_persistence::TaskError::BadDecision(e),
                        ) => Err(Status::invalid_argument(e.message)),
                        Err(cadence_persistence::TaskError::NotFound(e)) => {
                            Err(Status::not_found(e.message))
                        }
                        Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                            Err(Status::failed_precondition(e.message))
                        }
                    }
                },
            )
            .await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn add_activity_task(
        &self,
        request: Request<m::AddActivityTaskRequest>,
    ) -> Result<Response<m::AddActivityTaskResponse>, Status> {
        crate::matching_panic::capture::<Response<m::AddActivityTaskResponse>, Status>("add_activity_task", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "add_activity_task")?;
        self.metrics
            .matching_request(
                "AddActivityTask",
                self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                    .as_deref(),
                request
                    .get_ref()
                    .task_list
                    .as_ref()
                    .map(|t| (t.name.as_str(), Some(t.kind))),
                Some(
                    crate::matching_metrics::MatchingMetrics::context_from_metadata(
                        request.metadata(),
                        "grpc",
                    ),
                ),
                async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::AddActivityTask, Some(&request.get_ref().domain_id), None)?;
                    let add_started = tokio::time::Instant::now();
                    let parent_deadline = crate::matching_sync::request_deadline(&request);
                    let r = request.get_ref().clone();
                    log_received_add_task("Received AddActivityTask", &r.domain_id, r.task_list.as_ref(), r.workflow_execution.as_ref(), 1, r.schedule_id);
                    task_list_name(
                        r.task_list
                            .as_ref()
                            .map(|t| t.name.as_str())
                            .unwrap_or_default(),
                    )?;
                    let domain = self.domain_name(&r.domain_id)?;
                    if !r.forwarded_from.is_empty() {
                        self.metrics.forwarded_per_task_list(
                            &crate::matching_metrics::PollMetricContext {
                                domain: domain.clone(),
                                task_list: r
                                    .task_list
                                    .as_ref()
                                    .map(|task_list| task_list.name.clone())
                                    .unwrap_or_default(),
                                task_list_kind: r
                                    .task_list
                                    .as_ref()
                                    .map(|task_list| task_list.kind)
                                    .unwrap_or_default(),
                                task_type: "activity_task",
                                poller_isolation_group: String::new(),
                            },
                        );
                    }
                    self.metrics.add_task_request(
                        &domain,
                        r.task_list
                            .as_ref()
                            .map(|task_list| task_list.name.as_str())
                            .unwrap_or_default(),
                        r.task_list
                            .as_ref()
                            .map(|task_list| task_list.kind)
                            .unwrap_or_default(),
                        !r.forwarded_from.is_empty(),
                        "activity_task",
                        matching_host_name(),
                    );
                    if r.task_list.as_ref().map_or(0, |t| t.kind) <= 1 && r.forwarded_from.is_empty() {
                        log_tasklist_counter("Emitting tasklist counter on activity task", &r.domain_id, r.task_list.as_ref());
                    }
                    if self.queues.is_some() {
                        // Go Add* use nil-safe getters: an absent TaskList or Execution is
                        // empty and still accepted (Go oracle run 40175488464).
                        let default_list = p::TaskList::default();
                        let list = r.task_list.as_ref().unwrap_or(&default_list);
                        let default_execution = p::WorkflowExecution::default();
                        let execution = r.workflow_execution.as_ref().unwrap_or(&default_execution);
                        self.domain_name(&r.source_domain_id)?;
                        let source = uuid::Uuid::parse_str(&r.source_domain_id).map_err(|_| {
                            Status::invalid_argument("Source domain UUID is not valid.")
                        })?;
                        let queue = self.durable_queue(&r.domain_id, list, 1).await?;
                        if queue.stop_if_isolation_changed() {
                            return Err(Status::internal("task list shutting down"));
                        }
                        self.check_write_partition(&r.domain_id, list, 1, &queue)?;
                        let data = cadence_persistence::matching_blob::TaskData {
                            workflow_id: execution.workflow_id.clone(),
                            schedule_id: r.schedule_id,
                            partition_config: if r.partition_config.is_empty() {
                                None
                            } else {
                                Some(r.partition_config.into_iter().collect())
                            },
                            ..Default::default()
                        }
                        .with_request_run_id(&execution.run_id)
                        .at_creation(matching_now(), 0);
                        let forwarded = !r.forwarded_from.is_empty();
                        if !forwarded {
                            report_arrival(&queue, &data);
                            self.metrics.task_list_gauge(
                                crate::matching_metrics::ESTIMATED_ADD_TASK_QPS,
                                &crate::matching_metrics::PollMetricContext {
                                    domain: domain.clone(), task_list: list.name.clone(),
                                    task_list_kind: list.kind, task_type: "activity_task",
                                    poller_isolation_group: String::new(),
                                }, queue.qps.qps());
                        }
                        let deadline = if forwarded {
                            parent_deadline.unwrap_or_else(|| {
                                tokio::time::Instant::now() + std::time::Duration::from_millis(200)
                            })
                        } else {
                            let remaining = parent_deadline.map(|deadline| {
                                deadline.saturating_duration_since(tokio::time::Instant::now())
                            });
                            tokio::time::Instant::now()
                                + crate::matching_sync::sync_match_budget(remaining)
                        };
                        let active = self
                            .frontend
                            .state
                            .domains
                            .is_active_in(
                                &source.to_string(),
                                &self.frontend.state.current_cluster_name,
                            )
                            .map_err(|error| crate::grpc::domain_not_found(error.message))?;
                        if active {
                            let matched = self
                                .sync_match_chain(ForwardedAdd {
                                    domain_id: &r.domain_id,
                                    list,
                                    task_type: 1,
                                    data: &data,
                                    source,
                                    entry: &queue,
                                    deadline,
                                    forwarded,
                                    backlog: r.source == 2,
                                    task_source: r.source,
                                    schedule_to_start_timeout: r.schedule_to_start_timeout,
                                })
                                .await?;
                            if let Some(partition_config) = matched {
                                // Go records the handler-level latency only when AddTask sync matched.
                                self.metrics.handler_sync_match_latency(
                                    "AddActivityTask",
                                    Some(domain.as_str()),
                                    r.task_list.as_ref().map(|t| (t.name.as_str(), Some(t.kind))),
                                    Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                        request.metadata(),
                                        "grpc",
                                    )),
                                    add_started.elapsed(),
                                );
                                return Ok(Response::new(m::AddActivityTaskResponse {
                                    partition_config,
                                }));
                            }
                        }
                        if forwarded {
                            return Err(Self::remote_sync_failed());
                        }
                        queue
                            .enqueue(
                                data,
                                source,
                                r.schedule_to_start_timeout
                                    .map(|timeout| timeout.seconds as i32)
                                    .unwrap_or_default(),
                            )
                            .await?;
                        return Ok(Response::new(m::AddActivityTaskResponse {
                            partition_config: queue
                                .partitions
                                .get()
                                .map(partition_config_to_internal),
                        }));
                    }
                    let execution = r.workflow_execution.unwrap_or_default();
                    let store = self
                        .frontend
                        .state
                        .execution_store(&domain)
                        .map_err(|e| crate::grpc::domain_not_found(e.message))?;
                    match store.requeue_activity_task(
                        &domain,
                        &execution.workflow_id,
                        &execution.run_id,
                        r.schedule_id,
                    ) {
                        Ok(()) => Ok(Response::new(m::AddActivityTaskResponse {
                            partition_config: None,
                        })),
                        Err(
                            cadence_persistence::TaskError::InvalidToken(e)
                            | cadence_persistence::TaskError::BadDecision(e),
                        ) => Err(Status::invalid_argument(e.message)),
                        Err(cadence_persistence::TaskError::NotFound(e)) => {
                            Err(Status::not_found(e.message))
                        }
                        Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                            Err(Status::failed_precondition(e.message))
                        }
                    }
                },
            )
            .await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn query_workflow(
        &self,
        request: Request<m::QueryWorkflowRequest>,
    ) -> Result<Response<m::QueryWorkflowResponse>, Status> {
        crate::matching_panic::capture::<Response<m::QueryWorkflowResponse>, Status>(
            "query_workflow",
            async move {
                self.domain_lookup_guard(&request.get_ref().domain_id, "query_workflow")?;
                self.metrics
                    .matching_request(
                        "QueryWorkflow",
                        self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                            .as_deref(),
                        request
                            .get_ref()
                            .task_list
                            .as_ref()
                            .map(|t| (t.name.as_str(), Some(t.kind))),
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::QueryWorkflow,
                                Some(&request.get_ref().domain_id),
                                None,
                            )?;
                            let incoming_deadline =
                                crate::matching_sync::request_deadline(&request);
                            let r = request.get_ref().clone();
                            // Go getters are nil-safe: an absent QueryRequest, Query or
                            // TaskList dispatches normally and only the poller's response
                            // builder dereferences them. Keep the shape as runtime metadata.
                            let mut shape = crate::matching_query::QueryShape {
                                task_list_absent: r.task_list.is_none(),
                                query_absent: false,
                            };
                            let mut payload = r.request.unwrap_or_default();
                            shape.query_absent = payload.query.is_none();
                            payload.domain = self.domain_name(&r.domain_id)?;
                            if !r.forwarded_from.is_empty() {
                                self.metrics.forwarded_per_task_list(
                                    &crate::matching_metrics::PollMetricContext {
                                        domain: payload.domain.clone(),
                                        task_list: r
                                            .task_list
                                            .as_ref()
                                            .map(|task_list| task_list.name.clone())
                                            .unwrap_or_default(),
                                        task_list_kind: r
                                            .task_list
                                            .as_ref()
                                            .map(|task_list| task_list.kind)
                                            .unwrap_or_default(),
                                        task_type: "decision_task",
                                        poller_isolation_group: String::new(),
                                    },
                                );
                            }
                            // Go `waitForQueryResult(..., consistencyLevel == Strong, ...)`: the
                            // strong-consistency gate only applies to the local durable path.
                            let strong = payload.query_consistency_level
                                == p::QueryConsistencyLevel::Strong as i32;
                            if self.queues.is_some() {
                                // Go `matcher.OfferQuery`: offer to a waiting local decision
                                // poller, else forward up the partition chain with a forward-task
                                // token (no rate limit, unlike `ForwardTask`). Sticky lists and the
                                // root never forward and wait for a local poller until the deadline
                                // (Go has no fail-fast here). Permits are held until the query
                                // resolves, like Go releasing the token when `ForwardQueryTask` returns.
                                let task_list = r.task_list.clone().unwrap_or_default();
                                let name = task_list_name(&task_list.name)?;
                                let queue = self.durable_queue(&r.domain_id, &task_list, 0).await?;
                                check_sticky_poller(&queue, &task_list)?;
                                let task_id = uuid::Uuid::new_v4().to_string();
                                let root = name.root().to_owned();
                                let degree = {
                                    let config = self
                                        .frontend
                                        .state
                                        .admin
                                        .dynamic_config
                                        .lock()
                                        .expect("admin lock");
                                    cadence_persistence::dynamic_config::task_list_integer(
                                        config.property_values(
                                            "matching.forwarderMaxChildrenPerNode",
                                        ),
                                        domain_name_for(&self.frontend.state.domains, &r.domain_id)
                                            .as_deref(),
                                        root.as_str(),
                                        0,
                                        20,
                                    )
                                    // Go task_list_manager clamps ForwarderMaxChildrenPerNode to >= 1.
                                    .max(1)
                                };
                                let deadline = incoming_deadline.unwrap_or_else(|| {
                                    tokio::time::Instant::now() + self.poll_expiration
                                });
                                let mut list = task_list.clone();
                                // Go `resp.PartitionConfig = tlMgr.TaskListPartitionConfig()` on the
                                // task list the caller asked for, not on the partition it forwards to.
                                let partitions = queue.partitions.clone();
                                let mut handle = queue;
                                let mut permits = Vec::new();
                                let mut in_process_forward_metrics: Vec<(
                                    tokio::time::Instant,
                                    crate::matching_metrics::PollMetricContext,
                                )> = Vec::new();
                                let mut force_local = false;
                                loop {
                                    let query = crate::matching_query::PendingQuery {
                                        task_id: task_id.clone(),
                                        domain_id: r.domain_id.clone(),
                                        task_list: list.name.clone(),
                                        request: payload.clone(),
                                        shape,
                                    };
                                    let (receiver, _registration) =
                                        handle.queries.register_guarded(&task_id);
                                    let offered = if force_local {
                                        handle.queries.offer(query);
                                        true
                                    } else {
                                        handle.queries.try_offer(query)
                                    };
                                    if offered {
                                        let result =
                                            match tokio::time::timeout_at(deadline, receiver).await
                                            {
                                                Ok(Ok(result)) => {
                                                    crate::matching_query::query_outcome(
                                                        result, strong,
                                                    )
                                                    .map(|response| {
                                                        Response::new(m::QueryWorkflowResponse {
                                                            partition_config: partitions
                                                                .get()
                                                                .map(partition_config_to_proto),
                                                            ..response
                                                        })
                                                    })
                                                }
                                                Ok(Err(_)) => Err(Status::unavailable(
                                                    "Matching query channel closed.",
                                                )),
                                                // Go ctx expiry in waitForQueryResult/OfferQuery is a raw
                                                // ctx error, which handleErr maps to InternalServiceError.
                                                Err(_) => Err(Status::internal(
                                                    "context deadline exceeded",
                                                )),
                                            };
                                        for (started, context) in in_process_forward_metrics {
                                            self.metrics
                                                .forward_query_latency(&context, started.elapsed());
                                        }
                                        return result;
                                    }
                                    // `_registration` withdraws the unoffered entry; Go's
                                    // unbuffered send likewise leaves nothing behind on failure.
                                    let current =
                                        TaskListName::parse(&list.name).map_err(|_| {
                                            Status::invalid_argument("TaskList is not valid.")
                                        })?;
                                    // Go has no forwarder for root or sticky lists, so OfferQuery
                                    // blocks on the unbuffered queryTaskC send until a poller takes
                                    // the query or the RPC context ends. Wait on this level instead.
                                    if list.kind == 2 || current.is_root() {
                                        force_local = true;
                                        continue;
                                    }
                                    if tokio::time::Instant::now() >= deadline {
                                        for (started, context) in
                                            in_process_forward_metrics.drain(..)
                                        {
                                            self.metrics
                                                .forward_query_latency(&context, started.elapsed());
                                        }
                                        return Err(Status::internal("context deadline exceeded"));
                                    }
                                    let parent = current.parent(degree);
                                    if parent.is_empty() {
                                        for (started, context) in
                                            in_process_forward_metrics.drain(..)
                                        {
                                            self.metrics
                                                .forward_query_latency(&context, started.elapsed());
                                        }
                                        return Err(Status::internal(
                                            "cannot find parent task list for forwarding",
                                        ));
                                    }
                                    match handle.try_forward_query() {
                                        Some(permit) => {
                                            permits.push(permit);
                                            let forwarded_from = list.name.clone();
                                            let forwarded_from_name = forwarded_from.clone();
                                            let forward_context =
                                                crate::matching_metrics::PollMetricContext {
                                                    domain: payload.domain.clone(),
                                                    task_list: forwarded_from.clone(),
                                                    task_list_kind: list.kind,
                                                    task_type: "decision_task",
                                                    poller_isolation_group: String::new(),
                                                };
                                            list.name = parent.clone();
                                            // The forwarder builds a fresh TaskList for the parent.
                                            shape.task_list_absent = false;
                                            if let Some(channel) = self
                                                .remote_channel_for_task_list(&list.name)
                                                .await?
                                            {
                                                let mut request =
                                                    Request::new(m::QueryWorkflowRequest {
                                                        domain_id: r.domain_id.clone(),
                                                        request: Some(payload.clone()),
                                                        task_list: Some(list.clone()),
                                                        forwarded_from,
                                                    });
                                                request.set_timeout(
                                                    deadline.saturating_duration_since(
                                                        tokio::time::Instant::now(),
                                                    ),
                                                );
                                                let forward_started = tokio::time::Instant::now();
                                                let result =
                                                    m::matching_api_client::MatchingApiClient::new(
                                                        channel,
                                                    )
                                                    .query_workflow(request)
                                                    .await;
                                                self.metrics.forward_query_latency(
                                                    &forward_context,
                                                    forward_started.elapsed(),
                                                );
                                                // Go forwarder.handleErr maps a parent ServiceBusy to
                                                // ErrForwarderSlowDown: keep waiting for a local poller.
                                                if result.as_ref().err().is_some_and(
                                                    crate::matching_query::query_forward_slow_down,
                                                ) {
                                                    permits.pop();
                                                    force_local = true;
                                                    list.name = forwarded_from_name;
                                                    continue;
                                                }
                                                // Go overwrites the forwarded response's config with the local one.
                                                return result.map(|mut response| {
                                                    response.get_mut().partition_config =
                                                        partitions
                                                            .get()
                                                            .map(partition_config_to_proto);
                                                    response
                                                });
                                            }
                                            in_process_forward_metrics.push((
                                                tokio::time::Instant::now(),
                                                crate::matching_metrics::PollMetricContext {
                                                    domain: payload.domain.clone(),
                                                    task_list: forwarded_from,
                                                    task_list_kind: list.kind,
                                                    task_type: "decision_task",
                                                    poller_isolation_group: String::new(),
                                                },
                                            ));
                                            list.name = parent.clone();
                                            handle = match self
                                                .durable_queue(&r.domain_id, &list, 0)
                                                .await
                                            {
                                                Ok(handle) => handle,
                                                Err(error) => {
                                                    for (started, context) in
                                                        in_process_forward_metrics.drain(..)
                                                    {
                                                        self.metrics.forward_query_latency(
                                                            &context,
                                                            started.elapsed(),
                                                        );
                                                    }
                                                    return Err(error);
                                                }
                                            };
                                        }
                                        // Go blocks on the token while racing a local match; with
                                        // an exhausted pool wait on the current level instead.
                                        None => force_local = true,
                                    }
                                }
                            }
                            let v = p::workflow_api_server::WorkflowApi::query_workflow(
                                &self.frontend,
                                Request::new(payload),
                            )
                            .await?
                            .into_inner();
                            Ok(Response::new(m::QueryWorkflowResponse {
                                query_result: v.query_result,
                                query_rejected: v.query_rejected,
                                partition_config: None,
                            }))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn get_task_lists_by_domain(
        &self,
        request: Request<m::GetTaskListsByDomainRequest>,
    ) -> Result<Response<m::GetTaskListsByDomainResponse>, Status> {
        crate::matching_panic::capture::<Response<m::GetTaskListsByDomainResponse>, Status>(
            "get_task_lists_by_domain",
            async move {
                self.metrics
                    .matching_request(
                        "GetTaskListsByDomain",
                        Some(request.get_ref().domain.as_str()),
                        None,
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::GetTaskListsByDomain,
                                None,
                                Some(&request.get_ref().domain),
                            )?;
                            let r = request.get_ref().clone();
                            let domain = self.domain_id_by_name(&r.domain).await?;
                            if let Some(queues) = &self.queues {
                                let domain = uuid::Uuid::parse_str(&domain)
                                    .map_err(|_| Status::internal("Invalid domain ID."))?;
                                let all_kinds = {
                                    let config = self
                                        .frontend
                                        .state
                                        .admin
                                        .dynamic_config
                                        .lock()
                                        .expect("admin lock");
                                    cadence_persistence::dynamic_config::boolean(
                                        config.property_values(
                                            "matching.matchingReturnAllTaskListKinds",
                                        ),
                                        None,
                                        false,
                                    )
                                };
                                return Ok(Response::new(queues.describe_by_domain(
                                    domain,
                                    all_kinds,
                                    matching_now(),
                                )));
                            }
                            // Go enumerates loaded Matching managers, not frontend workflow
                            // state. Without a registry there are no loaded managers.
                            Ok(Response::new(m::GetTaskListsByDomainResponse::default()))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn list_task_list_partitions(
        &self,
        request: Request<m::ListTaskListPartitionsRequest>,
    ) -> Result<Response<m::ListTaskListPartitionsResponse>, Status> {
        crate::matching_panic::capture::<Response<m::ListTaskListPartitionsResponse>, Status>(
            "list_task_list_partitions",
            async move {
                self.metrics
                    .matching_request(
                        "ListTaskListPartitions",
                        Some(request.get_ref().domain.as_str()),
                        request
                            .get_ref()
                            .task_list
                            .as_ref()
                            .map(|t| (t.name.as_str(), Some(t.kind))),
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::ListTaskListPartitions,
                                None,
                                Some(&request.get_ref().domain),
                            )?;
                            let r = request.get_ref().clone();
                            let task_list = r.task_list.unwrap_or_default();
                            // Go resolves `request.GetDomain()` through the domain cache; the
                            // single-process store is keyed by name and yields the UUID.
                            let domain_id = self.domain_id_by_name(&r.domain).await?;
                            // Go Identifier returns an untyped error, wrapped by handleErr.
                            let name =
                                TaskListName::parse(&task_list.name).map_err(Status::internal)?;
                            #[cfg(test)]
                            self.injected_engine_fault("ListTaskListPartitions")?;
                            // Go getAllPartitions returns root first, then numbered write
                            // partitions, regardless of whether those managers are loaded.
                            let activity_task_list_partitions = self
                                .task_list_partition_metadata(&domain_id, &name, 1)
                                .await?;
                            let decision_task_list_partitions = self
                                .task_list_partition_metadata(&domain_id, &name, 0)
                                .await?;
                            Ok(Response::new(m::ListTaskListPartitionsResponse {
                                activity_task_list_partitions,
                                decision_task_list_partitions,
                            }))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn acquire_semaphore(
        &self,
        request: Request<m::AcquireSemaphoreRequest>,
    ) -> Result<Response<m::AcquireSemaphoreResponse>, Status> {
        crate::matching_panic::capture::<Response<m::AcquireSemaphoreResponse>, Status>("acquire_semaphore", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "acquire_semaphore")?;
        let metric_domain_name = self
            .metric_domain_name(request.get_ref().domain_id.as_str())
            .await;
        self.metrics
            .matching_request(
                "AcquireSemaphore",
                metric_domain_name.as_deref(),
                Some((request.get_ref().semaphore_name.as_str(), Some(0))),
                Some(
                    crate::matching_metrics::MatchingMetrics::context_from_metadata(
                        request.metadata(),
                        "grpc",
                    ),
                ),
                async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::AcquireSemaphore, Some(&request.get_ref().domain_id), None)?;
                    let deadline = crate::matching_sync::request_deadline(&request);
                    let cancelled = request.extensions().get::<CallerCancel>().map(|cancel| cancel.0.clone());
                    let result: Result<Response<m::AcquireSemaphoreResponse>, Status> = async {
                    use crate::matching_semaphore::Identifier;
                    use cadence_api::enums::SemaphoreAcquireOutcome;
                    let r = request.get_ref().clone();
                    // Owner parsing mirrors Go's `ParseRequestOwner`.
                    let owner = cadence_common::semaphore::parse_owner(&r.owner_id)
                        .map_err(|e| Status::invalid_argument(format!("invalid owner id: {e}")))?;
                    crate::matching_task_info_log::emit(
                        &r.domain_id,
                        "Received AcquireSemaphore",
                        &[
                            ("wf-id", owner.workflow_id),
                            ("wf-run-id", owner.run_id),
                            ("wf-domain-id", r.domain_id.clone()),
                            ("wf-schedule-id", owner.hold_id.to_string()),
                        ],
                    );
                    let identifier = Identifier::new(&r.domain_id, &r.semaphore_name, r.bucket)
                        .map_err(semaphore_status)?;
                    // Go's cached-manager fast path skips both domain admission
                    // and Ring ownership; Start is still called on every request.
                    let manager = if let Some(manager) = self.semaphores.by_identifier(&identifier) {
                        manager.start().await.map_err(semaphore_status)?;
                        manager
                    } else {
                        let domain_name = self.domain_name(&r.domain_id)?;
                        let (enabled, idle_ttl) = {
                            let overlay = self.frontend.state.admin.dynamic_config.lock().expect("admin lock");
                            (
                                cadence_persistence::dynamic_config::boolean(
                                    overlay.property_values("system.enableDistributedSemaphore"),
                                    Some(&domain_name), false,
                                ),
                                crate::matching_config::semaphore_idle_time(
                                    overlay.property_values("matching.semaphoreIdleTime"), &domain_name,
                                ),
                            )
                        };
                        if !enabled {
                            return Err(Status::invalid_argument("distributed semaphore is not enabled for this domain"));
                        }
                        self.check_semaphore_ownership(&identifier)?;
                        self.semaphores.get_or_create_with_idle_ttl(identifier, idle_ttl)
                            .await.map_err(semaphore_status)?
                    };
                    // Go's grant checks the caller's context before its first write, so an
                    // ended caller returns without claiming a slot.
                    if let Some(ended) = caller_ended(deadline, cancelled.as_ref()) {
                        return Err(Status::internal(ended));
                    }
                    let result = manager.acquire(&r.owner_id, deadline).await.map_err(semaphore_status)?;
                    let outcome = match result.outcome {
                        SemaphoreAcquireOutcome::Acquired => 1,
                        SemaphoreAcquireOutcome::NoSlot => 2,
                        SemaphoreAcquireOutcome::Invalid => 0,
                        _ => 0,
                    };
                    Ok(Response::new(m::AcquireSemaphoreResponse {
                        outcome,
                        token_id: result.token_id,
                    }))
                    }
                    .await;
                    // Go `AcquireSemaphore`: an error after the caller's context ended is
                    // counted and returned as an InternalServiceError carrying ctx.Err().
                    match result {
                        Err(error) => match caller_ended(deadline, cancelled.as_ref()) {
                            Some(ended) => {
                                self.metrics.semaphore_context_timeout(
                                    metric_domain_name.as_deref(),
                                    request.get_ref().semaphore_name.as_str(),
                                    Some(crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                        request.metadata(),
                                        "grpc",
                                    )),
                                );
                                Err(Status::internal(ended))
                            }
                            None => Err(error),
                        },
                        ok => ok,
                    }
                },
            )
            .await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn update_task_list_partition_config(
        &self,
        request: Request<m::UpdateTaskListPartitionConfigRequest>,
    ) -> Result<Response<m::UpdateTaskListPartitionConfigResponse>, Status> {
        crate::matching_panic::capture::<Response<m::UpdateTaskListPartitionConfigResponse>, Status>("update_task_list_partition_config", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "update_task_list_partition_config")?;
        self.metrics
            .matching_request(
                "UpdateTaskListPartitionConfig",
                self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                    .as_deref(),
                request
                    .get_ref()
                    .task_list
                    .as_ref()
                    .map(|t| (t.name.as_str(), Some(t.kind))),
                Some(
                    crate::matching_metrics::MatchingMetrics::context_from_metadata(
                        request.metadata(),
                        "grpc",
                    ),
                ),
                async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::UpdateTaskListPartitionConfig, Some(&request.get_ref().domain_id), None)?;
                    let caller_deadline = crate::matching_sync::request_deadline(&request);
                    let r = request.get_ref().clone();
                    let domain = self.domain_name(&r.domain_id)?;
                    // Go `request.TaskList.GetName()/GetKind()` are nil-safe: an absent list is the
                    // empty-named normal list and goes through the same checks.
                    let task_list = r.task_list.clone().unwrap_or_default();
                    // Go rejects manual updates while the scaler owns the config, before
                    // the kind checks.
                    if let Some(scaler) = &self.scaler_config {
                        let task_type = if r.task_list_type == 2 { 1 } else { 0 };
                        if scaler(&r.domain_id, &task_list.name, task_type).enabled {
                            return Err(Status::invalid_argument(
                                "Manual update is not allowed because adaptive scaler is enabled.",
                            ));
                        }
                    }
                    // Go `ToTaskListKind` maps INVALID and unknown values to nil, which
                    // `GetKind()` reads as Normal; only STICKY (2) and EPHEMERAL (3) reject.
                    let kind_is_normal = !matches!(task_list.kind, 2 | 3);
                    if !kind_is_normal {
                        return Err(Status::invalid_argument(
                            "Only normal tasklist's partition config can be updated.",
                        ));
                    }
                    let config = r.partition_config.ok_or_else(|| {
                        Status::invalid_argument(
                            "Task list partition config is not set in the request.",
                        )
                    })?;
                    let name = task_list_name(&task_list.name)?;
                    if !name.is_root() {
                        return Err(Status::invalid_argument(
                            "Only root partition's partition config can be updated.",
                        ));
                    }
                    #[cfg(test)]
                    self.injected_engine_fault("UpdateTaskListPartitionConfig")?;
                    if let Some(registry) = &self.queues {
                        let queue = self
                            .durable_queue(&r.domain_id, &task_list, crate::matching_config::persistence_task_list_type(r.task_list_type))
                            .await?;
                        let update = registry
                            .update_partition_config_for_domain(
                                cadence_persistence::matching_sqlite::TaskListKey {
                                    domain_id: uuid::Uuid::parse_str(&r.domain_id)
                                        .expect("validated domain"),
                                    name: task_list.name.clone(),
                                    task_type: crate::matching_config::persistence_task_list_type(r.task_list_type),
                                },
                                queue,
                                partition_config_from_proto(config),
                                matching_now(),
                                caller_deadline,
                                domain.clone(),
                            )
                            .await;
                        // The failure counter is emitted by the manager at the
                        // persistence failure point (not for validation errors).
                        update?;
                        return Ok(Response::new(m::UpdateTaskListPartitionConfigResponse {}));
                    }
                    let store = self.frontend.state.executions.clone();
                    let stored = partition_config_from_proto(config);
                    let update = store.update_partition_config(
                        &domain,
                        &task_list.name,
                        crate::matching_config::persistence_task_list_type(r.task_list_type) + 1,
                        kind_is_normal,
                        stored,
                    );
                    // Go counts only persistence failures; the in-memory store's validation
                    // errors (InvalidToken) correspond to Go's pre-counter BadRequest.
                    if matches!(
                        &update,
                        Err(cadence_persistence::TaskError::NotFound(_)
                            | cadence_persistence::TaskError::AlreadyCompleted(_))
                    ) {
                        self.metrics.task_list_counter_event(
                            crate::matching_metrics::PARTITION_UPDATE_FAILED,
                            &crate::matching_metrics::PollMetricContext {
                                domain: domain.clone(),
                                task_list: task_list.name.clone(),
                                task_list_kind: task_list.kind,
                                task_type: if r.task_list_type == 2 {
                                    "activity_task"
                                } else {
                                    "decision_task"
                                },
                                poller_isolation_group: String::new(),
                            },
                        );
                    }
                    match update {
                        Ok(()) => Ok(Response::new(m::UpdateTaskListPartitionConfigResponse {})),
                        Err(
                            cadence_persistence::TaskError::InvalidToken(e)
                            | cadence_persistence::TaskError::BadDecision(e),
                        ) => Err(Status::invalid_argument(e.message)),
                        Err(cadence_persistence::TaskError::NotFound(e)) => {
                            Err(Status::not_found(e.message))
                        }
                        Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                            Err(Status::failed_precondition(e.message))
                        }
                    }
                },
            )
            .await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn refresh_task_list_partition_config(
        &self,
        request: Request<m::RefreshTaskListPartitionConfigRequest>,
    ) -> Result<Response<m::RefreshTaskListPartitionConfigResponse>, Status> {
        crate::matching_panic::capture::<Response<m::RefreshTaskListPartitionConfigResponse>, Status>("refresh_task_list_partition_config", async move {
        self.domain_lookup_guard(&request.get_ref().domain_id, "refresh_task_list_partition_config")?;
        self.metrics
            .matching_request(
                "RefreshTaskListPartitionConfig",
                self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                    .as_deref(),
                request
                    .get_ref()
                    .task_list
                    .as_ref()
                    .map(|t| (t.name.as_str(), Some(t.kind))),
                Some(
                    crate::matching_metrics::MatchingMetrics::context_from_metadata(
                        request.metadata(),
                        "grpc",
                    ),
                ),
                async {
            self.check_rpc_quota(crate::matching_quota::MatchingRpc::RefreshTaskListPartitionConfig, Some(&request.get_ref().domain_id), None)?;
                    let r = request.get_ref().clone();
                    let domain = self.domain_name(&r.domain_id)?;
                    let task_list = r.task_list.unwrap_or_default();
                    // Same Go `ToTaskListKind` mapping as UpdateTaskListPartitionConfig.
                    if matches!(task_list.kind, 2 | 3) {
                        return Err(Status::invalid_argument(
                            "Only normal tasklist's partition config can be updated.",
                        ));
                    }
                    let name = task_list_name(&task_list.name)?;
                    if name.is_root() && r.partition_config.is_some() {
                        return Err(Status::invalid_argument(
                            "PartitionConfig must be nil for root partition.",
                        ));
                    }
                    #[cfg(test)]
                    self.injected_engine_fault("RefreshTaskListPartitionConfig")?;
                    if let Some(registry) = &self.queues {
                        let queue = self
                            .durable_queue(&r.domain_id, &task_list, crate::matching_config::persistence_task_list_type(r.task_list_type))
                            .await?;
                        registry
                            .refresh_partition_config(
                                cadence_persistence::matching_sqlite::TaskListKey {
                                    domain_id: uuid::Uuid::parse_str(&r.domain_id)
                                        .expect("validated domain"),
                                    name: task_list.name,
                                    task_type: crate::matching_config::persistence_task_list_type(r.task_list_type),
                                },
                                queue,
                                r.partition_config.map(partition_config_from_proto),
                            )
                            .await?;
                        return Ok(Response::new(m::RefreshTaskListPartitionConfigResponse {}));
                    }
                    let store = self.frontend.state.executions.clone();
                    let pushed = r.partition_config.map(partition_config_from_proto);
                    match store.refresh_partition_config_with(
                        &domain,
                        &task_list.name,
                        crate::matching_config::persistence_task_list_type(r.task_list_type) + 1,
                        pushed,
                    ) {
                        Ok(()) => Ok(Response::new(m::RefreshTaskListPartitionConfigResponse {})),
                        Err(
                            cadence_persistence::TaskError::InvalidToken(e)
                            | cadence_persistence::TaskError::BadDecision(e),
                        ) => Err(Status::invalid_argument(e.message)),
                        Err(cadence_persistence::TaskError::NotFound(e)) => {
                            Err(Status::not_found(e.message))
                        }
                        Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                            Err(Status::failed_precondition(e.message))
                        }
                    }
                },
            )
            .await

        })
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn cancel_outstanding_poll(
        &self,
        request: Request<m::CancelOutstandingPollRequest>,
    ) -> Result<Response<m::CancelOutstandingPollResponse>, Status> {
        crate::matching_panic::capture::<Response<m::CancelOutstandingPollResponse>, Status>(
            "cancel_outstanding_poll",
            async move {
                self.domain_lookup_guard(&request.get_ref().domain_id, "cancel_outstanding_poll")?;
                self.metrics
                    .matching_request(
                        "CancelOutstandingPoll",
                        self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                            .as_deref(),
                        request
                            .get_ref()
                            .task_list
                            .as_ref()
                            .map(|t| (t.name.as_str(), Some(t.kind))),
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::CancelOutstandingPoll,
                                Some(&request.get_ref().domain_id),
                                None,
                            )?;
                            let r = request.get_ref().clone();
                            self.domain_name(&r.domain_id)?;
                            let list = r.task_list.unwrap_or_default();
                            let task_list = list.name.clone();
                            task_list_name(&task_list)?;
                            // Go getOrCreateTaskListManager: a non-owner gets an error and
                            // an unloaded list is loaded before the cancel.
                            if self.queues.is_some() {
                                let queue = self
                                    .durable_queue(
                                        &r.domain_id,
                                        &list,
                                        crate::matching_config::persistence_task_list_type(
                                            r.task_list_type,
                                        ),
                                    )
                                    .await?;
                                queue
                                    .pollers
                                    .lock()
                                    .expect("poller history lock")
                                    .cancel(&r.poller_id);
                                return Ok(Response::new(m::CancelOutstandingPollResponse {}));
                            }
                            let key = PollKey {
                                domain: r.domain_id,
                                task_list,
                                task_type: crate::matching_config::persistence_task_list_type(
                                    r.task_list_type,
                                ) + 1,
                                poller: r.poller_id,
                            };
                            let cancel = self
                                .polls
                                .lock()
                                .expect("poll cancellation lock")
                                .get(&key)
                                .cloned();
                            // Go CancelPoller returns nil whether or not a poll matched.
                            if let Some(cancel) = cancel {
                                cancel.notify_one();
                            }
                            Ok(Response::new(m::CancelOutstandingPollResponse {}))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn respond_query_task_completed(
        &self,
        request: Request<m::RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<m::RespondQueryTaskCompletedResponse>, Status> {
        crate::matching_panic::capture::<Response<m::RespondQueryTaskCompletedResponse>, Status>(
            "respond_query_task_completed",
            async move {
                // Go's generated gRPC wrapper runs the proto mapper before the handler:
                // `ToRespondQueryTaskCompletedRequest` dereferences a nil `Result` when
                // the nested request is present. YARPC recovers it as a plain error that
                // gRPC reports as Unknown, before any domain, quota or metric side effect.
                if request
                    .get_ref()
                    .request
                    .as_ref()
                    .is_some_and(|nested| nested.result.is_none())
                {
                    return Err(Status::unknown(
                        "panic: runtime error: invalid memory address or nil pointer dereference",
                    ));
                }
                self.domain_lookup_guard(
                    &request.get_ref().domain_id,
                    "respond_query_task_completed",
                )?;
                self.metrics
                    .matching_request(
                        "RespondQueryTaskCompleted",
                        self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                            .as_deref(),
                        request
                            .get_ref()
                            .task_list
                            .as_ref()
                            .map(|t| (t.name.as_str(), Some(t.kind))),
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::RespondQueryTaskCompleted,
                                Some(&request.get_ref().domain_id),
                                None,
                            )?;
                            use crate::grpc_convert as cv;
                            let r = request.get_ref().clone();
                            if let Some(queues) = self.queues.as_ref() {
                                // Go `RespondQueryTaskCompleted` -> `deliverQueryResult`: the
                                // engine-wide query map is keyed by task ID only. Domain, task
                                // list and the embedded token are not validated, and a missing
                                // nested request is delivered as an empty completion.
                                let payload = r.request.clone().unwrap_or_default();
                                let result = payload.result.clone().unwrap_or_default();
                                let answer = crate::matching_query::WorkerAnswer {
                                    result,
                                    worker_version_info: payload.worker_version_info.clone(),
                                };
                                let completed = queues.deliver_query_result(
                                    &r.task_id,
                                    crate::matching_query::QueryOutcome::Worker(answer),
                                );
                                if completed.is_err() {
                                    let task_list = r.task_list.clone().unwrap_or_default();
                                    self.metrics.task_list_counter_event(
                                        crate::matching_metrics::RESPOND_QUERY_TASK_FAILED,
                                        &crate::matching_metrics::PollMetricContext {
                                            domain: self
                                                .domain_name(&r.domain_id)
                                                .unwrap_or_default(),
                                            task_list: task_list.name.clone(),
                                            task_list_kind: task_list.kind,
                                            task_type: "decision_task",
                                            poller_isolation_group: String::new(),
                                        },
                                    );
                                }
                                completed?;
                                return Ok(Response::new(m::RespondQueryTaskCompletedResponse {}));
                            }
                            let mut payload = r
                                .request
                                .clone()
                                .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
                            // The internal request carries the authoritative domain/task routing;
                            // the embedded worker token must agree with it, like Go's domain-cache
                            // lookup by UUID before delivering to the query channel.
                            self.domain_name(&r.domain_id)?;
                            if !r.task_id.is_empty() && !payload.task_token.is_empty() {
                                if let Ok(token) =
                cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
                    &payload.task_token,
                )
            {
                if token.task_id != r.task_id || token.domain_id != r.domain_id {
                    return Err(Status::invalid_argument(
                        "TaskToken does not match request.",
                    ));
                }
            }
                            }
                            if payload.task_token.is_empty() {
                                // A missing embedded token is rebuilt from the routing fields so
                                // direct matching callers do not need to mint query tokens.
                                let token = cadence_api::tasktoken::QueryTaskToken {
                                    domain_id: r.domain_id.clone(),
                                    workflow_id: String::new(),
                                    run_id: String::new(),
                                    task_list: r
                                        .task_list
                                        .as_ref()
                                        .map(|t| t.name.clone())
                                        .unwrap_or_default(),
                                    task_id: r.task_id.clone(),
                                };
                                payload.task_token =
                cadence_api::tasktoken::JsonTaskTokenSerializer::serialize_query_token(&token)
                    .map_err(|_| Status::internal("Failed to encode query task token."))?;
                            }
                            let input = cv::query_completed(payload)?;
                            let _ = p::worker_api_server::WorkerApi::respond_query_task_completed(
                                &self.frontend,
                                Request::new(cv::query_completed_out(input)),
                            )
                            .await?;
                            Ok(Response::new(m::RespondQueryTaskCompletedResponse {}))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
    async fn describe_task_list(
        &self,
        request: Request<m::DescribeTaskListRequest>,
    ) -> Result<Response<m::DescribeTaskListResponse>, Status> {
        crate::matching_panic::capture::<Response<m::DescribeTaskListResponse>, Status>(
            "describe_task_list",
            async move {
                self.domain_lookup_guard(&request.get_ref().domain_id, "describe_task_list")?;
                self.metrics
                    .matching_request(
                        "DescribeTaskList",
                        self.metric_domain_name(request.get_ref().domain_id.as_str()).await
                            .as_deref(),
                        request
                            .get_ref()
                            .request
                            .as_ref()
                            .and_then(|r| r.task_list.as_ref())
                            .map(|t| (t.name.as_str(), Some(t.kind))),
                        Some(
                            crate::matching_metrics::MatchingMetrics::context_from_metadata(
                                request.metadata(),
                                "grpc",
                            ),
                        ),
                        async {
                            self.check_rpc_quota(
                                crate::matching_quota::MatchingRpc::DescribeTaskList,
                                Some(&request.get_ref().domain_id),
                                None,
                            )?;
                            let r = request.get_ref().clone();
                            let mut payload = r.request.unwrap_or_default();
                            let task_list = payload.task_list.clone();
                            let task_list_type = crate::matching_config::persistence_task_list_type(
                                payload.task_list_type,
                            ) + 1;
                            payload.task_list_type = task_list_type;
                            let task_list_name =
                                task_list.clone().map(|t| t.name).unwrap_or_default();
                            // Go validates the identifier before domain-cache/manager lookup;
                            // untyped identifier errors become InternalServiceError.
                            TaskListName::parse(&task_list_name).map_err(Status::internal)?;
                            payload.domain = self.domain_name(&r.domain_id)?;
                            #[cfg(test)]
                            self.injected_engine_fault("DescribeTaskList")?;
                            if self.queues.is_some() {
                                let queue = self
                                    .durable_queue(
                                        &r.domain_id,
                                        &task_list.clone().unwrap_or_default(),
                                        crate::matching_config::persistence_task_list_type(
                                            task_list_type,
                                        ),
                                    )
                                    .await?;
                                let pollers = queue
                                    .pollers
                                    .lock()
                                    .expect("poller history lock")
                                    .list(matching_now())
                                    .into_iter()
                                    .map(|poller| p::PollerInfo {
                                        identity: poller.identity,
                                        rate_per_second: poller.rate_per_second,
                                        last_access_time: Some(prost_types::Timestamp {
                                            seconds: poller
                                                .last_access_time
                                                .div_euclid(1_000_000_000),
                                            nanos: poller.last_access_time.rem_euclid(1_000_000_000)
                                                as i32,
                                        }),
                                    })
                                    .collect();
                                let task_list_status = if payload.include_task_list_status {
                                    let (
                                        read_level,
                                        ack_level,
                                        backlog_count_hint,
                                        start_id,
                                        end_id,
                                        empty,
                                    ) = queue.queue.status();
                                    // Go reads the configured group universe and the live poller
                                    // history window on every Describe, even with isolation off.
                                    let (groups, window) = {
                                        let config = self
                                            .frontend
                                            .state
                                            .admin
                                            .dynamic_config
                                            .lock()
                                            .expect("admin lock");
                                        (
                                            cadence_persistence::dynamic_config::string_list(
                                                config.property_values("system.allIsolationGroups"),
                                            ),
                                            cadence_persistence::dynamic_config::task_list_duration(
                                                config.property_values(
                                                    "matching.taskIsolationPollerWindow",
                                                ),
                                                Some(&payload.domain),
                                                &task_list_name,
                                                crate::matching_config::persistence_task_list_type(
                                                    task_list_type,
                                                ),
                                                2_000_000_000,
                                            ),
                                        )
                                    };
                                    let now = matching_now();
                                    let counts = queue
                                        .pollers
                                        .lock()
                                        .expect("poller history lock")
                                        .count_by_group(now, now.saturating_sub(window));
                                    let isolation_group_metrics = groups
                                        .into_iter()
                                        .map(|group| {
                                            let metrics = p::IsolationGroupMetrics {
                                                new_tasks_per_second: queue.qps.group_qps(&group),
                                                poller_count: counts
                                                    .get(&group)
                                                    .copied()
                                                    .unwrap_or_default()
                                                    as i64,
                                            };
                                            (group, metrics)
                                        })
                                        .collect();
                                    Some(p::TaskListStatus {
                                        read_level,
                                        ack_level,
                                        backlog_count_hint,
                                        task_id_block: Some(p::TaskIdBlock { start_id, end_id }),
                                        empty,
                                        rate_per_second: queue.limiter.limit(),
                                        new_tasks_per_second: queue.qps.qps(),
                                        isolation_group_metrics,
                                    })
                                } else {
                                    None
                                };
                                let partition_config =
                                    queue.partitions.get().map(partition_config_to_proto);
                                return Ok(Response::new(m::DescribeTaskListResponse {
                                    pollers,
                                    task_list_status,
                                    task_list: Some(p::TaskList {
                                        name: task_list_name.clone(),
                                        kind: i32::from(queue.queue.task_list_kind()) + 1,
                                        ..Default::default()
                                    }),
                                    partition_config,
                                }));
                            }
                            let v = p::workflow_api_server::WorkflowApi::describe_task_list(
                                &self.frontend,
                                Request::new(payload),
                            )
                            .await?
                            .into_inner();
                            let partition_config = self
                                .frontend
                                .state
                                .executions
                                .partition_config(&r.domain_id, &task_list_name, task_list_type)
                                .map(partition_config_to_proto);
                            Ok(Response::new(m::DescribeTaskListResponse {
                                pollers: v.pollers,
                                task_list_status: v.task_list_status,
                                task_list,
                                partition_config,
                            }))
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap_or_else(|message| Err(Status::unknown(message)))
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    state: crate::handlers::AppState,
) -> anyhow::Result<()> {
    serve_with(addr, Matching::new(state)).await
}

/// Serve a prebuilt matcher. The standalone server passes a durable-queue
/// matcher so production traffic uses the same partition/forwarding path as
/// the mixed-service harness instead of the in-memory engine.
pub async fn serve_with(addr: std::net::SocketAddr, matching: Matching) -> anyhow::Result<()> {
    serve_with_tls(addr, matching, None).await
}

#[tonic::async_trait]
impl p::meta_api_server::MetaApi for Matching {
    async fn health(
        &self,
        _: Request<p::HealthRequest>,
    ) -> Result<Response<p::HealthResponse>, Status> {
        self.wait_started().await;
        Ok(Response::new(p::HealthResponse {
            ok: true,
            message: "matching good".into(),
        }))
    }
}

/// Serve Matching on `addr` with the Go `rpc.tls` listener config, if any.
pub async fn serve_with_tls(
    addr: std::net::SocketAddr,
    matching: Matching,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_with_listener_tls(listener, matching, tls).await
}

/// Serve a prebound Matching socket so production can fail listener startup
/// before Ringpop advertises this node's gRPC endpoint.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener,
    matching: Matching,
) -> anyhow::Result<()> {
    serve_with_listener_tls(listener, matching, None).await
}

/// Like `serve_with_listener`, with the Go `rpc.tls` listener config applied.
pub async fn serve_with_listener_tls(
    listener: tokio::net::TcpListener,
    matching: Matching,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> anyhow::Result<()> {
    serve_with_listener_tls_inner(listener, matching, tls, None).await
}

pub async fn serve_with_listener_tls_or_no_identity(
    listener: tokio::net::TcpListener,
    matching: Matching,
    tls: Option<tonic::transport::ServerTlsConfig>,
    no_identity: Option<std::sync::Arc<rustls::ServerConfig>>,
) -> anyhow::Result<()> {
    serve_with_listener_tls_inner(listener, matching, tls, no_identity).await
}

pub async fn serve_with_tls_or_no_identity(
    addr: std::net::SocketAddr,
    matching: Matching,
    tls: Option<tonic::transport::ServerTlsConfig>,
    no_identity: Option<std::sync::Arc<rustls::ServerConfig>>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_with_listener_tls_inner(listener, matching, tls, no_identity).await
}

async fn serve_with_listener_tls_inner(
    listener: tokio::net::TcpListener,
    matching: Matching,
    tls: Option<tonic::transport::ServerTlsConfig>,
    no_identity: Option<std::sync::Arc<rustls::ServerConfig>>,
) -> anyhow::Result<()> {
    let limit = matching.frontend.state.grpc_max_message_size;
    // Go runs a per-bucket liveness timer; a shared ticker unloads idle
    // semaphore buckets so a quiet host does not keep them forever.
    {
        let sweeper = matching.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                ticker.tick().await;
                sweeper.semaphores.sweep_idle(semaphore_now());
            }
        });
    }
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("uber.cadence.matching.v1.MatchingAPI")
        .with_service_name("uber.cadence.api.v1.MetaAPI")
        .build_v1()?;
    let server = crate::grpc_tls::server_builder(tls)?
        .add_service(reflection)
        .add_service(
            p::meta_api_server::MetaApiServer::new(matching.clone())
                .max_decoding_message_size(limit),
        )
        .add_service(
            m::matching_api_server::MatchingApiServer::new(matching)
                .max_decoding_message_size(limit),
        );
    if let Some(config) = no_identity {
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let incoming = no_identity_tls_incoming(listener, acceptor);
        server.serve_with_incoming(incoming).await?;
    } else {
        server
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await?;
    }
    Ok(())
}

fn no_identity_tls_incoming(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
) -> impl tokio_stream::Stream<Item = std::io::Result<NoIdentityTlsStream>> + Send + 'static {
    use futures_util::{stream::FuturesUnordered, StreamExt as _};
    use std::{future::Future, pin::Pin};

    type Handshake = Pin<
        Box<
            dyn Future<
                    Output = std::io::Result<
                        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
                    >,
                > + Send,
        >,
    >;

    let handshakes = FuturesUnordered::<Handshake>::new();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    futures_util::stream::unfold(
        (incoming, handshakes),
        move |(mut listener, mut handshakes)| {
            let acceptor = acceptor.clone();
            async move {
                loop {
                    tokio::select! {
                        accepted = listener.next() => match accepted {
                            Some(Ok(socket)) => {
                                let acceptor = acceptor.clone();
                                handshakes.push(Box::pin(async move {
                                    tokio::time::timeout(
                                        std::time::Duration::from_secs(120),
                                        acceptor.accept(socket),
                                    )
                                    .await
                                    .map_err(|_| {
                                        std::io::Error::new(
                                            std::io::ErrorKind::TimedOut,
                                            "TLS handshake timed out",
                                        )
                                    })
                                    .and_then(|stream| stream.map_err(std::io::Error::other))
                                }));
                            }
                            Some(Err(_)) => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            None => return None,
                        },
                        result = handshakes.next(), if !handshakes.is_empty() => {
                            if let Some(Ok(stream)) = result {
                                return Some((
                                    Ok(NoIdentityTlsStream(stream)),
                                    (listener, handshakes),
                                ));
                            }
                        }
                    }
                }
            }
        },
    )
}

struct NoIdentityTlsStream(tokio_rustls::server::TlsStream<tokio::net::TcpStream>);
impl tokio::io::AsyncRead for NoIdentityTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.0), cx, buf)
    }
}
impl tokio::io::AsyncWrite for NoIdentityTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.0), cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.0), cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.0), cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write_vectored(std::pin::Pin::new(&mut self.0), cx, bufs)
    }
}
impl tonic::transport::server::Connected for NoIdentityTlsStream {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}

/// Go `tag.WorkflowTaskListKind` is the Go enum (Normal=0); proto kinds are +1.
fn go_task_list_kind(proto_kind: i32) -> i32 {
    (proto_kind - 1).max(0)
}

/// Go `engine.go` AddDecisionTask/AddActivityTask "Received ..." event.
fn log_received_add_task(
    message: &str,
    domain_id: &str,
    task_list: Option<&p::TaskList>,
    execution: Option<&p::WorkflowExecution>,
    task_type: i32,
    schedule_id: i64,
) {
    let execution = execution.cloned().unwrap_or_default();
    crate::matching_task_info_log::emit(
        domain_id,
        message,
        &[
            (
                "wf-task-list-name",
                task_list.map(|t| t.name.clone()).unwrap_or_default(),
            ),
            ("wf-id", execution.workflow_id),
            ("wf-run-id", execution.run_id),
            ("wf-domain-id", domain_id.to_owned()),
            ("wf-task-list-type", task_type.to_string()),
            ("wf-schedule-id", schedule_id.to_string()),
            (
                "wf-task-list-kind",
                go_task_list_kind(task_list.map_or(0, |t| t.kind)).to_string(),
            ),
        ],
    );
}

/// Bounded tracing for the production adaptive-scaler SDK probe. Enable by
/// setting CADENCE_MATCHING_ADAPTIVE_TRACE to the root task-list name.
fn adaptive_scaler_trace(task_list: &str, boundary: &str, fields: &str) {
    const PROBE: &str = "production-adaptive-scaler-probe";
    if std::env::var("CADENCE_MATCHING_ADAPTIVE_TRACE").as_deref() != Ok(PROBE) {
        return;
    }
    if cadence_common::task_list::TaskListName::parse(task_list)
        .is_ok_and(|parsed| parsed.root() == PROBE)
    {
        eprintln!(
            "matching_adaptive_trace ts={} task_list={} boundary={} {}",
            matching_now(), task_list, boundary, fields
        );
    }
}

/// Go `engine.go` "Emitting tasklist counter on ..." event.
fn log_tasklist_counter(message: &str, domain_id: &str, task_list: Option<&p::TaskList>) {
    let name = task_list.map(|t| t.name.clone()).unwrap_or_default();
    let root = cadence_common::task_list::TaskListName::parse(&name)
        .map(|n| n.root().to_owned())
        .unwrap_or_default();
    crate::matching_task_info_log::emit(
        domain_id,
        message,
        &[("wf-task-list-name", name), ("taskListBaseName", root)],
    );
}

#[cfg(test)]
#[path = "matching_rpc_tests.rs"]
mod durable_tests;

#[cfg(test)]
#[path = "matching_query_forward_family_tests.rs"]
mod query_forward_family_tests;

#[cfg(test)]
#[path = "matching_handler_go_tests.rs"]
mod handler_go_tests;

#[cfg(test)]
#[path = "matching_cold_domain_metric_tests.rs"]
mod cold_domain_metric_tests;
