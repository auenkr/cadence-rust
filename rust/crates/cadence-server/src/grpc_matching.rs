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
fn task_list_name(name: &str) -> Result<TaskListName, Status> {
    TaskListName::parse(name).map_err(Status::invalid_argument)
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
/// Map a semaphore manager error to the Go-compatible gRPC status.
fn semaphore_status(error: crate::matching_semaphore::ManagerError) -> Status {
    use crate::matching_semaphore::ManagerError;
    match error {
        ManagerError::BadRequest(message) => Status::invalid_argument(message),
        ManagerError::Disabled => {
            Status::invalid_argument("distributed semaphore is not enabled for this domain")
        }
        ManagerError::NotFound(message) => Status::not_found(message),
        ManagerError::NotReady => crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            "semaphore manager is not ready",
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
        let mut polls = self.polls.lock().expect("poll cancellation lock");
        if polls
            .get(&self.key)
            .is_some_and(|cancel| Arc::ptr_eq(cancel, &self.cancel))
        {
            polls.remove(&self.key);
        }
    }
}
#[derive(Clone)]
pub struct Matching {
    pub frontend: Frontend,
    polls: Polls,
    queues: Option<crate::matching_registry::QueueRegistry>,
    /// Go `cfg.NumTasklistWritePartitions`, shared with the registry limiter.
    partition_count: Option<crate::matching_registry::PartitionCountProvider>,
    /// Go `cfg.EnableAdaptiveScaler`, guarding manual partition updates.
    scaler_config: Option<crate::matching_registry::ScalerConfigProvider>,
    /// Go `semaphoreRegistry`: one manager per semaphore bucket, in-memory.
    semaphores: crate::matching_semaphore::Registry,
    membership: Option<crate::matching_membership::Membership>,
    shard_router: Option<Arc<crate::matching_shard_routing::MatchingShardRouter>>,
    task_list_ownership: Option<TaskListOwnershipCheck>,
    pub(crate) poll_expiration: std::time::Duration,
}
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
        let semaphore_store = state
            .semaphore_persistence
            .clone()
            .unwrap_or_else(|| Arc::new(state.executions.clone()));
        let semaphores = crate::matching_semaphore::Registry::new(semaphore_store);
        Self {
            frontend: Frontend { state },
            polls: Arc::new(Mutex::new(HashMap::new())),
            queues: None,
            partition_count: None,
            scaler_config: None,
            semaphores,
            membership: None,
            shard_router: None,
            task_list_ownership: None,
            poll_expiration: std::time::Duration::from_secs(60),
        }
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
                    .property_values("matching.numTasklistWritePartitions")
                    .to_vec();
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
                    .property_values("matching.qpsTrackerInterval")
                    .to_vec();
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
                    let (group, duration) = policy.select();
                    (group.to_owned(), duration)
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
        hostname: &str,
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
                .await?
                .unwrap_or_else(|| hostname.to_owned());
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
    /// The service owns this future and cancels it during shutdown. Watch
    /// coalescing is safe: cleanup always consults the latest complete ring.
    pub async fn maintain_membership_until<F>(&self, shutdown: F) -> Result<(), Status>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let Some(membership) = &self.membership else {
            return Ok(());
        };
        let mut changes = membership.subscribe();
        tokio::pin!(shutdown);
        loop {
            let queues = match &self.queues {
                Some(queues) => queues.unload_non_owned(matching_now()).await,
                None => Ok(()),
            };
            // Do not let a SQL cleanup failure prevent semaphore cleanup.
            for manager in self.semaphores.all_managers() {
                match membership.owns(&manager.identifier().ring_key()) {
                    Ok(false) => {
                        manager.stop();
                        self.semaphores.unregister(&manager);
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
    }
    fn poll_rate(
        &self,
        domain: &str,
        list: &str,
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
                limiter.report_limit(rate);
                rate
            }
            None => crate::matching_limiter::TASK_DISPATCH_RPS,
        }
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
        };
        let mut list = list.clone();
        let mut handle = entry.clone();
        let mut ancestor = false;
        let mut permits = Vec::new();
        loop {
            let matched = if ancestor || forwarded {
                handle
                    .try_sync_match_forwarded(data, source, deadline)
                    .await
            } else {
                handle.try_sync_match(data, source, deadline).await
            };
            if matched {
                return Ok(Some(
                    entry.partitions.get().map(partition_config_to_internal),
                ));
            }
            let name = TaskListName::parse(&list.name)
                .map_err(|_| Status::invalid_argument("TaskList is not valid."))?;
            if backlog && (ancestor || forwarded) && name.is_root() {
                return Ok(handle
                    .offer_forwarded_backlog(data, source, deadline)
                    .await
                    .then(|| entry.partitions.get().map(partition_config_to_internal)));
            }
            if name.is_root()
                || list.kind == 2
                || degree <= 0
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(None);
            }
            let parent = name.parent(degree);
            if parent.is_empty() {
                return Ok(None);
            }
            let Some(permit) = handle.try_forward_task() else {
                return Ok(None);
            };
            permits.push(permit);
            let forwarded_from = list.name.clone();
            list.name = parent;
            if let Some(channel) = self.remote_channel_for_task_list(&list.name).await? {
                    let execution = Some(p::WorkflowExecution {
                        workflow_id: data.workflow_id.clone(),
                        run_id: data
                            .run_id
                            .as_deref()
                            .and_then(|id| uuid::Uuid::from_slice(id).ok())
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                    });
                    let partition_config = data
                        .partition_config
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    let mut client = m::matching_api_client::MatchingApiClient::new(channel);
                    let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
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
                    return Ok(result
                        .ok()
                        .map(|_| entry.partitions.get().map(partition_config_to_internal)));
            }
            handle = match self.durable_queue(domain_id, &list, task_type).await {
                Ok(handle) => handle,
                Err(_) => return Ok(None),
            };
            if handle.stop_if_isolation_changed()
                || self
                    .check_write_partition(domain_id, &list, task_type, &handle)
                    .is_err()
            {
                return Ok(None);
            }
            ancestor = true;
        }
    }
    fn domain_name(&self, id: &str) -> Result<String, Status> {
        History {
            frontend: self.frontend.clone(),
        }
        .domain_name(id)
    }
    fn register_poll(&self, key: PollKey) -> PollGuard {
        let cancel = Arc::new(Notify::new());
        if let Some(previous) = self
            .polls
            .lock()
            .expect("poll cancellation lock")
            .insert(key.clone(), cancel.clone())
        {
            previous.notify_one();
        }
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
        let incoming_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        let mut failover = self.frontend.state.domains.subscribe_failover(&r.domain_id);
        payload.domain = self.domain_name(&r.domain_id)?;
        let task_list = payload
            .task_list
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?
            .name
            .clone();
        task_list_name(&task_list)?;
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
            let poll_started = std::time::Instant::now();
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
            let rate = self.poll_rate(&r.domain_id, &guard.key.task_list, 0, None, &queue.limiter);
            let lifecycle = crate::matching_dispatch::PollLifecycle {
                manager: queue.pollers.clone(),
                id: guard.key.poller.clone(),
                info: cadence_common::poller::Info {
                    identity: payload.identity.clone(),
                    rate_per_second: rate,
                    isolation_group: r.isolation_group,
                },
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
                .map_err(|error| Status::not_found(error.message))?;
            let _standby_wait = (!active).then(|| lifecycle.begin());
            let query_lifecycle = lifecycle.clone();
            return tokio::select! {
                biased;
                query=queue.queries.next()=>{
                    let _wait=query_lifecycle.begin();
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
                        queue.dispatcher.poll_decision_with_poller(payload, expiration, lifecycle).await
                    } else {
                        // Go standby getTask polls only queryTaskC, never the
                        // durable backlog or forwarded workflow tasks.
                        tokio::time::sleep(expiration).await;
                        Ok(m::PollForDecisionTaskResponse::default())
                    }
                }=>
                    result.map(|mut response| {
                        response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                        response.auto_config_hint=Some(self.auto_config_hint(&r.domain_id, &guard.key.task_list, 0, poll_started));
                        Response::new(response)
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
                _=guard.cancel.notified()=>Err(Status::cancelled("Outstanding poll cancelled.")),
            };
        }
        let expiration =
            self.poll_timeout(&r.domain_id, &guard.key.task_list, 0, incoming_deadline);
        let poll_started = std::time::Instant::now();
        let v = tokio::select! {
            result=tokio::time::timeout(expiration, p::worker_api_server::WorkerApi::poll_for_decision_task(&self.frontend,Request::new(payload)))=>match result { Ok(result)=>result?.into_inner(), Err(_)=>p::PollForDecisionTaskResponse::default() },
            _=guard.cancel.notified()=>return Err(Status::cancelled("Outstanding poll cancelled.")),
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
    }
    async fn poll_for_activity_task(
        &self,
        request: Request<m::PollForActivityTaskRequest>,
    ) -> Result<Response<m::PollForActivityTaskResponse>, Status> {
        let incoming_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        let mut failover = self.frontend.state.domains.subscribe_failover(&r.domain_id);
        payload.domain = self.domain_name(&r.domain_id)?;
        let task_list = payload
            .task_list
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?
            .name
            .clone();
        task_list_name(&task_list)?;
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
                1,
                max_dispatch,
                &queue.limiter,
            );
            let lifecycle = crate::matching_dispatch::PollLifecycle {
                manager: queue.pollers.clone(),
                id: guard.key.poller.clone(),
                info: cadence_common::poller::Info {
                    identity: payload.identity.clone(),
                    rate_per_second: rate,
                    isolation_group: r.isolation_group,
                },
            };
            return tokio::select! {
                result=async {
                    let active = self.frontend.state.domains.is_active_in(&r.domain_id, &self.frontend.state.current_cluster_name)
                        .map_err(|error| Status::not_found(error.message))?;
                    if active {
                        queue.dispatcher.poll_activity_with_poller(payload, expiration, lifecycle).await
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
                _=guard.cancel.notified()=>Err(Status::cancelled("Outstanding poll cancelled.")),
            };
        }
        let expiration =
            self.poll_timeout(&r.domain_id, &guard.key.task_list, 1, incoming_deadline);
        let poll_started = std::time::Instant::now();
        let v = tokio::select! {
            result=tokio::time::timeout(expiration, p::worker_api_server::WorkerApi::poll_for_activity_task(&self.frontend,Request::new(payload)))=>match result { Ok(result)=>result?.into_inner(), Err(_)=>p::PollForActivityTaskResponse::default() },
            _=guard.cancel.notified()=>return Err(Status::cancelled("Outstanding poll cancelled.")),
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
    }
    async fn add_decision_task(
        &self,
        request: Request<m::AddDecisionTaskRequest>,
    ) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        let parent_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        task_list_name(
            r.task_list
                .as_ref()
                .map(|t| t.name.as_str())
                .unwrap_or_default(),
        )?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            let list = r
                .task_list
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r
                .workflow_execution
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id)
                .map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            let source = uuid::Uuid::parse_str(&r.domain_id).expect("domain validated");
            let queue = self.durable_queue(&r.domain_id, list, 0).await?;
            if queue.stop_if_isolation_changed() {
                return Err(Status::unavailable("task list shutting down"));
            }
            self.check_write_partition(&r.domain_id, list, 0, &queue)?;
            check_sticky_poller(&queue, list)?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(),
                run_id: Some(run.as_bytes().to_vec()),
                schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() {
                    None
                } else {
                    Some(r.partition_config.into_iter().collect())
                },
                ..Default::default()
            }
            .at_creation(matching_now(), 0);
            let forwarded = !r.forwarded_from.is_empty();
            // Go `taskListManagerImpl.AddTask`: only a non-forwarded add marks
            // liveness and reports arrival QPS, grouped by the task's isolation
            // group when it has one.
            if !forwarded {
                report_arrival(&queue, &data);
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
                tokio::time::Instant::now() + crate::matching_sync::sync_match_budget(remaining)
            };
            let active = self
                .frontend
                .state
                .domains
                .is_active_in(
                    &source.to_string(),
                    &self.frontend.state.current_cluster_name,
                )
                .map_err(|error| Status::not_found(error.message))?;
            if active {
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
                if let Some(partition_config) = matched {
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
            queue
                .enqueue(
                    data,
                    source,
                    r.schedule_to_start_timeout
                        .map(|timeout| timeout.seconds as i32)
                        .unwrap_or_default(),
                )
                .await?;
            return Ok(Response::new(m::AddDecisionTaskResponse {
                partition_config: queue.partitions.get().map(partition_config_to_internal),
            }));
        }
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| Status::not_found(e.message))?;
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
            Err(cadence_persistence::TaskError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }
    async fn add_activity_task(
        &self,
        request: Request<m::AddActivityTaskRequest>,
    ) -> Result<Response<m::AddActivityTaskResponse>, Status> {
        let parent_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        task_list_name(
            r.task_list
                .as_ref()
                .map(|t| t.name.as_str())
                .unwrap_or_default(),
        )?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            let list = r
                .task_list
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r
                .workflow_execution
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id)
                .map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            self.domain_name(&r.source_domain_id)?;
            let source = uuid::Uuid::parse_str(&r.source_domain_id)
                .map_err(|_| Status::invalid_argument("Source domain UUID is not valid."))?;
            let queue = self.durable_queue(&r.domain_id, list, 1).await?;
            if queue.stop_if_isolation_changed() {
                return Err(Status::unavailable("task list shutting down"));
            }
            self.check_write_partition(&r.domain_id, list, 1, &queue)?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(),
                run_id: Some(run.as_bytes().to_vec()),
                schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() {
                    None
                } else {
                    Some(r.partition_config.into_iter().collect())
                },
                ..Default::default()
            }
            .at_creation(matching_now(), 0);
            let forwarded = !r.forwarded_from.is_empty();
            if !forwarded {
                report_arrival(&queue, &data);
            }
            let deadline = if forwarded {
                parent_deadline.unwrap_or_else(|| {
                    tokio::time::Instant::now() + std::time::Duration::from_millis(200)
                })
            } else {
                let remaining = parent_deadline.map(|deadline| {
                    deadline.saturating_duration_since(tokio::time::Instant::now())
                });
                tokio::time::Instant::now() + crate::matching_sync::sync_match_budget(remaining)
            };
            let active = self
                .frontend
                .state
                .domains
                .is_active_in(
                    &source.to_string(),
                    &self.frontend.state.current_cluster_name,
                )
                .map_err(|error| Status::not_found(error.message))?;
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
                partition_config: queue.partitions.get().map(partition_config_to_internal),
            }));
        }
        let execution = r.workflow_execution.unwrap_or_default();
        let store = self
            .frontend
            .state
            .execution_store(&domain)
            .map_err(|e| Status::not_found(e.message))?;
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
            Err(cadence_persistence::TaskError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }
    async fn query_workflow(
        &self,
        request: Request<m::QueryWorkflowRequest>,
    ) -> Result<Response<m::QueryWorkflowResponse>, Status> {
        let incoming_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        // Go `waitForQueryResult(..., consistencyLevel == Strong, ...)`: the
        // strong-consistency gate only applies to the local durable path.
        let strong = payload.query_consistency_level == p::QueryConsistencyLevel::Strong as i32;
        if self.queues.is_some() {
            // Go `matcher.OfferQuery`: offer to a waiting local decision
            // poller, else forward up the partition chain with a forward-task
            // token (no rate limit, unlike `ForwardTask`). The root fails
            // fast with `ErrNoParent` when no poller waits anywhere; sticky
            // lists never forward. Permits are held until the query resolves,
            // like Go releasing the token when `ForwardQueryTask` returns.
            let task_list = r
                .task_list
                .clone()
                .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
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
                    config.property_values("matching.forwarderMaxChildrenPerNode"),
                    domain_name_for(&self.frontend.state.domains, &r.domain_id).as_deref(),
                    root.as_str(),
                    0,
                    20,
                )
            };
            let deadline = incoming_deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + self.poll_expiration);
            let mut list = task_list.clone();
            let mut handle = queue;
            let mut permits = Vec::new();
            let mut force_local = false;
            loop {
                let query = crate::matching_query::PendingQuery {
                    task_id: task_id.clone(),
                    domain_id: r.domain_id.clone(),
                    task_list: list.name.clone(),
                    request: payload.clone(),
                };
                let receiver = handle.queries.register_waiter(&task_id);
                let _registration = handle.queries.registration(&task_id);
                let offered = if force_local {
                    handle.queries.offer(query);
                    true
                } else {
                    handle.queries.try_offer(query)
                };
                if offered {
                    return match tokio::time::timeout_at(deadline, receiver).await {
                        Ok(Ok(result)) => {
                            crate::matching_query::query_outcome(result, strong).map(Response::new)
                        }
                        Ok(Err(_)) => Err(Status::unavailable("Matching query channel closed.")),
                        Err(_) => Err(Status::deadline_exceeded(
                                "Query task not completed before the deadline.",
                        )),
                    };
                }
                // `_registration` withdraws the unoffered entry; Go's
                // unbuffered send likewise leaves nothing behind on failure.
                let current = TaskListName::parse(&list.name)
                    .map_err(|_| Status::invalid_argument("TaskList is not valid."))?;
                if list.kind == 2
                    || current.is_root()
                    || degree <= 0
                    || tokio::time::Instant::now() >= deadline
                {
                    return Err(Status::internal(
                        "cannot find parent task list for forwarding",
                    ));
                }
                let parent = current.parent(degree);
                if parent.is_empty() {
                    return Err(Status::internal(
                        "cannot find parent task list for forwarding",
                    ));
                }
                match handle.try_forward_query() {
                    Some(permit) => {
                        permits.push(permit);
                        let forwarded_from = list.name.clone();
                        list.name = parent;
                        if let Some(channel) = self.remote_channel_for_task_list(&list.name).await? {
                                let mut request = Request::new(m::QueryWorkflowRequest {
                                    domain_id: r.domain_id.clone(),
                                    request: Some(payload.clone()),
                                    task_list: Some(list.clone()),
                                    forwarded_from,
                                });
                                request.set_timeout(
                                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                                );
                                crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
                                return m::matching_api_client::MatchingApiClient::new(channel)
                                    .query_workflow(request)
                                    .await;
                        }
                        handle = self.durable_queue(&r.domain_id, &list, 0).await?;
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
    }
    async fn get_task_lists_by_domain(
        &self,
        request: Request<m::GetTaskListsByDomainRequest>,
    ) -> Result<Response<m::GetTaskListsByDomainResponse>, Status> {
        let r = request.into_inner();
        if let Some(queues) = &self.queues {
            let domain = self
                .frontend
                .state
                .domains
                .domain_id(&r.domain)
                .map_err(|error| Status::not_found(error.message))?;
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
                    config.property_values("matching.matchingReturnAllTaskListKinds"),
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
        // Unlike the UUID-routed methods, this request carries the domain
        // name directly, like Go's `GetDomainID(request.GetDomain())`.
        let store = self
            .frontend
            .state
            .execution_store(&r.domain)
            .map_err(|e| Status::not_found(e.message))?;
        let (decisions, activities) = store.task_lists(&r.domain);
        let mut decision_task_list_map = std::collections::HashMap::new();
        for name in decisions {
            let v = p::workflow_api_server::WorkflowApi::describe_task_list(
                &self.frontend,
                Request::new(p::DescribeTaskListRequest {
                    domain: r.domain.clone(),
                    task_list: Some(p::TaskList {
                        name: name.clone(),
                        ..Default::default()
                    }),
                    task_list_type: 0,
                    include_task_list_status: true,
                }),
            )
            .await?
            .into_inner();
            decision_task_list_map.insert(
                name,
                m::DescribeTaskListResponse {
                    pollers: v.pollers,
                    task_list_status: v.task_list_status,
                    partition_config: None,
                    task_list: v.task_list,
                },
            );
        }
        let mut activity_task_list_map = std::collections::HashMap::new();
        for name in activities {
            let v = p::workflow_api_server::WorkflowApi::describe_task_list(
                &self.frontend,
                Request::new(p::DescribeTaskListRequest {
                    domain: r.domain.clone(),
                    task_list: Some(p::TaskList {
                        name: name.clone(),
                        ..Default::default()
                    }),
                    task_list_type: 1,
                    include_task_list_status: true,
                }),
            )
            .await?
            .into_inner();
            activity_task_list_map.insert(
                name,
                m::DescribeTaskListResponse {
                    pollers: v.pollers,
                    task_list_status: v.task_list_status,
                    partition_config: None,
                    task_list: v.task_list,
                },
            );
        }
        Ok(Response::new(m::GetTaskListsByDomainResponse {
            decision_task_list_map,
            activity_task_list_map,
        }))
    }
    async fn list_task_list_partitions(
        &self,
        request: Request<m::ListTaskListPartitionsRequest>,
    ) -> Result<Response<m::ListTaskListPartitionsResponse>, Status> {
        let r = request.into_inner();
        let task_list = r.task_list.unwrap_or_default();
        // Go resolves `request.GetDomain()` through the domain cache; the
        // single-process store is keyed by name and yields the UUID.
        let domain_id = self
            .frontend
            .state
            .domains
            .domain_id(&r.domain)
            .map_err(|e| Status::not_found(e.message))?;
        if task_list.name.is_empty() {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
        let name = task_list_name(&task_list.name)?;
        // Single process: report this host like Go's `getHostInfo`, but
        // enumerate every write partition like Go's `getAllPartitions` (root
        // first, then `__cadence|<root>/<i>`).
        let hostname = std::env::var("HOSTNAME").unwrap_or_default();
        let activity_task_list_partitions = self
            .task_list_partition_metadata(&domain_id, &name, 1, &hostname)
            .await?;
        let decision_task_list_partitions = self
            .task_list_partition_metadata(&domain_id, &name, 0, &hostname)
            .await?;
        Ok(Response::new(m::ListTaskListPartitionsResponse {
            activity_task_list_partitions,
            decision_task_list_partitions,
        }))
    }
    async fn acquire_semaphore(
        &self,
        request: Request<m::AcquireSemaphoreRequest>,
    ) -> Result<Response<m::AcquireSemaphoreResponse>, Status> {
        use crate::matching_semaphore::Identifier;
        use cadence_api::enums::SemaphoreAcquireOutcome;
        let r = request.into_inner();
        // Owner parsing mirrors Go's `ParseRequestOwner`.
        cadence_common::semaphore::parse_owner(&r.owner_id)
            .map_err(|e| Status::invalid_argument(format!("invalid owner id: {e}")))?;
        // The domain must exist, like Go's domain-cache lookup.
        self.domain_name(&r.domain_id)?;
        if !self.frontend.state.semaphore_enabled {
            return Err(Status::invalid_argument(
                "distributed semaphore is not enabled for this domain",
            ));
        }
        // Go `EnableDistributedSemaphore` is a per-domain flag; the standalone
        // host reads one global toggle, matching the rest of the server.
        let identifier =
            Identifier::new(&r.domain_id, &r.semaphore_name, r.bucket).map_err(semaphore_status)?;
        if let Some(membership) = &self.membership {
            membership.check_semaphore(&identifier)?;
        }
        // Idle buckets unload like Go's liveness callback.
        self.semaphores.sweep_idle(semaphore_now());
        let manager = self
            .semaphores
            .get_or_create(identifier)
            .await
            .map_err(semaphore_status)?;
        let result = manager.acquire(&r.owner_id).await.map_err(semaphore_status)?;
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
    async fn update_task_list_partition_config(
        &self,
        request: Request<m::UpdateTaskListPartitionConfigRequest>,
    ) -> Result<Response<m::UpdateTaskListPartitionConfigResponse>, Status> {
        let r = request.into_inner();
        self.domain_name(&r.domain_id)?;
        let task_list = r
            .task_list
            .clone()
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
        if task_list.name.is_empty() {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
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
        let kind_is_normal = matches!(task_list.kind, 0 | 1);
        if !kind_is_normal {
            return Err(Status::invalid_argument(
                "Only normal tasklist's partition config can be updated.",
            ));
        }
        let config = r.partition_config.ok_or_else(|| {
            Status::invalid_argument("Task list partition config is not set in the request.")
        })?;
        let name = task_list_name(&task_list.name)?;
        if !name.is_root() {
            return Err(Status::invalid_argument(
                "Only root partition's partition config can be updated.",
            ));
        }
        if let Some(registry) = &self.queues {
            if !matches!(r.task_list_type, 1 | 2) {
                return Err(Status::invalid_argument("TaskListType is invalid."));
            }
            let queue = self
                .durable_queue(&r.domain_id, &task_list, r.task_list_type - 1)
                .await?;
            registry
                .update_partition_config(
                    cadence_persistence::matching_sqlite::TaskListKey {
                        domain_id: uuid::Uuid::parse_str(&r.domain_id).expect("validated domain"),
                        name: task_list.name,
                        task_type: r.task_list_type - 1,
                    },
                    queue,
                    partition_config_from_proto(config),
                    matching_now(),
                )
                .await?;
            return Ok(Response::new(m::UpdateTaskListPartitionConfigResponse {}));
        }
        let store = self.frontend.state.executions.clone();
        let domain = self.domain_name(&r.domain_id)?;
        let stored = partition_config_from_proto(config);
        match store.update_partition_config(
            &domain,
            &task_list.name,
            r.task_list_type,
            kind_is_normal,
            stored,
        ) {
            Ok(()) => Ok(Response::new(m::UpdateTaskListPartitionConfigResponse {})),
            Err(
                cadence_persistence::TaskError::InvalidToken(e)
                | cadence_persistence::TaskError::BadDecision(e),
            ) => Err(Status::invalid_argument(e.message)),
            Err(cadence_persistence::TaskError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }
    async fn refresh_task_list_partition_config(
        &self,
        request: Request<m::RefreshTaskListPartitionConfigRequest>,
    ) -> Result<Response<m::RefreshTaskListPartitionConfigResponse>, Status> {
        let r = request.into_inner();
        let domain = self.domain_name(&r.domain_id)?;
        let task_list = r
            .task_list
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
        if task_list.name.is_empty() {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
        if !matches!(task_list.kind, 0 | 1) {
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
        if let Some(registry) = &self.queues {
            if !matches!(r.task_list_type, 1 | 2) {
                return Err(Status::invalid_argument("TaskListType is invalid."));
            }
            let queue = self
                .durable_queue(&r.domain_id, &task_list, r.task_list_type - 1)
                .await?;
            registry
                .refresh_partition_config(
                    cadence_persistence::matching_sqlite::TaskListKey {
                        domain_id: uuid::Uuid::parse_str(&r.domain_id).expect("validated domain"),
                        name: task_list.name,
                        task_type: r.task_list_type - 1,
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
            r.task_list_type,
            pushed,
        ) {
            Ok(()) => Ok(Response::new(m::RefreshTaskListPartitionConfigResponse {})),
            Err(
                cadence_persistence::TaskError::InvalidToken(e)
                | cadence_persistence::TaskError::BadDecision(e),
            ) => Err(Status::invalid_argument(e.message)),
            Err(cadence_persistence::TaskError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::TaskError::AlreadyCompleted(e)) => {
                Err(Status::failed_precondition(e.message))
            }
        }
    }
    async fn cancel_outstanding_poll(
        &self,
        request: Request<m::CancelOutstandingPollRequest>,
    ) -> Result<Response<m::CancelOutstandingPollResponse>, Status> {
        let r = request.into_inner();
        self.domain_name(&r.domain_id)?;
        if !matches!(r.task_list_type, 1 | 2) {
            return Err(Status::invalid_argument("TaskListType is invalid."));
        }
        let task_list = r
            .task_list
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?
            .name;
        task_list_name(&task_list)?;
        let key = PollKey {
            domain: r.domain_id,
            task_list,
            task_type: r.task_list_type,
            poller: r.poller_id,
        };
        let cancel = self
            .polls
            .lock()
            .expect("poll cancellation lock")
            .get(&key)
            .cloned();
        if let Some(cancel) = cancel {
            cancel.notify_one();
        }
        Ok(Response::new(m::CancelOutstandingPollResponse {}))
    }
    async fn respond_query_task_completed(
        &self,
        request: Request<m::RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<m::RespondQueryTaskCompletedResponse>, Status> {
        use crate::grpc_convert as cv;
        let r = request.into_inner();
        let mut payload = r
            .request
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
        if self.queues.is_some() {
            // Go `RespondQueryTaskCompleted` -> `deliverQueryResult`: hand the
            // worker's result to the waiting `QueryWorkflow` call.
            let task_list = r
                .task_list
                .clone()
                .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let queue = self.durable_queue(&r.domain_id, &task_list, 0).await?;
            let result = payload.result.clone().unwrap_or_default();
            let answer = crate::matching_query::WorkerAnswer {
                result,
                worker_version_info: payload.worker_version_info.clone(),
            };
            queue.queries.complete(
                &r.task_id,
                crate::matching_query::QueryOutcome::Worker(answer),
            )?;
            return Ok(Response::new(m::RespondQueryTaskCompletedResponse {}));
        }
        let input = cv::query_completed(payload)?;
        let _ = p::worker_api_server::WorkerApi::respond_query_task_completed(
            &self.frontend,
            Request::new(cv::query_completed_out(input)),
        )
        .await?;
        Ok(Response::new(m::RespondQueryTaskCompletedResponse {}))
    }
    async fn describe_task_list(
        &self,
        request: Request<m::DescribeTaskListRequest>,
    ) -> Result<Response<m::DescribeTaskListResponse>, Status> {
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let task_list = payload.task_list.clone();
        let task_list_type = payload.task_list_type;
        let task_list_name = task_list.clone().map(|t| t.name).unwrap_or_default();
        self::task_list_name(&task_list_name)?;
        if self.queues.is_some() {
            if !matches!(task_list_type, 1 | 2) {
                return Err(Status::invalid_argument("TaskListType is invalid."));
            }
            let queue = self
                .durable_queue(
                    &r.domain_id,
                    task_list.as_ref().expect("validated task list"),
                    task_list_type - 1,
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
                        seconds: poller.last_access_time.div_euclid(1_000_000_000),
                        nanos: poller.last_access_time.rem_euclid(1_000_000_000) as i32,
                    }),
                })
                .collect();
            let task_list_status = if payload.include_task_list_status {
                let (read_level, ack_level, backlog_count_hint, start_id, end_id, empty) =
                    queue.queue.status();
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
                            config.property_values("matching.taskIsolationPollerWindow"),
                            Some(&payload.domain),
                            &task_list_name,
                            task_list_type - 1,
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
                        poller_count: counts.get(&group).copied().unwrap_or_default() as i64,
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
            let partition_config = queue.partitions.get().map(partition_config_to_proto);
            return Ok(Response::new(m::DescribeTaskListResponse {
                pollers,
                task_list_status,
                task_list,
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
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_with_listener(listener, matching).await
}

/// Serve a prebound Matching socket so production can fail listener startup
/// before Ringpop advertises this node's gRPC endpoint.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener,
    matching: Matching,
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
        .build_v1()?;
    tonic::transport::Server::builder()
        .add_service(reflection)
        .add_service(
            m::matching_api_server::MatchingApiServer::new(matching)
                .max_decoding_message_size(limit),
        )
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "matching_rpc_tests.rs"]
mod durable_tests;
