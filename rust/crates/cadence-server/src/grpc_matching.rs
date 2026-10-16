//! Native internal MatchingAPI adapters and outstanding-poll cancellation.
//! Queue storage currently shares the single-process Rust execution engine.
use crate::{grpc::Frontend, grpc_internal::History};
use cadence_proto::{api as p, matching as m, shared};
use cadence_common::task_list::TaskListName;

fn matching_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|time| time.as_nanos() as i64).unwrap_or_default()
}
/// Resolve a domain UUID to its name the way Go's domain cache does before
/// applying a dynamic-property filter. Unknown or unnamed domains fall back to
/// unfiltered values.
fn domain_name_for(domains: &cadence_persistence::InMemoryDomainStore, domain_id: &str) -> Option<String> {
    domains
        .describe(&cadence_api::service_types::DescribeDomainRequest {
            name: None,
            uuid: Some(domain_id.to_owned()),
        })
        .ok()
        .and_then(|response| response.domain_info.map(|info| info.name))
        .filter(|name| !name.is_empty())
}
/// Go `taskListManagerImpl.AddTask` QPS reporting: an arrival counts toward the
/// root, and toward its isolation group when the task carries one.
fn report_arrival(
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
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[allow(deprecated)]
fn partition_config_from_proto(config: p::TaskListPartitionConfig) -> cadence_persistence::StoredPartitionConfig {
    let convert = |count, map| cadence_common::task_list::partition_map::<p::TaskListPartition>(count, map)
        .into_iter().map(|(id, partition)| (id, partition.isolation_groups)).collect();
    cadence_persistence::StoredPartitionConfig {
        version: config.version,
        read_partitions: convert(config.num_read_partitions, config.read_partitions),
        write_partitions: convert(config.num_write_partitions, config.write_partitions),
    }
}

#[allow(deprecated)]
fn partition_config_to_proto(c: cadence_persistence::StoredPartitionConfig) -> p::TaskListPartitionConfig {
    let partitions = |map: std::collections::HashMap<i32, Vec<String>>| map.into_iter().map(|(id, groups)| (id, p::TaskListPartition { isolation_groups: groups })).collect();
    p::TaskListPartitionConfig { version: c.version, num_read_partitions: c.read_partitions.len() as i32, num_write_partitions: c.write_partitions.len() as i32, read_partitions: partitions(c.read_partitions), write_partitions: partitions(c.write_partitions) }
}

#[allow(deprecated)]
fn partition_config_to_internal(c: cadence_persistence::StoredPartitionConfig) -> m::TaskListPartitionConfig {
    let partitions = |map: std::collections::HashMap<i32, Vec<String>>| map.into_iter().map(|(id, groups)| (id, m::TaskListPartition { isolation_groups: groups })).collect();
    m::TaskListPartitionConfig { version: c.version, num_read_partitions: c.read_partitions.len() as i32, num_write_partitions: c.write_partitions.len() as i32, read_partitions: partitions(c.read_partitions), write_partitions: partitions(c.write_partitions) }
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
}impl Drop for PollGuard {
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
    poll_expiration: std::time::Duration,
}
impl Matching {
    pub fn new(state: crate::handlers::AppState) -> Self {
        Self {
            frontend: Frontend { state },
            polls: Arc::new(Mutex::new(HashMap::new())),
            queues: None,
            partition_count: None,
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
            Arc::new(move |domain_id: &str, task_list: &str, task_type: i32| -> i64 {
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
            })
        };
        let read_provider: crate::matching_registry::PartitionCountProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(config.property_values("matching.numTasklistReadPartitions"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 1)
            })
        };
        let sync_wait: crate::matching_registry::SyncWaitProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_duration(config.property_values("matching.localTaskWaitTime"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 10_000_000)
            })
        };
        let cache_enabled: crate::matching_registry::PartitionCacheEnabledProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_boolean(config.property_values("matching.enableGetNumberOfPartitionsFromCache"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, false)
            })
        };
        // Go `cfg.QPSTrackerInterval(domain, taskList, type)`
        // (`matching.qpsTrackerInterval`, default 10s in nanoseconds).
        let qps_interval: crate::matching_registry::QpsIntervalProvider = {
            let overlay = overlay.clone();
            let domains = domains.clone();
            Arc::new(move |domain_id: &str, task_list: &str, task_type: i32| -> i64 {
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
            })
        };
        let forward_degree: crate::matching_registry::ForwardDegreeProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(config.property_values("matching.forwarderMaxChildrenPerNode"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 20)
            })
        };
        let forward_polls: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(config.property_values("matching.forwarderMaxOutstandingPolls"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 1)
            })
        };
        let forward_tasks: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(config.property_values("matching.forwarderMaxOutstandingTasks"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 1)
            })
        };
        let forward_rate: crate::matching_registry::ForwardPollsProvider = {
            let domains = domains.clone();
            let overlay = overlay.clone();
            Arc::new(move |domain_id, task_list, task_type| {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(config.property_values("matching.forwarderMaxRatePerSecond"), domain_name_for(&domains, domain_id).as_deref(), task_list, task_type, 10)
            })
        };
        let isolation: crate::matching_registry::IsolationProvider = Arc::new(move |domain_id, task_list, task_type, kind, pollers, partitions| {
            let name = domain_name_for(&domains, domain_id);
            let config = overlay.lock().expect("admin lock");
            let enabled = cadence_persistence::dynamic_config::boolean(config.property_values("system.enableTasklistIsolation"), name.as_deref(), false);
            let groups = if enabled && kind != 1 { cadence_persistence::dynamic_config::string_list(config.property_values("system.allIsolationGroups")) } else { Vec::new() };
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
                    cadence_persistence::dynamic_config::boolean(config.property_values("system.enableTasklistIsolation"), reload_name.as_deref(), false) != enabled
                }),
                select: Arc::new(move |task| {
                    let group = task.partition_config.as_ref().and_then(|map| map.get(ISOLATION_GROUP_KEY)).map(String::as_str).unwrap_or_default();
                    let now = matching_now();
                    let config = overlay.lock().expect("admin lock");
                    let duration = |key, default| cadence_persistence::dynamic_config::task_list_duration(config.property_values(key), name.as_deref(), &task_list, task_type, default);
                    let window = duration("matching.taskIsolationPollerWindow", 2_000_000_000);
                    let total_duration = duration("matching.taskIsolationDuration", 200_000_000);
                    drop(config);
                    let drained = name.as_deref().ok_or(()).and_then(|name| domains.domain_isolation_groups(name).map_err(|_| ())).map(|config| {
                        config.into_iter().flat_map(|config| config.isolation_groups).any(|partition| partition.name == group && partition.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                        || domains.global_isolation_groups().into_iter().flat_map(|config| config.isolation_groups).any(|partition| partition.name == group && partition.state == Some(cadence_api::enums::IsolationGroupState::Drained))
                    });
                    let partition = partitions.get();
                    let partition_id = TaskListName::parse(&task_list).map(|name| name.partition() as i32).unwrap_or_default();
                    let partition_groups = partition.as_ref().map(|config| config.read_partitions.get(&partition_id));
                    let empty = Vec::new();
                    let policy = cadence_common::task_isolation::TaskIsolation {
                        enabled, sticky: kind == 1, group, known: known.iter().any(|known| known == group), drained,
                        recent_poller: pollers.lock().expect("poller history lock").has_group_after(group, now.saturating_sub(window)),
                        partition_groups: match partition_groups { None => Some(empty.as_slice()), Some(groups) => groups.map(Vec::as_slice) },
                        total_duration, task_latency: now.saturating_sub(task.created_nanos),
                    };
                    let (group, duration) = policy.select();
                    (group.to_owned(), duration)
                }),
            }
        });
        let queues = queues
            .with_partition_count(read_provider)
            .with_partition_cache_enabled(cache_enabled)
            .with_sync_wait(sync_wait)
            .with_forward_degree(forward_degree)
            .with_forward_polls(forward_polls)
            .with_forward_tasks(forward_tasks, forward_rate)
            .with_qps_interval(qps_interval)
            .with_isolation(isolation)
            .with_domain_metadata(state.domains.clone(), state.current_cluster_name.clone());
        let mut matching = Self::new(state);
        matching.queues = Some(queues);
        matching.partition_count = Some(provider);
        matching.poll_expiration = poll_expiration;
        matching
    }
    async fn durable_queue(&self, domain: &str, list: &p::TaskList, task_type: i32) -> Result<crate::matching_registry::QueueHandle, Status> {
        task_list_name(&list.name)?;
        let domain_id = uuid::Uuid::parse_str(domain).map_err(|_| Status::invalid_argument("Domain UUID is not valid."))?;
        let kind = match list.kind { 2 => 1, 3 => 2, _ => 0 };
        self.queues.as_ref().expect("durable backend selected").get(
            cadence_persistence::matching_sqlite::TaskListKey { domain_id, name: list.name.clone(), task_type }, kind, matching_now(),
        ).await
    }
    fn check_write_partition(&self, domain_id: &str, list: &p::TaskList, task_type: i32, queue: &crate::matching_registry::QueueHandle) -> Result<(), Status> {
        let name = task_list_name(&list.name)?;
        let config = self.frontend.state.admin.dynamic_config.lock().expect("admin lock");
        let enabled = cadence_persistence::dynamic_config::task_list_boolean(config.property_values("matching.enableGetNumberOfPartitionsFromCache"), domain_name_for(&self.frontend.state.domains, domain_id).as_deref(), name.root(), task_type, false);
        drop(config);
        if enabled && queue.partitions.get().is_some_and(|config| !config.write_partitions.contains_key(&(name.partition() as i32))) {
            return Err(crate::grpc::detailed_status(tonic::Code::Aborted, "Current partition is drained.", "ReadOnlyPartitionError", p::ReadOnlyPartitionError {}));
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
    async fn sync_match_chain(&self, add: ForwardedAdd<'_>) -> Result<Option<Option<m::TaskListPartitionConfig>>, Status> {
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
        } = add;
        let name = task_list_name(&list.name)?;
        let degree = {
            let config = self.frontend.state.admin.dynamic_config.lock().expect("admin lock");
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
                handle.try_sync_match_forwarded(data, source, deadline).await
            } else {
                handle.try_sync_match(data, source, deadline).await
            };
            if matched {
                return Ok(Some(entry.partitions.get().map(partition_config_to_internal)));
            }
            let name = TaskListName::parse(&list.name).map_err(|_| Status::invalid_argument("TaskList is not valid."))?;
            if backlog && (ancestor || forwarded) && name.is_root() {
                return Ok(handle.offer_forwarded_backlog(data, source, deadline).await.then(|| entry.partitions.get().map(partition_config_to_internal)));
            }
            if name.is_root() || list.kind == 2 || degree <= 0 || tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            let parent = name.parent(degree);
            if parent.is_empty() {
                return Ok(None);
            }
            let Some(permit) = handle.try_forward_task() else { return Ok(None); };
            permits.push(permit);
            list.name = parent;
            handle = match self.durable_queue(domain_id, &list, task_type).await {
                Ok(handle) => handle,
                Err(_) => return Ok(None),
            };
            if handle.stop_if_isolation_changed() || self.check_write_partition(domain_id, &list, task_type, &handle).is_err() {
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
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
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
            let queue = self.durable_queue(&r.domain_id, payload.task_list.as_ref().expect("validated task list"), 0).await?;
            if queue.stop_if_isolation_changed() { return Ok(Response::new(m::PollForDecisionTaskResponse::default())); }
            let lifecycle = crate::matching_dispatch::PollLifecycle { manager: queue.pollers.clone(), id: guard.key.poller.clone(), info: cadence_common::poller::Info { identity: payload.identity.clone(), rate_per_second: crate::matching_limiter::TASK_DISPATCH_RPS, isolation_group: r.isolation_group } };
            // Go's matcher serves `queryTaskC` on the same long poll as the
            // decision backlog; a ready query is handed to this poller instead
            // of waiting for a durable task. Racing is safe: dropping the
            // dispatch future makes its worker rewrite any claimed task.
            let query_lifecycle = lifecycle.clone();
            return tokio::select! {
                biased;
                query=queue.queries.next()=>{
                    let _wait=query_lifecycle.begin();
                    let execution=query.request.workflow_execution.clone().unwrap_or_default();
                    match queue.dispatcher.query_mutable_state(&execution).await {
                        Ok(state)=>{
                            let mut response=crate::matching_query::query_response(query, &state);
                            response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                            Ok(Response::new(response))
                        }
                        // Go fails the waiting query and returns an empty poll.
                        Err(error)=>{
                            let _=queue.queries.complete(&query.task_id, crate::matching_query::QueryOutcome::Internal(error));
                            Ok(Response::new(m::PollForDecisionTaskResponse { load_balancer_hints: queue.load_balancer_hints(matching_now()), partition_config: queue.partitions.get().map(partition_config_to_internal), ..Default::default() }))
                        }
                    }
                }
                result=queue.dispatcher.poll_decision_with_poller(payload, self.poll_expiration, lifecycle)=>
                    result.map(|mut response| {
                        response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                        Response::new(response)
                    }),
                _=guard.cancel.notified()=>Err(Status::cancelled("Outstanding poll cancelled.")),
            };
        }
        let v = tokio::select! {
            result=p::worker_api_server::WorkerApi::poll_for_decision_task(&self.frontend,Request::new(payload))=>result?.into_inner(),
            _=guard.cancel.notified()=>return Err(Status::cancelled("Outstanding poll cancelled.")),
        };
        let sticky_execution_enabled = !v.task_token.is_empty()
            && v.history
                .as_ref()
                .and_then(|history| history.events.first())
                .is_some_and(|event| event.event_id > 1);
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
            auto_config_hint: v.auto_config_hint,
            ..Default::default()
        }))
    }
    async fn poll_for_activity_task(
        &self,
        request: Request<m::PollForActivityTaskRequest>,
    ) -> Result<Response<m::PollForActivityTaskResponse>, Status> {
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
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
            let queue = self.durable_queue(&r.domain_id, payload.task_list.as_ref().expect("validated task list"), 1).await?;
            if queue.stop_if_isolation_changed() { return Ok(Response::new(m::PollForActivityTaskResponse::default())); }
            // Go `getTask`: the worker-reported rate is applied to the shared
            // dispatch limiter before the poll is recorded; a missing or
            // negative metadata rate keeps the configured default for poller
            // history without resetting the shared limiter.
            let max_dispatch = payload.task_list_metadata.as_ref().and_then(|metadata| metadata.max_tasks_per_second);
            let rate = match max_dispatch.filter(|rate| *rate >= 0.0) {
                Some(rate) => {
                    queue.limiter.report_limit(rate);
                    rate
                }
                None => crate::matching_limiter::TASK_DISPATCH_RPS,
            };
            let lifecycle = crate::matching_dispatch::PollLifecycle { manager: queue.pollers.clone(), id: guard.key.poller.clone(), info: cadence_common::poller::Info { identity: payload.identity.clone(), rate_per_second: rate, isolation_group: r.isolation_group } };
            return tokio::select! {
                result=queue.dispatcher.poll_activity_with_poller(payload, self.poll_expiration, lifecycle)=>
                    result.map(|mut response| {
                        response.load_balancer_hints=queue.load_balancer_hints(matching_now());
                            response.partition_config=queue.partitions.get().map(partition_config_to_internal);
                        Response::new(response)
                    }),
                _=guard.cancel.notified()=>Err(Status::cancelled("Outstanding poll cancelled.")),
            };
        }
        let v = tokio::select! {
            result=p::worker_api_server::WorkerApi::poll_for_activity_task(&self.frontend,Request::new(payload))=>result?.into_inner(),
            _=guard.cancel.notified()=>return Err(Status::cancelled("Outstanding poll cancelled.")),
        };
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
            auto_config_hint: v.auto_config_hint,
            ..Default::default()
        }))
    }
    async fn add_decision_task(
        &self,
        request: Request<m::AddDecisionTaskRequest>,
    ) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        let parent_deadline = crate::matching_sync::request_deadline(&request);
        let r = request.into_inner();
        task_list_name(r.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default())?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            let list = r.task_list.as_ref().ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r.workflow_execution.as_ref().ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id).map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            let source = uuid::Uuid::parse_str(&r.domain_id).expect("domain validated");
            let queue = self.durable_queue(&r.domain_id, list, 0).await?;
            if queue.stop_if_isolation_changed() { return Err(Status::unavailable("task list shutting down")); }
            self.check_write_partition(&r.domain_id, list, 0, &queue)?;
            check_sticky_poller(&queue, list)?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(), run_id: Some(run.as_bytes().to_vec()), schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() { None } else { Some(r.partition_config.into_iter().collect()) }, ..Default::default()
            }.at_creation(matching_now(), 0);
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
                parent_deadline.unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_millis(200))
            } else {
                let remaining = parent_deadline.map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()));
                tokio::time::Instant::now() + crate::matching_sync::sync_match_budget(remaining)
            };
            let active = self.frontend.state.domains.is_active_in(&source.to_string(), &self.frontend.state.current_cluster_name).map_err(|error| Status::not_found(error.message))?;
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
                    })
                    .await?;
                if let Some(partition_config) = matched {
                    return Ok(Response::new(m::AddDecisionTaskResponse { partition_config }));
                }
            }
            // Go `errRemoteSyncMatchFailed`: a forwarded add never persists;
            // the child partition keeps the task when sync matching fails.
            if forwarded {
                return Err(Self::remote_sync_failed());
            }
            queue.enqueue(data, source, r.schedule_to_start_timeout.map(|timeout| timeout.seconds as i32).unwrap_or_default()).await?;
            return Ok(Response::new(m::AddDecisionTaskResponse { partition_config: queue.partitions.get().map(partition_config_to_internal) }));
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
        task_list_name(r.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default())?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            let list = r.task_list.as_ref().ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r.workflow_execution.as_ref().ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id).map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            self.domain_name(&r.source_domain_id)?;
            let source = uuid::Uuid::parse_str(&r.source_domain_id).map_err(|_| Status::invalid_argument("Source domain UUID is not valid."))?;
            let queue = self.durable_queue(&r.domain_id, list, 1).await?;
            if queue.stop_if_isolation_changed() { return Err(Status::unavailable("task list shutting down")); }
            self.check_write_partition(&r.domain_id, list, 1, &queue)?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(), run_id: Some(run.as_bytes().to_vec()), schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() { None } else { Some(r.partition_config.into_iter().collect()) }, ..Default::default()
            }.at_creation(matching_now(), 0);
            let forwarded = !r.forwarded_from.is_empty();
            if !forwarded {
                report_arrival(&queue, &data);
            }
            let deadline = if forwarded {
                parent_deadline.unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_millis(200))
            } else {
                let remaining = parent_deadline.map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()));
                tokio::time::Instant::now() + crate::matching_sync::sync_match_budget(remaining)
            };
            let active = self.frontend.state.domains.is_active_in(&source.to_string(), &self.frontend.state.current_cluster_name).map_err(|error| Status::not_found(error.message))?;
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
                    })
                    .await?;
                if let Some(partition_config) = matched {
                    return Ok(Response::new(m::AddActivityTaskResponse { partition_config }));
                }
            }
            if forwarded {
                return Err(Self::remote_sync_failed());
            }
            queue.enqueue(data, source, r.schedule_to_start_timeout.map(|timeout| timeout.seconds as i32).unwrap_or_default()).await?;
            return Ok(Response::new(m::AddActivityTaskResponse { partition_config: queue.partitions.get().map(partition_config_to_internal) }));
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
        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
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
                let config = self.frontend.state.admin.dynamic_config.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values("matching.forwarderMaxChildrenPerNode"),
                    domain_name_for(&self.frontend.state.domains, &r.domain_id).as_deref(),
                    root.as_str(),
                    0,
                    20,
                )
            };
            let deadline = tokio::time::Instant::now() + self.poll_expiration;
            let mut list = task_list.clone();
            let mut handle = queue;
            let mut permits = Vec::new();
            let mut force_local = false;
            loop {
                let query = crate::matching_query::PendingQuery {
                    task_id: task_id.clone(),
                    domain_id: r.domain_id.clone(),
                    task_list: root.clone(),
                    request: payload.clone(),
                };
                let receiver = handle.queries.register(query.clone());
                let _registration = handle.queries.registration(&task_id);
                if force_local || handle.queries.try_offer(query) {
                    return match tokio::time::timeout_at(deadline, receiver).await {
                        Ok(Ok(result)) => crate::matching_query::query_outcome(result).map(Response::new),
                        Ok(Err(_)) => Err(Status::unavailable("Matching query channel closed.")),
                        Err(_) => {
                            Err(Status::deadline_exceeded(
                                "Query task not completed before the deadline.",
                            ))
                        }
                    };
                }
                // `_registration` withdraws the unoffered entry; Go's
                // unbuffered send likewise leaves nothing behind on failure.
                let current = TaskListName::parse(&list.name).map_err(|_| Status::invalid_argument("TaskList is not valid."))?;
                if list.kind == 2 || current.is_root() || degree <= 0 || tokio::time::Instant::now() >= deadline {
                    return Err(Status::internal("cannot find parent task list for forwarding"));
                }
                let parent = current.parent(degree);
                if parent.is_empty() {
                    return Err(Status::internal("cannot find parent task list for forwarding"));
                }
                match handle.try_forward_query() {
                    Some(permit) => {
                        permits.push(permit);
                        list.name = parent;
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
        let partitions = |task_type: i32| -> Vec<p::TaskListPartitionMetadata> {
            let count = self
                .partition_count
                .as_ref()
                .map(|provider| provider(&domain_id, name.root(), task_type))
                .unwrap_or(1)
                .max(1);
            (0..count)
                .map(|partition| p::TaskListPartitionMetadata {
                    key: name.partition_name(partition),
                    owner_host_name: hostname.clone(),
                })
                .collect()
        };
        Ok(Response::new(m::ListTaskListPartitionsResponse {
            activity_task_list_partitions: partitions(1),
            decision_task_list_partitions: partitions(0),
        }))
    }
    async fn acquire_semaphore(
        &self,
        request: Request<m::AcquireSemaphoreRequest>,
    ) -> Result<Response<m::AcquireSemaphoreResponse>, Status> {
        use cadence_api::enums::SemaphoreAcquireOutcome;
        let r = request.into_inner();
        // Identifier validation mirrors Go's `NewIdentifier`.
        if r.domain_id.is_empty() {
            return Err(Status::invalid_argument("domainID is required."));
        }
        if r.semaphore_name.is_empty() {
            return Err(Status::invalid_argument("semaphoreName is required."));
        }
        if r.bucket < 0 {
            return Err(Status::invalid_argument(format!(
                "bucket must not be negative, got {}.",
                r.bucket
            )));
        }
        if r.owner_id.is_empty() {
            return Err(Status::invalid_argument("ownerID is required."));
        }
        // Owner parsing mirrors Go's `ParseRequestOwner`.
        cadence_common::semaphore::parse_owner(&r.owner_id)
            .map_err(|e| Status::invalid_argument(format!("invalid owner id: {e}")))?;
        // The domain must exist, like Go's domain-cache lookup.
        self.domain_name(&r.domain_id)?;
        if !self.frontend.state.semaphore_enabled {
            return Err(Status::failed_precondition(
                "Semaphores are not enabled on this host.",
            ));
        }
        // Single host serves every bucket: no ownership routing needed.
        let store = self
            .frontend
            .state
            .executions
            .clone();
        match store.acquire_semaphore(&r.domain_id, &r.semaphore_name, r.bucket, &r.owner_id) {
            Ok((outcome, token_id)) => {
                let outcome = match outcome {
                    SemaphoreAcquireOutcome::Acquired => 1,
                    SemaphoreAcquireOutcome::NoSlot => 2,
                    SemaphoreAcquireOutcome::Invalid => 0,
                    _ => 0,
                };
                Ok(Response::new(m::AcquireSemaphoreResponse {
                    outcome,
                    token_id,
                }))
            }
            Err(cadence_persistence::AcquireError::NotFound(e)) => Err(Status::not_found(e.message)),
            Err(cadence_persistence::AcquireError::Invalid(message)) => {
                Err(Status::invalid_argument(message))
            }
        }
    }
    async fn update_task_list_partition_config(
        &self,
        request: Request<m::UpdateTaskListPartitionConfigRequest>,
    ) -> Result<Response<m::UpdateTaskListPartitionConfigResponse>, Status> {
        let r = request.into_inner();
        self.domain_name(&r.domain_id)?;
        let task_list = r
            .task_list
            .ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
        if task_list.name.is_empty() {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
        let kind_is_normal = matches!(task_list.kind, 0 | 1);
        if !kind_is_normal {
            return Err(Status::invalid_argument("Only normal tasklist's partition config can be updated."));
        }
        let config = r
            .partition_config
            .ok_or_else(|| Status::invalid_argument(
                "Task list partition config is not set in the request.",
            ))?;
        let name = task_list_name(&task_list.name)?;
        if !name.is_root() {
            return Err(Status::invalid_argument("Only root partition's partition config can be updated."));
        }
        if let Some(registry) = &self.queues {
            if !matches!(r.task_list_type, 1 | 2) { return Err(Status::invalid_argument("TaskListType is invalid.")); }
            let queue = self.durable_queue(&r.domain_id, &task_list, r.task_list_type - 1).await?;
            registry.update_partition_config(
                cadence_persistence::matching_sqlite::TaskListKey { domain_id: uuid::Uuid::parse_str(&r.domain_id).expect("validated domain"), name: task_list.name, task_type: r.task_list_type - 1 },
                queue, partition_config_from_proto(config), matching_now(),
            ).await?;
            return Ok(Response::new(m::UpdateTaskListPartitionConfigResponse {}));
        }
        let store = self.frontend.state.executions.clone();
        let domain = self.domain_name(&r.domain_id)?;
        let stored = partition_config_from_proto(config);
        match store.update_partition_config(&domain, &task_list.name, r.task_list_type, kind_is_normal, stored) {
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
            return Err(Status::invalid_argument("Only normal tasklist's partition config can be updated."));
        }
        let name = task_list_name(&task_list.name)?;
        if name.is_root() && r.partition_config.is_some() {
            return Err(Status::invalid_argument("PartitionConfig must be nil for root partition."));
        }
        if let Some(registry) = &self.queues {
            if !matches!(r.task_list_type, 1 | 2) { return Err(Status::invalid_argument("TaskListType is invalid.")); }
            let queue = self.durable_queue(&r.domain_id, &task_list, r.task_list_type - 1).await?;
            registry.refresh_partition_config(
                cadence_persistence::matching_sqlite::TaskListKey { domain_id: uuid::Uuid::parse_str(&r.domain_id).expect("validated domain"), name: task_list.name, task_type: r.task_list_type - 1 },
                queue, r.partition_config.map(partition_config_from_proto),
            ).await?;
            return Ok(Response::new(m::RefreshTaskListPartitionConfigResponse {}));
        }
        let store = self.frontend.state.executions.clone();
        let pushed = r.partition_config.map(partition_config_from_proto);
        match store.refresh_partition_config_with(&domain, &task_list.name, r.task_list_type, pushed) {
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
    ) -> Result<Response<m::CancelOutstandingPollResponse>, Status> {        let r = request.into_inner();
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
                    return Err(Status::invalid_argument("TaskToken does not match request."));
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
            queue
                .queries
                .complete(&r.task_id, crate::matching_query::QueryOutcome::Worker(result))?;
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
    ) -> Result<Response<m::DescribeTaskListResponse>, Status> {        let r = request.into_inner();
        let mut payload = r
            .request
            .ok_or_else(|| Status::invalid_argument("Request is not set."))?;
        payload.domain = self.domain_name(&r.domain_id)?;
        let task_list = payload.task_list.clone();
        let task_list_type = payload.task_list_type;
        let task_list_name = task_list.clone().map(|t| t.name).unwrap_or_default();
        self::task_list_name(&task_list_name)?;
        if self.queues.is_some() {
            if !matches!(task_list_type, 1 | 2) { return Err(Status::invalid_argument("TaskListType is invalid.")); }
            let queue = self.durable_queue(&r.domain_id, task_list.as_ref().expect("validated task list"), task_list_type - 1).await?;
            let pollers = queue.pollers.lock().expect("poller history lock").list(matching_now()).into_iter().map(|poller| p::PollerInfo {
                identity: poller.identity, rate_per_second: poller.rate_per_second,
                last_access_time: Some(prost_types::Timestamp { seconds: poller.last_access_time.div_euclid(1_000_000_000), nanos: poller.last_access_time.rem_euclid(1_000_000_000) as i32 }),
            }).collect();
            let task_list_status = if payload.include_task_list_status {
                let (read_level, ack_level, backlog_count_hint, start_id, end_id, empty) = queue.queue.status();
                // Go reads the configured group universe and the live poller
                // history window on every Describe, even with isolation off.
                let (groups, window) = {
                    let config = self.frontend.state.admin.dynamic_config.lock().expect("admin lock");
                    (
                        cadence_persistence::dynamic_config::string_list(config.property_values("system.allIsolationGroups")),
                        cadence_persistence::dynamic_config::task_list_duration(
                            config.property_values("matching.taskIsolationPollerWindow"),
                            Some(&payload.domain), &task_list_name, task_list_type - 1, 2_000_000_000,
                        ),
                    )
                };
                let now = matching_now();
                let counts = queue.pollers.lock().expect("poller history lock").count_by_group(now, now.saturating_sub(window));
                let isolation_group_metrics = groups.into_iter().map(|group| {
                    let metrics = p::IsolationGroupMetrics {
                        new_tasks_per_second: queue.qps.group_qps(&group),
                        poller_count: counts.get(&group).copied().unwrap_or_default() as i64,
                    };
                    (group, metrics)
                }).collect();
                Some(p::TaskListStatus { read_level, ack_level, backlog_count_hint, task_id_block: Some(p::TaskIdBlock { start_id, end_id }), empty, rate_per_second: queue.limiter.limit(), new_tasks_per_second: queue.qps.qps(), isolation_group_metrics })
            } else { None };
            let partition_config = queue.partitions.get().map(partition_config_to_proto);
            return Ok(Response::new(m::DescribeTaskListResponse { pollers, task_list_status, task_list, partition_config }));
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
    let limit = matching.frontend.state.grpc_max_message_size;
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
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "matching_rpc_tests.rs"]
mod durable_tests;
