//! Native internal MatchingAPI adapters and outstanding-poll cancellation.
//! Queue storage currently shares the single-process Rust execution engine.
use crate::{grpc::Frontend, grpc_internal::History};
use cadence_proto::{api as p, matching as m};
use cadence_common::task_list::TaskListName;

fn matching_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|time| time.as_nanos() as i64).unwrap_or_default()
}
fn task_list_name(name: &str) -> Result<TaskListName, Status> {
    TaskListName::parse(name).map_err(Status::invalid_argument)
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
    poll_expiration: std::time::Duration,
}
impl Matching {
    pub fn new(state: crate::handlers::AppState) -> Self {
        Self {
            frontend: Frontend { state },
            polls: Arc::new(Mutex::new(HashMap::new())),
            queues: None,
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
        let queues = queues.with_domain_metadata(state.domains.clone(), state.current_cluster_name.clone());
        let mut matching = Self::new(state);
        matching.queues = Some(queues);
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
            let lifecycle = crate::matching_dispatch::PollLifecycle { manager: queue.pollers.clone(), id: guard.key.poller.clone(), info: cadence_common::poller::Info { identity: payload.identity.clone(), rate_per_second: 100000.0, isolation_group: String::new() } };
            return tokio::select! {
                result=queue.dispatcher.poll_decision_with_poller(payload, self.poll_expiration, lifecycle)=>result.map(Response::new),
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
            let lifecycle = crate::matching_dispatch::PollLifecycle { manager: queue.pollers.clone(), id: guard.key.poller.clone(), info: cadence_common::poller::Info { identity: payload.identity.clone(), rate_per_second: payload.task_list_metadata.as_ref().and_then(|metadata| metadata.max_tasks_per_second).filter(|rate| *rate >= 0.0).unwrap_or(100000.0), isolation_group: String::new() } };
            return tokio::select! {
                result=queue.dispatcher.poll_activity_with_poller(payload, self.poll_expiration, lifecycle)=>result.map(Response::new),
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
        let r = request.into_inner();
        task_list_name(r.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default())?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            if !r.forwarded_from.is_empty() { return Err(Status::unimplemented("Durable forwarded sync matching is not migrated yet.")); }
            let list = r.task_list.as_ref().ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r.workflow_execution.as_ref().ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id).map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            let source = uuid::Uuid::parse_str(&r.domain_id).expect("domain validated");
            let queue = self.durable_queue(&r.domain_id, list, 0).await?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(), run_id: Some(run.as_bytes().to_vec()), schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() { None } else { Some(r.partition_config.into_iter().collect()) }, ..Default::default()
            };
            queue.enqueue(data, source, r.schedule_to_start_timeout.map(|timeout| timeout.seconds as i32).unwrap_or_default()).await?;
            return Ok(Response::new(m::AddDecisionTaskResponse { partition_config: None }));
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
        let r = request.into_inner();
        task_list_name(r.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or_default())?;
        let domain = self.domain_name(&r.domain_id)?;
        if self.queues.is_some() {
            if !r.forwarded_from.is_empty() { return Err(Status::unimplemented("Durable forwarded sync matching is not migrated yet.")); }
            let list = r.task_list.as_ref().ok_or_else(|| Status::invalid_argument("TaskList is not set."))?;
            let execution = r.workflow_execution.as_ref().ok_or_else(|| Status::invalid_argument("WorkflowExecution is not set."))?;
            let run = uuid::Uuid::parse_str(&execution.run_id).map_err(|_| Status::invalid_argument("Run UUID is not valid."))?;
            self.domain_name(&r.source_domain_id)?;
            let source = uuid::Uuid::parse_str(&r.source_domain_id).map_err(|_| Status::invalid_argument("Source domain UUID is not valid."))?;
            let queue = self.durable_queue(&r.domain_id, list, 1).await?;
            let data = cadence_persistence::matching_blob::TaskData {
                workflow_id: execution.workflow_id.clone(), run_id: Some(run.as_bytes().to_vec()), schedule_id: r.schedule_id,
                partition_config: if r.partition_config.is_empty() { None } else { Some(r.partition_config.into_iter().collect()) }, ..Default::default()
            };
            queue.enqueue(data, source, r.schedule_to_start_timeout.map(|timeout| timeout.seconds as i32).unwrap_or_default()).await?;
            return Ok(Response::new(m::AddActivityTaskResponse { partition_config: None }));
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
        // Single-host deployment: every list has exactly its root partition,
        // mirroring Go's `getAllPartitions` without a partition config.
        let task_list = r.task_list.unwrap_or_default();
        self.frontend
            .state
            .execution_store(&r.domain)
            .map_err(|e| Status::not_found(e.message))?;
        if task_list.name.is_empty() {
            return Err(Status::invalid_argument("TaskList is not set."));
        }
        // Single process: report this host like Go's `getHostInfo`.
        let hostname = std::env::var("HOSTNAME").unwrap_or_default();
        let name = task_list_name(&task_list.name)?;
        let partition = p::TaskListPartitionMetadata {
            key: name.root().to_owned(),
            owner_host_name: hostname,
        };
        Ok(Response::new(m::ListTaskListPartitionsResponse {
            activity_task_list_partitions: vec![partition.clone()],
            decision_task_list_partitions: vec![partition],
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
                task_list: r.task_list.map(|t| t.name).unwrap_or_default(),
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
                Some(p::TaskListStatus { read_level, ack_level, backlog_count_hint, task_id_block: Some(p::TaskIdBlock { start_id, end_id }), empty, rate_per_second: 100000.0, ..Default::default() })
            } else { None };
            let partition_config = self.frontend.state.executions.partition_config(&r.domain_id, &task_list_name, task_list_type).map(partition_config_to_proto);
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
    let limit = state.grpc_max_message_size;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("uber.cadence.matching.v1.MatchingAPI")
        .build_v1()?;
    tonic::transport::Server::builder()
        .add_service(reflection)
        .add_service(
            m::matching_api_server::MatchingApiServer::new(Matching::new(state))
                .max_decoding_message_size(limit),
        )
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "matching_rpc_tests.rs"]
mod durable_tests;
