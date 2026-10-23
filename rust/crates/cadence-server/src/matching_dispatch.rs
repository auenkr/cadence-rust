//! Cancellation-safe ownership of asynchronous Matching task delivery.
//! A detached delivery worker retains responsibility for a claimed task even
//! when the waiting RPC future disappears. This is not the service registry.
use crate::matching_history::{start_error_kind, MatchingHistoryClient, StartErrorKind};
use cadence_persistence::{
    matching_queue::{DurableTaskQueue, QueueError},
    matching_sqlite::AllocatedTask,
};
use cadence_proto::{api as p, history as h, matching as m};
use std::{
    future::Future,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

/// Observation of one getTask wait. EndPoll precedes History's start RPC, as
/// in Go taskListManager.getTask; failed deliveries begin another wait cycle.
#[derive(Clone)]
pub struct PollLifecycle {
    pub manager: Arc<std::sync::Mutex<cadence_common::poller::Manager>>,
    pub id: String,
    pub info: cadence_common::poller::Info,
    pub forwarded: bool,
    pub metric_context: Option<crate::matching_metrics::PollMetricContext>,
    pub metrics: crate::matching_metrics::MatchingMetrics,
}
pub(crate) struct PollWaitGuard(PollLifecycle);
impl PollLifecycle {
    pub(crate) fn begin(&self) -> PollWaitGuard {
        let count = {
            let mut manager = self.manager.lock().expect("poller history lock");
            let timestamp = now();
            manager.start(&self.id, self.info.clone(), timestamp);
            manager.count_at(timestamp)
        };
        if !self.info.identity.is_empty() {
            if let Some(context) = self.metric_context.as_ref() {
                self.metrics.poller_count(context, count);
            }
        }
        PollWaitGuard(self.clone())
    }
}
impl Drop for PollWaitGuard {
    fn drop(&mut self) {
        let count = {
            let mut manager = self.0.manager.lock().expect("poller history lock");
            let timestamp = now();
            manager.end(&self.0.id, timestamp);
            manager.count_at(timestamp)
        };
        if !self.0.info.identity.is_empty() {
            if let Some(context) = self.0.metric_context.as_ref() {
                self.0.metrics.poller_count(context, count);
            }
        }
    }
}

#[derive(Clone)]
pub struct TaskDispatcher {
    queue: Arc<DurableTaskQueue>,
    history: MatchingHistoryClient,
    domain_id: String,
    wake: Arc<Notify>,
    liveness: Option<Arc<cadence_common::liveness::Liveness>>,
    writer: Option<crate::matching_writer::TaskWriter>,
    reader: Option<crate::matching_reader::TaskReader>,
    sync_matcher: Option<Arc<crate::matching_sync::SyncMatcher>>,
    forward: Option<ForwardState>,
    pollers: Option<Arc<std::sync::Mutex<cadence_common::poller::Manager>>>,
    /// Go matcher cancel context: release blocked long polls without stopping
    /// this dispatcher, queue, or poller manager.
    blocked_pollers: Arc<Notify>,
}
/// Go `forwarder.ForwardPoll`: a child partition with no local task hands its
/// poll to the parent partition, which runs its full poll (History start
/// included) and returns the started response. Resolution is lazy so parent
/// reloads are picked up on the next forward; permits bound concurrent
/// forwarded polls like Go's `ForwarderMaxOutstandingPolls`.
#[derive(Clone)]
pub(crate) struct ForwardState {
    resolve: Arc<ParentResolver>,
    source: String,
    local_wait: crate::matching_forward::Count,
    permits: Arc<crate::matching_forward::RequestTokens>,
    add_permits: Arc<crate::matching_forward::RequestTokens>,
    add_rate: Arc<crate::matching_forward::ForwardRate>,
    task_metrics: Option<(crate::matching_metrics::MatchingMetrics, crate::matching_metrics::PollMetricContext)>,
}
pub(crate) enum Parent {
    Dispatcher(Box<TaskDispatcher>),
    Service(Box<crate::grpc_matching::Matching>),
    Remote(tonic::transport::Channel),
}
type ParentFuture = std::pin::Pin<Box<dyn Future<Output = Option<(Parent, String)>> + Send>>;
type ParentResolver = dyn Fn() -> ParentFuture + Send + Sync;
struct ForwardPoll {
    target: Parent,
    domain_id: String,
    parent_name: String,
    forwarded_from: String,
    poller_id: String,
    isolation_group: String,
    deadline: tokio::time::Instant,
}
type ForwardResult<R> = std::pin::Pin<Box<dyn Future<Output = Result<R, Status>> + Send>>;
trait ForwardPollRequest<R>: Clone + Send {
    fn for_parent(&self, name: &str) -> Self;
    fn forward(self, target: ForwardPoll) -> ForwardResult<R>;
}
impl ForwardPollRequest<m::PollForDecisionTaskResponse> for p::PollForDecisionTaskRequest {
    fn for_parent(&self, name: &str) -> Self {
        Self {
            task_list: Some(p::TaskList {
                name: name.into(),
                kind: self
                    .task_list
                    .as_ref()
                    .map(|list| list.kind)
                    .unwrap_or_default(),
                ..Default::default()
            }),
            identity: self.identity.clone(),
            ..Default::default()
        }
    }
    fn forward(self, target: ForwardPoll) -> ForwardResult<m::PollForDecisionTaskResponse> {
        Box::pin(async move {
            let payload = self.for_parent(&target.parent_name);
            let mut request = tonic::Request::new(m::PollForDecisionTaskRequest {
                domain_id: target.domain_id,
                poller_id: target.poller_id,
                request: Some(payload),
                forwarded_from: target.forwarded_from,
                isolation_group: target.isolation_group,
            });
            request.set_timeout(
                target
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now()),
            );
            match target.target {
                Parent::Service(service) => {
                    m::matching_api_server::MatchingApi::poll_for_decision_task(&*service, request)
                        .await
                }
                Parent::Remote(channel) => {
                    matching_yarpc_grpc_metadata(&mut request);
                    m::matching_api_client::MatchingApiClient::new(channel)
                        .poll_for_decision_task(request)
                        .await
                }
                Parent::Dispatcher(_) => {
                    unreachable!("library dispatcher handled by forwarding loop")
                }
            }
            .map(tonic::Response::into_inner)
        })
    }
}
impl ForwardPollRequest<m::PollForActivityTaskResponse> for p::PollForActivityTaskRequest {
    fn for_parent(&self, name: &str) -> Self {
        Self {
            task_list: Some(p::TaskList {
                name: name.into(),
                kind: self
                    .task_list
                    .as_ref()
                    .map(|list| list.kind)
                    .unwrap_or_default(),
                ..Default::default()
            }),
            identity: self.identity.clone(),
            ..Default::default()
        }
    }
    fn forward(self, target: ForwardPoll) -> ForwardResult<m::PollForActivityTaskResponse> {
        Box::pin(async move {
            let payload = self.for_parent(&target.parent_name);
            let mut request = tonic::Request::new(m::PollForActivityTaskRequest {
                domain_id: target.domain_id,
                poller_id: target.poller_id,
                request: Some(payload),
                forwarded_from: target.forwarded_from,
                isolation_group: target.isolation_group,
            });
            request.set_timeout(
                target
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now()),
            );
            match target.target {
                Parent::Service(service) => {
                    m::matching_api_server::MatchingApi::poll_for_activity_task(&*service, request)
                        .await
                }
                Parent::Remote(channel) => {
                    matching_yarpc_grpc_metadata(&mut request);
                    m::matching_api_client::MatchingApiClient::new(channel)
                        .poll_for_activity_task(request)
                        .await
                }
                Parent::Dispatcher(_) => {
                    unreachable!("library dispatcher handled by forwarding loop")
                }
            }
            .map(tonic::Response::into_inner)
        })
    }
}

/// Go Matching's generated YARPC gRPC client sends these routing headers on
/// every outbound Matching call. Preserve them when Rust forwards a worker poll
/// to a remote parent, alongside the remaining caller deadline above.
pub(crate) fn matching_yarpc_grpc_metadata<T>(request: &mut tonic::Request<T>) {
    let metadata = request.metadata_mut();
    metadata.insert(
        "rpc-service",
        tonic::metadata::MetadataValue::from_static("cadence-matching"),
    );
    metadata.insert(
        "rpc-caller",
        tonic::metadata::MetadataValue::from_static("cadence-matching"),
    );
    metadata.insert(
        "rpc-encoding",
        tonic::metadata::MetadataValue::from_static("proto"),
    );
}
#[cfg(test)]
impl<R: Send + 'static> ForwardPollRequest<R> for () {
    fn for_parent(&self, _name: &str) -> Self {}
    fn forward(self, _target: ForwardPoll) -> ForwardResult<R> {
        Box::pin(async { Err(Status::unimplemented("test poll")) })
    }
}
// Go's ForwardTask timer is deferred: remote busy responses and context
// cancellation are timed too, while local limiter rejection is not.
struct ForwardTaskTimer<'a> {
    metrics: Option<&'a (crate::matching_metrics::MatchingMetrics, crate::matching_metrics::PollMetricContext)>,
    started: std::time::Instant,
}
impl Drop for ForwardTaskTimer<'_> {
    fn drop(&mut self) {
        if let Some((metrics, context)) = self.metrics {
            metrics.forward_task_latency(context, self.started.elapsed());
        }
    }
}
impl ForwardState {
    pub(crate) fn new(
        resolve: impl Fn() -> ParentFuture + Send + Sync + 'static,
        max_outstanding: crate::matching_forward::Count,
        max_outstanding_adds: Arc<crate::matching_forward::RequestTokens>,
        add_rate: Arc<crate::matching_forward::ForwardRate>,
        source: String,
        local_wait: crate::matching_forward::Count,
    ) -> Self {
        Self {
            resolve: Arc::new(resolve),
            source,
            local_wait,
            permits: Arc::new(crate::matching_forward::RequestTokens::new(max_outstanding)),
            add_permits: max_outstanding_adds,
            add_rate,
            task_metrics: None,
        }
    }

    pub(crate) fn with_task_metrics(mut self, metrics: crate::matching_metrics::MatchingMetrics, context: crate::matching_metrics::PollMetricContext) -> Self {
        self.task_metrics = Some((metrics, context));
        self
    }

    /// Go `fwdrAddReqTokenC`: outstanding forwarded-add tokens.
    pub(crate) fn add_tokens(&self) -> Arc<crate::matching_forward::RequestTokens> {
        self.add_permits.clone()
    }

    pub(crate) async fn forward_backlog_task(
        &self,
        task: AllocatedTask,
        domain_id: &str,
        task_type: i32,
        kind: i16,
    ) -> Result<(), crate::matching_forward::ForwardError> {
        if kind == 1 {
            return Err(crate::matching_forward::ForwardError::TaskListKind);
        }
        // Go `ForwardTask`: the parent name is resolved before the rate limit
        // is charged; the outstanding-add token is taken by the caller.
        let name = cadence_common::task_list::TaskListName::parse(&self.source)
            .map_err(|error| crate::matching_forward::ForwardError::Remote(Status::invalid_argument(error)))?;
        if name.is_root() {
            return Err(crate::matching_forward::ForwardError::NoParent);
        }
        if !self.add_rate.allow(now()) {
            return Err(crate::matching_forward::ForwardError::SlowDown);
        }
        let _timer = ForwardTaskTimer { metrics: self.task_metrics.as_ref(), started: std::time::Instant::now() };
        if !matches!(task_type, 0 | 1) {
            return Err(crate::matching_forward::ForwardError::InvalidTaskListType);
        }
        let (parent, parent_name) = (self.resolve)().await.ok_or_else(||
            crate::matching_forward::ForwardError::Remote(Status::unavailable("cannot resolve parent task list for forwarding")))?;
        let run_id = task.data.run_id.as_deref()
            .and_then(|id| uuid::Uuid::from_slice(id).ok())
            .map(|id| id.to_string()).unwrap_or_default();
        let execution = Some(p::WorkflowExecution { workflow_id: task.data.workflow_id.clone(), run_id });
        let partition_config = task.data.partition_config.clone().unwrap_or_default().into_iter().collect();
        // Go persistence GetTasks leaves ScheduleToStartTimeoutSeconds at
        // zero (SQL and NoSQL). Expiry is for reader filtering, not a new
        // timeout to reconstruct when forwarding an already-persisted task.
        let timeout_seconds = 0i32;
        let forwarded_from = self.source.clone();
        let result = match parent {
            Parent::Remote(channel) => {
                let mut client = m::matching_api_client::MatchingApiClient::new(channel);
                if task_type == 0 {
                    let mut request = tonic::Request::new(m::AddDecisionTaskRequest {
                        domain_id: domain_id.to_owned(), workflow_execution: execution,
                        task_list: Some(p::TaskList { name: parent_name, kind: i32::from(kind) + 1, ..Default::default() }),
                        schedule_id: task.data.schedule_id, schedule_to_start_timeout: Some(prost_types::Duration { seconds: i64::from(timeout_seconds), nanos: 0 }),
                        source: 2, forwarded_from, partition_config,
                    });
                    request.set_timeout(Duration::from_secs(2));
                    matching_yarpc_grpc_metadata(&mut request);
                    client.add_decision_task(request).await.map(|_| ())
                } else {
                    let mut request = tonic::Request::new(m::AddActivityTaskRequest {
                        domain_id: domain_id.to_owned(), source_domain_id: domain_id.to_owned(),
                        workflow_execution: execution,
                        task_list: Some(p::TaskList { name: parent_name, kind: i32::from(kind) + 1, ..Default::default() }),
                        schedule_id: task.data.schedule_id, schedule_to_start_timeout: Some(prost_types::Duration { seconds: i64::from(timeout_seconds), nanos: 0 }),
                        source: 2, forwarded_from, partition_config, activity_task_dispatch_info: None,
                    });
                    request.set_timeout(Duration::from_secs(2));
                    matching_yarpc_grpc_metadata(&mut request);
                    client.add_activity_task(request).await.map(|_| ())
                }
            }
            Parent::Service(service) => {
                use m::matching_api_server::MatchingApi;
                if task_type == 0 {
                    let mut request = tonic::Request::new(m::AddDecisionTaskRequest {
                        domain_id: domain_id.to_owned(), workflow_execution: execution,
                        task_list: Some(p::TaskList { name: parent_name, kind: i32::from(kind) + 1, ..Default::default() }),
                        schedule_id: task.data.schedule_id, schedule_to_start_timeout: Some(prost_types::Duration { seconds: i64::from(timeout_seconds), nanos: 0 }),
                        source: 2, forwarded_from, partition_config,
                    });
                    request.set_timeout(Duration::from_secs(2));
                    service.add_decision_task(request).await.map(|_| ())
                } else {
                    let mut request = tonic::Request::new(m::AddActivityTaskRequest {
                        domain_id: domain_id.to_owned(), source_domain_id: domain_id.to_owned(),
                        workflow_execution: execution,
                        task_list: Some(p::TaskList { name: parent_name, kind: i32::from(kind) + 1, ..Default::default() }),
                        schedule_id: task.data.schedule_id, schedule_to_start_timeout: Some(prost_types::Duration { seconds: i64::from(timeout_seconds), nanos: 0 }),
                        source: 2, forwarded_from, partition_config, activity_task_dispatch_info: None,
                    });
                    request.set_timeout(Duration::from_secs(2));
                    service.add_activity_task(request).await.map(|_| ())
                }
            }
            Parent::Dispatcher(dispatcher) => {
                dispatcher.accept_forwarded_backlog(task, domain_id, &parent_name, task_type).await
            }
        };
        result.map_err(crate::matching_forward::ForwardError::classify)
    }
}
pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
struct DeliveryTask {
    task: AllocatedTask,
    sync: Option<crate::matching_sync::SyncTask>,
    /// Forwarding is request scoped in Go and absent from persisted TaskData.
    forwarded: bool,
}
enum ClaimError {
    Queue(QueueError),
    Join(tokio::task::JoinError),
    Forward(tokio::sync::OwnedSemaphorePermit),
    Released,
}
enum WaitResult {
    Closed,
    Released,
    Woken,
}
/// Go maps SQL throttling to ServiceBusyError (`convertCommonErrors`); every
/// other persistence or shutdown error reaches handleErr's default: Internal.
fn queue_status(error: QueueError) -> Status {
    if matches!(
        error,
        QueueError::Storage(cadence_persistence::matching_sqlite::StorageError::Backend {
            kind: cadence_persistence::matching_task_store::BackendErrorKind::Throttled,
            ..
        })
    ) {
        return crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            error.to_string(),
            "ServiceBusyError",
            cadence_proto::api::ServiceBusyError { reason: String::new() },
        );
    }
    Status::internal(error.to_string())
}
impl TaskDispatcher {
    pub(crate) async fn accept_forwarded_backlog(
        &self,
        task: AllocatedTask,
        source_domain: &str,
        parent_name: &str,
        task_type: i32,
    ) -> Result<(), Status> {
        let source_domain = uuid::Uuid::parse_str(source_domain)
            .map_err(|_| Status::invalid_argument("DomainUUID is not valid."))?;
        let matcher = self.sync_matcher.as_ref().ok_or_else(|| Status::unavailable("remote sync match failed"))?;
        let root = cadence_common::task_list::TaskListName::parse(parent_name)
            .map_err(|_| Status::invalid_argument("TaskList is not valid."))?
            .is_root();
        let local_wait = if root {
            Duration::from_secs(2)
        } else {
            self.forward.as_ref().map(|forward| Duration::from_nanos((forward.local_wait)().max(0) as u64))
                .unwrap_or(Duration::from_millis(10))
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let group = task.data.partition_config.as_ref()
            .and_then(|config| config.get("isolation-group"))
            .map(String::as_str).unwrap_or_default().to_owned();
        if matcher.offer(task.data.clone(), &group, &source_domain.to_string(), true, local_wait, deadline).await {
            return Ok(());
        }
        // Library users have no local Matching RPC factory. Preserve the
        // forwarded Offer chain here too; root backlog waits above until the
        // caller's two-second context expires, and failed offers never persist.
        if !root {
            if let Some(forward) = &self.forward {
                if let Some(_permit) = forward.add_permits.try_acquire() {
                    if Box::pin(forward.forward_backlog_task(task, &self.domain_id, task_type, 0)).await.is_ok() {
                        return Ok(());
                    }
                }
            }
        }
        Err(Status::unavailable("remote sync match failed"))
    }

    pub(crate) fn reader_should_reload(&self) -> bool {
        self.reader
            .as_ref()
            .is_some_and(|reader| reader.should_reload())
    }
    pub fn new(
        queue: Arc<DurableTaskQueue>,
        history: MatchingHistoryClient,
        domain_id: String,
    ) -> Self {
        Self {
            queue,
            history,
            domain_id,
            wake: Arc::new(Notify::new()),
            liveness: None,
            writer: None,
            reader: None,
            sync_matcher: None,
            forward: None,
            pollers: None,
            blocked_pollers: Arc::new(Notify::new()),
        }
    }
    pub fn with_liveness(mut self, liveness: Arc<cadence_common::liveness::Liveness>) -> Self {
        self.liveness = Some(liveness);
        self
    }
    pub fn with_writer(mut self, writer: crate::matching_writer::TaskWriter) -> Self {
        self.writer = Some(writer);
        self
    }
    pub fn with_reader(mut self, reader: crate::matching_reader::TaskReader) -> Self {
        self.reader = Some(reader);
        self
    }
    pub fn with_sync_matcher(mut self, matcher: Arc<crate::matching_sync::SyncMatcher>) -> Self {
        self.sync_matcher = Some(matcher);
        self
    }
    pub(crate) fn with_forward(mut self, forward: ForwardState) -> Self {
        self.forward = Some(forward);
        self
    }
    pub(crate) fn with_pollers(
        mut self,
        pollers: Arc<std::sync::Mutex<cadence_common::poller::Manager>>,
    ) -> Self {
        self.pollers = Some(pollers);
        self
    }
    pub fn task_notification(&self) -> Arc<Notify> {
        self.wake.clone()
    }
    pub fn notify_new_tasks(&self) {
        self.wake.notify_waiters();
    }
    pub fn release_blocked_pollers(&self) {
        self.blocked_pollers.notify_waiters();
    }
    async fn claim(&self, group: &str) -> Result<(Option<AllocatedTask>, bool), ClaimError> {
        if let Some(reader) = &self.reader {
            return reader
                .poll_group(group)
                .await
                .map(|task| (Some(task), false))
                .ok_or(ClaimError::Queue(QueueError::Stopped));
        }
        let queue = self.queue.clone();
        crate::matching_blocking::run(move || {
            queue.read_batch_with_clock(100000, NonZeroU32::new(1).unwrap(), now)
        })
        .await
        .map_err(ClaimError::Join)?
        .map(|mut batch| (batch.tasks.pop(), batch.scan_pending))
        .map_err(ClaimError::Queue)
    }
    async fn claim_delivery(
        &self,
        group: &str,
    ) -> Result<(Option<DeliveryTask>, bool), ClaimError> {
        if let Some(matcher) = self.sync_matcher.as_ref().filter(|_| self.reader.is_some()) {
            tokio::select! {
                task = matcher.poll(group) => task.map(|sync| { let forwarded = sync.forwarded; (Some(DeliveryTask { task: sync.task.clone(), sync: Some(sync), forwarded }), false) }).ok_or(ClaimError::Queue(QueueError::Stopped)),
                result = self.claim(group) => result.map(|(task, pending)| (task.map(|task| DeliveryTask { task, sync: None, forwarded: false }), pending)),
            }
        } else {
            self.claim(group).await.map(|(task, pending)| {
                (
                    task.map(|task| DeliveryTask {
                        task,
                        sync: None,
                        forwarded: false,
                    }),
                    pending,
                )
            })
        }
    }
    async fn finish_delivery(&self, task: DeliveryTask, result: Result<(), Status>) {
        if let Some(sync) = task.sync {
            sync.finish(result);
        } else if result.is_ok() {
            self.complete(task.task.task_id).await;
        } else {
            self.rewrite(task.task.task_id).await;
        }
    }
    async fn rewrite(&self, task: i64) {
        if let Some(writer) = &self.writer {
            let queue = self.queue.clone();
            let data = crate::matching_blocking::run(move || queue.retry_data(task)).await;
            let result = match data {
                Ok(Ok(data)) => writer
                    .append_rewrite(crate::matching_writer::WriteTask {
                        data,
                        source_domain: uuid::Uuid::parse_str(&self.domain_id)
                            .expect("validated Matching domain"),
                        schedule_to_start_seconds: 0,
                    })
                    .await
                    .map(|_| ()),
                Ok(Err(error)) => Err(queue_status(error)),
                Err(error) => Err(Status::internal(error.to_string())),
            };
            if let Err(error) = result {
                eprintln!("Matching task rewrite failed: {error}");
                self.queue.stop();
            } else {
                self.complete(task).await;
            }
            self.wake.notify_waiters();
            return;
        }
        let queue = self.queue.clone();
        let result = crate::matching_blocking::run(move || {
            if let Err(error) = queue.rewrite_failed(task, now()) {
                queue.stop();
                return Err(error);
            }
            queue.run_garbage_collection(false)?;
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => eprintln!("Matching task rewrite failed: {error}"),
            Err(error) => {
                eprintln!("Matching task rewrite worker failed: {error}");
                self.queue.stop();
            }
        }
        self.wake.notify_waiters();
    }
    async fn complete(&self, task: i64) {
        let queue = self.queue.clone();
        let result = crate::matching_blocking::run(move || {
            queue.complete(task)?;
            queue.run_garbage_collection(false)?;
            Ok::<_, QueueError>(())
        })
        .await;
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => eprintln!("Matching task completion failed: {error}"),
            Err(error) => {
                eprintln!("Matching task completion worker failed: {error}");
                self.queue.stop();
            }
        }
    }
    /// Go `pollOrForward`'s forward leg: with no local task, run the parent
    /// partition's delivery loop through this same poll. The parent claims
    /// from its own queue and this poll reports the parent's started task;
    /// the response channel is shared, so a sent parent response ends this
    /// poll. Returns true when polling is done (response sent or poll dead).
    /// A missing parent, an exhausted permit budget, or no remaining time
    /// returns false so the caller falls back to the local wait, mirroring
    /// Go recursing into the local poll when forwarding is unavailable.
    /// Boxed (not `async`) to break the `delivery_loop` recursion cycle:
    /// child loops delegate to parent loops through this future.
    fn forward_poll<'a, Q, R, F, Fut>(
        &'a self,
        poll: Q,
        sender: &'a mut Option<oneshot::Sender<Result<R, Status>>>,
        deliver: &'a mut F,
        budget: (tokio::time::Instant, tokio::sync::OwnedSemaphorePermit),
        lifecycle: Option<PollLifecycle>,
        poll_started: tokio::time::Instant,
    ) -> std::pin::Pin<Box<dyn Future<Output = bool> + Send + 'a>>
    where
        Q: ForwardPollRequest<R> + 'a,
        R: Default + Send + 'a,
        F: FnMut(AllocatedTask, Q, Option<String>) -> Fut + Send + 'a,
        Fut: Future<Output = Result<R, Status>> + Send + 'a,
    {
        Box::pin(async move {
            let (deadline, _permit) = budget;
            let Some(forward) = self.forward.as_ref() else {
                return false;
            };
            if sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                return true;
            }
            if deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .is_zero()
            {
                return false;
            }
            let Some((parent, parent_name)) = (forward.resolve)().await else {
                return false;
            };
            let parent = match parent {
                Parent::Dispatcher(parent) => parent,
                target => {
                    let (poller_id, isolation_group) = lifecycle
                        .as_ref()
                        .map(|poll| (poll.id.clone(), poll.info.isolation_group.clone()))
                        .unwrap_or_default();
                    let target = ForwardPoll {
                        target,
                        domain_id: self.domain_id.clone(),
                        parent_name,
                        forwarded_from: forward.source.clone(),
                        poller_id,
                        isolation_group,
                        deadline,
                    };
                    // A cancelled child RPC must drop the upstream RPC as well.
                    // The dispatcher owns the response channel, so watch it
                    // directly rather than waiting out a full parent long poll.
                    let Some(response) = sender.as_mut() else {
                        return true;
                    };
                    let forward_started = tokio::time::Instant::now();
                    let result = tokio::select! {
                        result = poll.forward(target) => Some(result),
                        _ = response.closed() => None,
                    };
                    if let (Some(lifecycle), Some(context)) = (
                        lifecycle.as_ref(),
                        lifecycle
                            .as_ref()
                            .and_then(|poll| poll.metric_context.as_ref()),
                    ) {
                        lifecycle
                            .metrics
                            .forward_poll_latency(context, forward_started.elapsed());
                    }
                    let Some(result) = result else {
                        return true;
                    };
                    return match result {
                        Ok(value) => {
                            if let (Some(lifecycle), Some(context)) = (
                                lifecycle.as_ref(),
                                lifecycle
                                    .as_ref()
                                    .and_then(|poll| poll.metric_context.as_ref()),
                            ) {
                                lifecycle.metrics.poll_match_latency(
                                    context,
                                    poll_started.elapsed(),
                                    true,
                                    false,
                                );
                            }
                            if let Some(sender) = sender.take() {
                                let _ = sender.send(Ok(value));
                            }
                            true
                        }
                        Err(_) => false,
                    };
                }
            };
            // The parent loop governs its own lifecycle chain; this poll keeps
            // its own waiter registration while forwarded, as in Go where the
            // child poller stays registered during `ForwardPoll`.
            let forward_context = lifecycle
                .as_ref()
                .and_then(|poll| poll.metric_context.clone());
            let forward_metrics = lifecycle.as_ref().map(|poll| poll.metrics.clone());
            let forward_started = tokio::time::Instant::now();
            let lifecycle = lifecycle.and_then(|mut lifecycle| {
                lifecycle.manager = parent.pollers.clone()?;
                // ForwardPoll rebuilds the request without the worker's rate.
                lifecycle.info.rate_per_second = crate::matching_limiter::TASK_DISPATCH_RPS;
                lifecycle.forwarded = true;
                if let Some(context) = lifecycle.metric_context.as_mut() {
                    context.task_list = parent_name.clone();
                }
                Some(lifecycle)
            });
            parent
                .delivery_loop(
                    poll.for_parent(&parent_name),
                    sender,
                    deliver,
                    lifecycle,
                    deadline,
                    poll_started,
                )
                .await;
            if let (Some(metrics), Some(context)) = (forward_metrics, forward_context.as_ref()) {
                metrics.forward_poll_latency(context, forward_started.elapsed());
            }
            true
        })
    }
    /// Caller/controller supplies the configured long-poll expiration. Dropping
    /// this future closes the receiver; the worker rewrites any claimed task.
    /// Go matching query polling: History `GetMutableState` for a query task,
    /// so the frontend can load history for the non-sticky query.
    pub async fn query_mutable_state(
        &self,
        execution: &p::WorkflowExecution,
    ) -> Result<h::GetMutableStateResponse, Status> {
        self.query_mutable_state_with_deadline(execution, None)
            .await
    }
    pub async fn query_mutable_state_with_deadline(
        &self,
        execution: &p::WorkflowExecution,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<h::GetMutableStateResponse, Status> {
        self.history
            .mutable_state_with_deadline(&self.domain_id, execution, caller_deadline)
            .await
    }

    pub async fn poll_decision(
        &self,
        poll: p::PollForDecisionTaskRequest,
        expiration: Duration,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        let caller_deadline = tokio::time::Instant::now() + expiration;
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll, source_domain: Option<String>| {
                let history = history.clone();
                let domain = source_domain.unwrap_or_else(|| domain.clone());
                async move {
                    history
                        .start_decision_with_deadline(&domain, &task, poll, Some(caller_deadline))
                        .await
                }
            },
            None,
        )
        .await
    }

    pub async fn poll_decision_with_poller(
        &self,
        poll: p::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: PollLifecycle,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        let caller_deadline = tokio::time::Instant::now() + expiration;
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll, source_domain: Option<String>| {
                let history = history.clone();
                let domain = source_domain.unwrap_or_else(|| domain.clone());
                async move {
                    history
                        .start_decision_with_deadline(&domain, &task, poll, Some(caller_deadline))
                        .await
                }
            },
            Some(lifecycle),
        )
        .await
    }
    pub async fn poll_activity(
        &self,
        poll: p::PollForActivityTaskRequest,
        expiration: Duration,
    ) -> Result<m::PollForActivityTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        let caller_deadline = tokio::time::Instant::now() + expiration;
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll, source_domain: Option<String>| {
                let history = history.clone();
                let domain = source_domain.unwrap_or_else(|| domain.clone());
                async move {
                    history
                        .start_activity_with_deadline(&domain, &task, poll, Some(caller_deadline))
                        .await
                }
            },
            None,
        )
        .await
    }

    pub async fn poll_activity_with_poller(
        &self,
        poll: p::PollForActivityTaskRequest,
        expiration: Duration,
        lifecycle: PollLifecycle,
    ) -> Result<m::PollForActivityTaskResponse, Status> {
        let history = self.history.clone();
        let domain = self.domain_id.clone();
        let caller_deadline = tokio::time::Instant::now() + expiration;
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll, source_domain: Option<String>| {
                let history = history.clone();
                let domain = source_domain.unwrap_or_else(|| domain.clone());
                async move {
                    history
                        .start_activity_with_deadline(&domain, &task, poll, Some(caller_deadline))
                        .await
                }
            },
            Some(lifecycle),
        )
        .await
    }
    #[cfg(test)]
    async fn poll<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        mut deliver: F,
    ) -> Result<R, Status>
    where
        Q: ForwardPollRequest<R> + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTask, Q) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        self.poll_with_lifecycle(
            poll,
            expiration,
            move |task, poll, _| deliver(task, poll),
            None,
        )
        .await
    }
    async fn poll_with_lifecycle<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        deliver: F,
        lifecycle: Option<PollLifecycle>,
    ) -> Result<R, Status>
    where
        Q: ForwardPollRequest<R> + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTask, Q, Option<String>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let (sender, receiver) = oneshot::channel();
        let timeout_metrics = lifecycle
            .as_ref()
            .and_then(|l| Some((l.metrics.clone(), l.metric_context.clone()?)));
        let worker = self.clone();
        let deadline = tokio::time::Instant::now() + expiration;
        let poll_started = tokio::time::Instant::now();
        tokio::spawn(async move {
            worker
                .delivery_worker(poll, sender, deliver, lifecycle, deadline, poll_started)
                .await;
        });
        match tokio::time::timeout(expiration, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Status::unavailable("Matching delivery worker stopped.")),
            Err(_) => {
                if let Some((metrics, context)) = timeout_metrics {
                    metrics.poll_timeout(&context);
                }
                Ok(R::default())
            }
        }
    }
    async fn delivery_worker<Q, R, F, Fut>(
        &self,
        poll: Q,
        sender: oneshot::Sender<Result<R, Status>>,
        mut deliver: F,
        lifecycle: Option<PollLifecycle>,
        deadline: tokio::time::Instant,
        poll_started: tokio::time::Instant,
    ) where
        Q: ForwardPollRequest<R>,
        R: Default + Send,
        F: FnMut(AllocatedTask, Q, Option<String>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        if let (Some(reader), Some(lifecycle)) = (self.reader.as_ref(), lifecycle.as_ref()) {
            if !lifecycle.info.isolation_group.is_empty()
                && !reader.has_isolation_group(&lifecycle.info.isolation_group)
            {
                if let Some(context) = lifecycle.metric_context.as_ref() {
                    lifecycle.metrics.invalid_isolation_group(context);
                }
            }
        }
        let mut sender = Some(sender);
        self.delivery_loop(
            poll,
            &mut sender,
            &mut deliver,
            lifecycle,
            deadline,
            poll_started,
        )
        .await;
    }
    /// The loop borrows the response channel and the History starter so a
    /// forwarded parent poll can deliver through this same poll: the parent
    /// claims from its own queue and this poll reports the parent's started
    /// task, exactly like Go's `ForwardPoll` returning a started task.
    async fn delivery_loop<Q, R, F, Fut>(
        &self,
        poll: Q,
        sender: &mut Option<oneshot::Sender<Result<R, Status>>>,
        deliver: &mut F,
        lifecycle: Option<PollLifecycle>,
        deadline: tokio::time::Instant,
        poll_started: tokio::time::Instant,
    ) where
        Q: ForwardPollRequest<R>,
        R: Default + Send,
        F: FnMut(AllocatedTask, Q, Option<String>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let mut waiting = None;
        let mut waiting_started = false;
        let mut forward_failed = false;
        loop {
            if sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                return;
            }
            if !waiting_started {
                if let Some(liveness) = &self.liveness {
                    liveness.mark_alive(now());
                }
                waiting = lifecycle.as_ref().map(PollLifecycle::begin);
                waiting_started = true;
            }
            // Register before reading storage: an append between an empty read
            // and waiting must not lose its notification (important for short polls).
            let notification = self.wake.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let released = self.blocked_pollers.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let claim = {
                let closed = async {
                    match sender.as_mut() {
                        Some(sender) => sender.closed().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(closed);
                if self.reader.is_some() {
                    // Waiting on the rendezvous owns no task. Cancellation can
                    // withdraw this poll without stealing a later backlog row.
                    tokio::select! {
                        biased;
                        _ = &mut closed => return,
                        _ = &mut released => Err(ClaimError::Released),
                        result = self.claim_delivery(lifecycle.as_ref().map(|l| l.info.isolation_group.as_str()).unwrap_or_default()) => result,
                        permit = async {
                            match &self.forward {
                                Some(forward) if !forward_failed => {
                                    // Go pollNonBlocking gives local tasks a
                                    // configured head start before ForwardPoll.
                                    let wait = Duration::from_nanos((forward.local_wait)().max(0) as u64);
                                    tokio::time::sleep(wait).await;
                                    forward.permits.acquire().await
                                },
                                _ => std::future::pending().await,
                            }
                        } => match permit {
                            Some(permit) => Err(ClaimError::Forward(permit)),
                            None => Err(ClaimError::Queue(QueueError::Stopped)),
                        },
                    }
                } else {
                    self.claim_delivery("").await
                }
            };
            let (task, scan_pending) = match claim {
                Ok(value) => value,
                Err(ClaimError::Forward(permit)) => {
                    if self
                        .forward_poll(
                            poll.clone(),
                            sender,
                            deliver,
                            (deadline, permit),
                            lifecycle.clone(),
                            poll_started,
                        )
                        .await
                    {
                        return;
                    }
                    forward_failed = true;
                    continue;
                }
                Err(ClaimError::Released) => {
                    drop(waiting.take());
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(Ok(R::default()));
                    }
                    return;
                }
                Err(ClaimError::Queue(QueueError::Stopped)) => {
                    drop(waiting.take());
                    // Go maps a closed task-list pump to an empty successful
                    // poll, allowing workers to poll its replacement normally.
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(Ok(R::default()));
                    }
                    return;
                }
                Err(error) => {
                    drop(waiting.take());
                    let status = match error {
                        ClaimError::Queue(error) => queue_status(error),
                        ClaimError::Join(error) => Status::internal(error.to_string()),
                        ClaimError::Forward(_) => unreachable!("handled above"),
                        ClaimError::Released => unreachable!("handled above"),
                    };
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(Err(status));
                    }
                    return;
                }
            };
            let Some(task) = task else {
                if scan_pending {
                    tokio::task::yield_now().await;
                    continue;
                }
                if sender.is_none() || sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                    return;
                }
                let waited = {
                    let closed = async {
                        match sender.as_mut() {
                            Some(sender) => sender.closed().await,
                            None => std::future::pending().await,
                        }
                    };
                    tokio::pin!(closed);
                    tokio::select! {
                        _=&mut closed=>WaitResult::Closed,
                        _=&mut released=>WaitResult::Released,
                        _=notification=>WaitResult::Woken,
                        _=tokio::time::sleep(Duration::from_millis(100))=>WaitResult::Woken
                    }
                };
                match waited {
                    WaitResult::Closed => return,
                    WaitResult::Released => {
                        drop(waiting.take());
                        if let Some(sender) = sender.take() {
                            let _ = sender.send(Ok(R::default()));
                        }
                        return;
                    }
                    WaitResult::Woken => {}
                }
                continue;
            };
            if let Some(lifecycle) = lifecycle.as_ref() {
                if let Some(context) = lifecycle.metric_context.as_ref() {
                    lifecycle.metrics.poll_success(context, task.sync.is_some());
                    lifecycle.metrics.poll_match_latency(
                        context,
                        poll_started.elapsed(),
                        lifecycle.forwarded,
                        forward_failed,
                    );
                    lifecycle.metrics.poll_match(
                        context,
                        task.forwarded,
                        lifecycle.forwarded,
                        task.task.data.partition_config.as_ref(),
                    );
                }
            }
            drop(waiting.take());
            waiting_started = false;
            if sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                self.finish_delivery(task, Err(Status::cancelled("Poll cancelled before start.")))
                    .await;
                return;
            }
            let result = {
                let closed = async {
                    match sender.as_mut() {
                        Some(sender) => sender.closed().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(closed);
                tokio::select! {
                    biased;
                    _=&mut closed=>{self.finish_delivery(task, Err(Status::cancelled("Poll cancelled during start."))).await;return;},
                    result=deliver(task.task.clone(),poll.clone(),Some(task.sync.as_ref().map(|sync| sync.source_domain.clone()).unwrap_or_else(|| self.domain_id.clone())))=>result,
                }
            };
            match result {
                Ok(response) => {
                    self.finish_delivery(task, Ok(())).await;
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(Ok(response));
                    }
                    return;
                }
                Err(error) => match start_error_kind(&error) {
                    StartErrorKind::Discard => {
                        if let (Some(lifecycle), Some(context)) = (
                            lifecycle.as_ref(),
                            lifecycle.as_ref().and_then(|l| l.metric_context.as_ref()),
                        ) {
                            lifecycle.metrics.poll_already_started(context);
                        }
                        self.finish_delivery(task, Ok(())).await
                    }
                    StartErrorKind::InactiveDomain => {
                        if self
                            .history
                            .refresh_workflow_tasks_with_deadline(
                                task.sync
                                    .as_ref()
                                    .map(|sync| sync.source_domain.as_str())
                                    .unwrap_or(&self.domain_id),
                                &task.task,
                                Some(deadline),
                            )
                            .await
                            .is_ok()
                        {
                            self.finish_delivery(task, Ok(())).await;
                        } else {
                            self.finish_delivery(task, Err(error)).await;
                        }
                    }
                    StartErrorKind::Retry => {
                        if !matches!(error.code(), tonic::Code::Cancelled | tonic::Code::DeadlineExceeded) {
                            if let Some((metrics, context)) = lifecycle
                                .as_ref()
                                .and_then(|lifecycle| {
                                    lifecycle
                                        .metric_context
                                        .as_ref()
                                        .map(|context| (lifecycle.metrics.clone(), context.clone()))
                                })
                            {
                                metrics.task_list_counter_event(
                                    crate::matching_metrics::BUFFER_UNKNOWN_DISPATCH_ERROR,
                                    &context,
                                );
                            }
                        }
                        self.finish_delivery(task, Err(error)).await;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData},
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };

    #[test]
    fn queue_status_maps_throttling_to_service_busy_and_other_errors_to_internal() {
        use cadence_persistence::{
            matching_sqlite::StorageError,
            matching_task_store::{BackendErrorKind, MatchingTaskStoreError},
        };
        // Go convertCommonErrors: throttling becomes ServiceBusyError (ResourceExhausted).
        let throttled = QueueError::Storage(StorageError::Backend {
            kind: BackendErrorKind::Throttled,
            message: "too many connections".into(),
        });
        assert_eq!(queue_status(throttled).code(), tonic::Code::ResourceExhausted);
        // Timeouts, persistence failures, condition failures and shutdown reach
        // handleErr's default branch: InternalServiceError.
        let timeout = QueueError::Storage(StorageError::Backend {
            kind: BackendErrorKind::Timeout,
            message: "timed out".into(),
        });
        assert_eq!(queue_status(timeout).code(), tonic::Code::Internal);
        let condition = QueueError::Storage(MatchingTaskStoreError::ConditionFailed("fenced".into()));
        assert_eq!(queue_status(condition).code(), tonic::Code::Internal);
        assert_eq!(queue_status(QueueError::Stopped).code(), tonic::Code::Internal);
    }
    use prost::Message;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use uuid::Uuid;
    #[test]
    fn matching_forwarded_poll_rebuilds_parent_request() {
        let activity = p::PollForActivityTaskRequest {
            domain: "worker-domain".into(),
            identity: "worker".into(),
            task_list: Some(p::TaskList {
                name: "/__cadence_sys/queue/1".into(),
                kind: 1,
                ..Default::default()
            }),
            task_list_metadata: Some(p::TaskListMetadata {
                max_tasks_per_second: Some(3.0),
            }),
        }
        .for_parent("queue");
        assert_eq!(activity.task_list.unwrap().name, "queue");
        assert_eq!(activity.identity, "worker");
        assert!(activity.domain.is_empty());
        assert!(activity.task_list_metadata.is_none());
    }
    #[tokio::test]
    async fn matching_remote_forward_reaches_go_yarpc_peer() {
        let Ok(address) = std::env::var("CADENCE_MATCHING_GO_PEER") else {
            return;
        };
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let target = || ForwardPoll {
            target: Parent::Remote(channel.clone()),
            domain_id: "00000000-0000-0000-0000-000000000001".into(),
            parent_name: "parent".into(),
            forwarded_from: "child".into(),
            poller_id: "oracle-poller".into(),
            isolation_group: "oracle-group".into(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(8),
        };
        let decision = p::PollForDecisionTaskRequest {
            identity: "oracle-worker".into(),
            ..Default::default()
        }
        .forward(target())
        .await
        .unwrap();
        let decision_observation = std::str::from_utf8(&decision.task_token).unwrap();
        assert!(decision_observation.starts_with("go-yarpc-decision|"));
        assert!(
            decision_observation.contains("|cadence-matching|cadence-matching|true|proto"),
            "unexpected Go YARPC decision observation: {decision_observation}"
        );
        assert_eq!(decision.started_event_id, 17);
        assert_eq!(
            decision.workflow_execution.unwrap().workflow_id,
            "go-peer-workflow"
        );
        let activity = p::PollForActivityTaskRequest {
            identity: "oracle-worker".into(),
            ..Default::default()
        }
        .forward(target())
        .await
        .unwrap();
        let activity_observation = std::str::from_utf8(&activity.task_token).unwrap();
        assert!(activity_observation.starts_with("go-yarpc-activity|"));
        assert!(
            activity_observation.contains("|cadence-matching|cadence-matching|true|proto"),
            "unexpected Go YARPC activity observation: {activity_observation}"
        );
        assert_eq!(activity.activity_id, "go-peer-activity");
        assert_eq!(
            activity.workflow_execution.unwrap().workflow_id,
            "go-peer-workflow"
        );

    }
    fn fixture(
        count: usize,
    ) -> (
        tempfile::TempDir,
        Arc<SqliteTaskStore>,
        TaskListKey,
        TaskDispatcher,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        };
        let queue = Arc::new(
            DurableTaskQueue::open(
                store.clone(),
                key.clone(),
                0,
                NonZeroU32::new(100).unwrap(),
                now(),
            )
            .unwrap(),
        );
        let task = TaskData {
            workflow_id: "workflow".into(),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            schedule_id: 2,
            ..Default::default()
        }
        .at_creation(now(), 0);
        queue.append(&vec![task; count], now()).unwrap();
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        (
            directory,
            store,
            key,
            TaskDispatcher::new(queue, history, Uuid::from_u128(1).to_string()),
        )
    }
    #[tokio::test]
    async fn matching_dispatch_writer_rewrite_retries_sql_before_acknowledging_original() {
        let (directory, store, key, dispatcher) = fixture(1);
        let task = dispatcher.claim("").await.ok().unwrap().0.unwrap();
        let writer = crate::matching_writer::TaskWriter::new(
            dispatcher.queue.clone(),
            dispatcher.task_notification(),
            crate::matching_writer::WriterConfig::default(),
        );
        let connection = rusqlite::Connection::open(directory.path().join("tasks.db")).unwrap();
        connection.execute_batch("CREATE TRIGGER reject_rewrite BEFORE INSERT ON tasks BEGIN SELECT RAISE(FAIL, 'temporary outage'); END;").unwrap();
        // A normal AddTask still gets its first failure, without outer retry.
        assert!(writer
            .append(crate::matching_writer::WriteTask {
                data: task.data.clone(),
                source_domain: key.domain_id,
                schedule_to_start_seconds: 0,
            })
            .await
            .is_err());
        assert!(!dispatcher.queue.is_stopped());
        let dispatcher = dispatcher.with_writer(writer.clone());
        let copy = dispatcher.clone();
        let rewrite = tokio::spawn(async move { copy.rewrite(task.task_id).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            // Failed writes still reserve IDs. Seeing read level >2 proves
            // an actual rewrite transaction failed before recovery is enabled.
            while dispatcher.queue.levels().0 <= 2 {
                assert!(dispatcher
                    .queue
                    .read_batch(100, NonZeroU32::new(1).unwrap(), now())
                    .unwrap()
                    .tasks
                    .is_empty());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.get_tasks(&key, 0, 100, 10).unwrap().len(), 1);
        assert_eq!(
            dispatcher.queue.levels().1,
            0,
            "original must remain unacknowledged"
        );
        connection
            .execute_batch("DROP TRIGGER reject_rewrite")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), rewrite)
            .await
            .unwrap()
            .unwrap();
        assert!(!dispatcher.queue.is_stopped());
        let rows = store.get_tasks(&key, 0, 100, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].task_id > 3);
        assert_eq!(dispatcher.queue.levels().1, 1);
        writer.stop();
        dispatcher.queue.stop();
    }
    #[tokio::test]
    async fn matching_dispatch_cancelled_poll_rewrites_claim_before_worker_exits() {
        let (_directory, store, key, dispatcher) = fixture(1);
        let claimed = Arc::new(Notify::new());
        let copy = claimed.clone();
        let worker = dispatcher.clone();
        let poll = tokio::spawn(async move {
            worker
                .poll((), Duration::from_secs(10), move |_, _| {
                    copy.notify_one();
                    std::future::pending::<Result<m::PollForDecisionTaskResponse, Status>>()
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .unwrap();
        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let tasks = store.get_tasks(&key, 0, 100, 10).unwrap();
                if tasks.len() == 1 && tasks[0].task_id == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(dispatcher.queue.levels(), (1, 1, 0));
    }
    #[tokio::test]
    async fn matching_dispatch_transient_failure_rewrites_before_successful_redelivery() {
        let (_directory, store, key, dispatcher) = fixture(1);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let copy = seen.clone();
        let response = dispatcher
            .poll((), Duration::from_secs(2), move |task, _| {
                let mut seen = copy.lock().unwrap();
                seen.push(task.task_id);
                std::future::ready(if seen.len() == 1 {
                    Err(Status::internal("retry after exhausted History retries"))
                } else {
                    Ok(m::PollForDecisionTaskResponse {
                        task_token: vec![42],
                        ..Default::default()
                    })
                })
            })
            .await
            .unwrap();
        assert_eq!(response.task_token, vec![42]);
        assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
        dispatcher.queue.run_garbage_collection(true).unwrap();
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
    }
    #[tokio::test]
    async fn matching_dispatch_typed_terminal_error_drops_without_rewrite() {
        #[derive(Clone, PartialEq, Message)]
        struct StatusDetails {
            #[prost(message, repeated, tag = "3")]
            details: Vec<prost_types::Any>,
        }
        let (_directory, store, key, dispatcher) = fixture(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        dispatcher
            .poll((), Duration::from_secs(2), move |task, _| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(if task.task_id == 1 {
                    Err(Status::with_details(
                        tonic::Code::NotFound,
                        "gone",
                        StatusDetails {
                            details: vec![prost_types::Any {
                                type_url:
                                    "type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError"
                                        .into(),
                                value: Vec::new(),
                            }],
                        }
                        .encode_to_vec()
                        .into(),
                    ))
                } else {
                    Ok(m::PollForDecisionTaskResponse::default())
                })
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        dispatcher.queue.run_garbage_collection(true).unwrap();
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert_eq!(
            dispatcher
                .queue
                .append(&[TaskData::default()], now())
                .unwrap()[0]
                .task_id,
            3
        );
    }
    #[tokio::test]
    async fn matching_dispatch_background_timeout_unregisters_wait_before_next_task() {
        let (_directory, _store, _key, dispatcher) = fixture(0);
        let reader = crate::matching_reader::TaskReader::new(
            dispatcher.queue.clone(),
            dispatcher.task_notification(),
            crate::matching_reader::ReaderConfig::default(),
        );
        let dispatcher = dispatcher.with_reader(reader.clone());
        let manager = Arc::new(Mutex::new(cadence_common::poller::Manager::default()));
        let response = dispatcher
            .poll_with_lifecycle(
                (),
                Duration::from_millis(10),
                |_, _, _| -> std::future::Ready<Result<m::PollForDecisionTaskResponse, Status>> {
                    panic!("empty poll must not deliver")
                },
                Some(PollLifecycle {
                    manager: manager.clone(),
                    id: "expired".into(),
                    info: cadence_common::poller::Info {
                        identity: "worker".into(),
                        rate_per_second: 1.0,
                        isolation_group: String::new(),
                    },
                    forwarded: false,
                    metric_context: None,
                    metrics: crate::matching_metrics::MatchingMetrics::default(),
                }),
            )
            .await
            .unwrap();
        assert!(response.task_token.is_empty());
        tokio::time::timeout(Duration::from_secs(2), async {
            while manager.lock().unwrap().contains_poll("expired") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        dispatcher
            .queue
            .append(&[TaskData::default()], now())
            .unwrap();
        dispatcher.notify_new_tasks();
        dispatcher
            .poll((), Duration::from_secs(2), |task, _| {
                assert_eq!(
                    task.task_id, 1,
                    "cancelled waiter must not claim/rewrite this task"
                );
                std::future::ready(Ok(m::PollForDecisionTaskResponse::default()))
            })
            .await
            .unwrap();
        assert_eq!(dispatcher.queue.levels().1, 1);
        reader.stop();
        dispatcher.queue.stop();
        reader.wait_stopped().await;
    }
    #[tokio::test]
    async fn matching_dispatch_empty_long_poll_expires_without_delivery() {
        let (_directory, _store, _key, dispatcher) = fixture(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
        let metrics = crate::matching_metrics::MatchingMetrics::new(sink.clone());
        let response = dispatcher
            .poll_with_lifecycle(
                (),
                Duration::from_millis(10),
                move |_, _, _| {
                    copy.fetch_add(1, Ordering::Relaxed);
                    std::future::ready(Ok(m::PollForActivityTaskResponse::default()))
                },
                Some(PollLifecycle {
                    manager: Arc::new(Mutex::new(cadence_common::poller::Manager::default())),
                    id: "timeout".into(),
                    info: cadence_common::poller::Info {
                        identity: "worker".into(),
                        rate_per_second: 1.0,
                        isolation_group: String::new(),
                    },
                    forwarded: false,
                    metric_context: Some(crate::matching_metrics::PollMetricContext {
                        domain: "metric-domain".into(),
                        task_list: "metric-list".into(),
                        task_list_kind: 0,
                        task_type: "activity_task",
                        poller_isolation_group: String::new(),
                    }),
                    metrics,
                }),
            )
            .await
            .unwrap();
        assert!(response.task_token.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(dispatcher.queue.levels().2, 0);
        assert!(sink
            .events()
            .iter()
            .any(|event| event.name == crate::matching_metrics::POLL_TIMEOUT));
    }
    #[tokio::test]
    async fn matching_dispatch_poller_wait_ends_before_history_and_restarts_after_failure() {
        let (_directory, _store, _key, dispatcher) = fixture(1);
        let manager = Arc::new(Mutex::new(cadence_common::poller::Manager::default()));
        let lifecycle = PollLifecycle {
            manager: manager.clone(),
            id: "poll".into(),
            info: cadence_common::poller::Info {
                identity: "worker".into(),
                rate_per_second: 42.0,
                isolation_group: "a".into(),
            },
            forwarded: false,
            metric_context: None,
            metrics: crate::matching_metrics::MatchingMetrics::default(),
        };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let timestamps = seen.clone();
        dispatcher
            .poll_with_lifecycle(
                (),
                Duration::from_secs(2),
                move |_, _, _| {
                    let mut manager = manager.lock().unwrap();
                    assert!(
                        !manager.contains_poll("poll"),
                        "Go EndPoll precedes History start"
                    );
                    assert!(!manager.has_after(i64::MAX));
                    let mut seen = timestamps.lock().unwrap();
                    seen.push(manager.list(now())[0].last_access_time);
                    std::future::ready(if seen.len() == 1 {
                        Err(Status::internal("exhausted History retries"))
                    } else {
                        Ok(m::PollForDecisionTaskResponse::default())
                    })
                },
                Some(lifecycle),
            )
            .await
            .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[1] >= seen[0]);
    }
    #[tokio::test]
    async fn matching_dispatch_empty_wait_does_not_refresh_liveness_repeatedly() {
        let (_directory, _store, _key, dispatcher) = fixture(0);
        let liveness = Arc::new(cadence_common::liveness::Liveness::new(0, 300_000_000_000));
        let worker = dispatcher.with_liveness(liveness.clone());
        let poll = tokio::spawn(async move {
            worker
                .poll_decision(
                    p::PollForDecisionTaskRequest::default(),
                    Duration::from_secs(10),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !liveness.is_alive(now()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Force the stored time backwards while the same GetTask wait is active.
        // Internal 100ms persistence reads must not count as another client poll.
        liveness.mark_alive(0);
        tokio::time::sleep(Duration::from_millis(220)).await;
        assert!(!liveness.is_alive(now()));
        poll.abort();
        assert!(poll.await.unwrap_err().is_cancelled());
    }
    #[tokio::test]
    async fn matching_dispatch_records_poll_events_from_claimed_task_and_real_poll_context() {
        let directions = [
            (false, false, crate::matching_metrics::LOCAL_TO_LOCAL_MATCH),
            (false, true, crate::matching_metrics::LOCAL_TO_REMOTE_MATCH),
            (true, false, crate::matching_metrics::REMOTE_TO_LOCAL_MATCH),
            (true, true, crate::matching_metrics::REMOTE_TO_REMOTE_MATCH),
        ];
        for (case, (task_forwarded, poll_forwarded, direction)) in
            directions.into_iter().enumerate()
        {
            let (_directory, _store, _key, dispatcher) = fixture(0);
            let original = if case == 3 { Some("red") } else { None };
            let mut partition_config =
                std::collections::BTreeMap::from([("isolation-group".into(), "blue".into())]);
            if let Some(original) = original {
                partition_config.insert("original-isolation-group".into(), original.into());
            }
            let data = TaskData {
                workflow_id: format!("telemetry-{case}"),
                partition_config: Some(partition_config),
                ..Default::default()
            }
            .at_creation(now(), 0);
            let reader = crate::matching_reader::TaskReader::new(
                dispatcher.queue.clone(),
                dispatcher.task_notification(),
                crate::matching_reader::ReaderConfig::default(),
            );
            let matcher = Arc::new(crate::matching_sync::SyncMatcher::new(
                &["blue".into()],
                reader.stop_signal().subscribe(),
            ));
            let dispatcher = dispatcher
                .with_reader(reader.clone())
                .with_sync_matcher(matcher.clone());
            let producer = tokio::spawn(async move {
                matcher
                    .offer(
                        data,
                        "blue",
                        "00000000-0000-0000-0000-000000000001",
                        task_forwarded,
                        Duration::from_secs(1),
                        tokio::time::Instant::now() + Duration::from_secs(2),
                    )
                    .await
            });
            let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
            let metrics = crate::matching_metrics::MatchingMetrics::new(sink.clone());
            let lifecycle = PollLifecycle {
                manager: Arc::new(Mutex::new(cadence_common::poller::Manager::default())),
                id: format!("metric-poller-{case}"),
                info: cadence_common::poller::Info {
                    identity: "worker".into(),
                    rate_per_second: 1.0,
                    isolation_group: "blue".into(),
                },
                forwarded: poll_forwarded,
                metric_context: Some(crate::matching_metrics::PollMetricContext {
                    domain: "metric-domain".into(),
                    task_list: "metric-list".into(),
                    task_list_kind: 0,
                    task_type: "decision_task",
                    poller_isolation_group: "blue".into(),
                }),
                metrics,
            };
            dispatcher
                .poll_with_lifecycle(
                    p::PollForDecisionTaskRequest::default(),
                    Duration::from_secs(2),
                    |_, _, _| std::future::ready(Ok(m::PollForDecisionTaskResponse::default())),
                    Some(lifecycle),
                )
                .await
                .unwrap();
            assert!(producer.await.unwrap());
            let events = sink.events();
            let poll_success = events
                .iter()
                .find(|e| e.name == crate::matching_metrics::POLL_SUCCESS)
                .unwrap();
            assert_eq!(
                poll_success.tags[crate::matching_metrics::DOMAIN_TAG],
                "metric-domain"
            );
            assert_eq!(
                poll_success.tags[crate::matching_metrics::TASK_LIST_TAG],
                "metric_list"
            );
            assert!(events
                .iter()
                .any(|e| e.name == crate::matching_metrics::POLL_SUCCESS_SYNC));
            let latency = if poll_forwarded {
                crate::matching_metrics::POLL_FORWARD_MATCH_LATENCY
            } else {
                crate::matching_metrics::POLL_LOCAL_MATCH_LATENCY
            };
            assert!(events.iter().any(|event| event.name == latency));
            assert!(events.iter().any(|event| {
                event.name
                    == if poll_forwarded {
                        crate::matching_metrics::POLL_FORWARD_MATCH_LATENCY_NS
                    } else {
                        crate::matching_metrics::POLL_LOCAL_MATCH_LATENCY_NS
                    }
            }));
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.name.contains("_to_") && e.name.ends_with("matches_per_tl"))
                    .map(|e| e.name)
                    .collect::<Vec<_>>(),
                vec![direction]
            );
            let isolation = events
                .iter()
                .find(|e| e.name == crate::matching_metrics::ISOLATION_TASK_MATCH)
                .unwrap();
            assert_eq!(
                isolation.tags[crate::matching_metrics::ISOLATION_GROUP_TAG],
                original.unwrap_or("blue")
            );
            assert_eq!(
                isolation.tags[crate::matching_metrics::POLLER_ISOLATION_GROUP_TAG],
                "blue"
            );
            assert_eq!(
                events
                    .iter()
                    .any(|e| e.name == crate::matching_metrics::ISOLATION_SUCCESS),
                original.is_none()
            );
            reader.stop();
            dispatcher.queue.stop();
            reader.wait_stopped().await;
        }
    }

    #[tokio::test]
    async fn matching_dispatch_sync_offer_delivers_without_durable_claim() {
        let (_directory, store, key, dispatcher) = fixture(0);
        let reader = crate::matching_reader::TaskReader::new(
            dispatcher.queue.clone(),
            dispatcher.task_notification(),
            crate::matching_reader::ReaderConfig::default(),
        );
        let matcher = Arc::new(crate::matching_sync::SyncMatcher::new(
            &[],
            reader.stop_signal().subscribe(),
        ));
        let dispatcher = dispatcher
            .with_reader(reader)
            .with_sync_matcher(matcher.clone());
        let data = TaskData {
            workflow_id: "sync".into(),
            ..Default::default()
        };
        let producer = tokio::spawn(async move {
            matcher
                .offer(
                    data,
                    "",
                    &Uuid::from_u128(1).to_string(),
                    false,
                    Duration::from_secs(10),
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .await
        });
        // Go `matcher.Offer` with a response channel succeeds only when the
        // poller reports delivery success; nothing is claimed from SQLite.
        let response = dispatcher
            .poll((), Duration::from_secs(10), |task, _| {
                assert_eq!(task.task_id, 0, "sync tasks carry no durable ID");
                std::future::ready(Ok(m::PollForDecisionTaskResponse {
                    task_token: vec![7],
                    ..Default::default()
                }))
            })
            .await
            .unwrap();
        assert_eq!(response.task_token, vec![7]);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), producer)
                .await
                .expect("producer resolves")
                .unwrap(),
            "History-gated handoff reports success to the adder"
        );
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert_eq!(dispatcher.queue.levels(), (0, 0, 0));
    }
    struct BacklogParent;
    #[tonic::async_trait]
    impl m::matching_api_server::MatchingApi for BacklogParent {
        async fn add_decision_task(&self, request: tonic::Request<m::AddDecisionTaskRequest>) -> Result<tonic::Response<m::AddDecisionTaskResponse>, Status> {
            assert_eq!(request.metadata().get("rpc-service").unwrap(), "cadence-matching");
            let task = request.into_inner();
            assert_eq!(task.task_list.unwrap(), p::TaskList { name: "queue".into(), kind: 1, ..Default::default() });
            assert_eq!(task.domain_id, uuid::Uuid::from_u128(1).to_string());
            assert_eq!(task.forwarded_from, "/__cadence_sys/queue/1");
            assert_eq!(task.source, 2);
            assert_eq!(task.schedule_id, 91);
            assert_eq!(task.schedule_to_start_timeout.unwrap().seconds, 0);
            assert_eq!(task.partition_config.get("isolation-group").unwrap(), "zone-a");
            Ok(tonic::Response::new(m::AddDecisionTaskResponse::default()))
        }
        async fn add_activity_task(&self, request: tonic::Request<m::AddActivityTaskRequest>) -> Result<tonic::Response<m::AddActivityTaskResponse>, Status> {
            assert_eq!(request.metadata().get("rpc-caller").unwrap(), "cadence-matching");
            let task = request.into_inner();
            assert_eq!(task.task_list.unwrap().name, "queue");
            assert_eq!(task.domain_id, task.source_domain_id);
            assert_eq!(task.source, 2);
            assert_eq!(task.forwarded_from, "/__cadence_sys/queue/1");
            Err(crate::grpc::detailed_status(tonic::Code::ResourceExhausted, "parent busy", "ServiceBusyError", p::ServiceBusyError { reason: String::new() }))
        }
    }
    #[tokio::test]
    async fn matching_backlog_forward_remote_fields_busy_and_rate_limit() {
        use crate::matching_forward::{ForwardError, ForwardRate, RequestTokens};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(m::matching_api_server::MatchingApiServer::new(BacklogParent))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap().connect().await.unwrap();
        let state = ForwardState::new(move || {
            let channel = channel.clone();
            Box::pin(async move { Some((Parent::Remote(channel), "queue".into())) })
        }, Arc::new(|| 1), Arc::new(RequestTokens::new(Arc::new(|| 1))), Arc::new(ForwardRate::new(Arc::new(|| 2), now())), "/__cadence_sys/queue/1".into(), Arc::new(|| 0));
        let task = AllocatedTask { task_id: 1, data: cadence_persistence::matching_blob::TaskData {
            schedule_id: 91, created_nanos: 1_000_000_000, expiry_nanos: 10_000_000_000,
            partition_config: Some(std::collections::BTreeMap::from([("isolation-group".into(), "zone-a".into())])),
            ..Default::default()
        }};
        let domain = uuid::Uuid::from_u128(1).to_string();
        state.forward_backlog_task(task.clone(), &domain, 0, 0).await.unwrap();
        assert!(matches!(state.forward_backlog_task(task.clone(), &domain, 1, 0).await, Err(ForwardError::SlowDown)));
        let mut limited = state.clone();
        limited.add_rate = Arc::new(ForwardRate::new(Arc::new(|| 0), now()));
        assert!(matches!(limited.forward_backlog_task(task.clone(), &domain, 0, 0).await, Err(ForwardError::SlowDown)), "zero forwarding rate rejects before issuing an RPC");
        assert!(matches!(state.forward_backlog_task(task, &domain, 0, 1).await, Err(ForwardError::TaskListKind)));
        let task = AllocatedTask { task_id: 2, data: Default::default() };
        let mut root = state.clone();
        root.source = "queue".into();
        assert!(matches!(root.forward_backlog_task(task.clone(), &domain, 0, 0).await, Err(ForwardError::NoParent)));
        let mut invalid = state.clone();
        invalid.add_rate = Arc::new(ForwardRate::new(Arc::new(|| 1), now()));
        assert!(matches!(invalid.forward_backlog_task(task, &domain, 2, 0).await, Err(ForwardError::InvalidTaskListType)));
        server.abort();
    }

}
