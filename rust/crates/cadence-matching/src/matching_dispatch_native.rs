//! Byte-preserving native twin of `matching_dispatch.rs`: cancellation-safe
//! ownership of asynchronous Matching task delivery over `DurableTaskQueueBytes`,
//! `NativeTaskReader`/`NativeTaskWriter`/`NativeSyncMatcher`, `GoString`
//! identifiers and the native protobuf model. Task keys and data are never
//! converted through `String`. A detached delivery worker keeps responsibility
//! for a claimed task even when the waiting RPC future disappears.
//!
//! Additive; production registry adoption is still required.
use crate::matching_history::{start_error_kind, MatchingHistoryClient, StartErrorKind};
use crate::matching_reader_native::NativeTaskReader;
use crate::matching_sync_native::{NativeSyncMatcher, NativeSyncTask};
use crate::matching_writer_native::NativeTaskWriter;
use cadence_common::go_string::GoString;
use cadence_persistence::{
    matching_native_queue::{DurableTaskQueueBytes, NativeQueueError, PendingTaskBytes},
    matching_native_task_store::{AllocatedTaskBytes, MatchingTaskStoreBytesError},
    matching_task_store::{BackendErrorKind, MatchingTaskStoreError},
};
use cadence_proto::native as n;
use std::{
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

pub type NativeFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Narrow stand-in for the not-yet-existing native Matching client/service
/// surface used by forwarding. Local parents implement it in-process; remote
/// parents implement it over a byte-preserving transport. `deadline` is the
/// remaining caller budget (legacy: `request.set_timeout`).
pub trait NativeMatchingApi: Send + Sync {
    fn poll_for_decision_task(
        &self,
        request: n::matching::PollForDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForDecisionTaskResponse, Status>>;
    fn poll_for_activity_task(
        &self,
        request: n::matching::PollForActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<n::matching::PollForActivityTaskResponse, Status>>;
    fn add_decision_task(
        &self,
        request: n::matching::AddDecisionTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>>;
    fn add_activity_task(
        &self,
        request: n::matching::AddActivityTaskRequest,
        deadline: tokio::time::Instant,
    ) -> NativeFuture<Result<(), Status>>;
}

/// Observation hooks standing in for `matching_metrics` (whose context is
/// UTF-8 `String` based). All methods default to no-ops.
pub trait NativePollObserver: Send + Sync {
    fn poller_count(&self, _count: usize) {}
    fn poll_success(&self, _sync: bool) {}
    fn poll_match_latency(&self, _elapsed: Duration, _forwarded_poll: bool, _forward_failed: bool) {
    }
    fn poll_match(&self, _task_forwarded: bool, _poll_forwarded: bool) {}
    fn async_match_latency(&self, _latency: Duration) {}
    fn poll_already_started(&self) {}
    fn poll_timeout(&self) {}
    fn forward_poll_latency(&self, _elapsed: Duration) {}
    fn forward_task_latency(&self, _elapsed: Duration) {}
    fn invalid_isolation_group(&self) {}
}

/// Shared Go-compatible poller history with byte-exact identities and groups.
pub type NativePollerManager = Mutex<cadence_common::poller_bytes::Manager>;

/// Observation of one getTask wait. EndPoll precedes History's start RPC.
#[derive(Clone)]
pub struct NativePollLifecycle {
    pub manager: Arc<NativePollerManager>,
    pub id: GoString,
    pub cancel: Option<Arc<Notify>>,
    pub info: cadence_common::poller_bytes::Info,
    pub forwarded: bool,
    pub observer: Option<Arc<dyn NativePollObserver>>,
}
pub(crate) struct NativePollWaitGuard(NativePollLifecycle);
impl NativePollLifecycle {
    pub(crate) fn begin(&self) -> NativePollWaitGuard {
        let count = {
            let mut manager = self.manager.lock().expect("poller history lock");
            let timestamp = now();
            manager.start(self.id.as_bytes(), self.info.clone(), timestamp);
            if let Some(cancel) = &self.cancel {
                let cancel = cancel.clone();
                manager.set_cancel(self.id.as_bytes(), Arc::new(move || cancel.notify_one()));
            }
            manager.count_at(timestamp)
        };
        if !self.info.identity.is_empty() {
            if let Some(observer) = &self.observer {
                observer.poller_count(count);
            }
        }
        NativePollWaitGuard(self.clone())
    }
}
impl Drop for NativePollWaitGuard {
    fn drop(&mut self) {
        let count = {
            let mut manager = self.0.manager.lock().expect("poller history lock");
            let timestamp = now();
            manager.end(self.0.id.as_bytes(), timestamp);
            manager.count_at(timestamp)
        };
        if !self.0.info.identity.is_empty() {
            if let Some(observer) = &self.0.observer {
                observer.poller_count(count);
            }
        }
    }
}

pub enum NativeParent {
    Dispatcher(Box<NativeTaskDispatcher>),
    Service(Arc<dyn NativeMatchingApi>),
}
type ParentFuture = Pin<Box<dyn Future<Output = Option<(NativeParent, GoString)>> + Send>>;
type ParentResolver = dyn Fn() -> ParentFuture + Send + Sync;

/// Go `forwarder.ForwardPoll` / `ForwardTask` state; see legacy `ForwardState`.
#[derive(Clone)]
pub struct NativeForwardState {
    resolve: Arc<ParentResolver>,
    source: GoString,
    local_wait: crate::matching_forward::Count,
    permits: Arc<crate::matching_forward::RequestTokens>,
    add_permits: Arc<crate::matching_forward::RequestTokens>,
    add_rate: Arc<crate::matching_forward::ForwardRate>,
    observer: Option<Arc<dyn NativePollObserver>>,
}

struct ForwardPoll {
    target: Arc<dyn NativeMatchingApi>,
    domain_id: GoString,
    parent_name: GoString,
    forwarded_from: GoString,
    poller_id: GoString,
    isolation_group: GoString,
    deadline: tokio::time::Instant,
}
type ForwardResult<R> = NativeFuture<Result<R, Status>>;
trait NativeForwardPollRequest<R>: Clone + Send {
    fn for_parent(&self, name: &GoString) -> Self;
    fn forward(self, target: ForwardPoll) -> ForwardResult<R>;
}
fn parent_task_list(
    request_list: Option<&n::api::TaskList>,
    name: &GoString,
) -> Option<n::api::TaskList> {
    Some(n::api::TaskList {
        name: name.clone(),
        kind: request_list.map(|list| list.kind).unwrap_or_default(),
        ..Default::default()
    })
}
impl NativeForwardPollRequest<n::matching::PollForDecisionTaskResponse>
    for n::api::PollForDecisionTaskRequest
{
    fn for_parent(&self, name: &GoString) -> Self {
        Self {
            task_list: parent_task_list(self.task_list.as_ref(), name),
            identity: self.identity.clone(),
            ..Default::default()
        }
    }
    fn forward(
        self,
        target: ForwardPoll,
    ) -> ForwardResult<n::matching::PollForDecisionTaskResponse> {
        Box::pin(async move {
            let payload = self.for_parent(&target.parent_name);
            let request = n::matching::PollForDecisionTaskRequest {
                domain_id: target.domain_id,
                poller_id: target.poller_id,
                request: Some(payload),
                forwarded_from: target.forwarded_from,
                isolation_group: target.isolation_group,
            };
            target
                .target
                .poll_for_decision_task(request, target.deadline)
                .await
        })
    }
}
impl NativeForwardPollRequest<n::matching::PollForActivityTaskResponse>
    for n::api::PollForActivityTaskRequest
{
    fn for_parent(&self, name: &GoString) -> Self {
        Self {
            task_list: parent_task_list(self.task_list.as_ref(), name),
            identity: self.identity.clone(),
            ..Default::default()
        }
    }
    fn forward(
        self,
        target: ForwardPoll,
    ) -> ForwardResult<n::matching::PollForActivityTaskResponse> {
        Box::pin(async move {
            let payload = self.for_parent(&target.parent_name);
            let request = n::matching::PollForActivityTaskRequest {
                domain_id: target.domain_id,
                poller_id: target.poller_id,
                request: Some(payload),
                forwarded_from: target.forwarded_from,
                isolation_group: target.isolation_group,
            };
            target
                .target
                .poll_for_activity_task(request, target.deadline)
                .await
        })
    }
}
#[cfg(test)]
impl<R: Send + 'static> NativeForwardPollRequest<R> for () {
    fn for_parent(&self, _name: &GoString) -> Self {}
    fn forward(self, _target: ForwardPoll) -> ForwardResult<R> {
        Box::pin(async { Err(Status::unimplemented("test poll")) })
    }
}

struct ForwardTaskTimer<'a> {
    observer: Option<&'a Arc<dyn NativePollObserver>>,
    started: std::time::Instant,
}
impl Drop for ForwardTaskTimer<'_> {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer.forward_task_latency(self.started.elapsed());
        }
    }
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

fn run_id_text(task: &AllocatedTaskBytes) -> String {
    if let Some(raw) = &task.data.raw_run_id {
        return raw.go_json_string();
    }
    task.data
        .run_id
        .as_deref()
        .and_then(|bytes| uuid::Uuid::from_slice(bytes).ok())
        .map(|uuid| uuid.to_string())
        .unwrap_or_default()
}

/// Go `engine.go` poll-path info log. The sink filters by a `&str` domain; a
/// non-UTF-8 domain cannot be matched against it, so the event is skipped
/// (documented seam: the log sink has no byte-domain API). Field values use
/// Go-JSON escaping, never lossy conversion.
fn log_task_event(
    message: &str,
    task_domain_id: &GoString,
    poll_domain_id: &GoString,
    task_list: &GoString,
    task: &AllocatedTaskBytes,
    error: Option<&str>,
) {
    let Ok(task_domain) = task_domain_id.as_utf8() else {
        return;
    };
    let mut fields = vec![
        ("wf-domain-id", poll_domain_id.go_json_string()),
        ("wf-id", task.data.workflow_id.go_json_string()),
        ("wf-run-id", run_id_text(task)),
        ("wf-task-list-name", task_list.go_json_string()),
        ("wf-schedule-id", task.data.schedule_id.to_string()),
        ("queue-task-id", task.task_id.to_string()),
    ];
    if let Some(error) = error {
        fields.push(("error", error.to_owned()));
    }
    crate::matching_task_info_log::emit(task_domain, message, &fields);
}

struct DeliveryTask {
    task: AllocatedTaskBytes,
    sync: Option<NativeSyncTask>,
    forwarded: bool,
}
enum ClaimError {
    Queue(NativeQueueError),
    Join(tokio::task::JoinError),
    Forward(tokio::sync::OwnedSemaphorePermit),
    Released,
}
enum WaitResult {
    Closed,
    Released,
    Woken,
}

fn throttled(error: &MatchingTaskStoreError) -> bool {
    matches!(
        error,
        MatchingTaskStoreError::Backend {
            kind: BackendErrorKind::Throttled,
            ..
        }
    )
}
/// Go maps SQL throttling to ServiceBusyError; everything else is Internal.
fn queue_status(error: NativeQueueError) -> Status {
    let busy = match &error {
        NativeQueueError::Storage(MatchingTaskStoreBytesError::Backend {
            kind: BackendErrorKind::Throttled,
            ..
        }) => true,
        NativeQueueError::Storage(MatchingTaskStoreBytesError::Legacy(legacy)) => throttled(legacy),
        _ => false,
    };
    let message = match &error {
        NativeQueueError::Storage(MatchingTaskStoreBytesError::ConditionFailed { message }) => {
            message.clone()
        }
        other => other.to_string().into(),
    };
    if busy {
        return crate::matching_native_status::raw_detailed_status(
            tonic::Code::ResourceExhausted,
            &message,
            "ServiceBusyError",
            Vec::new(),
        );
    }
    crate::matching_native_status::raw_status(
        tonic::Code::Internal,
        &message,
        tonic::codegen::Bytes::new(),
    )
}

impl NativeForwardState {
    pub fn new(
        resolve: impl Fn() -> ParentFuture + Send + Sync + 'static,
        max_outstanding: crate::matching_forward::Count,
        max_outstanding_adds: Arc<crate::matching_forward::RequestTokens>,
        add_rate: Arc<crate::matching_forward::ForwardRate>,
        source: GoString,
        local_wait: crate::matching_forward::Count,
    ) -> Self {
        Self {
            resolve: Arc::new(resolve),
            source,
            local_wait,
            permits: Arc::new(crate::matching_forward::RequestTokens::new(max_outstanding)),
            add_permits: max_outstanding_adds,
            add_rate,
            observer: None,
        }
    }
    pub fn with_observer(mut self, observer: Arc<dyn NativePollObserver>) -> Self {
        self.observer = Some(observer);
        self
    }
    pub fn add_tokens(&self) -> Arc<crate::matching_forward::RequestTokens> {
        self.add_permits.clone()
    }

    /// `domain_id` is the task-list domain; `source_domain` the task's own
    /// domain. `timeout_seconds` stays zero as in Go persistence GetTasks.
    pub async fn forward_backlog_task(
        &self,
        task: AllocatedTaskBytes,
        domain_id: &GoString,
        source_domain: &GoString,
        task_type: i32,
        kind: i16,
    ) -> Result<(), crate::matching_forward::ForwardError> {
        use crate::matching_forward::ForwardError;
        if kind == 1 {
            return Err(ForwardError::TaskListKind);
        }
        let name = cadence_common::task_list::TaskListNameBytes::parse(self.source.as_bytes())
            .map_err(|message| {
                ForwardError::Remote(crate::matching_native_status::raw_status(
                    tonic::Code::InvalidArgument,
                    &message,
                    tonic::codegen::Bytes::new(),
                ))
            })?;
        if name.is_root() {
            return Err(ForwardError::NoParent);
        }
        if !self.add_rate.allow(now()) {
            return Err(ForwardError::SlowDown);
        }
        let _timer = ForwardTaskTimer {
            observer: self.observer.as_ref(),
            started: std::time::Instant::now(),
        };
        if !matches!(task_type, 0 | 1) {
            return Err(ForwardError::InvalidTaskListType);
        }
        let (parent, parent_name) = (self.resolve)().await.ok_or_else(|| {
            ForwardError::Remote(Status::unavailable(
                "cannot resolve parent task list for forwarding",
            ))
        })?;
        let execution = Some(n::api::WorkflowExecution {
            workflow_id: task.data.workflow_id.clone(),
            run_id: GoString::from(task.data.run_id_bytes()),
        });
        let partition_config = task
            .data
            .partition_config
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let schedule_id = task.data.schedule_id;
        let forwarded_from = self.source.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let task_list = Some(n::api::TaskList {
            name: parent_name.clone(),
            kind: i32::from(kind) + 1,
            ..Default::default()
        });
        let timeout = Some(prost_types::Duration {
            seconds: 0,
            nanos: 0,
        });
        let result = match parent {
            NativeParent::Service(service) => {
                if task_type == 0 {
                    service
                        .add_decision_task(
                            n::matching::AddDecisionTaskRequest {
                                domain_id: source_domain.clone(),
                                workflow_execution: execution,
                                task_list,
                                schedule_id,
                                schedule_to_start_timeout: timeout,
                                source: 2,
                                forwarded_from,
                                partition_config,
                            },
                            deadline,
                        )
                        .await
                } else {
                    service
                        .add_activity_task(
                            n::matching::AddActivityTaskRequest {
                                domain_id: domain_id.clone(),
                                source_domain_id: source_domain.clone(),
                                workflow_execution: execution,
                                task_list,
                                schedule_id,
                                schedule_to_start_timeout: timeout,
                                source: 2,
                                forwarded_from,
                                partition_config,
                                activity_task_dispatch_info: None,
                            },
                            deadline,
                        )
                        .await
                }
            }
            NativeParent::Dispatcher(dispatcher) => {
                let operation = if task_type == 0 {
                    "matching-add-decision-task"
                } else {
                    "matching-add-activity-task"
                };
                crate::matching_fault_injection::inject_client_call(operation, async {
                    dispatcher.accept_forwarded_backlog(task, source_domain, &parent_name, task_type).await
                }).await
            }
        };
        result.map_err(ForwardError::classify)
    }
}

/// Native dispatcher; mirrors `TaskDispatcher` field for field.
#[derive(Clone)]
pub struct NativeTaskDispatcher {
    queue: Arc<DurableTaskQueueBytes>,
    history: MatchingHistoryClient,
    domain_id: GoString,
    wake: Arc<Notify>,
    liveness: Option<Arc<cadence_common::liveness::Liveness>>,
    writer: Option<NativeTaskWriter>,
    reader: Option<NativeTaskReader>,
    sync_matcher: Option<Arc<NativeSyncMatcher>>,
    forward: Option<NativeForwardState>,
    pollers: Option<Arc<NativePollerManager>>,
    blocked_pollers: Arc<Notify>,
}

impl NativeTaskDispatcher {
    pub fn new(
        queue: Arc<DurableTaskQueueBytes>,
        history: MatchingHistoryClient,
        domain_id: GoString,
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
    pub fn with_writer(mut self, writer: NativeTaskWriter) -> Self {
        self.writer = Some(writer);
        self
    }
    pub fn with_reader(mut self, reader: NativeTaskReader) -> Self {
        self.reader = Some(reader);
        self
    }
    pub fn with_sync_matcher(mut self, matcher: Arc<NativeSyncMatcher>) -> Self {
        self.sync_matcher = Some(matcher);
        self
    }
    pub fn with_forward(mut self, forward: NativeForwardState) -> Self {
        self.forward = Some(forward);
        self
    }
    pub fn with_pollers(mut self, pollers: Arc<NativePollerManager>) -> Self {
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
    pub fn reader_should_reload(&self) -> bool {
        self.reader
            .as_ref()
            .is_some_and(|reader| reader.should_reload())
    }

    pub async fn accept_forwarded_backlog(
        &self,
        task: AllocatedTaskBytes,
        source_domain: &GoString,
        parent_name: &GoString,
        task_type: i32,
    ) -> Result<(), Status> {
        // Go uuid.Parse on the wire domain: non-UTF-8 is simply invalid.
        source_domain
            .as_utf8()
            .ok()
            .and_then(|text| uuid::Uuid::parse_str(text).ok())
            .ok_or_else(|| Status::invalid_argument("DomainUUID is not valid."))?;
        let matcher = self
            .sync_matcher
            .as_ref()
            .ok_or_else(crate::matching_dispatch::remote_sync_match_failed)?;
        let root = cadence_common::task_list::TaskListNameBytes::parse(parent_name.as_bytes())
            .map_err(|_| Status::invalid_argument("TaskList is not valid."))?
            .is_root();
        let local_wait = if root {
            Duration::from_secs(2)
        } else {
            self.forward
                .as_ref()
                .map(|forward| Duration::from_nanos((forward.local_wait)().max(0) as u64))
                .unwrap_or(Duration::from_millis(10))
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let group = task
            .data
            .partition_config
            .as_ref()
            .and_then(|config| config.get(&GoString::from("isolation-group")))
            .cloned()
            .unwrap_or_default();
        if matcher
            .offer(
                task.clone(),
                &group,
                source_domain,
                true,
                local_wait,
                deadline,
            )
            .await
        {
            return Ok(());
        }
        if !root {
            if let Some(forward) = &self.forward {
                if let Some(_permit) = forward.add_permits.try_acquire() {
                    if Box::pin(forward.forward_backlog_task(
                        task,
                        &self.domain_id,
                        source_domain,
                        task_type,
                        0,
                    ))
                    .await
                    .is_ok()
                    {
                        return Ok(());
                    }
                }
            }
        }
        Err(crate::matching_dispatch::remote_sync_match_failed())
    }

    async fn claim(
        &self,
        group: &GoString,
    ) -> Result<(Option<AllocatedTaskBytes>, bool), ClaimError> {
        if let Some(reader) = &self.reader {
            return reader
                .poll_group(group.as_bytes())
                .await
                .map(|task| (Some(task), false))
                .ok_or(ClaimError::Queue(NativeQueueError::Stopped));
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
        group: &GoString,
    ) -> Result<(Option<DeliveryTask>, bool), ClaimError> {
        if let Some(matcher) = self.sync_matcher.as_ref().filter(|_| self.reader.is_some()) {
            tokio::select! {
                task = matcher.poll(group) => task
                    .map(|sync| {
                        let forwarded = sync.forwarded;
                        (Some(DeliveryTask { task: sync.task.clone(), sync: Some(sync), forwarded }), false)
                    })
                    .ok_or(ClaimError::Queue(NativeQueueError::Stopped)),
                result = self.claim(group) => result.map(|(task, pending)| {
                    (task.map(|task| DeliveryTask { task, sync: None, forwarded: false }), pending)
                }),
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
            let source_domain = self
                .domain_id
                .as_utf8()
                .ok()
                .and_then(|text| uuid::Uuid::parse_str(text).ok());
            let result = match (data, source_domain) {
                (Ok(Ok(data)), Some(source_domain)) => writer
                    .append_rewrite(PendingTaskBytes {
                        data,
                        source_domain,
                        schedule_to_start_seconds: 0,
                    })
                    .await
                    .map(|_| ()),
                (Ok(Ok(_)), None) => Err(Status::internal("invalid Matching domain for rewrite")),
                (Ok(Err(error)), _) => Err(queue_status(error)),
                (Err(error), _) => Err(Status::internal(error.to_string())),
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
            Ok(Err::<(), NativeQueueError>(error)) => {
                eprintln!("Matching task rewrite failed: {error}")
            }
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
            Ok::<_, NativeQueueError>(())
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

    /// Go `pollOrForward`'s forward leg; see legacy `forward_poll`.
    fn forward_poll<'a, Q, R, F, Fut>(
        &'a self,
        poll: Q,
        sender: &'a mut Option<oneshot::Sender<Result<R, Status>>>,
        deliver: &'a mut F,
        budget: (tokio::time::Instant, tokio::sync::OwnedSemaphorePermit),
        lifecycle: Option<NativePollLifecycle>,
        poll_started: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>
    where
        Q: NativeForwardPollRequest<R> + 'a,
        R: Default + Send + 'a,
        F: FnMut(AllocatedTaskBytes, Q, GoString) -> Fut + Send + 'a,
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
            let resolved = tokio::select! {
                resolved = (forward.resolve)() => resolved,
                _ = async {
                    if let Some(sender) = sender.as_mut() {
                        sender.closed().await;
                    }
                } => return true,
            };
            let Some((parent, parent_name)) = resolved else {
                return false;
            };
            let parent = match parent {
                NativeParent::Dispatcher(parent) => parent,
                NativeParent::Service(service) => {
                    let (poller_id, isolation_group) = lifecycle
                        .as_ref()
                        .map(|poll| (poll.id.clone(), poll.info.isolation_group.clone()))
                        .unwrap_or_default();
                    let target = ForwardPoll {
                        target: service,
                        domain_id: self.domain_id.clone(),
                        parent_name,
                        forwarded_from: forward.source.clone(),
                        poller_id,
                        isolation_group,
                        deadline,
                    };
                    let Some(response) = sender.as_mut() else {
                        return true;
                    };
                    let forward_started = tokio::time::Instant::now();
                    let result = tokio::select! {
                        result = poll.forward(target) => Some(result),
                        _ = response.closed() => None,
                    };
                    if let Some(observer) = lifecycle.as_ref().and_then(|l| l.observer.as_ref()) {
                        observer.forward_poll_latency(forward_started.elapsed());
                    }
                    let Some(result) = result else {
                        return true;
                    };
                    return match result {
                        Ok(value) => {
                            if let Some(observer) =
                                lifecycle.as_ref().and_then(|l| l.observer.as_ref())
                            {
                                observer.poll_match_latency(poll_started.elapsed(), true, false);
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
            let observer = lifecycle.as_ref().and_then(|poll| poll.observer.clone());
            let forward_started = tokio::time::Instant::now();
            let lifecycle = lifecycle.and_then(|mut lifecycle| {
                lifecycle.manager = parent.pollers.clone()?;
                lifecycle.info.rate_per_second = crate::matching_limiter::TASK_DISPATCH_RPS;
                lifecycle.forwarded = true;
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
                    None,
                )
                .await;
            if let Some(observer) = observer {
                observer.forward_poll_latency(forward_started.elapsed());
            }
            true
        })
    }

    pub async fn query_mutable_state(
        &self,
        execution: &n::api::WorkflowExecution,
    ) -> Result<n::history::GetMutableStateResponse, Status> {
        self.query_mutable_state_with_deadline(execution, None)
            .await
    }
    pub async fn query_mutable_state_with_deadline(
        &self,
        execution: &n::api::WorkflowExecution,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::history::GetMutableStateResponse, Status> {
        self.history
            .mutable_state_native_with_deadline(&self.domain_id, execution, caller_deadline)
            .await
    }

    pub async fn poll_decision(
        &self,
        poll: n::api::PollForDecisionTaskRequest,
        expiration: Duration,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        self.poll_decision_inner(poll, expiration, None, None).await
    }
    pub async fn poll_decision_with_poller(
        &self,
        poll: n::api::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: NativePollLifecycle,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        self.poll_decision_inner(poll, expiration, Some(lifecycle), caller_deadline)
            .await
    }
    async fn poll_decision_inner(
        &self,
        poll: n::api::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: Option<NativePollLifecycle>,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        let history = self.history.clone();
        let caller_deadline =
            caller_deadline.unwrap_or_else(|| tokio::time::Instant::now() + expiration);
        self.poll_with_lifecycle(
            poll,
            expiration,
            caller_deadline,
            move |task, poll, domain: GoString| {
                let history = history.clone();
                async move {
                    history
                        .start_decision_native_with_deadline(
                            &domain,
                            &task,
                            poll,
                            Some(caller_deadline),
                        )
                        .await
                }
            },
            lifecycle,
        )
        .await
    }
    pub async fn poll_activity(
        &self,
        poll: n::api::PollForActivityTaskRequest,
        expiration: Duration,
    ) -> Result<n::matching::PollForActivityTaskResponse, Status> {
        self.poll_activity_inner(poll, expiration, None, None).await
    }
    pub async fn poll_activity_with_poller(
        &self,
        poll: n::api::PollForActivityTaskRequest,
        expiration: Duration,
        lifecycle: NativePollLifecycle,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForActivityTaskResponse, Status> {
        self.poll_activity_inner(poll, expiration, Some(lifecycle), caller_deadline)
            .await
    }
    async fn poll_activity_inner(
        &self,
        poll: n::api::PollForActivityTaskRequest,
        expiration: Duration,
        lifecycle: Option<NativePollLifecycle>,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<n::matching::PollForActivityTaskResponse, Status> {
        let history = self.history.clone();
        let caller_deadline =
            caller_deadline.unwrap_or_else(|| tokio::time::Instant::now() + expiration);
        self.poll_with_lifecycle(
            poll,
            expiration,
            caller_deadline,
            move |task, poll, domain: GoString| {
                let history = history.clone();
                async move {
                    history
                        .start_activity_native_with_deadline(
                            &domain,
                            &task,
                            poll,
                            Some(caller_deadline),
                        )
                        .await
                }
            },
            lifecycle,
        )
        .await
    }

    /// Go standby `getTask` -> `matcher.PollForQuery`: wait locally (the
    /// caller's native query channel is served meanwhile), then forward the poll
    /// to the parent. Never claims backlog. Forward error waits out the poll.
    pub async fn forward_query_poll(
        &self,
        poll: n::api::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: NativePollLifecycle,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        let released = self.blocked_pollers.notified();
        tokio::pin!(released);
        released.as_mut().enable();
        tokio::select! {
            biased;
            _ = &mut released => Ok(Default::default()),
            _ = tokio::time::sleep(expiration) => Ok(Default::default()),
            result = self.forward_query_poll_inner(poll, expiration, lifecycle) => result,
        }
    }
    async fn forward_query_poll_inner(
        &self,
        poll: n::api::PollForDecisionTaskRequest,
        expiration: Duration,
        lifecycle: NativePollLifecycle,
    ) -> Result<n::matching::PollForDecisionTaskResponse, Status> {
        let deadline = tokio::time::Instant::now() + expiration;
        if let Some(forward) = self.forward.as_ref() {
            let wait = Duration::from_nanos((forward.local_wait)().max(0) as u64);
            tokio::time::sleep(wait.min(expiration)).await;
            let permit = tokio::select! {
                permit = forward.permits.acquire() => permit,
                _ = tokio::time::sleep_until(deadline) => None,
            };
            if let (Some(_permit), Some((NativeParent::Service(service), parent_name))) =
                (permit, (forward.resolve)().await)
            {
                let target = ForwardPoll {
                    target: service,
                    domain_id: self.domain_id.clone(),
                    parent_name,
                    forwarded_from: forward.source.clone(),
                    poller_id: lifecycle.id.clone(),
                    isolation_group: lifecycle.info.isolation_group.clone(),
                    deadline,
                };
                let result = tokio::select! {
                    result = poll.forward(target) => Some(result),
                    _ = tokio::time::sleep_until(deadline) => None,
                };
                if let Some(Ok(response)) = result {
                    return Ok(response);
                }
            }
        }
        tokio::time::sleep_until(deadline).await;
        Ok(Default::default())
    }

    #[cfg(test)]
    async fn poll<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        mut deliver: F,
    ) -> Result<R, Status>
    where
        Q: NativeForwardPollRequest<R> + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTaskBytes, Q) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let caller_deadline = tokio::time::Instant::now() + expiration;
        self.poll_with_lifecycle(
            poll,
            expiration,
            caller_deadline,
            move |task, poll, _| deliver(task, poll),
            None,
        )
        .await
    }

    async fn poll_with_lifecycle<Q, R, F, Fut>(
        &self,
        poll: Q,
        expiration: Duration,
        caller_deadline: tokio::time::Instant,
        deliver: F,
        lifecycle: Option<NativePollLifecycle>,
    ) -> Result<R, Status>
    where
        Q: NativeForwardPollRequest<R> + 'static,
        R: Default + Send + 'static,
        F: FnMut(AllocatedTaskBytes, Q, GoString) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let released = self.blocked_pollers.notified();
        tokio::pin!(released);
        released.as_mut().enable();
        let (sender, mut receiver) = oneshot::channel();
        let observer = lifecycle.as_ref().and_then(|l| l.observer.clone());
        let blocked_poller = lifecycle
            .as_ref()
            .map(|poll| (poll.manager.clone(), poll.id.clone()));
        let worker = self.clone();
        let deadline = tokio::time::Instant::now() + expiration;
        let poll_started = tokio::time::Instant::now();
        let claimed = Arc::new(AtomicBool::new(false));
        let worker_claimed = claimed.clone();
        tokio::spawn(async move {
            worker
                .delivery_worker(
                    poll,
                    sender,
                    deliver,
                    lifecycle,
                    deadline,
                    poll_started,
                    Some(worker_claimed),
                )
                .await;
        });
        let response = {
            let response = tokio::time::timeout(expiration, &mut receiver);
            tokio::pin!(response);
            tokio::select! {
                biased;
                response = &mut response => response,
                _ = &mut released => {
                    if blocked_poller.as_ref().is_some_and(|(manager, id)| manager.lock().expect("poller history lock").contains_poll(id.as_bytes())) {
                        return Ok(R::default());
                    }
                    response.await
                },
            }
        };
        let response = match response {
            Err(_) if claimed.load(Ordering::SeqCst) => {
                tokio::time::timeout_at(caller_deadline, &mut receiver).await
            }
            response => response,
        };
        match response {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Status::unavailable("Matching delivery worker stopped.")),
            Err(_) => {
                if let Some(observer) = observer {
                    observer.poll_timeout();
                }
                Ok(R::default())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn delivery_worker<Q, R, F, Fut>(
        &self,
        poll: Q,
        sender: oneshot::Sender<Result<R, Status>>,
        mut deliver: F,
        lifecycle: Option<NativePollLifecycle>,
        deadline: tokio::time::Instant,
        poll_started: tokio::time::Instant,
        claimed: Option<Arc<AtomicBool>>,
    ) where
        Q: NativeForwardPollRequest<R>,
        R: Default + Send,
        F: FnMut(AllocatedTaskBytes, Q, GoString) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        if let (Some(reader), Some(lifecycle)) = (self.reader.as_ref(), lifecycle.as_ref()) {
            if reader.isolation_enabled()
                && !lifecycle.info.isolation_group.is_empty()
                && !reader.has_isolation_group(lifecycle.info.isolation_group.as_bytes())
            {
                if let Some(observer) = &lifecycle.observer {
                    observer.invalid_isolation_group();
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
            claimed,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn delivery_loop<Q, R, F, Fut>(
        &self,
        poll: Q,
        sender: &mut Option<oneshot::Sender<Result<R, Status>>>,
        deliver: &mut F,
        lifecycle: Option<NativePollLifecycle>,
        deadline: tokio::time::Instant,
        poll_started: tokio::time::Instant,
        claimed: Option<Arc<AtomicBool>>,
    ) where
        Q: NativeForwardPollRequest<R>,
        R: Default + Send,
        F: FnMut(AllocatedTaskBytes, Q, GoString) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let mut waiting = None;
        let mut waiting_started = false;
        let mut forward_failed = false;
        let group = lifecycle
            .as_ref()
            .map(|l| l.info.isolation_group.clone())
            .unwrap_or_default();
        loop {
            if sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                return;
            }
            if !waiting_started {
                if let Some(liveness) = &self.liveness {
                    liveness.mark_alive(now());
                }
                if let Some(claimed) = claimed.as_ref() {
                    claimed.store(false, Ordering::SeqCst);
                }
                waiting = lifecycle.as_ref().map(NativePollLifecycle::begin);
                waiting_started = true;
            }
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
                    tokio::select! {
                        biased;
                        _ = &mut closed => return,
                        _ = &mut released => Err(ClaimError::Released),
                        result = self.claim_delivery(&group) => result,
                        permit = async {
                            match &self.forward {
                                Some(forward) if !forward_failed => {
                                    let wait = Duration::from_nanos((forward.local_wait)().max(0) as u64);
                                    tokio::time::sleep(wait).await;
                                    forward.permits.acquire().await
                                },
                                _ => std::future::pending().await,
                            }
                        } => match permit {
                            Some(permit) => Err(ClaimError::Forward(permit)),
                            None => Err(ClaimError::Queue(NativeQueueError::Stopped)),
                        },
                    }
                } else {
                    self.claim_delivery(&GoString::default()).await
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
                Err(ClaimError::Released) | Err(ClaimError::Queue(NativeQueueError::Stopped)) => {
                    // Release and a closed pump are empty successful polls.
                    drop(waiting.take());
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
                        ClaimError::Forward(_) | ClaimError::Released => {
                            unreachable!("handled above")
                        }
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
                        _ = &mut closed => WaitResult::Closed,
                        _ = &mut released => WaitResult::Released,
                        _ = notification => WaitResult::Woken,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => WaitResult::Woken
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
            if let Some(claimed) = claimed.as_ref() {
                claimed.store(true, Ordering::SeqCst);
            }
            if let Some(observer) = lifecycle.as_ref().and_then(|l| l.observer.as_ref()) {
                let poll_forwarded = lifecycle.as_ref().is_some_and(|l| l.forwarded);
                observer.poll_success(task.sync.is_some());
                observer.poll_match_latency(poll_started.elapsed(), poll_forwarded, forward_failed);
                observer.poll_match(task.forwarded, poll_forwarded);
                if task.sync.is_none() {
                    observer.async_match_latency(Duration::from_nanos(
                        (now().saturating_sub(task.task.data.created_nanos)).max(0) as u64,
                    ));
                }
            }
            drop(waiting.take());
            waiting_started = false;
            if sender.as_ref().is_some_and(|sender| sender.is_closed()) {
                self.finish_delivery(task, Err(Status::cancelled("Poll cancelled before start.")))
                    .await;
                return;
            }
            let task_domain = task
                .sync
                .as_ref()
                .map(|sync| sync.source_domain.clone())
                .unwrap_or_else(|| self.domain_id.clone());
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
                    _ = &mut closed => {
                        self.finish_delivery(task, Err(Status::cancelled("Poll cancelled during start."))).await;
                        return;
                    },
                    result = deliver(task.task.clone(), poll.clone(), task_domain.clone()) => result,
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
                        if lifecycle.is_some() {
                            // Task type is not carried by the raw lifecycle; Go's
                            // message distinguishes decision from activity.
                            log_task_event(
                                "Duplicated task",
                                &task_domain,
                                &self.domain_id,
                                &GoString::default(),
                                &task.task,
                                Some(error.message()),
                            );
                        }
                        if let Some(observer) = lifecycle.as_ref().and_then(|l| l.observer.as_ref())
                        {
                            observer.poll_already_started();
                        }
                        self.finish_delivery(task, Ok(())).await
                    }
                    StartErrorKind::InactiveDomain => {
                        if lifecycle.is_some() {
                            log_task_event(
                                "Decision task dropped because domain is not active",
                                &task_domain,
                                &self.domain_id,
                                &GoString::default(),
                                &task.task,
                                None,
                            );
                        }
                        let caller_gone = async {
                            match sender.as_mut() {
                                Some(sender) => sender.closed().await,
                                None => std::future::pending().await,
                            }
                        };
                        if self
                            .history
                            .refresh_workflow_tasks_native_until(
                                &task_domain,
                                &task.task,
                                Some(deadline),
                                caller_gone,
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
        matching_blob::{BlobEncoding, TaskDataBytes},
        matching_native_task_store::TaskListKeyBytes,
        matching_sqlite::SqliteTaskStore,
    };
    use std::sync::atomic::AtomicUsize;
    use uuid::Uuid;

    fn gs(bytes: &[u8]) -> GoString {
        GoString::from(bytes)
    }
    fn fixture(
        workflows: &[&[u8]],
    ) -> (
        tempfile::TempDir,
        Arc<DurableTaskQueueBytes>,
        NativeTaskDispatcher,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("t.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let key = TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: gs(b"q\xff"),
            task_type: 0,
        };
        let queue = Arc::new(
            DurableTaskQueueBytes::open(store, key, 0, NonZeroU32::new(100).unwrap(), now())
                .unwrap(),
        );
        let tasks: Vec<TaskDataBytes> = workflows
            .iter()
            .map(|id| {
                TaskDataBytes {
                    workflow_id: gs(id),
                    schedule_id: 2,
                    created_nanos: now(),
                    expiry_nanos: cadence_persistence::matching_blob::GO_ZERO_TIME_NANOS,
                    ..Default::default()
                }
            })
            .collect();
        queue.append(&tasks, now()).unwrap();
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let dispatcher = NativeTaskDispatcher::new(
            queue.clone(),
            history,
            GoString::from(Uuid::from_u128(1).to_string()),
        );
        (directory, queue, dispatcher)
    }
    fn pending(queue: &DurableTaskQueueBytes) -> usize {
        queue
            .read_batch(100000, NonZeroU32::new(10).unwrap(), now())
            .unwrap()
            .tasks
            .len()
    }

    #[test]
    fn native_queue_status_maps_throttling_to_service_busy() {
        let busy = NativeQueueError::Storage(MatchingTaskStoreBytesError::Backend {
            kind: BackendErrorKind::Throttled,
            message: "too many".into(),
        });
        assert_eq!(queue_status(busy).code(), tonic::Code::ResourceExhausted);
        let timeout = NativeQueueError::Storage(MatchingTaskStoreBytesError::Backend {
            kind: BackendErrorKind::Timeout,
            message: "slow".into(),
        });
        assert_eq!(queue_status(timeout).code(), tonic::Code::Internal);
        assert_eq!(
            queue_status(NativeQueueError::Stopped).code(),
            tonic::Code::Internal
        );
    }

    #[test]
    fn native_forward_poll_rebuilds_parent_request_with_raw_bytes() {
        let request = n::api::PollForActivityTaskRequest {
            domain: gs(b"d\xff"),
            identity: gs(b"w\xfe"),
            task_list: Some(n::api::TaskList {
                name: gs(b"/__cadence_sys/q\xff/1"),
                kind: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
        .for_parent(&gs(b"q\xff"));
        assert_eq!(request.task_list.unwrap().name.as_bytes(), b"q\xff");
        assert_eq!(request.identity.as_bytes(), b"w\xfe");
        assert!(request.domain.is_empty());
    }

    #[tokio::test]
    async fn native_poll_delivers_non_utf8_workflow_id_and_acknowledges() {
        let (_directory, queue, dispatcher) = fixture(&[b"\xff\xfewf"]);
        let seen: Result<Vec<u8>, Status> = dispatcher
            .poll((), Duration::from_secs(2), |task, ()| async move {
                Ok(task.data.workflow_id.into_bytes())
            })
            .await;
        assert_eq!(seen.unwrap(), b"\xff\xfewf");
        assert_eq!(pending(&queue), 0);
    }

    #[tokio::test]
    async fn native_retryable_start_error_rewrites_then_redelivers_raw_bytes() {
        let (_directory, queue, dispatcher) = fixture(&[b"a\xffb"]);
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let result: Result<Vec<u8>, Status> = dispatcher
            .poll((), Duration::from_secs(3), move |task, ()| {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(Status::unavailable("history busy"))
                    } else {
                        Ok(task.data.workflow_id.into_bytes())
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), b"a\xffb");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(pending(&queue), 0);
    }

    #[tokio::test]
    async fn native_dropped_poll_after_claim_rewrites_task_for_next_poller() {
        let (_directory, queue, dispatcher) = fixture(&[b"keep\xff"]);
        let (started, entered) = oneshot::channel::<()>();
        let started = Arc::new(Mutex::new(Some(started)));
        let first = {
            let dispatcher = dispatcher.clone();
            tokio::spawn(async move {
                let _: Result<Vec<u8>, Status> = dispatcher
                    .poll((), Duration::from_secs(5), move |_task, ()| {
                        let started = started.clone();
                        async move {
                            if let Some(started) = started.lock().unwrap().take() {
                                let _ = started.send(());
                            }
                            std::future::pending::<Result<Vec<u8>, Status>>().await
                        }
                    })
                    .await;
            })
        };
        entered.await.unwrap();
        first.abort();
        let _ = first.await;
        let again: Result<Vec<u8>, Status> = dispatcher
            .poll((), Duration::from_secs(3), |task, ()| async move {
                Ok(task.data.workflow_id.into_bytes())
            })
            .await;
        assert_eq!(again.unwrap(), b"keep\xff");
        assert_eq!(pending(&queue), 0);
    }

    #[tokio::test]
    async fn native_poll_lifecycle_registers_and_releases_raw_poller_id() {
        let manager = Arc::new(NativePollerManager::default());
        let lifecycle = NativePollLifecycle {
            manager: manager.clone(),
            id: gs(b"p\xff"),
            cancel: None,
            info: cadence_common::poller_bytes::Info {
                identity: gs(b"id"),
                isolation_group: GoString::default(),
                rate_per_second: 42.0,
            },
            forwarded: false,
            observer: None,
        };
        let guard = lifecycle.begin();
        assert!(manager.lock().unwrap().contains_poll(b"p\xff"));
        assert!(!manager.lock().unwrap().contains_poll(b"p\xef\xbf\xbd"));
        drop(guard);
        assert!(!manager.lock().unwrap().contains_poll(b"p\xff"));
        assert_eq!(manager.lock().unwrap().list(now())[0].rate_per_second, 42.0);
    }

    #[tokio::test]
    async fn native_accept_forwarded_backlog_rejects_non_uuid_source_domain() {
        let (_directory, _queue, dispatcher) = fixture(&[]);
        let task = AllocatedTaskBytes {
            task_id: 1,
            data: TaskDataBytes::default(),
        };
        let error = dispatcher
            .accept_forwarded_backlog(task, &gs(b"\xff\xfe"), &gs(b"q"), 0)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn native_release_blocked_pollers_returns_empty_poll() {
        let (_directory, _queue, dispatcher) = fixture(&[]);
        let releaser = dispatcher.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            releaser.release_blocked_pollers();
        });
        let result: Result<Vec<u8>, Status> = dispatcher
            .poll((), Duration::from_secs(5), |_task, ()| async move {
                Ok(Vec::new())
            })
            .await;
        assert!(result.unwrap().is_empty());
    }

    #[derive(Default)]
    struct ScriptedApi {
        decisions: Mutex<Vec<n::matching::AddDecisionTaskRequest>>,
        activities: Mutex<Vec<n::matching::AddActivityTaskRequest>>,
        fail: bool,
    }
    impl NativeMatchingApi for ScriptedApi {
        fn poll_for_decision_task(
            &self,
            _r: n::matching::PollForDecisionTaskRequest,
            _d: tokio::time::Instant,
        ) -> NativeFuture<Result<n::matching::PollForDecisionTaskResponse, Status>> {
            Box::pin(async { Err(Status::unimplemented("scripted")) })
        }
        fn poll_for_activity_task(
            &self,
            _r: n::matching::PollForActivityTaskRequest,
            _d: tokio::time::Instant,
        ) -> NativeFuture<Result<n::matching::PollForActivityTaskResponse, Status>> {
            Box::pin(async { Err(Status::unimplemented("scripted")) })
        }
        fn add_decision_task(
            &self,
            r: n::matching::AddDecisionTaskRequest,
            _d: tokio::time::Instant,
        ) -> NativeFuture<Result<(), Status>> {
            self.decisions.lock().unwrap().push(r);
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    Err(Status::unavailable("parent down"))
                } else {
                    Ok(())
                }
            })
        }
        fn add_activity_task(
            &self,
            r: n::matching::AddActivityTaskRequest,
            _d: tokio::time::Instant,
        ) -> NativeFuture<Result<(), Status>> {
            self.activities.lock().unwrap().push(r);
            Box::pin(async { Ok(()) })
        }
    }

    fn forward_state(api: Arc<ScriptedApi>, source: &[u8], parent: &'static [u8]) -> NativeForwardState {
        NativeForwardState::new(
            move || {
                let api = api.clone();
                Box::pin(async move { Some((NativeParent::Service(api), gs(parent))) })
            },
            Arc::new(|| 10),
            Arc::new(crate::matching_forward::RequestTokens::new(Arc::new(|| 10))),
            Arc::new(crate::matching_forward::ForwardRate::new(
                Arc::new(|| 100),
                now(),
            )),
            gs(source),
            Arc::new(|| 0),
        )
    }
    const RAW_RUNS: [&[u8]; 3] = [b"\xff\xfe", b"a\x00b\xff", b"\"q\\\xfe"];
    fn raw_task(run: &[u8]) -> AllocatedTaskBytes {
        AllocatedTaskBytes {
            task_id: 7,
            data: TaskDataBytes {
                workflow_id: gs(b"wf\xff"),
                raw_run_id: Some(gs(run)),
                schedule_id: 9,
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn native_forward_backlog_keeps_raw_run_id_exact_for_decision_and_activity() {
        let source = b"/__cadence_sys/q\xff/1";
        for run in RAW_RUNS {
            let api = Arc::new(ScriptedApi::default());
            let state = forward_state(api.clone(), source, b"q\xff");
            state
                .forward_backlog_task(raw_task(run), &gs(b"dom"), &gs(b"src"), 0, 0)
                .await
                .unwrap();
            state
                .forward_backlog_task(raw_task(run), &gs(b"dom"), &gs(b"src"), 1, 0)
                .await
                .unwrap();
            let d = api.decisions.lock().unwrap();
            let a = api.activities.lock().unwrap();
            for (exec, list, from) in [
                (&d[0].workflow_execution, &d[0].task_list, &d[0].forwarded_from),
                (&a[0].workflow_execution, &a[0].task_list, &a[0].forwarded_from),
            ] {
                let exec = exec.as_ref().unwrap();
                assert_eq!(exec.run_id.as_bytes(), run, "RunID must not be quoted");
                assert_eq!(exec.workflow_id.as_bytes(), b"wf\xff");
                assert_eq!(list.as_ref().unwrap().name.as_bytes(), b"q\xff");
                assert_eq!(from.as_bytes(), source);
            }
            assert_eq!(d[0].domain_id.as_bytes(), b"src");
            assert_eq!(a[0].domain_id.as_bytes(), b"dom");
            assert_eq!(a[0].source_domain_id.as_bytes(), b"src");
            assert_eq!(d[0].schedule_id, 9);
        }
    }

    #[tokio::test]
    async fn native_forward_backlog_rejects_root_and_maps_parent_failure() {
        let api = Arc::new(ScriptedApi::default());
        let root = forward_state(api.clone(), b"q\xff", b"q\xff");
        assert!(matches!(
            root.forward_backlog_task(raw_task(b"r"), &gs(b"d"), &gs(b"d"), 0, 0)
                .await,
            Err(crate::matching_forward::ForwardError::NoParent)
        ));
        assert!(api.decisions.lock().unwrap().is_empty());
        let failing = Arc::new(ScriptedApi {
            fail: true,
            ..Default::default()
        });
        let state = forward_state(failing.clone(), b"/__cadence_sys/q\xff/1", b"q\xff");
        assert!(state
            .forward_backlog_task(raw_task(b"r"), &gs(b"d"), &gs(b"d"), 0, 0)
            .await
            .is_err());
        assert_eq!(failing.decisions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn native_sync_matcher_acknowledges_outcome_to_offerer_and_isolates_groups() {
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        let matcher = Arc::new(NativeSyncMatcher::new(&[gs(b"g\xff")], receiver));
        let poller = {
            let matcher = matcher.clone();
            tokio::spawn(async move { matcher.poll(&gs(b"g\xff")).await })
        };
        let offered = matcher
            .offer(
                raw_task(b"\xff\xfe"),
                &gs(b"g\xff"),
                &gs(b"src"),
                false,
                Duration::from_millis(500),
                tokio::time::Instant::now() + Duration::from_secs(2),
            );
        let (accepted, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(offered, async {
                let task = poller.await.unwrap().expect("poller gets task");
                assert_eq!(task.task.data.run_id_bytes(), b"\xff\xfe");
                task.finish(Ok(()));
            })
        }).await.expect("sync matcher handoff timed out");
        assert!(accepted);
        // A task bound to another group is not handed to a g\xff poller.
        let refused = matcher
            .offer(
                raw_task(b"r"),
                &gs(b"other"),
                &gs(b"src"),
                false,
                Duration::ZERO,
                tokio::time::Instant::now() + Duration::from_millis(200),
            )
            .await;
        assert!(!refused);
    }
}
