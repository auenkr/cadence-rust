//! Byte-preserving port of `matching_reader.rs`: the same backlog pump,
//! dispatch loop, rate limiting, isolation selection, forwarding and ack timer,
//! carried over `DurableTaskQueueBytes`/`AllocatedTaskBytes`/`TaskDataBytes`.
//! Isolation groups are `GoString` keys, so invalid-UTF-8 groups never collide.
//! Lossy text exists only in bounded diagnostics and metric tags.
use crate::matching_limiter::{TaskListLimiter, WaitOutcome};
pub use crate::matching_reader::{
    ReaderConfig, ReaderFatalSink, ReaderLive, DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS,
    DEFAULT_READ_RANGE_SIZE, DEFAULT_TASKS_BATCH_SIZE, DEFAULT_UPDATE_ACK_INTERVAL_NANOS,
    DISPATCH_TIMEOUT_BUFFER_NANOS,
};
use cadence_common::go_string::GoString;
use cadence_persistence::{
    matching_blob::TaskDataBytes,
    matching_native_queue::{DurableTaskQueueBytes, NativeQueueError},
    matching_native_task_store::AllocatedTaskBytes,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch, Notify};

/// Longest byte prefix of any raw identifier rendered into logs or tags.
const DIAGNOSTIC_LIMIT: usize = 256;
/// Diagnostic/metric text only. Never use for selection, routing or keys.
fn diag(bytes: &[u8]) -> String {
    let mut text =
        String::from_utf8_lossy(&bytes[..bytes.len().min(DIAGNOSTIC_LIMIT)]).into_owned();
    if bytes.len() > DIAGNOSTIC_LIMIT {
        text.push_str("...");
    }
    text
}

const ISOLATION_GROUP_KEY: &[u8] = b"isolation-group";
const ORIGINAL_ISOLATION_GROUP_KEY: &[u8] = b"original-isolation-group";
const WORKFLOW_ID_KEY: &[u8] = b"wf-id";

/// Byte-keyed twin of `cadence_common::task_isolation::dispatch_partition_config`.
fn dispatch_partition_config(
    config: &Option<BTreeMap<GoString, GoString>>,
    group: &GoString,
) -> Option<BTreeMap<GoString, GoString>> {
    let map = config.as_ref()?;
    let Some(original) = map.get(ISOLATION_GROUP_KEY) else {
        return config.clone();
    };
    Some(BTreeMap::from([
        (GoString::from(ISOLATION_GROUP_KEY), group.clone()),
        (
            GoString::from(ORIGINAL_ISOLATION_GROUP_KEY),
            map.get(ORIGINAL_ISOLATION_GROUP_KEY)
                .unwrap_or(original)
                .clone(),
        ),
        (
            GoString::from(WORKFLOW_ID_KEY),
            map.get(WORKFLOW_ID_KEY).cloned().unwrap_or_default(),
        ),
    ]))
}

fn adaptive_reader_trace(task_list: &str, event: &str, fields: std::fmt::Arguments<'_>) {
    if std::env::var("CADENCE_MATCHING_ADAPTIVE_TRACE").as_deref()
        == Ok("production-adaptive-scaler-probe")
        && cadence_common::task_list::TaskListName::parse(task_list)
            .is_ok_and(|parsed| parsed.root() == "production-adaptive-scaler-probe")
    {
        eprintln!(
            "matching_adaptive_trace ts={} task_list={task_list} boundary={event} {fields}",
            crate::grpc_matching::matching_now()
        );
    }
}

fn reader_partition_parts(name: &str) -> (&str, &str) {
    match name
        .strip_prefix("/__cadence_sys/")
        .and_then(|name| name.rsplit_once('/'))
    {
        Some((root, leaf)) if leaf.parse::<u32>().is_ok() => (root, "child"),
        _ => (name, "root"),
    }
}

// `ReaderConfig`'s accessors are private to matching_reader; these mirror them
// from its public fields and the public `raw_batch_size`.
fn buffer_batch_size(config: &ReaderConfig) -> u32 {
    let value = config.raw_batch_size();
    if value <= 0 {
        DEFAULT_TASKS_BATCH_SIZE as u32
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}
fn read_batch_size(config: &ReaderConfig) -> NonZeroU32 {
    NonZeroU32::new(buffer_batch_size(config)).expect("batch size falls back to a positive default")
}
fn read_range_size(config: &ReaderConfig) -> u32 {
    match &config.live {
        Some(live) => u32::try_from((live.read_range_size)()).unwrap_or(0),
        None => config.read_range_size,
    }
}
fn dispatch_timeout_nanos(config: &ReaderConfig) -> i64 {
    match &config.live {
        Some(live) => (live.dispatch_timeout)(),
        None => config.dispatch_timeout.as_nanos() as i64,
    }
}
fn update_ack_interval(config: &ReaderConfig) -> Duration {
    match &config.live {
        Some(live) => Duration::from_nanos((live.update_ack_interval)().max(1) as u64),
        None => Duration::from_nanos(DEFAULT_UPDATE_ACK_INTERVAL_NANOS as u64),
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

struct Pending<T> {
    item: Mutex<Option<T>>,
    accepted: Notify,
}
struct Rendezvous<T> {
    offers: Mutex<VecDeque<Arc<Pending<T>>>>,
    available: Notify,
}
impl<T> Default for Rendezvous<T> {
    fn default() -> Self {
        Self {
            offers: Mutex::new(VecDeque::new()),
            available: Notify::new(),
        }
    }
}
struct OfferGuard<'a, T> {
    channel: &'a Rendezvous<T>,
    pending: Arc<Pending<T>>,
}
impl<T> Drop for OfferGuard<'_, T> {
    fn drop(&mut self) {
        self.channel
            .offers
            .lock()
            .expect("Matching handoff lock")
            .retain(|offer| !Arc::ptr_eq(offer, &self.pending));
    }
}
impl<T> Rendezvous<T> {
    async fn offer(&self, item: T, mut stop: watch::Receiver<bool>) -> bool {
        if *stop.borrow() {
            return false;
        }
        let pending = Arc::new(Pending {
            item: Mutex::new(Some(item)),
            accepted: Notify::new(),
        });
        let _guard = OfferGuard {
            channel: self,
            pending: pending.clone(),
        };
        self.offers
            .lock()
            .expect("Matching handoff lock")
            .push_back(pending.clone());
        self.available.notify_one();
        loop {
            let accepted = pending.accepted.notified();
            tokio::pin!(accepted);
            accepted.as_mut().enable();
            if pending
                .item
                .lock()
                .expect("Matching offered task lock")
                .is_none()
            {
                return true;
            }
            tokio::select! {
                _ = accepted => (),
                _ = stop.changed() => return false,
            }
        }
    }
    async fn offer_until(
        &self,
        item: T,
        mut stop: watch::Receiver<bool>,
        deadline: tokio::time::Instant,
    ) -> Option<bool> {
        if *stop.borrow() {
            return None;
        }
        let pending = Arc::new(Pending {
            item: Mutex::new(Some(item)),
            accepted: Notify::new(),
        });
        let _guard = OfferGuard {
            channel: self,
            pending: pending.clone(),
        };
        self.offers
            .lock()
            .expect("Matching handoff lock")
            .push_back(pending.clone());
        self.available.notify_one();
        tokio::select! {
            _ = pending.accepted.notified() => return Some(true),
            _ = tokio::time::sleep_until(deadline) => (),
            _ = stop.changed() => return None,
        }
        let mut offers = self.offers.lock().expect("Matching handoff lock");
        if pending
            .item
            .lock()
            .expect("Matching offered task lock")
            .is_none()
        {
            return Some(true);
        }
        offers.retain(|offer| !Arc::ptr_eq(offer, &pending));
        Some(false)
    }
    // Select the local rendezvous and forward token atomically with respect
    // to pollers. Dropping a raced offer future alone could forward a task
    // that another runtime thread has already claimed locally.
    async fn offer_or_token(
        &self,
        item: T,
        mut stop: watch::Receiver<bool>,
        tokens: &crate::matching_forward::RequestTokens,
    ) -> Option<Result<tokio::sync::OwnedSemaphorePermit, ()>> {
        if *stop.borrow() {
            return None;
        }
        let pending = Arc::new(Pending {
            item: Mutex::new(Some(item)),
            accepted: Notify::new(),
        });
        let _guard = OfferGuard {
            channel: self,
            pending: pending.clone(),
        };
        self.offers
            .lock()
            .expect("Matching handoff lock")
            .push_back(pending.clone());
        self.available.notify_one();
        let acquired = tokio::select! {
            _ = pending.accepted.notified() => return Some(Err(())),
            token = tokens.acquire() => token?,
            _ = stop.changed() => return None,
        };
        let mut offers = self.offers.lock().expect("Matching handoff lock");
        if pending
            .item
            .lock()
            .expect("Matching offered task lock")
            .is_none()
        {
            return Some(Err(()));
        }
        offers.retain(|offer| !Arc::ptr_eq(offer, &pending));
        Some(Ok(acquired))
    }
    async fn poll(&self, mut stop: watch::Receiver<bool>) -> Option<T> {
        loop {
            if *stop.borrow() {
                return None;
            }
            let available = self.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            // Ownership moves synchronously during a poll of this future. A
            // cancelled waiting future never owns a task hidden in a oneshot.
            if let Some(pending) = self
                .offers
                .lock()
                .expect("Matching handoff lock")
                .pop_front()
            {
                let item = pending
                    .item
                    .lock()
                    .expect("Matching offered task lock")
                    .take();
                pending.accepted.notify_one();
                return item;
            }
            tokio::select! {
                _ = available => (),
                _ = stop.changed() => return None,
            }
        }
    }
}
#[derive(Default)]
struct Completion {
    count: Mutex<usize>,
    changed: Notify,
}
struct Completed(Arc<Completion>);
impl Drop for Completed {
    fn drop(&mut self) {
        *self
            .0
            .count
            .lock()
            .expect("Matching reader completion lock") += 1;
        self.0.changed.notify_waiters();
    }
}
struct Inner {
    matcher: Arc<Rendezvous<AllocatedTaskBytes>>,
    isolated: HashMap<GoString, Arc<Rendezvous<AllocatedTaskBytes>>>,
    stop: watch::Sender<bool>,
    completion: Arc<Completion>,
    loops: usize,
    should_reload: Arc<dyn Fn() -> bool + Send + Sync>,
    metric_context: Arc<Mutex<Option<ReaderMetricContext>>>,
    backlog_forward: Arc<Mutex<Option<NativeBacklogForward>>>,
}
pub type NativeBacklogForwardResult = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), crate::matching_forward::ForwardError>> + Send>,
>;
pub type NativeBacklogForwardCall =
    Arc<dyn Fn(AllocatedTaskBytes, tokio::time::Instant) -> NativeBacklogForwardResult + Send + Sync>;
#[derive(Clone)]
pub struct NativeBacklogForward {
    /// Go `fwdrAddReqTokenC`: a token is taken before each forward attempt.
    pub tokens: Arc<crate::matching_forward::RequestTokens>,
    pub forward: NativeBacklogForwardCall,
    pub local_wait: Arc<dyn Fn() -> Duration + Send + Sync>,
}
#[derive(Clone)]
struct ReaderMetricContext {
    metrics: crate::matching_metrics::MatchingMetrics,
    context: crate::matching_metrics::PollMetricContext,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
/// Native backlog reader; usable by a future registry in place of `TaskReader`.
#[derive(Clone)]
pub struct NativeTaskReader {
    inner: Arc<Inner>,
}
/// Outcome of one dispatch attempt for the head task.
#[derive(Debug)]
enum Attempt {
    Offered,
    Forwarded(u64),
    OfferedAfterForwardFailure(u64),
    OfferedAfterForwardRetry(u64),
    Throttled,
    TimedOut,
    Stopped,
}
enum BufferSender {
    Buffered(mpsc::Sender<AllocatedTaskBytes>),
    Unbuffered(Arc<Rendezvous<AllocatedTaskBytes>>),
}
enum BufferReceiver {
    Buffered(mpsc::Receiver<AllocatedTaskBytes>),
    Unbuffered(Arc<Rendezvous<AllocatedTaskBytes>>),
}
/// Selector result: raw group bytes, isolation duration nanos, leak cause.
pub type NativeIsolationSelector =
    Arc<dyn Fn(&TaskDataBytes) -> (GoString, i64, Option<&'static str>) + Send + Sync>;
/// Groups are snapshotted at manager creation, as in Go. Selection is
/// reevaluated before buffering and on every dispatch attempt.
#[derive(Clone)]
pub struct NativeIsolationConfig {
    pub groups: Vec<GoString>,
    pub select: NativeIsolationSelector,
    pub should_reload: Arc<dyn Fn() -> bool + Send + Sync>,
}
impl Default for NativeIsolationConfig {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            select: Arc::new(|_| (GoString::default(), 0, None)),
            should_reload: Arc::new(|| false),
        }
    }
}
pub type NativeDispatchCheck = Arc<
    dyn Fn(
            AllocatedTaskBytes,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bool, tonic::Status>> + Send>,
        > + Send
        + Sync,
>;

fn original_isolation_group(data: &TaskDataBytes) -> &[u8] {
    data.partition_config
        .as_ref()
        .and_then(|config| config.get(ISOLATION_GROUP_KEY))
        .map(GoString::as_bytes)
        .unwrap_or_default()
}

impl NativeTaskReader {
    pub fn set_backlog_forwarder(&self, forward: NativeBacklogForward) {
        *self
            .inner
            .backlog_forward
            .lock()
            .expect("matching reader forwarder lock") = Some(forward);
    }
    pub fn with_metric_context(
        &self,
        metrics: crate::matching_metrics::MatchingMetrics,
        context: crate::matching_metrics::PollMetricContext,
    ) {
        *self
            .inner
            .metric_context
            .lock()
            .expect("matching reader metrics lock") =
            Some(ReaderMetricContext { metrics, context });
    }
    pub fn new(queue: Arc<DurableTaskQueueBytes>, wake: Arc<Notify>, config: ReaderConfig) -> Self {
        Self::with_fatal_invariant_sink(
            queue,
            wake,
            config,
            Arc::new(|| true),
            Arc::new(TaskListLimiter::with_defaults()),
            NativeIsolationConfig::default(),
            None,
            None,
            None,
            None,
        )
    }
    /// The fatal sink is a constructor argument, not a setter: the pump task is
    /// spawned below and may admit a duplicate row immediately. The sink runs
    /// before the pump's completion guard drops, so `wait_stopped` cannot
    /// return before the host-fatal callback finished.
    #[allow(clippy::too_many_arguments)]
    pub fn with_fatal_invariant_sink(
        queue: Arc<DurableTaskQueueBytes>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
        isolation: NativeIsolationConfig,
        dispatch_check: Option<NativeDispatchCheck>,
        metric_context: Option<(
            crate::matching_metrics::MatchingMetrics,
            crate::matching_metrics::PollMetricContext,
        )>,
        backlog_forward: Option<NativeBacklogForward>,
        fatal_invariant: Option<ReaderFatalSink>,
    ) -> Self {
        Self::with_start_state(
            queue,
            wake,
            config,
            active,
            limiter,
            isolation,
            dispatch_check,
            metric_context,
            backlog_forward,
            fatal_invariant,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_start_state(
        queue: Arc<DurableTaskQueueBytes>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
        isolation: NativeIsolationConfig,
        dispatch_check: Option<NativeDispatchCheck>,
        metric_context: Option<(
            crate::matching_metrics::MatchingMetrics,
            crate::matching_metrics::PollMetricContext,
        )>,
        backlog_forward: Option<NativeBacklogForward>,
        fatal_invariant: Option<ReaderFatalSink>,
        start: bool,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let matcher = Arc::new(Rendezvous::default());
        let isolated: HashMap<GoString, _> = isolation
            .groups
            .iter()
            .filter(|g| !g.is_empty())
            .map(|g| (g.clone(), Arc::new(Rendezvous::default())))
            .collect();
        let buffers = isolated.len() + 1;
        let completion = Arc::new(Completion::default());
        let inner = Arc::new(Inner {
            matcher: matcher.clone(),
            isolated: isolated.clone(),
            stop,
            completion: completion.clone(),
            loops: buffers + 1,
            should_reload: isolation.should_reload.clone(),
            metric_context: Arc::new(Mutex::new(
                metric_context.map(|(metrics, context)| ReaderMetricContext { metrics, context }),
            )),
            backlog_forward: Arc::new(Mutex::new(backlog_forward)),
        });
        let mut buffer_senders: HashMap<GoString, BufferSender> = HashMap::new();
        for buffer_group in std::iter::once(GoString::default()).chain(isolated.keys().cloned()) {
            let (buffer_send, mut buffer_receive) = if buffer_batch_size(&config) == 1 {
                let buffer = Arc::new(Rendezvous::default());
                (
                    BufferSender::Unbuffered(buffer.clone()),
                    BufferReceiver::Unbuffered(buffer),
                )
            } else {
                let (sender, receiver) = mpsc::channel(buffer_batch_size(&config) as usize - 1);
                (
                    BufferSender::Buffered(sender),
                    BufferReceiver::Buffered(receiver),
                )
            };
            buffer_senders.insert(buffer_group, buffer_send);
            let dispatched = Completed(completion.clone());
            let mut dispatch_stop = stopped.clone();
            let dispatch_queue = queue.clone();
            let dispatch_limiter = limiter.clone();
            let active = active.clone();
            let dispatch_check = dispatch_check.clone();
            let matcher = matcher.clone();
            let isolated = isolated.clone();
            let isolation = isolation.clone();
            let timeout_buffer = config.dispatch_timeout_buffer;
            let dispatch_config = config.clone();
            let dispatch_metrics = inner.metric_context.clone();
            let backlog_forward = inner.backlog_forward.clone();
            tokio::spawn(async move {
                let _completed = dispatched;
                if !start {
                    let _ = dispatch_stop.changed().await;
                    return;
                }
                'dispatch: loop {
                    if *dispatch_stop.borrow() {
                        break;
                    }
                    let task = match &mut buffer_receive {
                        BufferReceiver::Buffered(receiver) => tokio::select! {
                            task = receiver.recv() => task,
                            _ = dispatch_stop.changed() => None,
                        },
                        BufferReceiver::Unbuffered(buffer) => {
                            buffer.poll(dispatch_stop.clone()).await
                        }
                    };
                    let Some(task) = task else { break };
                    let dispatch_started = std::time::Instant::now();
                    let trace_list = dispatch_metrics
                        .lock()
                        .expect("matching reader metrics lock")
                        .as_ref()
                        .map(|metrics| metrics.context.task_list.clone())
                        .unwrap_or_default();
                    let (trace_root, trace_partition) = reader_partition_parts(&trace_list);
                    let trace_workflow = diag(task.data.workflow_id.as_bytes());
                    let trace_run = diag(&task.data.run_id_bytes());
                    adaptive_reader_trace(&trace_list, "reader_head_dispatch_begin", format_args!(
                        "root_partition={} partition={} workflow_id={} run_id={} schedule_id={} task_id={}",
                        trace_root, trace_partition, trace_workflow, trace_run,
                        task.data.schedule_id, task.task_id,
                    ));
                    let mut dispatch_attempt = 0u64;
                    loop {
                        // Go rechecks expiry at each dispatch attempt.
                        if task.data.expiry_nanos > 0 && now() > task.data.expiry_nanos {
                            if let Some(reader_metrics) = dispatch_metrics
                                .lock()
                                .expect("matching reader metrics lock")
                                .as_ref()
                            {
                                reader_metrics.metrics.expired_task(&reader_metrics.context);
                            }
                            let queue = dispatch_queue.clone();
                            let id = task.task_id;
                            let result =
                                crate::matching_blocking::run(move || queue.complete(id)).await;
                            if !matches!(result, Ok(Ok(_))) {
                                dispatch_queue.stop();
                                break 'dispatch;
                            }
                            break;
                        }
                        if let Some(check) = &dispatch_check {
                            let result = tokio::select! {
                                result = check(task.clone()) => result,
                                _ = dispatch_stop.changed() => break 'dispatch,
                            };
                            match result {
                                Ok(true) => {
                                    let queue = dispatch_queue.clone();
                                    let id = task.task_id;
                                    if !matches!(
                                        crate::matching_blocking::run(move || {
                                            queue.complete(id)?;
                                            queue.run_garbage_collection(false).map(|_| ())
                                        })
                                        .await,
                                        Ok(Ok(_))
                                    ) {
                                        dispatch_queue.stop();
                                        break 'dispatch;
                                    }
                                    break;
                                }
                                Ok(false) => (),
                                Err(error) => {
                                    // Go taskReader counts and logs any completer error
                                    // except the retryable "task not started".
                                    if error.message() != "task not started" {
                                        if let Some(reader_metrics) = dispatch_metrics
                                            .lock()
                                            .expect("matching reader metrics lock")
                                            .as_ref()
                                        {
                                            reader_metrics.metrics.task_list_counter_event(
                                                crate::matching_metrics::BUFFER_UNKNOWN_DISPATCH_ERROR,
                                                &reader_metrics.context,
                                            );
                                        }
                                        eprintln!("Matching unknown buffered task dispatch error: {error}");
                                    }
                                    tokio::task::yield_now().await;
                                    continue;
                                }
                            }
                        }
                        let (group, mut isolation_duration, leak_cause) =
                            (isolation.select)(&task.data);
                        if let (Some(cause), Some(reader_metrics)) = (
                            leak_cause,
                            dispatch_metrics
                                .lock()
                                .expect("matching reader metrics lock")
                                .as_ref(),
                        ) {
                            reader_metrics.metrics.task_isolation_leak(
                                &reader_metrics.context,
                                &GoString::from(original_isolation_group(&task.data))
                                    .go_rune_string(),
                                cause,
                            );
                        }
                        let target = isolated.get(group.as_bytes()).unwrap_or_else(|| {
                            isolation_duration = 0;
                            &matcher
                        });
                        let group_known = isolated.contains_key(group.as_bytes());
                        let rps = dispatch_limiter.limit();
                        let async_timeout = dispatch_timeout_nanos(&dispatch_config);
                        let deadline = ((group_known || rps > 1e-7) && active()).then(|| {
                            dispatch_limiter.now().saturating_add(
                                cadence_common::task_isolation::dispatch_timeout(
                                    rps,
                                    buffers,
                                    async_timeout,
                                    isolation_duration,
                                    timeout_buffer.as_nanos() as i64,
                                ),
                            )
                        });
                        let outer_deadline = deadline.map(|at| {
                            let remaining = Duration::from_nanos(
                                at.saturating_sub(dispatch_limiter.now()).max(0) as u64,
                            );
                            tokio::time::Instant::now() + remaining
                        });
                        let attempt_started = std::time::Instant::now();
                        let attempt = async {
                            match dispatch_limiter
                                .wait_until(dispatch_stop.clone(), deadline)
                                .await
                            {
                                WaitOutcome::Acquired => (),
                                WaitOutcome::Stopped => return Attempt::Stopped,
                                WaitOutcome::Throttled => return Attempt::Throttled,
                            }
                            let mut dispatched = task.clone();
                            let dispatch_group = if group_known {
                                group.clone()
                            } else {
                                GoString::default()
                            };
                            dispatched.data.partition_config = dispatch_partition_config(
                                &task.data.partition_config,
                                &dispatch_group,
                            );
                            let forwarder = backlog_forward
                                .lock()
                                .expect("matching reader forwarder lock")
                                .clone();
                            let Some(forwarder) = forwarder else {
                                return if target.offer(dispatched, dispatch_stop.clone()).await {
                                    Attempt::Offered
                                } else {
                                    Attempt::Stopped
                                };
                            };
                            let first_deadline =
                                tokio::time::Instant::now() + (forwarder.local_wait)();
                            match target
                                .offer_until(
                                    dispatched.clone(),
                                    dispatch_stop.clone(),
                                    first_deadline,
                                )
                                .await
                            {
                                Some(true) => return Attempt::Offered,
                                None => return Attempt::Stopped,
                                Some(false) => (),
                            }
                            let mut attempt = 0u64;
                            loop {
                                if *dispatch_stop.borrow() {
                                    return Attempt::Stopped;
                                }
                                let token = match target
                                    .offer_or_token(
                                        dispatched.clone(),
                                        dispatch_stop.clone(),
                                        &forwarder.tokens,
                                    )
                                    .await
                                {
                                    Some(Ok(token)) => token,
                                    Some(Err(())) => {
                                        return Attempt::OfferedAfterForwardRetry(attempt)
                                    }
                                    None => return Attempt::Stopped,
                                };
                                let forward_deadline = outer_deadline.map_or_else(
                                    || tokio::time::Instant::now() + Duration::from_secs(2),
                                    |parent| {
                                        parent.min(tokio::time::Instant::now() + Duration::from_secs(2))
                                    },
                                );
                                let forward_started = std::time::Instant::now();
                                let result = tokio::select! {
                                    result = tokio::time::timeout_at(forward_deadline, (forwarder.forward)(dispatched.clone(), forward_deadline)) => result,
                                    _ = dispatch_stop.changed() => return Attempt::Stopped,
                                };
                                let forward_status = match &result {
                                    Ok(Ok(())) => "ok".to_owned(),
                                    Ok(Err(error)) => format!("error:{error:?}"),
                                    Err(_) => "deadline_exceeded".to_owned(),
                                };
                                let parent_destination = "unknown";
                                adaptive_reader_trace(&trace_list, "reader_forward_attempt", format_args!(
                                    "root_partition={} partition={} parent_destination={} workflow_id={} run_id={} schedule_id={} task_id={} elapsed_ms={} status={:?} outer_remaining_after_forward_ms={} child_forward_deadline_ms=2000",
                                    trace_root, trace_partition, parent_destination, trace_workflow,
                                    trace_run, task.data.schedule_id, task.task_id,
                                    forward_started.elapsed().as_millis(), forward_status,
                                    deadline.map(|at| at.saturating_sub(dispatch_limiter.now()).max(0) / 1_000_000).unwrap_or(-1),
                                ));
                                drop(token);
                                match result {
                                    Ok(Ok(())) => return Attempt::Forwarded(attempt),
                                    Ok(Err(error)) => {
                                        if matches!(
                                            error,
                                            crate::matching_forward::ForwardError::SlowDown
                                        ) {
                                            if let Some(metrics) = dispatch_metrics
                                                .lock()
                                                .expect("matching reader metrics lock")
                                                .as_ref()
                                            {
                                                metrics
                                                    .metrics
                                                    .async_forward_throttled(&metrics.context);
                                            }
                                        }
                                    }
                                    Err(_) => (),
                                }
                                match target
                                    .offer_until(
                                        dispatched.clone(),
                                        dispatch_stop.clone(),
                                        forward_deadline,
                                    )
                                    .await
                                {
                                    Some(true) => {
                                        return Attempt::OfferedAfterForwardFailure(attempt)
                                    }
                                    None => return Attempt::Stopped,
                                    Some(false) => (),
                                }
                                attempt = attempt.saturating_add(1);
                            }
                        };
                        let outcome = match deadline {
                            Some(deadline) => {
                                let remaining = Duration::from_nanos(
                                    deadline.saturating_sub(dispatch_limiter.now()).max(0) as u64,
                                );
                                tokio::time::timeout(remaining, attempt)
                                    .await
                                    .unwrap_or(Attempt::TimedOut)
                            }
                            None => attempt.await,
                        };
                        if let Some(reader_metrics) = dispatch_metrics
                            .lock()
                            .expect("matching reader metrics lock")
                            .as_ref()
                        {
                            match &outcome {
                                Attempt::Throttled => {
                                    reader_metrics.metrics.task_list_counter_event(
                                        crate::matching_metrics::BUFFER_THROTTLE_COUNT,
                                        &reader_metrics.context,
                                    )
                                }
                                Attempt::TimedOut => {
                                    reader_metrics.metrics.task_list_counter_event(
                                        crate::matching_metrics::ASYNC_MATCH_DISPATCH_TIMEOUTS,
                                        &reader_metrics.context,
                                    )
                                }
                                _ => (),
                            }
                            reader_metrics.metrics.async_match_latency(
                                &reader_metrics.context,
                                attempt_started.elapsed(),
                            );
                        }
                        adaptive_reader_trace(&trace_list, "reader_head_dispatch_end", format_args!(
                            "root_partition={} partition={} workflow_id={} run_id={} schedule_id={} task_id={} outcome={:?} elapsed_ms={}",
                            trace_root, trace_partition, trace_workflow, trace_run,
                            task.data.schedule_id, task.task_id, outcome, dispatch_started.elapsed().as_millis(),
                        ));
                        match &outcome {
                            Attempt::Offered => {
                                if let Some(reader_metrics) = dispatch_metrics
                                    .lock()
                                    .expect("matching reader metrics lock")
                                    .as_ref()
                                {
                                    reader_metrics.metrics.async_match_local(
                                        &reader_metrics.context,
                                        (dispatch_attempt > 0).then_some(dispatch_attempt),
                                        dispatch_started.elapsed(),
                                    );
                                }
                                break;
                            }
                            Attempt::OfferedAfterForwardRetry(attempt) => {
                                if let Some(metrics) = dispatch_metrics
                                    .lock()
                                    .expect("matching reader metrics lock")
                                    .as_ref()
                                {
                                    metrics.metrics.async_match_local(
                                        &metrics.context,
                                        Some(*attempt),
                                        dispatch_started.elapsed(),
                                    );
                                }
                                break;
                            }
                            Attempt::Forwarded(attempt) => {
                                if let Some(reader_metrics) = dispatch_metrics
                                    .lock()
                                    .expect("matching reader metrics lock")
                                    .as_ref()
                                {
                                    reader_metrics.metrics.async_match_forward(
                                        &reader_metrics.context,
                                        *attempt,
                                        dispatch_started.elapsed(),
                                    );
                                }
                                let queue = dispatch_queue.clone();
                                let id = task.task_id;
                                let completion = crate::matching_blocking::run(move || {
                                    queue.complete(id)?;
                                    queue.run_garbage_collection(false).map(|_| ())
                                })
                                .await;
                                let completion_ok = matches!(&completion, Ok(Ok(_)));
                                adaptive_reader_trace(&trace_list, "reader_durable_completion", format_args!(
                                    "root_partition={} partition={} workflow_id={} run_id={} schedule_id={} task_id={} outcome={}",
                                    trace_root, trace_partition, trace_workflow, trace_run,
                                    task.data.schedule_id, task.task_id,
                                    if completion_ok { "acknowledged" } else { "failed" },
                                ));
                                if !completion_ok {
                                    dispatch_queue.stop();
                                    break 'dispatch;
                                }
                                break;
                            }
                            Attempt::OfferedAfterForwardFailure(attempt) => {
                                if let Some(reader_metrics) = dispatch_metrics
                                    .lock()
                                    .expect("matching reader metrics lock")
                                    .as_ref()
                                {
                                    reader_metrics
                                        .metrics
                                        .async_match_local_after_forward_failure(
                                            &reader_metrics.context,
                                            *attempt,
                                            dispatch_started.elapsed(),
                                        );
                                }
                                break;
                            }
                            Attempt::Stopped => break 'dispatch,
                            Attempt::Throttled | Attempt::TimedOut => {
                                dispatch_attempt = dispatch_attempt.saturating_add(1);
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }
                }
            });
        }
        let pumped = Completed(completion);
        let mut pump_stop = stopped;
        let reader_metrics = inner.metric_context.clone();
        tokio::spawn(async move {
            let _completed = pumped;
            if !start {
                let _ = pump_stop.changed().await;
                return;
            }
            let mut ack_timer = Box::pin(tokio::time::sleep(update_ack_interval(&config)));
            'pump: loop {
                if *pump_stop.borrow() {
                    break;
                }
                if let Some(reader_metrics) = reader_metrics
                    .lock()
                    .expect("matching reader metrics lock")
                    .as_ref()
                {
                    reader_metrics.metrics.task_list_gauge(
                        crate::matching_metrics::TASK_BACKLOG_PER_TASK_LIST,
                        &reader_metrics.context,
                        queue.levels().2 as f64,
                    );
                }
                // Subscribe before the store scan so appends during it are not lost.
                let notification = wake.notified();
                tokio::pin!(notification);
                notification.as_mut().enable();
                let queue_copy = queue.clone();
                let (range, batch) = (read_range_size(&config), read_batch_size(&config));
                let result =
                    crate::matching_blocking::run(move || queue_copy.reader_turn(range, batch))
                        .await;
                if *pump_stop.borrow() || queue.is_stopped() {
                    break;
                }
                let mut turn = match result {
                    Ok(Ok(turn)) => turn,
                    Ok(Err(error)) => {
                        eprintln!("Matching backlog read failed: {error}");
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => {
                        eprintln!("Matching backlog pump failed: {error}");
                        queue.stop();
                        break;
                    }
                };
                let pending = turn.scan_pending();
                loop {
                    let admitted = crate::matching_blocking::run(move || {
                        let result = turn.next(now);
                        (turn, result)
                    })
                    .await;
                    let result = match admitted {
                        Ok((loaded, result)) => {
                            turn = loaded;
                            result
                        }
                        Err(error) => {
                            eprintln!("Matching backlog admission worker failed: {error}");
                            queue.stop();
                            break 'pump;
                        }
                    };
                    let expired = turn.take_expired_count();
                    if expired > 0 {
                        if let Some(reader_metrics) = reader_metrics
                            .lock()
                            .expect("matching reader metrics lock")
                            .as_ref()
                        {
                            reader_metrics
                                .metrics
                                .expired_tasks(&reader_metrics.context, expired);
                        }
                    }
                    let task = match result {
                        Ok(Some(task)) => task,
                        Ok(None) => break,
                        Err(error) => {
                            // Go logger.Fatal: the host-fatal callback runs here,
                            // before this task's completion guard drops.
                            if let NativeQueueError::ReaderState(detail) = &error {
                                if let Some(sink) = &fatal_invariant {
                                    sink(detail.clone());
                                }
                            }
                            if !queue.is_stopped() {
                                eprintln!("Matching backlog admission failed: {error}");
                                queue.stop();
                            }
                            break 'pump;
                        }
                    };
                    let (group, _, leak_cause) = (isolation.select)(&task.data);
                    if let (Some(cause), Some(reader_metrics)) = (
                        leak_cause,
                        reader_metrics
                            .lock()
                            .expect("matching reader metrics lock")
                            .as_ref(),
                    ) {
                        reader_metrics.metrics.task_isolation_leak(
                            &reader_metrics.context,
                            &GoString::from(original_isolation_group(&task.data)).go_rune_string(),
                            cause,
                        );
                    }
                    let buffer_send = buffer_senders
                        .get(group.as_bytes())
                        .or_else(|| buffer_senders.get(&b""[..]))
                        .expect("shared buffer always exists");
                    let sent = match buffer_send {
                        BufferSender::Buffered(sender) => tokio::select! {
                            result = sender.send(task) => result.is_ok(),
                            _ = pump_stop.changed() => false,
                        },
                        BufferSender::Unbuffered(buffer) => {
                            buffer.offer(task, pump_stop.clone()).await
                        }
                    };
                    if !sent {
                        break 'pump;
                    }
                }
                // Release the exclusive cursor before the next store turn.
                drop(turn);
                if let Some(reader_metrics) = reader_metrics
                    .lock()
                    .expect("matching reader metrics lock")
                    .as_ref()
                {
                    reader_metrics.metrics.task_list_gauge(
                        crate::matching_metrics::TASK_BACKLOG_PER_TASK_LIST,
                        &reader_metrics.context,
                        queue.levels().2 as f64,
                    );
                }
                if pending {
                    continue;
                }
                tokio::select! {
                    _ = notification => (),
                    _ = ack_timer.as_mut() => {
                        // Go re-signals the pump on every ack tick.
                        ack_timer
                            .as_mut()
                            .reset(tokio::time::Instant::now() + update_ack_interval(&config));
                    }
                    _ = pump_stop.changed() => break,
                }
            }
        });
        Self { inner }
    }
    pub async fn poll(&self) -> Option<AllocatedTaskBytes> {
        self.inner.matcher.poll(self.inner.stop.subscribe()).await
    }
    pub fn isolation_enabled(&self) -> bool {
        !self.inner.isolated.is_empty()
    }
    pub fn has_isolation_group(&self, group: &[u8]) -> bool {
        self.inner.isolated.contains_key(group)
    }
    pub fn record_task_count(&self, task_count: i64) {
        self.record_gauge(
            crate::matching_metrics::TASK_COUNT_PER_TASK_LIST,
            task_count,
        );
    }
    pub fn record_task_lag(&self, task_lag: i64) {
        self.record_gauge(crate::matching_metrics::TASK_LAG_PER_TASK_LIST, task_lag);
    }
    fn record_gauge(&self, name: &'static str, value: i64) {
        if let Some(reader_metrics) = self
            .inner
            .metric_context
            .lock()
            .expect("matching reader metrics lock")
            .as_ref()
        {
            reader_metrics
                .metrics
                .task_list_gauge(name, &reader_metrics.context, value as f64);
        }
    }
    pub fn should_reload(&self) -> bool {
        (self.inner.should_reload)()
    }
    /// Known pollers read their isolated channel and the shared channel. Unknown
    /// or empty groups only read the shared channel, matching Go matcher.Poll.
    pub async fn poll_group(&self, group: &[u8]) -> Option<AllocatedTaskBytes> {
        match self.inner.isolated.get(group) {
            Some(channel) => tokio::select! {
                task = channel.poll(self.inner.stop.subscribe()) => task,
                task = self.poll() => task,
            },
            None => self.poll().await,
        }
    }
    pub fn stop_signal(&self) -> watch::Sender<bool> {
        self.inner.stop.clone()
    }
    pub fn stop(&self) {
        self.inner.stop.send_replace(true);
    }
    /// Join reader loops, including a running host-fatal callback.
    pub async fn wait_stopped(&self) {
        let completion = &self.inner.completion;
        loop {
            let changed = completion.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if *completion
                .count
                .lock()
                .expect("Matching reader completion lock")
                >= self.inner.loops
            {
                return;
            }
            changed.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskListDataBytes},
        matching_native_task_store::{
            MatchingTaskStoreBytes, Result as StoreResult, TaskListKeyBytes, TaskListRecordBytes,
        },
        matching_sqlite::SqliteTaskStore,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use uuid::Uuid;

    fn n(v: u32) -> NonZeroU32 {
        NonZeroU32::new(v).unwrap()
    }
    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn task_data(workflow: &[u8], group: Option<&[u8]>) -> TaskDataBytes {
        TaskDataBytes {
            workflow_id: GoString::from(workflow),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            schedule_id: 2,
            partition_config: group.map(|g| {
                BTreeMap::from([(GoString::from(ISOLATION_GROUP_KEY), GoString::from(g))])
            }),
            ..Default::default()
        }
    }
    async fn recv(reader: &NativeTaskReader, group: &[u8]) -> AllocatedTaskBytes {
        tokio::time::timeout(Duration::from_secs(5), reader.poll_group(group))
            .await
            .expect("task delivered")
            .expect("reader running")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn raw_invalid_byte_groups_and_tasks_are_delivered_distinctly() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let queue =
            Arc::new(DurableTaskQueueBytes::open(store, key(b"q\xff"), 0, n(100), 1).unwrap());
        let groups: [&[u8]; 3] = [b"\xff", b"\xfe", "\u{fffd}".as_bytes()];
        let data: Vec<_> = groups
            .iter()
            .enumerate()
            .map(|(i, g)| task_data(&[b'w', 0xff, i as u8], Some(g)))
            .collect();
        queue.append(&data, now()).unwrap();
        let isolation = NativeIsolationConfig {
            groups: groups.iter().map(|g| GoString::from(*g)).collect(),
            select: Arc::new(|d| (GoString::from(original_isolation_group(d)), 0, None)),
            should_reload: Arc::new(|| false),
        };
        let reader = NativeTaskReader::with_fatal_invariant_sink(
            queue,
            Arc::new(Notify::new()),
            ReaderConfig::default(),
            Arc::new(|| true),
            Arc::new(TaskListLimiter::with_defaults()),
            isolation,
            None,
            None,
            None,
            None,
        );
        assert!(reader.isolation_enabled());
        for (i, g) in groups.iter().enumerate() {
            assert!(reader.has_isolation_group(g));
            let task = recv(&reader, g).await;
            assert_eq!(task.data.workflow_id.as_bytes(), &[b'w', 0xff, i as u8]);
            assert_eq!(
                task.data.partition_config.as_ref().unwrap()[ISOLATION_GROUP_KEY].as_bytes(),
                *g
            );
        }
        reader.stop();
        reader.wait_stopped().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn normal_delivery_uses_shared_channel() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let queue =
            Arc::new(DurableTaskQueueBytes::open(store, key(b"plain"), 0, n(100), 1).unwrap());
        queue.append(&[task_data(b"wf", None)], now()).unwrap();
        let reader = NativeTaskReader::new(queue, Arc::new(Notify::new()), ReaderConfig::default());
        assert_eq!(recv(&reader, b"").await.data.workflow_id.as_bytes(), b"wf");
        reader.stop();
        reader.wait_stopped().await;
    }

    /// Fake store that accepts appends and scans back exactly the scripted ids.
    struct ScriptedStore {
        ids: Vec<i64>,
    }
    impl MatchingTaskStoreBytes for ScriptedStore {
        fn get_task_list(&self, _: &TaskListKeyBytes) -> StoreResult<TaskListRecordBytes> {
            unreachable!()
        }
        fn lease(
            &self,
            _: &TaskListKeyBytes,
            _: i16,
            _: i64,
            _: i64,
        ) -> StoreResult<TaskListRecordBytes> {
            Ok(TaskListRecordBytes {
                range_id: 1,
                data: TaskListDataBytes::default(),
            })
        }
        fn update_task_list(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: TaskListDataBytes,
            _: i64,
        ) -> StoreResult<()> {
            Ok(())
        }
        fn create_tasks_with_domains(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
            _: &[(Uuid, &AllocatedTaskBytes)],
        ) -> StoreResult<()> {
            Ok(())
        }
        fn get_tasks(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
            _: i64,
        ) -> StoreResult<Vec<AllocatedTaskBytes>> {
            Ok(self
                .ids
                .iter()
                .map(|id| AllocatedTaskBytes {
                    task_id: *id,
                    data: task_data(b"dup\xff", None),
                })
                .collect())
        }
        fn task_list_size(&self, _: &TaskListKeyBytes, _: i64) -> StoreResult<i64> {
            Ok(0)
        }
        fn complete_task(&self, _: &TaskListKeyBytes, _: i64) -> StoreResult<()> {
            Ok(())
        }
        fn complete_tasks_less_than(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
        ) -> StoreResult<usize> {
            Ok(0)
        }
        fn delete_task_list(&self, _: &TaskListKeyBytes, _: i64) -> StoreResult<()> {
            Ok(())
        }
    }

    /// A blocked fatal sink holds shutdown completion until it returns.
    /// Like the production registry, this fixture stops all reader loops after
    /// the host receives the invariant notification.
    async fn fatal_case(ids: Vec<i64>) {
        let queue = Arc::new(
            DurableTaskQueueBytes::open(
                Arc::new(ScriptedStore { ids }),
                key(b"fatal\xff"),
                0,
                n(100),
                1,
            )
            .unwrap(),
        );
        queue
            .append(&[task_data(b"a", None), task_data(b"b", None)], now())
            .unwrap();
        let finished = Arc::new(AtomicBool::new(false));
        let details = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let sink: ReaderFatalSink = {
            let (finished, details, entered, release) = (
                finished.clone(),
                details.clone(),
                entered.clone(),
                release.clone(),
            );
            Arc::new(move |detail| {
                details.lock().unwrap().push(detail);
                entered.notify_one();
                let (lock, changed) = &*release;
                let mut open = lock.lock().unwrap();
                while !*open {
                    open = changed.wait(open).unwrap();
                }
                finished.store(true, Ordering::SeqCst);
            })
        };
        let reader = NativeTaskReader::with_fatal_invariant_sink(
            queue.clone(),
            Arc::new(Notify::new()),
            ReaderConfig::default(),
            Arc::new(|| true),
            Arc::new(TaskListLimiter::with_defaults()),
            NativeIsolationConfig::default(),
            None,
            None,
            None,
            Some(sink),
        );
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("reader reports the invariant");
        reader.stop();
        let returned_while_blocked =
            tokio::time::timeout(Duration::from_millis(50), reader.wait_stopped())
                .await
                .is_ok();
        {
            let (lock, changed) = &*release;
            *lock.lock().unwrap() = true;
            changed.notify_all();
        }
        assert!(
            !returned_while_blocked,
            "shutdown outran the fatal callback"
        );
        tokio::time::timeout(Duration::from_secs(5), reader.wait_stopped())
            .await
            .expect("reader stops after fatal invariant");
        assert!(
            finished.load(Ordering::SeqCst),
            "wait_stopped outran the callback"
        );
        assert_eq!(details.lock().unwrap().len(), 1);
        assert!(queue.is_stopped());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_task_id_fires_fatal_callback_before_stop_completes() {
        fatal_case(vec![1, 1]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn out_of_order_task_id_fires_fatal_callback_before_stop_completes() {
        fatal_case(vec![2, 1]).await;
    }
}
