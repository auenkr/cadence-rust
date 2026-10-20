//! Matching's local backlog reader: one SQL pump, a bounded task buffer,
//! and a separate head waiting for an unbuffered local poller handoff.
use crate::matching_limiter::{TaskListLimiter, WaitOutcome};
use cadence_persistence::{matching_queue::DurableTaskQueue, matching_sqlite::AllocatedTask};
use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch, Notify};

/// Go `matching.getTasksBatchSize` property default. Go's reader falls back to
/// it whenever the live value is not positive.
pub const DEFAULT_TASKS_BATCH_SIZE: i64 = 1000;
/// Go `matching.readRangeSize` default (a plain, unfiltered integer property).
pub const DEFAULT_READ_RANGE_SIZE: i64 = 50000;
/// Go `matching.asyncTaskDispatchTimeout` default.
pub const DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS: i64 = 3_000_000_000;
/// Go `taskDispatchTimeoutBuffer` (100 ms), added to the minimum timeout
/// needed to drain the buffers at the configured dispatch rate.
pub const DISPATCH_TIMEOUT_BUFFER_NANOS: i64 = 100_000_000;

/// Live reads of the reader's Go `TaskListConfig` properties. Go's `taskReader`
/// consults each one per operation (buffer sizing, per-range batch, per-attempt
/// dispatch timeout), so a runtime config change takes effect without
/// recreating the manager. `None` in `ReaderConfig` keeps the fixed values.
pub type LiveInt = Arc<dyn Fn() -> i64 + Send + Sync>;
#[derive(Clone)]
pub struct ReaderLive {
    /// Go `cfg.GetTasksBatchSize()`.
    pub batch_size: LiveInt,
    /// Go `cfg.ReadRangeSize()`.
    pub read_range_size: LiveInt,
    /// Go `cfg.AsyncTaskDispatchTimeout()` in nanoseconds.
    pub dispatch_timeout: LiveInt,
}
impl ReaderLive {
    /// Go `taskReader` batch reads: a non-positive value uses the property
    /// default instead of querying for zero tasks.
    fn batch_size(&self) -> u32 {
        let value = (self.batch_size)();
        if value <= 0 {
            DEFAULT_TASKS_BATCH_SIZE as u32
        } else {
            u32::try_from(value).unwrap_or(u32::MAX)
        }
    }
    /// Go advances by `ReadRangeSize` each scan step and tolerates a
    /// non-positive value by making no progress (the ten-step scan is bounded).
    fn read_range_size(&self) -> u32 {
        u32::try_from((self.read_range_size)()).unwrap_or(0)
    }
    fn dispatch_timeout_nanos(&self) -> i64 {
        (self.dispatch_timeout)()
    }
}
#[derive(Clone)]
pub struct ReaderConfig {
    pub batch_size: NonZeroU32,
    pub read_range_size: u32,
    /// Go `AsyncTaskDispatchTimeout` (default 3 s).
    pub dispatch_timeout: Duration,
    /// Go `taskDispatchTimeoutBuffer` (100 ms).
    pub dispatch_timeout_buffer: Duration,
    /// Live dynamic-property reads; absent keeps the fixed values above.
    pub live: Option<Arc<ReaderLive>>,
}
impl ReaderConfig {
    /// Go `newTaskReader` sizes each group buffer from the configured batch
    /// size, reserving one slot for the dispatch head.
    fn buffer_batch_size(&self) -> u32 {
        self.live
            .as_ref()
            .map_or_else(|| self.batch_size.get(), |live| live.batch_size())
    }
    /// Batch size for the next bounded read (Go `getTaskBatchWithRange`).
    fn read_batch_size(&self) -> NonZeroU32 {
        NonZeroU32::new(self.buffer_batch_size())
            .expect("batch size falls back to a positive default")
    }
    /// Range width for the next scan step (Go `getTaskBatch`).
    fn read_range_size(&self) -> u32 {
        self.live
            .as_ref()
            .map_or(self.read_range_size, |live| live.read_range_size())
    }
    /// Dispatch timeout for the next attempt (Go `getDispatchTimeout`).
    fn dispatch_timeout_nanos(&self) -> i64 {
        self.live
            .as_ref()
            .map_or(self.dispatch_timeout.as_nanos() as i64, |live| {
                live.dispatch_timeout_nanos()
            })
    }
}
impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: NonZeroU32::new(DEFAULT_TASKS_BATCH_SIZE as u32).unwrap(),
            read_range_size: DEFAULT_READ_RANGE_SIZE as u32,
            dispatch_timeout: Duration::from_nanos(
                DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS as u64,
            ),
            dispatch_timeout_buffer: Duration::from_nanos(DISPATCH_TIMEOUT_BUFFER_NANOS as u64),
            live: None,
        }
    }
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
    matcher: Arc<Rendezvous<AllocatedTask>>,
    isolated: HashMap<String, Arc<Rendezvous<AllocatedTask>>>,
    stop: watch::Sender<bool>,
    completion: Arc<Completion>,
    loops: usize,
    should_reload: Arc<dyn Fn() -> bool + Send + Sync>,
    metric_context: Arc<Mutex<Option<ReaderMetricContext>>>,
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
#[derive(Clone)]
pub struct TaskReader {
    inner: Arc<Inner>,
}
/// Outcome of one dispatch attempt for the head task.
enum Attempt {
    /// Handed off to a poller.
    Offered,
    /// Retry the same head after dispatch throttling.
    Throttled,
    TimedOut,
    /// The reader is stopping.
    Stopped,
}
enum BufferSender {
    Buffered(mpsc::Sender<AllocatedTask>),
    Unbuffered(Arc<Rendezvous<AllocatedTask>>),
}
enum BufferReceiver {
    Buffered(mpsc::Receiver<AllocatedTask>),
    Unbuffered(Arc<Rendezvous<AllocatedTask>>),
}
/// Groups are snapshotted when a manager is created, as in Go. Selection is
/// reevaluated before buffering and on every dispatch attempt.
#[derive(Clone)]
pub struct IsolationConfig {
    pub groups: Vec<String>,
    pub select: IsolationSelector,
    pub should_reload: Arc<dyn Fn() -> bool + Send + Sync>,
}
pub type IsolationSelector =
    Arc<dyn Fn(&cadence_persistence::matching_blob::TaskData) -> (String, i64) + Send + Sync>;
impl Default for IsolationConfig {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            select: Arc::new(|_| (String::new(), 0)),
            should_reload: Arc::new(|| false),
        }
    }
}
pub type DispatchCheck = Arc<
    dyn Fn(
            AllocatedTask,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bool, tonic::Status>> + Send>,
        > + Send
        + Sync,
>;
impl TaskReader {
    pub fn new(queue: Arc<DurableTaskQueue>, wake: Arc<Notify>, config: ReaderConfig) -> Self {
        Self::with_domain_activity(queue, wake, config, Arc::new(|| true))
    }
    pub fn with_domain_activity(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self::with_domain_activity_and_limiter(
            queue,
            wake,
            config,
            active,
            Arc::new(TaskListLimiter::with_defaults()),
        )
    }
    /// One shared dispatch limiter per queue owner, matching Go's per-manager
    /// `taskListLimiter`. The reader waits on it before each matcher handoff and
    /// reads its current limit for the active-domain dispatch deadline.
    pub fn with_domain_activity_and_limiter(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
    ) -> Self {
        Self::with_isolation(
            queue,
            wake,
            config,
            active,
            limiter,
            IsolationConfig::default(),
        )
    }
    pub fn with_isolation(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
        isolation: IsolationConfig,
    ) -> Self {
        Self::with_dispatch_check(queue, wake, config, active, limiter, isolation, None)
    }
    pub fn with_dispatch_check(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
        isolation: IsolationConfig,
        dispatch_check: Option<DispatchCheck>,
    ) -> Self {
        Self::with_dispatch_check_metrics(
            queue,
            wake,
            config,
            active,
            limiter,
            isolation,
            dispatch_check,
            None,
        )
    }
    pub fn with_dispatch_check_metrics(
        queue: Arc<DurableTaskQueue>,
        wake: Arc<Notify>,
        config: ReaderConfig,
        active: Arc<dyn Fn() -> bool + Send + Sync>,
        limiter: Arc<TaskListLimiter>,
        isolation: IsolationConfig,
        dispatch_check: Option<DispatchCheck>,
        metric_context: Option<(
            crate::matching_metrics::MatchingMetrics,
            crate::matching_metrics::PollMetricContext,
        )>,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let matcher = Arc::new(Rendezvous::default());
        let isolated: HashMap<_, _> = isolation
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
        });
        let mut buffer_senders = HashMap::new();
        for buffer_group in std::iter::once(String::new()).chain(isolated.keys().cloned()) {
            let (buffer_send, mut buffer_receive) = if config.buffer_batch_size() == 1 {
                let buffer = Arc::new(Rendezvous::default());
                (
                    BufferSender::Unbuffered(buffer.clone()),
                    BufferReceiver::Unbuffered(buffer),
                )
            } else {
                let (sender, receiver) = mpsc::channel(config.buffer_batch_size() as usize - 1);
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
            tokio::spawn(async move {
                let _completed = dispatched;
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
                    let mut dispatch_attempt = 0u64;
                    loop {
                        // Go rechecks expiry at each dispatch attempt, including
                        // after a row spent time waiting behind another head.
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
                                tokio::task::spawn_blocking(move || queue.complete(id)).await;
                            if !matches!(result, Ok(Ok(_))) {
                                dispatch_queue.stop();
                                break 'dispatch;
                            }
                            // Expired buffered rows are acknowledged without an
                            // immediate GC run, matching taskReader's expiry branch.
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
                                        tokio::task::spawn_blocking(move || {
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
                                Err(_) => {
                                    tokio::task::yield_now().await;
                                    continue;
                                }
                            }
                        }
                        // Go `newDispatchContext`: the deadline only applies to an
                        // active (or unknown) domain above the 1e-7 noise floor.
                        let (group, mut isolation_duration) = (isolation.select)(&task.data);
                        let target = isolated.get(&group).unwrap_or_else(|| {
                            isolation_duration = 0;
                            &matcher
                        });
                        let rps = dispatch_limiter.limit();
                        // Go `getDispatchTimeout` re-reads the configured
                        // timeout on every dispatch attempt.
                        let async_timeout = dispatch_config.dispatch_timeout_nanos();
                        let deadline = ((isolated.contains_key(&group) || rps > 1e-7) && active())
                            .then(|| {
                                let timeout = cadence_common::task_isolation::dispatch_timeout(
                                    rps,
                                    buffers,
                                    async_timeout,
                                    isolation_duration,
                                    timeout_buffer.as_nanos() as i64,
                                );
                                dispatch_limiter.now().saturating_add(timeout)
                            });
                        let attempt_started = std::time::Instant::now();
                        let attempt = async {
                            // Go `taskMatcher.ratelimit` maps ErrCannotWait (zero
                            // burst or an unreachable deadline) to a throttled retry.
                            match dispatch_limiter
                                .wait_until(dispatch_stop.clone(), deadline)
                                .await
                            {
                                WaitOutcome::Acquired => (),
                                WaitOutcome::Stopped => return Attempt::Stopped,
                                WaitOutcome::Throttled => return Attempt::Throttled,
                            }
                            let mut dispatched = task.clone();
                            let dispatch_group = if isolated.contains_key(&group) {
                                group.as_str()
                            } else {
                                ""
                            };
                            dispatched.data.partition_config =
                                cadence_common::task_isolation::dispatch_partition_config(
                                    &task.data.partition_config,
                                    dispatch_group,
                                );
                            if target.offer(dispatched, dispatch_stop.clone()).await {
                                Attempt::Offered
                            } else {
                                Attempt::Stopped
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
                        }
                        if let Some(reader_metrics) = dispatch_metrics
                            .lock()
                            .expect("matching reader metrics lock")
                            .as_ref()
                        {
                            reader_metrics.metrics.async_match_latency(
                                &reader_metrics.context,
                                attempt_started.elapsed(),
                            );
                        }
                        match outcome {
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
                            Attempt::Stopped => break 'dispatch,
                            Attempt::Throttled | Attempt::TimedOut => {
                                dispatch_attempt = dispatch_attempt.saturating_add(1);
                                // Go yields before retrying a throttled/timeout head.
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }
                }
                // Dropping the receiver releases a pump blocked on its next send.
            });
        }
        let pumped = Completed(completion);
        let mut pump_stop = stopped;
        let reader_metrics = inner.metric_context.clone();
        tokio::spawn(async move {
            let _completed = pumped;
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
                // Subscribe before SQL so appends during a scan are not lost.
                let notification = wake.notified();
                tokio::pin!(notification);
                notification.as_mut().enable();
                let queue_copy = queue.clone();
                // Go reads the scan range and batch size on every bounded read.
                let (range, batch) = (config.read_range_size(), config.read_batch_size());
                let result =
                    tokio::task::spawn_blocking(move || queue_copy.reader_turn(range, batch)).await;
                if *pump_stop.borrow() || queue.is_stopped() {
                    break;
                }
                let mut turn = match result {
                    Ok(Ok(turn)) => turn,
                    Ok(Err(error)) => {
                        // Go re-signals even after a bounded retry terminates.
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
                    // Admission shares queue state with lease renewal; keep a
                    // potentially contended mutex off the async runtime too.
                    let admitted = tokio::task::spawn_blocking(move || {
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
                            if !queue.is_stopped() {
                                eprintln!("Matching backlog admission failed: {error}");
                                queue.stop();
                            }
                            break 'pump;
                        }
                    };
                    let (group, _) = (isolation.select)(&task.data);
                    let buffer_send = buffer_senders
                        .get(&group)
                        .unwrap_or_else(|| &buffer_senders[""]);
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
                // Release the exclusive cursor before the next SQL turn.
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
                    _ = pump_stop.changed() => break,
                }
            }
        });
        Self { inner }
    }
    pub async fn poll(&self) -> Option<AllocatedTask> {
        self.inner.matcher.poll(self.inner.stop.subscribe()).await
    }
    pub(crate) fn has_isolation_group(&self, group: &str) -> bool {
        self.inner.isolated.contains_key(group)
    }
    pub(crate) fn record_task_count(&self, task_count: i64) {
        if let Some(reader_metrics) = self
            .inner
            .metric_context
            .lock()
            .expect("matching reader metrics lock")
            .as_ref()
        {
            reader_metrics.metrics.task_list_gauge(
                crate::matching_metrics::TASK_COUNT_PER_TASK_LIST,
                &reader_metrics.context,
                task_count as f64,
            );
        }
    }
    pub(crate) fn record_task_lag(&self, task_lag: i64) {
        if let Some(reader_metrics) = self
            .inner
            .metric_context
            .lock()
            .expect("matching reader metrics lock")
            .as_ref()
        {
            reader_metrics.metrics.task_list_gauge(
                crate::matching_metrics::TASK_LAG_PER_TASK_LIST,
                &reader_metrics.context,
                task_lag as f64,
            );
        }
    }
    pub fn should_reload(&self) -> bool {
        (self.inner.should_reload)()
    }
    /// Known pollers read their isolated channel and the shared channel. Unknown
    /// or empty groups only read the shared channel, matching Go matcher.Poll.
    pub async fn poll_group(&self, group: &str) -> Option<AllocatedTask> {
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
    /// Join reader loops asynchronously after final ack persistence/GC. History
    /// delivery RPCs have separate ownership and are not included in this join.
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
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData},
        matching_sqlite::{SqliteTaskStore, TaskListKey},
    };
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn reader_live_properties_match_go_reads() {
        let batch = Arc::new(AtomicI64::new(4));
        let range = Arc::new(AtomicI64::new(7));
        let timeout = Arc::new(AtomicI64::new(2_000_000_000));
        let live = Arc::new(ReaderLive {
            batch_size: {
                let batch = batch.clone();
                Arc::new(move || batch.load(Ordering::SeqCst))
            },
            read_range_size: {
                let range = range.clone();
                Arc::new(move || range.load(Ordering::SeqCst))
            },
            dispatch_timeout: {
                let timeout = timeout.clone();
                Arc::new(move || timeout.load(Ordering::SeqCst))
            },
        });
        let config = ReaderConfig {
            live: Some(live),
            ..Default::default()
        };
        // Every read happens live, so a runtime change applies to the next
        // buffer sizing, scan step and dispatch attempt.
        assert_eq!(config.buffer_batch_size(), 4);
        assert_eq!(config.read_batch_size().get(), 4);
        assert_eq!(config.read_range_size(), 7);
        assert_eq!(config.dispatch_timeout_nanos(), 2_000_000_000);
        batch.store(9, Ordering::SeqCst);
        range.store(11, Ordering::SeqCst);
        timeout.store(5_000_000, Ordering::SeqCst);
        assert_eq!(config.buffer_batch_size(), 9);
        assert_eq!(config.read_range_size(), 11);
        assert_eq!(config.dispatch_timeout_nanos(), 5_000_000);
        // Go `getTaskBatchWithRange` falls back to the property default when
        // the configured batch size is not positive.
        batch.store(0, Ordering::SeqCst);
        assert_eq!(
            config.read_batch_size().get(),
            DEFAULT_TASKS_BATCH_SIZE as u32
        );
        batch.store(-3, Ordering::SeqCst);
        assert_eq!(config.buffer_batch_size(), DEFAULT_TASKS_BATCH_SIZE as u32);
        // Go tolerates a non-positive range by making no progress in its
        // bounded ten-step scan rather than substituting a default.
        range.store(0, Ordering::SeqCst);
        assert_eq!(config.read_range_size(), 0);
        // Without live providers the fixed values and Go defaults still apply.
        let fixed = ReaderConfig::default();
        assert_eq!(fixed.buffer_batch_size(), DEFAULT_TASKS_BATCH_SIZE as u32);
        assert_eq!(fixed.read_range_size(), DEFAULT_READ_RANGE_SIZE as u32);
        assert_eq!(
            fixed.dispatch_timeout_nanos(),
            DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS
        );
    }

    fn fixture(
        count: usize,
        batch: u32,
    ) -> (
        tempfile::TempDir,
        Arc<DurableTaskQueue>,
        TaskReader,
        Arc<Notify>,
    ) {
        configured_fixture(count, batch, Duration::from_secs(3), true)
    }
    fn configured_fixture(
        count: usize,
        batch: u32,
        dispatch_timeout: Duration,
        active: bool,
    ) -> (
        tempfile::TempDir,
        Arc<DurableTaskQueue>,
        TaskReader,
        Arc<Notify>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = Arc::new(
            DurableTaskQueue::open(
                store,
                TaskListKey {
                    domain_id: Uuid::from_u128(1),
                    name: "reader".into(),
                    task_type: 0,
                },
                0,
                NonZeroU32::new(100).unwrap(),
                now(),
            )
            .unwrap(),
        );
        queue
            .append(&vec![TaskData::default(); count], now())
            .unwrap();
        let wake = Arc::new(Notify::new());
        let reader = TaskReader::with_domain_activity(
            queue.clone(),
            wake.clone(),
            ReaderConfig {
                batch_size: NonZeroU32::new(batch).unwrap(),
                read_range_size: 100,
                dispatch_timeout,
                ..ReaderConfig::default()
            },
            Arc::new(move || active),
        );
        (directory, queue, reader, wake)
    }
    async fn stop(queue: Arc<DurableTaskQueue>, reader: TaskReader) {
        reader.stop();
        queue.stop();
        tokio::time::timeout(Duration::from_secs(2), reader.wait_stopped())
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn matching_reader_reports_go_ack_backlog_gauge() {
        let (_directory, queue, reader, wake) = fixture(0, 3);
        let sink = Arc::new(crate::matching_metrics::RecordingMetricSink::default());
        reader.with_metric_context(
            crate::matching_metrics::MatchingMetrics::new(sink.clone()),
            crate::matching_metrics::PollMetricContext {
                domain: "reader-domain".into(),
                task_list: "reader-list".into(),
                task_list_kind: 0,
                task_type: "decision_task",
                poller_isolation_group: String::new(),
            },
        );
        queue.append(&[TaskData::default()], now()).unwrap();
        wake.notify_waiters();
        wait_read(&queue, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !sink
                .events()
                .iter()
                .any(|event| event.name == crate::matching_metrics::TASK_BACKLOG_PER_TASK_LIST)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(queue.levels().2, 1);
        let backlog = sink
            .events()
            .into_iter()
            .filter(|event| event.name == crate::matching_metrics::TASK_BACKLOG_PER_TASK_LIST)
            .last()
            .unwrap();
        assert_eq!(
            backlog.value,
            crate::matching_metrics::MetricValue::Number(1.0)
        );
        stop(queue, reader).await;
    }
    async fn wait_read(queue: &DurableTaskQueue, expected: i64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while queue.levels().0 < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn matching_reader_isolation_buffers_avoid_cross_group_head_blocking() {
        let (_directory, queue, initial, wake) = fixture(0, 3);
        initial.stop();
        initial.wait_stopped().await;
        let reader = TaskReader::with_isolation(
            queue.clone(),
            wake.clone(),
            ReaderConfig {
                batch_size: NonZeroU32::new(3).unwrap(),
                ..Default::default()
            },
            Arc::new(|| true),
            Arc::new(TaskListLimiter::with_defaults()),
            IsolationConfig {
                groups: vec!["a".into(), "b".into()],
                should_reload: Arc::new(|| false),
                select: Arc::new(|task| {
                    (
                        task.partition_config
                            .as_ref()
                            .and_then(|config| config.get("isolation-group"))
                            .cloned()
                            .unwrap_or_default(),
                        0,
                    )
                }),
            },
        );
        let task = |group: &str| TaskData {
            partition_config: Some(std::collections::BTreeMap::from([(
                "isolation-group".into(),
                group.into(),
            )])),
            ..Default::default()
        };
        queue.append(&[task("a"), task("b")], now()).unwrap();
        wake.notify_waiters();
        wait_read(&queue, 2).await;
        // A's head waits for A; it cannot block B's independent buffer.
        let b = tokio::time::timeout(Duration::from_secs(2), reader.poll_group("b"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.task_id, 2);
        queue.append(&[task("")], now()).unwrap();
        wake.notify_waiters();
        let shared = tokio::time::timeout(Duration::from_secs(2), reader.poll_group("unknown"))
            .await
            .unwrap();
        assert_eq!(shared.unwrap().task_id, 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reader.poll())
                .await
                .is_err()
        );
        assert_eq!(reader.poll_group("a").await.unwrap().task_id, 1);
        stop(queue, reader).await;
    }
    #[tokio::test]
    async fn matching_reader_isolation_timeout_reselects_without_rewriting_storage() {
        let (_directory, queue, initial, wake) = fixture(0, 3);
        initial.stop();
        initial.wait_stopped().await;
        let expires = now() + 200_000_000;
        let reader = TaskReader::with_isolation(
            queue.clone(),
            wake.clone(),
            ReaderConfig::default(),
            Arc::new(|| true),
            Arc::new(TaskListLimiter::with_defaults()),
            IsolationConfig {
                groups: vec!["a".into()],
                should_reload: Arc::new(|| false),
                select: Arc::new(move |_| {
                    let remaining = expires - now();
                    if remaining > 50_000_000 {
                        ("a".into(), remaining)
                    } else {
                        (String::new(), 0)
                    }
                }),
            },
        );
        let original_config = Some(std::collections::BTreeMap::from([
            ("isolation-group".into(), "a".into()),
            ("wf-id".into(), "workflow".into()),
            ("extra".into(), "persisted-only".into()),
        ]));
        queue
            .append(
                &[TaskData {
                    partition_config: original_config.clone(),
                    ..Default::default()
                }],
                now(),
            )
            .unwrap();
        wake.notify_waiters();
        wait_read(&queue, 1).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), reader.poll_group("b"))
                .await
                .is_err()
        );
        let delivered = tokio::time::timeout(Duration::from_secs(2), reader.poll_group("b"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.task_id, 1);
        assert_eq!(
            queue.levels().0,
            1,
            "leaking a task must not allocate another ID"
        );
        let dispatched = delivered.data.partition_config.unwrap();
        assert_eq!(dispatched["isolation-group"], "");
        assert_eq!(dispatched["original-isolation-group"], "a");
        assert_eq!(dispatched["wf-id"], "workflow");
        assert!(!dispatched.contains_key("extra"));
        assert_eq!(
            queue.retry_data(1).unwrap().partition_config,
            original_config
        );
        stop(queue, reader).await;
    }
    #[tokio::test]
    async fn matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows() {
        for batch in [1, 3] {
            let (_directory, queue, reader, _) = fixture(8, batch);
            // buffer=batch-1, separate dispatch head=1, blocked reader send=1.
            wait_read(&queue, i64::from(batch + 1)).await;
            assert_eq!(queue.levels().2, i64::from(batch + 1));
            let first = reader.poll().await.unwrap();
            assert_eq!(first.task_id, 1);
            queue.complete(first.task_id).unwrap();
            wait_read(&queue, i64::from(batch + 2)).await;
            assert_eq!(queue.levels().2, i64::from(batch + 1));
            stop(queue.clone(), reader).await;
            assert_eq!(queue.levels().1, 1);
            assert!(queue.retry_data(2).is_ok());
        }
    }
    #[tokio::test]
    async fn matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads() {
        for active in [true, false] {
            let (_directory, queue, reader, wake) =
                configured_fixture(0, 3, Duration::from_millis(10), active);
            let task = TaskData {
                expiry_nanos: now() + 500_000_000,
                ..TaskData::default()
            };
            queue.append(&[task], now()).unwrap();
            wake.notify_waiters();
            wait_read(&queue, 1).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while reader.inner.matcher.offers.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(550)).await;
            if active {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while queue.levels().1 != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            } else {
                assert_eq!(
                    queue.levels().2,
                    1,
                    "passive handoff has no active-domain deadline"
                );
                assert_eq!(reader.poll().await.unwrap().task_id, 1);
            }
            stop(queue, reader).await;
        }
    }
    #[tokio::test]
    async fn matching_reader_drops_row_that_expired_behind_dispatch_head() {
        let (_directory, queue, reader, wake) = fixture(0, 3);
        queue
            .append(
                &[
                    TaskData::default(),
                    TaskData {
                        expiry_nanos: now() + 200_000_000,
                        ..TaskData::default()
                    },
                ],
                now(),
            )
            .unwrap();
        wake.notify_waiters();
        wait_read(&queue, 2).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let first = reader.poll().await.unwrap();
        queue.complete(first.task_id).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while queue.levels().1 != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(queue.levels().2, 0);
        stop(queue, reader).await;
    }
    #[test]
    fn matching_reader_full_buffer_does_not_occupy_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_directory, queue, reader, _) = fixture(8, 3);
            wait_read(&queue, 4).await;
            let available = tokio::time::timeout(
                Duration::from_millis(500),
                tokio::task::spawn_blocking(|| 42),
            )
            .await;
            stop(queue, reader).await;
            assert_eq!(
                available
                    .expect("full reader buffer occupied blocking pool")
                    .unwrap(),
                42
            );
        });
    }
    #[tokio::test]
    async fn matching_reader_cancelled_wait_does_not_steal_later_task() {
        let (_directory, queue, reader, wake) = fixture(0, 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reader.poll())
                .await
                .is_err()
        );
        let written = queue.append(&[TaskData::default()], now()).unwrap();
        wake.notify_waiters();
        let task = tokio::time::timeout(Duration::from_secs(2), reader.poll())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task, written[0]);
        stop(queue, reader).await;
    }
    #[tokio::test]
    async fn matching_reader_stop_disconnects_waiting_poll_and_joins_empty_pump() {
        let (_directory, queue, reader, _) = fixture(0, 3);
        let copy = reader.clone();
        let poll = tokio::spawn(async move { copy.poll().await });
        tokio::task::yield_now().await;
        stop(queue, reader).await;
        assert!(poll.await.unwrap().is_none());
    }
    #[tokio::test]
    async fn matching_reader_rendezvous_offer_waits_for_actual_acceptance() {
        let channel = Arc::new(Rendezvous::default());
        let (_stop, stopped) = watch::channel(false);
        let copy = channel.clone();
        let receiver = stopped.clone();
        let offered = tokio::spawn(async move { copy.offer(7, receiver).await });
        while channel.offers.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        assert!(!offered.is_finished());
        assert_eq!(channel.poll(stopped).await, Some(7));
        assert!(offered.await.unwrap());
    }
    #[tokio::test]
    async fn matching_reader_cancelled_offer_withdraws_before_handoff() {
        let channel = Arc::new(Rendezvous::default());
        let (stop, stopped) = watch::channel(false);
        let copy = channel.clone();
        let receiver = stopped.clone();
        let offered = tokio::spawn(async move { copy.offer(7, receiver).await });
        while channel.offers.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        offered.abort();
        let _ = offered.await;
        assert!(channel.offers.lock().unwrap().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), channel.poll(stopped.clone()))
                .await
                .is_err()
        );
        stop.send_replace(true);
        assert_eq!(channel.poll(stopped).await, None);
    }
}
