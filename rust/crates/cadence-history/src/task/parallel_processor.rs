//! Port of `go/common/task/{parallel_task_processor,weighted_round_robin_task_scheduler}.go`
//! (+ the IWRR schedule and weighted channel pool they rely on).
//!
//! Intentionally not ported: idle channel TTL cleanup of the WRR pool (hour-scale GC of
//! unused channel keys), metrics scopes, and the FIFO / hierarchical WRR schedulers.

use std::{
    collections::HashMap,
    hash::Hash,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use cadence_common::backoff::{ExponentialRetryPolicy, Retrier};
use futures_util::FutureExt;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, Notify};

use super::deps::{Daemon, TaskError};

/// Go `ErrTaskProcessorClosed` / `ErrTaskSchedulerClosed`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    #[error("task processor has already shutdown")]
    ProcessorClosed,
    #[error("task scheduler has already shutdown")]
    SchedulerClosed,
}

/// Go `common/task.Task`.
pub trait CommonTask: Send + Sync {
    fn execute(&self) -> futures_util::future::BoxFuture<'_, Result<(), TaskError>>;
    fn handle_err(&self, err: TaskError) -> Option<TaskError>;
    fn retry_err(&self, err: &TaskError) -> bool;
    fn ack(&self);
    fn nack(&self, err: Option<TaskError>);
    /// Lets scheduler key functions recover the concrete task (Go type-asserts the generic task).
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
}

pub type TaskRef = Arc<dyn CommonTask>;

/// Go `common/task.Processor`.
#[async_trait]
pub trait CommonProcessor: Daemon {
    async fn submit(&self, task: TaskRef) -> Result<(), SubmitError>;
}

const STATUS_INITIALIZED: i32 = 0;
const STATUS_STARTED: i32 = 1;
const STATUS_STOPPED: i32 = 2;
const DEFAULT_MONITOR_TICK: Duration = Duration::from_secs(5);
const SHUTDOWN_WAIT: Duration = Duration::from_secs(60);

pub struct ParallelTaskProcessorOptions {
    pub queue_size: usize,
    pub worker_count: Arc<dyn Fn() -> usize + Send + Sync>,
    pub retry_policy: ExponentialRetryPolicy,
}

struct ProcInner {
    status: AtomicI32,
    tx: mpsc::Sender<TaskRef>,
    rx: AsyncMutex<mpsc::Receiver<TaskRef>>,
    shutdown_tx: watch::Sender<bool>,
    options: ParallelTaskProcessorOptions,
    workers: Mutex<Vec<watch::Sender<bool>>>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    monitor_tick: Duration,
}

pub struct ParallelTaskProcessor {
    inner: Arc<ProcInner>,
}

impl ParallelTaskProcessor {
    pub fn new(options: ParallelTaskProcessorOptions) -> Self {
        Self::with_monitor_tick(options, DEFAULT_MONITOR_TICK)
    }

    pub fn with_monitor_tick(options: ParallelTaskProcessorOptions, monitor_tick: Duration) -> Self {
        let (tx, rx) = mpsc::channel(options.queue_size.max(1));
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(ProcInner {
                status: AtomicI32::new(STATUS_INITIALIZED),
                tx,
                rx: AsyncMutex::new(rx),
                shutdown_tx,
                options,
                workers: Mutex::new(Vec::new()),
                handles: Mutex::new(Vec::new()),
                monitor_tick,
            }),
        }
    }
}

impl ProcInner {
    fn is_stopped(&self) -> bool {
        self.status.load(Ordering::SeqCst) == STATUS_STOPPED
    }

    fn add_worker(self: &Arc<Self>, count: usize) {
        for _ in 0..count {
            let (tx, rx) = watch::channel(false);
            self.workers.lock().unwrap().push(tx);
            let me = self.clone();
            let h = tokio::spawn(async move { me.task_worker(rx).await });
            self.handles.lock().unwrap().push(h);
        }
    }

    fn remove_worker(&self, count: usize) {
        let mut w = self.workers.lock().unwrap();
        let count = count.min(w.len());
        let keep = w.len() - count;
        for s in w.split_off(keep) {
            let _ = s.send(true);
        }
    }

    async fn task_worker(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        loop {
            let task = {
                let mut rx = self.rx.lock().await;
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => return,
                    t = rx.recv() => match t { Some(t) => t, None => return },
                }
            };
            self.execute_task(task, &shutdown).await;
        }
    }

    async fn execute_task(&self, task: TaskRef, worker_shutdown: &watch::Receiver<bool>) {
        tracing::debug!("parallel processor worker executing task");
        let run = AssertUnwindSafe(self.run_with_retry(&task, worker_shutdown)).catch_unwind().await;
        match run {
            Ok(Ok(())) => task.ack(),
            Ok(Err(e)) => task.nack(Some(e)),
            Err(p) => {
                let msg = p
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| p.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown".into());
                tracing::error!("recovered panic in task execution: {msg}");
                let _ = task.handle_err(TaskError::Other(format!("recovered panic: {msg}")));
                task.nack(None);
            }
        }
    }

    /// Go `ThrottleRetry.Do(op)` with `isRetryable` = not shut down && task.RetryErr.
    async fn run_with_retry(
        &self,
        task: &TaskRef,
        worker_shutdown: &watch::Receiver<bool>,
    ) -> Result<(), TaskError> {
        let mut retrier = Retrier::new(self.options.retry_policy.clone());
        loop {
            let err = match task.execute().await {
                Ok(()) => return Ok(()),
                Err(e) => match task.handle_err(e) {
                    None => return Ok(()),
                    Some(e) => e,
                },
            };
            let shut = *worker_shutdown.borrow() || *self.shutdown_tx.borrow();
            if shut || !task.retry_err(&err) {
                return Err(err);
            }
            let delay = retrier.next_backoff(&mut rand::thread_rng());
            match delay {
                None => return Err(err),
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }

    fn drain_and_nack(&self) {
        if let Ok(mut rx) = self.rx.try_lock() {
            while let Ok(t) = rx.try_recv() {
                t.nack(None);
            }
        }
    }

    async fn worker_monitor(self: Arc<Self>) {
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut ticker = tokio::time::interval(self.monitor_tick);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    let n = self.workers.lock().unwrap().len();
                    self.remove_worker(n);
                    return;
                }
                _ = ticker.tick() => {
                    let target = (self.options.worker_count)();
                    let cur = self.workers.lock().unwrap().len();
                    self.add_worker(target.saturating_sub(cur));
                    self.remove_worker(cur.saturating_sub(target));
                }
            }
        }
    }
}

#[async_trait]
impl Daemon for ParallelTaskProcessor {
    async fn start(&self) {
        let i = &self.inner;
        if i.status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        i.add_worker((i.options.worker_count)());
        let me = i.clone();
        let h = tokio::spawn(async move { me.worker_monitor().await });
        i.handles.lock().unwrap().push(h);
        tracing::info!("Parallel task processor started.");
    }

    async fn stop(&self) {
        let i = &self.inner;
        if i.status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let _ = i.shutdown_tx.send(true);
        i.drain_and_nack();
        let handles: Vec<_> = std::mem::take(&mut *i.handles.lock().unwrap());
        let wait = async {
            for h in handles {
                let _ = h.await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_WAIT, wait).await.is_err() {
            tracing::warn!("Parallel task processor timedout on shutdown.");
        }
        // workers may have been mid-flight; nack anything submitted meanwhile
        i.drain_and_nack();
        tracing::info!("Parallel task processor shutdown.");
    }
}

#[async_trait]
impl CommonProcessor for ParallelTaskProcessor {
    async fn submit(&self, task: TaskRef) -> Result<(), SubmitError> {
        let i = &self.inner;
        if i.is_stopped() {
            return Err(SubmitError::ProcessorClosed);
        }
        let mut shutdown = i.shutdown_tx.subscribe();
        tokio::select! {
            r = i.tx.send(task) => {
                if r.is_err() { return Err(SubmitError::ProcessorClosed); }
                if i.is_stopped() { i.drain_and_nack(); }
                Ok(())
            }
            _ = async { while !*shutdown.borrow() { if shutdown.changed().await.is_err() { break; } } } => {
                Err(SubmitError::ProcessorClosed)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Weighted round robin scheduler
// ---------------------------------------------------------------------------

struct TaskChannel {
    tx: mpsc::Sender<TaskRef>,
    rx: Mutex<mpsc::Receiver<TaskRef>>,
}

impl TaskChannel {
    fn try_recv(&self) -> Option<TaskRef> {
        self.rx.lock().unwrap().try_recv().ok()
    }
}

/// Go `iwrrSchedule`: snapshot of channels sorted ascending by weight.
#[derive(Default)]
struct IwrrSchedule {
    items: Vec<(Arc<TaskChannel>, usize)>,
    max_weight: usize,
}

struct IwrrIterator {
    schedule: Arc<IwrrSchedule>,
    round: isize,
    index: isize,
}

impl IwrrSchedule {
    fn iter(self: &Arc<Self>) -> IwrrIterator {
        IwrrIterator {
            schedule: self.clone(),
            round: self.max_weight as isize - 1,
            index: self.items.len() as isize - 1,
        }
    }
}

impl IwrrIterator {
    fn try_next(&mut self) -> Option<Arc<TaskChannel>> {
        let items = &self.schedule.items;
        if items.is_empty() {
            return None;
        }
        while self.round >= 0 {
            if self.index >= 0 && (items[self.index as usize].1 as isize) > self.round {
                let c = items[self.index as usize].0.clone();
                self.index -= 1;
                return Some(c);
            }
            self.round -= 1;
            self.index = items.len() as isize - 1;
        }
        None
    }
}

struct WrrPool<K> {
    buffer_size: usize,
    map: Mutex<HashMap<K, (Arc<TaskChannel>, usize)>>,
    schedule: Mutex<Arc<IwrrSchedule>>,
}

impl<K: Hash + Eq + Clone> WrrPool<K> {
    fn get_or_create(&self, key: &K, weight: usize) -> Arc<TaskChannel> {
        let mut map = self.map.lock().unwrap();
        if let Some((c, w)) = map.get_mut(key) {
            let c = c.clone();
            if *w != weight {
                *w = weight;
                self.update_schedule(&map);
            }
            return c;
        }
        let (tx, rx) = mpsc::channel(self.buffer_size.max(1));
        let c = Arc::new(TaskChannel { tx, rx: Mutex::new(rx) });
        map.insert(key.clone(), (c.clone(), weight));
        self.update_schedule(&map);
        c
    }

    fn update_schedule(&self, map: &HashMap<K, (Arc<TaskChannel>, usize)>) {
        let mut items: Vec<_> = map.values().filter(|(_, w)| *w > 0).cloned().collect();
        items.sort_by_key(|(_, w)| *w);
        let max_weight = items.last().map(|(_, w)| *w).unwrap_or(0);
        *self.schedule.lock().unwrap() = Arc::new(IwrrSchedule { items, max_weight });
    }

    fn all_channels(&self) -> Vec<Arc<TaskChannel>> {
        self.map.lock().unwrap().values().map(|(c, _)| c.clone()).collect()
    }

    fn schedule(&self) -> Arc<IwrrSchedule> {
        self.schedule.lock().unwrap().clone()
    }
}

pub struct WeightedRoundRobinTaskSchedulerOptions<K> {
    pub queue_size: usize,
    pub dispatcher_count: usize,
    pub task_to_channel_key_fn: Arc<dyn Fn(&TaskRef) -> K + Send + Sync>,
    pub channel_key_to_weight_fn: Arc<dyn Fn(&K) -> usize + Send + Sync>,
}

struct SchedInner<K> {
    status: AtomicI32,
    pool: WrrPool<K>,
    cancel: watch::Sender<bool>,
    notify: Notify,
    processor: Arc<dyn CommonProcessor>,
    options: WeightedRoundRobinTaskSchedulerOptions<K>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    dispatched: AtomicUsize,
}

pub struct WeightedRoundRobinTaskScheduler<K> {
    inner: Arc<SchedInner<K>>,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> WeightedRoundRobinTaskScheduler<K> {
    pub fn new(
        processor: Arc<dyn CommonProcessor>,
        options: WeightedRoundRobinTaskSchedulerOptions<K>,
    ) -> Self {
        let (cancel, _) = watch::channel(false);
        Self {
            inner: Arc::new(SchedInner {
                status: AtomicI32::new(STATUS_INITIALIZED),
                pool: WrrPool {
                    buffer_size: options.queue_size,
                    map: Mutex::new(HashMap::new()),
                    schedule: Mutex::new(Arc::new(IwrrSchedule::default())),
                },
                cancel,
                notify: Notify::new(),
                processor,
                options,
                handles: Mutex::new(Vec::new()),
                dispatched: AtomicUsize::new(0),
            }),
        }
    }

    pub fn dispatched_count(&self) -> usize {
        self.inner.dispatched.load(Ordering::SeqCst)
    }

    fn is_stopped(&self) -> bool {
        self.inner.status.load(Ordering::SeqCst) == STATUS_STOPPED
    }

    fn drain(c: &TaskChannel) {
        while let Some(t) = c.try_recv() {
            t.nack(None);
        }
    }

    /// Go `Submit`: blocks while the channel is full.
    pub async fn submit(&self, task: TaskRef) -> Result<(), SubmitError> {
        let i = &self.inner;
        if self.is_stopped() {
            return Err(SubmitError::SchedulerClosed);
        }
        let key = (i.options.task_to_channel_key_fn)(&task);
        let weight = (i.options.channel_key_to_weight_fn)(&key);
        let ch = i.pool.get_or_create(&key, weight);
        let mut cancel = i.cancel.subscribe();
        tokio::select! {
            r = ch.tx.send(task) => {
                if r.is_err() { return Err(SubmitError::SchedulerClosed); }
                i.notify.notify_one();
                if self.is_stopped() { Self::drain(&ch); }
                Ok(())
            }
            _ = async { while !*cancel.borrow() { if cancel.changed().await.is_err() { break; } } } => {
                Err(SubmitError::SchedulerClosed)
            }
        }
    }

    /// Go `TrySubmit`: Ok(false) if the channel is full.
    pub fn try_submit(&self, task: TaskRef) -> Result<bool, SubmitError> {
        let i = &self.inner;
        if self.is_stopped() {
            return Err(SubmitError::SchedulerClosed);
        }
        let key = (i.options.task_to_channel_key_fn)(&task);
        let weight = (i.options.channel_key_to_weight_fn)(&key);
        tracing::debug!(weight, "scheduler try_submit");
        let ch = i.pool.get_or_create(&key, weight);
        match ch.tx.try_send(task) {
            Ok(()) => {
                if self.is_stopped() {
                    Self::drain(&ch);
                } else {
                    i.notify.notify_one();
                }
                Ok(true)
            }
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SubmitError::SchedulerClosed),
        }
    }
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> SchedInner<K> {
    async fn dispatcher(self: Arc<Self>) {
        let mut cancel = self.cancel.subscribe();
        loop {
            tokio::select! {
                _ = self.notify.notified() => {
                    if self.dispatch_tasks(&cancel).await { return; }
                }
                _ = cancel.changed() => return,
            }
        }
    }

    /// Returns true if cancelled.
    async fn dispatch_tasks(&self, cancel: &watch::Receiver<bool>) -> bool {
        let mut has_task = true;
        while has_task {
            has_task = false;
            let mut iter = self.pool.schedule().iter();
            while let Some(ch) = iter.try_next() {
                if *cancel.borrow() {
                    return true;
                }
                if let Some(task) = ch.try_recv() {
                    has_task = true;
                    self.dispatched.fetch_add(1, Ordering::SeqCst);
                    tracing::debug!("scheduler dispatching task to processor");
                    if let Err(e) = self.processor.submit(task.clone()).await {
                        tracing::error!("fail to submit task to processor: {e}");
                        task.nack(Some(TaskError::Other(e.to_string())));
                    }
                }
            }
        }
        false
    }
}

#[async_trait]
impl<K: Hash + Eq + Clone + Send + Sync + 'static> Daemon for WeightedRoundRobinTaskScheduler<K> {
    async fn start(&self) {
        let i = &self.inner;
        if i.status
            .compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        for _ in 0..i.options.dispatcher_count {
            let me = i.clone();
            let h = tokio::spawn(async move { me.dispatcher().await });
            i.handles.lock().unwrap().push(h);
        }
        tracing::info!("Weighted round robin task scheduler started.");
    }

    async fn stop(&self) {
        let i = &self.inner;
        if i.status
            .compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let _ = i.cancel.send(true);
        for c in i.pool.all_channels() {
            Self::drain(&c);
        }
        let handles: Vec<_> = std::mem::take(&mut *i.handles.lock().unwrap());
        let wait = async {
            for h in handles {
                let _ = h.await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_WAIT, wait).await.is_err() {
            tracing::warn!("Weighted round robin task scheduler timedout on shutdown.");
        }
        tracing::info!("Weighted round robin task scheduler shutdown.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[derive(Default)]
    struct T {
        fail_times: AtomicUsize,
        retryable: bool,
        panic: bool,
        execs: AtomicUsize,
        acked: AtomicBool,
        nacked: AtomicBool,
        nack_err: Mutex<Option<TaskError>>,
        key: u32,
        order: Option<Arc<Mutex<Vec<u32>>>>,
    }
    impl CommonTask for T {
        fn execute(&self) -> futures_util::future::BoxFuture<'_, Result<(), TaskError>> {
            Box::pin(async move {
                self.execs.fetch_add(1, Ordering::SeqCst);
                if self.panic {
                    panic!("boom");
                }
                if let Some(o) = &self.order {
                    o.lock().unwrap().push(self.key);
                }
                if self.fail_times.load(Ordering::SeqCst) > 0 {
                    self.fail_times.fetch_sub(1, Ordering::SeqCst);
                    return Err(TaskError::Other("fail".into()));
                }
                Ok(())
            })
        }
        fn handle_err(&self, e: TaskError) -> Option<TaskError> {
            Some(e)
        }
        fn retry_err(&self, _: &TaskError) -> bool {
            self.retryable
        }
        fn ack(&self) {
            self.acked.store(true, Ordering::SeqCst);
        }
        fn nack(&self, e: Option<TaskError>) {
            self.nacked.store(true, Ordering::SeqCst);
            *self.nack_err.lock().unwrap() = e;
        }
    }

    fn proc(workers: usize) -> ParallelTaskProcessor {
        ParallelTaskProcessor::new(ParallelTaskProcessorOptions {
            queue_size: 10,
            worker_count: Arc::new(move || workers),
            retry_policy: ExponentialRetryPolicy::new(Duration::from_millis(1))
                .with_maximum_interval(Duration::from_millis(2))
                .with_maximum_attempts(5),
        })
    }

    async fn wait_for(f: impl Fn() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timeout");
    }

    #[tokio::test]
    async fn submit_and_ack() {
        let p = proc(2);
        p.start().await;
        let t = Arc::new(T::default());
        p.submit(t.clone()).await.unwrap();
        wait_for(|| t.acked.load(Ordering::SeqCst)).await;
        assert!(!t.nacked.load(Ordering::SeqCst));
        p.stop().await;
    }

    #[tokio::test]
    async fn nack_on_non_retryable() {
        let p = proc(1);
        p.start().await;
        let t = Arc::new(T { fail_times: AtomicUsize::new(10), ..Default::default() });
        p.submit(t.clone()).await.unwrap();
        wait_for(|| t.nacked.load(Ordering::SeqCst)).await;
        assert_eq!(t.execs.load(Ordering::SeqCst), 1);
        assert!(t.nack_err.lock().unwrap().is_some());
        p.stop().await;
    }

    #[tokio::test]
    async fn retry_then_success() {
        let p = proc(1);
        p.start().await;
        let t = Arc::new(T { fail_times: AtomicUsize::new(2), retryable: true, ..Default::default() });
        p.submit(t.clone()).await.unwrap();
        wait_for(|| t.acked.load(Ordering::SeqCst)).await;
        assert_eq!(t.execs.load(Ordering::SeqCst), 3);
        p.stop().await;
    }

    #[tokio::test]
    async fn panic_recovery_nacks() {
        let p = proc(1);
        p.start().await;
        let t = Arc::new(T { panic: true, ..Default::default() });
        p.submit(t.clone()).await.unwrap();
        wait_for(|| t.nacked.load(Ordering::SeqCst)).await;
        assert!(t.nack_err.lock().unwrap().is_none());
        // worker survives
        let t2 = Arc::new(T::default());
        p.submit(t2.clone()).await.unwrap();
        wait_for(|| t2.acked.load(Ordering::SeqCst)).await;
        p.stop().await;
    }

    #[tokio::test]
    async fn stop_drains_and_nacks_then_closed() {
        // zero workers: tasks stay queued
        let p = proc(0);
        p.start().await;
        let t = Arc::new(T::default());
        p.submit(t.clone()).await.unwrap();
        p.stop().await;
        assert!(t.nacked.load(Ordering::SeqCst));
        assert!(!t.acked.load(Ordering::SeqCst));
        let r = p.submit(Arc::new(T::default())).await;
        assert_eq!(r, Err(SubmitError::ProcessorClosed));
    }

    fn wrr(
        processor: Arc<dyn CommonProcessor>,
        queue: usize,
    ) -> WeightedRoundRobinTaskScheduler<u32> {
        WeightedRoundRobinTaskScheduler::new(
            processor,
            WeightedRoundRobinTaskSchedulerOptions {
                queue_size: queue,
                dispatcher_count: 1,
                task_to_channel_key_fn: Arc::new(|t: &TaskRef| {
                    // key encoded through a downcast-free trick: use pointer-free hash via test map
                    KEYS.with(|k| *k.borrow().get(&(Arc::as_ptr(t) as *const () as usize)).unwrap())
                }),
                channel_key_to_weight_fn: Arc::new(|k| if *k == 1 { 3 } else { 1 }),
            },
        )
    }

    thread_local! {
        static KEYS: std::cell::RefCell<HashMap<usize, u32>> = std::cell::RefCell::new(HashMap::new());
    }

    /// Records dispatch order without executing anything.
    struct Recorder {
        order: Mutex<Vec<u32>>,
        tasks: Mutex<HashMap<usize, u32>>,
    }
    #[async_trait]
    impl Daemon for Recorder {
        async fn start(&self) {}
        async fn stop(&self) {}
    }
    #[async_trait]
    impl CommonProcessor for Recorder {
        async fn submit(&self, task: TaskRef) -> Result<(), SubmitError> {
            let k = self.tasks.lock().unwrap()[&(Arc::as_ptr(&task) as *const () as usize)];
            self.order.lock().unwrap().push(k);
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrr_honors_weights() {
        let rec = Arc::new(Recorder { order: Mutex::new(vec![]), tasks: Mutex::new(HashMap::new()) });
        let s = wrr(rec.clone(), 10);
        // enqueue before start so the dispatcher sees all tasks at once
        for key in [1u32, 2] {
            for _ in 0..3 {
                let t: TaskRef = Arc::new(T::default());
                let id = Arc::as_ptr(&t) as *const () as usize;
                KEYS.with(|k| k.borrow_mut().insert(id, key));
                rec.tasks.lock().unwrap().insert(id, key);
                assert!(s.try_submit(t).unwrap());
            }
        }
        s.start().await;
        wait_for(|| rec.order.lock().unwrap().len() == 6).await;
        // weight(1)=3, weight(2)=1: Go IWRR cycle is [1,1,1,2]; key 2 leftovers drain next cycle
        assert_eq!(*rec.order.lock().unwrap(), vec![1, 1, 1, 2, 2, 2]);
        s.stop().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrr_try_submit_full_and_closed() {
        let rec = Arc::new(Recorder { order: Mutex::new(vec![]), tasks: Mutex::new(HashMap::new()) });
        let s = wrr(rec.clone(), 1);
        let mk = |key: u32| -> TaskRef {
            let t: TaskRef = Arc::new(T::default());
            let id = Arc::as_ptr(&t) as *const () as usize;
            KEYS.with(|k| k.borrow_mut().insert(id, key));
            rec.tasks.lock().unwrap().insert(id, key);
            t
        };
        s.start().await;
        let a = Arc::new(T::default());
        let a_ref: TaskRef = a.clone();
        let id = Arc::as_ptr(&a_ref) as *const () as usize;
        KEYS.with(|k| k.borrow_mut().insert(id, 2));
        rec.tasks.lock().unwrap().insert(id, 2);
        assert!(s.try_submit(a_ref).unwrap());
        assert!(!s.try_submit(mk(2)).unwrap()); // dispatcher has not run yet (current_thread)
        s.stop().await;
        assert!(a.nacked.load(Ordering::SeqCst));
        assert_eq!(s.try_submit(mk(2)), Err(SubmitError::SchedulerClosed));
        assert_eq!(s.submit(mk(2)).await, Err(SubmitError::SchedulerClosed));
    }
}
