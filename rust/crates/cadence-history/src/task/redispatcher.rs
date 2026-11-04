//! Port of `go/service/history/task/redispatcher.go`.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::deps::*;
use super::history_task::{REDISPATCH_BACKOFF_COEFFICIENT, REDISPATCH_FAILURE_BACKOFF_INTERVAL, REDISPATCH_MAX_BACKOFF_INTERVAL};
use super::interface::*;

const DEFAULT_BUFFER_SIZE: i64 = 200;

struct RedispatchNotification {
    target_size: i64,
    done: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct RedispatcherOptions {
    pub task_redispatch_interval: DurationPropertyFn,
}

#[derive(Clone)]
struct RedispatchTask {
    task: Arc<dyn Task>,
    redispatch_time: Time,
    seq: u64,
}

impl PartialEq for RedispatchTask {
    fn eq(&self, o: &Self) -> bool {
        self.redispatch_time == o.redispatch_time && self.seq == o.seq
    }
}
impl Eq for RedispatchTask {}
impl PartialOrd for RedispatchTask {
    fn partial_cmp(&self, o: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(o))
    }
}
impl Ord for RedispatchTask {
    /// min-heap by redispatch time (`redispatchTaskCompareLess`)
    fn cmp(&self, o: &Self) -> CmpOrdering {
        o.redispatch_time.cmp(&self.redispatch_time).then(o.seq.cmp(&self.seq))
    }
}

#[derive(Default)]
struct State {
    pq_map: HashMap<i32, BinaryHeap<RedispatchTask>>, // priority -> redispatch queue
    task_ch_full: HashMap<i32, bool>,                 // priority -> if taskCh is full
    next_seq: u64,
}

impl State {
    fn get_or_create_pq(&mut self, priority: i32) -> &mut BinaryHeap<RedispatchTask> {
        self.pq_map.entry(priority).or_default()
    }
    fn add(&mut self, priority: i32, task: Arc<dyn Task>, t: Time) {
        self.next_seq += 1;
        let seq = self.next_seq;
        self.get_or_create_pq(priority).push(RedispatchTask { task, redispatch_time: t, seq });
    }
    fn size(&self) -> usize {
        self.pq_map.values().map(|q| q.len()).sum()
    }
}

pub struct RedispatcherImpl {
    task_processor: Arc<dyn Processor>,
    time_source: Arc<dyn TimeSource>,
    logger: Logger,
    metrics_scope: Arc<dyn MetricsScope>,
    status: AtomicI32,
    shutdown: CancellationToken,
    redispatch_tx: mpsc::Sender<RedispatchNotification>,
    redispatch_rx: Mutex<Option<mpsc::Receiver<RedispatchNotification>>>,
    timer_gate: Arc<TimerGate>,
    backoff_policy: RetryPolicy,
    state: tokio::sync::Mutex<State>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    weak_self: Weak<RedispatcherImpl>,
}

/// `NewRedispatcher`
pub fn new_redispatcher(
    task_processor: Arc<dyn Processor>,
    time_source: Arc<dyn TimeSource>,
    options: &RedispatcherOptions,
    logger: Logger,
    metrics_scope: Arc<dyn MetricsScope>,
) -> Arc<dyn Redispatcher> {
    let backoff_policy = RetryPolicy::new((options.task_redispatch_interval)())
        .with_backoff_coefficient(REDISPATCH_BACKOFF_COEFFICIENT)
        .with_maximum_interval(REDISPATCH_MAX_BACKOFF_INTERVAL)
        .with_expiration_interval(NO_INTERVAL);
    let (tx, rx) = mpsc::channel(1);
    Arc::new_cyclic(|weak| RedispatcherImpl {
        weak_self: weak.clone(),
        task_processor,
        timer_gate: Arc::new(TimerGate::new(time_source.clone())),
        time_source,
        logger,
        metrics_scope,
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        shutdown: CancellationToken::new(),
        redispatch_tx: tx,
        redispatch_rx: Mutex::new(Some(rx)),
        backoff_policy,
        state: tokio::sync::Mutex::new(State::default()),
        handle: Mutex::new(None),
    })
}

impl RedispatcherImpl {
    fn is_stopped(&self) -> bool {
        self.status.load(Ordering::SeqCst) == DAEMON_STATUS_STOPPED
    }

    fn get_redispatch_time(&self, attempt: i32) -> Time {
        let d = compute_next_delay(&self.backoff_policy, Duration::ZERO, attempt as usize);
        self.time_source.now().add(d)
    }

    async fn redispatch_loop(self: Arc<Self>, mut rx: mpsc::Receiver<RedispatchNotification>) {
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = self.timer_gate.fired() => {
                    self.redispatch_tasks(RedispatchNotification { target_size: 0, done: None }).await;
                }
                Some(n) = rx.recv() => {
                    self.redispatch_tasks(n).await;
                }
            }
        }
    }

    async fn redispatch_tasks(&self, mut notification: RedispatchNotification) {
        let mut st = self.state.lock().await;
        // deferred: close doneCh
        let done = notification.done.take();
        self.redispatch_locked(&mut st, notification.target_size).await;
        if let Some(d) = done {
            let _ = d.send(());
        }
    }

    async fn redispatch_locked(&self, st: &mut State, target_size: i64) {
        if self.is_stopped() {
            return;
        }
        let queue_size = st.size() as i64;
        self.metrics_scope
            .record_timer(metrics::TASK_REDISPATCH_QUEUE_PENDING_TASKS_TIMER, Duration::from_nanos(queue_size as u64));
        self.metrics_scope
            .int_exponential_histogram(metrics::TASK_REDISPATCH_QUEUE_PENDING_TASKS_HISTOGRAM, queue_size as usize);

        let target_redispatched = queue_size + DEFAULT_BUFFER_SIZE - target_size;
        if target_redispatched <= 0 {
            return;
        }

        let mut total_redispatched = 0i64;
        let now = self.time_source.now();
        let priorities: Vec<i32> = st.pq_map.keys().cloned().collect();
        for p in &priorities {
            st.task_ch_full.insert(*p, false);
        }

        for priority in priorities {
            #[allow(clippy::while_let_loop)]
            loop {
                let Some(pq) = st.pq_map.get_mut(&priority) else { break };
                if pq.is_empty()
                    || total_redispatched >= target_redispatched
                    || *st.task_ch_full.get(&priority).unwrap_or(&false)
                {
                    break;
                }
                let item = pq.peek().unwrap().clone();
                if item.redispatch_time.after(now) {
                    break;
                }
                if item.task.state() != TaskState::Pending {
                    pq.pop();
                    continue;
                }
                if item.task.get_attempt() == 0 {
                    item.task.set_initial_submit_time(now);
                }
                let res = self.task_processor.try_submit(item.task.clone()).await;
                let (submitted, err) = match res {
                    Ok(s) => (s, None),
                    Err(e) => (false, Some(e)),
                };
                if let Some(e) = &err {
                    if self.is_stopped() {
                        break;
                    }
                    self.logger.error("Failed to redispatch task", &[tag::error(e)]);
                }
                st.pq_map.get_mut(&priority).unwrap().pop();
                let new_priority = item.task.priority();
                if err.is_some() || !submitted {
                    let t = self.time_source.now().add(REDISPATCH_FAILURE_BACKOFF_INTERVAL);
                    st.add(new_priority, item.task.clone(), t);
                }
                if err.is_none() && !submitted {
                    st.task_ch_full.insert(new_priority, true);
                }
                if submitted {
                    total_redispatched += 1;
                }
            }
            if let Some(pq) = st.pq_map.get(&priority) {
                if let Some(item) = pq.peek() {
                    self.timer_gate.update(item.redispatch_time);
                }
            }
            if self.is_stopped() {
                return;
            }
        }
    }
}

#[async_trait]
impl Daemon for RedispatcherImpl {
    async fn start(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let rx = self.redispatch_rx.lock().unwrap().take().expect("redispatch receiver");
        let me = self.weak_self.upgrade().expect("redispatcher dropped");
        *self.handle.lock().unwrap() = Some(tokio::spawn(async move { me.redispatch_loop(rx).await }));
        self.logger.info("Task redispatcher started.", &[tag::life_cycle_started()]);
    }

    async fn stop(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.shutdown.cancel();
        self.timer_gate.stop();
        let h = self.handle.lock().unwrap().take();
        if let Some(h) = h {
            if tokio::time::timeout(Duration::from_secs(60), h).await.is_err() {
                self.logger.warn("Task redispatcher timedout on shutdown.", &[tag::life_cycle_stop_timedout()]);
            }
        }
        self.logger.info("Task redispatcher stopped.", &[tag::life_cycle_stopped()]);
    }
}

#[async_trait]
impl Redispatcher for RedispatcherImpl {
    fn add_task(&self, task: Arc<dyn Task>) {
        let priority = task.priority();
        let attempt = task.get_attempt();
        let t = self.get_redispatch_time(attempt);
        // Go holds the mutex; std blocking lock via try-loop keeps this fn sync.
        let mut st = loop {
            if let Ok(g) = self.state.try_lock() {
                break g;
            }
            std::thread::yield_now();
        };
        st.add(priority, task, t);
        drop(st);
        self.timer_gate.update(t);
    }

    fn redispatch_task(&self, task: Arc<dyn Task>, t: Time) {
        let priority = task.priority();
        let mut st = loop {
            if let Ok(g) = self.state.try_lock() {
                break g;
            }
            std::thread::yield_now();
        };
        st.add(priority, task, t);
        drop(st);
        self.timer_gate.update(t);
    }

    async fn redispatch(&self, target_size: usize) {
        let (tx, rx) = oneshot::channel();
        let ntf = RedispatchNotification { target_size: target_size as i64, done: Some(tx) };
        tokio::select! {
            r = self.redispatch_tx.send(ntf) => {
                if r.is_ok() {
                    let _ = rx.await;
                }
            }
            _ = self.shutdown.cancelled() => {}
        }
    }

    fn size(&self) -> usize {
        loop {
            if let Ok(g) = self.state.try_lock() {
                return g.size();
            }
            std::thread::yield_now();
        }
    }
}
