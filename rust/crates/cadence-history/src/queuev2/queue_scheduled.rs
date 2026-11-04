//! Port of queuev2/queue_scheduled.go

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::deps::*;
use super::interface::{Daemon, Queue};
use super::queue_base::{new_queue_base, Options, QueueBase};
use super::queue_reader::{LookAHeadRequest, QueueReader};
use super::virtual_queue::{DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};

pub struct ScheduledQueue {
    pub base: Arc<QueueBase>,

    timer_gate: Arc<dyn TimerGate>,
    new_timer_tx: mpsc::Sender<()>,
    new_timer_rx: StdMutex<Option<mpsc::Receiver<()>>>,
    /// Go `nextTimeLock` + `nextTime`.
    next_time: StdMutex<Time>,

    status: AtomicI32,
    shutdown: StdMutex<Option<JoinHandle<()>>>,
    ctx: CancelToken,
    weak: Weak<ScheduledQueue>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_scheduled_queue(
    env: &Arc<dyn QueueEnv>,
    shard: Arc<dyn ShardContext>,
    category: HistoryTaskCategory,
    task_processor: Arc<dyn Processor>,
    task_executor: Arc<dyn TaskExecutor>,
    logger: Arc<dyn Logger>,
    metrics_client: Arc<dyn MetricsClient>,
    metrics_scope: Arc<dyn MetricsScope>,
    queue_reader: Arc<dyn QueueReader>,
    options: Options,
) -> Arc<ScheduledQueue> {
    let (new_timer_tx, new_timer_rx) = mpsc::channel(1);
    let timer_gate = env.new_timer_gate(shard.get_time_source());
    Arc::new_cyclic(|weak| ScheduledQueue {
        weak: weak.clone(),
        base: Arc::new(new_queue_base(
            env,
            shard,
            task_processor,
            logger,
            metrics_client,
            metrics_scope,
            category,
            task_executor,
            queue_reader,
            options,
        )),
        timer_gate,
        new_timer_tx,
        new_timer_rx: StdMutex::new(Some(new_timer_rx)),
        next_time: StdMutex::new(zero_time()),
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        shutdown: StdMutex::new(None),
        ctx: CancelToken::new(),
    })
}

impl ScheduledQueue {
    fn notify(&self, t: Time) {
        let mut next_time = self.next_time.lock().unwrap();
        if *next_time == zero_time() || t < *next_time
        {
            *next_time = t;
            let _ = self.new_timer_tx.try_send(());
        }
    }

    fn process_next_time(&self) {
        let next_time = {
            let mut g = self.next_time.lock().unwrap();
            std::mem::replace(&mut *g, zero_time())
        };
        self.timer_gate.update(next_time);
    }

    async fn process_event_loop(self: Arc<Self>, mut new_timer_rx: mpsc::Receiver<()>) {
        let mut alert_rx = self.base.take_alert_rx().expect("alert receiver already taken");
        let update_timer = self
            .base
            .update_queue_state_timer()
            .expect("queue base not started");
        loop {
            tokio::select! {
                Some(()) = new_timer_rx.recv() => {
                    self.process_next_time();
                }
                _ = self.timer_gate.fired() => {
                    self.base.process_new_tasks().await;
                    self.look_ahead_task().await;
                }
                _ = update_timer.fired() => {
                    self.base.update_queue_state_fn(&self.ctx).await;
                }
                alert = alert_rx.recv() => {
                    self.base.handle_alert(&self.ctx, alert).await;
                }
                _ = self.ctx.cancelled() => {
                    return;
                }
            }
        }
    }

    async fn look_ahead_task(&self) {
        let min_key = self
            .base
            .new_virtual_slice_inclusive_min()
            .await;
        let resp = self
            .base
            .queue_reader
            .look_ahead(&LookAHeadRequest {
                inclusive_min_task_key: min_key,
            })
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                self.timer_gate.update(min_key.get_scheduled_time());
                self.base
                    .logger
                    .error(&format!("Failed to look ahead task error={e}"));
                return;
            }
        };
        if let Some(task) = resp.task {
            self.base.logger.debug(&format!(
                "look ahead found next task nextTaskTime={:?}",
                task.get_visibility_timestamp()
            ));
            self.timer_gate.update(task.get_visibility_timestamp());
            return;
        }
        self.base.logger.debug(&format!(
            "look ahead: no task in window, scheduling until look-ahead max time lookAheadMaxTime={:?}",
            resp.look_ahead_max_time
        ));
        self.timer_gate.update(resp.look_ahead_max_time);
    }
}

#[async_trait]
impl Daemon for ScheduledQueue {
    async fn start(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_INITIALIZED,
                DAEMON_STATUS_STARTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        self.base.logger.info("History queue state changed LifeCycleStarting");

        self.base.start().await;

        let rx = self.new_timer_rx.lock().unwrap().take().expect("receiver taken");
        let me = self.weak.upgrade().expect("queue dropped");
        *self.shutdown.lock().unwrap() = Some(tokio::spawn(me.process_event_loop(rx)));

        self.notify(zero_time());
        self.base.logger.info("History queue state changed LifeCycleStarted");
    }

    async fn stop(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_STARTED,
                DAEMON_STATUS_STOPPED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        self.base.logger.info("History queue state changed LifeCycleStopping");

        self.ctx.cancel();
        self.timer_gate.stop();
        let h = self.shutdown.lock().unwrap().take();
        if let Some(h) = h {
            let _ = h.await;
        }
        self.base.stop().await;
        self.base.logger.info("History queue state changed LifeCycleStopped");
    }
}

#[async_trait]
impl Queue for ScheduledQueue {
    fn category(&self) -> HistoryTaskCategory {
        self.base.category()
    }

    fn notify_new_task(&self, _cluster_name: &str, info: &NotifyTaskInfo) {
        let num_tasks = info.tasks.len();
        if num_tasks == 0 {
            return;
        }
        let mut next_time = info.tasks[0].get_visibility_timestamp();
        for t in &info.tasks[1..] {
            let ts = t.get_visibility_timestamp();
            if ts < next_time {
                next_time = ts;
            }
        }
        self.notify(next_time);
        self.base
            .metrics_scope
            .add_counter(metric::NEW_HISTORY_TASK_COUNTER, num_tasks as i64);
    }

    fn failover_domain(&self, domain_ids: &HashSet<String>) {
        self.base.failover_domain(domain_ids);
    }

    async fn handle_action(
        &self,
        cluster_name: &str,
        action: &Action,
    ) -> Result<Option<ActionResult>, DepError> {
        self.base.handle_action(cluster_name, action).await
    }

    fn lock_task_processing(&self) {
        self.base.lock_task_processing();
    }

    fn unlock_task_processing(&self) {
        self.base.unlock_task_processing();
    }
}
