//! Port of queuev2/queue_immediate.go

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::deps::*;
use super::interface::{Daemon, Queue};
use super::queue_base::{new_queue_base, Options, QueueBase};
use super::queue_reader::QueueReader;
use super::virtual_queue::{DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED};

pub struct ImmediateQueue {
    pub base: Arc<QueueBase>,
    notify_tx: mpsc::Sender<()>,
    notify_rx: StdMutex<Option<mpsc::Receiver<()>>>,

    status: AtomicI32,
    shutdown: StdMutex<Option<JoinHandle<()>>>,
    ctx: CancelToken,
    last_poll_time: StdMutex<Time>,
    weak: Weak<ImmediateQueue>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_immediate_queue(
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
) -> Arc<ImmediateQueue> {
    let (notify_tx, notify_rx) = mpsc::channel(1);
    Arc::new_cyclic(|weak| ImmediateQueue {
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
        notify_tx,
        notify_rx: StdMutex::new(Some(notify_rx)),
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        shutdown: StdMutex::new(None),
        ctx: CancelToken::new(),
        last_poll_time: StdMutex::new(zero_time()),
    })
}

impl ImmediateQueue {
    fn notify(&self) {
        let _ = self.notify_tx.try_send(());
    }

    async fn process_poll_timer(&self, poll_timer: &Arc<dyn Timer>) {
        let opts = &self.base.options;
        let last = *self.last_poll_time.lock().unwrap();
        let backoff = chrono::Duration::from_std((opts.poll_backoff_interval)()).unwrap_or_default();
        if last + backoff < self.base.time_source.now() {
            let new_tasks = self.base.process_new_tasks().await;
            if new_tasks {
                self.base
                    .logger
                    .info("processing new tasks because poll timer fired");
            }
            *self.last_poll_time.lock().unwrap() = self.base.time_source.now();
        }
        poll_timer.reset(jit_duration(
            (opts.max_poll_interval)(),
            (opts.max_poll_interval_jitter_coefficient)(),
        ));
    }

    async fn process_event_loop(self: Arc<Self>, mut notify_rx: mpsc::Receiver<()>) {
        let opts = &self.base.options;
        let poll_timer = self.base.time_source.new_timer(jit_duration(
            (opts.max_poll_interval)(),
            (opts.max_poll_interval_jitter_coefficient)(),
        ));
        let mut alert_rx = self.base.take_alert_rx().expect("alert receiver already taken");
        let update_timer = self
            .base
            .update_queue_state_timer()
            .expect("queue base not started");
        loop {
            tokio::select! {
                Some(()) = notify_rx.recv() => {
                    self.base.process_new_tasks().await;
                    *self.last_poll_time.lock().unwrap() = self.base.time_source.now();
                }
                _ = poll_timer.fired() => {
                    self.process_poll_timer(&poll_timer).await;
                }
                _ = update_timer.fired() => {
                    self.base.update_queue_state_fn(&self.ctx).await;
                }
                alert = alert_rx.recv() => {
                    self.base.handle_alert(&self.ctx, alert).await;
                }
                _ = self.ctx.cancelled() => {
                    poll_timer.stop();
                    return;
                }
            }
        }
    }
}

#[async_trait]
impl Daemon for ImmediateQueue {
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

        let rx = self.notify_rx.lock().unwrap().take().expect("notify receiver taken");
        let me = self.weak.upgrade().expect("queue dropped");
        *self.shutdown.lock().unwrap() = Some(tokio::spawn(me.process_event_loop(rx)));

        self.notify();
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
        let h = self.shutdown.lock().unwrap().take();
        if let Some(h) = h {
            let _ = h.await;
        }

        self.base.stop().await;
        self.base.logger.info("History queue state changed LifeCycleStopped");
    }
}

#[async_trait]
impl Queue for ImmediateQueue {
    fn category(&self) -> HistoryTaskCategory {
        self.base.category()
    }

    fn notify_new_task(&self, _cluster_name: &str, info: &NotifyTaskInfo) {
        let num_tasks = info.tasks.len();
        if num_tasks == 0 {
            return;
        }
        self.notify();
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
