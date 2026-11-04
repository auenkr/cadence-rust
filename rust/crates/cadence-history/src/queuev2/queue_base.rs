//! Port of queuev2/queue_base.go

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use super::alert::Alert;
use super::convert::{from_persistence_queue_state, to_persistence_queue_state};
use super::deps::*;
use super::mitigator::{new_mitigator, Mitigator, MitigatorOptions};
use super::monitor::{new_monitor, Monitor, MonitorOptions};
use super::predicate::Predicate;
use super::queue_reader::QueueReader;
use super::queue_state::{QueueState, VirtualSliceState};
use super::queue_reader_cached_base::CachedQueueReader;
use super::range::Range;
use super::virtual_queue::VirtualQueueOptions;
use super::virtual_queue_manager::{
    new_virtual_queue_manager, VirtualQueueManager, VirtualQueueManagerOptions,
};
use super::virtual_slice::new_virtual_slice;
use super::pending_task_tracker::new_pending_task_tracker;

pub const ALERT_CH_SIZE: usize = 10;
pub const NON_ROOT_QUEUE_MAX_PENDING_TASK_COEFFICIENT: f64 = 0.8;

#[derive(Clone)]
pub struct Options {
    pub page_size: IntFn,
    pub delete_batch_size: IntFn,
    pub max_poll_rps: IntFn,
    pub max_poll_interval: DurationFn,
    pub max_poll_interval_jitter_coefficient: FloatFn,
    pub update_ack_interval: DurationFn,
    pub update_ack_interval_jitter_coefficient: FloatFn,
    pub redispatch_interval: DurationFn,
    pub max_pending_tasks_count: IntFn,
    pub poll_backoff_interval: DurationFn,
    pub poll_backoff_interval_jitter_coefficient: FloatFn,
    pub virtual_slice_force_append_interval: DurationFn,

    pub critical_pending_task_count: IntFn,
    pub enable_pending_task_count_alert: BoolFn,
    pub max_virtual_queue_count: IntFn,

    pub enable_validator: BoolFn,
    pub validation_interval: DurationFn,
    pub max_start_jitter_interval: DurationFn,
}

struct BaseState {
    exclusive_ack_level: HistoryTaskKey,
    new_virtual_slice_state: VirtualSliceState,
}

pub struct QueueBase {
    pub shard: Arc<dyn ShardContext>,
    pub task_processor: Arc<dyn Processor>,
    pub logger: Arc<dyn Logger>,
    pub metrics_client: Arc<dyn MetricsClient>,
    pub metrics_scope: Arc<dyn MetricsScope>,
    pub category: HistoryTaskCategory,
    pub options: Options,
    pub time_source: Arc<dyn TimeSource>,
    pub task_initializer: TaskInitializer,

    pub rescheduler: Arc<dyn Rescheduler>,
    pub queue_reader: Arc<dyn QueueReader>,
    pub monitor: Arc<dyn Monitor>,
    pub mitigator: Arc<dyn Mitigator>,
    update_queue_state_timer: StdMutex<Option<Arc<dyn Timer>>>,
    /// Set by `new_cached_queue`; replaces Go's wrapping of `updateQueueStateFn`.
    read_level_listener: StdMutex<Option<Arc<dyn CachedQueueReader>>>,
    pub virtual_queue_manager: Arc<dyn VirtualQueueManager>,
    state: Mutex<BaseState>,
    alert_tx: mpsc::Sender<Alert>,
    alert_rx: StdMutex<Option<mpsc::Receiver<Alert>>>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_queue_base(
    env: &Arc<dyn QueueEnv>,
    shard: Arc<dyn ShardContext>,
    task_processor: Arc<dyn Processor>,
    logger: Arc<dyn Logger>,
    metrics_client: Arc<dyn MetricsClient>,
    metrics_scope: Arc<dyn MetricsScope>,
    category: HistoryTaskCategory,
    task_executor: Arc<dyn TaskExecutor>,
    queue_reader: Arc<dyn QueueReader>,
    options: Options,
) -> QueueBase {
    let time_source = shard.get_time_source();
    let persistence_queue_state = match shard.get_queue_state(category) {
        Ok(s) => s,
        Err(e) => {
            logger.fatal(&format!(
                "Failed to get queue state, probably task category is not supported error={e} category={category:?}"
            ));
            panic!("Failed to get queue state: {e}");
        }
    };
    logger.info(&format!("loading queue state queue-state={persistence_queue_state:?}"));
    let queue_state = from_persistence_queue_state(&persistence_queue_state);
    let (exclusive_ack_level, _) = get_exclusive_ack_level_and_max_queue_id_from_queue_state(&queue_state);

    let rescheduler = env.new_rescheduler(
        &shard,
        task_processor.clone(),
        logger.clone(),
        metrics_scope.clone(),
    );
    let queue_type = if category == HistoryTaskCategory::TRANSFER {
        QueueType::Transfer
    } else {
        QueueType::Timer
    };
    let task_initializer: TaskInitializer = {
        let env = env.clone();
        let shard = shard.clone();
        let executor = task_executor.clone();
        let processor = task_processor.clone();
        let rescheduler = rescheduler.clone();
        let logger = logger.clone();
        Arc::new(move |t: PTask| {
            env.new_history_task(
                &shard,
                t,
                queue_type,
                executor.clone(),
                processor.clone(),
                rescheduler.clone(),
                logger.clone(),
            )
        })
    };
    let monitor = new_monitor(
        category,
        MonitorOptions {
            critical_pending_task_count: options.critical_pending_task_count.clone(),
            enable_pending_task_count_alert: options.enable_pending_task_count_alert.clone(),
        },
    );
    let critical = options.critical_pending_task_count.clone();
    let virtual_queue_manager: Arc<dyn VirtualQueueManager> = new_virtual_queue_manager(
        task_processor.clone(),
        rescheduler.clone(),
        task_initializer.clone(),
        queue_reader.clone(),
        logger.clone(),
        metrics_scope.clone(),
        time_source.clone(),
        env.new_dynamic_rate_limiter(options.max_poll_rps.clone()),
        monitor.clone(),
        VirtualQueueManagerOptions {
            root_queue_options: VirtualQueueOptions {
                page_size: options.page_size.clone(),
                max_pending_tasks_count: options.max_pending_tasks_count.clone(),
                poll_backoff_interval: options.poll_backoff_interval.clone(),
                poll_backoff_interval_jitter_coefficient: options
                    .poll_backoff_interval_jitter_coefficient
                    .clone(),
            },
            non_root_queue_options: VirtualQueueOptions {
                page_size: options.page_size.clone(),
                max_pending_tasks_count: Arc::new(move || {
                    (critical() as f64 * NON_ROOT_QUEUE_MAX_PENDING_TASK_COEFFICIENT) as i64
                }),
                poll_backoff_interval: options.poll_backoff_interval.clone(),
                poll_backoff_interval_jitter_coefficient: options
                    .poll_backoff_interval_jitter_coefficient
                    .clone(),
            },
            virtual_slice_force_append_interval: options.virtual_slice_force_append_interval.clone(),
        },
        queue_state.virtual_queue_states.clone().into_iter().collect::<HashMap<_, _>>(),
    );
    let mitigator = new_mitigator(
        virtual_queue_manager.clone(),
        monitor.clone(),
        logger.clone(),
        metrics_scope.clone(),
        MitigatorOptions {
            max_virtual_queue_count: options.max_virtual_queue_count.clone(),
        },
    );
    let (alert_tx, alert_rx) = mpsc::channel(ALERT_CH_SIZE);
    QueueBase {
        shard,
        task_processor,
        logger,
        metrics_client,
        metrics_scope,
        category,
        options,
        time_source,
        task_initializer,
        rescheduler,
        queue_reader,
        monitor,
        mitigator,
        update_queue_state_timer: StdMutex::new(None),
        read_level_listener: StdMutex::new(None),
        virtual_queue_manager,
        state: Mutex::new(BaseState {
            exclusive_ack_level,
            new_virtual_slice_state: VirtualSliceState {
                range: Range {
                    inclusive_min_task_key: queue_state.exclusive_max_read_level,
                    exclusive_max_task_key: HistoryTaskKey::maximum(),
                },
                predicate: Predicate::new_universal(),
            },
        }),
        alert_tx,
        alert_rx: StdMutex::new(Some(alert_rx)),
    }
}

impl QueueBase {
    fn jit_update_ack_interval(&self) -> Duration {
        jit_duration(
            (self.options.update_ack_interval)(),
            (self.options.update_ack_interval_jitter_coefficient)(),
        )
    }

    /// Receiver side of Go's `alertCh`; the queue's run loop takes it once.
    pub fn take_alert_rx(&self) -> Option<mpsc::Receiver<Alert>> {
        self.alert_rx.lock().unwrap().take()
    }

    /// Go's `q.updateQueueStateTimer` (valid after `start`).
    pub fn update_queue_state_timer(&self) -> Option<Arc<dyn Timer>> {
        self.update_queue_state_timer.lock().unwrap().clone()
    }

    pub async fn start(&self) {
        self.rescheduler.start();
        self.virtual_queue_manager.start().await;

        *self.update_queue_state_timer.lock().unwrap() = Some(
            self.time_source
                .new_timer(self.jit_update_ack_interval()),
        );

        self.monitor.subscribe(self.alert_tx.clone());
    }

    pub async fn stop(&self) {
        self.monitor.unsubscribe();
        if let Some(t) = self.update_queue_state_timer() {
            t.stop();
        }
        self.virtual_queue_manager.stop().await;
        self.rescheduler.stop();
    }

    /// Go `q.newVirtualSliceState.Range.InclusiveMinTaskKey` (read unlocked in Go).
    pub async fn new_virtual_slice_inclusive_min(&self) -> HistoryTaskKey {
        self.state.lock().await.new_virtual_slice_state.range.inclusive_min_task_key
    }

    pub fn category(&self) -> HistoryTaskCategory {
        self.category
    }

    pub fn failover_domain(&self, domain_ids: &std::collections::HashSet<String>) {
        self.rescheduler.reschedule_domains(domain_ids);
    }

    pub async fn handle_action(
        &self,
        _cluster_name: &str,
        _action: &Action,
    ) -> Result<Option<ActionResult>, DepError> {
        Ok(None)
    }

    pub fn lock_task_processing(&self) {}

    pub fn unlock_task_processing(&self) {}

    pub async fn process_new_tasks(&self) -> bool {
        let mut new_exclusive_max_task_key = self
            .shard
            .update_if_needed_and_get_queue_max_read_level(
                self.category,
                &self.shard.get_current_cluster_name(),
            );
        if self.category.type_() == HISTORY_TASK_CATEGORY_TYPE_IMMEDIATE {
            new_exclusive_max_task_key =
                HistoryTaskKey::new_immediate(new_exclusive_max_task_key.get_task_id() + 1);
        }

        let mut st = self.state.lock().await;
        let (new_virtual_slice_state, remaining_virtual_slice_state) = match st
            .new_virtual_slice_state
            .try_split_by_task_key(new_exclusive_max_task_key)
        {
            Some(x) => x,
            None => return false,
        };
        st.new_virtual_slice_state = remaining_virtual_slice_state;
        drop(st);

        let new_virtual_slice = new_virtual_slice(
            new_virtual_slice_state.clone(),
            self.task_initializer.clone(),
            self.queue_reader.clone(),
            new_pending_task_tracker(),
            self.logger.clone(),
        );

        self.logger.debug(&format!(
            "processing new tasks inclusiveMinTaskKey={:?} exclusiveMaxTaskKey={:?}",
            new_virtual_slice_state.range.inclusive_min_task_key,
            new_virtual_slice_state.range.exclusive_max_task_key
        ));
        self.virtual_queue_manager
            .add_new_virtual_slice_to_root_queue(new_virtual_slice)
            .await;
        true
    }

    pub fn set_read_level_listener(&self, r: Arc<dyn CachedQueueReader>) {
        *self.read_level_listener.lock().unwrap() = Some(r);
    }

    /// Go's `q.updateQueueStateFn` (original, or wrapped by cachedQueue).
    pub async fn update_queue_state_fn(&self, ctx: &CancelToken) {
        self.update_queue_state(ctx).await;
        let reader = self.read_level_listener.lock().unwrap().clone();
        if let Some(reader) = reader {
            let mut read_level = self.virtual_queue_manager.get_min_read_level().await;
            if read_level.equal(&HistoryTaskKey::maximum()) {
                read_level = self.state.lock().await.exclusive_ack_level;
            }
            reader.update_read_level(read_level);
        }
    }

    pub async fn update_queue_state(&self, _ctx: &CancelToken) {
        self.metrics_scope.inc_counter(metric::ACK_LEVEL_UPDATE_COUNTER);
        let vq_states = self.virtual_queue_manager.update_and_get_state().await;
        let mut st = self.state.lock().await;
        let queue_state = QueueState {
            virtual_queue_states: vq_states.into_iter().collect(),
            exclusive_max_read_level: st.new_virtual_slice_state.range.inclusive_min_task_key,
        };
        let (new_exclusive_ack_level, max_queue_id) =
            get_exclusive_ack_level_and_max_queue_id_from_queue_state(&queue_state);
        self.metrics_scope
            .update_gauge(metric::VIRTUAL_QUEUE_COUNT_GAUGE, (max_queue_id + 1) as f64);

        let pending_task_count = self.monitor.get_total_pending_task_count();
        if self.category == HistoryTaskCategory::TRANSFER {
            self.metrics_client.record_timer(
                metric::SHARD_INFO_SCOPE,
                metric::SHARD_INFO_TRANSFER_ACTIVE_PENDING_TASKS_TIMER,
                Duration::from_nanos(pending_task_count.max(0) as u64),
            );
            self.metrics_client
                .scope(metric::SHARD_INFO_SCOPE)
                .int_exponential_histogram(
                    metric::SHARD_INFO_TRANSFER_ACTIVE_PENDING_TASKS_HISTOGRAM,
                    pending_task_count,
                );
        } else if self.category == HistoryTaskCategory::TIMER {
            self.metrics_client.record_timer(
                metric::SHARD_INFO_SCOPE,
                metric::SHARD_INFO_TIMER_ACTIVE_PENDING_TASKS_TIMER,
                Duration::from_nanos(pending_task_count.max(0) as u64),
            );
            self.metrics_client
                .scope(metric::SHARD_INFO_SCOPE)
                .int_exponential_histogram(
                    metric::SHARD_INFO_TIMER_ACTIVE_PENDING_TASKS_HISTOGRAM,
                    pending_task_count,
                );
        }

        self.metrics_scope
            .update_gauge(metric::PENDING_TASK_GAUGE, pending_task_count as f64);

        self.logger.debug(&format!(
            "complete history tasks oldExclusiveAckLevel={:?} newExclusiveAckLevel={:?}",
            st.exclusive_ack_level, new_exclusive_ack_level
        ));
        if new_exclusive_ack_level.compare(&st.exclusive_ack_level) > 0 {
            self.metrics_scope.inc_counter(metric::TASK_BATCH_COMPLETE_COUNTER);
            let mut inclusive_min_task_key = st.exclusive_ack_level;
            let mut exclusive_max_task_key = new_exclusive_ack_level;
            if self.category.type_() == HISTORY_TASK_CATEGORY_TYPE_SCHEDULED {
                inclusive_min_task_key =
                    HistoryTaskKey::new(inclusive_min_task_key.get_scheduled_time(), 0);
                exclusive_max_task_key =
                    HistoryTaskKey::new(exclusive_max_task_key.get_scheduled_time(), 0);
            }
            loop {
                let page_size = (self.options.delete_batch_size)();
                let resp = self
                    .shard
                    .get_execution_manager()
                    .range_complete_history_task(RangeCompleteHistoryTaskRequest {
                        task_category: self.category,
                        inclusive_min_task_key,
                        exclusive_max_task_key,
                        page_size,
                        shard_id: Some(self.shard.get_shard_id()),
                    })
                    .await;
                let resp = match resp {
                    Ok(r) => r,
                    Err(e) => {
                        self.logger.error(&format!(
                            "Failed to range complete history tasks error={e}"
                        ));
                        self.reset_update_timer();
                        return;
                    }
                };
                if !has_more_rows_to_delete(resp.tasks_completed, page_size) {
                    break;
                }
            }
            st.exclusive_ack_level = new_exclusive_ack_level;
        }
        drop(st);

        let persistence_queue_state = to_persistence_queue_state(&queue_state);
        self.logger.debug(&format!(
            "store queue state queue-state={persistence_queue_state:?} pendingTaskCount={pending_task_count}"
        ));
        if let Err(e) = self
            .shard
            .update_queue_state(self.category, persistence_queue_state)
        {
            self.logger
                .error(&format!("Failed to update queue state error={e}"));
            self.metrics_scope
                .inc_counter(metric::ACK_LEVEL_UPDATE_FAILED_COUNTER);
        }

        self.reset_update_timer();
    }

    fn reset_update_timer(&self) {
        if let Some(t) = self.update_queue_state_timer() {
            t.reset(self.jit_update_ack_interval());
        }
    }

    pub async fn handle_alert(&self, ctx: &CancelToken, alert: Option<Alert>) {
        let alert = match alert {
            None => return,
            Some(a) => a,
        };
        self.mitigator.mitigate(alert).await;
        self.update_queue_state_fn(ctx).await;
    }
}

pub fn get_exclusive_ack_level_and_max_queue_id_from_queue_state(
    state: &QueueState,
) -> (HistoryTaskKey, i64) {
    let mut max_queue_id = 0i64;
    let mut new_exclusive_ack_level = state.exclusive_max_read_level;
    for (queue_id, virtual_queue_state) in &state.virtual_queue_states {
        if !virtual_queue_state.is_empty() {
            new_exclusive_ack_level = min_history_task_key(
                new_exclusive_ack_level,
                virtual_queue_state[0].range.inclusive_min_task_key,
            );
        }
        max_queue_id = max_queue_id.max(*queue_id);
    }
    (new_exclusive_ack_level, max_queue_id)
}
