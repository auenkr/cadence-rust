//! Port of `go/service/history/task/task.go`.

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;

use super::constants::*;
use super::deps::*;
use super::event_logger::{new_event_logger, EventLogger};
use super::interface::*;

/// `redispatchError` is the error indicating that the timer / transfer task should be
/// redispatched and retried. Represented as `TaskError::Redispatch(reason)`.
pub fn new_redispatch_error(reason: impl Into<String>) -> TaskError {
    TaskError::Redispatch(reason.into())
}

/// `isRedispatchErr`
pub fn is_redispatch_err(err: &TaskError) -> bool {
    err.is_redispatch()
}

pub const ACTIVE_TASK_REDISPATCH_INITIAL_INTERVAL: Duration = Duration::from_secs(5);
pub const STANDBY_TASK_REDISPATCH_INITIAL_INTERVAL: Duration = Duration::from_secs(30);
pub const REDISPATCH_BACKOFF_COEFFICIENT: f64 = 1.05;
pub const REDISPATCH_MAX_BACKOFF_INTERVAL: Duration = Duration::from_secs(2 * 60);
pub const REDISPATCH_FAILURE_BACKOFF_INTERVAL: Duration = Duration::from_secs(2);

/// `ErrTaskDiscarded` is the error indicating that the timer / transfer task is pending for too long and discarded.
pub fn err_task_discarded() -> TaskError {
    TaskError::TaskDiscarded
}
/// `ErrTaskPendingActive` is the error indicating that the task should be re-dispatched
pub fn err_task_pending_active() -> TaskError {
    TaskError::TaskPendingActive
}

/// `createTaskRedispatchPolicy`
pub fn create_task_redispatch_policy(initial_interval: Duration) -> RetryPolicy {
    RetryPolicy::new(initial_interval)
        .with_backoff_coefficient(REDISPATCH_BACKOFF_COEFFICIENT)
        .with_maximum_interval(REDISPATCH_MAX_BACKOFF_INTERVAL)
        .with_expiration_interval(NO_INTERVAL)
}

fn active_task_redispatch_policy() -> RetryPolicy {
    create_task_redispatch_policy(ACTIVE_TASK_REDISPATCH_INITIAL_INTERVAL)
}
fn standby_task_redispatch_policy() -> RetryPolicy {
    create_task_redispatch_policy(STANDBY_TASK_REDISPATCH_INITIAL_INTERVAL)
}

struct TaskMutable {
    state: TaskState,
    priority: i32,
    attempt: i32,
    initial_submit_time: Time,
    scope: Arc<dyn MetricsScope>,
    is_previous_executor_active: bool,
    should_process_task: bool,
}

/// Go `taskImpl`.
pub struct TaskImpl {
    task: Arc<dyn PersistenceTask>,
    shard: Arc<dyn ShardContext>,
    time_source: Arc<dyn TimeSource>,
    logger: Logger,
    event_logger: Option<Arc<dyn EventLogger>>,
    task_executor: Arc<dyn Executor>,
    task_processor: Arc<dyn Processor>,
    redispatcher: Option<Arc<dyn Redispatcher>>,
    rescheduler: Option<Arc<dyn Rescheduler>>,
    critical_retry_count: IntPropertyFn,
    // TODO: following three fields should be removed after new task lifecycle is implemented
    task_filter: Filter,
    queue_type: QueueType,
    inner: Mutex<TaskMutable>,
    weak_self: Weak<TaskImpl>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_history_task(
    shard: Arc<dyn ShardContext>,
    task_info: Arc<dyn PersistenceTask>,
    queue_type: QueueType,
    logger: Logger,
    task_filter: Filter,
    task_executor: Arc<dyn Executor>,
    task_processor: Arc<dyn Processor>,
    redispatcher: Arc<dyn Redispatcher>,
    critical_retry_count: IntPropertyFn,
) -> Arc<dyn Task> {
    TaskImpl::build(
        shard,
        task_info,
        queue_type,
        logger,
        task_filter,
        task_executor,
        task_processor,
        Some(redispatcher),
        None,
        critical_retry_count,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn new_history_task_v2(
    shard: Arc<dyn ShardContext>,
    task_info: Arc<dyn PersistenceTask>,
    queue_type: QueueType,
    logger: Logger,
    task_executor: Arc<dyn Executor>,
    task_processor: Arc<dyn Processor>,
    rescheduler: Arc<dyn Rescheduler>,
    critical_retry_count: IntPropertyFn,
) -> Arc<dyn Task> {
    let filter: Filter = Arc::new(|_task: &dyn PersistenceTask| Ok(true));
    TaskImpl::build(
        shard,
        task_info,
        queue_type,
        logger,
        filter,
        task_executor,
        task_processor,
        None,
        Some(rescheduler),
        critical_retry_count,
    )
}

fn log_event(event_logger: &Option<Arc<dyn EventLogger>>, msg: &str, detail: Vec<String>) {
    if let Some(l) = event_logger {
        l.add_event(msg, detail);
    }
}

impl TaskImpl {
    #[allow(clippy::too_many_arguments)]
    fn build(
        shard: Arc<dyn ShardContext>,
        task_info: Arc<dyn PersistenceTask>,
        queue_type: QueueType,
        logger: Logger,
        task_filter: Filter,
        task_executor: Arc<dyn Executor>,
        task_processor: Arc<dyn Processor>,
        redispatcher: Option<Arc<dyn Redispatcher>>,
        rescheduler: Option<Arc<dyn Rescheduler>>,
        critical_retry_count: IntPropertyFn,
    ) -> Arc<dyn Task> {
        let time_source = shard.get_time_source();
        let mut event_logger: Option<Arc<dyn EventLogger>> = None;
        let config = shard.get_config();
        if config.enable_debug_mode()
            && (queue_type == QueueType::ActiveTimer || queue_type == QueueType::ActiveTransfer)
            && config.enable_task_info_log_by_domain_id(task_info.get_domain_id())
        {
            let l = new_event_logger(logger.clone(), time_source.clone(), DEFAULT_TASK_EVENT_LOGGER_SIZE);
            l.add_event("Created task", vec![]);
            event_logger = Some(l);
        }
        let initial = time_source.now();
        Arc::new_cyclic(|weak| TaskImpl {
            task: task_info,
            shard,
            time_source,
            logger,
            event_logger,
            task_executor,
            task_processor,
            redispatcher,
            rescheduler,
            critical_retry_count,
            task_filter,
            queue_type,
            inner: Mutex::new(TaskMutable {
                state: TaskState::Pending,
                priority: NO_PRIORITY,
                attempt: 0,
                initial_submit_time: initial,
                scope: noop_scope(),
                is_previous_executor_active: false,
                should_process_task: false,
            }),
            weak_self: weak.clone(),
        })
    }

    fn arc_self(&self) -> Arc<TaskImpl> {
        self.weak_self.upgrade().expect("task dropped while in use")
    }

    fn reset_attempt(&self) {
        self.inner.lock().unwrap().attempt = 0;
    }

    fn scope(&self) -> Arc<dyn MetricsScope> {
        self.inner.lock().unwrap().scope.clone()
    }

    fn should_resubmit_on_nack(&self, err: &Option<TaskError>) -> bool {
        // TODO: for now only resubmit active task on Nack()
        if let Some(e) = err {
            if e.is(&TaskError::DomainBecomesActive) {
                return true;
            }
        }
        let prev_active = self.inner.lock().unwrap().is_previous_executor_active;
        self.get_attempt() < ACTIVE_TASK_RESUBMIT_MAX_ATTEMPTS
            && (self.queue_type == QueueType::ActiveTransfer
                || self.queue_type == QueueType::ActiveTimer
                || ((self.queue_type == QueueType::Transfer || self.queue_type == QueueType::Timer)
                    && prev_active))
    }

    fn backoff_duration(&self, attempt: i32) -> Duration {
        // TODO: we might need to consider using the same backoff policy for active and standby tasks,
        let prev_active = self.inner.lock().unwrap().is_previous_executor_active;
        if prev_active {
            return compute_next_delay(&active_task_redispatch_policy(), Duration::ZERO, attempt as usize);
        }
        compute_next_delay(&standby_task_redispatch_policy(), Duration::ZERO, attempt as usize)
    }
}

impl std::fmt::Debug for TaskImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.task)
    }
}

impl PersistenceTask for TaskImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_task_category(&self) -> HistoryTaskCategory {
        self.task.get_task_category()
    }
    fn get_task_type(&self) -> i32 {
        self.task.get_task_type()
    }
    fn get_domain_id(&self) -> &str {
        self.task.get_domain_id()
    }
    fn get_workflow_id(&self) -> &str {
        self.task.get_workflow_id()
    }
    fn get_run_id(&self) -> &str {
        self.task.get_run_id()
    }
    fn get_version(&self) -> i64 {
        self.task.get_version()
    }
    fn get_task_id(&self) -> i64 {
        self.task.get_task_id()
    }
    fn get_visibility_timestamp(&self) -> Time {
        self.task.get_visibility_timestamp()
    }
    fn get_original_task_list(&self) -> String {
        self.task.get_original_task_list()
    }
    fn get_original_task_list_kind(&self) -> TaskListKind {
        self.task.get_original_task_list_kind()
    }
}

#[async_trait]
impl Task for TaskImpl {
    async fn execute(&self) -> Result<()> {
        if self.state() != TaskState::Pending {
            return Ok(());
        }
        let schedule_latency = {
            let init = self.inner.lock().unwrap().initial_submit_time;
            self.time_source.now().sub(init)
        };
        let should_process = match (self.task_filter)(self.task.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                log_event(&self.event_logger, "TaskFilter execution failed", vec![detail(&e)]);
                tokio::time::sleep(LOAD_DOMAIN_ENTRY_FOR_TASK_RETRY_DELAY).await;
                return Err(e);
            }
        };
        self.inner.lock().unwrap().should_process_task = should_process;
        log_event(&self.event_logger, "Executing task", vec![detail(should_process)]);
        if !should_process {
            return Ok(());
        }

        let execution_start_time = std::time::Instant::now();
        let (execute_response, err) = self.task_executor.execute(self.arc_self()).await;
        let task_list_tagged_scope;
        {
            let mut m = self.inner.lock().unwrap();
            m.scope = execute_response.scope.clone();
            task_list_tagged_scope = m.scope.tagged(vec![get_task_list_tag(
                &self.task.get_original_task_list(),
                self.task.get_original_task_list_kind(),
            )]);
        }
        let scope = self.scope();
        if self.get_attempt() == 0 {
            task_list_tagged_scope
                .exponential_histogram(metrics::TASK_SCHEDULE_LATENCY_PER_TASK_LIST_HISTOGRAM, schedule_latency);
            // domain level metrics for the duration between task being submitted to task scheduler and being executed
            scope.record_histogram_duration(metrics::TASK_SCHEDULE_LATENCY_PER_DOMAIN, schedule_latency);
        }
        {
            let changed = self.inner.lock().unwrap().is_previous_executor_active != execute_response.is_active_task;
            if changed {
                self.reset_attempt();
            }
            self.inner.lock().unwrap().is_previous_executor_active = execute_response.is_active_task;
        }

        // deferred metrics
        let scope = self.scope();
        scope.inc_counter(metrics::TASK_REQUESTS_PER_DOMAIN);
        let processing_latency = execution_start_time.elapsed();
        scope.record_timer(metrics::TASK_PROCESSING_LATENCY_PER_DOMAIN, processing_latency);
        scope.exponential_histogram(metrics::TASK_PROCESSING_LATENCY_PER_DOMAIN_HISTOGRAM, processing_latency);
        task_list_tagged_scope.inc_counter(metrics::TASK_REQUESTS_PER_TASK_LIST);
        task_list_tagged_scope
            .exponential_histogram(metrics::TASK_PROCESSING_LATENCY_PER_TASK_LIST_HISTOGRAM, processing_latency);

        err
    }

    fn handle_err(&self, err: Option<TaskError>) -> Option<TaskError> {
        let ret = self.handle_err_inner(&err);
        if let Some(ret_err) = &ret {
            log_event(&self.event_logger, "Failed to handle error", vec![detail(ret_err)]);
            let attempt = {
                let mut m = self.inner.lock().unwrap();
                m.attempt += 1;
                m.attempt
            };
            if attempt > (self.critical_retry_count)() {
                let scope = self.scope();
                scope.record_timer(metrics::TASK_ATTEMPT_TIMER_PER_DOMAIN, Duration::from_nanos(attempt as u64));
                scope.int_exponential_histogram(metrics::TASK_ATTEMPT_PER_DOMAIN_COUNTS_HISTOGRAM, attempt as usize);
                let mut tags = vec![
                    tag::operation_critical(),
                    tag::task_type(self.get_task_type()),
                    tag::attempt_count(attempt as i64),
                ];
                if let Some(e) = &err {
                    tags.insert(0, tag::error(e));
                }
                self.logger.helper().error("Critical error processing task, retrying.", &tags);
            }
        }
        ret
    }

    fn retry_err(&self, err: &TaskError) -> bool {
        if self.state() != TaskState::Pending {
            return false;
        }
        if err.is_shard_closed().is_some()
            || err.is(&TaskError::WorkflowBusy)
            || is_redispatch_err(err)
            || err.is(&TaskError::TaskPendingActive)
            || err.is_context_timeout()
        {
            return false;
        }
        true
    }

    fn ack(&self) {
        log_event(&self.event_logger, "Acked task", vec![]);

        let (should_process, attempt, init, scope) = {
            let mut m = self.inner.lock().unwrap();
            if m.state != TaskState::Pending {
                return;
            }
            m.state = TaskState::Acked;
            (m.should_process_task, m.attempt, m.initial_submit_time, m.scope.clone())
        };
        if should_process {
            // Record attempt count as duration so timer mean ≈ average attempt count.
            scope.record_timer(metrics::TASK_ATTEMPT_TIMER_PER_DOMAIN, Duration::from_nanos(attempt as u64));

            let now = self.time_source.now();
            let latency = now.sub(init);
            let queue_latency = now.sub(self.task.get_visibility_timestamp());
            scope.int_exponential_histogram(metrics::TASK_ATTEMPT_PER_DOMAIN_COUNTS_HISTOGRAM, attempt as usize);
            scope.record_timer(metrics::TASK_LATENCY_PER_DOMAIN, latency);
            scope.exponential_histogram(metrics::TASK_LATENCY_PER_DOMAIN_HISTOGRAM, latency);
            scope.record_timer(metrics::TASK_QUEUE_LATENCY_PER_DOMAIN, queue_latency);
            scope.exponential_histogram(metrics::TASK_QUEUE_LATENCY_PER_DOMAIN_HISTOGRAM, queue_latency);

            let task_list_tagged_scope = scope.tagged(vec![get_task_list_tag(
                &self.task.get_original_task_list(),
                self.task.get_original_task_list_kind(),
            )]);
            task_list_tagged_scope.exponential_histogram(metrics::TASK_LATENCY_PER_TASK_LIST_HISTOGRAM, latency);
            task_list_tagged_scope
                .exponential_histogram(metrics::TASK_QUEUE_LATENCY_PER_TASK_LIST_HISTOGRAM, queue_latency);
        }

        if let Some(l) = &self.event_logger {
            if should_process && attempt != 0 {
                // only dump events when the task should be processed and has been retried
                l.flush_events("Task processing events");
            }
        }
    }

    async fn nack(&self, err: Option<TaskError>) {
        if self.state() != TaskState::Pending {
            return;
        }

        log_event(&self.event_logger, "Nacked task", vec![]);

        if self.should_resubmit_on_nack(&err) {
            if let Ok(true) = self.task_processor.try_submit(self.arc_self()).await {
                return;
            }
        }

        let when = self.time_source.now().add(self.backoff_duration(self.get_attempt()));
        if let Some(r) = &self.rescheduler {
            r.reschedule_task(self.arc_self(), when);
        } else if let Some(r) = &self.redispatcher {
            r.redispatch_task(self.arc_self(), when);
        }
    }

    fn cancel(&self) {
        let mut m = self.inner.lock().unwrap();
        if m.state == TaskState::Pending {
            m.state = TaskState::Canceled;
        }
    }

    fn state(&self) -> TaskState {
        self.inner.lock().unwrap().state
    }

    fn priority(&self) -> i32 {
        self.inner.lock().unwrap().priority
    }

    fn set_priority(&self, priority: i32) {
        self.inner.lock().unwrap().priority = priority;
    }

    fn get_queue_type(&self) -> QueueType {
        self.queue_type
    }

    fn get_shard(&self) -> Arc<dyn ShardContext> {
        self.shard.clone()
    }

    fn get_attempt(&self) -> i32 {
        self.inner.lock().unwrap().attempt
    }

    fn get_info(&self) -> Arc<dyn PersistenceTask> {
        self.task.clone()
    }

    fn set_initial_submit_time(&self, submit_time: Time) {
        self.inner.lock().unwrap().initial_submit_time = submit_time;
    }
}

impl TaskImpl {
    /// Body of Go `HandleErr` without the deferred attempt bookkeeping.
    fn handle_err_inner(&self, err_opt: &Option<TaskError>) -> Option<TaskError> {
        let logger = self.logger.helper();
        let mut err = match err_opt {
            None => return None,
            Some(e) => e.clone(),
        };

        log_event(&self.event_logger, "Handling task processing error", vec![detail(&err)]);

        let scope = self.scope();

        if matches!(err, TaskError::EntityNotExists(_)) {
            return None;
        } else if matches!(err, TaskError::WorkflowExecutionAlreadyCompleted(_)) {
            return None;
        }

        if self.task.as_any().is::<CloseExecutionTask>()
            && err == TaskError::MissingWorkflowStartEvent
            && self.shard.get_config().enable_drop_stuck_task_by_domain_id(self.get_domain_id())
        {
            // use domainID here to avoid accessing domainCache
            scope.inc_counter(metrics::TRANSFER_TASK_MISSING_EVENT_COUNTER_PER_DOMAIN);
            logger.error(
                "Drop close execution transfer task due to corrupted workflow history",
                &[tag::error(&err), tag::life_cycle_processing_failed()],
            );
            return None;
        }

        if err == TaskError::WorkflowBusy {
            scope.inc_counter(metrics::TASK_WORKFLOW_BUSY_PER_DOMAIN);
            return Some(err);
        }

        if err == TaskError::WorkflowRateLimited {
            // metrics are emitted within the rate limiter
            return Some(err);
        }

        // If the shard were recently closed we just return an error, so we retry in a bit.
        if let Some(closed_at) = err.is_shard_closed() {
            if self.time_source.now().sub(closed_at) < TIME_BEFORE_SHARD_CLOSED_IS_ERROR {
                return Some(err);
            }
        }

        // If the task list is not owned by the host we connected to we just return an error, so we retry in a bit
        // with the new membership information.
        if err.is_task_list_not_owned_by_host() {
            scope.inc_counter(metrics::TASK_LIST_NOT_OWNED_BY_HOST_COUNTER_PER_DOMAIN);
            return Some(err);
        }

        // this is a transient error
        if is_redispatch_err(&err) {
            scope.inc_counter(metrics::TASK_STANDBY_RETRY_COUNTER_PER_DOMAIN);
            return Some(err);
        }

        // this is a transient error during graceful failover
        if err == TaskError::TaskPendingActive {
            scope.inc_counter(metrics::TASK_PENDING_ACTIVE_COUNTER_PER_DOMAIN);
            return Some(err);
        }

        if err == TaskError::TaskDiscarded {
            scope.inc_counter(metrics::TASK_DISCARDED_PER_DOMAIN);
            return None;
        }

        if err == TaskError::MissingVersionHistories {
            self.logger.error("Encounter 2DC workflow during task processing.", &[]);
            scope.inc_counter(metrics::TASK_UNSUPPORTED_PER_DOMAIN);
            return None;
        }

        // using a fairly long timeout here because domain updates is an async process
        // which could take a fair while to be processed by the domain queue, the DB updated
        // the domain cache refeshed and then updated here.
        if err.is_domain_not_active() {
            let init = self.inner.lock().unwrap().initial_submit_time;
            if self.time_source.now().sub(init) > 5 * DOMAIN_CACHE_REFRESH_INTERVAL {
                scope.inc_counter(metrics::TASK_NOT_ACTIVE_COUNTER_PER_DOMAIN);
                // If the domain is *still* not active, drop after a while.
                return None;
            }
            return Some(err);
        }

        scope.inc_counter(metrics::TASK_FAILURES_PER_DOMAIN);

        if matches!(err, TaskError::CurrentWorkflowConditionFailed) {
            logger.error(
                "More than 2 workflow are running.",
                &[tag::error(&err), tag::life_cycle_processing_failed()],
            );
            return None;
        }

        let attempt = self.get_attempt();
        if attempt > STICKY_TASK_MAX_RETRY_COUNT && is_sticky_task_condition_error(&err) {
            // sticky task could end up into endless loop in rare cases and
            // cause worker to keep getting decision timeout unless restart.
            // return nil here to break the endless loop
            return None;
        }

        logger.error(
            "Fail to process task",
            &[tag::error(&err), tag::life_cycle_processing_failed(), tag::attempt_count(attempt as i64)],
        );
        let _ = &mut err;
        Some(err)
    }
}

/// `getOrCreateDomainTaggedScope` returns cached domain-tagged metrics scope if exists
/// otherwise, it creates a new domain-tagged scope, cache and return the scope
pub fn get_or_create_domain_tagged_scope(
    shard: &Arc<dyn ShardContext>,
    scope_idx: ScopeIdx,
    domain_id: &str,
    logger: &Logger,
) -> Arc<dyn MetricsScope> {
    let scope_cache = shard.get_service().get_domain_metrics_scope_cache();
    match scope_cache.get(domain_id, scope_idx) {
        Some(s) => s,
        None => {
            let (domain_tag, err) = get_domain_tag_by_id(shard.get_domain_cache().as_ref(), domain_id);
            let scope = shard.get_metrics_client().scope(scope_idx, vec![domain_tag]);
            match err {
                None => scope_cache.put(domain_id, scope_idx, scope.clone()),
                Some(e) => logger.error("Unable to get domainName", &[tag::error(&e)]),
            }
            scope
        }
    }
}

/// `getDomainTagByID` — the Go `(Tag, error)` pair is returned as `(Tag, Option<TaskError>)`.
pub fn get_domain_tag_by_id(domain_cache: &dyn DomainCache, domain_id: &str) -> (Tag, Option<TaskError>) {
    match domain_cache.get_domain_name(domain_id) {
        Err(e) => (domain_unknown_tag(), Some(e)),
        Ok(name) => (domain_tag(&name), None),
    }
}
