//! Glue between the queue v1 processors (`queue::*`, which use the `queue::deps` trait universe) and
//! the task package (`task::*`, which has its own universe): rate limiter, task / processor /
//! redispatcher adapters, the `HistoryTaskFactory` (Go `task.NewHistoryTask`) and the builders for
//! the transfer and timer queue processors.
//!
//! `TaskInfoAdapter` decodes the full task blob (`SqlHistoryTask`) into the task-type specific
//! structs (`task::deps::*Task`) that the executors downcast to.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::queue::deps as q;
use crate::queue::shard_deps::{HistoryTaskFactory, QueueShardContext, QueueType as QQueueType, TaskFilter};
use crate::queue::task_allocator::TaskAllocator;
use crate::queue::timer_queue_processor::{TimerQueueDeps, TimerQueueProcessor};
use crate::queue::transfer_queue_processor::{TransferQueueDeps, TransferQueueProcessor};
use crate::task::deps as t;
use crate::task::history_task::new_history_task;
use crate::task::interface as ti;

// ---------------------------------------------------------------------------
// rate limiter
// ---------------------------------------------------------------------------

/// Go `quotas.NewDynamicRateLimiter(rps)`: delegates to the x/time/rate-faithful
/// `cadence_common::dynamic_rate_limiter::DynamicRateLimiter` (bucket starts full, burst
/// `max(ceil(rps), 1)`, rps re-read at most every 5s, cancelled waits return their token).
pub struct DynamicRateLimiter {
    inner: cadence_common::dynamic_rate_limiter::DynamicRateLimiter,
}

impl DynamicRateLimiter {
    pub fn new(rps: q::IntFn) -> Arc<Self> {
        Arc::new(Self {
            inner: cadence_common::dynamic_rate_limiter::DynamicRateLimiter::new(Box::new(
                move || rps() as f64,
            )),
        })
    }

    /// Go `ratelimiter.Wait` reservation step at clock sample `now` (unix nanos).
    pub(crate) fn reserve_at(
        &self,
        now: i64,
        max_wait: Duration,
    ) -> Option<(Duration, cadence_common::rate_limiter::Reservation)> {
        self.inner.reserve_at(now, Some(max_wait))
    }

    pub(crate) fn reserve(
        &self,
        max_wait: Duration,
    ) -> Option<(Duration, cadence_common::rate_limiter::Reservation)> {
        self.inner.reserve_now(Some(max_wait))
    }

    pub(crate) fn cancel(&self, r: &cadence_common::rate_limiter::Reservation) {
        self.inner.cancel(r)
    }
}

#[async_trait]
impl q::Limiter for DynamicRateLimiter {
    async fn wait(&self, timeout: Duration) -> Result<(), q::QueueError> {
        match self.reserve(timeout) {
            None => Err(q::QueueError::Other("rate: Wait(n=1) would exceed context deadline".into())),
            Some((wait, _)) => {
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod dynamic_rate_limiter_tests {
    use super::*;

    const MS: i64 = 1_000_000;

    /// Independent reference of x/time/rate single-token semantics (float tokens, reserve then sleep).
    struct Reference {
        rate: f64,
        burst: f64,
        tokens: f64,
        last: i64,
    }
    impl Reference {
        fn reserve(&mut self, now: i64) -> i64 {
            let t = (self.tokens + (now - self.last).max(0) as f64 / 1e9 * self.rate).min(self.burst) - 1.0;
            self.tokens = t;
            self.last = now;
            if t < 0.0 { ((-t / self.rate) * 1e9) as i64 } else { 0 }
        }
    }

    #[test]
    fn bursts_up_to_ceil_rps_then_paces() {
        let l = DynamicRateLimiter::new(Arc::new(|| 20));
        let t0 = 1_700_000_000_000_000_000i64;
        for _ in 0..20 {
            assert_eq!(l.reserve_at(t0, Duration::from_secs(1)).unwrap().0, Duration::ZERO);
        }
        let w = l.reserve_at(t0, Duration::from_secs(1)).unwrap().0;
        assert!((w.as_secs_f64() - 0.05).abs() < 1e-6, "{w:?}");
        // a wait beyond the deadline consumes nothing
        assert!(l.reserve_at(t0, Duration::from_millis(10)).is_none());
        assert!(l.reserve_at(t0 + 2_000_000_000, Duration::ZERO).is_some());
    }

    #[test]
    fn zero_rps_rejects() {
        let l = DynamicRateLimiter::new(Arc::new(|| 0));
        assert!(l.reserve_at(1_700_000_000_000_000_000, Duration::from_secs(10)).is_none());
    }

    #[test]
    fn replay_request_series_matches_reference_and_never_waits_longer() {
        let l = DynamicRateLimiter::new(Arc::new(|| 20));
        let t0 = 1_700_000_000_000_000_000i64;
        let mut r = Reference { rate: 20.0, burst: 20.0, tokens: 20.0, last: t0 };
        // bursty / sparse / closely packed arrivals (ms offsets)
        let mut now = t0;
        for (i, gap_ms) in [0, 0, 1, 2, 0, 3, 40, 0, 0, 120, 1, 1, 1, 500, 0, 0, 0, 0, 7, 7, 7, 900, 0, 1]
            .iter()
            .cycle()
            .take(400)
            .enumerate()
        {
            now += gap_ms * MS;
            let want = r.reserve(now);
            let got = l.reserve_at(now, Duration::MAX).unwrap().0.as_nanos() as i64;
            assert!((got - want).abs() <= 1, "req {i} now+{}ms: got {got} want {want}", (now - t0) / MS);
            // Go sleeps then proceeds; the sleeping caller does not advance the limiter clock.
        }
    }

    #[test]
    fn cancelled_wait_returns_token() {
        let l = DynamicRateLimiter::new(Arc::new(|| 1));
        let t0 = 1_700_000_000_000_000_000i64;
        assert_eq!(l.reserve_at(t0, Duration::MAX).unwrap().0, Duration::ZERO);
        let (d, res) = l.reserve_at(t0, Duration::MAX).unwrap();
        assert_eq!(d, Duration::from_secs(1));
        l.cancel(&res);
        assert_eq!(l.reserve_at(t0, Duration::MAX).unwrap().0, Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------------------
// enum / error / time conversions
// ---------------------------------------------------------------------------

fn to_task_queue_type(t: QQueueType) -> ti::QueueType {
    match t {
        QQueueType::ActiveTransfer => ti::QueueType::ActiveTransfer,
        QQueueType::StandbyTransfer => ti::QueueType::StandbyTransfer,
        QQueueType::ActiveTimer => ti::QueueType::ActiveTimer,
        QQueueType::StandbyTimer => ti::QueueType::StandbyTimer,
    }
}

fn to_task_category(c: q::HistoryTaskCategory) -> t::HistoryTaskCategory {
    match c {
        q::HistoryTaskCategory::Transfer => t::HistoryTaskCategory::Transfer,
        q::HistoryTaskCategory::Timer => t::HistoryTaskCategory::Timer,
    }
}

fn from_task_category(c: t::HistoryTaskCategory) -> q::HistoryTaskCategory {
    match c {
        t::HistoryTaskCategory::Timer => q::HistoryTaskCategory::Timer,
        _ => q::HistoryTaskCategory::Transfer,
    }
}

fn from_task_err(e: t::TaskError) -> q::QueueError {
    match e {
        t::TaskError::ShardClosed { .. } => q::QueueError::ShardClosed(e.to_string()),
        t::TaskError::EntityNotExists(m) => q::QueueError::EntityNotExists(m),
        other => q::QueueError::Other(other.to_string()),
    }
}

fn to_task_err(e: q::QueueError) -> t::TaskError {
    match e {
        q::QueueError::ShardClosed(_) => t::TaskError::ShardClosed { closed_at: t::Time::ZERO },
        q::QueueError::EntityNotExists(m) => t::TaskError::EntityNotExists(m),
        other => t::TaskError::Other(other.to_string()),
    }
}

fn from_task_state(s: ti::TaskState) -> q::TaskState {
    match s {
        ti::TaskState::Pending => q::TaskState::Pending,
        ti::TaskState::Acked => q::TaskState::Acked,
        ti::TaskState::Nacked => q::TaskState::Nacked,
        ti::TaskState::Canceled => q::TaskState::Canceled,
    }
}

// ---------------------------------------------------------------------------
// persistence task adapter (queue -> task)
// ---------------------------------------------------------------------------

/// Exposes a `queue::deps::PersistenceTask` as a `task::deps::PersistenceTask`.
#[derive(Debug)]
pub struct TaskInfoAdapter {
    inner: Arc<dyn q::PersistenceTask>,
    domain_id: String,
    workflow_id: String,
    run_id: String,
    /// Fully decoded task (what executors downcast to); `None` for unknown task types.
    typed: Option<Arc<dyn t::PersistenceTask>>,
}

fn tl_kind(v: i32) -> t::TaskListKind {
    match v {
        1 => t::TaskListKind::Sticky,
        2 => t::TaskListKind::Ephemeral,
        _ => t::TaskListKind::Normal,
    }
}

/// Go `persistence.Task` -> typed task struct by task type.
fn typed_task(inner: &Arc<dyn q::PersistenceTask>) -> Option<Arc<dyn t::PersistenceTask>> {
    use crate::wiring::persistence::queues::SqlHistoryTask;
    let row = inner.as_any()?.downcast_ref::<SqlHistoryTask>()?;
    if let Some(x) = row.transfer_task() {
        let wf = t::WorkflowIdentifier { domain_id: x.domain_id.clone(), workflow_id: x.workflow_id.clone(), run_id: x.run_id.clone() };
        let td = t::TaskData { version: x.version, task_id: inner.get_task_id(), visibility_timestamp: t::Time(inner.get_visibility_timestamp().0) };
        let (wf, td) = (|| wf.clone(), || td.clone());
        return Some(match x.task_type {
            0 => Arc::new(t::DecisionTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), task_list: x.task_list.clone(), schedule_id: x.schedule_id, original_task_list: x.original_task_list.clone(), original_task_list_kind: tl_kind(x.original_task_list_kind) }) as Arc<dyn t::PersistenceTask>,
            1 => Arc::new(t::ActivityTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), task_list: x.task_list.clone(), schedule_id: x.schedule_id, original_task_list: x.original_task_list.clone(), original_task_list_kind: tl_kind(x.original_task_list_kind) }),
            2 => Arc::new(t::CloseExecutionTask { workflow_identifier: wf(), task_data: td() }),
            3 => Arc::new(t::CancelExecutionTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), target_workflow_id: x.target_workflow_id.clone(), target_run_id: x.target_run_id.clone(), target_child_workflow_only: x.target_child_workflow_only, initiated_id: x.schedule_id }),
            4 => Arc::new(t::StartChildExecutionTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), target_workflow_id: x.target_workflow_id.clone(), initiated_id: x.schedule_id, task_list: x.task_list.clone() }),
            5 => Arc::new(t::SignalExecutionTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), target_workflow_id: x.target_workflow_id.clone(), target_run_id: x.target_run_id.clone(), target_child_workflow_only: x.target_child_workflow_only, initiated_id: x.schedule_id }),
            6 => Arc::new(t::RecordWorkflowStartedTask { workflow_identifier: wf(), task_data: td() }),
            7 => Arc::new(t::ResetWorkflowTask { workflow_identifier: wf(), task_data: td() }),
            8 => Arc::new(t::UpsertWorkflowSearchAttributesTask { workflow_identifier: wf(), task_data: td(), task_list: x.task_list.clone() }),
            9 => Arc::new(t::RecordWorkflowClosedTask { workflow_identifier: wf(), task_data: td() }),
            10 => Arc::new(t::RecordChildExecutionCompletedTask { workflow_identifier: wf(), task_data: td(), target_domain_id: x.target_domain_id.clone(), target_workflow_id: x.target_workflow_id.clone(), target_run_id: x.target_run_id.clone() }),
            11 => Arc::new(t::ApplyParentClosePolicyTask { workflow_identifier: wf(), task_data: td(), target_domain_ids: x.target_domain_ids.clone() }),
            _ => return None,
        });
    }
    let x = row.timer_task()?;
    let wf = t::WorkflowIdentifier { domain_id: x.domain_id.clone(), workflow_id: x.workflow_id.clone(), run_id: x.run_id.clone() };
    let td = t::TaskData { version: x.version, task_id: inner.get_task_id(), visibility_timestamp: t::Time(inner.get_visibility_timestamp().0) };
    Some(match x.task_type {
        0 => Arc::new(t::DecisionTimeoutTask { workflow_identifier: wf, task_data: td, event_id: x.event_id, schedule_attempt: x.schedule_attempt, timeout_type: x.timeout_type }) as Arc<dyn t::PersistenceTask>,
        1 => Arc::new(t::ActivityTimeoutTask { workflow_identifier: wf, task_data: td, event_id: x.event_id, attempt: x.schedule_attempt, timeout_type: x.timeout_type }),
        2 => Arc::new(t::UserTimerTask { workflow_identifier: wf, task_data: td, event_id: x.event_id }),
        3 => Arc::new(t::WorkflowTimeoutTask { workflow_identifier: wf, task_data: td }),
        4 => Arc::new(t::DeleteHistoryEventTask { workflow_identifier: wf, task_data: td }),
        5 => Arc::new(t::ActivityRetryTimerTask { workflow_identifier: wf, task_data: td, event_id: x.event_id, attempt: x.schedule_attempt }),
        6 => Arc::new(t::WorkflowBackoffTimerTask { workflow_identifier: wf, task_data: td, event_id: x.event_id, timeout_type: x.timeout_type }),
        _ => return None,
    })
}

impl TaskInfoAdapter {
    pub fn new(inner: Arc<dyn q::PersistenceTask>) -> Arc<Self> {
        Arc::new(Self {
            typed: typed_task(&inner),
            domain_id: inner.get_domain_id(),
            workflow_id: inner.get_workflow_id(),
            run_id: inner.get_run_id(),
            inner,
        })
    }
    pub fn inner(&self) -> &Arc<dyn q::PersistenceTask> {
        &self.inner
    }
}

impl t::PersistenceTask for TaskInfoAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        match &self.typed {
            Some(typed) => typed.as_any(),
            None => self,
        }
    }
    fn wrapper_any(&self) -> &dyn std::any::Any {
        self
    }
    fn get_original_task_list(&self) -> String {
        self.typed.as_ref().map(|x| x.get_original_task_list()).unwrap_or_default()
    }
    fn get_original_task_list_kind(&self) -> t::TaskListKind {
        self.typed.as_ref().map(|x| x.get_original_task_list_kind()).unwrap_or_default()
    }
    fn get_task_category(&self) -> t::HistoryTaskCategory {
        to_task_category(self.inner.get_task_category())
    }
    fn get_task_type(&self) -> i32 {
        self.inner.get_task_type()
    }
    fn get_domain_id(&self) -> &str {
        &self.domain_id
    }
    fn get_workflow_id(&self) -> &str {
        &self.workflow_id
    }
    fn get_run_id(&self) -> &str {
        &self.run_id
    }
    fn get_version(&self) -> i64 {
        self.inner.get_version()
    }
    fn get_task_id(&self) -> i64 {
        self.inner.get_task_id()
    }
    fn get_visibility_timestamp(&self) -> t::Time {
        t::Time(self.inner.get_visibility_timestamp().0)
    }
    fn clone_arc(&self) -> Arc<dyn t::PersistenceTask> {
        TaskInfoAdapter::new(self.inner.clone())
    }
}

// ---------------------------------------------------------------------------
// task adapter (task -> queue)
// ---------------------------------------------------------------------------

/// Exposes a `task::interface::Task` as the queue package's `Task`.
pub struct QueueTaskAdapter {
    inner: Arc<dyn ti::Task>,
}

impl QueueTaskAdapter {
    pub fn new(inner: Arc<dyn ti::Task>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
    pub fn inner(&self) -> &Arc<dyn ti::Task> {
        &self.inner
    }
}

fn unwrap_task(task: &q::TaskRef) -> Result<Arc<dyn ti::Task>, q::QueueError> {
    task.as_any()
        .and_then(|a| a.downcast_ref::<QueueTaskAdapter>())
        .map(|a| a.inner.clone())
        .ok_or_else(|| q::QueueError::Other("unexpected queue task type".into()))
}

impl q::Task for QueueTaskAdapter {
    fn get_domain_id(&self) -> String {
        t::PersistenceTask::get_domain_id(&*self.inner).to_string()
    }
    fn get_workflow_id(&self) -> String {
        t::PersistenceTask::get_workflow_id(&*self.inner).to_string()
    }
    fn get_run_id(&self) -> String {
        t::PersistenceTask::get_run_id(&*self.inner).to_string()
    }
    fn get_task_type(&self) -> i32 {
        t::PersistenceTask::get_task_type(&*self.inner)
    }
    fn get_task_id(&self) -> i64 {
        t::PersistenceTask::get_task_id(&*self.inner)
    }
    fn get_visibility_timestamp(&self) -> q::Time {
        q::Time(t::PersistenceTask::get_visibility_timestamp(&*self.inner).0)
    }
    fn get_version(&self) -> i64 {
        t::PersistenceTask::get_version(&*self.inner)
    }
    fn get_task_key(&self) -> q::TaskKey {
        let task_id = t::PersistenceTask::get_task_id(&*self.inner);
        match from_task_category(t::PersistenceTask::get_task_category(&*self.inner)) {
            q::HistoryTaskCategory::Timer => q::TaskKey::Timer {
                visibility_timestamp: q::Time(t::PersistenceTask::get_visibility_timestamp(&*self.inner).0),
                task_id,
            },
            q::HistoryTaskCategory::Transfer => q::TaskKey::Transfer { task_id },
        }
    }
    fn state(&self) -> q::TaskState {
        from_task_state(self.inner.state())
    }
    fn get_attempt(&self) -> i32 {
        self.inner.get_attempt()
    }
    fn ack(&self) {
        self.inner.ack()
    }
    fn nack(&self) {
        // task::Task::nack is async (it may redispatch / run the DLQ path); run it detached.
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.nack(None).await });
    }
    fn set_initial_submit_time(&self, time: q::Time) {
        self.inner.set_initial_submit_time(t::Time(time.0))
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// processor / redispatcher adapters (task -> queue)
// ---------------------------------------------------------------------------

/// `queue::deps::TaskProcessor` over `task::interface::Processor`.
pub struct TaskProcessorAdapter {
    inner: Arc<dyn ti::Processor>,
}

impl TaskProcessorAdapter {
    pub fn new(inner: Arc<dyn ti::Processor>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl q::TaskProcessor for TaskProcessorAdapter {
    async fn try_submit(&self, task: q::TaskRef) -> Result<bool, q::QueueError> {
        let inner = unwrap_task(&task)?;
        self.inner.try_submit(inner).await.map_err(from_task_err)
    }
}

/// `queue::deps::Redispatcher` over `task::interface::Redispatcher`.
pub struct RedispatcherAdapter {
    inner: Arc<dyn ti::Redispatcher>,
}

impl RedispatcherAdapter {
    pub fn new(inner: Arc<dyn ti::Redispatcher>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl q::Redispatcher for RedispatcherAdapter {
    fn start(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.start().await });
    }
    fn stop(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.stop().await });
    }
    fn add_task(&self, task: q::TaskRef) {
        match unwrap_task(&task) {
            Ok(t) => self.inner.add_task(t),
            Err(e) => tracing::error!(error = %e, "redispatcher dropped a foreign task"),
        }
    }
    async fn redispatch(&self, target_size: usize) {
        self.inner.redispatch(target_size).await
    }
    fn redispatch_task(&self, task: q::TaskRef, time: q::Time) {
        match unwrap_task(&task) {
            Ok(t) => self.inner.redispatch_task(t, t::Time(time.0)),
            Err(e) => tracing::error!(error = %e, "redispatcher dropped a foreign task"),
        }
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
}

// ---------------------------------------------------------------------------
// HistoryTaskFactory (Go task.NewHistoryTask)
// ---------------------------------------------------------------------------

/// Adapts the queue's async `TaskFilter` to `task::Filter` (sync). The filter only does
/// in-memory cache lookups plus domain cache reads, so blocking the worker thread briefly is the
/// Go-equivalent of a plain function call.
fn to_task_filter(filter: TaskFilter) -> ti::Filter {
    Arc::new(move |info: &dyn t::PersistenceTask| -> t::Result<bool> {
        let adapter = info
            .wrapper_any()
            .downcast_ref::<TaskInfoAdapter>()
            .ok_or_else(|| t::TaskError::Other("unexpected persistence task type".into()))?;
        let fut = filter(adapter.inner().clone());
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut)).map_err(to_task_err)
    })
}

/// Everything `new_history_task` needs besides the task itself.
#[derive(Clone)]
pub struct HistoryTaskFactoryDeps {
    pub shard: Arc<dyn t::ShardContext>,
    pub executor: Arc<dyn ti::Executor>,
    pub processor: Arc<dyn ti::Processor>,
    pub redispatcher: Arc<dyn ti::Redispatcher>,
    pub critical_retry_count: t::IntPropertyFn,
    pub logger: t::Logger,
}

/// Builds the closure the queue processors use as `task.NewHistoryTask`.
pub fn new_history_task_factory(deps: HistoryTaskFactoryDeps) -> HistoryTaskFactory {
    Arc::new(move |info: Arc<dyn q::PersistenceTask>, queue_type: QQueueType, filter: TaskFilter| {
        let task = new_history_task(
            deps.shard.clone(),
            TaskInfoAdapter::new(info),
            to_task_queue_type(queue_type),
            deps.logger.clone(),
            to_task_filter(filter),
            deps.executor.clone(),
            deps.processor.clone(),
            deps.redispatcher.clone(),
            deps.critical_retry_count.clone(),
        );
        QueueTaskAdapter::new(task) as q::TaskRef
    })
}

// ---------------------------------------------------------------------------
// builders
// ---------------------------------------------------------------------------

/// Creates a `task::Redispatcher` for one processor (Go `task.NewRedispatcher`).
pub type TaskRedispatcherFactory = Arc<dyn Fn(ti::QueueType) -> Arc<dyn ti::Redispatcher> + Send + Sync>;

/// Dependencies shared by the transfer and timer processors of one shard.
#[derive(Clone)]
pub struct QueueExecCommon {
    pub queue_shard: Arc<dyn QueueShardContext>,
    pub task_shard: Arc<dyn t::ShardContext>,
    pub task_processor: Arc<dyn ti::Processor>,
    /// Go `NewTaskAllocator(shard)`: each queue processor owns its allocator (own lock).
    pub task_allocator_factory: Arc<dyn Fn() -> Arc<dyn TaskAllocator> + Send + Sync>,
    pub metrics: Arc<dyn q::MetricsScope>,
    pub redispatcher_factory: TaskRedispatcherFactory,
    pub critical_retry_count: t::IntPropertyFn,
    pub logger: t::Logger,
}

/// Executors of one queue category: active plus one per remote (standby) cluster.
#[derive(Clone)]
pub struct CategoryExecutors {
    pub active: Arc<dyn ti::Executor>,
    pub standby: HashMap<String, Arc<dyn ti::Executor>>,
}

struct Prepared {
    task_processor: Arc<dyn q::TaskProcessor>,
    redispatcher_factory: Arc<dyn Fn(QQueueType) -> Arc<dyn q::Redispatcher> + Send + Sync>,
    active_task_factory: HistoryTaskFactory,
    standby_task_factories: HashMap<String, HistoryTaskFactory>,
}

/// Pre-builds the redispatchers (one per processor; the queue constructors ask for the standby
/// ones in `remote_cluster_names()` order) so each task factory and its queue share the same instance.
fn prepare(common: &QueueExecCommon, active_type: ti::QueueType, standby_type: ti::QueueType, ex: CategoryExecutors) -> Prepared {
    let make_factory = |executor: Arc<dyn ti::Executor>, redispatcher: Arc<dyn ti::Redispatcher>| {
        new_history_task_factory(HistoryTaskFactoryDeps {
            shard: common.task_shard.clone(),
            executor,
            processor: common.task_processor.clone(),
            redispatcher,
            critical_retry_count: common.critical_retry_count.clone(),
            logger: common.logger.clone(),
        })
    };

    let active_redispatcher = (common.redispatcher_factory)(active_type);
    let active_task_factory = make_factory(ex.active.clone(), active_redispatcher.clone());

    let mut standby_task_factories = HashMap::new();
    let mut standby_queue_redispatchers = VecDeque::new();
    for cluster in common.queue_shard.remote_cluster_names() {
        let executor = ex.standby.get(&cluster).unwrap_or_else(|| panic!("missing standby executor for {cluster}")).clone();
        let rd = (common.redispatcher_factory)(standby_type);
        standby_task_factories.insert(cluster, make_factory(executor, rd.clone()));
        standby_queue_redispatchers.push_back(rd);
    }

    let active_q: Arc<dyn q::Redispatcher> = RedispatcherAdapter::new(active_redispatcher);
    let standby_q = Mutex::new(standby_queue_redispatchers);
    let redispatcher_factory = Arc::new(move |qt: QQueueType| -> Arc<dyn q::Redispatcher> {
        match qt {
            QQueueType::ActiveTransfer | QQueueType::ActiveTimer => active_q.clone(),
            QQueueType::StandbyTransfer | QQueueType::StandbyTimer => {
                let rd = standby_q.lock().unwrap().pop_front().expect("standby redispatcher");
                RedispatcherAdapter::new(rd)
            }
        }
    });

    Prepared {
        task_processor: TaskProcessorAdapter::new(common.task_processor.clone()),
        redispatcher_factory,
        active_task_factory,
        standby_task_factories,
    }
}

fn rate_limiter_factory() -> Arc<dyn Fn(q::IntFn) -> Arc<dyn q::Limiter> + Send + Sync> {
    Arc::new(|rps| DynamicRateLimiter::new(rps) as Arc<dyn q::Limiter>)
}

/// Go `NewTransferQueueProcessor` wired to real task executors.
pub fn build_transfer_queue_processor(common: &QueueExecCommon, executors: CategoryExecutors) -> Arc<TransferQueueProcessor> {
    let p = prepare(common, ti::QueueType::ActiveTransfer, ti::QueueType::StandbyTransfer, executors);
    TransferQueueProcessor::new(TransferQueueDeps {
        shard: common.queue_shard.clone(),
        task_processor: p.task_processor,
        task_allocator: (common.task_allocator_factory)(),
        metrics: common.metrics.clone(),
        rate_limiter_factory: rate_limiter_factory(),
        redispatcher_factory: p.redispatcher_factory,
        active_task_factory: p.active_task_factory,
        standby_task_factories: p.standby_task_factories,
    })
}

/// Go `NewTimerQueueProcessor` wired to real task executors.
pub fn build_timer_queue_processor(common: &QueueExecCommon, executors: CategoryExecutors) -> Arc<TimerQueueProcessor> {
    let p = prepare(common, ti::QueueType::ActiveTimer, ti::QueueType::StandbyTimer, executors);
    TimerQueueProcessor::new(TimerQueueDeps {
        shard: common.queue_shard.clone(),
        task_processor: p.task_processor,
        task_allocator: (common.task_allocator_factory)(),
        metrics: common.metrics.clone(),
        rate_limiter_factory: rate_limiter_factory(),
        redispatcher_factory: p.redispatcher_factory,
        active_task_factory: p.active_task_factory,
        standby_task_factories: p.standby_task_factories,
    })
}
