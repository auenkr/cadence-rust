//! Glue between queue v2 (`queuev2::*`, its own `deps` trait universe) and the task package
//! (`task::*`): the `QueueEnv` implementation (Go `task.NewRescheduler`, `task.NewHistoryTaskV2`,
//! `quotas.NewDynamicRateLimiter`, `clock.NewTimerGate`), the task / processor / rescheduler /
//! executor adapters, and the conversion of engine notifications (which carry the full persisted
//! task since queue v2 piece A) into `queuev2` `NotifyTaskInfo` for the cached queue reader.
//!
//! Not wired into the queue factory yet (queue v2 piece C); queue v1 stays the only active path.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::sync::Notify;

use super::queue_exec::{DynamicRateLimiter, TaskInfoAdapter};
use crate::engine::notify_tasks::{FullPersistedTask, NotifiedTask};
use crate::queue::deps as q;
use crate::queuev2::deps as v2;
use crate::task::deps as t;
use crate::task::history_task::new_history_task_v2;
use crate::task::interface as ti;
use crate::task::rescheduler::new_rescheduler;
use crate::wiring::persistence::queues::SqlHistoryTask;

fn to_task_time(x: v2::Time) -> t::Time {
    t::Time(v2::time_unix_nano(&x))
}

fn from_task_time(x: t::Time) -> v2::Time {
    v2::unix_nano_time(x.0)
}

// ---------------------------------------------------------------------------
// notifications -> cached reader
// ---------------------------------------------------------------------------

/// The full persisted task of an engine notification as a queue v2 task, `None` when the
/// notification carries no payload (failover callbacks, which Go also notifies without tasks).
pub fn notified_to_ptask(n: &NotifiedTask) -> Option<v2::PTask> {
    match n.full.as_ref()? {
        FullPersistedTask::Transfer(x) => Some(Arc::new(SqlHistoryTask::from_transfer(x.clone()))),
        FullPersistedTask::Timer(x) => Some(Arc::new(SqlHistoryTask::from_timer(x.clone()))),
    }
}

/// Go `hcommon.NotifyTaskInfo{Tasks, PersistenceError}` built from engine notifications.
pub fn notify_task_info(tasks: &[NotifiedTask], persistence_error: bool) -> v2::NotifyTaskInfo {
    v2::NotifyTaskInfo { tasks: tasks.iter().filter_map(notified_to_ptask).collect(), persistence_error }
}

// ---------------------------------------------------------------------------
// persistence task (v2 -> task package)
// ---------------------------------------------------------------------------

/// Fallback `queue::deps::PersistenceTask` for a v2 task that is not a `SqlHistoryTask`.
#[derive(Debug)]
struct ForeignV2Task {
    domain_id: String,
    workflow_id: String,
    run_id: String,
    task_id: i64,
    category: q::HistoryTaskCategory,
    visibility_nanos: i64,
}

impl q::PersistenceTask for ForeignV2Task {
    fn get_domain_id(&self) -> String {
        self.domain_id.clone()
    }
    fn get_workflow_id(&self) -> String {
        self.workflow_id.clone()
    }
    fn get_run_id(&self) -> String {
        self.run_id.clone()
    }
    fn get_task_id(&self) -> i64 {
        self.task_id
    }
    fn get_task_type(&self) -> i32 {
        -1
    }
    fn get_task_category(&self) -> q::HistoryTaskCategory {
        self.category
    }
    fn get_version(&self) -> i64 {
        0
    }
    fn get_visibility_timestamp(&self) -> q::Time {
        q::Time(self.visibility_nanos)
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn is_corrupted(&self) -> bool {
        self.domain_id.is_empty() || self.workflow_id.is_empty() || self.run_id.is_empty()
    }
}

/// The task-package view of a v2 persistence task (decoded into the typed task structs the
/// executors downcast to, like the v1 path).
pub fn to_task_info(task: &v2::PTask, category: v2::HistoryTaskCategory) -> Arc<TaskInfoAdapter> {
    if let Some(row) = task.as_any().and_then(|a| a.downcast_ref::<SqlHistoryTask>()) {
        return TaskInfoAdapter::new(Arc::new(row.clone()));
    }
    tracing::error!(task_id = task.get_task_id(), "queue v2 task is not a decoded history task row");
    TaskInfoAdapter::new(Arc::new(ForeignV2Task {
        domain_id: task.get_domain_id(),
        workflow_id: task.get_workflow_id(),
        run_id: task.get_run_id(),
        task_id: task.get_task_id(),
        category: if category == v2::HistoryTaskCategory::TIMER {
            q::HistoryTaskCategory::Timer
        } else {
            q::HistoryTaskCategory::Transfer
        },
        visibility_nanos: v2::time_unix_nano(&task.get_visibility_timestamp()),
    }))
}

// ---------------------------------------------------------------------------
// task adapter (task package -> v2)
// ---------------------------------------------------------------------------

/// Exposes a `task::interface::Task` as `queuev2::deps::Task`.
pub struct V2HistoryTask {
    inner: Arc<dyn ti::Task>,
    key: v2::HistoryTaskKey,
}

impl V2HistoryTask {
    pub fn new(inner: Arc<dyn ti::Task>, key: v2::HistoryTaskKey) -> Arc<Self> {
        Arc::new(Self { inner, key })
    }
    pub fn inner(&self) -> &Arc<dyn ti::Task> {
        &self.inner
    }
}

fn unwrap_v2_task(task: &v2::HTask) -> Result<Arc<dyn ti::Task>, v2::DepError> {
    task.as_any()
        .and_then(|a| a.downcast_ref::<V2HistoryTask>())
        .map(|a| a.inner.clone())
        .ok_or_else(|| "unexpected queue v2 task type".to_string())
}

impl v2::PersistenceTask for V2HistoryTask {
    fn get_task_key(&self) -> v2::HistoryTaskKey {
        self.key
    }
    fn get_task_id(&self) -> i64 {
        t::PersistenceTask::get_task_id(&*self.inner)
    }
    fn get_domain_id(&self) -> String {
        t::PersistenceTask::get_domain_id(&*self.inner).to_string()
    }
    fn get_workflow_id(&self) -> String {
        t::PersistenceTask::get_workflow_id(&*self.inner).to_string()
    }
    fn get_run_id(&self) -> String {
        t::PersistenceTask::get_run_id(&*self.inner).to_string()
    }
    fn get_visibility_timestamp(&self) -> v2::Time {
        from_task_time(t::PersistenceTask::get_visibility_timestamp(&*self.inner))
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

impl v2::Task for V2HistoryTask {
    fn state(&self) -> v2::TaskState {
        match self.inner.state() {
            ti::TaskState::Pending => v2::TaskState::Pending,
            ti::TaskState::Acked => v2::TaskState::Acked,
            // Go queue v2 only distinguishes Acked from not acked.
            ti::TaskState::Nacked | ti::TaskState::Canceled => v2::TaskState::Nacked,
        }
    }
    fn ack(&self) {
        self.inner.ack()
    }
    fn cancel(&self) {
        self.inner.cancel()
    }
    fn set_initial_submit_time(&self, x: v2::Time) {
        self.inner.set_initial_submit_time(to_task_time(x))
    }
    fn get_original_task_list(&self) -> String {
        t::PersistenceTask::get_original_task_list(&*self.inner)
    }
    fn get_original_task_list_kind(&self) -> i32 {
        match t::PersistenceTask::get_original_task_list_kind(&*self.inner) {
            t::TaskListKind::Normal => 0,
            t::TaskListKind::Sticky => 1,
            t::TaskListKind::Ephemeral => 2,
        }
    }
}

// ---------------------------------------------------------------------------
// processor / rescheduler / executor adapters
// ---------------------------------------------------------------------------

/// `queuev2::deps::Processor` over `task::interface::Processor` (Go `task.Processor.TrySubmit`).
pub struct V2ProcessorAdapter {
    inner: Arc<dyn ti::Processor>,
}

impl V2ProcessorAdapter {
    pub fn new(inner: Arc<dyn ti::Processor>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
    pub fn inner(&self) -> &Arc<dyn ti::Processor> {
        &self.inner
    }
}

impl v2::Processor for V2ProcessorAdapter {
    fn try_submit(&self, task: v2::HTask) -> Result<bool, v2::DepError> {
        let inner = unwrap_v2_task(&task)?;
        // Go TrySubmit never blocks; the Rust scheduler's try_submit completes without awaiting in
        // practice, block only if it does not.
        let mut fut = self.inner.try_submit(inner);
        let res = match (&mut fut).now_or_never() {
            Some(r) => r,
            None => tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut)),
        };
        res.map_err(|e| e.to_string())
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

/// `queuev2::deps::Rescheduler` over `task::interface::Rescheduler`.
pub struct V2ReschedulerAdapter {
    inner: Arc<dyn ti::Rescheduler>,
}

impl V2ReschedulerAdapter {
    pub fn new(inner: Arc<dyn ti::Rescheduler>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl v2::Rescheduler for V2ReschedulerAdapter {
    fn start(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.start().await });
    }
    fn stop(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.stop().await });
    }
    fn reschedule_task(&self, task: v2::HTask, at: v2::Time) {
        match unwrap_v2_task(&task) {
            Ok(x) => self.inner.reschedule_task(x, to_task_time(at)),
            Err(e) => tracing::error!(error = %e, "rescheduler dropped a foreign task"),
        }
    }
    fn reschedule_domains(&self, domain_ids: &std::collections::HashSet<String>) {
        self.inner.reschedule_domains(domain_ids)
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

/// `queuev2::deps::TaskExecutor` carrying a `task::interface::Executor` (the executor wrapper).
pub struct V2ExecutorAdapter {
    inner: Arc<dyn ti::Executor>,
}

impl V2ExecutorAdapter {
    pub fn new(inner: Arc<dyn ti::Executor>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl v2::TaskExecutor for V2ExecutorAdapter {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// rate limiter / timer gate
// ---------------------------------------------------------------------------

/// Go `quotas.NewDynamicRateLimiter(rps).Wait(ctx)`: the v1 token bucket, waiting until the
/// token is available or the context is cancelled.
pub struct V2RateLimiter {
    inner: Arc<DynamicRateLimiter>,
}

#[async_trait]
impl v2::Limiter for V2RateLimiter {
    async fn wait(&self, ctx: &v2::CancelToken) -> Result<(), v2::DepError> {
        if ctx.is_cancelled() {
            return Err("context canceled".into());
        }
        let wait = self
            .inner
            .reserve(Instant::now(), Duration::MAX)
            .ok_or_else(|| "rate: Wait(n=1) exceeds limiter's burst".to_string())?;
        if wait.is_zero() {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(wait) => Ok(()),
            _ = ctx.cancelled() => Err("context canceled".into()),
        }
    }
}

/// Go `clock.NewTimerGate(timeSource)`: fires once at the earliest time it was updated to.
pub struct V2TimerGate {
    ts: Arc<dyn v2::TimeSource>,
    next: Mutex<Option<v2::Time>>,
    changed: Notify,
}

impl V2TimerGate {
    pub fn new(ts: Arc<dyn v2::TimeSource>) -> Arc<Self> {
        Arc::new(Self { ts, next: Mutex::new(None), changed: Notify::new() })
    }
}

#[async_trait]
impl v2::TimerGate for V2TimerGate {
    fn update(&self, x: v2::Time) -> bool {
        let mut next = self.next.lock().unwrap();
        if matches!(*next, Some(n) if n <= x) {
            return false;
        }
        *next = Some(x);
        drop(next);
        self.changed.notify_one();
        true
    }
    fn stop(&self) {
        *self.next.lock().unwrap() = None;
        self.changed.notify_one();
    }
    async fn fired(&self) {
        loop {
            let next = *self.next.lock().unwrap();
            match next {
                None => self.changed.notified().await,
                Some(at) => {
                    let now = self.ts.now();
                    if at <= now {
                        let mut g = self.next.lock().unwrap();
                        if *g == Some(at) {
                            *g = None;
                            return;
                        }
                        continue;
                    }
                    let d = (at - now).to_std().unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = self.changed.notified() => {}
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// QueueEnv
// ---------------------------------------------------------------------------

/// `queuev2::deps::QueueEnv` over the task package of one shard.
pub struct V2QueueEnv {
    pub task_shard: Arc<dyn t::ShardContext>,
    /// Used when the factory passes a processor that is not a `V2ProcessorAdapter`.
    pub task_processor: Arc<dyn ti::Processor>,
    pub logger: t::Logger,
    pub critical_retry_count: t::IntPropertyFn,
    pub rescheduler_scope: Arc<dyn t::MetricsScope>,
}

impl V2QueueEnv {
    fn task_processor(&self, p: &Arc<dyn v2::Processor>) -> Arc<dyn ti::Processor> {
        p.as_any()
            .and_then(|a| a.downcast_ref::<V2ProcessorAdapter>())
            .map(|a| a.inner.clone())
            .unwrap_or_else(|| self.task_processor.clone())
    }
}

fn to_ti_queue_type(x: v2::QueueType) -> ti::QueueType {
    match x {
        v2::QueueType::Transfer => ti::QueueType::Transfer,
        v2::QueueType::Timer => ti::QueueType::Timer,
    }
}

impl v2::QueueEnv for V2QueueEnv {
    fn new_rescheduler(
        &self,
        _shard: &Arc<dyn v2::ShardContext>,
        processor: Arc<dyn v2::Processor>,
        _logger: Arc<dyn v2::Logger>,
        _scope: Arc<dyn v2::MetricsScope>,
    ) -> Arc<dyn v2::Rescheduler> {
        let r = new_rescheduler(
            self.task_processor(&processor),
            self.task_shard.get_time_source(),
            self.logger.clone(),
            self.rescheduler_scope.clone(),
        );
        V2ReschedulerAdapter::new(r)
    }

    fn new_history_task(
        &self,
        _shard: &Arc<dyn v2::ShardContext>,
        task: v2::PTask,
        queue_type: v2::QueueType,
        executor: Arc<dyn v2::TaskExecutor>,
        processor: Arc<dyn v2::Processor>,
        rescheduler: Arc<dyn v2::Rescheduler>,
        _logger: Arc<dyn v2::Logger>,
    ) -> v2::HTask {
        let executor = executor
            .as_any()
            .and_then(|a| a.downcast_ref::<V2ExecutorAdapter>())
            .map(|a| a.inner.clone())
            .expect("queue v2 executor must be a V2ExecutorAdapter");
        let rescheduler = rescheduler
            .as_any()
            .and_then(|a| a.downcast_ref::<V2ReschedulerAdapter>())
            .map(|a| a.inner.clone())
            .expect("queue v2 rescheduler must be a V2ReschedulerAdapter");
        let category = match queue_type {
            v2::QueueType::Transfer => v2::HistoryTaskCategory::TRANSFER,
            v2::QueueType::Timer => v2::HistoryTaskCategory::TIMER,
        };
        let key = task.get_task_key();
        let inner = new_history_task_v2(
            self.task_shard.clone(),
            to_task_info(&task, category),
            to_ti_queue_type(queue_type),
            self.logger.clone(),
            executor,
            self.task_processor(&processor),
            rescheduler,
            self.critical_retry_count.clone(),
        );
        V2HistoryTask::new(inner, key)
    }

    fn new_dynamic_rate_limiter(&self, rps: v2::IntFn) -> Arc<dyn v2::Limiter> {
        Arc::new(V2RateLimiter { inner: DynamicRateLimiter::new(rps) })
    }

    fn new_timer_gate(&self, ts: Arc<dyn v2::TimeSource>) -> Arc<dyn v2::TimerGate> {
        V2TimerGate::new(ts)
    }
}

#[cfg(test)]
#[path = "queue_v2_adapters_tests.rs"]
mod tests;
