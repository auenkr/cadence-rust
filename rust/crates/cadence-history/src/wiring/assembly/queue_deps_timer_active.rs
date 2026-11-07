//! `TimerActiveDependencies` adapter (Go `task/timer_active_task_executor.go` dependencies).
//!
//! Real: config accessors, matching `AddActivityTask` (delegates to `TransferDepsImpl`), workflow loading via the
//! shard execution cache, and the `TimerWorkflow` read accessors / `delete_activity` /
//! `update_workflow_execution_as_active` over `EngineMutableState`.
//!
//! Not done (explicit `TODO(link)` errors, see memory/history-gap-queuetimeractive-20261107.md): everything that needs
//! timer sequences or timer/decision/activity timeout history events on `EngineMutableState`, which does not expose them
//! yet. `user_timers` / `activity_timers` are infallible in the trait and return empty; DO NOT wire this adapter into
//! the launcher before `TimerSequence` is backed by real state, or timers would be completed without firing.
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use super::engine_state::EngineMutableState;
use super::env::HistoryEnv;
use super::queue_deps::TransferDepsImpl;
use crate::engine::deps::{HistoryError, ReleaseFunc, WorkflowExecution};
use crate::engine::deps_ms::MutableState as _;
use crate::task::deps::{BytesMap, PersistenceTask, Result as TaskResult, TaskError, Time};
use crate::task::timer_active_task_executor::{
    TimerActiveDependencies, TimerActivityInfo, TimerDecisionInfo, TimerSequenceId, TimerType, TimerWorkflow, UserTimerInfo,
};
use crate::task::transfer_task_executor_base::{AddMatchingTaskRequest, TransferDependencies};

fn todo_err<T>(what: &str) -> TaskResult<T> {
    Err(TaskError::Other(format!("timer active: {what} is not supported yet (TODO(link): EngineMutableState timer support)")))
}

fn he(e: HistoryError) -> TaskError {
    match e {
        HistoryError::DeadlineExceeded => TaskError::ContextDeadlineExceeded,
        HistoryError::Canceled => TaskError::ContextCanceled,
        HistoryError::PersistenceShardOwnershipLost(m) => TaskError::ShardOwnershipLost(m),
        other => {
            let text = other.to_string();
            if text.contains("not exist") {
                TaskError::EntityNotExists(text)
            } else {
                TaskError::InternalService(text)
            }
        }
    }
}

/// `TimerWorkflow` over a locked execution context and its loaded mutable state.
struct EngineTimerWorkflow {
    state: Arc<EngineMutableState>,
    context: Arc<EngineExecutionContext>,
    release: ReleaseFunc,
}

#[async_trait]
impl TimerWorkflow for EngineTimerWorkflow {
    fn release(&self, error: Option<TaskError>) {
        match error {
            Some(e) => (self.release)(Some(&HistoryError::internal(e.to_string()))),
            None => (self.release)(None),
        }
    }
    fn running(&self) -> bool {
        self.state.is_workflow_execution_running()
    }
    fn domain_name(&self) -> String {
        self.state.get_domain_entry().info.name.clone()
    }
    fn workflow_type(&self) -> Option<String> {
        self.state.get_workflow_type().map(|t| t.name)
    }
    fn user_timers(&self) -> Vec<TimerSequenceId> {
        // TODO(link): TimerSequence over EngineMutableState timer infos.
        Vec::new()
    }
    fn activity_timers(&self) -> Vec<TimerSequenceId> {
        // TODO(link): TimerSequence over EngineMutableState activity infos.
        Vec::new()
    }
    fn user_timer_info(&self, _event_id: i64) -> Option<UserTimerInfo> {
        None
    }
    fn activity_info(&self, event_id: i64) -> Option<TimerActivityInfo> {
        let a = self.state.get_activity_info(event_id)?;
        Some(TimerActivityInfo {
            schedule_id: a.schedule_id,
            started_id: a.started_id,
            attempt: i64::from(a.attempt),
            version: a.version,
            domain_id: self.state.get_domain_entry().info.id.clone(),
            task_list: a.task_list,
            // TODO(link): ScheduleToStartTimeout and timer_task_status are not kept in the engine ActivityInfo.
            schedule_to_start_timeout: 0,
            last_heartbeat_timeout_visibility_in_seconds: 0,
            timer_task_status: 0,
            details: a.details,
        })
    }
    fn decision_info(&self, event_id: i64) -> Option<TimerDecisionInfo> {
        let d = self.state.get_pending_decision()?;
        (d.schedule_id == event_id).then_some(TimerDecisionInfo {
            schedule_id: d.schedule_id,
            started_id: d.started_id,
            attempt: d.attempt,
            version: d.version,
        })
    }
    async fn resurrected_timers(&self) -> TaskResult<HashSet<String>> {
        todo_err("resurrected timers")
    }
    async fn resurrected_activities(&self) -> TaskResult<HashSet<i64>> {
        self.state.get_resurrected_activities().await.map_err(he)
    }
    fn delete_user_timer(&self, _id: &str) -> TaskResult<()> {
        todo_err("delete_user_timer")
    }
    fn delete_activity(&self, id: i64) -> TaskResult<()> {
        self.state.delete_activity(id).map_err(he)
    }
    fn add_timer_fired_event(&self, _id: &str) -> TaskResult<()> {
        todo_err("add_timer_fired_event")
    }
    fn update_activity(&self, _info: TimerActivityInfo) -> TaskResult<()> {
        todo_err("update_activity")
    }
    fn retry_activity(&self, _info: &TimerActivityInfo, _timer_type: TimerType) -> TaskResult<bool> {
        todo_err("retry_activity")
    }
    fn add_activity_task_timed_out_event(&self, _info: &TimerActivityInfo, _timer_type: TimerType) -> TaskResult<()> {
        todo_err("add_activity_task_timed_out_event")
    }
    fn sticky(&self) -> bool {
        self.state.is_sticky_task_list_enabled()
    }
    fn add_decision_task_timed_out_event(&self, _info: &TimerDecisionInfo) -> TaskResult<()> {
        todo_err("add_decision_task_timed_out_event")
    }
    fn add_decision_task_schedule_to_start_timeout_event(&self, _schedule_id: i64) -> TaskResult<()> {
        todo_err("add_decision_task_schedule_to_start_timeout_event")
    }
    fn decision_attempt(&self) -> i64 {
        self.state.get_pending_decision().map(|d| d.attempt).unwrap_or(0)
    }
    fn terminate_decision_attempts_exceeded(&self, _message: &str) -> TaskResult<()> {
        todo_err("terminate_decision_attempts_exceeded")
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.state.has_processed_or_pending_decision()
    }
    fn decision_schedule_id(&self) -> i64 {
        self.state.get_pending_decision().map(|d| d.schedule_id).unwrap_or(0)
    }
    fn add_upsert_workflow_search_attributes_task(&self) -> TaskResult<()> {
        todo_err("add_upsert_workflow_search_attributes_task")
    }
    fn schedule_decision(&self) -> TaskResult<()> {
        self.state.add_decision_task_scheduled_event(false).map_err(he)
    }
    async fn update_workflow_execution_as_active(&self, now: Time) -> TaskResult<()> {
        self.context.update_as_active(now.0).await.map_err(he)
    }
    fn start_version(&self) -> TaskResult<i64> {
        Ok(self.state.snapshot().start_version)
    }
    fn next_event_id(&self) -> i64 {
        self.state.get_next_event_id()
    }
    fn retry_backoff_duration(&self, _reason: &str) -> Option<Duration> {
        // TODO(link): retry policy backoff (workflow retry).
        None
    }
    async fn cron_backoff_duration(&self) -> TaskResult<Option<Duration>> {
        todo_err("cron_backoff_duration")
    }
    fn cancel_requested(&self) -> bool {
        self.state.is_cancel_requested().0
    }
    fn timeout_workflow(&self, _first_event_id: i64) -> TaskResult<()> {
        todo_err("timeout_workflow")
    }
    async fn retry_workflow(&self, _first_event_id: i64, _backoff: Duration, _cron: bool, _reason: &str) -> TaskResult<()> {
        todo_err("retry_workflow")
    }
    async fn update_workflow_execution_with_new_as_active(&self, _now: Time) -> TaskResult<()> {
        todo_err("update_workflow_execution_with_new_as_active")
    }
    async fn activity_scheduled_domain(&self, _event_id: i64) -> TaskResult<String> {
        // TODO(link): read ActivityTaskScheduled event domain override; same-domain default.
        Ok(self.state.get_domain_entry().info.name.clone())
    }
    fn partition_config(&self) -> BytesMap {
        BytesMap::new()
    }
}

/// `TimerActiveDependencies` over the shard's execution cache, config and matching client.
pub struct TimerActiveDepsImpl {
    env: Arc<HistoryEnv>,
    cache: Arc<EngineExecutionCache>,
    matching: Arc<TransferDepsImpl>,
}

impl TimerActiveDepsImpl {
    pub fn new(env: Arc<HistoryEnv>, cache: Arc<EngineExecutionCache>) -> Arc<Self> {
        let matching = TransferDepsImpl::new(env.clone());
        Arc::new(Self { env, cache, matching })
    }
}

#[async_trait]
impl TimerActiveDependencies for TimerActiveDepsImpl {
    async fn load_workflow(&self, task: &dyn PersistenceTask, _event_id: i64) -> TaskResult<Option<Arc<dyn TimerWorkflow>>> {
        let execution = WorkflowExecution {
            workflow_id: task.get_workflow_id().to_string(),
            run_id: task.get_run_id().to_string(),
        };
        let (context, release) = self.cache.get_or_create(task.get_domain_id(), &execution).await.map_err(he)?;
        match context.load_state().await {
            Ok(state) => Ok(Some(Arc::new(EngineTimerWorkflow { state, context, release }))),
            Err(e) => {
                release(Some(&e));
                match he(e) {
                    TaskError::EntityNotExists(_) => Ok(None),
                    other => Err(other),
                }
            }
        }
    }

    fn resurrection_check_min_delay(&self, domain: &str) -> Duration {
        (self.env.config.resurrection_check_min_delay)(domain)
    }

    fn activity_max_schedule_to_start_timeout_for_retry(&self, domain: &str) -> Duration {
        (self.env.config.activity_max_schedule_to_start_timeout_for_retry)(domain)
    }

    fn decision_retry_max_attempts(&self, domain: &str) -> i32 {
        (self.env.config.decision_retry_max_attempts)(domain) as i32
    }

    fn enforce_decision_task_attempts(&self, domain: &str) -> bool {
        (self.env.config.enforce_decision_task_attempts)(domain)
    }

    fn advanced_visibility_writing_enabled(&self) -> bool {
        // Go `AdvancedVisibilityWritingMode() != "off"`: advanced store configured and written to.
        self.env.config.is_advanced_vis_config_exist && !matches!((self.env.config.write_visibility_store_name)().as_str(), "db" | "off")
    }

    fn delete_history_event_context_timeout(&self) -> Duration {
        Duration::from_secs(u64::try_from((self.env.config.delete_history_event_context_timeout)()).unwrap_or(0))
    }

    async fn add_activity_task(&self, request: AddMatchingTaskRequest) -> TaskResult<()> {
        self.matching.add_activity_task(request).await
    }
}
