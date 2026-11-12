//! `TimerActiveDependencies` adapter (Go `task/timer_active_task_executor.go` dependencies).
//!
//! Real: config accessors, matching `AddActivityTask` (delegates to `TransferDepsImpl`), workflow loading via the
//! shard execution cache, and the `TimerWorkflow` read accessors / `delete_activity` /
//! `update_workflow_execution_as_active` over `EngineMutableState`.
//!
//! Timer support comes from `EngineMutableState` (engine_timer.rs). `retry_workflow` /
//! `update_workflow_execution_with_new_as_active` use the continue-as-new path (`engine_continue.rs`,
//! `EngineExecutionContext::update_with_new_run`).
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use super::engine_state::EngineMutableState;
use super::engine_timer::SeqTimer;
use super::env::HistoryEnv;
use cadence_api::enums::TimeoutType;
use super::queue_deps::TransferDepsImpl;
use crate::engine::deps::{HistoryError, ReleaseFunc, WorkflowExecution};
use crate::engine::deps_ms::MutableState as _;
use crate::task::deps::{BytesMap, PersistenceTask, Result as TaskResult, TaskError, Time};
use crate::task::timer_active_task_executor::{
    TimerActiveDependencies, TimerActivityInfo, TimerDecisionInfo, TimerSequenceId, TimerType, TimerWorkflow, UserTimerInfo,
};
use crate::task::standby_task_util::HistoryResendInfo;
use crate::task::timer_standby_task_executor::StandbyTimerWorkflow;
use crate::task::transfer_task_executor_base::{AddMatchingTaskRequest, TransferDependencies};

fn seq(t: SeqTimer) -> TimerSequenceId {
    TimerSequenceId {
        event_id: t.event_id,
        attempt: t.attempt,
        timer_type: match t.timeout_type {
            0 => TimerType::StartToClose,
            1 => TimerType::ScheduleToStart,
            2 => TimerType::ScheduleToClose,
            _ => TimerType::Heartbeat,
        },
        expiry: Time(t.expiry_nanos),
    }
}

fn timeout_type(t: TimerType) -> &'static str {
    match t {
        TimerType::StartToClose => "START_TO_CLOSE",
        TimerType::ScheduleToStart => "SCHEDULE_TO_START",
        TimerType::ScheduleToClose => "SCHEDULE_TO_CLOSE",
        TimerType::Heartbeat => "HEARTBEAT",
    }
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
pub(super) struct EngineTimerWorkflow {
    state: Arc<EngineMutableState>,
    context: Arc<EngineExecutionContext>,
    release: ReleaseFunc,
    /// New run produced by `retry_workflow`, persisted by `update_workflow_execution_with_new_as_active`.
    new_run: std::sync::Mutex<Option<Arc<EngineMutableState>>>,
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
        self.state.seq_user_timers().into_iter().map(seq).collect()
    }
    fn activity_timers(&self) -> Vec<TimerSequenceId> {
        self.state.seq_activity_timers().into_iter().map(seq).collect()
    }
    fn user_timer_info(&self, event_id: i64) -> Option<UserTimerInfo> {
        self.state.user_timer_by_started_id(event_id).map(|(timer_id, started_id)| UserTimerInfo { timer_id, started_id })
    }
    fn activity_info(&self, event_id: i64) -> Option<TimerActivityInfo> {
        let a = self.state.get_activity_info(event_id)?;
        let f = self.state.activity_timer_fields(event_id)?;
        Some(TimerActivityInfo {
            schedule_id: a.schedule_id,
            started_id: a.started_id,
            attempt: i64::from(a.attempt),
            version: a.version,
            domain_id: self.state.get_domain_entry().info.id.clone(),
            task_list: a.task_list,
            schedule_to_start_timeout: f.schedule_to_start_timeout_seconds,
            // TODO(link): LastHeartbeatTimeoutVisibilityInSeconds is not kept in persistence ActivityInfo.
            last_heartbeat_timeout_visibility_in_seconds: 0,
            timer_task_status: f.timer_task_status,
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
        self.state.resurrected_timers().await.map_err(he)
    }
    async fn resurrected_activities(&self) -> TaskResult<HashSet<i64>> {
        self.state.get_resurrected_activities().await.map_err(he)
    }
    fn delete_user_timer(&self, id: &str) -> TaskResult<()> {
        self.state.delete_user_timer(id).map_err(he)
    }
    fn delete_activity(&self, id: i64) -> TaskResult<()> {
        self.state.delete_activity(id).map_err(he)
    }
    fn add_timer_fired_event(&self, id: &str) -> TaskResult<()> {
        self.state.add_timer_fired(id).map_err(he)
    }
    fn update_activity(&self, info: TimerActivityInfo) -> TaskResult<()> {
        self.state.set_activity_timer_status(info.schedule_id, info.timer_task_status).map_err(he)
    }
    fn retry_activity(&self, info: &TimerActivityInfo, timer_type: TimerType) -> TaskResult<bool> {
        let Some(a) = self.state.engine_activity_info(info.schedule_id) else {
            return Err(TaskError::InternalService(format!("activity {} not found", info.schedule_id)));
        };
        let reason = format!("cadenceInternal:Timeout {}", timeout_type(timer_type));
        self.state.retry_activity(&a, &reason, &[], None).map_err(he)
    }
    fn add_activity_task_timed_out_event(&self, info: &TimerActivityInfo, timer_type: TimerType) -> TaskResult<()> {
        let t = match timer_type {
            TimerType::StartToClose => TimeoutType::StartToClose,
            TimerType::ScheduleToStart => TimeoutType::ScheduleToStart,
            TimerType::ScheduleToClose => TimeoutType::ScheduleToClose,
            TimerType::Heartbeat => TimeoutType::Heartbeat,
        };
        self.state.add_activity_timed_out(info.schedule_id, info.started_id, t).map_err(he)
    }
    fn sticky(&self) -> bool {
        self.state.is_sticky_task_list_enabled()
    }
    fn add_decision_task_timed_out_event(&self, info: &TimerDecisionInfo) -> TaskResult<()> {
        self.state.add_decision_timed_out(info.schedule_id, info.started_id).map_err(he)
    }
    fn add_decision_task_schedule_to_start_timeout_event(&self, schedule_id: i64) -> TaskResult<()> {
        self.state.add_decision_task_schedule_to_start_timeout(schedule_id).map_err(he)
    }
    fn decision_attempt(&self) -> i64 {
        self.state.get_pending_decision().map(|d| d.attempt).unwrap_or(0)
    }
    fn terminate_decision_attempts_exceeded(&self, message: &str) -> TaskResult<()> {
        self.state
            .terminate_workflow(self.state.get_next_event_id(), "DECISION_ATTEMPTS_EXCEEDS_LIMIT", message.as_bytes(), "history-service")
            .map_err(he)
    }
    fn has_processed_or_pending_decision(&self) -> bool {
        self.state.has_processed_or_pending_decision()
    }
    fn decision_schedule_id(&self) -> i64 {
        self.state.get_pending_decision().map(|d| d.schedule_id).unwrap_or(0)
    }
    fn add_upsert_workflow_search_attributes_task(&self) -> TaskResult<()> {
        self.state.add_search_attributes_task();
        Ok(())
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
    fn retry_backoff_duration(&self, reason: &str) -> Option<Duration> {
        self.state.workflow_retry_backoff(reason)
    }
    async fn cron_backoff_duration(&self) -> TaskResult<Option<Duration>> {
        self.state.workflow_cron_backoff().map_err(he)
    }
    fn cancel_requested(&self) -> bool {
        self.state.is_cancel_requested().0
    }
    fn timeout_workflow(&self, first_event_id: i64) -> TaskResult<()> {
        self.state.force_close_in_flight_decision().map_err(he)?;
        self.state.add_timeout_workflow(first_event_id).map_err(he)
    }
    async fn retry_workflow(&self, first_event_id: i64, backoff: Duration, cron: bool, reason: &str) -> TaskResult<()> {
        let new_run = self.state.retry_workflow_new_run(first_event_id, backoff, cron, reason).map_err(he)?;
        *self.new_run.lock().unwrap() = Some(new_run);
        Ok(())
    }
    async fn update_workflow_execution_with_new_as_active(&self, now: Time) -> TaskResult<()> {
        let new_run = self
            .new_run
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| TaskError::InternalService("no new run to persist".into()))?;
        self.context.update_with_new_run(now.0, &new_run).await.map_err(he)
    }
    async fn activity_scheduled_domain(&self, event_id: i64) -> TaskResult<String> {
        let ev = self.state.get_activity_scheduled_event(event_id).await.map_err(he)?;
        match ev.activity_task_scheduled_event_attributes.and_then(|a| a.domain) {
            Some(d) if !d.is_empty() => Ok(d),
            _ => Ok(self.state.get_domain_entry().info.name.clone()),
        }
    }
    fn partition_config(&self) -> BytesMap {
        self.state
            .get_execution_info()
            .partition_config
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect()
    }
}

/// Go `StandbyTimerWorkflow` operations (mutable state `GetLastWriteVersion`, `GetVersionHistories`,
/// `CreateNextActivityTimer`, `UpdateCurrentVersion` and context `UpdateWorkflowExecutionAsPassive`).
#[async_trait]
impl StandbyTimerWorkflow for EngineTimerWorkflow {
    fn last_write_version(&self) -> TaskResult<i64> {
        self.state.get_last_write_version().map_err(he)
    }
    fn history_resend_info(&self) -> TaskResult<HistoryResendInfo> {
        let histories = self.state.get_version_histories().ok_or(TaskError::MissingVersionHistories)?;
        let current = histories
            .histories
            .get(histories.current_version_history_index as usize)
            .ok_or_else(|| TaskError::Other("current version history index out of range".into()))?;
        let last = current.items.last().ok_or_else(|| TaskError::Other("version history is empty".into()))?;
        Ok(HistoryResendInfo { last_event_id: Some(last.event_id), last_event_version: Some(last.version) })
    }
    fn create_next_activity_timer(&self) -> TaskResult<bool> {
        Ok(self.state.create_next_activity_timer_task())
    }
    fn update_current_version(&self, version: i64, force: bool) -> TaskResult<()> {
        self.state.update_current_version(version, force).map_err(he)
    }
    async fn update_workflow_execution_as_passive(&self, now: Time) -> TaskResult<()> {
        self.context.update_as_passive(now.0).await.map_err(he)
    }
}

/// Loads the workflow through the shard execution cache (Go `loadWorkflowOnce`); a missing
/// execution yields `None`.
pub(super) async fn load_engine_timer_workflow(
    cache: &EngineExecutionCache,
    task: &dyn PersistenceTask,
) -> TaskResult<Option<Arc<EngineTimerWorkflow>>> {
    let execution = WorkflowExecution {
        workflow_id: task.get_workflow_id().to_string(),
        run_id: task.get_run_id().to_string(),
    };
    let (context, release) = cache.get_or_create(task.get_domain_id(), &execution).await.map_err(he)?;
    match context.load_state().await {
        Ok(state) => Ok(Some(Arc::new(EngineTimerWorkflow { state, context, release, new_run: std::sync::Mutex::new(None) }))),
        Err(e) => {
            release(Some(&e));
            match he(e) {
                TaskError::EntityNotExists(_) => Ok(None),
                other => Err(other),
            }
        }
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
        Ok(load_engine_timer_workflow(&self.cache, task).await?.map(|w| w as Arc<dyn TimerWorkflow>))
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
