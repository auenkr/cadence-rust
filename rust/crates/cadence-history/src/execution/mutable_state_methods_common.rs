//! Shared helpers used by the `mutable_state_methods_*` ports of Go's
//! `mutable_state_builder_methods_*.go` on [`TypedMutableState`].
//!
//! Go mapping: `checkMutability`, `createInternalServerError`, `createCallerError`,
//! `ClearStickyness`, `UpdateWorkflowStateCloseStatus` and the
//! `hBuilder.addEvent` tail. Task generation (`taskGenerator.*`), the events
//! cache (`writeEventToCache`) and logging are intentionally not wired yet.

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;

use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError};
use super::mutable_state_task_generator::{MutableStateTaskGenerator, TaskGeneratorDomains, TypedTaskGeneratorState};
use crate::mutable_state_builder_ext as ext;

pub(crate) type Result<T> = std::result::Result<T, TypedMutableStateError>;

pub const WORKFLOW_STATE_CREATED: i32 = ext::WORKFLOW_STATE_CREATED as i32;
pub const WORKFLOW_STATE_RUNNING: i32 = ext::WORKFLOW_STATE_RUNNING as i32;
pub const WORKFLOW_STATE_COMPLETED: i32 = ext::WORKFLOW_STATE_COMPLETED as i32;
pub const WORKFLOW_STATE_ZOMBIE: i32 = ext::WORKFLOW_STATE_ZOMBIE as i32;
pub const WORKFLOW_STATE_VOID: i32 = ext::WORKFLOW_STATE_VOID as i32;

pub const CLOSE_STATUS_NONE: i32 = 0;
pub const CLOSE_STATUS_COMPLETED: i32 = 1;
pub const CLOSE_STATUS_FAILED: i32 = 2;
pub const CLOSE_STATUS_CANCELED: i32 = 3;
pub const CLOSE_STATUS_TERMINATED: i32 = 4;
pub const CLOSE_STATUS_CONTINUED_AS_NEW: i32 = 5;
pub const CLOSE_STATUS_TIMED_OUT: i32 = 6;

/// Go `TimerTaskStatusNone`.
pub const TIMER_TASK_STATUS_NONE: i32 = 0;

/// Go `ErrWorkflowFinished` message.
pub const ERR_WORKFLOW_FINISHED: &str = "invalid mutable state action: mutation after finish";
/// Go `ErrMissingTimerInfo` message.
pub const ERR_MISSING_TIMER_INFO: &str = "unable to get timer info";
/// Go `timerCancellationMsgTimerIDUnknown`.
pub const TIMER_CANCELLATION_MSG_TIMER_ID_UNKNOWN: &str = "TIMER_ID_UNKNOWN";

pub(crate) fn internal_err<T>(message: impl Into<String>) -> Result<T> {
    Err(TypedMutableStateError::Internal(message.into()))
}

impl TypedMutableState {
    /// Go `checkMutability`.
    pub(crate) fn check_mutability(&self) -> Result<()> {
        if !self.is_workflow_execution_running() {
            return internal_err(ERR_WORKFLOW_FINISHED);
        }
        Ok(())
    }

    /// Go `createInternalServerError`.
    pub(crate) fn create_internal_server_error(&self, op: &str) -> TypedMutableStateError {
        TypedMutableStateError::Internal(format!("{op} operation failed"))
    }

    /// Go `createCallerError`.
    pub(crate) fn create_caller_error(&self, op: &str) -> TypedMutableStateError {
        TypedMutableStateError::BadRequest(format!("invalid history builder state for action: {op}"))
    }

    /// Go `ClearStickyness`.
    pub fn clear_stickyness(&mut self) {
        self.execution_info.sticky_task_list = String::new();
        self.execution_info.sticky_schedule_to_start_timeout_nanos = 0;
        self.execution_info.client_library_version = String::new();
        self.execution_info.client_feature_version = String::new();
        self.execution_info.client_impl = String::new();
    }

    /// Go `WorkflowExecutionInfo.UpdateWorkflowStateCloseStatus`.
    pub fn update_workflow_state_close_status(&mut self, state: i32, close_status: i32) -> Result<()> {
        let cur = self.execution_info.state;
        let cur_close = self.execution_info.close_status;
        let bad = || -> Result<()> {
            internal_err(format!(
                "unable to change workflow state from {cur} to {state}, close status {close_status}"
            ))
        };
        let unknown = || -> Result<()> { internal_err(format!("unknown workflow state: {state}")) };
        let none = close_status == CLOSE_STATUS_NONE;
        match cur {
            WORKFLOW_STATE_VOID => {}
            WORKFLOW_STATE_CREATED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED => {
                    if ![CLOSE_STATUS_TERMINATED, CLOSE_STATUS_TIMED_OUT, CLOSE_STATUS_CONTINUED_AS_NEW]
                        .contains(&close_status)
                    {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_RUNNING => match state {
                WORKFLOW_STATE_CREATED => return bad(),
                WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED => {
                    if none {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_COMPLETED => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING | WORKFLOW_STATE_ZOMBIE => return bad(),
                WORKFLOW_STATE_COMPLETED => {
                    if close_status != cur_close {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            WORKFLOW_STATE_ZOMBIE => match state {
                WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING => {
                    if !none {
                        return bad();
                    }
                }
                WORKFLOW_STATE_COMPLETED | WORKFLOW_STATE_ZOMBIE => {
                    if none {
                        return bad();
                    }
                }
                _ => return unknown(),
            },
            _ => return unknown(),
        }
        self.execution_info.state = state;
        self.execution_info.close_status = close_status;
        Ok(())
    }

    /// Go `hBuilder.addEvent` for a freshly created event: create the event,
    /// let `fill` set its attributes, then route it into the transaction.
    pub(crate) fn add_history_event(
        &mut self,
        timestamp_unix_nanos: i64,
        event_type: EventType,
        fill: impl FnOnce(&mut HistoryEvent),
    ) -> Result<HistoryEvent> {
        let mut event = self.create_new_history_event(event_type, timestamp_unix_nanos)?;
        fill(&mut event);
        self.append_event(event.clone());
        Ok(event)
    }

    /// Go `checkAndClearTimerFiredEvent` (method form): searches buffered
    /// events, update-buffered events, then the history builder events.
    pub(crate) fn check_and_clear_timer_fired_event_in_state(&mut self, timer_id: &str) -> Option<HistoryEvent> {
        if let Some(e) = check_and_clear_timer_fired_event(&mut self.buffered_events, timer_id) {
            return Some(e);
        }
        if let Some(e) = check_and_clear_timer_fired_event(&mut self.update_buffered_events, timer_id) {
            return Some(e);
        }
        check_and_clear_timer_fired_event(&mut self.history_events, timer_id)
    }
}

/// Go mutable state `taskGenerator` environment: domain cache, rand and
/// `WorkflowDeletionJitterRange(domain)` config.
#[derive(Clone, Default)]
pub struct TaskGenEnv {
    pub domains: Option<std::sync::Arc<dyn TaskGeneratorDomains + Send + Sync>>,
    pub deletion_jitter_range: i32,
    /// Go config NormalDecisionScheduleToStartTimeout / ...MaxAttempts (domain-scoped).
    pub normal_schedule_to_start_timeout_nanos: i64,
    pub normal_schedule_to_start_max_attempts: i64,
    rng_state: u64,
}

impl std::fmt::Debug for TaskGenEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskGenEnv")
            .field("domains", &self.domains.is_some())
            .field("deletion_jitter_range", &self.deletion_jitter_range)
            .finish()
    }
}

/// Delegating handle so the generic adapter can borrow the shared domain cache.
pub struct DomainsRef(Option<std::sync::Arc<dyn TaskGeneratorDomains + Send + Sync>>);

impl DomainsRef {
    fn get(&self) -> Result<&(dyn TaskGeneratorDomains + Send + Sync)> {
        match &self.0 {
            Some(d) => Ok(d.as_ref()),
            None => internal_err("task generator domain cache is not configured"),
        }
    }
}

impl TaskGeneratorDomains for DomainsRef {
    fn domain_id_by_name(&self, name: &str) -> Result<String> {
        self.get()?.domain_id_by_name(name)
    }
    fn retention_days(&self, domain_id: &str, workflow_id: &str) -> Result<Option<i32>> {
        match &self.0 {
            Some(d) => d.retention_days(domain_id, workflow_id),
            None => Ok(None),
        }
    }
    fn is_global_domain(&self, domain_id: &str) -> Result<bool> {
        self.get()?.is_global_domain(domain_id)
    }
}

impl TypedMutableState {
    /// Runs one Go `e.taskGenerator.Generate*` call against this state and
    /// drains the generated tasks into `transfer_tasks`/`timer_tasks`
    /// (Go `AddTransferTasks`/`AddTimerTasks`).
    pub(crate) fn run_task_generator<R>(
        &mut self,
        decision_schedule_to_start_timeout_nanos: i64,
        f: impl FnOnce(&mut MutableStateTaskGenerator<'_, TypedTaskGeneratorState<'_, DomainsRef>>) -> Result<R>,
    ) -> Result<R> {
        let domains = DomainsRef(self.task_gen_env.domains.clone());
        let mut rng = self.task_gen_env.rng_state;
        if rng == 0 {
            rng = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
                | 1;
        }
        let mut random = |upper: i64| -> i64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            if upper <= 0 { 0 } else { (rng % upper as u64) as i64 }
        };
        let (result, transfer, timer, timer_status) = {
            let mut adapter = TypedTaskGeneratorState::new(self, &domains, &mut random);
            adapter.decision_schedule_to_start_timeout_nanos = decision_schedule_to_start_timeout_nanos;
            let result = f(&mut MutableStateTaskGenerator { state: &mut adapter });
            (result, adapter.transfer_tasks, adapter.timer_tasks, adapter.activity_timer_status)
        };
        for (schedule_id, (timer_task_status, last_heartbeat_visibility)) in timer_status {
            if let Some(activity) = self.pending_activity_info_ids.get_mut(&schedule_id) {
                activity.timer_task_status = timer_task_status;
                activity.last_heartbeat_timeout_visibility_in_seconds = last_heartbeat_visibility;
                self.update_activity_infos.insert(schedule_id, activity.clone());
            }
        }
        self.task_gen_env.rng_state = rng;
        self.transfer_tasks.extend(transfer);
        self.timer_tasks.extend(timer);
        result
    }

    /// Go `GetDecisionScheduleToStartTimeout` from the stored task-generator env.
    pub(crate) fn env_decision_schedule_to_start_timeout_nanos(&self) -> i64 {
        if !self.execution_info.sticky_task_list.is_empty() {
            return self.execution_info.sticky_schedule_to_start_timeout_nanos;
        }
        if self.execution_info.decision_attempt < self.task_gen_env.normal_schedule_to_start_max_attempts {
            return self.task_gen_env.normal_schedule_to_start_timeout_nanos;
        }
        0
    }

    /// Go `taskGenerator.GenerateWorkflowCloseTasks(event, WorkflowDeletionJitterRange(domain))`.
    pub(crate) fn generate_workflow_close_tasks(&mut self, event: &HistoryEvent) -> Result<()> {
        let jitter = self.task_gen_env.deletion_jitter_range;
        self.run_task_generator(0, |g| g.generate_workflow_close_tasks(event, jitter))
    }
}

/// Go package-level `checkAndClearTimerFiredEvent`.
pub fn check_and_clear_timer_fired_event(events: &mut Vec<HistoryEvent>, timer_id: &str) -> Option<HistoryEvent> {
    let idx = events.iter().position(|event| {
        matches!(event.event_type, Some(EventType::TimerFired))
            && event
                .timer_fired_event_attributes
                .as_ref()
                .is_some_and(|a| a.timer_id == timer_id)
    })?;
    Some(events.remove(idx))
}
