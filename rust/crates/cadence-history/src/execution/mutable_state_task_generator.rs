//! Durable transfer/timer task construction from Go execution task generator.
use cadence_api::{enums::ContinueAsNewInitiator, service_types::HistoryEvent};
use crate::{mutable_state::DecisionInfo, mutable_state_activity::ActivityInfo};
use super::{child_workflow::ChildExecutionInfo, context::WorkflowExecutionKey, timer_sequence::{TimerSequence, TimerSequenceState, TimerType}};

#[derive(Debug, Clone, Default)]
pub struct ExecutionTaskInfo {
    pub key: Option<WorkflowExecutionKey>,
    pub task_list: String,
    pub task_list_kind: i32,
    pub workflow_timeout_seconds: i32,
    pub expiration_unix_nanos: Option<i64>,
    pub decision_start_to_close_timeout_seconds: i32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskKind {
    WorkflowTimeout, CloseExecution, DeleteHistoryEvent, RecordWorkflowStarted,
    WorkflowBackoff { timeout_type: i32 },
    Decision { target_domain_id: String, schedule_id: i64, original_task_list: String, original_task_list_kind: i32 },
    DecisionTimeout { timeout_type: TimerType, event_id: i64, schedule_attempt: i64 },
    Activity { target_domain_id: String, schedule_id: i64 },
    ActivityRetry { event_id: i64, attempt: i64 },
    StartChild { target_domain_id: String, target_workflow_id: String, initiated_id: i64 },
    CancelExecution { target_domain_id: String, target_workflow_id: String, target_run_id: String, child_only: bool, initiated_id: i64 },
    SignalExecution { target_domain_id: String, target_workflow_id: String, target_run_id: String, child_only: bool, initiated_id: i64 },
    UserTimer { event_id: i64 },
    ActivityTimeout { timeout_type: TimerType, event_id: i64, attempt: i64 },
    UpsertWorkflowSearchAttributes, ResetWorkflow,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedTask {
    pub workflow: WorkflowExecutionKey,
    pub version: i64,
    /// None means the shard supplies visibility time while assigning task IDs.
    pub visibility_timestamp_unix_nanos: Option<i64>,
    pub task_list: String,
    pub kind: TaskKind,
}

pub trait TaskGeneratorState: TimerSequenceState {
    fn execution(&self) -> ExecutionTaskInfo;
    fn workflow_key(&self) -> WorkflowExecutionKey;
    fn current_version(&self) -> i64;
    fn decision(&self, schedule_id: i64) -> Option<DecisionInfo>;
    fn update_decision(&mut self, decision: DecisionInfo);
    fn decision_schedule_to_start_timeout_nanos(&self) -> i64;
    fn activity_info(&self, schedule_id: i64) -> Option<ActivityInfo>;
    fn child_info(&self, initiated_id: i64) -> Option<ChildExecutionInfo>;
    fn has_request_cancel(&self, initiated_id: i64) -> bool;
    fn has_signal(&self, initiated_id: i64) -> bool;
    /// Rust's signal-initiated attributes lack Go's ChildWorkflowOnly; callers supply it.
    fn signal_child_only(&self, initiated_id: i64) -> bool;
    fn domain_id(&self, name: &str) -> Result<String, Self::Error>;
    /// Missing domains return None (one-day retention); all other errors propagate.
    fn retention_days(&self) -> Result<Option<i32>, Self::Error>;
    fn is_global_domain(&self, domain_id: &str) -> Result<bool, Self::Error>;
    fn internal_error(&self, message: String) -> Self::Error;
    fn bad_request(&self, message: String) -> Self::Error;
    fn target_domain_error(&self, error: Self::Error) -> Self::Error;
    /// Uniform [0, upper), matching Go rand.Intn, injectable for replay tests.
    fn random_below(&mut self, upper: i64) -> i64;
    fn add_transfer_task(&mut self, task: GeneratedTask);
    fn add_timer_task(&mut self, task: GeneratedTask);
}
pub struct MutableStateTaskGenerator<'a, S> { pub state: &'a mut S }
impl<S: TaskGeneratorState> MutableStateTaskGenerator<'_, S> {
    fn task(&self, version: i64, timestamp: Option<i64>, task_list: String, kind: TaskKind) -> GeneratedTask {
        GeneratedTask { workflow: self.state.workflow_key(), version, visibility_timestamp_unix_nanos: timestamp, task_list, kind }
    }
    pub fn generate_workflow_start_tasks(&mut self, start_time: i64, event: &HistoryEvent) -> Result<(), S::Error> {
        let attr = event.workflow_execution_started_event_attributes.as_ref().ok_or_else(|| self.state.internal_error("missing workflow start attributes".into()))?;
        let info = self.state.execution();
        let mut timeout = start_time + (i64::from(info.workflow_timeout_seconds) + i64::from(attr.first_decision_task_backoff_seconds.unwrap_or_default())) * 1_000_000_000;
        if attr.attempt > 0 { if let Some(expiration) = info.expiration_unix_nanos { timeout = timeout.min(expiration); } }
        let task = self.task(event.version, Some(timeout), info.task_list, TaskKind::WorkflowTimeout);
        self.state.add_timer_task(task);
        Ok(())
    }
    pub fn generate_workflow_close_tasks(&mut self, event: &HistoryEvent, jitter_range: i32) -> Result<(), S::Error> {
        let info = self.state.execution();
        let task = self.task(event.version, None, info.task_list.clone(), TaskKind::CloseExecution);
        self.state.add_transfer_task(task);
        let retention = self.state.retention_days()?.unwrap_or(1);
        let mut duration = i64::from(retention) * 24 * 60 * 60 * 1_000_000_000;
        if jitter_range > 1 { duration += self.state.random_below(i64::from(jitter_range) * 60) * 1_000_000_000; }
        let task = self.task(event.version, Some(event.timestamp.unwrap_or_default() + duration), info.task_list, TaskKind::DeleteHistoryEvent);
        self.state.add_timer_task(task);
        Ok(())
    }
    pub fn generate_delayed_decision_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let attr = event.workflow_execution_started_event_attributes.as_ref().ok_or_else(|| self.state.internal_error("missing workflow start attributes".into()))?;
        let timeout_type = match attr.initiator {
            None | Some(ContinueAsNewInitiator::CronSchedule) => 1,
            Some(ContinueAsNewInitiator::RetryPolicy) => 0,
            Some(ContinueAsNewInitiator::Decider) => return Err(self.state.internal_error("encounter continue as new iterator & first decision delay not 0".into())),
            Some(unknown) => return Err(self.state.internal_error(format!("unknown iterator retry policy: {unknown:?}"))),
        };
        let timestamp = event.timestamp.unwrap_or_default() + i64::from(attr.first_decision_task_backoff_seconds.unwrap_or_default()) * 1_000_000_000;
        let task = self.task(event.version, Some(timestamp), self.state.execution().task_list, TaskKind::WorkflowBackoff { timeout_type });
        self.state.add_timer_task(task);
        Ok(())
    }
    pub fn generate_record_workflow_started_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let task = self.task(event.version, None, self.state.execution().task_list, TaskKind::RecordWorkflowStarted);
        self.state.add_transfer_task(task); Ok(())
    }
    pub fn generate_decision_schedule_tasks(&mut self, id: i64) -> Result<(), S::Error> {
        let decision = self.state.decision(id).ok_or_else(|| self.state.internal_error(format!("it could be a bug, cannot get pending decision: {id}")))?;
        let info = self.state.execution();
        let task = self.task(decision.version, None, decision.task_list, TaskKind::Decision { target_domain_id: self.state.workflow_key().domain_id, schedule_id: decision.schedule_id, original_task_list: info.task_list.clone(), original_task_list_kind: info.task_list_kind });
        self.state.add_transfer_task(task);
        let timeout = self.state.decision_schedule_to_start_timeout_nanos();
        if timeout != 0 {
            let task = self.task(decision.version, Some(decision.schedule_timestamp + timeout), info.task_list, TaskKind::DecisionTimeout { timeout_type: TimerType::ScheduleToStart, event_id: decision.schedule_id, schedule_attempt: decision.attempt });
            self.state.add_timer_task(task);
        }
        Ok(())
    }
    pub fn generate_decision_start_tasks(&mut self, id: i64) -> Result<(), S::Error> {
        let mut decision = self.state.decision(id).ok_or_else(|| self.state.internal_error(format!("it could be a bug, cannot get pending decision: {id}")))?;
        let info = self.state.execution();
        let mut timeout = i64::from(decision.start_to_close_timeout_seconds) * 1_000_000_000;
        if decision.attempt > 1 {
            timeout = next_decision_timeout(decision.attempt, i64::from(info.decision_start_to_close_timeout_seconds) * 1_000_000_000, |upper| self.state.random_below(upper));
            decision.start_to_close_timeout_seconds = (timeout / 1_000_000_000) as i32;
            self.state.update_decision(decision.clone());
        }
        let task = self.task(decision.version, Some(decision.started_timestamp + timeout), info.task_list, TaskKind::DecisionTimeout { timeout_type: TimerType::StartToClose, event_id: decision.schedule_id, schedule_attempt: decision.attempt });
        self.state.add_timer_task(task); Ok(())
    }
    pub fn generate_activity_transfer_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let ai = self.activity(event.event_id)?;
        let target_domain_id = if ai.domain_id.is_empty() {
            let attr = event.activity_task_scheduled_event_attributes.as_ref().ok_or_else(|| self.state.internal_error("missing activity scheduled attributes".into()))?;
            self.target_domain_id(attr.domain.as_deref().unwrap_or_default())?
        } else { ai.domain_id };
        let task = self.task(ai.version, None, ai.task_list, TaskKind::Activity { target_domain_id, schedule_id: ai.schedule_id });
        self.state.add_transfer_task(task); Ok(())
    }
    fn activity(&self, id: i64) -> Result<ActivityInfo, S::Error> { self.state.activity_info(id).ok_or_else(|| self.state.internal_error(format!("it could be a bug, cannot get pending activity: {id}"))) }
    pub fn generate_activity_retry_tasks(&mut self, id: i64) -> Result<(), S::Error> {
        let ai = self.activity(id)?;
        let task = self.task(ai.version, Some(ai.scheduled_time), ai.task_list, TaskKind::ActivityRetry { event_id: ai.schedule_id, attempt: i64::from(ai.attempt) });
        self.state.add_timer_task(task); Ok(())
    }
    pub fn generate_child_workflow_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let id = event.event_id;
        let child = self.state.child_info(id).ok_or_else(|| self.state.internal_error(format!("it could be a bug, cannot get pending child workflow: {id}")))?;
        let domain = self.state.workflow_key().domain_id;
        let target = if child.domain_id.is_empty() { domain.clone() } else { child.domain_id };
        self.validate_child_workflow_parameters(&domain, &target)?;
        let task = self.task(child.version, None, self.state.execution().task_list, TaskKind::StartChild { target_domain_id: target, target_workflow_id: child.started_workflow_id, initiated_id: child.initiated_id });
        self.state.add_transfer_task(task); Ok(())
    }
    pub fn generate_request_cancel_external_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let attr = event.request_cancel_external_workflow_execution_initiated_event_attributes.as_ref().ok_or_else(|| self.state.internal_error("missing request cancel attributes".into()))?;
        if !self.state.has_request_cancel(event.event_id) { return Err(self.state.internal_error(format!("it could be a bug, cannot get pending request cancel external workflow: {}", event.event_id))); }
        let execution = attr.workflow_execution.clone().unwrap_or_default();
        let kind = TaskKind::CancelExecution { target_domain_id: self.target_domain_id(&attr.domain)?, target_workflow_id: execution.workflow_id, target_run_id: execution.run_id, child_only: attr.child_workflow_only, initiated_id: event.event_id };
        let task = self.task(event.version, None, self.state.execution().task_list, kind);
        self.state.add_transfer_task(task); Ok(())
    }
    pub fn generate_signal_external_tasks(&mut self, event: &HistoryEvent) -> Result<(), S::Error> {
        let attr = event.signal_external_workflow_execution_initiated_event_attributes.as_ref().ok_or_else(|| self.state.internal_error("missing signal attributes".into()))?;
        if !self.state.has_signal(event.event_id) { return Err(self.state.internal_error(format!("it could be a bug, cannot get pending signal external workflow: {}", event.event_id))); }
        let execution = attr.workflow_execution.clone().unwrap_or_default();
        let kind = TaskKind::SignalExecution { target_domain_id: self.target_domain_id(&attr.domain)?, target_workflow_id: execution.workflow_id, target_run_id: execution.run_id, child_only: self.state.signal_child_only(event.event_id), initiated_id: event.event_id };
        let task = self.task(event.version, None, self.state.execution().task_list, kind);
        self.state.add_transfer_task(task); Ok(())
    }
    pub fn generate_workflow_search_attr_tasks(&mut self) -> Result<(), S::Error> { let task = self.task(self.state.current_version(), None, self.state.execution().task_list, TaskKind::UpsertWorkflowSearchAttributes); self.state.add_transfer_task(task); Ok(()) }
    pub fn generate_workflow_reset_tasks(&mut self) -> Result<(), S::Error> { let task = self.task(self.state.current_version(), None, self.state.execution().task_list, TaskKind::ResetWorkflow); self.state.add_transfer_task(task); Ok(()) }
    pub fn generate_activity_timer_tasks(&mut self) -> Result<(), S::Error> { TimerSequence { state: self.state }.create_next_activity_timer()?; Ok(()) }
    pub fn generate_user_timer_tasks(&mut self) -> Result<(), S::Error> { TimerSequence { state: self.state }.create_next_user_timer()?; Ok(()) }
    fn target_domain_id(&self, name: &str) -> Result<String, S::Error> { if name.is_empty() { Ok(self.state.workflow_key().domain_id) } else { self.state.domain_id(name) } }
    pub fn validate_child_workflow_parameters(&self, domain: &str, target: &str) -> Result<(), S::Error> {
        if domain == target { return Ok(()); }
        let target_global = self.state.is_global_domain(target).map_err(|e| self.state.target_domain_error(e))?;
        if self.state.is_global_domain(domain)? || target_global { return Err(self.state.bad_request(format!("The child workflow is trying to use domain {target} but it's running in domain {domain}. Cross-cluster and cross domain child workflows are not supported for global domains"))); }
        Ok(())
    }
}
pub fn next_decision_timeout(attempt: i64, default_timeout: i64, mut random_below: impl FnMut(i64) -> i64) -> i64 {
    if attempt <= 1 { return default_timeout; }
    let next = (60_000_000_000_f64 * 2_f64.powf((attempt - 2) as f64)).min(300_000_000_000_f64);
    let jitter = ((0.2 * next) as i64).max(1);
    (next * 0.8 + random_below(jitter) as f64) as i64
}

// ---------------------------------------------------------------------------
// Adapter binding the generator to `TypedMutableState` (Go: the generator's
// `mutableState MutableState` + `domainCache` + `rand`).
//
// Callers (Go call sites of NewMutableStateTaskGenerator in the
// mutable_state_methods_* files) build a `TypedTaskGeneratorState`, run a
// `MutableStateTaskGenerator { state: &mut adapter }` method, then drain
// `transfer_tasks`/`timer_tasks` into the state's task categories
// (Go `AddTransferTasks`/`AddTimerTasks`) and write back
// `activity_timer_status` (Go ActivityInfo.TimerTaskStatus, which the base
// `ActivityInfo` port does not carry yet).
// ---------------------------------------------------------------------------
use std::collections::BTreeMap;

use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError};
use super::timer::TimerInfo;
use super::timer_sequence::{ActivityTimerInfo, TimerSequenceId};

/// Go `cache.DomainCache` subset used by the task generator.
pub trait TaskGeneratorDomains {
    /// Go `GetDomainID(name)`.
    fn domain_id_by_name(&self, name: &str) -> Result<String, TypedMutableStateError>;
    /// Go `GetDomainByID(id)` then `GetRetentionDays(workflowID)`; `Ok(None)` for EntityNotExists.
    fn retention_days(&self, domain_id: &str, workflow_id: &str) -> Result<Option<i32>, TypedMutableStateError>;
    /// Go `GetDomainByID(id).IsGlobalDomain()`.
    fn is_global_domain(&self, domain_id: &str) -> Result<bool, TypedMutableStateError>;
}

pub struct TypedTaskGeneratorState<'a, D: TaskGeneratorDomains> {
    pub state: &'a mut TypedMutableState,
    pub domains: &'a D,
    /// Go `rand.Intn`: uniform in [0, upper).
    pub random: &'a mut dyn FnMut(i64) -> i64,
    /// Go `GetDecisionScheduleToStartTimeout` (sticky timeout, normal
    /// schedule-to-start while attempt < max attempts, else 0), nanoseconds.
    pub decision_schedule_to_start_timeout_nanos: i64,
    /// schedule_id -> (TimerTaskStatus, LastHeartbeatTimeoutVisibilityInSeconds).
    pub activity_timer_status: BTreeMap<i64, (i32, i64)>,
    /// initiated_id -> ChildWorkflowOnly for pending signals.
    pub signal_child_only: BTreeMap<i64, bool>,
    pub transfer_tasks: Vec<GeneratedTask>,
    pub timer_tasks: Vec<GeneratedTask>,
}

impl<'a, D: TaskGeneratorDomains> TypedTaskGeneratorState<'a, D> {
    pub fn new(state: &'a mut TypedMutableState, domains: &'a D, random: &'a mut dyn FnMut(i64) -> i64) -> Self {
        Self {
            state, domains, random, decision_schedule_to_start_timeout_nanos: 0,
            activity_timer_status: BTreeMap::new(), signal_child_only: BTreeMap::new(),
            transfer_tasks: Vec::new(), timer_tasks: Vec::new(),
        }
    }
    fn key(&self) -> WorkflowExecutionKey {
        let e = &self.state.execution_info;
        WorkflowExecutionKey { domain_id: e.domain_id.clone(), workflow_id: e.workflow_id.clone(), run_id: e.run_id.clone() }
    }
    fn generated(&self, version: i64, timestamp: Option<i64>, task_list: String, kind: TaskKind) -> GeneratedTask {
        GeneratedTask { workflow: self.key(), version, visibility_timestamp_unix_nanos: timestamp, task_list, kind }
    }
}

impl<D: TaskGeneratorDomains> TimerSequenceState for TypedTaskGeneratorState<'_, D> {
    type Error = TypedMutableStateError;
    fn user_timers(&self) -> Vec<TimerInfo> { self.state.pending_timer_info_ids.values().cloned().collect() }
    fn activities(&self) -> Vec<ActivityTimerInfo> {
        self.state.pending_activity_info_ids.values().map(|a| {
            let (status, hb) = self.activity_timer_status.get(&a.schedule_id).copied().unwrap_or_default();
            ActivityTimerInfo { activity: a.clone(), timer_task_status: status, last_heartbeat_timeout_visibility_in_seconds: hb }
        }).collect()
    }
    fn user_timer(&self, event_id: i64) -> Option<TimerInfo> {
        self.state.pending_timer_event_id_to_id.get(&event_id).and_then(|id| self.state.pending_timer_info_ids.get(id)).cloned()
    }
    fn activity(&self, event_id: i64) -> Option<ActivityTimerInfo> {
        let a = self.state.pending_activity_info_ids.get(&event_id)?;
        let (status, hb) = self.activity_timer_status.get(&event_id).copied().unwrap_or_default();
        Some(ActivityTimerInfo { activity: a.clone(), timer_task_status: status, last_heartbeat_timeout_visibility_in_seconds: hb })
    }
    fn missing_activity_info(&self, event_id: i64) -> Self::Error {
        TypedMutableStateError::Internal(format!("unable to load activity info {event_id}"))
    }
    fn update_user_timer(&mut self, timer: TimerInfo) -> Result<(), Self::Error> {
        self.state.update_user_timer(timer);
        Ok(())
    }
    fn update_activity_timer(&mut self, activity: ActivityTimerInfo) -> Result<(), Self::Error> {
        self.activity_timer_status.insert(activity.activity.schedule_id, (activity.timer_task_status, activity.last_heartbeat_timeout_visibility_in_seconds));
        self.state.update_activity(activity.activity);
        Ok(())
    }
    fn add_user_timer_task(&mut self, timer: TimerSequenceId) {
        let task = self.generated(self.state.get_current_version(), Some(timer.timestamp_unix_nanos), self.state.execution_info.task_list.clone(), TaskKind::UserTimer { event_id: timer.event_id });
        self.timer_tasks.push(task);
    }
    fn add_activity_timeout_task(&mut self, timer: TimerSequenceId, task_list: String) {
        let task = self.generated(self.state.get_current_version(), Some(timer.timestamp_unix_nanos), task_list, TaskKind::ActivityTimeout { timeout_type: timer.timer_type, event_id: timer.event_id, attempt: i64::from(timer.attempt) });
        self.timer_tasks.push(task);
    }
}

impl<D: TaskGeneratorDomains> TaskGeneratorState for TypedTaskGeneratorState<'_, D> {
    fn execution(&self) -> ExecutionTaskInfo {
        let e = &self.state.execution_info;
        ExecutionTaskInfo {
            key: Some(self.key()),
            task_list: e.task_list.clone(),
            task_list_kind: e.task_list_kind,
            workflow_timeout_seconds: (e.workflow_timeout_nanos / 1_000_000_000) as i32,
            expiration_unix_nanos: if e.expiration_time_unix_nanos != 0 { Some(e.expiration_time_unix_nanos) } else { None },
            decision_start_to_close_timeout_seconds: (e.decision_start_to_close_timeout_nanos / 1_000_000_000) as i32,
        }
    }
    fn workflow_key(&self) -> WorkflowExecutionKey { self.key() }
    fn current_version(&self) -> i64 { self.state.get_current_version() }
    fn decision(&self, schedule_id: i64) -> Option<DecisionInfo> {
        let e = &self.state.execution_info;
        if e.decision_schedule_id != schedule_id { return None; }
        let task_list = if e.sticky_task_list.is_empty() { e.task_list.clone() } else { e.sticky_task_list.clone() };
        Some(DecisionInfo {
            schedule_id: e.decision_schedule_id,
            started_id: e.decision_started_id,
            version: e.decision_version,
            request_id: e.decision_request_id.clone(),
            task_list,
            start_to_close_timeout_seconds: (e.decision_timeout_nanos / 1_000_000_000) as i32,
            started_timestamp: e.decision_started_time_unix_nanos,
            attempt: e.decision_attempt,
            schedule_timestamp: e.decision_scheduled_time_unix_nanos,
            original_schedule_timestamp: e.decision_original_scheduled_time_unix_nanos,
        })
    }
    fn update_decision(&mut self, decision: DecisionInfo) {
        let e = &mut self.state.execution_info;
        e.decision_version = decision.version;
        e.decision_schedule_id = decision.schedule_id;
        e.decision_started_id = decision.started_id;
        e.decision_request_id = decision.request_id;
        e.decision_timeout_nanos = i64::from(decision.start_to_close_timeout_seconds) * 1_000_000_000;
        e.decision_attempt = decision.attempt;
        e.decision_started_time_unix_nanos = decision.started_timestamp;
        e.decision_scheduled_time_unix_nanos = decision.schedule_timestamp;
        e.decision_original_scheduled_time_unix_nanos = decision.original_schedule_timestamp;
    }
    fn decision_schedule_to_start_timeout_nanos(&self) -> i64 { self.decision_schedule_to_start_timeout_nanos }
    fn activity_info(&self, schedule_id: i64) -> Option<ActivityInfo> { self.state.pending_activity_info_ids.get(&schedule_id).cloned() }
    fn child_info(&self, initiated_id: i64) -> Option<ChildExecutionInfo> { self.state.pending_child_execution_info_ids.get(&initiated_id).cloned() }
    fn has_request_cancel(&self, initiated_id: i64) -> bool { self.state.pending_request_cancel_info_ids.contains_key(&initiated_id) }
    fn has_signal(&self, initiated_id: i64) -> bool { self.state.pending_signal_info_ids.contains_key(&initiated_id) }
    fn signal_child_only(&self, initiated_id: i64) -> bool { self.signal_child_only.get(&initiated_id).copied().unwrap_or(false) }
    fn domain_id(&self, name: &str) -> Result<String, Self::Error> { self.domains.domain_id_by_name(name) }
    fn retention_days(&self) -> Result<Option<i32>, Self::Error> {
        let e = &self.state.execution_info;
        self.domains.retention_days(&e.domain_id, &e.workflow_id)
    }
    fn is_global_domain(&self, domain_id: &str) -> Result<bool, Self::Error> { self.domains.is_global_domain(domain_id) }
    fn internal_error(&self, message: String) -> Self::Error { TypedMutableStateError::Internal(message) }
    fn bad_request(&self, message: String) -> Self::Error { TypedMutableStateError::BadRequest(message) }
    fn target_domain_error(&self, error: Self::Error) -> Self::Error {
        TypedMutableStateError::Internal(format!("cannot get target domain for child workflow: {error}"))
    }
    fn random_below(&mut self, upper: i64) -> i64 { (self.random)(upper) }
    fn add_transfer_task(&mut self, task: GeneratedTask) { self.transfer_tasks.push(task); }
    fn add_timer_task(&mut self, task: GeneratedTask) { self.timer_tasks.push(task); }
}

#[cfg(test)]
mod typed_adapter_tests {
    use super::*;

    struct Domains;
    impl TaskGeneratorDomains for Domains {
        fn domain_id_by_name(&self, name: &str) -> Result<String, TypedMutableStateError> { Ok(format!("id-{name}")) }
        fn retention_days(&self, _: &str, _: &str) -> Result<Option<i32>, TypedMutableStateError> { Ok(Some(3)) }
        fn is_global_domain(&self, id: &str) -> Result<bool, TypedMutableStateError> { Ok(id == "global") }
    }

    fn state() -> TypedMutableState {
        let mut s = TypedMutableState::new();
        s.execution_info.domain_id = "d".into();
        s.execution_info.workflow_id = "w".into();
        s.execution_info.run_id = "r".into();
        s.execution_info.task_list = "tl".into();
        s.execution_info.workflow_timeout_nanos = 10_000_000_000;
        s
    }

    #[test]
    fn workflow_start_emits_timeout_timer() {
        let mut s = state();
        let mut rng = |_: i64| 0;
        let mut adapter = TypedTaskGeneratorState::new(&mut s, &Domains, &mut rng);
        let mut event = HistoryEvent::default();
        event.version = 7;
        event.workflow_execution_started_event_attributes = Some(Default::default());
        MutableStateTaskGenerator { state: &mut adapter }.generate_workflow_start_tasks(1_000_000_000, &event).unwrap();
        assert_eq!(adapter.timer_tasks.len(), 1);
        assert_eq!(adapter.timer_tasks[0].visibility_timestamp_unix_nanos, Some(11_000_000_000));
        assert_eq!(adapter.timer_tasks[0].version, 7);
        assert_eq!(adapter.timer_tasks[0].kind, TaskKind::WorkflowTimeout);
    }

    #[test]
    fn missing_decision_is_internal_error() {
        let mut s = state();
        let mut rng = |_: i64| 0;
        let mut adapter = TypedTaskGeneratorState::new(&mut s, &Domains, &mut rng);
        let err = MutableStateTaskGenerator { state: &mut adapter }.generate_decision_schedule_tasks(5).unwrap_err();
        assert!(matches!(err, TypedMutableStateError::Internal(m) if m.contains("cannot get pending decision: 5")));
    }
}
