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
            Some(ContinueAsNewInitiator::Decider) | Some(_) => return Err(self.state.internal_error("encounter continue as new iterator & first decision delay not 0".into())),
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
