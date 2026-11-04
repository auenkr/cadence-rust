//! Task regeneration from Go `mutable_state_task_refresher.go`.
use cadence_api::service_types::HistoryEvent;
use super::timer_sequence::{TimerSequence, TimerSequenceState, TIMER_TASK_STATUS_NONE};
use crate::mutable_state::DecisionInfo;

#[derive(Debug, Clone, Copy)]
pub struct InitiatedInfo { pub event_batch_id: i64, pub initiated_id: i64, pub started_id: i64 }
#[derive(Debug, Clone, Copy)]
pub enum ExternalTaskKind { ChildWorkflow, RequestCancel, Signal }

/// Context bridges event cache, mutable-state updates, and task generator.
/// Cache reads must use the current branch token, execution identity and shard
/// ID; batch IDs are retained separately from event IDs for history-v2 reads.
pub trait TaskRefreshState: TimerSequenceState {
    fn start_event(&mut self) -> Result<HistoryEvent, Self::Error>;
    fn completion_event(&mut self) -> Result<HistoryEvent, Self::Error>;
    fn current_branch_token(&mut self) -> Result<Vec<u8>, Self::Error>;
    fn cached_event(&mut self, batch_id: i64, event_id: i64, branch: &[u8]) -> Result<HistoryEvent, Self::Error>;
    fn has_processed_or_pending_decision(&self) -> bool;
    fn is_closed(&self) -> bool;
    fn has_pending_decision(&self) -> bool;
    fn pending_decision(&self) -> Option<DecisionInfo>;
    fn missing_pending_decision(&self) -> Self::Error;
    fn pending_external(&self, kind: ExternalTaskKind) -> Vec<InitiatedInfo>;
    fn generate_workflow_start_tasks(&mut self, start_time: i64, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn generate_delayed_decision_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn generate_workflow_close_tasks(&mut self, event: &HistoryEvent, jitter_range: i32) -> Result<(), Self::Error>;
    fn generate_record_workflow_started_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn generate_decision_start_tasks(&mut self, schedule_id: i64) -> Result<(), Self::Error>;
    fn generate_decision_schedule_tasks(&mut self, schedule_id: i64) -> Result<(), Self::Error>;
    fn generate_activity_transfer_tasks(&mut self, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn generate_external_tasks(&mut self, kind: ExternalTaskKind, event: &HistoryEvent) -> Result<(), Self::Error>;
    fn generate_workflow_search_attr_tasks(&mut self) -> Result<(), Self::Error>;
}

pub fn refresh_tasks<S: TaskRefreshState>(state: &mut S, start_time: i64, deletion_jitter_range: i32, advanced_visibility_writing: bool) -> Result<(), S::Error> {
    refresh_tasks_for_workflow_start(state, start_time)?;
    refresh_tasks_for_workflow_close(state, deletion_jitter_range)?;
    refresh_tasks_for_record_workflow_started(state)?;
    refresh_tasks_for_decision(state)?;
    refresh_tasks_for_activity(state)?;
    refresh_tasks_for_timer(state)?;
    refresh_tasks_for_external(state, ExternalTaskKind::ChildWorkflow)?;
    refresh_tasks_for_external(state, ExternalTaskKind::RequestCancel)?;
    refresh_tasks_for_external(state, ExternalTaskKind::Signal)?;
    if advanced_visibility_writing { state.generate_workflow_search_attr_tasks()?; }
    Ok(())
}
pub fn refresh_tasks_for_workflow_start<S: TaskRefreshState>(state: &mut S, start_time: i64) -> Result<(), S::Error> {
    let event = state.start_event()?;
    state.generate_workflow_start_tasks(start_time, &event)?;
    let backoff = event.workflow_execution_started_event_attributes.as_ref().and_then(|a| a.first_decision_task_backoff_seconds).unwrap_or_default();
    if !state.has_processed_or_pending_decision() && backoff > 0 { state.generate_delayed_decision_tasks(&event)?; }
    Ok(())
}
pub fn refresh_tasks_for_workflow_close<S: TaskRefreshState>(state: &mut S, jitter_range: i32) -> Result<(), S::Error> {
    if state.is_closed() { let event = state.completion_event()?; state.generate_workflow_close_tasks(&event, jitter_range)?; }
    Ok(())
}
pub fn refresh_tasks_for_record_workflow_started<S: TaskRefreshState>(state: &mut S) -> Result<(), S::Error> {
    if !state.is_closed() { let event = state.start_event()?; state.generate_record_workflow_started_tasks(&event)?; }
    Ok(())
}
pub fn refresh_tasks_for_decision<S: TaskRefreshState>(state: &mut S) -> Result<(), S::Error> {
    if !state.has_pending_decision() { return Ok(()); }
    let decision = state.pending_decision().ok_or_else(|| state.missing_pending_decision())?;
    if decision.started_id != 0 { state.generate_decision_start_tasks(decision.schedule_id) } else { state.generate_decision_schedule_tasks(decision.schedule_id) }
}
pub fn refresh_tasks_for_activity<S: TaskRefreshState>(state: &mut S) -> Result<(), S::Error> {
    let branch = state.current_branch_token()?;
    for mut info in state.activities() {
        info.timer_task_status = TIMER_TASK_STATUS_NONE;
        state.update_activity_timer(info.clone())?;
        if info.activity.started_id != 0 { continue; }
        let event = state.cached_event(info.activity.scheduled_event_batch_id, info.activity.schedule_id, &branch)?;
        state.generate_activity_transfer_tasks(&event)?;
    }
    TimerSequence { state }.create_next_activity_timer()?;
    Ok(())
}
pub fn refresh_tasks_for_timer<S: TaskRefreshState>(state: &mut S) -> Result<(), S::Error> {
    for mut info in state.user_timers() { info.task_status = TIMER_TASK_STATUS_NONE; state.update_user_timer(info)?; }
    TimerSequence { state }.create_next_user_timer()?;
    Ok(())
}
pub fn refresh_tasks_for_external<S: TaskRefreshState>(state: &mut S, kind: ExternalTaskKind) -> Result<(), S::Error> {
    let branch = state.current_branch_token()?;
    for info in state.pending_external(kind) {
        if matches!(kind, ExternalTaskKind::ChildWorkflow) && info.started_id != 0 { continue; }
        let event = state.cached_event(info.event_batch_id, info.initiated_id, &branch)?;
        state.generate_external_tasks(kind, &event)?;
    }
    Ok(())
}
