//! Workflow termination helper from Go `workflow_execution_util.go`.
use cadence_api::enums::DecisionTaskFailedCause;
use crate::mutable_state::DecisionInfo;
use super::workflow::IDENTITY_HISTORY_SERVICE;

pub trait DecisionWorkflowState {
    type Error;
    fn in_flight_decision(&self) -> Option<DecisionInfo>;
    fn has_pending_decision(&self) -> bool;
    /// All failure fields other than IDs/cause/identity are empty or zero.
    fn add_decision_failed(&mut self, decision: &DecisionInfo, cause: DecisionTaskFailedCause, identity: &str) -> Result<(), Self::Error>;
    fn flush_buffered_events(&mut self) -> Result<(), Self::Error>;
    fn add_decision_scheduled(&mut self, bypass_task_generation: bool) -> Result<(), Self::Error>;
    fn decision_schedule_error(&self) -> Self::Error;
    fn add_workflow_terminated(&mut self, batch_first_event_id: i64, reason: &str, details: Vec<u8>, identity: &str) -> Result<(), Self::Error>;
}
pub fn fail_decision<S: DecisionWorkflowState>(state: &mut S, decision: &DecisionInfo, cause: DecisionTaskFailedCause) -> Result<(), S::Error> {
    state.add_decision_failed(decision, cause, IDENTITY_HISTORY_SERVICE)?;
    state.flush_buffered_events()
}
pub fn schedule_decision<S: DecisionWorkflowState>(state: &mut S) -> Result<(), S::Error> {
    if state.has_pending_decision() { return Ok(()); }
    state.add_decision_scheduled(false).map_err(|_| state.decision_schedule_error())
}
pub fn terminate_workflow<S: DecisionWorkflowState>(state: &mut S, batch_first_event_id: i64, reason: &str, details: Vec<u8>, identity: &str) -> Result<(), S::Error> {
    if let Some(decision) = state.in_flight_decision() { fail_decision(state, &decision, DecisionTaskFailedCause::ForceCloseDecision)?; }
    state.add_workflow_terminated(batch_first_event_id, reason, details, identity)
}
