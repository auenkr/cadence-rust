//! Decision RPC state transitions from handler.go.
//! TODO(link): execution cache owns locking, conditional persistence and context release.
use super::DecisionError;
use crate::query::{Registry, TerminationState, TerminationType};
use cadence_api::tasks::WorkflowQueryResult;
use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct DecisionInfo {
    pub schedule_id: i64, pub started_id: i64, pub request_id: String, pub attempt: i64,
    pub original_scheduled_timestamp: i64, pub scheduled_timestamp: i64, pub started_timestamp: i64,
}
#[derive(Clone, Copy, Default)]
pub struct UpdateAction { pub noop: bool }
pub trait LifecycleState {
    type Started;
    fn is_running(&self) -> bool;
    fn has_processed_or_pending_decision(&self) -> bool;
    fn add_first_decision_task_scheduled(&mut self) -> Result<(), DecisionError>;
    fn next_event_id(&self) -> i64;
    fn decision_info(&self, schedule_id: i64) -> Option<DecisionInfo>;
    fn stale_state(&mut self, operation: &str);
    fn add_decision_started(&mut self, schedule_id: i64, request_id: &str, identity: &str) -> Result<DecisionInfo, DecisionError>;
    fn started_response(&self, info: &DecisionInfo, identity: &str) -> Result<Self::Started, DecisionError>;
    fn add_decision_failed(&mut self, info: &DecisionInfo, cause: &str, details: &[u8], identity: &str, checksum: &str) -> Result<(), DecisionError>;
}
pub fn handle_decision_task_scheduled(state: &mut impl LifecycleState) -> Result<UpdateAction, DecisionError> {
    if !state.is_running() { return Err(DecisionError::new("workflow execution already completed")); }
    if state.has_processed_or_pending_decision() { return Ok(UpdateAction { noop: true }); }
    state.add_first_decision_task_scheduled()?;
    Ok(UpdateAction::default())
}
pub fn handle_decision_task_started<S: LifecycleState>(state: &mut S, schedule_id: i64, request_id: &str, identity: &str) -> Result<(UpdateAction, S::Started), DecisionError> {
    if !state.is_running() { return Err(DecisionError::new("workflow execution already completed")); }
    let decision = state.decision_info(schedule_id);
    if decision.is_none() && schedule_id >= state.next_event_id() {
        state.stale_state("RecordDecisionTaskStarted");
        return Err(DecisionError::new("cache mutable state could potentially be stale"));
    }
    let decision = decision.ok_or_else(|| DecisionError::new("Decision task not found."))?;
    if decision.started_id != -23 {
        if decision.request_id == request_id { return Ok((UpdateAction { noop: true }, state.started_response(&decision, identity)?)); }
        return Err(DecisionError::new("Decision task already started."));
    }
    let decision = state.add_decision_started(schedule_id, request_id, identity).map_err(|_| DecisionError::new("Unable to add DecisionTaskStarted event to history."))?;
    Ok((UpdateAction::default(), state.started_response(&decision, identity)?))
}
#[allow(clippy::too_many_arguments)]
pub fn handle_decision_task_failed(state: &mut impl LifecycleState, schedule_id: i64, attempt: i64, cause: &str, details: &[u8], identity: &str, checksum: &str) -> Result<(), DecisionError> {
    if !state.is_running() { return Err(DecisionError::new("workflow execution already completed")); }
    let decision = state.decision_info(schedule_id).filter(|d| d.attempt == attempt && d.started_id != -23).ok_or_else(|| DecisionError::new("Decision task not found."))?;
    state.add_decision_failed(&decision, cause, details, identity, checksum)
}
pub trait QueryObserver {
    fn worker_not_supports_consistent_query(&self, error: &str);
    fn invalid_query_state(&self, id: &str, state: TerminationType, error: &str);
    fn check_blob_size(&self, answer: &[u8]) -> Result<(), String>;
}
#[allow(clippy::too_many_arguments)]
pub fn handle_buffered_queries(registry: &Registry, observer: &impl QueryObserver, client_impl: &str, feature_version: &str, version_error: Option<&str>, results: &HashMap<String, WorkflowQueryResult>, create_new_decision: bool, heartbeating: bool) {
    if !registry.has_buffered_query() { return; }
    let set = |id: &str, state: TerminationState| {
        let kind = state.termination_type;
        if let Err(error) = registry.set_termination_state(id, state) { observer.invalid_query_state(id, kind, &error.to_string()); }
    };
    if let Some(error) = version_error.filter(|_| !client_impl.is_empty() && !feature_version.is_empty()) {
        observer.worker_not_supports_consistent_query(error);
        for id in registry.get_buffered_ids() { set(&id, TerminationState { termination_type: TerminationType::Failed, query_result: None, failure: Some(error.into()) }); }
        return;
    }
    if heartbeating { return; }
    for (id, result) in results {
        let state = match observer.check_blob_size(&result.answer) {
            Err(error) => TerminationState { termination_type: TerminationType::Failed, query_result: None, failure: Some(error) },
            Ok(()) => TerminationState { termination_type: TerminationType::Completed, query_result: Some(result.clone()), failure: None },
        };
        set(id, state);
    }
    if !create_new_decision {
        for id in registry.get_buffered_ids() { set(&id, TerminationState { termination_type: TerminationType::Unblocked, query_result: None, failure: None }); }
    }
}

#[derive(Default)]
pub struct CompletionRequest {
    pub schedule_id: i64, pub attempt: i64, pub force_new: bool, pub return_new: bool,
    pub decisions_empty: bool, pub checksum: String, pub identity: String,
}
pub struct DecisionOutcome {
    pub failed: Option<(String, Vec<u8>)>, pub activity_not_started_cancelled: bool,
    pub continued_as_new: bool, pub has_unhandled_events: bool,
}
#[derive(Debug)]
pub enum UpdateError { Conflict, TransactionSize(String), Other(DecisionError) }
pub trait CompletionState: LifecycleState {
    type Response;
    fn load(&mut self) -> Result<(), DecisionError>;
    fn load_execution_stats(&mut self) -> Result<(), DecisionError>;
    fn clear(&mut self);
    fn now_unix_nanos(&self) -> i64;
    fn heartbeat_timeout_nanos(&self) -> i64;
    fn max_reset_points(&self) -> usize;
    fn reset_point_count(&self) -> Option<usize>;
    fn metric(&mut self, name: &str);
    fn add_decision_timed_out(&mut self, decision: &DecisionInfo) -> Result<i64, DecisionError>;
    fn clear_stickiness(&mut self);
    fn add_decision_completed(&mut self, decision: &DecisionInfo, max_reset_points: usize) -> Result<i64, DecisionError>;
    fn set_sticky_and_client_headers(&mut self);
    fn bad_binary(&self, checksum: &str) -> bool;
    fn has_buffered_events(&self) -> bool;
    fn process_decisions(&mut self, completed_id: i64) -> Result<DecisionOutcome, DecisionError>;
    fn decision_attempt(&self) -> i64;
    fn decision_retry_max_attempts(&self) -> i64;
    fn terminate(&mut self, reason: &str, details: &[u8]) -> Result<(), DecisionError>;
    fn schedule_decision(&mut self, return_new: bool, heartbeat_timestamp: Option<i64>) -> Result<DecisionInfo, DecisionError>;
    fn update_active(&mut self, continued_as_new: bool) -> std::result::Result<(), UpdateError>;
    fn buffered_queries(&mut self, create_new: bool, heartbeat: bool);
    fn response(&mut self, new_decision: Option<&DecisionInfo>, return_new: bool, dispatch_activities: bool) -> Result<Self::Response, DecisionError>;
}
pub fn handle_decision_task_completed<S: CompletionState>(state: &mut S, request: &CompletionRequest, retry_count: usize) -> Result<S::Response, DecisionError> {
    for _ in 0..retry_count {
        state.load()?;
        if !state.is_running() { return Err(DecisionError::new("workflow execution already completed")); }
        state.load_execution_stats()?;
        let decision = state.decision_info(request.schedule_id);
        if decision.is_none() && request.schedule_id >= state.next_event_id() {
            state.stale_state("RespondDecisionTaskCompleted"); state.clear(); continue;
        }
        let decision = decision.filter(|d| d.attempt == request.attempt && d.started_id != -23).ok_or_else(|| DecisionError::new("Decision task not found."))?;
        let max_points = state.max_reset_points();
        if state.reset_point_count() == Some(max_points) { state.metric("AutoResetPointsLimitExceededCounter"); }
        let heartbeat = request.force_new && request.decisions_empty;
        let timed_out = heartbeat && decision.original_scheduled_timestamp > 0 && state.now_unix_nanos() > decision.original_scheduled_timestamp.saturating_add(state.heartbeat_timeout_nanos());
        let completed_id = if timed_out {
            state.metric("DecisionHeartbeatTimeoutCounter");
            let id = state.add_decision_timed_out(&decision).map_err(|_| DecisionError::new("Failed to add decision timeout event."))?;
            state.clear_stickiness(); id
        } else {
            state.add_decision_completed(&decision, max_points).map_err(|_| DecisionError::new("Unable to add DecisionTaskCompleted event to history."))?
        };
        let buffered = state.has_buffered_events();
        state.set_sticky_and_client_headers();
        let mut outcome = if state.bad_binary(&request.checksum) {
            DecisionOutcome { failed: Some(("BAD_BINARY".into(), format!("binary {} is already marked as bad deployment", request.checksum).into_bytes())), activity_not_started_cancelled: false, continued_as_new: false, has_unhandled_events: buffered }
        } else { state.process_decisions(completed_id)? };
        if let Some((cause, details)) = outcome.failed.as_ref() {
            state.metric("FailedDecisionsCounter");
            state.clear(); state.load()?;
            state.add_decision_failed(&decision, cause, details, &request.identity, &request.checksum)?;
            let max = state.decision_retry_max_attempts();
            if max > 0 && state.decision_attempt() > max {
                let message = format!("Decision attempt exceeds limit. Last decision failure cause and details: {} - {:?}", cause, details);
                state.metric("DecisionRetriesExceededCounter");
                state.terminate("DECISION_ATTEMPTS_EXCEEDS_LIMIT", message.as_bytes())?;
            }
            outcome.has_unhandled_events = true; outcome.continued_as_new = false;
        }
        let create_new = state.is_running() && (outcome.has_unhandled_events || request.force_new || outcome.activity_not_started_cancelled);
        let new_decision = if create_new {
            let new = state.schedule_decision(request.return_new, if heartbeat && !timed_out { Some(decision.original_scheduled_timestamp) } else { None }).map_err(|_| DecisionError::new("Failed to add decision scheduled event."))?;
            if request.return_new { state.add_decision_started(new.schedule_id, "request-from-RespondDecisionTaskCompleted", &request.identity)?; }
            Some(new)
        } else { None };
        match state.update_active(outcome.continued_as_new) {
            Err(UpdateError::Conflict) => { state.metric("ConcurrencyUpdateFailureCounter"); continue; },
            Err(UpdateError::TransactionSize(message)) => {
                state.load()?; state.terminate("TRANSACTION_SIZE_EXCEEDS_LIMIT", message.as_bytes())?;
                state.update_active(false).map_err(|error| DecisionError::new(format!("{error:?}")))?;
                return Err(DecisionError::new(message));
            },
            Err(UpdateError::Other(error)) => return Err(error), Ok(()) => {},
        }
        state.buffered_queries(create_new, heartbeat);
        if timed_out { return Err(DecisionError::new("decision heartbeat timeout")); }
        return state.response(new_decision.as_ref(), request.return_new && create_new, state.is_running());
    }
    Err(DecisionError::new("maximum attempts exceeded to update history"))
}
pub fn get_decision_info_attempt(info: Option<&DecisionInfo>) -> i64 { info.map_or(0, |i| i.attempt) }
pub fn get_decision_info_started_id(info: Option<&DecisionInfo>) -> i64 { info.map_or(0, |i| i.started_id) }
