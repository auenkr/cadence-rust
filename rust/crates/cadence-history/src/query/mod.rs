//! In-memory workflow query registry, ported from Go `service/history/query`.
use cadence_api::{
    enums::QueryResultType,
    tasks::WorkflowQuery,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;
use uuid::Uuid;

/// Preserve Go's distinction between a nil answer and a present empty answer.
#[derive(Debug, Clone)]
pub struct WorkflowQueryResult {
    pub result_type: Option<QueryResultType>,
    pub answer: Option<Vec<u8>>,
    pub error_message: String,
}
impl From<cadence_api::tasks::WorkflowQueryResult> for WorkflowQueryResult {
    fn from(result: cadence_api::tasks::WorkflowQueryResult) -> Self {
        let answered = result.result_type.unwrap_or(QueryResultType::Answered) == QueryResultType::Answered;
        Self { result_type: result.result_type, answer: if answered || !result.answer.is_empty() { Some(result.answer) } else { None }, error_message: result.error_message }
    }
}
pub async fn wait_for_termination(mut channel: watch::Receiver<bool>) {
    while !*channel.borrow_and_update() {
        if channel.changed().await.is_err() { return; }
    }
}
pub const TERMINATION_TYPE_COMPLETED: i32 = 0;
pub const TERMINATION_TYPE_UNBLOCKED: i32 = 1;
pub const TERMINATION_TYPE_FAILED: i32 = 2;
pub const ERR_TERMINATION_STATE_INVALID: &str = "query termination state invalid";
pub const ERR_ALREADY_IN_TERMINAL_STATE: &str = "query already in terminal state";
pub const ERR_QUERY_NOT_IN_TERMINAL_STATE: &str = "query not in terminal state";
pub const ERR_QUERY_NOT_EXISTS: &str = "query does not exist";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError(pub &'static str);
impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for QueryError {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationType {
    Completed,
    Unblocked,
    Failed,
}
#[derive(Debug, Clone)]
pub struct TerminationState {
    pub termination_type: TerminationType,
    pub query_result: Option<WorkflowQueryResult>,
    pub failure: Option<String>,
}
struct Query {
    id: String,
    input: WorkflowQuery,
    term: watch::Sender<bool>,
    state: Mutex<Option<TerminationState>>,
}
impl Query {
    fn new(input: WorkflowQuery) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            input,
            term: watch::channel(false).0,
            state: Mutex::new(None),
        }
    }
    fn termination(&self) -> Result<TerminationState, QueryError> {
        self.state
            .lock()
            .unwrap()
            .clone()
            .ok_or(QueryError(ERR_QUERY_NOT_IN_TERMINAL_STATE))
    }
    fn set_termination(&self, s: TerminationState) -> Result<(), QueryError> {
        validate_termination_state(Some(&s))?;
        let mut state = self.state.lock().unwrap();
        if state.is_some() {
            return Err(QueryError(ERR_ALREADY_IN_TERMINAL_STATE));
        }
        *state = Some(s);
        self.term.send_replace(true);
        Ok(())
    }
}
pub fn validate_termination_state(s: Option<&TerminationState>) -> Result<(), QueryError> {
    let s = s.ok_or(QueryError(ERR_TERMINATION_STATE_INVALID))?;
    match s.termination_type {
        TerminationType::Completed => {
            let r = s
                .query_result
                .as_ref()
                .ok_or(QueryError(ERR_TERMINATION_STATE_INVALID))?;
            if s.failure.is_some() {
                return Err(QueryError(ERR_TERMINATION_STATE_INVALID));
            }
            let answered = r.result_type.unwrap_or(QueryResultType::Answered) == QueryResultType::Answered
                && r.answer.is_some()
                && r.error_message.is_empty();
            let failed = r.result_type == Some(QueryResultType::Failed)
                && r.answer.is_none()
                && !r.error_message.is_empty();
            if answered || failed {
                Ok(())
            } else {
                Err(QueryError(ERR_TERMINATION_STATE_INVALID))
            }
        }
        TerminationType::Unblocked if s.query_result.is_none() && s.failure.is_none() => Ok(()),
        TerminationType::Failed if s.query_result.is_none() && s.failure.is_some() => Ok(()),
        _ => Err(QueryError(ERR_TERMINATION_STATE_INVALID)),
    }
}
#[derive(Default)]
struct RegistryState {
    buffered: HashMap<String, Arc<Query>>,
    completed: HashMap<String, Arc<Query>>,
    unblocked: HashMap<String, Arc<Query>>,
    failed: HashMap<String, Arc<Query>>,
}
#[derive(Default)]
pub struct Registry {
    state: Mutex<RegistryState>,
}
impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn has_buffered_query(&self) -> bool {
        !self.state.lock().unwrap().buffered.is_empty()
    }
    pub fn get_buffered_ids(&self) -> Vec<String> {
        ids(&self.state.lock().unwrap().buffered)
    }
    pub fn has_completed_query(&self) -> bool {
        !self.state.lock().unwrap().completed.is_empty()
    }
    pub fn get_completed_ids(&self) -> Vec<String> {
        ids(&self.state.lock().unwrap().completed)
    }
    pub fn has_unblocked_query(&self) -> bool {
        !self.state.lock().unwrap().unblocked.is_empty()
    }
    pub fn get_unblocked_ids(&self) -> Vec<String> {
        ids(&self.state.lock().unwrap().unblocked)
    }
    pub fn has_failed_query(&self) -> bool {
        !self.state.lock().unwrap().failed.is_empty()
    }
    pub fn get_failed_ids(&self) -> Vec<String> {
        ids(&self.state.lock().unwrap().failed)
    }
    pub fn buffer_query(&self, input: WorkflowQuery) -> (String, watch::Receiver<bool>) {
        let q = Arc::new(Query::new(input));
        let id = q.id.clone();
        let ch = q.term.subscribe();
        self.state.lock().unwrap().buffered.insert(id.clone(), q);
        (id, ch)
    }
    fn get(&self, id: &str) -> Result<Arc<Query>, QueryError> {
        let state = self.state.lock().unwrap();
        state
            .buffered
            .get(id)
            .or_else(|| state.completed.get(id))
            .or_else(|| state.unblocked.get(id))
            .or_else(|| state.failed.get(id))
            .cloned()
            .ok_or(QueryError(ERR_QUERY_NOT_EXISTS))
    }
    pub fn get_query_term_ch(&self, id: &str) -> Result<watch::Receiver<bool>, QueryError> {
        Ok(self.get(id)?.term.subscribe())
    }
    pub fn get_query_input(&self, id: &str) -> Result<WorkflowQuery, QueryError> {
        Ok(self.get(id)?.input.clone())
    }
    pub fn get_termination_state(&self, id: &str) -> Result<TerminationState, QueryError> {
        self.get(id)?.termination()
    }
    pub fn set_termination_state(&self, id: &str, s: TerminationState) -> Result<(), QueryError> {
        let mut state = self.state.lock().unwrap();
        let q = state
            .buffered
            .get(id)
            .ok_or(QueryError(ERR_QUERY_NOT_EXISTS))?
            .clone();
        q.set_termination(s.clone())?;
        state.buffered.remove(id);
        match s.termination_type {
            TerminationType::Completed => {
                state.completed.insert(id.to_owned(), q);
            }
            TerminationType::Unblocked => {
                state.unblocked.insert(id.to_owned(), q);
            }
            TerminationType::Failed => {
                state.failed.insert(id.to_owned(), q);
            }
        }
        Ok(())
    }
    pub fn remove_query(&self, id: &str) {
        let mut state = self.state.lock().unwrap();
        state.buffered.remove(id);
        state.completed.remove(id);
        state.unblocked.remove(id);
        state.failed.remove(id);
    }
}
fn ids(m: &HashMap<String, Arc<Query>>) -> Vec<String> {
    m.keys().cloned().collect()
}
