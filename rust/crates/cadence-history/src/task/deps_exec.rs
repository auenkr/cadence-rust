//! Minimal stand-ins for the `execution` package surface used by task executors.
//! TODO(link): replace by `crate::execution::{Context, MutableState}`.

use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;

#[derive(Clone, Debug, Default)]
pub struct WorkflowExecutionInfo {
    pub decision_schedule_id: i64,
    pub decision_attempt: i64,
}

#[derive(Clone, Debug, Default)]
pub struct DecisionInfo {
    pub schedule_id: i64,
    pub started_id: i64,
}

#[derive(Clone, Debug, Default)]
pub struct ContinueAsNewWorkflowExecutionDecisionAttributes;

pub const DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION: &str = "ForceCloseDecision";

/// Go `execution.MutableState` (subset used by task_util; extended by executors).
#[async_trait]
pub trait MutableState: Send + Sync {
    fn get_execution_info(&self) -> WorkflowExecutionInfo;
    fn get_next_event_id(&self) -> i64;
    fn get_domain_entry(&self) -> Arc<DomainCacheEntry>;
    fn get_in_flight_decision(&self) -> Option<DecisionInfo>;
    fn add_timeout_workflow_event(&self, first_event_id: i64) -> Result<()>;
    async fn add_continue_as_new_event(
        &self,
        first_event_id: i64,
        decision_finished_event_id: i64,
        parent_domain_name: &str,
        attrs: &ContinueAsNewWorkflowExecutionDecisionAttributes,
    ) -> Result<Arc<dyn MutableState>>;
}

/// Go `execution.Context`.
#[async_trait]
pub trait ExecutionContext: Send + Sync {
    async fn load_workflow_execution(&self) -> Result<Arc<dyn MutableState>>;
    fn clear(&self);
}

/// Go `execution.FailDecision`.
/// TODO(link): replace by execution::fail_decision
pub fn fail_decision(_ms: &dyn MutableState, _decision: &DecisionInfo, _cause: &str) -> Result<()> {
    Err(TaskError::Other("fail_decision not linked".into()))
}
