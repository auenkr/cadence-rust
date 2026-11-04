//! Workflow execution identity and transaction coordination primitives.
//!
//! Persistence operations are implemented by the history persistence service;
//! this module owns the stable workflow identity used by load/update calls.

use cadence_api::types::WorkflowExecution;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkflowExecutionKey {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
}

impl WorkflowExecutionKey {
    pub fn new(domain_id: impl Into<String>, execution: &WorkflowExecution) -> Self {
        Self { domain_id: domain_id.into(), workflow_id: execution.workflow_id.clone(), run_id: execution.run_id.clone() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowContextError {
    #[error("workflow identity requires non-empty domain, workflow, and run IDs")]
    EmptyIdentity,
    #[error("workflow update conflict for {0:?}")]
    Conflict(WorkflowExecutionKey),
}

impl WorkflowExecutionKey {
    pub fn validate(&self) -> Result<(), WorkflowContextError> {
        if self.domain_id.is_empty() || self.workflow_id.is_empty() || self.run_id.is_empty() { return Err(WorkflowContextError::EmptyIdentity); }
        Ok(())
    }
}
