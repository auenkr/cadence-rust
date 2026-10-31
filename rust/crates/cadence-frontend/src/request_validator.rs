//! Request validation entry points shared with the public Cadence API model.

use cadence_api::errors::BadRequestError;
use cadence_api::types::{StartWorkflowExecutionRequest, WorkflowExecution};

#[derive(Debug, Clone)]
pub struct RequestValidator {
    pub limits: cadence_api::validation::Limits,
}

impl Default for RequestValidator {
    fn default() -> Self {
        Self { limits: cadence_api::validation::Limits::cadence_default() }
    }
}

impl RequestValidator {
    pub fn validate_execution(&self, execution: Option<&WorkflowExecution>) -> Result<(), BadRequestError> {
        cadence_api::validation::check_execution(execution)
    }

    pub fn validate_start(&self, request: Option<&StartWorkflowExecutionRequest>) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(cadence_api::validation::err_request_not_set)?;
        cadence_api::validation::validate_start_workflow_execution_request(Some(request), &self.limits)
    }
}
