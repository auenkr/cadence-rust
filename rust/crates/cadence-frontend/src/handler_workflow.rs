//! Workflow API dispatch boundary. Implementations are added incrementally;
//! all public RPC method names stay in the generated API/transport surface.

use cadence_api::errors::InternalServiceError;

pub trait WorkflowHandler: Send + Sync {
    fn dispatch(&self, operation: &str) -> Result<(), InternalServiceError>;
}

#[derive(Debug, Default)]
pub struct Handler;

impl WorkflowHandler for Handler {
    fn dispatch(&self, operation: &str) -> Result<(), InternalServiceError> {
        Err(unimplemented_operation(operation))
    }
}

pub fn unimplemented_operation(operation: &str) -> InternalServiceError {
    InternalServiceError::new(format!("Frontend operation {operation} is not implemented"))
}
