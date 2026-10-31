//! Admin API handler boundary.

use cadence_api::errors::InternalServiceError;

#[derive(Debug, Default)]
pub struct Handler;

impl Handler {
    pub fn dispatch(&self, operation: &str) -> Result<(), InternalServiceError> {
        Err(crate::handler_workflow::unimplemented_operation(operation))
    }
}
