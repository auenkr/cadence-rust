//! Thrift dispatch wrapper boundary (Go: wrappers/thrift).
use cadence_api::errors::CadenceError;

pub const SERVICE_NAME: &str = "WorkflowService";

/// Return the generated method definition, including the pinned Thrift
/// exception field IDs, for the public WorkflowService surface.
pub fn method(name: &str) -> Option<&'static cadence_thrift::service::MethodDef> {
    cadence_thrift::service::METHODS
        .iter()
        .find(|method| method.service == SERVICE_NAME && method.method == name)
}

pub fn method_names() -> impl Iterator<Item = &'static str> {
    cadence_thrift::service::METHODS
        .iter()
        .filter(|method| method.service == SERVICE_NAME)
        .map(|method| method.method)
}

/// Resolve the declared Thrift exception field for a Cadence error. Go's
/// generated YARPC wrapper only returns exceptions declared by the method.
pub fn declared_exception(
    method: &cadence_thrift::service::MethodDef,
    error: &CadenceError,
) -> Option<(&'static str, i16)> {
    let name = match error {
        CadenceError::AccessDenied(_) => "AccessDeniedError",
        CadenceError::BadRequest(_) => "BadRequestError",
        CadenceError::CancellationAlreadyRequested(_) => "CancellationAlreadyRequestedError",
        CadenceError::ClientVersionNotSupported(_) => "ClientVersionNotSupportedError",
        CadenceError::DomainAlreadyExists(_) => "DomainAlreadyExistsError",
        CadenceError::DomainNotActive(_) => "DomainNotActiveError",
        CadenceError::EntityNotExists(_) => "EntityNotExistsError",
        // The metered Go wrapper turns this into an ordinary error before
        // either transport converter sees it.
        CadenceError::Internal(_) => return None,
        CadenceError::LimitExceeded(_) => "LimitExceededError",
        CadenceError::QueryFailed(_) => "QueryFailedError",
        CadenceError::ServiceBusy(_) => "ServiceBusyError",
        CadenceError::WorkflowAlreadyCompleted(_) => "WorkflowExecutionAlreadyCompletedError",
        CadenceError::WorkflowAlreadyStarted(_) => "WorkflowExecutionAlreadyStartedError",
        CadenceError::ShardOwnershipLost(_) => "ShardOwnershipLostError",
        CadenceError::RetryTaskV2(_) => "RetryTaskV2Error",
        CadenceError::Unknown(_) => return None,
    };
    method
        .exceptions
        .iter()
        .find(|(exception, _)| *exception == name)
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thrift_wrapper_uses_pinned_workflow_service_contract() {
        let start = method("StartWorkflowExecution").unwrap();
        assert_eq!(start.arg_type, "StartWorkflowExecutionRequest");
        assert!(start
            .exceptions
            .iter()
            .any(|(name, id)| *name == "BadRequestError" && *id == 1));
        assert!(method("NotAWorkflowMethod").is_none());
        assert!(method_names().any(|name| name == "GetWorkflowExecutionHistory"));
    }

    #[test]
    fn thrift_error_mapping_only_returns_declared_method_exceptions() {
        use cadence_api::errors::{BadRequestError, CadenceError, ServiceBusyError};
        let start = method("StartWorkflowExecution").unwrap();
        assert_eq!(
            declared_exception(start, &CadenceError::BadRequest(BadRequestError::new("bad"))),
            Some(("BadRequestError", 1))
        );
        assert!(declared_exception(
            start,
            &CadenceError::ServiceBusy(ServiceBusyError::new("busy"))
        )
        .is_some());
        let health = method("Health").unwrap();
        assert!(declared_exception(
            health,
            &CadenceError::BadRequest(BadRequestError::new("bad"))
        )
        .is_none());
    }
}
