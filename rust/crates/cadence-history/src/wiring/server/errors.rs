//! HistoryError -> transport error mapping.
use crate::engine::deps::HistoryError;
use cadence_api::CadenceError;

/// Thrift exception name for a handler error (names as declared in history.thrift).
pub fn thrift_exception(e: &HistoryError) -> (&'static str, String) {
    match e {
        HistoryError::Cadence(c) => match &**c {
            CadenceError::BadRequest(x) => ("BadRequestError", x.to_string()),
            CadenceError::EntityNotExists(x) => ("EntityNotExistsError", x.to_string()),
            CadenceError::DomainNotActive(x) => ("DomainNotActiveError", x.to_string()),
            CadenceError::WorkflowAlreadyStarted(x) => ("WorkflowExecutionAlreadyStartedError", x.to_string()),
            CadenceError::ServiceBusy(x) => ("ServiceBusyError", x.to_string()),
            CadenceError::LimitExceeded(x) => ("LimitExceededError", x.to_string()),
            CadenceError::ShardOwnershipLost(x) => ("ShardOwnershipLostError", x.to_string()),
            CadenceError::AccessDenied(x) => ("AccessDeniedError", x.to_string()),
            other => ("InternalServiceError", other.to_string()),
        },
        other => ("InternalServiceError", other.to_string()),
    }
}

pub fn grpc_status(e: &HistoryError) -> tonic::Status {
    let (name, msg) = thrift_exception(e);
    match name {
        "BadRequestError" => tonic::Status::invalid_argument(msg),
        "EntityNotExistsError" => tonic::Status::not_found(msg),
        "DomainNotActiveError" => tonic::Status::failed_precondition(msg),
        "WorkflowExecutionAlreadyStartedError" => tonic::Status::already_exists(msg),
        "ServiceBusyError" | "LimitExceededError" => tonic::Status::resource_exhausted(msg),
        "ShardOwnershipLostError" => tonic::Status::aborted(msg),
        "AccessDeniedError" => tonic::Status::permission_denied(msg),
        _ => tonic::Status::internal(msg),
    }
}

pub fn busy() -> HistoryError {
    cadence_api::errors::ServiceBusyError { message: "Too many outstanding requests to the cadence service".into(), reason: None }.into()
}
