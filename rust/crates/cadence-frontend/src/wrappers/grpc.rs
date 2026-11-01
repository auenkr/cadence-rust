//! gRPC/YARPC adapter registration surface (Go: wrappers/grpc).
//!
//! The generated protobuf bindings own field conversion. This module keeps
//! the service registration groups aligned with Go's APIHandler.Register.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiGroup {
    Domain,
    Workflow,
    Worker,
    Visibility,
    Meta,
    Schedule,
    Semaphore,
}

pub const API_GROUPS: &[ApiGroup] = &[
    ApiGroup::Domain,
    ApiGroup::Workflow,
    ApiGroup::Worker,
    ApiGroup::Visibility,
    ApiGroup::Meta,
    ApiGroup::Schedule,
    ApiGroup::Semaphore,
];

/// gRPC transport errors are converted from the same internal Cadence error
/// values used by the Thrift adapter; never turn a typed API error into text.
pub fn error_message(error: &cadence_api::errors::CadenceError) -> String {
    error.to_string()
}

/// Status code selected by Go's `common/types/mapper/proto.FromError` for the
/// Cadence errors represented by this API crate.
pub fn error_code(error: &cadence_api::errors::CadenceError) -> tonic::Code {
    use cadence_api::errors::CadenceError;
    match error {
        CadenceError::AccessDenied(_) => tonic::Code::PermissionDenied,
        CadenceError::Internal(_) => tonic::Code::Internal,
        CadenceError::EntityNotExists(_) | CadenceError::WorkflowAlreadyCompleted(_) => {
            tonic::Code::NotFound
        }
        CadenceError::BadRequest(_) | CadenceError::QueryFailed(_) => tonic::Code::InvalidArgument,
        CadenceError::CancellationAlreadyRequested(_)
        | CadenceError::DomainAlreadyExists(_)
        | CadenceError::WorkflowAlreadyStarted(_) => tonic::Code::AlreadyExists,
        CadenceError::DomainNotActive(_) | CadenceError::ClientVersionNotSupported(_) => {
            tonic::Code::FailedPrecondition
        }
        CadenceError::LimitExceeded(_) | CadenceError::ServiceBusy(_) => {
            tonic::Code::ResourceExhausted
        }
        CadenceError::ShardOwnershipLost(_) | CadenceError::RetryTaskV2(_) => tonic::Code::Aborted,
        CadenceError::Unknown(_) => tonic::Code::Unknown,
    }
}

/// Convert a Cadence error at the gRPC boundary using Go's protobuf mapper.
/// Keeping this as a single operation prevents adapters from retaining a
/// typed error's message while accidentally assigning tonic's default code.
pub fn to_status(error: &cadence_api::errors::CadenceError) -> tonic::Status {
    tonic::Status::new(error_code(error), error_message(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_all_go_api_procedure_groups() {
        assert_eq!(API_GROUPS.len(), 7);
        assert!(API_GROUPS.contains(&ApiGroup::Workflow));
        assert!(API_GROUPS.contains(&ApiGroup::Semaphore));
    }

    #[test]
    fn grpc_error_status_preserves_go_error_categories() {
        use cadence_api::errors::{BadRequestError, CadenceError, ServiceBusyError};
        assert_eq!(
            error_code(&CadenceError::BadRequest(BadRequestError::new("bad"))),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            error_code(&CadenceError::ServiceBusy(ServiceBusyError::new("busy"))),
            tonic::Code::ResourceExhausted
        );
        let status = to_status(&CadenceError::BadRequest(BadRequestError::new("bad")));
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "bad");
    }
}
