//! HistoryError -> transport error mapping.
use crate::engine::deps::HistoryError;
use cadence_api::CadenceError;
use prost::Message;
use tonic::Code;

/// Thrift exception name for a handler error (names as declared in history.thrift).
pub fn thrift_exception(e: &HistoryError) -> (&'static str, String) {
    match e {
        HistoryError::Cadence(c) => match &**c {
            CadenceError::BadRequest(x) => ("BadRequestError", x.to_string()),
            CadenceError::EntityNotExists(x) => ("EntityNotExistsError", x.to_string()),
            CadenceError::DomainNotActive(x) => ("DomainNotActiveError", x.to_string()),
            CadenceError::WorkflowAlreadyStarted(x) => ("WorkflowExecutionAlreadyStartedError", x.to_string()),
            CadenceError::WorkflowAlreadyCompleted(x) => ("WorkflowExecutionAlreadyCompletedError", x.to_string()),
            CadenceError::DomainAlreadyExists(x) => ("DomainAlreadyExistsError", x.to_string()),
            CadenceError::CancellationAlreadyRequested(x) => ("CancellationAlreadyRequestedError", x.to_string()),
            CadenceError::QueryFailed(x) => ("QueryFailedError", x.to_string()),
            CadenceError::ServiceBusy(x) => ("ServiceBusyError", x.to_string()),
            CadenceError::LimitExceeded(x) => ("LimitExceededError", x.to_string()),
            CadenceError::ShardOwnershipLost(x) => ("ShardOwnershipLostError", x.to_string()),
            CadenceError::RetryTaskV2(x) => ("RetryTaskV2Error", x.to_string()),
            CadenceError::AccessDenied(x) => ("AccessDeniedError", x.to_string()),
            CadenceError::ClientVersionNotSupported(x) => ("ClientVersionNotSupportedError", x.to_string()),
            other => ("InternalServiceError", other.to_string()),
        },
        HistoryError::EventAlreadyStarted(x) => ("EventAlreadyStartedError", x.to_string()),
        other => ("InternalServiceError", other.to_string()),
    }
}

/// gRPC code per Go `proto.FromError` (`common/types/mapper/proto/errors.go`).
fn grpc_code(name: &str) -> Code {
    match name {
        "BadRequestError" | "QueryFailedError" => Code::InvalidArgument,
        "EntityNotExistsError" | "WorkflowExecutionAlreadyCompletedError" => Code::NotFound,
        "DomainNotActiveError" | "ClientVersionNotSupportedError" | "FeatureNotEnabledError" => Code::FailedPrecondition,
        "WorkflowExecutionAlreadyStartedError"
        | "DomainAlreadyExistsError"
        | "CancellationAlreadyRequestedError"
        | "EventAlreadyStartedError" => Code::AlreadyExists,
        "ServiceBusyError" | "LimitExceededError" => Code::ResourceExhausted,
        "ShardOwnershipLostError" | "RetryTaskV2Error" => Code::Aborted,
        "AccessDeniedError" => Code::PermissionDenied,
        "InternalDataInconsistencyError" => Code::DataLoss,
        "RemoteSyncMatchedError" | "StickyWorkerUnavailableError" => Code::Unavailable,
        _ => Code::Internal,
    }
}

#[derive(Clone, PartialEq, Message)]
struct AnyMsg {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<AnyMsg>,
}

#[derive(Clone, PartialEq, Message)]
struct WorkflowStartedDetail {
    #[prost(string, tag = "1")]
    start_request_id: String,
    #[prost(string, tag = "2")]
    run_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct EntityNotExistsDetail {
    #[prost(string, tag = "1")]
    current_cluster: String,
    #[prost(string, tag = "2")]
    active_cluster: String,
    #[prost(string, repeated, tag = "3")]
    active_clusters: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct DomainNotActiveDetail {
    #[prost(string, tag = "1")]
    domain: String,
    #[prost(string, tag = "2")]
    current_cluster: String,
    #[prost(string, tag = "3")]
    active_cluster: String,
    #[prost(string, repeated, tag = "4")]
    active_clusters: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ClientVersionDetail {
    #[prost(string, tag = "1")]
    feature_version: String,
    #[prost(string, tag = "2")]
    client_impl: String,
    #[prost(string, tag = "3")]
    supported_versions: String,
}

#[derive(Clone, PartialEq, Message)]
struct ReasonDetail {
    #[prost(string, tag = "1")]
    reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct EmptyDetail {}

/// Typed `uber.cadence.api.v1.*` detail (name, encoded bytes) like Go `FromError`.
fn grpc_detail(e: &HistoryError) -> Option<(&'static str, Vec<u8>)> {
    let HistoryError::Cadence(c) = e else {
        return None;
    };
    Some(match &**c {
        CadenceError::EntityNotExists(x) => (
            "EntityNotExistsError",
            EntityNotExistsDetail {
                current_cluster: x.current_cluster.clone(),
                active_cluster: x.active_cluster.clone(),
                active_clusters: x.active_clusters.clone(),
            }
            .encode_to_vec(),
        ),
        CadenceError::WorkflowAlreadyCompleted(_) => ("WorkflowExecutionAlreadyCompletedError", EmptyDetail {}.encode_to_vec()),
        CadenceError::DomainNotActive(x) => (
            "DomainNotActiveError",
            DomainNotActiveDetail {
                domain: x.domain_name.clone(),
                current_cluster: x.current_cluster.clone(),
                active_cluster: x.active_cluster.clone(),
                active_clusters: x.active_clusters.clone(),
            }
            .encode_to_vec(),
        ),
        CadenceError::WorkflowAlreadyStarted(x) => (
            "WorkflowExecutionAlreadyStartedError",
            WorkflowStartedDetail { start_request_id: x.start_request_id.clone(), run_id: x.run_id.clone() }.encode_to_vec(),
        ),
        CadenceError::ClientVersionNotSupported(x) => (
            "ClientVersionNotSupportedError",
            ClientVersionDetail {
                feature_version: x.feature_version.clone(),
                client_impl: x.client_impl.clone(),
                supported_versions: x.supported_versions.clone(),
            }
            .encode_to_vec(),
        ),
        CadenceError::ServiceBusy(x) => (
            "ServiceBusyError",
            ReasonDetail { reason: x.reason.clone().unwrap_or_default() }.encode_to_vec(),
        ),
        CadenceError::LimitExceeded(_) => ("LimitExceededError", EmptyDetail {}.encode_to_vec()),
        CadenceError::QueryFailed(_) => ("QueryFailedError", EmptyDetail {}.encode_to_vec()),
        CadenceError::CancellationAlreadyRequested(_) => ("CancellationAlreadyRequestedError", EmptyDetail {}.encode_to_vec()),
        CadenceError::DomainAlreadyExists(_) => ("DomainAlreadyExistsError", EmptyDetail {}.encode_to_vec()),
        _ => return None,
    })
}

pub fn grpc_status(e: &HistoryError) -> tonic::Status {
    let (name, msg) = thrift_exception(e);
    let code = grpc_code(name);
    match grpc_detail(e) {
        Some((dname, value)) => {
            let status = RpcStatus {
                code: code as i32,
                message: msg.clone(),
                details: vec![AnyMsg { type_url: format!("type.googleapis.com/uber.cadence.api.v1.{dname}"), value }],
            };
            tonic::Status::with_details(code, msg, status.encode_to_vec().into())
        }
        None => tonic::Status::new(code, msg),
    }
}

pub fn busy() -> HistoryError {
    cadence_api::errors::ServiceBusyError { message: "Too many outstanding requests to the cadence service".into(), reason: None }.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cad(e: impl Into<CadenceError>) -> HistoryError {
        HistoryError::Cadence(Box::new(e.into()))
    }

    #[test]
    fn codes_match_go_from_error() {
        assert_eq!(grpc_status(&cad(cadence_api::errors::BadRequestError::new("x"))).code(), Code::InvalidArgument);
        assert_eq!(grpc_status(&busy()).code(), Code::ResourceExhausted);
        assert_eq!(grpc_status(&cad(cadence_api::errors::EntityNotExistsError::new("x"))).code(), Code::NotFound);
    }

    #[test]
    fn service_busy_carries_typed_detail() {
        let st = grpc_status(&busy());
        let decoded = RpcStatus::decode(st.details()).unwrap();
        assert_eq!(decoded.code, Code::ResourceExhausted as i32);
        assert_eq!(decoded.details[0].type_url, "type.googleapis.com/uber.cadence.api.v1.ServiceBusyError");
    }

    #[test]
    fn other_errors_are_internal_without_detail() {
        let st = grpc_status(&HistoryError::StaleState);
        assert_eq!(st.code(), Code::Internal);
        assert!(st.details().is_empty());
    }
}
