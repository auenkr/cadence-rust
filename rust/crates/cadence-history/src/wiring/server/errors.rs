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
        HistoryError::PersistenceShardOwnershipLost { message: m, .. } => ("ShardOwnershipLostError", m.clone()),
        HistoryError::CurrentBranchChanged { message, .. } => ("CurrentBranchChangedError", message.clone()),
        // Go handler.convertError: TransactionSizeLimitError -> BadRequestError.
        HistoryError::TransactionSizeLimit(m) => ("BadRequestError", m.clone()),
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
        "ShardOwnershipLostError" | "RetryTaskV2Error" | "CurrentBranchChangedError" => Code::Aborted,
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

#[derive(Clone, PartialEq, Message)]
struct ShardOwnershipLostDetail {
    #[prost(string, tag = "1")]
    owner: String,
}

#[derive(Clone, PartialEq, Message)]
struct CurrentBranchChangedDetail {
    #[prost(bytes = "vec", tag = "1")]
    current_branch_token: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct WorkflowExecutionDetail {
    #[prost(string, tag = "1")]
    workflow_id: String,
    #[prost(string, tag = "2")]
    run_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct VersionHistoryItemDetail {
    #[prost(int64, tag = "1")]
    event_id: i64,
    #[prost(int64, tag = "2")]
    version: i64,
}

#[derive(Clone, PartialEq, Message)]
struct RetryTaskV2Detail {
    #[prost(string, tag = "1")]
    domain_id: String,
    #[prost(message, optional, tag = "2")]
    workflow_execution: Option<WorkflowExecutionDetail>,
    #[prost(message, optional, tag = "3")]
    start_event: Option<VersionHistoryItemDetail>,
    #[prost(message, optional, tag = "4")]
    end_event: Option<VersionHistoryItemDetail>,
}

/// Go `FromEventIDVersionPair`: nil unless both values are present.
fn version_pair(id: Option<i64>, version: Option<i64>) -> Option<VersionHistoryItemDetail> {
    match (id, version) {
        (Some(event_id), Some(version)) => Some(VersionHistoryItemDetail { event_id, version }),
        _ => None,
    }
}

/// Protobuf package of a detail type: shared.v1 for the internal-service errors, api.v1 otherwise.
fn detail_package(name: &str) -> &'static str {
    match name {
        "ShardOwnershipLostError" | "RetryTaskV2Error" | "CurrentBranchChangedError" | "EventAlreadyStartedError" => {
            "uber.cadence.shared.v1"
        }
        _ => "uber.cadence.api.v1",
    }
}

/// Typed `uber.cadence.api.v1.*` detail (name, encoded bytes) like Go `FromError`.
fn grpc_detail(e: &HistoryError) -> Option<(&'static str, Vec<u8>)> {
    match e {
        HistoryError::EventAlreadyStarted(_) => return Some(("EventAlreadyStartedError", EmptyDetail {}.encode_to_vec())),
        HistoryError::PersistenceShardOwnershipLost { .. } => {
            return Some(("ShardOwnershipLostError", ShardOwnershipLostDetail::default().encode_to_vec()))
        }
        HistoryError::CurrentBranchChanged { current_branch_token, .. } => {
            return Some((
                "CurrentBranchChangedError",
                CurrentBranchChangedDetail { current_branch_token: current_branch_token.clone() }.encode_to_vec(),
            ))
        }
        _ => {}
    }
    let HistoryError::Cadence(c) = e else {
        return None;
    };
    Some(match &**c {
        CadenceError::ShardOwnershipLost(x) => {
            ("ShardOwnershipLostError", ShardOwnershipLostDetail { owner: x.owner.clone() }.encode_to_vec())
        }
        CadenceError::RetryTaskV2(x) => (
            "RetryTaskV2Error",
            RetryTaskV2Detail {
                domain_id: x.domain_id.clone(),
                workflow_execution: Some(WorkflowExecutionDetail { workflow_id: x.workflow_id.clone(), run_id: x.run_id.clone() }),
                start_event: version_pair(x.start_event_id, x.start_event_version),
                end_event: version_pair(x.end_event_id, x.end_event_version),
            }
            .encode_to_vec(),
        ),
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
                details: vec![AnyMsg { type_url: format!("type.googleapis.com/{}.{dname}", detail_package(dname)), value }],
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
    fn transaction_size_limit_is_bad_request_like_go_convert_error() {
        let e = HistoryError::TransactionSizeLimit("transaction size of 5 bytes exceeds limit of 4 bytes".into());
        assert_eq!(thrift_exception(&e), ("BadRequestError", "transaction size of 5 bytes exceeds limit of 4 bytes".to_string()));
        assert_eq!(grpc_status(&e).code(), Code::InvalidArgument);
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

#[cfg(test)]
mod typed_detail_tests {
    use super::*;

    fn first_detail(st: &tonic::Status) -> AnyMsg {
        RpcStatus::decode(st.details()).unwrap().details.remove(0)
    }

    #[test]
    fn shard_ownership_lost_carries_owner_detail() {
        let e = HistoryError::Cadence(Box::new(cadence_api::errors::ShardOwnershipLostError::with_owner("m", "host:7934").into()));
        let st = grpc_status(&e);
        assert_eq!(st.code(), Code::Aborted);
        let d = first_detail(&st);
        assert_eq!(d.type_url, "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError");
        assert_eq!(ShardOwnershipLostDetail::decode(d.value.as_slice()).unwrap().owner, "host:7934");
    }

    #[test]
    fn persistence_shard_lost_is_aborted_not_internal() {
        let st = grpc_status(&HistoryError::PersistenceShardOwnershipLost { message: "lost".into(), shard_id: None });
        assert_eq!(st.code(), Code::Aborted);
        assert!(first_detail(&st).type_url.ends_with("shared.v1.ShardOwnershipLostError"));
    }

    #[test]
    fn retry_task_v2_current_branch_and_event_already_started_have_details() {
        let retry = cadence_api::errors::RetryTaskV2Error {
            message: "r".into(),
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r1".into(),
            start_event_id: Some(3),
            start_event_version: Some(4),
            ..Default::default()
        };
        let d = first_detail(&grpc_status(&HistoryError::Cadence(Box::new(retry.into()))));
        let msg = RetryTaskV2Detail::decode(d.value.as_slice()).unwrap();
        assert_eq!(msg.domain_id, "d");
        assert_eq!(msg.start_event, Some(VersionHistoryItemDetail { event_id: 3, version: 4 }));
        assert_eq!(msg.end_event, None);

        let st = grpc_status(&HistoryError::CurrentBranchChanged { message: "c".into(), current_branch_token: vec![1, 2] });
        assert_eq!(st.code(), Code::Aborted);
        let d = first_detail(&st);
        assert_eq!(CurrentBranchChangedDetail::decode(d.value.as_slice()).unwrap().current_branch_token, vec![1, 2]);

        let st = grpc_status(&HistoryError::EventAlreadyStarted(cadence_api::errors::EventAlreadyStartedError::new("e")));
        assert!(first_detail(&st).type_url.ends_with("shared.v1.EventAlreadyStartedError"));
    }
}
