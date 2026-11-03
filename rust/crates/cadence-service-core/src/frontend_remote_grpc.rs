//! gRPC transport for the remote Frontend client (Go `rpc.NewCrossDCOutbounds`
//! with `rpcTransport: grpc`, TLS from the cluster's `tls` block).
//!
//! The Rust Frontend forwards JSON-shaped API requests. This module converts
//! them to the Cadence proto API and back. Operations without a request
//! converter fail with a clear error instead of being served locally.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use cadence_api::errors::{
    BadRequestError, CadenceError, DomainNotActiveError, EntityNotExistsError, InternalServiceError,
};
use cadence_api::types as t;
use cadence_proto::api as p;
use cadence_proto::convert as cv;
use prost::Message;
use serde_json::Value;
use tonic::transport::Channel;

use crate::frontend_remote::AUTOFORWARDING_CLUSTER_HEADER;

/// google.rpc.Status carried in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// Operations this client can forward over gRPC.
pub const GRPC_FORWARDED_OPERATIONS: &[&str] = &[
    "StartWorkflowExecution",
    "SignalWorkflowExecution",
    "TerminateWorkflowExecution",
    "RequestCancelWorkflowExecution",
];

fn bad(error: serde_json::Error) -> CadenceError {
    CadenceError::BadRequest(BadRequestError::new(error.to_string()))
}

fn seconds(v: Option<i32>) -> Option<prost_types::Duration> {
    v.map(|s| cv::duration(i64::from(s)))
}

/// Go `proto.FromStartWorkflowExecutionRequest`.
pub fn start_request_out(v: t::StartWorkflowExecutionRequest) -> p::StartWorkflowExecutionRequest {
    p::StartWorkflowExecutionRequest {
        domain: v.domain,
        workflow_id: v.workflow_id,
        workflow_type: v.workflow_type.map(|x| p::WorkflowType { name: x.name }),
        task_list: v.task_list.map(cv::task_list_out),
        input: cv::payload(v.input),
        execution_start_to_close_timeout: seconds(v.execution_start_to_close_timeout_seconds),
        task_start_to_close_timeout: seconds(v.task_start_to_close_timeout_seconds),
        identity: v.identity,
        request_id: v.request_id,
        workflow_id_reuse_policy: v.workflow_id_reuse_policy.map_or(0, |x| x as i32 + 1),
        retry_policy: v.retry_policy.map(cv::retry_out),
        cron_schedule: v.cron_schedule,
        memo: v.memo.map(cv::memo),
        search_attributes: v.search_attributes.map(cv::search_attributes),
        header: v.header.map(cv::header),
        delay_start: seconds(v.delay_start_seconds),
        jitter_start: seconds(v.jitter_start_seconds),
        first_run_at: v.first_run_at_timestamp.map(cv::timestamp),
        cron_overlap_policy: v.cron_overlap_policy.map_or(0, |x| x as i32 + 1),
        active_cluster_selection_policy: None,
    }
}

pub fn signal_request_out(v: t::SignalWorkflowExecutionRequest) -> p::SignalWorkflowExecutionRequest {
    p::SignalWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(cv::execution_out),
        identity: v.identity,
        request_id: v.request_id,
        signal_name: v.signal_name,
        signal_input: cv::payload(v.input),
        control: v.control,
    }
}

pub fn terminate_request_out(
    v: t::TerminateWorkflowExecutionRequest,
) -> p::TerminateWorkflowExecutionRequest {
    p::TerminateWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(cv::execution_out),
        reason: v.reason,
        details: cv::payload(v.details),
        identity: v.identity,
        first_execution_run_id: v.first_execution_run_id,
    }
}

pub fn cancel_request_out(
    v: cadence_api::service_types::RequestCancelWorkflowExecutionRequest,
) -> p::RequestCancelWorkflowExecutionRequest {
    p::RequestCancelWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(cv::execution_out),
        identity: v.identity,
        request_id: v.request_id,
        cause: v.cause,
        first_execution_run_id: v.first_execution_run_id,
    }
}

/// Go `proto.ToError`: typed details first, then the status code.
pub fn error_from_status(status: &tonic::Status) -> CadenceError {
    if let Ok(rpc) = RpcStatus::decode(status.details()) {
        for any in rpc.details {
            if any.type_url.ends_with(".DomainNotActiveError") {
                if let Ok(d) = p::DomainNotActiveError::decode(any.value.as_slice()) {
                    return CadenceError::DomainNotActive(DomainNotActiveError {
                        message: status.message().to_owned(),
                        domain_name: d.domain,
                        current_cluster: d.current_cluster,
                        active_cluster: d.active_cluster,
                        active_clusters: d.active_clusters,
                    });
                }
            }
        }
    }
    let message = status.message().to_owned();
    match status.code() {
        tonic::Code::NotFound => CadenceError::EntityNotExists(EntityNotExistsError {
            message,
            ..Default::default()
        }),
        tonic::Code::InvalidArgument => CadenceError::BadRequest(BadRequestError::new(message)),
        _ => CadenceError::Internal(InternalServiceError::new(message)),
    }
}

fn decorate<M>(
    message: M,
    service: &str,
    caller: &str,
    forwarding_cluster: &str,
    timeout: Duration,
) -> tonic::Request<M> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(timeout);
    for (key, value) in [
        ("rpc-service", service),
        ("rpc-caller", caller),
        ("rpc-encoding", "proto"),
        (AUTOFORWARDING_CLUSTER_HEADER, forwarding_cluster),
    ] {
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(value) {
            request.metadata_mut().insert(key, value);
        }
    }
    request
}

/// Forward one WorkflowAPI call over gRPC and return the JSON response.
pub async fn call(
    channel: Channel,
    service: &str,
    caller: &str,
    forwarding_cluster: &str,
    operation: &str,
    request: Value,
    timeout: Duration,
) -> Result<Value, CadenceError> {
    let mut client = p::workflow_api_client::WorkflowApiClient::new(channel);
    match operation {
        "StartWorkflowExecution" => {
            let typed: t::StartWorkflowExecutionRequest =
                serde_json::from_value(request).map_err(bad)?;
            let response = client
                .start_workflow_execution(decorate(start_request_out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner();
            serde_json::to_value(t::StartWorkflowExecutionResponse {
                run_id: response.run_id,
            })
            .map_err(bad)
        }
        "SignalWorkflowExecution" => {
            let typed: t::SignalWorkflowExecutionRequest =
                serde_json::from_value(request).map_err(bad)?;
            client
                .signal_workflow_execution(decorate(signal_request_out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?;
            Ok(Value::Null)
        }
        "TerminateWorkflowExecution" => {
            let typed: t::TerminateWorkflowExecutionRequest =
                serde_json::from_value(request).map_err(bad)?;
            client
                .terminate_workflow_execution(decorate(terminate_request_out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?;
            Ok(Value::Null)
        }
        "RequestCancelWorkflowExecution" => {
            let typed: cadence_api::service_types::RequestCancelWorkflowExecutionRequest =
                serde_json::from_value(request).map_err(bad)?;
            client
                .request_cancel_workflow_execution(decorate(cancel_request_out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?;
            Ok(Value::Null)
        }
        other => Err(CadenceError::Internal(InternalServiceError::new(format!(
            "operation {other} is not supported over the gRPC remote Frontend transport"
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_request_maps_to_proto() {
        let out = signal_request_out(t::SignalWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_execution: Some(t::WorkflowExecution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            }),
            signal_name: "s".into(),
            input: b"in".to_vec(),
            identity: "i".into(),
            request_id: "q".into(),
            control: b"c".to_vec(),
        });
        assert_eq!(out.signal_name, "s");
        assert_eq!(out.signal_input.unwrap().data, b"in");
        assert_eq!(out.workflow_execution.unwrap().run_id, "r");
        assert_eq!(out.control, b"c");
    }

    #[test]
    fn start_request_maps_durations_and_enums() {
        let out = start_request_out(t::StartWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_id: "w".into(),
            workflow_type: Some(t::WorkflowType { name: "T".into() }),
            execution_start_to_close_timeout_seconds: Some(60),
            workflow_id_reuse_policy: Some(cadence_api::enums::WorkflowIDReusePolicy::AllowDuplicate),
            ..Default::default()
        });
        assert_eq!(out.execution_start_to_close_timeout.unwrap().seconds, 60);
        assert_eq!(out.workflow_id_reuse_policy, 2);
        assert_eq!(out.workflow_type.unwrap().name, "T");
        assert!(out.task_start_to_close_timeout.is_none());
    }

    #[test]
    fn domain_not_active_detail_is_decoded() {
        let detail = p::DomainNotActiveError {
            domain: "d".into(),
            current_cluster: "a".into(),
            active_cluster: "b".into(),
            active_clusters: vec![],
        };
        let rpc = RpcStatus {
            code: tonic::Code::FailedPrecondition as i32,
            message: "m".into(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/uber.cadence.api.v1.DomainNotActiveError".into(),
                value: detail.encode_to_vec(),
            }],
        };
        let status = tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "m",
            rpc.encode_to_vec().into(),
        );
        match error_from_status(&status) {
            CadenceError::DomainNotActive(e) => {
                assert_eq!((e.active_cluster.as_str(), e.domain_name.as_str()), ("b", "d"))
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn status_codes_map_to_cadence_errors() {
        assert!(matches!(
            error_from_status(&tonic::Status::not_found("x")),
            CadenceError::EntityNotExists(_)
        ));
        assert!(matches!(
            error_from_status(&tonic::Status::invalid_argument("x")),
            CadenceError::BadRequest(_)
        ));
        assert!(matches!(
            error_from_status(&tonic::Status::unavailable("x")),
            CadenceError::Internal(_)
        ));
    }
}
