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

pub use cv::client::{
    cancel_request_out, signal_request_out, start_request_out, terminate_request_out,
};

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

/// Operation names forwarded over gRPC (all clusterRedirection-wrapped APIs).
fn internal(status: tonic::Status) -> CadenceError {
    CadenceError::Internal(InternalServiceError::new(status.message().to_owned()))
}

/// Forward one call over gRPC and return the JSON response.
pub async fn call(
    channel: Channel,
    service: &str,
    caller: &str,
    forwarding_cluster: &str,
    operation: &str,
    request: Value,
    timeout: Duration,
) -> Result<Value, CadenceError> {
    let mut workflow = p::workflow_api_client::WorkflowApiClient::new(channel.clone());
    let mut visibility = p::visibility_api_client::VisibilityApiClient::new(channel.clone());
    let mut worker = p::worker_api_client::WorkerApiClient::new(channel.clone());
    let mut schedule = p::schedule_api_client::ScheduleApiClient::new(channel);
    let _ = &mut schedule;
    // `forward!(client, rpc, RequestType, request_fn, response_fn)`: JSON -> typed
    // request -> proto, then proto response -> typed -> JSON. `void` ignores the response.
    macro_rules! forward {
        ($client:ident, $rpc:ident, $ty:ty, fallible $out:expr, $inn:expr) => {{
            let typed: $ty = serde_json::from_value(request).map_err(bad)?;
            let proto_request = $out(typed).map_err(internal)?;
            let response = $client
                .$rpc(decorate(proto_request, service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner();
            let converted = $inn(response).map_err(internal)?;
            serde_json::to_value(converted).map_err(bad)
        }};
        ($client:ident, $rpc:ident, $ty:ty, $out:expr, void) => {{
            let typed: $ty = serde_json::from_value(request).map_err(bad)?;
            $client
                .$rpc(decorate($out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?;
            Ok(Value::Null)
        }};
        ($client:ident, $rpc:ident, $ty:ty, $out:expr, $inn:expr) => {{
            let typed: $ty = serde_json::from_value(request).map_err(bad)?;
            let response = $client
                .$rpc(decorate($out(typed), service, caller, forwarding_cluster, timeout))
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner();
            let converted = $inn(response).map_err(internal)?;
            serde_json::to_value(converted).map_err(bad)
        }};
    }
    use cadence_api::service_types as st;
    use cadence_api::tasks as tk;
    use cv::client as c;
    match operation {
        "StartWorkflowExecution" => forward!(
            workflow, start_workflow_execution, t::StartWorkflowExecutionRequest,
            start_request_out,
            |r: p::StartWorkflowExecutionResponse| Ok::<_, tonic::Status>(t::StartWorkflowExecutionResponse { run_id: r.run_id })
        ),
        "StartWorkflowExecutionAsync" => forward!(
            workflow, start_workflow_execution_async, t::StartWorkflowExecutionAsyncRequest,
            c::start_async_request_out,
            |_r: p::StartWorkflowExecutionAsyncResponse| Ok::<_, tonic::Status>(t::StartWorkflowExecutionAsyncResponse::default())
        ),
        "SignalWorkflowExecution" => forward!(
            workflow, signal_workflow_execution, t::SignalWorkflowExecutionRequest,
            signal_request_out, void
        ),
        "SignalWithStartWorkflowExecution" => forward!(
            workflow, signal_with_start_workflow_execution, st::SignalWithStartWorkflowExecutionRequest,
            c::signal_with_start_request_out,
            |r: p::SignalWithStartWorkflowExecutionResponse| Ok::<_, tonic::Status>(t::StartWorkflowExecutionResponse { run_id: r.run_id })
        ),
        "SignalWithStartWorkflowExecutionAsync" => forward!(
            workflow, signal_with_start_workflow_execution_async, st::SignalWithStartWorkflowExecutionAsyncRequest,
            c::signal_with_start_async_request_out,
            |_r: p::SignalWithStartWorkflowExecutionAsyncResponse| Ok::<_, tonic::Status>(st::SignalWithStartWorkflowExecutionAsyncResponse::default())
        ),
        "TerminateWorkflowExecution" => forward!(
            workflow, terminate_workflow_execution, t::TerminateWorkflowExecutionRequest,
            terminate_request_out, void
        ),
        "RequestCancelWorkflowExecution" => forward!(
            workflow, request_cancel_workflow_execution, st::RequestCancelWorkflowExecutionRequest,
            cancel_request_out, void
        ),
        "ResetWorkflowExecution" => forward!(
            workflow, reset_workflow_execution, t::ResetWorkflowExecutionRequest,
            c::reset_request_out,
            |r: p::ResetWorkflowExecutionResponse| Ok::<_, tonic::Status>(t::ResetWorkflowExecutionResponse { run_id: r.run_id })
        ),
        "RestartWorkflowExecution" => forward!(
            workflow, restart_workflow_execution, t::RestartWorkflowExecutionRequest,
            c::restart_request_out,
            |r: p::RestartWorkflowExecutionResponse| Ok::<_, tonic::Status>(t::RestartWorkflowExecutionResponse { run_id: r.run_id })
        ),
        "DiagnoseWorkflowExecution" => forward!(
            workflow, diagnose_workflow_execution, t::DiagnoseWorkflowExecutionRequest,
            c::diagnose_request_out,
            |r: p::DiagnoseWorkflowExecutionResponse| Ok::<_, tonic::Status>(c::diagnose_response_in(r))
        ),
        "DescribeWorkflowExecution" => forward!(
            workflow, describe_workflow_execution, st::DescribeWorkflowExecutionRequest,
            c::describe_request_out, c::describe_response_in
        ),
        "QueryWorkflow" => forward!(
            workflow, query_workflow, tk::QueryWorkflowRequest,
            c::query_request_out, c::query_response_in
        ),
        "ResetStickyTaskList" => forward!(
            worker, reset_sticky_task_list, st::ResetStickyTaskListRequest,
            c::reset_sticky_request_out,
            |_r: p::ResetStickyTaskListResponse| Ok::<_, tonic::Status>(st::ResetStickyTaskListResponse::default())
        ),
        "RefreshWorkflowTasks" => forward!(
            workflow, refresh_workflow_tasks, t::RefreshWorkflowTasksRequest,
            c::refresh_request_out, void
        ),
        "GetWorkflowExecutionHistory" => forward!(
            workflow, get_workflow_execution_history, st::GetWorkflowExecutionHistoryRequest,
            c::history_request_out, c::history_response_in
        ),
        "ListOpenWorkflowExecutions" => forward!(
            visibility, list_open_workflow_executions, st::ListOpenWorkflowExecutionsRequest,
            c::list_open_request_out, c::list_open_response_in
        ),
        "ListClosedWorkflowExecutions" => forward!(
            visibility, list_closed_workflow_executions, st::ListClosedWorkflowExecutionsRequest,
            c::list_closed_request_out, c::list_closed_response_in
        ),
        "ListWorkflowExecutions" => forward!(
            visibility, list_workflow_executions, st::ListWorkflowExecutionsRequest,
            c::list_request_out, c::list_response_in
        ),
        "ScanWorkflowExecutions" => forward!(
            visibility, scan_workflow_executions, st::ListWorkflowExecutionsRequest,
            c::scan_request_out, c::scan_response_in
        ),
        "ListArchivedWorkflowExecutions" => forward!(
            visibility, list_archived_workflow_executions, st::ListArchivedWorkflowExecutionsRequest,
            c::list_archived_request_out, c::list_archived_response_in
        ),
        "CountWorkflowExecutions" => forward!(
            visibility, count_workflow_executions, st::CountWorkflowExecutionsRequest,
            c::count_request_out,
            |r: p::CountWorkflowExecutionsResponse| Ok::<_, tonic::Status>(st::CountWorkflowExecutionsResponse { count: r.count })
        ),
        "DescribeTaskList" => forward!(
            workflow, describe_task_list, tk::DescribeTaskListRequest,
            cv::describe_task_list_request_out, cv::describe_task_list_in
        ),
        "ListTaskListPartitions" => forward!(
            workflow, list_task_list_partitions, tk::ListTaskListPartitionsRequest,
            c::partitions_request_out, c::partitions_response_in
        ),
        "PollForDecisionTask" => forward!(
            worker, poll_for_decision_task, tk::PollForDecisionTaskRequest,
            cv::poll_for_decision_task_request_out, c::poll_decision_response_in
        ),
        "PollForActivityTask" => forward!(
            worker, poll_for_activity_task, tk::PollForActivityTaskRequest,
            cv::poll_for_activity_task_request_out, c::poll_activity_response_in
        ),
        "RespondDecisionTaskCompleted" => forward!(
            worker, respond_decision_task_completed, tk::RespondDecisionTaskCompletedRequest,
            fallible c::respond_decision_completed_request_out, c::respond_decision_completed_response_in
        ),
        "RespondDecisionTaskFailed" => forward!(
            worker, respond_decision_task_failed, tk::RespondDecisionTaskFailedRequest,
            c::respond_decision_failed_request_out, void
        ),
        "RespondActivityTaskCompleted" => forward!(
            worker, respond_activity_task_completed, tk::RespondActivityTaskCompletedRequest,
            c::respond_activity_completed_request_out, void
        ),
        "RespondActivityTaskCompletedByID" => forward!(
            worker, respond_activity_task_completed_by_id, tk::RespondActivityTaskCompletedByIDRequest,
            c::respond_activity_completed_by_id_request_out, void
        ),
        "RespondActivityTaskFailed" => forward!(
            worker, respond_activity_task_failed, tk::RespondActivityTaskFailedRequest,
            c::respond_activity_failed_request_out, void
        ),
        "RespondActivityTaskFailedByID" => forward!(
            worker, respond_activity_task_failed_by_id, tk::RespondActivityTaskFailedByIDRequest,
            c::respond_activity_failed_by_id_request_out, void
        ),
        "RespondActivityTaskCanceled" => forward!(
            worker, respond_activity_task_canceled, tk::RespondActivityTaskCanceledRequest,
            c::respond_activity_canceled_request_out, void
        ),
        "RespondActivityTaskCanceledByID" => forward!(
            worker, respond_activity_task_canceled_by_id, tk::RespondActivityTaskCanceledByIDRequest,
            c::respond_activity_canceled_by_id_request_out, void
        ),
        "RespondQueryTaskCompleted" => forward!(
            worker, respond_query_task_completed, tk::RespondQueryTaskCompletedRequest,
            cv::query_completed_out, void
        ),
        "RecordActivityTaskHeartbeat" => forward!(
            worker, record_activity_task_heartbeat, tk::RecordActivityTaskHeartbeatRequest,
            c::heartbeat_request_out,
            |r: p::RecordActivityTaskHeartbeatResponse| Ok::<_, tonic::Status>(tk::RecordActivityTaskHeartbeatResponse { cancel_requested: r.cancel_requested })
        ),
        "RecordActivityTaskHeartbeatByID" => forward!(
            worker, record_activity_task_heartbeat_by_id, tk::RecordActivityTaskHeartbeatByIDRequest,
            c::heartbeat_by_id_request_out,
            |r: p::RecordActivityTaskHeartbeatByIdResponse| Ok::<_, tonic::Status>(tk::RecordActivityTaskHeartbeatResponse { cancel_requested: r.cancel_requested })
        ),
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
