//! Outbound gRPC transport for the Frontend's History and Matching calls.
//!
//! Go selects the transport in `common/rpc/params.go` with the dynamic config
//! key `system.enableGRPCOutbound` (default `true`): when on, the client
//! factory builds gRPC clients (`client/clientfactory.go`) and resolves peers
//! on the gRPC port; when off it uses the Thrift/TChannel clients.
//!
//! `ProductionBackend` builds Thrift envelopes for every outbound call. This
//! module converts those envelopes to the History/Matching protobuf messages
//! (reusing `cadence_proto::convert`) and converts replies back into the
//! Thrift `Struct` layout the callers already decode. Methods without a
//! mapping here return `None` and keep using TChannel.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cadence_api::errors::{CadenceError, InternalServiceError};
use cadence_api::{service_types as st, tasks as tk, types as ty};
use cadence_proto::convert::{self, client as cv};
use cadence_proto::{api, history as h, matching as m, shared};
use cadence_thrift::value::{Fields, FromThrift, Struct, ToThrift, Value};
use tonic::transport::Channel;
use tonic::Request;

/// Which wire protocol an outbound call uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboundTransport {
    Grpc,
    Tchannel,
}

/// Go `dynamicproperties.EnableGRPCOutbound` default.
pub const ENABLE_GRPC_OUTBOUND_DEFAULT: bool = true;
/// Go key name of `EnableGRPCOutbound`.
pub const ENABLE_GRPC_OUTBOUND_KEY: &str = "system.enableGRPCOutbound";

/// `value` is the configured `system.enableGRPCOutbound` (None = unset).
pub fn select_transport(value: Option<bool>) -> OutboundTransport {
    if value.unwrap_or(ENABLE_GRPC_OUTBOUND_DEFAULT) {
        OutboundTransport::Grpc
    } else {
        OutboundTransport::Tchannel
    }
}

/// Replace the port of a `host:port` TChannel address with `grpc_port`
/// (Go resolves peers with `membership.PortGRPC` instead of `PortTchannel`).
pub fn grpc_address(tchannel_addr: &str, grpc_port: u16) -> String {
    match tchannel_addr.rsplit_once(':') {
        Some((host, _)) => format!("{host}:{grpc_port}"),
        None => format!("{tchannel_addr}:{grpc_port}"),
    }
}

/// True for the methods this module maps to gRPC.
pub fn supported(service: &str, method: &str) -> bool {
    match service {
        "cadence-history" => matches!(
            method,
            "StartWorkflowExecution"
                | "SignalWithStartWorkflowExecution"
                | "SignalWorkflowExecution"
                | "TerminateWorkflowExecution"
                | "RequestCancelWorkflowExecution"
                | "ResetWorkflowExecution"
                | "QueryWorkflow"
                | "ResetStickyTaskList"
                | "RefreshWorkflowTasks"
                | "RecordActivityTaskHeartbeat"
                | "RespondActivityTaskCompleted"
                | "RespondActivityTaskFailed"
                | "RespondActivityTaskCanceled"
                | "RespondDecisionTaskFailed"
                | "PollMutableState"
                | "RespondDecisionTaskCompleted"
                | "DescribeWorkflowExecution"
                | "RatelimitUpdate"
        ),
        "cadence-matching" => matches!(
            method,
            "CancelOutstandingPoll"
                | "RespondQueryTaskCompleted"
                | "DescribeTaskList"
                | "ListTaskListPartitions"
                | "GetTaskListsByDomain"
                | "PollForDecisionTask"
                | "PollForActivityTask"
        ),
        _ => false,
    }
}

/// Shared gRPC outbound state: dynamic selector, ports and channel cache.
#[derive(Clone)]
pub struct GrpcOutbound {
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    history_port: u16,
    matching_port: u16,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl std::fmt::Debug for GrpcOutbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcOutbound")
            .field("history_port", &self.history_port)
            .field("matching_port", &self.matching_port)
            .finish()
    }
}

fn internal(message: impl Into<String>) -> CadenceError {
    CadenceError::Internal(InternalServiceError::new(message.into()))
}

fn thrift_err<E: std::fmt::Debug>(what: &str, e: E) -> CadenceError {
    internal(format!("invalid {what}: {e:?}"))
}

impl GrpcOutbound {
    pub fn new(
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
        history_port: u16,
        matching_port: u16,
    ) -> Self {
        Self {
            enabled,
            history_port,
            matching_port,
            channels: Default::default(),
        }
    }

    pub fn transport(&self) -> OutboundTransport {
        select_transport(Some((self.enabled)()))
    }

    fn channel(&self, addr: &str) -> Result<Channel, CadenceError> {
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| internal("channel cache"))?;
        if let Some(channel) = channels.get(addr) {
            return Ok(channel.clone());
        }
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| internal(format!("invalid gRPC address {addr}")))?;
        let port: u16 = port
            .parse()
            .map_err(|_| internal(format!("invalid gRPC port in {addr}")))?;
        let channel =
            crate::history_client::connect(host, port).map_err(|e| internal(e.to_string()))?;
        channels.insert(addr.to_owned(), channel.clone());
        Ok(channel)
    }

    fn request<T>(
        service: &'static str,
        payload: T,
        timeout: Duration,
        forwarded: &[(String, String)],
    ) -> Request<T> {
        let mut request = Request::new(payload);
        let md = request.metadata_mut();
        md.insert(
            "rpc-service",
            tonic::metadata::MetadataValue::from_static(service),
        );
        md.insert(
            "rpc-caller",
            tonic::metadata::MetadataValue::from_static("cadence-frontend"),
        );
        md.insert(
            "rpc-encoding",
            tonic::metadata::MetadataValue::from_static("proto"),
        );
        for (k, v) in forwarded {
            // Transport routing headers belong to this hop, never the inbound one
            // (the inbound `rpc-service` is cadence-frontend).
            let lk = k.to_ascii_lowercase();
            if matches!(
                lk.as_str(),
                "rpc-service"
                    | "rpc-caller"
                    | "rpc-encoding"
                    | "rpc-procedure"
                    | "rpc-ttl"
                    | "rpc-shard-key"
                    | "rpc-routing-key"
                    | "rpc-routing-delegate"
                    | "rpc-caller-procedure"
            ) {
                continue;
            }
            if let (Ok(key), Ok(val)) = (
                tonic::metadata::MetadataKey::from_bytes(k.to_ascii_lowercase().as_bytes()),
                tonic::metadata::MetadataValue::try_from(v.as_str()),
            ) {
                md.insert(key, val);
            }
        }
        cadence_service_core::header_forwarding::apply_caller_type(md);
        request.set_timeout(timeout);
        request
    }

    /// Returns `None` when `method` has no gRPC mapping (caller uses TChannel).
    /// `addr` is the TChannel `host:port` of the owner.
    pub async fn try_call(
        &self,
        addr: &str,
        service: &str,
        method: &str,
        envelope: &Struct,
        timeout: Duration,
        header_rules: &[cadence_service_core::header_forwarding::HeaderRule],
    ) -> Option<Result<Struct, CadenceError>> {
        if self.transport() != OutboundTransport::Grpc || !supported(service, method) {
            return None;
        }
        let forwarded =
            cadence_service_core::header_forwarding::forward_for_current_call(header_rules, &[]);
        let timeout = cadence_service_core::header_forwarding::clamp_to_inbound_deadline(timeout);
        let started = std::time::Instant::now();
        let (target, result) = match service {
            "cadence-history" => {
                let target = grpc_address(addr, self.history_port);
                let r = self
                    .history(&target, method, envelope, timeout, &forwarded)
                    .await;
                (target, r)
            }
            _ => {
                let target = grpc_address(addr, self.matching_port);
                let r = self
                    .matching(&target, method, envelope, timeout, &forwarded)
                    .await;
                (target, r)
            }
        };
        if let Err(e) = &result {
            eprintln!(
                "WARN gRPC outbound {service}/{method} to {target} failed after {:?} (timeout {timeout:?}): {e:?}",
                started.elapsed()
            );
        }
        Some(result)
    }

    async fn history(
        &self,
        target: &str,
        method: &str,
        env: &Struct,
        timeout: Duration,
        fwd: &[(String, String)],
    ) -> Result<Struct, CadenceError> {
        let mut client = crate::history_client::generated(self.channel(target)?, 64 << 20);
        let domain_id = if method == "RatelimitUpdate" {
            String::new()
        } else {
            domain_of(env)?
        };
        macro_rules! r {
            ($p:expr) => {
                Self::request("cadence-history", $p, timeout, fwd)
            };
        }
        let st_err = |s: tonic::Status| {
            cadence_service_core::frontend_remote_grpc::error_from_status_typed(&s)
        };
        match method {
            "StartWorkflowExecution" => {
                let msg = start_workflow_request(env, domain_id)?;
                let resp = client
                    .start_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(start_workflow_response(resp))
            }
            "SignalWithStartWorkflowExecution" => {
                let msg = signal_with_start_request(env, domain_id)?;
                let resp = client
                    .signal_with_start_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(signal_with_start_response(resp))
            }
            "SignalWorkflowExecution" => {
                let req: ty::SignalWorkflowExecutionRequest = inner(env, 20)?;
                let msg = h::SignalWorkflowExecutionRequest {
                    request: Some(cv::signal_request_out(req)),
                    domain_id,
                    ..Default::default()
                };
                client
                    .signal_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "TerminateWorkflowExecution" => {
                let req: ty::TerminateWorkflowExecutionRequest = inner(env, 20)?;
                let msg = h::TerminateWorkflowExecutionRequest {
                    request: Some(cv::terminate_request_out(req)),
                    domain_id,
                    ..Default::default()
                };
                client
                    .terminate_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RequestCancelWorkflowExecution" => {
                let req: st::RequestCancelWorkflowExecutionRequest = inner(env, 20)?;
                let msg = h::RequestCancelWorkflowExecutionRequest {
                    cancel_request: Some(cv::cancel_request_out(req)),
                    domain_id,
                    ..Default::default()
                };
                client
                    .request_cancel_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "ResetWorkflowExecution" => {
                let msg = reset_workflow_request(env, domain_id)?;
                let resp = client
                    .reset_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(reset_workflow_response(resp))
            }
            "QueryWorkflow" => {
                let msg = query_workflow_request(env, domain_id)?;
                let resp = client
                    .query_workflow(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                query_workflow_response(resp)
            }
            "ResetStickyTaskList" => {
                let execution: ty::WorkflowExecution = inner(env, 20)?;
                let msg = h::ResetStickyTaskListRequest {
                    request: Some(cadence_proto::api::ResetStickyTaskListRequest {
                        domain: String::new(),
                        workflow_execution: Some(convert::execution_out(execution)),
                    }),
                    domain_id,
                };
                client
                    .reset_sticky_task_list(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RefreshWorkflowTasks" => {
                let req: ty::RefreshWorkflowTasksRequest = inner(env, 20)?;
                let base = cv::refresh_request_out(req);
                let msg = h::RefreshWorkflowTasksRequest {
                    domain: base.domain,
                    workflow_execution: base.workflow_execution,
                    domain_id,
                };
                client
                    .refresh_workflow_tasks(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RecordActivityTaskHeartbeat" => {
                let msg = heartbeat_request(env, domain_id)?;
                let resp = client
                    .record_activity_task_heartbeat(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(heartbeat_response(resp))
            }
            "RespondActivityTaskCompleted" => {
                let req: tk::RespondActivityTaskCompletedRequest = inner(env, 20)?;
                let msg = h::RespondActivityTaskCompletedRequest {
                    request: Some(cv::respond_activity_completed_request_out(req)),
                    domain_id,
                };
                client
                    .respond_activity_task_completed(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RespondActivityTaskFailed" => {
                let req: tk::RespondActivityTaskFailedRequest = inner(env, 20)?;
                let msg = h::RespondActivityTaskFailedRequest {
                    request: Some(cv::respond_activity_failed_request_out(req)),
                    domain_id,
                };
                client
                    .respond_activity_task_failed(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RespondActivityTaskCanceled" => {
                let req: tk::RespondActivityTaskCanceledRequest = inner(env, 20)?;
                let msg = h::RespondActivityTaskCanceledRequest {
                    request: Some(cv::respond_activity_canceled_request_out(req)),
                    domain_id,
                };
                client
                    .respond_activity_task_canceled(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RespondDecisionTaskFailed" => {
                let req: tk::RespondDecisionTaskFailedRequest = inner(env, 20)?;
                let msg = h::RespondDecisionTaskFailedRequest {
                    request: Some(cv::respond_decision_failed_request_out(req)),
                    domain_id,
                };
                client
                    .respond_decision_task_failed(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "PollMutableState" => {
                let reply = client
                    .poll_mutable_state(r!(poll_mutable_state_request(env, domain_id)?))
                    .await
                    .map_err(st_err)?;
                poll_mutable_state_response(reply.into_inner()).map_err(st_err)
            }
            "RespondDecisionTaskCompleted" => {
                let reply = client
                    .respond_decision_task_completed(r!(decision_completed_request(
                        env, domain_id
                    )?))
                    .await
                    .map_err(st_err)?;
                decision_completed_response(reply.into_inner()).map_err(st_err)
            }
            "DescribeWorkflowExecution" => {
                let reply = client
                    .describe_workflow_execution(r!(describe_workflow_request(env, domain_id)?))
                    .await
                    .map_err(st_err)?;
                describe_workflow_response(reply.into_inner()).map_err(st_err)
            }
            "RatelimitUpdate" => {
                let reply = client
                    .ratelimit_update(r!(ratelimit_request(env)?))
                    .await
                    .map_err(st_err)?;
                Ok(ratelimit_response(reply.into_inner()))
            }
            other => Err(internal(format!("no gRPC mapping for History {other}"))),
        }
    }

    async fn matching(
        &self,
        target: &str,
        method: &str,
        env: &Struct,
        timeout: Duration,
        fwd: &[(String, String)],
    ) -> Result<Struct, CadenceError> {
        let mut client = crate::matching_client::generated(self.channel(target)?, 64 << 20);
        macro_rules! r {
            ($p:expr) => {
                Self::request("cadence-matching", $p, timeout, fwd)
            };
        }
        let st_err = |s: tonic::Status| {
            cadence_service_core::frontend_remote_grpc::error_from_status_typed(&s)
        };
        match method {
            "CancelOutstandingPoll" => {
                let task_list: Option<ty::TaskList> =
                    env.opt_struct(30).map_err(|e| thrift_err("task list", e))?;
                let msg = m::CancelOutstandingPollRequest {
                    domain_id: domain_of(env)?,
                    poller_id: env
                        .opt_string(40)
                        .map_err(|e| thrift_err("poller id", e))?
                        .unwrap_or_default(),
                    task_list_type: env
                        .opt_i32(20)
                        .map_err(|e| thrift_err("task list type", e))?
                        .map_or(0, |t| t + 1),
                    task_list: task_list.map(convert::task_list_out),
                };
                client
                    .cancel_outstanding_poll(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "RespondQueryTaskCompleted" => {
                let task_list: ty::TaskList = inner(env, 20)?;
                let completed: tk::RespondQueryTaskCompletedRequest = inner(env, 40)?;
                let msg = m::RespondQueryTaskCompletedRequest {
                    request: Some(convert::query_completed_out(completed)),
                    domain_id: domain_of(env)?,
                    task_list: Some(convert::task_list_out(task_list)),
                    task_id: env
                        .opt_string(30)
                        .map_err(|e| thrift_err("task id", e))?
                        .unwrap_or_default(),
                };
                client
                    .respond_query_task_completed(r!(msg))
                    .await
                    .map_err(st_err)?;
                Ok(Struct::default())
            }
            "DescribeTaskList" => {
                let desc: tk::DescribeTaskListRequest = inner(env, 20)?;
                let msg = m::DescribeTaskListRequest {
                    request: Some(convert::describe_task_list_request_out(desc)),
                    domain_id: domain_of(env)?,
                };
                let resp = client
                    .describe_task_list(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                let public =
                    convert::describe_task_list_in(cadence_proto::api::DescribeTaskListResponse {
                        pollers: resp.pollers,
                        task_list_status: resp.task_list_status,
                        partition_config: resp.partition_config,
                        task_list: resp.task_list,
                    })
                    .map_err(|s| internal(s.to_string()))?;
                Ok(public.to_thrift())
            }
            "ListTaskListPartitions" => {
                let req = tk::ListTaskListPartitionsRequest::from_thrift(env)
                    .map_err(|e| thrift_err("ListTaskListPartitions request", e))?;
                let resp = client
                    .list_task_list_partitions(r!(convert::list_task_list_partitions_request_out(
                        req,
                    )))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(convert::list_task_list_partitions_in(resp).to_thrift())
            }
            "GetTaskListsByDomain" => {
                let req = tk::GetTaskListsByDomainRequest::from_thrift(env)
                    .map_err(|e| thrift_err("GetTaskListsByDomain request", e))?;
                let resp = client
                    .get_task_lists_by_domain(r!(convert::get_task_lists_by_domain_request_out(
                        req
                    )))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(convert::get_task_lists_by_domain_in(resp)
                    .map_err(|s| internal(s.to_string()))?
                    .to_thrift())
            }
            "PollForDecisionTask" => {
                let reply = client
                    .poll_for_decision_task(r!(poll_decision_request(env)?))
                    .await
                    .map_err(st_err)?;
                poll_decision_response(reply.into_inner()).map_err(st_err)
            }
            "PollForActivityTask" => {
                let reply = client
                    .poll_for_activity_task(r!(poll_activity_request(env)?))
                    .await
                    .map_err(st_err)?;
                poll_activity_response(reply.into_inner()).map_err(st_err)
            }
            other => Err(internal(format!("no gRPC mapping for Matching {other}"))),
        }
    }
}

fn start_workflow_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::StartWorkflowExecutionRequest, CadenceError> {
    let start: ty::StartWorkflowExecutionRequest = inner(env, 20)?;
    let backoff = env.opt_i32(60).map_err(|e| thrift_err("backoff", e))?;
    let expiration = env.opt_i64(50).map_err(|e| thrift_err("expiration", e))?;
    Ok(h::StartWorkflowExecutionRequest {
        request: Some(cv::start_request_out(start)),
        domain_id,
        expiration_time: expiration.map(convert::timestamp_nanos),
        first_decision_task_backoff: backoff.map(|s| convert::duration(i64::from(s))),
        partition_config: str_map(env, 62)?,
        ..Default::default()
    })
}

fn start_workflow_response(resp: h::StartWorkflowExecutionResponse) -> Struct {
    ty::StartWorkflowExecutionResponse {
        run_id: resp.run_id,
    }
    .to_thrift()
}

fn signal_with_start_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::SignalWithStartWorkflowExecutionRequest, CadenceError> {
    let req: st::SignalWithStartWorkflowExecutionRequest = inner(env, 20)?;
    Ok(h::SignalWithStartWorkflowExecutionRequest {
        request: Some(cv::signal_with_start_request_out(req)),
        domain_id,
        partition_config: str_map(env, 30)?,
    })
}

fn signal_with_start_response(resp: h::SignalWithStartWorkflowExecutionResponse) -> Struct {
    ty::StartWorkflowExecutionResponse {
        run_id: resp.run_id,
    }
    .to_thrift()
}

fn reset_workflow_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::ResetWorkflowExecutionRequest, CadenceError> {
    let req: ty::ResetWorkflowExecutionRequest = inner(env, 20)?;
    Ok(h::ResetWorkflowExecutionRequest {
        request: Some(cv::reset_request_out(req)),
        domain_id,
    })
}

fn reset_workflow_response(resp: h::ResetWorkflowExecutionResponse) -> Struct {
    ty::ResetWorkflowExecutionResponse {
        run_id: resp.run_id,
    }
    .to_thrift()
}

fn query_workflow_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::QueryWorkflowRequest, CadenceError> {
    let req: tk::QueryWorkflowRequest = inner(env, 20)?;
    Ok(h::QueryWorkflowRequest {
        request: Some(cv::query_request_out(req)),
        domain_id,
    })
}

fn query_workflow_response(resp: h::QueryWorkflowResponse) -> Result<Struct, CadenceError> {
    let public = cv::query_response_in(cadence_proto::api::QueryWorkflowResponse {
        query_result: resp.query_result,
        query_rejected: resp.query_rejected,
    })
    .map_err(|s| internal(s.to_string()))?;
    // History's QueryWorkflowResponse wraps the public one at field 10.
    Ok(Struct {
        fields: vec![(10, Value::Struct(public.to_thrift()))],
    })
}

fn heartbeat_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::RecordActivityTaskHeartbeatRequest, CadenceError> {
    let req: tk::RecordActivityTaskHeartbeatRequest = inner(env, 20)?;
    Ok(h::RecordActivityTaskHeartbeatRequest {
        request: Some(cv::heartbeat_request_out(req)),
        domain_id,
    })
}

fn heartbeat_response(resp: h::RecordActivityTaskHeartbeatResponse) -> Struct {
    tk::RecordActivityTaskHeartbeatResponse {
        cancel_requested: resp.cancel_requested,
    }
    .to_thrift()
}

fn envelope_string(env: &Struct, id: i16) -> Result<String, CadenceError> {
    env.req_string(id, "")
        .map_err(|e| thrift_err("envelope", e))
}

fn envelope_value<T: FromThrift>(env: &Struct, id: i16) -> Result<Option<T>, CadenceError> {
    env.opt_struct(id).map_err(|e| thrift_err("envelope", e))
}

fn poll_mutable_state_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::PollMutableStateRequest, CadenceError> {
    // Go's History proto mapper has no version history item on this request.
    Ok(h::PollMutableStateRequest {
        domain_id,
        workflow_execution: envelope_value(env, 20)?.map(convert::execution_out),
        expected_next_event_id: env.i64_or_zero(30).map_err(|e| thrift_err("envelope", e))?,
        current_branch_token: env
            .bytes_or_empty(40)
            .map_err(|e| thrift_err("envelope", e))?,
    })
}

fn poll_mutable_state_response(v: h::PollMutableStateResponse) -> Result<Struct, tonic::Status> {
    let mut fields = Fields::new();
    fields.ostruct(10, v.workflow_execution.map(convert::execution).as_ref());
    fields.ostruct(20, v.workflow_type.map(cv::wf_type_in).as_ref());
    fields.oi64(30, Some(v.next_event_id));
    fields.oi64(35, v.previous_started_event_id);
    fields.oi64(40, Some(v.last_first_event_id));
    fields.ostruct(
        50,
        v.task_list.map(convert::task_list).transpose()?.as_ref(),
    );
    fields.ostruct(
        60,
        v.sticky_task_list
            .map(convert::task_list)
            .transpose()?
            .as_ref(),
    );
    fields.sstr(70, &v.client_library_version);
    fields.sstr(80, &v.client_feature_version);
    fields.sstr(90, &v.client_impl);
    fields.oi32(
        100,
        convert::seconds(v.sticky_task_list_schedule_to_start_timeout)?,
    );
    fields.sbytes(110, &v.current_branch_token);
    if let Some(histories) = v.version_histories {
        let mut thrift = Fields::new();
        thrift.oi32(10, Some(histories.current_version_history_index));
        let histories: Vec<_> = histories
            .histories
            .into_iter()
            .map(|h| ty::VersionHistory {
                branch_token: h.branch_token,
                items: h
                    .items
                    .into_iter()
                    .map(|item| ty::VersionHistoryItem {
                        event_id: item.event_id,
                        version: item.version,
                    })
                    .collect(),
            })
            .collect();
        thrift.olist(20, &histories);
        fields.fields.push((130, Value::Struct(thrift.finish())));
    }
    fields.oi32(
        140,
        (1..=6)
            .contains(&v.workflow_state)
            .then(|| v.workflow_state - 1),
    );
    fields.oi32(
        150,
        Some(if (1..=6).contains(&v.workflow_close_state) {
            v.workflow_close_state
        } else {
            0
        }),
    );
    Ok(fields.finish())
}

fn decision_completed_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::RespondDecisionTaskCompletedRequest, CadenceError> {
    Ok(h::RespondDecisionTaskCompletedRequest {
        domain_id,
        request: envelope_value(env, 20)?
            .map(cv::respond_decision_completed_request_out)
            .transpose()
            .map_err(|s| thrift_err("decision completion", s))?,
    })
}

fn decision_completed_response(
    v: h::RespondDecisionTaskCompletedResponse,
) -> Result<Struct, tonic::Status> {
    let mut fields = Fields::new();
    if let Some(started) = v.started_response {
        fields
            .fields
            .push((10, Value::Struct(decision_started_response(started)?)));
    }
    let activities = v
        .activities_to_dispatch_locally
        .into_iter()
        .map(|(id, activity)| (id, cv::activity_local_dispatch_in(activity)))
        .collect();
    fields.omap_str_struct(20, &Some(activities));
    Ok(fields.finish())
}

fn decision_started_response(
    v: h::RecordDecisionTaskStartedResponse,
) -> Result<Struct, tonic::Status> {
    let mut fields = Fields::new();
    fields.ostruct(10, v.workflow_type.map(cv::wf_type_in).as_ref());
    fields.oi64(20, v.previous_started_event_id);
    fields.oi64(30, Some(v.scheduled_event_id));
    fields.oi64(40, Some(v.started_event_id));
    fields.oi64(50, Some(v.next_event_id));
    fields.oi64(60, Some(i64::from(v.attempt)));
    fields.obool(70, v.sticky_execution_enabled);
    push_transient(&mut fields, 80, v.decision_info)?;
    fields.ostruct(
        90,
        v.workflow_execution_task_list
            .map(convert::task_list)
            .transpose()?
            .as_ref(),
    );
    fields.oi32(100, Some(v.event_store_version));
    fields.sbytes(110, &v.branch_token);
    fields.oi64(120, convert::nanos(v.scheduled_time));
    fields.oi64(130, convert::nanos(v.started_time));
    fields.omap_str_struct(
        140,
        &Some(
            v.queries
                .into_iter()
                .map(|(id, query)| (id, cv::query_in(query)))
                .collect(),
        ),
    );
    fields.oi64(150, Some(v.history_size));
    Ok(fields.finish())
}

fn push_transient(
    fields: &mut Fields,
    id: i16,
    v: Option<shared::TransientDecisionInfo>,
) -> Result<(), tonic::Status> {
    if let Some(v) = v {
        let mut decision = Fields::new();
        decision.ostruct(
            10,
            v.scheduled_event
                .map(transient_event_in)
                .transpose()?
                .as_ref(),
        );
        decision.ostruct(
            20,
            v.started_event
                .map(transient_event_in)
                .transpose()?
                .as_ref(),
        );
        fields.fields.push((id, Value::Struct(decision.finish())));
    }
    Ok(())
}

fn transient_event_in(mut event: api::HistoryEvent) -> Result<st::HistoryEvent, tonic::Status> {
    let timestamp = convert::nanos(event.event_time.take());
    let mut event = convert::history_in::event(event)?;
    event.timestamp = timestamp;
    Ok(event)
}

fn describe_workflow_request(
    env: &Struct,
    domain_id: String,
) -> Result<h::DescribeWorkflowExecutionRequest, CadenceError> {
    Ok(h::DescribeWorkflowExecutionRequest {
        domain_id,
        request: envelope_value(env, 20)?.map(cv::describe_request_out),
    })
}

fn describe_workflow_response(
    v: h::DescribeWorkflowExecutionResponse,
) -> Result<Struct, tonic::Status> {
    Ok(
        cv::describe_response_in(api::DescribeWorkflowExecutionResponse {
            execution_configuration: v.execution_configuration,
            workflow_execution_info: v.workflow_execution_info,
            pending_activities: v.pending_activities,
            pending_children: v.pending_children,
            pending_decision: v.pending_decision,
        })?
        .to_thrift(),
    )
}

fn ratelimit_request(env: &Struct) -> Result<h::RatelimitUpdateRequest, CadenceError> {
    let data = match env.get(10) {
        Some(Value::Struct(data)) => Some(shared::Any {
            value_type: envelope_string(data, 10)?,
            value: data
                .bytes_or_empty(20)
                .map_err(|e| thrift_err("ratelimit data", e))?,
        }),
        None => None,
        _ => return Err(internal("invalid ratelimit data")),
    };
    Ok(h::RatelimitUpdateRequest { data })
}

fn ratelimit_response(v: h::RatelimitUpdateResponse) -> Struct {
    let mut fields = Fields::new();
    if let Some(data) = v.data {
        let mut any = Fields::new();
        any.sstr(10, &data.value_type);
        any.sbytes(20, &data.value);
        fields.fields.push((10, Value::Struct(any.finish())));
    }
    fields.finish()
}

fn poll_decision_request(env: &Struct) -> Result<m::PollForDecisionTaskRequest, CadenceError> {
    Ok(m::PollForDecisionTaskRequest {
        domain_id: domain_of(env)?,
        poller_id: envelope_string(env, 15)?,
        request: envelope_value(env, 20)?.map(convert::poll_for_decision_task_request_out),
        forwarded_from: envelope_string(env, 30)?,
        isolation_group: envelope_string(env, 40)?,
    })
}

fn poll_activity_request(env: &Struct) -> Result<m::PollForActivityTaskRequest, CadenceError> {
    Ok(m::PollForActivityTaskRequest {
        domain_id: domain_of(env)?,
        poller_id: envelope_string(env, 15)?,
        request: envelope_value(env, 20)?.map(convert::poll_for_activity_task_request_out),
        forwarded_from: envelope_string(env, 30)?,
        isolation_group: envelope_string(env, 40)?,
    })
}

fn poll_decision_response(v: m::PollForDecisionTaskResponse) -> Result<Struct, tonic::Status> {
    let mut fields = Fields::new();
    fields.sbytes(10, &v.task_token);
    fields.ostruct(20, v.workflow_execution.map(convert::execution).as_ref());
    fields.ostruct(30, v.workflow_type.map(cv::wf_type_in).as_ref());
    fields.oi64(40, v.previous_started_event_id);
    fields.oi64(50, Some(v.started_event_id));
    fields.oi64(51, Some(i64::from(v.attempt)));
    fields.oi64(60, Some(v.next_event_id));
    fields.oi64(65, Some(v.backlog_count_hint));
    fields.obool(70, v.sticky_execution_enabled);
    fields.ostruct(80, v.query.map(cv::query_in).as_ref());
    push_transient(&mut fields, 90, v.decision_info)?;
    fields.ostruct(
        100,
        v.workflow_execution_task_list
            .map(convert::task_list)
            .transpose()?
            .as_ref(),
    );
    fields.oi32(110, Some(v.event_store_version));
    fields.sbytes(120, &v.branch_token);
    fields.oi64(130, convert::nanos(v.scheduled_time));
    fields.oi64(140, convert::nanos(v.started_time));
    fields.omap_str_struct(
        150,
        &Some(
            v.queries
                .into_iter()
                .map(|(id, query)| (id, cv::query_in(query)))
                .collect(),
        ),
    );
    fields.oi64(160, Some(v.total_history_bytes));
    fields.ostruct(
        170,
        v.auto_config_hint
            .map(convert::auto_config_hint_in)
            .as_ref(),
    );
    let mut reply = fields.finish();
    push_poll_feedback(&mut reply, v.partition_config, v.load_balancer_hints);
    Ok(reply)
}

fn poll_activity_response(v: m::PollForActivityTaskResponse) -> Result<Struct, tonic::Status> {
    let mut activity = cv::poll_activity_response_in(api::PollForActivityTaskResponse {
        task_token: v.task_token,
        workflow_execution: v.workflow_execution,
        activity_id: v.activity_id,
        activity_type: v.activity_type,
        input: v.input,
        scheduled_time: v.scheduled_time,
        started_time: v.started_time,
        schedule_to_close_timeout: v.schedule_to_close_timeout,
        start_to_close_timeout: v.start_to_close_timeout,
        heartbeat_timeout: v.heartbeat_timeout,
        attempt: v.attempt,
        scheduled_time_of_this_attempt: v.scheduled_time_of_this_attempt,
        heartbeat_details: v.heartbeat_details,
        workflow_type: v.workflow_type,
        workflow_domain: v.workflow_domain,
        header: v.header,
        auto_config_hint: v.auto_config_hint,
    })?;
    // The public Thrift converter multiplies millisecond timestamps by a
    // million. Matching's protobuf timestamps already map to wire nanoseconds.
    let timestamps = [
        (70, activity.scheduled_timestamp.take()),
        (90, activity.started_timestamp.take()),
        (130, activity.scheduled_timestamp_of_this_attempt.take()),
    ];
    let mut reply = activity.to_thrift();
    reply.fields.extend(
        timestamps
            .into_iter()
            .filter_map(|(id, timestamp)| timestamp.map(|value| (id, Value::I64(value)))),
    );
    push_poll_feedback(&mut reply, v.partition_config, v.load_balancer_hints);
    Ok(reply)
}

// Internal fields on the adapter's reply. Go's Matching Thrift IDL has
// no fields for feedback, so only gRPC replies populate these.
const POLL_PARTITION_CONFIG: i16 = -1;
const POLL_LOAD_BALANCER_HINTS: i16 = -2;

#[allow(deprecated)]
fn push_poll_feedback(
    reply: &mut Struct,
    config: Option<m::TaskListPartitionConfig>,
    hints: Option<m::LoadBalancerHints>,
) {
    if let Some(config) = config {
        let partitions = |map: HashMap<i32, m::TaskListPartition>| {
            Value::Map(
                map.into_iter()
                    .map(|(id, partition)| {
                        (
                            Value::I32(id),
                            Value::List(
                                partition
                                    .isolation_groups
                                    .into_iter()
                                    .map(|group| Value::Bytes(group.into_bytes()))
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            )
        };
        reply.fields.push((
            POLL_PARTITION_CONFIG,
            Value::Struct(Struct {
                fields: vec![
                    (10, Value::I64(config.version)),
                    (20, Value::I32(config.num_read_partitions)),
                    (30, partitions(config.read_partitions)),
                    (40, Value::I32(config.num_write_partitions)),
                    (50, partitions(config.write_partitions)),
                ],
            }),
        ));
    }
    if let Some(hints) = hints {
        reply.fields.push((
            POLL_LOAD_BALANCER_HINTS,
            Value::Struct(Struct {
                fields: vec![
                    (10, Value::I64(hints.backlog_count)),
                    (20, Value::F64(hints.rate_per_second)),
                ],
            }),
        ));
    }
}

pub(crate) fn poll_feedback(
    reply: &Struct,
) -> (
    Option<cadence_common::matching_lb::PartitionConfig>,
    Option<cadence_common::matching_lb::LoadBalancerHints>,
) {
    use cadence_common::matching_lb::{LoadBalancerHints, PartitionConfig};

    let config = match reply.get(POLL_PARTITION_CONFIG) {
        Some(Value::Struct(config)) => {
            let partitions = |field| match config.get(field) {
                Some(Value::Map(entries)) => entries
                    .iter()
                    .filter_map(|(id, groups)| match (id, groups) {
                        (Value::I32(id), Value::List(groups)) => Some((
                            *id,
                            groups
                                .iter()
                                .filter_map(|group| match group {
                                    Value::Bytes(bytes) => String::from_utf8(bytes.clone()).ok(),
                                    _ => None,
                                })
                                .collect(),
                        )),
                        _ => None,
                    })
                    .collect(),
                _ => HashMap::new(),
            };
            Some(PartitionConfig::from_parts(
                config.i64_or_zero(10).unwrap_or_default(),
                config.opt_i32(20).ok().flatten().unwrap_or_default(),
                partitions(30),
                config.opt_i32(40).ok().flatten().unwrap_or_default(),
                partitions(50),
            ))
        }
        _ => None,
    };
    let hints = match reply.get(POLL_LOAD_BALANCER_HINTS) {
        Some(Value::Struct(hints)) => Some(LoadBalancerHints {
            backlog_count: hints.i64_or_zero(10).unwrap_or_default(),
            rate_per_second: match hints.get(20) {
                Some(Value::F64(rate)) => *rate,
                _ => 0.0,
            },
        }),
        _ => None,
    };
    (config, hints)
}

fn domain_of(env: &Struct) -> Result<String, CadenceError> {
    Ok(env
        .opt_string(10)
        .map_err(|e| thrift_err("domain uuid", e))?
        .unwrap_or_default())
}

fn inner<T: FromThrift>(env: &Struct, id: i16) -> Result<T, CadenceError> {
    match env.get(id) {
        Some(Value::Struct(s)) => T::from_thrift(s).map_err(|e| thrift_err("envelope", e)),
        _ => Err(internal(format!("missing envelope field {id}"))),
    }
}

fn str_map(env: &Struct, id: i16) -> Result<HashMap<String, String>, CadenceError> {
    let mut out = HashMap::new();
    if let Some(Value::Map(entries)) = env.get(id) {
        for (k, v) in entries {
            if let (Value::Bytes(k), Value::Bytes(v)) = (k, v) {
                out.insert(
                    String::from_utf8_lossy(k).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                );
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_defaults_to_grpc_like_go() {
        assert_eq!(select_transport(None), OutboundTransport::Grpc);
        assert_eq!(select_transport(Some(true)), OutboundTransport::Grpc);
        assert_eq!(select_transport(Some(false)), OutboundTransport::Tchannel);
    }

    #[test]
    fn grpc_address_swaps_port() {
        assert_eq!(grpc_address("10.0.0.1:7934", 7834), "10.0.0.1:7834");
        assert_eq!(grpc_address("host", 7834), "host:7834");
    }

    #[test]
    fn unmapped_methods_stay_on_tchannel() {
        assert!(supported("cadence-history", "SignalWorkflowExecution"));
        assert!(supported("cadence-matching", "DescribeTaskList"));
        assert!(supported("cadence-history", "PollMutableState"));
        assert!(!supported("cadence-history", "GetMutableState"));
        assert!(supported("cadence-matching", "PollForDecisionTask"));
    }

    fn envelope(request: &impl ToThrift) -> Struct {
        let mut fields = Fields::new();
        fields.sstr(10, "domain-id");
        fields.ostruct(20, Some(request));
        fields.finish()
    }

    #[test]
    fn poll_mutable_state_mapping_round_trip() {
        let execution = ty::WorkflowExecution {
            workflow_id: "workflow".into(),
            run_id: "run".into(),
        };
        let mut env = envelope(&execution);
        env.fields.extend([
            (30, Value::I64(42)),
            (40, Value::Bytes(vec![1, 2])),
            (
                50,
                Value::Struct(
                    ty::VersionHistoryItem {
                        event_id: 17,
                        version: 3,
                    }
                    .to_thrift(),
                ),
            ),
        ]);
        let req = poll_mutable_state_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let reply = poll_mutable_state_response(h::PollMutableStateResponse {
            workflow_execution: req.workflow_execution,
            next_event_id: req.expected_next_event_id,
            previous_started_event_id: Some(0),
            current_branch_token: req.current_branch_token,
            version_histories: Some(convert::version_histories_out(ty::VersionHistories {
                current_version_history_index: 0,
                histories: vec![ty::VersionHistory {
                    branch_token: vec![1, 2],
                    items: vec![ty::VersionHistoryItem {
                        event_id: 17,
                        version: 3,
                    }],
                }],
            })),
            workflow_state: 2,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            reply.opt_struct::<ty::WorkflowExecution>(10).unwrap(),
            Some(execution)
        );
        assert_eq!(reply.opt_i64(30).unwrap(), Some(42));
        assert_eq!(reply.opt_i64(35).unwrap(), Some(0));
        assert_eq!(reply.bytes_or_empty(110).unwrap(), vec![1, 2]);
        let Value::Struct(histories) = reply.get(130).unwrap() else {
            panic!("version histories missing")
        };
        let histories = histories
            .opt_list::<ty::VersionHistory>(20)
            .unwrap()
            .unwrap();
        assert_eq!(histories[0].items[0].version, 3);
        assert_eq!(reply.opt_i32(140).unwrap(), Some(1));
        assert_eq!(reply.opt_i32(150).unwrap(), Some(0));
    }

    #[test]
    fn decision_completion_mapping_round_trip() {
        let env = envelope(&tk::RespondDecisionTaskCompletedRequest {
            task_token: vec![8, 9],
            return_new_decision_task: true,
            ..Default::default()
        });
        let req = decision_completed_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let complete = req.request.unwrap();
        assert!(complete.return_new_decision_task);
        let reply = decision_completed_response(h::RespondDecisionTaskCompletedResponse {
            started_response: Some(h::RecordDecisionTaskStartedResponse {
                scheduled_event_id: 7,
                attempt: 2,
                scheduled_time: Some(convert::timestamp_from_nanos(123456789)),
                decision_info: Some(shared::TransientDecisionInfo {
                    scheduled_event: Some(api::HistoryEvent {
                        event_id: 7,
                        event_time: Some(convert::timestamp_from_nanos(123456789)),
                        attributes: Some(
                            api::history_event::Attributes::DecisionTaskScheduledEventAttributes(
                                api::DecisionTaskScheduledEventAttributes::default(),
                            ),
                        ),
                        ..Default::default()
                    }),
                    started_event: None,
                }),
                ..Default::default()
            }),
            activities_to_dispatch_locally: HashMap::from([(
                "activity".into(),
                api::ActivityLocalDispatchInfo {
                    task_token: complete.task_token,
                    ..Default::default()
                },
            )]),
        })
        .unwrap();
        let Value::Struct(started) = reply.get(10).unwrap() else {
            panic!("started response missing")
        };
        assert_eq!(started.opt_i64(30).unwrap(), Some(7));
        assert_eq!(started.opt_i64(60).unwrap(), Some(2));
        assert_eq!(started.opt_i64(120).unwrap(), Some(123456789));
        let Value::Struct(decision) = started.get(80).unwrap() else {
            panic!("transient decision missing");
        };
        let event = decision.opt_struct::<st::HistoryEvent>(10).unwrap().unwrap();
        assert_eq!(event.timestamp, Some(123456789));
        let dispatch = reply
            .opt_map_str::<tk::ActivityLocalDispatchInfo>(20)
            .unwrap()
            .unwrap();
        assert_eq!(dispatch["activity"].task_token, vec![8, 9]);
    }

    #[test]
    fn describe_workflow_mapping_round_trip() {
        let execution = ty::WorkflowExecution {
            workflow_id: "test-workflow".into(),
            run_id: "run".into(),
        };
        let env = envelope(&st::DescribeWorkflowExecutionRequest {
            execution: Some(execution.clone()),
            ..Default::default()
        });
        let req = describe_workflow_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let reply = describe_workflow_response(h::DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(api::WorkflowExecutionInfo {
                workflow_execution: req.request.unwrap().workflow_execution,
                start_time: Some(convert::timestamp_from_nanos(123456789)),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        let described = st::DescribeWorkflowExecutionResponse::from_thrift(&reply)
            .unwrap()
            .workflow_execution_info
            .unwrap();
        assert_eq!(described.execution, Some(execution));
        assert_eq!(described.start_time, Some(123456789));
    }

    #[test]
    fn ratelimit_mapping_round_trip() {
        let env = Struct {
            fields: vec![(
                10,
                Value::Struct(Struct {
                    fields: vec![
                        (10, Value::Bytes(b"usage".to_vec())),
                        (20, Value::Bytes(vec![0, 255])),
                    ],
                }),
            )],
        };
        let req = ratelimit_request(&env).unwrap();
        assert_eq!(req.data.as_ref().unwrap().value_type, "usage");
        assert_eq!(
            ratelimit_response(h::RatelimitUpdateResponse { data: req.data }),
            env
        );
    }

    #[test]
    #[allow(deprecated)]
    fn decision_poll_mapping_round_trip() {
        let mut env = envelope(&tk::PollForDecisionTaskRequest {
            identity: "worker".into(),
            ..Default::default()
        });
        env.fields.extend([
            (15, Value::Bytes(b"poller".to_vec())),
            (30, Value::Bytes(b"partition".to_vec())),
            (40, Value::Bytes(b"group".to_vec())),
        ]);
        let req = poll_decision_request(&env).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        assert_eq!(req.poller_id, "poller");
        assert_eq!(req.forwarded_from, "partition");
        assert_eq!(req.isolation_group, "group");
        assert_eq!(req.request.unwrap().identity, "worker");
        let reply = poll_decision_response(m::PollForDecisionTaskResponse {
            previous_started_event_id: Some(0),
            attempt: 2,
            query: Some(api::WorkflowQuery {
                query_type: "query".into(),
                query_args: convert::payload(vec![1, 2]),
            }),
            partition_config: Some(m::TaskListPartitionConfig {
                version: 5,
                num_read_partitions: 2,
                read_partitions: HashMap::from([(
                    0,
                    m::TaskListPartition {
                        isolation_groups: vec!["group".into()],
                    },
                )]),
                ..Default::default()
            }),
            auto_config_hint: Some(api::AutoConfigHint {
                enable_auto_config: true,
                poller_wait_time_in_ms: 37,
            }),
            ..Default::default()
        })
        .unwrap();
        assert!(reply.get(20).is_none()); // Empty poll still carries feedback and auto-config.
        assert_eq!(reply.opt_i64(40).unwrap(), Some(0));
        assert_eq!(reply.opt_i64(51).unwrap(), Some(2));
        assert_eq!(
            reply
                .opt_struct::<tk::WorkflowQuery>(80)
                .unwrap()
                .unwrap()
                .query_args,
            vec![1, 2]
        );
        assert_eq!(
            reply
                .opt_struct::<ty::AutoConfigHint>(170)
                .unwrap()
                .unwrap()
                .poller_wait_time_in_ms,
            37
        );
        let (config, _) = poll_feedback(&reply);
        let config = config.unwrap();
        assert_eq!(config.version, 5);
        assert_eq!(config.read_partitions.len(), 2);
        assert!(config.read_partitions[&0].is_empty()); // Deprecated count wins on mismatch.
    }

    #[test]
    fn activity_poll_mapping_round_trip() {
        let mut env = envelope(&tk::PollForActivityTaskRequest {
            identity: "worker".into(),
            ..Default::default()
        });
        env.fields.extend([
            (15, Value::Bytes(b"poller".to_vec())),
            (30, Value::Bytes(b"partition".to_vec())),
            (40, Value::Bytes(b"group".to_vec())),
        ]);
        let req = poll_activity_request(&env).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        assert_eq!(req.poller_id, "poller");
        assert_eq!(req.forwarded_from, "partition");
        assert_eq!(req.isolation_group, "group");
        assert_eq!(req.request.unwrap().identity, "worker");
        let activity = tk::PollForActivityTaskResponse {
            task_token: vec![1],
            activity_id: "activity".into(),
            input: vec![2],
            scheduled_timestamp: Some(1_700_000_000_123),
            heartbeat_timeout_seconds: Some(42),
            ..Default::default()
        };
        let proto = convert::activity_task(activity.clone());
        let reply = poll_activity_response(m::PollForActivityTaskResponse {
            task_token: proto.task_token,
            activity_id: proto.activity_id,
            input: proto.input,
            scheduled_time: Some(convert::timestamp_from_nanos(1_700_000_000_123_456_789)),
            heartbeat_timeout: proto.heartbeat_timeout,
            load_balancer_hints: Some(m::LoadBalancerHints {
                backlog_count: 3,
                rate_per_second: 1.5,
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(reply.opt_i64(70).unwrap(), Some(1_700_000_000_123_456_789));
        let decoded = tk::PollForActivityTaskResponse::from_thrift(&reply).unwrap();
        assert_eq!(decoded, activity);
        let (_, hints) = poll_feedback(&reply);
        let hints = hints.unwrap();
        assert_eq!(hints.backlog_count, 3);
        assert_eq!(hints.rate_per_second, 1.5);
    }

    #[test]
    fn existing_start_workflow_mapping_round_trip() {
        let mut env = envelope(&ty::StartWorkflowExecutionRequest {
            workflow_id: "workflow".into(),
            ..Default::default()
        });
        env.fields.push((60, Value::I32(17)));
        let req = start_workflow_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        assert_eq!(req.first_decision_task_backoff.unwrap().seconds, 17);
        let reply = start_workflow_response(h::StartWorkflowExecutionResponse {
            run_id: req.request.unwrap().workflow_id,
        });
        assert_eq!(
            ty::StartWorkflowExecutionResponse::from_thrift(&reply)
                .unwrap()
                .run_id,
            "workflow"
        );
    }

    #[test]
    fn existing_signal_with_start_mapping_round_trip() {
        let env = envelope(&st::SignalWithStartWorkflowExecutionRequest {
            workflow_id: "workflow".into(),
            signal_name: "signal".into(),
            ..Default::default()
        });
        let req = signal_with_start_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let request = req.request.unwrap();
        assert_eq!(request.signal_name, "signal");
        let reply = signal_with_start_response(h::SignalWithStartWorkflowExecutionResponse {
            run_id: request.start_request.unwrap().workflow_id,
        });
        assert_eq!(
            ty::StartWorkflowExecutionResponse::from_thrift(&reply)
                .unwrap()
                .run_id,
            "workflow"
        );
    }

    #[test]
    fn existing_reset_workflow_mapping_round_trip() {
        let env = envelope(&ty::ResetWorkflowExecutionRequest {
            workflow_execution: Some(ty::WorkflowExecution {
                workflow_id: "workflow".into(),
                run_id: "run".into(),
            }),
            decision_finish_event_id: 5,
            ..Default::default()
        });
        let req = reset_workflow_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let request = req.request.unwrap();
        assert_eq!(request.decision_finish_event_id, 5);
        let reply = reset_workflow_response(h::ResetWorkflowExecutionResponse {
            run_id: request.workflow_execution.unwrap().run_id,
        });
        assert_eq!(
            ty::ResetWorkflowExecutionResponse::from_thrift(&reply)
                .unwrap()
                .run_id,
            "run"
        );
    }

    #[test]
    fn existing_query_workflow_mapping_round_trip() {
        let env = envelope(&tk::QueryWorkflowRequest {
            query: Some(tk::WorkflowQuery {
                query_type: "test-query".into(),
                query_args: vec![1, 2],
            }),
            ..Default::default()
        });
        let req = query_workflow_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let query = req.request.unwrap().query.unwrap();
        assert_eq!(query.query_type, "test-query");
        let reply = query_workflow_response(h::QueryWorkflowResponse {
            query_result: query.query_args,
            query_rejected: None,
        })
        .unwrap();
        assert_eq!(
            reply
                .opt_struct::<tk::QueryWorkflowResponse>(10)
                .unwrap()
                .unwrap()
                .query_result,
            vec![1, 2]
        );
    }

    #[test]
    fn existing_activity_heartbeat_mapping_round_trip() {
        let env = envelope(&tk::RecordActivityTaskHeartbeatRequest {
            task_token: vec![1, 2],
            ..Default::default()
        });
        let req = heartbeat_request(&env, domain_of(&env).unwrap()).unwrap();
        assert_eq!(req.domain_id, "domain-id");
        let reply = heartbeat_response(h::RecordActivityTaskHeartbeatResponse {
            cancel_requested: req.request.unwrap().task_token == vec![1, 2],
        });
        assert!(
            tk::RecordActivityTaskHeartbeatResponse::from_thrift(&reply)
                .unwrap()
                .cancel_requested
        );
    }

    #[tokio::test]
    async fn tchannel_selected_returns_none() {
        let g = GrpcOutbound::new(Arc::new(|| false), 7834, 7835);
        let r = g
            .try_call(
                "127.0.0.1:1",
                "cadence-history",
                "SignalWorkflowExecution",
                &Struct::default(),
                Duration::from_secs(1),
                &[],
            )
            .await;
        assert!(r.is_none());
    }

    #[test]
    fn str_map_reads_partition_config() {
        let env = Struct {
            fields: vec![(
                62,
                Value::Map(vec![(
                    Value::Bytes(b"k".to_vec()),
                    Value::Bytes(b"v".to_vec()),
                )]),
            )],
        };
        assert_eq!(
            str_map(&env, 62).unwrap().get("k").map(String::as_str),
            Some("v")
        );
    }
}
