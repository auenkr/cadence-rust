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
use cadence_proto::history as h;
use cadence_proto::matching as m;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};
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
        ),
        "cadence-matching" => matches!(
            method,
            "CancelOutstandingPoll"
                | "RespondQueryTaskCompleted"
                | "DescribeTaskList"
                | "ListTaskListPartitions"
                | "GetTaskListsByDomain"
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
        let domain_id = domain_of(env)?;
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
                let start: ty::StartWorkflowExecutionRequest = inner(env, 20)?;
                let backoff = env.opt_i32(60).map_err(|e| thrift_err("backoff", e))?;
                let expiration = env.opt_i64(50).map_err(|e| thrift_err("expiration", e))?;
                let msg = h::StartWorkflowExecutionRequest {
                    request: Some(cv::start_request_out(start)),
                    domain_id,
                    expiration_time: expiration.map(convert::timestamp_nanos),
                    first_decision_task_backoff: backoff.map(|s| convert::duration(i64::from(s))),
                    partition_config: str_map(env, 62)?,
                    ..Default::default()
                };
                let resp = client
                    .start_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(ty::StartWorkflowExecutionResponse {
                    run_id: resp.run_id,
                }
                .to_thrift())
            }
            "SignalWithStartWorkflowExecution" => {
                let req: st::SignalWithStartWorkflowExecutionRequest = inner(env, 20)?;
                let msg = h::SignalWithStartWorkflowExecutionRequest {
                    request: Some(cv::signal_with_start_request_out(req)),
                    domain_id,
                    partition_config: str_map(env, 30)?,
                };
                let resp = client
                    .signal_with_start_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(ty::StartWorkflowExecutionResponse {
                    run_id: resp.run_id,
                }
                .to_thrift())
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
                let req: ty::ResetWorkflowExecutionRequest = inner(env, 20)?;
                let msg = h::ResetWorkflowExecutionRequest {
                    request: Some(cv::reset_request_out(req)),
                    domain_id,
                };
                let resp = client
                    .reset_workflow_execution(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(ty::ResetWorkflowExecutionResponse {
                    run_id: resp.run_id,
                }
                .to_thrift())
            }
            "QueryWorkflow" => {
                let req: tk::QueryWorkflowRequest = inner(env, 20)?;
                let msg = h::QueryWorkflowRequest {
                    request: Some(cv::query_request_out(req)),
                    domain_id,
                };
                let resp = client
                    .query_workflow(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
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
                let req: tk::RecordActivityTaskHeartbeatRequest = inner(env, 20)?;
                let msg = h::RecordActivityTaskHeartbeatRequest {
                    request: Some(cv::heartbeat_request_out(req)),
                    domain_id,
                };
                let resp = client
                    .record_activity_task_heartbeat(r!(msg))
                    .await
                    .map_err(st_err)?
                    .into_inner();
                Ok(tk::RecordActivityTaskHeartbeatResponse {
                    cancel_requested: resp.cancel_requested,
                }
                .to_thrift())
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
            other => Err(internal(format!("no gRPC mapping for Matching {other}"))),
        }
    }
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
        assert!(!supported("cadence-history", "PollMutableState"));
        assert!(!supported("cadence-matching", "PollForDecisionTask"));
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
