//! Frontend -> Go History forwarding over the generated protobuf History API.
//!
//! Go's Frontend never mutates executions itself: each workflow API resolves
//! the domain UUID and forwards to the History host that owns the workflow ID's
//! shard. This module holds the typed request mapping and the History error
//! mapping shared by the Thrift/TChannel and JSON/gRPC bridges. Shard owner
//! lookup is the `HistoryPeers` seam: the launcher installs a static peer today
//! and the ringpop-backed resolver when Frontend joins membership.
use cadence_api::{errors::WorkflowExecutionAlreadyStartedError, service_types as st, types as t};
use cadence_proto::{api as p, convert as cv, history as h};
use prost::Message;
use std::sync::Arc;
use tonic::{transport::Channel, Code, Status};

/// Resolve the History gRPC channel that owns a workflow ID.
pub trait HistoryPeers: Send + Sync + std::fmt::Debug {
    fn channel_for_workflow(&self, workflow_id: &str) -> Result<Channel, ForwardError>;
}

/// One configured History endpoint owning every shard.
#[derive(Debug, Clone)]
pub struct StaticHistoryPeer(pub Channel);

impl HistoryPeers for StaticHistoryPeer {
    fn channel_for_workflow(&self, _workflow_id: &str) -> Result<Channel, ForwardError> {
        Ok(self.0.clone())
    }
}

#[derive(Debug, Clone)]
pub struct HistoryForward {
    peers: Arc<dyn HistoryPeers>,
    max_message_size: usize,
}

/// Cadence application errors recovered from a History gRPC status.
#[derive(Debug, Clone, PartialEq)]
pub enum ForwardError {
    NotFound(String),
    AlreadyCompleted(String),
    AlreadyStarted(WorkflowExecutionAlreadyStartedError),
    BadRequest(String),
    Busy(String),
    Internal(String),
}

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// Go proto `ToError`: the first typed detail refines the gRPC code.
pub fn map_status(status: &Status) -> ForwardError {
    let detail = RpcStatus::decode(status.details())
        .ok()
        .and_then(|s| s.details.into_iter().next());
    let name = detail
        .as_ref()
        .map(|d| d.type_url.rsplit('/').next().unwrap_or_default().to_owned())
        .unwrap_or_default();
    let message = status.message().to_owned();
    match status.code() {
        Code::NotFound if name.ends_with("WorkflowExecutionAlreadyCompletedError") => {
            ForwardError::AlreadyCompleted(message)
        }
        Code::NotFound => ForwardError::NotFound(message),
        Code::AlreadyExists => {
            let started = detail
                .and_then(|d| p::WorkflowExecutionAlreadyStartedError::decode(&d.value[..]).ok())
                .unwrap_or_default();
            ForwardError::AlreadyStarted(WorkflowExecutionAlreadyStartedError {
                message,
                start_request_id: started.start_request_id,
                run_id: started.run_id,
            })
        }
        Code::InvalidArgument => ForwardError::BadRequest(message),
        Code::ResourceExhausted => ForwardError::Busy(message),
        _ => ForwardError::Internal(message),
    }
}

fn dur(seconds: Option<i32>) -> Option<prost_types::Duration> {
    seconds.map(|s| cv::duration(i64::from(s)))
}

fn pb_enum<T: Into<i32>>(value: Option<T>) -> i32 {
    value.map_or(0, |v| v.into() + 1)
}

/// Thrift `StartWorkflowExecutionRequest` -> `api.v1` (inverse of `cv::start`).
pub fn start_out(v: &t::StartWorkflowExecutionRequest) -> p::StartWorkflowExecutionRequest {
    let v = v.clone();
    p::StartWorkflowExecutionRequest {
        domain: v.domain,
        workflow_id: v.workflow_id,
        workflow_type: v.workflow_type.map(|x| p::WorkflowType { name: x.name }),
        task_list: v.task_list.map(cv::task_list_out),
        input: cv::payload(v.input),
        execution_start_to_close_timeout: dur(v.execution_start_to_close_timeout_seconds),
        task_start_to_close_timeout: dur(v.task_start_to_close_timeout_seconds),
        identity: v.identity,
        request_id: v.request_id,
        workflow_id_reuse_policy: pb_enum(v.workflow_id_reuse_policy.map(|x| x as i32)),
        retry_policy: v.retry_policy.map(cv::retry_out),
        cron_schedule: v.cron_schedule,
        memo: v.memo.map(cv::memo),
        search_attributes: v.search_attributes.map(cv::search_attributes),
        header: v.header.map(cv::header),
        delay_start: dur(v.delay_start_seconds),
        jitter_start: dur(v.jitter_start_seconds),
        first_run_at: v.first_run_at_timestamp.map(cv::timestamp),
        cron_overlap_policy: pb_enum(v.cron_overlap_policy.map(|x| x as i32)),
        active_cluster_selection_policy: None,
    }
}

fn sws_start_out(v: &st::SignalWithStartWorkflowExecutionRequest) -> p::StartWorkflowExecutionRequest {
    start_out(&t::StartWorkflowExecutionRequest {
        domain: v.domain.clone(),
        workflow_id: v.workflow_id.clone(),
        workflow_type: v.workflow_type.clone(),
        task_list: v.task_list.clone(),
        input: v.input.clone(),
        execution_start_to_close_timeout_seconds: v.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: v.task_start_to_close_timeout_seconds,
        identity: v.identity.clone(),
        request_id: v.request_id.clone(),
        workflow_id_reuse_policy: v.workflow_id_reuse_policy,
        retry_policy: v.retry_policy.clone(),
        cron_schedule: v.cron_schedule.clone(),
        memo: v.memo.clone(),
        search_attributes: v.search_attributes.clone(),
        header: v.header.clone(),
        cron_overlap_policy: v.cron_overlap_policy,
        delay_start_seconds: v.delay_start_seconds,
        jitter_start_seconds: v.jitter_start_seconds,
        first_run_at_timestamp: v.first_run_at_timestamp,
    })
}

impl HistoryForward {
    pub fn new(peers: Arc<dyn HistoryPeers>, max_message_size: usize) -> Self {
        Self {
            peers,
            max_message_size,
        }
    }

    fn client(
        &self,
        workflow_id: &str,
    ) -> Result<h::history_api_client::HistoryApiClient<Channel>, ForwardError> {
        let channel = self
            .peers
            .channel_for_workflow(workflow_id)?;
        Ok(h::history_api_client::HistoryApiClient::new(channel)
            .max_decoding_message_size(self.max_message_size)
            .max_encoding_message_size(self.max_message_size))
    }

    fn request<T>(payload: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(payload);
        let md = request.metadata_mut();
        md.insert("rpc-service", "cadence-history".parse().expect("static"));
        md.insert("rpc-caller", "cadence-frontend".parse().expect("static"));
        md.insert("rpc-encoding", "proto".parse().expect("static"));
        request.set_timeout(std::time::Duration::from_secs(30));
        request
    }

    pub async fn start(
        &self,
        domain_id: &str,
        req: &t::StartWorkflowExecutionRequest,
    ) -> Result<t::StartWorkflowExecutionResponse, ForwardError> {
        let mut client = self.client(&req.workflow_id)?;
        let response = client
            .start_workflow_execution(Self::request(h::StartWorkflowExecutionRequest {
                request: Some(start_out(req)),
                domain_id: domain_id.to_owned(),
                ..Default::default()
            }))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(t::StartWorkflowExecutionResponse {
            run_id: response.run_id,
        })
    }

    pub async fn signal_with_start(
        &self,
        domain_id: &str,
        req: &st::SignalWithStartWorkflowExecutionRequest,
    ) -> Result<t::StartWorkflowExecutionResponse, ForwardError> {
        let mut client = self.client(&req.workflow_id)?;
        let response = client
            .signal_with_start_workflow_execution(Self::request(
                h::SignalWithStartWorkflowExecutionRequest {
                    request: Some(p::SignalWithStartWorkflowExecutionRequest {
                        start_request: Some(sws_start_out(req)),
                        signal_name: req.signal_name.clone(),
                        signal_input: cv::payload(req.signal_input.clone()),
                        control: req.control.clone(),
                    }),
                    domain_id: domain_id.to_owned(),
                    ..Default::default()
                },
            ))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(t::StartWorkflowExecutionResponse {
            run_id: response.run_id,
        })
    }

    pub async fn signal(
        &self,
        domain_id: &str,
        req: &t::SignalWorkflowExecutionRequest,
    ) -> Result<(), ForwardError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        client
            .signal_workflow_execution(Self::request(h::SignalWorkflowExecutionRequest {
                request: Some(p::SignalWorkflowExecutionRequest {
                    domain: req.domain.clone(),
                    workflow_execution: Some(cv::execution_out(execution)),
                    identity: req.identity.clone(),
                    request_id: req.request_id.clone(),
                    signal_name: req.signal_name.clone(),
                    signal_input: cv::payload(req.input.clone()),
                    control: req.control.clone(),
                }),
                domain_id: domain_id.to_owned(),
                ..Default::default()
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn terminate(
        &self,
        domain_id: &str,
        req: &t::TerminateWorkflowExecutionRequest,
    ) -> Result<(), ForwardError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        client
            .terminate_workflow_execution(Self::request(h::TerminateWorkflowExecutionRequest {
                request: Some(p::TerminateWorkflowExecutionRequest {
                    domain: req.domain.clone(),
                    workflow_execution: Some(cv::execution_out(execution)),
                    reason: req.reason.clone(),
                    details: cv::payload(req.details.clone()),
                    identity: req.identity.clone(),
                    first_execution_run_id: req.first_execution_run_id.clone(),
                }),
                domain_id: domain_id.to_owned(),
                ..Default::default()
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn request_cancel(
        &self,
        domain_id: &str,
        req: &st::RequestCancelWorkflowExecutionRequest,
    ) -> Result<(), ForwardError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        client
            .request_cancel_workflow_execution(Self::request(
                h::RequestCancelWorkflowExecutionRequest {
                    domain_id: domain_id.to_owned(),
                    cancel_request: Some(p::RequestCancelWorkflowExecutionRequest {
                        domain: req.domain.clone(),
                        workflow_execution: Some(cv::execution_out(execution)),
                        identity: req.identity.clone(),
                        request_id: req.request_id.clone(),
                        cause: req.cause.clone(),
                        first_execution_run_id: req.first_execution_run_id.clone(),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums as e;

    #[test]
    fn start_out_maps_seconds_enums_and_payload() {
        let req = t::StartWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_id: "w".into(),
            input: b"in".to_vec(),
            execution_start_to_close_timeout_seconds: Some(30),
            workflow_id_reuse_policy: Some(e::WorkflowIDReusePolicy::RejectDuplicate),
            ..Default::default()
        };
        let out = start_out(&req);
        assert_eq!(out.execution_start_to_close_timeout.unwrap().seconds, 30);
        assert_eq!(out.input.clone().unwrap().data, b"in");
        assert_eq!(
            out.workflow_id_reuse_policy,
            e::WorkflowIDReusePolicy::RejectDuplicate as i32 + 1
        );
        // Round trip through the inverse conversion.
        let back = cv::start(out).unwrap();
        assert_eq!(back.workflow_id_reuse_policy, req.workflow_id_reuse_policy);
    }

    #[test]
    fn plain_codes_map_without_details() {
        assert!(matches!(
            map_status(&Status::invalid_argument("x")),
            ForwardError::BadRequest(_)
        ));
        assert!(matches!(
            map_status(&Status::not_found("x")),
            ForwardError::NotFound(_)
        ));
        assert!(matches!(
            map_status(&Status::resource_exhausted("x")),
            ForwardError::Busy(_)
        ));
        assert!(matches!(
            map_status(&Status::unavailable("x")),
            ForwardError::Internal(_)
        ));
    }
}
