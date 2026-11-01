//! Frontend -> Go History forwarding over the generated protobuf History API.
//!
//! Go's Frontend never mutates executions itself: each workflow API resolves
//! the domain UUID and forwards to the History host that owns the workflow ID's
//! shard. This module holds the typed request mapping and the History error
//! mapping shared by the Thrift/TChannel and JSON/gRPC bridges. Shard owner
//! lookup is the `HistoryPeers` seam: the launcher installs a static peer today
//! and the ringpop-backed resolver when Frontend joins membership.
use cadence_api::{
    enums as e, errors::WorkflowExecutionAlreadyStartedError, service_types as st, tasks as tk,
    types as t,
};
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

fn en<T>(value: i32, from: fn(i32) -> Option<T>) -> Option<T> {
    if value <= 0 {
        None
    } else {
        from(value - 1)
    }
}

fn secs(d: Option<prost_types::Duration>) -> Option<i32> {
    d.map(|d| d.seconds as i32)
}

fn payload_map(
    fields: std::collections::HashMap<String, p::Payload>,
) -> std::collections::HashMap<String, Vec<u8>> {
    fields.into_iter().map(|(k, v)| (k, v.data)).collect()
}

fn info_in(v: p::WorkflowExecutionInfo) -> st::WorkflowExecutionInfo {
    let parent = v.parent_execution_info;
    st::WorkflowExecutionInfo {
        execution: v.workflow_execution.map(cv::execution),
        workflow_type: v.r#type.map(|x| t::WorkflowType { name: x.name }),
        start_time: millis_nanos(v.start_time),
        close_time: millis_nanos(v.close_time),
        close_status: en(v.close_status, e::WorkflowExecutionCloseStatus::from_i32),
        history_length: v.history_length,
        memo: v.memo.map(|m| t::Memo {
            fields: Some(payload_map(m.fields)),
        }),
        parent_domain: parent.as_ref().map(|x| x.domain_name.clone()),
        parent_execution: parent
            .as_ref()
            .and_then(|x| x.workflow_execution.clone())
            .map(cv::execution),
        parent_initiated_id: parent.as_ref().map(|x| x.initiated_id),
        execution_time: millis_nanos(v.execution_time),
        search_attributes: v.search_attributes.map(|m| t::SearchAttributes {
            indexed_fields: Some(
                m.indexed_fields
                    .into_iter()
                    .map(|(k, v)| (k, v.data))
                    .collect(),
            ),
        }),
        task_list: v
            .task_list_info
            .and_then(|x| cv::task_list(x).ok())
            .or_else(|| {
                (!v.task_list.is_empty()).then(|| t::TaskList {
                    name: v.task_list.clone(),
                    ..Default::default()
                })
            }),
        is_cron: v.is_cron,
        cron_schedule: (!v.cron_schedule.is_empty()).then_some(v.cron_schedule),
        cron_overlap_policy: en(v.cron_overlap_policy, e::CronOverlapPolicy::from_i32),
        update_time: millis_nanos(v.update_time),
        partition_config: (!v.partition_config.is_empty()).then_some(v.partition_config),
        execution_status: en(v.execution_status, e::WorkflowExecutionStatus::from_i32),
        scheduled_execution_time: millis_nanos(v.scheduled_execution_time),
    }
}

/// `start_time`/`close_time` etc. are nanoseconds in the Thrift model
/// (Go `UnixNano`), carried in `api.v1` as a Timestamp.
fn millis_nanos(ts: Option<prost_types::Timestamp>) -> Option<i64> {
    ts.map(|t| t.seconds * 1_000_000_000 + i64::from(t.nanos))
}

/// `api.v1` describe response -> Thrift (inverse of `cv::describe`).
pub fn describe_in(v: p::DescribeWorkflowExecutionResponse) -> st::DescribeWorkflowExecutionResponse {
    st::DescribeWorkflowExecutionResponse {
        execution_configuration: v.execution_configuration.map(|c| {
            st::WorkflowExecutionConfiguration {
                task_list: c.task_list.and_then(|x| cv::task_list(x).ok()),
                execution_start_to_close_timeout_seconds: secs(c.execution_start_to_close_timeout),
                task_start_to_close_timeout_seconds: secs(c.task_start_to_close_timeout),
            }
        }),
        workflow_execution_info: v.workflow_execution_info.map(info_in),
        pending_activities: v
            .pending_activities
            .into_iter()
            .map(|a| tk::PendingActivityInfo {
                activity_id: a.activity_id,
                activity_type: a.activity_type.map(|x| t::ActivityType { name: x.name }),
                state: en(a.state, e::PendingActivityState::from_i32),
                attempt: a.attempt,
                scheduled_timestamp: millis_nanos(a.scheduled_time),
                last_started_timestamp: millis_nanos(a.last_started_time),
                heartbeat_details: cv::data(a.heartbeat_details),
                maximum_attempts: a.maximum_attempts,
                schedule_id: a.schedule_id,
                last_failure_reason: a.last_failure.map(|f| f.reason),
            })
            .collect(),
        pending_children: v
            .pending_children
            .into_iter()
            .map(|c| {
                let execution = c.workflow_execution.unwrap_or_default();
                tk::PendingChildExecutionInfo {
                    domain: c.domain,
                    workflow_id: execution.workflow_id,
                    run_id: execution.run_id,
                    workflow_type_name: c.workflow_type_name,
                    initiated_id: c.initiated_id,
                    parent_close_policy: en(c.parent_close_policy, e::ParentClosePolicy::from_i32),
                }
            })
            .collect(),
        pending_decision: v.pending_decision.map(|d| tk::PendingDecisionInfo {
            state: en(d.state, e::PendingDecisionState::from_i32),
            scheduled_timestamp: millis_nanos(d.scheduled_time),
            started_timestamp: millis_nanos(d.started_time),
            attempt: i64::from(d.attempt),
            schedule_id: d.schedule_id,
            original_scheduled_timestamp: millis_nanos(d.original_scheduled_time),
        }),
    }
}

impl HistoryForward {
    pub async fn describe(
        &self,
        domain_id: &str,
        req: &st::DescribeWorkflowExecutionRequest,
    ) -> Result<st::DescribeWorkflowExecutionResponse, ForwardError> {
        let execution = req.execution.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        let response = client
            .describe_workflow_execution(Self::request(h::DescribeWorkflowExecutionRequest {
                request: Some(p::DescribeWorkflowExecutionRequest {
                    domain: req.domain.clone(),
                    workflow_execution: Some(cv::execution_out(execution)),
                    query_consistency_level: pb_enum(
                        req.query_consistency_level.map(|x| x as i32),
                    ),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(describe_in(h_describe(response)))
    }

    pub async fn query(
        &self,
        domain_id: &str,
        req: &tk::QueryWorkflowRequest,
    ) -> Result<tk::QueryWorkflowResponse, ForwardError> {
        let execution = req.execution.clone().unwrap_or_default();
        let query = req.query.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        let response = client
            .query_workflow(Self::request(h::QueryWorkflowRequest {
                request: Some(p::QueryWorkflowRequest {
                    domain: req.domain.clone(),
                    workflow_execution: Some(cv::execution_out(execution)),
                    query: Some(p::WorkflowQuery {
                        query_type: query.query_type,
                        query_args: cv::payload(query.query_args),
                    }),
                    query_reject_condition: pb_enum(req.query_reject_condition.map(|x| x as i32)),
                    query_consistency_level: pb_enum(
                        req.query_consistency_level.map(|x| x as i32),
                    ),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(tk::QueryWorkflowResponse {
            query_result: cv::data(response.query_result),
            query_rejected: response.query_rejected.map(|r| cadence_api::errors::QueryRejected {
                close_status: en(r.close_status, e::WorkflowExecutionCloseStatus::from_i32),
            }),
        })
    }
}

/// Result of `RespondDecisionTaskCompleted` at History.
#[derive(Debug, Clone, Default)]
pub struct DecisionCompleted {
    pub started: Option<h::RecordDecisionTaskStartedResponse>,
}

pub fn failure_out(reason: Option<String>, details: Vec<u8>, options: Option<t::FailureOptions>) -> Option<p::Failure> {
    cv::failure(reason, details, options)
}

pub fn respond_decision_completed_out(
    req: &tk::RespondDecisionTaskCompletedRequest,
) -> Result<p::RespondDecisionTaskCompletedRequest, ForwardError> {
    let mut decisions = Vec::with_capacity(req.decisions.len());
    for decision in req.decisions.clone() {
        decisions.push(
            cv::decision_out(decision)
                .map_err(|s| ForwardError::BadRequest(s.message().to_owned()))?,
        );
    }
    Ok(p::RespondDecisionTaskCompletedRequest {
        task_token: req.task_token.clone(),
        decisions,
        execution_context: req.execution_context.clone(),
        identity: req.identity.clone(),
        sticky_attributes: req.sticky_attributes.clone().map(|s| p::StickyExecutionAttributes {
            worker_task_list: s.worker_task_list.map(cv::task_list_out),
            schedule_to_start_timeout: dur(s.schedule_to_start_timeout_seconds),
        }),
        return_new_decision_task: req.return_new_decision_task,
        force_create_new_decision_task: req.force_create_new_decision_task,
        binary_checksum: req.binary_checksum.clone(),
        query_results: req
            .query_results
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, r)| {
                (
                    k,
                    p::WorkflowQueryResult {
                        result_type: pb_enum(r.result_type.map(|x| x as i32)),
                        answer: cv::payload(r.answer),
                        error_message: r.error_message,
                    },
                )
            })
            .collect(),
    })
}

impl HistoryForward {
    pub async fn respond_decision_completed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RespondDecisionTaskCompletedRequest,
    ) -> Result<DecisionCompleted, ForwardError> {
        let request = respond_decision_completed_out(req)?;
        let mut client = self.client(workflow_id)?;
        let response = client
            .respond_decision_task_completed(Self::request(h::RespondDecisionTaskCompletedRequest {
                request: Some(request),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(DecisionCompleted {
            started: response.started_response,
        })
    }

    pub async fn respond_decision_failed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RespondDecisionTaskFailedRequest,
    ) -> Result<(), ForwardError> {
        let mut client = self.client(workflow_id)?;
        client
            .respond_decision_task_failed(Self::request(h::RespondDecisionTaskFailedRequest {
                request: Some(p::RespondDecisionTaskFailedRequest {
                    task_token: req.task_token.clone(),
                    cause: pb_enum(req.cause.map(|x| x as i32)),
                    details: cv::payload(req.details.clone()),
                    identity: req.identity.clone(),
                    binary_checksum: req.binary_checksum.clone(),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn respond_activity_completed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RespondActivityTaskCompletedRequest,
    ) -> Result<(), ForwardError> {
        let mut client = self.client(workflow_id)?;
        client
            .respond_activity_task_completed(Self::request(h::RespondActivityTaskCompletedRequest {
                request: Some(p::RespondActivityTaskCompletedRequest {
                    task_token: req.task_token.clone(),
                    result: cv::payload(req.result.clone()),
                    identity: req.identity.clone(),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn respond_activity_failed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RespondActivityTaskFailedRequest,
    ) -> Result<(), ForwardError> {
        let mut client = self.client(workflow_id)?;
        client
            .respond_activity_task_failed(Self::request(h::RespondActivityTaskFailedRequest {
                request: Some(p::RespondActivityTaskFailedRequest {
                    task_token: req.task_token.clone(),
                    failure: failure_out(
                        req.reason.clone(),
                        req.details.clone(),
                        req.failure_options.clone(),
                    ),
                    identity: req.identity.clone(),
                    heartbeat_details: cv::payload(req.heartbeat_details.clone()),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn respond_activity_canceled(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RespondActivityTaskCanceledRequest,
    ) -> Result<(), ForwardError> {
        let mut client = self.client(workflow_id)?;
        client
            .respond_activity_task_canceled(Self::request(h::RespondActivityTaskCanceledRequest {
                request: Some(p::RespondActivityTaskCanceledRequest {
                    task_token: req.task_token.clone(),
                    details: cv::payload(req.details.clone()),
                    identity: req.identity.clone(),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    pub async fn record_heartbeat(
        &self,
        domain_id: &str,
        workflow_id: &str,
        req: &tk::RecordActivityTaskHeartbeatRequest,
    ) -> Result<bool, ForwardError> {
        let mut client = self.client(workflow_id)?;
        let response = client
            .record_activity_task_heartbeat(Self::request(h::RecordActivityTaskHeartbeatRequest {
                request: Some(p::RecordActivityTaskHeartbeatRequest {
                    task_token: req.task_token.clone(),
                    details: cv::payload(req.details.clone()),
                    identity: req.identity.clone(),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(response.cancel_requested)
    }

    pub async fn reset_sticky_task_list(
        &self,
        domain_id: &str,
        req: &st::ResetStickyTaskListRequest,
    ) -> Result<(), ForwardError> {
        let execution = req.execution.clone().unwrap_or_default();
        let mut client = self.client(&execution.workflow_id)?;
        client
            .reset_sticky_task_list(Self::request(h::ResetStickyTaskListRequest {
                request: Some(p::ResetStickyTaskListRequest {
                    domain: req.domain.clone(),
                    workflow_execution: Some(cv::execution_out(execution)),
                }),
                domain_id: domain_id.to_owned(),
            }))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }
}

/// History wraps the public describe response field-for-field.
fn h_describe(v: h::DescribeWorkflowExecutionResponse) -> p::DescribeWorkflowExecutionResponse {
    p::DescribeWorkflowExecutionResponse {
        execution_configuration: v.execution_configuration,
        workflow_execution_info: v.workflow_execution_info,
        pending_activities: v.pending_activities,
        pending_children: v.pending_children,
        pending_decision: v.pending_decision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
