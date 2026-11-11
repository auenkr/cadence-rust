//! gRPC handlers for the group-2 History RPCs (proto <-> engine types).
use super::{errors, ServerState};
use crate::engine::deps::HistoryError;
use crate::engine::types as et;
use cadence_api::{enums, tasks, types as t};
use cadence_proto::convert as cv;
use cadence_proto::history as pb;
use cadence_proto::{admin, api, shared};
use tonic::Status;

fn gate(state: &ServerState) -> Result<(), Status> {
    if state.limiter.allow() {
        Ok(())
    } else {
        Err(errors::grpc_status(&errors::busy()))
    }
}

fn err(e: HistoryError) -> Status {
    errors::grpc_status(&e)
}

fn exec_in(v: Option<api::WorkflowExecution>) -> Option<t::WorkflowExecution> {
    v.map(cv::execution)
}
fn exec_out(v: Option<t::WorkflowExecution>) -> Option<api::WorkflowExecution> {
    v.map(cv::execution_out)
}
fn wf_type_out(v: Option<t::WorkflowType>) -> Option<api::WorkflowType> {
    v.map(|x| api::WorkflowType { name: x.name })
}
fn tl_out(v: Option<t::TaskList>) -> Option<api::TaskList> {
    v.map(cv::task_list_out)
}
fn event_out(v: Option<cadence_api::service_types::HistoryEvent>) -> Result<Option<api::HistoryEvent>, Status> {
    v.map(cv::history::event).transpose()
}
fn event_in(v: Option<api::HistoryEvent>) -> Result<Option<cadence_api::service_types::HistoryEvent>, Status> {
    v.map(cv::history_in::event).transpose()
}
fn workflow_state_out(v: Option<i32>) -> i32 {
    match v {
        Some(s) if (0..=5).contains(&s) => s + 1,
        _ => 0,
    }
}
fn close_state_out(v: Option<i32>) -> i32 {
    match v {
        Some(s) if (1..=6).contains(&s) => s,
        _ => 0,
    }
}
fn query_out(q: tasks::WorkflowQuery) -> api::WorkflowQuery {
    api::WorkflowQuery { query_type: q.query_type, query_args: cv::payload(q.query_args) }
}

fn get_resp_out(r: et::GetMutableStateResponse) -> pb::GetMutableStateResponse {
    pb::GetMutableStateResponse {
        workflow_execution: exec_out(r.execution),
        workflow_type: wf_type_out(r.workflow_type),
        next_event_id: r.next_event_id,
        previous_started_event_id: r.previous_started_event_id,
        last_first_event_id: r.last_first_event_id,
        task_list: tl_out(r.task_list),
        sticky_task_list: tl_out(r.sticky_task_list),
        client_library_version: r.client_library_version,
        client_feature_version: r.client_feature_version,
        client_impl: r.client_impl,
        sticky_task_list_schedule_to_start_timeout: r
            .sticky_task_list_schedule_to_start_timeout
            .map(|s| cv::duration(i64::from(s))),
        event_store_version: r.event_store_version,
        current_branch_token: r.current_branch_token,
        workflow_state: workflow_state_out(r.workflow_state),
        workflow_close_state: close_state_out(r.workflow_close_state),
        version_histories: r.version_histories.map(cv::version_histories_out),
        is_sticky_task_list_enabled: r.is_sticky_task_list_enabled,
        history_size: r.history_size,
    }
}

pub async fn get_mutable_state(
    state: &ServerState,
    req: pb::GetMutableStateRequest,
) -> Result<pb::GetMutableStateResponse, Status> {
    gate(state)?;
    let r = et::GetMutableStateRequest {
        domain_uuid: req.domain_id,
        execution: exec_in(req.workflow_execution),
        expected_next_event_id: req.expected_next_event_id,
        current_branch_token: req.current_branch_token,
        version_history_item: req
            .version_history_item
            .map(|i: admin::VersionHistoryItem| t::VersionHistoryItem { event_id: i.event_id, version: i.version }),
    };
    let resp = state.handler.get_mutable_state(Some(&r)).await.map_err(err)?;
    Ok(get_resp_out(resp))
}

pub async fn poll_mutable_state(
    state: &ServerState,
    req: pb::PollMutableStateRequest,
) -> Result<pb::PollMutableStateResponse, Status> {
    gate(state)?;
    let r = et::PollMutableStateRequest {
        domain_uuid: req.domain_id,
        execution: exec_in(req.workflow_execution),
        expected_next_event_id: req.expected_next_event_id,
        current_branch_token: req.current_branch_token,
        version_history_item: None,
    };
    let r = state.handler.poll_mutable_state(Some(&r)).await.map_err(err)?;
    Ok(pb::PollMutableStateResponse {
        workflow_execution: exec_out(r.execution),
        workflow_type: wf_type_out(r.workflow_type),
        next_event_id: r.next_event_id,
        previous_started_event_id: r.previous_started_event_id,
        last_first_event_id: r.last_first_event_id,
        task_list: tl_out(r.task_list),
        sticky_task_list: tl_out(r.sticky_task_list),
        client_library_version: r.client_library_version,
        client_feature_version: r.client_feature_version,
        client_impl: r.client_impl,
        sticky_task_list_schedule_to_start_timeout: r
            .sticky_task_list_schedule_to_start_timeout
            .map(|s| cv::duration(i64::from(s))),
        current_branch_token: r.current_branch_token,
        workflow_state: workflow_state_out(r.workflow_state),
        workflow_close_state: close_state_out(r.workflow_close_state),
        version_histories: r.version_histories.map(cv::version_histories_out),
    })
}

pub async fn describe_mutable_state(
    state: &ServerState,
    req: pb::DescribeMutableStateRequest,
) -> Result<pb::DescribeMutableStateResponse, Status> {
    gate(state)?;
    let r = et::DescribeMutableStateRequest {
        domain_uuid: req.domain_id,
        execution: exec_in(req.workflow_execution),
    };
    let r = state.handler.describe_mutable_state(Some(&r)).await.map_err(err)?;
    Ok(pb::DescribeMutableStateResponse {
        mutable_state_in_cache: r.mutable_state_in_cache,
        mutable_state_in_database: r.mutable_state_in_database,
    })
}

fn decision_started_out(r: et::RecordDecisionTaskStartedResponse) -> Result<pb::RecordDecisionTaskStartedResponse, Status> {
    let decision_info = match r.decision_info {
        Some(d) => Some(shared::TransientDecisionInfo {
            scheduled_event: event_out(d.scheduled_event)?,
            started_event: event_out(d.started_event)?,
        }),
        None => None,
    };
    Ok(pb::RecordDecisionTaskStartedResponse {
        workflow_type: wf_type_out(r.workflow_type),
        previous_started_event_id: r.previous_started_event_id,
        scheduled_event_id: r.scheduled_event_id,
        started_event_id: r.started_event_id,
        next_event_id: r.next_event_id,
        attempt: r.attempt as i32,
        sticky_execution_enabled: r.sticky_execution_enabled,
        decision_info,
        workflow_execution_task_list: tl_out(r.workflow_execution_task_list),
        event_store_version: r.event_store_version,
        branch_token: r.branch_token,
        scheduled_time: r.scheduled_timestamp.map(cv::timestamp_from_nanos),
        started_time: r.started_timestamp.map(cv::timestamp_from_nanos),
        queries: r.queries.into_iter().map(|(k, q)| (k, query_out(q))).collect(),
        history_size: r.history_size,
    })
}

pub async fn record_decision_task_started(
    state: &ServerState,
    req: pb::RecordDecisionTaskStartedRequest,
) -> Result<pb::RecordDecisionTaskStartedResponse, Status> {
    gate(state)?;
    let r = et::RecordDecisionTaskStartedRequest {
        domain_uuid: req.domain_id,
        workflow_execution: exec_in(req.workflow_execution),
        schedule_id: req.schedule_id,
        task_id: req.task_id,
        request_id: req.request_id,
        poll_request: req.poll_request.map(cv::poll_for_decision_task_request_in).transpose()?,
    };
    let resp = state.handler.record_decision_task_started(Some(&r)).await.map_err(err)?;
    decision_started_out(resp)
}

pub async fn record_activity_task_started(
    state: &ServerState,
    req: pb::RecordActivityTaskStartedRequest,
) -> Result<pb::RecordActivityTaskStartedResponse, Status> {
    gate(state)?;
    let r = et::RecordActivityTaskStartedRequest {
        domain_uuid: req.domain_id,
        workflow_execution: exec_in(req.workflow_execution),
        schedule_id: req.schedule_id,
        task_id: req.task_id,
        request_id: req.request_id,
        poll_request: req.poll_request.map(cv::poll_for_activity_task_request_in).transpose()?,
    };
    let r = state.handler.record_activity_task_started(Some(&r)).await.map_err(|e| {
        tracing::warn!(error = %e, "RecordActivityTaskStarted failed");
        err(e)
    })?;
    Ok(pb::RecordActivityTaskStartedResponse {
        scheduled_event: event_out(r.scheduled_event)?,
        started_time: r.started_timestamp.map(cv::timestamp_from_nanos),
        attempt: r.attempt as i32,
        scheduled_time_of_this_attempt: r.scheduled_timestamp_of_this_attempt.map(cv::timestamp_from_nanos),
        heartbeat_details: cv::payload(r.heartbeat_details),
        workflow_type: wf_type_out(r.workflow_type),
        workflow_domain: r.workflow_domain,
    })
}

pub async fn respond_decision_task_completed(
    state: &ServerState,
    req: pb::RespondDecisionTaskCompletedRequest,
) -> Result<pb::RespondDecisionTaskCompletedResponse, Status> {
    gate(state)?;
    let complete_request = match req.request {
        Some(r) => Some(tasks::RespondDecisionTaskCompletedRequest {
            task_token: r.task_token,
            decisions: r
                .decisions
                .into_iter()
                .map(cv::decisions::decision)
                .collect::<Result<_, _>>()?,
            execution_context: r.execution_context,
            identity: r.identity,
            binary_checksum: r.binary_checksum,
            sticky_attributes: r
                .sticky_attributes
                .map(|s| {
                    Ok::<_, Status>(t::StickyExecutionAttributes {
                        worker_task_list: s.worker_task_list.map(cv::task_list).transpose()?,
                        schedule_to_start_timeout_seconds: cv::seconds(s.schedule_to_start_timeout)?,
                    })
                })
                .transpose()?,
            return_new_decision_task: r.return_new_decision_task,
            force_create_new_decision_task: r.force_create_new_decision_task,
            query_results: Some(
                r.query_results
                    .into_iter()
                    .map(|(k, v)| {
                        Ok::<_, Status>((
                            k,
                            tasks::WorkflowQueryResult {
                                result_type: cv::enum_in(v.result_type, enums::QueryResultType::from_i32)?,
                                answer: cv::data(v.answer),
                                error_message: v.error_message,
                            },
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            ),
        }),
        None => None,
    };
    let r = et::HistoryRespondDecisionTaskCompletedRequest { domain_uuid: req.domain_id, complete_request };
    let resp = state.handler.respond_decision_task_completed(Some(&r)).await.map_err(err)?;
    let started_response = match resp.started_response {
        Some(s) => Some(decision_started_out(s)?),
        None => None,
    };
    Ok(pb::RespondDecisionTaskCompletedResponse {
        started_response,
        activities_to_dispatch_locally: resp
            .activities_to_dispatch_locally
            .into_iter()
            .map(|(k, a)| {
                (
                    k,
                    api::ActivityLocalDispatchInfo {
                        activity_id: a.activity_id,
                        scheduled_time: a.scheduled_timestamp.map(cv::timestamp_from_nanos),
                        started_time: a.started_timestamp.map(cv::timestamp_from_nanos),
                        scheduled_time_of_this_attempt: a
                            .scheduled_timestamp_of_this_attempt
                            .map(cv::timestamp_from_nanos),
                        task_token: a.task_token,
                    },
                )
            })
            .collect(),
    })
}

pub async fn respond_decision_task_failed(
    state: &ServerState,
    req: pb::RespondDecisionTaskFailedRequest,
) -> Result<pb::RespondDecisionTaskFailedResponse, Status> {
    gate(state)?;
    let failed_request = match req.request {
        Some(r) => Some(tasks::RespondDecisionTaskFailedRequest {
            task_token: r.task_token,
            cause: cv::enum_in(r.cause, enums::DecisionTaskFailedCause::from_i32)?,
            details: cv::data(r.details),
            identity: r.identity,
            binary_checksum: r.binary_checksum,
        }),
        None => None,
    };
    let r = et::HistoryRespondDecisionTaskFailedRequest { domain_uuid: req.domain_id, failed_request };
    state.handler.respond_decision_task_failed(Some(&r)).await.map_err(err)?;
    Ok(pb::RespondDecisionTaskFailedResponse {})
}

pub async fn respond_activity_task_completed(
    state: &ServerState,
    req: pb::RespondActivityTaskCompletedRequest,
) -> Result<pb::RespondActivityTaskCompletedResponse, Status> {
    gate(state)?;
    let complete_request = req.request.map(|r| tasks::RespondActivityTaskCompletedRequest {
        task_token: r.task_token,
        result: cv::data(r.result),
        identity: r.identity,
    });
    let r = et::HistoryRespondActivityTaskCompletedRequest { domain_uuid: req.domain_id, complete_request };
    state.handler.respond_activity_task_completed(Some(&r)).await.map_err(err)?;
    Ok(pb::RespondActivityTaskCompletedResponse {})
}

pub async fn respond_activity_task_failed(
    state: &ServerState,
    req: pb::RespondActivityTaskFailedRequest,
) -> Result<pb::RespondActivityTaskFailedResponse, Status> {
    gate(state)?;
    let failed_request = match req.request {
        Some(r) => {
            let (reason, details, failure_options) = cv::failure_in(r.failure)?;
            Some(tasks::RespondActivityTaskFailedRequest {
                task_token: r.task_token,
                reason,
                details,
                failure_options,
                identity: r.identity,
                heartbeat_details: cv::data(r.heartbeat_details),
            })
        }
        None => None,
    };
    let r = et::HistoryRespondActivityTaskFailedRequest { domain_uuid: req.domain_id, failed_request };
    state.handler.respond_activity_task_failed(Some(&r)).await.map_err(err)?;
    Ok(pb::RespondActivityTaskFailedResponse {})
}

pub async fn respond_activity_task_canceled(
    state: &ServerState,
    req: pb::RespondActivityTaskCanceledRequest,
) -> Result<pb::RespondActivityTaskCanceledResponse, Status> {
    gate(state)?;
    let cancel_request = req.request.map(|r| tasks::RespondActivityTaskCanceledRequest {
        task_token: r.task_token,
        details: cv::data(r.details),
        identity: r.identity,
    });
    let r = et::HistoryRespondActivityTaskCanceledRequest { domain_uuid: req.domain_id, cancel_request };
    state.handler.respond_activity_task_canceled(Some(&r)).await.map_err(err)?;
    Ok(pb::RespondActivityTaskCanceledResponse {})
}

pub async fn record_activity_task_heartbeat(
    state: &ServerState,
    req: pb::RecordActivityTaskHeartbeatRequest,
) -> Result<pb::RecordActivityTaskHeartbeatResponse, Status> {
    gate(state)?;
    let heartbeat_request = req.request.map(|r| tasks::RecordActivityTaskHeartbeatRequest {
        task_token: r.task_token,
        details: cv::data(r.details),
        identity: r.identity,
    });
    let r = et::HistoryRecordActivityTaskHeartbeatRequest { domain_uuid: req.domain_id, heartbeat_request };
    let resp = state.handler.record_activity_task_heartbeat(Some(&r)).await.map_err(err)?;
    Ok(pb::RecordActivityTaskHeartbeatResponse { cancel_requested: resp.cancel_requested })
}

pub async fn record_child_execution_completed(
    state: &ServerState,
    req: pb::RecordChildExecutionCompletedRequest,
) -> Result<pb::RecordChildExecutionCompletedResponse, Status> {
    gate(state)?;
    let r = et::RecordChildExecutionCompletedRequest {
        domain_uuid: req.domain_id,
        workflow_execution: exec_in(req.workflow_execution),
        initiated_id: req.initiated_id,
        completed_execution: exec_in(req.completed_execution),
        completion_event: event_in(req.completion_event)?,
        started_id: req.started_id,
    };
    state.handler.record_child_execution_completed(Some(&r)).await.map_err(err)?;
    Ok(pb::RecordChildExecutionCompletedResponse {})
}

pub async fn remove_signal_mutable_state(
    state: &ServerState,
    req: pb::RemoveSignalMutableStateRequest,
) -> Result<pb::RemoveSignalMutableStateResponse, Status> {
    gate(state)?;
    let r = et::RemoveSignalMutableStateRequest {
        domain_uuid: req.domain_id,
        workflow_execution: exec_in(req.workflow_execution),
        request_id: req.request_id,
    };
    state.handler.remove_signal_mutable_state(Some(&r)).await.map_err(err)?;
    Ok(pb::RemoveSignalMutableStateResponse {})
}
