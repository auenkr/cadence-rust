//! gRPC handlers for the group-1 History RPCs. Each function applies the
//! rate-limit gate, converts proto -> engine types, calls the handler and maps
//! the result/error back. The orchestrator wires them into `GrpcHistory`.
//!
use super::{errors, ServerState};
use cadence_api::{enums as e, service_types as st, tasks as tk, types as t};
use cadence_proto::api as papi;
use cadence_proto::convert as c;
use cadence_proto::history as pb;
use tonic::Status;

use crate::engine::types as et;

fn gate(state: &ServerState) -> Result<(), Status> {
    if state.limiter.allow() {
        Ok(())
    } else {
        Err(errors::grpc_status(&errors::busy()))
    }
}

fn st_err(e: &crate::engine::deps::HistoryError) -> Status {
    errors::grpc_status(e)
}

// ---------------------------------------------------------------------------
// proto -> engine/api type converters
// ---------------------------------------------------------------------------

pub fn signal_in(v: papi::SignalWorkflowExecutionRequest) -> t::SignalWorkflowExecutionRequest {
    t::SignalWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(c::execution),
        signal_name: v.signal_name,
        input: c::data(v.signal_input),
        identity: v.identity,
        request_id: v.request_id,
        control: v.control,
    }
}

pub fn terminate_in(v: papi::TerminateWorkflowExecutionRequest) -> t::TerminateWorkflowExecutionRequest {
    t::TerminateWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(c::execution),
        reason: v.reason,
        details: c::data(v.details),
        identity: v.identity,
        first_execution_run_id: v.first_execution_run_id,
    }
}

pub fn cancel_in(v: papi::RequestCancelWorkflowExecutionRequest) -> st::RequestCancelWorkflowExecutionRequest {
    st::RequestCancelWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(c::execution),
        identity: v.identity,
        request_id: v.request_id,
        cause: v.cause,
        first_execution_run_id: v.first_execution_run_id,
    }
}

pub fn signal_with_start_in(v: papi::SignalWithStartWorkflowExecutionRequest) -> Result<st::SignalWithStartWorkflowExecutionRequest, Status> {
    let s = c::start(v.start_request.unwrap_or_default())?;
    Ok(st::SignalWithStartWorkflowExecutionRequest {
        domain: s.domain,
        workflow_id: s.workflow_id,
        workflow_type: s.workflow_type,
        task_list: s.task_list,
        input: s.input,
        execution_start_to_close_timeout_seconds: s.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: s.task_start_to_close_timeout_seconds,
        identity: s.identity,
        request_id: s.request_id,
        workflow_id_reuse_policy: s.workflow_id_reuse_policy,
        retry_policy: s.retry_policy,
        control: v.control,
        signal_name: v.signal_name,
        signal_input: c::data(v.signal_input),
        cron_schedule: s.cron_schedule,
        memo: s.memo,
        search_attributes: s.search_attributes,
        header: s.header,
        delay_start_seconds: s.delay_start_seconds,
        jitter_start_seconds: s.jitter_start_seconds,
        first_run_at_timestamp: s.first_run_at_timestamp,
        cron_overlap_policy: s.cron_overlap_policy,
        active_cluster_selection_policy: s.active_cluster_selection_policy,
    })
}

pub fn query_in(v: papi::QueryWorkflowRequest) -> Result<tk::QueryWorkflowRequest, Status> {
    Ok(tk::QueryWorkflowRequest {
        domain: v.domain,
        execution: v.workflow_execution.map(c::execution),
        query: v.query.map(|q| tk::WorkflowQuery {
            query_type: q.query_type,
            query_args: c::data(q.query_args),
        }),
        query_reject_condition: c::enum_in(v.query_reject_condition, e::QueryRejectCondition::from_i32)?,
        query_consistency_level: c::enum_in(v.query_consistency_level, e::QueryConsistencyLevel::from_i32)?,
    })
}

pub fn describe_in(v: papi::DescribeWorkflowExecutionRequest) -> Result<st::DescribeWorkflowExecutionRequest, Status> {
    Ok(st::DescribeWorkflowExecutionRequest {
        domain: v.domain,
        execution: v.workflow_execution.map(c::execution),
        query_consistency_level: c::enum_in(v.query_consistency_level, e::QueryConsistencyLevel::from_i32)?,
    })
}

pub fn query_response_out(v: tk::QueryWorkflowResponse) -> pb::QueryWorkflowResponse {
    pb::QueryWorkflowResponse {
        query_result: c::payload(v.query_result),
        query_rejected: v.query_rejected.map(|q| papi::QueryRejected {
            close_status: q.close_status.map_or(0, |s| s as i32 + 1),
        }),
    }
}

// ---------------------------------------------------------------------------
// RPCs
// ---------------------------------------------------------------------------

pub async fn start_workflow_execution(state: &ServerState, req: pb::StartWorkflowExecutionRequest) -> Result<pb::StartWorkflowExecutionResponse, Status> {
    gate(state)?;
    let (reason, details, _options) = c::failure_in(req.continued_failure)?;
    let r = et::HistoryStartWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        start_request: req.request.map(c::start).transpose()?,
        parent_execution_info: req.parent_execution_info.map(|p| et::ParentExecutionInfo {
            domain_uuid: p.domain_id,
            domain: p.domain_name,
            execution: p.workflow_execution.map(c::execution),
            initiated_id: p.initiated_id,
        }),
        attempt: req.attempt,
        expiration_timestamp: c::nanos(req.expiration_time),
        continue_as_new_initiator: c::enum_in(req.continue_as_new_initiator, e::ContinueAsNewInitiator::from_i32)?,
        continued_failure_reason: reason,
        continued_failure_details: details,
        last_completion_result: c::data(req.last_completion_result),
        first_decision_task_backoff_seconds: c::seconds(req.first_decision_task_backoff)?,
        partition_config: req.partition_config,
    };
    let resp = state.handler.start_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::StartWorkflowExecutionResponse { run_id: resp.run_id })
}

pub async fn signal_with_start_workflow_execution(state: &ServerState, req: pb::SignalWithStartWorkflowExecutionRequest) -> Result<pb::SignalWithStartWorkflowExecutionResponse, Status> {
    gate(state)?;
    let r = et::HistorySignalWithStartWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        signal_with_start_request: req.request.map(signal_with_start_in).transpose()?,
        partition_config: req.partition_config,
    };
    let resp = state.handler.signal_with_start_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::SignalWithStartWorkflowExecutionResponse { run_id: resp.run_id })
}

pub async fn signal_workflow_execution(state: &ServerState, req: pb::SignalWorkflowExecutionRequest) -> Result<pb::SignalWorkflowExecutionResponse, Status> {
    gate(state)?;
    let r = et::HistorySignalWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        signal_request: req.request.map(signal_in),
        external_workflow_execution: req.external_workflow_execution.map(c::execution),
        child_workflow_only: req.child_workflow_only,
    };
    state.handler.signal_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::SignalWorkflowExecutionResponse {})
}

pub async fn terminate_workflow_execution(state: &ServerState, req: pb::TerminateWorkflowExecutionRequest) -> Result<pb::TerminateWorkflowExecutionResponse, Status> {
    gate(state)?;
    let r = et::HistoryTerminateWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        terminate_request: req.request.map(terminate_in),
        external_workflow_execution: req.external_workflow_execution.map(c::execution),
        child_workflow_only: req.child_workflow_only,
    };
    state.handler.terminate_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::TerminateWorkflowExecutionResponse {})
}

pub async fn request_cancel_workflow_execution(state: &ServerState, req: pb::RequestCancelWorkflowExecutionRequest) -> Result<pb::RequestCancelWorkflowExecutionResponse, Status> {
    gate(state)?;
    let (ext_exec, ext_id) = match req.external_execution_info {
        Some(x) => (x.workflow_execution.map(c::execution), Some(x.initiated_id)),
        None => (None, None),
    };
    let r = et::HistoryRequestCancelWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        cancel_request: req.cancel_request.map(cancel_in),
        external_initiated_event_id: ext_id,
        external_workflow_execution: ext_exec,
        child_workflow_only: req.child_workflow_only,
    };
    state.handler.request_cancel_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::RequestCancelWorkflowExecutionResponse {})
}

pub fn reset_request_in(v: papi::ResetWorkflowExecutionRequest) -> t::ResetWorkflowExecutionRequest {
    t::ResetWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(c::execution),
        reason: v.reason,
        decision_finish_event_id: v.decision_finish_event_id,
        request_id: v.request_id,
        skip_signal_reapply: v.skip_signal_reapply,
    }
}

pub async fn reset_workflow_execution(state: &ServerState, req: pb::ResetWorkflowExecutionRequest) -> Result<pb::ResetWorkflowExecutionResponse, Status> {
    gate(state)?;
    let r = et::HistoryResetWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        reset_request: req.request.map(reset_request_in),
    };
    let resp = state.handler.reset_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::ResetWorkflowExecutionResponse { run_id: resp.run_id })
}

pub async fn reset_sticky_task_list(state: &ServerState, req: pb::ResetStickyTaskListRequest) -> Result<pb::ResetStickyTaskListResponse, Status> {
    gate(state)?;
    let r = et::HistoryResetStickyTaskListRequest {
        domain_uuid: req.domain_id,
        execution: req.request.and_then(|x| x.workflow_execution).map(c::execution),
    };
    state.handler.reset_sticky_task_list(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::ResetStickyTaskListResponse {})
}

pub async fn query_workflow(state: &ServerState, req: pb::QueryWorkflowRequest) -> Result<pb::QueryWorkflowResponse, Status> {
    gate(state)?;
    let r = et::HistoryQueryWorkflowRequest {
        domain_uuid: req.domain_id,
        request: req.request.map(query_in).transpose()?,
    };
    let resp = state.handler.query_workflow(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(query_response_out(resp.response.unwrap_or_default()))
}

pub async fn describe_workflow_execution(state: &ServerState, req: pb::DescribeWorkflowExecutionRequest) -> Result<pb::DescribeWorkflowExecutionResponse, Status> {
    gate(state)?;
    let r = et::HistoryDescribeWorkflowExecutionRequest {
        domain_uuid: req.domain_id,
        request: req.request.map(describe_in).transpose()?,
    };
    let resp = state.handler.describe_workflow_execution(Some(&r)).await.map_err(|e| st_err(&e))?;
    // History timestamps are nanoseconds; `convert::describe` renders the
    // execution info with milliseconds, so redo that part.
    let info = resp.workflow_execution_info.clone().map(c::info_nanos);
    let d = c::describe(resp);
    Ok(pb::DescribeWorkflowExecutionResponse {
        execution_configuration: d.execution_configuration,
        workflow_execution_info: info,
        pending_activities: d.pending_activities,
        pending_children: d.pending_children,
        pending_decision: d.pending_decision,
    })
}

pub async fn reapply_events(state: &ServerState, req: pb::ReapplyEventsRequest) -> Result<pb::ReapplyEventsResponse, Status> {
    gate(state)?;
    let events = req
        .events
        .map(|b| {
            Ok::<_, Status>(t::DataBlob {
                encoding_type: c::enum_in(b.encoding_type, e::EncodingType::from_i32)?,
                data: b.data,
            })
        })
        .transpose()?;
    let r = et::HistoryReapplyEventsRequest {
        domain_uuid: req.domain_id,
        request: Some(cadence_api::admin::ReapplyEventsRequest {
            domain: req.domain,
            workflow_execution: req.workflow_execution.map(c::execution),
            events,
        }),
    };
    state.handler.reapply_events(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::ReapplyEventsResponse {})
}

pub async fn refresh_workflow_tasks(state: &ServerState, req: pb::RefreshWorkflowTasksRequest) -> Result<pb::RefreshWorkflowTasksResponse, Status> {
    gate(state)?;
    let r = et::HistoryRefreshWorkflowTasksRequest {
        domain_uuid: req.domain_id.clone(),
        request: Some(cadence_api::admin::RefreshWorkflowTasksRequest {
            domain: req.domain,
            domain_id: req.domain_id,
            workflow_execution: req.workflow_execution.map(c::execution),
        }),
    };
    state.handler.refresh_workflow_tasks(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::RefreshWorkflowTasksResponse {})
}

pub async fn schedule_decision_task(state: &ServerState, req: pb::ScheduleDecisionTaskRequest) -> Result<pb::ScheduleDecisionTaskResponse, Status> {
    gate(state)?;
    let r = et::ScheduleDecisionTaskRequest {
        domain_uuid: req.domain_id,
        workflow_execution: req.workflow_execution.map(c::execution),
        is_first_decision: req.is_first_decision,
    };
    state.handler.schedule_decision_task(Some(&r)).await.map_err(|e| st_err(&e))?;
    Ok(pb::ScheduleDecisionTaskResponse {})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_converter_maps_fields() {
        let v = papi::RequestCancelWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_execution: Some(papi::WorkflowExecution { workflow_id: "w".into(), run_id: "r".into() }),
            identity: "i".into(),
            request_id: "q".into(),
            cause: "c".into(),
            first_execution_run_id: "f".into(),
        };
        let o = cancel_in(v);
        assert_eq!(o.domain, "d");
        assert_eq!(o.workflow_execution.unwrap().run_id, "r");
        assert_eq!(o.first_execution_run_id, "f");
    }

    #[test]
    fn query_response_round_trip_rejected() {
        let v = tk::QueryWorkflowResponse {
            query_result: vec![1, 2],
            query_rejected: Some(cadence_api::errors::QueryRejected {
                close_status: Some(e::WorkflowExecutionCloseStatus::Completed),
            }),
        };
        let out = query_response_out(v);
        assert_eq!(out.query_result.unwrap().data, vec![1, 2]);
        assert_eq!(out.query_rejected.unwrap().close_status, e::WorkflowExecutionCloseStatus::Completed as i32 + 1);
    }

    #[test]
    fn signal_converter_maps_input() {
        let v = papi::SignalWorkflowExecutionRequest {
            domain: "d".into(),
            signal_name: "s".into(),
            signal_input: Some(papi::Payload { data: vec![9] }),
            ..Default::default()
        };
        let o = signal_in(v);
        assert_eq!(o.signal_name, "s");
        assert_eq!(o.input, vec![9]);
    }
}
