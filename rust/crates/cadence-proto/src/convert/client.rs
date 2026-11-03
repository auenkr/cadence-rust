//! Client-direction conversions (internal request -> proto request, proto
//! response -> internal response) for the remote Frontend gRPC client.
//!
//! Mirrors Go `common/types/mapper/proto` `From*Request` / `To*Response`.
//! Remote clusters speak Thrift-style unix nanoseconds, so timestamps in
//! responses are returned as nanoseconds (like the TChannel client).
use super::*;
use crate::api as p;
use cadence_api::{enums as e, service_types as st, tasks as tk, types as t};
use tonic::Status;

fn ns(v: i64) -> prost_types::Timestamp {
    timestamp_nanos(v)
}

fn wf_type(v: t::WorkflowType) -> p::WorkflowType {
    p::WorkflowType { name: v.name }
}

fn wf_type_in(v: p::WorkflowType) -> t::WorkflowType {
    t::WorkflowType { name: v.name }
}

fn secs(v: Option<i32>) -> Option<prost_types::Duration> {
    v.map(|s| duration(i64::from(s)))
}

/// Go `proto.FromStartWorkflowExecutionRequest`.
pub fn start_request_out(v: t::StartWorkflowExecutionRequest) -> p::StartWorkflowExecutionRequest {
    p::StartWorkflowExecutionRequest {
        domain: v.domain,
        workflow_id: v.workflow_id,
        workflow_type: v.workflow_type.map(wf_type),
        task_list: v.task_list.map(task_list_out),
        input: payload(v.input),
        execution_start_to_close_timeout: secs(v.execution_start_to_close_timeout_seconds),
        task_start_to_close_timeout: secs(v.task_start_to_close_timeout_seconds),
        identity: v.identity,
        request_id: v.request_id,
        workflow_id_reuse_policy: v.workflow_id_reuse_policy.map_or(0, |x| x as i32 + 1),
        retry_policy: v.retry_policy.map(retry_out),
        cron_schedule: v.cron_schedule,
        memo: v.memo.map(memo),
        search_attributes: v.search_attributes.map(search_attributes),
        header: v.header.map(header),
        delay_start: secs(v.delay_start_seconds),
        jitter_start: secs(v.jitter_start_seconds),
        first_run_at: v.first_run_at_timestamp.map(ns),
        cron_overlap_policy: v.cron_overlap_policy.map_or(0, |x| x as i32 + 1),
        active_cluster_selection_policy: None,
    }
}

pub fn signal_request_out(
    v: t::SignalWorkflowExecutionRequest,
) -> p::SignalWorkflowExecutionRequest {
    p::SignalWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        identity: v.identity,
        request_id: v.request_id,
        signal_name: v.signal_name,
        signal_input: payload(v.input),
        control: v.control,
    }
}

pub fn terminate_request_out(
    v: t::TerminateWorkflowExecutionRequest,
) -> p::TerminateWorkflowExecutionRequest {
    p::TerminateWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        reason: v.reason,
        details: payload(v.details),
        identity: v.identity,
        first_execution_run_id: v.first_execution_run_id,
    }
}

pub fn cancel_request_out(
    v: st::RequestCancelWorkflowExecutionRequest,
) -> p::RequestCancelWorkflowExecutionRequest {
    p::RequestCancelWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        identity: v.identity,
        request_id: v.request_id,
        cause: v.cause,
        first_execution_run_id: v.first_execution_run_id,
    }
}

pub fn start_async_request_out(
    v: t::StartWorkflowExecutionAsyncRequest,
) -> p::StartWorkflowExecutionAsyncRequest {
    p::StartWorkflowExecutionAsyncRequest {
        request: v.request.map(start_request_out),
    }
}

pub fn signal_with_start_request_out(
    v: st::SignalWithStartWorkflowExecutionRequest,
) -> p::SignalWithStartWorkflowExecutionRequest {
    p::SignalWithStartWorkflowExecutionRequest {
        start_request: Some(p::StartWorkflowExecutionRequest {
            domain: v.domain,
            workflow_id: v.workflow_id,
            workflow_type: v.workflow_type.map(wf_type),
            task_list: v.task_list.map(task_list_out),
            input: payload(v.input),
            execution_start_to_close_timeout: secs(v.execution_start_to_close_timeout_seconds),
            task_start_to_close_timeout: secs(v.task_start_to_close_timeout_seconds),
            identity: v.identity,
            request_id: v.request_id,
            workflow_id_reuse_policy: v.workflow_id_reuse_policy.map_or(0, |x| x as i32 + 1),
            retry_policy: v.retry_policy.map(retry_out),
            cron_schedule: v.cron_schedule,
            memo: v.memo.map(memo),
            search_attributes: v.search_attributes.map(search_attributes),
            header: v.header.map(header),
            delay_start: secs(v.delay_start_seconds),
            jitter_start: secs(v.jitter_start_seconds),
            first_run_at: v.first_run_at_timestamp.map(ns),
            cron_overlap_policy: v.cron_overlap_policy.map_or(0, |x| x as i32 + 1),
            active_cluster_selection_policy: None,
        }),
        signal_name: v.signal_name,
        signal_input: payload(v.signal_input),
        control: v.control,
    }
}

pub fn signal_with_start_async_request_out(
    v: st::SignalWithStartWorkflowExecutionAsyncRequest,
) -> p::SignalWithStartWorkflowExecutionAsyncRequest {
    p::SignalWithStartWorkflowExecutionAsyncRequest {
        request: v.request.map(signal_with_start_request_out),
    }
}

pub fn reset_request_out(v: t::ResetWorkflowExecutionRequest) -> p::ResetWorkflowExecutionRequest {
    p::ResetWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        reason: v.reason,
        decision_finish_event_id: v.decision_finish_event_id,
        request_id: v.request_id,
        skip_signal_reapply: v.skip_signal_reapply,
    }
}

pub fn restart_request_out(
    v: t::RestartWorkflowExecutionRequest,
) -> p::RestartWorkflowExecutionRequest {
    p::RestartWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        identity: v.identity,
        reason: v.reason,
    }
}

pub fn diagnose_request_out(
    v: t::DiagnoseWorkflowExecutionRequest,
) -> p::DiagnoseWorkflowExecutionRequest {
    p::DiagnoseWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.workflow_execution.map(execution_out),
        identity: v.identity,
    }
}

pub fn diagnose_response_in(
    v: p::DiagnoseWorkflowExecutionResponse,
) -> t::DiagnoseWorkflowExecutionResponse {
    t::DiagnoseWorkflowExecutionResponse {
        domain: v.domain,
        diagnostic_workflow_execution: v.diagnostic_workflow_execution.map(execution),
    }
}

pub fn refresh_request_out(v: t::RefreshWorkflowTasksRequest) -> p::RefreshWorkflowTasksRequest {
    p::RefreshWorkflowTasksRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(execution_out),
    }
}

pub fn reset_sticky_request_out(v: st::ResetStickyTaskListRequest) -> p::ResetStickyTaskListRequest {
    p::ResetStickyTaskListRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(execution_out),
    }
}

fn consistency_out(v: Option<e::QueryConsistencyLevel>) -> i32 {
    v.map_or(0, |x| x as i32 + 1)
}

pub fn describe_request_out(
    v: st::DescribeWorkflowExecutionRequest,
) -> p::DescribeWorkflowExecutionRequest {
    p::DescribeWorkflowExecutionRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(execution_out),
        query_consistency_level: consistency_out(v.query_consistency_level),
    }
}

pub fn query_request_out(v: tk::QueryWorkflowRequest) -> p::QueryWorkflowRequest {
    p::QueryWorkflowRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(execution_out),
        query: v.query.map(|q| p::WorkflowQuery {
            query_type: q.query_type,
            query_args: payload(q.query_args),
        }),
        query_reject_condition: v.query_reject_condition.map_or(0, |x| x as i32 + 1),
        query_consistency_level: consistency_out(v.query_consistency_level),
    }
}

pub fn query_response_in(v: p::QueryWorkflowResponse) -> Result<tk::QueryWorkflowResponse, Status> {
    Ok(tk::QueryWorkflowResponse {
        query_result: data(v.query_result),
        query_rejected: v
            .query_rejected
            .map(|q| {
                Ok::<_, Status>(cadence_api::errors::QueryRejected {
                    close_status: enum_in(q.close_status, e::WorkflowExecutionCloseStatus::from_i32)?,
                })
            })
            .transpose()?,
    })
}

pub fn history_request_out(
    v: st::GetWorkflowExecutionHistoryRequest,
) -> p::GetWorkflowExecutionHistoryRequest {
    p::GetWorkflowExecutionHistoryRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(execution_out),
        page_size: v.maximum_page_size,
        next_page_token: v.next_page_token,
        wait_for_new_event: v.wait_for_new_event,
        history_event_filter_type: v.history_event_filter_type.map_or(0, |x| x as i32 + 1),
        skip_archival: v.skip_archival,
        query_consistency_level: consistency_out(v.query_consistency_level),
    }
}

pub fn history_in(v: p::History) -> Result<st::History, Status> {
    Ok(st::History {
        events: v
            .events
            .into_iter()
            .map(history_in::event)
            .collect::<Result<_, _>>()?,
    })
}

pub fn history_response_in(
    v: p::GetWorkflowExecutionHistoryResponse,
) -> Result<st::GetWorkflowExecutionHistoryResponse, Status> {
    Ok(st::GetWorkflowExecutionHistoryResponse {
        history: v.history.map(history_in).transpose()?,
        raw_history: v
            .raw_history
            .into_iter()
            .map(|b| {
                Ok::<_, Status>(t::DataBlob {
                    encoding_type: enum_in(b.encoding_type, e::EncodingType::from_i32)?,
                    data: b.data,
                })
            })
            .collect::<Result<_, _>>()?,
        next_page_token: v.next_page_token,
        archived: v.archived,
    })
}

fn time_filter_out(v: st::StartTimeFilter) -> p::StartTimeFilter {
    p::StartTimeFilter {
        earliest_time: v.earliest_time.map(ns),
        latest_time: v.latest_time.map(ns),
    }
}

pub fn list_open_request_out(
    v: st::ListOpenWorkflowExecutionsRequest,
) -> p::ListOpenWorkflowExecutionsRequest {
    use p::list_open_workflow_executions_request::Filters;
    let filters = if let Some(f) = v.execution_filter {
        Some(Filters::ExecutionFilter(p::WorkflowExecutionFilter {
            workflow_id: f.workflow_id,
            run_id: f.run_id,
        }))
    } else {
        v.type_filter
            .map(|f| Filters::TypeFilter(p::WorkflowTypeFilter { name: f.name }))
    };
    p::ListOpenWorkflowExecutionsRequest {
        domain: v.domain,
        page_size: v.maximum_page_size,
        next_page_token: v.next_page_token,
        start_time_filter: v.start_time_filter.map(time_filter_out),
        filters,
    }
}

pub fn list_closed_request_out(
    v: st::ListClosedWorkflowExecutionsRequest,
) -> p::ListClosedWorkflowExecutionsRequest {
    use p::list_closed_workflow_executions_request::Filters;
    let filters = if let Some(f) = v.execution_filter {
        Some(Filters::ExecutionFilter(p::WorkflowExecutionFilter {
            workflow_id: f.workflow_id,
            run_id: f.run_id,
        }))
    } else if let Some(f) = v.type_filter {
        Some(Filters::TypeFilter(p::WorkflowTypeFilter { name: f.name }))
    } else {
        v.status_filter.map(|s| {
            Filters::StatusFilter(p::StatusFilter {
                status: s as i32 + 1,
            })
        })
    };
    p::ListClosedWorkflowExecutionsRequest {
        domain: v.domain,
        page_size: v.maximum_page_size,
        next_page_token: v.next_page_token,
        start_time_filter: v.start_time_filter.map(time_filter_out),
        filters,
    }
}

pub fn list_request_out(v: st::ListWorkflowExecutionsRequest) -> p::ListWorkflowExecutionsRequest {
    p::ListWorkflowExecutionsRequest {
        domain: v.domain,
        page_size: v.page_size,
        next_page_token: v.next_page_token,
        query: v.query,
    }
}

pub fn scan_request_out(v: st::ListWorkflowExecutionsRequest) -> p::ScanWorkflowExecutionsRequest {
    p::ScanWorkflowExecutionsRequest {
        domain: v.domain,
        page_size: v.page_size,
        next_page_token: v.next_page_token,
        query: v.query,
    }
}

pub fn list_archived_request_out(
    v: st::ListArchivedWorkflowExecutionsRequest,
) -> p::ListArchivedWorkflowExecutionsRequest {
    p::ListArchivedWorkflowExecutionsRequest {
        domain: v.domain,
        page_size: v.page_size,
        next_page_token: v.next_page_token,
        query: v.query,
    }
}

pub fn count_request_out(
    v: st::CountWorkflowExecutionsRequest,
) -> p::CountWorkflowExecutionsRequest {
    p::CountWorkflowExecutionsRequest {
        domain: v.domain,
        query: v.query,
    }
}

/// Go `proto.ToWorkflowExecutionInfo` (timestamps as unix nanoseconds).
pub fn info_in(v: p::WorkflowExecutionInfo) -> Result<st::WorkflowExecutionInfo, Status> {
    let parent = v.parent_execution_info;
    Ok(st::WorkflowExecutionInfo {
        execution: v.workflow_execution.map(execution),
        workflow_type: v.r#type.map(wf_type_in),
        start_time: nanos(v.start_time),
        close_time: nanos(v.close_time),
        close_status: enum_in(v.close_status, e::WorkflowExecutionCloseStatus::from_i32)?,
        history_length: v.history_length,
        memo: v.memo.map(memo_in),
        parent_domain: parent.as_ref().map(|x| x.domain_name.clone()),
        parent_execution: parent
            .as_ref()
            .and_then(|x| x.workflow_execution.clone())
            .map(execution),
        parent_initiated_id: parent.as_ref().map(|x| x.initiated_id),
        execution_time: nanos(v.execution_time),
        search_attributes: v.search_attributes.map(search_attributes_in),
        task_list: v.task_list_info.map(task_list).transpose()?,
        is_cron: v.is_cron,
        cron_schedule: if v.cron_schedule.is_empty() {
            None
        } else {
            Some(v.cron_schedule)
        },
        cron_overlap_policy: enum_in(v.cron_overlap_policy, e::CronOverlapPolicy::from_i32)?,
        update_time: nanos(v.update_time),
        partition_config: if v.partition_config.is_empty() {
            None
        } else {
            Some(v.partition_config)
        },
        execution_status: enum_in(v.execution_status, e::WorkflowExecutionStatus::from_i32)?,
        scheduled_execution_time: nanos(v.scheduled_execution_time),
    })
}

fn infos_in(v: Vec<p::WorkflowExecutionInfo>) -> Result<Vec<st::WorkflowExecutionInfo>, Status> {
    v.into_iter().map(info_in).collect()
}

pub fn list_open_response_in(
    v: p::ListOpenWorkflowExecutionsResponse,
) -> Result<st::ListOpenWorkflowExecutionsResponse, Status> {
    Ok(st::ListOpenWorkflowExecutionsResponse {
        executions: infos_in(v.executions)?,
        next_page_token: v.next_page_token,
    })
}

pub fn list_closed_response_in(
    v: p::ListClosedWorkflowExecutionsResponse,
) -> Result<st::ListClosedWorkflowExecutionsResponse, Status> {
    Ok(st::ListClosedWorkflowExecutionsResponse {
        executions: infos_in(v.executions)?,
        next_page_token: v.next_page_token,
    })
}

pub fn list_response_in(
    v: p::ListWorkflowExecutionsResponse,
) -> Result<st::ListWorkflowExecutionsResponse, Status> {
    Ok(st::ListWorkflowExecutionsResponse {
        executions: infos_in(v.executions)?,
        next_page_token: v.next_page_token,
    })
}

pub fn scan_response_in(
    v: p::ScanWorkflowExecutionsResponse,
) -> Result<st::ListWorkflowExecutionsResponse, Status> {
    Ok(st::ListWorkflowExecutionsResponse {
        executions: infos_in(v.executions)?,
        next_page_token: v.next_page_token,
    })
}

pub fn list_archived_response_in(
    v: p::ListArchivedWorkflowExecutionsResponse,
) -> Result<st::ListArchivedWorkflowExecutionsResponse, Status> {
    Ok(st::ListArchivedWorkflowExecutionsResponse {
        executions: infos_in(v.executions)?,
        next_page_token: v.next_page_token,
    })
}

pub fn describe_response_in(
    v: p::DescribeWorkflowExecutionResponse,
) -> Result<st::DescribeWorkflowExecutionResponse, Status> {
    Ok(st::DescribeWorkflowExecutionResponse {
        execution_configuration: v
            .execution_configuration
            .map(|c| {
                Ok::<_, Status>(st::WorkflowExecutionConfiguration {
                    task_list: c.task_list.map(task_list).transpose()?,
                    execution_start_to_close_timeout_seconds: seconds(
                        c.execution_start_to_close_timeout,
                    )?,
                    task_start_to_close_timeout_seconds: seconds(c.task_start_to_close_timeout)?,
                })
            })
            .transpose()?,
        workflow_execution_info: v.workflow_execution_info.map(info_in).transpose()?,
        pending_activities: v
            .pending_activities
            .into_iter()
            .map(|a| {
                Ok::<_, Status>(tk::PendingActivityInfo {
                    activity_id: a.activity_id,
                    activity_type: a.activity_type.map(|t| t::ActivityType { name: t.name }),
                    state: enum_in(a.state, e::PendingActivityState::from_i32)?,
                    attempt: a.attempt,
                    scheduled_timestamp: nanos(a.scheduled_time),
                    last_started_timestamp: nanos(a.last_started_time),
                    heartbeat_details: data(a.heartbeat_details),
                    maximum_attempts: a.maximum_attempts,
                    schedule_id: a.schedule_id,
                    last_failure_reason: a.last_failure.map(|f| f.reason),
                    ..Default::default()
                })
            })
            .collect::<Result<_, _>>()?,
        pending_children: v
            .pending_children
            .into_iter()
            .map(|c| {
                let execution = c.workflow_execution.unwrap_or_default();
                Ok::<_, Status>(tk::PendingChildExecutionInfo {
                    domain: c.domain,
                    workflow_id: execution.workflow_id,
                    run_id: execution.run_id,
                    workflow_type_name: c.workflow_type_name,
                    initiated_id: c.initiated_id,
                    parent_close_policy: enum_in(
                        c.parent_close_policy,
                        e::ParentClosePolicy::from_i32,
                    )?,
                })
            })
            .collect::<Result<_, _>>()?,
        pending_decision: v
            .pending_decision
            .map(|d| {
                Ok::<_, Status>(tk::PendingDecisionInfo {
                    state: enum_in(d.state, e::PendingDecisionState::from_i32)?,
                    scheduled_timestamp: nanos(d.scheduled_time),
                    started_timestamp: nanos(d.started_time),
                    attempt: i64::from(d.attempt),
                    schedule_id: d.schedule_id,
                    original_scheduled_timestamp: nanos(d.original_scheduled_time),
                })
            })
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec() -> t::WorkflowExecution {
        t::WorkflowExecution {
            workflow_id: "w".into(),
            run_id: "r".into(),
        }
    }

    #[test]
    fn describe_request_round_trip() {
        let out = describe_request_out(st::DescribeWorkflowExecutionRequest {
            domain: "d".into(),
            execution: Some(exec()),
            query_consistency_level: Some(e::QueryConsistencyLevel::Strong),
        });
        assert_eq!(out.domain, "d");
        assert_eq!(out.query_consistency_level, 2);
        assert_eq!(execution(out.workflow_execution.unwrap()), exec());
    }

    #[test]
    fn query_round_trip() {
        let out = query_request_out(tk::QueryWorkflowRequest {
            domain: "d".into(),
            execution: Some(exec()),
            query: Some(tk::WorkflowQuery {
                query_type: "q".into(),
                query_args: b"a".to_vec(),
            }),
            query_reject_condition: Some(e::QueryRejectCondition::NotOpen),
            query_consistency_level: None,
        });
        assert_eq!(out.query.as_ref().unwrap().query_type, "q");
        assert_eq!(out.query_reject_condition, 1);
        let back = query_response_in(p::QueryWorkflowResponse {
            query_result: payload(b"ans".to_vec()),
            query_rejected: Some(p::QueryRejected {
                close_status: 2,
                ..Default::default()
            }),
        })
        .unwrap();
        assert_eq!(back.query_result, b"ans");
        assert_eq!(
            back.query_rejected.unwrap().close_status,
            Some(e::WorkflowExecutionCloseStatus::Failed)
        );
    }

    #[test]
    fn history_request_round_trip() {
        let out = history_request_out(st::GetWorkflowExecutionHistoryRequest {
            domain: "d".into(),
            execution: Some(exec()),
            maximum_page_size: 7,
            next_page_token: b"t".to_vec(),
            wait_for_new_event: true,
            history_event_filter_type: Some(e::HistoryEventFilterType::CloseEvent),
            skip_archival: true,
            query_consistency_level: None,
        });
        assert_eq!(out.page_size, 7);
        assert_eq!(out.history_event_filter_type, 2);
        assert!(out.wait_for_new_event && out.skip_archival);
    }

    #[test]
    fn history_events_round_trip() {
        let event = st::HistoryEvent {
            event_id: 5,
            timestamp: Some(1_700_000_000_123_456_789),
            event_type: Some(e::EventType::WorkflowExecutionSignaled),
            version: 3,
            task_id: 9,
            workflow_execution_signaled_event_attributes: Some(
                st::WorkflowExecutionSignaledEventAttributes {
                    signal_name: "s".into(),
                    input: b"i".to_vec(),
                    identity: "id".into(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        let proto = history::event(event.clone()).unwrap();
        let back = history_in::event(proto).unwrap();
        assert_eq!(back.event_id, 5);
        assert_eq!(back.event_type, event.event_type);
        assert_eq!(
            back.workflow_execution_signaled_event_attributes
                .unwrap()
                .signal_name,
            "s"
        );
    }

    #[test]
    fn list_closed_filters_round_trip() {
        let out = list_closed_request_out(st::ListClosedWorkflowExecutionsRequest {
            domain: "d".into(),
            maximum_page_size: 3,
            status_filter: Some(e::WorkflowExecutionCloseStatus::Failed),
            ..Default::default()
        });
        assert!(matches!(
            out.filters,
            Some(p::list_closed_workflow_executions_request::Filters::StatusFilter(
                p::StatusFilter { status: 2 }
            ))
        ));
        let open = list_open_request_out(st::ListOpenWorkflowExecutionsRequest {
            domain: "d".into(),
            execution_filter: Some(st::WorkflowExecutionFilter {
                workflow_id: "w".into(),
                run_id: String::new(),
            }),
            ..Default::default()
        });
        assert!(matches!(
            open.filters,
            Some(p::list_open_workflow_executions_request::Filters::ExecutionFilter(_))
        ));
    }

    #[test]
    fn execution_info_round_trip() {
        let info = st::WorkflowExecutionInfo {
            execution: Some(exec()),
            workflow_type: Some(t::WorkflowType { name: "T".into() }),
            start_time: Some(1_700_000_000_000_000_000),
            close_status: Some(e::WorkflowExecutionCloseStatus::Completed),
            history_length: 4,
            is_cron: true,
            cron_schedule: Some("* * * * *".into()),
            ..Default::default()
        };
        let back = info_in(info_nanos(info.clone())).unwrap();
        assert_eq!(back.execution, info.execution);
        assert_eq!(back.start_time, info.start_time);
        assert_eq!(back.close_status, info.close_status);
        assert_eq!(back.cron_schedule, info.cron_schedule);
        assert_eq!(back.history_length, 4);
    }

    #[test]
    fn describe_response_round_trip() {
        let resp = st::DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(st::WorkflowExecutionInfo {
                execution: Some(exec()),
                ..Default::default()
            }),
            pending_children: vec![tk::PendingChildExecutionInfo {
                domain: "d".into(),
                workflow_id: "cw".into(),
                run_id: "cr".into(),
                workflow_type_name: "CT".into(),
                initiated_id: 8,
                parent_close_policy: Some(e::ParentClosePolicy::Terminate),
            }],
            ..Default::default()
        };
        let back = describe_response_in(describe(resp.clone())).unwrap();
        assert_eq!(back.pending_children[0].workflow_id, "cw");
        assert_eq!(
            back.pending_children[0].parent_close_policy,
            Some(e::ParentClosePolicy::Terminate)
        );
    }

    #[test]
    fn start_family_requests() {
        let start = start_request_out(t::StartWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_id: "w".into(),
            first_run_at_timestamp: Some(1_500_000_000),
            ..Default::default()
        });
        assert_eq!(start.first_run_at.unwrap().nanos, 500_000_000);
        let sws = signal_with_start_request_out(st::SignalWithStartWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_id: "w".into(),
            signal_name: "s".into(),
            signal_input: b"x".to_vec(),
            ..Default::default()
        });
        assert_eq!(sws.start_request.unwrap().workflow_id, "w");
        assert_eq!(sws.signal_name, "s");
        let reset = reset_request_out(t::ResetWorkflowExecutionRequest {
            decision_finish_event_id: 4,
            skip_signal_reapply: true,
            ..Default::default()
        });
        assert!(reset.skip_signal_reapply && reset.decision_finish_event_id == 4);
    }
}
