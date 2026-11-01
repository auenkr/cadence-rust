//! Thrift decisions -> `api.v1.Decision` (inverse of `decisions.rs`), used
//! when Frontend forwards `RespondDecisionTaskCompleted` to History.
use super::*;
use crate::api as p;
use cadence_api::{enums as e, tasks, types as t};

fn dur(v: Option<i32>) -> Option<prost_types::Duration> {
    v.map(|s| duration(i64::from(s)))
}
fn pb_enum<T: Into<i32>>(v: Option<T>) -> i32 {
    v.map_or(0, |x| x.into() + 1)
}
fn failure_pb(reason: Option<String>, details: Vec<u8>) -> Option<p::Failure> {
    if reason.is_none() && details.is_empty() {
        None
    } else {
        Some(p::Failure {
            reason: reason.unwrap_or_default(),
            details,
        })
    }
}
fn header_pb(v: Option<t::Header>) -> Option<p::Header> {
    v.map(header)
}

pub fn decision_out(v: tasks::Decision) -> Result<p::Decision, Status> {
    use p::decision::Attributes as A;
    let kind = v
        .decision_type
        .ok_or_else(|| Status::invalid_argument("Decision type is not set."))?;
    let missing = || Status::invalid_argument("Decision attributes are not set.");
    let attributes = match kind {
        e::DecisionType::ScheduleActivityTask => {
            let a = v.schedule_activity_task_decision_attributes.ok_or_else(missing)?;
            A::ScheduleActivityTaskDecisionAttributes(p::ScheduleActivityTaskDecisionAttributes {
                activity_id: a.activity_id,
                activity_type: a.activity_type.map(|t| p::ActivityType { name: t.name }),
                domain: a.domain,
                task_list: a.task_list.map(task_list_out),
                input: payload(a.input),
                schedule_to_close_timeout: dur(a.schedule_to_close_timeout_seconds),
                schedule_to_start_timeout: dur(a.schedule_to_start_timeout_seconds),
                start_to_close_timeout: dur(a.start_to_close_timeout_seconds),
                heartbeat_timeout: dur(a.heartbeat_timeout_seconds),
                retry_policy: a.retry_policy.map(retry_out),
                header: header_pb(a.header),
                request_local_dispatch: false,
            })
        }
        e::DecisionType::CompleteWorkflowExecution => {
            let a = v
                .complete_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::CompleteWorkflowExecutionDecisionAttributes(
                p::CompleteWorkflowExecutionDecisionAttributes {
                    result: payload(a.result),
                },
            )
        }
        e::DecisionType::FailWorkflowExecution => {
            let a = v
                .fail_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::FailWorkflowExecutionDecisionAttributes(p::FailWorkflowExecutionDecisionAttributes {
                failure: failure_pb(a.reason, a.details),
            })
        }
        e::DecisionType::RecordMarker => {
            let a = v.record_marker_decision_attributes.ok_or_else(missing)?;
            A::RecordMarkerDecisionAttributes(p::RecordMarkerDecisionAttributes {
                marker_name: a.marker_name,
                details: payload(a.details),
                header: header_pb(a.header),
            })
        }
        e::DecisionType::StartTimer => {
            let a = v.start_timer_decision_attributes.ok_or_else(missing)?;
            A::StartTimerDecisionAttributes(p::StartTimerDecisionAttributes {
                timer_id: a.timer_id,
                start_to_fire_timeout: a.start_to_fire_timeout_seconds.map(duration),
            })
        }
        e::DecisionType::CancelTimer => {
            let a = v.cancel_timer_decision_attributes.ok_or_else(missing)?;
            A::CancelTimerDecisionAttributes(p::CancelTimerDecisionAttributes {
                timer_id: a.timer_id,
            })
        }
        e::DecisionType::RequestCancelActivityTask => {
            let a = v
                .request_cancel_activity_task_decision_attributes
                .ok_or_else(missing)?;
            A::RequestCancelActivityTaskDecisionAttributes(
                p::RequestCancelActivityTaskDecisionAttributes {
                    activity_id: a.activity_id,
                },
            )
        }
        e::DecisionType::CancelWorkflowExecution => {
            let a = v
                .cancel_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::CancelWorkflowExecutionDecisionAttributes(
                p::CancelWorkflowExecutionDecisionAttributes {
                    details: payload(a.details),
                },
            )
        }
        e::DecisionType::ContinueAsNewWorkflowExecution => {
            let a = v
                .continue_as_new_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::ContinueAsNewWorkflowExecutionDecisionAttributes(
                p::ContinueAsNewWorkflowExecutionDecisionAttributes {
                    workflow_type: a.workflow_type.map(|t| p::WorkflowType { name: t.name }),
                    task_list: a.task_list.map(task_list_out),
                    input: payload(a.input),
                    execution_start_to_close_timeout: dur(a.execution_start_to_close_timeout_seconds),
                    task_start_to_close_timeout: dur(a.task_start_to_close_timeout_seconds),
                    backoff_start_interval: dur(a.backoff_start_interval_in_seconds),
                    retry_policy: a.retry_policy.map(retry_out),
                    initiator: pb_enum(a.initiator.map(|x| x as i32)),
                    failure: failure_pb(a.failure_reason, a.failure_details),
                    last_completion_result: payload(a.last_completion_result),
                    cron_schedule: a.cron_schedule,
                    header: header_pb(a.header),
                    memo: a.memo.map(memo),
                    search_attributes: a.search_attributes.map(search_attributes),
                    jitter_start: dur(a.jitter_start_seconds),
                    cron_overlap_policy: pb_enum(a.cron_overlap_policy.map(|x| x as i32)),
                    active_cluster_selection_policy: None,
                },
            )
        }
        e::DecisionType::StartChildWorkflowExecution => {
            let a = v
                .start_child_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::StartChildWorkflowExecutionDecisionAttributes(
                p::StartChildWorkflowExecutionDecisionAttributes {
                    domain: a.domain,
                    workflow_id: a.workflow_id,
                    workflow_type: a.workflow_type.map(|t| p::WorkflowType { name: t.name }),
                    task_list: a.task_list.map(task_list_out),
                    input: payload(a.input),
                    execution_start_to_close_timeout: dur(a.execution_start_to_close_timeout_seconds),
                    task_start_to_close_timeout: dur(a.task_start_to_close_timeout_seconds),
                    parent_close_policy: pb_enum(a.parent_close_policy.map(|x| x as i32)),
                    control: a.control,
                    workflow_id_reuse_policy: pb_enum(a.workflow_id_reuse_policy.map(|x| x as i32)),
                    retry_policy: a.retry_policy.map(retry_out),
                    header: header_pb(a.header),
                    memo: a.memo.map(memo),
                    search_attributes: a.search_attributes.map(search_attributes),
                    cron_schedule: a.cron_schedule,
                    cron_overlap_policy: pb_enum(a.cron_overlap_policy.map(|x| x as i32)),
                    active_cluster_selection_policy: None,
                },
            )
        }
        e::DecisionType::SignalExternalWorkflowExecution => {
            let a = v
                .signal_external_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::SignalExternalWorkflowExecutionDecisionAttributes(
                p::SignalExternalWorkflowExecutionDecisionAttributes {
                    domain: a.domain,
                    workflow_execution: a.execution.map(execution_out),
                    signal_name: a.signal_name,
                    input: payload(a.input),
                    control: a.control,
                    child_workflow_only: a.child_workflow_only,
                },
            )
        }
        e::DecisionType::UpsertWorkflowSearchAttributes => {
            let a = v
                .upsert_workflow_search_attributes_decision_attributes
                .ok_or_else(missing)?;
            A::UpsertWorkflowSearchAttributesDecisionAttributes(
                p::UpsertWorkflowSearchAttributesDecisionAttributes {
                    search_attributes: a.search_attributes.map(search_attributes),
                },
            )
        }
        e::DecisionType::RequestCancelExternalWorkflowExecution => {
            let a = v
                .request_cancel_external_workflow_execution_decision_attributes
                .ok_or_else(missing)?;
            A::RequestCancelExternalWorkflowExecutionDecisionAttributes(
                p::RequestCancelExternalWorkflowExecutionDecisionAttributes {
                    domain: a.domain,
                    workflow_execution: Some(p::WorkflowExecution {
                        workflow_id: a.workflow_id,
                        run_id: a.run_id,
                    }),
                    control: a.control,
                    child_workflow_only: a.child_workflow_only,
                },
            )
        }
        #[allow(unreachable_patterns)]
        _ => return Err(Status::unimplemented("This decision type has not been ported.")),
    };
    Ok(p::Decision {
        attributes: Some(attributes),
    })
}
