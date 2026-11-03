//! Cross-cluster task payloads: admin.v1 protobuf <-> Rust API model, after
//! Go `common/types/mapper/proto/admin.go` (CrossClusterTask*). History's
//! internal proto reuses the admin.v1 messages, so these serve both the
//! History client and the AdminAPI gRPC server.

use super::history::{event, start_child_workflow_execution_initiated_event_attributes};
use super::*;
use crate::admin as pa;
use cadence_api::admin as a;
use cadence_api::enums::EventType;
use pa::cross_cluster_task_request::Attributes as ReqAttr;
use pa::cross_cluster_task_response::Attributes as RespAttr;
use std::collections::HashMap;

fn ms(ts: prost_types::Timestamp) -> i64 {
    ts.seconds * 1000 + i64::from(ts.nanos) / 1_000_000
}
fn ns(ts: prost_types::Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + i64::from(ts.nanos)
}
fn exec(v: Option<p::WorkflowExecution>) -> (String, String) {
    v.map_or_else(Default::default, |e| (e.workflow_id, e.run_id))
}
fn exec_out(workflow_id: String, run_id: String) -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id,
        run_id,
    })
}

// --- enums (protobuf = thrift + 1, 0 = INVALID/unset) --------------------

fn task_type_in(v: i32) -> Result<Option<e::CrossClusterTaskType>, Status> {
    enum_in(v, e::CrossClusterTaskType::from_i32)
}
fn failed_cause_in(v: i32) -> Result<Option<e::CrossClusterTaskFailedCause>, Status> {
    enum_in(v, e::CrossClusterTaskFailedCause::from_i32)
}
fn task_type_out(v: Option<e::CrossClusterTaskType>) -> i32 {
    v.map_or(0, |e| e.value() + 1)
}
fn failed_cause_out(v: Option<e::CrossClusterTaskFailedCause>) -> i32 {
    v.map_or(0, |e| e.value() + 1)
}

// --- history pieces ------------------------------------------------------

fn start_child_initiated_in(
    v: p::StartChildWorkflowExecutionInitiatedEventAttributes,
) -> Result<tk::StartChildWorkflowExecutionInitiatedEventAttributes, Status> {
    if v.active_cluster_selection_policy.is_some() {
        return Err(Status::unimplemented(
            "Active cluster selection policy has not been ported.",
        ));
    }
    Ok(tk::StartChildWorkflowExecutionInitiatedEventAttributes {
        domain: v.domain,
        workflow_id: v.workflow_id,
        workflow_type: v.workflow_type.map(|x| t::WorkflowType { name: x.name }),
        task_list: v.task_list.map(task_list).transpose()?,
        input: data(v.input),
        execution_start_to_close_timeout_seconds: seconds(v.execution_start_to_close_timeout)?,
        task_start_to_close_timeout_seconds: seconds(v.task_start_to_close_timeout)?,
        parent_close_policy: enum_in(v.parent_close_policy, e::ParentClosePolicy::from_i32)?,
        control: v.control,
        decision_task_completed_event_id: v.decision_task_completed_event_id,
        workflow_id_reuse_policy: enum_in(
            v.workflow_id_reuse_policy,
            e::WorkflowIDReusePolicy::from_i32,
        )?,
        header: v.header.map(|m| t::Header {
            fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
        }),
        retry_policy: v.retry_policy.map(retry).transpose()?,
        cron_schedule: v.cron_schedule,
        memo: v.memo.map(|m| t::Memo {
            fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
        }),
        search_attributes: v.search_attributes.map(|m| t::SearchAttributes {
            indexed_fields: Some(
                m.indexed_fields
                    .into_iter()
                    .map(|(k, v)| (k, v.data))
                    .collect(),
            ),
        }),
        delay_start_seconds: seconds(v.delay_start)?,
        jitter_start_seconds: seconds(v.jitter_start)?,
        first_run_at_timestamp: v.first_run_at.map(ms),
        cron_overlap_policy: enum_in(v.cron_overlap_policy, e::CronOverlapPolicy::from_i32)?,
    })
}

/// A child's completion event: one of the workflow close events. Other event
/// types never appear in RecordChildWorkflowExecutionComplete tasks.
fn completion_event_in(v: p::HistoryEvent) -> Result<st::HistoryEvent, Status> {
    use p::history_event::Attributes as A;
    let mut out = st::HistoryEvent {
        event_id: v.event_id,
        timestamp: v.event_time.map(ms),
        version: v.version,
        task_id: v.task_id,
        ..Default::default()
    };
    match v.attributes {
        Some(A::WorkflowExecutionCompletedEventAttributes(x)) => {
            out.event_type = Some(EventType::WorkflowExecutionCompleted);
            out.workflow_execution_completed_event_attributes =
                Some(tk::WorkflowExecutionCompletedEventAttributes {
                    result: data(x.result),
                    decision_task_completed_event_id: x.decision_task_completed_event_id,
                });
        }
        Some(A::WorkflowExecutionFailedEventAttributes(x)) => {
            let (reason, details, _) = failure_in(x.failure)?;
            out.event_type = Some(EventType::WorkflowExecutionFailed);
            out.workflow_execution_failed_event_attributes =
                Some(tk::WorkflowExecutionFailedEventAttributes {
                    reason,
                    details,
                    decision_task_completed_event_id: x.decision_task_completed_event_id,
                });
        }
        Some(A::WorkflowExecutionCanceledEventAttributes(x)) => {
            out.event_type = Some(EventType::WorkflowExecutionCanceled);
            out.workflow_execution_canceled_event_attributes =
                Some(tk::WorkflowExecutionCanceledEventAttributes {
                    decision_task_completed_event_id: x.decision_task_completed_event_id,
                    details: data(x.details),
                });
        }
        Some(A::WorkflowExecutionTimedOutEventAttributes(x)) => {
            out.event_type = Some(EventType::WorkflowExecutionTimedOut);
            out.workflow_execution_timed_out_event_attributes =
                Some(tk::WorkflowExecutionTimedOutEventAttributes {
                    timeout_type: enum_in(x.timeout_type, e::TimeoutType::from_i32)?,
                });
        }
        Some(A::WorkflowExecutionTerminatedEventAttributes(x)) => {
            out.event_type = Some(EventType::WorkflowExecutionTerminated);
            out.workflow_execution_terminated_event_attributes =
                Some(st::WorkflowExecutionTerminatedEventAttributes {
                    reason: x.reason,
                    details: data(x.details),
                    identity: x.identity,
                });
        }
        None => {}
        Some(_) => {
            return Err(Status::unimplemented(
                "Cross-cluster completion event type has not been ported.",
            ))
        }
    }
    Ok(out)
}

// --- request attributes --------------------------------------------------

fn apply_attrs_in(v: pa::ApplyParentClosePolicyAttributes) -> Result<a::ApplyParentClosePolicyAttributes, Status> {
    Ok(a::ApplyParentClosePolicyAttributes {
        child_domain_id: v.child_domain_id,
        child_workflow_id: v.child_workflow_id,
        child_run_id: v.child_run_id,
        parent_close_policy: enum_in(v.parent_close_policy, e::ParentClosePolicy::from_i32)?,
    })
}
fn apply_attrs_out(v: a::ApplyParentClosePolicyAttributes) -> pa::ApplyParentClosePolicyAttributes {
    pa::ApplyParentClosePolicyAttributes {
        child_domain_id: v.child_domain_id,
        child_workflow_id: v.child_workflow_id,
        child_run_id: v.child_run_id,
        parent_close_policy: v.parent_close_policy.map_or(0, |e| e.value() + 1),
    }
}

pub fn task_request_in(v: pa::CrossClusterTaskRequest) -> Result<a::CrossClusterTaskRequest, Status> {
    let mut out = a::CrossClusterTaskRequest {
        task_info: v
            .task_info
            .map(|i| {
                let (workflow_id, run_id) = exec(i.workflow_execution);
                Ok::<_, Status>(a::CrossClusterTaskInfo {
                    domain_id: i.domain_id,
                    workflow_id,
                    run_id,
                    task_type: task_type_in(i.task_type)?,
                    task_state: i16::try_from(i.task_state)
                        .map_err(|_| Status::invalid_argument("task_state out of range"))?,
                    task_id: i.task_id,
                    visibility_timestamp: i.visibility_timestamp.map(ns),
                })
            })
            .transpose()?,
        ..Default::default()
    };
    match v.attributes {
        Some(ReqAttr::StartChildExecutionAttributes(x)) => {
            out.start_child_execution_attributes =
                Some(a::CrossClusterStartChildExecutionRequestAttributes {
                    target_domain_id: x.target_domain_id,
                    request_id: x.request_id,
                    initiated_event_id: x.initiated_event_id,
                    initiated_event_attributes: x
                        .initiated_event_attributes
                        .map(start_child_initiated_in)
                        .transpose()?,
                    // Go: TargetRunID: &t.TargetRunId (always set).
                    target_run_id: Some(x.target_run_id),
                    // Go mapper passes the map through (nil when empty).
                    partition_config: (!x.partition_config.is_empty())
                        .then_some(x.partition_config),
                });
        }
        Some(ReqAttr::CancelExecutionAttributes(x)) => {
            let (target_workflow_id, target_run_id) = exec(x.target_workflow_execution);
            out.cancel_execution_attributes = Some(a::CrossClusterCancelExecutionRequestAttributes {
                target_domain_id: x.target_domain_id,
                target_workflow_id,
                target_run_id,
                request_id: x.request_id,
                initiated_event_id: x.initiated_event_id,
                child_workflow_only: x.child_workflow_only,
            });
        }
        Some(ReqAttr::SignalExecutionAttributes(x)) => {
            let (target_workflow_id, target_run_id) = exec(x.target_workflow_execution);
            out.signal_execution_attributes = Some(a::CrossClusterSignalExecutionRequestAttributes {
                target_domain_id: x.target_domain_id,
                target_workflow_id,
                target_run_id,
                request_id: x.request_id,
                initiated_event_id: x.initiated_event_id,
                child_workflow_only: x.child_workflow_only,
                signal_name: x.signal_name,
                signal_input: x.signal_input.map(|p| p.data),
                control: (!x.control.is_empty()).then_some(x.control),
            });
        }
        Some(ReqAttr::RecordChildWorkflowExecutionCompleteRequestAttributes(x)) => {
            let (target_workflow_id, target_run_id) = exec(x.target_workflow_execution);
            out.record_child_workflow_execution_complete_attributes = Some(
                a::CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
                    target_domain_id: x.target_domain_id,
                    target_workflow_id,
                    target_run_id,
                    initiated_event_id: x.initiated_event_id,
                    completion_event: x.completion_event.map(completion_event_in).transpose()?,
                },
            );
        }
        Some(ReqAttr::ApplyParentClosePolicyRequestAttributes(x)) => {
            out.apply_parent_close_policy_attributes =
                Some(a::CrossClusterApplyParentClosePolicyRequestAttributes {
                    children: x
                        .children
                        .into_iter()
                        .map(|c| {
                            Ok::<_, Status>(a::ApplyParentClosePolicyRequest {
                                child: c.child.map(apply_attrs_in).transpose()?,
                                status: c
                                    .status
                                    .map(|s| {
                                        Ok::<_, Status>(a::ApplyParentClosePolicyStatus {
                                            completed: s.completed,
                                            failed_cause: failed_cause_in(s.failed_cause)?,
                                        })
                                    })
                                    .transpose()?,
                            })
                        })
                        .collect::<Result<_, _>>()?,
                });
        }
        None => {}
    }
    Ok(out)
}

pub fn task_request_out(v: a::CrossClusterTaskRequest) -> Result<pa::CrossClusterTaskRequest, Status> {
    let attributes = if let Some(x) = v.start_child_execution_attributes {
        Some(ReqAttr::StartChildExecutionAttributes(
            pa::CrossClusterStartChildExecutionRequestAttributes {
                target_domain_id: x.target_domain_id,
                request_id: x.request_id,
                initiated_event_id: x.initiated_event_id,
                initiated_event_attributes: x
                    .initiated_event_attributes
                    .map(start_child_workflow_execution_initiated_event_attributes),
                target_run_id: x.target_run_id.unwrap_or_default(),
                partition_config: x.partition_config.unwrap_or_default(),
            },
        ))
    } else if let Some(x) = v.cancel_execution_attributes {
        Some(ReqAttr::CancelExecutionAttributes(
            pa::CrossClusterCancelExecutionRequestAttributes {
                target_domain_id: x.target_domain_id,
                target_workflow_execution: exec_out(x.target_workflow_id, x.target_run_id),
                request_id: x.request_id,
                initiated_event_id: x.initiated_event_id,
                child_workflow_only: x.child_workflow_only,
            },
        ))
    } else if let Some(x) = v.signal_execution_attributes {
        Some(ReqAttr::SignalExecutionAttributes(
            pa::CrossClusterSignalExecutionRequestAttributes {
                target_domain_id: x.target_domain_id,
                target_workflow_execution: exec_out(x.target_workflow_id, x.target_run_id),
                request_id: x.request_id,
                initiated_event_id: x.initiated_event_id,
                child_workflow_only: x.child_workflow_only,
                signal_name: x.signal_name,
                signal_input: x.signal_input.map(|data| p::Payload { data }),
                control: x.control.unwrap_or_default(),
            },
        ))
    } else if let Some(x) = v.record_child_workflow_execution_complete_attributes {
        Some(ReqAttr::RecordChildWorkflowExecutionCompleteRequestAttributes(
            pa::CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
                target_domain_id: x.target_domain_id,
                target_workflow_execution: exec_out(x.target_workflow_id, x.target_run_id),
                initiated_event_id: x.initiated_event_id,
                completion_event: x.completion_event.map(event).transpose()?,
            },
        ))
    } else {
        v.apply_parent_close_policy_attributes.map(|x| {
            ReqAttr::ApplyParentClosePolicyRequestAttributes(
                pa::CrossClusterApplyParentClosePolicyRequestAttributes {
                    children: x
                        .children
                        .into_iter()
                        .map(|c| pa::ApplyParentClosePolicyRequest {
                            child: c.child.map(apply_attrs_out),
                            status: c.status.map(|s| pa::ApplyParentClosePolicyStatus {
                                completed: s.completed,
                                failed_cause: failed_cause_out(s.failed_cause),
                            }),
                        })
                        .collect(),
                },
            )
        })
    };
    Ok(pa::CrossClusterTaskRequest {
        task_info: v.task_info.map(|i| pa::CrossClusterTaskInfo {
            domain_id: i.domain_id,
            workflow_execution: exec_out(i.workflow_id, i.run_id),
            task_type: task_type_out(i.task_type),
            task_state: i32::from(i.task_state),
            task_id: i.task_id,
            visibility_timestamp: i.visibility_timestamp.map(timestamp_nanos),
        }),
        attributes,
    })
}

pub fn task_requests_in(v: Option<pa::CrossClusterTaskRequests>) -> Result<Vec<a::CrossClusterTaskRequest>, Status> {
    v.map_or_else(|| Ok(Vec::new()), |r| r.task_requests.into_iter().map(task_request_in).collect())
}

pub fn task_requests_out(v: Vec<a::CrossClusterTaskRequest>) -> Result<Option<pa::CrossClusterTaskRequests>, Status> {
    if v.is_empty() {
        return Ok(None);
    }
    Ok(Some(pa::CrossClusterTaskRequests {
        task_requests: v.into_iter().map(task_request_out).collect::<Result<_, _>>()?,
    }))
}

// --- responses -----------------------------------------------------------

pub fn task_response_in(v: pa::CrossClusterTaskResponse) -> Result<a::CrossClusterTaskResponse, Status> {
    let mut out = a::CrossClusterTaskResponse {
        task_id: v.task_id,
        task_type: task_type_in(v.task_type)?,
        task_state: i16::try_from(v.task_state)
            .map_err(|_| Status::invalid_argument("task_state out of range"))?,
        failed_cause: failed_cause_in(v.failed_cause)?,
        ..Default::default()
    };
    match v.attributes {
        Some(RespAttr::StartChildExecutionAttributes(x)) => {
            out.start_child_execution_attributes =
                Some(a::CrossClusterStartChildExecutionResponseAttributes { run_id: x.run_id });
        }
        Some(RespAttr::CancelExecutionAttributes(_)) => {
            out.cancel_execution_attributes = Some(Default::default());
        }
        Some(RespAttr::SignalExecutionAttributes(_)) => {
            out.signal_execution_attributes = Some(Default::default());
        }
        Some(RespAttr::RecordChildWorkflowExecutionCompleteRequestAttributes(_)) => {
            out.record_child_workflow_execution_complete_attributes = Some(Default::default());
        }
        Some(RespAttr::ApplyParentClosePolicyResponseAttributes(x)) => {
            out.apply_parent_close_policy_attributes =
                Some(a::CrossClusterApplyParentClosePolicyResponseAttributes {
                    children_status: x
                        .children_status
                        .into_iter()
                        .map(|c| {
                            Ok::<_, Status>(a::ApplyParentClosePolicyResult {
                                child: c.child.map(apply_attrs_in).transpose()?,
                                failed_cause: failed_cause_in(c.failed_cause)?,
                            })
                        })
                        .collect::<Result<_, _>>()?,
                });
        }
        None => {}
    }
    Ok(out)
}

pub fn task_response_out(v: a::CrossClusterTaskResponse) -> pa::CrossClusterTaskResponse {
    let attributes = if let Some(x) = v.start_child_execution_attributes {
        Some(RespAttr::StartChildExecutionAttributes(
            pa::CrossClusterStartChildExecutionResponseAttributes { run_id: x.run_id },
        ))
    } else if v.cancel_execution_attributes.is_some() {
        Some(RespAttr::CancelExecutionAttributes(Default::default()))
    } else if v.signal_execution_attributes.is_some() {
        Some(RespAttr::SignalExecutionAttributes(Default::default()))
    } else if v.record_child_workflow_execution_complete_attributes.is_some() {
        Some(RespAttr::RecordChildWorkflowExecutionCompleteRequestAttributes(
            Default::default(),
        ))
    } else {
        v.apply_parent_close_policy_attributes.map(|x| {
            RespAttr::ApplyParentClosePolicyResponseAttributes(
                pa::CrossClusterApplyParentClosePolicyResponseAttributes {
                    children_status: x
                        .children_status
                        .into_iter()
                        .map(|c| pa::ApplyParentClosePolicyResult {
                            child: c.child.map(apply_attrs_out),
                            failed_cause: failed_cause_out(c.failed_cause),
                        })
                        .collect(),
                },
            )
        })
    };
    pa::CrossClusterTaskResponse {
        task_id: v.task_id,
        task_type: task_type_out(v.task_type),
        task_state: i32::from(v.task_state),
        failed_cause: failed_cause_out(v.failed_cause),
        attributes,
    }
}

pub fn tasks_by_shard_in(
    v: HashMap<i32, pa::CrossClusterTaskRequests>,
) -> Result<HashMap<i32, Vec<a::CrossClusterTaskRequest>>, Status> {
    v.into_iter()
        .map(|(k, r)| Ok((k, task_requests_in(Some(r))?)))
        .collect()
}

pub fn tasks_by_shard_out(
    v: HashMap<i32, Vec<a::CrossClusterTaskRequest>>,
) -> Result<HashMap<i32, pa::CrossClusterTaskRequests>, Status> {
    v.into_iter()
        .map(|(k, r)| {
            Ok((
                k,
                pa::CrossClusterTaskRequests {
                    task_requests: r.into_iter().map(task_request_out).collect::<Result<_, _>>()?,
                },
            ))
        })
        .collect()
}

pub fn failed_causes_in(v: HashMap<i32, i32>) -> Result<HashMap<i32, e::GetTaskFailedCause>, Status> {
    let mut out = HashMap::new();
    for (k, c) in v {
        // Go drops INVALID (nil) causes from the map.
        if let Some(cause) = enum_in(c, e::GetTaskFailedCause::from_i32)? {
            out.insert(k, cause);
        }
    }
    Ok(out)
}

pub fn failed_causes_out(v: HashMap<i32, e::GetTaskFailedCause>) -> HashMap<i32, i32> {
    v.into_iter().map(|(k, c)| (k, c.value() + 1)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requests() -> Vec<a::CrossClusterTaskRequest> {
        let info = a::CrossClusterTaskInfo {
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            task_type: Some(e::CrossClusterTaskType::SignalExecution),
            task_state: 1,
            task_id: 9,
            visibility_timestamp: Some(1_700_000_000_123_456_789),
        };
        vec![
            a::CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                start_child_execution_attributes: Some(
                    a::CrossClusterStartChildExecutionRequestAttributes {
                        target_domain_id: "td".into(),
                        request_id: "rq".into(),
                        initiated_event_id: 4,
                        initiated_event_attributes: Some(
                            tk::StartChildWorkflowExecutionInitiatedEventAttributes {
                                domain: "dn".into(),
                                workflow_id: "c".into(),
                                execution_start_to_close_timeout_seconds: Some(10),
                                parent_close_policy: Some(e::ParentClosePolicy::Terminate),
                                ..Default::default()
                            },
                        ),
                        target_run_id: Some("tr".into()),
                        partition_config: Some(HashMap::from([("a".into(), "b".into())])),
                    },
                ),
                ..Default::default()
            },
            a::CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                signal_execution_attributes: Some(a::CrossClusterSignalExecutionRequestAttributes {
                    target_domain_id: "td".into(),
                    target_workflow_id: "tw".into(),
                    target_run_id: "tr".into(),
                    signal_name: "s".into(),
                    signal_input: Some(b"i".to_vec()),
                    control: Some(b"c".to_vec()),
                    child_workflow_only: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
            a::CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                cancel_execution_attributes: Some(a::CrossClusterCancelExecutionRequestAttributes {
                    target_domain_id: "td".into(),
                    target_workflow_id: "tw".into(),
                    target_run_id: "tr".into(),
                    request_id: "q".into(),
                    initiated_event_id: 2,
                    child_workflow_only: false,
                }),
                ..Default::default()
            },
            a::CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                record_child_workflow_execution_complete_attributes: Some(
                    a::CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
                        target_domain_id: "td".into(),
                        target_workflow_id: "tw".into(),
                        target_run_id: "tr".into(),
                        initiated_event_id: 3,
                        completion_event: Some(st::HistoryEvent {
                            event_id: 7,
                            timestamp: Some(1_000),
                            event_type: Some(EventType::WorkflowExecutionTerminated),
                            version: 2,
                            task_id: 3,
                            workflow_execution_terminated_event_attributes: Some(
                                st::WorkflowExecutionTerminatedEventAttributes {
                                    reason: "why".into(),
                                    details: b"d".to_vec(),
                                    identity: "me".into(),
                                },
                            ),
                            ..Default::default()
                        }),
                    },
                ),
                ..Default::default()
            },
            a::CrossClusterTaskRequest {
                task_info: Some(info),
                apply_parent_close_policy_attributes: Some(
                    a::CrossClusterApplyParentClosePolicyRequestAttributes {
                        children: vec![a::ApplyParentClosePolicyRequest {
                            child: Some(a::ApplyParentClosePolicyAttributes {
                                child_domain_id: "cd".into(),
                                child_workflow_id: "cw".into(),
                                child_run_id: "cr".into(),
                                parent_close_policy: Some(e::ParentClosePolicy::Abandon),
                            }),
                            status: Some(a::ApplyParentClosePolicyStatus {
                                completed: true,
                                failed_cause: Some(
                                    e::CrossClusterTaskFailedCause::WorkflowAlreadyCompleted,
                                ),
                            }),
                        }],
                    },
                ),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn task_requests_round_trip() {
        for r in requests() {
            let proto = task_request_out(r.clone()).unwrap();
            assert_eq!(task_request_in(proto).unwrap(), r);
        }
        let map = HashMap::from([(5, requests())]);
        assert_eq!(tasks_by_shard_in(tasks_by_shard_out(map.clone()).unwrap()).unwrap(), map);
    }

    #[test]
    fn task_responses_round_trip() {
        let responses = vec![
            a::CrossClusterTaskResponse {
                task_id: 1,
                task_type: Some(e::CrossClusterTaskType::StartChildExecution),
                task_state: 2,
                failed_cause: Some(e::CrossClusterTaskFailedCause::DomainNotExists),
                start_child_execution_attributes: Some(
                    a::CrossClusterStartChildExecutionResponseAttributes { run_id: "r".into() },
                ),
                ..Default::default()
            },
            a::CrossClusterTaskResponse {
                task_id: 2,
                cancel_execution_attributes: Some(Default::default()),
                ..Default::default()
            },
            a::CrossClusterTaskResponse {
                task_id: 3,
                signal_execution_attributes: Some(Default::default()),
                ..Default::default()
            },
            a::CrossClusterTaskResponse {
                task_id: 4,
                record_child_workflow_execution_complete_attributes: Some(Default::default()),
                ..Default::default()
            },
            a::CrossClusterTaskResponse {
                task_id: 5,
                apply_parent_close_policy_attributes: Some(
                    a::CrossClusterApplyParentClosePolicyResponseAttributes {
                        children_status: vec![a::ApplyParentClosePolicyResult {
                            child: Some(Default::default()),
                            failed_cause: Some(e::CrossClusterTaskFailedCause::Uncategorized),
                        }],
                    },
                ),
                ..Default::default()
            },
        ];
        for r in responses {
            assert_eq!(task_response_in(task_response_out(r.clone())).unwrap(), r);
        }
    }

    #[test]
    fn enums_are_offset_by_one_and_invalid_causes_dropped() {
        let proto = task_response_out(a::CrossClusterTaskResponse {
            task_type: Some(e::CrossClusterTaskType::StartChildExecution),
            failed_cause: Some(e::CrossClusterTaskFailedCause::DomainNotActive),
            ..Default::default()
        });
        assert_eq!((proto.task_type, proto.failed_cause), (1, 1));
        let causes = failed_causes_in(HashMap::from([(1, 3), (2, 0)])).unwrap();
        assert_eq!(causes, HashMap::from([(1, e::GetTaskFailedCause::ShardOwnershipLost)]));
        assert_eq!(failed_causes_out(causes), HashMap::from([(1, 3)]));
    }

    #[test]
    fn non_close_completion_event_is_rejected() {
        let ev = p::HistoryEvent {
            attributes: Some(p::history_event::Attributes::TimerFiredEventAttributes(
                Default::default(),
            )),
            ..Default::default()
        };
        assert!(completion_event_in(ev).is_err());
    }
}
