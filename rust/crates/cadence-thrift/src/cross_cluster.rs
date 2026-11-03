//! Thrift converters for the cross-cluster task API (`shared.thrift`
//! CrossClusterTask*, ApplyParentClosePolicy*, Get/RespondCrossClusterTasks*).
//! Field presence follows Go's `common/types/mapper/thrift/shared.go`:
//! Go value fields are always written, pointer fields only when set.

use crate::protocol::{Error, Result};
use crate::value::{Fields, FromThrift, Struct, ToThrift, Value};
use cadence_api::admin::*;
use std::collections::HashMap;

fn put_str(f: &mut Fields, id: i16, v: &str) {
    f.fields.push((id, Value::Bytes(v.as_bytes().to_vec())));
}
fn put_i64(f: &mut Fields, id: i16, v: i64) {
    f.fields.push((id, Value::I64(v)));
}
fn put_i16(f: &mut Fields, id: i16, v: i16) {
    f.fields.push((id, Value::I16(v)));
}
fn put_bool(f: &mut Fields, id: i16, v: bool) {
    f.fields.push((id, Value::Bool(v)));
}
fn put_list<T: ToThrift>(f: &mut Fields, id: i16, v: &[T]) {
    // Go maps a nil slice to an absent field; an empty slice is never
    // distinguishable on the Rust side, so absent is used for both.
    if !v.is_empty() {
        f.fields.push((
            id,
            Value::List(v.iter().map(|x| Value::Struct(x.to_thrift())).collect()),
        ));
    }
}
fn get_i16(s: &Struct, id: i16) -> Result<i16> {
    match s.get(id) {
        None => Ok(0),
        Some(Value::I16(v)) => Ok(*v),
        Some(v) => Err(Error::Invalid(format!("field {id}: expected i16, got {v:?}"))),
    }
}
fn str_or_empty(s: &Struct, id: i16) -> Result<String> {
    Ok(s.opt_string(id)?.unwrap_or_default())
}

impl ToThrift for CrossClusterTaskInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.domain_id);
        put_str(&mut f, 20, &self.workflow_id);
        put_str(&mut f, 30, &self.run_id);
        f.oenum(40, self.task_type);
        put_i16(&mut f, 50, self.task_state);
        put_i64(&mut f, 60, self.task_id);
        f.oi64(70, self.visibility_timestamp);
        f.finish()
    }
}
impl FromThrift for CrossClusterTaskInfo {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_id: str_or_empty(s, 10)?,
            workflow_id: str_or_empty(s, 20)?,
            run_id: str_or_empty(s, 30)?,
            task_type: s.opt_enum(40)?,
            task_state: get_i16(s, 50)?,
            task_id: s.i64_or_zero(60)?,
            visibility_timestamp: s.opt_i64(70)?,
        })
    }
}

impl ToThrift for CrossClusterStartChildExecutionRequestAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.target_domain_id);
        put_str(&mut f, 20, &self.request_id);
        put_i64(&mut f, 30, self.initiated_event_id);
        f.ostruct(40, self.initiated_event_attributes.as_ref());
        f.ostr(50, &self.target_run_id);
        if let Some(m) = &self.partition_config {
            let mut items: Vec<(Value, Value)> = m
                .iter()
                .map(|(k, v)| {
                    (
                        Value::Bytes(k.as_bytes().to_vec()),
                        Value::Bytes(v.as_bytes().to_vec()),
                    )
                })
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            f.fields.push((60, Value::Map(items)));
        }
        f.finish()
    }
}
impl FromThrift for CrossClusterStartChildExecutionRequestAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            target_domain_id: str_or_empty(s, 10)?,
            request_id: str_or_empty(s, 20)?,
            initiated_event_id: s.i64_or_zero(30)?,
            initiated_event_attributes: s.opt_struct(40)?,
            target_run_id: s.opt_string(50)?,
            partition_config: s.opt_map_str(60)?,
        })
    }
}

impl ToThrift for CrossClusterStartChildExecutionResponseAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.run_id);
        f.finish()
    }
}
impl FromThrift for CrossClusterStartChildExecutionResponseAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            run_id: str_or_empty(s, 10)?,
        })
    }
}

impl ToThrift for CrossClusterCancelExecutionRequestAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.target_domain_id);
        put_str(&mut f, 20, &self.target_workflow_id);
        put_str(&mut f, 30, &self.target_run_id);
        put_str(&mut f, 40, &self.request_id);
        put_i64(&mut f, 50, self.initiated_event_id);
        put_bool(&mut f, 60, self.child_workflow_only);
        f.finish()
    }
}
impl FromThrift for CrossClusterCancelExecutionRequestAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            target_domain_id: str_or_empty(s, 10)?,
            target_workflow_id: str_or_empty(s, 20)?,
            target_run_id: str_or_empty(s, 30)?,
            request_id: str_or_empty(s, 40)?,
            initiated_event_id: s.i64_or_zero(50)?,
            child_workflow_only: s.bool_or_false(60)?,
        })
    }
}

macro_rules! empty_struct {
    ($($t:ty),*) => {$(
        impl ToThrift for $t {
            fn to_thrift(&self) -> Struct {
                Fields::new().finish()
            }
        }
        impl FromThrift for $t {
            fn from_thrift(_: &Struct) -> Result<Self> {
                Ok(Self {})
            }
        }
    )*};
}
empty_struct!(
    CrossClusterCancelExecutionResponseAttributes,
    CrossClusterSignalExecutionResponseAttributes,
    CrossClusterRecordChildWorkflowExecutionCompleteResponseAttributes
);

impl ToThrift for CrossClusterSignalExecutionRequestAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.target_domain_id);
        put_str(&mut f, 20, &self.target_workflow_id);
        put_str(&mut f, 30, &self.target_run_id);
        put_str(&mut f, 40, &self.request_id);
        put_i64(&mut f, 50, self.initiated_event_id);
        put_bool(&mut f, 60, self.child_workflow_only);
        put_str(&mut f, 70, &self.signal_name);
        f.obytes(80, &self.signal_input);
        f.obytes(90, &self.control);
        f.finish()
    }
}
impl FromThrift for CrossClusterSignalExecutionRequestAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            target_domain_id: str_or_empty(s, 10)?,
            target_workflow_id: str_or_empty(s, 20)?,
            target_run_id: str_or_empty(s, 30)?,
            request_id: str_or_empty(s, 40)?,
            initiated_event_id: s.i64_or_zero(50)?,
            child_workflow_only: s.bool_or_false(60)?,
            signal_name: str_or_empty(s, 70)?,
            signal_input: s.opt_bytes(80)?,
            control: s.opt_bytes(90)?,
        })
    }
}

impl ToThrift for CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.target_domain_id);
        put_str(&mut f, 20, &self.target_workflow_id);
        put_str(&mut f, 30, &self.target_run_id);
        put_i64(&mut f, 40, self.initiated_event_id);
        f.ostruct(50, self.completion_event.as_ref());
        f.finish()
    }
}
impl FromThrift for CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            target_domain_id: str_or_empty(s, 10)?,
            target_workflow_id: str_or_empty(s, 20)?,
            target_run_id: str_or_empty(s, 30)?,
            initiated_event_id: s.i64_or_zero(40)?,
            completion_event: s.opt_struct(50)?,
        })
    }
}

impl ToThrift for ApplyParentClosePolicyAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_str(&mut f, 10, &self.child_domain_id);
        put_str(&mut f, 20, &self.child_workflow_id);
        put_str(&mut f, 30, &self.child_run_id);
        f.oenum(40, self.parent_close_policy);
        f.finish()
    }
}
impl FromThrift for ApplyParentClosePolicyAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            child_domain_id: str_or_empty(s, 10)?,
            child_workflow_id: str_or_empty(s, 20)?,
            child_run_id: str_or_empty(s, 30)?,
            parent_close_policy: s.opt_enum(40)?,
        })
    }
}

impl ToThrift for ApplyParentClosePolicyStatus {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_bool(&mut f, 10, self.completed);
        f.oenum(20, self.failed_cause);
        f.finish()
    }
}
impl FromThrift for ApplyParentClosePolicyStatus {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            completed: s.bool_or_false(10)?,
            failed_cause: s.opt_enum(20)?,
        })
    }
}

impl ToThrift for ApplyParentClosePolicyRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.child.as_ref());
        f.ostruct(20, self.status.as_ref());
        f.finish()
    }
}
impl FromThrift for ApplyParentClosePolicyRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            child: s.opt_struct(10)?,
            status: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for ApplyParentClosePolicyResult {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.child.as_ref());
        f.oenum(20, self.failed_cause);
        f.finish()
    }
}
impl FromThrift for ApplyParentClosePolicyResult {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            child: s.opt_struct(10)?,
            failed_cause: s.opt_enum(20)?,
        })
    }
}

impl ToThrift for CrossClusterApplyParentClosePolicyRequestAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_list(&mut f, 10, &self.children);
        f.finish()
    }
}
impl FromThrift for CrossClusterApplyParentClosePolicyRequestAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            children: s.opt_list(10)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for CrossClusterApplyParentClosePolicyResponseAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_list(&mut f, 10, &self.children_status);
        f.finish()
    }
}
impl FromThrift for CrossClusterApplyParentClosePolicyResponseAttributes {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            children_status: s.opt_list(10)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for CrossClusterTaskRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.task_info.as_ref());
        f.ostruct(20, self.start_child_execution_attributes.as_ref());
        f.ostruct(30, self.cancel_execution_attributes.as_ref());
        f.ostruct(40, self.signal_execution_attributes.as_ref());
        f.ostruct(
            50,
            self.record_child_workflow_execution_complete_attributes
                .as_ref(),
        );
        f.ostruct(60, self.apply_parent_close_policy_attributes.as_ref());
        f.finish()
    }
}
impl FromThrift for CrossClusterTaskRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            task_info: s.opt_struct(10)?,
            start_child_execution_attributes: s.opt_struct(20)?,
            cancel_execution_attributes: s.opt_struct(30)?,
            signal_execution_attributes: s.opt_struct(40)?,
            record_child_workflow_execution_complete_attributes: s.opt_struct(50)?,
            apply_parent_close_policy_attributes: s.opt_struct(60)?,
        })
    }
}

impl ToThrift for CrossClusterTaskResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_i64(&mut f, 10, self.task_id);
        f.oenum(20, self.task_type);
        put_i16(&mut f, 30, self.task_state);
        f.oenum(40, self.failed_cause);
        f.ostruct(50, self.start_child_execution_attributes.as_ref());
        f.ostruct(60, self.cancel_execution_attributes.as_ref());
        f.ostruct(70, self.signal_execution_attributes.as_ref());
        f.ostruct(
            80,
            self.record_child_workflow_execution_complete_attributes
                .as_ref(),
        );
        f.ostruct(90, self.apply_parent_close_policy_attributes.as_ref());
        f.finish()
    }
}
impl FromThrift for CrossClusterTaskResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            task_id: s.i64_or_zero(10)?,
            task_type: s.opt_enum(20)?,
            task_state: get_i16(s, 30)?,
            failed_cause: s.opt_enum(40)?,
            start_child_execution_attributes: s.opt_struct(50)?,
            cancel_execution_attributes: s.opt_struct(60)?,
            signal_execution_attributes: s.opt_struct(70)?,
            record_child_workflow_execution_complete_attributes: s.opt_struct(80)?,
            apply_parent_close_policy_attributes: s.opt_struct(90)?,
        })
    }
}

impl ToThrift for GetCrossClusterTasksRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if !self.shard_ids.is_empty() {
            f.fields.push((
                10,
                Value::List(self.shard_ids.iter().map(|x| Value::I32(*x)).collect()),
            ));
        }
        put_str(&mut f, 20, &self.target_cluster);
        f.finish()
    }
}
impl FromThrift for GetCrossClusterTasksRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            shard_ids: s.opt_list(10)?.unwrap_or_default(),
            target_cluster: str_or_empty(s, 20)?,
        })
    }
}

fn sorted_i32_map(mut items: Vec<(i32, Value)>) -> Value {
    items.sort_by_key(|(k, _)| *k);
    Value::Map(items.into_iter().map(|(k, v)| (Value::I32(k), v)).collect())
}

impl ToThrift for GetCrossClusterTasksResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if !self.tasks_by_shard.is_empty() {
            f.fields.push((
                10,
                sorted_i32_map(
                    self.tasks_by_shard
                        .iter()
                        .map(|(k, v)| {
                            (
                                *k,
                                Value::List(
                                    v.iter().map(|t| Value::Struct(t.to_thrift())).collect(),
                                ),
                            )
                        })
                        .collect(),
                ),
            ));
        }
        if !self.failed_cause_by_shard.is_empty() {
            f.fields.push((
                20,
                sorted_i32_map(
                    self.failed_cause_by_shard
                        .iter()
                        .map(|(k, v)| (*k, Value::I32(v.value())))
                        .collect(),
                ),
            ));
        }
        f.finish()
    }
}
impl FromThrift for GetCrossClusterTasksResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        let mut out = Self::default();
        if let Some(value) = s.get(10) {
            let Value::Map(entries) = value else {
                return Err(Error::Invalid(format!("field 10: expected map, got {value:?}")));
            };
            for (k, v) in entries {
                let Value::I32(shard) = k else {
                    return Err(Error::Invalid(format!("tasksByShard key: {k:?}")));
                };
                let Value::List(items) = v else {
                    return Err(Error::Invalid(format!("tasksByShard value: {v:?}")));
                };
                let mut tasks = Vec::with_capacity(items.len());
                for item in items {
                    let Value::Struct(t) = item else {
                        return Err(Error::Invalid(format!("task request: {item:?}")));
                    };
                    tasks.push(CrossClusterTaskRequest::from_thrift(t)?);
                }
                out.tasks_by_shard.insert(*shard, tasks);
            }
        }
        if let Some(value) = s.get(20) {
            let Value::Map(entries) = value else {
                return Err(Error::Invalid(format!("field 20: expected map, got {value:?}")));
            };
            let mut causes = HashMap::new();
            for (k, v) in entries {
                let (Value::I32(shard), Value::I32(cause)) = (k, v) else {
                    return Err(Error::Invalid(format!("failedCauseByShard entry: {k:?}")));
                };
                let cause = cadence_api::enums::GetTaskFailedCause::from_i32(*cause)
                    .ok_or_else(|| Error::Invalid(format!("unknown GetTaskFailedCause {cause}")))?;
                causes.insert(*shard, cause);
            }
            out.failed_cause_by_shard = causes;
        }
        Ok(out)
    }
}

impl ToThrift for RespondCrossClusterTasksCompletedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields.push((10, Value::I32(self.shard_id)));
        put_str(&mut f, 20, &self.target_cluster);
        put_list(&mut f, 30, &self.task_responses);
        put_bool(&mut f, 40, self.fetch_new_tasks);
        f.finish()
    }
}
impl FromThrift for RespondCrossClusterTasksCompletedRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            shard_id: s.opt_i32(10)?.unwrap_or_default(),
            target_cluster: str_or_empty(s, 20)?,
            task_responses: s.opt_list(30)?.unwrap_or_default(),
            fetch_new_tasks: s.bool_or_false(40)?,
        })
    }
}

impl ToThrift for RespondCrossClusterTasksCompletedResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        put_list(&mut f, 10, &self.tasks);
        f.finish()
    }
}
impl FromThrift for RespondCrossClusterTasksCompletedResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            tasks: s.opt_list(10)?.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums::{
        CrossClusterTaskFailedCause, CrossClusterTaskType, EventType, GetTaskFailedCause,
        ParentClosePolicy,
    };

    fn round_trip<T: ToThrift + FromThrift + PartialEq + std::fmt::Debug>(v: T) {
        // Encode to bytes and back, so wire types are checked too.
        let wire = v.to_thrift();
        let mut w = crate::protocol::Writer::new();
        crate::value::write_struct(&mut w, &wire);
        let bytes = w.into_bytes();
        let read = crate::value::read_struct(&mut crate::protocol::Reader::new(&bytes)).unwrap();
        assert_eq!(T::from_thrift(&read).unwrap(), v);
    }

    fn sample_requests() -> Vec<CrossClusterTaskRequest> {
        let info = CrossClusterTaskInfo {
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            task_type: Some(CrossClusterTaskType::StartChildExecution),
            task_state: 2,
            task_id: 77,
            visibility_timestamp: Some(1_700_000_000_123_456_789),
        };
        vec![
            CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                start_child_execution_attributes: Some(
                    CrossClusterStartChildExecutionRequestAttributes {
                        target_domain_id: "td".into(),
                        request_id: "req".into(),
                        initiated_event_id: 5,
                        initiated_event_attributes: Some(
                            cadence_api::tasks::StartChildWorkflowExecutionInitiatedEventAttributes {
                                domain: "td-name".into(),
                                workflow_id: "child".into(),
                                ..Default::default()
                            },
                        ),
                        target_run_id: Some("trun".into()),
                        partition_config: Some(HashMap::from([("k".into(), "v".into())])),
                    },
                ),
                ..Default::default()
            },
            CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                cancel_execution_attributes: Some(CrossClusterCancelExecutionRequestAttributes {
                    target_domain_id: "td".into(),
                    target_workflow_id: "tw".into(),
                    target_run_id: "tr".into(),
                    request_id: "rq".into(),
                    initiated_event_id: 9,
                    child_workflow_only: true,
                }),
                ..Default::default()
            },
            CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                signal_execution_attributes: Some(CrossClusterSignalExecutionRequestAttributes {
                    target_domain_id: "td".into(),
                    signal_name: "sig".into(),
                    signal_input: Some(b"in".to_vec()),
                    control: Some(b"ctl".to_vec()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            CrossClusterTaskRequest {
                task_info: Some(info.clone()),
                record_child_workflow_execution_complete_attributes: Some(
                    CrossClusterRecordChildWorkflowExecutionCompleteRequestAttributes {
                        target_domain_id: "td".into(),
                        initiated_event_id: 3,
                        completion_event: Some(cadence_api::service_types::HistoryEvent {
                            event_id: 12,
                            // HistoryEvent thrift timestamps are not symmetric (ms out, ns in).
                            timestamp: None,
                            event_type: Some(EventType::WorkflowExecutionCompleted),
                            version: 1,
                            task_id: 2,
                            workflow_execution_completed_event_attributes: Some(
                                cadence_api::tasks::WorkflowExecutionCompletedEventAttributes {
                                    result: b"ok".to_vec(),
                                    decision_task_completed_event_id: 11,
                                },
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            CrossClusterTaskRequest {
                task_info: Some(info),
                apply_parent_close_policy_attributes: Some(
                    CrossClusterApplyParentClosePolicyRequestAttributes {
                        children: vec![ApplyParentClosePolicyRequest {
                            child: Some(ApplyParentClosePolicyAttributes {
                                child_domain_id: "cd".into(),
                                child_workflow_id: "cw".into(),
                                child_run_id: "cr".into(),
                                parent_close_policy: Some(ParentClosePolicy::Terminate),
                            }),
                            status: Some(ApplyParentClosePolicyStatus {
                                completed: true,
                                failed_cause: Some(CrossClusterTaskFailedCause::DomainNotActive),
                            }),
                        }],
                    },
                ),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn get_cross_cluster_tasks_round_trips() {
        round_trip(GetCrossClusterTasksRequest {
            shard_ids: vec![1, 2],
            target_cluster: "c".into(),
        });
        round_trip(GetCrossClusterTasksResponse {
            tasks_by_shard: HashMap::from([(3, sample_requests())]),
            failed_cause_by_shard: HashMap::from([(4, GetTaskFailedCause::ShardOwnershipLost)]),
        });
    }

    #[test]
    fn respond_cross_cluster_tasks_round_trips() {
        round_trip(RespondCrossClusterTasksCompletedRequest {
            shard_id: 3,
            target_cluster: "c".into(),
            task_responses: vec![
                CrossClusterTaskResponse {
                    task_id: 1,
                    task_type: Some(CrossClusterTaskType::StartChildExecution),
                    task_state: 1,
                    start_child_execution_attributes: Some(
                        CrossClusterStartChildExecutionResponseAttributes { run_id: "r".into() },
                    ),
                    ..Default::default()
                },
                CrossClusterTaskResponse {
                    task_id: 2,
                    task_type: Some(CrossClusterTaskType::ApplyParentClosePolicy),
                    failed_cause: Some(CrossClusterTaskFailedCause::WorkflowNotExists),
                    apply_parent_close_policy_attributes: Some(
                        CrossClusterApplyParentClosePolicyResponseAttributes {
                            children_status: vec![ApplyParentClosePolicyResult {
                                child: Some(ApplyParentClosePolicyAttributes::default()),
                                failed_cause: Some(CrossClusterTaskFailedCause::Uncategorized),
                            }],
                        },
                    ),
                    ..Default::default()
                },
                CrossClusterTaskResponse {
                    task_id: 3,
                    cancel_execution_attributes: Some(Default::default()),
                    signal_execution_attributes: Some(Default::default()),
                    record_child_workflow_execution_complete_attributes: Some(Default::default()),
                    ..Default::default()
                },
            ],
            fetch_new_tasks: true,
        });
        round_trip(RespondCrossClusterTasksCompletedResponse {
            tasks: sample_requests(),
        });
    }

    #[test]
    fn value_fields_are_written_like_go() {
        // Go writes value-typed fields even when zero (&t.Field).
        let s = CrossClusterTaskInfo::default().to_thrift();
        for id in [10, 20, 30, 50, 60] {
            assert!(s.get(id).is_some(), "field {id}");
        }
        assert!(s.get(40).is_none() && s.get(70).is_none());
    }
}
