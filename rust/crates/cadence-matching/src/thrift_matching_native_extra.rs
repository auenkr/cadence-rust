//! Byte-valued Thrift request decoders for Matching methods beyond decision
//! polling. Field IDs follow `thrift_matching.rs::dispatch_inner`; nested API
//! field IDs follow the pinned Thrift descriptors consumed by `cadence-thrift`.
#![allow(dead_code)] // Parent registers this isolated adapter with native handlers.

use cadence_common::go_string::GoString;
use cadence_proto::native::{api as a, matching as m};
use cadence_thrift::{
    value::{FromValue, Struct, Value},
    Error,
};

fn raw(s: &Struct, id: i16) -> Result<GoString, Error> {
    Ok(s.fields
        .iter()
        .rev()
        .find_map(|(f, v)| {
            if *f == id {
                GoString::from_value(v).ok()
            } else {
                None
            }
        })
        .unwrap_or_default())
}
fn integer(s: &Struct, id: i16) -> Option<i32> {
    s.fields.iter().rev().find_map(|(f, v)| {
        if *f == id {
            if let Value::I32(x) = v {
                Some(*x)
            } else {
                None
            }
        } else {
            None
        }
    })
}
fn int64(s: &Struct, id: i16) -> i64 {
    optional_i64(s, id).unwrap_or_default()
}
fn optional_i64(s: &Struct, id: i16) -> Option<i64> {
    s.fields.iter().rev().find_map(|(f, v)| {
        if *f == id {
            if let Value::I64(x) = v {
                Some(*x)
            } else {
                None
            }
        } else {
            None
        }
    })
}
fn obj(s: &Struct, id: i16) -> Option<&Struct> {
    s.fields.iter().rev().find_map(|(f, v)| {
        if *f == id {
            if let Value::Struct(x) = v {
                Some(x)
            } else {
                None
            }
        } else {
            None
        }
    })
}
fn list(s: &Struct) -> a::TaskList {
    a::TaskList {
        name: raw(s, 10).unwrap_or_default(),
        kind: integer(s, 20).map_or(0, |v| v.wrapping_add(1)),
        base_name: raw(s, 30).unwrap_or_default(),
    }
}
fn execution(s: &Struct) -> a::WorkflowExecution {
    a::WorkflowExecution {
        workflow_id: raw(s, 10).unwrap_or_default(),
        run_id: raw(s, 20).unwrap_or_default(),
    }
}
fn byte_map(s: &Struct, id: i16) -> Result<std::collections::HashMap<GoString, GoString>, Error> {
    let Some(entries) = s.fields.iter().rev().find_map(|(f, v)| {
        if *f == id {
            if let Value::Map(entries) = v {
                Some(entries)
            } else {
                None
            }
        } else {
            None
        }
    }) else {
        return Ok(Default::default());
    };
    let mut out = std::collections::HashMap::new();
    if entries
        .iter()
        .any(|(k, v)| !matches!((k, v), (Value::Bytes(_), Value::Bytes(_))))
    {
        return Ok(out);
    }
    for (k, v) in entries {
        if let (Ok(k), Ok(v)) = (GoString::from_value(k), GoString::from_value(v)) {
            out.insert(k, v);
        }
    }
    Ok(out)
}
fn duration(s: &Struct, id: i16) -> Option<prost_types::Duration> {
    integer(s, id).map(|seconds| prost_types::Duration {
        seconds: seconds.into(),
        nanos: 0,
    })
}
fn payload(s: &Struct, id: i16) -> Option<a::Payload> {
    s.fields
        .iter()
        .rev()
        .find_map(|(f, v)| {
            if *f == id {
                match v {
                    Value::Bytes(b) => Some(b),
                    _ => None,
                }
            } else {
                None
            }
        })
        .filter(|b| !b.is_empty())
        .map(|b| a::Payload { data: b.clone() })
}
fn timestamp(s: &Struct, id: i16) -> Option<prost_types::Timestamp> {
    optional_i64(s, id).map(|nanos| prost_types::Timestamp {
        seconds: nanos.div_euclid(1_000_000_000),
        nanos: nanos.rem_euclid(1_000_000_000) as i32,
    })
}

fn float(s: &Struct, id: i16) -> f64 {
    s.fields
        .iter()
        .rev()
        .find_map(|(f, v)| match v {
            Value::F64(value) if *f == id => Some(*value),
            _ => None,
        })
        .unwrap_or_default()
}

fn retry_policy(s: &Struct) -> a::RetryPolicy {
    let reasons = s
        .fields
        .iter()
        .rev()
        .find_map(|(f, v)| match v {
            Value::List(values) if *f == 50 => Some(values),
            _ => None,
        })
        .and_then(|values| {
            values
                .iter()
                .map(GoString::from_value)
                .collect::<Result<Vec<_>, _>>()
                .ok()
        })
        .unwrap_or_default();
    // Go's types model makes retry intervals scalar before protobuf mapping,
    // so an absent interval still produces a present zero Duration.
    let interval = |id| {
        Some(prost_types::Duration {
            seconds: i64::from(integer(s, id).unwrap_or_default()),
            nanos: 0,
        })
    };
    a::RetryPolicy {
        initial_interval: interval(10),
        backoff_coefficient: float(s, 20),
        maximum_interval: interval(30),
        maximum_attempts: integer(s, 40).unwrap_or_default(),
        non_retryable_error_reasons: reasons,
        expiration_interval: interval(60),
        jitter_coefficient: float(s, 70),
    }
}

fn scheduled_activity_event(s: &Struct) -> Result<a::HistoryEvent, Error> {
    if integer(s, 30) != Some(9) {
        return Err(Error::Invalid(
            "Native dispatch only decodes activity-scheduled history events".into(),
        ));
    }
    let attributes = obj(s, 130)
        .ok_or_else(|| Error::Invalid("History event attributes are missing.".into()))?;
    let header = obj(attributes, 120)
        .map(|header| {
            Ok::<_, Error>(a::Header {
                fields: byte_map(header, 10)?
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            key,
                            a::Payload {
                                data: value.into_bytes(),
                            },
                        )
                    })
                    .collect(),
            })
        })
        .transpose()?;
    let attributes = a::ActivityTaskScheduledEventAttributes {
        activity_id: raw(attributes, 10)?,
        activity_type: obj(attributes, 20).map(|value| a::ActivityType {
            name: raw(value, 10).unwrap_or_default(),
        }),
        domain: raw(attributes, 25)?,
        task_list: obj(attributes, 30).map(list),
        input: payload(attributes, 40),
        schedule_to_close_timeout: duration(attributes, 45),
        schedule_to_start_timeout: duration(attributes, 50),
        start_to_close_timeout: duration(attributes, 55),
        heartbeat_timeout: duration(attributes, 60),
        decision_task_completed_event_id: int64(attributes, 90),
        retry_policy: obj(attributes, 110).map(retry_policy),
        header,
    };
    Ok(a::HistoryEvent {
        event_id: int64(s, 10),
        event_time: timestamp(s, 20),
        version: int64(s, 35),
        task_id: int64(s, 36),
        attributes: Some(
            a::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes),
        ),
    })
}

pub fn poll_for_activity_task(s: &Struct) -> Result<m::PollForActivityTaskRequest, Error> {
    let request = obj(s, 20).map(|r| a::PollForActivityTaskRequest {
        domain: raw(r, 10).unwrap_or_default(),
        task_list: obj(r, 20).map(list),
        identity: raw(r, 30).unwrap_or_default(),
        task_list_metadata: obj(r, 40).map(|x| a::TaskListMetadata {
            max_tasks_per_second: x.fields.iter().rev().find_map(|(f, v)| {
                if *f == 10 {
                    if let Value::F64(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
        }),
    });
    Ok(m::PollForActivityTaskRequest {
        request,
        domain_id: raw(s, 10)?,
        poller_id: raw(s, 15)?,
        forwarded_from: raw(s, 30)?,
        isolation_group: raw(s, 40)?,
    })
}

pub fn add_activity_task(s: &Struct) -> Result<m::AddActivityTaskRequest, Error> {
    let info = if let Some(x) = obj(s, 80) {
        Some(m::ActivityTaskDispatchInfo {
            scheduled_event: obj(x, 10).map(scheduled_activity_event).transpose()?,
            started_time: timestamp(x, 20),
            attempt: int64(x, 30) as i32,
            scheduled_time_of_this_attempt: timestamp(x, 40),
            heartbeat_details: payload(x, 60),
            workflow_type: obj(x, 70).map(|w| a::WorkflowType {
                name: raw(w, 10).unwrap_or_default(),
            }),
            workflow_domain: raw(x, 80)?,
        })
    } else {
        None
    };
    Ok(m::AddActivityTaskRequest {
        domain_id: raw(s, 10)?,
        workflow_execution: obj(s, 20).map(execution),
        source_domain_id: raw(s, 30)?,
        task_list: obj(s, 40).map(list),
        schedule_id: int64(s, 50),
        schedule_to_start_timeout: duration(s, 60),
        source: integer(s, 69).map_or(0, |v| v.wrapping_add(1)),
        forwarded_from: raw(s, 70)?,
        activity_task_dispatch_info: info,
        partition_config: byte_map(s, 90)?,
    })
}

pub fn query_workflow(s: &Struct) -> Result<m::QueryWorkflowRequest, Error> {
    let req = obj(s, 30).map(|r| a::QueryWorkflowRequest {
        domain: raw(r, 10).unwrap_or_default(),
        workflow_execution: obj(r, 20).map(execution),
        query: obj(r, 30).map(|q| a::WorkflowQuery {
            query_type: raw(q, 10).unwrap_or_default(),
            query_args: payload(q, 20),
        }),
        query_reject_condition: integer(r, 40).map_or(0, |v| v.wrapping_add(1)),
        query_consistency_level: integer(r, 50).map_or(0, |v| v.wrapping_add(1)),
    });
    Ok(m::QueryWorkflowRequest {
        request: req,
        domain_id: raw(s, 10)?,
        task_list: obj(s, 20).map(list),
        forwarded_from: raw(s, 40)?,
    })
}

pub fn respond_query_task_completed(
    s: &Struct,
) -> Result<m::RespondQueryTaskCompletedRequest, Error> {
    let request = if let Some(r) = obj(s, 40) {
        Some(a::RespondQueryTaskCompletedRequest {
            task_token: raw(r, 10)?.into_bytes(),
            result: Some(a::WorkflowQueryResult {
                result_type: match integer(r, 20) {
                    Some(0) => 1,
                    Some(1) => 2,
                    _ => 0,
                },
                answer: payload(r, 30),
                error_message: raw(r, 40)?,
            }),
            worker_version_info: obj(r, 50).map(|w| a::WorkerVersionInfo {
                r#impl: raw(w, 10).unwrap_or_default(),
                feature_version: raw(w, 20).unwrap_or_default(),
            }),
        })
    } else {
        None
    };
    Ok(m::RespondQueryTaskCompletedRequest {
        request,
        domain_id: raw(s, 10)?,
        task_list: obj(s, 20).map(list),
        task_id: raw(s, 30)?,
    })
}

pub fn cancel_outstanding_poll(s: &Struct) -> Result<m::CancelOutstandingPollRequest, Error> {
    Ok(m::CancelOutstandingPollRequest {
        domain_id: raw(s, 10)?,
        poller_id: raw(s, 40)?,
        task_list_type: integer(s, 20).map_or(0, |v| v.wrapping_add(1)),
        task_list: obj(s, 30).map(list),
    })
}
pub fn describe_task_list(s: &Struct) -> Result<m::DescribeTaskListRequest, Error> {
    let request = obj(s, 20).map(|r| a::DescribeTaskListRequest {
        domain: raw(r, 10).unwrap_or_default(),
        task_list: obj(r, 20).map(list),
        task_list_type: integer(r, 30).map_or(0, |v| v.wrapping_add(1)),
        include_task_list_status: r
            .fields
            .iter()
            .rev()
            .find_map(|(f, v)| {
                if *f == 40 {
                    if let Value::Bool(b) = v {
                        Some(*b)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default(),
    });
    Ok(m::DescribeTaskListRequest {
        domain_id: raw(s, 10)?,
        request,
    })
}
pub fn list_task_list_partitions(s: &Struct) -> Result<m::ListTaskListPartitionsRequest, Error> {
    Ok(m::ListTaskListPartitionsRequest {
        domain: raw(s, 10)?,
        task_list: obj(s, 20).map(list),
    })
}
pub fn get_task_lists_by_domain(s: &Struct) -> Result<m::GetTaskListsByDomainRequest, Error> {
    Ok(m::GetTaskListsByDomainRequest {
        domain: raw(s, 10)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_scheduled_activity_dispatch_preserves_complete_raw_graph() {
        let raw_value = || Value::Bytes(vec![0xff, 0xfe]);
        let attributes = st(vec![
            (10, raw_value()),
            (20, Value::Struct(st(vec![(10, raw_value())]))),
            (25, raw_value()),
            (
                30,
                Value::Struct(st(vec![(10, raw_value()), (20, Value::I32(0))])),
            ),
            (40, raw_value()),
            (45, Value::I32(0)),
            (50, Value::I32(2)),
            (55, Value::I32(3)),
            (60, Value::I32(4)),
            (90, Value::I64(8)),
            (
                110,
                Value::Struct(st(vec![
                    (20, Value::F64(2.5)),
                    (40, Value::I32(6)),
                    (50, Value::List(vec![raw_value()])),
                    (70, Value::F64(0.2)),
                ])),
            ),
            (
                120,
                Value::Struct(st(vec![(10, Value::Map(vec![(raw_value(), raw_value())]))])),
            ),
        ]);
        let event = st(vec![
            (10, Value::I64(11)),
            (20, Value::I64(-1)),
            (30, Value::I32(9)),
            (35, Value::I64(12)),
            (36, Value::I64(13)),
            (130, Value::Struct(attributes)),
        ]);
        let request = add_activity_task(&st(vec![(
            80,
            Value::Struct(st(vec![(10, Value::Struct(event))])),
        )]))
        .unwrap();
        let event = request
            .activity_task_dispatch_info
            .unwrap()
            .scheduled_event
            .unwrap();
        assert_eq!((event.event_id, event.version, event.task_id), (11, 12, 13));
        assert_eq!(
            event.event_time,
            Some(prost_types::Timestamp {
                seconds: -1,
                nanos: 999_999_999
            })
        );
        let Some(a::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes)) =
            event.attributes
        else {
            panic!("wrong event attributes")
        };
        assert_eq!(attributes.activity_id.as_bytes(), &[0xff, 0xfe]);
        assert_eq!(
            attributes.activity_type.unwrap().name.as_bytes(),
            &[0xff, 0xfe]
        );
        assert_eq!(attributes.domain.as_bytes(), &[0xff, 0xfe]);
        assert_eq!(attributes.task_list.unwrap().name.as_bytes(), &[0xff, 0xfe]);
        assert_eq!(attributes.input.unwrap().data, [0xff, 0xfe]);
        assert_eq!(attributes.schedule_to_close_timeout.unwrap().seconds, 0);
        assert_eq!(attributes.schedule_to_start_timeout.unwrap().seconds, 2);
        assert_eq!(attributes.start_to_close_timeout.unwrap().seconds, 3);
        assert_eq!(attributes.heartbeat_timeout.unwrap().seconds, 4);
        assert_eq!(attributes.decision_task_completed_event_id, 8);
        let policy = attributes.retry_policy.unwrap();
        assert_eq!(policy.backoff_coefficient, 2.5);
        assert_eq!(policy.maximum_attempts, 6);
        assert_eq!(policy.jitter_coefficient, 0.2);
        assert_eq!(
            policy.non_retryable_error_reasons[0].as_bytes(),
            &[0xff, 0xfe]
        );
        assert_eq!(
            policy.initial_interval,
            Some(prost_types::Duration {
                seconds: 0,
                nanos: 0
            })
        );
        assert_eq!(policy.maximum_interval, policy.initial_interval);
        assert_eq!(policy.expiration_interval, policy.initial_interval);
        assert_eq!(
            attributes.header.unwrap().fields[&GoString::from(&[0xff, 0xfe][..])].data,
            [0xff, 0xfe]
        );
    }

    #[test]
    fn scheduled_activity_absent_fields_and_wrong_typed_duplicates() {
        let event = st(vec![
            (30, Value::I32(9)),
            (
                130,
                Value::Struct(st(vec![
                    (10, Value::Bytes(vec![0xff])),
                    (10, Value::I32(1)),
                    (
                        110,
                        Value::Struct(st(vec![
                            (50, Value::List(vec![Value::Bytes(vec![0xff])])),
                            (50, Value::List(vec![Value::I32(1)])),
                        ])),
                    ),
                ])),
            ),
        ]);
        let event = scheduled_activity_event(&event).unwrap();
        assert!(event.event_time.is_none());
        let Some(a::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes)) =
            event.attributes
        else {
            panic!("wrong event attributes")
        };
        assert_eq!(attributes.activity_id.as_bytes(), &[0xff]);
        assert!(attributes.input.is_none());
        assert!(attributes.schedule_to_close_timeout.is_none());
        assert!(attributes.header.is_none());
        assert!(attributes
            .retry_policy
            .unwrap()
            .non_retryable_error_reasons
            .is_empty());
    }

    fn st(f: Vec<(i16, Value)>) -> Struct {
        Struct { fields: f }
    }
    #[test]
    fn all_eight_shapes_and_raw_bytes() {
        let raw = vec![0xff, 0xfe];
        let poll = poll_for_activity_task(&st(vec![
            (10, Value::Bytes(raw.clone())),
            (20, Value::Struct(st(vec![(10, Value::Bytes(raw.clone()))]))),
        ]))
        .unwrap();
        assert_eq!(poll.domain_id.as_bytes(), raw);
        assert_eq!(poll.request.unwrap().domain.as_bytes(), raw);
        assert_eq!(
            add_activity_task(&st(vec![
                (10, Value::Bytes(raw.clone())),
                (80, Value::Struct(st(vec![(80, Value::Bytes(raw.clone()))])))
            ]))
            .unwrap()
            .activity_task_dispatch_info
            .unwrap()
            .workflow_domain
            .as_bytes(),
            raw
        );
        assert_eq!(
            query_workflow(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain_id
                .as_bytes(),
            raw
        );
        assert_eq!(
            respond_query_task_completed(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain_id
                .as_bytes(),
            raw
        );
        assert_eq!(
            cancel_outstanding_poll(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain_id
                .as_bytes(),
            raw
        );
        assert_eq!(
            describe_task_list(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain_id
                .as_bytes(),
            raw
        );
        assert_eq!(
            list_task_list_partitions(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain
                .as_bytes(),
            raw
        );
        assert_eq!(
            get_task_lists_by_domain(&st(vec![(10, Value::Bytes(raw.clone()))]))
                .unwrap()
                .domain
                .as_bytes(),
            raw
        );
    }
    #[test]
    fn duplicate_wrong_type_skipped_and_latest_typed_wins() {
        let r = st(vec![
            (10, Value::Bytes(vec![0xff])),
            (10, Value::I32(3)),
            (10, Value::Bytes(vec![0xfe])),
        ]);
        assert_eq!(
            get_task_lists_by_domain(&r).unwrap().domain.as_bytes(),
            &[0xfe]
        );
    }
    #[test]
    fn enum_zero_offsets_and_wrong_wire_type_preserves_earlier_struct() {
        let task = st(vec![(20, Value::I32(0))]);
        let d = cancel_outstanding_poll(&st(vec![
            (20, Value::I32(0)),
            (30, Value::Struct(task.clone())),
            (30, Value::I32(9)),
        ]))
        .unwrap();
        assert_eq!(d.task_list_type, 1);
        assert_eq!(d.task_list.unwrap().kind, 1);
        assert_eq!(
            describe_task_list(&st(vec![(
                20,
                Value::Struct(st(vec![(20, Value::Struct(task))]))
            )]))
            .unwrap()
            .request
            .unwrap()
            .task_list
            .unwrap()
            .kind,
            1
        );
    }
    #[test]
    fn raw_replacement_rune_bytes_are_not_utf8_normalized() {
        let value =
            get_task_lists_by_domain(&st(vec![(10, Value::Bytes(vec![0xef, 0xbf, 0xbd, 0xff]))]))
                .unwrap();
        assert_eq!(value.domain.as_bytes(), &[0xef, 0xbf, 0xbd, 0xff]);
    }

    #[test]
    fn native_activity_metadata_payload_and_timestamp_presence() {
        let request = poll_for_activity_task(&st(vec![(
            20,
            Value::Struct(st(vec![(
                40,
                Value::Struct(st(vec![(10, Value::F64(0.0))])),
            )])),
        )]))
        .unwrap();
        assert_eq!(
            request
                .request
                .unwrap()
                .task_list_metadata
                .unwrap()
                .max_tasks_per_second,
            Some(0.0)
        );
        let request = add_activity_task(&st(vec![(
            80,
            Value::Struct(st(vec![
                (20, Value::I64(-1)),
                (40, Value::I64(0)),
                (60, Value::Bytes(vec![0xff])),
            ])),
        )]))
        .unwrap();
        let dispatch = request.activity_task_dispatch_info.unwrap();
        assert_eq!(
            dispatch.started_time,
            Some(prost_types::Timestamp {
                seconds: -1,
                nanos: 999_999_999
            })
        );
        assert_eq!(
            dispatch.scheduled_time_of_this_attempt,
            Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0
            })
        );
        assert_eq!(dispatch.heartbeat_details.unwrap().data, vec![0xff]);
        let request = add_activity_task(&st(vec![(
            80,
            Value::Struct(st(vec![(60, Value::Bytes(Vec::new()))])),
        )]))
        .unwrap();
        assert!(request
            .activity_task_dispatch_info
            .unwrap()
            .heartbeat_details
            .is_none());
    }

    #[test]
    fn native_query_completion_preserves_worker_bytes_and_go_enum_mapping() {
        let request = respond_query_task_completed(&st(vec![(
            40,
            Value::Struct(st(vec![
                (10, Value::Bytes(vec![0xff])),
                (20, Value::I32(99)),
                (30, Value::Bytes(Vec::new())),
                (
                    50,
                    Value::Struct(st(vec![
                        (10, Value::Bytes(vec![0xfe])),
                        (20, Value::Bytes(vec![0xfd])),
                    ])),
                ),
            ])),
        )]))
        .unwrap()
        .request
        .unwrap();
        assert_eq!(request.task_token, vec![0xff]);
        let result = request.result.unwrap();
        assert_eq!(result.result_type, 0);
        assert!(result.answer.is_none());
        let version = request.worker_version_info.unwrap();
        assert_eq!(version.r#impl.as_bytes(), &[0xfe]);
        assert_eq!(version.feature_version.as_bytes(), &[0xfd]);
    }
}
