//! Byte-preserving native Thrift response encoders for the non-decision,
//! non-void Matching RPCs (PollForActivityTask, QueryWorkflow,
//! DescribeTaskList, GetTaskListsByDomain, ListTaskListPartitions). Field IDs
//! and unit conversions mirror the legacy `thrift_matching.rs` handlers. Not
//! wired into production dispatch.
#![allow(dead_code)] // Temporary: consumed by the forthcoming native handler path.

use cadence_common::go_string::GoString;
use cadence_proto::native::{api as n_api, matching as n};
use cadence_thrift::value::{Struct, Value};

fn bytes(v: GoString) -> Value {
    Value::Bytes(v.into_bytes())
}
fn structure(fields: Vec<(i16, Value)>) -> Value {
    Value::Struct(Struct { fields })
}
fn nanos(v: prost_types::Timestamp) -> Value {
    Value::I64(
        v.seconds
            .wrapping_mul(1_000_000_000)
            .wrapping_add(i64::from(v.nanos)),
    )
}
fn secs(v: prost_types::Duration) -> Value {
    Value::I32(v.seconds as i32)
}
fn put<T>(f: &mut Vec<(i16, Value)>, id: i16, v: Option<T>, c: impl FnOnce(T) -> Value) {
    if let Some(v) = v {
        f.push((id, c(v)));
    }
}
fn named(name: GoString) -> Value {
    structure(vec![(10, bytes(name))])
}
fn execution(v: n_api::WorkflowExecution) -> Value {
    structure(vec![(10, bytes(v.workflow_id)), (20, bytes(v.run_id))])
}
fn task_list(v: n_api::TaskList) -> Value {
    let mut f = vec![(10, bytes(v.name))];
    if v.kind > 0 {
        f.push((20, Value::I32(v.kind - 1)));
    }
    if !v.base_name.is_empty() {
        f.push((30, bytes(v.base_name)));
    }
    structure(f)
}
fn hint(v: n_api::AutoConfigHint) -> Value {
    structure(vec![
        (10, Value::Bool(v.enable_auto_config)),
        (20, Value::I64(v.poller_wait_time_in_ms)),
    ])
}
fn partition(v: n_api::TaskListPartitionMetadata) -> Value {
    structure(vec![(10, bytes(v.key)), (20, bytes(v.owner_host_name))])
}

/// Legacy `activity`. The native partition-config and load-balancer-hints
/// fields have no field in the legacy Thrift response and are omitted.
pub fn poll_for_activity_task_response(v: n::PollForActivityTaskResponse) -> Struct {
    let mut f = vec![
        (10, Value::Bytes(v.task_token)),
        (30, bytes(v.activity_id)),
        (120, Value::I32(v.attempt)),
        (160, bytes(v.workflow_domain)),
    ];
    put(&mut f, 20, v.workflow_execution, execution);
    put(&mut f, 40, v.activity_type, |v| named(v.name));
    put(&mut f, 50, v.input, |v| Value::Bytes(v.data));
    put(&mut f, 70, v.scheduled_time, nanos);
    put(&mut f, 80, v.schedule_to_close_timeout, secs);
    put(&mut f, 90, v.started_time, nanos);
    put(&mut f, 100, v.start_to_close_timeout, secs);
    put(&mut f, 110, v.heartbeat_timeout, secs);
    put(&mut f, 130, v.scheduled_time_of_this_attempt, nanos);
    put(&mut f, 140, v.heartbeat_details, |v| Value::Bytes(v.data));
    put(&mut f, 150, v.workflow_type, |v| named(v.name));
    put(&mut f, 170, v.header, |v| {
        structure(vec![(
            10,
            Value::Map(
                v.fields
                    .into_iter()
                    .map(|(k, v)| (bytes(k), Value::Bytes(v.data)))
                    .collect(),
            ),
        )])
    });
    put(&mut f, 180, v.auto_config_hint, hint);
    Struct { fields: f }
}

/// Legacy QueryWorkflow response; `partition_config` has no Thrift field.
pub fn query_workflow_response(v: n::QueryWorkflowResponse) -> Struct {
    let mut f = Vec::new();
    put(&mut f, 10, v.query_result, |v| Value::Bytes(v.data));
    put(&mut f, 20, v.query_rejected, |v| {
        structure(if v.close_status > 0 {
            vec![(10, Value::I32(v.close_status - 1))]
        } else {
            vec![]
        })
    });
    Struct { fields: f }
}

fn description_fields(v: n::DescribeTaskListResponse) -> Vec<(i16, Value)> {
    let pollers = v
        .pollers
        .into_iter()
        .map(|v| {
            let mut f = vec![(20, bytes(v.identity)), (30, Value::F64(v.rate_per_second))];
            put(&mut f, 10, v.last_access_time, nanos);
            structure(f)
        })
        .collect();
    let mut f = vec![(10, Value::List(pollers))];
    put(&mut f, 30, v.task_list, task_list);
    put(&mut f, 20, v.task_list_status, |v| {
        let mut f = vec![
            (10, Value::I64(v.backlog_count_hint)),
            (20, Value::I64(v.read_level)),
            (30, Value::I64(v.ack_level)),
            (35, Value::F64(v.rate_per_second)),
            (60, Value::F64(v.new_tasks_per_second)),
            (70, Value::Bool(v.empty)),
        ];
        put(&mut f, 40, v.task_id_block, |b| {
            structure(vec![
                (10, Value::I64(b.start_id)),
                (20, Value::I64(b.end_id)),
            ])
        });
        f.push((
            50,
            Value::Map(
                v.isolation_group_metrics
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            bytes(k),
                            structure(vec![
                                (10, Value::F64(v.new_tasks_per_second)),
                                (20, Value::I64(v.poller_count)),
                            ]),
                        )
                    })
                    .collect(),
            ),
        ));
        structure(f)
    });
    f
}

/// Legacy `description`; `partition_config` has no Thrift field.
pub fn describe_task_list_response(v: n::DescribeTaskListResponse) -> Struct {
    Struct {
        fields: description_fields(v),
    }
}

pub fn get_task_lists_by_domain_response(v: n::GetTaskListsByDomainResponse) -> Struct {
    let map = |m: std::collections::HashMap<GoString, n::DescribeTaskListResponse>| {
        Value::Map(
            m.into_iter()
                .map(|(k, v)| (bytes(k), structure(description_fields(v))))
                .collect(),
        )
    };
    Struct {
        fields: vec![
            (10, map(v.decision_task_list_map)),
            (20, map(v.activity_task_list_map)),
        ],
    }
}

pub fn list_task_list_partitions_response(v: n::ListTaskListPartitionsResponse) -> Struct {
    let list = |l: Vec<n_api::TaskListPartitionMetadata>| {
        Value::List(l.into_iter().map(partition).collect())
    };
    Struct {
        fields: vec![
            (10, list(v.activity_task_list_partitions)),
            (20, list(v.decision_task_list_partitions)),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn get(s: &Struct, id: i16) -> Option<&Value> {
        s.fields.iter().find(|(i, _)| *i == id).map(|(_, v)| v)
    }
    fn gs(b: &[u8]) -> GoString {
        GoString::from(b.to_vec())
    }

    #[test]
    fn activity_complete_shape_raw_bytes_and_units() {
        let r = n::PollForActivityTaskResponse {
            task_token: vec![1, 0xff],
            workflow_execution: Some(n_api::WorkflowExecution {
                workflow_id: gs(&[0xff]),
                run_id: gs(&[0xfe]),
            }),
            activity_id: gs(&[0xef, 0xbf]),
            activity_type: Some(n_api::ActivityType { name: gs(&[0xc0]) }),
            input: Some(n_api::Payload { data: vec![] }),
            scheduled_time: Some(prost_types::Timestamp { seconds: 2, nanos: 3 }),
            schedule_to_close_timeout: Some(prost_types::Duration { seconds: 9, nanos: 0 }),
            attempt: 4,
            header: Some(n_api::Header {
                fields: [(gs(&[0xfd]), n_api::Payload { data: vec![7] })].into(),
            }),
            workflow_domain: gs("\u{fffd}".as_bytes()),
            ..Default::default()
        };
        let s = poll_for_activity_task_response(r);
        assert_eq!(get(&s, 30), Some(&Value::Bytes(vec![0xef, 0xbf])));
        assert_eq!(get(&s, 160), Some(&Value::Bytes(vec![0xef, 0xbf, 0xbd])));
        assert_eq!(get(&s, 70), Some(&Value::I64(2_000_000_003)));
        assert_eq!(get(&s, 80), Some(&Value::I32(9)));
        assert_eq!(get(&s, 120), Some(&Value::I32(4)));
        assert_eq!(get(&s, 50), Some(&Value::Bytes(vec![])));
        assert!(get(&s, 140).is_none() && get(&s, 90).is_none());
        let Some(Value::Struct(h)) = get(&s, 170) else {
            panic!()
        };
        assert_eq!(
            get(h, 10),
            Some(&Value::Map(vec![(Value::Bytes(vec![0xfd]), Value::Bytes(vec![7]))]))
        );
        let Some(Value::Struct(e)) = get(&s, 20) else {
            panic!()
        };
        assert_eq!(get(e, 10), Some(&Value::Bytes(vec![0xff])));
    }

    #[test]
    fn query_enum_offset_and_payload_presence() {
        let s = query_workflow_response(n::QueryWorkflowResponse {
            query_result: Some(n_api::Payload { data: vec![] }),
            query_rejected: Some(n_api::QueryRejected { close_status: 2 }),
            ..Default::default()
        });
        assert_eq!(get(&s, 10), Some(&Value::Bytes(vec![])));
        let Some(Value::Struct(q)) = get(&s, 20) else {
            panic!()
        };
        assert_eq!(get(q, 10), Some(&Value::I32(1)));
        let s = query_workflow_response(n::QueryWorkflowResponse::default());
        assert!(s.fields.is_empty());
        let s = query_workflow_response(n::QueryWorkflowResponse {
            query_rejected: Some(n_api::QueryRejected { close_status: 0 }),
            ..Default::default()
        });
        let Some(Value::Struct(q)) = get(&s, 20) else {
            panic!()
        };
        assert!(q.fields.is_empty());
    }

    #[test]
    fn describe_and_by_domain_full_shape() {
        let d = || n::DescribeTaskListResponse {
            pollers: vec![n_api::PollerInfo {
                last_access_time: Some(prost_types::Timestamp { seconds: 1, nanos: 5 }),
                identity: gs(&[0xff]),
                rate_per_second: 1.5,
            }],
            task_list_status: Some(n_api::TaskListStatus {
                backlog_count_hint: 3,
                task_id_block: Some(n_api::TaskIdBlock { start_id: 1, end_id: 2 }),
                isolation_group_metrics: [(
                    gs(&[0xfe]),
                    n_api::IsolationGroupMetrics { new_tasks_per_second: 2.0, poller_count: 6 },
                )]
                .into(),
                empty: true,
                ..Default::default()
            }),
            task_list: Some(n_api::TaskList { name: gs(&[0xfd]), kind: 2, base_name: gs(b"") }),
            ..Default::default()
        };
        let s = describe_task_list_response(d());
        let Some(Value::List(p)) = get(&s, 10) else {
            panic!()
        };
        let Value::Struct(p) = &p[0] else { panic!() };
        assert_eq!(get(p, 10), Some(&Value::I64(1_000_000_005)));
        assert_eq!(get(p, 20), Some(&Value::Bytes(vec![0xff])));
        let Some(Value::Struct(t)) = get(&s, 30) else {
            panic!()
        };
        assert_eq!(get(t, 20), Some(&Value::I32(1)));
        assert!(get(t, 30).is_none());
        let Some(Value::Struct(st)) = get(&s, 20) else {
            panic!()
        };
        assert_eq!(get(st, 70), Some(&Value::Bool(true)));
        let Some(Value::Map(m)) = get(st, 50) else {
            panic!()
        };
        assert_eq!(m[0].0, Value::Bytes(vec![0xfe]));
        let by = get_task_lists_by_domain_response(n::GetTaskListsByDomainResponse {
            decision_task_list_map: [(gs(&[0xfc]), d())].into(),
            activity_task_list_map: Default::default(),
        });
        let Some(Value::Map(m)) = get(&by, 10) else {
            panic!()
        };
        assert_eq!(m[0].0, Value::Bytes(vec![0xfc]));
        assert_eq!(get(&by, 20), Some(&Value::Map(vec![])));
    }

    #[test]
    fn partitions_keep_invalid_bytes_vs_replacement_rune() {
        let s = list_task_list_partitions_response(n::ListTaskListPartitionsResponse {
            activity_task_list_partitions: vec![n_api::TaskListPartitionMetadata {
                key: gs(&[0xff]),
                owner_host_name: gs("\u{fffd}".as_bytes()),
            }],
            decision_task_list_partitions: vec![],
        });
        let Some(Value::List(a)) = get(&s, 10) else {
            panic!()
        };
        let Value::Struct(p) = &a[0] else { panic!() };
        assert_eq!(get(p, 10), Some(&Value::Bytes(vec![0xff])));
        assert_eq!(get(p, 20), Some(&Value::Bytes(vec![0xef, 0xbf, 0xbd])));
        assert_eq!(get(&s, 20), Some(&Value::List(vec![])));
    }
}
