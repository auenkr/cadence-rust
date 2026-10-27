//! Byte-preserving request and response adapters for the Matching decision-task
//! Thrift methods. This module is intentionally not wired into production
//! dispatch until native handlers are available.
#![allow(dead_code)] // Temporary: adapters are consumed by the forthcoming native handler path.

use cadence_common::go_string::GoString;
use cadence_proto::native::{api as n_api, matching as n};
use cadence_thrift::{value::{FromValue, Struct, Value}, Error};
use std::collections::HashMap;

fn raw(s: &Struct, id: i16) -> Result<GoString, Error> {
    Ok(s.fields.iter().rev().find_map(|(field, value)|
        (*field == id).then(|| GoString::from_value(value).ok()).flatten()
    ).unwrap_or_default())
}
fn nested(s: &Struct, id: i16) -> Result<Option<&Struct>, Error> {
    match s.fields.iter().rev().find(|(field, value)| *field == id && matches!(value, Value::Struct(_))).map(|(_, v)| v) {
        None => Ok(None),
        Some(Value::Struct(v)) => Ok(Some(v)),
        Some(v) => Err(Error::Invalid(format!("field {id}: expected struct, got {v:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn st(fields: Vec<(i16, Value)>) -> Struct { Struct { fields } }
    #[test]
    fn raw_decode_preserves_invalid_bytes_nul_and_nested_identifiers() {
        let req = st(vec![(10, Value::Bytes(vec![0xff, 0xfe])), (20, Value::Struct(st(vec![
            (10, Value::Bytes(b"a\0b".to_vec())), (20, Value::Bytes(vec![0xfe])),
        ])))]);
        let decoded = add_decision_task(&req).unwrap();
        assert_eq!(decoded.domain_id.as_bytes(), &[0xff, 0xfe]);
        let execution = decoded.workflow_execution.unwrap();
        assert_eq!(execution.workflow_id.as_bytes(), b"a\0b");
        assert_eq!(execution.run_id.as_bytes(), &[0xfe]);
    }
    #[test]
    fn raw_map_equal_keys_are_last_wins_and_wrong_scalar_is_skipped() {
        let req = st(vec![
            (10, Value::Bytes(b"first".to_vec())), (10, Value::I32(9)),
            (70, Value::Map(vec![
                (Value::Bytes(vec![0xff]), Value::Bytes(b"old".to_vec())),
                (Value::Bytes(vec![0xff]), Value::Bytes(b"new".to_vec())),
            ])),
        ]);
        let decoded = add_decision_task(&req).unwrap();
        assert_eq!(decoded.domain_id.as_bytes(), b"first");
        assert_eq!(decoded.partition_config.get([0xff_u8].as_slice()).unwrap().as_bytes(), b"new");
    }
    #[test]
    fn poll_nested_api_request_keeps_invalid_identity_and_default_presence() {
        let req = st(vec![(20, Value::Struct(st(vec![
            (10, Value::Bytes(vec![0xff])), (30, Value::Bytes(vec![0xfe])),
            (20, Value::Struct(st(vec![(10, Value::Bytes(b"queue".to_vec()))]))),
        ])))]);
        let decoded = poll_for_decision_task(&req).unwrap();
        let inner = decoded.request.unwrap();
        assert_eq!(inner.identity.as_bytes(), &[0xfe]);
        assert_eq!(inner.task_list.unwrap().name.as_bytes(), b"queue");
        assert_eq!(inner.domain.as_bytes(), &[0xff]);
    }

    #[test]
    fn response_encodes_raw_query_bytes_map_keys_hints_and_counts() {
        let response = n::PollForDecisionTaskResponse {
            query: Some(n_api::WorkflowQuery {
                query_type: GoString::from(vec![0xff]),
                query_args: Some(n_api::Payload { data: vec![0xfe, 0] }),
            }),
            queries: [(GoString::from(vec![0xfd]), n_api::WorkflowQuery {
                query_type: GoString::from(b"q".to_vec()),
                query_args: Some(n_api::Payload { data: vec![0xfc] }),
            })].into(),
            auto_config_hint: Some(n_api::AutoConfigHint {
                enable_auto_config: true,
                poller_wait_time_in_ms: 17,
            }),
            ..Default::default()
        };
        let encoded = poll_for_decision_task_response(response).unwrap();
        let query = encoded.fields.iter().find(|(id, _)| *id == 80).unwrap();
        let Value::Struct(query) = &query.1 else { panic!("query is not a struct") };
        assert!(query.fields.iter().any(|(id, v)| *id == 10 && v == &Value::Bytes(vec![0xff])));
        assert!(query.fields.iter().any(|(id, v)| *id == 20 && v == &Value::Bytes(vec![0xfe, 0])));
        let queries = encoded.fields.iter().find(|(id, _)| *id == 150).unwrap();
        let Value::Map(entries) = &queries.1 else { panic!("queries is not a map") };
        assert_eq!(entries[0].0, Value::Bytes(vec![0xfd]));
        let hint = encoded.fields.iter().find(|(id, _)| *id == 170).unwrap();
        assert_eq!(hint.1, structure(vec![(10, Value::Bool(true)), (20, Value::I64(17))]));
        assert!(encoded.fields.iter().any(|(id, value)| *id == 65 && *value == Value::I64(0)));
        assert!(encoded.fields.iter().any(|(id, value)| *id == 51 && *value == Value::I64(0)));
    }

    #[test]
    fn response_encodes_transient_decision_events_and_rejects_other_variants() {
        use n_api::history_event::Attributes;
        let scheduled = n_api::HistoryEvent {
            event_id: 8,
            attributes: Some(Attributes::DecisionTaskScheduledEventAttributes(
                n_api::DecisionTaskScheduledEventAttributes {
                    attempt: 3,
                    task_list: Some(n_api::TaskList {
                        name: GoString::from(vec![0xff]), ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let started = n_api::HistoryEvent {
            event_id: 9,
            attributes: Some(Attributes::DecisionTaskStartedEventAttributes(
                n_api::DecisionTaskStartedEventAttributes {
                    scheduled_event_id: 8,
                    identity: GoString::from(vec![0xfe]),
                    request_id: GoString::from(vec![0xfd]),
                },
            )),
            ..Default::default()
        };
        let response = n::PollForDecisionTaskResponse {
            decision_info: Some(cadence_proto::native::shared::TransientDecisionInfo {
                scheduled_event: Some(scheduled),
                started_event: Some(started),
            }),
            ..Default::default()
        };
        let encoded = poll_for_decision_task_response(response).unwrap();
        let info = encoded.fields.iter().find(|(id, _)| *id == 90).unwrap();
        let Value::Struct(info) = &info.1 else { panic!("decision info is not a struct") };
        assert_eq!(info.fields.len(), 2);
        let Value::Struct(started) = &info.fields[1].1 else { panic!("started event is not a struct") };
        let Value::Struct(attrs) = &started.fields.iter().find(|(id, _)| *id == 90).unwrap().1 else { panic!("started attrs are not a struct") };
        assert!(attrs.fields.iter().any(|(id, value)| *id == 20 && *value == Value::Bytes(vec![0xfe])));

        let unsupported = n_api::HistoryEvent {
            attributes: Some(Attributes::ActivityTaskScheduledEventAttributes(Default::default())),
            ..Default::default()
        };
        assert_eq!(transient_event_out(unsupported), Err("Unexpected event in transient decision info."));
    }

    #[test]
    fn partition_metadata_preserves_raw_and_empty_values() {
        let encoded = partition_metadata_out(n_api::TaskListPartitionMetadata {
            key: GoString::from(vec![0xff]),
            owner_host_name: GoString::default(),
        });
        assert_eq!(encoded, structure(vec![
            (10, Value::Bytes(vec![0xff])),
            (20, Value::Bytes(Vec::new())),
        ]));
    }
}
fn int(s: &Struct, id: i16) -> Option<i32> {
    s.fields.iter().rev().find_map(|(field, v)| if *field == id { match v { Value::I32(x) => Some(*x), _ => None } } else { None })
}
fn long(s: &Struct, id: i16) -> Result<i64, Error> {
    Ok(s.fields.iter().rev().find_map(|(field, v)| if *field == id { match v { Value::I64(x) => Some(*x), _ => None } } else { None }).unwrap_or_default())
}
fn list(s: &Struct) -> Result<n_api::TaskList, Error> {
    Ok(n_api::TaskList { name: raw(s, 10)?, kind: int(s, 20).map_or(0, |v| v.wrapping_add(1)), base_name: raw(s, 30)? })
}
fn execution(s: &Struct) -> Result<n_api::WorkflowExecution, Error> {
    Ok(n_api::WorkflowExecution { workflow_id: raw(s, 10)?, run_id: raw(s, 20)? })
}
fn string_map(s: &Struct, id: i16) -> Result<HashMap<GoString, GoString>, Error> {
    match s.fields.iter().rev().find_map(|(field, value)| if *field == id { if let Value::Map(v) = value { Some(v) } else { None } } else { None }) {
        None => Ok(HashMap::new()),
        Some(entries) => {
            let mut map = HashMap::new();
            if entries.iter().any(|(k, v)| !matches!((k, v), (Value::Bytes(_), Value::Bytes(_)))) {
                return Ok(map);
            }
            for (k, v) in entries {
                let (Ok(k), Ok(v)) = (GoString::from_value(k), GoString::from_value(v)) else { continue };
                map.insert(k, v);
            }
            Ok(map)
        }
    }
}

/// Decode MatchingService::AddDecisionTask argument 1 (field IDs from the
/// pinned matching.thrift). Wrong scalar wire types are treated as absent.
pub fn add_decision_task(s: &Struct) -> Result<n::AddDecisionTaskRequest, Error> {
    Ok(n::AddDecisionTaskRequest {
        domain_id: raw(s, 10)?,
        workflow_execution: nested(s, 20)?.map(execution).transpose()?,
        task_list: nested(s, 30)?.map(list).transpose()?,
        schedule_id: long(s, 40)?,
        schedule_to_start_timeout: int(s, 50).map(|seconds| prost_types::Duration { seconds: i64::from(seconds), nanos: 0 }),
        source: int(s, 59).map_or(0, |v| v.wrapping_add(1)),
        forwarded_from: raw(s, 60)?,
        partition_config: string_map(s, 70)?,
    })
}

/// Decode MatchingService::PollForDecisionTask argument 1, including its
/// nested shared API request, without converting any string through UTF-8.
pub fn poll_for_decision_task(s: &Struct) -> Result<n::PollForDecisionTaskRequest, Error> {
    let request = nested(s, 20)?.map(|r| -> Result<_, Error> {
        Ok(n_api::PollForDecisionTaskRequest {
            domain: raw(r, 10)?,
            task_list: nested(r, 20)?.map(list).transpose()?,
            identity: raw(r, 30)?,
            binary_checksum: raw(r, 40)?,
        })
    }).transpose()?;
    Ok(n::PollForDecisionTaskRequest {
        request,
        domain_id: raw(s, 10)?,
        poller_id: raw(s, 15)?,
        forwarded_from: raw(s, 30)?,
        isolation_group: raw(s, 40)?,
    })
}

fn bytes(v: GoString) -> Value { Value::Bytes(v.into_bytes()) }
fn structure(fields: Vec<(i16, Value)>) -> Value { Value::Struct(Struct { fields }) }
fn exec_out(v: n_api::WorkflowExecution) -> Value {
    structure(vec![(10, bytes(v.workflow_id)), (20, bytes(v.run_id))])
}
fn task_list_out(v: n_api::TaskList) -> Value {
    let mut f = vec![(10, bytes(v.name))];
    if v.kind > 0 { f.push((20, Value::I32(v.kind - 1))); }
    if !v.base_name.is_empty() { f.push((30, bytes(v.base_name))); }
    structure(f)
}

fn query_out(v: n_api::WorkflowQuery) -> Value {
    let mut fields = vec![(10, bytes(v.query_type))];
    if let Some(payload) = v.query_args {
        fields.push((20, Value::Bytes(payload.data)));
    }
    structure(fields)
}

fn hint_out(v: n_api::AutoConfigHint) -> Value {
    structure(vec![
        (10, Value::Bool(v.enable_auto_config)),
        (20, Value::I64(v.poller_wait_time_in_ms)),
    ])
}

/// Thrift's partition metadata shape is shared by the matching partition
/// listing response. Native fields stay byte-valued, including empty values.
pub fn partition_metadata_out(v: n_api::TaskListPartitionMetadata) -> Value {
    structure(vec![(10, bytes(v.key)), (20, bytes(v.owner_host_name))])
}

fn transient_event_out(v: n_api::HistoryEvent) -> Result<Value, &'static str> {
    use n_api::history_event::Attributes;
    let mut fields = vec![
        (10, Value::I64(v.event_id)),
        (35, Value::I64(v.version)),
        (36, Value::I64(v.task_id)),
    ];
    if let Some(time) = v.event_time {
        fields.push((20, Value::I64(time.seconds.wrapping_mul(1_000_000_000).wrapping_add(i64::from(time.nanos)))));
    }
    match v.attributes {
        Some(Attributes::DecisionTaskScheduledEventAttributes(a)) => {
            fields.push((30, Value::I32(cadence_api::enums::EventType::DecisionTaskScheduled as i32)));
            let mut attrs = vec![(30, Value::I64(i64::from(a.attempt)))];
            if let Some(task_list) = a.task_list { attrs.push((10, task_list_out(task_list))); }
            if let Some(timeout) = a.start_to_close_timeout { attrs.push((20, Value::I32(timeout.seconds as i32))); }
            fields.push((80, structure(attrs)));
        }
        Some(Attributes::DecisionTaskStartedEventAttributes(a)) => {
            fields.push((30, Value::I32(cadence_api::enums::EventType::DecisionTaskStarted as i32)));
            fields.push((90, structure(vec![
                (10, Value::I64(a.scheduled_event_id)),
                (20, bytes(a.identity)),
                (30, bytes(a.request_id)),
            ])));
        }
        None => (),
        _ => return Err("Unexpected event in transient decision info."),
    }
    Ok(structure(fields))
}

/// Encode the byte-valued identifier/task-list portion of a native decision
/// response. All supported response string fields are preserved verbatim.
pub fn poll_for_decision_task_response(v: n::PollForDecisionTaskResponse) -> Result<Struct, &'static str> {
    let mut f = vec![
        (10, Value::Bytes(v.task_token)), (50, Value::I64(v.started_event_id)),
        (51, Value::I64(i64::from(v.attempt))), (60, Value::I64(v.next_event_id)),
        (65, Value::I64(v.backlog_count_hint)), (70, Value::Bool(v.sticky_execution_enabled)),
        (110, Value::I32(v.event_store_version)), (120, Value::Bytes(v.branch_token)),
        (160, Value::I64(v.total_history_bytes)),
    ];
    if let Some(x) = v.workflow_execution { f.push((20, exec_out(x))); }
    if let Some(x) = v.workflow_type { f.push((30, structure(vec![(10, bytes(x.name))]))); }
    if let Some(x) = v.workflow_execution_task_list { f.push((100, task_list_out(x))); }
    if let Some(x) = v.scheduled_time { f.push((130, Value::I64(x.seconds.wrapping_mul(1_000_000_000).wrapping_add(i64::from(x.nanos))))); }
    if let Some(x) = v.started_time { f.push((140, Value::I64(x.seconds.wrapping_mul(1_000_000_000).wrapping_add(i64::from(x.nanos))))); }
    if let Some(x) = v.previous_started_event_id { f.push((40, Value::I64(x))); }
    if let Some(query) = v.query { f.push((80, query_out(query))); }
    if let Some(info) = v.decision_info {
        let mut info_fields = Vec::new();
        if let Some(event) = info.scheduled_event { info_fields.push((10, transient_event_out(event)?)); }
        if let Some(event) = info.started_event { info_fields.push((20, transient_event_out(event)?)); }
        f.push((90, structure(info_fields)));
    }
    if let Some(x) = v.auto_config_hint { f.push((170, hint_out(x))); }
    f.push((150, Value::Map(v.queries.into_iter().map(|(k, v)| (bytes(k), query_out(v))).collect()));
    // The production Thrift decision adapter predates partition-config and
    // load-balancer fields; these have no fields in its Thrift response.
    Ok(Struct { fields: f })
}

