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
    if v.query.is_some() || !v.queries.is_empty() || v.decision_info.is_some() || v.auto_config_hint.is_some() || v.partition_config.is_some() || v.load_balancer_hints.is_some() {
        return Err("unsupported native response graph: queries, transient events, config and load-balancer hints require native nested encoders");
    }
    Ok(Struct { fields: f })
}

