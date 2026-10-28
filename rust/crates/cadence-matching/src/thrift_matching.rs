//! Native adapters for the pinned MatchingService Thrift contract. Wire field
//! IDs follow matching.thrift/shared.thrift and the Go thrift/proto mappers.
use crate::{grpc_convert as cv, grpc_matching::Matching};
use cadence_api::{service_types as st, tasks as tk, types as t};
use cadence_proto::matching::matching_api_server::MatchingApi;
use cadence_proto::{api as p, matching as m};
use cadence_thrift::{
    value::{FromThrift, Struct, Value},
    Error,
};
use prost::Message;
use std::{collections::HashMap, time::Instant};
use tonic::{Request, Status};

#[rustfmt::skip]
#[path = "thrift_matching_schema.rs"]
mod schema;

#[path = "thrift_matching_native_dispatch.rs"]
mod native_dispatch;
pub use native_dispatch::dispatch_native_with_context;

fn wire_type(value: &Value) -> u8 {
    match value {
        Value::Bool(_) => 2,
        Value::I8(_) => 3,
        Value::F64(_) => 4,
        Value::I16(_) => 6,
        Value::I32(_) => 8,
        Value::I64(_) => 10,
        Value::Bytes(_) => 11,
        Value::Struct(_) => 12,
        Value::Map(_) => 13,
        Value::Set(_) => 14,
        Value::List(_) => 15,
    }
}

/// Mirrors Go's generated `FromWire` for the Matching request structs: a known field id whose wire
/// type differs from the IDL type is skipped (never widened or rejected), a later duplicate wins,
/// and a list/map whose declared element type differs decodes to nil. The `cadence-thrift`
/// getters stay strict because persistence and other services decode through them.
fn normalize(name: &str, value: &Struct) -> Struct {
    let Some(schema) = schema::fields(name) else {
        return value.clone();
    };
    let mut fields: Vec<(i16, Value)> = Vec::new();
    for (id, field) in &value.fields {
        let Some((_, wire, inner)) = schema.iter().find(|(known, _, _)| known == id) else {
            continue;
        };
        if wire_type(field) != *wire {
            continue;
        }
        // A correctly typed container with a wrong element type assigns nil,
        // replacing an earlier occurrence of this field in Go's FromWire.
        fields.retain(|(existing, _)| existing != id);
        let field = match (inner, field) {
            (schema::Inner::Struct(name), Value::Struct(v)) => Value::Struct(normalize(name, v)),
            (schema::Inner::List(element, nested), Value::List(items)) => {
                if items.first().is_some_and(|v| wire_type(v) != *element) {
                    continue;
                }
                match nested {
                    Some(name) => Value::List(
                        items
                            .iter()
                            .map(|item| match item {
                                Value::Struct(v) => Value::Struct(normalize(name, v)),
                                other => other.clone(),
                            })
                            .collect(),
                    ),
                    None => field.clone(),
                }
            }
            (schema::Inner::Map(key, element, nested), Value::Map(entries)) => {
                if entries
                    .first()
                    .is_some_and(|(k, v)| wire_type(k) != *key || wire_type(v) != *element)
                {
                    continue;
                }
                match nested {
                    Some(name) => Value::Map(
                        entries
                            .iter()
                            .map(|(k, v)| match v {
                                Value::Struct(v) => (k.clone(), Value::Struct(normalize(name, v))),
                                other => (k.clone(), other.clone()),
                            })
                            .collect(),
                    ),
                    None => field.clone(),
                }
            }
            _ => field.clone(),
        };
        fields.retain(|(existing, _)| existing != id);
        fields.push((*id, field));
    }
    Struct { fields }
}

/// Go request struct (`matching` package, except the shared `GetTaskListsByDomainRequest`) each
/// MatchingService method decodes as argument 1.
fn request_struct(method: &str) -> Option<&'static str> {
    Some(match method {
        "PollForDecisionTask" => "matching.PollForDecisionTaskRequest",
        "PollForActivityTask" => "matching.PollForActivityTaskRequest",
        "AddDecisionTask" => "matching.AddDecisionTaskRequest",
        "AddActivityTask" => "matching.AddActivityTaskRequest",
        "QueryWorkflow" => "matching.QueryWorkflowRequest",
        "RespondQueryTaskCompleted" => "matching.RespondQueryTaskCompletedRequest",
        "CancelOutstandingPoll" => "matching.CancelOutstandingPollRequest",
        "DescribeTaskList" => "matching.DescribeTaskListRequest",
        "GetTaskListsByDomain" => "shared.GetTaskListsByDomainRequest",
        "ListTaskListPartitions" => "matching.ListTaskListPartitionsRequest",
        _ => return None,
    })
}

fn invalid(error: Error) -> Status {
    Status::invalid_argument(error.to_string())
}
fn string(s: &Struct, id: i16) -> Result<String, Status> {
    s.req_string(id, "").map_err(invalid)
}
fn long(s: &Struct, id: i16) -> Result<i64, Status> {
    s.opt_i64(id)
        .map_err(invalid)
        .map(Option::unwrap_or_default)
}
fn nested<T: FromThrift>(s: &Struct, id: i16) -> Result<Option<T>, Status> {
    s.opt_struct(id).map_err(invalid)
}
fn task_list(s: &Struct, id: i16) -> Result<Option<p::TaskList>, Status> {
    Ok(nested::<t::TaskList>(s, id)?.map(cv::task_list_out))
}
fn execution(s: &Struct, id: i16) -> Result<Option<p::WorkflowExecution>, Status> {
    Ok(nested::<t::WorkflowExecution>(s, id)?.map(cv::execution_out))
}
fn duration(s: &Struct, id: i16) -> Result<Option<prost_types::Duration>, Status> {
    Ok(s.opt_i32(id)
        .map_err(invalid)?
        .map(|v| cv::duration(v.into())))
}
fn text(value: impl Into<String>) -> Value {
    Value::Bytes(value.into().into_bytes())
}
fn structure(fields: Vec<(i16, Value)>) -> Value {
    Value::Struct(Struct { fields })
}
fn put<T>(
    fields: &mut Vec<(i16, Value)>,
    id: i16,
    value: Option<T>,
    convert: impl FnOnce(T) -> Value,
) {
    if let Some(value) = value {
        fields.push((id, convert(value)));
    }
}
fn nanos(value: prost_types::Timestamp) -> Value {
    Value::I64(
        value
            .seconds
            .wrapping_mul(1_000_000_000)
            .wrapping_add(i64::from(value.nanos)),
    )
}
fn execution_out(v: p::WorkflowExecution) -> Value {
    structure(vec![(10, text(v.workflow_id)), (20, text(v.run_id))])
}
fn named(name: String) -> Value {
    structure(vec![(10, text(name))])
}
fn list_out(v: p::TaskList) -> Value {
    let mut fields = vec![(10, text(v.name))];
    if v.kind > 0 {
        fields.push((20, Value::I32(v.kind - 1)));
    }
    // Go `thrift.FromTaskList` leaves BaseName unset when it is empty.
    if !v.base_name.is_empty() {
        fields.push((30, text(v.base_name)));
    }
    structure(fields)
}
fn query_out(v: p::WorkflowQuery) -> Value {
    let mut fields = vec![(10, text(v.query_type))];
    put(&mut fields, 20, v.query_args, |v| Value::Bytes(v.data));
    structure(fields)
}
fn hint(v: p::AutoConfigHint) -> Value {
    structure(vec![
        (10, Value::Bool(v.enable_auto_config)),
        (20, Value::I64(v.poller_wait_time_in_ms)),
    ])
}
fn string_map(s: &Struct, id: i16) -> Result<HashMap<String, String>, Status> {
    match s.get(id) {
        None => Ok(HashMap::new()),
        Some(Value::Map(entries)) => entries
            .iter()
            .map(|(key, value)| match (key, value) {
                (Value::Bytes(key), Value::Bytes(value)) => Ok((
                    String::from_utf8(key.clone())
                        .map_err(|_| Status::invalid_argument("Invalid map key."))?,
                    String::from_utf8(value.clone())
                        .map_err(|_| Status::invalid_argument("Invalid map value."))?,
                )),
                _ => Err(Status::invalid_argument("Expected string map.")),
            })
            .collect(),
        _ => Err(Status::invalid_argument("Expected map.")),
    }
}
fn query_in(v: tk::QueryWorkflowRequest) -> p::QueryWorkflowRequest {
    p::QueryWorkflowRequest {
        domain: v.domain,
        workflow_execution: v.execution.map(cv::execution_out),
        query: v.query.map(|q| p::WorkflowQuery {
            query_type: q.query_type,
            query_args: cv::payload(q.query_args),
        }),
        query_reject_condition: v.query_reject_condition.map_or(0, |v| v as i32 + 1),
        query_consistency_level: v.query_consistency_level.map_or(0, |v| v as i32 + 1),
    }
}
fn description(v: m::DescribeTaskListResponse) -> Value {
    let pollers = v
        .pollers
        .into_iter()
        .map(|v| {
            let mut fields = vec![(20, text(v.identity)), (30, Value::F64(v.rate_per_second))];
            put(&mut fields, 10, v.last_access_time, nanos);
            structure(fields)
        })
        .collect();
    let mut fields = vec![(10, Value::List(pollers))];
    put(&mut fields, 30, v.task_list, list_out);
    put(&mut fields, 20, v.task_list_status, |v| {
        let mut fields = vec![
            (10, Value::I64(v.backlog_count_hint)),
            (20, Value::I64(v.read_level)),
            (30, Value::I64(v.ack_level)),
            (35, Value::F64(v.rate_per_second)),
            (60, Value::F64(v.new_tasks_per_second)),
            (70, Value::Bool(v.empty)),
        ];
        put(&mut fields, 40, v.task_id_block, |b| {
            structure(vec![
                (10, Value::I64(b.start_id)),
                (20, Value::I64(b.end_id)),
            ])
        });
        fields.push((
            50,
            Value::Map(
                v.isolation_group_metrics
                    .into_iter()
                    .map(|(key, v)| {
                        (
                            text(key),
                            structure(vec![
                                (10, Value::F64(v.new_tasks_per_second)),
                                (20, Value::I64(v.poller_count)),
                            ]),
                        )
                    })
                    .collect(),
            ),
        ));
        structure(fields)
    });
    structure(fields)
}
fn partition(v: p::TaskListPartitionMetadata) -> Value {
    structure(vec![(10, text(v.key)), (20, text(v.owner_host_name))])
}

// A transient decision contains just the two decision events, so preserve
// their timestamp precision and optional attributes without a JSON round trip.
fn transient_event(v: p::HistoryEvent) -> Result<Value, Status> {
    use p::history_event::Attributes;
    let mut fields = vec![
        (10, Value::I64(v.event_id)),
        (35, Value::I64(v.version)),
        (36, Value::I64(v.task_id)),
    ];
    put(&mut fields, 20, v.event_time, nanos);
    match v.attributes {
        Some(Attributes::DecisionTaskScheduledEventAttributes(v)) => {
            fields.push((
                30,
                Value::I32(cadence_api::enums::EventType::DecisionTaskScheduled as i32),
            ));
            let mut attributes = vec![(30, Value::I64(v.attempt.into()))];
            put(&mut attributes, 10, v.task_list, list_out);
            put(&mut attributes, 20, v.start_to_close_timeout, |v| {
                Value::I32(v.seconds as i32)
            });
            fields.push((80, structure(attributes)));
        }
        Some(Attributes::DecisionTaskStartedEventAttributes(v)) => {
            fields.push((
                30,
                Value::I32(cadence_api::enums::EventType::DecisionTaskStarted as i32),
            ));
            fields.push((
                90,
                structure(vec![
                    (10, Value::I64(v.scheduled_event_id)),
                    (20, text(v.identity)),
                    (30, text(v.request_id)),
                ]),
            ));
        }
        None => (),
        _ => {
            return Err(Status::internal(
                "Unexpected event in transient decision info.",
            ))
        }
    }
    Ok(structure(fields))
}
fn decision(v: m::PollForDecisionTaskResponse) -> Result<Struct, Status> {
    let mut fields = vec![
        (10, Value::Bytes(v.task_token)),
        (50, Value::I64(v.started_event_id)),
        (51, Value::I64(v.attempt.into())),
        (60, Value::I64(v.next_event_id)),
        (65, Value::I64(v.backlog_count_hint)),
        (70, Value::Bool(v.sticky_execution_enabled)),
        (110, Value::I32(v.event_store_version)),
        (120, Value::Bytes(v.branch_token)),
        (160, Value::I64(v.total_history_bytes)),
    ];
    put(&mut fields, 20, v.workflow_execution, execution_out);
    put(&mut fields, 30, v.workflow_type, |v| named(v.name));
    put(&mut fields, 40, v.previous_started_event_id, Value::I64);
    put(&mut fields, 80, v.query, query_out);
    put(&mut fields, 100, v.workflow_execution_task_list, list_out);
    put(&mut fields, 130, v.scheduled_time, nanos);
    put(&mut fields, 140, v.started_time, nanos);
    put(&mut fields, 170, v.auto_config_hint, hint);
    if let Some(v) = v.decision_info {
        let mut info = Vec::new();
        if let Some(v) = v.scheduled_event {
            info.push((10, transient_event(v)?));
        }
        if let Some(v) = v.started_event {
            info.push((20, transient_event(v)?));
        }
        fields.push((90, structure(info)));
    }
    fields.push((
        150,
        Value::Map(
            v.queries
                .into_iter()
                .map(|(key, value)| (text(key), query_out(value)))
                .collect(),
        ),
    ));
    Ok(Struct { fields })
}
fn activity(v: m::PollForActivityTaskResponse) -> Struct {
    let mut fields = vec![
        (10, Value::Bytes(v.task_token)),
        (30, text(v.activity_id)),
        (120, Value::I32(v.attempt)),
        (160, text(v.workflow_domain)),
    ];
    put(&mut fields, 20, v.workflow_execution, execution_out);
    put(&mut fields, 40, v.activity_type, |v| named(v.name));
    put(&mut fields, 50, v.input, |v| Value::Bytes(v.data));
    put(&mut fields, 70, v.scheduled_time, nanos);
    put(&mut fields, 80, v.schedule_to_close_timeout, |v| {
        Value::I32(v.seconds as i32)
    });
    put(&mut fields, 90, v.started_time, nanos);
    put(&mut fields, 100, v.start_to_close_timeout, |v| {
        Value::I32(v.seconds as i32)
    });
    put(&mut fields, 110, v.heartbeat_timeout, |v| {
        Value::I32(v.seconds as i32)
    });
    put(&mut fields, 130, v.scheduled_time_of_this_attempt, nanos);
    put(&mut fields, 140, v.heartbeat_details, |v| {
        Value::Bytes(v.data)
    });
    put(&mut fields, 150, v.workflow_type, |v| named(v.name));
    put(&mut fields, 170, v.header, |v| {
        structure(vec![(
            10,
            Value::Map(
                v.fields
                    .into_iter()
                    .map(|(key, v)| (text(key), Value::Bytes(v.data)))
                    .collect(),
            ),
        )])
    });
    put(&mut fields, 180, v.auto_config_hint, hint);
    Struct { fields }
}

fn rpc_request<T>(payload: T, deadline: Instant) -> Request<T> {
    let mut request = Request::new(payload);
    request.set_timeout(deadline.saturating_duration_since(Instant::now()));
    request
}

async fn invoke(
    matching: &Matching,
    method: &str,
    s: &Struct,
    deadline: Instant,
) -> Result<Option<Struct>, Status> {
    Ok(match method {
        "PollForDecisionTask" => {
            let request = nested::<tk::PollForDecisionTaskRequest>(s, 20)?.map(|v| {
                p::PollForDecisionTaskRequest {
                    domain: v.domain,
                    task_list: v.task_list.map(cv::task_list_out),
                    identity: v.identity,
                    binary_checksum: v.binary_checksum,
                }
            });
            let v = matching
                .poll_for_decision_task(rpc_request(
                    m::PollForDecisionTaskRequest {
                        request,
                        domain_id: string(s, 10)?,
                        poller_id: string(s, 15)?,
                        forwarded_from: string(s, 30)?,
                        isolation_group: string(s, 40)?,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            Some(decision(v)?)
        }
        "PollForActivityTask" => {
            let request = nested::<tk::PollForActivityTaskRequest>(s, 20)?.map(|v| {
                p::PollForActivityTaskRequest {
                    domain: v.domain,
                    task_list: v.task_list.map(cv::task_list_out),
                    identity: v.identity,
                    task_list_metadata: v.task_list_metadata.map(|v| p::TaskListMetadata {
                        max_tasks_per_second: v.max_tasks_per_second,
                    }),
                }
            });
            let v = matching
                .poll_for_activity_task(rpc_request(
                    m::PollForActivityTaskRequest {
                        request,
                        domain_id: string(s, 10)?,
                        poller_id: string(s, 15)?,
                        forwarded_from: string(s, 30)?,
                        isolation_group: string(s, 40)?,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            Some(activity(v))
        }
        "AddDecisionTask" => {
            matching
                .add_decision_task(rpc_request(
                    m::AddDecisionTaskRequest {
                        domain_id: string(s, 10)?,
                        workflow_execution: execution(s, 20)?,
                        task_list: task_list(s, 30)?,
                        schedule_id: long(s, 40)?,
                        schedule_to_start_timeout: duration(s, 50)?,
                        source: s
                            .opt_i32(59)
                            .map_err(invalid)?
                            .map_or(0, |v| v.wrapping_add(1)),
                        forwarded_from: string(s, 60)?,
                        partition_config: string_map(s, 70)?,
                    },
                    deadline,
                ))
                .await?;
            None
        }
        "AddActivityTask" => {
            let activity_task_dispatch_info = match s.get(80) {
                Some(Value::Struct(info)) => Some(m::ActivityTaskDispatchInfo {
                    scheduled_event: nested::<st::HistoryEvent>(info, 10)?
                        .map(crate::grpc_history::event)
                        .transpose()?,
                    started_time: info.opt_i64(20).map_err(invalid)?.map(cv::timestamp_nanos),
                    attempt: long(info, 30)? as i32,
                    scheduled_time_of_this_attempt: info
                        .opt_i64(40)
                        .map_err(invalid)?
                        .map(cv::timestamp_nanos),
                    heartbeat_details: cv::payload(info.bytes_or_empty(60).map_err(invalid)?),
                    workflow_type: nested::<t::WorkflowType>(info, 70)?
                        .map(|v| p::WorkflowType { name: v.name }),
                    workflow_domain: string(info, 80)?,
                }),
                // Go `FromWire` skips a field of the wrong wire type, leaving the info nil.
                _ => None,
            };
            matching
                .add_activity_task(rpc_request(
                    m::AddActivityTaskRequest {
                        domain_id: string(s, 10)?,
                        workflow_execution: execution(s, 20)?,
                        source_domain_id: string(s, 30)?,
                        task_list: task_list(s, 40)?,
                        schedule_id: long(s, 50)?,
                        schedule_to_start_timeout: duration(s, 60)?,
                        source: s
                            .opt_i32(69)
                            .map_err(invalid)?
                            .map_or(0, |v| v.wrapping_add(1)),
                        forwarded_from: string(s, 70)?,
                        activity_task_dispatch_info,
                        partition_config: string_map(s, 90)?,
                    },
                    deadline,
                ))
                .await?;
            None
        }
        "QueryWorkflow" => {
            let v = matching
                .query_workflow(rpc_request(
                    m::QueryWorkflowRequest {
                        domain_id: string(s, 10)?,
                        task_list: task_list(s, 20)?,
                        request: nested::<tk::QueryWorkflowRequest>(s, 30)?.map(query_in),
                        forwarded_from: string(s, 40)?,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            let mut fields = Vec::new();
            put(&mut fields, 10, v.query_result, |v| Value::Bytes(v.data));
            put(&mut fields, 20, v.query_rejected, |v| {
                structure(if v.close_status > 0 {
                    vec![(10, Value::I32(v.close_status - 1))]
                } else {
                    vec![]
                })
            });
            Some(Struct { fields })
        }
        "RespondQueryTaskCompleted" => {
            matching
                .respond_query_task_completed(rpc_request(
                    m::RespondQueryTaskCompletedRequest {
                        domain_id: string(s, 10)?,
                        task_list: task_list(s, 20)?,
                        task_id: string(s, 30)?,
                        request: nested::<tk::RespondQueryTaskCompletedRequest>(s, 40)?
                            .map(cv::query_completed_out),
                    },
                    deadline,
                ))
                .await?;
            None
        }
        "CancelOutstandingPoll" => {
            matching
                .cancel_outstanding_poll(rpc_request(
                    m::CancelOutstandingPollRequest {
                        domain_id: string(s, 10)?,
                        task_list_type: s
                            .opt_i32(20)
                            .map_err(invalid)?
                            .map_or(0, |v| v.wrapping_add(1)),
                        task_list: task_list(s, 30)?,
                        poller_id: string(s, 40)?,
                    },
                    deadline,
                ))
                .await?;
            None
        }
        "DescribeTaskList" => {
            let request =
                nested::<tk::DescribeTaskListRequest>(s, 20)?.map(|v| p::DescribeTaskListRequest {
                    domain: v.domain,
                    task_list: v.task_list.map(cv::task_list_out),
                    task_list_type: v.task_list_type.map_or(0, |v| v as i32 + 1),
                    include_task_list_status: v.include_task_list_status,
                });
            let v = matching
                .describe_task_list(rpc_request(
                    m::DescribeTaskListRequest {
                        domain_id: string(s, 10)?,
                        request,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            match description(v) {
                Value::Struct(v) => Some(v),
                _ => unreachable!(),
            }
        }
        "GetTaskListsByDomain" => {
            let v = matching
                .get_task_lists_by_domain(rpc_request(
                    m::GetTaskListsByDomainRequest {
                        domain: string(s, 10)?,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            Some(Struct {
                fields: vec![
                    (
                        10,
                        Value::Map(
                            v.decision_task_list_map
                                .into_iter()
                                .map(|(key, v)| (text(key), description(v)))
                                .collect(),
                        ),
                    ),
                    (
                        20,
                        Value::Map(
                            v.activity_task_list_map
                                .into_iter()
                                .map(|(key, v)| (text(key), description(v)))
                                .collect(),
                        ),
                    ),
                ],
            })
        }
        "ListTaskListPartitions" => {
            let v = matching
                .list_task_list_partitions(rpc_request(
                    m::ListTaskListPartitionsRequest {
                        domain: string(s, 10)?,
                        task_list: task_list(s, 20)?,
                    },
                    deadline,
                ))
                .await?
                .into_inner();
            Some(Struct {
                fields: vec![
                    (
                        10,
                        Value::List(
                            v.activity_task_list_partitions
                                .into_iter()
                                .map(partition)
                                .collect(),
                        ),
                    ),
                    (
                        20,
                        Value::List(
                            v.decision_task_list_partitions
                                .into_iter()
                                .map(partition)
                                .collect(),
                        ),
                    ),
                ],
            })
        }
        _ => return Err(Status::unimplemented("Unknown MatchingService method.")),
    })
}

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
fn application_error(
    method: &cadence_thrift::service::MethodDef,
    status: Status,
) -> (Struct, Option<(&'static str, String)>) {
    let details = RpcStatus::decode(status.details())
        .ok()
        .and_then(|v| v.details.into_iter().next());
    let typed = details.as_ref().and_then(|v| v.type_url.rsplit('.').next());
    let name = typed
        .and_then(|name| {
            method
                .exceptions
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map(|(name, _)| *name)
        })
        .unwrap_or_else(|| match status.code() {
            tonic::Code::InvalidArgument => "BadRequestError",
            tonic::Code::ResourceExhausted => "ServiceBusyError",
            tonic::Code::NotFound
                if method
                    .exceptions
                    .iter()
                    .any(|(name, _)| *name == "EntityNotExistsError") =>
            {
                "EntityNotExistsError"
            }
            _ => "InternalServiceError",
        });
    let mut fields = vec![(
        if name == "RemoteSyncMatchedError" {
            10
        } else {
            1
        },
        text(status.message()),
    )];
    // Go's thrift mapper (common/types/mapper/thrift/shared.go) always fills
    // these fields, and thriftrw rejects a reply that omits a required one.
    match name {
        "DomainNotActiveError" => {
            let v = details
                .as_ref()
                .and_then(|v| p::DomainNotActiveError::decode(v.value.as_slice()).ok())
                .unwrap_or_default();
            fields.extend([
                (2, text(v.domain)),
                (3, text(v.current_cluster)),
                (4, text(v.active_cluster)),
                (
                    5,
                    Value::List(v.active_clusters.into_iter().map(text).collect()),
                ),
            ]);
        }
        "EntityNotExistsError" => {
            let v = details
                .as_ref()
                .and_then(|v| p::EntityNotExistsError::decode(v.value.as_slice()).ok())
                .unwrap_or_default();
            fields.extend([
                (2, text(v.current_cluster)),
                (3, text(v.active_cluster)),
                (
                    4,
                    Value::List(v.active_clusters.into_iter().map(text).collect()),
                ),
            ]);
        }
        "ServiceBusyError" => {
            let v = details
                .as_ref()
                .and_then(|v| p::ServiceBusyError::decode(v.value.as_slice()).ok())
                .unwrap_or_default();
            fields.push((2, text(v.reason)));
        }
        _ => {}
    }
    if name == "TaskListNotOwnedByHostError" {
        if let Some(v) = details.as_ref().and_then(|v| {
            cadence_proto::shared::TaskListNotOwnedByHostError::decode(v.value.as_slice()).ok()
        }) {
            fields = vec![
                (1, text(v.owned_by_identity)),
                (2, text(v.my_identity)),
                (3, text(v.task_list_name)),
            ];
        }
    }
    let id = method
        .exceptions
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, id)| *id)
        .unwrap_or(2);
    (
        Struct {
            fields: vec![(id, structure(fields))],
        },
        Some((name, status.message().into())),
    )
}

pub async fn dispatch(
    matching: &Matching,
    procedure: &str,
    args: &Struct,
    deadline: Instant,
) -> Result<(Struct, Option<(&'static str, String)>), String> {
    dispatch_with_context(
        matching,
        procedure,
        args,
        deadline,
        crate::matching_metrics::InboundMetricContext::default(),
    )
    .await
}

pub async fn dispatch_with_context(
    matching: &Matching,
    procedure: &str,
    args: &Struct,
    deadline: Instant,
    context: crate::matching_metrics::InboundMetricContext,
) -> Result<(Struct, Option<(&'static str, String)>), String> {
    if let Some(native) = matching.native_service() {
        return dispatch_native_with_context(
            matching,
            native.as_ref(),
            procedure,
            args,
            deadline,
            context,
        )
        .await;
    }
    matching
        .inbound_metrics()
        .with_inbound_context(context, dispatch_inner(matching, procedure, args, deadline))
        .await
}

async fn dispatch_inner(
    matching: &Matching,
    procedure: &str,
    args: &Struct,
    deadline: Instant,
) -> Result<(Struct, Option<(&'static str, String)>), String> {
    let method = cadence_thrift::service::find_method(procedure)
        .ok_or_else(|| format!("Unknown Matching procedure {procedure}"))?;
    if method.service == "Meta" {
        matching.wait_started().await;
        return Ok((
            Struct {
                fields: vec![(
                    0,
                    Value::Struct(Struct {
                        fields: vec![
                            (1, Value::Bool(true)),
                            (2, Value::Bytes(b"matching good".to_vec())),
                        ],
                    }),
                )],
            },
            None,
        ));
    }
    if method.service != "MatchingService" {
        return Err("Unknown Matching service".into());
    }
    // Go's generated `Args.FromWire` skips field 1 when it is absent or has a different wire
    // type and leaves the argument nil; it is never a decode error. The mapper and handler
    // getters are nil-safe, so those methods behave like an empty request. Poll, Add* and
    // DescribeTaskList dereference the nil request in the engine (`req.PollRequest`,
    // `request.Execution`, `request.DescRequest`); `CapturePanic` returns the plain runtime
    // error, which the transport reports as an unexpected error.
    matching.wait_started().await;
    let empty = Struct::default();
    let request = match args
        .fields
        .iter()
        .rev()
        .find_map(|(id, value)| match (id, value) {
            (1, Value::Struct(value)) => Some(value),
            _ => None,
        }) {
        Some(v) => v,
        _ => {
            use crate::matching_quota::MatchingRpc as Rpc;
            let nil_deref = match method.method {
                "PollForDecisionTask" => Some(("PollForDecisionTask", Rpc::PollForDecisionTask)),
                "PollForActivityTask" => Some(("PollForActivityTask", Rpc::PollForActivityTask)),
                "AddDecisionTask" => Some(("AddDecisionTask", Rpc::AddDecisionTask)),
                "AddActivityTask" => Some(("AddActivityTask", Rpc::AddActivityTask)),
                "DescribeTaskList" => Some(("DescribeTaskList", Rpc::DescribeTaskList)),
                _ => None,
            };
            if let Some((name, rpc)) = nil_deref {
                let status = matching.thrift_nil_request(name, rpc).await;
                return if crate::matching_panic::is_captured_panic(&status) {
                    Err(status.message().to_owned())
                } else {
                    Ok(application_error(method, status))
                };
            }
            &empty
        }
    };
    let normalized = request_struct(method.method).map(|name| normalize(name, request));
    let request = normalized.as_ref().unwrap_or(request);
    let outcome = if matches!(method.method, "PollForDecisionTask" | "PollForActivityTask") {
        let budget = deadline
            .saturating_duration_since(Instant::now())
            .saturating_sub(std::time::Duration::from_secs(1))
            .min(matching.poll_expiration);
        match tokio::time::timeout(budget, invoke(matching, method.method, request, deadline)).await
        {
            Ok(result) => result,
            Err(_) if method.method == "PollForDecisionTask" => {
                decision(m::PollForDecisionTaskResponse::default()).map(Some)
            }
            Err(_) => Ok(Some(activity(m::PollForActivityTaskResponse::default()))),
        }
    } else {
        invoke(matching, method.method, request, deadline).await
    };
    match outcome {
        Ok(Some(value)) => Ok((
            Struct {
                fields: vec![(0, Value::Struct(value))],
            },
            None,
        )),
        Ok(None) => Ok((Struct::default(), None)),
        // Go's captured panic is a plain error, not a declared exception: the transport
        // reports it as an unexpected error (see `transport_failure`).
        Err(status) if crate::matching_panic::is_captured_panic(&status) => {
            Err(status.message().to_owned())
        }
        Err(status) => Ok(application_error(method, status)),
    }
}

/// TChannel error code for a dispatch failure. Go's `handleErr` and `thrift.FromError`
/// send a captured panic as `ErrCodeUnexpected` (0x05); other dispatch failures, such
/// as an unknown procedure, are `ErrCodeBadRequest` (0x06).
pub fn tchannel_error_code(message: &str) -> u8 {
    if crate::matching_panic::is_captured_panic_message(message) {
        0x05
    } else {
        0x06
    }
}

/// HTTP response for a dispatch failure. A captured panic matches Go's yarpc HTTP
/// inbound for a plain error: 500, `Rpc-Error-Code: unknown`, the message as text.
fn transport_failure(message: String) -> axum::response::Response {
    use axum::http::{header, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    if !crate::matching_panic::is_captured_panic_message(&message) {
        return crate::yarpc::transport_error(StatusCode::INTERNAL_SERVER_ERROR, message);
    }
    let mut response = (StatusCode::INTERNAL_SERVER_ERROR, format!("{message}\n")).into_response();
    let headers = response.headers_mut();
    headers.insert("Rpc-Status", HeaderValue::from_static("error"));
    headers.insert("Rpc-Error-Code", HeaderValue::from_static("unknown"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf8"),
    );
    response
}

/// YARPC HTTP registration, served at the Matching-only root and at /matching
/// on a combined server. Both transports share the same matcher instance.
pub fn router(matching: Matching) -> axum::Router {
    let limit = matching.frontend.state.grpc_max_message_size;
    axum::Router::new()
        .route("/", axum::routing::post(http_handler))
        .route("/{*rest}", axum::routing::post(http_handler))
        .layer(axum::extract::DefaultBodyLimit::max(limit))
        .with_state(std::sync::Arc::new(matching))
}

/// Dedicated YARPC HTTP inbound with Go-compatible procedure allowlisting.
pub fn router_with_procedures(matching: Matching, procedures: Vec<String>) -> axum::Router {
    let limit = matching.frontend.state.grpc_max_message_size;
    axum::Router::new()
        .route("/", axum::routing::post(http_handler_with_procedures))
        .route(
            "/{*rest}",
            axum::routing::post(http_handler_with_procedures),
        )
        .layer(axum::extract::DefaultBodyLimit::max(limit))
        .with_state((
            std::sync::Arc::new(matching),
            std::sync::Arc::new(procedures),
        ))
}
/// Mount both spellings explicitly: Axum's nested root only matches the
/// prefix without a trailing slash; otherwise the frontend wildcard wins.
pub fn combined_router(matching: Matching) -> axum::Router {
    let limit = matching.frontend.state.grpc_max_message_size;
    axum::Router::new()
        .nest("/matching", router(matching.clone()))
        .route(
            "/matching/",
            axum::routing::post(http_handler).with_state(std::sync::Arc::new(matching)),
        )
        .layer(axum::extract::DefaultBodyLimit::max(limit))
}
async fn http_handler(
    axum::extract::State(matching): axum::extract::State<std::sync::Arc<Matching>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::http::StatusCode;
    let header = |name| headers.get(name).and_then(|value| value.to_str().ok());
    if header("Rpc-Service").is_some_and(|service| service != "cadence-matching") {
        return crate::yarpc::transport_error(
            StatusCode::NOT_FOUND,
            "Unknown Matching service".into(),
        );
    }
    if header("Rpc-Encoding") != Some("thrift") {
        return crate::yarpc::transport_error(
            StatusCode::BAD_REQUEST,
            "Matching requires Rpc-Encoding: thrift".into(),
        );
    }
    let Some(procedure) = header("Rpc-Procedure") else {
        return crate::yarpc::transport_error(
            StatusCode::BAD_REQUEST,
            "Missing Rpc-Procedure".into(),
        );
    };
    if !cadence_thrift::service::find_method(procedure)
        .is_some_and(|method| matches!(method.service, "MatchingService" | "Meta"))
    {
        return crate::yarpc::transport_error(
            StatusCode::NOT_FOUND,
            "Unknown Matching procedure".into(),
        );
    }
    let args = match cadence_thrift::value::read_struct(&mut cadence_thrift::Reader::new(&body)) {
        Ok(args) => args,
        Err(error) => {
            return crate::yarpc::transport_error(StatusCode::BAD_REQUEST, error.to_string())
        }
    };
    let deadline = crate::yarpc::ttl_deadline(&headers, std::time::Duration::from_secs(60));
    match tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        dispatch_with_context(
            &matching,
            procedure,
            &args,
            deadline,
            crate::matching_metrics::InboundMetricContext {
                caller: header("Rpc-Caller").map(str::to_owned),
                transport: Some("http".to_owned()),
            },
        ),
    )
    .await
    {
        Ok(Ok((result, error))) => crate::yarpc::ok_reply("cadence-matching", result, error),
        Ok(Err(error)) => transport_failure(error),
        Err(_) => crate::yarpc::transport_error(
            StatusCode::GATEWAY_TIMEOUT,
            "Matching request timed out".into(),
        ),
    }
}

async fn http_handler_with_procedures(
    axum::extract::State((matching, procedures)): axum::extract::State<(
        std::sync::Arc<Matching>,
        std::sync::Arc<Vec<String>>,
    )>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let procedure = headers
        .get("Rpc-Procedure")
        .and_then(|value| value.to_str().ok());
    if !procedure.is_some_and(|name| procedures.iter().any(|allowed| allowed == name)) {
        return crate::yarpc::transport_error(
            axum::http::StatusCode::NOT_FOUND,
            "procedure is not enabled".into(),
        );
    }
    http_handler(axum::extract::State(matching), headers, body).await
}

/// Persistence drivers enable rustls' aws-lc-rs backend next to the ring
/// backend selected here, so rustls cannot pick a process default itself.
pub use cadence_service_core::grpc_tls::install_tls_crypto_provider;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn matching_inbound_metrics_count_native_and_thrift_dispatch_once_each() {
        use crate::matching_metrics::{
            MetricKind, MetricValue, RecordingMetricSink, CADENCE_REQUESTS_PER_TASK_LIST,
        };
        use std::sync::Arc;

        let sink = Arc::new(RecordingMetricSink::default());
        let state = crate::handlers::AppState::default();
        state
            .domains
            .create(
                cadence_api::types::DomainInfo {
                    name: "domain-a".into(),
                    uuid: "00000000-0000-0000-0000-000000000001".into(),
                    ..Default::default()
                },
                Default::default(),
                Default::default(),
                false,
            )
            .unwrap();
        let matching = Matching::new(state).with_metrics(sink.clone());
        let request = m::GetTaskListsByDomainRequest {
            domain: "domain-a".into(),
        };
        let mut request = Request::new(request);
        request
            .metadata_mut()
            .insert("rpc-caller", "sdk-client".parse().unwrap());
        m::matching_api_server::MatchingApi::get_task_lists_by_domain(&matching, request)
            .await
            .unwrap();

        let thrift_args = Struct {
            fields: vec![(
                1,
                Value::Struct(Struct {
                    fields: vec![(10, text("domain-a"))],
                }),
            )],
        };
        dispatch_with_context(
            &matching,
            "MatchingService::GetTaskListsByDomain",
            &thrift_args,
            Instant::now() + std::time::Duration::from_secs(5),
            crate::matching_metrics::InboundMetricContext {
                caller: Some("worker-client".into()),
                transport: Some("http".into()),
            },
        )
        .await
        .unwrap();

        let events = sink.events();
        let requests = events
            .iter()
            .filter(|event| event.name == CADENCE_REQUESTS_PER_TASK_LIST)
            .collect::<Vec<_>>();
        assert_eq!(
            requests.len(),
            2,
            "one event for each logical transport request"
        );
        assert!(requests
            .iter()
            .all(|event| event.tags.get("operation").map(String::as_str)
                == Some("GetTaskListsByDomain")));
        assert_eq!(
            requests[0].tags.get("caller").map(String::as_str),
            Some("sdk-client")
        );
        assert_eq!(
            requests[0].tags.get("transport").map(String::as_str),
            Some("grpc")
        );
        assert_eq!(
            requests[1].tags.get("caller").map(String::as_str),
            Some("worker-client")
        );
        assert_eq!(
            requests[1].tags.get("transport").map(String::as_str),
            Some("http")
        );
        assert!(events
            .iter()
            .filter(|event| event.name == "cadence_latency_per_tl")
            .all(|event| event.kind == MetricKind::Timer
                && matches!(event.value, MetricValue::Duration(_))));
    }
    #[test]
    fn matching_thrift_preserves_nanos_and_internal_decision_field_ids() {
        let response = m::PollForDecisionTaskResponse {
            started_event_id: 8,
            attempt: 2,
            next_event_id: 9,
            scheduled_time: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 123,
            }),
            started_time: Some(prost_types::Timestamp {
                seconds: 2,
                nanos: 456,
            }),
            previous_started_event_id: Some(3),
            query: Some(p::WorkflowQuery {
                query_type: "query".into(),
                query_args: Some(p::Payload { data: vec![0xff] }),
            }),
            queries: [(
                "query-id".into(),
                p::WorkflowQuery {
                    query_type: "type".into(),
                    query_args: None,
                },
            )]
            .into(),
            auto_config_hint: Some(p::AutoConfigHint {
                enable_auto_config: true,
                poller_wait_time_in_ms: 17,
            }),
            workflow_execution_task_list: Some(p::TaskList {
                name: "normal".into(),
                kind: 1,
                ..Default::default()
            }),
            decision_info: Some(cadence_proto::shared::TransientDecisionInfo {
                scheduled_event: Some(p::HistoryEvent {
                    event_id: 7,
                    event_time: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 789,
                    }),
                    attributes: Some(
                        p::history_event::Attributes::DecisionTaskScheduledEventAttributes(
                            p::DecisionTaskScheduledEventAttributes {
                                attempt: 2,
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }),
                started_event: None,
            }),
            ..Default::default()
        };
        let native =
            crate::thrift_matching_native::poll_for_decision_task_response(response.clone().into())
                .unwrap();
        let value = decision(response).unwrap();
        fn canonical(value: Value) -> Value {
            match value {
                Value::Struct(value) => {
                    let mut fields: Vec<_> = value
                        .fields
                        .into_iter()
                        .map(|(id, value)| (id, canonical(value)))
                        .collect();
                    fields.sort_by_key(|(id, _)| *id);
                    Value::Struct(Struct { fields })
                }
                Value::Map(entries) => {
                    let mut entries: Vec<_> = entries
                        .into_iter()
                        .map(|(key, value)| (canonical(key), canonical(value)))
                        .collect();
                    entries.sort_by_key(|(key, _)| format!("{key:?}"));
                    Value::Map(entries)
                }
                Value::List(entries) => Value::List(entries.into_iter().map(canonical).collect()),
                other => other,
            }
        }
        assert_eq!(
            canonical(Value::Struct(native)),
            canonical(Value::Struct(value.clone()))
        );
        assert_eq!(value.opt_i64(130).unwrap(), Some(1_000_000_123));
        assert_eq!(value.opt_i64(140).unwrap(), Some(2_000_000_456));
        assert_eq!(value.opt_i64(40).unwrap(), Some(3));
        assert_eq!(value.opt_i64(51).unwrap(), Some(2));
        let Value::Struct(info) = value.get(90).unwrap() else {
            panic!("transient info missing")
        };
        let Value::Struct(event) = info.get(10).unwrap() else {
            panic!("transient event missing")
        };
        assert_eq!(event.opt_i64(20).unwrap(), Some(1_000_000_789));
        assert!(event.get(80).is_some());
        assert_eq!(
            event.opt_i32(30).unwrap(),
            Some(cadence_api::enums::EventType::DecisionTaskScheduled as i32)
        );
    }
    #[test]
    fn matching_thrift_task_list_omits_empty_base_name_like_go_mapper() {
        let Value::Struct(plain) = list_out(p::TaskList {
            name: "tl".into(),
            kind: 2,
            ..Default::default()
        }) else {
            panic!("task list is not a struct")
        };
        assert!(plain.get(30).is_none());
        assert_eq!(plain.opt_i32(20).unwrap(), Some(1));
        let Value::Struct(partition) = list_out(p::TaskList {
            name: "/__cadence_sys/tl/1".into(),
            base_name: "tl".into(),
            kind: 1,
        }) else {
            panic!("task list is not a struct")
        };
        assert!(partition.get(30).is_some());
        assert_eq!(partition.opt_i32(20).unwrap(), Some(0));
    }
    #[test]
    fn matching_thrift_exceptions_fill_required_fields_like_go_mapper() {
        let method = cadence_thrift::find_method("MatchingService::QueryWorkflow").unwrap();
        // Untyped NotFound: thriftrw requires EntityNotExistsError.activeClusters (4).
        let (wire, error) = application_error(method, Status::not_found("gone"));
        assert_eq!(error.unwrap().0, "EntityNotExistsError");
        let Value::Struct(details) = wire.get(3).unwrap() else {
            panic!("wrong field")
        };
        assert_eq!(details.opt_string(1).unwrap().as_deref(), Some("gone"));
        assert_eq!(details.opt_string(2).unwrap().as_deref(), Some(""));
        assert_eq!(details.opt_string(3).unwrap().as_deref(), Some(""));
        assert!(matches!(details.get(4), Some(Value::List(v)) if v.is_empty()));
        // Untyped ResourceExhausted: Go always sends ServiceBusyError.reason (2).
        let (wire, error) = application_error(method, Status::resource_exhausted("busy"));
        assert_eq!(error.unwrap().0, "ServiceBusyError");
        let Value::Struct(details) = wire.get(6).unwrap() else {
            panic!("wrong field")
        };
        assert_eq!(details.opt_string(2).unwrap().as_deref(), Some(""));
        // DomainNotActiveError without decodable details still carries 2-5.
        let add = cadence_thrift::find_method("MatchingService::AddDecisionTask").unwrap();
        let status = crate::grpc::detailed_status(
            tonic::Code::FailedPrecondition,
            "inactive",
            "DomainNotActiveError",
            p::DomainNotActiveError::default(),
        );
        let (wire, error) = application_error(add, status);
        assert_eq!(error.unwrap().0, "DomainNotActiveError");
        let Value::Struct(details) = wire.get(5).unwrap() else {
            panic!("wrong field")
        };
        assert!(matches!(details.get(5), Some(Value::List(v)) if v.is_empty()));
        assert!(details.get(2).is_some() && details.get(3).is_some() && details.get(4).is_some());
    }
    #[test]
    fn matching_thrift_captured_panic_is_not_a_declared_exception() {
        // Go: CapturePanic returns a plain fmt error, which thrift.FromError passes through.
        // The same untyped status from another source stays a typed InternalServiceError.
        let method =
            cadence_thrift::service::find_method("MatchingService::AddActivityTask").unwrap();
        let panic = Status::unknown(r#"panic object is not error: "boom""#);
        assert!(crate::matching_panic::is_captured_panic(&panic));
        let (_, error) = application_error(method, Status::unknown("fake unhandled error"));
        assert_eq!(error.unwrap().0, "InternalServiceError");
        assert!(!crate::matching_panic::is_captured_panic(&Status::unknown(
            "fake unhandled error"
        )));
        assert!(!crate::matching_panic::is_captured_panic(
            &Status::internal(r#"panic object is not error: "boom""#)
        ));
    }

    #[tokio::test]
    async fn matching_thrift_nil_request_argument_follows_go_from_wire() {
        // Go `Args.FromWire` leaves a missing or wrong-typed field 1 nil. Poll, Add* and
        // DescribeTaskList dereference it in the engine and CapturePanic returns a plain
        // error; every other method reads through nil-safe getters like an empty request.
        let matching = Matching::new(crate::handlers::AppState::default());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let wrong_type = Struct {
            fields: vec![(1, Value::I32(7))],
        };
        for method in [
            "PollForDecisionTask",
            "PollForActivityTask",
            "AddDecisionTask",
            "AddActivityTask",
            "DescribeTaskList",
        ] {
            let procedure = format!("MatchingService::{method}");
            for args in [&Struct::default(), &wrong_type] {
                let error = dispatch(&matching, &procedure, args, deadline)
                    .await
                    .expect_err(method);
                assert_eq!(error, crate::matching_panic::NIL_DEREFERENCE_MESSAGE);
                assert_eq!(tchannel_error_code(&error), 0x05);
            }
        }
        // Nil-safe in Go: handled as an empty request, never a transport error.
        let (_, error) = dispatch(
            &matching,
            "MatchingService::CancelOutstandingPoll",
            &wrong_type,
            deadline,
        )
        .await
        .expect("nil-safe method");
        assert!(error.is_none_or(|(_, message)| !message.contains("Invalid request argument")));
    }

    fn quota_value(value: i64) -> cadence_api::admin::DynamicConfigValue {
        cadence_api::admin::DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(
                cadence_api::enums::EncodingType::JSON,
                serde_json::to_vec(&serde_json::json!(value)).unwrap(),
            )),
            filters: vec![],
        }
    }

    #[tokio::test]
    async fn matching_thrift_nil_request_runs_metrics_and_host_quota_before_engine_panic() {
        // Go handler order: startProfiling (startup wait, request counter), host limiter, then the
        // engine's nil dereference. A throttled nil request is therefore ServiceBusy, not the panic.
        use crate::matching_metrics::{RecordingMetricSink, CADENCE_REQUESTS_PER_TASK_LIST};
        let sink = std::sync::Arc::new(RecordingMetricSink::default());
        let state = crate::handlers::AppState::default();
        {
            let mut config = state.admin.dynamic_config.lock().unwrap();
            config.update(false, "matching.rps", vec![quota_value(1)]);
            config.update(false, "matching.workerrps", vec![quota_value(1)]);
        }
        let matching = Matching::new(state).with_metrics(sink.clone());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let nil = Struct::default();
        for method in [
            "DescribeTaskList", // user host limiter
            "AddDecisionTask",  // worker host limiter
            "PollForDecisionTask",
        ] {
            let procedure = format!("MatchingService::{method}");
            // The first call spends the single token and reaches the engine panic.
            let first = dispatch(&matching, &procedure, &nil, deadline).await;
            if method == "PollForDecisionTask" {
                // Shares the worker bucket already spent by AddDecisionTask.
                let (_, error) = first.expect("throttled, not a panic");
                assert!(
                    error.unwrap().1.contains("Matching host rps exceeded"),
                    "{method}"
                );
                continue;
            }
            assert_eq!(
                first.unwrap_err(),
                crate::matching_panic::NIL_DEREFERENCE_MESSAGE,
                "{method}"
            );
            let (_, error) = dispatch(&matching, &procedure, &nil, deadline)
                .await
                .expect("throttled, not a panic");
            let (name, message) = error.expect("ServiceBusy exception");
            assert_eq!(name, "ServiceBusyError", "{method}");
            assert_eq!(message, "Matching host rps exceeded", "{method}");
        }
        let counted = |operation: &str| {
            sink.events()
                .iter()
                .filter(|event| {
                    event.name == CADENCE_REQUESTS_PER_TASK_LIST
                        && event.tags.get("operation").map(String::as_str) == Some(operation)
                })
                .count()
        };
        assert_eq!(counted("DescribeTaskList"), 2);
        assert_eq!(counted("AddDecisionTask"), 2);
        assert_eq!(counted("PollForDecisionTask"), 1);
    }

    #[tokio::test]
    async fn matching_thrift_nil_request_waits_for_startup_like_go_start_profiling() {
        let matching = Matching::new(crate::handlers::AppState::default()).with_start_pending();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let args = Struct::default();
        let pending = dispatch(
            &matching,
            "MatchingService::AddActivityTask",
            &args,
            deadline,
        );
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "must wait for Start() like Go's startWG"
        );
        matching.mark_started();
        assert_eq!(
            pending.await.unwrap_err(),
            crate::matching_panic::NIL_DEREFERENCE_MESSAGE
        );
    }

    fn field(s: &Struct, id: i16) -> Option<&Value> {
        s.fields.iter().find(|(f, _)| *f == id).map(|(_, v)| v)
    }

    #[test]
    fn matching_thrift_wrong_wire_type_fields_are_skipped_like_go_from_wire() {
        // Go: `if field.Value.Type() == wire.TI64 { ... }` per field; anything else is skipped,
        // so i32 on an i64 field, i64 on an i32 field and i16 on an i32/enum field are all absent.
        let args = Struct {
            fields: vec![
                (10, text("domain")),
                (20, Value::I32(1)), // execution: struct expected
                (
                    30,
                    structure(vec![(10, Value::I32(5)), (20, Value::I32(2))]),
                ), // taskList: name i32, kind i32 ok
                (40, Value::I32(9)), // scheduleId i64 expected, sent i32
                (50, Value::I64(9)), // scheduleToStart i32 expected, sent i64
                (59, Value::I16(1)), // source (enum i32), sent i16
                (60, Value::I64(3)), // forwardedFrom string expected
                (70, Value::List(vec![text("a")])), // partitionConfig map expected
            ],
        };
        let out = normalize("matching.AddDecisionTaskRequest", &args);
        assert_eq!(
            out.fields.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![10, 30]
        );
        // Nested struct is normalised too: TaskList.name is binary (id 10), kind i32 (id 20).
        let Some(Value::Struct(list)) = field(&out, 30) else {
            panic!()
        };
        assert_eq!(list.fields, vec![(20, Value::I32(2))]);
        // The strict getter would have widened or rejected these; the normalised struct is absent.
        assert!(args.opt_i64(40).unwrap().is_some());
        assert_eq!(out.opt_i64(40).unwrap(), None);
        assert!(args.opt_i32(59).unwrap().is_some());
        assert_eq!(out.opt_i32(59).unwrap(), None);
        assert!(args.opt_i32(50).is_err());
        assert_eq!(out.opt_i32(50).unwrap(), None);
    }

    #[test]
    fn matching_thrift_wrong_type_known_fields_cover_every_request_struct() {
        // (struct, field id, wrong-typed value): each is present before and skipped after.
        let cases: Vec<(&str, i16, Value)> = vec![
            ("matching.PollForDecisionTaskRequest", 10, Value::I32(1)),
            ("matching.PollForDecisionTaskRequest", 15, Value::I64(1)),
            (
                "matching.PollForDecisionTaskRequest",
                20,
                Value::Bytes(vec![]),
            ),
            ("matching.PollForDecisionTaskRequest", 30, Value::Bool(true)),
            ("matching.PollForDecisionTaskRequest", 40, Value::I32(1)),
            ("matching.PollForActivityTaskRequest", 20, Value::I32(1)),
            ("matching.PollForActivityTaskRequest", 40, Value::I16(1)),
            ("matching.AddDecisionTaskRequest", 40, Value::I32(1)),
            ("matching.AddActivityTaskRequest", 50, Value::I32(1)),
            ("matching.AddActivityTaskRequest", 60, Value::I64(1)),
            ("matching.AddActivityTaskRequest", 69, Value::I64(1)),
            ("matching.AddActivityTaskRequest", 80, Value::Bytes(vec![])),
            ("matching.QueryWorkflowRequest", 20, Value::I32(1)),
            ("matching.QueryWorkflowRequest", 30, Value::I32(1)),
            (
                "matching.RespondQueryTaskCompletedRequest",
                30,
                Value::I32(1),
            ),
            (
                "matching.RespondQueryTaskCompletedRequest",
                40,
                Value::Bytes(vec![]),
            ),
            ("matching.CancelOutstandingPollRequest", 20, Value::I16(1)),
            ("matching.CancelOutstandingPollRequest", 20, Value::I64(1)),
            ("matching.DescribeTaskListRequest", 10, Value::I32(1)),
            ("matching.DescribeTaskListRequest", 20, Value::I32(1)),
            ("shared.GetTaskListsByDomainRequest", 10, Value::I32(1)),
            ("matching.ListTaskListPartitionsRequest", 10, Value::I32(1)),
            ("matching.ListTaskListPartitionsRequest", 20, Value::I32(1)),
            // Nested requests and structs reached through them.
            ("shared.PollForDecisionTaskRequest", 10, Value::I32(1)),
            ("shared.PollForActivityTaskRequest", 30, Value::I32(1)),
            ("shared.TaskListMetadata", 10, Value::I32(1)),
            ("shared.QueryWorkflowRequest", 20, Value::I32(1)),
            ("shared.WorkflowQuery", 10, Value::I32(1)),
            ("shared.RespondQueryTaskCompletedRequest", 10, Value::I64(1)),
            ("shared.DescribeTaskListRequest", 20, Value::I64(1)),
            ("shared.WorkflowExecution", 10, Value::I32(1)),
            ("shared.TaskList", 20, Value::I64(1)),
            ("matching.ActivityTaskDispatchInfo", 20, Value::I32(1)),
            ("matching.ActivityTaskDispatchInfo", 30, Value::I32(1)),
            ("matching.ActivityTaskDispatchInfo", 70, Value::I32(1)),
            ("shared.HistoryEvent", 10, Value::I32(1)),
        ];
        for (name, id, wrong) in cases {
            let input = Struct {
                fields: vec![(id, wrong)],
            };
            assert!(
                normalize(name, &input).fields.is_empty(),
                "{name} field {id} should be skipped"
            );
        }
        // Right-typed fields survive unchanged.
        let ok = Struct {
            fields: vec![(40, Value::I64(7)), (10, text("d"))],
        };
        assert_eq!(
            normalize("matching.AddDecisionTaskRequest", &ok)
                .fields
                .len(),
            2
        );
    }

    #[test]
    fn matching_thrift_container_and_nested_wrong_types_follow_go_readers() {
        // `_Map_String_String_Read` returns nil for a non-binary key/value type; a later duplicate
        // field replaces the earlier one; nested structs inside a dispatch info are normalised.
        let bad_map = Value::Map(vec![(Value::I32(1), text("v"))]);
        let good_map = Value::Map(vec![(text("k"), text("v"))]);
        let out = normalize(
            "matching.AddDecisionTaskRequest",
            &Struct {
                fields: vec![(70, bad_map.clone())],
            },
        );
        assert!(out.fields.is_empty());
        let out = normalize(
            "matching.AddDecisionTaskRequest",
            &Struct {
                fields: vec![
                    (70, good_map.clone()),
                    (70, bad_map),
                    (10, Value::I32(1)),
                    (10, text("d")),
                ],
            },
        );
        assert_eq!(out.fields, vec![(10, text("d"))]);

        let info = structure(vec![
            (
                10,
                structure(vec![(10, Value::I32(1)), (20, Value::I64(2))]),
            ), // HistoryEvent: eventId i64
            (70, structure(vec![(10, Value::I64(1))])), // WorkflowType.name wrong
        ]);
        let out = normalize(
            "matching.AddActivityTaskRequest",
            &Struct {
                fields: vec![(80, info)],
            },
        );
        let Some(Value::Struct(info)) = field(&out, 80) else {
            panic!()
        };
        let Some(Value::Struct(event)) = field(info, 10) else {
            panic!()
        };
        assert_eq!(event.fields, vec![(20, Value::I64(2))]);
        let Some(Value::Struct(workflow_type)) = field(info, 70) else {
            panic!()
        };
        assert!(workflow_type.fields.is_empty());
    }

    #[tokio::test]
    async fn matching_thrift_last_struct_request_argument_wins_like_go() {
        let matching = Matching::new(crate::handlers::AppState::default());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let valid = structure(vec![(10, text("domain"))]);
        let expected = dispatch(
            &matching,
            "MatchingService::AddDecisionTask",
            &Struct {
                fields: vec![(1, valid.clone())],
            },
            deadline,
        )
        .await;
        for fields in [
            vec![(1, Value::I32(1)), (1, valid.clone())],
            vec![(1, valid.clone()), (1, Value::I32(1))],
            vec![(1, structure(vec![])), (1, valid)],
        ] {
            assert_eq!(
                dispatch(
                    &matching,
                    "MatchingService::AddDecisionTask",
                    &Struct { fields },
                    deadline,
                )
                .await,
                expected
            );
        }
    }

    #[test]
    fn matching_tchannel_dispatch_failures_use_go_error_codes() {
        // Go TChannel: ErrCodeUnexpected (0x05) for a captured panic, ErrCodeBadRequest (0x06)
        // for an unknown procedure.
        assert_eq!(
            tchannel_error_code(r#"panic object is not error: "boom""#),
            0x05
        );
        assert_eq!(tchannel_error_code("Unknown Matching procedure X"), 0x06);
    }

    #[tokio::test]
    async fn matching_http_captured_panic_matches_go_plain_error_response() {
        // Go yarpc HTTP: plain error -> status 500, Rpc-Error-Code unknown, message plus newline.
        use axum::response::IntoResponse;
        let response = transport_failure(r#"panic object is not error: "boom""#.into());
        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(response.headers().get("Rpc-Error-Code").unwrap(), "unknown");
        assert_eq!(response.headers().get("Rpc-Status").unwrap(), "error");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"panic object is not error: \"boom\"\n");
        let other = transport_failure("Unknown Matching procedure X".into()).into_response();
        assert!(other.headers().get("Rpc-Error-Code").is_none());
    }

    #[test]
    fn matching_thrift_uses_each_methods_exception_field_ids() {
        let decision = cadence_thrift::find_method("MatchingService::AddDecisionTask").unwrap();
        let activity = cadence_thrift::find_method("MatchingService::AddActivityTask").unwrap();
        let remote = || {
            crate::grpc::detailed_status_type(
                tonic::Code::Unavailable,
                "remote sync match failed",
                "type.googleapis.com/uber.cadence.shared.v1.RemoteSyncMatchedError".into(),
                cadence_proto::shared::RemoteSyncMatchedError {},
            )
        };
        for method in [decision, activity] {
            let (wire, error) = application_error(method, remote());
            assert_eq!(error.unwrap().0, "RemoteSyncMatchedError");
            let Value::Struct(details) = wire.get(6).unwrap() else {
                panic!("wrong remote exception field")
            };
            assert_eq!(
                details.opt_string(10).unwrap().as_deref(),
                Some("remote sync match failed")
            );
        }
        let status = crate::grpc::detailed_status(
            tonic::Code::Unavailable,
            "sticky",
            "StickyWorkerUnavailableError",
            p::StickyWorkerUnavailableError {},
        );
        let (wire, error) = application_error(decision, status);
        assert_eq!(error.unwrap().0, "StickyWorkerUnavailableError");
        assert!(wire.get(7).is_some());
    }

    #[test]
    fn matching_thrift_forwarded_sync_miss_and_failed_query_use_go_exception_types() {
        // Go errRemoteSyncMatchFailed is declared on AddDecisionTask (field 6).
        let decision = cadence_thrift::find_method("MatchingService::AddDecisionTask").unwrap();
        let (wire, error) = application_error(
            decision,
            crate::matching_dispatch::remote_sync_match_failed(),
        );
        assert_eq!(error.unwrap().0, "RemoteSyncMatchedError");
        let Value::Struct(details) = wire.get(6).unwrap() else {
            panic!("wrong remote exception field")
        };
        assert_eq!(
            details.opt_string(10).unwrap().as_deref(),
            Some("remote sync match failed")
        );
        // Go QueryFailedError{Message: worker error} is declared on QueryWorkflow (field 4).
        let query = cadence_thrift::find_method("MatchingService::QueryWorkflow").unwrap();
        let failed = crate::matching_query::query_outcome(
            crate::matching_query::QueryOutcome::Worker(crate::matching_query::WorkerAnswer::new(
                p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Failed as i32,
                    answer: None,
                    error_message: "boom".into(),
                },
            )),
            false,
        )
        .unwrap_err();
        let (wire, error) = application_error(query, failed);
        assert_eq!(error.unwrap().0, "QueryFailedError");
        let Value::Struct(details) = wire.get(4).unwrap() else {
            panic!("wrong query exception field")
        };
        assert_eq!(details.opt_string(1).unwrap().as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn dedicated_http_listener_applies_configured_procedure_allowlist() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let matching = Matching::new(crate::handlers::AppState::default());
        let app =
            router_with_procedures(matching, vec!["MatchingService::DescribeTaskList".into()]);
        let request = |procedure: &str| {
            Request::post("/")
                .header("Rpc-Service", "cadence-matching")
                .header("Rpc-Encoding", "thrift")
                .header("Rpc-Procedure", procedure)
                .body(Body::from(Vec::<u8>::new()))
                .unwrap()
        };
        let rejected = app
            .clone()
            .oneshot(request("MatchingService::PollForDecisionTask"))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::NOT_FOUND);
        let allowed = app
            .oneshot(request("MatchingService::DescribeTaskList"))
            .await
            .unwrap();
        assert_ne!(allowed.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dedicated_http_listener_enforces_tls_handshake() {
        install_tls_crypto_provider();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = tempfile::NamedTempFile::new().unwrap();
        let key_path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(cert_path.path(), generated.cert.pem()).unwrap();
        std::fs::write(key_path.path(), generated.key_pair.serialize_pem()).unwrap();
        let tls =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path.path(), key_path.path())
                .await
                .unwrap();
        let app = router_with_procedures(
            Matching::new(crate::handlers::AppState::default()),
            vec!["Meta::health".into()],
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let listener = listener.into_std().unwrap();
        let server = tokio::spawn(
            axum_server::from_tcp_rustls(listener, tls)
                .unwrap()
                .serve(app.into_make_service()),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(generated.cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut stream = connector
            .connect("localhost".try_into().unwrap(), tcp)
            .await
            .unwrap();
        stream.write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nRpc-Service: cadence-matching\r\nRpc-Encoding: thrift\r\nRpc-Procedure: Meta::health\r\nContent-Length: 1\r\nConnection: close\r\n\r\n\0").await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        let mut plaintext = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = plaintext
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        let mut response = [0_u8; 256];
        let plaintext_read = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            plaintext.read(&mut response),
        )
        .await;
        assert!(
            matches!(plaintext_read, Ok(Ok(count)) if count > 0 && response[0] == 0x15),
            "plaintext did not receive a TLS alert: {plaintext_read:?}, bytes={response:?}"
        );
        server.abort();
    }
}
