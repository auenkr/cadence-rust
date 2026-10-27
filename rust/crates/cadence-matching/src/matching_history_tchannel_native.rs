//! Byte-preserving (Go string) RecordDecisionTaskStarted / RecordActivityTaskStarted
//! over the outbound TChannel History client. Private child of
//! `matching_history_tchannel`; reuses its `TypedStruct` wire model and `call`,
//! but maps the native proto models (`GoString` fields) directly, never through
//! `String`.
//!
//! Supported History event graph (same as the legacy converter, and all that
//! Go's Record*Started responses can carry): DecisionTaskScheduled (4),
//! DecisionTaskStarted (5) and ActivityTaskScheduled (9) events. Any other
//! event type is rejected with an error rather than silently dropped.
use super::*;
use cadence_common::go_string::GoString;
use cadence_proto::native as n;

fn gs(value: &GoString) -> TypedValue {
    TypedValue::Bytes(value.as_bytes().to_vec())
}

fn from_gs(value: Vec<u8>) -> GoString {
    GoString::from(value)
}

fn field_gs(s: &TypedStruct, id: i16) -> cadence_thrift::Result<GoString> {
    field_bytes(s, id).map(from_gs)
}

fn workflow_execution_n(value: &n::api::WorkflowExecution) -> TypedValue {
    struct_value(vec![
        field(10, gs(&value.workflow_id)),
        field(20, gs(&value.run_id)),
    ])
}

fn task_list_n(value: &n::api::TaskList) -> TypedValue {
    let mut fields = vec![field(10, gs(&value.name))];
    if (1..=3).contains(&value.kind) {
        fields.push(field(20, TypedValue::I32(value.kind - 1)));
    }
    if !value.base_name.is_empty() {
        fields.push(field(30, gs(&value.base_name)));
    }
    struct_value(fields)
}

fn poll_decision_n(value: &n::api::PollForDecisionTaskRequest) -> TypedValue {
    let mut fields = vec![
        field(10, gs(&value.domain)),
        field(30, gs(&value.identity)),
        field(40, gs(&value.binary_checksum)),
    ];
    if let Some(task) = &value.task_list {
        fields.push(field(20, task_list_n(task)));
    }
    struct_value(finish(fields).fields)
}

fn poll_activity_n(value: &n::api::PollForActivityTaskRequest) -> TypedValue {
    let mut fields = vec![field(10, gs(&value.domain)), field(30, gs(&value.identity))];
    if let Some(task) = &value.task_list {
        fields.push(field(20, task_list_n(task)));
    }
    if let Some(metadata) = &value.task_list_metadata {
        let mut inner = Vec::new();
        if let Some(maximum) = metadata.max_tasks_per_second {
            inner.push(field(10, TypedValue::F64(maximum)));
        }
        fields.push(field(40, struct_value(inner)));
    }
    struct_value(finish(fields).fields)
}

pub(crate) fn record_decision_task_started_request_native(
    value: &n::history::RecordDecisionTaskStartedRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, gs(&value.domain_id)),
        field(30, i64_value(value.schedule_id)),
        field(40, i64_value(value.task_id)),
        field(45, gs(&value.request_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution_n(execution)));
    }
    if let Some(request) = &value.poll_request {
        fields.push(field(50, poll_decision_n(request)));
    }
    finish(fields)
}

pub(crate) fn record_activity_task_started_request_native(
    value: &n::history::RecordActivityTaskStartedRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, gs(&value.domain_id)),
        field(30, i64_value(value.schedule_id)),
        field(40, i64_value(value.task_id)),
        field(45, gs(&value.request_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution_n(execution)));
    }
    if let Some(request) = &value.poll_request {
        fields.push(field(50, poll_activity_n(request)));
    }
    finish(fields)
}

fn task_list_from_thrift_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::TaskList> {
    let kind = typed_field(s, 20)
        .map(|v| as_i32(v, 20))
        .transpose()?
        .map_or(0, |v| if (0..=2).contains(&v) { v + 1 } else { 0 });
    Ok(n::api::TaskList {
        name: field_gs(s, 10)?,
        kind,
        base_name: field_gs(s, 30)?,
    })
}

fn workflow_type_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::WorkflowType> {
    Ok(n::api::WorkflowType {
        name: field_gs(s, 10)?,
    })
}

fn retry_policy_n(policy: &TypedStruct) -> cadence_thrift::Result<n::api::RetryPolicy> {
    let non_retryable_error_reasons = match typed_field(policy, 50) {
        None => Vec::new(),
        Some(TypedValue::List(TType::String, values)) => values
            .iter()
            .map(|value| bytes_value(value, 50).map(from_gs))
            .collect::<cadence_thrift::Result<Vec<_>>>()?,
        Some(_) => {
            return Err(thrift_invalid(
                "RetryPolicy.nonRetriableErrorReasons: expected list<string>",
            ))
        }
    };
    Ok(n::api::RetryPolicy {
        initial_interval: duration_from_seconds(Some(field_i32(policy, 10)?)),
        backoff_coefficient: typed_field(policy, 20)
            .map(|v| as_f64(v, 20))
            .transpose()?
            .unwrap_or_default(),
        maximum_interval: duration_from_seconds(Some(field_i32(policy, 30)?)),
        maximum_attempts: field_i32(policy, 40)?,
        non_retryable_error_reasons,
        expiration_interval: duration_from_seconds(Some(field_i32(policy, 60)?)),
        jitter_coefficient: typed_field(policy, 70)
            .map(|v| as_f64(v, 70))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn history_event_n(value: &TypedStruct) -> cadence_thrift::Result<n::api::HistoryEvent> {
    use n::api::history_event::Attributes;
    let event_type = field_i32(value, 30)?;
    let attributes = match event_type {
        4 => {
            let attrs = field_struct(value, 80)?
                .ok_or_else(|| thrift_invalid("decision scheduled event is missing field 80"))?;
            Some(Attributes::DecisionTaskScheduledEventAttributes(
                n::api::DecisionTaskScheduledEventAttributes {
                    task_list: field_struct(attrs, 10)?
                        .map(task_list_from_thrift_n)
                        .transpose()?,
                    start_to_close_timeout: duration_from_seconds(
                        typed_field(attrs, 20).map(|v| as_i32(v, 20)).transpose()?,
                    ),
                    attempt: field_i64(attrs, 30)? as i32,
                },
            ))
        }
        5 => {
            let attrs = field_struct(value, 90)?
                .ok_or_else(|| thrift_invalid("decision started event is missing field 90"))?;
            Some(Attributes::DecisionTaskStartedEventAttributes(
                n::api::DecisionTaskStartedEventAttributes {
                    scheduled_event_id: field_i64(attrs, 10)?,
                    identity: field_gs(attrs, 20)?,
                    request_id: field_gs(attrs, 30)?,
                },
            ))
        }
        9 => {
            let attrs = field_struct(value, 130)?
                .ok_or_else(|| thrift_invalid("activity scheduled event is missing field 130"))?;
            let activity_type = field_struct(attrs, 20)?
                .map(|t| -> cadence_thrift::Result<_> {
                    Ok(n::api::ActivityType {
                        name: field_gs(t, 10)?,
                    })
                })
                .transpose()?;
            let input = field_bytes(attrs, 40)?;
            let retry_policy = field_struct(attrs, 110)?.map(retry_policy_n).transpose()?;
            let header = field_struct(attrs, 120)?
                .map(|header| {
                    let mut fields = std::collections::HashMap::new();
                    for (key, data) in typed_map_entries(header, 10)? {
                        fields.insert(
                            from_gs(bytes_value(key, 10)?),
                            n::api::Payload {
                                data: bytes_value(data, 10)?,
                            },
                        );
                    }
                    Ok::<_, cadence_thrift::Error>(n::api::Header { fields })
                })
                .transpose()?;
            Some(Attributes::ActivityTaskScheduledEventAttributes(
                n::api::ActivityTaskScheduledEventAttributes {
                    activity_id: field_gs(attrs, 10)?,
                    activity_type,
                    domain: field_gs(attrs, 25)?,
                    task_list: field_struct(attrs, 30)?
                        .map(task_list_from_thrift_n)
                        .transpose()?,
                    input: (!input.is_empty() || typed_field(attrs, 40).is_some())
                        .then_some(n::api::Payload { data: input }),
                    schedule_to_close_timeout: duration_from_seconds(
                        typed_field(attrs, 45).map(|v| as_i32(v, 45)).transpose()?,
                    ),
                    schedule_to_start_timeout: duration_from_seconds(
                        typed_field(attrs, 50).map(|v| as_i32(v, 50)).transpose()?,
                    ),
                    start_to_close_timeout: duration_from_seconds(
                        typed_field(attrs, 55).map(|v| as_i32(v, 55)).transpose()?,
                    ),
                    heartbeat_timeout: duration_from_seconds(
                        typed_field(attrs, 60).map(|v| as_i32(v, 60)).transpose()?,
                    ),
                    decision_task_completed_event_id: field_i64(attrs, 90)?,
                    retry_policy,
                    header,
                },
            ))
        }
        _ => {
            return Err(thrift_invalid(format!(
                "unsupported Matching History event type {event_type}"
            )))
        }
    };
    Ok(n::api::HistoryEvent {
        event_id: field_i64(value, 10)?,
        event_time: timestamp_from_nanos(
            typed_field(value, 20).map(|v| as_i64(v, 20)).transpose()?,
        ),
        version: field_i64(value, 35)?,
        task_id: field_i64(value, 36)?,
        attributes,
    })
}

pub(crate) fn record_decision_task_started_response_native(
    value: &TypedStruct,
) -> cadence_thrift::Result<n::history::RecordDecisionTaskStartedResponse> {
    let mut queries = std::collections::HashMap::new();
    match typed_field(value, 140) {
        None => {}
        Some(TypedValue::Map(TType::String, TType::Struct, values)) => {
            for (key, query) in values {
                let TypedValue::Struct(query) = query else {
                    return Err(thrift_invalid("queries map value is not struct"));
                };
                let query_args = typed_field(query, 20)
                    .map(|v| bytes_value(v, 20))
                    .transpose()?
                    .map(|data| n::api::Payload { data });
                queries.insert(
                    from_gs(bytes_value(key, 140)?),
                    n::api::WorkflowQuery {
                        query_type: field_gs(query, 10)?,
                        query_args,
                    },
                );
            }
        }
        Some(_) => return Err(thrift_invalid("field 140: expected map<string,struct>")),
    }
    let decision_info = field_struct(value, 80)?
        .map(|info| -> cadence_thrift::Result<_> {
            Ok(n::shared::TransientDecisionInfo {
                scheduled_event: field_struct(info, 10)?.map(history_event_n).transpose()?,
                started_event: field_struct(info, 20)?.map(history_event_n).transpose()?,
            })
        })
        .transpose()?;
    Ok(n::history::RecordDecisionTaskStartedResponse {
        workflow_type: field_struct(value, 10)?.map(workflow_type_n).transpose()?,
        previous_started_event_id: typed_field(value, 20).map(|v| as_i64(v, 20)).transpose()?,
        scheduled_event_id: field_i64(value, 30)?,
        started_event_id: field_i64(value, 40)?,
        next_event_id: field_i64(value, 50)?,
        attempt: field_i64(value, 60)? as i32,
        sticky_execution_enabled: field_bool(value, 70)?,
        decision_info,
        workflow_execution_task_list: field_struct(value, 90)?
            .map(task_list_from_thrift_n)
            .transpose()?,
        event_store_version: field_i32(value, 100)?,
        branch_token: field_bytes(value, 110)?,
        scheduled_time: timestamp_from_nanos(
            typed_field(value, 120)
                .map(|v| as_i64(v, 120))
                .transpose()?,
        ),
        started_time: timestamp_from_nanos(
            typed_field(value, 130)
                .map(|v| as_i64(v, 130))
                .transpose()?,
        ),
        queries,
        history_size: field_i64(value, 150)?,
    })
}

pub(crate) fn record_activity_task_started_response_native(
    value: &TypedStruct,
) -> cadence_thrift::Result<n::history::RecordActivityTaskStartedResponse> {
    let heartbeat = field_bytes(value, 60)?;
    Ok(n::history::RecordActivityTaskStartedResponse {
        scheduled_event: field_struct(value, 20)?.map(history_event_n).transpose()?,
        started_time: timestamp_from_nanos(
            typed_field(value, 30).map(|v| as_i64(v, 30)).transpose()?,
        ),
        attempt: field_i64(value, 40)? as i32,
        scheduled_time_of_this_attempt: timestamp_from_nanos(
            typed_field(value, 50).map(|v| as_i64(v, 50)).transpose()?,
        ),
        heartbeat_details: (!heartbeat.is_empty() || typed_field(value, 60).is_some())
            .then_some(n::api::Payload { data: heartbeat }),
        workflow_type: field_struct(value, 70)?.map(workflow_type_n).transpose()?,
        workflow_domain: field_gs(value, 80)?,
    })
}

impl TChannelHistoryClient {
    pub(crate) async fn record_decision_task_started_native(
        &self,
        request: &n::history::RecordDecisionTaskStartedRequest,
        timeout: Duration,
    ) -> Result<n::history::RecordDecisionTaskStartedResponse, Status> {
        let response = self
            .call(
                "RecordDecisionTaskStarted",
                &record_decision_task_started_request_native(request),
                timeout,
            )
            .await?;
        record_decision_task_started_response_native(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History RecordDecisionTaskStarted response: {error}"
            ))
        })
    }

    pub(crate) async fn record_activity_task_started_native(
        &self,
        request: &n::history::RecordActivityTaskStartedRequest,
        timeout: Duration,
    ) -> Result<n::history::RecordActivityTaskStartedResponse, Status> {
        let response = self
            .call(
                "RecordActivityTaskStarted",
                &record_activity_task_started_request_native(request),
                timeout,
            )
            .await?;
        record_activity_task_started_response_native(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History RecordActivityTaskStarted response: {error}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_request_keeps_raw_bytes_and_field_ids() {
        let request = n::history::RecordDecisionTaskStartedRequest {
            domain_id: GoString::from(&b"d\xff"[..]),
            workflow_execution: Some(n::api::WorkflowExecution {
                workflow_id: GoString::from(&b"w\xfe"[..]),
                run_id: GoString::from(&b"r\xfd"[..]),
            }),
            schedule_id: 3,
            task_id: 4,
            request_id: GoString::from(&b"q\xfc"[..]),
            poll_request: Some(n::api::PollForDecisionTaskRequest {
                domain: GoString::from(&b"n\xfb"[..]),
                task_list: Some(n::api::TaskList {
                    name: GoString::from(&b"t\xfa"[..]),
                    kind: 2,
                    base_name: GoString::from(&b"b\xf9"[..]),
                }),
                identity: GoString::from(&b"i\xf8"[..]),
                binary_checksum: GoString::from(&b"c\xf7"[..]),
            }),
        };
        let wire = record_decision_task_started_request_native(&request);
        assert_eq!(
            typed_field(&wire, 10),
            Some(&TypedValue::Bytes(b"d\xff".to_vec()))
        );
        assert_eq!(
            typed_field(&wire, 45),
            Some(&TypedValue::Bytes(b"q\xfc".to_vec()))
        );
        let Some(TypedValue::Struct(execution)) = typed_field(&wire, 20) else {
            panic!("execution")
        };
        assert_eq!(
            typed_field(execution, 10),
            Some(&TypedValue::Bytes(b"w\xfe".to_vec()))
        );
        let Some(TypedValue::Struct(poll)) = typed_field(&wire, 50) else {
            panic!("poll")
        };
        assert_eq!(
            typed_field(poll, 40),
            Some(&TypedValue::Bytes(b"c\xf7".to_vec()))
        );
        let Some(TypedValue::Struct(list)) = typed_field(poll, 20) else {
            panic!("list")
        };
        assert_eq!(typed_field(list, 20), Some(&TypedValue::I32(1)));
        assert_eq!(
            typed_field(list, 30),
            Some(&TypedValue::Bytes(b"b\xf9".to_vec()))
        );
        // Encoded bytes survive the Thrift codec unchanged.
        assert_eq!(
            decode_typed_struct(&encode_typed_struct(&wire)).unwrap(),
            wire
        );
    }

    #[test]
    fn decision_response_keeps_raw_query_map_and_transient_events() {
        let started = TypedStruct {
            fields: vec![
                field(10, struct_value(vec![field(10, binary(b"wt\xff"))])),
                field(30, i64_value(7)),
                field(
                    80,
                    struct_value(vec![field(
                        20,
                        struct_value(vec![
                            field(10, i64_value(8)),
                            field(30, TypedValue::I32(5)),
                            field(
                                90,
                                struct_value(vec![
                                    field(10, i64_value(7)),
                                    field(20, binary(b"id\xfe")),
                                    field(30, binary(b"rq\xfd")),
                                ]),
                            ),
                        ]),
                    )]),
                ),
                field(
                    140,
                    TypedValue::Map(
                        TType::String,
                        TType::Struct,
                        vec![(
                            binary(b"k\xfc"),
                            struct_value(vec![
                                field(10, binary(b"qt\xfb")),
                                field(20, binary(b"\x00\xff")),
                            ]),
                        )],
                    ),
                ),
            ],
        };
        let response = record_decision_task_started_response_native(&started).unwrap();
        assert_eq!(response.workflow_type.unwrap().name.as_bytes(), b"wt\xff");
        let query = &response.queries[&b"k\xfc"[..]];
        assert_eq!(query.query_type.as_bytes(), b"qt\xfb");
        assert_eq!(query.query_args.as_ref().unwrap().data, b"\x00\xff");
        let event = response.decision_info.unwrap().started_event.unwrap();
        let Some(n::api::history_event::Attributes::DecisionTaskStartedEventAttributes(attrs)) =
            event.attributes
        else {
            panic!("attributes")
        };
        assert_eq!(attrs.identity.as_bytes(), b"id\xfe");
        assert_eq!(attrs.request_id.as_bytes(), b"rq\xfd");
    }

    #[test]
    fn activity_response_keeps_raw_header_retry_and_rejects_unknown_events() {
        let scheduled = TypedStruct {
            fields: vec![
                field(10, i64_value(2)),
                field(30, TypedValue::I32(9)),
                field(
                    130,
                    struct_value(vec![
                        field(10, binary(b"a\xff")),
                        field(20, struct_value(vec![field(10, binary(b"at\xfe"))])),
                        field(25, binary(b"dm\xfd")),
                        field(40, binary(b"in\xfc")),
                        field(
                            110,
                            struct_value(vec![field(
                                50,
                                TypedValue::List(TType::String, vec![binary(b"e\xfb")]),
                            )]),
                        ),
                        field(
                            120,
                            struct_value(vec![field(
                                10,
                                TypedValue::Map(
                                    TType::String,
                                    TType::String,
                                    vec![(binary(b"h\xfa"), binary(b"v\xf9"))],
                                ),
                            )]),
                        ),
                    ]),
                ),
            ],
        };
        let wire = TypedStruct {
            fields: vec![
                field(20, TypedValue::Struct(scheduled.clone())),
                field(80, binary(b"wd\xf8")),
            ],
        };
        let response = record_activity_task_started_response_native(&wire).unwrap();
        assert_eq!(response.workflow_domain.as_bytes(), b"wd\xf8");
        let Some(n::api::history_event::Attributes::ActivityTaskScheduledEventAttributes(a)) =
            response.scheduled_event.unwrap().attributes
        else {
            panic!("attributes")
        };
        assert_eq!(a.activity_id.as_bytes(), b"a\xff");
        assert_eq!(a.domain.as_bytes(), b"dm\xfd");
        assert_eq!(a.input.unwrap().data, b"in\xfc");
        assert_eq!(
            a.retry_policy.unwrap().non_retryable_error_reasons[0].as_bytes(),
            b"e\xfb"
        );
        assert_eq!(a.header.unwrap().fields[&b"h\xfa"[..]].data, b"v\xf9");

        let mut unknown = scheduled;
        unknown.fields[1] = field(30, TypedValue::I32(1));
        assert!(history_event_n(&unknown).is_err());
    }
}
