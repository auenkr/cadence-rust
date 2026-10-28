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

// ---- GetMutableState / DescribeWorkflowExecution / RefreshWorkflowTasks ----
// Native (GoString) ports of the legacy encoders/decoders in the parent module.
// Same Thrift field ids, enum offsets, units and optional presence; no UTF-8
// validation of any string field.

pub(crate) fn get_mutable_state_request_native(
    value: &n::history::GetMutableStateRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, gs(&value.domain_id)),
        field(30, i64_value(value.expected_next_event_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution_n(execution)));
    }
    if !value.current_branch_token.is_empty() {
        fields.push(field(40, binary(&value.current_branch_token)));
    }
    if let Some(item) = &value.version_history_item {
        // Go's FromVersionHistoryItem always sets both pointers.
        fields.push(field(
            50,
            struct_value(vec![
                field(10, i64_value(item.event_id)),
                field(20, i64_value(item.version)),
            ]),
        ));
    }
    finish(fields)
}

pub(crate) fn refresh_workflow_tasks_request_native(
    value: &n::history::RefreshWorkflowTasksRequest,
) -> TypedStruct {
    let mut inner = vec![field(10, gs(&value.domain))];
    if let Some(execution) = &value.workflow_execution {
        inner.push(field(20, workflow_execution_n(execution)));
    }
    // The outer IDL misspells domainUUID; ids are what matter on the wire.
    finish(vec![
        field(10, gs(&value.domain_id)),
        field(20, TypedValue::Struct(finish(inner))),
    ])
}

pub(crate) fn describe_workflow_execution_request_native(
    value: &n::history::DescribeWorkflowExecutionRequest,
) -> TypedStruct {
    let mut fields = vec![field(10, gs(&value.domain_id))];
    let Some(request) = value.request.as_ref() else {
        return finish(fields);
    };
    let mut nested = vec![field(10, gs(&request.domain))];
    if let Some(execution) = request.workflow_execution.as_ref() {
        nested.push(field(20, workflow_execution_n(execution)));
    }
    let level = request.query_consistency_level;
    if (1..=2).contains(&level) {
        // Thrift QueryConsistencyLevel is zero based; proto reserves INVALID.
        nested.push(field(30, TypedValue::I32(level - 1)));
    }
    fields.push(field(20, TypedValue::Struct(finish(nested))));
    finish(fields)
}

fn workflow_execution_from_thrift_n(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::api::WorkflowExecution> {
    Ok(n::api::WorkflowExecution {
        workflow_id: field_gs(s, 10)?,
        run_id: field_gs(s, 20)?,
    })
}

fn version_histories_from_thrift_n(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::shared::VersionHistories> {
    let histories = field_list_struct(s, 20)
        .map_err(|_| thrift_invalid("VersionHistories.histories: expected list<struct>"))?
        .into_iter()
        .map(|history| {
            let items = field_list_struct(history, 20)
                .map_err(|_| thrift_invalid("VersionHistory.items: expected list<struct>"))?
                .into_iter()
                .map(|item| {
                    Ok(n::admin::VersionHistoryItem {
                        event_id: field_i64(item, 10)?,
                        version: field_i64(item, 20)?,
                    })
                })
                .collect::<cadence_thrift::Result<Vec<_>>>()?;
            Ok(n::admin::VersionHistory {
                branch_token: field_bytes(history, 10)?,
                items,
            })
        })
        .collect::<cadence_thrift::Result<Vec<_>>>()?;
    Ok(n::shared::VersionHistories {
        current_version_history_index: field_i32(s, 10)?,
        histories,
    })
}

/// Raw-byte `GetMutableStateResponse`; mirrors the legacy decoder.
pub(crate) fn get_mutable_state_response_native(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::history::GetMutableStateResponse> {
    let timeout = typed_field(s, 110)
        .map(|v| as_i32(v, 110))
        .transpose()?
        .map(|seconds| prost_types::Duration {
            seconds: i64::from(seconds),
            nanos: 0,
        });
    Ok(n::history::GetMutableStateResponse {
        workflow_execution: field_struct(s, 10)?
            .map(workflow_execution_from_thrift_n)
            .transpose()?,
        workflow_type: field_struct(s, 20)?
            .map(workflow_type_n)
            .transpose()?,
        next_event_id: field_i64(s, 30)?,
        previous_started_event_id: typed_field(s, 35).map(|v| as_i64(v, 35)).transpose()?,
        last_first_event_id: field_i64(s, 40)?,
        task_list: field_struct(s, 50)?
            .map(task_list_from_thrift_n)
            .transpose()?,
        sticky_task_list: field_struct(s, 60)?
            .map(task_list_from_thrift_n)
            .transpose()?,
        client_library_version: field_gs(s, 70)?,
        client_feature_version: field_gs(s, 80)?,
        client_impl: field_gs(s, 90)?,
        sticky_task_list_schedule_to_start_timeout: timeout,
        event_store_version: field_i32(s, 120)?,
        current_branch_token: field_bytes(s, 130)?,
        workflow_state: workflow_state_from_thrift(field_i32(s, 150)?),
        workflow_close_state: close_state_from_thrift(field_i32(s, 160)?),
        version_histories: field_struct(s, 170)?
            .map(version_histories_from_thrift_n)
            .transpose()?,
        is_sticky_task_list_enabled: field_bool(s, 180)?,
        history_size: field_i64(s, 190)?,
    })
}

fn gs_value_n(value: &TypedValue, id: i16) -> cadence_thrift::Result<GoString> {
    bytes_value(value, id).map(from_gs)
}

fn payload_map_n(
    s: &TypedStruct,
    id: i16,
) -> cadence_thrift::Result<std::collections::HashMap<GoString, n::api::Payload>> {
    let mut result = std::collections::HashMap::new();
    if let Some(fields) = field_struct(s, id)? {
        for (key, value) in typed_map_entries(fields, 10)? {
            result.insert(
                gs_value_n(key, id)?,
                n::api::Payload {
                    data: bytes_value(value, id)?,
                },
            );
        }
    }
    Ok(result)
}

fn string_map_n(
    s: &TypedStruct,
    id: i16,
) -> cadence_thrift::Result<std::collections::HashMap<GoString, GoString>> {
    let mut result = std::collections::HashMap::new();
    for (key, value) in typed_map_entries(s, id)? {
        result.insert(gs_value_n(key, id)?, gs_value_n(value, id)?);
    }
    Ok(result)
}

fn reset_points_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::ResetPoints> {
    let points = field_list_struct(s, 10)?
        .into_iter()
        .map(|point| {
            Ok(n::api::ResetPointInfo {
                binary_checksum: field_gs(point, 10)?,
                run_id: field_gs(point, 20)?,
                first_decision_completed_id: field_i64(point, 30)?,
                created_time: timestamp_from_nanos(
                    typed_field(point, 40).map(|v| as_i64(v, 40)).transpose()?,
                ),
                expiring_time: timestamp_from_nanos(
                    typed_field(point, 50).map(|v| as_i64(v, 50)).transpose()?,
                ),
                resettable: field_bool(point, 60)?,
            })
        })
        .collect::<cadence_thrift::Result<Vec<_>>>()?;
    Ok(n::api::ResetPoints { points })
}

fn active_cluster_selection_policy_n(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::api::ActiveClusterSelectionPolicy> {
    let cluster_attribute = field_struct(s, 1)?
        .map(|attribute| -> cadence_thrift::Result<_> {
            Ok(n::api::ClusterAttribute {
                scope: field_gs(attribute, 1)?,
                name: field_gs(attribute, 2)?,
            })
        })
        .transpose()?;
    Ok(n::api::ActiveClusterSelectionPolicy { cluster_attribute })
}

fn workflow_execution_info_n(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::api::WorkflowExecutionInfo> {
    let workflow_execution = field_struct(s, 10)?
        .map(workflow_execution_from_thrift_n)
        .transpose()?;
    let parent_execution = field_struct(s, 80)?
        .map(workflow_execution_from_thrift_n)
        .transpose()?;
    let parent_execution_info = if [70, 71, 72].iter().any(|id| typed_field(s, *id).is_some())
        || parent_execution.is_some()
    {
        Some(n::api::ParentExecutionInfo {
            domain_id: field_gs(s, 70)?,
            domain_name: field_gs(s, 71)?,
            workflow_execution: parent_execution,
            initiated_id: field_i64(s, 72)?,
        })
    } else {
        None
    };
    let mut task_list_info = field_struct(s, 121)?
        .map(task_list_from_thrift_n)
        .transpose()?;
    let task_list = match typed_field(s, 120) {
        Some(value) => gs_value_n(value, 120)?,
        None => task_list_info
            .as_ref()
            .map(|task| task.name.clone())
            .unwrap_or_default(),
    };
    // Go's MigrateTaskList synthesizes a NORMAL TaskList from the legacy name.
    if task_list_info.is_none() && !task_list.is_empty() {
        task_list_info = Some(n::api::TaskList {
            name: task_list.clone(),
            kind: 1,
            ..Default::default()
        });
    }
    let time = |id: i16| -> cadence_thrift::Result<_> {
        Ok(timestamp_from_nanos(
            typed_field(s, id).map(|v| as_i64(v, id)).transpose()?,
        ))
    };
    Ok(n::api::WorkflowExecutionInfo {
        workflow_execution,
        r#type: field_struct(s, 20)?.map(workflow_type_n).transpose()?,
        start_time: time(30)?,
        close_time: time(40)?,
        close_status: enum_field_value(s, 50, 6)?,
        history_length: field_i64(s, 60)?,
        parent_execution_info,
        execution_time: time(90)?,
        memo: field_struct(s, 100)?
            .map(|_| payload_map_n(s, 100).map(|fields| n::api::Memo { fields }))
            .transpose()?,
        search_attributes: field_struct(s, 101)?
            .map(|_| {
                payload_map_n(s, 101).map(|indexed_fields| n::api::SearchAttributes {
                    indexed_fields,
                })
            })
            .transpose()?,
        auto_reset_points: field_struct(s, 110)?
            .map(reset_points_n)
            .transpose()?,
        task_list,
        task_list_info,
        is_cron: field_bool(s, 130)?,
        update_time: time(140)?,
        partition_config: string_map_n(s, 150)?,
        cron_overlap_policy: enum_field_value(s, 160, 2)?,
        active_cluster_selection_policy: field_struct(s, 170)?
            .map(active_cluster_selection_policy_n)
            .transpose()?,
        cron_schedule: field_gs(s, 180)?,
        execution_status: enum_field_value(s, 190, 8)?,
        scheduled_execution_time: time(200)?,
    })
}

fn workflow_execution_configuration_n(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::api::WorkflowExecutionConfiguration> {
    let duration = |id| -> cadence_thrift::Result<Option<prost_types::Duration>> {
        Ok(duration_from_seconds(
            typed_field(s, id).map(|v| as_i32(v, id)).transpose()?,
        ))
    };
    Ok(n::api::WorkflowExecutionConfiguration {
        task_list: field_struct(s, 10)?
            .map(task_list_from_thrift_n)
            .transpose()?,
        execution_start_to_close_timeout: duration(20)?,
        task_start_to_close_timeout: duration(30)?,
    })
}

fn pending_activity_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::PendingActivityInfo> {
    let activity_type = field_struct(s, 20)?
        .map(|value| -> cadence_thrift::Result<_> {
            Ok(n::api::ActivityType {
                name: field_gs(value, 10)?,
            })
        })
        .transpose()?;
    let reason = typed_field(s, 110)
        .map(|value| gs_value_n(value, 110))
        .transpose()?;
    let details = field_bytes(s, 130)?;
    let options = field_struct(s, 135)?
        .map(|options| -> cadence_thrift::Result<_> {
            Ok(n::api::FailureOptions {
                failure_category: enum_field_value(options, 10, 3)?,
                next_retry_interval: duration_from_seconds(
                    typed_field(options, 20).map(|v| as_i32(v, 20)).transpose()?,
                ),
            })
        })
        .transpose()?;
    let last_failure = if reason.is_some() || !details.is_empty() || options.is_some() {
        Some(n::api::Failure {
            reason: reason.unwrap_or_default(),
            details,
            options,
        })
    } else {
        None
    };
    let heartbeat = field_bytes(s, 40)?;
    let time = |id: i16| -> cadence_thrift::Result<_> {
        Ok(timestamp_from_nanos(
            typed_field(s, id).map(|v| as_i64(v, id)).transpose()?,
        ))
    };
    Ok(n::api::PendingActivityInfo {
        activity_id: field_gs(s, 10)?,
        activity_type,
        state: enum_field_value(s, 30, 3)?,
        heartbeat_details: (!heartbeat.is_empty()).then_some(n::api::Payload { data: heartbeat }),
        last_heartbeat_time: time(50)?,
        last_started_time: time(60)?,
        attempt: field_i32(s, 70)?,
        maximum_attempts: field_i32(s, 80)?,
        scheduled_time: time(90)?,
        expiration_time: time(100)?,
        last_failure,
        last_worker_identity: field_gs(s, 120)?,
        started_worker_identity: field_gs(s, 140)?,
        schedule_id: field_i64(s, 150)?,
    })
}

fn pending_child_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::PendingChildExecutionInfo> {
    Ok(n::api::PendingChildExecutionInfo {
        workflow_execution: Some(n::api::WorkflowExecution {
            workflow_id: field_gs(s, 10)?,
            run_id: field_gs(s, 20)?,
        }),
        workflow_type_name: field_gs(s, 30)?,
        initiated_id: field_i64(s, 40)?,
        parent_close_policy: enum_field_value(s, 50, 3)?,
        domain: field_gs(s, 1)?,
    })
}

fn pending_decision_n(s: &TypedStruct) -> cadence_thrift::Result<n::api::PendingDecisionInfo> {
    let time = |id: i16| -> cadence_thrift::Result<_> {
        Ok(timestamp_from_nanos(
            typed_field(s, id).map(|v| as_i64(v, id)).transpose()?,
        ))
    };
    Ok(n::api::PendingDecisionInfo {
        state: enum_field_value(s, 10, 2)?,
        scheduled_time: time(20)?,
        started_time: time(30)?,
        attempt: field_i64(s, 40)? as i32,
        original_scheduled_time: time(50)?,
        schedule_id: field_i64(s, 60)?,
    })
}

pub(crate) fn describe_workflow_execution_response_native(
    s: &TypedStruct,
) -> cadence_thrift::Result<n::history::DescribeWorkflowExecutionResponse> {
    Ok(n::history::DescribeWorkflowExecutionResponse {
        execution_configuration: field_struct(s, 10)?
            .map(workflow_execution_configuration_n)
            .transpose()?,
        workflow_execution_info: field_struct(s, 20)?
            .map(workflow_execution_info_n)
            .transpose()?,
        pending_activities: field_list_struct(s, 30)?
            .into_iter()
            .map(pending_activity_n)
            .collect::<cadence_thrift::Result<Vec<_>>>()?,
        pending_children: field_list_struct(s, 40)?
            .into_iter()
            .map(pending_child_n)
            .collect::<cadence_thrift::Result<Vec<_>>>()?,
        pending_decision: field_struct(s, 50)?
            .map(pending_decision_n)
            .transpose()?,
    })
}

impl TChannelHistoryClient {
    pub(crate) async fn get_mutable_state_native(
        &self,
        request: &n::history::GetMutableStateRequest,
        timeout: Duration,
    ) -> Result<n::history::GetMutableStateResponse, Status> {
        let response = self
            .call(
                "GetMutableState",
                &get_mutable_state_request_native(request),
                timeout,
            )
            .await?;
        get_mutable_state_response_native(&response).map_err(|error| {
            Status::internal(format!("invalid History GetMutableState response: {error}"))
        })
    }

    pub(crate) async fn describe_workflow_execution_native(
        &self,
        request: &n::history::DescribeWorkflowExecutionRequest,
        timeout: Duration,
    ) -> Result<n::history::DescribeWorkflowExecutionResponse, Status> {
        let response = self
            .call(
                "DescribeWorkflowExecution",
                &describe_workflow_execution_request_native(request),
                timeout,
            )
            .await?;
        describe_workflow_execution_response_native(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History DescribeWorkflowExecution response: {error}"
            ))
        })
    }

    pub(crate) async fn refresh_workflow_tasks_native(
        &self,
        request: &n::history::RefreshWorkflowTasksRequest,
        timeout: Duration,
    ) -> Result<(), Status> {
        self.call(
            "RefreshWorkflowTasks",
            &refresh_workflow_tasks_request_native(request),
            timeout,
        )
        .await?;
        Ok(())
    }
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

    #[test]
    fn control_requests_keep_raw_bytes_and_field_ids() {
        let execution = Some(n::api::WorkflowExecution {
            workflow_id: GoString::from(&b"w\xfe"[..]),
            run_id: GoString::from(&b"r\xfd"[..]),
        });
        let state = get_mutable_state_request_native(&n::history::GetMutableStateRequest {
            domain_id: GoString::from(&b"d\xff"[..]),
            workflow_execution: execution.clone(),
            expected_next_event_id: 7,
            current_branch_token: vec![1, 2],
            version_history_item: Some(Default::default()),
        });
        assert_eq!(
            typed_field(&state, 10),
            Some(&TypedValue::Bytes(b"d\xff".to_vec()))
        );
        assert_eq!(typed_field(&state, 30), Some(&TypedValue::I64(7)));
        assert!(typed_field(&state, 40).is_some());
        let Some(TypedValue::Struct(item)) = typed_field(&state, 50) else {
            panic!("item")
        };
        assert_eq!(typed_field(item, 10), Some(&TypedValue::I64(0)));
        assert_eq!(typed_field(item, 20), Some(&TypedValue::I64(0)));

        let refresh =
            refresh_workflow_tasks_request_native(&n::history::RefreshWorkflowTasksRequest {
                domain: GoString::default(),
                domain_id: GoString::from(&b"d\xff"[..]),
                workflow_execution: execution.clone(),
            });
        let Some(TypedValue::Struct(inner)) = typed_field(&refresh, 20) else {
            panic!("inner")
        };
        assert_eq!(
            typed_field(inner, 10),
            Some(&TypedValue::Bytes(Vec::new()))
        );
        let Some(TypedValue::Struct(run)) = typed_field(inner, 20) else {
            panic!("execution")
        };
        assert_eq!(
            typed_field(run, 20),
            Some(&TypedValue::Bytes(b"r\xfd".to_vec()))
        );

        let describe = describe_workflow_execution_request_native(
            &n::history::DescribeWorkflowExecutionRequest {
                domain_id: GoString::from(&b"d\xff"[..]),
                request: Some(n::api::DescribeWorkflowExecutionRequest {
                    domain: GoString::from(&b"n\xfb"[..]),
                    workflow_execution: execution,
                    query_consistency_level: 2,
                }),
            },
        );
        let Some(TypedValue::Struct(inner)) = typed_field(&describe, 20) else {
            panic!("inner")
        };
        assert_eq!(
            typed_field(inner, 10),
            Some(&TypedValue::Bytes(b"n\xfb".to_vec()))
        );
        assert_eq!(typed_field(inner, 30), Some(&TypedValue::I32(1)));
    }

    #[test]
    fn mutable_state_and_describe_responses_keep_raw_bytes() {
        let task_list = struct_value(vec![field(10, TypedValue::Bytes(b"s\xff".to_vec()))]);
        let state = finish(vec![
            field(30, TypedValue::I64(9)),
            field(35, TypedValue::I64(4)),
            field(50, task_list.clone()),
            field(60, task_list),
            field(70, TypedValue::Bytes(b"lib\xfe".to_vec())),
            field(110, TypedValue::I32(5)),
            field(150, TypedValue::I32(1)),
            field(180, TypedValue::Bool(true)),
        ]);
        let state = get_mutable_state_response_native(&state).unwrap();
        assert_eq!(state.previous_started_event_id, Some(4));
        assert_eq!(state.client_library_version.as_bytes(), b"lib\xfe");
        assert_eq!(state.sticky_task_list.unwrap().name.as_bytes(), b"s\xff");
        assert_eq!(
            state.sticky_task_list_schedule_to_start_timeout.unwrap().seconds,
            5
        );
        assert_eq!(state.workflow_state, 2);
        assert!(state.is_sticky_task_list_enabled);

        let info = finish(vec![
            field(120, TypedValue::Bytes(b"tl\xfd".to_vec())),
            field(180, TypedValue::Bytes(b"cron\xfc".to_vec())),
        ]);
        let describe = describe_workflow_execution_response_native(&finish(vec![field(
            20,
            TypedValue::Struct(info),
        )]))
        .unwrap();
        let info = describe.workflow_execution_info.unwrap();
        assert_eq!(info.task_list.as_bytes(), b"tl\xfd");
        assert_eq!(info.task_list_info.unwrap().kind, 1);
        assert_eq!(info.cron_schedule.as_bytes(), b"cron\xfc");
    }
}
