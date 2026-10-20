//! Native outbound YARPC/TChannel Thrift calls used by Matching's History
//! client when Go's `EnableGRPCOutbound` selector chooses TChannel.
//!
//! Operation schemas and wire conversion live beside the History client;
//! retries and operation deadlines remain owned by the History client.

use cadence_thrift::{protocol::Reader, TType};
use std::time::Duration;
use tonic::{Code, Status};

const HISTORY_SERVICE: &str = "cadence-history";

#[derive(Clone, Debug)]
pub(crate) struct TChannelHistoryClient {
    address: std::sync::Arc<str>,
}

impl TChannelHistoryClient {
    pub(crate) fn new(address: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            address: address.into(),
        }
    }

    /// `arg0` is YARPC's fully-qualified Thrift method, `arg1` is binary
    /// metadata, and `arg2` is the raw Thrift argument/result struct. This is
    /// not a thrift RPC envelope: YARPC carries the method in arg0.
    pub(crate) async fn call(
        &self,
        method: &'static str,
        body: &TypedStruct,
        timeout: Duration,
    ) -> Result<TypedStruct, Status> {
        let method = format!("History::{method}");
        let headers = vec![
            ("$rpc$-caller".to_owned(), "internal".to_owned()),
            ("$rpc$-service".to_owned(), HISTORY_SERVICE.to_owned()),
        ];
        let response = crate::tchannel_json_client::call_thrift(
            &self.address,
            HISTORY_SERVICE,
            &method,
            &headers,
            &encode_typed_struct(body),
            timeout,
        )
        .await
        .map_err(|error| match error {
            crate::tchannel_json_client::JsonCallError::Application(message) => {
                Status::unknown(format!("History TChannel application error: {message}"))
            }
            crate::tchannel_json_client::JsonCallError::Transport(error) => {
                if error.to_string().contains("deadline exceeded") {
                    Status::deadline_exceeded(error.to_string())
                } else {
                    Status::unavailable(error.to_string())
                }
            }
        })?;
        if response.application_error {
            return Err(application_error(&response.headers, &response.body));
        }
        decode_typed_struct(&response.body)
            .map_err(|error| Status::internal(format!("invalid History Thrift reply: {error}")))
    }
}

fn application_error(headers: &[(String, String)], body: &[u8]) -> Status {
    let name = headers
        .iter()
        .find(|(key, _)| key == "$rpc$-application-error-name")
        .map(|(_, value)| value.as_str())
        .unwrap_or("UnknownHistoryError");
    let details = headers
        .iter()
        .find(|(key, _)| key == "$rpc$-application-error-details")
        .map(|(_, value)| value.as_str())
        .unwrap_or("History returned a Thrift application error.");
    // Typed Cadence exception translation is performed by the operation
    // adapters after decoding the exception schema. Keep the class and raw
    // payload visible here until that mapping is applied.
    let _ = body;
    Status::new(Code::Unknown, format!("{name}: {details}"))
}

/// Local typed representation used by schema adapters. Unlike the shared
/// `cadence_thrift::Value`, containers retain their declared element/key/value
/// types even when empty (Thrift's binary protocol still writes those bytes).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypedValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    Struct(TypedStruct),
    List(TType, Vec<TypedValue>),
    Set(TType, Vec<TypedValue>),
    Map(TType, TType, Vec<(TypedValue, TypedValue)>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TypedStruct {
    pub fields: Vec<(i16, TypedValue)>,
}

impl TypedValue {
    fn ty(&self) -> TType {
        match self {
            Self::Bool(_) => TType::Bool,
            Self::I8(_) => TType::Byte,
            Self::I16(_) => TType::I16,
            Self::I32(_) => TType::I32,
            Self::I64(_) => TType::I64,
            Self::F64(_) => TType::Double,
            Self::Bytes(_) => TType::String,
            Self::Struct(_) => TType::Struct,
            Self::List(..) => TType::List,
            Self::Set(..) => TType::Set,
            Self::Map(..) => TType::Map,
        }
    }
}

fn field(id: i16, value: TypedValue) -> (i16, TypedValue) {
    (id, value)
}
fn string(value: &str) -> TypedValue {
    TypedValue::Bytes(value.as_bytes().to_vec())
}
fn binary(value: &[u8]) -> TypedValue {
    TypedValue::Bytes(value.to_vec())
}
fn i64_value(value: i64) -> TypedValue {
    TypedValue::I64(value)
}
fn struct_value(fields: Vec<(i16, TypedValue)>) -> TypedValue {
    TypedValue::Struct(TypedStruct { fields })
}
fn finish(mut fields: Vec<(i16, TypedValue)>) -> TypedStruct {
    fields.sort_by_key(|(id, _)| *id);
    TypedStruct { fields }
}

fn workflow_execution(value: &cadence_proto::api::WorkflowExecution) -> TypedValue {
    struct_value(vec![
        field(10, string(&value.workflow_id)),
        field(20, string(&value.run_id)),
    ])
}

fn version_history_item(value: &cadence_proto::admin::VersionHistoryItem) -> TypedValue {
    // Go's FromVersionHistoryItem always sets both optional Thrift pointers,
    // including when a protobuf scalar has its zero value.
    struct_value(vec![
        field(10, i64_value(value.event_id)),
        field(20, i64_value(value.version)),
    ])
}

fn version_history_item_list(values: &[cadence_proto::admin::VersionHistoryItem]) -> TypedValue {
    TypedValue::List(
        TType::Struct,
        values.iter().map(version_history_item).collect(),
    )
}

fn version_history(value: &cadence_proto::admin::VersionHistory) -> TypedValue {
    let mut fields = Vec::new();
    if !value.branch_token.is_empty() {
        fields.push(field(10, binary(&value.branch_token)));
    }
    if !value.items.is_empty() {
        fields.push(field(20, version_history_item_list(&value.items)));
    }
    struct_value(fields)
}

fn version_histories(value: &cadence_proto::shared::VersionHistories) -> TypedValue {
    let mut fields = vec![field(
        10,
        TypedValue::I32(value.current_version_history_index),
    )];
    if !value.histories.is_empty() {
        fields.push(field(
            20,
            TypedValue::List(
                TType::Struct,
                value.histories.iter().map(version_history).collect(),
            ),
        ));
    }
    TypedValue::Struct(finish(fields))
}

fn workflow_state_to_thrift(proto: i32) -> Option<i32> {
    // proto reserves INVALID=0; Thrift uses persistence's zero-based enum.
    match proto {
        1..=6 => Some(proto - 1),
        _ => None,
    }
}
fn workflow_state_from_thrift(value: i32) -> i32 {
    match value {
        0..=5 => value + 1,
        _ => 0,
    }
}
fn close_state_to_thrift(proto: i32) -> Option<i32> {
    // proto reserves INVALID=0; Thrift starts at COMPLETED=0.
    match proto {
        1..=6 => Some(proto - 1),
        _ => None,
    }
}
fn close_state_from_thrift(value: i32) -> i32 {
    match value {
        0..=5 => value + 1,
        _ => 0,
    }
}

fn typed_field(s: &TypedStruct, id: i16) -> Option<&TypedValue> {
    s.fields
        .iter()
        .find(|(field_id, _)| *field_id == id)
        .map(|(_, value)| value)
}
fn thrift_invalid(message: impl Into<String>) -> cadence_thrift::Error {
    cadence_thrift::Error::Invalid(message.into())
}
fn as_i32(value: &TypedValue, id: i16) -> cadence_thrift::Result<i32> {
    match value {
        TypedValue::I32(value) => Ok(*value),
        TypedValue::I16(value) => Ok(i32::from(*value)),
        TypedValue::I8(value) => Ok(i32::from(*value)),
        _ => Err(thrift_invalid(format!(
            "field {id}: expected i32 enum/integer"
        ))),
    }
}
fn as_i64(value: &TypedValue, id: i16) -> cadence_thrift::Result<i64> {
    match value {
        TypedValue::I64(value) => Ok(*value),
        TypedValue::I32(value) => Ok(i64::from(*value)),
        TypedValue::I16(value) => Ok(i64::from(*value)),
        _ => Err(thrift_invalid(format!("field {id}: expected i64"))),
    }
}
fn field_i32(s: &TypedStruct, id: i16) -> cadence_thrift::Result<i32> {
    typed_field(s, id).map_or(Ok(0), |v| as_i32(v, id))
}
fn field_i64(s: &TypedStruct, id: i16) -> cadence_thrift::Result<i64> {
    typed_field(s, id).map_or(Ok(0), |v| as_i64(v, id))
}
fn field_bytes(s: &TypedStruct, id: i16) -> cadence_thrift::Result<Vec<u8>> {
    match typed_field(s, id) {
        None => Ok(Vec::new()),
        Some(TypedValue::Bytes(value)) => Ok(value.clone()),
        Some(_) => Err(thrift_invalid(format!(
            "field {id}: expected binary/string"
        ))),
    }
}
fn field_string(s: &TypedStruct, id: i16) -> cadence_thrift::Result<String> {
    String::from_utf8(field_bytes(s, id)?)
        .map_err(|error| thrift_invalid(format!("field {id}: {error}")))
}
fn field_bool(s: &TypedStruct, id: i16) -> cadence_thrift::Result<bool> {
    match typed_field(s, id) {
        None => Ok(false),
        Some(TypedValue::Bool(value)) => Ok(*value),
        Some(_) => Err(thrift_invalid(format!("field {id}: expected bool"))),
    }
}
fn field_struct(s: &TypedStruct, id: i16) -> cadence_thrift::Result<Option<&TypedStruct>> {
    match typed_field(s, id) {
        None => Ok(None),
        Some(TypedValue::Struct(value)) => Ok(Some(value)),
        Some(_) => Err(thrift_invalid(format!("field {id}: expected struct"))),
    }
}
fn workflow_execution_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::WorkflowExecution> {
    Ok(cadence_proto::api::WorkflowExecution {
        workflow_id: field_string(s, 10)?,
        run_id: field_string(s, 20)?,
    })
}
fn version_history_item_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::admin::VersionHistoryItem> {
    Ok(cadence_proto::admin::VersionHistoryItem {
        event_id: field_i64(s, 10)?,
        version: field_i64(s, 20)?,
    })
}
pub(crate) fn get_mutable_state_request_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::GetMutableStateRequest> {
    let workflow_execution = field_struct(s, 20)?
        .map(workflow_execution_from_thrift)
        .transpose()?;
    let version_history_item = field_struct(s, 50)?
        .map(version_history_item_from_thrift)
        .transpose()?;
    Ok(cadence_proto::history::GetMutableStateRequest {
        domain_id: field_string(s, 10)?,
        workflow_execution,
        expected_next_event_id: field_i64(s, 30)?,
        current_branch_token: field_bytes(s, 40)?,
        version_history_item,
    })
}
fn version_history_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::admin::VersionHistory> {
    let items = match typed_field(s, 20) {
        None => Vec::new(),
        Some(TypedValue::List(TType::Struct, values)) => values
            .iter()
            .map(|v| match v {
                TypedValue::Struct(item) => version_history_item_from_thrift(item),
                _ => Err(thrift_invalid("VersionHistory.items contains non-struct")),
            })
            .collect::<cadence_thrift::Result<Vec<_>>>()?,
        Some(_) => {
            return Err(thrift_invalid(
                "VersionHistory.items: expected list<struct>",
            ))
        }
    };
    Ok(cadence_proto::admin::VersionHistory {
        branch_token: field_bytes(s, 10)?,
        items,
    })
}
fn version_histories_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::shared::VersionHistories> {
    let histories = match typed_field(s, 20) {
        None => Vec::new(),
        Some(TypedValue::List(TType::Struct, values)) => values
            .iter()
            .map(|v| match v {
                TypedValue::Struct(history) => version_history_from_thrift(history),
                _ => Err(thrift_invalid(
                    "VersionHistories.histories contains non-struct",
                )),
            })
            .collect::<cadence_thrift::Result<Vec<_>>>()?,
        Some(_) => {
            return Err(thrift_invalid(
                "VersionHistories.histories: expected list<struct>",
            ))
        }
    };
    Ok(cadence_proto::shared::VersionHistories {
        current_version_history_index: field_i32(s, 10)?,
        histories,
    })
}
fn task_list_from_thrift(s: &TypedStruct) -> cadence_thrift::Result<cadence_proto::api::TaskList> {
    let kind = typed_field(s, 20)
        .map(|v| as_i32(v, 20))
        .transpose()?
        .map_or(0, |v| if (0..=2).contains(&v) { v + 1 } else { 0 });
    Ok(cadence_proto::api::TaskList {
        name: field_string(s, 10)?,
        kind,
        base_name: field_string(s, 30)?,
    })
}
fn workflow_type_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::WorkflowType> {
    Ok(cadence_proto::api::WorkflowType {
        name: field_string(s, 10)?,
    })
}

/// Reverse the pinned Go `ToHistoryGetMutableStateResponse` and proto mapper
/// sequence, preserving omitted optional fields and enum offsets.
pub(crate) fn get_mutable_state_response_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::GetMutableStateResponse> {
    let timeout = typed_field(s, 110)
        .map(|v| as_i32(v, 110))
        .transpose()?
        .map(|seconds| prost_types::Duration {
            seconds: i64::from(seconds),
            nanos: 0,
        });
    let workflow_execution = field_struct(s, 10)?
        .map(workflow_execution_from_thrift)
        .transpose()?;
    let workflow_type = field_struct(s, 20)?
        .map(workflow_type_from_thrift)
        .transpose()?;
    let task_list = field_struct(s, 50)?
        .map(task_list_from_thrift)
        .transpose()?;
    let sticky_task_list = field_struct(s, 60)?
        .map(task_list_from_thrift)
        .transpose()?;
    let version_histories = field_struct(s, 170)?
        .map(version_histories_from_thrift)
        .transpose()?;
    let previous_started_event_id = typed_field(s, 35)
        .map(|v| as_i64(v, 35))
        .transpose()?
        .map(|value| prost_types::Int64Value { value });
    Ok(cadence_proto::history::GetMutableStateResponse {
        workflow_execution,
        workflow_type,
        next_event_id: field_i64(s, 30)?,
        previous_started_event_id,
        last_first_event_id: field_i64(s, 40)?,
        task_list,
        sticky_task_list,
        client_library_version: field_string(s, 70)?,
        client_feature_version: field_string(s, 80)?,
        client_impl: field_string(s, 90)?,
        sticky_task_list_schedule_to_start_timeout: timeout,
        event_store_version: field_i32(s, 120)?,
        current_branch_token: field_bytes(s, 130)?,
        workflow_state: workflow_state_from_thrift(field_i32(s, 150)?),
        workflow_close_state: close_state_from_thrift(field_i32(s, 160)?),
        version_histories,
        is_sticky_task_list_enabled: field_bool(s, 180)?,
        history_size: field_i64(s, 190)?,
    })
}

/// Convert the exact internal History protobuf request using the Go
/// `FromHistoryGetMutableStateRequest` mapper and pinned Thrift IDs.
pub(crate) fn get_mutable_state_request(
    value: &cadence_proto::history::GetMutableStateRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, string(&value.domain_id)),
        field(30, i64_value(value.expected_next_event_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution(execution)));
    }
    if !value.current_branch_token.is_empty() {
        fields.push(field(40, binary(&value.current_branch_token)));
    }
    if let Some(item) = &value.version_history_item {
        fields.push(field(50, version_history_item(item)));
    }
    finish(fields)
}

fn workflow_type(value: &cadence_proto::api::WorkflowType) -> TypedValue {
    struct_value(vec![field(10, string(&value.name))])
}

/// Convert the two-level RefreshWorkflowTasks wrapper. The outer IDL misspells
/// domainUUID as `domainUIID`; the inner request is shared.thrift's request.
pub(crate) fn refresh_workflow_tasks_request(
    value: &cadence_proto::history::RefreshWorkflowTasksRequest,
) -> TypedStruct {
    let mut inner = vec![field(10, string(&value.domain))];
    if let Some(execution) = &value.workflow_execution {
        inner.push(field(20, workflow_execution(execution)));
    }
    finish(vec![
        field(10, string(&value.domain_id)),
        field(20, TypedValue::Struct(finish(inner))),
    ])
}

pub(crate) fn refresh_workflow_tasks_request_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::RefreshWorkflowTasksRequest> {
    let inner = field_struct(value, 20)?;
    let domain = inner
        .map(|request| field_string(request, 10))
        .transpose()?
        .unwrap_or_default();
    let workflow_execution = inner
        .map(|request| field_struct(request, 20))
        .transpose()?
        .flatten()
        .map(workflow_execution_from_thrift)
        .transpose()?;
    Ok(cadence_proto::history::RefreshWorkflowTasksRequest {
        domain,
        domain_id: field_string(value, 10)?,
        workflow_execution,
    })
}

pub(crate) fn describe_workflow_execution_request(
    value: &cadence_proto::history::DescribeWorkflowExecutionRequest,
) -> TypedStruct {
    let request = value.request.as_ref();
    let mut fields = vec![field(10, string(&value.domain_id))];
    let Some(request) = request else {
        return finish(fields);
    };
    let mut nested = vec![field(10, string(&request.domain))];
    if let Some(execution) = request.workflow_execution.as_ref() {
        nested.push(field(20, workflow_execution(execution)));
    }
    let level = request.query_consistency_level;
    if (1..=2).contains(&level) {
        // Thrift QueryConsistencyLevel is zero based; proto reserves INVALID.
        nested.push(field(30, TypedValue::I32(level - 1)));
    }
    fields.push(field(20, TypedValue::Struct(finish(nested))));
    finish(fields)
}

pub(crate) fn describe_workflow_execution_request_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::DescribeWorkflowExecutionRequest> {
    let inner = field_struct(value, 20)?;
    let request = inner
        .map(|s| {
            let workflow_execution = field_struct(s, 20)?
                .map(workflow_execution_from_thrift)
                .transpose()?;
            let query_consistency_level = typed_field(s, 30)
                .map(|v| as_i32(v, 30))
                .transpose()?
                .map_or(0, |v| if (0..=1).contains(&v) { v + 1 } else { 0 });
            Ok::<_, cadence_thrift::Error>(cadence_proto::api::DescribeWorkflowExecutionRequest {
                domain: field_string(s, 10)?,
                workflow_execution,
                query_consistency_level,
            })
        })
        .transpose()?;
    Ok(cadence_proto::history::DescribeWorkflowExecutionRequest {
        domain_id: field_string(value, 10)?,
        request,
    })
}

fn timestamp_nanos(value: &prost_types::Timestamp) -> i64 {
    (i128::from(value.seconds) * 1_000_000_000 + i128::from(value.nanos)) as i64
}
fn timestamp_field(id: i16, value: Option<&prost_types::Timestamp>) -> Option<(i16, TypedValue)> {
    value.map(|value| field(id, i64_value(timestamp_nanos(value))))
}
fn enum_field(id: i16, value: i32, proto_max: i32) -> Option<(i16, TypedValue)> {
    (1..=proto_max)
        .contains(&value)
        .then(|| field(id, TypedValue::I32(value - 1)))
}
fn string_map(value: &std::collections::HashMap<String, String>) -> TypedValue {
    TypedValue::Map(
        TType::String,
        TType::String,
        value.iter().map(|(k, v)| (string(k), string(v))).collect(),
    )
}
fn payload_map(
    value: &std::collections::HashMap<String, cadence_proto::api::Payload>,
) -> TypedValue {
    TypedValue::Map(
        TType::String,
        TType::String,
        value
            .iter()
            .map(|(k, v)| (string(k), binary(&v.data)))
            .collect(),
    )
}
fn optional_payload_map_field(
    id: i16,
    value: Option<&std::collections::HashMap<String, cadence_proto::api::Payload>>,
) -> Option<(i16, TypedValue)> {
    value.map(|map| {
        let fields = if map.is_empty() {
            Vec::new()
        } else {
            vec![field(10, payload_map(map))]
        };
        field(id, struct_value(fields))
    })
}
fn reset_points(value: &cadence_proto::api::ResetPoints) -> TypedValue {
    let points = value
        .points
        .iter()
        .map(|point| {
            let mut fields = vec![
                field(10, string(&point.binary_checksum)),
                field(20, string(&point.run_id)),
                field(30, i64_value(point.first_decision_completed_id)),
                field(60, TypedValue::Bool(point.resettable)),
            ];
            if let Some(created) = timestamp_field(40, point.created_time.as_ref()) {
                fields.push(created);
            }
            if let Some(expiring) = timestamp_field(50, point.expiring_time.as_ref()) {
                fields.push(expiring);
            }
            TypedValue::Struct(finish(fields))
        })
        .collect();
    let fields = if points.is_empty() {
        Vec::new()
    } else {
        vec![field(10, TypedValue::List(TType::Struct, points))]
    };
    struct_value(fields)
}
fn active_cluster_selection_policy(
    value: &cadence_proto::api::ActiveClusterSelectionPolicy,
) -> TypedValue {
    let mut fields = Vec::new();
    if let Some(attribute) = &value.cluster_attribute {
        fields.push(field(
            1,
            struct_value(vec![
                field(1, string(&attribute.scope)),
                field(2, string(&attribute.name)),
            ]),
        ));
    }
    struct_value(fields)
}
fn workflow_execution_info(value: &cadence_proto::api::WorkflowExecutionInfo) -> TypedValue {
    let mut fields = Vec::new();
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(10, workflow_execution(execution)));
    }
    if let Some(kind) = &value.r#type {
        fields.push(field(20, workflow_type(kind)));
    }
    fields.extend(timestamp_field(30, value.start_time.as_ref()));
    fields.extend(timestamp_field(40, value.close_time.as_ref()));
    fields.extend(enum_field(50, value.close_status, 6));
    fields.push(field(60, i64_value(value.history_length)));
    if let Some(parent) = &value.parent_execution_info {
        fields.push(field(70, string(&parent.domain_id)));
        fields.push(field(71, string(&parent.domain_name)));
        fields.push(field(72, i64_value(parent.initiated_id)));
        if let Some(execution) = &parent.workflow_execution {
            fields.push(field(80, workflow_execution(execution)));
        }
    }
    fields.extend(timestamp_field(90, value.execution_time.as_ref()));
    fields.extend(optional_payload_map_field(
        100,
        value.memo.as_ref().map(|m| &m.fields),
    ));
    fields.extend(optional_payload_map_field(
        101,
        value.search_attributes.as_ref().map(|m| &m.indexed_fields),
    ));
    if let Some(reset) = &value.auto_reset_points {
        fields.push(field(110, reset_points(reset)));
    }
    // Go's MigrateTaskList prefers TaskListInfo, otherwise synthesizes a
    // NORMAL TaskList from the legacy `task_list` string when it is nonempty.
    let task_list_info = value.task_list_info.as_ref().cloned().or_else(|| {
        (!value.task_list.is_empty()).then(|| cadence_proto::api::TaskList {
            name: value.task_list.clone(),
            kind: 1,
            ..Default::default()
        })
    });
    let legacy_task_list = task_list_info
        .as_ref()
        .map(|task| task.name.as_str())
        .unwrap_or(&value.task_list);
    fields.push(field(120, string(legacy_task_list)));
    if let Some(task) = &task_list_info {
        fields.push(field(121, task_list(task)));
    }
    fields.push(field(130, TypedValue::Bool(value.is_cron)));
    fields.extend(timestamp_field(140, value.update_time.as_ref()));
    if !value.partition_config.is_empty() {
        fields.push(field(150, string_map(&value.partition_config)));
    }
    fields.extend(enum_field(160, value.cron_overlap_policy, 2));
    if let Some(policy) = &value.active_cluster_selection_policy {
        fields.push(field(170, active_cluster_selection_policy(policy)));
    }
    if !value.cron_schedule.is_empty() {
        fields.push(field(180, string(&value.cron_schedule)));
    }
    fields.extend(enum_field(190, value.execution_status, 8));
    fields.extend(timestamp_field(
        200,
        value.scheduled_execution_time.as_ref(),
    ));
    TypedValue::Struct(finish(fields))
}
fn workflow_execution_configuration(
    value: &cadence_proto::api::WorkflowExecutionConfiguration,
) -> TypedValue {
    let mut fields = Vec::new();
    if let Some(task) = &value.task_list {
        fields.push(field(10, task_list(task)));
    }
    for (id, timeout) in [
        (20, value.execution_start_to_close_timeout.as_ref()),
        (30, value.task_start_to_close_timeout.as_ref()),
    ] {
        if let Some(timeout) = timeout {
            let nanos = i128::from(timeout.seconds) * 1_000_000_000 + i128::from(timeout.nanos);
            fields.push(field(id, TypedValue::I32((nanos / 1_000_000_000) as i32)));
        }
    }
    TypedValue::Struct(finish(fields))
}

fn activity_type(value: &cadence_proto::api::ActivityType) -> TypedValue {
    struct_value(vec![field(10, string(&value.name))])
}
fn failure_options(value: &cadence_proto::api::FailureOptions) -> TypedValue {
    let mut fields = Vec::new();
    fields.extend(enum_field(10, value.failure_category, 3));
    if let Some(duration) = &value.next_retry_interval {
        let nanos = i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos);
        fields.push(field(20, TypedValue::I32((nanos / 1_000_000_000) as i32)));
    }
    struct_value(finish(fields).fields)
}
fn pending_activity(value: &cadence_proto::api::PendingActivityInfo) -> TypedValue {
    let mut fields = vec![
        field(10, string(&value.activity_id)),
        field(70, TypedValue::I32(value.attempt)),
        field(80, TypedValue::I32(value.maximum_attempts)),
        field(120, string(&value.last_worker_identity)),
        field(140, string(&value.started_worker_identity)),
        field(150, i64_value(value.schedule_id)),
    ];
    if let Some(kind) = &value.activity_type {
        fields.push(field(20, activity_type(kind)));
    }
    fields.extend(enum_field(30, value.state, 3));
    if let Some(details) = &value.heartbeat_details {
        if !details.data.is_empty() {
            fields.push(field(40, binary(&details.data)));
        }
    }
    fields.extend(timestamp_field(50, value.last_heartbeat_time.as_ref()));
    fields.extend(timestamp_field(60, value.last_started_time.as_ref()));
    fields.extend(timestamp_field(90, value.scheduled_time.as_ref()));
    fields.extend(timestamp_field(100, value.expiration_time.as_ref()));
    if let Some(failure) = &value.last_failure {
        // ToFailureReason returns a pointer whenever the Failure is present,
        // even when Reason is empty.
        fields.push(field(110, string(&failure.reason)));
        if !failure.details.is_empty() {
            fields.push(field(130, binary(&failure.details)));
        }
        if let Some(options) = &failure.options {
            fields.push(field(135, failure_options(options)));
        }
    }
    TypedValue::Struct(finish(fields))
}
fn pending_child(value: &cadence_proto::api::PendingChildExecutionInfo) -> TypedValue {
    let mut fields = vec![
        field(1, string(&value.domain)),
        field(30, string(&value.workflow_type_name)),
        field(40, i64_value(value.initiated_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(10, string(&execution.workflow_id)));
        fields.push(field(20, string(&execution.run_id)));
    } else {
        // Go ToPendingChildExecutionInfo fills scalar IDs with their zero values
        // and FromPendingChildExecutionInfo takes pointers to each scalar.
        fields.push(field(10, string("")));
        fields.push(field(20, string("")));
    }
    fields.extend(enum_field(50, value.parent_close_policy, 3));
    TypedValue::Struct(finish(fields))
}
fn pending_decision(value: &cadence_proto::api::PendingDecisionInfo) -> TypedValue {
    let mut fields = vec![
        field(40, i64_value(i64::from(value.attempt))),
        field(60, i64_value(value.schedule_id)),
    ];
    fields.extend(enum_field(10, value.state, 2));
    fields.extend(timestamp_field(20, value.scheduled_time.as_ref()));
    fields.extend(timestamp_field(30, value.started_time.as_ref()));
    fields.extend(timestamp_field(50, value.original_scheduled_time.as_ref()));
    TypedValue::Struct(finish(fields))
}

pub(crate) fn describe_workflow_execution_response(
    value: &cadence_proto::api::DescribeWorkflowExecutionResponse,
) -> TypedStruct {
    let mut fields = Vec::new();
    if let Some(configuration) = &value.execution_configuration {
        fields.push(field(10, workflow_execution_configuration(configuration)));
    }
    if let Some(info) = &value.workflow_execution_info {
        fields.push(field(20, workflow_execution_info(info)));
    }
    if !value.pending_activities.is_empty() {
        fields.push(field(
            30,
            TypedValue::List(
                TType::Struct,
                value
                    .pending_activities
                    .iter()
                    .map(pending_activity)
                    .collect(),
            ),
        ));
    }
    if !value.pending_children.is_empty() {
        fields.push(field(
            40,
            TypedValue::List(
                TType::Struct,
                value.pending_children.iter().map(pending_child).collect(),
            ),
        ));
    }
    if let Some(decision) = &value.pending_decision {
        fields.push(field(50, pending_decision(decision)));
    }
    finish(fields)
}

fn enum_from_thrift(value: Option<i32>, thrift_max: i32) -> i32 {
    value
        .filter(|value| (0..thrift_max).contains(value))
        .map_or(0, |value| value + 1)
}
fn timestamp_from_nanos(value: Option<i64>) -> Option<prost_types::Timestamp> {
    value.map(|nanos| prost_types::Timestamp {
        seconds: nanos.div_euclid(1_000_000_000),
        nanos: nanos.rem_euclid(1_000_000_000) as i32,
    })
}
fn enum_field_value(s: &TypedStruct, id: i16, thrift_max: i32) -> cadence_thrift::Result<i32> {
    typed_field(s, id)
        .map(|v| as_i32(v, id))
        .transpose()
        .map(|v| enum_from_thrift(v, thrift_max))
}
fn field_list_struct(s: &TypedStruct, id: i16) -> cadence_thrift::Result<Vec<&TypedStruct>> {
    match typed_field(s, id) {
        None => Ok(Vec::new()),
        Some(TypedValue::List(TType::Struct, values)) => values
            .iter()
            .map(|value| match value {
                TypedValue::Struct(value) => Ok(value),
                _ => Err(thrift_invalid(format!(
                    "field {id}: list has non-struct element"
                ))),
            })
            .collect(),
        Some(_) => Err(thrift_invalid(format!("field {id}: expected list<struct>"))),
    }
}
fn typed_map_entries(
    s: &TypedStruct,
    id: i16,
) -> cadence_thrift::Result<Vec<(&TypedValue, &TypedValue)>> {
    match typed_field(s, id) {
        None => Ok(Vec::new()),
        Some(TypedValue::Map(TType::String, TType::String, values)) => {
            Ok(values.iter().map(|(k, v)| (k, v)).collect())
        }
        Some(_) => Err(thrift_invalid(format!(
            "field {id}: expected map<string,string/binary>"
        ))),
    }
}
fn string_value(value: &TypedValue, id: i16) -> cadence_thrift::Result<String> {
    match value {
        TypedValue::Bytes(value) => String::from_utf8(value.clone())
            .map_err(|error| thrift_invalid(format!("field {id}: {error}"))),
        _ => Err(thrift_invalid(format!("field {id}: expected string"))),
    }
}
fn bytes_value(value: &TypedValue, id: i16) -> cadence_thrift::Result<Vec<u8>> {
    match value {
        TypedValue::Bytes(value) => Ok(value.clone()),
        _ => Err(thrift_invalid(format!("field {id}: expected binary"))),
    }
}
fn payload_map_from_thrift(
    s: &TypedStruct,
    id: i16,
) -> cadence_thrift::Result<std::collections::HashMap<String, cadence_proto::api::Payload>> {
    let fields = field_struct(s, id)?;
    let mut result = std::collections::HashMap::new();
    if let Some(fields) = fields {
        for (key, value) in typed_map_entries(fields, 10)? {
            result.insert(
                string_value(key, id)?,
                cadence_proto::api::Payload {
                    data: bytes_value(value, id)?,
                },
            );
        }
    }
    Ok(result)
}
fn string_map_from_thrift(
    s: &TypedStruct,
    id: i16,
) -> cadence_thrift::Result<std::collections::HashMap<String, String>> {
    let mut result = std::collections::HashMap::new();
    for (key, value) in typed_map_entries(s, id)? {
        result.insert(string_value(key, id)?, string_value(value, id)?);
    }
    Ok(result)
}
fn reset_points_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::ResetPoints> {
    let points = field_list_struct(s, 10)?
        .into_iter()
        .map(|point| {
            Ok(cadence_proto::api::ResetPointInfo {
                binary_checksum: field_string(point, 10)?,
                run_id: field_string(point, 20)?,
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
    Ok(cadence_proto::api::ResetPoints { points })
}
fn active_cluster_selection_policy_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::ActiveClusterSelectionPolicy> {
    let cluster_attribute = field_struct(s, 1)?
        .map(|attribute| {
            Ok(cadence_proto::api::ClusterAttribute {
                scope: field_string(attribute, 1)?,
                name: field_string(attribute, 2)?,
            })
        })
        .transpose()?;
    Ok(cadence_proto::api::ActiveClusterSelectionPolicy { cluster_attribute })
}
fn workflow_execution_info_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::WorkflowExecutionInfo> {
    let workflow_execution = field_struct(s, 10)?
        .map(workflow_execution_from_thrift)
        .transpose()?;
    let workflow_type = field_struct(s, 20)?
        .map(workflow_type_from_thrift)
        .transpose()?;
    let parent_execution = field_struct(s, 80)?
        .map(workflow_execution_from_thrift)
        .transpose()?;
    let parent_execution_info = if [70, 71, 72].iter().any(|id| typed_field(s, *id).is_some())
        || parent_execution.is_some()
    {
        Some(cadence_proto::api::ParentExecutionInfo {
            domain_id: field_string(s, 70)?,
            domain_name: field_string(s, 71)?,
            workflow_execution: parent_execution,
            initiated_id: field_i64(s, 72)?,
        })
    } else {
        None
    };
    let mut task_list_info = field_struct(s, 121)?
        .map(task_list_from_thrift)
        .transpose()?;
    let task_list = typed_field(s, 120)
        .map(|value| string_value(value, 120))
        .transpose()?
        .unwrap_or_else(|| {
            task_list_info
                .as_ref()
                .map_or_else(String::new, |task| task.name.clone())
        });
    // If only the legacy name is present, Go's MigrateTaskList synthesizes a
    // NORMAL TaskList, which the proto mapper returns in task_list_info.
    if task_list_info.is_none() && !task_list.is_empty() {
        task_list_info = Some(cadence_proto::api::TaskList {
            name: task_list.clone(),
            kind: 1,
            ..Default::default()
        });
    }
    let auto_reset_points = field_struct(s, 110)?
        .map(reset_points_from_thrift)
        .transpose()?;
    let active_cluster_selection_policy = field_struct(s, 170)?
        .map(active_cluster_selection_policy_from_thrift)
        .transpose()?;
    let start_time = timestamp_from_nanos(typed_field(s, 30).map(|v| as_i64(v, 30)).transpose()?);
    let close_time = timestamp_from_nanos(typed_field(s, 40).map(|v| as_i64(v, 40)).transpose()?);
    let execution_time =
        timestamp_from_nanos(typed_field(s, 90).map(|v| as_i64(v, 90)).transpose()?);
    let update_time =
        timestamp_from_nanos(typed_field(s, 140).map(|v| as_i64(v, 140)).transpose()?);
    let scheduled_execution_time =
        timestamp_from_nanos(typed_field(s, 200).map(|v| as_i64(v, 200)).transpose()?);
    let memo = field_struct(s, 100)?
        .map(|_| payload_map_from_thrift(s, 100).map(|fields| cadence_proto::api::Memo { fields }))
        .transpose()?;
    let search_attributes = field_struct(s, 101)?
        .map(|_| {
            payload_map_from_thrift(s, 101)
                .map(|indexed_fields| cadence_proto::api::SearchAttributes { indexed_fields })
        })
        .transpose()?;
    Ok(cadence_proto::api::WorkflowExecutionInfo {
        workflow_execution,
        r#type: workflow_type,
        start_time,
        close_time,
        close_status: enum_field_value(s, 50, 6)?,
        history_length: field_i64(s, 60)?,
        parent_execution_info,
        execution_time,
        memo,
        search_attributes,
        auto_reset_points,
        task_list,
        task_list_info,
        is_cron: field_bool(s, 130)?,
        update_time,
        partition_config: string_map_from_thrift(s, 150)?,
        cron_overlap_policy: enum_field_value(s, 160, 2)?,
        active_cluster_selection_policy,
        cron_schedule: field_string(s, 180)?,
        execution_status: enum_field_value(s, 190, 8)?,
        scheduled_execution_time,
    })
}

fn workflow_execution_configuration_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::WorkflowExecutionConfiguration> {
    let task_list = field_struct(s, 10)?
        .map(task_list_from_thrift)
        .transpose()?;
    let duration = |id| -> cadence_thrift::Result<Option<prost_types::Duration>> {
        typed_field(s, id)
            .map(|value| as_i32(value, id))
            .transpose()
            .map(|seconds| {
                seconds.map(|seconds| prost_types::Duration {
                    seconds: i64::from(seconds),
                    nanos: 0,
                })
            })
    };
    Ok(cadence_proto::api::WorkflowExecutionConfiguration {
        task_list,
        execution_start_to_close_timeout: duration(20)?,
        task_start_to_close_timeout: duration(30)?,
    })
}
fn failure_options_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::FailureOptions> {
    let category = enum_field_value(s, 10, 3)?;
    let next_retry_interval = typed_field(s, 20)
        .map(|value| as_i32(value, 20))
        .transpose()?
        .map(|seconds| prost_types::Duration {
            seconds: i64::from(seconds),
            nanos: 0,
        });
    Ok(cadence_proto::api::FailureOptions {
        failure_category: category,
        next_retry_interval,
    })
}
fn pending_activity_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::PendingActivityInfo> {
    let activity_type = field_struct(s, 20)?
        .map(|value| {
            Ok(cadence_proto::api::ActivityType {
                name: field_string(value, 10)?,
            })
        })
        .transpose()?;
    let last_failure_reason = typed_field(s, 110)
        .map(|value| string_value(value, 110))
        .transpose()?;
    let last_failure_details = field_bytes(s, 130)?;
    let last_failure_options = field_struct(s, 135)?
        .map(failure_options_from_thrift)
        .transpose()?;
    let last_failure = if last_failure_reason.is_some()
        || !last_failure_details.is_empty()
        || last_failure_options.is_some()
    {
        Some(cadence_proto::api::Failure {
            reason: last_failure_reason.unwrap_or_default(),
            details: last_failure_details,
            options: last_failure_options,
        })
    } else {
        None
    };
    let heartbeat_details = field_bytes(s, 40)?;
    Ok(cadence_proto::api::PendingActivityInfo {
        activity_id: field_string(s, 10)?,
        activity_type,
        state: enum_field_value(s, 30, 3)?,
        heartbeat_details: (!heartbeat_details.is_empty()).then_some(cadence_proto::api::Payload {
            data: heartbeat_details,
        }),
        last_heartbeat_time: timestamp_from_nanos(
            typed_field(s, 50).map(|v| as_i64(v, 50)).transpose()?,
        ),
        last_started_time: timestamp_from_nanos(
            typed_field(s, 60).map(|v| as_i64(v, 60)).transpose()?,
        ),
        attempt: field_i32(s, 70)?,
        maximum_attempts: field_i32(s, 80)?,
        scheduled_time: timestamp_from_nanos(
            typed_field(s, 90).map(|v| as_i64(v, 90)).transpose()?,
        ),
        expiration_time: timestamp_from_nanos(
            typed_field(s, 100).map(|v| as_i64(v, 100)).transpose()?,
        ),
        last_failure,
        last_worker_identity: field_string(s, 120)?,
        started_worker_identity: field_string(s, 140)?,
        schedule_id: field_i64(s, 150)?,
    })
}
fn pending_child_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::PendingChildExecutionInfo> {
    Ok(cadence_proto::api::PendingChildExecutionInfo {
        workflow_execution: Some(cadence_proto::api::WorkflowExecution {
            workflow_id: field_string(s, 10)?,
            run_id: field_string(s, 20)?,
        }),
        workflow_type_name: field_string(s, 30)?,
        initiated_id: field_i64(s, 40)?,
        parent_close_policy: enum_field_value(s, 50, 3)?,
        domain: field_string(s, 1)?,
    })
}
fn pending_decision_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::PendingDecisionInfo> {
    Ok(cadence_proto::api::PendingDecisionInfo {
        state: enum_field_value(s, 10, 2)?,
        scheduled_time: timestamp_from_nanos(
            typed_field(s, 20).map(|v| as_i64(v, 20)).transpose()?,
        ),
        started_time: timestamp_from_nanos(typed_field(s, 30).map(|v| as_i64(v, 30)).transpose()?),
        attempt: field_i64(s, 40)? as i32,
        original_scheduled_time: timestamp_from_nanos(
            typed_field(s, 50).map(|v| as_i64(v, 50)).transpose()?,
        ),
        schedule_id: field_i64(s, 60)?,
    })
}

pub(crate) fn describe_workflow_execution_response_from_thrift(
    s: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::DescribeWorkflowExecutionResponse> {
    let execution_configuration = field_struct(s, 10)?
        .map(workflow_execution_configuration_from_thrift)
        .transpose()?;
    let workflow_execution_info = field_struct(s, 20)?
        .map(workflow_execution_info_from_thrift)
        .transpose()?;
    let pending_activities = field_list_struct(s, 30)?
        .into_iter()
        .map(pending_activity_from_thrift)
        .collect::<cadence_thrift::Result<Vec<_>>>()?;
    let pending_children = field_list_struct(s, 40)?
        .into_iter()
        .map(pending_child_from_thrift)
        .collect::<cadence_thrift::Result<Vec<_>>>()?;
    let pending_decision = field_struct(s, 50)?
        .map(pending_decision_from_thrift)
        .transpose()?;
    Ok(cadence_proto::history::DescribeWorkflowExecutionResponse {
        execution_configuration,
        workflow_execution_info,
        pending_activities,
        pending_children,
        pending_decision,
    })
}

fn task_list(value: &cadence_proto::api::TaskList) -> TypedValue {
    let mut fields = vec![field(10, string(&value.name))];
    if (1..=3).contains(&value.kind) {
        fields.push(field(20, TypedValue::I32(value.kind - 1)));
    }
    if !value.base_name.is_empty() {
        fields.push(field(30, string(&value.base_name)));
    }
    struct_value(fields)
}

/// Convert every field in GetMutableStateResponse, including optional
/// nested version histories. Field IDs are from history.thrift/shared.thrift.
pub(crate) fn get_mutable_state_response(
    value: &cadence_proto::history::GetMutableStateResponse,
) -> TypedStruct {
    let mut fields = vec![
        field(30, i64_value(value.next_event_id)),
        field(40, i64_value(value.last_first_event_id)),
        field(70, string(&value.client_library_version)),
        field(80, string(&value.client_feature_version)),
        field(90, string(&value.client_impl)),
        field(
            100,
            TypedValue::Bool(
                value.workflow_state == cadence_proto::shared::WorkflowState::Running as i32,
            ),
        ),
        field(120, TypedValue::I32(value.event_store_version)),
        field(180, TypedValue::Bool(value.is_sticky_task_list_enabled)),
        field(190, i64_value(value.history_size)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(10, workflow_execution(execution)));
    }
    if let Some(kind) = &value.workflow_type {
        fields.push(field(20, workflow_type(kind)));
    }
    if let Some(previous) = &value.previous_started_event_id {
        fields.push(field(35, i64_value(previous.value)));
    }
    if let Some(task) = &value.task_list {
        fields.push(field(50, task_list(task)));
    }
    if let Some(task) = &value.sticky_task_list {
        fields.push(field(60, task_list(task)));
    }
    if !value.current_branch_token.is_empty() {
        fields.push(field(130, binary(&value.current_branch_token)));
    }
    if let Some(state) = workflow_state_to_thrift(value.workflow_state) {
        fields.push(field(150, TypedValue::I32(state)));
    }
    if let Some(state) = close_state_to_thrift(value.workflow_close_state) {
        fields.push(field(160, TypedValue::I32(state)));
    }
    if let Some(histories) = &value.version_histories {
        fields.push(field(170, version_histories(histories)));
    }
    if let Some(timeout) = &value.sticky_task_list_schedule_to_start_timeout {
        // Go durationToSeconds truncates the complete duration toward zero.
        let nanos = i128::from(timeout.seconds) * 1_000_000_000 + i128::from(timeout.nanos);
        fields.push(field(110, TypedValue::I32((nanos / 1_000_000_000) as i32)));
    }
    finish(fields)
}

pub(crate) fn decode_typed_struct(bytes: &[u8]) -> cadence_thrift::Result<TypedStruct> {
    let mut reader = Reader::new(bytes);
    let value = read_typed_struct(&mut reader)?;
    if reader.remaining() != 0 {
        return Err(cadence_thrift::Error::Invalid(
            "trailing bytes after Thrift struct".into(),
        ));
    }
    Ok(value)
}

pub(crate) fn encode_typed_struct(value: &TypedStruct) -> Vec<u8> {
    let mut writer = cadence_thrift::protocol::Writer::new();
    write_typed_struct(&mut writer, value);
    writer.into_bytes()
}

fn read_typed_struct(r: &mut Reader<'_>) -> cadence_thrift::Result<TypedStruct> {
    let mut fields = Vec::new();
    while let Some((ty, id)) = r.read_field_begin()? {
        fields.push((id, read_typed_value(r, ty)?));
    }
    Ok(TypedStruct { fields })
}

fn read_typed_value(r: &mut Reader<'_>, ty: TType) -> cadence_thrift::Result<TypedValue> {
    Ok(match ty {
        TType::Bool => TypedValue::Bool(r.read_bool()?),
        TType::Byte => TypedValue::I8(r.read_u8()? as i8),
        TType::I16 => TypedValue::I16(r.read_i16()?),
        TType::I32 => TypedValue::I32(r.read_i32()?),
        TType::I64 => TypedValue::I64(r.read_i64()?),
        TType::Double => TypedValue::F64(r.read_f64()?),
        TType::String => TypedValue::Bytes(r.read_bytes()?.to_vec()),
        TType::Struct => TypedValue::Struct(read_typed_struct(r)?),
        TType::List | TType::Set => {
            let (element, n) = r.read_list_begin()?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                values.push(read_typed_value(r, element)?);
            }
            if ty == TType::List {
                TypedValue::List(element, values)
            } else {
                TypedValue::Set(element, values)
            }
        }
        TType::Map => {
            let (key, val, n) = r.read_map_begin()?;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push((read_typed_value(r, key)?, read_typed_value(r, val)?));
            }
            TypedValue::Map(key, val, entries)
        }
        TType::Stop => {
            return Err(cadence_thrift::Error::Invalid(
                "STOP is not a value type".into(),
            ))
        }
    })
}

fn write_typed_struct(w: &mut cadence_thrift::protocol::Writer, s: &TypedStruct) {
    for (id, value) in &s.fields {
        w.write_field_begin(value.ty(), *id);
        write_typed_value(w, value);
    }
    w.write_field_stop();
}

fn write_typed_value(w: &mut cadence_thrift::protocol::Writer, v: &TypedValue) {
    match v {
        TypedValue::Bool(x) => w.write_bool(*x),
        TypedValue::I8(x) => w.write_u8(*x as u8),
        TypedValue::I16(x) => w.write_i16(*x),
        TypedValue::I32(x) => w.write_i32(*x),
        TypedValue::I64(x) => w.write_i64(*x),
        TypedValue::F64(x) => w.write_f64(*x),
        TypedValue::Bytes(x) => w.write_bytes(x),
        TypedValue::Struct(x) => write_typed_struct(w, x),
        TypedValue::List(ty, values) | TypedValue::Set(ty, values) => {
            w.write_list_begin(*ty, values.len());
            for v in values {
                write_typed_value(w, v);
            }
        }
        TypedValue::Map(key, val, entries) => {
            w.write_map_begin(*key, *val, entries.len());
            for (k, v) in entries {
                write_typed_value(w, k);
                write_typed_value(w, v);
            }
        }
    }
}

#[cfg(test)]
mod typed_codec_tests {
    use super::*;

    #[test]
    fn empty_container_types_survive_tchannel_codec_round_trip() {
        let value = TypedStruct {
            fields: vec![
                (10, TypedValue::List(TType::Struct, vec![])),
                (20, TypedValue::Set(TType::I64, vec![])),
                (30, TypedValue::Map(TType::String, TType::Struct, vec![])),
            ],
        };
        let encoded = encode_typed_struct(&value);
        assert_eq!(decode_typed_struct(&encoded).unwrap(), value);
    }

    #[test]
    fn version_histories_uses_pinned_shared_thrift_ids_and_container_types() {
        let value = cadence_proto::shared::VersionHistories {
            current_version_history_index: 1,
            histories: vec![cadence_proto::admin::VersionHistory {
                branch_token: vec![b'A'],
                items: vec![cadence_proto::admin::VersionHistoryItem {
                    event_id: 7,
                    version: 2,
                }],
            }],
        };
        let encoded = encode_typed_struct(&TypedStruct {
            fields: vec![field(10, version_histories(&value))],
        });
        // Golden binary payload from shared.thrift: VersionHistories(10,20),
        // VersionHistory(10,20), VersionHistoryItem(10,20).
        let expected = vec![
            12, 0, 10, 8, 0, 10, 0, 0, 0, 1, 15, 0, 20, 12, 0, 0, 0, 1, 11, 0, 10, 0, 0, 0, 1,
            b'A', 15, 0, 20, 12, 0, 0, 0, 1, 10, 0, 10, 0, 0, 0, 0, 0, 0, 0, 7, 10, 0, 20, 0, 0, 0,
            0, 0, 0, 0, 2, 0, 0, 0, 0,
        ];
        assert_eq!(encoded, expected);
        let decoded = decode_typed_struct(&encoded).unwrap();
        let TypedValue::Struct(histories) = typed_field(&decoded, 10).unwrap() else {
            panic!("field 10 is VersionHistories")
        };
        assert_eq!(version_histories_from_thrift(histories).unwrap(), value);
    }

    #[test]
    fn mutable_state_schema_preserves_go_zero_and_enum_mapping() {
        let request = cadence_proto::history::GetMutableStateRequest {
            domain_id: "domain".into(),
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: "wf".into(),
                run_id: "run".into(),
            }),
            expected_next_event_id: 0,
            current_branch_token: Vec::new(),
            version_history_item: Some(cadence_proto::admin::VersionHistoryItem {
                event_id: 0,
                version: 0,
            }),
        };
        let encoded_request = encode_typed_struct(&get_mutable_state_request(&request));
        let expected_request = vec![
            11, 0, 10, 0, 0, 0, 6, b'd', b'o', b'm', b'a', b'i', b'n', 12, 0, 20, 11, 0, 10, 0, 0,
            0, 2, b'w', b'f', 11, 0, 20, 0, 0, 0, 3, b'r', b'u', b'n', 0, 10, 0, 30, 0, 0, 0, 0, 0,
            0, 0, 0, 12, 0, 50, 10, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 20, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        assert_eq!(encoded_request, expected_request);
        let decoded_request = decode_typed_struct(&encoded_request).unwrap();
        assert_eq!(
            get_mutable_state_request_from_thrift(&decoded_request).unwrap(),
            request
        );
        // Go mapper takes a protobuf enum and emits the zero-based persistence
        // value required by the Thrift IDL; the reverse mapping restores it.
        assert_eq!(
            workflow_state_to_thrift(cadence_proto::shared::WorkflowState::Created as i32),
            Some(0)
        );
        assert_eq!(
            workflow_state_from_thrift(0),
            cadence_proto::shared::WorkflowState::Created as i32
        );
        assert_eq!(
            close_state_to_thrift(
                cadence_proto::api::WorkflowExecutionCloseStatus::Completed as i32
            ),
            Some(0)
        );
        assert_eq!(
            close_state_from_thrift(0),
            cadence_proto::api::WorkflowExecutionCloseStatus::Completed as i32
        );
    }

    #[test]
    fn mutable_state_response_round_trips_all_used_schema_fields() {
        let response = cadence_proto::history::GetMutableStateResponse {
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: "wf".into(),
                run_id: "run".into(),
            }),
            workflow_type: Some(cadence_proto::api::WorkflowType {
                name: "type".into(),
            }),
            next_event_id: 23,
            previous_started_event_id: Some(prost_types::Int64Value { value: 17 }),
            last_first_event_id: 2,
            task_list: Some(cadence_proto::api::TaskList {
                name: "normal".into(),
                kind: 1,
                base_name: String::new(),
            }),
            sticky_task_list: Some(cadence_proto::api::TaskList {
                name: "sticky".into(),
                kind: 2,
                base_name: "base".into(),
            }),
            client_library_version: "sdk".into(),
            client_feature_version: "feature".into(),
            client_impl: "impl".into(),
            sticky_task_list_schedule_to_start_timeout: Some(prost_types::Duration {
                seconds: 3,
                nanos: 500_000_000,
            }),
            event_store_version: 2,
            current_branch_token: vec![1, 2, 3],
            workflow_state: cadence_proto::shared::WorkflowState::Running as i32,
            workflow_close_state: cadence_proto::api::WorkflowExecutionCloseStatus::Completed
                as i32,
            version_histories: Some(cadence_proto::shared::VersionHistories {
                current_version_history_index: 0,
                histories: vec![cadence_proto::admin::VersionHistory {
                    branch_token: vec![4],
                    items: vec![cadence_proto::admin::VersionHistoryItem {
                        event_id: 9,
                        version: 1,
                    }],
                }],
            }),
            is_sticky_task_list_enabled: true,
            history_size: 4096,
        };
        let encoded = encode_typed_struct(&get_mutable_state_response(&response));
        let decoded = decode_typed_struct(&encoded).unwrap();
        assert_eq!(typed_field(&decoded, 110), Some(&TypedValue::I32(3)));
        let mut expected = response;
        expected.sticky_task_list_schedule_to_start_timeout = Some(prost_types::Duration {
            seconds: 3,
            nanos: 0,
        });
        assert_eq!(
            get_mutable_state_response_from_thrift(&decoded).unwrap(),
            expected
        );
    }

    #[test]
    fn refresh_workflow_tasks_matches_nested_thrift_request_fixture() {
        let request = cadence_proto::history::RefreshWorkflowTasksRequest {
            domain: "name".into(),
            domain_id: "id".into(),
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: "wf".into(),
                run_id: "run".into(),
            }),
        };
        let encoded = encode_typed_struct(&refresh_workflow_tasks_request(&request));
        let expected = vec![
            11, 0, 10, 0, 0, 0, 2, b'i', b'd', 12, 0, 20, 11, 0, 10, 0, 0, 0, 4, b'n', b'a', b'm',
            b'e', 12, 0, 20, 11, 0, 10, 0, 0, 0, 2, b'w', b'f', 11, 0, 20, 0, 0, 0, 3, b'r', b'u',
            b'n', 0, 0, 0,
        ];
        assert_eq!(encoded, expected);
        let decoded = decode_typed_struct(&encoded).unwrap();
        assert_eq!(
            refresh_workflow_tasks_request_from_thrift(&decoded).unwrap(),
            request
        );
    }

    #[test]
    fn describe_workflow_request_maps_query_consistency_enum_offset() {
        let request = cadence_proto::history::DescribeWorkflowExecutionRequest {
            domain_id: "id".into(),
            request: Some(cadence_proto::api::DescribeWorkflowExecutionRequest {
                domain: "name".into(),
                workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                    workflow_id: "wf".into(),
                    run_id: "run".into(),
                }),
                query_consistency_level: cadence_proto::api::QueryConsistencyLevel::Strong as i32,
            }),
        };
        let encoded = encode_typed_struct(&describe_workflow_execution_request(&request));
        let decoded = decode_typed_struct(&encoded).unwrap();
        let TypedValue::Struct(inner) = typed_field(&decoded, 20).unwrap() else {
            panic!("field 20 is the shared DescribeWorkflowExecutionRequest")
        };
        assert_eq!(typed_field(inner, 30), Some(&TypedValue::I32(1)));
        assert_eq!(
            describe_workflow_execution_request_from_thrift(&decoded).unwrap(),
            request
        );
    }

    #[test]
    fn describe_response_encodes_matching_decision_and_activity_fields() {
        let mut response = cadence_proto::api::DescribeWorkflowExecutionResponse::default();
        let mut info = cadence_proto::api::WorkflowExecutionInfo::default();
        info.workflow_execution = Some(cadence_proto::api::WorkflowExecution {
            workflow_id: "wf".into(),
            run_id: "run".into(),
        });
        info.close_status = cadence_proto::api::WorkflowExecutionCloseStatus::Invalid as i32;
        info.history_length = 42;
        response.workflow_execution_info = Some(info);
        response.pending_decision = Some(cadence_proto::api::PendingDecisionInfo {
            state: cadence_proto::api::PendingDecisionState::Started as i32,
            attempt: 3,
            schedule_id: 17,
            ..Default::default()
        });
        response
            .pending_activities
            .push(cadence_proto::api::PendingActivityInfo {
                activity_id: "act".into(),
                state: cadence_proto::api::PendingActivityState::Started as i32,
                attempt: 2,
                schedule_id: 19,
                ..Default::default()
            });
        let encoded = encode_typed_struct(&describe_workflow_execution_response(&response));
        let decoded = decode_typed_struct(&encoded).unwrap();
        let TypedValue::Struct(info) = typed_field(&decoded, 20).unwrap() else {
            panic!("field 20 is WorkflowExecutionInfo")
        };
        assert_eq!(typed_field(info, 60), Some(&TypedValue::I64(42)));
        let TypedValue::Struct(decision) = typed_field(&decoded, 50).unwrap() else {
            panic!("field 50 is PendingDecisionInfo")
        };
        assert_eq!(typed_field(decision, 10), Some(&TypedValue::I32(1)));
        assert_eq!(typed_field(decision, 40), Some(&TypedValue::I64(3)));
        let Some(TypedValue::List(TType::Struct, activities)) = typed_field(&decoded, 30) else {
            panic!("field 30 is list<PendingActivityInfo>")
        };
        let TypedValue::Struct(activity) = &activities[0] else {
            panic!("pending activity is a struct")
        };
        assert_eq!(typed_field(activity, 30), Some(&TypedValue::I32(1)));
        assert_eq!(typed_field(activity, 150), Some(&TypedValue::I64(19)));
        let decoded_response = describe_workflow_execution_response_from_thrift(&decoded).unwrap();
        assert_eq!(
            decoded_response
                .workflow_execution_info
                .as_ref()
                .unwrap()
                .history_length,
            42
        );
        assert_eq!(
            decoded_response.pending_decision.as_ref().unwrap().state,
            cadence_proto::api::PendingDecisionState::Started as i32
        );
        assert_eq!(
            decoded_response.pending_activities[0].state,
            cadence_proto::api::PendingActivityState::Started as i32
        );
        assert_eq!(decoded_response.pending_activities[0].schedule_id, 19);
    }
}
