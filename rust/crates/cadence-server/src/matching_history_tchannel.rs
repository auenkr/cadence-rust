//! Native outbound YARPC/TChannel Thrift calls used by Matching's History
//! client when Go's `EnableGRPCOutbound` selector chooses TChannel.
//!
//! The five operation adapters are intentionally colocated with the History
//! client; this module owns only the TChannel/Thrift wire boundary and never
//! owns retries or operation deadlines.

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
}
