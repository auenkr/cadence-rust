//! Native outbound YARPC/TChannel Thrift calls used by Matching's History
//! client when Go's `EnableGRPCOutbound` selector chooses TChannel.
//!
//! Operation schemas and wire conversion live beside the History client;
//! retries and operation deadlines remain owned by the History client.

use cadence_thrift::{protocol::Reader, TType};
use std::time::Duration;
use tonic::{Code, Status};

#[path = "matching_history_tchannel_native.rs"]
mod native;

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
        let procedure = format!("HistoryService::{method}");
        // YARPC Thrift methods receive an Args struct whose first field is
        // the single IDL argument. The method-specific request mappers above
        // produce that nested argument value.
        let arguments = TypedStruct {
            fields: vec![field(1, TypedValue::Struct(body.clone()))],
        };
        // These are TChannel transport headers. YARPC places the caller name
        // in `cn` and the selected encoder in `as`; RPC service metadata is
        // carried by the TChannel call itself, not in application headers.
        let response = crate::tchannel_json_client::call_thrift(
            &self.address,
            HISTORY_SERVICE,
            &procedure,
            "cadence-matching",
            &[],
            &encode_typed_struct(&arguments),
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
        let result = decode_typed_struct(&response.body)
            .map_err(|error| Status::internal(format!("invalid History Thrift result: {error}")))?;
        if response.application_error {
            return Err(application_error(method, &response.headers, &result));
        }
        if method == "RefreshWorkflowTasks" && result.fields.is_empty() {
            return Ok(TypedStruct::default());
        }
        field_struct(&result, 0)
            .map_err(|error| {
                Status::internal(format!("invalid History Thrift success result: {error}"))
            })?
            .cloned()
            .ok_or_else(|| Status::internal("History Thrift success result is missing field 0."))
    }

    pub(crate) async fn refresh_workflow_tasks(
        &self,
        request: &cadence_proto::history::RefreshWorkflowTasksRequest,
        timeout: Duration,
    ) -> Result<(), Status> {
        self.call(
            "RefreshWorkflowTasks",
            &refresh_workflow_tasks_request(request),
            timeout,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn describe_workflow_execution(
        &self,
        request: &cadence_proto::history::DescribeWorkflowExecutionRequest,
        timeout: Duration,
    ) -> Result<cadence_proto::history::DescribeWorkflowExecutionResponse, Status> {
        let response = self
            .call(
                "DescribeWorkflowExecution",
                &describe_workflow_execution_request(request),
                timeout,
            )
            .await?;
        describe_workflow_execution_response_from_thrift(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History DescribeWorkflowExecution response: {error}"
            ))
        })
    }

    pub(crate) async fn get_mutable_state(
        &self,
        request: &cadence_proto::history::GetMutableStateRequest,
        timeout: Duration,
    ) -> Result<cadence_proto::history::GetMutableStateResponse, Status> {
        let response = self
            .call(
                "GetMutableState",
                &get_mutable_state_request(request),
                timeout,
            )
            .await?;
        get_mutable_state_response_from_thrift(&response).map_err(|error| {
            Status::internal(format!("invalid History GetMutableState response: {error}"))
        })
    }

    pub(crate) async fn record_decision_task_started(
        &self,
        request: &cadence_proto::history::RecordDecisionTaskStartedRequest,
        timeout: Duration,
    ) -> Result<cadence_proto::history::RecordDecisionTaskStartedResponse, Status> {
        let response = self
            .call(
                "RecordDecisionTaskStarted",
                &record_decision_task_started_request(request),
                timeout,
            )
            .await?;
        record_decision_task_started_response_from_thrift(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History RecordDecisionTaskStarted response: {error}"
            ))
        })
    }

    pub(crate) async fn record_activity_task_started(
        &self,
        request: &cadence_proto::history::RecordActivityTaskStartedRequest,
        timeout: Duration,
    ) -> Result<cadence_proto::history::RecordActivityTaskStartedResponse, Status> {
        let response = self
            .call(
                "RecordActivityTaskStarted",
                &record_activity_task_started_request(request),
                timeout,
            )
            .await?;
        record_activity_task_started_response_from_thrift(&response).map_err(|error| {
            Status::internal(format!(
                "invalid History RecordActivityTaskStarted response: {error}"
            ))
        })
    }
}

fn exception_name(method: &str, id: i16) -> Option<&'static str> {
    match (method, id) {
        ("GetMutableState" | "DescribeWorkflowExecution", 1) => Some("BadRequestError"),
        ("GetMutableState" | "DescribeWorkflowExecution", 2) => Some("InternalServiceError"),
        ("GetMutableState" | "DescribeWorkflowExecution", 3) => Some("EntityNotExistsError"),
        ("GetMutableState" | "DescribeWorkflowExecution", 4) => Some("ShardOwnershipLostError"),
        ("GetMutableState" | "DescribeWorkflowExecution", 5) => Some("LimitExceededError"),
        ("GetMutableState" | "DescribeWorkflowExecution", 6) => Some("ServiceBusyError"),
        ("GetMutableState", 7) => Some("CurrentBranchChangedError"),
        ("RefreshWorkflowTasks", 1) => Some("BadRequestError"),
        ("RefreshWorkflowTasks", 2) => Some("InternalServiceError"),
        ("RefreshWorkflowTasks", 3) => Some("DomainNotActiveError"),
        ("RefreshWorkflowTasks", 4) => Some("ShardOwnershipLostError"),
        ("RefreshWorkflowTasks", 5) => Some("ServiceBusyError"),
        ("RefreshWorkflowTasks", 6) => Some("EntityNotExistsError"),
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 1) => Some("BadRequestError"),
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 2) => {
            Some("InternalServiceError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 3) => {
            Some("EventAlreadyStartedError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 4) => {
            Some("EntityNotExistsError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 5) => {
            Some("ShardOwnershipLostError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 6) => {
            Some("DomainNotActiveError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 7) => {
            Some("LimitExceededError")
        }
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 8) => Some("ServiceBusyError"),
        ("RecordDecisionTaskStarted" | "RecordActivityTaskStarted", 9) => {
            Some("WorkflowExecutionAlreadyCompletedError")
        }
        _ => None,
    }
}

fn error_string_list(value: Option<&TypedValue>, id: i16) -> Vec<String> {
    match value {
        Some(TypedValue::List(TType::String, values)) => values
            .iter()
            .filter_map(|value| string_value(value, id).ok())
            .collect(),
        _ => Vec::new(),
    }
}

fn application_error(method: &str, headers: &[(String, String)], result: &TypedStruct) -> Status {
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
    let (exception_id, exception) = result
        .fields
        .first()
        .and_then(|(id, value)| match value {
            TypedValue::Struct(exception) => Some((*id, exception)),
            _ => None,
        })
        .map_or((None, None), |(id, value)| (Some(id), Some(value)));
    let name = exception_id
        .and_then(|id| exception_name(method, id))
        .unwrap_or(name);
    let message_id = if matches!(
        name,
        "ShardOwnershipLostError" | "CurrentBranchChangedError"
    ) {
        10
    } else {
        1
    };
    let message = exception
        .and_then(|value| field_string(value, message_id).ok())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| details.to_owned());
    use cadence_proto::{api, shared};
    match name {
        "BadRequestError" => Status::invalid_argument(message),
        "EntityNotExistsError" => crate::grpc::detailed_status(
            Code::NotFound,
            message,
            name,
            api::EntityNotExistsError {
                current_cluster: exception
                    .and_then(|value| field_string(value, 2).ok())
                    .unwrap_or_default(),
                active_cluster: exception
                    .and_then(|value| field_string(value, 3).ok())
                    .unwrap_or_default(),
                active_clusters: error_string_list(
                    exception.and_then(|value| typed_field(value, 4)),
                    4,
                ),
            },
        ),
        "WorkflowExecutionAlreadyCompletedError" => crate::grpc::detailed_status(
            Code::NotFound,
            message,
            name,
            api::WorkflowExecutionAlreadyCompletedError {},
        ),
        "EventAlreadyStartedError" => crate::grpc::detailed_status_type(
            Code::AlreadyExists,
            message,
            "type.googleapis.com/uber.cadence.shared.v1.EventAlreadyStartedError".into(),
            shared::EventAlreadyStartedError {},
        ),
        "DomainNotActiveError" => crate::grpc::detailed_status(
            Code::FailedPrecondition,
            message,
            name,
            api::DomainNotActiveError {
                domain: exception
                    .and_then(|value| field_string(value, 2).ok())
                    .unwrap_or_default(),
                current_cluster: exception
                    .and_then(|value| field_string(value, 3).ok())
                    .unwrap_or_default(),
                active_cluster: exception
                    .and_then(|value| field_string(value, 4).ok())
                    .unwrap_or_default(),
                active_clusters: error_string_list(
                    exception.and_then(|value| typed_field(value, 5)),
                    5,
                ),
            },
        ),
        "ShardOwnershipLostError" => {
            let owner = exception
                .and_then(|value| field_string(value, 20).ok())
                .unwrap_or_default();
            crate::grpc::detailed_status_type(
                Code::Aborted,
                message,
                "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError".into(),
                shared::ShardOwnershipLostError { owner },
            )
        }
        "ServiceBusyError" => crate::grpc::detailed_status(
            Code::ResourceExhausted,
            message,
            name,
            api::ServiceBusyError {
                reason: exception
                    .and_then(|value| field_string(value, 2).ok())
                    .unwrap_or_default(),
            },
        ),
        "LimitExceededError" => crate::grpc::detailed_status(
            Code::ResourceExhausted,
            message,
            name,
            api::LimitExceededError {},
        ),
        "CurrentBranchChangedError" => crate::grpc::detailed_status_type(
            Code::Aborted,
            message,
            "type.googleapis.com/uber.cadence.shared.v1.CurrentBranchChangedError".into(),
            shared::CurrentBranchChangedError {
                current_branch_token: exception
                    .and_then(|value| field_bytes(value, 20).ok())
                    .unwrap_or_default(),
            },
        ),
        // FromInternalServiceError intentionally maps CodeInternal without
        // details in Go's proto mapper.
        "InternalServiceError" => Status::internal(message),
        _ => Status::new(Code::Unknown, format!("{name}: {details}")),
    }
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

#[cfg(test)]
fn version_history_item_list(values: &[cadence_proto::admin::VersionHistoryItem]) -> TypedValue {
    TypedValue::List(
        TType::Struct,
        values.iter().map(version_history_item).collect(),
    )
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
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
fn as_f64(value: &TypedValue, id: i16) -> cadence_thrift::Result<f64> {
    match value {
        TypedValue::F64(value) => Ok(*value),
        _ => Err(thrift_invalid(format!("field {id}: expected double"))),
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
#[cfg(test)]
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
    let previous_started_event_id = typed_field(s, 35).map(|v| as_i64(v, 35)).transpose()?;
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn timestamp_nanos(value: &prost_types::Timestamp) -> i64 {
    (i128::from(value.seconds) * 1_000_000_000 + i128::from(value.nanos)) as i64
}
#[cfg(test)]
fn timestamp_field(id: i16, value: Option<&prost_types::Timestamp>) -> Option<(i16, TypedValue)> {
    value.map(|value| field(id, i64_value(timestamp_nanos(value))))
}
#[cfg(test)]
fn enum_field(id: i16, value: i32, proto_max: i32) -> Option<(i16, TypedValue)> {
    (1..=proto_max)
        .contains(&value)
        .then(|| field(id, TypedValue::I32(value - 1)))
}
#[cfg(test)]
fn string_map(value: &std::collections::HashMap<String, String>) -> TypedValue {
    TypedValue::Map(
        TType::String,
        TType::String,
        value.iter().map(|(k, v)| (string(k), string(v))).collect(),
    )
}
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
fn reset_points(value: &cadence_proto::api::ResetPoints) -> TypedValue {
    let points: Vec<TypedValue> = value
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
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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

#[cfg(test)]
fn activity_type(value: &cadence_proto::api::ActivityType) -> TypedValue {
    struct_value(vec![field(10, string(&value.name))])
}
#[cfg(test)]
fn failure_options(value: &cadence_proto::api::FailureOptions) -> TypedValue {
    let mut fields = Vec::new();
    fields.extend(enum_field(10, value.failure_category, 3));
    if let Some(duration) = &value.next_retry_interval {
        let nanos = i128::from(duration.seconds) * 1_000_000_000 + i128::from(duration.nanos);
        fields.push(field(20, TypedValue::I32((nanos / 1_000_000_000) as i32)));
    }
    struct_value(finish(fields).fields)
}
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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

#[cfg(test)]
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
        .map(|attribute| -> cadence_thrift::Result<_> {
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
        .map(|value| -> cadence_thrift::Result<_> {
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

fn poll_for_decision_task_request(
    value: &cadence_proto::api::PollForDecisionTaskRequest,
) -> TypedValue {
    let mut fields = vec![
        field(10, string(&value.domain)),
        field(30, string(&value.identity)),
        field(40, string(&value.binary_checksum)),
    ];
    if let Some(task) = &value.task_list {
        fields.push(field(20, task_list(task)));
    }
    struct_value(finish(fields).fields)
}
fn poll_for_activity_task_request(
    value: &cadence_proto::api::PollForActivityTaskRequest,
) -> TypedValue {
    let mut fields = vec![
        field(10, string(&value.domain)),
        field(30, string(&value.identity)),
    ];
    if let Some(task) = &value.task_list {
        fields.push(field(20, task_list(task)));
    }
    if let Some(metadata) = &value.task_list_metadata {
        let mut metadata_fields = Vec::new();
        if let Some(maximum) = metadata.max_tasks_per_second {
            metadata_fields.push(field(10, TypedValue::F64(maximum)));
        }
        fields.push(field(40, struct_value(metadata_fields)));
    }
    struct_value(finish(fields).fields)
}
pub(crate) fn record_decision_task_started_request(
    value: &cadence_proto::history::RecordDecisionTaskStartedRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, string(&value.domain_id)),
        field(30, i64_value(value.schedule_id)),
        field(40, i64_value(value.task_id)),
        field(45, string(&value.request_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution(execution)));
    }
    if let Some(request) = &value.poll_request {
        fields.push(field(50, poll_for_decision_task_request(request)));
    }
    finish(fields)
}
pub(crate) fn record_activity_task_started_request(
    value: &cadence_proto::history::RecordActivityTaskStartedRequest,
) -> TypedStruct {
    let mut fields = vec![
        field(10, string(&value.domain_id)),
        field(30, i64_value(value.schedule_id)),
        field(40, i64_value(value.task_id)),
        field(45, string(&value.request_id)),
    ];
    if let Some(execution) = &value.workflow_execution {
        fields.push(field(20, workflow_execution(execution)));
    }
    if let Some(request) = &value.poll_request {
        fields.push(field(50, poll_for_activity_task_request(request)));
    }
    finish(fields)
}

fn duration_from_seconds(value: Option<i32>) -> Option<prost_types::Duration> {
    value.map(|seconds| prost_types::Duration {
        seconds: i64::from(seconds),
        nanos: 0,
    })
}
fn history_event_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::api::HistoryEvent> {
    use cadence_proto::api::history_event::Attributes;
    let event_type = field_i32(value, 30)?;
    let attributes = match event_type {
        4 => {
            let attrs = field_struct(value, 80)?
                .ok_or_else(|| thrift_invalid("decision scheduled event is missing field 80"))?;
            Some(Attributes::DecisionTaskScheduledEventAttributes(
                cadence_proto::api::DecisionTaskScheduledEventAttributes {
                    task_list: field_struct(attrs, 10)?
                        .map(task_list_from_thrift)
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
                cadence_proto::api::DecisionTaskStartedEventAttributes {
                    scheduled_event_id: field_i64(attrs, 10)?,
                    identity: field_string(attrs, 20)?,
                    request_id: field_string(attrs, 30)?,
                },
            ))
        }
        9 => {
            let attrs = field_struct(value, 130)?
                .ok_or_else(|| thrift_invalid("activity scheduled event is missing field 130"))?;
            let activity_type = field_struct(attrs, 20)?
                .map(|t| -> cadence_thrift::Result<_> {
                    Ok(cadence_proto::api::ActivityType {
                        name: field_string(t, 10)?,
                    })
                })
                .transpose()?;
            let input = field_bytes(attrs, 40)?;
            let retry_policy = field_struct(attrs, 110)?
                .map(|policy| {
                    let non_retryable_error_reasons = match typed_field(policy, 50) {
                        None => Vec::new(),
                        Some(TypedValue::List(TType::String, values)) => values
                            .iter()
                            .map(|value| string_value(value, 50))
                            .collect::<cadence_thrift::Result<Vec<_>>>()?,
                        Some(_) => {
                            return Err(thrift_invalid(
                                "RetryPolicy.nonRetriableErrorReasons: expected list<string>",
                            ))
                        }
                    };
                    Ok(cadence_proto::api::RetryPolicy {
                        initial_interval: duration_from_seconds(
                            typed_field(policy, 10).map(|v| as_i32(v, 10)).transpose()?,
                        ),
                        backoff_coefficient: typed_field(policy, 20)
                            .map(|v| as_f64(v, 20))
                            .transpose()?
                            .unwrap_or_default(),
                        maximum_interval: duration_from_seconds(
                            typed_field(policy, 30).map(|v| as_i32(v, 30)).transpose()?,
                        ),
                        maximum_attempts: field_i32(policy, 40)?,
                        non_retryable_error_reasons,
                        expiration_interval: duration_from_seconds(
                            typed_field(policy, 60).map(|v| as_i32(v, 60)).transpose()?,
                        ),
                        jitter_coefficient: typed_field(policy, 70)
                            .map(|v| as_f64(v, 70))
                            .transpose()?
                            .unwrap_or_default(),
                    })
                })
                .transpose()?;
            let header = field_struct(attrs, 120)?
                .map(|header| {
                    let mut fields = std::collections::HashMap::new();
                    for (key, data) in typed_map_entries(header, 10)? {
                        fields.insert(
                            string_value(key, 10)?,
                            cadence_proto::api::Payload {
                                data: bytes_value(data, 10)?,
                            },
                        );
                    }
                    Ok::<_, cadence_thrift::Error>(cadence_proto::api::Header { fields })
                })
                .transpose()?;
            Some(Attributes::ActivityTaskScheduledEventAttributes(
                cadence_proto::api::ActivityTaskScheduledEventAttributes {
                    activity_id: field_string(attrs, 10)?,
                    activity_type,
                    domain: field_string(attrs, 25)?,
                    task_list: field_struct(attrs, 30)?
                        .map(task_list_from_thrift)
                        .transpose()?,
                    input: (!input.is_empty() || typed_field(attrs, 40).is_some())
                        .then_some(cadence_proto::api::Payload { data: input }),
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
    Ok(cadence_proto::api::HistoryEvent {
        event_id: field_i64(value, 10)?,
        event_time: timestamp_from_nanos(
            typed_field(value, 20).map(|v| as_i64(v, 20)).transpose()?,
        ),
        version: field_i64(value, 35)?,
        task_id: field_i64(value, 36)?,
        attributes,
    })
}

fn transient_decision_info_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::shared::TransientDecisionInfo> {
    Ok(cadence_proto::shared::TransientDecisionInfo {
        scheduled_event: field_struct(value, 10)?
            .map(history_event_from_thrift)
            .transpose()?,
        started_event: field_struct(value, 20)?
            .map(history_event_from_thrift)
            .transpose()?,
    })
}

pub(crate) fn record_decision_task_started_response_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::RecordDecisionTaskStartedResponse> {
    let mut queries = std::collections::HashMap::new();
    if let Some(TypedValue::Map(TType::String, TType::Struct, values)) = typed_field(value, 140) {
        for (key, query) in values {
            let TypedValue::Struct(query) = query else {
                return Err(thrift_invalid("queries map value is not struct"));
            };
            let query_args = typed_field(query, 20)
                .map(|v| bytes_value(v, 20))
                .transpose()?
                .map(|data| cadence_proto::api::Payload { data });
            queries.insert(
                string_value(key, 140)?,
                cadence_proto::api::WorkflowQuery {
                    query_type: field_string(query, 10)?,
                    query_args,
                },
            );
        }
    } else if typed_field(value, 140).is_some() {
        return Err(thrift_invalid("field 140: expected map<string,struct>"));
    }
    let workflow_type = field_struct(value, 10)?
        .map(workflow_type_from_thrift)
        .transpose()?;
    let previous_started_event_id = typed_field(value, 20).map(|v| as_i64(v, 20)).transpose()?;
    let decision_info = field_struct(value, 80)?
        .map(transient_decision_info_from_thrift)
        .transpose()?;
    Ok(cadence_proto::history::RecordDecisionTaskStartedResponse {
        workflow_type,
        previous_started_event_id,
        scheduled_event_id: field_i64(value, 30)?,
        started_event_id: field_i64(value, 40)?,
        next_event_id: field_i64(value, 50)?,
        attempt: field_i64(value, 60)? as i32,
        sticky_execution_enabled: field_bool(value, 70)?,
        decision_info,
        workflow_execution_task_list: field_struct(value, 90)?
            .map(task_list_from_thrift)
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

pub(crate) fn record_activity_task_started_response_from_thrift(
    value: &TypedStruct,
) -> cadence_thrift::Result<cadence_proto::history::RecordActivityTaskStartedResponse> {
    let scheduled_event = field_struct(value, 20)?
        .map(history_event_from_thrift)
        .transpose()?;
    let heartbeat = field_bytes(value, 60)?;
    Ok(cadence_proto::history::RecordActivityTaskStartedResponse {
        scheduled_event,
        started_time: timestamp_from_nanos(
            typed_field(value, 30).map(|v| as_i64(v, 30)).transpose()?,
        ),
        attempt: field_i64(value, 40)? as i32,
        scheduled_time_of_this_attempt: timestamp_from_nanos(
            typed_field(value, 50).map(|v| as_i64(v, 50)).transpose()?,
        ),
        heartbeat_details: (!heartbeat.is_empty() || typed_field(value, 60).is_some())
            .then_some(cadence_proto::api::Payload { data: heartbeat }),
        workflow_type: field_struct(value, 70)?
            .map(workflow_type_from_thrift)
            .transpose()?,
        workflow_domain: field_string(value, 80)?,
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
#[cfg(test)]
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
    if let Some(previous) = value.previous_started_event_id {
        fields.push(field(35, i64_value(previous)));
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
    use prost::Message as _;

    #[derive(serde::Deserialize)]
    struct GoWireFixture {
        name: String,
        thrift_type: String,
        base64: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }

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

    // The reviewed fixture is emitted by the pinned Go ThriftRW generator.
    // CI regenerates and compares its bytes before running this Rust consumer.
    #[test]
    fn go_history_tchannel_wire_oracle_consumes_reviewed_fixture() {
        use base64::Engine;
        use std::collections::{BTreeMap, BTreeSet};

        let fixture_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/history_tchannel_wire_oracle.json"
        );
        let fixture_bytes = std::fs::read(fixture_path)
            .expect("run scripts/verify-history-tchannel-wire-oracle.sh after checking in oracle");
        let fixtures: Vec<GoWireFixture> = serde_json::from_slice(&fixture_bytes).unwrap();
        let fixtures: BTreeMap<_, _> = fixtures
            .into_iter()
            .map(|fixture| (fixture.name.clone(), fixture))
            .collect();
        let expected_names: BTreeSet<_> = [
            "get_mutable_state_request",
            "get_mutable_state_response",
            "refresh_workflow_tasks_request",
            "refresh_workflow_tasks_response",
            "describe_workflow_execution_request",
            "describe_workflow_execution_response",
            "record_decision_task_started_request",
            "record_decision_task_started_response",
            "record_activity_task_started_request",
            "record_activity_task_started_response",
            "record_activity_empty_header",
            "record_activity_absent_header",
            "record_activity_empty_input",
            "record_activity_absent_input",
            "record_decision_empty_queries",
            "record_decision_absent_queries",
            "get_mutable_empty_version_histories",
            "get_mutable_absent_version_histories",
            "exception_entity_not_exists",
            "exception_workflow_already_completed",
            "exception_event_already_started",
            "exception_domain_not_active",
            "exception_shard_ownership_lost",
            "exception_service_busy",
            "exception_internal_service",
        ]
        .into_iter()
        .collect();
        assert_eq!(
            fixtures.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            expected_names
        );

        let bytes_for = |name: &str| {
            let fixture = &fixtures[name];
            assert_eq!(fixture.thrift_type, "thrift-binary-struct", "{name}");
            base64::engine::general_purpose::STANDARD
                .decode(&fixture.base64)
                .unwrap_or_else(|error| panic!("{name}: invalid base64: {error}"))
        };
        let request_names = [
            "get_mutable_state_request",
            "refresh_workflow_tasks_request",
            "describe_workflow_execution_request",
            "record_decision_task_started_request",
            "record_activity_task_started_request",
        ];
        for name in request_names {
            let decoded = decode_typed_struct(&bytes_for(name)).unwrap();
            assert!(
                matches!(typed_field(&decoded, 1), Some(TypedValue::Struct(_))),
                "{name} Args field 1"
            );
        }

        for name in [
            "get_mutable_state_response",
            "describe_workflow_execution_response",
            "record_decision_task_started_response",
            "record_activity_task_started_response",
        ] {
            let result = decode_typed_struct(&bytes_for(name)).unwrap();
            assert!(
                matches!(typed_field(&result, 0), Some(TypedValue::Struct(_))),
                "{name} success field 0"
            );
        }
        assert!(
            decode_typed_struct(&bytes_for("refresh_workflow_tasks_response"))
                .unwrap()
                .fields
                .is_empty()
        );

        let exception_cases = [
            (
                "exception_entity_not_exists",
                "GetMutableState",
                "EntityNotExistsError",
                3,
                Code::NotFound,
            ),
            (
                "exception_workflow_already_completed",
                "RecordActivityTaskStarted",
                "WorkflowExecutionAlreadyCompletedError",
                9,
                Code::NotFound,
            ),
            (
                "exception_event_already_started",
                "RecordDecisionTaskStarted",
                "EventAlreadyStartedError",
                3,
                Code::AlreadyExists,
            ),
            (
                "exception_domain_not_active",
                "RefreshWorkflowTasks",
                "DomainNotActiveError",
                3,
                Code::FailedPrecondition,
            ),
            (
                "exception_shard_ownership_lost",
                "DescribeWorkflowExecution",
                "ShardOwnershipLostError",
                4,
                Code::Aborted,
            ),
            (
                "exception_service_busy",
                "RecordActivityTaskStarted",
                "ServiceBusyError",
                8,
                Code::ResourceExhausted,
            ),
            (
                "exception_internal_service",
                "GetMutableState",
                "InternalServiceError",
                2,
                Code::Internal,
            ),
        ];
        for (fixture_name, method, error_name, field_id, expected_code) in exception_cases {
            let result = decode_typed_struct(&bytes_for(fixture_name)).unwrap();
            assert!(
                matches!(typed_field(&result, field_id), Some(TypedValue::Struct(_))),
                "{fixture_name} exception field {field_id}"
            );
            let status = application_error(
                method,
                &[("$rpc$-application-error-name".into(), error_name.into())],
                &result,
            );
            assert_eq!(status.code(), expected_code, "{fixture_name}");
        }

        // Distinguish explicit empty structs/containers from omitted fields
        // in the actual Go bytes; their semantic mapper outputs differ.
        for (present_name, absent_name, field_id) in [
            (
                "record_decision_empty_queries",
                "record_decision_absent_queries",
                140,
            ),
            (
                "get_mutable_empty_version_histories",
                "get_mutable_absent_version_histories",
                170,
            ),
        ] {
            let present = decode_typed_struct(&bytes_for(present_name)).unwrap();
            let absent = decode_typed_struct(&bytes_for(absent_name)).unwrap();
            let TypedValue::Struct(success) = typed_field(&present, 0).unwrap() else {
                panic!("{present_name} result success must be a struct")
            };
            let TypedValue::Struct(absent_success) = typed_field(&absent, 0).unwrap() else {
                panic!("{absent_name} result success must be a struct")
            };
            assert!(
                typed_field(success, field_id).is_some(),
                "{present_name} field {field_id}"
            );
            assert!(
                typed_field(absent_success, field_id).is_none(),
                "{absent_name} field {field_id}"
            );
            assert_ne!(bytes_for(present_name), bytes_for(absent_name));
        }
        for (present_name, absent_name, field_id) in [
            (
                "record_activity_empty_header",
                "record_activity_absent_header",
                120,
            ),
            (
                "record_activity_empty_input",
                "record_activity_absent_input",
                40,
            ),
        ] {
            let present = decode_typed_struct(&bytes_for(present_name)).unwrap();
            let absent = decode_typed_struct(&bytes_for(absent_name)).unwrap();
            let TypedValue::Struct(success) = typed_field(&present, 0).unwrap() else {
                panic!("{present_name} result success must be a struct")
            };
            let TypedValue::Struct(absent_success) = typed_field(&absent, 0).unwrap() else {
                panic!("{absent_name} result success must be a struct")
            };
            let TypedValue::Struct(event) = typed_field(success, 20).unwrap() else {
                panic!("{present_name} must contain scheduled event")
            };
            let TypedValue::Struct(attrs) = typed_field(event, 130).unwrap() else {
                panic!("{present_name} must contain ActivityTaskScheduled attributes")
            };
            assert!(
                typed_field(attrs, field_id).is_some(),
                "{present_name} field {field_id}"
            );
            if field_id == 120 {
                assert!(matches!(
                    typed_field(attrs, 120),
                    Some(TypedValue::Struct(_))
                ));
            } else {
                assert!(
                    matches!(typed_field(attrs, 40), Some(TypedValue::Bytes(bytes)) if bytes.is_empty())
                );
            }
            let absent_event = match typed_field(absent_success, 20) {
                Some(TypedValue::Struct(event)) => event,
                _ => panic!("{absent_name} must contain scheduled event"),
            };
            let absent_attrs = match typed_field(absent_event, 130) {
                Some(TypedValue::Struct(attrs)) => attrs,
                _ => panic!("{absent_name} must contain ActivityTaskScheduled attributes"),
            };
            assert!(
                typed_field(absent_attrs, field_id).is_none(),
                "{absent_name} field {field_id}"
            );
            assert_ne!(bytes_for(present_name), bytes_for(absent_name));
        }
    }

    async fn peer_read_frame(stream: &mut tokio::net::TcpStream) -> (u8, u32, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        let mut header = [0; 16];
        stream.read_exact(&mut header).await.unwrap();
        let size = u16::from_be_bytes([header[0], header[1]]) as usize;
        let mut payload = vec![0; size - 16];
        stream.read_exact(&mut payload).await.unwrap();
        (
            header[2],
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            payload,
        )
    }

    async fn peer_write_frame(
        stream: &mut tokio::net::TcpStream,
        kind: u8,
        id: u32,
        payload: &[u8],
    ) {
        use tokio::io::AsyncWriteExt;
        let mut header = [0; 16];
        header[..2].copy_from_slice(&((16 + payload.len()) as u16).to_be_bytes());
        header[2] = kind;
        header[4..8].copy_from_slice(&id.to_be_bytes());
        stream.write_all(&header).await.unwrap();
        stream.write_all(payload).await.unwrap();
    }

    fn take_u16(bytes: &[u8], offset: &mut usize) -> usize {
        let len = u16::from_be_bytes(bytes[*offset..*offset + 2].try_into().unwrap()) as usize;
        *offset += 2;
        len
    }

    fn take_s8(bytes: &[u8], offset: &mut usize) -> String {
        let len = bytes[*offset] as usize;
        *offset += 1;
        let value = String::from_utf8(bytes[*offset..*offset + len].to_vec()).unwrap();
        *offset += len;
        value
    }

    fn take_arg(bytes: &[u8], offset: &mut usize) -> Vec<u8> {
        let len = take_u16(bytes, offset);
        let value = bytes[*offset..*offset + len].to_vec();
        *offset += len;
        value
    }

    #[tokio::test]
    async fn native_tchannel_peer_exercises_all_five_history_methods_and_wrappers() {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let methods = [
            "RefreshWorkflowTasks",
            "DescribeWorkflowExecution",
            "GetMutableState",
            "RecordDecisionTaskStarted",
            "RecordActivityTaskStarted",
        ];
        let peer_methods = methods;
        let peer = tokio::spawn(async move {
            for expected_method in peer_methods {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (kind, id, init) = peer_read_frame(&mut stream).await;
                assert_eq!((kind, id), (1, 1));
                assert_eq!(&init[..2], &[0, 2]);
                peer_write_frame(&mut stream, 2, 1, &[0, 2, 0, 0]).await;

                let (kind, id, payload) = peer_read_frame(&mut stream).await;
                assert_eq!((kind, id), (3, 2));
                let mut cursor = 30;
                assert_eq!(take_s8(&payload, &mut cursor), "cadence-history");
                let transport_headers = payload[cursor] as usize;
                cursor += 1;
                let mut headers = Vec::new();
                for _ in 0..transport_headers {
                    headers.push((
                        take_s8(&payload, &mut cursor),
                        take_s8(&payload, &mut cursor),
                    ));
                }
                assert!(headers
                    .iter()
                    .any(|(key, value)| key == "as" && value == "thrift"));
                assert!(headers
                    .iter()
                    .any(|(key, value)| key == "cn" && value == "cadence-matching"));
                cursor += 1; // checksum type
                let method = String::from_utf8(take_arg(&payload, &mut cursor)).unwrap();
                assert_eq!(method, format!("HistoryService::{expected_method}"));
                let metadata = take_arg(&payload, &mut cursor);
                assert_eq!(metadata, [0, 0]); // no application headers
                let request = decode_typed_struct(&take_arg(&payload, &mut cursor)).unwrap();
                assert!(matches!(
                    typed_field(&request, 1),
                    Some(TypedValue::Struct(_))
                ));

                let result = if expected_method == "RefreshWorkflowTasks" {
                    encode_typed_struct(&TypedStruct::default())
                } else {
                    encode_typed_struct(&TypedStruct {
                        fields: vec![field(0, TypedValue::Struct(TypedStruct::default()))],
                    })
                };
                let mut reply = vec![0, 0]; // final frame, success
                reply.extend([0; 25]);
                reply.extend([0, 0]); // no transport headers, no checksum
                for argument in [method.as_bytes(), &[0, 0], result.as_slice()] {
                    reply.extend((argument.len() as u16).to_be_bytes());
                    reply.extend(argument);
                }
                peer_write_frame(&mut stream, 4, 2, &reply).await;
            }
        });

        let client = TChannelHistoryClient::new(address);
        for method in methods {
            let response = client
                .call(method, &TypedStruct::default(), Duration::from_secs(2))
                .await
                .unwrap();
            assert!(response.fields.is_empty(), "{method}");
        }
        peer.await.unwrap();
    }

    // Build `go/tools/cadence-rust-history-peer` with GOTOOLCHAIN=go1.25.14,
    // then run this ignored test with CADENCE_HISTORY_PEER_BIN=/path/to/binary.
    // Keeping it opt-in avoids making the normal Rust unit suite depend on Go.
    #[tokio::test]
    #[ignore = "mixed Go/Rust wire gate; requires CADENCE_HISTORY_PEER_BIN"]
    async fn generated_go_history_server_accepts_native_tchannel_calls() {
        use std::{
            io::{BufRead, BufReader, Read},
            net::TcpListener,
            process::{Child, ChildStdout, Command, Stdio},
        };

        struct GoPeer {
            child: Child,
            output: BufReader<ChildStdout>,
        }
        impl Drop for GoPeer {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
        impl GoPeer {
            fn start(mode: &str) -> (Self, String) {
                let binary = std::env::var("CADENCE_HISTORY_PEER_BIN")
                    .expect("set CADENCE_HISTORY_PEER_BIN to the built Go peer");
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap().to_string();
                drop(listener);
                let mut child = Command::new(binary)
                    .args(["-address", &address, "-mode", mode])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap();
                let mut output = BufReader::new(child.stdout.take().unwrap());
                let mut ready = String::new();
                output.read_line(&mut ready).unwrap();
                assert_eq!(ready.trim(), format!("READY {address}"));
                (Self { child, output }, address)
            }
            fn stop_and_read(mut self) -> String {
                self.child.kill().unwrap();
                self.child.wait().unwrap();
                let mut rest = String::new();
                self.output.read_to_string(&mut rest).unwrap();
                rest
            }
        }

        let (peer, address) = GoPeer::start("success");
        let client = TChannelHistoryClient::new(address);
        let execution = cadence_proto::api::WorkflowExecution {
            workflow_id: "workflow-id".into(),
            run_id: "run-id".into(),
        };
        client
            .refresh_workflow_tasks(
                &cadence_proto::history::RefreshWorkflowTasksRequest {
                    domain: "domain".into(),
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution.clone()),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let described = client
            .describe_workflow_execution(
                &cadence_proto::history::DescribeWorkflowExecutionRequest {
                    domain_id: "domain-id".into(),
                    request: Some(cadence_proto::api::DescribeWorkflowExecutionRequest {
                        domain: "domain".into(),
                        workflow_execution: Some(execution.clone()),
                        ..Default::default()
                    }),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(described.pending_decision.unwrap().attempt, 3);
        let mutable = client
            .get_mutable_state(
                &cadence_proto::history::GetMutableStateRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution.clone()),
                    expected_next_event_id: 9,
                    current_branch_token: b"input-branch".to_vec(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(mutable.next_event_id, 17);
        assert_eq!(mutable.client_library_version, "go-peer");
        assert_eq!(mutable.current_branch_token, b"branch");
        let decision = client
            .record_decision_task_started(
                &cadence_proto::history::RecordDecisionTaskStartedRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution.clone()),
                    schedule_id: 51,
                    task_id: 52,
                    request_id: "decision-request".into(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(decision.started_event_id, 22);
        assert_eq!(decision.branch_token, b"decision");
        let activity = client
            .record_activity_task_started(
                &cadence_proto::history::RecordActivityTaskStartedRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution),
                    schedule_id: 61,
                    task_id: 62,
                    request_id: "activity-request".into(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(activity.workflow_domain, "go-domain");
        assert_eq!(activity.heartbeat_details.unwrap().data, b"beat");
        drop(client);
        let calls = peer.stop_and_read();
        let calls: Vec<serde_json::Value> = calls
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(calls.len(), 5, "Go peer output: {calls:?}");
        let methods = [
            "HistoryService::RefreshWorkflowTasks",
            "HistoryService::DescribeWorkflowExecution",
            "HistoryService::GetMutableState",
            "HistoryService::RecordDecisionTaskStarted",
            "HistoryService::RecordActivityTaskStarted",
        ];
        for (call, procedure) in calls.iter().zip(methods) {
            assert_eq!(call["caller"], "cadence-matching");
            assert_eq!(call["service"], "cadence-history");
            assert_eq!(call["transport"], "tchannel");
            assert_eq!(call["encoding"], "thrift");
            assert_eq!(call["procedure"], procedure);
        }

        let (peer, address) = GoPeer::start("redirect");
        let client = TChannelHistoryClient::new(address);
        let redirect_execution = cadence_proto::api::WorkflowExecution {
            workflow_id: "workflow-id".into(),
            run_id: "run-id".into(),
        };
        let error = client
            .describe_workflow_execution(
                &cadence_proto::history::DescribeWorkflowExecutionRequest {
                    domain_id: "domain-id".into(),
                    request: Some(cadence_proto::api::DescribeWorkflowExecutionRequest {
                        domain: "domain".into(),
                        workflow_execution: Some(redirect_execution),
                        ..Default::default()
                    }),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Code::Aborted,
            "unexpected typed redirect error: {error:?}"
        );
        assert_eq!(error.message(), "wrong owner");
        let rpc_status = RpcStatus::decode(error.details()).unwrap();
        assert!(rpc_status.details.iter().any(|detail| {
            detail.type_url == "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
        }));
        let _ = peer.stop_and_read();

        let (peer, address) = GoPeer::start("typed");
        let client = TChannelHistoryClient::new(address);
        let execution = cadence_proto::api::WorkflowExecution {
            workflow_id: "workflow-id".into(),
            run_id: "run-id".into(),
        };
        let mutable_error = client
            .get_mutable_state(
                &cadence_proto::history::GetMutableStateRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution.clone()),
                    expected_next_event_id: 9,
                    current_branch_token: b"input-branch".to_vec(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert_eq!(mutable_error.code(), Code::NotFound);
        assert_eq!(mutable_error.message(), "mutable missing");
        let decision_error = client
            .record_decision_task_started(
                &cadence_proto::history::RecordDecisionTaskStartedRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution.clone()),
                    schedule_id: 51,
                    task_id: 52,
                    request_id: "decision-request".into(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert_eq!(decision_error.code(), Code::AlreadyExists);
        assert_eq!(decision_error.message(), "decision already started");
        let activity_error = client
            .record_activity_task_started(
                &cadence_proto::history::RecordActivityTaskStartedRequest {
                    domain_id: "domain-id".into(),
                    workflow_execution: Some(execution),
                    schedule_id: 61,
                    task_id: 62,
                    request_id: "activity-request".into(),
                    ..Default::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert_eq!(activity_error.code(), Code::NotFound);
        assert_eq!(activity_error.message(), "activity missing");
        let _ = peer.stop_and_read();

        // Exercise real generated Go decoders/encoders with non-UTF-8 strings.
        use cadence_common::go_string::GoString;
        use cadence_proto::native as n;
        let raw = |value: &[u8]| GoString::from(value);
        let (peer, address) = GoPeer::start("raw");
        let client = TChannelHistoryClient::new(address);
        let execution = n::api::WorkflowExecution {
            workflow_id: raw(b"workflow-\xfe"), run_id: raw(b"run-\xfd"),
        };
        let task_list = n::api::TaskList { name: raw(b"queue-\xfa"), ..Default::default() };
        client.refresh_workflow_tasks_native(
            &n::history::RefreshWorkflowTasksRequest {
                domain: raw(b"domain-name-\xf8"), domain_id: raw(b"domain-\xff"),
                workflow_execution: Some(execution.clone()),
            }, Duration::from_secs(3),
        ).await.unwrap();
        let described = client.describe_workflow_execution_native(
            &n::history::DescribeWorkflowExecutionRequest {
                domain_id: raw(b"domain-\xff"),
                request: Some(n::api::DescribeWorkflowExecutionRequest {
                    domain: raw(b"domain-name-\xf8"), workflow_execution: Some(execution.clone()),
                    ..Default::default()
                }),
            }, Duration::from_secs(3),
        ).await.unwrap();
        assert_eq!(described.pending_decision.unwrap().schedule_id, 41);
        let info = described.workflow_execution_info.unwrap();
        assert_eq!(info.workflow_execution.unwrap().workflow_id.as_bytes(), b"workflow-\xfe");
        assert_eq!(info.r#type.unwrap().name.as_bytes(), b"type-\xf7");
        let mutable = client.get_mutable_state_native(
            &n::history::GetMutableStateRequest {
                domain_id: raw(b"domain-\xff"), workflow_execution: Some(execution.clone()),
                expected_next_event_id: 9, current_branch_token: b"input-branch".to_vec(),
                ..Default::default()
            }, Duration::from_secs(3),
        ).await.unwrap();
        assert_eq!(mutable.workflow_execution.unwrap().run_id.as_bytes(), b"run-\xfd");
        assert_eq!(mutable.workflow_type.unwrap().name.as_bytes(), b"type-\xf7");
        assert_eq!(mutable.task_list.unwrap().name.as_bytes(), b"queue-\xfa");
        assert_eq!(mutable.sticky_task_list.unwrap().name.as_bytes(), b"sticky-\xf6");
        assert_eq!(mutable.client_library_version.as_bytes(), b"library-\xf5");
        assert_eq!(mutable.client_feature_version.as_bytes(), b"feature-\xf4");
        assert_eq!(mutable.client_impl.as_bytes(), b"impl-\xf3");
        assert_eq!(mutable.current_branch_token, [0xf2, 0]);
        assert_eq!(mutable.history_size, 42);
        let decision = client.record_decision_task_started_native(
            &n::history::RecordDecisionTaskStartedRequest {
                domain_id: raw(b"domain-\xff"), workflow_execution: Some(execution.clone()),
                schedule_id: 51, task_id: 52, request_id: raw(b"decision-\xfc"),
                poll_request: Some(n::api::PollForDecisionTaskRequest {
                    task_list: Some(task_list.clone()), identity: raw(b"worker-\xfb"), ..Default::default()
                }),
            }, Duration::from_secs(3),
        ).await.unwrap();
        assert_eq!(decision.workflow_type.unwrap().name.as_bytes(), b"type-\xff");
        let query = &decision.queries[&b"query-\xfe"[..]];
        assert_eq!(query.query_type.as_bytes(), b"query-type-\xfd");
        assert_eq!(query.query_args.as_ref().unwrap().data, [0xff, 0x00]);
        let activity = client.record_activity_task_started_native(
            &n::history::RecordActivityTaskStartedRequest {
                domain_id: raw(b"domain-\xff"), workflow_execution: Some(execution),
                schedule_id: 61, task_id: 62, request_id: raw(b"activity-\xfc"),
                poll_request: Some(n::api::PollForActivityTaskRequest {
                    task_list: Some(task_list), identity: raw(b"worker-\xfb"), ..Default::default()
                }),
            }, Duration::from_secs(3),
        ).await.unwrap();
        assert_eq!(activity.workflow_domain.as_bytes(), b"workflow-domain-\xff");
        assert_eq!(activity.heartbeat_details.unwrap().data, [0xfe, 0x00]);
        let Some(n::api::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes)) = activity.scheduled_event.unwrap().attributes else { panic!("missing activity attributes") };
        assert_eq!(attributes.activity_id.as_bytes(), b"activity-\xfd");
        assert_eq!(attributes.input.unwrap().data, [0xfc, 0x00]);
        let retry = attributes.retry_policy.unwrap();
        assert_eq!(retry.non_retryable_error_reasons[0].as_bytes(), b"reason-\xfb");
        assert_eq!(retry.initial_interval, Some(prost_types::Duration { seconds: 0, nanos: 0 }));
        assert_eq!(retry.maximum_interval, retry.initial_interval);
        assert_eq!(retry.expiration_interval, retry.initial_interval);
        assert_eq!(attributes.header.unwrap().fields[&b"header-\xfa"[..]].data, [0xf9, 0x00]);
        let calls = peer.stop_and_read();
        assert_eq!(calls.lines().filter(|line| !line.is_empty()).count(), 5);

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
    fn history_application_errors_keep_retry_and_redirect_details() {
        let ownership = encode_typed_struct(&TypedStruct {
            fields: vec![
                field(10, string("wrong shard owner")),
                field(20, string("history-b:7935")),
            ],
        });
        let ownership = decode_typed_struct(&ownership).unwrap();
        let status = application_error(
            "RecordActivityTaskStarted",
            &[
                (
                    "$rpc$-application-error-name".into(),
                    "ShardOwnershipLostError".into(),
                ),
                (
                    "$rpc$-application-error-details".into(),
                    "wrong shard owner".into(),
                ),
            ],
            &TypedStruct {
                fields: vec![field(5, TypedValue::Struct(ownership))],
            },
        );
        assert_eq!(status.code(), Code::Aborted);
        let status = RpcStatus::decode(status.details()).unwrap();
        let detail = status.details.first().unwrap();
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
        );
        assert_eq!(
            cadence_proto::shared::ShardOwnershipLostError::decode(detail.value.as_slice())
                .unwrap()
                .owner,
            "history-b:7935"
        );

        let status = application_error(
            "RecordActivityTaskStarted",
            &[
                (
                    "$rpc$-application-error-name".into(),
                    "EventAlreadyStartedError".into(),
                ),
                (
                    "$rpc$-application-error-details".into(),
                    "event already started".into(),
                ),
            ],
            &TypedStruct {
                fields: vec![field(
                    3,
                    struct_value(vec![field(1, string("event already started"))]),
                )],
            },
        );
        assert_eq!(status.code(), Code::AlreadyExists);
        let status = RpcStatus::decode(status.details()).unwrap();
        assert_eq!(
            status.details[0].type_url,
            "type.googleapis.com/uber.cadence.shared.v1.EventAlreadyStartedError"
        );
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
            previous_started_event_id: Some(17),
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
        let info = cadence_proto::api::WorkflowExecutionInfo {
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: "wf".into(),
                run_id: "run".into(),
            }),
            close_status: cadence_proto::api::WorkflowExecutionCloseStatus::Invalid as i32,
            history_length: 42,
            ..Default::default()
        };
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
