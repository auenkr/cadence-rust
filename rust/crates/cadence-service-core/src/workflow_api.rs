//! Production WorkflowService hook.
//!
//! `cadence-frontend` depends on the shared server runtime, so the runtime
//! cannot name the ported `handler_workflow::Handler`. The Frontend installs a
//! `WorkflowApi` implementation into `AppState`; the Thrift/TChannel and gRPC
//! transports then route every workflow operation through it instead of the
//! legacy in-process engine. The hook speaks Cadence API JSON (camelCase
//! `cadence_api` serde shapes) so each transport only converts its wire format.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use cadence_api::errors::CadenceError;
use cadence_thrift::service::find_method;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Request context a transport hands to the workflow handler (the Go request
/// context: caller headers and the inbound deadline).
#[derive(Clone, Debug, Default)]
pub struct CallerContext {
    pub deadline: Option<Instant>,
    /// Transport headers, lower-cased names.
    pub headers: HashMap<String, String>,
}

pub type ApiFuture<'a> =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, CadenceError>> + Send + 'a>>;

pub trait WorkflowApi: Send + Sync + std::fmt::Debug {
    /// True when the operation is served by this implementation.
    fn serves(&self, operation: &str) -> bool;
    fn call<'a>(
        &'a self,
        operation: &'a str,
        request: serde_json::Value,
        ctx: CallerContext,
    ) -> ApiFuture<'a>;
}

pub type SharedWorkflowApi = Arc<dyn WorkflowApi>;

type Decode = fn(&Struct) -> Result<serde_json::Value, String>;
type Encode = fn(serde_json::Value) -> Result<Struct, String>;

struct Codec {
    decode: Decode,
    encode: Encode,
}

fn decode_req<T: FromThrift + Serialize>(args: &Struct) -> Result<serde_json::Value, String> {
    match args.get(1) {
        Some(Value::Struct(s)) => {
            let typed = T::from_thrift(s).map_err(|e| e.to_string())?;
            serde_json::to_value(typed).map_err(|e| e.to_string())
        }
        _ => Err("missing args field 1".to_string()),
    }
}

fn decode_none(_: &Struct) -> Result<serde_json::Value, String> {
    Ok(serde_json::Value::Null)
}

fn encode_resp<T: DeserializeOwned + ToThrift>(v: serde_json::Value) -> Result<Struct, String> {
    let typed: T = serde_json::from_value(v).map_err(|e| e.to_string())?;
    Ok(Struct {
        fields: vec![(0, Value::Struct(typed.to_thrift()))],
    })
}

fn encode_void(_: serde_json::Value) -> Result<Struct, String> {
    Ok(Struct { fields: vec![] })
}

fn encode_cluster_info(v: serde_json::Value) -> Result<Struct, String> {
    let versions = v
        .get("supportedClientVersions")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let mut info = Struct { fields: vec![] };
    if !versions.is_null() {
        let typed: cadence_api::types::SupportedClientVersions =
            serde_json::from_value(versions).map_err(|e| e.to_string())?;
        info.fields.push((1, Value::Struct(typed.to_thrift())));
    }
    Ok(Struct {
        fields: vec![(0, Value::Struct(info))],
    })
}

macro_rules! op {
    ($req:ty, void) => {
        Codec {
            decode: decode_req::<$req>,
            encode: encode_void,
        }
    };
    ($req:ty, $resp:ty) => {
        Codec {
            decode: decode_req::<$req>,
            encode: encode_resp::<$resp>,
        }
    };
}

fn codec(method: &str) -> Option<Codec> {
    use cadence_api::service_types as st;
    use cadence_api::tasks as tk;
    use cadence_api::types as ty;
    Some(match method {
        "StartWorkflowExecution" => op!(
            ty::StartWorkflowExecutionRequest,
            ty::StartWorkflowExecutionResponse
        ),
        "StartWorkflowExecutionAsync" => op!(
            ty::StartWorkflowExecutionAsyncRequest,
            ty::StartWorkflowExecutionAsyncResponse
        ),
        "SignalWorkflowExecution" => op!(ty::SignalWorkflowExecutionRequest, void),
        "SignalWithStartWorkflowExecution" => op!(
            st::SignalWithStartWorkflowExecutionRequest,
            ty::StartWorkflowExecutionResponse
        ),
        "SignalWithStartWorkflowExecutionAsync" => op!(
            st::SignalWithStartWorkflowExecutionAsyncRequest,
            st::SignalWithStartWorkflowExecutionAsyncResponse
        ),
        "TerminateWorkflowExecution" => op!(ty::TerminateWorkflowExecutionRequest, void),
        "RequestCancelWorkflowExecution" => op!(st::RequestCancelWorkflowExecutionRequest, void),
        "ResetWorkflowExecution" => op!(
            ty::ResetWorkflowExecutionRequest,
            ty::ResetWorkflowExecutionResponse
        ),
        "RestartWorkflowExecution" => op!(
            ty::RestartWorkflowExecutionRequest,
            ty::RestartWorkflowExecutionResponse
        ),
        "DiagnoseWorkflowExecution" => op!(
            ty::DiagnoseWorkflowExecutionRequest,
            ty::DiagnoseWorkflowExecutionResponse
        ),
        "DescribeWorkflowExecution" => op!(
            st::DescribeWorkflowExecutionRequest,
            st::DescribeWorkflowExecutionResponse
        ),
        "QueryWorkflow" => op!(tk::QueryWorkflowRequest, tk::QueryWorkflowResponse),
        "ResetStickyTaskList" => op!(
            st::ResetStickyTaskListRequest,
            st::ResetStickyTaskListResponse
        ),
        "RefreshWorkflowTasks" => op!(ty::RefreshWorkflowTasksRequest, void),
        "GetWorkflowExecutionHistory" => op!(
            st::GetWorkflowExecutionHistoryRequest,
            st::GetWorkflowExecutionHistoryResponse
        ),
        "PollForDecisionTask" => op!(
            tk::PollForDecisionTaskRequest,
            tk::PollForDecisionTaskResponse
        ),
        "PollForActivityTask" => op!(
            tk::PollForActivityTaskRequest,
            tk::PollForActivityTaskResponse
        ),
        "RespondDecisionTaskCompleted" => op!(
            tk::RespondDecisionTaskCompletedRequest,
            tk::RespondDecisionTaskCompletedResponse
        ),
        "RespondDecisionTaskFailed" => op!(tk::RespondDecisionTaskFailedRequest, void),
        "RespondActivityTaskCompleted" => op!(tk::RespondActivityTaskCompletedRequest, void),
        "RespondActivityTaskFailed" => op!(tk::RespondActivityTaskFailedRequest, void),
        "RespondActivityTaskCanceled" => op!(tk::RespondActivityTaskCanceledRequest, void),
        "RecordActivityTaskHeartbeat" => op!(
            tk::RecordActivityTaskHeartbeatRequest,
            tk::RecordActivityTaskHeartbeatResponse
        ),
        "RespondActivityTaskCompletedByID" => {
            op!(tk::RespondActivityTaskCompletedByIDRequest, void)
        }
        "RespondActivityTaskFailedByID" => op!(tk::RespondActivityTaskFailedByIDRequest, void),
        "RespondActivityTaskCanceledByID" => {
            op!(tk::RespondActivityTaskCanceledByIDRequest, void)
        }
        "RecordActivityTaskHeartbeatByID" => op!(
            tk::RecordActivityTaskHeartbeatByIDRequest,
            tk::RecordActivityTaskHeartbeatResponse
        ),
        "RespondQueryTaskCompleted" => op!(tk::RespondQueryTaskCompletedRequest, void),
        "ListOpenWorkflowExecutions" => op!(
            st::ListOpenWorkflowExecutionsRequest,
            st::ListOpenWorkflowExecutionsResponse
        ),
        "ListClosedWorkflowExecutions" => op!(
            st::ListClosedWorkflowExecutionsRequest,
            st::ListClosedWorkflowExecutionsResponse
        ),
        "ListWorkflowExecutions" | "ScanWorkflowExecutions" => op!(
            st::ListWorkflowExecutionsRequest,
            st::ListWorkflowExecutionsResponse
        ),
        "ListArchivedWorkflowExecutions" => op!(
            st::ListArchivedWorkflowExecutionsRequest,
            st::ListArchivedWorkflowExecutionsResponse
        ),
        "CountWorkflowExecutions" => op!(
            st::CountWorkflowExecutionsRequest,
            st::CountWorkflowExecutionsResponse
        ),
        "DescribeTaskList" => op!(tk::DescribeTaskListRequest, tk::DescribeTaskListResponse),
        "GetTaskListsByDomain" => op!(
            tk::GetTaskListsByDomainRequest,
            tk::GetTaskListsByDomainResponse
        ),
        "ListTaskListPartitions" => op!(
            tk::ListTaskListPartitionsRequest,
            tk::ListTaskListPartitionsResponse
        ),
        "GetSearchAttributes" => Codec {
            decode: decode_none,
            encode: encode_resp::<tk::GetSearchAttributesResponse>,
        },
        "GetClusterInfo" => Codec {
            decode: decode_none,
            encode: encode_cluster_info,
        },
        _ => return None,
    })
}

/// True when `method` has a Thrift codec in this table.
pub fn thrift_supported(method: &str) -> bool {
    codec(method).is_some()
}

/// Convert a Cadence error into its Thrift exception name, value and message.
pub fn error_to_thrift(error: &CadenceError) -> (&'static str, Struct, String) {
    fn msg(message: &str) -> Struct {
        Struct {
            fields: vec![(1, Value::Bytes(message.as_bytes().to_vec()))],
        }
    }
    match error {
        CadenceError::BadRequest(x) => ("BadRequestError", x.to_thrift(), x.message.clone()),
        CadenceError::Internal(x) => ("InternalServiceError", x.to_thrift(), x.message.clone()),
        CadenceError::EntityNotExists(x) => {
            ("EntityNotExistsError", x.to_thrift(), x.message.clone())
        }
        CadenceError::DomainNotActive(x) => {
            ("DomainNotActiveError", x.to_thrift(), x.message.clone())
        }
        CadenceError::WorkflowAlreadyStarted(x) => (
            "WorkflowExecutionAlreadyStartedError",
            x.to_thrift(),
            x.message.clone(),
        ),
        CadenceError::WorkflowAlreadyCompleted(x) => (
            "WorkflowExecutionAlreadyCompletedError",
            x.to_thrift(),
            x.message.clone(),
        ),
        CadenceError::DomainAlreadyExists(x) => {
            ("DomainAlreadyExistsError", x.to_thrift(), x.message.clone())
        }
        CadenceError::CancellationAlreadyRequested(x) => (
            "CancellationAlreadyRequestedError",
            x.to_thrift(),
            x.message.clone(),
        ),
        CadenceError::QueryFailed(x) => ("QueryFailedError", x.to_thrift(), x.message.clone()),
        CadenceError::ServiceBusy(x) => ("ServiceBusyError", x.to_thrift(), x.message.clone()),
        CadenceError::LimitExceeded(x) => ("LimitExceededError", x.to_thrift(), x.message.clone()),
        CadenceError::ShardOwnershipLost(x) => {
            ("ShardOwnershipLostError", x.to_thrift(), x.message.clone())
        }
        CadenceError::RetryTaskV2(x) => ("RetryTaskV2Error", x.to_thrift(), x.message.clone()),
        CadenceError::AccessDenied(x) => ("AccessDeniedError", x.to_thrift(), x.message.clone()),
        CadenceError::ClientVersionNotSupported(x) => (
            "ClientVersionNotSupportedError",
            x.to_thrift(),
            x.to_string(),
        ),
        CadenceError::Unknown(m) => ("InternalServiceError", msg(m), m.clone()),
    }
}

/// Thrift reply: `(result struct, application error (name, details))`.
pub type ThriftReply = (Struct, Option<(&'static str, String)>);

/// Serve one WorkflowService Thrift call through the installed hook. Returns
/// `Ok(None)` when the hook does not serve the method.
pub async fn dispatch_thrift(
    api: &SharedWorkflowApi,
    method: &str,
    args: &Struct,
    ctx: CallerContext,
) -> Result<Option<ThriftReply>, String> {
    if !api.serves(method) {
        return Ok(None);
    }
    let Some(codec) = codec(method) else {
        return Ok(None);
    };
    let def = find_method(method).ok_or_else(|| format!("unknown method {method}"))?;
    let request = (codec.decode)(args)?;
    match api.call(method, request, ctx).await {
        Ok(response) => Ok(Some(((codec.encode)(response)?, None))),
        Err(error) => {
            let (mut name, mut value, message) = error_to_thrift(&error);
            let mut field = cadence_thrift::service::exception_field(def, name);
            if field.is_none() {
                // Undeclared exceptions are reported as InternalServiceError
                // when declared, matching Go's ordinary-error fallthrough.
                if let Some(id) =
                    cadence_thrift::service::exception_field(def, "InternalServiceError")
                {
                    name = "InternalServiceError";
                    value = Struct {
                        fields: vec![(1, Value::Bytes(message.as_bytes().to_vec()))],
                    };
                    field = Some(id);
                }
            }
            let field = field.ok_or_else(|| format!("error {name} not declared for {method}"))?;
            Ok(Some((
                Struct {
                    fields: vec![(field, Value::Struct(value))],
                },
                Some((name, message)),
            )))
        }
    }
}
