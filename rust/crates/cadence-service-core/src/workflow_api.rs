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

/// Failure of the Admin API access-control wrapper (Go
/// `accesscontrolled.adminHandler`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAuthError {
    /// Go `errUnauthorized` (`AccessDeniedError`, "Request unauthorized.").
    Denied(String),
    /// Authorizer returned an error; Go returns it unchanged.
    Failed(String),
}

/// Seam so the Admin dispatch in this crate can be wrapped by the Frontend's
/// authorizer (the Frontend crate depends on this one, not the reverse).
pub trait AdminAuthorizer: Send + Sync {
    /// `api` is the Admin method name (e.g. `GetDynamicConfig`); `token` the
    /// `cadence-authorization` header value.
    fn authorize_admin(&self, api: &str, token: &str) -> Result<(), AdminAuthError>;
    /// Go `accesscontrolled.apiHandler` check for the domain APIs served by
    /// the legacy handlers (`DescribeDomain`, `RegisterDomain`, ...).
    /// `domain` is the Go `DomainName` attribute ("" when Go sets none).
    fn authorize_domain_api(
        &self,
        _api: &str,
        _domain: &str,
        _token: &str,
    ) -> Result<(), AdminAuthError> {
        Ok(())
    }
}

pub trait WorkflowApi: Send + Sync + std::fmt::Debug {
    /// Admin access control installed by the Frontend; `None` allows all.
    fn admin_authorizer(&self) -> Option<&dyn AdminAuthorizer> {
        None
    }
    /// True when the operation is served by this implementation.
    fn serves(&self, operation: &str) -> bool;
    fn call<'a>(
        &'a self,
        operation: &'a str,
        request: serde_json::Value,
        ctx: CallerContext,
    ) -> ApiFuture<'a>;
    /// Go cluster-redirection wrapper for operations this implementation does
    /// not serve (schedule APIs): `Some(response)` when the call was forwarded
    /// to a remote cluster, `None` to serve it locally.
    fn redirect<'a>(
        &'a self,
        operation: &'a str,
        request: &'a serde_json::Value,
        ctx: &'a CallerContext,
    ) -> RedirectFuture<'a> {
        let _ = (operation, request, ctx);
        Box::pin(async { Ok(None) })
    }
}

pub type RedirectFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Option<serde_json::Value>, CadenceError>>
            + Send
            + 'a,
    >,
>;

/// Operations served by the core schedule handlers that Go's
/// `clusterRedirectionHandler` still wraps with `Redirect`.
pub const REDIRECTED_SCHEDULE_OPERATIONS: &[&str] = &[
    "BackfillSchedule",
    "CreateSchedule",
    "DeleteSchedule",
    "DescribeSchedule",
    "ListSchedules",
    "PauseSchedule",
    "UnpauseSchedule",
    "UpdateSchedule",
];

pub type SharedWorkflowApi = Arc<dyn WorkflowApi>;

type Decode = fn(&Struct) -> Result<serde_json::Value, String>;
type Encode = fn(serde_json::Value) -> Result<Struct, String>;

type ClientEncode = fn(serde_json::Value) -> Result<Struct, String>;
type ClientDecode = fn(&Struct) -> Result<serde_json::Value, String>;

struct Codec {
    decode: Decode,
    encode: Encode,
    /// Client direction (remote Frontend calls): request JSON -> Thrift args,
    /// Thrift result struct -> response JSON.
    client_encode: ClientEncode,
    client_decode: ClientDecode,
}

fn client_encode_args<T: DeserializeOwned + ToThrift>(
    request: serde_json::Value,
) -> Result<Struct, String> {
    let typed: T = serde_json::from_value(request).map_err(|e| e.to_string())?;
    Ok(Struct {
        fields: vec![(1, Value::Struct(typed.to_thrift()))],
    })
}

fn client_encode_none(_: serde_json::Value) -> Result<Struct, String> {
    Ok(Struct { fields: vec![] })
}

fn client_decode_result<T: FromThrift + Serialize>(
    result: &Struct,
) -> Result<serde_json::Value, String> {
    match result.get(0) {
        Some(Value::Struct(s)) => {
            let typed = T::from_thrift(s).map_err(|e| e.to_string())?;
            serde_json::to_value(typed).map_err(|e| e.to_string())
        }
        _ => Err("missing result field 0".to_string()),
    }
}

fn client_decode_void(_: &Struct) -> Result<serde_json::Value, String> {
    Ok(serde_json::Value::Null)
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
            client_encode: client_encode_args::<$req>,
            client_decode: client_decode_void,
        }
    };
    ($req:ty, $resp:ty) => {
        Codec {
            decode: decode_req::<$req>,
            encode: encode_resp::<$resp>,
            client_encode: client_encode_args::<$req>,
            client_decode: client_decode_result::<$resp>,
        }
    };
}

fn codec(method: &str) -> Option<Codec> {
    use cadence_api::service_types as st;
    use cadence_api::tasks as tk;
    use cadence_api::types as ty;
    use cadence_api::schedule as sc;
    Some(match method {
        "CreateSchedule" => op!(sc::CreateScheduleRequest, sc::CreateScheduleResponse),
        "DescribeSchedule" => op!(sc::DescribeScheduleRequest, sc::DescribeScheduleResponse),
        "ListSchedules" => op!(sc::ListSchedulesRequest, sc::ListSchedulesResponse),
        "DeleteSchedule" => op!(sc::DeleteScheduleRequest, sc::DeleteScheduleResponse),
        "PauseSchedule" => op!(sc::PauseScheduleRequest, sc::PauseScheduleResponse),
        "UnpauseSchedule" => op!(sc::UnpauseScheduleRequest, sc::UnpauseScheduleResponse),
        "BackfillSchedule" => op!(sc::BackfillScheduleRequest, sc::BackfillScheduleResponse),
        "UpdateSchedule" => op!(sc::UpdateScheduleRequest, sc::UpdateScheduleResponse),
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
            client_encode: client_encode_none,
            client_decode: client_decode_result::<tk::GetSearchAttributesResponse>,
        },
        "GetClusterInfo" => Codec {
            decode: decode_none,
            encode: encode_cluster_info,
            client_encode: client_encode_none,
            client_decode: client_decode_void,
        },
        _ => return None,
    })
}

/// Encode a WorkflowService request as the strict Thrift args struct bytes
/// used for a TChannel call to a remote Frontend.
pub fn encode_client_request(method: &str, request: serde_json::Value) -> Result<Vec<u8>, String> {
    let codec = codec(method).ok_or_else(|| format!("unsupported remote method {method}"))?;
    let args = (codec.client_encode)(request)?;
    let mut writer = cadence_thrift::protocol::Writer::new();
    cadence_thrift::value::write_struct(&mut writer, &args);
    Ok(writer.into_bytes())
}

/// Decode a remote Frontend Thrift result. Declared exceptions are mapped back
/// to the matching typed `CadenceError`.
pub fn decode_client_response(
    method: &str,
    application_error: bool,
    body: &[u8],
) -> Result<serde_json::Value, CadenceError> {
    let codec = codec(method)
        .ok_or_else(|| CadenceError::Unknown(format!("unsupported method {method}")))?;
    let mut reader = cadence_thrift::protocol::Reader::new(body);
    let result = cadence_thrift::value::read_struct(&mut reader)
        .map_err(|e| CadenceError::Unknown(format!("invalid remote Thrift result: {e}")))?;
    if !application_error {
        return (codec.client_decode)(&result).map_err(CadenceError::Unknown);
    }
    let def = find_method(&format!("WorkflowService::{method}"));
    for (field, value) in &result.fields {
        if let Value::Struct(exception) = value {
            let name = def
                .and_then(|d| d.exceptions.iter().find(|(_, id)| id == field))
                .map_or("", |(name, _)| *name);
            return Err(exception_from_thrift(name, exception));
        }
    }
    Err(CadenceError::Unknown(
        "remote application error without exception".to_string(),
    ))
}

/// Typed error from a Thrift exception struct (inverse of `error_to_thrift`).
pub fn exception_from_thrift(name: &str, exception: &Struct) -> CadenceError {
    use cadence_api::errors as e;
    let message = exception.opt_string(1).ok().flatten().unwrap_or_default();
    let unknown = || CadenceError::Unknown(format!("{name}: {message}"));
    let from_message = |ctor: fn(serde_json::Value) -> Option<CadenceError>| {
        ctor(serde_json::json!({ "message": message })).unwrap_or_else(unknown)
    };
    match name {
        "DomainNotActiveError" => e::DomainNotActiveError::from_thrift(exception)
            .map_or_else(|_| unknown(), CadenceError::DomainNotActive),
        "EntityNotExistsError" => e::EntityNotExistsError::from_thrift(exception)
            .map_or_else(|_| unknown(), CadenceError::EntityNotExists),
        "ServiceBusyError" => e::ServiceBusyError::from_thrift(exception)
            .map_or_else(|_| unknown(), CadenceError::ServiceBusy),
        "ClientVersionNotSupportedError" => {
            e::ClientVersionNotSupportedError::from_thrift(exception)
                .map_or_else(|_| unknown(), CadenceError::ClientVersionNotSupported)
        }
        "WorkflowExecutionAlreadyStartedError" => {
            e::WorkflowExecutionAlreadyStartedError::from_thrift(exception)
                .map_or_else(|_| unknown(), CadenceError::WorkflowAlreadyStarted)
        }
        "BadRequestError" => CadenceError::BadRequest(e::BadRequestError::new(message.clone())),
        "InternalServiceError" => {
            CadenceError::Internal(e::InternalServiceError::new(message.clone()))
        }
        "AccessDeniedError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::AccessDenied)
        }),
        "WorkflowExecutionAlreadyCompletedError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::WorkflowAlreadyCompleted)
        }),
        "DomainAlreadyExistsError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::DomainAlreadyExists)
        }),
        "CancellationAlreadyRequestedError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::CancellationAlreadyRequested)
        }),
        "QueryFailedError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::QueryFailed)
        }),
        "LimitExceededError" => from_message(|v| {
            serde_json::from_value(v)
                .ok()
                .map(CadenceError::LimitExceeded)
        }),
        _ => unknown(),
    }
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
    let name = method.rsplit("::").next().unwrap_or(method);
    let redirected = !method.starts_with("AdminService::")
        && REDIRECTED_SCHEDULE_OPERATIONS.contains(&name)
        && !api.serves(method);
    if !redirected && !api.serves(method) {
        return Ok(None);
    }
    let Some(codec) = codec(method) else {
        return Ok(None);
    };
    let def = find_method(method).ok_or_else(|| format!("unknown method {method}"))?;
    let request = (codec.decode)(args)?;
    let result = if redirected {
        match api.redirect(name, &request, &ctx).await {
            Ok(None) => return Ok(None),
            Ok(Some(response)) => Ok(response),
            Err(error) => Err(error),
        }
    } else {
        api.call(method, request, ctx).await
    };
    match result {
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
            let field = field
                .ok_or_else(|| format!("error {name} ({message}) not declared for {method}"))?;
            Ok(Some((
                Struct {
                    fields: vec![(field, Value::Struct(value))],
                },
                Some((name, message)),
            )))
        }
    }
}
