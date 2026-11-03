//! Production `WorkflowBackend`: Go Frontend's outbound History/Matching calls.
//!
//! The Handler (Go `WorkflowHandler`) builds the internal request envelopes
//! (`domainUUID`, `startRequest`, ...) as JSON. This module encodes them into
//! the History/Matching Thrift envelopes (field IDs from
//! `go/idls/thrift/history.thrift`) and sends them over YARPC/TChannel, which
//! is how Go Frontend reaches History and Matching with `EnableGRPCOutbound`
//! off. Replies are decoded with the shared `FromThrift` converters.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cadence_api::errors::{
    AccessDeniedError, BadRequestError, CadenceError, CancellationAlreadyRequestedError,
    ClientVersionNotSupportedError, DomainNotActiveError, EntityNotExistsError,
    InternalServiceError, LimitExceededError, QueryFailedError, RetryTaskV2Error, ServiceBusyError,
    ShardOwnershipLostError, WorkflowExecutionAlreadyCompletedError,
    WorkflowExecutionAlreadyStartedError,
};
use cadence_api::{service_types as st, tasks as tk, types as ty};
use cadence_service_core::tchannel_json_client::{call_thrift, JsonCallError};
use cadence_thrift::protocol::{Reader, Writer};
use cadence_thrift::value::{read_struct, write_struct, FromThrift, Struct, ToThrift, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::handler_workflow::{WorkflowBackend, WorkflowOperation};
use crate::workflow_visibility::EsOp;

const HISTORY_SERVICE: &str = "cadence-history";
const CALLER: &str = "cadence-frontend";
/// Go's frontend client default timeout for History calls.
const HISTORY_TIMEOUT: Duration = Duration::from_secs(30);
const HISTORY_RETRY_BUDGET: Duration = Duration::from_secs(25);
const HISTORY_RETRY_BACKOFF_UNIT: Duration = Duration::from_millis(250);
const HISTORY_RETRY_MAX_ATTEMPTS: u32 = 20;

#[derive(Clone)]
pub struct ProductionBackend {
    history_addr: String,
    matching_addr: String,
    membership: cadence_service_core::admin::MembershipSlot,
    num_history_shards: i32,
    pub(crate) history_access: Option<crate::workflow_matching::HistoryAccess>,
    pub(crate) domains: Option<std::sync::Arc<dyn crate::handler_workflow::WorkflowDomainLookup>>,
    pub(crate) visibility:
        Option<std::sync::Arc<dyn cadence_persistence::visibility_reader::VisibilityStore>>,
    pub(crate) es: Option<crate::workflow_visibility::EsBinding>,
}

impl std::fmt::Debug for ProductionBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProductionBackend")
            .field("history_addr", &self.history_addr)
            .field("matching_addr", &self.matching_addr)
            .finish_non_exhaustive()
    }
}

type BackendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, CadenceError>> + Send + 'a>>;

pub(crate) fn bad_request(message: impl Into<String>) -> CadenceError {
    CadenceError::BadRequest(BadRequestError::new(message))
}

pub(crate) fn internal(message: impl Into<String>) -> CadenceError {
    CadenceError::Internal(InternalServiceError::new(message))
}

pub(crate) fn domain_uuid(envelope: &serde_json::Value, key: &str) -> Result<Value, CadenceError> {
    let id = envelope
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    Ok(Value::Bytes(id.as_bytes().to_vec()))
}

pub(crate) fn typed<T: DeserializeOwned>(
    envelope: &serde_json::Value,
    key: &str,
) -> Result<T, CadenceError> {
    let inner = envelope
        .get(key)
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    serde_json::from_value(inner).map_err(|e| bad_request(format!("invalid {key}: {e}")))
}

pub(crate) fn to_json<T: Serialize>(value: &T) -> Result<serde_json::Value, CadenceError> {
    serde_json::to_value(value).map_err(|e| internal(e.to_string()))
}

fn message_of(exception: &Struct) -> String {
    match exception.get(1) {
        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    }
}

/// Map a History/Matching Thrift exception to the public Cadence error, the
/// way Go's thrift client adapters rebuild `types.*Error` values.
fn exception_to_error(name: &str, exception: &Struct, details: &str) -> CadenceError {
    let mut message = message_of(exception);
    if message.is_empty() {
        message = details.to_owned();
    }
    match name {
        "BadRequestError" => CadenceError::BadRequest(BadRequestError::new(message)),
        "EntityNotExistsError" => match EntityNotExistsError::from_thrift(exception) {
            Ok(e) => CadenceError::EntityNotExists(e),
            Err(_) => CadenceError::EntityNotExists(EntityNotExistsError::new(message)),
        },
        "WorkflowExecutionAlreadyCompletedError" => CadenceError::WorkflowAlreadyCompleted(
            WorkflowExecutionAlreadyCompletedError::new(message),
        ),
        "WorkflowExecutionAlreadyStartedError" => {
            match WorkflowExecutionAlreadyStartedError::from_thrift(exception) {
                Ok(e) => CadenceError::WorkflowAlreadyStarted(e),
                Err(_) => {
                    CadenceError::WorkflowAlreadyStarted(WorkflowExecutionAlreadyStartedError {
                        message,
                        ..Default::default()
                    })
                }
            }
        }
        "DomainNotActiveError" => match DomainNotActiveError::from_thrift(exception) {
            Ok(e) => CadenceError::DomainNotActive(e),
            Err(_) => CadenceError::DomainNotActive(DomainNotActiveError {
                message,
                ..Default::default()
            }),
        },
        "ServiceBusyError" => match ServiceBusyError::from_thrift(exception) {
            Ok(e) => CadenceError::ServiceBusy(e),
            Err(_) => CadenceError::ServiceBusy(ServiceBusyError::new(message)),
        },
        "LimitExceededError" => CadenceError::LimitExceeded(LimitExceededError::new(message)),
        "QueryFailedError" => CadenceError::QueryFailed(QueryFailedError::new(message)),
        "CancellationAlreadyRequestedError" => CadenceError::CancellationAlreadyRequested(
            CancellationAlreadyRequestedError::new(message),
        ),
        "AccessDeniedError" => CadenceError::AccessDenied(AccessDeniedError::new(message)),
        "ClientVersionNotSupportedError" => {
            match ClientVersionNotSupportedError::from_thrift(exception) {
                Ok(e) => CadenceError::ClientVersionNotSupported(e),
                Err(_) => CadenceError::ClientVersionNotSupported(Default::default()),
            }
        }
        "RetryTaskV2Error" => match RetryTaskV2Error::from_thrift(exception) {
            Ok(e) => CadenceError::RetryTaskV2(e),
            Err(_) => internal(message),
        },
        // History reports these with the message in field 10.
        "ShardOwnershipLostError" => CadenceError::ShardOwnershipLost(
            ShardOwnershipLostError::new(match exception.get(10) {
                Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
                _ => message,
            }),
        ),
        _ => internal(message),
    }
}

/// Decode only the routing selector from the already validated History envelope.
/// Field IDs mirror history.thrift and shared.thrift; task tokens use Go JSON.
fn history_workflow_id(method: &str, envelope: &Struct) -> Result<String, CadenceError> {
    let Some(Value::Struct(request)) = envelope.get(20) else {
        return Err(bad_request("History request is missing"));
    };
    let bytes = if method.starts_with("Respond") || method == "RecordActivityTaskHeartbeat" {
        let Some(Value::Bytes(token)) = request.get(10) else {
            return Err(bad_request("Invalid TaskToken."));
        };
        let token: serde_json::Value =
            serde_json::from_slice(token).map_err(|_| bad_request("Invalid TaskToken."))?;
        return token
            .get("workflowId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| bad_request("Invalid TaskToken."));
    } else if method == "StartWorkflowExecution" || method == "SignalWithStartWorkflowExecution" {
        request.get(20)
    } else if method == "ResetStickyTaskList" {
        request.get(10)
    } else {
        match request.get(20) {
            Some(Value::Struct(execution)) => execution.get(10),
            _ => None,
        }
    };
    match bytes {
        Some(Value::Bytes(bytes)) => {
            String::from_utf8(bytes.clone()).map_err(|_| bad_request("Invalid WorkflowId."))
        }
        _ => Err(bad_request("WorkflowId is not set on request.")),
    }
}

impl ProductionBackend {
    pub fn new(history_addr: String, matching_addr: String) -> Self {
        Self {
            history_addr,
            matching_addr,
            membership: Default::default(),
            num_history_shards: 4,
            history_access: None,
            visibility: None,
            es: None,
            domains: None,
        }
    }

    pub fn with_history_access(mut self, access: crate::workflow_matching::HistoryAccess) -> Self {
        self.history_access = Some(access);
        self
    }

    pub fn with_domains(
        mut self,
        domains: std::sync::Arc<dyn crate::handler_workflow::WorkflowDomainLookup>,
    ) -> Self {
        self.domains = Some(domains);
        self
    }

    pub fn with_history_routing(
        mut self,
        membership: cadence_service_core::admin::MembershipSlot,
        num_history_shards: i32,
    ) -> Self {
        self.membership = membership;
        self.num_history_shards = num_history_shards.max(1);
        self
    }

    pub(crate) fn history_addr_for_workflow(
        &self,
        workflow_id: &str,
    ) -> Result<String, CadenceError> {
        let shard = crate::workflow_matching::workflow_id_to_history_shard(
            workflow_id,
            self.num_history_shards,
        );
        let owner = self
            .membership
            .get()
            .map(|resolver| resolver.history_owner(shard))
            .transpose()
            .map_err(internal)?
            .flatten();
        Ok(owner
            .map(|host| host.address)
            .unwrap_or_else(|| self.history_addr.clone()))
    }

    pub(crate) fn matching_addr(&self) -> &str {
        &self.matching_addr
    }

    /// One History call: `envelope` is the single IDL argument. Returns the
    /// success struct (empty for `void` methods).
    pub(crate) async fn history(
        &self,
        method: &str,
        envelope: Struct,
    ) -> Result<Struct, CadenceError> {
        let workflow_id = history_workflow_id(method, &envelope)?;
        retry_history_call(
            || self.history_addr_for_workflow(&workflow_id),
            |address| {
                let envelope = envelope.clone();
                async move {
                    self.call(
                        &address,
                        HISTORY_SERVICE,
                        "HistoryService",
                        method,
                        envelope,
                    )
                    .await
                }
            },
            HISTORY_RETRY_BACKOFF_UNIT,
            HISTORY_RETRY_BUDGET,
        )
        .await
    }

    async fn call(
        &self,
        addr: &str,
        service: &str,
        thrift_service: &str,
        method: &str,
        envelope: Struct,
    ) -> Result<Struct, CadenceError> {
        self.call_with_timeout(
            addr,
            service,
            thrift_service,
            method,
            envelope,
            HISTORY_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn call_with_timeout(
        &self,
        addr: &str,
        service: &str,
        thrift_service: &str,
        method: &str,
        envelope: Struct,
        timeout: Duration,
    ) -> Result<Struct, CadenceError> {
        let args = Struct {
            fields: vec![(1, Value::Struct(envelope))],
        };
        let mut writer = Writer::new();
        write_struct(&mut writer, &args);
        let procedure = format!("{thrift_service}::{method}");
        let __t0 = std::time::Instant::now();
        eprintln!("BACKEND-TRACE begin {addr} {procedure}");
        let response = call_thrift(
            addr,
            service,
            &procedure,
            CALLER,
            &[],
            &writer.into_bytes(),
            timeout,
        )
        .await;
        eprintln!(
            "BACKEND-TRACE end {procedure} {}ms {}",
            __t0.elapsed().as_millis(),
            match &response {
                Ok(r) => format!("ok app_err={}", r.application_error),
                Err(e) => format!("err {e}"),
            }
        );
        let response = response.map_err(|error| match error {
            JsonCallError::Application(message) => {
                internal(format!("{service} {method}: {message}"))
            }
            JsonCallError::Transport(error) => {
                if error.to_string().contains("deadline exceeded") {
                    internal(format!("context deadline exceeded: {error}"))
                } else {
                    CadenceError::ServiceBusy(ServiceBusyError::new(format!(
                        "{service} unavailable: {error}"
                    )))
                }
            }
        })?;
        let result = read_struct(&mut Reader::new(&response.body))
            .map_err(|e| internal(format!("invalid {service} Thrift result: {e}")))?;
        if response.application_error {
            let header = |name: &str| {
                response
                    .headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.as_str())
                    .unwrap_or_default()
            };
            let name = header("$rpc$-application-error-name");
            let details = header("$rpc$-application-error-details");
            let exception = match result.fields.first() {
                Some((_, Value::Struct(s))) => s.clone(),
                _ => Struct::default(),
            };
            return Err(exception_to_error(name, &exception, details));
        }
        Ok(match result.get(0) {
            Some(Value::Struct(s)) => s.clone(),
            _ => Struct::default(),
        })
    }

    async fn history_void(
        &self,
        method: &str,
        envelope: Struct,
    ) -> Result<serde_json::Value, CadenceError> {
        self.history(method, envelope).await?;
        Ok(serde_json::json!({}))
    }

    async fn history_typed<R: FromThrift + Serialize>(
        &self,
        method: &str,
        envelope: Struct,
    ) -> Result<serde_json::Value, CadenceError> {
        let reply = self.history(method, envelope).await?;
        let response = R::from_thrift(&reply)
            .map_err(|e| internal(format!("invalid {method} response: {e}")))?;
        to_json(&response)
    }

    async fn dispatch(
        &self,
        operation: WorkflowOperation,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        use WorkflowOperation as Op;
        // Envelope field 10 is `domainUUID`; field 20 is the public request.
        let env = |inner: Struct, extras: Vec<(i16, Value)>| -> Result<Struct, CadenceError> {
            let mut fields = vec![
                (10, domain_uuid(&request, "domainUUID")?),
                (20, Value::Struct(inner)),
            ];
            fields.extend(extras);
            Ok(Struct { fields })
        };
        match operation {
            Op::StartWorkflowExecution => {
                let start: ty::StartWorkflowExecutionRequest = typed(&request, "startRequest")?;
                let extras = start_extras(&start)?;
                let envelope = env(start.to_thrift(), extras)?;
                self.history_typed::<ty::StartWorkflowExecutionResponse>(
                    "StartWorkflowExecution",
                    envelope,
                )
                .await
            }
            Op::SignalWithStartWorkflowExecution => {
                let inner: st::SignalWithStartWorkflowExecutionRequest =
                    typed(&request, "signalWithStartRequest")?;
                let envelope = env(inner.to_thrift(), vec![])?;
                self.history_typed::<ty::StartWorkflowExecutionResponse>(
                    "SignalWithStartWorkflowExecution",
                    envelope,
                )
                .await
            }
            Op::SignalWorkflowExecution => {
                let inner: ty::SignalWorkflowExecutionRequest = typed(&request, "signalRequest")?;
                self.history_void("SignalWorkflowExecution", env(inner.to_thrift(), vec![])?)
                    .await
            }
            Op::TerminateWorkflowExecution => {
                let inner: ty::TerminateWorkflowExecutionRequest =
                    typed(&request, "terminateRequest")?;
                self.history_void(
                    "TerminateWorkflowExecution",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::RequestCancelWorkflowExecution => {
                let inner: st::RequestCancelWorkflowExecutionRequest =
                    typed(&request, "cancelRequest")?;
                self.history_void(
                    "RequestCancelWorkflowExecution",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::ResetWorkflowExecution => {
                let inner: ty::ResetWorkflowExecutionRequest = typed(&request, "resetRequest")?;
                self.history_typed::<ty::ResetWorkflowExecutionResponse>(
                    "ResetWorkflowExecution",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::DescribeWorkflowExecution => {
                let inner: st::DescribeWorkflowExecutionRequest = typed(&request, "request")?;
                self.history_typed::<st::DescribeWorkflowExecutionResponse>(
                    "DescribeWorkflowExecution",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::QueryWorkflow => {
                let inner: tk::QueryWorkflowRequest = typed(&request, "request")?;
                let reply = self
                    .history("QueryWorkflow", env(inner.to_thrift(), vec![])?)
                    .await?;
                // History's QueryWorkflowResponse wraps the public one at field 10.
                match reply.get(10) {
                    Some(Value::Struct(s)) => {
                        let response = tk::QueryWorkflowResponse::from_thrift(s).map_err(|e| {
                            internal(format!("invalid QueryWorkflow response: {e}"))
                        })?;
                        to_json(&response)
                    }
                    _ => Ok(serde_json::json!({})),
                }
            }
            Op::ResetStickyTaskList => {
                let execution: ty::WorkflowExecution = typed(&request, "execution")?;
                self.history_typed::<st::ResetStickyTaskListResponse>(
                    "ResetStickyTaskList",
                    env(execution.to_thrift(), vec![])?,
                )
                .await
            }
            Op::RefreshWorkflowTasks => {
                let inner: ty::RefreshWorkflowTasksRequest = typed(&request, "request")?;
                // The IDL spells this field `domainUIID` (still field 10).
                self.history_void(
                    "RefreshWorkflowTasks",
                    Struct {
                        fields: vec![
                            (10, domain_uuid(&request, "domainUIID")?),
                            (20, Value::Struct(inner.to_thrift())),
                        ],
                    },
                )
                .await
            }
            Op::RecordActivityTaskHeartbeat | Op::RecordActivityTaskHeartbeatByID => {
                let inner: tk::RecordActivityTaskHeartbeatRequest =
                    typed(&request, "heartbeatRequest")?;
                self.history_typed::<tk::RecordActivityTaskHeartbeatResponse>(
                    "RecordActivityTaskHeartbeat",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::RespondActivityTaskCompleted | Op::RespondActivityTaskCompletedByID => {
                let inner: tk::RespondActivityTaskCompletedRequest =
                    typed(&request, "completeRequest")?;
                self.history_void(
                    "RespondActivityTaskCompleted",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::RespondActivityTaskFailed | Op::RespondActivityTaskFailedByID => {
                let inner: tk::RespondActivityTaskFailedRequest = typed(&request, "failedRequest")?;
                self.history_void("RespondActivityTaskFailed", env(inner.to_thrift(), vec![])?)
                    .await
            }
            Op::RespondActivityTaskCanceled | Op::RespondActivityTaskCanceledByID => {
                let inner: tk::RespondActivityTaskCanceledRequest =
                    typed(&request, "cancelRequest")?;
                self.history_void(
                    "RespondActivityTaskCanceled",
                    env(inner.to_thrift(), vec![])?,
                )
                .await
            }
            Op::RespondDecisionTaskFailed => {
                let inner: tk::RespondDecisionTaskFailedRequest = typed(&request, "failedRequest")?;
                self.history_void("RespondDecisionTaskFailed", env(inner.to_thrift(), vec![])?)
                    .await
            }
            Op::PollForDecisionTask => self.poll_decision(&request).await,
            Op::PollForActivityTask => self.poll_activity(&request).await,
            Op::RespondDecisionTaskCompleted => self.respond_decision_completed(&request).await,
            Op::RespondQueryTaskCompleted => self.respond_query_completed(&request).await,
            Op::DescribeTaskList => self.describe_task_list(&request).await,
            Op::ListTaskListPartitions => self.list_task_list_partitions(&request).await,
            Op::GetTaskListsByDomain => self.get_task_lists_by_domain(&request).await,
            Op::GetWorkflowExecutionHistory => self.get_workflow_execution_history(request).await,
            Op::ListOpenWorkflowExecutions => self.list_visibility(&request, false).await,
            Op::ListClosedWorkflowExecutions => self.list_visibility(&request, true).await,
            Op::ListWorkflowExecutions => self.es_query_op(&request, EsOp::List).await,
            Op::ScanWorkflowExecutions => self.es_query_op(&request, EsOp::Scan).await,
            Op::CountWorkflowExecutions => self.es_query_op(&request, EsOp::Count).await,
            other => Err(crate::handler_workflow::unimplemented_operation(other.name()).into()),
        }
    }
}

/// Go `common.CreateHistoryStartWorkflowRequest`: first-decision backoff and
/// the retry expiration timestamp (History fields 60 and 50).
fn start_extras(
    start: &ty::StartWorkflowExecutionRequest,
) -> Result<Vec<(i16, Value)>, CadenceError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| internal(e.to_string()))?;
    let now_nanos = now.as_nanos() as i64;
    let delay = start.delay_start_seconds.unwrap_or(0);
    let jitter = start.jitter_start_seconds.unwrap_or(0);
    let first_run_at = start.first_run_at_timestamp.unwrap_or(0);
    let mut backoff = delay;
    if first_run_at > now_nanos {
        backoff = ((first_run_at - now_nanos) / 1_000_000_000) as i32;
    } else if !start.cron_schedule.is_empty() {
        let delayed = chrono::DateTime::from_timestamp(
            now.as_secs() as i64 + i64::from(delay),
            now.subsec_nanos(),
        )
        .ok_or_else(|| internal("invalid time"))?
        .naive_utc();
        backoff = cadence_common::cron::get_backoff_for_next_schedule_in_seconds(
            &start.cron_schedule,
            delayed,
            delayed,
            start.cron_overlap_policy.map(|p| p.value()),
            delayed,
            Duration::from_secs(jitter.max(0) as u64),
        )
        .map_err(bad_request)?
            + delay;
    } else if jitter > 0 {
        backoff += (uuid::Uuid::new_v4().as_u128() % (jitter as u128 + 1)) as i32;
    }
    let mut extras = vec![(60, Value::I32(backoff))];
    if let Some(policy) = &start.retry_policy {
        if policy.expiration_interval_in_seconds > 0 {
            let seconds = i64::from(policy.expiration_interval_in_seconds) + i64::from(backoff);
            let deadline = now_nanos + seconds * 1_000_000_000;
            // Go rounds to the millisecond.
            let rounded = (deadline + 500_000) / 1_000_000 * 1_000_000;
            extras.push((50, Value::I64(rounded)));
        }
    }
    Ok(extras)
}

impl WorkflowBackend for ProductionBackend {
    fn invoke<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: serde_json::Value,
    ) -> BackendFuture<'a> {
        Box::pin(self.dispatch(operation, request))
    }
}

/// `WorkflowApi` installed into the shared transports: the ported Go
/// `WorkflowHandler` over the production History/Matching backend.
pub struct HandlerApi {
    handler: crate::handler_workflow::Handler<
        crate::wrappers::redirection_runtime::ForwardingBackend<ProductionBackend>,
    >,
    admin_auth: AdminAccessControl,
}

/// Go `accesscontrolled.NewAdminHandler`: every Admin method needs
/// `PermissionAdmin` except `DescribeCluster` (`PermissionRead`).
struct AdminAccessControl(std::sync::Arc<SelectedAuthorizer>);

impl cadence_service_core::workflow_api::AdminAuthorizer for AdminAccessControl {
    fn authorize_admin(
        &self,
        api: &str,
        token: &str,
    ) -> Result<(), cadence_service_core::workflow_api::AdminAuthError> {
        use crate::wrappers::accesscontrolled::{Attributes, Authorizer, Decision, Permission};
        use cadence_service_core::workflow_api::AdminAuthError;
        let attributes = Attributes {
            operation: api.to_owned(),
            permission: Some(if api == "DescribeCluster" {
                Permission::Read
            } else {
                Permission::Admin
            }),
            auth_token: token.to_owned(),
            ..Default::default()
        };
        match self.0.authorize(&attributes) {
            Err(e) => Err(AdminAuthError::Failed(e)),
            Ok(Decision::Allow) => Ok(()),
            Ok(Decision::Deny) => Err(AdminAuthError::Denied("Request unauthorized.".into())),
        }
    }

    fn authorize_domain_api(
        &self,
        api: &str,
        domain: &str,
        token: &str,
    ) -> Result<(), cadence_service_core::workflow_api::AdminAuthError> {
        use crate::wrappers::accesscontrolled::{Attributes, Authorizer, Decision, Permission};
        use cadence_service_core::workflow_api::AdminAuthError;
        let permission = match api {
            "DescribeDomain" => Permission::Read,
            "FailoverDomain" => Permission::Write,
            "RegisterDomain" | "UpdateDomain" | "DeprecateDomain" | "DeleteDomain" => {
                Permission::Admin
            }
            _ => return Ok(()),
        };
        let attributes = Attributes {
            operation: api.to_owned(),
            permission: Some(permission),
            domain: domain.to_owned(),
            auth_token: token.to_owned(),
            ..Default::default()
        };
        match self.0.authorize(&attributes) {
            Err(e) => Err(AdminAuthError::Failed(e)),
            Ok(Decision::Allow) => Ok(()),
            Ok(Decision::Deny) => Err(AdminAuthError::Denied("Request unauthorized.".into())),
        }
    }
}

/// Shares one `SelectedAuthorizer` between the Workflow and Admin wrappers.
struct SharedAuthorizer(std::sync::Arc<SelectedAuthorizer>);

impl crate::wrappers::accesscontrolled::Authorizer for SharedAuthorizer {
    type Error = String;

    fn authorize(
        &self,
        attributes: &crate::wrappers::accesscontrolled::Attributes,
    ) -> Result<crate::wrappers::accesscontrolled::Decision, Self::Error> {
        crate::wrappers::accesscontrolled::Authorizer::authorize(&*self.0, attributes)
    }
}

impl std::fmt::Debug for HandlerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HandlerApi")
    }
}

impl cadence_service_core::workflow_api::WorkflowApi for HandlerApi {
    fn admin_authorizer(&self) -> Option<&dyn cadence_service_core::workflow_api::AdminAuthorizer> {
        Some(&self.admin_auth)
    }

    fn serves(&self, operation: &str) -> bool {
        WorkflowOperation::from_name(operation).is_some_and(|op| op != WorkflowOperation::Health)
    }

    fn call<'a>(
        &'a self,
        operation: &'a str,
        request: serde_json::Value,
        ctx: cadence_service_core::workflow_api::CallerContext,
    ) -> cadence_service_core::workflow_api::ApiFuture<'a> {
        Box::pin(async move {
            let header = |name: &str| ctx.headers.get(name).cloned().unwrap_or_default();
            let caller = crate::handler_workflow::CallerMetadata {
                client_impl: header("cadence-client-implementation"),
                feature_version: header("cadence-client-feature-version"),
                deadline: ctx.deadline,
                original_headers: ctx.headers.clone(),
                ..Default::default()
            };
            self.handler
                .dispatch_with_metadata(operation, request, &caller)
                .await
        })
    }
}

fn service_addr(
    config: &cadence_service_core::config::Config,
    service: &str,
    env: &str,
    default_port: u16,
) -> String {
    if let Ok(addr) = std::env::var(env) {
        return addr;
    }
    let rpc = config.services.get(service).map(|s| &s.rpc);
    let host = rpc
        .map(|r| r.host.as_str())
        .filter(|h| !h.is_empty())
        .or_else(|| {
            rpc.map(|r| r.bind_on_ip.as_str())
                .filter(|ip| !ip.is_empty() && *ip != "0.0.0.0")
        })
        .unwrap_or("127.0.0.1");
    let port = rpc
        .map(|r| r.port)
        .filter(|p| *p != 0)
        .unwrap_or(default_port);
    format!("{host}:{port}")
}

/// Live `frontend.validSearchAttributes` + validator limits (Go `SearchAttributesValidator`).
struct ProdSearchAttributes {
    state: cadence_service_core::handlers::AppState,
}

impl crate::handler_workflow::SearchAttributeProvider for ProdSearchAttributes {
    fn valid_search_attributes(
        &self,
    ) -> std::collections::HashMap<String, cadence_api::enums::IndexedValueType> {
        cadence_service_core::admin::valid_search_attributes(&self.state.admin)
    }

    fn search_attribute_limits(&self, domain: &str) -> crate::search_attr_validator::Limits {
        let d = crate::search_attr_validator::Limits::default();
        let Ok(o) = self.state.admin.dynamic_config.lock() else {
            return d;
        };
        let int = |key: &str, default: i64| {
            cadence_persistence::dynamic_config::integer(
                o.property_values(key),
                Some(domain),
                default,
            )
        };
        crate::search_attr_validator::Limits {
            number_of_keys: int(
                "frontend.searchAttributesNumberOfKeysLimit",
                d.number_of_keys,
            ),
            size_of_value: int("frontend.searchAttributesSizeOfValueLimit", d.size_of_value),
            total_size: int("frontend.searchAttributesTotalSizeLimit", d.total_size),
        }
    }

    fn validation_enabled(&self) -> bool {
        self.state.admin.dynamic_config.lock().map_or(true, |o| {
            cadence_persistence::dynamic_config::boolean(
                o.property_values("frontend.enableQueryAttributeValidation"),
                None,
                true,
            )
        })
    }
}

/// Build the production WorkflowService for the Frontend launcher.
pub fn build_workflow_api(
    ctx: cadence_matching::launcher::FrontendHookContext,
) -> anyhow::Result<cadence_service_core::workflow_api::SharedWorkflowApi> {
    let history = service_addr(
        &ctx.config,
        "history",
        "CADENCE_HISTORY_TCHANNEL_ADDR",
        7934,
    );
    let matching = service_addr(
        &ctx.config,
        "matching",
        "CADENCE_MATCHING_TCHANNEL_ADDR",
        7935,
    );
    let lookup: std::sync::Arc<dyn crate::handler_workflow::WorkflowDomainLookup> =
        std::sync::Arc::new(
            crate::handler_workflow::PersistenceWorkflowDomainLookup::new(
                ctx.state.domain_manager.clone(),
            ),
        );
    let mut backend = ProductionBackend::new(history, matching)
        .with_domains(lookup.clone())
        .with_history_routing(
            ctx.state.admin.membership.clone(),
            ctx.state.num_history_shards,
        );
    if let Some(store) = ctx.state.history_node_store.clone() {
        backend = backend.with_history_access(crate::workflow_matching::HistoryAccess::new(
            store,
            ctx.config.persistence.num_history_shards,
        ));
    }
    if let Some(store) = ctx.state.visibility_store.clone() {
        backend = backend.with_visibility_store(store);
    }
    match cadence_service_core::es_mapping::search_client_from_persistence(&ctx.config.persistence)
    {
        Ok(Some((client, index))) => {
            let state = ctx.state.clone();
            let overlay = ctx.state.admin.dynamic_config.clone();
            let window = overlay.lock().map_or(10000, |o| {
                cadence_persistence::dynamic_config::integer(
                    o.property_values("frontend.esIndexMaxResultWindow"),
                    None,
                    10000,
                )
            });
            let validate_overlay = overlay.clone();
            backend = backend.with_es_visibility(crate::workflow_visibility::EsBinding {
                es: std::sync::Arc::new(crate::es_visibility::EsVisibility::new(
                    std::sync::Arc::new(client),
                    index,
                    window,
                )),
                attrs: std::sync::Arc::new(move || {
                    cadence_service_core::admin::valid_search_attributes(&state.admin)
                }),
                validate: std::sync::Arc::new(move || {
                    validate_overlay.lock().map_or(true, |o| {
                        cadence_persistence::dynamic_config::boolean(
                            o.property_values("frontend.enableQueryAttributeValidation"),
                            None,
                            true,
                        )
                    })
                }),
            });
        }
        Ok(None) => {}
        Err(e) => return Err(anyhow::anyhow!(e)),
    }
    let version_check_enabled = {
        let overlay = ctx.state.admin.dynamic_config.clone();
        let guard = overlay.lock();
        guard.is_ok_and(|overlay| {
            cadence_persistence::dynamic_config::boolean(
                overlay.property_values("frontend.enableClientVersionCheck"),
                None,
                false,
            )
        })
    };
    let overlay = ctx.state.admin.dynamic_config.clone();
    let read_rps = |key: &'static str, default: i64, per_domain: bool| {
        let overlay = overlay.clone();
        move |domain: &str| -> f64 {
            let guard = overlay.lock();
            guard.map_or(default as f64, |o| {
                let domain = per_domain.then_some(domain);
                cadence_persistence::dynamic_config::integer(
                    o.property_values(key),
                    domain,
                    default,
                ) as f64
            })
        }
    };
    let policy = |global: (&'static str, i64), domain: (&'static str, i64)| {
        let global_rps = read_rps(global.0, global.1, false);
        let domain_rps = read_rps(domain.0, domain.1, true);
        std::sync::Arc::new(crate::wrappers::quota_policy::TokenBucketPolicy::new(
            std::sync::Arc::new(move || global_rps("")),
            std::sync::Arc::new(domain_rps),
        )) as std::sync::Arc<dyn crate::wrappers::ratelimited::Policy>
    };
    let unlimited = crate::wrappers::quota_policy::UNLIMITED_RPS as i64;
    let max_poll_delay_nanos = {
        let guard = overlay.lock();
        guard.map_or(0, |o| {
            cadence_persistence::dynamic_config::duration(
                o.property_values("frontend.maxWorkerPollDelay"),
                None,
                0,
            )
        })
    };
    let rate_limit = crate::wrappers::ratelimited::HandlerRateLimit::new(
        std::sync::Arc::new(crate::wrappers::quota_policy::LookupDomainNames::new(
            lookup.clone(),
        )),
        policy(("frontend.rps", 1200), ("frontend.domainrps", 1200)),
        policy(
            ("frontend.workerrps", unlimited),
            ("frontend.domainworkerrps", unlimited),
        ),
        policy(
            ("frontend.visibilityrps", unlimited),
            ("frontend.domainvisibilityrps", unlimited),
        ),
        policy(
            ("frontend.asyncrps", 10000),
            ("frontend.domainasyncrps", 10000),
        ),
        std::sync::Arc::new(crate::wrappers::quota_policy::NoBypass),
        std::time::Duration::from_nanos(max_poll_delay_nanos.max(0) as u64),
    );
    let async_domains: std::sync::Arc<dyn crate::async_publisher::AsyncDomainConfigLookup> =
        std::sync::Arc::new(crate::async_publisher::PersistenceAsyncDomainConfig::new(
            ctx.state.domain_manager.clone(),
        ));
    let probe: std::sync::Arc<dyn crate::archive_readers::ExecutionProbe> =
        std::sync::Arc::new(backend.clone());
    let (archived_history, archived_visibility) =
        build_archive_readers(&ctx, probe).map_err(anyhow::Error::msg)?;
    let selected_authorizer = {
        let manager = ctx.state.domain_manager.clone();
        let domain_data: crate::oauth_authorizer::DomainDataLookup =
            std::sync::Arc::new(move |name: &str| {
                manager
                    .describe(&cadence_api::service_types::DescribeDomainRequest {
                        name: Some(name.to_owned()),
                        ..Default::default()
                    })
                    .map(|d| d.domain_info.and_then(|i| i.data).unwrap_or_default())
                    .map_err(|e| e.to_string())
            });
        match crate::oauth_authorizer::new_authorizer(&ctx.config.authorization, domain_data)
            .map_err(anyhow::Error::msg)?
        {
            Some(oauth) => SelectedAuthorizer::OAuth(oauth),
            None => SelectedAuthorizer::Nop,
        }
    };
    let selected_authorizer = std::sync::Arc::new(selected_authorizer);
    let default_metadata = cadence_common::cluster::GroupMetadata::default();
    let metadata = ctx
        .config
        .cluster_metadata
        .as_ref()
        .unwrap_or(&default_metadata);
    let redirection_policy = {
        let name = metadata
            .cluster_redirection_policy
            .as_ref()
            .map_or("", |p| p.policy.as_str());
        crate::wrappers::clusterredirection::ForwardingPolicy::from_name(name)
            .ok_or_else(|| anyhow::anyhow!("unknown cluster redirection policy {name:?}"))?
    };
    let forwarding_backend = crate::wrappers::redirection_runtime::ForwardingBackend::new(
        backend,
        metadata.current_cluster_name.clone(),
        crate::wrappers::redirection_runtime::RemoteFrontends::from_metadata(metadata),
    );
    let shared_metrics_sink = frontend_metrics_sink(&ctx.config);
    let handler = install_async_publisher(
        crate::handler_workflow::Handler::new(forwarding_backend),
        async_domains,
        ctx.config.async_workflow_queues.clone(),
    )
    .with_domain_lookup(lookup)
    .with_archived_history_reader(archived_history)
    .with_search_attribute_provider(std::sync::Arc::new(ProdSearchAttributes {
        state: ctx.state.clone(),
    }))
    .with_archived_visibility_reader(archived_visibility)
    .with_metrics_sink(shared_metrics_sink.clone())
    .with_rate_limit_policies(rate_limit)
    .with_access_control(std::sync::Arc::new(
        crate::wrappers::accesscontrolled::HandlerAuthorization::new(
            SharedAuthorizer(selected_authorizer.clone()),
            NopAuthorizationMetrics,
        ),
    ))
    .with_version_check(
        version_check_enabled,
        std::sync::Arc::new(crate::wrappers::versioncheck::GoVersionChecker),
    );
    let handler =
        if redirection_policy == crate::wrappers::clusterredirection::ForwardingPolicy::Noop {
            handler
        } else {
            let overlay = ctx.state.admin.dynamic_config.clone();
            let config = crate::wrappers::redirection_runtime::FnRuntimeConfig(
                std::sync::Arc::new(move |domain: &str| {
                    overlay.lock().map_or(true, |o| {
                        cadence_persistence::dynamic_config::boolean(
                            o.property_values("system.enableDomainNotActiveAutoForwarding"),
                            Some(domain),
                            true,
                        )
                    })
                }),
            );
            handler.with_cluster_redirection(std::sync::Arc::new(
                crate::wrappers::clusterredirection::WorkflowRedirector {
                    policy: redirection_policy,
                    current_cluster: metadata.current_cluster_name.clone(),
                    configured_target: metadata
                        .cluster_redirection_policy
                        .as_ref()
                        .map(|p| p.all_domain_apis_forwarding_target_cluster.clone())
                        .unwrap_or_default(),
                    config,
                    manager: crate::wrappers::redirection_runtime::UnsupportedActiveClusterManager,
                    metrics: std::sync::Arc::new(
                        crate::wrappers::redirection_runtime::SinkRedirectMetrics::new(
                            shared_metrics_sink.clone(),
                        ),
                    ),
                    requests: crate::wrappers::redirection_runtime::DomainCacheResolver::new(
                        ctx.state.domain_manager.clone(),
                    ),
                },
            ))
        };
    Ok(std::sync::Arc::new(HandlerApi {
        handler,
        admin_auth: AdminAccessControl(selected_authorizer),
    }))
}

/// Builds the Go `ArchivalMetadata` + archiver provider readers from the
/// service config `archival` section and the dynamic-config overlay.
type ArchiveReaders = (
    std::sync::Arc<dyn crate::handler_workflow::ArchivedHistoryReader>,
    std::sync::Arc<dyn crate::handler_workflow::ArchivedVisibilityReader>,
);

fn build_archive_readers(
    ctx: &cadence_matching::launcher::FrontendHookContext,
    probe: std::sync::Arc<dyn crate::archive_readers::ExecutionProbe>,
) -> Result<ArchiveReaders, String> {
    use crate::archive_readers as ar;
    let section = ar::ArchivalSection::from_yaml(ctx.config.archival.as_ref());
    let overlay = ctx.state.admin.dynamic_config.clone();
    let status = |key: &'static str| {
        let overlay = overlay.clone();
        std::sync::Arc::new(move || {
            overlay.lock().map_or_else(
                |_| "enabled".to_owned(),
                |o| cadence_persistence::dynamic_config::string(o.property_values(key), "enabled"),
            )
        }) as std::sync::Arc<dyn Fn() -> String + Send + Sync>
    };
    let read = |key: &'static str| {
        let overlay = overlay.clone();
        std::sync::Arc::new(move || {
            overlay.lock().map_or(true, |o| {
                cadence_persistence::dynamic_config::boolean(o.property_values(key), None, true)
            })
        }) as std::sync::Arc<dyn Fn() -> bool + Send + Sync>
    };
    let history_cfg = ar::ArchivalConfig::new(
        &section.history_status,
        status("system.historyArchivalStatus"),
        section.history_enable_read,
        read("system.enableReadFromHistoryArchival"),
    )?;
    let visibility_cfg = ar::ArchivalConfig::new(
        &section.visibility_status,
        status("system.visibilityArchivalStatus"),
        section.visibility_enable_read,
        read("system.enableReadFromVisibilityArchival"),
    )?;
    let provider = cadence_common::archiver::ArchiverProvider::new(section.provider_keys.clone());
    let defaults = |kind: &str| {
        let node = ctx
            .config
            .domain_defaults
            .as_ref()
            .and_then(|d| d.get("archival"))
            .and_then(|a| a.get(kind));
        let text = |k: &str| {
            node.and_then(|n| n.get(k))
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_lowercase()
        };
        let uri = node
            .and_then(|n| n.get("URI"))
            .and_then(serde_yaml::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        match text("status").as_str() {
            "" | "disabled" => Ok((false, uri)),
            "enabled" => Ok((true, uri)),
            other => Err(format!(
                "invalid archival status of {other} for domain, valid status are: {{\"\", \"disabled\", \"enabled\"}}"
            )),
        }
    };
    let (h_on, h_uri) = defaults("history")?;
    let (v_on, v_uri) = defaults("visibility")?;
    let kind_policy = |cfg: &ar::ArchivalConfig, on: bool, uri: String| {
        let cfg = cfg.clone();
        cadence_common::archival_state::KindPolicy {
            configured: std::sync::Arc::new(move || cfg.cluster_configured_for_archival()),
            default_enabled: on,
            default_uri: uri,
        }
    };
    cadence_common::archival_state::set_domain_archival_policy(
        cadence_common::archival_state::DomainArchivalPolicy {
            history: kind_policy(&history_cfg, h_on, h_uri),
            visibility: kind_policy(&visibility_cfg, v_on, v_uri),
            provider: provider.clone(),
        },
    );
    let domains: std::sync::Arc<dyn ar::ArchivalDomainConfig> = std::sync::Arc::new(
        ar::PersistenceArchivalDomainConfig::new(ctx.state.domain_manager.clone()),
    );
    Ok((
        std::sync::Arc::new(ar::ArchivedHistory::new(
            history_cfg,
            provider.clone(),
            domains.clone(),
            probe,
        )),
        std::sync::Arc::new(ar::ArchivedVisibility::new(
            visibility_cfg,
            provider,
            domains,
        )),
    ))
}

/// Builds the Frontend metrics sink from `services.frontend.metrics` using the
/// shared exporter runtime (StatsD/M3/Prometheus). An absent/empty reporter is
/// a no-op scope like Go. The runtime lives for the process lifetime.
fn frontend_metrics_sink(
    config: &cadence_service_core::config::Config,
) -> std::sync::Arc<dyn cadence_service_core::matching_metrics::MetricSink> {
    use cadence_service_core::matching_metrics_runtime::MatchingMetricsRuntime;
    let Some(service) = config.services.get("frontend") else {
        return std::sync::Arc::new(cadence_service_core::matching_metrics::NoopMetricSink);
    };
    let runtime = MatchingMetricsRuntime::new_for_service(
        &service.metrics,
        Some(&service.pprof),
        cadence_service_core::matching_metrics_migration::MigrationPolicy {
            histograms: config.histograms.clone(),
            gauges: config.gauge_migration.clone(),
            counters: config.counter_migration.clone(),
        },
        "cadence-frontend",
    );
    match runtime {
        Ok(mut runtime) => {
            let sink = runtime.sink();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = runtime.start().await;
                    std::future::pending::<()>().await;
                });
            }
            sink
        }
        Err(error) => {
            eprintln!("frontend metrics disabled: {error}");
            std::sync::Arc::new(cadence_service_core::matching_metrics::NoopMetricSink)
        }
    }
}

/// Installs the production async publisher on the handler.
fn install_async_publisher<B: WorkflowBackend>(
    handler: crate::handler_workflow::Handler<B>,
    domains: std::sync::Arc<dyn crate::async_publisher::AsyncDomainConfigLookup>,
    queues: std::collections::HashMap<
        String,
        cadence_service_core::config::AsyncWorkflowQueueProviderConfig,
    >,
) -> crate::handler_workflow::Handler<B> {
    handler.with_async_publisher(crate::async_publisher::production_publisher(
        domains, queues,
    ))
}

/// Go's `authorization.NewAuthorizer`: nop unless oauth is enabled.
enum SelectedAuthorizer {
    /// `authorization.NewNopAuthorizer`: allows every request.
    Nop,
    OAuth(crate::oauth_authorizer::OAuthAuthorizer),
}

impl crate::wrappers::accesscontrolled::Authorizer for SelectedAuthorizer {
    type Error = String;

    fn authorize(
        &self,
        attributes: &crate::wrappers::accesscontrolled::Attributes,
    ) -> Result<crate::wrappers::accesscontrolled::Decision, Self::Error> {
        match self {
            Self::Nop => Ok(crate::wrappers::accesscontrolled::Decision::Allow),
            Self::OAuth(oauth) => {
                crate::wrappers::accesscontrolled::Authorizer::authorize(oauth, attributes)
            }
        }
    }
}

struct NopAuthorizationMetrics;

impl crate::wrappers::accesscontrolled::AuthorizationMetrics for NopAuthorizationMetrics {
    fn authorization_latency(&self, _elapsed: std::time::Duration) {}
    fn authorization_failed(&self) {}
    fn unauthorized(&self) {}
}

#[cfg(test)]
mod history_routing_tests {
    use super::*;
    use cadence_service_core::admin::{ClusterMembership, MembershipSlot};
    use std::sync::{Arc, Mutex};

    struct Members(Mutex<Result<Vec<String>, String>>);
    impl ClusterMembership for Members {
        fn who_am_i(&self) -> Result<String, String> {
            Ok("frontend".into())
        }
        fn members(&self, role: &str) -> Result<Vec<String>, String> {
            assert_eq!(role, "cadence-history");
            self.0.lock().unwrap().clone()
        }
    }

    #[test]
    fn routing_reads_live_members_and_only_empty_ring_uses_static_address() {
        let slot = MembershipSlot::default();
        let backend = ProductionBackend::new("fallback:7934".into(), "matching:7935".into())
            .with_history_routing(slot.clone(), 1024);
        assert_eq!(
            backend.history_addr_for_workflow("hello").unwrap(),
            "fallback:7934"
        );
        let members = Arc::new(Members(Mutex::new(Ok(vec![]))));
        slot.set(Some(members.clone()));
        assert_eq!(
            backend.history_addr_for_workflow("hello").unwrap(),
            "fallback:7934"
        );
        *members.0.lock().unwrap() = Ok(vec!["history-a:7934".into(), "history-b:7934".into()]);
        let expected = members.history_owner(870).unwrap().unwrap().address;
        assert_eq!(
            backend.history_addr_for_workflow("hello").unwrap(),
            expected
        );
        *members.0.lock().unwrap() = Ok(vec!["history-c:7934".into()]);
        assert_eq!(
            backend.history_addr_for_workflow("hello").unwrap(),
            "history-c:7934"
        );
        *members.0.lock().unwrap() = Err("ring unavailable".into());
        assert!(backend.history_addr_for_workflow("hello").is_err());
    }

    #[test]
    fn history_routing_extracts_start_execution_and_go_task_token_selectors() {
        let execution = Struct {
            fields: vec![(10, Value::Bytes(b"workflow".to_vec()))],
        };
        let envelope = |inner| Struct {
            fields: vec![(20, Value::Struct(inner))],
        };
        assert_eq!(
            history_workflow_id(
                "StartWorkflowExecution",
                &envelope(Struct {
                    fields: vec![(20, Value::Bytes(b"start".to_vec()))]
                })
            )
            .unwrap(),
            "start"
        );
        assert_eq!(
            history_workflow_id(
                "SignalWorkflowExecution",
                &envelope(Struct {
                    fields: vec![(20, Value::Struct(execution.clone()))]
                })
            )
            .unwrap(),
            "workflow"
        );
        assert_eq!(
            history_workflow_id("ResetStickyTaskList", &envelope(execution)).unwrap(),
            "workflow"
        );
        for method in [
            "RespondDecisionTaskCompleted",
            "RespondActivityTaskFailed",
            "RecordActivityTaskHeartbeat",
        ] {
            assert_eq!(
                history_workflow_id(
                    method,
                    &envelope(Struct {
                        fields: vec![(
                            10,
                            Value::Bytes(br#"{"workflowId":"token-workflow"}"#.to_vec())
                        )]
                    })
                )
                .unwrap(),
                "token-workflow"
            );
        }
        assert!(history_workflow_id(
            "RespondDecisionTaskFailed",
            &envelope(Struct {
                fields: vec![(10, Value::Bytes(b"invalid".to_vec()))]
            })
        )
        .is_err());
    }

    #[test]
    fn shard_hash_matches_go_source_derived_vectors() {
        // Derived without executing Go: util.go's Fingerprint32 % shard count;
        // go-farm 3414d57e47da fp_generic.go, farmhashcc.go (signed int8 short
        // path), farmhashmk.go (5..12 bytes), basics.go (mur/fmix), u32 wrapping.
        for (workflow, hash) in [
            ("", 3_696_677_242_u32),
            ("a", 1_016_544_589),
            ("ab", 1_098_068_221),
            ("test", 1_633_095_781),
            ("é", 1_973_972_843),
            ("�", 4_143_110_158),
            ("hello", 2_039_911_270),
            ("workflow-123", 1_545_591_485),
        ] {
            for count in [1, 4, 1024, 16384] {
                assert_eq!(
                    crate::workflow_matching::workflow_id_to_history_shard(workflow, count),
                    (hash % count as u32) as i32
                );
            }
        }
    }
}

/// Go client/history: the owner is re-resolved from Ringpop on every attempt;
/// ShardOwnershipLost (request not applied) and ServiceBusy (host down /
/// overloaded) are retried with linear backoff (capped at 8 units) while shard
/// ownership moves. Any other result is returned immediately.
pub(crate) async fn retry_history_call<R, C, Fut>(
    mut resolve: R,
    mut call: C,
    backoff_unit: Duration,
    budget: Duration,
) -> Result<Struct, CadenceError>
where
    R: FnMut() -> Result<String, CadenceError>,
    C: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<Struct, CadenceError>>,
{
    let started = std::time::Instant::now();
    let mut attempt = 0u32;
    loop {
        let address = resolve()?;
        let result = call(address).await;
        match &result {
            Err(CadenceError::ShardOwnershipLost(_)) | Err(CadenceError::ServiceBusy(_))
                if attempt < HISTORY_RETRY_MAX_ATTEMPTS && started.elapsed() < budget =>
            {
                attempt += 1;
                tokio::time::sleep(backoff_unit * attempt.min(8)).await;
            }
            _ => return result,
        }
    }
}

#[cfg(test)]
mod history_retry_tests {
    use super::*;
    use cadence_api::errors::{ServiceBusyError, ShardOwnershipLostError};
    use std::sync::{Arc, Mutex};

    fn lost() -> CadenceError {
        CadenceError::ShardOwnershipLost(ShardOwnershipLostError::new("moved"))
    }

    #[tokio::test]
    async fn retries_ownership_lost_and_re_resolves_owner_each_attempt() {
        let owners = Arc::new(Mutex::new(vec!["h3:1", "h2:1", "h1:1"]));
        let called = Arc::new(Mutex::new(Vec::new()));
        let (o, c) = (owners.clone(), called.clone());
        let result = retry_history_call(
            move || Ok(o.lock().unwrap().pop().unwrap().to_string()),
            move |addr| {
                let c = c.clone();
                async move {
                    let mut c = c.lock().unwrap();
                    c.push(addr);
                    if c.len() < 3 {
                        Err(lost())
                    } else {
                        Ok(Struct::default())
                    }
                }
            },
            Duration::from_millis(1),
            Duration::from_secs(5),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(called.lock().unwrap().as_slice(), ["h1:1", "h2:1", "h3:1"]);
    }

    #[tokio::test]
    async fn retries_service_busy_then_succeeds() {
        let n = Arc::new(Mutex::new(0));
        let n2 = n.clone();
        let result = retry_history_call(
            || Ok("h:1".into()),
            move |_| {
                let n = n2.clone();
                async move {
                    let mut n = n.lock().unwrap();
                    *n += 1;
                    if *n == 1 {
                        Err(CadenceError::ServiceBusy(ServiceBusyError::new("busy")))
                    } else {
                        Ok(Struct::default())
                    }
                }
            },
            Duration::from_millis(1),
            Duration::from_secs(5),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(*n.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn other_errors_are_not_retried() {
        let n = Arc::new(Mutex::new(0));
        let n2 = n.clone();
        let result = retry_history_call(
            || Ok("h:1".into()),
            move |_| {
                let n = n2.clone();
                async move {
                    *n.lock().unwrap() += 1;
                    Err(internal("boom"))
                }
            },
            Duration::from_millis(1),
            Duration::from_secs(5),
        )
        .await;
        assert!(matches!(result, Err(CadenceError::Internal(_))));
        assert_eq!(*n.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_with_last_error() {
        let n = Arc::new(Mutex::new(0u32));
        let n2 = n.clone();
        let result = retry_history_call(
            || Ok("h:1".into()),
            move |_| {
                let n = n2.clone();
                async move {
                    *n.lock().unwrap() += 1;
                    Err(lost())
                }
            },
            Duration::from_micros(1),
            Duration::from_secs(60),
        )
        .await;
        assert!(matches!(result, Err(CadenceError::ShardOwnershipLost(_))));
        assert_eq!(*n.lock().unwrap(), HISTORY_RETRY_MAX_ATTEMPTS + 1);
    }

    #[tokio::test]
    async fn stops_when_budget_is_spent_and_propagates_resolver_errors() {
        let n = Arc::new(Mutex::new(0u32));
        let n2 = n.clone();
        let result = retry_history_call(
            || Ok("h:1".into()),
            move |_| {
                let n = n2.clone();
                async move {
                    *n.lock().unwrap() += 1;
                    Err(lost())
                }
            },
            Duration::from_millis(1),
            Duration::ZERO,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*n.lock().unwrap(), 1);

        let result = retry_history_call(
            || Err(internal("no owner")),
            |_| async { Ok(Struct::default()) },
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(result, Err(CadenceError::Internal(_))));
    }
}

#[cfg(test)]
mod async_publisher_install_tests {
    use super::*;
    use cadence_api::types::AsyncWorkflowConfiguration;
    use std::sync::Arc;

    struct NoDomains;
    impl crate::async_publisher::AsyncDomainConfigLookup for NoDomains {
        fn async_config(
            &self,
            _: &str,
        ) -> Result<AsyncWorkflowConfiguration, cadence_api::errors::CadenceError> {
            Ok(AsyncWorkflowConfiguration::default())
        }
    }

    #[test]
    fn production_install_sets_async_publisher() {
        let plain = crate::handler_workflow::Handler::new(ProductionBackend::new(
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
        ));
        assert!(!plain.has_async_publisher());
        let installed = install_async_publisher(plain, Arc::new(NoDomains), Default::default());
        assert!(installed.has_async_publisher());
    }
}
