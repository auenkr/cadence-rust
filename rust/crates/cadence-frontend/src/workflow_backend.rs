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

const HISTORY_SERVICE: &str = "cadence-history";
const CALLER: &str = "cadence-frontend";
/// Go's frontend client default timeout for History calls.
const HISTORY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ProductionBackend {
    history_addr: String,
    matching_addr: String,
    pub(crate) history_access: Option<crate::workflow_matching::HistoryAccess>,
    pub(crate) domains: Option<std::sync::Arc<dyn crate::handler_workflow::WorkflowDomainLookup>>,
    pub(crate) visibility:
        Option<std::sync::Arc<dyn cadence_persistence::visibility_reader::VisibilityStore>>,
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

impl ProductionBackend {
    pub fn new(history_addr: String, matching_addr: String) -> Self {
        Self {
            history_addr,
            matching_addr,
            history_access: None,
            visibility: None,
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

    pub(crate) fn history_addr(&self) -> &str {
        &self.history_addr
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
        self.call(
            &self.history_addr,
            HISTORY_SERVICE,
            "HistoryService",
            method,
            envelope,
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
        eprintln!("BACKEND-TRACE end {procedure} {}ms {}", __t0.elapsed().as_millis(), match &response { Ok(r) => format!("ok app_err={}", r.application_error), Err(e) => format!("err {e}") });
        let response = response
        .map_err(|error| match error {
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
                self.history_void("RefreshWorkflowTasks", env(inner.to_thrift(), vec![])?)
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
            Op::ListWorkflowExecutions
            | Op::ScanWorkflowExecutions
            | Op::CountWorkflowExecutions => Err(crate::workflow_visibility::not_supported()),
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
    handler: crate::handler_workflow::Handler<ProductionBackend>,
}

impl std::fmt::Debug for HandlerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HandlerApi")
    }
}

impl cadence_service_core::workflow_api::WorkflowApi for HandlerApi {
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
    let mut backend = ProductionBackend::new(history, matching).with_domains(lookup.clone());
    if let Some(store) = ctx.state.history_node_store.clone() {
        backend = backend.with_history_access(crate::workflow_matching::HistoryAccess::new(
            store,
            ctx.config.persistence.num_history_shards,
        ));
    }
    if let Some(store) = ctx.state.visibility_store.clone() {
        backend = backend.with_visibility_store(store);
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
    let handler = crate::handler_workflow::Handler::new(backend)
        .with_domain_lookup(lookup)
        .with_rate_limit_policies(rate_limit)
        .with_access_control(std::sync::Arc::new(
            crate::wrappers::accesscontrolled::HandlerAuthorization::new(
                NopAuthorizer,
                NopAuthorizationMetrics,
            ),
        ))
        .with_version_check(
            version_check_enabled,
            std::sync::Arc::new(crate::wrappers::versioncheck::GoVersionChecker),
        );
    Ok(std::sync::Arc::new(HandlerApi { handler }))
}

/// Go's `authorization.NewNopAuthorizer`: used when authorization is not
/// enabled in the service configuration, allowing every request.
struct NopAuthorizer;

impl crate::wrappers::accesscontrolled::Authorizer for NopAuthorizer {
    type Error = std::convert::Infallible;

    fn authorize(
        &self,
        _attributes: &crate::wrappers::accesscontrolled::Attributes,
    ) -> Result<crate::wrappers::accesscontrolled::Decision, Self::Error> {
        Ok(crate::wrappers::accesscontrolled::Decision::Allow)
    }
}

struct NopAuthorizationMetrics;

impl crate::wrappers::accesscontrolled::AuthorizationMetrics for NopAuthorizationMetrics {
    fn authorization_latency(&self, _elapsed: std::time::Duration) {}
    fn authorization_failed(&self) {}
    fn unauthorized(&self) {}
}
