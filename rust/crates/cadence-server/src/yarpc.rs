//! YARPC HTTP transport for Thrift-encoded Cadence calls.
//!
//! Speaks the same protocol as Go's YARPC stack (`Rpc-Service`,
//! `Rpc-Procedure` (`WorkflowService::Method`), `Rpc-Encoding: thrift`,
//! strict Thrift binary envelopes). Declared Thrift exceptions are returned
//! in the REPLY body with `Rpc-Status: error` plus
//! `Rpc-Application-Error-Name/Details`, mirroring the generated Go
//! handlers; transport failures use HTTP 400/404/501.
//!
//! Long-poll RPCs (task polling, queries) block up to the client's
//! `Context-TTL-MS` deadline, like Go matching.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use cadence_api::errors::{BadRequestError, EntityNotExistsError};
use cadence_api::service_types::{DeleteDomainRequest, DescribeDomainRequest};
use cadence_thrift::protocol::{Reader, Writer};
use cadence_thrift::service::find_method;
use cadence_thrift::value::{read_struct, write_struct, FromThrift, Struct, ToThrift, Value};

use crate::handlers::AppState;

const SERVICE: &str = "cadence-frontend";

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

pub(crate) fn ttl_deadline(headers: &HeaderMap, default: Duration) -> Instant {
    let millis: u64 = header(headers, "Context-TTL-MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(default.as_millis() as u64)
        .clamp(100, 300_000);
    Instant::now() + Duration::from_millis(millis)
}

pub(crate) fn ok_reply(service_echo: &str, result: Struct, app_error: Option<(&str, String)>) -> Response {
    let mut w = Writer::new();
    write_struct(&mut w, &result);
    let mut resp = w.into_bytes().into_response();
    let headers = resp.headers_mut();
    headers.insert(
        "Rpc-Status",
        (if app_error.is_some() {
            "error"
        } else {
            "success"
        })
        .parse()
        .unwrap(),
    );
    headers.insert(
        "Rpc-Service",
        service_echo
            .parse()
            .unwrap_or_else(|_| "cadence-frontend".parse().unwrap()),
    );
    if let Some((name, details)) = app_error {
        headers.insert(
            "Rpc-Application-Error-Name",
            name.parse().unwrap_or_else(|_| "Unknown".parse().unwrap()),
        );
        let mut details = details;
        if details.len() > 256 {
            details.truncate(256);
        }
        if let Ok(v) = details.parse() {
            headers.insert("Rpc-Application-Error-Details", v);
        }
    }
    resp
}

pub(crate) fn transport_error(status: StatusCode, message: String) -> Response {
    let mut resp = (status, message).into_response();
    resp.headers_mut()
        .insert("Rpc-Status", "error".parse().unwrap());
    resp
}

/// A typed application error: Thrift exception name + encoded value.
struct AppError {
    exc_name: &'static str,
    value: Struct,
    message: String,
}

impl AppError {
    fn bad_request(message: String) -> Self {
        let e = BadRequestError {
            message: message.clone(),
        };
        Self {
            exc_name: "BadRequestError",
            value: e.to_thrift(),
            message,
        }
    }

    fn busy(message: String) -> Self {
        let error = cadence_api::errors::ServiceBusyError::new(message.clone());
        Self { exc_name: "ServiceBusyError", value: error.to_thrift(), message }
    }

    fn entity_not_exists(message: String) -> Self {
        let e = EntityNotExistsError::new(message.clone());
        Self {
            exc_name: "EntityNotExistsError",
            value: e.to_thrift(),
            message,
        }
    }
}

type HandlerResult = Result<Struct, AppError>;

/// Decode the args struct (field 1) into a typed request.
fn decode_args<T: FromThrift>(body: &Struct) -> Result<T, String> {
    match body.get(1) {
        Some(Value::Struct(s)) => T::from_thrift(s).map_err(|e| e.to_string()),
        _ => Err("missing args field 1".to_string()),
    }
}

fn wrap_success<T: ToThrift>(v: &T) -> Struct {
    Struct {
        fields: vec![(0, Value::Struct(v.to_thrift()))],
    }
}

fn require_domain_thrift(
    state: &AppState,
    domain: &str,
) -> Result<cadence_persistence::InMemoryExecutionStore, AppError> {
    if domain.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    state
        .execution_store(domain)
        .map_err(|e| AppError::entity_not_exists(e.message))
}

fn require_task_domain_thrift(state: &AppState, bytes: &[u8]) -> Result<(), AppError> {
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(bytes)
        .map_err(|_| AppError::bad_request("Invalid TaskToken.".to_string()))?;
    if uuid::Uuid::parse_str(&token.domain_id).is_err() {
        return Err(AppError::bad_request(
            "Invalid TaskToken domain UUID.".to_string(),
        ));
    }
    state
        .domains
        .describe(&DescribeDomainRequest {
            name: None,
            uuid: Some(token.domain_id),
        })
        .map(|_| ())
        .map_err(|e| AppError::entity_not_exists(e.message))
}

async fn handle_delete_domain(state: &AppState, body: &Struct) -> HandlerResult {
    let req: DeleteDomainRequest = decode_args(body).map_err(AppError::bad_request)?;
    if req.name.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    state
        .domains
        .delete(&req.name)
        .map_err(|e| AppError::entity_not_exists(e.message))?;
    state.executions.unregister_domain(&req.name);
    Ok(Struct { fields: Vec::new() })
}

// ---------------------------------------------------------------------------
// Per-method handlers
// ---------------------------------------------------------------------------

async fn handle_start(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::StartWorkflowExecutionRequest;
    use cadence_api::validation::{self, Limits};
    let req: StartWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::validate_start_workflow_execution_request(Some(&req), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    match executions.start(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => Err(AppError {
            exc_name: "WorkflowExecutionAlreadyStartedError",
            value: e.to_thrift(),
            message: e.message.clone(),
        }),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_register(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::enums::DomainStatus;
    use cadence_api::types::RegisterDomainRequest;
    let req: RegisterDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    if req.workflow_execution_retention_period_in_days < state.domain_policy(&req.name).min_retention_days {
        return Err(AppError::bad_request(
            "A valid retention period is not set on request.".to_string(),
        ));
    }
    let info = cadence_api::types::DomainInfo {
        name: req.name.clone(),
        status: Some(DomainStatus::Registered),
        description: req.description.clone(),
        owner_email: req.owner_email.clone(),
        data: req.data.clone(),
        uuid: uuid::Uuid::new_v4().to_string(),
    };
    let configuration = cadence_api::types::DomainConfiguration {
        workflow_execution_retention_period_in_days: req
            .workflow_execution_retention_period_in_days,
        emit_metric: req.emit_metric.unwrap_or(true),
        history_archival_status: req.history_archival_status,
        history_archival_uri: req.history_archival_uri.clone(),
        visibility_archival_status: req.visibility_archival_status,
        visibility_archival_uri: req.visibility_archival_uri.clone(),
        ..Default::default()
    };
    let replication = crate::handlers::domain_replication(state, &req)
        .map_err(|e| AppError::bad_request(e.message))?;
    let failover_version = if req.is_global_domain {
        state.cluster_metadata.next_failover_version(&replication.active_cluster_name, 0, false)
            .map_err(AppError::bad_request)?
    } else { cadence_persistence::version_history::EMPTY_VERSION };
    match state
        .domains
        .create_with_version(info, configuration, replication, req.is_global_domain, failover_version)
    {
        Ok(_) => {
            state.executions.register_domain(
                &req.name,
                &state
                    .domains
                    .domain_id(&req.name)
                    .expect("registered domain"),
            );
            // void method: empty success struct.
            Ok(Struct { fields: vec![] })
        }
        Err(e) => Err(AppError {
            exc_name: "DomainAlreadyExistsError",
            value: cadence_api::errors::DomainAlreadyExistsError {
                message: e.message.clone(),
            }
            .to_thrift(),
            message: e.message,
        }),
    }
}

async fn handle_describe_domain(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::DescribeDomainRequest;
    let req: DescribeDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_none() && req.uuid.is_none() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    match state.domains.describe(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_list_domains(state: &AppState, body: &Struct) -> HandlerResult {
    let request: cadence_api::service_types::ListDomainsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    let response = state
        .domains
        .list_page(&request)
        .map_err(|e| AppError::bad_request(e.message))?;
    Ok(wrap_success(&response))
}

async fn handle_visibility_list(state: &AppState, body: &Struct) -> HandlerResult {
    let request: cadence_api::service_types::ListWorkflowExecutionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    let executions = require_domain_thrift(state, &request.domain)?;
    let response = executions
        .visibility_page(&request)
        .map_err(|e| AppError::bad_request(e.message))?;
    Ok(wrap_success(&response))
}
async fn handle_visibility_count(state: &AppState, body: &Struct) -> HandlerResult {
    let request: cadence_api::service_types::CountWorkflowExecutionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    let executions = require_domain_thrift(state, &request.domain)?;
    let rows = executions
        .query_visible(&request.domain, &request.query)
        .map_err(|e| AppError::bad_request(e.message))?;
    Ok(wrap_success(
        &cadence_api::service_types::CountWorkflowExecutionsResponse {
            count: rows.len() as i64,
        },
    ))
}
async fn handle_visibility_archived(_state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::validation;
    let r: cadence_api::service_types::ListArchivedWorkflowExecutionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    validation::validate_list_archived_request(Some(&r))
        .map_err(|e| AppError::bad_request(e.message))?;
    // Mirror Go's gating order: cluster archival is checked before the domain
    // lookup. Visibility archival has no backend in this port yet, so the
    // cluster is never configured and the exact Go error is returned.
    Err(AppError::bad_request(
        "Cluster is not configured for visibility archival".to_string(),
    ))
}

async fn handle_visibility_open(state: &AppState, body: &Struct) -> HandlerResult {
    let r: cadence_api::service_types::ListOpenWorkflowExecutionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    let executions = require_domain_thrift(state, &r.domain)?;
    if r.execution_filter.is_some() && r.type_filter.is_some() {
        return Err(AppError::bad_request(
            "Only one of ExecutionFilter or TypeFilter is allowed".into(),
        ));
    }
    let mut rows = executions.list_filtered(
        &r.domain,
        true,
        r.execution_filter.as_ref().map(|f| f.workflow_id.as_str()),
        r.type_filter.as_ref().map(|f| f.name.as_str()),
        None,
    );
    cadence_persistence::visibility::filter_start_time(&mut rows, r.start_time_filter.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    cadence_persistence::visibility::apply(&mut rows, "")
        .map_err(|e| AppError::bad_request(e.message))?;
    let v = cadence_persistence::visibility::page(rows, r.maximum_page_size, &r.next_page_token)
        .map_err(|e| AppError::bad_request(e.message))?;
    Ok(wrap_success(
        &cadence_api::service_types::ListOpenWorkflowExecutionsResponse {
            executions: v.executions,
            next_page_token: v.next_page_token,
        },
    ))
}
async fn handle_visibility_closed(state: &AppState, body: &Struct) -> HandlerResult {
    let r: cadence_api::service_types::ListClosedWorkflowExecutionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(e.to_string()))?;
    let executions = require_domain_thrift(state, &r.domain)?;
    if u8::from(r.execution_filter.is_some())
        + u8::from(r.type_filter.is_some())
        + u8::from(r.status_filter.is_some())
        > 1
    {
        return Err(AppError::bad_request(
            "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed".into(),
        ));
    }
    let mut rows = executions.list_filtered(
        &r.domain,
        false,
        r.execution_filter.as_ref().map(|f| f.workflow_id.as_str()),
        r.type_filter.as_ref().map(|f| f.name.as_str()),
        r.status_filter,
    );
    cadence_persistence::visibility::filter_start_time(&mut rows, r.start_time_filter.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    cadence_persistence::visibility::apply(&mut rows, "")
        .map_err(|e| AppError::bad_request(e.message))?;
    let v = cadence_persistence::visibility::page(rows, r.maximum_page_size, &r.next_page_token)
        .map_err(|e| AppError::bad_request(e.message))?;
    Ok(wrap_success(
        &cadence_api::service_types::ListClosedWorkflowExecutionsResponse {
            executions: v.executions,
            next_page_token: v.next_page_token,
        },
    ))
}

async fn handle_update_domain(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::UpdateDomainRequest;
    let req: UpdateDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    match state.domains.update(&req, &state.cluster_metadata, &state.domain_policy(&req.name)) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::FailoverError::NotFound(e)) => Err(AppError::entity_not_exists(e.message)),
        Err(cadence_persistence::FailoverError::Busy(message)) => Err(AppError::busy(message)),
        Err(cadence_persistence::FailoverError::Invalid(message)) => Err(AppError::bad_request(message)),
    }
}

async fn handle_deprecate_domain(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::DeprecateDomainRequest;
    let req: DeprecateDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    match state.domains.deprecate(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_signal(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::SignalWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: SignalWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.signal_name.is_empty() {
        return Err(AppError::bad_request(
            "SignalName is not set on request.".to_string(),
        ));
    }
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    mutation_to_thrift(executions.signal(&req))
}

async fn handle_signal_with_start(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
    use cadence_api::validation::{self, Limits};
    let req: SignalWithStartWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::validate_signal_with_start_request(Some(&req), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    match executions.signal_with_start(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => Err(AppError {
            exc_name: "WorkflowExecutionAlreadyStartedError",
            value: e.to_thrift(),
            message: e.message.clone(),
        }),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_terminate(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::TerminateWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: TerminateWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    match executions.terminate(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(mutation_error(e)),
    }
}

async fn handle_request_cancel(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::RequestCancelWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: RequestCancelWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    match executions.request_cancel(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(mutation_error(e)),
    }
}

async fn handle_reset_sticky(state: &AppState, body: &Struct) -> HandlerResult {
    let request: cadence_api::service_types::ResetStickyTaskListRequest = decode_args(body)
        .map_err(|error| AppError::bad_request(format!("invalid args: {error}")))?;
    let store = require_domain_thrift(state, &request.domain)?;
    cadence_api::validation::check_execution(request.execution.as_ref())
        .map_err(|error| AppError::bad_request(error.message))?;
    store
        .reset_sticky_task_list(&request.domain, request.execution.as_ref().unwrap())
        .map_err(mutation_error)?;
    Ok(wrap_success(
        &cadence_api::service_types::ResetStickyTaskListResponse {},
    ))
}

fn mutation_error(e: cadence_persistence::WorkflowMutationError) -> AppError {
    match e {
        cadence_persistence::WorkflowMutationError::NotFound(n) => {
            AppError::entity_not_exists(n.message)
        }
        cadence_persistence::WorkflowMutationError::AlreadyCompleted(c) => AppError {
            exc_name: "WorkflowExecutionAlreadyCompletedError",
            value: c.to_thrift(),
            message: c.message.clone(),
        },
    }
}

fn task_error(e: cadence_persistence::TaskError) -> AppError {
    match e {
        cadence_persistence::TaskError::InvalidToken(b)
        | cadence_persistence::TaskError::BadDecision(b) => AppError::bad_request(b.message),
        cadence_persistence::TaskError::NotFound(n) => AppError::entity_not_exists(n.message),
        cadence_persistence::TaskError::AlreadyCompleted(c) => AppError {
            exc_name: "WorkflowExecutionAlreadyCompletedError",
            value: c.to_thrift(),
            message: c.message.clone(),
        },
    }
}

async fn handle_describe_workflow(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::DescribeWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: DescribeWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution)).map_err(|e| AppError::bad_request(e.message))?;
    match executions.describe(&req.domain, &execution) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_get_history(state: &AppState, body: &Struct, deadline: Instant) -> HandlerResult {
    use cadence_api::service_types::GetWorkflowExecutionHistoryRequest;
    use cadence_api::validation;
    let req: GetWorkflowExecutionHistoryRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution)).map_err(|e| AppError::bad_request(e.message))?;
    if req.wait_for_new_event {
        loop {
            match executions.history_page(
                &req.domain,
                &execution,
                req.maximum_page_size,
                &req.next_page_token,
                req.history_event_filter_type,
            ) {
                Ok(resp)
                    if (!resp
                        .history
                        .as_ref()
                        .map(|h| h.events.is_empty())
                        .unwrap_or(true))
                        || !req.next_page_token.is_empty() =>
                {
                    return Ok(wrap_success(&resp));
                }
                Ok(_) if Instant::now() >= deadline => {
                    // Timed out long-poll: return empty history page (mirrors Go).
                    return Ok(wrap_success(
                        &cadence_api::service_types::GetWorkflowExecutionHistoryResponse::default(),
                    ));
                }
                Ok(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(cadence_persistence::HistoryPageError::NotFound(e)) => {
                    return Err(AppError::entity_not_exists(e.message))
                }
                Err(cadence_persistence::HistoryPageError::InvalidToken(e)) => {
                    return Err(AppError::bad_request(e.message))
                }
            }
        }
    }
    match executions.history_page(
        &req.domain,
        &execution,
        req.maximum_page_size,
        &req.next_page_token,
        req.history_event_filter_type,
    ) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::HistoryPageError::NotFound(e)) => {
            Err(AppError::entity_not_exists(e.message))
        }
        Err(cadence_persistence::HistoryPageError::InvalidToken(e)) => {
            Err(AppError::bad_request(e.message))
        }
    }
}

async fn handle_poll_decision(state: &AppState, body: &Struct, deadline: Instant) -> HandlerResult {
    use cadence_api::tasks::PollForDecisionTaskRequest;
    use cadence_api::validation;
    let req: PollForDecisionTaskRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::validate_poll_request(&req.domain, req.task_list.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    crate::handlers::note_poller(
        state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "decision",
    );
    loop {
        if let Some(task) = executions.poll_decision(&req.domain, &task_list, &req.identity) {
            return Ok(wrap_success(&task));
        }
        if Instant::now() >= deadline {
            // Long-poll expiry yields an empty response, like Go matching.
            return Ok(wrap_success(
                &cadence_api::tasks::PollForDecisionTaskResponse::default(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn handle_respond_decision_completed(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::RespondDecisionTaskCompletedRequest;
    let req: RespondDecisionTaskCompletedRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state.executions.complete_decision(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_decision_failed(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::RespondDecisionTaskFailedRequest;
    let req: RespondDecisionTaskFailedRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state.executions.fail_decision(
        &req.task_token,
        req.cause,
        req.details.clone(),
        &req.identity,
        &req.binary_checksum,
    ) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_poll_activity(state: &AppState, body: &Struct, deadline: Instant) -> HandlerResult {
    use cadence_api::tasks::PollForActivityTaskRequest;
    use cadence_api::validation;
    let req: PollForActivityTaskRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::validate_poll_request(&req.domain, req.task_list.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    crate::handlers::note_poller(
        state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "activity",
    );
    loop {
        if let Some(task) = executions.poll_activity(&req.domain, &task_list, &req.identity) {
            return Ok(wrap_success(&task));
        }
        if Instant::now() >= deadline {
            return Ok(wrap_success(
                &cadence_api::tasks::PollForActivityTaskResponse::default(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn handle_respond_activity_completed(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskCompletedRequest;
    let req: RespondActivityTaskCompletedRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state.executions.complete_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_activity_failed(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskFailedRequest;
    let req: RespondActivityTaskFailedRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state.executions.fail_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_activity_canceled(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskCanceledRequest;
    let req: RespondActivityTaskCanceledRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state.executions.cancel_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_record_heartbeat(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::{
        RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse,
    };
    let req: RecordActivityTaskHeartbeatRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    require_task_domain_thrift(state, &req.task_token)?;
    match state
        .executions
        .record_heartbeat(&req.task_token, req.details.clone())
    {
        Ok(cancel_requested) => Ok(wrap_success(&RecordActivityTaskHeartbeatResponse {
            cancel_requested,
        })),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_start_async(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::{StartWorkflowExecutionAsyncRequest, StartWorkflowExecutionAsyncResponse};
    use cadence_api::validation::{self, Limits};
    let req: StartWorkflowExecutionAsyncRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let inner = req.request.ok_or_else(|| {
        AppError::bad_request("Start request is not set.".to_string())
    })?;
    let executions = require_domain_thrift(state, &inner.domain)?;
    validation::validate_start_workflow_execution_request(Some(&inner), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    executions.enqueue_async_start(inner);
    Ok(wrap_success(&StartWorkflowExecutionAsyncResponse {}))
}

async fn handle_signal_with_start_async(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::service_types::{
        SignalWithStartWorkflowExecutionAsyncRequest,
        SignalWithStartWorkflowExecutionAsyncResponse,
    };
    use cadence_api::validation::{self, Limits};
    let req: SignalWithStartWorkflowExecutionAsyncRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let inner = req.request.ok_or_else(|| {
        AppError::bad_request("Signal-with-start request is not set.".to_string())
    })?;
    let executions = require_domain_thrift(state, &inner.domain)?;
    validation::validate_signal_with_start_request(Some(&inner), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    executions.enqueue_async_signal_with_start(inner);
    Ok(wrap_success(&SignalWithStartWorkflowExecutionAsyncResponse {}))
}

async fn handle_refresh_workflow_tasks(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::RefreshWorkflowTasksRequest;
    use cadence_api::validation;
    let req: RefreshWorkflowTasksRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let execution = req.execution.clone().unwrap_or_default();
    executions
        .refresh_tasks(&req.domain, &execution)
        .map_err(mutation_error)?;
    Ok(Struct { fields: vec![] })
}

async fn handle_failover_domain(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::FailoverDomainRequest;
    use cadence_api::validation;
    let req: FailoverDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    validation::validate_failover_domain_request(Some(&req))
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.domains.failover(&req, &state.current_cluster_name, &state.cluster_metadata, &state.domain_policy(&req.domain_name)) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::FailoverError::NotFound(e)) => {
            Err(AppError::entity_not_exists(e.message))
        }
        Err(cadence_persistence::FailoverError::Busy(message)) => Err(AppError::busy(message)),
        Err(cadence_persistence::FailoverError::Invalid(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_list_failover_history(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::ListFailoverHistoryResponse;
    let req: cadence_api::types::ListFailoverHistoryRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let domain_id = match req.filters.as_ref() {
        Some(f) if !f.domain_id.is_empty() => f.domain_id.clone(),
        _ => {
            return Err(AppError::bad_request(
                "DomainID is required in filters".to_string(),
            ));
        }
    };
    let page_size = req
        .pagination
        .as_ref()
        .and_then(|p| p.page_size)
        .unwrap_or(50)
        .max(1) as usize;
    let page_token = req
        .pagination
        .as_ref()
        .map(|p| p.next_page_token.clone())
        .unwrap_or_default();
    match state
        .domains
        .failover_history(&domain_id, page_size, &page_token)
    {
        Ok((events, next_page_token)) => Ok(wrap_success(&ListFailoverHistoryResponse {
            failover_events: events,
            next_page_token,
        })),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

fn schedule_gate(state: &AppState, domain: &str) -> Result<(), AppError> {
    if state.scheduler_enabled {
        return Ok(());
    }
    Err(AppError::bad_request(format!(
        "Schedules are not enabled for domain {domain:?}. Set dynamic config worker.enableScheduler=true for this domain to enable them."
    )))
}

fn schedule_error(e: cadence_persistence::ScheduleError) -> AppError {
    match e {
        cadence_persistence::ScheduleError::NotFound(e) => {
            AppError::entity_not_exists(e.message)
        }
        cadence_persistence::ScheduleError::Invalid(message)
        | cadence_persistence::ScheduleError::Exists(message) => AppError::bad_request(message),
    }
}

async fn handle_create_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{CreateScheduleRequest, CreateScheduleResponse};
    use cadence_api::validation;
    let req: CreateScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    let spec = req.spec.as_ref().ok_or_else(|| {
        AppError::bad_request("Spec is not set on request.".to_string())
    })?;
    if spec.cron_expression.is_empty() {
        return Err(AppError::bad_request(
            "CronExpression is not set on request.".to_string(),
        ));
    }
    validate_schedule_cron_thrift(&spec.cron_expression)?;
    validation::validate_schedule_spec(Some(spec))
        .map_err(|e| AppError::bad_request(e.message))?;
    validation::validate_schedule_action_complete(req.action.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    validation::validate_schedule_policies(req.policies.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    validation::validate_schedule_search_attributes(req.search_attributes.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    executions
        .create_schedule(&req.domain, &req)
        .map(|id| wrap_success(&CreateScheduleResponse { schedule_id: id }))
        .map_err(schedule_error)
}

fn validate_schedule_cron_thrift(expr: &str) -> Result<(), AppError> {
    let now = cadence_common::cron::naive_from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    );
    cadence_common::cron::validate_schedule(expr, now)
        .map(|_| ())
        .map_err(AppError::bad_request)
}

async fn handle_describe_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::DescribeScheduleRequest;
    let req: DescribeScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    executions
        .describe_schedule(&req.domain, &req.schedule_id)
        .map(|resp| wrap_success(&resp))
        .map_err(schedule_error)
}

async fn handle_list_schedules(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{ListSchedulesRequest, ListSchedulesResponse};
    let req: ListSchedulesRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    let page_size = if req.page_size <= 0 { 10 } else { req.page_size as usize };
    let (schedules, next_page_token) =
        executions.list_schedules(&req.domain, page_size, &req.next_page_token);
    Ok(wrap_success(&ListSchedulesResponse {
        schedules,
        next_page_token,
    }))
}

async fn handle_delete_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{DeleteScheduleRequest, DeleteScheduleResponse};
    let req: DeleteScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    executions
        .delete_schedule(&req.domain, &req.schedule_id)
        .map(|()| wrap_success(&DeleteScheduleResponse {}))
        .map_err(schedule_error)
}

async fn handle_pause_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{PauseScheduleRequest, PauseScheduleResponse};
    let req: PauseScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    let actor = if req.identity.is_empty() {
        "unknown".to_string()
    } else {
        req.identity.clone()
    };
    executions
        .pause_schedule(&req.domain, &req.schedule_id, &req.reason, &actor)
        .map(|()| wrap_success(&PauseScheduleResponse {}))
        .map_err(schedule_error)
}

async fn handle_unpause_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{UnpauseScheduleRequest, UnpauseScheduleResponse};
    let req: UnpauseScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    executions
        .unpause_schedule(&req.domain, &req.schedule_id, req.catch_up_policy)
        .map(|()| wrap_success(&UnpauseScheduleResponse {}))
        .map_err(schedule_error)
}

async fn handle_backfill_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{BackfillScheduleRequest, BackfillScheduleResponse};
    let req: BackfillScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    let (start, end) = match (req.start_time_nano, req.end_time_nano) {
        (Some(s), Some(e)) => (s, e),
        (None, _) => {
            return Err(AppError::bad_request(
                "StartTime is not set on request.".to_string(),
            ));
        }
        (_, None) => {
            return Err(AppError::bad_request(
                "EndTime is not set on request.".to_string(),
            ));
        }
    };
    if end <= start {
        return Err(AppError::bad_request(
            "EndTime must be after StartTime.".to_string(),
        ));
    }
    let backfill_id = if req.backfill_id.trim().is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        req.backfill_id.trim().to_string()
    };
    executions
        .backfill_schedule(
            &req.domain,
            &req.schedule_id,
            start,
            end,
            req.overlap_policy,
            &backfill_id,
        )
        .map(|()| wrap_success(&BackfillScheduleResponse {}))
        .map_err(schedule_error)
}

async fn handle_update_schedule(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::schedule::{UpdateScheduleRequest, UpdateScheduleResponse};
    use cadence_api::validation;
    let req: UpdateScheduleRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    schedule_gate(state, &req.domain)?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.schedule_id.is_empty() {
        return Err(AppError::bad_request(
            "ScheduleID is not set on request.".to_string(),
        ));
    }
    if req.spec.is_none()
        && req.action.is_none()
        && req.policies.is_none()
        && req.search_attributes.is_none()
    {
        return Err(AppError::bad_request(
            "At least one of Spec, Action, Policies, or SearchAttributes must be set on request."
                .to_string(),
        ));
    }
    if let Some(spec) = req.spec.as_ref() {
        if !spec.cron_expression.is_empty() {
            validate_schedule_cron_thrift(&spec.cron_expression)?;
        }
        validation::validate_schedule_spec(Some(spec))
            .map_err(|e| AppError::bad_request(e.message))?;
    }
    if req.action.is_some() {
        validation::validate_schedule_action_complete(req.action.as_ref())
            .map_err(|e| AppError::bad_request(e.message))?;
    }
    validation::validate_schedule_policies(req.policies.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    validation::validate_schedule_search_attributes(req.search_attributes.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    executions
        .update_schedule(&req)
        .map(|()| wrap_success(&UpdateScheduleResponse {}))
        .map_err(schedule_error)
}

fn semaphore_gate(state: &AppState, domain: &str) -> Result<(), AppError> {
    if state.semaphore_enabled {
        return Ok(());
    }
    Err(AppError::bad_request(format!(
        "Semaphores are not enabled for domain {domain:?}. Set dynamic config system.enableDistributedSemaphore=true for this domain to enable them."
    )))
}

async fn handle_create_semaphore(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::{CreateSemaphoreRequest, CreateSemaphoreResponse, Semaphore};
    use cadence_api::validation;
    let req: CreateSemaphoreRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    semaphore_gate(state, &req.domain)?;
    validation::validate_create_semaphore_request(Some(&req))
        .map_err(|e| AppError::bad_request(e.message))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    match executions.create_semaphore(
        &req.domain,
        &req.semaphore_name,
        req.capacity,
        req.bucket_capacity,
    ) {
        Ok(meta) => Ok(wrap_success(&CreateSemaphoreResponse {
            semaphore: Some(Semaphore {
                semaphore_name: req.semaphore_name.clone(),
                capacity: meta.size,
                bucket_capacity: meta.bucket_size,
            }),
        })),
        Err(cadence_persistence::SemaphoreError::Exists(_)) => Err(AppError::bad_request(format!(
            "semaphore {:?} already exists in domain {:?}",
            req.semaphore_name, req.domain
        ))),
        Err(cadence_persistence::SemaphoreError::Invalid(message)) => {
            Err(AppError::bad_request(message))
        }
        Err(cadence_persistence::SemaphoreError::NotFound(e)) => {
            Err(AppError::entity_not_exists(e.message))
        }
    }
}

async fn handle_diagnose(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::enums::WorkflowIDReusePolicy;
    use cadence_api::types::{
        DiagnoseWorkflowExecutionRequest, DiagnoseWorkflowExecutionResponse,
        StartWorkflowExecutionRequest,
    };
    let req: DiagnoseWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    // Mirror Go: the target execution needs both workflow and run IDs.
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if req.domain.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    if execution.workflow_id.is_empty() || execution.run_id.is_empty() {
        return Err(AppError::bad_request(
            "Execution is not set on request.".to_string(),
        ));
    }
    require_domain_thrift(state, &req.domain)?;
    // The diagnostics starter runs in the system domain, like Go's
    // `constants.SystemLocalDomainName` ("cadence-system").
    let system_domain = "cadence-system";
    let executions = require_domain_thrift(state, system_domain)?;
    let diagnostic_id = format!("{}-{}", req.domain, execution.run_id);
    let input = serde_json::json!({
        "domain": req.domain,
        "workflowId": execution.workflow_id,
        "runId": execution.run_id,
        "identity": req.identity,
    });
    let start = StartWorkflowExecutionRequest {
        domain: system_domain.to_string(),
        workflow_id: diagnostic_id.clone(),
        workflow_type: Some(cadence_api::types::WorkflowType {
            name: "diagnostics-starter-workflow".to_string(),
        }),
        task_list: Some(cadence_api::types::TaskList {
            name: "diagnostics-wf-tasklist".to_string(),
            ..Default::default()
        }),
        input: serde_json::to_vec(&input).unwrap_or_default(),
        execution_start_to_close_timeout_seconds: Some(86400),
        task_start_to_close_timeout_seconds: Some(300),
        identity: req.identity.clone(),
        request_id: uuid::Uuid::new_v4().to_string(),
        workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::AllowDuplicate),
        ..Default::default()
    };
    match executions.start(&start) {
        Ok(resp) => Ok(wrap_success(&DiagnoseWorkflowExecutionResponse {
            domain: system_domain.to_string(),
            diagnostic_workflow_execution: Some(cadence_api::types::WorkflowExecution::new(
                diagnostic_id,
                resp.run_id,
            )),
        })),
        // A previous diagnosis is reused, like Go's AlreadyStarted branch.
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => {
            Ok(wrap_success(&DiagnoseWorkflowExecutionResponse {
                domain: system_domain.to_string(),
                diagnostic_workflow_execution: Some(
                    cadence_api::types::WorkflowExecution::new(diagnostic_id, e.run_id),
                ),
            }))
        }
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_reset(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::{ResetWorkflowExecutionRequest, ResetWorkflowExecutionResponse};
    use cadence_api::validation;
    let req: ResetWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    match executions.reset(&req) {
        Ok(resp) => Ok(wrap_success(&ResetWorkflowExecutionResponse {
            run_id: resp.run_id,
        })),
        Err(cadence_persistence::ResetError::NotFound(e)) => {
            Err(AppError::entity_not_exists(e.message))
        }
        Err(cadence_persistence::ResetError::Invalid(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_restart(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::types::{RestartWorkflowExecutionRequest, RestartWorkflowExecutionResponse};
    use cadence_api::validation;
    let req: RestartWorkflowExecutionRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let execution = req.workflow_execution.clone().unwrap_or_default();
    match executions.restart(&req.domain, &execution, &req.identity) {
        Ok(resp) => Ok(wrap_success(&RestartWorkflowExecutionResponse {
            run_id: resp.run_id,
        })),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => Err(AppError {
            exc_name: "WorkflowExecutionAlreadyStartedError",
            value: e.to_thrift(),
            message: e.message.clone(),
        }),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            Err(AppError::bad_request(message))
        }
    }
}

async fn handle_respond_query_completed(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::{RespondQueryTaskCompletedRequest, RespondQueryTaskCompletedResponse};
    let req: RespondQueryTaskCompletedRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request(
            "Task token not set on request.".to_string(),
        ));
    }
    match state.executions.answer_query_task(&req) {
        Ok(()) => Ok(wrap_success(&RespondQueryTaskCompletedResponse {})),
        Err(cadence_persistence::QueryError::NotFound(e)) => {
            Err(AppError::entity_not_exists(e.message))
        }
        Err(cadence_persistence::QueryError::Invalid(e)) => {
            Err(AppError::bad_request(e.message))
        }
        Err(cadence_persistence::QueryError::Rejected(_)) => Err(AppError::bad_request(
            "Query task was rejected.".to_string(),
        )),
    }
}

async fn handle_describe_task_list(state: &AppState, body: &Struct) -> HandlerResult {
    use cadence_api::tasks::DescribeTaskListRequest;
    let req: DescribeTaskListRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    if req.task_list.as_ref().is_none_or(|t| t.name.is_empty()) {
        return Err(AppError::bad_request(
            "TaskList is not set on request.".to_string(),
        ));
    }
    match crate::handlers::describe_task_list_response(state, &executions, &req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(crate::handlers::DescribeTaskListError::TaskListNotSet) => {
            Err(AppError::bad_request(
                "TaskList is not set on request.".to_string(),
            ))
        }
        Err(crate::handlers::DescribeTaskListError::DomainGone) => {
            Err(AppError::entity_not_exists("Domain does not exist.".to_string()))
        }
    }
}

async fn handle_get_task_lists_by_domain(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::{GetTaskListsByDomainRequest, GetTaskListsByDomainResponse};
    let req: GetTaskListsByDomainRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    let (decisions, activities) = executions.task_lists(&req.domain);
    let mut decision_task_list_map = std::collections::HashMap::new();
    for name in decisions {
        let desc = crate::handlers::describe_task_list_response(
            state,
            &executions,
            &cadence_api::tasks::DescribeTaskListRequest {
                domain: req.domain.clone(),
                task_list: Some(cadence_api::types::TaskList {
                    name: name.clone(),
                    ..Default::default()
                }),
                task_list_type: None,
                include_task_list_status: true,
            },
        )
        .map_err(|e| match e {
            crate::handlers::DescribeTaskListError::TaskListNotSet => {
                AppError::bad_request("TaskList is not set on request.".to_string())
            }
            crate::handlers::DescribeTaskListError::DomainGone => {
                AppError::entity_not_exists("Domain does not exist.".to_string())
            }
        })?;
        decision_task_list_map.insert(name, desc);
    }
    let mut activity_task_list_map = std::collections::HashMap::new();
    for name in activities {
        let desc = crate::handlers::describe_task_list_response(
            state,
            &executions,
            &cadence_api::tasks::DescribeTaskListRequest {
                domain: req.domain.clone(),
                task_list: Some(cadence_api::types::TaskList {
                    name: name.clone(),
                    ..Default::default()
                }),
                task_list_type: Some(cadence_api::enums::TaskListType::Activity),
                include_task_list_status: true,
            },
        )
        .map_err(|e| match e {
            crate::handlers::DescribeTaskListError::TaskListNotSet => {
                AppError::bad_request("TaskList is not set on request.".to_string())
            }
            crate::handlers::DescribeTaskListError::DomainGone => {
                AppError::entity_not_exists("Domain does not exist.".to_string())
            }
        })?;
        activity_task_list_map.insert(name, desc);
    }
    Ok(wrap_success(&GetTaskListsByDomainResponse {
        decision_task_list_map: Some(decision_task_list_map),
        activity_task_list_map: Some(activity_task_list_map),
    }))
}

async fn handle_list_task_list_partitions(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::{ListTaskListPartitionsRequest, ListTaskListPartitionsResponse};
    let req: ListTaskListPartitionsRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    let name = match req.task_list {
        Some(ref t) if !t.name.is_empty() => t.name.clone(),
        _ => {
            return Err(AppError::bad_request(
                "TaskList is not set on request.".to_string(),
            ));
        }
    };
    // Single-host deployment: every list has exactly its root partition.
    let owner = std::env::var("HOSTNAME").unwrap_or_default();
    let partition = cadence_api::tasks::TaskListPartitionMetadata {
        key: name,
        owner_host_name: owner,
    };
    Ok(wrap_success(&ListTaskListPartitionsResponse {
        activity_task_list_partitions: vec![partition.clone()],
        decision_task_list_partitions: vec![partition],
    }))
}

async fn handle_query(state: &AppState, body: &Struct, deadline: Instant) -> HandlerResult {
    use cadence_api::enums::QueryResultType;
    use cadence_api::tasks::{QueryWorkflowRequest, QueryWorkflowResponse};
    use cadence_api::validation;
    let req: QueryWorkflowRequest =
        decode_args(body).map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    let executions = require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution)).map_err(|e| AppError::bad_request(e.message))?;
    let query = match req.query.clone() {
        Some(q) if !q.query_type.is_empty() => q,
        _ => {
            return Err(AppError::bad_request(
                "WorkflowQuery is not set on request.".to_string(),
            ));
        }
    };
    let query_id = match executions.buffer_query(
        &req.domain,
        &execution,
        &query.query_type,
        query.query_args.clone(),
        req.query_reject_condition,
    ) {
        Ok(id) => id,
        Err(cadence_persistence::QueryError::NotFound(e)) => {
            return Err(AppError::entity_not_exists(e.message));
        }
        Err(cadence_persistence::QueryError::Invalid(e)) => {
            return Err(AppError::bad_request(e.message));
        }
        Err(cadence_persistence::QueryError::Rejected(status)) => {
            return Ok(wrap_success(&QueryWorkflowResponse {
                query_result: Vec::new(),
                query_rejected: Some(cadence_api::errors::QueryRejected {
                    close_status: status,
                }),
            }));
        }
    };
    loop {
        if let Some(result) = executions.take_query_result(&query_id) {
            match result.result_type {
                Some(QueryResultType::Answered) => {
                    return Ok(wrap_success(&QueryWorkflowResponse {
                        query_result: result.answer,
                        query_rejected: None,
                    }));
                }
                _ => {
                    return Err(AppError {
                        exc_name: "QueryFailedError",
                        value: cadence_api::errors::QueryFailedError {
                            message: if result.error_message.is_empty() {
                                "Query failed.".to_string()
                            } else {
                                result.error_message
                            },
                        }
                        .to_thrift(),
                        message: "Query failed.".to_string(),
                    });
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(AppError {
                exc_name: "QueryFailedError",
                value: cadence_api::errors::QueryFailedError {
                    message: "Query timed out.".to_string(),
                }
                .to_thrift(),
                message: "Query timed out.".to_string(),
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn mutation_to_thrift(r: Result<(), cadence_persistence::WorkflowMutationError>) -> HandlerResult {
    match r {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(mutation_error(e)),
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Dispatch one Thrift call; returns the REPLY body struct plus an optional
/// application error (exception name + details for headers).
pub async fn dispatch(
    state: &AppState,
    method: &str,
    body: &Struct,
    deadline: Instant,
) -> Result<(Struct, Option<(&'static str, String)>), String> {
    let outcome: HandlerResult = match method {
        "health" => Ok(Struct {
            fields: vec![(
                0,
                Value::Struct(Struct {
                    fields: vec![(1, Value::Bool(true)), (2, Value::Bytes(b"OK".to_vec()))],
                }),
            )],
        }),
        "RegisterDomain" => handle_register(state, body).await,
        "DescribeDomain" => handle_describe_domain(state, body).await,
        "ListDomains" => handle_list_domains(state, body).await,
        "ListWorkflowExecutions" | "ScanWorkflowExecutions" => {
            handle_visibility_list(state, body).await
        }
        "ListOpenWorkflowExecutions" => handle_visibility_open(state, body).await,
        "ListClosedWorkflowExecutions" => handle_visibility_closed(state, body).await,
        "ListArchivedWorkflowExecutions" => handle_visibility_archived(state, body).await,
        "CountWorkflowExecutions" => handle_visibility_count(state, body).await,
        "GetSearchAttributes" => Ok(wrap_success(&crate::handlers::search_attributes())),
        "UpdateDomain" => handle_update_domain(state, body).await,
        "DeprecateDomain" => handle_deprecate_domain(state, body).await,
        "DeleteDomain" => handle_delete_domain(state, body).await,
        "StartWorkflowExecution" => handle_start(state, body).await,
        "DescribeWorkflowExecution" => handle_describe_workflow(state, body).await,
        "GetWorkflowExecutionHistory" => handle_get_history(state, body, deadline).await,
        "SignalWorkflowExecution" => handle_signal(state, body).await,
        "SignalWithStartWorkflowExecution" => handle_signal_with_start(state, body).await,
        "TerminateWorkflowExecution" => handle_terminate(state, body).await,
        "RestartWorkflowExecution" => handle_restart(state, body).await,
        "ResetWorkflowExecution" => handle_reset(state, body).await,
        "RefreshWorkflowTasks" => handle_refresh_workflow_tasks(state, body).await,
        "DiagnoseWorkflowExecution" => handle_diagnose(state, body).await,
        "CreateSemaphore" => handle_create_semaphore(state, body).await,
        "CreateSchedule" => handle_create_schedule(state, body).await,
        "DescribeSchedule" => handle_describe_schedule(state, body).await,
        "ListSchedules" => handle_list_schedules(state, body).await,
        "DeleteSchedule" => handle_delete_schedule(state, body).await,
        "PauseSchedule" => handle_pause_schedule(state, body).await,
        "UnpauseSchedule" => handle_unpause_schedule(state, body).await,
        "BackfillSchedule" => handle_backfill_schedule(state, body).await,
        "UpdateSchedule" => handle_update_schedule(state, body).await,
        "FailoverDomain" => handle_failover_domain(state, body).await,
        "ListFailoverHistory" => handle_list_failover_history(state, body).await,
        "StartWorkflowExecutionAsync" => handle_start_async(state, body).await,
        "SignalWithStartWorkflowExecutionAsync" => handle_signal_with_start_async(state, body).await,
        "RequestCancelWorkflowExecution" => handle_request_cancel(state, body).await,
        "ResetStickyTaskList" => handle_reset_sticky(state, body).await,
        "PollForDecisionTask" => handle_poll_decision(state, body, deadline).await,
        "RespondDecisionTaskCompleted" => handle_respond_decision_completed(state, body).await,
        "RespondDecisionTaskFailed" => handle_respond_decision_failed(state, body).await,
        "PollForActivityTask" => handle_poll_activity(state, body, deadline).await,
        "RespondActivityTaskCompleted" => handle_respond_activity_completed(state, body).await,
        "RespondActivityTaskFailed" => handle_respond_activity_failed(state, body).await,
        "RespondActivityTaskCanceled" => handle_respond_activity_canceled(state, body).await,
        "RecordActivityTaskHeartbeat" => handle_record_heartbeat(state, body).await,
        "DescribeTaskList" => handle_describe_task_list(state, body).await,
        "GetTaskListsByDomain" => handle_get_task_lists_by_domain(state, body).await,
        "ListTaskListPartitions" => handle_list_task_list_partitions(state, body).await,
        "RespondQueryTaskCompleted" => handle_respond_query_completed(state, body).await,
        "QueryWorkflow" => handle_query(state, body, deadline).await,
        _ => return Err(format!("unknown method {method}")),
    };
    let def = find_method(method).ok_or_else(|| format!("unknown method {method}"))?;
    match outcome {
        Ok(success) => Ok((success, None)),
        Err(e) => {
            let field = cadence_thrift::service::exception_field(def, e.exc_name)
                .ok_or_else(|| format!("error {} not declared for {method}", e.exc_name))?;
            Ok((
                Struct {
                    fields: vec![(field, Value::Struct(e.value))],
                },
                Some((e.exc_name, e.message)),
            ))
        }
    }
}

/// Axum entry point for YARPC Thrift-over-HTTP calls.
pub async fn yarpc_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let service_echo = header(&headers, "Rpc-Service").unwrap_or(SERVICE);
    if service_echo == "cadence-matching" && state.matching_http_isolated {
        return transport_error(
            StatusCode::NOT_FOUND,
            "Matching YARPC is served on its configured listener".to_string(),
        );
    }
    let Some(procedure) = header(&headers, "Rpc-Procedure") else {
        return transport_error(StatusCode::BAD_REQUEST, "missing Rpc-Procedure".to_string());
    };
    if state.matching_http_isolated
        && find_method(procedure)
            .is_some_and(|method| matches!(method.service, "MatchingService" | "Meta"))
    {
        return transport_error(
            StatusCode::NOT_FOUND,
            "Matching YARPC is served on its configured listener".to_string(),
        );
    }
    match header(&headers, "Rpc-Encoding") {
        Some("thrift") => {}
        Some(other) => {
            return transport_error(
                StatusCode::BAD_REQUEST,
                format!("unsupported Rpc-Encoding {other:?}; want \"thrift\""),
            );
        }
        None => {
            return transport_error(StatusCode::BAD_REQUEST, "missing Rpc-Encoding".to_string());
        }
    }
    if find_method(procedure).is_none() {
        return transport_error(
            StatusCode::NOT_FOUND,
            format!("unknown procedure {procedure}"),
        );
    }
    let method = procedure
        .rsplit("::")
        .next()
        .unwrap_or(procedure)
        .to_string();
    let mut r = Reader::new(&body);
    let args = match read_struct(&mut r) {
        Ok(v) => v,
        Err(e) => {
            return transport_error(
                StatusCode::BAD_REQUEST,
                format!("bad args: {e} body_len={}", body.len()),
            )
        }
    };
    let deadline = ttl_deadline(&headers, Duration::from_secs(60));
    match dispatch(&state, &method, &args, deadline).await {
        Ok((result, app_error)) => ok_reply(service_echo, result, app_error),
        Err(e) => transport_error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
