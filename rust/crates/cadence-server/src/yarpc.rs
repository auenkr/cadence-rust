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
use cadence_thrift::protocol::{Reader, Writer};
use cadence_thrift::service::find_method;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value, read_struct, write_struct};

use crate::handlers::AppState;

const SERVICE: &str = "cadence-frontend";

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn ttl_deadline(headers: &HeaderMap, default: Duration) -> Instant {
    let millis: u64 = header(headers, "Context-TTL-MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(default.as_millis() as u64)
        .clamp(100, 300_000);
    Instant::now() + Duration::from_millis(millis)
}

fn ok_reply(
    service_echo: &str,
    result: Struct,
    app_error: Option<(&str, String)>,
) -> Response {
    let mut w = Writer::new();
    write_struct(&mut w, &result);
    let mut resp = w.into_bytes().into_response();
    let headers = resp.headers_mut();
    headers.insert("Rpc-Status", (if app_error.is_some() { "error" } else { "success" }).parse().unwrap());
    headers.insert("Rpc-Service", service_echo.parse().unwrap_or_else(|_| "cadence-frontend".parse().unwrap()));
    if let Some((name, details)) = app_error {
        headers.insert("Rpc-Application-Error-Name", name.parse().unwrap_or_else(|_| "Unknown".parse().unwrap()));
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

fn transport_error(status: StatusCode, message: String) -> Response {
    let mut resp = (status, message).into_response();
    resp.headers_mut().insert("Rpc-Status", "error".parse().unwrap());
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
        let e = BadRequestError { message: message.clone() };
        Self {
            exc_name: "BadRequestError",
            value: e.to_thrift(),
            message,
        }
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
) -> Result<(), AppError> {
    if domain.is_empty() {
        return Err(AppError::bad_request(
            "Domain not set on request.".to_string(),
        ));
    }
    state
        .domains
        .domain_id(domain)
        .map(|_| ())
        .map_err(|e| AppError::entity_not_exists(e.message))
}

// ---------------------------------------------------------------------------
// Per-method handlers
// ---------------------------------------------------------------------------

async fn handle_start(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::types::StartWorkflowExecutionRequest;
    use cadence_api::validation::{self, Limits};
    let req: StartWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::validate_start_workflow_execution_request(Some(&req), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.start(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => Err(AppError {
            exc_name: "WorkflowExecutionAlreadyStartedError",
            value: e.to_thrift(),
            message: e.message.clone(),
        }),
    }
}

async fn handle_register(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::enums::DomainStatus;
    use cadence_api::types::RegisterDomainRequest;
    let req: RegisterDomainRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request("Domain not set on request.".to_string()));
    }
    if req.workflow_execution_retention_period_in_days <= 0 {
        return Err(AppError::bad_request("RetentionDays is invalid.".to_string()));
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
        workflow_execution_retention_period_in_days: req.workflow_execution_retention_period_in_days,
        emit_metric: req.emit_metric.unwrap_or(true),
        history_archival_status: req.history_archival_status,
        history_archival_uri: req.history_archival_uri.clone(),
        visibility_archival_status: req.visibility_archival_status,
        visibility_archival_uri: req.visibility_archival_uri.clone(),
        ..Default::default()
    };
    let replication = cadence_api::types::DomainReplicationConfiguration {
        active_cluster_name: req.active_cluster_name.clone(),
        clusters: req.clusters.clone(),
        ..Default::default()
    };
    match state.domains.create(info, configuration, replication, req.is_global_domain) {
        Ok(_) => {
            state.executions.register_domain_name(&req.name);
            // void method: empty success struct.
            Ok(Struct { fields: vec![] })
        }
        Err(e) => Err(AppError {
            exc_name: "DomainAlreadyExistsError",
            value: cadence_api::errors::DomainAlreadyExistsError { message: e.message.clone() }
                .to_thrift(),
            message: e.message,
        }),
    }
}

async fn handle_describe_domain(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::DescribeDomainRequest;
    let req: DescribeDomainRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_none() && req.uuid.is_none() {
        return Err(AppError::bad_request("Domain not set on request.".to_string()));
    }
    match state.domains.describe(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_list_domains(
    state: &AppState,
    _body: &Struct,
) -> HandlerResult {
    Ok(wrap_success(&state.domains.list()))
}

async fn handle_update_domain(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::UpdateDomainRequest;
    let req: UpdateDomainRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request("Domain not set on request.".to_string()));
    }
    match state.domains.update(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_deprecate_domain(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::DeprecateDomainRequest;
    let req: DeprecateDomainRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.name.is_empty() {
        return Err(AppError::bad_request("Domain not set on request.".to_string()));
    }
    match state.domains.deprecate(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_signal(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::types::SignalWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: SignalWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    if req.signal_name.is_empty() {
        return Err(AppError::bad_request(
            "SignalName is not set on request.".to_string(),
        ));
    }
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    mutation_to_thrift(state.executions.signal(&req))
}

async fn handle_signal_with_start(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
    use cadence_api::validation::{self, Limits};
    let req: SignalWithStartWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::validate_signal_with_start_request(Some(&req), &Limits::cadence_default())
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.signal_with_start(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => Err(AppError {
            exc_name: "WorkflowExecutionAlreadyStartedError",
            value: e.to_thrift(),
            message: e.message.clone(),
        }),
    }
}

async fn handle_terminate(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::types::TerminateWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: TerminateWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.terminate(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(mutation_error(e)),
    }
}

async fn handle_request_cancel(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::RequestCancelWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: RequestCancelWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::check_execution(req.workflow_execution.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.request_cancel(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(mutation_error(e)),
    }
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

async fn handle_describe_workflow(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::DescribeWorkflowExecutionRequest;
    use cadence_api::validation;
    let req: DescribeWorkflowExecutionRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution))
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.describe(&req.domain, &execution) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(AppError::entity_not_exists(e.message)),
    }
}

async fn handle_get_history(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::service_types::GetWorkflowExecutionHistoryRequest;
    use cadence_api::validation;
    let req: GetWorkflowExecutionHistoryRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution))
        .map_err(|e| AppError::bad_request(e.message))?;
    match state.executions.history_page(
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

async fn handle_poll_decision(
    state: &AppState,
    body: &Struct,
    deadline: Instant,
) -> HandlerResult {
    use cadence_api::tasks::PollForDecisionTaskRequest;
    use cadence_api::validation;
    let req: PollForDecisionTaskRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::validate_poll_request(&req.domain, req.task_list.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let task_list = req.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
    crate::handlers::note_poller(state, &task_list, &req.identity, "decision");
    loop {
        if let Some(task) = state.executions.poll_decision(&req.domain, &task_list, &req.identity)
        {
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

async fn handle_respond_decision_completed(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::RespondDecisionTaskCompletedRequest;
    let req: RespondDecisionTaskCompletedRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
    match state.executions.complete_decision(&req) {
        Ok(resp) => Ok(wrap_success(&resp)),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_decision_failed(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::RespondDecisionTaskFailedRequest;
    let req: RespondDecisionTaskFailedRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
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

async fn handle_poll_activity(
    state: &AppState,
    body: &Struct,
    deadline: Instant,
) -> HandlerResult {
    use cadence_api::tasks::PollForActivityTaskRequest;
    use cadence_api::validation;
    let req: PollForActivityTaskRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    validation::validate_poll_request(&req.domain, req.task_list.as_ref())
        .map_err(|e| AppError::bad_request(e.message))?;
    let task_list = req.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
    crate::handlers::note_poller(state, &task_list, &req.identity, "activity");
    loop {
        if let Some(task) = state.executions.poll_activity(&req.domain, &task_list, &req.identity)
        {
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

async fn handle_respond_activity_completed(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskCompletedRequest;
    let req: RespondActivityTaskCompletedRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
    match state.executions.complete_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_activity_failed(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskFailedRequest;
    let req: RespondActivityTaskFailedRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
    match state.executions.fail_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_respond_activity_canceled(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::RespondActivityTaskCanceledRequest;
    let req: RespondActivityTaskCanceledRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
    match state.executions.cancel_activity(&req) {
        Ok(()) => Ok(Struct { fields: vec![] }),
        Err(e) => Err(task_error(e)),
    }
}

async fn handle_record_heartbeat(
    state: &AppState,
    body: &Struct,
) -> HandlerResult {
    use cadence_api::tasks::{RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse};
    let req: RecordActivityTaskHeartbeatRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    if req.task_token.is_empty() {
        return Err(AppError::bad_request("Task token not set on request.".to_string()));
    }
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

async fn handle_query(
    state: &AppState,
    body: &Struct,
    deadline: Instant,
) -> HandlerResult {
    use cadence_api::enums::QueryResultType;
    use cadence_api::tasks::{QueryWorkflowRequest, QueryWorkflowResponse};
    use cadence_api::validation;
    let req: QueryWorkflowRequest = decode_args(body)
        .map_err(|e| AppError::bad_request(format!("invalid args: {e}")))?;
    require_domain_thrift(state, &req.domain)?;
    let execution = req.execution.clone().unwrap_or_default();
    validation::check_execution(Some(&execution))
        .map_err(|e| AppError::bad_request(e.message))?;
    let query = match req.query.clone() {
        Some(q) if !q.query_type.is_empty() => q,
        _ => {
            return Err(AppError::bad_request(
                "WorkflowQuery is not set on request.".to_string(),
            ));
        }
    };
    let query_id = match state.executions.buffer_query(
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
        if let Some(result) = state.executions.take_query_result(&query_id) {
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

fn mutation_to_thrift(
    r: Result<(), cadence_persistence::WorkflowMutationError>,
) -> HandlerResult {
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
        "RegisterDomain" => handle_register(state, body).await,
        "DescribeDomain" => handle_describe_domain(state, body).await,
        "ListDomains" => handle_list_domains(state, body).await,
        "UpdateDomain" => handle_update_domain(state, body).await,
        "DeprecateDomain" => handle_deprecate_domain(state, body).await,
        "StartWorkflowExecution" => handle_start(state, body).await,
        "DescribeWorkflowExecution" => handle_describe_workflow(state, body).await,
        "GetWorkflowExecutionHistory" => handle_get_history(state, body).await,
        "SignalWorkflowExecution" => handle_signal(state, body).await,
        "SignalWithStartWorkflowExecution" => handle_signal_with_start(state, body).await,
        "TerminateWorkflowExecution" => handle_terminate(state, body).await,
        "RequestCancelWorkflowExecution" => handle_request_cancel(state, body).await,
        "PollForDecisionTask" => handle_poll_decision(state, body, deadline).await,
        "RespondDecisionTaskCompleted" => handle_respond_decision_completed(state, body).await,
        "RespondDecisionTaskFailed" => handle_respond_decision_failed(state, body).await,
        "PollForActivityTask" => handle_poll_activity(state, body, deadline).await,
        "RespondActivityTaskCompleted" => handle_respond_activity_completed(state, body).await,
        "RespondActivityTaskFailed" => handle_respond_activity_failed(state, body).await,
        "RespondActivityTaskCanceled" => handle_respond_activity_canceled(state, body).await,
        "RecordActivityTaskHeartbeat" => handle_record_heartbeat(state, body).await,
        "QueryWorkflow" => handle_query(state, body, deadline).await,
        _ => return Err(format!("unknown method {method}")),
    };
    let def = find_method(method).ok_or_else(|| format!("unknown method {method}"))?;
    match outcome {
        Ok(success) => Ok((success, None)),
        Err(e) => {
            let field = cadence_thrift::service::exception_field(def, e.exc_name).ok_or_else(
                || format!("error {} not declared for {method}", e.exc_name),
            )?;
            Ok((
                Struct {
                    fields: vec![(
                        field,
                        Value::Struct(e.value),
                    )],
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
    let Some(procedure) = header(&headers, "Rpc-Procedure") else {
        return transport_error(StatusCode::BAD_REQUEST, "missing Rpc-Procedure".to_string());
    };
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
        return transport_error(StatusCode::NOT_FOUND, format!("unknown procedure {procedure}"));
    }
    let method = procedure.rsplit("::").next().unwrap_or(procedure).to_string();
    let mut r = Reader::new(&body);
    let args = match read_struct(&mut r) {
        Ok(v) => v,
        Err(e) => return transport_error(StatusCode::BAD_REQUEST, format!("bad args: {e} body_len={}", body.len())),
    };
    let deadline = ttl_deadline(&headers, Duration::from_secs(60));
    match dispatch(&state, &method, &args, deadline).await {
        Ok((result, app_error)) => ok_reply(service_echo, result, app_error),
        Err(e) => transport_error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
