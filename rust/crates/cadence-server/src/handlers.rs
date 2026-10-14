//! HTTP JSON API served by the Rust Cadence server.
//!
//! Routes mirror the Go frontend `WorkflowHandler` methods; request/response
//! shapes are the Thrift-compatible types in `cadence-api`, so SDK payloads
//! keep the same field names. Errors are returned as JSON
//! `{"type": ..., "message": ...}` with a status code derived from the Go
//! error kind (400 bad request, 404 entity-not-exists, 409 already-started /
//! already-completed, 503 service-busy/shutting-down, 500 otherwise).
//!
//! Thrift-binary and gRPC transports are follow-ups; the JSON shapes they
//! carry are already the ones defined in `cadence-api`.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use cadence_api::enums::DomainStatus;
use cadence_api::errors::{
    BadRequestError, DomainAlreadyExistsError, EntityNotExistsError,
    WorkflowExecutionAlreadyCompletedError, WorkflowExecutionAlreadyStartedError,
};
use cadence_api::service_types::{
    DeleteDomainRequest, DeprecateDomainRequest, DescribeDomainRequest,
    RequestCancelWorkflowExecutionRequest, UpdateDomainRequest,
};
use cadence_api::types::{
    RegisterDomainRequest, SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
};
use cadence_api::validation::{self, Limits};
use cadence_persistence::{
    InMemoryDomainStore, InMemoryExecutionStore, TaskError, WorkflowMutationError,
};
use serde::Deserialize;

/// Recently seen pollers: (task-list, identity, kind) -> last-seen millis.
/// Mirrors the poller list Go's matching service reports.
type PollerMap = std::collections::HashMap<(String, String, String), i64>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub domains: InMemoryDomainStore,
    pub executions: InMemoryExecutionStore,
    pub limits: Limits,
    pub pollers: Arc<std::sync::Mutex<PollerMap>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            domains: InMemoryDomainStore::new(),
            executions: InMemoryExecutionStore::new(),
            limits: Limits::cadence_default(),
            pollers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn note_poller(state: &AppState, task_list: &str, identity: &str, kind: &str) {
    if task_list.is_empty() || identity.is_empty() {
        return;
    }
    state.pollers.lock().expect("poller lock").insert(
        (
            task_list.to_string(),
            identity.to_string(),
            kind.to_string(),
        ),
        now_millis(),
    );
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    kind: &'static str,
    message: String,
}

fn error_response(kind: &'static str, status: StatusCode, message: String) -> Response {
    (status, Json(ErrorBody { kind, message })).into_response()
}

fn bad_request(e: BadRequestError) -> Response {
    error_response("BadRequestError", StatusCode::BAD_REQUEST, e.message)
}

fn entity_not_exists(e: EntityNotExistsError) -> Response {
    error_response("EntityNotExistsError", StatusCode::NOT_FOUND, e.message)
}

fn task_error(e: TaskError) -> Response {
    match e {
        TaskError::InvalidToken(b) | TaskError::BadDecision(b) => bad_request(b),
        TaskError::NotFound(n) => entity_not_exists(n),
        TaskError::AlreadyCompleted(c) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::CONFLICT,
            c.message,
        ),
    }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true}))
}

// ---------------------------------------------------------------------------
// Domains
// ---------------------------------------------------------------------------

/// Mirrors Go `ValidateRegisterDomainRequest` core checks + `CreateDomain`.
async fn register_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if req.name.len() > state.limits.ids.domain_max {
        return bad_request(BadRequestError::new("Domain length exceeds limit."));
    }
    if req.workflow_execution_retention_period_in_days <= 0 {
        return bad_request(BadRequestError::new("RetentionDays is invalid."));
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
    let replication = cadence_api::types::DomainReplicationConfiguration {
        active_cluster_name: req.active_cluster_name.clone(),
        clusters: req.clusters.clone(),
        ..Default::default()
    };
    match state
        .domains
        .create(info, configuration, replication, req.is_global_domain)
    {
        Ok(resp) => {
            state.executions.register_domain_name(&req.name);
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(DomainAlreadyExistsError { message }) => {
            error_response("DomainAlreadyExistsError", StatusCode::CONFLICT, message)
        }
    }
}

#[derive(Debug, Deserialize)]
struct DescribeDomainQuery {
    name: Option<String>,
    uuid: Option<String>,
}

async fn describe_domain(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DescribeDomainQuery>,
) -> Response {
    if q.name.is_none() && q.uuid.is_none() {
        return bad_request(validation::err_domain_not_set());
    }
    match state.domains.describe(&DescribeDomainRequest {
        name: q.name,
        uuid: q.uuid,
    }) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => entity_not_exists(e),
    }
}

async fn list_domains(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.domains.list())
}

async fn update_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UpdateDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    match state.domains.update(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => entity_not_exists(e),
    }
}

async fn deprecate_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeprecateDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    match state.domains.deprecate(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => entity_not_exists(e),
    }
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

async fn require_domain(state: &AppState, domain: &str) -> Option<Response> {
    if domain.is_empty() {
        return Some(bad_request(validation::err_domain_not_set()));
    }
    state.domains.domain_id(domain).err().map(entity_not_exists)
}

async fn start_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if let Err(e) = validation::validate_start_workflow_execution_request(Some(&req), &state.limits)
    {
        return bad_request(e);
    }
    match state.executions.start(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(
            WorkflowExecutionAlreadyStartedError { message, .. },
        )) => error_response(
            "WorkflowExecutionAlreadyStartedError",
            StatusCode::CONFLICT,
            message,
        ),
    }
}

async fn describe_workflow(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DescribeWorkflowExecutionQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    let execution = cadence_api::types::WorkflowExecution::new(q.workflow_id, q.run_id);
    if let Err(e) = validation::check_execution(Some(&execution)) {
        // Empty workflow ID is a 400; a bad run ID is a 400 too.
        return bad_request(e);
    }
    match state.executions.describe(&q.domain, &execution) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => entity_not_exists(e),
    }
}

#[derive(Debug, Deserialize)]
struct DescribeWorkflowExecutionQuery {
    domain: String,
    #[serde(rename = "workflowId", default)]
    workflow_id: String,
    #[serde(rename = "runId", default)]
    run_id: String,
}

async fn signal_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SignalWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if req.signal_name.is_empty() {
        return bad_request(validation::err_signal_name_not_set());
    }
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match state.executions.signal(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(WorkflowMutationError::AlreadyCompleted(WorkflowExecutionAlreadyCompletedError {
            message,
        })) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::CONFLICT,
            message,
        ),
    }
}

async fn request_cancel_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RequestCancelWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match state.executions.request_cancel(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(WorkflowMutationError::AlreadyCompleted(WorkflowExecutionAlreadyCompletedError {
            message,
        })) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::CONFLICT,
            message,
        ),
    }
}

async fn terminate_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::TerminateWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match state.executions.terminate(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(WorkflowMutationError::AlreadyCompleted(WorkflowExecutionAlreadyCompletedError {
            message,
        })) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::CONFLICT,
            message,
        ),
    }
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    domain: String,
    #[serde(rename = "workflowId", default)]
    workflow_id: String,
    #[serde(rename = "runId", default)]
    run_id: String,
    #[serde(rename = "maximumPageSize", default)]
    maximum_page_size: i32,
    #[serde(rename = "nextPageToken", default)]
    next_page_token: String,
    #[serde(
        rename = "HistoryEventFilterType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    history_event_filter_type: Option<cadence_api::enums::HistoryEventFilterType>,
}

async fn get_history(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HistoryQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    let execution = cadence_api::types::WorkflowExecution::new(q.workflow_id, q.run_id);
    if let Err(e) = validation::check_execution(Some(&execution)) {
        return bad_request(e);
    }
    let token = if q.next_page_token.is_empty() {
        Vec::new()
    } else {
        match base64_decode(&q.next_page_token) {
            Some(b) => b,
            None => {
                return bad_request(BadRequestError::new("Invalid NextPageToken."));
            }
        }
    };
    match state.executions.history_page(
        &q.domain,
        &execution,
        q.maximum_page_size,
        &token,
        q.history_event_filter_type,
    )
    {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::HistoryPageError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::HistoryPageError::InvalidToken(e)) => bad_request(e),
    }
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    domain: String,
    #[serde(rename = "status", default = "default_list_status")]
    status: String,
    #[serde(rename = "workflowId", default)]
    workflow_id: Option<String>,
    #[serde(rename = "workflowType", default)]
    workflow_type: Option<String>,
    #[serde(rename = "closeStatus", default)]
    close_status: Option<i32>,
}

fn default_list_status() -> String {
    "open".to_string()
}

async fn list_workflows(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ListQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    match q.status.as_str() {
        "open" | "closed" => {
            let open_only = q.status.as_str() == "open";
            let close_status = q
                .close_status
                .and_then(cadence_api::enums::WorkflowExecutionCloseStatus::from_i32);
            let executions = state.executions.list_filtered(
                &q.domain,
                open_only,
                q.workflow_id.as_deref(),
                q.workflow_type.as_deref(),
                close_status,
            );
            (StatusCode::OK, Json(executions)).into_response()
        }
        other => bad_request(BadRequestError::new(format!(
            "invalid status filter {other:?}; want \"open\" or \"closed\""
        ))),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    let shared = Arc::new(state);
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/domains/register", post(register_domain))
        .route("/api/v1/domains/describe", get(describe_domain))
        .route("/api/v1/domains/list", get(list_domains))
        .route("/api/v1/domains/update", put(update_domain))
        .route("/api/v1/domains/deprecate", post(deprecate_domain))
        .route("/api/v1/workflows/start", post(start_workflow))
        .route("/api/v1/workflows/describe", get(describe_workflow))
        .route("/api/v1/workflows/signal", post(signal_workflow))
        .route(
            "/api/v1/workflows/request-cancel",
            post(request_cancel_workflow),
        )
        .route("/api/v1/workflows/terminate", post(terminate_workflow))
        .route("/api/v1/workflows/history", get(get_history))
        .route("/api/v1/workflows/list", get(list_workflows))
        .route(
            "/api/v1/workflows/signal-with-start",
            post(signal_with_start_workflow),
        )
        .route("/api/v1/tasks/poll-decision", post(poll_decision_task))
        .route(
            "/api/v1/tasks/respond-decision-complete",
            post(respond_decision_task_completed),
        )
        .route(
            "/api/v1/tasks/respond-decision-failed",
            post(respond_decision_task_failed),
        )
        .route("/api/v1/tasks/poll-activity", post(poll_activity_task))
        .route(
            "/api/v1/tasks/respond-activity-complete",
            post(respond_activity_task_completed),
        )
        .route(
            "/api/v1/tasks/respond-activity-failed",
            post(respond_activity_task_failed),
        )
        .route(
            "/api/v1/tasks/respond-activity-canceled",
            post(respond_activity_task_canceled),
        )
        .route(
            "/api/v1/tasks/record-activity-heartbeat",
            post(record_activity_task_heartbeat),
        )
        .route("/api/v1/workflows/query", post(query_workflow))
        .route("/api/v1/domains/delete", post(delete_domain))
        .route("/api/v1/tasklists/describe", post(describe_task_list))
        .route("/api/v1/search-attributes", get(get_search_attributes))
        .route("/api/v1/workflows/count", get(count_workflows))
        .route("/", post(crate::yarpc::yarpc_handler))
        .route("/{*rest}", post(crate::yarpc::yarpc_handler))
        .with_state(shared)
}

// ---------------------------------------------------------------------------
// Tasks (matching)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PollQuery {
    #[serde(rename = "timeoutSecs", default = "default_poll_timeout")]
    timeout_secs: u64,
}

fn default_poll_timeout() -> u64 {
    60
}

/// Long-poll for a task: return the first available task, or 204 when the
/// timeout elapses. Mirrors the matching-service long-poll behavior.
async fn long_poll<T, F>(timeout_secs: u64, mut poll: F) -> Response
where
    F: FnMut() -> Option<T>,
    T: serde::Serialize,
{
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs.min(300));
    loop {
        if let Some(task) = poll() {
            return (StatusCode::OK, Json(task)).into_response();
        }
        if tokio::time::Instant::now() >= deadline {
            return StatusCode::NO_CONTENT.into_response();
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn poll_decision_task(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PollQuery>,
    Json(req): Json<cadence_api::tasks::PollForDecisionTaskRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        // require_domain returns Option; keep the same shape as other handlers.
        return e;
    }
    if let Err(e) = validation::validate_poll_request(&req.domain, req.task_list.as_ref()) {
        return bad_request(e);
    }
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    note_poller(&state, &task_list, &req.identity, "decision");
    long_poll(q.timeout_secs, || {
        state
            .executions
            .poll_decision(&req.domain, &task_list, &req.identity)
    })
    .await
}

async fn respond_decision_task_completed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondDecisionTaskCompletedRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.complete_decision(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => task_error(e),
    }
}

async fn respond_decision_task_failed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondDecisionTaskFailedRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.fail_decision(
        &req.task_token,
        req.cause,
        req.details.clone(),
        &req.identity,
        &req.binary_checksum,
    ) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => task_error(e),
    }
}

async fn poll_activity_task(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PollQuery>,
    Json(req): Json<cadence_api::tasks::PollForActivityTaskRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if let Err(e) = validation::validate_poll_request(&req.domain, req.task_list.as_ref()) {
        return bad_request(e);
    }
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    note_poller(&state, &task_list, &req.identity, "activity");
    long_poll(q.timeout_secs, || {
        state
            .executions
            .poll_activity(&req.domain, &task_list, &req.identity)
    })
    .await
}

async fn respond_activity_task_completed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondActivityTaskCompletedRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.complete_activity(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => task_error(e),
    }
}

async fn respond_activity_task_failed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondActivityTaskFailedRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.fail_activity(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => task_error(e),
    }
}

async fn respond_activity_task_canceled(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondActivityTaskCanceledRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.cancel_activity(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => task_error(e),
    }
}

async fn record_activity_task_heartbeat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RecordActivityTaskHeartbeatRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state
        .executions
        .record_heartbeat(&req.task_token, req.details.clone())
    {
        Ok(cancel_requested) => (
            StatusCode::OK,
            Json(cadence_api::tasks::RecordActivityTaskHeartbeatResponse { cancel_requested }),
        )
            .into_response(),
        Err(e) => task_error(e),
    }
}

async fn signal_with_start_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::service_types::SignalWithStartWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if let Err(e) = validation::validate_signal_with_start_request(Some(&req), &state.limits) {
        return bad_request(e);
    }
    match state.executions.signal_with_start(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(
            WorkflowExecutionAlreadyStartedError { message, .. },
        )) => error_response(
            "WorkflowExecutionAlreadyStartedError",
            StatusCode::CONFLICT,
            message,
        ),
    }
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// Direct query: buffer on the execution and long-poll for the worker's
/// answer, mirroring Go's frontend query path (reject in-band, answers
/// and failures mapped to Go error shapes).
async fn query_workflow(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PollQuery>,
    Json(req): Json<cadence_api::tasks::QueryWorkflowRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let execution = req.execution.clone().unwrap_or_default();
    if let Err(e) = validation::check_execution(Some(&execution)) {
        return bad_request(e);
    }
    let query = match req.query.clone() {
        Some(q) if !q.query_type.is_empty() => q,
        _ => {
            return bad_request(BadRequestError::new("WorkflowQuery is not set on request."));
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
        Err(cadence_persistence::QueryError::NotFound(e)) => return entity_not_exists(e),
        Err(cadence_persistence::QueryError::Invalid(e)) => return bad_request(e),
        Err(cadence_persistence::QueryError::Rejected(status)) => {
            return (
                StatusCode::OK,
                Json(cadence_api::tasks::QueryWorkflowResponse {
                    query_result: Vec::new(),
                    query_rejected: Some(cadence_api::errors::QueryRejected {
                        close_status: status,
                    }),
                }),
            )
                .into_response();
        }
    };
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(q.timeout_secs.min(300));
    loop {
        if let Some(result) = state.executions.take_query_result(&query_id) {
            match result.result_type {
                Some(cadence_api::enums::QueryResultType::Answered) => {
                    return (
                        StatusCode::OK,
                        Json(cadence_api::tasks::QueryWorkflowResponse {
                            query_result: result.answer,
                            query_rejected: None,
                        }),
                    )
                        .into_response();
                }
                _ => {
                    return error_response(
                        "QueryFailedError",
                        StatusCode::BAD_REQUEST,
                        if result.error_message.is_empty() {
                            "Query failed.".to_string()
                        } else {
                            result.error_message
                        },
                    );
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return error_response(
                "QueryFailedError",
                StatusCode::BAD_REQUEST,
                "Query timed out.".to_string(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Admin / visibility
// ---------------------------------------------------------------------------

async fn delete_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeleteDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    match state.domains.delete(&req.name) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => entity_not_exists(e),
    }
}

async fn describe_task_list(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::DescribeTaskListRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let task_list_name = match req.task_list.clone() {
        Some(t) if !t.name.is_empty() => t.name,
        _ => return bad_request(validation::err_task_list_not_set()),
    };
    let (decision_backlog, activity_backlog) = state
        .executions
        .task_list_backlog(&req.domain, &task_list_name);
    let backlog = match req.task_list_type {
        Some(cadence_api::enums::TaskListType::Activity) => activity_backlog,
        _ => decision_backlog,
    };
    // Pollers seen in the last minute on this task list, like Go matching.
    let cutoff = now_millis() - 60_000;
    let mut pollers: Vec<cadence_api::types::PollerInfo> = state
        .pollers
        .lock()
        .expect("poller lock")
        .iter()
        .filter(|((tl, _, _), _)| tl == &task_list_name)
        .filter(|(_, seen)| **seen >= cutoff)
        .map(|((_, identity, _), seen)| cadence_api::types::PollerInfo {
            identity: identity.clone(),
            last_access_time: Some(*seen),
            rate_per_second: 0.0,
        })
        .collect();
    pollers.sort_by(|a, b| a.identity.cmp(&b.identity));
    pollers.dedup_by(|a, b| a.identity == b.identity);
    (
        StatusCode::OK,
        Json(cadence_api::tasks::DescribeTaskListResponse {
            pollers,
            task_list_status: req.include_task_list_status.then(|| {
                cadence_api::types::TaskListStatus {
                    backlog_count_hint: backlog as i64,
                    ..Default::default()
                }
            }),
            task_list: req.task_list.clone(),
        }),
    )
        .into_response()
}

async fn get_search_attributes() -> impl IntoResponse {
    use cadence_api::enums::IndexedValueType as T;
    let keys: std::collections::HashMap<String, T> = [
        ("WorkflowID".to_string(), T::Keyword),
        ("RunID".to_string(), T::Keyword),
        ("WorkflowType".to_string(), T::Keyword),
        ("StartTime".to_string(), T::Datetime),
        ("CloseTime".to_string(), T::Datetime),
        ("CloseStatus".to_string(), T::Keyword),
        ("HistoryLength".to_string(), T::Int),
        ("ExecutionTime".to_string(), T::Datetime),
        ("CustomKeywordField".to_string(), T::Keyword),
        ("CustomStringField".to_string(), T::String),
        ("CustomIntField".to_string(), T::Int),
        ("CustomDoubleField".to_string(), T::Double),
        ("CustomBoolField".to_string(), T::Bool),
        ("CustomDatetimeField".to_string(), T::Datetime),
    ]
    .into_iter()
    .collect();
    Json(cadence_api::tasks::GetSearchAttributesResponse { keys: Some(keys) })
}

#[derive(Debug, Deserialize)]
struct CountQuery {
    domain: String,
}

async fn count_workflows(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CountQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    let open = state.executions.list(&q.domain, true).len() as i64;
    let closed = state.executions.list(&q.domain, false).len() as i64;
    (
        StatusCode::OK,
        Json(
            cadence_api::service_types::CountWorkflowExecutionsResponse {
                count: open + closed,
            },
        ),
    )
        .into_response()
}
