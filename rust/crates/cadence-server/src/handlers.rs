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
type PollerMap = std::collections::HashMap<(String, String, String, String), i64>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub admin: crate::admin::AdminState,
    /// History shard count from config (Go `numHistoryShards`, dev default
    /// 4). Single host owns every shard; runs are not partitioned.
    pub num_history_shards: i32,
    pub scheduler_enabled: bool,
    pub semaphore_enabled: bool,
    pub current_cluster_name: String,
    pub cluster_metadata: cadence_common::cluster::Metadata,
    pub grpc_max_message_size: usize,
    pub domains: InMemoryDomainStore,
    pub executions: InMemoryExecutionStore,
    pub limits: Limits,
    pub pollers: Arc<std::sync::Mutex<PollerMap>>,
    /// Shard-distributor namespaces and ownership. Memory-only like Go's
    /// cached coordination state; namespaces appear on first heartbeat.
    pub shard_distributor:
        Arc<std::sync::Mutex<crate::grpc_shard::ShardDistributorState>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            admin: crate::admin::AdminState::default(),
            num_history_shards: 4,
            scheduler_enabled: false,
            semaphore_enabled: false,
            current_cluster_name: "cluster0".to_string(),
            cluster_metadata: cadence_common::cluster::Metadata::default(),
            grpc_max_message_size: 4 * 1024 * 1024,
            domains: InMemoryDomainStore::new(),
            executions: InMemoryExecutionStore::new(),
            limits: Limits::cadence_default(),
            pollers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            shard_distributor: Arc::new(std::sync::Mutex::new(
                crate::grpc_shard::ShardDistributorState::default(),
            )),
        }
    }
}

impl AppState {
    pub fn execution_store(
        &self,
        domain: &str,
    ) -> Result<InMemoryExecutionStore, EntityNotExistsError> {
        let metadata = self.domains.describe(&DescribeDomainRequest {
            name: Some(domain.to_string()), uuid: None,
        })?;
        let id = metadata.domain_info.unwrap_or_default().uuid;
        let version = if metadata.is_global_domain { metadata.failover_version }
            else { cadence_persistence::version_history::EMPTY_VERSION };
        self.executions.set_domain_version(&id, version);
        Ok(self.executions.with_domain_id(&id))
    }
}

/// Go domain registration uses the current cluster when replication fields
/// are omitted. Local domains belong to exactly that cluster.
pub(crate) fn domain_replication(
    state: &AppState,
    req: &RegisterDomainRequest,
) -> Result<cadence_api::types::DomainReplicationConfiguration, BadRequestError> {
    let active_cluster_name = if req.active_cluster_name.is_empty() {
        state.current_cluster_name.clone()
    } else {
        req.active_cluster_name.clone()
    };
    let clusters = if req.clusters.is_empty() {
        vec![cadence_api::types::ClusterReplicationConfiguration {
            cluster_name: active_cluster_name.clone(),
        }]
    } else {
        req.clusters.clone()
    };
    if !req.is_global_domain {
        if active_cluster_name != state.current_cluster_name {
            return Err(BadRequestError::new("Invalid local domain active cluster"));
        }
        if clusters.len() != 1 || clusters[0].cluster_name != active_cluster_name {
            return Err(BadRequestError::new("Invalid local domain clusters"));
        }
    } else {
        for cluster in clusters.iter().map(|c| &c.cluster_name).chain(std::iter::once(&active_cluster_name)) {
            if !state.cluster_metadata.clusters.get(cluster).is_some_and(|c| c.enabled) {
                return Err(BadRequestError::new(format!("Invalid cluster name: {cluster}")));
            }
        }
        if !clusters.iter().any(|c| c.cluster_name == active_cluster_name) {
            return Err(BadRequestError::new("Active cluster is not contained in all clusters."));
        }
    }
    Ok(cadence_api::types::DomainReplicationConfiguration {
        active_cluster_name,
        clusters,
        active_clusters: req.active_clusters.clone(),
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn note_poller(
    state: &AppState,
    domain: &str,
    task_list: &str,
    identity: &str,
    kind: &str,
) {
    if task_list.is_empty() || identity.is_empty() {
        return;
    }
    state.pollers.lock().expect("poller lock").insert(
        (
            domain.to_owned(),
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

pub(crate) fn error_response(kind: &'static str, status: StatusCode, message: String) -> Response {
    (status, Json(ErrorBody { kind, message })).into_response()
}

pub(crate) fn bad_request(e: BadRequestError) -> Response {
    error_response("BadRequestError", StatusCode::BAD_REQUEST, e.message)
}

pub(crate) fn entity_not_exists(e: EntityNotExistsError) -> Response {
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
    let replication = match domain_replication(&state, &req) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };
    let failover_version = if req.is_global_domain {
        match state.cluster_metadata.next_failover_version(&replication.active_cluster_name, 0, false) {
            Ok(version) => version,
            Err(e) => return bad_request(BadRequestError::new(e)),
        }
    } else { cadence_persistence::version_history::EMPTY_VERSION };
    match state
        .domains
        .create_with_version(info, configuration, replication, req.is_global_domain, failover_version)
    {
        Ok(resp) => {
            state.executions.register_domain(
                &req.name,
                &state
                    .domains
                    .domain_id(&req.name)
                    .expect("registered domain"),
            );
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

async fn list_domains(
    State(state): State<Arc<AppState>>,
    Query(request): Query<cadence_api::service_types::ListDomainsRequest>,
) -> Response {
    match state.domains.list_page(&request) {
        Ok(v) => Json(v).into_response(),
        Err(e) => bad_request(e),
    }
}

/// Create a semaphore, mirroring Go frontend `CreateSemaphore`.
async fn create_semaphore(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::CreateSemaphoreRequest>,
) -> Response {
    if let Err(e) = validation::validate_create_semaphore_request(Some(&req)) {
        return bad_request(e);
    }
    if !state.semaphore_enabled {
        return error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            format!(
                "Semaphores are not enabled for domain {:?}. Set dynamic config system.enableDistributedSemaphore=true for this domain to enable them.",
                req.domain
            ),
        );
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match executions.create_semaphore(
        &req.domain,
        &req.semaphore_name,
        req.capacity,
        req.bucket_capacity,
    ) {
        Ok(meta) => (
            StatusCode::OK,
            Json(cadence_api::types::CreateSemaphoreResponse {
                semaphore: Some(cadence_api::types::Semaphore {
                    semaphore_name: req.semaphore_name.clone(),
                    capacity: meta.size,
                    bucket_capacity: meta.bucket_size,
                }),
            }),
        )
            .into_response(),
        Err(cadence_persistence::SemaphoreError::Exists(_)) => error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            format!(
                "semaphore {:?} already exists in domain {:?}",
                req.semaphore_name, req.domain
            ),
        ),
        Err(cadence_persistence::SemaphoreError::Invalid(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
        Err(cadence_persistence::SemaphoreError::NotFound(e)) => entity_not_exists(e),
    }
}

/// Fail a domain over to another cluster, mirroring Go frontend
/// `FailoverDomain` (validation + domain-handler failover with audit).
async fn failover_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::FailoverDomainRequest>,
) -> Response {
    if let Err(e) = validation::validate_failover_domain_request(Some(&req)) {
        return bad_request(e);
    }
    match state
        .domains
        .failover(&req, &state.current_cluster_name, &state.cluster_metadata)
    {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::FailoverError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::FailoverError::Invalid(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

/// List a domain's failover audit history with cursor paging, mirroring Go
/// frontend `ListFailoverHistory`.
async fn list_failover_history(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::ListFailoverHistoryRequest>,
) -> Response {
    let domain_id = match req.filters.as_ref() {
        Some(f) if !f.domain_id.is_empty() => f.domain_id.clone(),
        _ => {
            return bad_request(BadRequestError::new("DomainID is required in filters"));
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
        Ok((events, next_page_token)) => (
            StatusCode::OK,
            Json(cadence_api::types::ListFailoverHistoryResponse {
                failover_events: events,
                next_page_token,
            }),
        )
            .into_response(),
        Err(e) => entity_not_exists(e),
    }
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

pub(crate) async fn require_domain(state: &AppState, domain: &str) -> Option<Response> {
    if domain.is_empty() {
        return Some(bad_request(validation::err_domain_not_set()));
    }
    state.domains.domain_id(domain).err().map(entity_not_exists)
}

fn require_task_domain(state: &AppState, bytes: &[u8]) -> Option<Response> {
    let token = match cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(bytes) {
        Ok(token) => token,
        Err(_) => return Some(bad_request(BadRequestError::new("Invalid TaskToken."))),
    };
    if uuid::Uuid::parse_str(&token.domain_id).is_err() {
        return Some(bad_request(BadRequestError::new(
            "Invalid TaskToken domain UUID.",
        )));
    }
    state
        .domains
        .describe(&DescribeDomainRequest {
            name: None,
            uuid: Some(token.domain_id),
        })
        .err()
        .map(entity_not_exists)
}

async fn start_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::validate_start_workflow_execution_request(Some(&req), &state.limits)
    {
        return bad_request(e);
    }
    match executions.start(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(
            WorkflowExecutionAlreadyStartedError { message, .. },
        )) => error_response(
            "WorkflowExecutionAlreadyStartedError",
            StatusCode::CONFLICT,
            message,
        ),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

async fn describe_workflow(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DescribeWorkflowExecutionQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    let executions = match state.execution_store(&q.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let execution = cadence_api::types::WorkflowExecution::new(q.workflow_id, q.run_id);
    if let Err(e) = validation::check_execution(Some(&execution)) {
        // Empty workflow ID is a 400; a bad run ID is a 400 too.
        return bad_request(e);
    }
    match executions.describe(&q.domain, &execution) {
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if req.signal_name.is_empty() {
        return bad_request(validation::err_signal_name_not_set());
    }
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match executions.signal(&req) {
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match executions.request_cancel(&req) {
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

async fn reset_sticky_task_list(
    State(state): State<Arc<AppState>>,
    Json(request): Json<cadence_api::service_types::ResetStickyTaskListRequest>,
) -> Response {
    if let Some(error) = require_domain(&state, &request.domain).await {
        return error;
    }
    let executions = match state.execution_store(&request.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(error) = validation::check_execution(request.execution.as_ref()) {
        return bad_request(error);
    }
    match executions.reset_sticky_task_list(&request.domain, request.execution.as_ref().unwrap()) {
        Ok(()) => Json(cadence_api::service_types::ResetStickyTaskListResponse {}).into_response(),
        Err(WorkflowMutationError::NotFound(error)) => entity_not_exists(error),
        Err(WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::CONFLICT,
            error.message,
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match executions.terminate(&req) {
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
    let executions = match state.execution_store(&q.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
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
    match executions.history_page(
        &q.domain,
        &execution,
        q.maximum_page_size,
        &token,
        q.history_event_filter_type,
    ) {
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
    let executions = match state.execution_store(&q.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match q.status.as_str() {
        "open" | "closed" => {
            let open_only = q.status.as_str() == "open";
            let close_status = q
                .close_status
                .and_then(cadence_api::enums::WorkflowExecutionCloseStatus::from_i32);
            let executions = executions.list_filtered(
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
        .route("/api/v1/tasks/reset-sticky", post(reset_sticky_task_list))
        .route("/api/v1/domains/register", post(register_domain))
        .route("/api/v1/domains/describe", get(describe_domain))
        .route("/api/v1/domains/list", get(list_domains))
        .route("/api/v1/domains/update", put(update_domain))
        .route("/api/v1/domains/failover", post(failover_domain))
        .route("/api/v1/domains/failover-history", post(list_failover_history))
        .route("/api/v1/semaphores/create", post(create_semaphore))
        .route("/api/v1/domains/deprecate", post(deprecate_domain))
        .route("/api/v1/workflows/start", post(start_workflow))
        .route("/api/v1/workflows/describe", get(describe_workflow))
        .route("/api/v1/workflows/signal", post(signal_workflow))
        .route(
            "/api/v1/workflows/request-cancel",
            post(request_cancel_workflow),
        )
        .route("/api/v1/workflows/terminate", post(terminate_workflow))
        .route("/api/v1/workflows/restart", post(restart_workflow))
        .route("/api/v1/workflows/reset", post(reset_workflow))
        .route("/api/v1/workflows/refresh-tasks", post(refresh_workflow_tasks))
        .route("/api/v1/workflows/diagnose", post(diagnose_workflow))
        .route("/api/v1/workflows/start-async", post(start_workflow_async))
        .route(
            "/api/v1/workflows/signal-with-start-async",
            post(signal_with_start_async),
        )
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
        .route(
            "/api/v1/tasks/respond-query-completed",
            post(respond_query_task_completed),
        )
        .route("/api/v1/domains/delete", post(delete_domain))
        .route("/api/v1/tasklists/describe", post(describe_task_list))
        .route(
            "/api/v1/tasklists/by-domain",
            post(get_task_lists_by_domain),
        )
        .route(
            "/api/v1/tasklists/partitions",
            post(list_task_list_partitions),
        )
        .route("/api/v1/schedules/create", post(create_schedule))
        .route("/api/v1/schedules/describe", post(describe_schedule))
        .route("/api/v1/schedules/list", post(list_schedules))
        .route("/api/v1/schedules/delete", post(delete_schedule))
        .route("/api/v1/schedules/pause", post(pause_schedule))
        .route("/api/v1/schedules/unpause", post(unpause_schedule))
        .route("/api/v1/schedules/backfill", post(backfill_schedule))
        .route("/api/v1/schedules/update", post(update_schedule))
        .route("/api/v1/search-attributes", get(get_search_attributes))
        .route("/api/v1/workflows/count", get(count_workflows))
        .route("/api/v1/admin/cluster/describe", get(crate::admin::describe_cluster))
        .route("/api/v1/admin/workflows/delete", post(crate::admin::delete_workflow))
        .route(
            "/api/v1/admin/workflows/refresh-tasks",
            post(crate::admin::refresh_workflow_tasks),
        )
        .route(
            "/api/v1/admin/search-attributes/add",
            post(crate::admin::add_search_attribute),
        )
        .route(
            "/api/v1/admin/dynamic-config/get",
            post(crate::admin::get_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config/update",
            post(crate::admin::update_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config/restore",
            post(crate::admin::restore_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config/list",
            post(crate::admin::list_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config-operational/get",
            post(crate::admin::get_operational_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config-operational/update",
            post(crate::admin::update_operational_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config-operational/restore",
            post(crate::admin::restore_operational_dynamic_config),
        )
        .route(
            "/api/v1/admin/dynamic-config-operational/list",
            post(crate::admin::list_operational_dynamic_config),
        )
        .route(
            "/api/v1/admin/isolation-groups/global/get",
            post(crate::admin::get_global_isolation_groups),
        )
        .route(
            "/api/v1/admin/isolation-groups/global/update",
            post(crate::admin::update_global_isolation_groups),
        )
        .route(
            "/api/v1/admin/isolation-groups/domain/get",
            post(crate::admin::get_domain_isolation_groups),
        )
        .route(
            "/api/v1/admin/isolation-groups/domain/update",
            post(crate::admin::update_domain_isolation_groups),
        )
        .route(
            "/api/v1/admin/async-workflow-config/get",
            post(crate::admin::get_domain_async_workflow_configuration),
        )
        .route(
            "/api/v1/admin/async-workflow-config/update",
            post(crate::admin::update_domain_async_workflow_configuration),
        )
        .route(
            "/api/v1/admin/history-host/describe",
            post(crate::admin::describe_history_host),
        )
        .route(
            "/api/v1/admin/shards/describe-distribution",
            get(crate::admin::describe_shard_distribution),
        )
        .route("/api/v1/admin/shards/close", post(crate::admin::close_shard))
        .route("/api/v1/admin/tasks/remove", post(crate::admin::remove_task))
        .route("/api/v1/admin/queues/reset", post(crate::admin::reset_queue))
        .route(
            "/api/v1/admin/queues/describe",
            post(crate::admin::describe_queue),
        )
        .route(
            "/api/v1/admin/workflows/raw-history",
            post(crate::admin::get_workflow_execution_raw_history),
        )
        .route(
            "/api/v1/admin/workflows/describe",
            post(crate::admin::describe_workflow_execution_admin),
        )
        .route(
            "/api/v1/admin/workflows/maintain-corrupt",
            post(crate::admin::maintain_corrupt_workflow),
        )
        .route(
            "/api/v1/admin/workflows/reapply-events",
            post(crate::admin::reapply_events),
        )
        .route(
            "/api/v1/admin/replication/messages",
            post(crate::admin::get_replication_messages),
        )
        .route(
            "/api/v1/admin/replication/dlq-messages",
            post(crate::admin::get_dlq_replication_messages),
        )
        .route(
            "/api/v1/admin/replication/domain-messages",
            post(crate::admin::get_domain_replication_messages),
        )
        .route(
            "/api/v1/admin/dlq/count",
            post(crate::admin::count_dlq_messages),
        )
        .route(
            "/api/v1/admin/dlq/read",
            post(crate::admin::read_dlq_messages),
        )
        .route(
            "/api/v1/admin/dlq/purge",
            post(crate::admin::purge_dlq_messages),
        )
        .route(
            "/api/v1/admin/dlq/merge",
            post(crate::admin::merge_dlq_messages),
        )
        .route(
            "/api/v1/admin/replication/resend",
            post(crate::admin::resend_replication_tasks),
        )
        .route(
            "/api/v1/admin/cross-cluster/tasks",
            post(crate::admin::get_cross_cluster_tasks),
        )
        .route(
            "/api/v1/admin/cross-cluster/tasks-completed",
            post(crate::admin::respond_cross_cluster_tasks_completed),
        )
        .route("/api/v1/workflows/archived", post(list_archived_workflows))
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::validate_poll_request(&req.domain, req.task_list.as_ref()) {
        return bad_request(e);
    }
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    note_poller(
        &state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "decision",
    );
    long_poll(q.timeout_secs, || {
        executions.poll_decision(&req.domain, &task_list, &req.identity)
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::validate_poll_request(&req.domain, req.task_list.as_ref()) {
        return bad_request(e);
    }
    let task_list = req
        .task_list
        .as_ref()
        .map(|t| t.name.clone())
        .unwrap_or_default();
    note_poller(
        &state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "activity",
    );
    long_poll(q.timeout_secs, || {
        executions.poll_activity(&req.domain, &task_list, &req.identity)
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    if let Some(error) = require_task_domain(&state, &req.task_token) {
        return error;
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::validate_signal_with_start_request(Some(&req), &state.limits) {
        return bad_request(e);
    }
    match executions.signal_with_start(&req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(
            WorkflowExecutionAlreadyStartedError { message, .. },
        )) => error_response(
            "WorkflowExecutionAlreadyStartedError",
            StatusCode::CONFLICT,
            message,
        ),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

/// Enqueue an async start request, mirroring Go frontend
/// `StartWorkflowExecutionAsync` (validate, then publish to the queue).
async fn start_workflow_async(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::StartWorkflowExecutionAsyncRequest>,
) -> Response {
    let inner = match req.request {
        Some(r) => r,
        None => return bad_request(BadRequestError::new("Request is not set.")),
    };
    if let Some(e) = require_domain(&state, &inner.domain).await {
        return e;
    }
    let executions = match state.execution_store(&inner.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) =
        validation::validate_start_workflow_execution_request(Some(&inner), &state.limits)
    {
        return bad_request(e);
    }
    executions.enqueue_async_start(inner);
    (
        StatusCode::OK,
        Json(cadence_api::types::StartWorkflowExecutionAsyncResponse {}),
    )
        .into_response()
}

/// Enqueue an async signal-with-start request, mirroring Go frontend
/// `SignalWithStartWorkflowExecutionAsync`.
async fn signal_with_start_async(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncRequest>,
) -> Response {
    let inner = match req.request {
        Some(r) => r,
        None => return bad_request(BadRequestError::new("Request is not set.")),
    };
    if let Some(e) = require_domain(&state, &inner.domain).await {
        return e;
    }
    let executions = match state.execution_store(&inner.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::validate_signal_with_start_request(Some(&inner), &state.limits) {
        return bad_request(e);
    }
    executions.enqueue_async_signal_with_start(inner);
    (
        StatusCode::OK,
        Json(cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncResponse {}),
    )
        .into_response()
}

/// Regenerate an execution's dispatch queue entries, mirroring Go frontend
/// `RefreshWorkflowTasks` (which delegates to history refresh).
async fn refresh_workflow_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::RefreshWorkflowTasksRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::check_execution(req.execution.as_ref()) {
        return bad_request(e);
    }
    let execution = req.execution.clone().unwrap_or_default();
    match executions.refresh_tasks(&req.domain, &execution) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::BAD_REQUEST,
            error.message,
        ),
    }
}

/// Diagnose a workflow by starting the diagnostics starter workflow in the
/// system domain, mirroring Go frontend `DiagnoseWorkflowExecution`.
async fn diagnose_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::DiagnoseWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() || execution.run_id.is_empty() {
        return bad_request(BadRequestError::new("Execution is not set on request."));
    }
    let system_domain = "cadence-system";
    let executions = match state.execution_store(system_domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let diagnostic_id = format!("{}-{}", req.domain, execution.run_id);
    let input = serde_json::json!({
        "domain": req.domain,
        "workflowId": execution.workflow_id,
        "runId": execution.run_id,
        "identity": req.identity,
    });
    let start = cadence_api::types::StartWorkflowExecutionRequest {
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
        workflow_id_reuse_policy: Some(cadence_api::enums::WorkflowIDReusePolicy::AllowDuplicate),
        ..Default::default()
    };
    match executions.start(&start) {
        Ok(resp) => (
            StatusCode::OK,
            Json(cadence_api::types::DiagnoseWorkflowExecutionResponse {
                domain: system_domain.to_string(),
                diagnostic_workflow_execution: Some(
                    cadence_api::types::WorkflowExecution::new(diagnostic_id, resp.run_id),
                ),
            }),
        )
            .into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => (
            StatusCode::OK,
            Json(cadence_api::types::DiagnoseWorkflowExecutionResponse {
                domain: system_domain.to_string(),
                diagnostic_workflow_execution: Some(
                    cadence_api::types::WorkflowExecution::new(diagnostic_id, e.run_id),
                ),
            }),
        )
            .into_response(),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

/// Reset a workflow execution to a decision-task boundary, mirroring Go
/// frontend `ResetWorkflowExecution` (which delegates to history reset).
async fn reset_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::ResetWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validation::check_execution(req.workflow_execution.as_ref()) {
        return bad_request(e);
    }
    match executions.reset(&req) {
        Ok(resp) => (
            StatusCode::OK,
            Json(cadence_api::types::ResetWorkflowExecutionResponse {
                run_id: resp.run_id,
            }),
        )
            .into_response(),
        Err(cadence_persistence::ResetError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::ResetError::Invalid(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

/// Restart a workflow execution from its first started attributes,
/// mirroring Go frontend `RestartWorkflowExecution`.
async fn restart_workflow(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::types::RestartWorkflowExecutionRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if let Err(e) = validation::check_execution(Some(&execution)) {
        return bad_request(e);
    }
    match executions.restart(&req.domain, &execution, &req.identity) {
        Ok(resp) => (
            StatusCode::OK,
            Json(cadence_api::types::RestartWorkflowExecutionResponse {
                run_id: resp.run_id,
            }),
        )
            .into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(
            WorkflowExecutionAlreadyStartedError { message, .. },
        )) => error_response(
            "WorkflowExecutionAlreadyStartedError",
            StatusCode::CONFLICT,
            message,
        ),
        Err(cadence_persistence::StartError::InvalidArgument(message)) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
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
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
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
    let query_id = match executions.buffer_query(
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
        if let Some(result) = executions.take_query_result(&query_id) {
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

/// Answer a directly-dispatched query task, mirroring Go frontend
/// `RespondQueryTaskCompleted` → matching `deliverQueryResult`.
async fn respond_query_task_completed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::RespondQueryTaskCompletedRequest>,
) -> Response {
    if req.task_token.is_empty() {
        return bad_request(BadRequestError::new("Task token not set on request."));
    }
    match state.executions.answer_query_task(&req) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::tasks::RespondQueryTaskCompletedResponse {}),
        )
            .into_response(),
        Err(cadence_persistence::QueryError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::QueryError::Invalid(e)) => bad_request(e),
        Err(cadence_persistence::QueryError::Rejected(_)) => bad_request(BadRequestError::new(
            "Query task was rejected.",
        )),
    }
}

async fn delete_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeleteDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    match state.domains.delete(&req.name) {
        Ok(()) => {
            state.executions.unregister_domain(&req.name);
            StatusCode::OK.into_response()
        }
        Err(e) => entity_not_exists(e),
    }
}

/// Small error for shared describe-task-list logic, keeping strict
/// `result_large_err` clippy clean at every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeTaskListError {
    TaskListNotSet,
    DomainGone,
}

/// Reject schedule APIs when the scheduler is disabled, mirroring Go's
/// `EnableScheduler` gate with its exact message.
fn require_scheduler(state: &AppState, domain: &str) -> Option<Response> {
    if state.scheduler_enabled {
        return None;
    }
    Some(error_response(
        "BadRequestError",
        StatusCode::BAD_REQUEST,
        format!(
            "Schedules are not enabled for domain {domain:?}. Set dynamic config worker.enableScheduler=true for this domain to enable them."
        ),
    ))
}

fn schedule_not_found(e: cadence_persistence::ScheduleError) -> Response {
    use cadence_persistence::ScheduleError;
    match e {
        ScheduleError::NotFound(e) => entity_not_exists(e),
        ScheduleError::Invalid(message) | ScheduleError::Exists(message) => {
            error_response("BadRequestError", StatusCode::BAD_REQUEST, message)
        }
    }
}

fn validate_schedule_id(schedule_id: &str) -> Result<(), BadRequestError> {
    if schedule_id.is_empty() {
        return Err(BadRequestError::new("ScheduleID is not set on request."));
    }
    Ok(())
}

fn schedule_bad_request(e: BadRequestError) -> Response {
    error_response("BadRequestError", StatusCode::BAD_REQUEST, e.message)
}

async fn create_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::CreateScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    let spec = match req.spec.as_ref() {
        Some(s) => s,
        None => {
            return error_response(
                "BadRequestError",
                StatusCode::BAD_REQUEST,
                "Spec is not set on request.".to_string(),
            );
        }
    };
    if spec.cron_expression.is_empty() {
        return error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            "CronExpression is not set on request.".to_string(),
        );
    }
    if let Err(message) = validate_schedule_cron(&spec.cron_expression) {
        return error_response("BadRequestError", StatusCode::BAD_REQUEST, message);
    }
    if let Err(e) = validation::validate_schedule_spec(Some(spec)) {
        return bad_request(e);
    }
    if let Err(e) = validation::validate_schedule_action_complete(req.action.as_ref()) {
        return bad_request(e);
    }
    if let Err(e) = validation::validate_schedule_policies(req.policies.as_ref()) {
        return bad_request(e);
    }
    if let Err(e) = validation::validate_schedule_search_attributes(req.search_attributes.as_ref())
    {
        return bad_request(e);
    }
    match executions.create_schedule(&req.domain, &req) {
        Ok(id) => (
            StatusCode::OK,
            Json(cadence_api::schedule::CreateScheduleResponse { schedule_id: id }),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

fn validate_schedule_cron(expr: &str) -> Result<(), String> {
    let now = cadence_common::cron::naive_from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    );
    cadence_common::cron::validate_schedule(expr, now).map(|_| ())
}

async fn describe_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::DescribeScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    match executions.describe_schedule(&req.domain, &req.schedule_id) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => schedule_not_found(e),
    }
}

async fn list_schedules(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::ListSchedulesRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let page_size = if req.page_size <= 0 { 10 } else { req.page_size as usize };
    let (schedules, next_page_token) =
        executions.list_schedules(&req.domain, page_size, &req.next_page_token);
    (
        StatusCode::OK,
        Json(cadence_api::schedule::ListSchedulesResponse {
            schedules,
            next_page_token,
        }),
    )
        .into_response()
}

async fn delete_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::DeleteScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    match executions.delete_schedule(&req.domain, &req.schedule_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::schedule::DeleteScheduleResponse {}),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

async fn pause_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::PauseScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    // Go derives the actor from identity headers; the JSON surface carries it
    // in the request identity field.
    let actor = if req.identity.is_empty() {
        "unknown".to_string()
    } else {
        req.identity.clone()
    };
    match executions.pause_schedule(&req.domain, &req.schedule_id, &req.reason, &actor) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::schedule::PauseScheduleResponse {}),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

async fn unpause_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::UnpauseScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    match executions.unpause_schedule(&req.domain, &req.schedule_id, req.catch_up_policy) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::schedule::UnpauseScheduleResponse {}),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

async fn backfill_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::BackfillScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    let (start, end) = match (req.start_time_nano, req.end_time_nano) {
        (Some(s), Some(e)) => (s, e),
        _ => {
            let missing_start = req.start_time_nano.is_none();
            return error_response(
                "BadRequestError",
                StatusCode::BAD_REQUEST,
                if missing_start {
                    "StartTime is not set on request.".to_string()
                } else {
                    "EndTime is not set on request.".to_string()
                },
            );
        }
    };
    if end <= start {
        return error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            "EndTime must be after StartTime.".to_string(),
        );
    }
    let backfill_id = if req.backfill_id.trim().is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        req.backfill_id.trim().to_string()
    };
    match executions.backfill_schedule(
        &req.domain,
        &req.schedule_id,
        start,
        end,
        req.overlap_policy,
        &backfill_id,
    ) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::schedule::BackfillScheduleResponse {}),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

async fn update_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::schedule::UpdateScheduleRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_scheduler(&state, &req.domain) {
        return e;
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    if let Err(e) = validate_schedule_id(&req.schedule_id) {
        return schedule_bad_request(e);
    }
    if req.spec.is_none()
        && req.action.is_none()
        && req.policies.is_none()
        && req.search_attributes.is_none()
    {
        return error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            "At least one of Spec, Action, Policies, or SearchAttributes must be set on request."
                .to_string(),
        );
    }
    if let Some(spec) = req.spec.as_ref() {
        if !spec.cron_expression.is_empty() {
            if let Err(message) = validate_schedule_cron(&spec.cron_expression) {
                return error_response("BadRequestError", StatusCode::BAD_REQUEST, message);
            }
        }
        if let Err(e) = validation::validate_schedule_spec(Some(spec)) {
            return bad_request(e);
        }
    }
    if req.action.is_some() {
        if let Err(e) = validation::validate_schedule_action_complete(req.action.as_ref()) {
            return bad_request(e);
        }
    }
    if let Err(e) = validation::validate_schedule_policies(req.policies.as_ref()) {
        return bad_request(e);
    }
    if let Err(e) = validation::validate_schedule_search_attributes(req.search_attributes.as_ref())
    {
        return bad_request(e);
    }
    match executions.update_schedule(&req) {
        Ok(()) => (
            StatusCode::OK,
            Json(cadence_api::schedule::UpdateScheduleResponse {}),
        )
            .into_response(),
        Err(e) => schedule_not_found(e),
    }
}

pub(crate) fn describe_task_list_response(
    state: &AppState,
    executions: &cadence_persistence::InMemoryExecutionStore,
    req: &cadence_api::tasks::DescribeTaskListRequest,
) -> Result<cadence_api::tasks::DescribeTaskListResponse, DescribeTaskListError> {
    use DescribeTaskListError::{DomainGone, TaskListNotSet};
    let task_list_name = match req.task_list.clone() {
        Some(t) if !t.name.is_empty() => t.name,
        _ => {
            return Err(TaskListNotSet);
        }
    };
    let (decision_backlog, activity_backlog) =
        executions.task_list_backlog(&req.domain, &task_list_name);
    let backlog = match req.task_list_type {
        Some(cadence_api::enums::TaskListType::Activity) => activity_backlog,
        _ => decision_backlog,
    };
    // Go matching retains poller history for five minutes per domain, list, and task type.
    let cutoff = now_millis() - 300_000;
    let kind = if req.task_list_type == Some(cadence_api::enums::TaskListType::Activity) {
        "activity"
    } else {
        "decision"
    };
    let domain_id = match executions.namespace_id() {
        Some(id) => id.to_owned(),
        None => {
            return Err(DomainGone);
        }
    };
    let mut pollers: Vec<cadence_api::types::PollerInfo> = state
        .pollers
        .lock()
        .expect("poller lock")
        .iter()
        .filter(|((domain, tl, _, poll_kind), _)| {
            domain == &domain_id && tl == &task_list_name && poll_kind == kind
        })
        .filter(|(_, seen)| **seen >= cutoff)
        .map(
            |((_, _, identity, _), seen)| cadence_api::types::PollerInfo {
                identity: identity.clone(),
                last_access_time: Some(*seen),
                rate_per_second: 0.0,
            },
        )
        .collect();
    pollers.sort_by(|a, b| a.identity.cmp(&b.identity));
    pollers.dedup_by(|a, b| a.identity == b.identity);
    Ok(cadence_api::tasks::DescribeTaskListResponse {
        pollers,
        task_list_status: req.include_task_list_status.then(|| {
            cadence_api::types::TaskListStatus {
                backlog_count_hint: backlog as i64,
                ..Default::default()
            }
        }),
        task_list: req.task_list.clone(),
    })
}

async fn describe_task_list(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::DescribeTaskListRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match describe_task_list_response(&state, &executions, &req) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(DescribeTaskListError::TaskListNotSet) => {
            bad_request(validation::err_task_list_not_set())
        }
        Err(DescribeTaskListError::DomainGone) => entity_not_exists(
            cadence_api::errors::EntityNotExistsError::new("Domain does not exist.".to_string()),
        ),
    }
}

/// List all task lists for a domain, mirroring Go frontend
/// `GetTaskListsByDomain` → matching. Each entry carries the same
/// poller/backlog detail as `DescribeTaskList`.
async fn get_task_lists_by_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::GetTaskListsByDomainRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let (decisions, activities) = executions.task_lists(&req.domain);
    let mut decision_task_list_map = std::collections::HashMap::new();
    for name in decisions {
        let desc_req = cadence_api::tasks::DescribeTaskListRequest {
            domain: req.domain.clone(),
            task_list: Some(cadence_api::types::TaskList {
                name: name.clone(),
                ..Default::default()
            }),
            task_list_type: None,
            include_task_list_status: true,
        };
        match describe_task_list_response(&state, &executions, &desc_req) {
            Ok(desc) => {
                decision_task_list_map.insert(name, desc);
            }
            Err(DescribeTaskListError::TaskListNotSet) => {
                return bad_request(validation::err_task_list_not_set());
            }
            Err(DescribeTaskListError::DomainGone) => {
                return entity_not_exists(cadence_api::errors::EntityNotExistsError::new(
                    "Domain does not exist.".to_string(),
                ));
            }
        }
    }
    let mut activity_task_list_map = std::collections::HashMap::new();
    for name in activities {
        let desc_req = cadence_api::tasks::DescribeTaskListRequest {
            domain: req.domain.clone(),
            task_list: Some(cadence_api::types::TaskList {
                name: name.clone(),
                ..Default::default()
            }),
            task_list_type: Some(cadence_api::enums::TaskListType::Activity),
            include_task_list_status: true,
        };
        match describe_task_list_response(&state, &executions, &desc_req) {
            Ok(desc) => {
                activity_task_list_map.insert(name, desc);
            }
            Err(DescribeTaskListError::TaskListNotSet) => {
                return bad_request(validation::err_task_list_not_set());
            }
            Err(DescribeTaskListError::DomainGone) => {
                return entity_not_exists(cadence_api::errors::EntityNotExistsError::new(
                    "Domain does not exist.".to_string(),
                ));
            }
        }
    }
    (
        StatusCode::OK,
        Json(cadence_api::tasks::GetTaskListsByDomainResponse {
            decision_task_list_map: Some(decision_task_list_map),
            activity_task_list_map: Some(activity_task_list_map),
        }),
    )
        .into_response()
}

/// List partitions for a task list, mirroring Go frontend
/// `ListTaskListPartitions` → matching. Single host: the root partition.
async fn list_task_list_partitions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::tasks::ListTaskListPartitionsRequest>,
) -> Response {
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    if state.execution_store(&req.domain).is_err() {
        return entity_not_exists(cadence_api::errors::EntityNotExistsError::new(
            "Domain does not exist.".to_string(),
        ));
    }
    let name = match req.task_list.clone() {
        Some(t) if !t.name.is_empty() => t.name,
        _ => return bad_request(validation::err_task_list_not_set()),
    };
    let owner = std::env::var("HOSTNAME").unwrap_or_default();
    let partition = cadence_api::tasks::TaskListPartitionMetadata {
        key: name,
        owner_host_name: owner,
    };
    (
        StatusCode::OK,
        Json(cadence_api::tasks::ListTaskListPartitionsResponse {
            activity_task_list_partitions: vec![partition.clone()],
            decision_task_list_partitions: vec![partition],
        }),
    )
        .into_response()
}

pub(crate) fn search_attributes() -> cadence_api::tasks::GetSearchAttributesResponse {
    cadence_api::tasks::GetSearchAttributesResponse {
        keys: Some(cadence_api::search_attributes::default_indexed_keys()),
    }
}
async fn get_search_attributes() -> impl IntoResponse {
    Json(search_attributes())
}

#[derive(Debug, Deserialize)]
struct CountQuery {
    domain: String,
    #[serde(default)]
    query: String,
}

/// List archived workflows, mirroring Go frontend
/// `ListArchivedWorkflowExecutions` gating: without a visibility archival
/// backend the cluster is never configured, so the exact Go error is
/// returned after validation.
async fn list_archived_workflows(
    State(state): State<Arc<AppState>>,
    Json(req): Json<cadence_api::service_types::ListArchivedWorkflowExecutionsRequest>,
) -> Response {
    if let Err(e) = validation::validate_list_archived_request(Some(&req)) {
        return bad_request(e);
    }
    let _ = &state;
    error_response(
        "BadRequestError",
        StatusCode::BAD_REQUEST,
        "Cluster is not configured for visibility archival".to_string(),
    )
}

async fn count_workflows(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CountQuery>,
) -> Response {
    if let Some(e) = require_domain(&state, &q.domain).await {
        return e;
    }
    let executions = match state.execution_store(&q.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let count = match executions.query_visible(&q.domain, &q.query) {
        Ok(v) => v.len() as i64,
        Err(e) => return bad_request(e),
    };
    (
        StatusCode::OK,
        Json(cadence_api::service_types::CountWorkflowExecutionsResponse { count }),
    )
        .into_response()
}
