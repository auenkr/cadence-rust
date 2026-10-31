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
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
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
use std::future::Future;
use std::pin::Pin;

/// A Frontend owned global quota collection. Go calls `OnStop` on each of its
/// seven collections before opening the request drain window.
pub trait FrontendLimiterCollection: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn on_stop(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

/// Recently seen pollers: (task-list, identity, kind) -> poll record.
/// Mirrors the poller list Go's matching service reports.
type PollerMap = std::collections::HashMap<(String, String, String, String), PollerRecord>;

/// Carries a poll's isolation group from matching to the frontend poll and
/// from the worker's HTTP request, like Go's `cadence-worker-isolation-group`.
pub const ISOLATION_GROUP_HEADER: &str = "cadence-worker-isolation-group";

/// Go `TaskDispatchRPS` default, used for a poller without a worker rate.
const TASK_DISPATCH_RPS: f64 = 100_000.0;

/// Go `persistence.TaskListTypeDecision` and `TaskListTypeActivity`.
pub const DECISION_TASK_TYPE: i32 = 0;
pub const ACTIVITY_TASK_TYPE: i32 = 1;

/// Go `poller.historicalPoller` for one identity: the latest poll's rate and
/// isolation group, its last start or end time, and whether it is outstanding.
#[derive(Debug, Clone, PartialEq)]
pub struct PollerRecord {
    pub seen_millis: i64,
    pub outstanding: bool,
    pub rate_per_second: f64,
    pub isolation_group: String,
}

#[derive(Debug, Clone)]
pub struct AppState {
    /// Dedicated Matching HTTP owns the `cadence-matching` YARPC service;
    /// reject attempts to route that service through the public JSON listener.
    pub matching_http_isolated: bool,
    pub admin: crate::admin::AdminState,
    /// History shard count from config (Go `numHistoryShards`, dev default
    /// 4). Single host owns every shard; runs are not partitioned.
    pub num_history_shards: i32,
    pub scheduler_enabled: bool,
    pub semaphore_enabled: bool,
    pub current_cluster_name: String,
    pub cluster_metadata: cadence_common::cluster::Metadata,
    pub grpc_max_message_size: usize,
    /// Frontend readiness and admission state are split so shutdown can first
    /// fail health checks, wait for discovery, and only then reject new RPCs.
    pub frontend_ready: Arc<std::sync::atomic::AtomicBool>,
    /// Prevents the Go-compatible warmup timer from marking a stopping
    /// Frontend healthy while shutdown is waiting for discovery/drain.
    pub frontend_shutting_down: Arc<std::sync::atomic::AtomicBool>,
    pub frontend_accepting: Arc<std::sync::atomic::AtomicBool>,
    /// Stop signal for AdminHandler-owned background workers (for example,
    /// the Go domain DLQ and failover watchers). This is separate from the
    /// API admission gate stopped by WorkflowHandler.Stop.
    pub frontend_admin_shutdown: tokio::sync::watch::Sender<bool>,
    /// Global limiter collections are service owned and stop between handler
    /// admission shutdown and the final request drain, as in Go Service.Stop.
    pub frontend_limiters: Arc<std::sync::Mutex<Vec<Arc<dyn FrontendLimiterCollection>>>>,
    /// Go Frontend's `stopC`, used by service-owned timer and snapshot workers.
    /// The launcher signals this only after request draining has completed.
    pub background_shutdown: tokio::sync::watch::Sender<bool>,
    pub domains: InMemoryDomainStore,
    /// Frontend's persistence-facing domain API. Matching retains the shared
    /// in-memory cache above; production Frontend injects a manager backed by
    /// the configured Go defaultStore and sharing that cache.
    pub domain_manager: Arc<dyn cadence_persistence::FrontendDomainManager>,
    pub executions: InMemoryExecutionStore,
    /// Optional backend-specific semaphore persistence. None is the embedded
    /// in-memory implementation; configured SQL installs Go's unsupported
    /// backend adapter because Cadence SQL has no semaphore schema.
    pub semaphore_persistence: Option<Arc<dyn cadence_persistence::SemaphorePersistence>>,
    pub limits: Limits,
    pub pollers: Arc<std::sync::Mutex<PollerMap>>,
    /// Shard-distributor namespaces and ownership. Memory-only like Go's
    /// cached coordination state; namespaces appear on first heartbeat.
    pub shard_distributor: Arc<std::sync::Mutex<crate::grpc_shard::ShardDistributorState>>,
}

impl Default for AppState {
    fn default() -> Self {
        let domains = InMemoryDomainStore::new();
        Self {
            matching_http_isolated: false,
            admin: crate::admin::AdminState::default(),
            num_history_shards: 4,
            scheduler_enabled: false,
            semaphore_enabled: false,
            current_cluster_name: "cluster0".to_string(),
            cluster_metadata: cadence_common::cluster::Metadata::default(),
            grpc_max_message_size: 4 * 1024 * 1024,
            frontend_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            frontend_shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            frontend_accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            frontend_admin_shutdown: tokio::sync::watch::channel(false).0,
            frontend_limiters: Arc::new(std::sync::Mutex::new(Vec::new())),
            background_shutdown: tokio::sync::watch::channel(false).0,
            domains: domains.clone(),
            domain_manager: Arc::new(domains),
            executions: InMemoryExecutionStore::new(),
            semaphore_persistence: None,
            limits: Limits::cadence_default(),
            pollers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            shard_distributor: Arc::new(std::sync::Mutex::new(
                crate::grpc_shard::ShardDistributorState::default(),
            )),
        }
    }
}

impl AppState {
    /// Stop accepting API requests, matching Go's WorkflowHandler.Stop.
    pub fn stop_frontend_handler(&self) {
        self.frontend_accepting
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Stop AdminHandler-owned background workers before request draining.
    /// Admin modules subscribe when they own a long-lived task.
    pub fn stop_frontend_admin_handler(&self) {
        self.frontend_admin_shutdown.send_replace(true);
    }

    /// Register a collection when the Frontend dependency graph constructs it.
    pub fn register_frontend_limiter(&self, limiter: Arc<dyn FrontendLimiterCollection>) {
        self.frontend_limiters
            .lock()
            .expect("frontend limiter registry lock")
            .push(limiter);
    }

    /// Stop all constructed global limiter collections with Go's shared
    /// one-second deadline. Failures are logged independently so one broken
    /// collection cannot prevent the remaining owners from stopping.
    pub async fn stop_frontend_limiters(&self) {
        let limiters = std::mem::take(
            &mut *self
                .frontend_limiters
                .lock()
                .expect("frontend limiter registry lock"),
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        for limiter in limiters {
            match tokio::time::timeout_at(deadline, limiter.on_stop()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!(
                    "failed to stop Frontend global rate limiter collection {}: {error}",
                    limiter.name()
                ),
                Err(_) => eprintln!(
                    "timed out stopping Frontend global rate limiter collection {}",
                    limiter.name()
                ),
            }
        }
    }

    /// Stop service-owned background loops, matching Go Frontend's `close(s.stopC)`.
    pub fn stop_background_workers(&self) {
        self.background_shutdown.send_replace(true);
    }

    pub fn semaphore_store_for_domain(
        &self,
        domain: &str,
    ) -> Result<(Arc<dyn cadence_persistence::SemaphorePersistence>, String), EntityNotExistsError> {
        let metadata = self.domain_manager.describe(&cadence_api::service_types::DescribeDomainRequest {
            name: Some(domain.to_string()), uuid: None,
        })?;
        let domain_id = metadata.domain_info.map(|info| info.uuid).unwrap_or_default();
        if let Some(store) = &self.semaphore_persistence {
            // Preserve the frontend metadata lookup before calling persistence.
            self.execution_store(domain)?;
            return Ok((store.clone(), domain_id));
        }
        Ok((Arc::new(self.execution_store(domain)?), domain_id))
    }

    pub fn domain_policy(&self, domain: &str) -> cadence_persistence::domain_update::DomainPolicy {
        use cadence_persistence::{domain_update::DomainPolicy, dynamic_config};
        let overlay = self.admin.dynamic_config.lock().expect("admin config lock");
        let defaults = DomainPolicy::default();
        DomainPolicy {
            cooldown_nanos: dynamic_config::duration(
                overlay.property_values("frontend.failoverCoolDown"),
                Some(domain),
                defaults.cooldown_nanos,
            ),
            min_retention_days: dynamic_config::integer(
                overlay.property_values("system.minRetentionDays"),
                None,
                i64::from(defaults.min_retention_days),
            ) as i32,
            max_bad_binaries: dynamic_config::integer(
                overlay.property_values("frontend.maxBadBinaries"),
                Some(domain),
                defaults.max_bad_binaries,
            ),
            history_limit: dynamic_config::integer(
                overlay.property_values("frontend.failoverHistoryMaxSize"),
                Some(domain),
                defaults.history_limit,
            ),
        }
    }

    /// Read the scheduler gate through the same per-domain dynamic-config
    /// overlay used by Frontend property readers and Admin updates.
    pub fn scheduler_enabled_for_domain(&self, domain: &str) -> bool {
        let overlay = self.admin.dynamic_config.lock().expect("admin config lock");
        cadence_persistence::dynamic_config::boolean(
            overlay.property_values("worker.enableScheduler"),
            Some(domain),
            self.scheduler_enabled,
        )
    }

    pub fn execution_store(
        &self,
        domain: &str,
    ) -> Result<InMemoryExecutionStore, EntityNotExistsError> {
        let metadata = self.domain_manager.describe(&DescribeDomainRequest {
            name: Some(domain.to_string()), uuid: None,
        })?;
        let id = metadata.domain_info.unwrap_or_default().uuid;
        let version = if metadata.is_global_domain {
            metadata.failover_version
        } else {
            cadence_persistence::version_history::EMPTY_VERSION
        };
        self.executions.set_domain_version(&id, version);
        Ok(self.executions.with_domain_id(&id))
    }
}

/// Go domain registration uses the current cluster when replication fields
/// are omitted. Local domains belong to exactly that cluster.
pub fn domain_replication(
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
    let active_clusters = cadence_persistence::domain_replication::registration_attributes(
        &state.cluster_metadata,
        req.is_global_domain,
        req.active_clusters.as_ref(),
    )?;
    let replication = cadence_api::types::DomainReplicationConfiguration {
        active_cluster_name,
        clusters,
        active_clusters,
    };
    if req.is_global_domain {
        cadence_persistence::domain_replication::validate_global(
            &state.cluster_metadata,
            &replication,
        )?;
    } else {
        if replication.active_cluster_name != state.current_cluster_name {
            return Err(BadRequestError::new("Invalid local domain active cluster"));
        }
        if replication.clusters.len() != 1
            || replication.clusters[0].cluster_name != replication.active_cluster_name
        {
            return Err(BadRequestError::new("Invalid local domain clusters"));
        }
    }
    Ok(replication)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Go `getTask` rate: the matching override wins, then the worker's
/// `maxTasksPerSecond`, and a missing or negative rate falls back to the
/// dispatch default.
pub fn poller_rate(
    state: &AppState,
    domain: &str,
    task_list: &str,
    task_type: i32,
    worker_rate: Option<f64>,
) -> f64 {
    let override_rate = {
        let overlay = state
            .admin
            .dynamic_config
            .lock()
            .expect("admin config lock");
        cadence_persistence::dynamic_config::task_list_float(
            overlay.property_values("matching.overrideTaskListRps"),
            Some(domain),
            task_list,
            task_type,
            0.0,
        )
    };
    let rate = if override_rate > 0.0 {
        Some(override_rate)
    } else {
        worker_rate
    };
    rate.filter(|rate| *rate >= 0.0)
        .unwrap_or(TASK_DISPATCH_RPS)
}

/// Go `StartPoll`: records the identity's poll as outstanding. Dropping the
/// returned guard is Go `EndPoll`, which clears outstanding and refreshes time.
pub fn note_poller(
    state: &AppState,
    domain: &str,
    task_list: &str,
    identity: &str,
    kind: &str,
    rate_per_second: f64,
    isolation_group: &str,
) -> Option<PollerGuard> {
    if task_list.is_empty() || identity.is_empty() {
        return None;
    }
    let key = (
        domain.to_owned(),
        task_list.to_string(),
        identity.to_string(),
        kind.to_string(),
    );
    state.pollers.lock().expect("poller lock").insert(
        key.clone(),
        PollerRecord {
            seen_millis: now_millis(),
            outstanding: true,
            rate_per_second,
            isolation_group: isolation_group.to_owned(),
        },
    );
    Some(PollerGuard {
        pollers: state.pollers.clone(),
        key,
    })
}

/// Ends one poll when the handler returns or its future is dropped.
pub struct PollerGuard {
    pollers: Arc<std::sync::Mutex<PollerMap>>,
    key: (String, String, String, String),
}

impl Drop for PollerGuard {
    fn drop(&mut self) {
        if let Some(record) = self.pollers.lock().expect("poller lock").get_mut(&self.key) {
            record.outstanding = false;
            record.seen_millis = now_millis();
        }
    }
}

/// Reads the worker's isolation group header; absent means no group.
fn isolation_group_header(headers: &HeaderMap) -> &str {
    headers
        .get(ISOLATION_GROUP_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
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

pub fn error_response(kind: &'static str, status: StatusCode, message: String) -> Response {
    (status, Json(ErrorBody { kind, message })).into_response()
}

pub fn bad_request(e: BadRequestError) -> Response {
    error_response("BadRequestError", StatusCode::BAD_REQUEST, e.message)
}

pub fn entity_not_exists(e: EntityNotExistsError) -> Response {
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

async fn frontend_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ok = state
        .frontend_ready
        .load(std::sync::atomic::Ordering::Acquire);
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(serde_json::json!({"ok": ok})))
}

async fn reject_frontend_during_shutdown(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if request.uri().path() != "/health"
        && !state
            .frontend_accepting
            .load(std::sync::atomic::Ordering::Acquire)
    {
        return error_response(
            "InternalServiceError",
            StatusCode::SERVICE_UNAVAILABLE,
            "Frontend is shutting down.".to_string(),
        );
    }
    next.run(request).await
}

// ---------------------------------------------------------------------------
// Domains
// ---------------------------------------------------------------------------

/// Mirrors Go `ValidateRegisterDomainRequest` core checks + `CreateDomain`.
async fn register_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterDomainRequest>,
) -> Response {
    // Go checks primary-cluster ownership before inspecting the requested
    // name, so preserve that error precedence for global-domain registration.
    if req.is_global_domain && !state.cluster_metadata.is_primary_cluster() {
        return bad_request(BadRequestError::new(
            "Cluster is not primary cluster, cannot do domain registration or domain update.",
        ));
    }
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if req.name.len() > state.limits.ids.domain_max {
        return bad_request(BadRequestError::new("Domain length exceeds limit."));
    }
    if req.workflow_execution_retention_period_in_days
        < state.domain_policy(&req.name).min_retention_days
    {
        return bad_request(BadRequestError::new(
            "A valid retention period is not set on request.",
        ));
    }
    let info = cadence_api::types::DomainInfo {
        name: req.name.clone(),
        status: Some(DomainStatus::Registered),
        description: req.description.clone(),
        owner_email: req.owner_email.clone(),
        data: req.data.clone(),
        uuid: uuid::Uuid::now_v7().to_string(),
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
        match state.cluster_metadata.next_failover_version(
            &replication.active_cluster_name,
            0,
            false,
        ) {
            Ok(version) => version,
            Err(e) => return bad_request(BadRequestError::new(e)),
        }
    } else { cadence_persistence::version_history::EMPTY_VERSION };
    match state
        .domain_manager
        .create_with_version(info, configuration, replication, req.is_global_domain, failover_version)
    {
        Ok(resp) => {
            state.executions.register_domain(
                &req.name,
                &state
                    .domain_manager
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
    match state.domain_manager.describe(&DescribeDomainRequest {
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
    match state.domain_manager.list_page(&request) {
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
    let (semaphores, domain_id) = match state.semaphore_store_for_domain(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match semaphores
        .create_semaphore(
            &domain_id,
            &req.semaphore_name,
            req.capacity,
            req.bucket_capacity,
        )
        .await
    {
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
        Err(cadence_persistence::SemaphoreError::UnsupportedBackend) => error_response(
            "BadRequestError",
            StatusCode::BAD_REQUEST,
            cadence_persistence::SqlUnsupportedSemaphorePersistence::MESSAGE.to_string(),
        ),
        Err(cadence_persistence::SemaphoreError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::SemaphoreError::Internal(message)) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
        ),
        Err(cadence_persistence::SemaphoreError::Throttled(message)) => {
            error_response("ServiceBusyError", StatusCode::SERVICE_UNAVAILABLE, message)
        }
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
        .domain_manager
        .failover(&req, &state.current_cluster_name, &state.cluster_metadata, &state.domain_policy(&req.domain_name))
    {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::FailoverError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::FailoverError::Busy(message)) => {
            error_response("ServiceBusyError", StatusCode::SERVICE_UNAVAILABLE, message)
        }
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
        .domain_manager
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
    match state.domain_manager.update(&req, &state.cluster_metadata, &state.domain_policy(&req.name)) {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(cadence_persistence::FailoverError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::FailoverError::Busy(message)) => {
            error_response("ServiceBusyError", StatusCode::SERVICE_UNAVAILABLE, message)
        }
        Err(cadence_persistence::FailoverError::Invalid(message)) => {
            bad_request(BadRequestError::new(message))
        }
    }
}

async fn deprecate_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeprecateDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    let current = match state.domain_manager.describe(&DescribeDomainRequest {
        name: Some(req.name.clone()),
        uuid: None,
    }) {
        Ok(domain) => domain,
        Err(error) => return entity_not_exists(error),
    };
    if current.is_global_domain && !state.cluster_metadata.is_primary_cluster() {
        return bad_request(BadRequestError::new(
            "Cluster is not primary cluster, cannot do domain registration or domain update.",
        ));
    }
    match state.domain_manager.deprecate(&req) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => entity_not_exists(e),
    }
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

pub async fn require_domain(state: &AppState, domain: &str) -> Option<Response> {
    if domain.is_empty() {
        return Some(bad_request(validation::err_domain_not_set()));
    }
    state.domain_manager.domain_id(domain).err().map(entity_not_exists)
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

/// HTTP liveness endpoint for roles that do not host public frontend APIs.
pub fn health_router() -> Router {
    Router::new().route("/health", get(health))
}

pub fn router(state: AppState) -> Router {
    let shared = Arc::new(state);
    Router::new()
        .route("/health", get(frontend_health))
        .route("/api/v1/tasks/reset-sticky", post(reset_sticky_task_list))
        .route("/api/v1/domains/register", post(register_domain))
        .route("/api/v1/domains/describe", get(describe_domain))
        .route("/api/v1/domains/list", get(list_domains))
        .route("/api/v1/domains/update", put(update_domain))
        .route("/api/v1/domains/failover", post(failover_domain))
        .route(
            "/api/v1/domains/failover-history",
            post(list_failover_history),
        )
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
        .route(
            "/api/v1/workflows/refresh-tasks",
            post(refresh_workflow_tasks),
        )
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
        .route(
            "/api/v1/admin/cluster/describe",
            get(crate::admin::describe_cluster),
        )
        .route(
            "/api/v1/admin/workflows/delete",
            post(crate::admin::delete_workflow),
        )
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
        .route(
            "/api/v1/admin/shards/close",
            post(crate::admin::close_shard),
        )
        .route(
            "/api/v1/admin/tasks/remove",
            post(crate::admin::remove_task),
        )
        .route(
            "/api/v1/admin/queues/reset",
            post(crate::admin::reset_queue),
        )
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
        .layer(middleware::from_fn_with_state(
            shared.clone(),
            reject_frontend_during_shutdown,
        ))
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
    headers: HeaderMap,
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
    let rate = poller_rate(&state, &req.domain, &task_list, DECISION_TASK_TYPE, None);
    let _poller = note_poller(
        &state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "decision",
        rate,
        isolation_group_header(&headers),
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
    headers: HeaderMap,
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
    let worker_rate = req
        .task_list_metadata
        .as_ref()
        .and_then(|metadata| metadata.max_tasks_per_second);
    let rate = poller_rate(
        &state,
        &req.domain,
        &task_list,
        ACTIVITY_TASK_TYPE,
        worker_rate,
    );
    let _poller = note_poller(
        &state,
        executions.namespace_id().expect("scoped domain"),
        &task_list,
        &req.identity,
        "activity",
        rate,
        isolation_group_header(&headers),
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
                diagnostic_workflow_execution: Some(cadence_api::types::WorkflowExecution::new(
                    diagnostic_id,
                    resp.run_id,
                )),
            }),
        )
            .into_response(),
        Err(cadence_persistence::StartError::AlreadyStarted(e)) => (
            StatusCode::OK,
            Json(cadence_api::types::DiagnoseWorkflowExecutionResponse {
                domain: system_domain.to_string(),
                diagnostic_workflow_execution: Some(cadence_api::types::WorkflowExecution::new(
                    diagnostic_id,
                    e.run_id,
                )),
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
        Err(cadence_persistence::QueryError::Rejected(_)) => {
            bad_request(BadRequestError::new("Query task was rejected."))
        }
    }
}

async fn delete_domain(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeleteDomainRequest>,
) -> Response {
    if req.name.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    let current = match state.domain_manager.describe(&DescribeDomainRequest {
        name: Some(req.name.clone()),
        uuid: None,
    }) {
        Ok(domain) => domain,
        Err(error) => return entity_not_exists(error),
    };
    if current.is_global_domain && !state.cluster_metadata.is_primary_cluster() {
        return bad_request(BadRequestError::new(
            "Cluster is not primary cluster, cannot do domain registration or domain update.",
        ));
    }
    match state.domain_manager.delete(&req.name) {
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
pub enum DescribeTaskListError {
    TaskListNotSet,
    DomainGone,
}

/// Reject schedule APIs when the scheduler is disabled, mirroring Go's
/// `EnableScheduler` gate with its exact message.
fn require_scheduler(state: &AppState, domain: &str) -> Option<Response> {
    if state.scheduler_enabled_for_domain(domain) {
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
    let page_size = if req.page_size <= 0 {
        10
    } else {
        req.page_size as usize
    };
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

pub fn describe_task_list_response(
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
    let task_type = if kind == "activity" {
        ACTIVITY_TASK_TYPE
    } else {
        DECISION_TASK_TYPE
    };
    let domain_id = match executions.namespace_id() {
        Some(id) => id.to_owned(),
        None => {
            return Err(DomainGone);
        }
    };
    // Go `DescribeTaskList` counts pollers per configured isolation group,
    // over outstanding polls and polls seen within the poller window.
    let (groups, window) = if req.include_task_list_status {
        let overlay = state
            .admin
            .dynamic_config
            .lock()
            .expect("admin config lock");
        (
            cadence_persistence::dynamic_config::string_list(
                overlay.property_values("system.allIsolationGroups"),
            ),
            cadence_persistence::dynamic_config::task_list_duration(
                overlay.property_values("matching.taskIsolationPollerWindow"),
                Some(req.domain.as_str()),
                &task_list_name,
                task_type,
                2_000_000_000,
            ),
        )
    } else {
        (Vec::new(), 0)
    };
    let pollers_map = state.pollers.lock().expect("poller lock");
    let mut pollers: Vec<cadence_api::types::PollerInfo> = pollers_map
        .iter()
        .filter(|((domain, tl, _, poll_kind), _)| {
            domain == &domain_id && tl == &task_list_name && poll_kind == kind
        })
        .filter(|(_, record)| record.seen_millis >= cutoff)
        .map(
            |((_, _, identity, _), record)| cadence_api::types::PollerInfo {
                identity: identity.clone(),
                last_access_time: Some(record.seen_millis),
                rate_per_second: record.rate_per_second,
            },
        )
        .collect();
    pollers.sort_by(|a, b| a.identity.cmp(&b.identity));
    pollers.dedup_by(|a, b| a.identity == b.identity);
    let task_list_status = if req.include_task_list_status {
        // Window is configured in nanoseconds; `seen_millis` is milliseconds.
        let window_start = now_millis() - window / 1_000_000;
        let mut poller_counts: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for ((domain, tl, _, poll_kind), record) in pollers_map.iter() {
            if domain == &domain_id
                && tl == &task_list_name
                && poll_kind == kind
                && !record.isolation_group.is_empty()
                && (record.outstanding || record.seen_millis > window_start)
            {
                *poller_counts
                    .entry(record.isolation_group.clone())
                    .or_default() += 1;
            }
        }
        let isolation_group_metrics = groups
            .into_iter()
            .map(|group| {
                let poller_count = poller_counts.get(&group).copied().unwrap_or_default();
                (
                    group,
                    cadence_api::types::IsolationGroupMetrics {
                        poller_count,
                        ..Default::default()
                    },
                )
            })
            .collect();
        Some(cadence_api::types::TaskListStatus {
            backlog_count_hint: backlog as i64,
            isolation_group_metrics,
            ..Default::default()
        })
    } else {
        None
    };
    drop(pollers_map);
    Ok(cadence_api::tasks::DescribeTaskListResponse {
        pollers,
        task_list_status,
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

pub fn search_attributes(state: &AppState) -> cadence_api::tasks::GetSearchAttributesResponse {
    let mut keys = cadence_api::search_attributes::default_indexed_keys();
    if let Some(value) = state
        .admin
        .dynamic_config
        .lock()
        .expect("admin config lock")
        .get(false, "frontend.validSearchAttributes", &[])
        .and_then(|entry| entry.value)
    {
        if let Ok(configured) =
            serde_json::from_slice::<std::collections::HashMap<String, i32>>(&value.data)
        {
            keys.extend(configured.into_iter().filter_map(|(key, raw_type)| {
                cadence_api::enums::IndexedValueType::from_i32(raw_type)
                    .map(|value_type| (key, value_type))
            }));
        }
    }
    keys.extend(
        state
            .admin
            .custom_search_attributes
            .lock()
            .expect("admin lock")
            .iter()
            .map(|(key, value_type)| (key.clone(), *value_type)),
    );
    cadence_api::tasks::GetSearchAttributesResponse { keys: Some(keys) }
}

#[cfg(test)]
mod frontend_drain_tests {
    use super::*;
    use axum::body::Body;
    use http::{Request, StatusCode};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    #[derive(Debug)]
    struct TestLimiter(Arc<AtomicUsize>);

    impl FrontendLimiterCollection for TestLimiter {
        fn name(&self) -> &'static str {
            "test"
        }

        fn on_stop(
            &self,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn stop_background_workers_broadcasts_the_go_stop_signal() {
        let state = AppState::default();
        let mut shutdown = state.background_shutdown.subscribe();
        assert!(!*shutdown.borrow());

        state.stop_background_workers();
        shutdown.changed().await.unwrap();

        assert!(*shutdown.borrow());
    }

    #[tokio::test]
    async fn frontend_handler_stop_hooks_are_independent() {
        let state = AppState::default();
        let mut admin_shutdown = state.frontend_admin_shutdown.subscribe();
        assert!(state
            .frontend_accepting
            .load(std::sync::atomic::Ordering::Acquire));

        state.stop_frontend_handler();
        assert!(!state
            .frontend_accepting
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(!*admin_shutdown.borrow());

        state.stop_frontend_admin_handler();
        admin_shutdown.changed().await.unwrap();
        assert!(*admin_shutdown.borrow());
    }

    #[tokio::test]
    async fn frontend_stop_invokes_registered_limiter_owners_once() {
        let state = AppState::default();
        let stopped = Arc::new(AtomicUsize::new(0));
        state.register_frontend_limiter(Arc::new(TestLimiter(stopped.clone())));

        state.stop_frontend_limiters().await;
        state.stop_frontend_limiters().await;

        assert_eq!(stopped.load(Ordering::SeqCst), 1);
        assert!(state.frontend_limiters.lock().unwrap().is_empty());
    #[test]
    fn scheduler_gate_reads_domain_filtered_dynamic_config_updates() {
        use cadence_api::{
            admin::{DynamicConfigFilter, DynamicConfigValue},
            enums::EncodingType,
            types::DataBlob,
        };

        let state = AppState::default();
        let domain_filter = DynamicConfigFilter {
            name: "domainName".into(),
            value: Some(DataBlob {
                encoding_type: Some(EncodingType::JSON),
                data: br#""payments""#.to_vec(),
            }),
        };
        state.admin.dynamic_config.lock().unwrap().update(
            false,
            "worker.enableScheduler",
            vec![DynamicConfigValue {
                value: Some(DataBlob {
                    encoding_type: Some(EncodingType::JSON),
                    data: b"true".to_vec(),
                }),
                filters: vec![domain_filter],
            }],
        );

        assert!(state.scheduler_enabled_for_domain("payments"));
        assert!(!state.scheduler_enabled_for_domain("billing"));
    }

    #[test]
    fn default_frontend_domain_manager_keeps_matching_cache_shared() {
        let state = AppState::default();
        state
            .domain_manager
            .create_with_version(
                cadence_api::types::DomainInfo {
                    name: "shared-domain".into(),
                    uuid: "shared-id".into(),
                    ..Default::default()
                },
                cadence_api::types::DomainConfiguration::default(),
                cadence_api::types::DomainReplicationConfiguration::default(),
                false,
                cadence_persistence::version_history::EMPTY_VERSION,
            )
            .unwrap();

        assert_eq!(
            state.domains.domain_id("shared-domain").unwrap(),
            "shared-id"
        );
    }

    #[tokio::test]
    async fn frontend_health_fails_before_admission_closes() {
        let state = AppState::default();
        let ready = state.frontend_ready.clone();
        let accepting = state.frontend_accepting.clone();
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        ready.store(false, std::sync::atomic::Ordering::Release);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Requests continue while peers discover the unhealthy state; the
        // admission gate closes only after that interval.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/domains/list")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        accepting.store(false, std::sync::atomic::Ordering::Release);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/domains/list")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

async fn get_search_attributes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(search_attributes(&state))
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
