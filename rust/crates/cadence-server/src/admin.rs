//! Admin service state and JSON handlers, mirroring Go's frontend admin
//! handler (`go/service/frontend/admin/handler.go`).
//!
//! The dynamic-config overlay stands in for Go's config-store client until
//! file/DB-backed dynamic config is ported: two namespaces (normal and
//! operational), each a map from config name to its ordered values. Gets
//! match the request filters exactly like the config-store client; updates
//! replace the value list; restores drop matching values (all values when
//! no filters are given).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};

#[derive(Debug, Clone, Default)]
pub struct DynamicConfigOverlay {
    normal: HashMap<String, Vec<DynamicConfigValue>>,
    operational: HashMap<String, Vec<DynamicConfigValue>>,
}

fn filters_match(value: &DynamicConfigValue, filters: &[DynamicConfigFilter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    if value.filters.len() != filters.len() {
        return false;
    }
    filters.iter().all(|f| {
        value.filters.iter().any(|vf| {
            vf.name == f.name
                && vf.value.as_ref().map(|v| v.data.clone())
                    == f.value.as_ref().map(|v| v.data.clone())
        })
    })
}

impl DynamicConfigOverlay {
    fn map(&self, operational: bool) -> &HashMap<String, Vec<DynamicConfigValue>> {
        if operational {
            &self.operational
        } else {
            &self.normal
        }
    }

    fn map_mut(&mut self, operational: bool) -> &mut HashMap<String, Vec<DynamicConfigValue>> {
        if operational {
            &mut self.operational
        } else {
            &mut self.normal
        }
    }

    /// Get the first value whose filters match, mirroring the config-store
    /// client's filtered read.
    pub fn get(
        &self,
        operational: bool,
        name: &str,
        filters: &[DynamicConfigFilter],
    ) -> Option<DynamicConfigValue> {
        self.map(operational).get(name)?.iter().find(|v| filters_match(v, filters)).cloned()
    }

    /// Replace the value list, mirroring the config-store client's update.
    pub fn update(&mut self, operational: bool, name: &str, values: Vec<DynamicConfigValue>) {
        self.map_mut(operational).insert(name.to_string(), values);
    }

    /// Drop matching values (all values when no filters are given),
    /// mirroring the config-store client's restore.
    pub fn restore(&mut self, operational: bool, name: &str, filters: &[DynamicConfigFilter]) {
        if let Some(values) = self.map_mut(operational).get_mut(name) {
            values.retain(|v| !filters_match(v, filters));
        }
    }

    /// List entries, optionally filtered to one config name like Go's
    /// `ListDynamicConfig` with a name filter.
    pub fn list(
        &self,
        operational: bool,
        name: &str,
    ) -> Vec<cadence_api::admin::DynamicConfigEntry> {
        self
            .map(operational)
            .iter()
            .filter(|(key, _)| name.is_empty() || *key == name)
            .map(|(key, values)| cadence_api::admin::DynamicConfigEntry {
                name: key.clone(),
                values: values.clone(),
            })
            .collect()
    }
}

/// Shared admin state: the dynamic-config overlay plus custom (non-system)
/// search attribute keys registered via `AddSearchAttribute`.
#[derive(Debug, Clone, Default)]
pub struct AdminState {
    pub dynamic_config: Arc<Mutex<DynamicConfigOverlay>>,
    pub custom_search_attributes:
        Arc<Mutex<HashMap<String, cadence_api::enums::IndexedValueType>>>,
}

// ---------------------------------------------------------------------------
// JSON handlers (also back the gRPC adapters via in-process calls)
// ---------------------------------------------------------------------------

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::sync::Arc as StdArc;

use crate::handlers::{bad_request, entity_not_exists, error_response, require_domain, AppState};
use cadence_api::errors::BadRequestError;
use cadence_api::validation;

fn ok_json<T: serde::Serialize>(value: T) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

/// Cluster membership and persistence backends, mirroring Go admin
/// `DescribeCluster`. Single host: one member per role, in-memory
/// persistence without advanced visibility.
pub fn describe_cluster_response(hostname: &str) -> cadence_api::admin::DescribeClusterResponse {
    let rings = ["frontend", "history", "matching", "worker"]
        .into_iter()
        .map(|role| cadence_api::admin::RingInfo {
            role: role.to_string(),
            member_count: 1,
            members: vec![cadence_api::admin::HostInfo {
                identity: hostname.to_string(),
            }],
        })
        .collect();
    let mut persistence_info = std::collections::HashMap::new();
    for backend in ["history", "visibility"] {
        persistence_info.insert(
            backend.to_string(),
            cadence_api::admin::PersistenceInfo {
                backend: "memory".to_string(),
                features: vec![cadence_api::admin::PersistenceFeature {
                    key: "advancedVisibilityEnabled".to_string(),
                    enabled: false,
                }],
            },
        );
    }
    cadence_api::admin::DescribeClusterResponse {
        supported_client_versions: Some(cadence_api::types::SupportedClientVersions {
            go_sdk: "v1.4.0".to_string(),
            java_sdk: "v3.9.0".to_string(),
        }),
        membership_info: Some(cadence_api::admin::MembershipInfo {
            current_host: Some(cadence_api::admin::HostInfo {
                identity: hostname.to_string(),
            }),
            reachable_members: vec![hostname.to_string()],
            rings,
        }),
        persistence_info: Some(persistence_info),
    }
}

pub async fn describe_cluster(State(_state): State<StdArc<AppState>>) -> Response {
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
    ok_json(describe_cluster_response(&hostname))
}

/// Delete a workflow run's data, mirroring Go admin `DeleteWorkflow`.
pub async fn delete_workflow(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::DeleteWorkflowRequest>,
) -> Response {
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() {
        return bad_request(BadRequestError::new("Execution is not set on request."));
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match executions.delete_run(&req.domain, &execution) {
        Ok((history, executions, visibility)) => {
            ok_json(cadence_api::admin::DeleteWorkflowResponse {
                history_deleted: history,
                executions_deleted: executions,
                visibility_deleted: visibility,
            })
        }
        Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => {
            error_response(
                "WorkflowExecutionAlreadyCompletedError",
                StatusCode::BAD_REQUEST,
                error.message,
            )
        }
    }
}

/// Refresh an execution's dispatch queues, mirroring Go admin
/// `RefreshWorkflowTasks` (which delegates to history refresh).
pub async fn refresh_workflow_tasks(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RefreshWorkflowTasksRequest>,
) -> Response {
    // Go resolves the domain by name, falling back to the UUID.
    let domain = if req.domain.is_empty() {
        match state.domains.describe(
            &cadence_api::service_types::DescribeDomainRequest {
                name: None,
                uuid: Some(req.domain_id.clone()),
            },
        ) {
            Ok(desc) => desc.domain_info.map(|i| i.name).unwrap_or_default(),
            Err(error) => return entity_not_exists(error),
        }
    } else {
        req.domain.clone()
    };
    if domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    if let Some(e) = require_domain(&state, &domain).await {
        return e;
    }
    let executions = match state.execution_store(&domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if let Err(e) = validation::check_execution(Some(&execution)) {
        return bad_request(e);
    }
    match executions.refresh_tasks(&domain, &execution) {
        Ok(()) => ok_json(cadence_api::admin::RefreshWorkflowTasksResponse {}),
        Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => {
            error_response(
                "WorkflowExecutionAlreadyCompletedError",
                StatusCode::BAD_REQUEST,
                error.message,
            )
        }
    }
}

/// Register custom search attributes, mirroring Go admin
/// `AddSearchAttribute`: system keys and type conflicts are rejected, and
/// accepted keys join the custom registry.
pub async fn add_search_attribute(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::AddSearchAttributeRequest>,
) -> Response {
    let attrs = match req.search_attribute {
        Some(attrs) if !attrs.is_empty() => attrs,
        _ => {
            return bad_request(BadRequestError::new("SearchAttributes are not provided"));
        }
    };
    let mut custom = state
        .admin
        .custom_search_attributes
        .lock()
        .expect("admin lock");
    for (key, value_type) in &attrs {
        if cadence_api::search_attributes::is_system_indexed_key(key) {
            return bad_request(BadRequestError::new(format!(
                "Key [{key}] is reserved by system"
            )));
        }
        if let Some(existing) = custom.get(key) {
            if existing != value_type {
                return bad_request(BadRequestError::new(format!(
                    "Key [{key}] is already whitelisted as a different type"
                )));
            }
        }
    }
    for (key, value_type) in attrs {
        custom.insert(key, value_type);
    }
    ok_json(cadence_api::admin::AddSearchAttributeResponse {})
}

/// Read a dynamic-config value, mirroring Go admin `GetDynamicConfig`.
pub async fn get_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    match overlay.get(false, &req.config_name, &req.filters) {
        Some(entry) => ok_json(cadence_api::admin::GetDynamicConfigResponse {
            value: entry.value,
        }),
        None => entity_not_exists(cadence_api::errors::EntityNotExistsError::new(format!(
            "Dynamic config {:?} does not have a value for the given filters.",
            req.config_name
        ))),
    }
}

/// Replace a dynamic-config value list, mirroring Go admin
/// `UpdateDynamicConfig`.
pub async fn update_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(false, &req.config_name, req.config_values);
    ok_json(cadence_api::admin::UpdateDynamicConfigResponse {})
}

/// Drop dynamic-config values, mirroring Go admin `RestoreDynamicConfig`.
pub async fn restore_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RestoreDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .restore(false, &req.config_name, &req.filters);
    ok_json(cadence_api::admin::RestoreDynamicConfigResponse {})
}

/// List dynamic-config entries, mirroring Go admin `ListDynamicConfig`.
pub async fn list_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ListDynamicConfigRequest>,
) -> Response {
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    ok_json(cadence_api::admin::ListDynamicConfigResponse {
        entries: overlay.list(false, &req.config_name),
    })
}

/// Operational-config variants share the overlay's second namespace,
/// mirroring Go's operational dynamic-config client.
pub async fn get_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    match overlay.get(true, &req.config_name, &req.filters) {
        Some(entry) => ok_json(cadence_api::admin::GetDynamicConfigResponse {
            value: entry.value,
        }),
        None => entity_not_exists(cadence_api::errors::EntityNotExistsError::new(format!(
            "Dynamic config {:?} does not have a value for the given filters.",
            req.config_name
        ))),
    }
}

/// Replace an operational dynamic-config value list.
pub async fn update_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .update(true, &req.config_name, req.config_values);
    ok_json(cadence_api::admin::UpdateDynamicConfigResponse {})
}

/// Drop operational dynamic-config values.
pub async fn restore_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RestoreDynamicConfigRequest>,
) -> Response {
    if req.config_name.is_empty() {
        return bad_request(BadRequestError::new("ConfigName is not set on request."));
    }
    state
        .admin
        .dynamic_config
        .lock()
        .expect("admin lock")
        .restore(true, &req.config_name, &req.filters);
    ok_json(cadence_api::admin::RestoreDynamicConfigResponse {})
}

/// List operational dynamic-config entries.
pub async fn list_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ListDynamicConfigRequest>,
) -> Response {
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    ok_json(cadence_api::admin::ListDynamicConfigResponse {
        entries: overlay.list(true, &req.config_name),
    })
}

/// Reapply signal events to the current run, mirroring Go admin
/// `ReapplyEvents` for the single-branch case. The events blob holds JSON
/// history events; only signal events are applied.
pub async fn reapply_events(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ReapplyEventsRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(validation::err_domain_not_set());
    }
    let execution = req.workflow_execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() {
        return bad_request(BadRequestError::new("WorkflowID is not set on request."));
    }
    // Go quirks a missing events blob into ErrWorkflowIDNotSet.
    let blob = match req.events.as_ref() {
        Some(b) => b,
        None => {
            return bad_request(BadRequestError::new("WorkflowID is not set on request."));
        }
    };
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    // Single-branch store: the blob is a JSON array of history events.
    let events: Vec<cadence_api::service_types::HistoryEvent> =
        serde_json::from_slice(&blob.data).unwrap_or_default();
    let signals: Vec<_> = events
        .into_iter()
        .filter(|e| {
            e.event_type == Some(cadence_api::enums::EventType::WorkflowExecutionSignaled)
        })
        .collect();
    match executions.reapply_signals(&req.domain, &execution.workflow_id, signals) {
        Ok(()) => ok_json(cadence_api::admin::ReapplyEventsResponse {}),
        Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => {
            error_response(
                "WorkflowExecutionAlreadyCompletedError",
                StatusCode::BAD_REQUEST,
                error.message,
            )
        }
    }
}

/// Single-host replication surface: no replication streams exist, so message
/// fetches are empty. Request validation mirrors Go.
pub async fn get_replication_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetReplicationMessagesRequest>,
) -> Response {
    if req.cluster_name.is_empty() {
        return bad_request(BadRequestError::new("ClusterName is not set on request."));
    }
    ok_json(serde_json::json!({ "shardMessages": {} }))
}

/// Single-host DLQ replication surface: no dead-letter queues exist.
pub async fn get_dlq_replication_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDLQReplicationMessagesRequest>,
) -> Response {
    let _ = req;
    ok_json(serde_json::json!({ "replicationTasks": [] }))
}

/// Domain replication requires the domain replication queue, which does not
/// exist on a single host. Mirrors Go's exact error.
pub async fn get_domain_replication_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDomainReplicationMessagesRequest>,
) -> Response {
    let _ = req;
    error_response(
        "InternalServiceError",
        StatusCode::INTERNAL_SERVER_ERROR,
        "domain replication queue not enabled for cluster".to_string(),
    )
}

/// Single-host DLQ counts: no dead letters exist.
pub async fn count_dlq_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::CountDLQMessagesRequest>,
) -> Response {
    let _ = req;
    ok_json(cadence_api::admin::CountDLQMessagesResponse {
        history: vec![],
        domain: 0,
    })
}

/// Single-host DLQ reads: callers must name the queue type, like Go.
pub async fn read_dlq_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ReadDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    ok_json(serde_json::json!({ "replicationTasks": [], "nextPageToken": "" }))
}

/// Single-host DLQ purge: nothing to purge.
pub async fn purge_dlq_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ReadDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    ok_json(cadence_api::admin::PurgeDLQMessagesResponse {})
}

/// Single-host DLQ merge: nothing to merge.
pub async fn merge_dlq_messages(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::MergeDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    ok_json(cadence_api::admin::MergeDLQMessagesResponse {
        next_page_token: vec![],
    })
}

/// Resend history to a remote cluster. Single host has no remotes: unknown
/// clusters are rejected, and the current cluster trivially succeeds.
pub async fn resend_replication_tasks(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ResendReplicationTasksRequest>,
) -> Response {
    if req.domain_id.is_empty() || req.workflow_id.is_empty() || req.run_id.is_empty() {
        return bad_request(BadRequestError::new("Workflow execution is not set on request."));
    }
    if req.remote_cluster.is_empty() {
        return bad_request(BadRequestError::new("Remote cluster is not set on request."));
    }
    if req.remote_cluster != state.current_cluster_name {
        return bad_request(BadRequestError::new(format!(
            "Unknown remote cluster {:?}.",
            req.remote_cluster
        )));
    }
    ok_json(cadence_api::admin::ResendReplicationTasksResponse {})
}

/// Single-host cross-cluster surface: no cross-cluster tasks exist.
pub async fn get_cross_cluster_tasks(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetCrossClusterTasksRequest>,
) -> Response {
    if req.target_cluster.is_empty() {
        return bad_request(BadRequestError::new("Target cluster is not set on request."));
    }
    ok_json(serde_json::json!({ "tasksByShard": {}, "failedCauseByShard": {} }))
}

/// Acknowledge cross-cluster tasks. Single host holds none.
pub async fn respond_cross_cluster_tasks_completed(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RespondCrossClusterTasksCompletedRequest>,
) -> Response {
    if req.target_cluster.is_empty() {
        return bad_request(BadRequestError::new("Target cluster is not set on request."));
    }
    ok_json(cadence_api::admin::RespondCrossClusterTasksCompletedResponse {})
}

fn shard_count(state: &AppState) -> i32 {
    state.num_history_shards.max(1)
}

/// Single-host shard introspection, mirroring Go admin `DescribeHistoryHost`.
/// This host owns every shard; there is no separate shard controller.
pub async fn describe_history_host(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::DescribeHistoryHostRequest>,
) -> Response {
    let _ = req;
    let count = shard_count(&state);
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
    let domains = state.domains.len() as i64;
    ok_json(cadence_api::admin::DescribeHistoryHostResponse {
        number_of_shards: count,
        shard_ids: (0..count).collect(),
        domain_cache: Some(cadence_api::admin::DomainCacheInfo {
            num_of_items_in_cache_by_id: domains,
            num_of_items_in_cache_by_name: domains,
        }),
        shard_controller_status: "running".to_string(),
        address: hostname,
    })
}

/// Single-host shard distribution: every shard maps to this host.
pub async fn describe_shard_distribution(
    State(state): State<StdArc<AppState>>,
) -> Response {
    let count = shard_count(&state);
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
    ok_json(cadence_api::admin::DescribeShardDistributionResponse {
        number_of_shards: count,
        shards: Some((0..count).map(|id| (id, hostname.clone())).collect()),
    })
}

fn check_shard_id(state: &AppState, shard_id: i32) -> Result<(), BadRequestError> {
    let count = shard_count(state);
    if shard_id < 0 || shard_id >= count {
        return Err(BadRequestError::new(format!(
            "Shard ID {shard_id} is out of range for {count} shards."
        )));
    }
    Ok(())
}

/// Close a shard. Single host keeps all shards loaded, so a valid ID is a
/// no-op success like Go's unload-then-lazy-reload.
pub async fn close_shard(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::CloseShardRequest>,
) -> Response {
    if let Err(e) = check_shard_id(&state, req.shard_id) {
        return bad_request(e);
    }
    ok_json(cadence_api::admin::CloseShardResponse {})
}

/// Remove one persistence task. Single host has no addressable persistence
/// task IDs, so a valid request is a no-op success.
pub async fn remove_task(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RemoveTaskRequest>,
) -> Response {
    if let Err(e) = check_shard_id(&state, req.shard_id) {
        return bad_request(e);
    }
    if req.task_type.is_none() {
        return bad_request(BadRequestError::new("TaskType is not set on request."));
    }
    ok_json(cadence_api::admin::RemoveTaskResponse {})
}

/// Reset a persistence queue. Single host has no transfer/timer/replication
/// queues (dispatch state is recomputed from runs), so a valid request is a
/// no-op success.
pub async fn reset_queue(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ResetQueueRequest>,
) -> Response {
    if let Err(e) = check_shard_id(&state, req.shard_id) {
        return bad_request(e);
    }
    if req.task_type.is_none() {
        return bad_request(BadRequestError::new("TaskType is not set on request."));
    }
    ok_json(cadence_api::admin::ResetQueueResponse {})
}

/// Report live dispatch-queue depths as processing-queue states.
pub async fn describe_queue(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::DescribeQueueRequest>,
) -> Response {
    if let Err(e) = check_shard_id(&state, req.shard_id) {
        return bad_request(e);
    }
    if req.task_type.is_none() {
        return bad_request(BadRequestError::new("TaskType is not set on request."));
    }
    let (decisions, activities, timers, backoffs, schedules) =
        state.executions.queue_depths();
    let states = vec![
        format!("{{\"queue\":\"decision\",\"pending\":{decisions}}}"),
        format!("{{\"queue\":\"activity\",\"pending\":{activities}}}"),
        format!("{{\"queue\":\"timer\",\"pending\":{timers}}}"),
        format!("{{\"queue\":\"backoff\",\"pending\":{backoffs}}}"),
        format!("{{\"queue\":\"schedule\",\"registered\":{schedules}}}"),
    ];
    ok_json(cadence_api::admin::DescribeQueueResponse {
        processing_queue_states: states,
    })
}

/// Serve one raw history page with an opaque offset cursor. Single branch:
/// versions are accepted but ignored, mirroring the single-branch store.
pub async fn get_workflow_execution_raw_history(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetWorkflowExecutionRawHistoryV2Request>,
) -> Response {
    use cadence_persistence::raw_history::{self, RawHistoryToken};

    if let Err(e) = raw_history::validate_request(&req) {
        return bad_request(e);
    }
    let execution = req.execution.clone().unwrap_or_default();
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };

    let mut req = req;
    let mut persistence_token = Vec::new();
    let histories: cadence_api::types::VersionHistories = if req.next_page_token.is_empty() {
        match executions.mutable_state_view(&req.domain, &execution) {
            Ok(view) => match view.version_histories {
                Some(vh) => vh,
                None => {
                    return bad_request(BadRequestError::new("Invalid version histories."))
                }
            },
            Err(e) => return entity_not_exists(e),
        }
    } else {
        match RawHistoryToken::from_bytes(&req.next_page_token) {
            Ok(token) => {
                if let Err(e) = token.validate(&req) {
                    return bad_request(e);
                }
                persistence_token = token.persistence_token.clone().unwrap_or_default();
                token.version_histories.clone().unwrap_or_default()
            }
            Err(e) => return bad_request(e),
        }
    };
    if histories.histories.is_empty() {
        return bad_request(BadRequestError::new("Invalid version histories."));
    }

    let target_history = match raw_history::select_history(&mut req, &histories) {
        Ok(h) => h.clone(),
        Err(e) => return bad_request(e),
    };

    // Exclusive-exclusive empty range check.
    if req.start_event_id.unwrap_or(0) + 1 == req.end_event_id.unwrap_or(0) {
        return ok_json(cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
            next_page_token: Vec::new(),
            history_batches: vec![],
            version_history: Some(target_history),
        });
    }

    match executions.raw_history(
        &req.domain,
        &execution,
        req.start_event_id,
        req.end_event_id,
        req.maximum_page_size as usize,
        &persistence_token,
    ) {
        Ok((events, new_persistence_token)) => {
            let batch = serde_json::to_vec(&events).unwrap_or_default();
            let next_page_token = if new_persistence_token.is_empty() {
                Vec::new()
            } else {
                let new_token = RawHistoryToken {
                    domain_name: req.domain.clone(),
                    workflow_id: execution.workflow_id.clone(),
                    run_id: execution.run_id.clone(),
                    start_event_id: req.start_event_id.unwrap_or(0),
                    start_event_version: req.start_event_version.unwrap_or(0),
                    end_event_id: req.end_event_id.unwrap_or(0),
                    end_event_version: req.end_event_version.unwrap_or(0),
                    persistence_token: if new_persistence_token.is_empty() {
                        None
                    } else {
                        Some(new_persistence_token)
                    },
                    version_histories: Some(histories.clone()),
                };
                new_token.to_bytes().unwrap_or_default()
            };
            ok_json(cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
                next_page_token,
                history_batches: vec![cadence_api::types::DataBlob {
                    encoding_type: None,
                    data: batch,
                }],
                version_history: Some(target_history),
            })
        }
        Err(e) => {
            // Go returns empty response on EntityNotExists from DB.
            let _ = e;
            ok_json(cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
                next_page_token: Vec::new(),
                history_batches: vec![],
                version_history: Some(target_history),
            })
        }
    }
}

/// Admin workflow describe: shard placement plus the mutable state as JSON.
pub async fn describe_workflow_execution_admin(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::AdminDescribeWorkflowExecutionRequest>,
) -> Response {
    let execution = req.execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() {
        return bad_request(BadRequestError::new("Execution is not set on request."));
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match executions.describe_admin(&req.domain, &execution) {
        Ok((shard_id, history_addr, state_json)) => {
            ok_json(cadence_api::admin::AdminDescribeWorkflowExecutionResponse {
                shard_id,
                history_addr,
                mutable_state_in_cache: state_json.clone(),
                mutable_state_in_database: state_json,
            })
        }
        Err(e) => entity_not_exists(e),
    }
}

/// Delete corrupt runs, mirroring Go admin `MaintainCorruptWorkflow`.
pub async fn maintain_corrupt_workflow(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::MaintainCorruptWorkflowRequest>,
) -> Response {
    let execution = req.execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() {
        return bad_request(BadRequestError::new("Execution is missing"));
    }
    if let Some(e) = require_domain(&state, &req.domain).await {
        return e;
    }
    let executions = match state.execution_store(&req.domain) {
        Ok(store) => store,
        Err(error) => return entity_not_exists(error),
    };
    match executions.maintain_corrupt_workflow(&req.domain, &execution) {
        Ok((history, executions, visibility)) => {
            ok_json(cadence_api::admin::MaintainCorruptWorkflowResponse {
                history_deleted: history,
                executions_deleted: executions,
                visibility_deleted: visibility,
            })
        }
        Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => {
            error_response(
                "WorkflowExecutionAlreadyCompletedError",
                StatusCode::BAD_REQUEST,
                error.message,
            )
        }
    }
}

/// Read the cluster-global isolation groups.
pub async fn get_global_isolation_groups(
    State(state): State<StdArc<AppState>>,
) -> Response {
    ok_json(cadence_api::admin::GetGlobalIsolationGroupsResponse {
        isolation_groups: state.domains.global_isolation_groups(),
    })
}

/// Replace the cluster-global isolation groups.
pub async fn update_global_isolation_groups(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateGlobalIsolationGroupsRequest>,
) -> Response {
    state
        .domains
        .update_global_isolation_groups(req.isolation_groups);
    ok_json(cadence_api::admin::UpdateGlobalIsolationGroupsResponse {})
}

/// Read a domain's isolation-group overrides.
pub async fn get_domain_isolation_groups(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDomainIsolationGroupsRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(BadRequestError::new("Domain is not set on request."));
    }
    match state.domains.domain_isolation_groups(&req.domain) {
        Ok(groups) => ok_json(cadence_api::admin::GetDomainIsolationGroupsResponse {
            isolation_groups: groups,
        }),
        Err(e) => entity_not_exists(e),
    }
}

/// Replace a domain's isolation-group overrides.
pub async fn update_domain_isolation_groups(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDomainIsolationGroupsRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(BadRequestError::new("Domain is not set on request."));
    }
    match state
        .domains
        .update_domain_isolation_groups(&req.domain, req.isolation_groups)
    {
        Ok(()) => ok_json(cadence_api::admin::UpdateDomainIsolationGroupsResponse {}),
        Err(e) => entity_not_exists(e),
    }
}

/// Read a domain's async-workflow queue configuration.
pub async fn get_domain_async_workflow_configuration(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDomainAsyncWorkflowConfiguratonRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(BadRequestError::new("Domain is not set on request."));
    }
    match state.domains.domain_async_workflow_config(&req.domain) {
        Ok(configuration) => ok_json(
            cadence_api::admin::GetDomainAsyncWorkflowConfiguratonResponse { configuration },
        ),
        Err(e) => entity_not_exists(e),
    }
}

/// Replace a domain's async-workflow queue configuration.
pub async fn update_domain_async_workflow_configuration(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonRequest>,
) -> Response {
    if req.domain.is_empty() {
        return bad_request(BadRequestError::new("Domain is not set on request."));
    }
    match state.domains.update_domain_async_workflow_config(
        &req.domain,
        req.configuration,
    ) {
        Ok(()) => ok_json(cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonResponse {}),
        Err(e) => entity_not_exists(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_get_update_restore_list() {
        let mut overlay = DynamicConfigOverlay::default();
        assert!(overlay.get(false, "k", &[]).is_none());
        let value = DynamicConfigValue {
            value: None,
            filters: vec![],
        };
        overlay.update(false, "k", vec![value.clone()]);
        assert_eq!(overlay.get(false, "k", &[]), Some(value.clone()));
        // Operational namespace is independent.
        assert!(overlay.get(true, "k", &[]).is_none());
        overlay.restore(false, "k", &[]);
        assert!(overlay.get(false, "k", &[]).is_none());
        overlay.update(false, "k", vec![value.clone()]);
        overlay.update(false, "j", vec![value.clone()]);
        assert_eq!(overlay.list(false, "").len(), 2);
        assert_eq!(overlay.list(false, "k").len(), 1);
    }
}
