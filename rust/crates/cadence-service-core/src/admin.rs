//! Admin service state and JSON handlers, mirroring Go's frontend admin
//! handler (`go/service/frontend/admin/handler.go`).
//!
//! Admin service state and the shared ordered snapshot used by runtime
//! dynamic-config property readers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
use cadence_persistence::dynamic_config::{
    PropertyValues, RuntimePropertyProvider, RuntimePropertyRead,
};

fn yaml_value_to_json(value: &serde_yaml::Value) -> anyhow::Result<serde_json::Value> {
    Ok(match value {
        serde_yaml::Value::Null => serde_json::Value::Null,
        serde_yaml::Value::Bool(value) => serde_json::Value::Bool(*value),
        serde_yaml::Value::Number(value) => serde_json::to_value(value)?,
        serde_yaml::Value::String(value) => serde_json::Value::String(value.clone()),
        serde_yaml::Value::Sequence(values) => serde_json::Value::Array(
            values
                .iter()
                .map(yaml_value_to_json)
                .collect::<anyhow::Result<_>>()?,
        ),
        serde_yaml::Value::Mapping(values) => {
            let mut output = serde_json::Map::new();
            for (key, value) in values {
                let key = key.as_str().ok_or_else(|| {
                    anyhow::anyhow!("dynamic config map key {key:?} is not a string")
                })?;
                output.insert(key.to_string(), yaml_value_to_json(value)?);
            }
            serde_json::Value::Object(output)
        }
        serde_yaml::Value::Tagged(value) => yaml_value_to_json(&value.value)?,
    })
}

#[derive(Debug, Clone, Default)]
pub struct DynamicConfigOverlay {
    normal: HashMap<String, Vec<DynamicConfigValue>>,
    operational: HashMap<String, Vec<DynamicConfigValue>>,
    runtime_provider: Option<Arc<dyn RuntimePropertyProvider>>,
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
    /// Build the property snapshot from a config-store snapshot; entry and
    /// value order is the stored order, as Go's `getValueWithFilters` walks it.
    pub fn from_config_store(snapshot: &cadence_persistence::config_store::Snapshot) -> Self {
        let mut overlay = Self::default();
        for entry in snapshot.entries.iter().flatten() {
            overlay
                .normal
                .insert(entry.name.clone(), entry.values.clone());
        }
        overlay
    }

    /// Parse the Go file-based client's YAML format (`key: [{value,
    /// constraints}]`) into the ordered property snapshot consumed by Matching.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())?;
        let root: serde_yaml::Value = serde_yaml::from_str(&text)?;
        // Go's yaml.Unmarshal accepts an empty document or a root `null` as
        // an empty/nil map.
        let Some(mapping) = root.as_mapping() else {
            if root.is_null() {
                return Ok(Self::default());
            }
            anyhow::bail!("dynamic config root must be a mapping");
        };
        let mut overlay = Self::default();
        for (key, entries) in mapping {
            let name = key
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("dynamic config keys must be strings"))?;
            let Some(entries) = entries.as_sequence() else {
                if entries.is_null() {
                    overlay.normal.insert(name.to_string(), Vec::new());
                    continue;
                }
                anyhow::bail!("dynamic config {name:?} must contain a list");
            };
            let mut values = Vec::with_capacity(entries.len());
            for entry in entries {
                let fields = entry.as_mapping().ok_or_else(|| {
                    anyhow::anyhow!("dynamic config entry for {name:?} must be a mapping")
                })?;
                let value_key = serde_yaml::Value::String("value".into());
                // Go's YAML decoder leaves an omitted interface value nil, the
                // same as an explicit YAML null.
                let null_value = serde_yaml::Value::Null;
                let raw_value = fields.get(&value_key).unwrap_or(&null_value);
                let data = serde_json::to_vec(&yaml_value_to_json(raw_value)?)?;
                let mut filters = Vec::new();
                if let Some(constraints) =
                    fields.get(serde_yaml::Value::String("constraints".into()))
                {
                    if constraints.is_null() { /* Go treats YAML null as a nil map. */
                    } else {
                        let constraints = constraints.as_mapping().ok_or_else(|| {
                            anyhow::anyhow!("constraints for {name:?} must be a mapping")
                        })?;
                        for (filter_name, filter_value) in constraints {
                            let filter_name = filter_name.as_str().ok_or_else(|| {
                                anyhow::anyhow!("constraint names must be strings")
                            })?;
                            filters.push(DynamicConfigFilter {
                                name: filter_name.to_string(),
                                value: Some(cadence_api::types::DataBlob {
                                    encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                                    // Go does not recursively convert constraints.
                                    // A nested non-string map key therefore stays
                                    // an unusable constraint without invalidating
                                    // the whole file snapshot.
                                    data: serde_json::to_vec(
                                        &yaml_value_to_json(filter_value)
                                            .unwrap_or(serde_json::Value::Null),
                                    )?,
                                }),
                            });
                        }
                    }
                }
                values.push(DynamicConfigValue {
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data,
                    }),
                    filters,
                });
            }
            overlay.normal.insert(name.to_string(), values);
        }
        Ok(overlay)
    }

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
        self.map(operational)
            .get(name)?
            .iter()
            .find(|v| filters_match(v, filters))
            .cloned()
    }

    /// Ordered normal config entries for runtime property resolution.
    pub fn property_values(&self, name: &str) -> PropertyValues {
        PropertyValues::snapshot(self.normal.get(name).cloned().unwrap_or_default()).with_runtime(
            self.runtime_provider
                .as_ref()
                .map(|provider| RuntimePropertyRead::new(name, provider.clone())),
        )
    }

    /// Enable live typed reads for OpenFeature while retaining ordered
    /// snapshot behavior for file and config-store sources.
    pub fn set_runtime_provider(&mut self, provider: Option<Arc<dyn RuntimePropertyProvider>>) {
        self.runtime_provider = provider;
    }

    /// Replace the value list, mirroring the config-store client's update.
    pub fn update(&mut self, operational: bool, name: &str, values: Vec<DynamicConfigValue>) {
        self.map_mut(operational).insert(name.to_string(), values);
    }

    /// Drop matching values (all values when no filters are given),
    /// mirroring the config-store client's restore.
    pub fn restore(&mut self, operational: bool, name: &str, filters: &[DynamicConfigFilter]) {
        if let Some(values) = self.map_mut(operational).get_mut(name) {
            values.retain(|v| {
                if filters.is_empty() {
                    return !v.filters.is_empty();
                }
                v.filters.is_empty() || !filters_match(v, filters)
            });
        }
    }

    /// List entries, optionally filtered to one config name like Go's
    /// `ListDynamicConfig` with a name filter.
    pub fn list(
        &self,
        operational: bool,
        name: &str,
    ) -> Vec<cadence_api::admin::DynamicConfigEntry> {
        self.map(operational)
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
    pub custom_search_attributes: Arc<Mutex<HashMap<String, cadence_api::enums::IndexedValueType>>>,
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
            go_sdk: "<=1.7.0".to_string(),
            java_sdk: "<=1.5.0".to_string(),
        }),
        membership_info: Some(cadence_api::admin::MembershipInfo {
            current_host: Some(cadence_api::admin::HostInfo {
                identity: hostname.to_string(),
            }),
            // Go appends every returned member in role iteration order, so a
            // single host serving all four roles appears four times here.
            reachable_members: vec![hostname.to_string(); 4],
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
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::BAD_REQUEST,
            error.message,
        ),
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
        match state
            .domains
            .describe(&cadence_api::service_types::DescribeDomainRequest {
                name: None,
                uuid: Some(req.domain_id.clone()),
            }) {
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
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::BAD_REQUEST,
            error.message,
        ),
    }
}

/// Register custom search attributes, mirroring Go admin
/// `AddSearchAttribute`: system keys and type conflicts are rejected, and
/// accepted keys join both the custom registry and the live dynamic-config
/// overlay used by visibility query validation.
pub async fn add_search_attribute(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::AddSearchAttributeRequest>,
) -> Response {
    let overlay = state.admin.dynamic_config.lock().expect("admin config lock");
    let protection_enabled = cadence_persistence::dynamic_config::boolean(
        overlay.property_values("history.enableAdminProtection"),
        None,
        false,
    );
    if protection_enabled {
        let required_token = cadence_persistence::dynamic_config::string(
            overlay.property_values("history.adminOperationToken"),
            "",
        );
        if req.security_token.is_empty() || req.security_token != required_token {
            return bad_request(BadRequestError::new("No permission to do this operation."));
        }
    }
    drop(overlay);
    let attrs = match req.search_attribute {
        Some(attrs) if !attrs.is_empty() => attrs,
        _ => {
            return bad_request(BadRequestError::new("SearchAttributes are not provided"));
        }
    };
    const VALID_ATTRIBUTES: &str = "frontend.validSearchAttributes";
    let mut overlay = state.admin.dynamic_config.lock().expect("admin config lock");
    let mut valid_attributes = match overlay
        .get(false, VALID_ATTRIBUTES, &[])
        .and_then(|value| value.value)
    {
        Some(value) => match serde_json::from_slice::<std::collections::HashMap<String, i32>>(&value.data) {
            Ok(attributes) => attributes,
            Err(error) => {
                return error_response(
                    "InternalServiceError",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to get dynamic config, err: {error}"),
                );
            }
        },
        None => cadence_api::search_attributes::default_indexed_keys()
            .into_iter()
            .map(|(key, value)| (key, value.value()))
            .collect(),
    };
    for (key, value_type) in &attrs {
        if cadence_api::search_attributes::is_system_indexed_key(key) {
            return bad_request(BadRequestError::new(format!(
                "Key [{key}] is reserved by system"
            )));
        }
        if valid_attributes
            .get(key)
            .is_some_and(|existing| *existing != value_type.value())
        {
                return bad_request(BadRequestError::new(format!(
                    "Key [{key}] is already whitelisted as a different type"
                )));
        }
        valid_attributes.insert(key.clone(), value_type.value());
    }
    let encoded = match serde_json::to_vec(&valid_attributes) {
        Ok(encoded) => encoded,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to encode dynamic config, err: {error}"),
            );
        }
    };
    overlay.update(
        false,
        VALID_ATTRIBUTES,
        vec![DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: encoded,
            }),
            filters: Vec::new(),
        }],
    );
    drop(overlay);
    let mut custom = state.admin.custom_search_attributes.lock().expect("admin lock");
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
        Some(entry) => ok_json(cadence_api::admin::GetDynamicConfigResponse { value: entry.value }),
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
        Some(entry) => ok_json(cadence_api::admin::GetDynamicConfigResponse { value: entry.value }),
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
        .filter(|e| e.event_type == Some(cadence_api::enums::EventType::WorkflowExecutionSignaled))
        .collect();
    match executions.reapply_signals(&req.domain, &execution.workflow_id, signals) {
        Ok(()) => ok_json(cadence_api::admin::ReapplyEventsResponse {}),
        Err(cadence_persistence::WorkflowMutationError::NotFound(e)) => entity_not_exists(e),
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::BAD_REQUEST,
            error.message,
        ),
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
        return bad_request(BadRequestError::new(
            "Workflow execution is not set on request.",
        ));
    }
    if req.remote_cluster.is_empty() {
        return bad_request(BadRequestError::new(
            "Remote cluster is not set on request.",
        ));
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
        return bad_request(BadRequestError::new(
            "Target cluster is not set on request.",
        ));
    }
    ok_json(serde_json::json!({ "tasksByShard": {}, "failedCauseByShard": {} }))
}

/// Acknowledge cross-cluster tasks. Single host holds none.
pub async fn respond_cross_cluster_tasks_completed(
    State(_state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RespondCrossClusterTasksCompletedRequest>,
) -> Response {
    if req.target_cluster.is_empty() {
        return bad_request(BadRequestError::new(
            "Target cluster is not set on request.",
        ));
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
pub async fn describe_shard_distribution(State(state): State<StdArc<AppState>>) -> Response {
    let count = shard_count(&state);
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
    ok_json(cadence_api::admin::DescribeShardDistributionResponse {
        number_of_shards: count,
        shards: Some((0..count).map(|id| (id, hostname.clone())).collect()),
    })
}

pub(crate) fn check_shard_id(state: &AppState, shard_id: i32) -> Result<(), BadRequestError> {
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
    let (decisions, activities, timers, backoffs, schedules) = state.executions.queue_depths();
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
    let mut continuation = None;
    let histories: cadence_api::types::VersionHistories = if req.next_page_token.is_empty() {
        match executions.mutable_state_view(&req.domain, &execution) {
            Ok(view) => match view.version_histories {
                Some(vh) => vh,
                None => return bad_request(BadRequestError::new("Invalid version histories.")),
            },
            Err(e) => return entity_not_exists(e),
        }
    } else {
        match RawHistoryToken::from_bytes(&req.next_page_token) {
            Ok(token) => {
                persistence_token = token.persistence_token.clone().unwrap_or_default();
                let histories = token.version_histories.clone().unwrap_or_default();
                continuation = Some(token);
                histories
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
    // Go fills omitted bounds from the cursor's history snapshot before it
    // compares request and cursor. Clients repeat their original unbounded
    // request on subsequent pages.
    if let Some(token) = continuation {
        if let Err(e) = token.validate(&req) {
            return bad_request(e);
        }
    }

    // Exclusive-exclusive empty range check.
    if req.start_event_id.unwrap_or(0) + 1 == req.end_event_id.unwrap_or(0) {
        return ok_json(
            cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
                next_page_token: Vec::new(),
                history_batches: vec![],
                version_history: Some(target_history),
            },
        );
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
            let batch = cadence_thrift::history_blob::encode(events);
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
            ok_json(
                cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
                    next_page_token,
                    history_batches: vec![batch],
                    version_history: Some(target_history),
                },
            )
        }
        Err(e) => {
            // Go returns empty response on EntityNotExists from DB.
            let _ = e;
            ok_json(
                cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response {
                    next_page_token: Vec::new(),
                    history_batches: vec![],
                    version_history: Some(target_history),
                },
            )
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
        Err(cadence_persistence::WorkflowMutationError::AlreadyCompleted(error)) => error_response(
            "WorkflowExecutionAlreadyCompletedError",
            StatusCode::BAD_REQUEST,
            error.message,
        ),
    }
}

/// Read the cluster-global isolation groups.
pub async fn get_global_isolation_groups(State(state): State<StdArc<AppState>>) -> Response {
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
    match state
        .domains
        .update_domain_async_workflow_config(&req.domain, req.configuration)
    {
        Ok(()) => ok_json(cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonResponse {}),
        Err(e) => entity_not_exists(e),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn config_store_snapshot_keeps_entry_value_order() {
        use cadence_persistence::config_store::Snapshot;
        let value = |n: &str| DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(
                cadence_api::enums::EncodingType::JSON,
                n.as_bytes().to_vec(),
            )),
            filters: vec![],
        };
        let snapshot = Snapshot {
            version: 2,
            schema_version: 0,
            entries: Some(vec![cadence_api::admin::DynamicConfigEntry {
                name: "k".into(),
                values: vec![value("1"), value("2")],
            }]),
        };
        let overlay = DynamicConfigOverlay::from_config_store(&snapshot);
        assert_eq!(overlay.property_values("k").snapshot_values().len(), 2);
        assert!(
            DynamicConfigOverlay::from_config_store(&Snapshot::default())
                .property_values("k")
                .snapshot_values()
                .is_empty()
        );
    }

    use super::*;

    #[test]
    fn describe_cluster_matches_go_versions_and_single_host_rings() {
        let response = describe_cluster_response("frontend-1");
        let versions = response.supported_client_versions.unwrap();
        assert_eq!(versions.go_sdk, "<=1.7.0");
        assert_eq!(versions.java_sdk, "<=1.5.0");
        let membership = response.membership_info.unwrap();
        assert_eq!(membership.current_host.unwrap().identity, "frontend-1");
        assert_eq!(membership.rings.len(), 4);
        assert_eq!(
            membership
                .reachable_members
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["frontend-1"; 4]
        );
    }

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

    #[test]
    fn file_based_yaml_preserves_order_and_constraints() {
        let path = std::env::temp_dir().join(format!(
            "cadence-dynamic-config-{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, "test.key:\n  - value: fallback\n  - value: constrained\n    constraints:\n      domainName: sample\n").unwrap();
        let overlay = DynamicConfigOverlay::from_file(&path).unwrap();
        let property_values = overlay.property_values("test.key");
        let values = property_values.snapshot_values();
        assert_eq!(values.len(), 2);
        assert_eq!(
            serde_json::from_slice::<String>(&values[0].value.as_ref().unwrap().data).unwrap(),
            "fallback"
        );
        assert_eq!(
            serde_json::from_slice::<String>(&values[1].value.as_ref().unwrap().data).unwrap(),
            "constrained"
        );
        assert_eq!(values[1].filters[0].name, "domainName");
        assert_eq!(
            serde_json::from_slice::<String>(&values[1].filters[0].value.as_ref().unwrap().data)
                .unwrap(),
            "sample"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_based_yaml_matches_go_null_and_map_key_conversion() {
        let path = std::env::temp_dir().join(format!(
            "cadence-dynamic-config-null-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            include_str!("../tests/fixtures/dynamic-config-go-null.yaml"),
        )
        .unwrap();
        let overlay = DynamicConfigOverlay::from_file(&path).unwrap();
        for key in ["missingValue", "explicitNull"] {
            let property_values = overlay.property_values(key);
            let value = property_values.snapshot_values()[0].value.as_ref().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&value.data).unwrap(),
                serde_json::Value::Null
            );
        }
        assert!(
            overlay.property_values("nullConstraints").snapshot_values()[0]
                .filters
                .is_empty()
        );
        std::fs::write(
            &path,
            include_str!("../tests/fixtures/dynamic-config-go-null-list.yaml"),
        )
        .unwrap();
        let overlay = DynamicConfigOverlay::from_file(&path).unwrap();
        assert!(overlay
            .property_values("emptyValues")
            .snapshot_values()
            .is_empty());
        std::fs::write(
            &path,
            include_str!("../tests/fixtures/dynamic-config-go-nested-constraint-key.yaml"),
        )
        .unwrap();
        assert!(
            DynamicConfigOverlay::from_file(&path).is_ok(),
            "Go does not recursively convert constraint values"
        );
        std::fs::write(
            &path,
            include_str!("../tests/fixtures/dynamic-config-go-nested-integer-key.yaml"),
        )
        .unwrap();
        assert!(DynamicConfigOverlay::from_file(&path)
            .unwrap_err()
            .to_string()
            .contains("not a string"));
        std::fs::write(
            &path,
            include_str!("../tests/fixtures/dynamic-config-go-nested-boolean-key.yaml"),
        )
        .unwrap();
        assert!(DynamicConfigOverlay::from_file(&path)
            .unwrap_err()
            .to_string()
            .contains("not a string"));
        std::fs::remove_file(path).unwrap();
        let empty_path = std::env::temp_dir().join(format!(
            "cadence-dynamic-config-empty-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &empty_path,
            include_str!("../tests/fixtures/dynamic-config-go-empty.yaml"),
        )
        .unwrap();
        assert!(DynamicConfigOverlay::from_file(&empty_path)
            .unwrap()
            .property_values("absent")
            .snapshot_values()
            .is_empty());
        std::fs::remove_file(empty_path).unwrap();
    }

    #[test]
    fn parses_the_go_filebased_client_fixture() {
        let path = std::env::temp_dir().join(format!(
            "cadence-go-dynamic-config-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            include_str!("../../../../go/common/dynamicconfig/config/testConfig.yaml"),
        )
        .unwrap();
        let overlay = DynamicConfigOverlay::from_file(&path).unwrap();
        let property_values = overlay.property_values("testGetBoolPropertyKey");
        let values = property_values.snapshot_values();
        assert_eq!(values.len(), 3);
        assert!(!serde_json::from_slice::<bool>(&values[0].value.as_ref().unwrap().data).unwrap());
        assert_eq!(values[1].filters[0].name, "domainName");
        let property_attributes = overlay.property_values("frontend.validSearchAttributes");
        let attributes = property_attributes.snapshot_values();
        let json: serde_json::Value =
            serde_json::from_slice(&attributes[0].value.as_ref().unwrap().data).unwrap();
        assert_eq!(json["DomainID"], 1);
        std::fs::remove_file(path).unwrap();
    }
}
