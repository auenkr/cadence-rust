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

/// Go `constants.ReadDLQMessagesPageSize`.
const READ_DLQ_MESSAGES_DEFAULT_PAGE_SIZE: usize = 1_000;

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
    store: Option<StoreBinding>,
}

/// Durable config-store binding (Go `configStoreClient`): fetch/write closures
/// plus the cached snapshot version used for the conditional insert.
#[derive(Clone)]
pub struct StoreBinding {
    pub fetch: cadence_persistence::config_store::ConfigRowFetcher,
    pub write: cadence_persistence::config_store::ConfigRowWriter,
    pub update_retry_attempts: i32,
    pub version: i64,
    pub schema_version: i64,
}

impl std::fmt::Debug for StoreBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreBinding")
            .field("version", &self.version)
            .finish()
    }
}

/// Failure of an Admin dynamic-config operation. `NotFound` is Go's
/// `dynamicconfig.NotFoundError` (`EntityNotExistsError`); `Internal` carries
/// Go's uncategorized errors, which `adminHandler.error` wraps as
/// `InternalServiceError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicConfigError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

const NOT_FOUND_MESSAGE: &str = "unable to find key";
const INVALID_FILTERS_MESSAGE: &str = "Request Filters are invalid, unable to parse.";

fn blob_to_json(blob: Option<&cadence_api::types::DataBlob>) -> Result<serde_json::Value, String> {
    let blob =
        blob.ok_or_else(|| "invalid memory address or nil pointer dereference".to_string())?;
    match blob.encoding_type {
        Some(cadence_api::enums::EncodingType::JSON) => {
            serde_json::from_slice(&blob.data).map_err(|e| e.to_string())
        }
        _ => Err("unsupported blob encoding".into()),
    }
}

fn go_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "<nil>",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "float64",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "[]interface {}",
        serde_json::Value::Object(_) => "map[string]interface {}",
    }
}

/// Go `validateKeyDataBlobPair`.
fn validate_key_blob(
    _name: &str,
    kind: cadence_persistence::dynamic_config_keys::KeyKind,
    blob: Option<&cadence_api::types::DataBlob>,
) -> Result<(), String> {
    use cadence_persistence::dynamic_config_keys::KeyKind;
    let value = blob_to_json(blob)?;
    let (key_type, ok) = match kind {
        KeyKind::Int => ("dynamicproperties.IntKey", value.is_number()),
        KeyKind::Bool => ("dynamicproperties.BoolKey", value.is_boolean()),
        KeyKind::Float => ("dynamicproperties.FloatKey", value.is_number()),
        KeyKind::String => ("dynamicproperties.StringKey", value.is_string()),
        KeyKind::Duration => ("dynamicproperties.DurationKey", value.is_string()),
        KeyKind::Map => ("dynamicproperties.MapKey", value.is_object()),
        KeyKind::List => ("dynamicproperties.ListKey", value.is_array()),
    };
    if !ok {
        return Err(format!(
            "key value pair mismatch, key type: {key_type}, value type: {}",
            go_value_type(&value)
        ));
    }
    match kind {
        KeyKind::Int => {
            let f = value.as_f64().unwrap_or(0.0);
            if f != f.trunc() {
                return Err("value type is not int".into());
            }
        }
        KeyKind::Duration
            if crate::config::parse_go_duration(value.as_str().unwrap_or_default()).is_err() =>
        {
            return Err("value string encoding cannot be parsed into duration".into());
        }
        _ => {}
    }
    Ok(())
}

/// Go `matchFilters`: every filter on the stored value must be present in the
/// request with an equal decoded JSON value.
fn go_match_filters(value: &DynamicConfigValue, request: &[(String, serde_json::Value)]) -> bool {
    if value.filters.len() > request.len() {
        return false;
    }
    value.filters.iter().all(|filter| {
        let Some((_, wanted)) = request.iter().find(|(n, _)| *n == filter.name) else {
            return false;
        };
        !wanted.is_null() && blob_to_json(filter.value.as_ref()).is_ok_and(|v| v == *wanted)
    })
}

fn convert_filters(
    filters: &[DynamicConfigFilter],
) -> Result<Vec<(String, serde_json::Value)>, String> {
    filters
        .iter()
        .map(|f| Ok((f.name.clone(), blob_to_json(f.value.as_ref())?)))
        .collect()
}

fn lookup_kind(
    name: &str,
) -> Result<cadence_persistence::dynamic_config_keys::KeyKind, DynamicConfigError> {
    if name.is_empty() {
        // Go `validate.ErrRequestNotSet`.
        return Err(DynamicConfigError::BadRequest("Request is nil.".into()));
    }
    cadence_persistence::dynamic_config_keys::key_kind(name).ok_or_else(|| {
        DynamicConfigError::Internal(format!("invalid dynamic config key name: {name}"))
    })
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

impl DynamicConfigOverlay {
    /// Bind a durable config store (Go `configStoreClient`). The overlay's
    /// normal map becomes the store snapshot.
    pub fn attach_store(
        &mut self,
        binding: StoreBinding,
        snapshot: &cadence_persistence::config_store::Snapshot,
    ) {
        let mut binding = binding;
        binding.version = snapshot.version;
        binding.schema_version = snapshot.schema_version;
        *self = Self::from_config_store(snapshot);
        self.store = Some(binding);
    }

    /// Go `GetValue`/`GetValueWithFilters` as called by admin `GetDynamicConfig`:
    /// the first filter-matching value wins, otherwise the last filterless
    /// value, otherwise `NotFoundError`. Returns the JSON blob.
    pub fn admin_get(
        &self,
        operational: bool,
        name: &str,
        filters: &[DynamicConfigFilter],
    ) -> Result<cadence_api::types::DataBlob, DynamicConfigError> {
        lookup_kind(name)?;
        let request = convert_filters(filters).map_err(DynamicConfigError::Internal)?;
        let not_found = || DynamicConfigError::NotFound(NOT_FOUND_MESSAGE.into());
        let mut fallback = None;
        for value in self.map(operational).get(name).ok_or_else(not_found)? {
            if value.filters.is_empty() {
                if let Ok(parsed) = blob_to_json(value.value.as_ref()) {
                    fallback = Some(parsed);
                }
                continue;
            }
            if go_match_filters(value, &request) {
                let parsed =
                    blob_to_json(value.value.as_ref()).map_err(DynamicConfigError::Internal)?;
                return Ok(json_blob(&parsed));
            }
        }
        fallback.map(|v| json_blob(&v)).ok_or_else(not_found)
    }

    /// Go `UpdateValue`: validate every value against the key type, replace
    /// the entry (empty list deletes it) and persist when a store is bound.
    pub fn admin_update(
        &mut self,
        operational: bool,
        name: &str,
        values: Vec<DynamicConfigValue>,
    ) -> Result<(), DynamicConfigError> {
        let kind = lookup_kind(name)?;
        for value in &values {
            validate_key_blob(name, kind, value.value.as_ref())
                .map_err(DynamicConfigError::Internal)?;
        }
        self.apply_values(operational, name, values)
    }

    /// Go `RestoreValue`.
    pub fn admin_restore(
        &mut self,
        operational: bool,
        name: &str,
        filters: &[DynamicConfigFilter],
    ) -> Result<(), DynamicConfigError> {
        lookup_kind(name)?;
        let request = if filters.is_empty() {
            None
        } else {
            Some(
                convert_filters(filters)
                    .map_err(|_| DynamicConfigError::BadRequest(INVALID_FILTERS_MESSAGE.into()))?,
            )
        };
        let current = self
            .map(operational)
            .get(name)
            .ok_or_else(|| DynamicConfigError::NotFound(NOT_FOUND_MESSAGE.into()))?;
        let kept: Vec<DynamicConfigValue> = current
            .iter()
            .filter(|v| match &request {
                None => !v.filters.is_empty(),
                Some(request) => !go_match_filters(v, request) || v.filters.is_empty(),
            })
            .cloned()
            .collect();
        self.apply_values(operational, name, kept)
    }

    /// Go `ListValue` via `listDynamicConfigValue`: an empty or unknown name
    /// lists everything (sorted by name for determinism; Go iterates a map).
    pub fn admin_list(
        &self,
        operational: bool,
        name: &str,
    ) -> Vec<cadence_api::admin::DynamicConfigEntry> {
        let map = self.map(operational);
        let mut entries: Vec<_> = if !name.is_empty() && map.contains_key(name) {
            self.list(operational, name)
        } else {
            self.list(operational, "")
        };
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        entries
    }

    /// Go `updateValue`: build the next snapshot from the cached one, insert it
    /// at `version + 1`, retry after a condition failure.
    fn apply_values(
        &mut self,
        operational: bool,
        name: &str,
        values: Vec<DynamicConfigValue>,
    ) -> Result<(), DynamicConfigError> {
        let Some(mut binding) = self.store.clone().filter(|_| !operational) else {
            // No durable store (file/nop provider): in-memory only.
            if values.is_empty() {
                self.map_mut(operational).remove(name);
            } else {
                self.update(operational, name, values);
            }
            return Ok(());
        };
        let mut retries = binding.update_retry_attempts;
        loop {
            let mut next = self.normal.clone();
            if values.is_empty() {
                next.remove(name);
            } else {
                next.insert(name.to_string(), values.clone());
            }
            let mut entries: Vec<_> = next
                .iter()
                .map(|(n, v)| cadence_api::admin::DynamicConfigEntry {
                    name: n.clone(),
                    values: v.clone(),
                })
                .collect();
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            let row = cadence_persistence::config_store::ConfigStoreRow {
                version: binding.version + 1,
                data: cadence_persistence::config_store::encode_snapshot(
                    binding.schema_version,
                    &entries,
                ),
                encoding: "thriftrw".into(),
            };
            match (binding.write)(
                cadence_persistence::config_store::CONFIG_TYPE_DYNAMIC_CONFIG,
                &row,
            ) {
                Ok(()) => {
                    binding.version = row.version;
                    self.normal = next;
                    self.store = Some(binding);
                    return Ok(());
                }
                Err(cadence_persistence::config_store::ConfigWriteError::ConditionFailed(_))
                    if retries > 0 =>
                {
                    let fetched = (binding.fetch)(
                        cadence_persistence::config_store::CONFIG_TYPE_DYNAMIC_CONFIG,
                    )
                    .map_err(|e| {
                        DynamicConfigError::Internal(format!(
                            "failed to fetch dynamic config snapshot {e}"
                        ))
                    })?;
                    if let Some(row) = fetched {
                        let snapshot = cadence_persistence::config_store::decode_snapshot(&row)
                            .map_err(|e| {
                                DynamicConfigError::Internal(format!(
                                    "failed to fetch dynamic config snapshot {e}"
                                ))
                            })?;
                        binding.version = snapshot.version;
                        binding.schema_version = snapshot.schema_version;
                        self.normal = Self::from_config_store(&snapshot).normal;
                    }
                    retries -= 1;
                }
                Err(_) if retries == 0 => {
                    return Err(DynamicConfigError::Internal(
                        "ran out of retry attempts on update".into(),
                    ))
                }
                Err(
                    cadence_persistence::config_store::ConfigWriteError::ConditionFailed(m)
                    | cadence_persistence::config_store::ConfigWriteError::Other(m),
                ) => return Err(DynamicConfigError::Internal(m)),
            }
        }
    }
}

fn json_blob(value: &serde_json::Value) -> cadence_api::types::DataBlob {
    cadence_api::types::DataBlob {
        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
        data: serde_json::to_vec(value).unwrap_or_default(),
    }
}

/// Shared admin state: the dynamic-config overlay plus custom (non-system)
/// search attribute keys registered via `AddSearchAttribute`.
#[derive(Debug, Clone, Default)]
pub struct AdminState {
    pub dynamic_config: Arc<Mutex<DynamicConfigOverlay>>,
    pub custom_search_attributes: Arc<Mutex<HashMap<String, cadence_api::enums::IndexedValueType>>>,
    pub membership: MembershipSlot,
    pub persistence_description: PersistenceDescription,
    /// Elasticsearch/OpenSearch mapping client; None when advanced visibility is not configured.
    pub es: Option<crate::es_mapping::EsVisibility>,
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

/// Go `service.ListWithRing`: roles reported by `DescribeCluster`, in order.
pub const RING_ROLES: [&str; 4] = [
    "cadence-frontend",
    "cadence-history",
    "cadence-matching",
    "cadence-worker",
];

/// Live membership view, mirroring the Go `membership.Resolver` methods used
/// by `DescribeCluster` (`WhoAmI` and `Members`). Errors are returned to the
/// caller unchanged, as Go does.
pub trait ClusterMembership: Send + Sync {
    fn who_am_i(&self) -> Result<String, String>;
    fn members(&self, role: &str) -> Result<Vec<String>, String>;

    fn history_host(
        &self,
        address: &str,
    ) -> Result<Option<cadence_common::membership::Host>, String> {
        Ok(self
            .members("cadence-history")?
            .into_iter()
            .find(|member| member == address)
            .map(|identity| cadence_common::membership::Host {
                address: identity.clone(),
                identity,
                grpc_address: String::new(),
            }))
    }

    /// Resolve the owner of a task list name on the live Matching ring (Go client
    /// routes Matching RPCs by task list on the `cadence-matching` hashring).
    fn matching_owner(&self, key: &str) -> Result<Option<cadence_common::membership::Host>, String> {
        let _ = key;
        Ok(None)
    }

    /// Resolve the Go rune shard key on the live History ring.
    fn history_owner(
        &self,
        shard: i32,
    ) -> Result<Option<cadence_common::membership::Host>, String> {
        use cadence_common::membership::{Host, Ring};
        let ring = Ring::new(
            self.members("cadence-history")?
                .into_iter()
                .map(|identity| Host {
                    address: identity.clone(),
                    identity,
                    grpc_address: String::new(),
                }),
        );
        Ok(ring
            .lookup(
                &char::from_u32(shard as u32)
                    .unwrap_or(char::REPLACEMENT_CHARACTER)
                    .to_string(),
            )
            .cloned())
    }
}

/// Late-bound slot for the membership resolver. Go's `GetMembershipResolver`
/// may be nil; in that case `DescribeCluster` returns empty membership info.
#[derive(Clone, Default)]
pub struct MembershipSlot(Arc<std::sync::RwLock<Option<Arc<dyn ClusterMembership>>>>);

impl std::fmt::Debug for MembershipSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MembershipSlot")
    }
}

impl MembershipSlot {
    pub fn set(&self, resolver: Option<Arc<dyn ClusterMembership>>) {
        *self.0.write().expect("membership slot lock") = resolver;
    }
    pub fn get(&self) -> Option<Arc<dyn ClusterMembership>> {
        self.0.read().expect("membership slot lock").clone()
    }
}

/// Persistence facts reported by `DescribeCluster` (Go manager `GetName()` and
/// whether Elasticsearch/OpenSearch advanced visibility is configured).
#[derive(Debug, Clone)]
pub struct PersistenceDescription {
    pub history_backend: String,
    pub visibility_backend: String,
    pub advanced_visibility: bool,
}

impl Default for PersistenceDescription {
    fn default() -> Self {
        Self {
            history_backend: "memory".into(),
            visibility_backend: "memory".into(),
            advanced_visibility: false,
        }
    }
}

/// Mirrors Go admin `DescribeCluster`: ring members for every role come from
/// the membership resolver; resolver errors are returned before any response
/// is built. Persistence keys are `historyStore` / `visibilityStore`.
pub fn describe_cluster_for(
    admin: &AdminState,
) -> Result<cadence_api::admin::DescribeClusterResponse, String> {
    use cadence_api::admin::{HostInfo, MembershipInfo, RingInfo};
    let mut membership_info = MembershipInfo::default();
    if let Some(resolver) = admin.membership.get() {
        membership_info.current_host = Some(HostInfo {
            identity: resolver.who_am_i()?,
        });
        for role in RING_ROLES {
            let identities = resolver.members(role)?;
            let members: Vec<HostInfo> = identities
                .iter()
                .map(|identity| HostInfo {
                    identity: identity.clone(),
                })
                .collect();
            membership_info.reachable_members.extend(identities);
            membership_info.rings.push(RingInfo {
                role: role.to_string(),
                member_count: members.len() as i32,
                members,
            });
        }
    }
    let persistence = admin.persistence_description.clone();
    let mut persistence_info = std::collections::HashMap::new();
    persistence_info.insert(
        "visibilityStore".to_string(),
        cadence_api::admin::PersistenceInfo {
            backend: persistence.visibility_backend,
            features: vec![cadence_api::admin::PersistenceFeature {
                key: "advancedVisibilityEnabled".to_string(),
                enabled: persistence.advanced_visibility,
            }],
        },
    );
    persistence_info.insert(
        "historyStore".to_string(),
        cadence_api::admin::PersistenceInfo {
            backend: persistence.history_backend,
            features: Vec::new(),
        },
    );
    Ok(cadence_api::admin::DescribeClusterResponse {
        supported_client_versions: Some(cadence_api::types::SupportedClientVersions {
            go_sdk: "<=1.7.0".to_string(),
            java_sdk: "<=1.5.0".to_string(),
        }),
        membership_info: Some(membership_info),
        persistence_info: Some(persistence_info),
    })
}

pub async fn describe_cluster(State(state): State<StdArc<AppState>>) -> Response {
    match describe_cluster_for(&state.admin) {
        Ok(response) => ok_json(response),
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error,
        ),
    }
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

/// Failure of the shared `AddSearchAttribute` implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddSearchAttributeError {
    BadRequest(String),
    Internal(String),
}

const VALID_SEARCH_ATTRIBUTES: &str = "frontend.validSearchAttributes";

/// Go `ValidSearchAttributes` dynamic property: the default indexed keys when
/// unset, otherwise the configured `key -> IndexedValueType` JSON map.
fn read_valid_attributes(
    overlay: &DynamicConfigOverlay,
) -> Result<std::collections::HashMap<String, i32>, String> {
    match overlay
        .get(false, VALID_SEARCH_ATTRIBUTES, &[])
        .and_then(|value| value.value)
    {
        Some(value) => serde_json::from_slice(&value.data).map_err(|error| error.to_string()),
        None => Ok(cadence_api::search_attributes::default_indexed_keys()
            .into_iter()
            .map(|(key, value)| (key, value.value()))
            .collect()),
    }
}

/// Keys returned by `GetSearchAttributes`: Go's `ValidSearchAttributes()` with
/// unknown value types skipped.
pub fn valid_search_attributes(
    admin: &AdminState,
) -> std::collections::HashMap<String, cadence_api::enums::IndexedValueType> {
    let mut keys = cadence_api::search_attributes::default_indexed_keys();
    let configured =
        read_valid_attributes(&admin.dynamic_config.lock().expect("admin config lock"));
    if let Ok(configured) = configured {
        keys.extend(configured.into_iter().filter_map(|(key, raw)| {
            cadence_api::enums::IndexedValueType::from_i32(raw).map(|t| (key, t))
        }));
    }
    keys.extend(
        admin
            .custom_search_attributes
            .lock()
            .expect("admin lock")
            .iter()
            .map(|(key, value_type)| (key.clone(), *value_type)),
    );
    keys
}

/// Go admin `AddSearchAttribute`: permission check, empty check, reject system
/// keys and type conflicts, then update the live `ValidSearchAttributes`
/// dynamic config (Go only warns when the config client cannot persist) and the
/// custom registry. The Elasticsearch/OpenSearch mapping update runs only when
/// advanced visibility is configured (`AdminState::es`).
pub fn add_search_attribute_core(
    admin: &AdminState,
    security_token: &str,
    attrs: Option<&std::collections::HashMap<String, cadence_api::enums::IndexedValueType>>,
) -> Result<(), AddSearchAttributeError> {
    use AddSearchAttributeError::{BadRequest, Internal};
    let mut overlay = admin.dynamic_config.lock().expect("admin config lock");
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
        if security_token.is_empty() || security_token != required_token {
            return Err(BadRequest("No permission to do this operation.".into()));
        }
    }
    let attrs = match attrs {
        Some(attrs) if !attrs.is_empty() => attrs,
        _ => return Err(BadRequest("SearchAttributes are not provided".into())),
    };
    let mut valid_attributes = read_valid_attributes(&overlay)
        .map_err(|error| Internal(format!("Failed to get dynamic config, err: {error}")))?;
    for (key, value_type) in attrs {
        if cadence_api::search_attributes::is_system_indexed_key(key) {
            return Err(BadRequest(format!("Key [{key}] is reserved by system")));
        }
        if valid_attributes
            .get(key)
            .is_some_and(|existing| *existing != value_type.value())
        {
            return Err(BadRequest(format!(
                "Key [{key}] is already whitelisted as a different type"
            )));
        }
        valid_attributes.insert(key.clone(), value_type.value());
    }
    let encoded = serde_json::to_vec(&valid_attributes)
        .map_err(|error| Internal(format!("Failed to encode dynamic config, err: {error}")))?;
    // Go `UpdateValue` failures here are only logged ("Failed to update dynamic config").
    if let Err(error) = overlay.admin_update(
        false,
        VALID_SEARCH_ATTRIBUTES,
        vec![DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: encoded,
            }),
            filters: Vec::new(),
        }],
    ) {
        eprintln!("warning: Failed to update dynamic config, err: {error:?}");
    }
    drop(overlay);
    // Go: when advanced visibility is configured, update the ES/OpenSearch mapping (never removed).
    if let Some(es) = &admin.es {
        crate::es_mapping::apply_search_attributes(es, attrs)
            .map_err(|crate::es_mapping::ApplyError::Internal(message)| Internal(message))?;
    }
    admin
        .custom_search_attributes
        .lock()
        .expect("admin lock")
        .extend(attrs.iter().map(|(key, value)| (key.clone(), *value)));
    Ok(())
}

/// Register custom search attributes, mirroring Go admin `AddSearchAttribute`.
pub async fn add_search_attribute(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::AddSearchAttributeRequest>,
) -> Response {
    match add_search_attribute_core(
        &state.admin,
        &req.security_token,
        req.search_attribute.as_ref(),
    ) {
        Ok(()) => ok_json(cadence_api::admin::AddSearchAttributeResponse {}),
        Err(AddSearchAttributeError::BadRequest(message)) => {
            bad_request(BadRequestError::new(message))
        }
        Err(AddSearchAttributeError::Internal(message)) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
        ),
    }
}

fn dynamic_config_error_response(error: DynamicConfigError) -> Response {
    match error {
        DynamicConfigError::BadRequest(message) => bad_request(BadRequestError::new(message)),
        DynamicConfigError::NotFound(message) => {
            entity_not_exists(cadence_api::errors::EntityNotExistsError::new(message))
        }
        DynamicConfigError::Internal(message) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
        ),
    }
}

fn get_config(
    state: &AppState,
    req: cadence_api::admin::GetDynamicConfigRequest,
    op: bool,
) -> Response {
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    match overlay.admin_get(op, &req.config_name, &req.filters) {
        Ok(value) => ok_json(cadence_api::admin::GetDynamicConfigResponse { value: Some(value) }),
        Err(error) => dynamic_config_error_response(error),
    }
}

fn update_config(
    state: &AppState,
    req: cadence_api::admin::UpdateDynamicConfigRequest,
    op: bool,
) -> Response {
    let mut overlay = state.admin.dynamic_config.lock().expect("admin lock");
    match overlay.admin_update(op, &req.config_name, req.config_values) {
        Ok(()) => ok_json(cadence_api::admin::UpdateDynamicConfigResponse {}),
        Err(error) => dynamic_config_error_response(error),
    }
}

fn restore_config(
    state: &AppState,
    req: cadence_api::admin::RestoreDynamicConfigRequest,
    op: bool,
) -> Response {
    let mut overlay = state.admin.dynamic_config.lock().expect("admin lock");
    match overlay.admin_restore(op, &req.config_name, &req.filters) {
        Ok(()) => ok_json(cadence_api::admin::RestoreDynamicConfigResponse {}),
        Err(error) => dynamic_config_error_response(error),
    }
}

fn list_config(
    state: &AppState,
    req: cadence_api::admin::ListDynamicConfigRequest,
    op: bool,
) -> Response {
    let overlay = state.admin.dynamic_config.lock().expect("admin lock");
    ok_json(cadence_api::admin::ListDynamicConfigResponse {
        entries: overlay.admin_list(op, &req.config_name),
    })
}

/// Read a dynamic-config value, mirroring Go admin `GetDynamicConfig`.
pub async fn get_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDynamicConfigRequest>,
) -> Response {
    get_config(&state, req, false)
}

/// Replace a dynamic-config value list, mirroring Go admin `UpdateDynamicConfig`.
pub async fn update_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDynamicConfigRequest>,
) -> Response {
    update_config(&state, req, false)
}

/// Drop dynamic-config values, mirroring Go admin `RestoreDynamicConfig`.
pub async fn restore_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RestoreDynamicConfigRequest>,
) -> Response {
    restore_config(&state, req, false)
}

/// List dynamic-config entries, mirroring Go admin `ListDynamicConfig`.
pub async fn list_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ListDynamicConfigRequest>,
) -> Response {
    list_config(&state, req, false)
}

/// Operational-config variants use the overlay's second namespace.
pub async fn get_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDynamicConfigRequest>,
) -> Response {
    get_config(&state, req, true)
}

/// Replace an operational dynamic-config value list.
pub async fn update_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::UpdateDynamicConfigRequest>,
) -> Response {
    update_config(&state, req, true)
}

/// Drop operational dynamic-config values.
pub async fn restore_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RestoreDynamicConfigRequest>,
) -> Response {
    restore_config(&state, req, true)
}

/// List operational dynamic-config entries.
pub async fn list_operational_dynamic_config(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ListDynamicConfigRequest>,
) -> Response {
    list_config(&state, req, true)
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

fn proxy_error_response(error: crate::admin_replication::ProxyError) -> Response {
    match error {
        crate::admin_replication::ProxyError::BadRequest(message) => {
            bad_request(BadRequestError::new(message))
        }
        crate::admin_replication::ProxyError::Internal(message) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
        ),
        crate::admin_replication::ProxyError::NotFound(message) => {
            error_response("EntityNotExistsError", StatusCode::NOT_FOUND, message)
        }
    }
}

/// Proxies to History like Go's admin handler.
pub async fn get_replication_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetReplicationMessagesRequest>,
) -> Response {
    match crate::admin_replication::get_replication_messages(&state, req).await {
        Ok(response) => ok_json(response),
        Err(error) => proxy_error_response(error),
    }
}

pub async fn get_dlq_replication_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDLQReplicationMessagesRequest>,
) -> Response {
    match crate::admin_replication::get_dlq_replication_messages(&state, req).await {
        Ok(response) => ok_json(response),
        Err(error) => proxy_error_response(error),
    }
}

/// Domain replication requires the domain replication queue, which does not
/// exist on a single host. Mirrors Go's exact error.
pub async fn get_domain_replication_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetDomainReplicationMessagesRequest>,
) -> Response {
    match domain_replication_messages_response(&state, req) {
        Ok(response) => ok_json(response),
        Err(error) if error == "domain replication queue not enabled for cluster" => {
            error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error,
        ),
    }
}

pub fn domain_replication_messages_response(
    state: &AppState,
    req: cadence_api::admin::GetDomainReplicationMessagesRequest,
) -> Result<cadence_api::admin::GetDomainReplicationMessagesResponse, String> {
    use cadence_thrift::{
        protocol::Reader,
        value::{read_struct, FromThrift},
    };

    let Some(persistence) = state.domain_metadata_persistence.as_ref() else {
        return Err("domain replication queue not enabled for cluster".into());
    };
    let mut cursor = req.last_retrieved_message_id.unwrap_or(-1);
    if cursor == -1 {
        if let Ok(ack_levels) = persistence.get_domain_replication_ack_levels() {
            cursor = ack_levels.get(&req.cluster_name).copied().unwrap_or(cursor);
        }
    }
    let rows = persistence.read_domain_replication_messages(cursor, 100)?;
    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded = read_struct(&mut Reader::new(&row.payload))
            .map_err(|error| error.to_string())
            .and_then(|value| {
                cadence_api::types::ReplicationTask::from_thrift(&value)
                    .map_err(|error| error.to_string())
            });
        match decoded {
            Ok(task) => {
                cursor = row.id;
                tasks.push(task);
            }
            Err(error) => return Err(format!("failed to decode domain replication task: {error}")),
        }
    }
    // Go calls UpdateAckLevel on every request. An omitted field uses the
    // queue's -1 sentinel, and update failures are warning-only.
    let last_processed = req.last_processed_message_id.unwrap_or(-1);
    let _ = persistence.update_domain_replication_ack_level(req.cluster_name, last_processed);
    Ok(cadence_api::admin::GetDomainReplicationMessagesResponse {
        messages: Some(cadence_api::admin::ReplicationMessages {
            replication_tasks: tasks,
            last_retrieved_message_id: cursor,
            has_more: false,
            sync_shard_status: None,
        }),
    })
}

pub async fn count_dlq_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::CountDLQMessagesRequest>,
) -> Response {
    match crate::admin_replication::count_dlq_messages(&state, req).await {
        Ok(response) => ok_json(response),
        Err(error) => proxy_error_response(error),
    }
}

pub(crate) fn history_dlq_count_entries(
    entries: impl IntoIterator<Item = (String, i64)>,
) -> Vec<cadence_api::admin::HistoryDLQCountEntry> {
    entries
        .into_iter()
        .map(
            |(source_cluster, message_count)| cadence_api::admin::HistoryDLQCountEntry {
                source_cluster,
                message_count,
            },
        )
        .collect()
}

/// Single-host DLQ reads: callers must name the queue type, like Go.
pub async fn read_dlq_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::ReadDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Invalid) {
        return bad_request(BadRequestError::new("The DLQ type is not supported."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Replication) {
        return match crate::admin_replication::read_dlq_messages(&state, req).await {
            Ok(response) => ok_json(response),
            Err(error) => proxy_error_response(error),
        };
    }
    if req.dlq_type != Some(cadence_api::enums::DLQType::Domain) {
        return crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History Admin client is not configured.".into(),
        );
    }
    let Some(persistence) = state.domain_metadata_persistence.as_ref() else {
        return crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "domain replication DLQ is unavailable for this persistence backend".into(),
        );
    };
    let ack_levels = match persistence.get_domain_replication_dlq_ack_levels() {
        Ok(levels) => levels,
        Err(error) => {
            return crate::handlers::error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let ack_level = ack_levels.get("domainReplication").copied().unwrap_or(-1);
    let first_message_id = match decode_domain_dlq_page_token(&req.next_page_token) {
        Ok(Some(id)) => id,
        Ok(None) => ack_level,
        Err(error) => {
            return crate::handlers::error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let end_message_id = req.inclusive_end_message_id.unwrap_or(i64::MAX - 1);
    let page_size = read_dlq_page_size(req.maximum_page_size);
    match persistence.read_domain_replication_dlq(first_message_id, end_message_id, page_size) {
        Ok(rows) => {
            let mut tasks = Vec::with_capacity(rows.len());
            for row in &rows {
                let task = (|| {
                    let thrift = cadence_thrift::value::read_struct(
                        &mut cadence_thrift::protocol::Reader::new(&row.payload),
                    )
                    .map_err(|error| error.to_string())?;
                    <cadence_api::types::ReplicationTask as cadence_thrift::FromThrift>::from_thrift(&thrift)
                        .map_err(|error| error.to_string())
                })();
                match task {
                    Ok(mut task) => {
                        task.source_task_id = Some(row.id);
                        tasks.push(task);
                    }
                    Err(error) => {
                        return crate::handlers::error_response(
                            "InternalServiceError",
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to decode domain DLQ task: {error}"),
                        )
                    }
                }
            }
            let next_page_token = if rows.len() >= page_size {
                encode_domain_dlq_page_token(rows.last().map_or(first_message_id, |row| row.id))
            } else {
                vec![]
            };
            ok_json(serde_json::json!({
                "replicationTasks": tasks,
                "nextPageToken": next_page_token,
            }))
        }
        Err(error) => crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error,
        ),
    }
}

/// Single-host DLQ purge: nothing to purge.
pub async fn purge_dlq_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::PurgeDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Invalid) {
        return bad_request(BadRequestError::new("The DLQ type is not supported."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Replication) {
        return match crate::admin_replication::purge_dlq_messages(&state, req).await {
            Ok(response) => ok_json(response),
            Err(error) => proxy_error_response(error),
        };
    }
    if req.dlq_type != Some(cadence_api::enums::DLQType::Domain) {
        return crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History Admin client is not configured.".into(),
        );
    }
    let Some(persistence) = state.domain_metadata_persistence.as_ref() else {
        return crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "domain replication DLQ is unavailable for this persistence backend".into(),
        );
    };
    let end_message_id = req.inclusive_end_message_id.unwrap_or(i64::MAX - 1);
    let ack_levels = match persistence.get_domain_replication_dlq_ack_levels() {
        Ok(levels) => levels,
        Err(error) => {
            return crate::handlers::error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let ack_level = ack_levels.get("domainReplication").copied().unwrap_or(-1);
    match persistence.delete_domain_replication_dlq_range(ack_level, end_message_id) {
        Ok(()) => {
            // Go logs, but does not fail the purge, when the acknowledgement
            // metadata update fails after the range delete succeeds.
            let _ = persistence.update_domain_replication_dlq_ack_level(
                "domainReplication".into(),
                end_message_id,
            );
            ok_json(cadence_api::admin::PurgeDLQMessagesResponse {})
        }
        Err(error) => crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error,
        ),
    }
}

pub(crate) fn decode_domain_dlq_page_token(token: &[u8]) -> Result<Option<i64>, String> {
    if token.is_empty() {
        return Ok(None);
    }
    if token.len() != 8 {
        return Err(format!("invalid next page token {token:?}"));
    }
    Ok(Some(i64::from_le_bytes(
        token.try_into().expect("8-byte token"),
    )))
}

pub(crate) fn encode_domain_dlq_page_token(message_id: i64) -> Vec<u8> {
    message_id.to_le_bytes().to_vec()
}

pub(crate) fn read_dlq_page_size(requested: i32) -> usize {
    if requested > 0 {
        requested as usize
    } else {
        READ_DLQ_MESSAGES_DEFAULT_PAGE_SIZE
    }
}

/// Merge domain replication DLQ tasks through the shared Frontend domain
/// manager. A task is deleted and acknowledged only after its mutation commits.
pub async fn merge_dlq_messages(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::MergeDLQMessagesRequest>,
) -> Response {
    if req.dlq_type.is_none() {
        return bad_request(BadRequestError::new("Queue type is not set."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Invalid) {
        return bad_request(BadRequestError::new("The DLQ type is not supported."));
    }
    if req.dlq_type == Some(cadence_api::enums::DLQType::Replication) {
        return match crate::admin_replication::merge_dlq_messages(&state, req).await {
            Ok(response) => ok_json(response),
            Err(error) => proxy_error_response(error),
        };
    }
    if req.dlq_type != Some(cadence_api::enums::DLQType::Domain) {
        return crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History Admin client is not configured.".into(),
        );
    }
    match merge_domain_dlq_messages(&state, req) {
        Ok(response) => ok_json(response),
        Err(error) => crate::handlers::error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error,
        ),
    }
}

pub fn merge_domain_dlq_messages(
    state: &AppState,
    req: cadence_api::admin::MergeDLQMessagesRequest,
) -> Result<cadence_api::admin::MergeDLQMessagesResponse, String> {
    use cadence_thrift::{
        protocol::Reader,
        value::{read_struct, FromThrift},
    };

    let persistence = state.domain_metadata_persistence.as_ref().ok_or_else(|| {
        "domain replication DLQ is unavailable for this persistence backend".to_string()
    })?;
    let ack_levels = persistence.get_domain_replication_dlq_ack_levels()?;
    let ack_level = ack_levels.get("domainReplication").copied().unwrap_or(-1);
    let first_message_id = decode_domain_dlq_page_token(&req.next_page_token)?.unwrap_or(ack_level);
    let end_message_id = req.inclusive_end_message_id.unwrap_or(i64::MAX - 1);
    // Go passes MergeDLQMessages.MaximumPageSize through unchanged. Unlike
    // ReadDLQMessages, it does not apply the 1000-message read default.
    let page_size = req.maximum_page_size.max(0) as usize;
    let rows =
        persistence.read_domain_replication_dlq(first_message_id, end_message_id, page_size)?;
    let mut last_processed = ack_level;
    for row in &rows {
        let thrift =
            read_struct(&mut Reader::new(&row.payload)).map_err(|error| error.to_string())?;
        let task = cadence_api::types::ReplicationTask::from_thrift(&thrift)
            .map_err(|error| error.to_string())?;
        if task.task_type != Some(cadence_api::enums::ReplicationTaskType::Domain) {
            return Err(
                "Encounter non domain replication task in domain replication queue.".into(),
            );
        }
        let attributes = task
            .domain_task_attributes
            .as_ref()
            .ok_or_else(|| "empty domain replication task".to_string())?;
        let info = attributes
            .info
            .as_ref()
            .ok_or_else(|| "invalid domain info attribute".to_string())?;
        state
            .domain_manager
            .apply_replication_task(attributes)
            .map_err(|error| error.to_string())?;
        match attributes.domain_operation {
            Some(cadence_api::enums::DomainOperation::Delete) => {
                state.executions.unregister_domain(&info.name);
            }
            _ => {
                let domain_id = state
                    .domain_manager
                    .domain_id(&info.name)
                    .map_err(|error| error.message)?;
                state.executions.register_domain(&info.name, &domain_id);
            }
        }
        last_processed = row.id;
    }

    if last_processed > ack_level {
        persistence.delete_domain_replication_dlq_range(ack_level, last_processed)?;
        // Go treats acknowledgement metadata as best-effort after delete.
        let _ = persistence
            .update_domain_replication_dlq_ack_level("domainReplication".into(), last_processed);
    }
    let next_page_token = if rows.len() >= page_size {
        encode_domain_dlq_page_token(rows.last().map_or(first_message_id, |row| row.id))
    } else {
        Vec::new()
    };
    Ok(cadence_api::admin::MergeDLQMessagesResponse { next_page_token })
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
    if state.history_admin_channel.is_some() {
        return match crate::admin_replication::resend_replication_tasks(&state, req).await {
            Ok(()) => ok_json(cadence_api::admin::ResendReplicationTasksResponse {}),
            Err(error) => proxy_error_response(error),
        };
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
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::GetCrossClusterTasksRequest>,
) -> Response {
    if req.target_cluster.is_empty() {
        return bad_request(BadRequestError::new(
            "Target cluster is not set on request.",
        ));
    }
    if state.history_admin_channel.is_none() {
        return ok_json(serde_json::json!({ "tasksByShard": {}, "failedCauseByShard": {} }));
    }
    match crate::admin_replication::get_cross_cluster_tasks(&state, req).await {
        Ok(value) => ok_json(value),
        Err(error) => proxy_error_response(error),
    }
}

/// Acknowledge cross-cluster tasks. Single host holds none.
pub async fn respond_cross_cluster_tasks_completed(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::RespondCrossClusterTasksCompletedRequest>,
) -> Response {
    if req.target_cluster.is_empty() {
        return bad_request(BadRequestError::new(
            "Target cluster is not set on request.",
        ));
    }
    if state.history_admin_channel.is_none() {
        return ok_json(cadence_api::admin::RespondCrossClusterTasksCompletedResponse {});
    }
    match crate::admin_replication::respond_cross_cluster_tasks_completed(&state, req).await {
        Ok(response) => ok_json(response),
        Err(error) => proxy_error_response(error),
    }
}

fn shard_count(state: &AppState) -> i32 {
    state.num_history_shards.max(1)
}

/// Go admin `DescribeHistoryHost`: validate the selector, then ask History
/// (which answers for the host it serves). Without a configured History
/// channel this host answers for itself, owning every shard.
pub async fn describe_history_host(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::DescribeHistoryHostRequest>,
) -> Response {
    if req.shard_id_for_host.is_none()
        && req.execution_for_host.is_none()
        && req.host_address.is_none()
    {
        return bad_request(BadRequestError::new("Request is nil."));
    }
    if let Some(execution) = &req.execution_for_host {
        if execution.workflow_id.is_empty() {
            return bad_request(BadRequestError::new("WorkflowId is not set on request."));
        }
        if !execution.run_id.is_empty() && uuid::Uuid::parse_str(&execution.run_id).is_err() {
            return bad_request(BadRequestError::new("RunId is not valid UUID."));
        }
    }
    let shard = req.shard_id_for_host.unwrap_or_else(|| {
        let workflow_id = req
            .execution_for_host
            .as_ref()
            .map(|execution| execution.workflow_id.as_str())
            .unwrap_or_default();
        (cadence_common::membership::fingerprint32(workflow_id.as_bytes())
            % shard_count(&state).max(1) as u32) as i32
    });
    let channel = match history_channel_for(
        &state,
        shard,
        req.host_address
            .as_deref()
            .filter(|_| req.shard_id_for_host.is_none() && req.execution_for_host.is_none()),
    ) {
        Ok(channel) => channel,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    if let Some(channel) = channel {
        let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
        return match client
            .describe_history_host(history_admin_request(
                cadence_proto::history::DescribeHistoryHostRequest {},
            ))
            .await
        {
            Ok(response) => {
                let v = response.into_inner();
                ok_json(cadence_api::admin::DescribeHistoryHostResponse {
                    number_of_shards: v.number_of_shards,
                    shard_ids: v.shard_ids,
                    domain_cache: v.domain_cache.map(|d| cadence_api::admin::DomainCacheInfo {
                        num_of_items_in_cache_by_id: d.num_of_items_in_cache_by_id,
                        num_of_items_in_cache_by_name: d.num_of_items_in_cache_by_name,
                    }),
                    shard_controller_status: v.shard_controller_status,
                    address: v.address,
                })
            }
            Err(error) => error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error.message().to_string(),
            ),
        };
    }
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

/// Go paginates shard IDs, reporting unknown when membership cannot resolve an owner.
pub fn shard_distribution_for(
    admin: &AdminState,
    count: i32,
    request: &cadence_api::admin::DescribeShardDistributionRequest,
) -> cadence_api::admin::DescribeShardDistributionResponse {
    let size = i64::from(request.page_size.unwrap_or_default());
    let offset = i64::from(request.page_id.unwrap_or_default()) * size;
    let end = (offset + size).min(i64::from(count));
    let resolver = admin.membership.get();
    let shards = (offset..end)
        .map(|id| {
            let owner = i32::try_from(id)
                .ok()
                .and_then(|id| resolver.as_ref()?.history_owner(id).ok().flatten());
            (
                id as i32,
                owner
                    .map(|host| host.identity().to_owned())
                    .unwrap_or_else(|| "unknown".into()),
            )
        })
        .collect();
    cadence_api::admin::DescribeShardDistributionResponse {
        number_of_shards: count,
        shards: Some(shards),
    }
}

pub async fn describe_shard_distribution(
    State(state): State<StdArc<AppState>>,
    axum::extract::Query(request): axum::extract::Query<
        cadence_api::admin::DescribeShardDistributionRequest,
    >,
) -> Response {
    ok_json(shard_distribution_for(
        &state.admin,
        shard_count(&state),
        &request,
    ))
}

/// Resolve an Admin request without losing the configured TLS policy. Only an
/// empty/uninstalled ring uses the static channel; resolver failures propagate.
fn history_channel_for(
    state: &AppState,
    shard: i32,
    explicit: Option<&str>,
) -> Result<Option<tonic::transport::Channel>, String> {
    let peer = match explicit {
        Some(address) => match state.admin.membership.get() {
            Some(resolver) => match resolver.history_host(address)? {
                Some(host) => Some(host.grpc_address),
                None if resolver.members("cadence-history")?.is_empty() => None,
                None => return Err("History host is not a member".into()),
            },
            None => None,
        },
        None => state
            .admin
            .membership
            .get()
            .map(|resolver| resolver.history_owner(shard))
            .transpose()?
            .flatten()
            .map(|host| host.grpc_address),
    };
    match peer {
        None => Ok(state.history_admin_channel.clone()),
        Some(peer) if peer.is_empty() => Err("History member has no gRPC port".into()),
        Some(peer) => crate::grpc_tls::endpoint_channel(&peer, state.history_admin_tls.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
    }
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

/// Close a shard through the History service, matching Go Admin's delegation.
pub async fn close_shard(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::CloseShardRequest>,
) -> Response {
    if let Err(e) = check_shard_id(&state, req.shard_id) {
        return bad_request(e);
    }
    let channel = match history_channel_for(&state, req.shard_id, None) {
        Ok(channel) => channel,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let Some(channel) = channel else {
        return error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History client is not configured.".to_string(),
        );
    };
    let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
    match client
        .close_shard(history_admin_request(
            cadence_proto::history::CloseShardRequest {
                shard_id: req.shard_id,
            },
        ))
        .await
    {
        Ok(_) => ok_json(cadence_api::admin::CloseShardResponse {}),
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error.message().to_string(),
        ),
    }
}

/// Remove one persistence task through the History service, matching Go
/// Admin's delegation.
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
    let channel = match history_channel_for(&state, req.shard_id, None) {
        Ok(channel) => channel,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let Some(channel) = channel else {
        return error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History client is not configured.".to_string(),
        );
    };
    let visibility_time = req
        .visibility_timestamp
        .map(|nanos| prost_types::Timestamp {
            seconds: nanos.div_euclid(1_000_000_000),
            nanos: nanos.rem_euclid(1_000_000_000) as i32,
        });
    let request = cadence_proto::history::RemoveTaskRequest {
        shard_id: req.shard_id,
        task_type: req.task_type.unwrap_or_default(),
        task_id: req.task_id,
        visibility_time,
        cluster_name: req.cluster_name,
    };
    let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
    match client.remove_task(history_admin_request(request)).await {
        Ok(_) => ok_json(cadence_api::admin::RemoveTaskResponse {}),
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error.message().to_string(),
        ),
    }
}

/// Reset a persistence queue through the configured History service, matching
/// Go Admin's HistoryClient delegation.
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
    if req.cluster_name.is_empty() {
        return bad_request(BadRequestError::new("Cluster name is not set."));
    }
    let channel = match history_channel_for(&state, req.shard_id, None) {
        Ok(channel) => channel,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let Some(channel) = channel else {
        return error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History client is not configured.".to_string(),
        );
    };
    let request = cadence_proto::history::ResetQueueRequest {
        shard_id: req.shard_id,
        cluster_name: req.cluster_name,
        task_type: req.task_type.unwrap_or_default(),
    };
    let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
    let response = match client.reset_queue(history_admin_request(request)).await {
        Ok(response) => response.into_inner(),
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error.message().to_string(),
            );
        }
    };
    let _ = response;
    ok_json(cadence_api::admin::ResetQueueResponse {})
}

/// Describe a History persistence queue through the configured History service.
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
    if req.cluster_name.is_empty() {
        return bad_request(BadRequestError::new("Cluster name is not set."));
    }
    let channel = match history_channel_for(&state, req.shard_id, None) {
        Ok(channel) => channel,
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let Some(channel) = channel else {
        return error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            "History client is not configured.".to_string(),
        );
    };
    let request = cadence_proto::history::DescribeQueueRequest {
        shard_id: req.shard_id,
        cluster_name: req.cluster_name,
        task_type: req.task_type.unwrap_or_default(),
    };
    let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
    match client.describe_queue(history_admin_request(request)).await {
        Ok(response) => ok_json(cadence_api::admin::DescribeQueueResponse {
            processing_queue_states: response.into_inner().processing_queue_states,
        }),
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error.message().to_string(),
        ),
    }
}

pub fn history_admin_request<T>(payload: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(payload);
    request.metadata_mut().insert(
        "rpc-service",
        tonic::metadata::MetadataValue::from_static("cadence-history"),
    );
    request.metadata_mut().insert(
        "rpc-caller",
        tonic::metadata::MetadataValue::from_static("cadence-frontend"),
    );
    request.metadata_mut().insert(
        "rpc-encoding",
        tonic::metadata::MetadataValue::from_static("proto"),
    );
    request.set_timeout(std::time::Duration::from_secs(30));
    request
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
    // Go's uuid.Parse accepts compact, braced and URN spellings, then the
    // persistence layer addresses the run by the canonical UUID bytes.
    let mut persistence_execution = execution.clone();
    persistence_execution.run_id = raw_history::canonical_run_id(&persistence_execution.run_id);
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
        match executions.mutable_state_view(&req.domain, &persistence_execution) {
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
                if let Err(error) = raw_history::validate_persistence_token(&persistence_token) {
                    return bad_request(error);
                }
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
        &persistence_execution,
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

/// Admin workflow describe: shard placement plus History's mutable-state
/// diagnostic. This is Admin's DescribeMutableState delegation, not the
/// WorkflowService DescribeWorkflowExecution operation.
pub async fn describe_workflow_execution_admin(
    State(state): State<StdArc<AppState>>,
    Json(req): Json<cadence_api::admin::AdminDescribeWorkflowExecutionRequest>,
) -> Response {
    let execution = req.execution.clone().unwrap_or_default();
    if execution.workflow_id.is_empty() {
        return bad_request(BadRequestError::new("Execution is not set on request."));
    }
    // An unconfigured History transport is retained for embedded/local
    // service-core use. Production Frontend always installs this channel.
    if state.history_admin_channel.is_none() && state.admin.membership.get().is_none() {
        if let Some(e) = require_domain(&state, &req.domain).await {
            return e;
        }
        let executions = match state.execution_store(&req.domain) {
            Ok(store) => store,
            Err(error) => return entity_not_exists(error),
        };
        return match executions.describe_admin(&req.domain, &execution) {
            Ok((shard_id, history_addr, state_json)) => {
                ok_json(cadence_api::admin::AdminDescribeWorkflowExecutionResponse {
                    shard_id,
                    history_addr,
                    mutable_state_in_cache: state_json.clone(),
                    mutable_state_in_database: state_json,
                })
            }
            Err(e) => entity_not_exists(e),
        };
    }

    let shard_id = (cadence_common::membership::fingerprint32(execution.workflow_id.as_bytes())
        % shard_count(&state).max(1) as u32) as i32;
    let domain_id = match state.domain_manager.domain_id(&req.domain) {
        Ok(id) => id,
        Err(error) => return entity_not_exists(error),
    };
    let owner = match state
        .admin
        .membership
        .get()
        .map(|resolver| resolver.history_owner(shard_id))
        .transpose()
    {
        Ok(owner) => owner.flatten(),
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let (history_addr, channel) = match history_channel_for(&state, shard_id, None) {
        Ok(Some(channel)) => (
            owner
                .as_ref()
                .map(|host| host.address.clone())
                .unwrap_or_default(),
            channel,
        ),
        Ok(None) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                "History client is not configured.".to_string(),
            )
        }
        Err(error) => {
            return error_response(
                "InternalServiceError",
                StatusCode::INTERNAL_SERVER_ERROR,
                error,
            )
        }
    };
    let request = cadence_proto::history::DescribeMutableStateRequest {
        domain_id,
        workflow_execution: Some(cadence_proto::api::WorkflowExecution {
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
        }),
    };
    let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
    match client
        .describe_mutable_state(history_admin_request(request))
        .await
    {
        Ok(response) => {
            let response = response.into_inner();
            ok_json(cadence_api::admin::AdminDescribeWorkflowExecutionResponse {
                shard_id,
                history_addr,
                mutable_state_in_cache: response.mutable_state_in_cache,
                mutable_state_in_database: response.mutable_state_in_database,
            })
        }
        Err(error) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            error.message().to_string(),
        ),
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
    match state.domain_manager.async_workflow_config(&req.domain) {
        Ok(configuration) => ok_json(
            cadence_api::admin::GetDomainAsyncWorkflowConfiguratonResponse { configuration },
        ),
        Err(cadence_persistence::FrontendDomainError::NotFound(e)) => entity_not_exists(e),
        Err(e) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ),
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
        .domain_manager
        .update_async_workflow_config(&req.domain, req.configuration)
    {
        Ok(()) => ok_json(cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonResponse {}),
        Err(cadence_persistence::FrontendDomainError::NotFound(e)) => entity_not_exists(e),
        Err(e) => error_response(
            "InternalServiceError",
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn history_membership_uses_go_rune_keys_including_replacement_for_invalid_runes() {
        struct HistoryMembers;
        impl ClusterMembership for HistoryMembers {
            fn who_am_i(&self) -> Result<String, String> {
                Ok("frontend".into())
            }
            fn members(&self, role: &str) -> Result<Vec<String>, String> {
                assert_eq!(role, "cadence-history");
                Ok(vec![
                    "history-a:7934".into(),
                    "history-b:7934".into(),
                    "history-c:7934".into(),
                ])
            }
        }
        use cadence_common::membership::{Host, Ring};
        let ring = Ring::new(
            HistoryMembers
                .members("cadence-history")
                .unwrap()
                .into_iter()
                .map(|address| Host {
                    identity: address.clone(),
                    address,
                    grpc_address: String::new(),
                }),
        );
        for (shard, key) in [
            (0, "\0"),
            (1, "\u{1}"),
            (127, "\u{7f}"),
            (128, "\u{80}"),
            (870, "\u{366}"),
            (0xd800, "�"),
            (-1, "�"),
        ] {
            assert_eq!(
                HistoryMembers.history_owner(shard).unwrap().as_ref(),
                ring.lookup(key)
            );
        }
        assert!((0..1024).any(
            |shard| HistoryMembers.history_owner(shard).unwrap().as_ref()
                != ring.lookup(&shard.to_string())
        ));
    }

    #[test]
    fn shard_distribution_paginates_and_reports_unknown_membership() {
        let admin = AdminState::default();
        let request = cadence_api::admin::DescribeShardDistributionRequest {
            page_id: Some(1),
            page_size: Some(3),
        };
        let response = shard_distribution_for(&admin, 8, &request);
        assert_eq!(response.number_of_shards, 8);
        let shards = response.shards.unwrap();
        assert_eq!(shards.len(), 3);
        for id in 3..6 {
            assert_eq!(shards[&id], "unknown");
        }
        admin
            .membership
            .set(Some(Arc::new(FakeMembership { fail_role: None })));
        let response = shard_distribution_for(&admin, 5, &request);
        let shards = response.shards.unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[&3], "h-1:7934");
        assert_eq!(shards[&4], "h-1:7934");
        assert!(shard_distribution_for(&admin, 5, &Default::default())
            .shards
            .unwrap()
            .is_empty());
    }

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
    fn domain_dlq_page_tokens_use_go_little_endian_message_ids() {
        let token = encode_domain_dlq_page_token(0x0102_0304_0506_0708);
        assert_eq!(token, vec![8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(
            decode_domain_dlq_page_token(&token),
            Ok(Some(0x0102_0304_0506_0708))
        );
        assert_eq!(decode_domain_dlq_page_token(&[]), Ok(None));
        assert!(decode_domain_dlq_page_token(&[1]).is_err());
    }

    #[test]
    fn read_dlq_uses_go_default_page_size() {
        assert_eq!(read_dlq_page_size(0), 1_000);
        assert_eq!(read_dlq_page_size(-1), 1_000);
        assert_eq!(read_dlq_page_size(12), 12);
    }

    #[test]
    fn history_dlq_counts_preserve_source_cluster_and_count() {
        assert_eq!(
            history_dlq_count_entries(
                [("cluster-a".to_string(), 4), ("cluster-b".to_string(), 9),]
            ),
            vec![
                cadence_api::admin::HistoryDLQCountEntry {
                    source_cluster: "cluster-a".into(),
                    message_count: 4,
                },
                cadence_api::admin::HistoryDLQCountEntry {
                    source_cluster: "cluster-b".into(),
                    message_count: 9,
                },
            ]
        );
    }

    struct FakeMembership {
        fail_role: Option<&'static str>,
    }
    impl ClusterMembership for FakeMembership {
        fn who_am_i(&self) -> Result<String, String> {
            Ok("fe-1:7933".into())
        }
        fn members(&self, role: &str) -> Result<Vec<String>, String> {
            if self.fail_role == Some(role) {
                return Err("ring unavailable".into());
            }
            Ok(match role {
                "cadence-frontend" => vec!["fe-1:7933".into(), "fe-2:7933".into()],
                "cadence-history" => vec!["h-1:7934".into()],
                "cadence-matching" => vec![],
                _ => vec!["w-1:7939".into()],
            })
        }
    }

    #[test]
    fn describe_cluster_uses_resolver_members_per_role_like_go() {
        let admin = AdminState::default();
        admin
            .membership
            .set(Some(Arc::new(FakeMembership { fail_role: None })));
        let response = describe_cluster_for(&admin).unwrap();
        let versions = response.supported_client_versions.unwrap();
        assert_eq!(versions.go_sdk, "<=1.7.0");
        assert_eq!(versions.java_sdk, "<=1.5.0");
        let membership = response.membership_info.unwrap();
        assert_eq!(membership.current_host.unwrap().identity, "fe-1:7933");
        let roles: Vec<_> = membership.rings.iter().map(|r| r.role.as_str()).collect();
        assert_eq!(
            roles,
            [
                "cadence-frontend",
                "cadence-history",
                "cadence-matching",
                "cadence-worker"
            ]
        );
        assert_eq!(membership.rings[0].member_count, 2);
        assert_eq!(membership.rings[2].member_count, 0);
        assert_eq!(
            membership.reachable_members,
            vec!["fe-1:7933", "fe-2:7933", "h-1:7934", "w-1:7939"]
        );
        let info = response.persistence_info.unwrap();
        assert!(info.contains_key("historyStore"));
        assert!(!info["visibilityStore"].features[0].enabled);
    }

    #[test]
    fn describe_cluster_without_resolver_has_empty_membership_and_errors_propagate() {
        let admin = AdminState::default();
        let membership = describe_cluster_for(&admin)
            .unwrap()
            .membership_info
            .unwrap();
        assert!(membership.current_host.is_none() && membership.rings.is_empty());
        admin.membership.set(Some(Arc::new(FakeMembership {
            fail_role: Some("cadence-history"),
        })));
        assert_eq!(
            describe_cluster_for(&admin).unwrap_err(),
            "ring unavailable"
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

#[cfg(test)]
mod dynamic_config_admin_tests {
    use super::*;
    use cadence_persistence::config_store as cs;
    use std::sync::Mutex as StdMutex;

    fn json(s: &str) -> cadence_api::types::DataBlob {
        cadence_api::types::DataBlob {
            encoding_type: Some(cadence_api::enums::EncodingType::JSON),
            data: s.as_bytes().to_vec(),
        }
    }

    fn value(s: &str, filters: &[(&str, &str)]) -> DynamicConfigValue {
        DynamicConfigValue {
            value: Some(json(s)),
            filters: filters
                .iter()
                .map(|(n, v)| DynamicConfigFilter {
                    name: (*n).into(),
                    value: Some(json(v)),
                })
                .collect(),
        }
    }

    fn filter(n: &str, v: &str) -> DynamicConfigFilter {
        DynamicConfigFilter {
            name: n.into(),
            value: Some(json(v)),
        }
    }

    const INT: &str = "frontend.visibilityListMaxQPS";
    const BOOL: &str = "worker.enableScheduler";

    #[test]
    fn name_and_key_validation() {
        let mut o = DynamicConfigOverlay::default();
        assert_eq!(
            o.admin_get(false, "", &[]),
            Err(DynamicConfigError::BadRequest("Request is nil.".into()))
        );
        assert_eq!(
            o.admin_update(false, "", vec![]),
            Err(DynamicConfigError::BadRequest("Request is nil.".into()))
        );
        assert_eq!(
            o.admin_restore(false, "", &[]),
            Err(DynamicConfigError::BadRequest("Request is nil.".into()))
        );
        assert_eq!(
            o.admin_get(false, "no.such.key", &[]),
            Err(DynamicConfigError::Internal(
                "invalid dynamic config key name: no.such.key".into()
            ))
        );
        assert!(matches!(
            o.admin_update(false, "no.such.key", vec![value("1", &[])]),
            Err(DynamicConfigError::Internal(_))
        ));
    }

    #[test]
    fn update_validates_value_types_like_go() {
        let mut o = DynamicConfigOverlay::default();
        let err = |r: Result<(), DynamicConfigError>| match r {
            Err(DynamicConfigError::Internal(m)) => m,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            err(o.admin_update(false, BOOL, vec![value("1", &[])])),
            "key value pair mismatch, key type: dynamicproperties.BoolKey, value type: float64"
        );
        assert_eq!(
            err(o.admin_update(false, INT, vec![value("1.5", &[])])),
            "value type is not int"
        );
        assert_eq!(
            err(o.admin_update(false, INT, vec![value("\"x\"", &[])])),
            "key value pair mismatch, key type: dynamicproperties.IntKey, value type: string"
        );
        assert_eq!(
            err(o.admin_update(
                false,
                "frontend.validSearchAttributes",
                vec![value("[1]", &[])]
            )),
            "key value pair mismatch, key type: dynamicproperties.MapKey, value type: []interface {}"
        );
        assert_eq!(
            err(o.admin_update(
                false,
                "matching.longPollExpirationInterval",
                vec![value("\"zz\"", &[])]
            )),
            "value string encoding cannot be parsed into duration"
        );
        o.admin_update(
            false,
            "matching.longPollExpirationInterval",
            vec![value("\"1m\"", &[])],
        )
        .unwrap();
        o.admin_update(false, INT, vec![value("5", &[])]).unwrap();
        let bad_encoding = DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
                data: vec![],
            }),
            filters: vec![],
        };
        assert_eq!(
            err(o.admin_update(false, INT, vec![bad_encoding])),
            "unsupported blob encoding"
        );
        // A rejected update leaves the previous value in place.
        assert_eq!(o.admin_get(false, INT, &[]).unwrap().data, b"5");
    }

    #[test]
    fn get_prefers_matching_filter_then_last_filterless_then_not_found() {
        let mut o = DynamicConfigOverlay::default();
        let not_found = Err(DynamicConfigError::NotFound("unable to find key".into()));
        assert_eq!(o.admin_get(false, INT, &[]), not_found);
        o.admin_update(
            false,
            INT,
            vec![
                value("1", &[]),
                value("7", &[("domainName", "\"d\"")]),
                value("2", &[]),
            ],
        )
        .unwrap();
        // nil filters: only filterless values; the last one wins.
        assert_eq!(o.admin_get(false, INT, &[]).unwrap().data, b"2");
        assert_eq!(
            o.admin_get(false, INT, &[filter("domainName", "\"d\"")])
                .unwrap()
                .data,
            b"7"
        );
        assert_eq!(
            o.admin_get(false, INT, &[filter("domainName", "\"other\"")])
                .unwrap()
                .data,
            b"2"
        );
        // A stored value with only filters and no fallback is NotFound.
        o.admin_update(false, BOOL, vec![value("true", &[("domainName", "\"d\"")])])
            .unwrap();
        assert_eq!(o.admin_get(false, BOOL, &[]), not_found);
        assert_eq!(
            o.admin_get(false, BOOL, &[filter("domainName", "\"d\"")])
                .unwrap()
                .data,
            b"true"
        );
        // Operational namespace is separate.
        assert_eq!(o.admin_get(true, INT, &[]), not_found);
    }

    #[test]
    fn restore_semantics_and_not_found() {
        let mut o = DynamicConfigOverlay::default();
        assert_eq!(
            o.admin_restore(false, INT, &[]),
            Err(DynamicConfigError::NotFound("unable to find key".into()))
        );
        o.admin_update(
            false,
            INT,
            vec![
                value("1", &[]),
                value("7", &[("domainName", "\"a\"")]),
                value("8", &[("domainName", "\"b\"")]),
            ],
        )
        .unwrap();
        // Filtered restore removes only the matching filtered value.
        o.admin_restore(false, INT, &[filter("domainName", "\"a\"")])
            .unwrap();
        assert_eq!(o.admin_list(false, INT)[0].values.len(), 2);
        // Unfiltered restore removes the fallback value only.
        o.admin_restore(false, INT, &[]).unwrap();
        let values = &o.admin_list(false, INT)[0].values;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].filters[0].name, "domainName");
        // Restoring the last filtered value deletes the entry.
        o.admin_restore(false, INT, &[filter("domainName", "\"b\"")])
            .unwrap();
        assert!(o.admin_list(false, INT).is_empty());
        // Unparseable filter value -> ErrInvalidFilters.
        o.admin_update(false, INT, vec![value("1", &[])]).unwrap();
        let bad = DynamicConfigFilter {
            name: "domainName".into(),
            value: Some(json("{")),
        };
        assert_eq!(
            o.admin_restore(false, INT, &[bad]),
            Err(DynamicConfigError::BadRequest(
                "Request Filters are invalid, unable to parse.".into()
            ))
        );
    }

    #[test]
    fn update_with_empty_values_deletes_and_list_falls_back_to_all() {
        let mut o = DynamicConfigOverlay::default();
        o.admin_update(false, INT, vec![value("1", &[])]).unwrap();
        o.admin_update(false, BOOL, vec![value("true", &[])])
            .unwrap();
        assert_eq!(o.admin_list(false, INT).len(), 1);
        assert_eq!(o.admin_list(false, "").len(), 2);
        // Unknown name (or a key without an entry) lists everything, like Go.
        assert_eq!(o.admin_list(false, "no.such.key").len(), 2);
        assert_eq!(
            o.admin_list(false, "matching.longPollExpirationInterval")
                .len(),
            2
        );
        assert_eq!(o.admin_list(false, "")[0].name, INT); // sorted by name
        o.admin_update(false, INT, vec![]).unwrap();
        assert_eq!(o.admin_list(false, "").len(), 1);
    }

    struct Db(StdMutex<rusqlite::Connection>);

    fn store_with(db: &Arc<Db>, retries: i32) -> StoreBinding {
        let (f, w) = (db.clone(), db.clone());
        StoreBinding {
            fetch: Arc::new(move |t| cs::sqlite_fetch_latest(&f.0.lock().unwrap(), t)),
            write: Arc::new(move |t, row| cs::sqlite_insert_config(&w.0.lock().unwrap(), t, row)),
            update_retry_attempts: retries,
            version: 0,
            schema_version: 0,
        }
    }

    fn new_db() -> Arc<Db> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE cluster_config (row_type INT NOT NULL, version BIGINT NOT NULL, timestamp DATETIME NOT NULL, data MEDIUMBLOB NOT NULL, data_encoding VARCHAR(16) NOT NULL, PRIMARY KEY (row_type, version));",
        )
        .unwrap();
        Arc::new(Db(StdMutex::new(conn)))
    }

    fn load(db: &Arc<Db>, retries: i32) -> DynamicConfigOverlay {
        let snapshot = match cs::sqlite_fetch_latest(&db.0.lock().unwrap(), 0).unwrap() {
            Some(row) => cs::decode_snapshot(&row).unwrap(),
            None => cs::Snapshot::default(),
        };
        let mut o = DynamicConfigOverlay::default();
        o.attach_store(store_with(db, retries), &snapshot);
        o
    }

    #[test]
    fn updates_are_durable_across_restart_and_versions_increase() {
        let db = new_db();
        let mut o = load(&db, 1);
        o.admin_update(
            false,
            INT,
            vec![value("5", &[("domainName", "\"d\"")]), value("1", &[])],
        )
        .unwrap();
        o.admin_update(false, BOOL, vec![value("true", &[])])
            .unwrap();
        let latest = cs::sqlite_fetch_latest(&db.0.lock().unwrap(), 0)
            .unwrap()
            .unwrap();
        assert_eq!((latest.version, latest.encoding.as_str()), (2, "thriftrw"));
        // "Restart": a fresh overlay loads the same state, filters and order intact.
        let restarted = load(&db, 1);
        assert_eq!(restarted.admin_get(false, INT, &[]).unwrap().data, b"1");
        assert_eq!(
            restarted
                .admin_get(false, INT, &[filter("domainName", "\"d\"")])
                .unwrap()
                .data,
            b"5"
        );
        assert_eq!(restarted.admin_list(false, "").len(), 2);
        // Restore and delete are persisted too.
        let mut o = restarted;
        o.admin_restore(false, INT, &[]).unwrap();
        o.admin_update(false, BOOL, vec![]).unwrap();
        let again = load(&db, 1);
        assert_eq!(again.admin_list(false, "").len(), 1);
        assert_eq!(
            again.admin_get(false, INT, &[]),
            Err(DynamicConfigError::NotFound("unable to find key".into()))
        );
    }

    #[test]
    fn condition_failed_refetches_and_retries_then_runs_out_of_attempts() {
        let db = new_db();
        let mut a = load(&db, 1);
        let mut b = load(&db, 1);
        a.admin_update(false, INT, vec![value("1", &[])]).unwrap();
        // `b` still thinks version 0: the insert of version 1 conflicts, it
        // refetches `a`'s snapshot and retries, keeping both writes.
        b.admin_update(false, BOOL, vec![value("true", &[])])
            .unwrap();
        let merged = load(&db, 1);
        assert_eq!(merged.admin_list(false, "").len(), 2);
        // With no retry budget the conflict is "ran out of retry attempts".
        let mut stale = load(&db, 0);
        a.admin_update(false, INT, vec![value("2", &[])]).unwrap();
        assert_eq!(
            stale.admin_update(false, INT, vec![value("3", &[])]),
            Err(DynamicConfigError::Internal(
                "ran out of retry attempts on update".into()
            ))
        );
        assert_eq!(merged.admin_get(false, INT, &[]).unwrap().data, b"1");
    }

    #[test]
    fn add_search_attribute_persists_valid_search_attributes() {
        let db = new_db();
        let admin = AdminState::default();
        {
            let mut o = admin.dynamic_config.lock().unwrap();
            let snapshot = cs::Snapshot::default();
            o.attach_store(store_with(&db, 1), &snapshot);
        }
        let attrs = std::collections::HashMap::from([(
            "CustomKw".to_string(),
            cadence_api::enums::IndexedValueType::Keyword,
        )]);
        add_search_attribute_core(&admin, "", Some(&attrs)).unwrap();
        let restarted = load(&db, 1);
        let stored = restarted
            .admin_get(false, "frontend.validSearchAttributes", &[])
            .unwrap();
        let stored: std::collections::HashMap<String, i32> =
            serde_json::from_slice(&stored.data).unwrap();
        assert_eq!(
            stored["CustomKw"],
            cadence_api::enums::IndexedValueType::Keyword.value()
        );
    }
}
