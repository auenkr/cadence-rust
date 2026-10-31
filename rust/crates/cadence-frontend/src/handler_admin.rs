//! Frontend Admin handler primitives ported from
//! `go/service/frontend/admin/handler.go`.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use cadence_api::{
    admin::{
        AddSearchAttributeRequest, AddSearchAttributeResponse, DescribeClusterResponse,
        DynamicConfigEntry, DynamicConfigFilter, DynamicConfigValue, GetDynamicConfigResponse,
        GetWorkflowExecutionRawHistoryV2Request, HostInfo, MembershipInfo, PersistenceFeature,
        PersistenceInfo, RestoreDynamicConfigResponse, RingInfo, UpdateDynamicConfigResponse,
    },
    enums::IndexedValueType,
    errors::{BadRequestError, InternalServiceError},
    types::SupportedClientVersions,
};
use cadence_service_core::admin::DynamicConfigOverlay;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    BadRequest(BadRequestError),
    Internal(InternalServiceError),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(e) => std::fmt::Display::fmt(e, f),
            Self::EntityNotExists(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for AdminError {}

/// Validate the request fields checked by Go before resolving a workflow's
/// version histories. The paired event/version fields are intentionally
/// validated here so malformed requests fail before persistence access.
pub fn validate_raw_history_request(
    request: &GetWorkflowExecutionRawHistoryV2Request,
) -> Result<(), BadRequestError> {
    let execution = request.execution.as_ref();
    if execution.is_none_or(|e| e.workflow_id.is_empty()) {
        return Err(BadRequestError::new("Invalid WorkflowID."));
    }
    if execution.is_none_or(|e| !is_uuid_parseable(&e.run_id)) {
        return Err(BadRequestError::new("Invalid RunID."));
    }
    if request.maximum_page_size <= 0 {
        return Err(BadRequestError::new("Invalid PageSize."));
    }
    if request.start_event_id.is_some() != request.start_event_version.is_some() {
        return Err(BadRequestError::new(
            "Invalid start event id and start event version combination.",
        ));
    }
    if request.end_event_id.is_some() != request.end_event_version.is_some() {
        return Err(BadRequestError::new(
            "Invalid end event id and end event version combination.",
        ));
    }
    Ok(())
}

/// Match the input forms accepted by Go's `google/uuid.Parse`.
fn is_uuid_parseable(value: &str) -> bool {
    let canonical = value
        .strip_prefix("urn:uuid:")
        .unwrap_or(value)
        .strip_prefix('{')
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or_else(|| value.strip_prefix("urn:uuid:").unwrap_or(value));
    let hex = if canonical.len() == 36
        && canonical.as_bytes().get(8) == Some(&b'-')
        && canonical.as_bytes().get(13) == Some(&b'-')
        && canonical.as_bytes().get(18) == Some(&b'-')
        && canonical.as_bytes().get(23) == Some(&b'-')
    {
        canonical.chars().filter(|c| *c != '-').collect::<String>()
    } else if canonical.len() == 32 {
        canonical.to_string()
    } else {
        return false;
    };
    hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Elasticsearch type mapping used when adding custom search attributes.
#[must_use]
pub fn search_attribute_es_type(value_type: IndexedValueType) -> Option<&'static str> {
    match value_type {
        IndexedValueType::String => Some("text"),
        IndexedValueType::Keyword => Some("keyword"),
        IndexedValueType::Int => Some("long"),
        IndexedValueType::Double => Some("double"),
        IndexedValueType::Bool => Some("boolean"),
        IndexedValueType::Datetime => Some("date"),
        _ => None,
    }
}

/// Membership snapshot collected from the live Frontend resolver before the
/// response is built. Keeping collection separate preserves Go's failure
/// boundary: resolver errors must be returned before this helper is called.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipSnapshot {
    pub current_host_identity: Option<String>,
    /// Ring order and member order are preserved from membership discovery.
    pub rings: Vec<(String, Vec<String>)>,
}

/// Build the `DescribeCluster` response using Go's public SDK constraints,
/// membership fields, and persistence feature keys.
#[must_use]
pub fn describe_cluster_response(
    membership: Option<&MembershipSnapshot>,
    history_backend: impl Into<String>,
    visibility_backend: impl Into<String>,
    advanced_visibility_enabled: bool,
) -> DescribeClusterResponse {
    let mut membership_info = MembershipInfo::default();
    if let Some(snapshot) = membership {
        membership_info.current_host =
            snapshot
                .current_host_identity
                .as_ref()
                .map(|identity| HostInfo {
                    identity: identity.clone(),
                });
        for (role, identities) in &snapshot.rings {
            let members = identities
                .iter()
                .map(|identity| HostInfo {
                    identity: identity.clone(),
                })
                .collect::<Vec<_>>();
            membership_info
                .reachable_members
                .extend(identities.iter().cloned());
            membership_info.rings.push(RingInfo {
                role: role.clone(),
                member_count: members.len() as i32,
                members,
            });
        }
    }

    let persistence_info = BTreeMap::from([
        (
            "historyStore".to_string(),
            PersistenceInfo {
                backend: history_backend.into(),
                features: Vec::new(),
            },
        ),
        (
            "visibilityStore".to_string(),
            PersistenceInfo {
                backend: visibility_backend.into(),
                features: vec![PersistenceFeature {
                    key: "advancedVisibilityEnabled".into(),
                    enabled: advanced_visibility_enabled,
                }],
            },
        ),
    ])
    .into_iter()
    .collect();

    DescribeClusterResponse {
        supported_client_versions: Some(SupportedClientVersions {
            go_sdk: "<=1.7.0".into(),
            java_sdk: "<=1.5.0".into(),
        }),
        membership_info: Some(membership_info),
        persistence_info: Some(persistence_info),
    }
}

/// State used by Admin RPCs. `DynamicConfigOverlay` preserves the ordered
/// values and normal/operational namespaces that Go's config clients expose.
#[derive(Debug, Clone, Default)]
pub struct Handler {
    dynamic_config: Arc<Mutex<DynamicConfigOverlay>>,
    custom_search_attributes: Arc<Mutex<BTreeMap<String, IndexedValueType>>>,
}

impl Handler {
    pub fn dispatch(&self, operation: &str) -> Result<(), InternalServiceError> {
        Err(crate::handler_workflow::unimplemented_operation(operation))
    }

    pub fn add_search_attributes(
        &self,
        request: Option<&AddSearchAttributeRequest>,
    ) -> Result<AddSearchAttributeResponse, AdminError> {
        let attrs = request
            .and_then(|r| r.search_attribute.as_ref())
            .filter(|attrs| !attrs.is_empty())
            .ok_or_else(|| {
                AdminError::BadRequest(BadRequestError::new("SearchAttributes are not provided"))
            })?;
        let mut custom = self.custom_search_attributes.lock().expect("admin lock");
        for (key, value_type) in attrs {
            if cadence_api::search_attributes::is_system_indexed_key(key) {
                return Err(AdminError::BadRequest(BadRequestError::new(format!(
                    "Key [{key}] is reserved by system"
                ))));
            }
            if custom.get(key).is_some_and(|old| old != value_type) {
                return Err(AdminError::BadRequest(BadRequestError::new(format!(
                    "Key [{key}] is already whitelisted as a different type"
                ))));
            }
        }
        custom.extend(attrs.iter().map(|(k, v)| (k.clone(), *v)));
        Ok(AddSearchAttributeResponse {})
    }

    pub fn get_dynamic_config(
        &self,
        config_name: &str,
        filters: &[DynamicConfigFilter],
        operational: bool,
    ) -> Result<GetDynamicConfigResponse, AdminError> {
        if config_name.is_empty() {
            return Err(AdminError::BadRequest(BadRequestError::new(
                "ConfigName is not set on request.",
            )));
        }
        let overlay = self.dynamic_config.lock().expect("admin lock");
        let value = overlay
            .get(operational, config_name, filters)
            .ok_or_else(|| {
                AdminError::Internal(InternalServiceError::new(format!(
                    "Failed to get dynamic config {config_name:?} for the given filters."
                )))
            })?;
        Ok(GetDynamicConfigResponse { value: value.value })
    }

    pub fn update_dynamic_config(
        &self,
        config_name: &str,
        values: Vec<DynamicConfigValue>,
        operational: bool,
    ) -> Result<UpdateDynamicConfigResponse, AdminError> {
        require_config_name(config_name)?;
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .update(operational, config_name, values);
        Ok(UpdateDynamicConfigResponse {})
    }

    pub fn restore_dynamic_config(
        &self,
        config_name: &str,
        filters: &[DynamicConfigFilter],
        operational: bool,
    ) -> Result<RestoreDynamicConfigResponse, AdminError> {
        require_config_name(config_name)?;
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .restore(operational, config_name, filters);
        Ok(RestoreDynamicConfigResponse {})
    }

    pub fn list_dynamic_config(
        &self,
        config_name: &str,
        operational: bool,
    ) -> Vec<DynamicConfigEntry> {
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .list(operational, config_name)
    }

    #[must_use]
    pub fn search_attributes(&self) -> BTreeMap<String, IndexedValueType> {
        self.custom_search_attributes
            .lock()
            .expect("admin lock")
            .clone()
    }
}

fn require_config_name(config_name: &str) -> Result<(), AdminError> {
    if config_name.is_empty() {
        Err(AdminError::BadRequest(BadRequestError::new(
            "ConfigName is not set on request.",
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_config_namespaces_and_restore_follow_go_overlay_behavior() {
        let handler = Handler::default();
        let values = vec![DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob {
                encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                data: b"true".to_vec(),
            }),
            filters: vec![],
        }];
        handler
            .update_dynamic_config("worker.enabled", values.clone(), false)
            .unwrap();
        assert!(handler
            .get_dynamic_config("worker.enabled", &[], true)
            .is_err());
        assert_eq!(
            handler
                .get_dynamic_config("worker.enabled", &[], false)
                .unwrap()
                .value
                .unwrap()
                .data,
            b"true"
        );
        handler
            .restore_dynamic_config("worker.enabled", &[], false)
            .unwrap();
        assert!(handler
            .get_dynamic_config("worker.enabled", &[], false)
            .is_err());
    }

    #[test]
    fn search_attribute_conflicts_are_rejected_without_partial_update() {
        let handler = Handler::default();
        let attrs =
            std::collections::HashMap::from([("CustomOne".to_string(), IndexedValueType::String)]);
        let request = AddSearchAttributeRequest {
            search_attribute: Some(attrs),
            security_token: String::new(),
        };
        handler.add_search_attributes(Some(&request)).unwrap();
        let conflicting = AddSearchAttributeRequest {
            search_attribute: Some(std::collections::HashMap::from([(
                "CustomOne".to_string(),
                IndexedValueType::Int,
            )])),
            security_token: String::new(),
        };
        assert!(matches!(
            handler.add_search_attributes(Some(&conflicting)),
            Err(AdminError::BadRequest(_))
        ));
        assert_eq!(
            handler.search_attributes()["CustomOne"],
            IndexedValueType::String
        );
    }

    #[test]
    fn describe_cluster_builds_go_shaped_membership_and_persistence_fields() {
        let membership = MembershipSnapshot {
            current_host_identity: Some("frontend-1".into()),
            rings: vec![
                (
                    "frontend".into(),
                    vec!["frontend-1".into(), "frontend-2".into()],
                ),
                ("history".into(), vec!["history-1".into()]),
            ],
        };
        let response = describe_cluster_response(Some(&membership), "sql", "elasticsearch", true);
        let versions = response.supported_client_versions.unwrap();
        assert_eq!(versions.go_sdk, "<=1.7.0");
        assert_eq!(versions.java_sdk, "<=1.5.0");
        let membership = response.membership_info.unwrap();
        assert_eq!(membership.current_host.unwrap().identity, "frontend-1");
        assert_eq!(
            membership.reachable_members,
            vec![
                "frontend-1".to_string(),
                "frontend-2".to_string(),
                "history-1".to_string(),
            ]
        );
        assert_eq!(membership.rings[0].member_count, 2);
        let persistence = response.persistence_info.unwrap();
        assert_eq!(persistence["historyStore"].backend, "sql");
        assert_eq!(persistence["visibilityStore"].backend, "elasticsearch");
        assert_eq!(
            persistence["visibilityStore"].features[0].key,
            "advancedVisibilityEnabled"
        );
        assert!(persistence["visibilityStore"].features[0].enabled);
    }

    #[test]
    fn raw_history_validation_matches_go_order_and_pairing_rules() {
        let mut request = GetWorkflowExecutionRawHistoryV2Request::default();
        assert_eq!(
            validate_raw_history_request(&request).unwrap_err().message,
            "Invalid WorkflowID."
        );
        request.execution = Some(cadence_api::types::WorkflowExecution::new("wf", "bad"));
        assert_eq!(
            validate_raw_history_request(&request).unwrap_err().message,
            "Invalid RunID."
        );
        request.execution = Some(cadence_api::types::WorkflowExecution::new(
            "wf",
            "123e4567-e89b-12d3-a456-426614174000",
        ));
        request.maximum_page_size = 10;
        request.start_event_id = Some(1);
        assert!(validate_raw_history_request(&request)
            .unwrap_err()
            .message
            .contains("start event id and start event version"));
        request.start_event_version = Some(2);
        assert!(validate_raw_history_request(&request).is_ok());
    }

    #[test]
    fn raw_history_uuid_parser_accepts_google_uuid_forms() {
        for value in [
            "123e4567-e89b-12d3-a456-426614174000",
            "123e4567e89b12d3a456426614174000",
            "{123e4567-e89b-12d3-a456-426614174000}",
            "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
        ] {
            assert!(is_uuid_parseable(value), "{value}");
        }
        assert!(!is_uuid_parseable("123e4567-e89b-12d3-a456-42661417400z"));
    }

    #[test]
    fn search_attribute_type_mapping_matches_go_elasticsearch_mapping() {
        assert_eq!(
            search_attribute_es_type(IndexedValueType::String),
            Some("text")
        );
        assert_eq!(
            search_attribute_es_type(IndexedValueType::Keyword),
            Some("keyword")
        );
        assert_eq!(
            search_attribute_es_type(IndexedValueType::Int),
            Some("long")
        );
        assert_eq!(
            search_attribute_es_type(IndexedValueType::Double),
            Some("double")
        );
        assert_eq!(
            search_attribute_es_type(IndexedValueType::Bool),
            Some("boolean")
        );
        assert_eq!(
            search_attribute_es_type(IndexedValueType::Datetime),
            Some("date")
        );
    }
}
