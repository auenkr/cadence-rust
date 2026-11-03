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
    errors::{BadRequestError, EntityNotExistsError, InternalServiceError},
    types::SupportedClientVersions,
};
use cadence_service_core::admin::DynamicConfigOverlay;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    BadRequest(BadRequestError),
    EntityNotExists(EntityNotExistsError),
    Internal(InternalServiceError),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(e) => std::fmt::Display::fmt(e, f),
            Self::EntityNotExists(e) => std::fmt::Display::fmt(e, f),
            Self::Internal(e) => std::fmt::Display::fmt(e, f),
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
    custom_search_attributes: Arc<Mutex<std::collections::HashMap<String, IndexedValueType>>>,
    es: Option<cadence_service_core::es_mapping::EsVisibility>,
}

impl Handler {
    /// Bind this handler to the same dynamic-config and search-attribute
    /// state used by Frontend's JSON and Thrift transports. Admin mutations
    /// are immediately visible to runtime property readers and other handlers.
    #[must_use]
    pub fn from_app_state(state: &cadence_service_core::handlers::AppState) -> Self {
        Self {
            dynamic_config: state.admin.dynamic_config.clone(),
            custom_search_attributes: state.admin.custom_search_attributes.clone(),
            es: state.admin.es.clone(),
        }
    }

    pub fn add_search_attributes(
        &self,
        request: Option<&AddSearchAttributeRequest>,
    ) -> Result<AddSearchAttributeResponse, AdminError> {
        let request = request
            .ok_or_else(|| AdminError::BadRequest(BadRequestError::new("Request is nil.")))?;
        let admin = cadence_service_core::admin::AdminState {
            dynamic_config: self.dynamic_config.clone(),
            custom_search_attributes: self.custom_search_attributes.clone(),
            es: self.es.clone(),
            ..Default::default()
        };
        cadence_service_core::admin::add_search_attribute_core(
            &admin,
            &request.security_token,
            request.search_attribute.as_ref(),
        )
        .map_err(|error| match error {
            cadence_service_core::admin::AddSearchAttributeError::BadRequest(message) => {
                AdminError::BadRequest(BadRequestError::new(message))
            }
            cadence_service_core::admin::AddSearchAttributeError::Internal(message) => {
                AdminError::Internal(InternalServiceError::new(message))
            }
        })?;
        Ok(AddSearchAttributeResponse {})
    }

    pub fn get_dynamic_config(
        &self,
        config_name: &str,
        filters: &[DynamicConfigFilter],
        operational: bool,
    ) -> Result<GetDynamicConfigResponse, AdminError> {
        let value = self
            .dynamic_config
            .lock()
            .expect("admin lock")
            .admin_get(operational, config_name, filters)
            .map_err(admin_error)?;
        Ok(GetDynamicConfigResponse { value: Some(value) })
    }

    pub fn update_dynamic_config(
        &self,
        config_name: &str,
        values: Vec<DynamicConfigValue>,
        operational: bool,
    ) -> Result<UpdateDynamicConfigResponse, AdminError> {
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .admin_update(operational, config_name, values)
            .map_err(admin_error)?;
        Ok(UpdateDynamicConfigResponse {})
    }

    pub fn restore_dynamic_config(
        &self,
        config_name: &str,
        filters: &[DynamicConfigFilter],
        operational: bool,
    ) -> Result<RestoreDynamicConfigResponse, AdminError> {
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .admin_restore(operational, config_name, filters)
            .map_err(admin_error)?;
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
            .admin_list(operational, config_name)
    }

    #[must_use]
    pub fn search_attributes(&self) -> BTreeMap<String, IndexedValueType> {
        let admin = cadence_service_core::admin::AdminState {
            dynamic_config: self.dynamic_config.clone(),
            custom_search_attributes: self.custom_search_attributes.clone(),
            ..Default::default()
        };
        cadence_service_core::admin::valid_search_attributes(&admin)
            .into_iter()
            .collect()
    }
}

fn admin_error(error: cadence_service_core::admin::DynamicConfigError) -> AdminError {
    use cadence_service_core::admin::DynamicConfigError as E;
    match error {
        E::BadRequest(m) => AdminError::BadRequest(BadRequestError::new(m)),
        E::NotFound(m) => AdminError::EntityNotExists(EntityNotExistsError::new(m)),
        E::Internal(m) => AdminError::Internal(InternalServiceError::new(m)),
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
            .update_dynamic_config("worker.enableScheduler", values.clone(), false)
            .unwrap();
        assert!(handler
            .get_dynamic_config("worker.enableScheduler", &[], true)
            .is_err());
        assert_eq!(
            handler
                .get_dynamic_config("worker.enableScheduler", &[], false)
                .unwrap()
                .value
                .unwrap()
                .data,
            b"true"
        );
        handler
            .restore_dynamic_config("worker.enableScheduler", &[], false)
            .unwrap();
        assert!(handler
            .get_dynamic_config("worker.enableScheduler", &[], false)
            .is_err());
    }

    #[derive(Debug, Default)]
    struct FakeEs {
        calls: Mutex<Vec<String>>,
        fail_put: bool,
    }
    impl cadence_service_core::es_mapping::EsMappingClient for FakeEs {
        fn put_mapping(
            &self,
            index: &str,
            key: &str,
            es_type: &str,
        ) -> Result<(), cadence_service_core::es_mapping::EsError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("put {index} {key} {es_type}"));
            if self.fail_put {
                Err(cadence_service_core::es_mapping::EsError::Other(
                    "down".into(),
                ))
            } else {
                Ok(())
            }
        }
        fn create_index(&self, _: &str) -> Result<(), cadence_service_core::es_mapping::EsError> {
            Ok(())
        }
    }

    #[test]
    fn add_search_attribute_updates_es_mapping_and_surfaces_failures() {
        let request = AddSearchAttributeRequest {
            search_attribute: Some(std::collections::HashMap::from([(
                "CustomInt".to_string(),
                IndexedValueType::Int,
            )])),
            security_token: String::new(),
        };
        let ok = Arc::new(FakeEs::default());
        let handler = Handler {
            es: Some(cadence_service_core::es_mapping::EsVisibility {
                client: ok.clone(),
                index: "vis".into(),
            }),
            ..Handler::default()
        };
        handler.add_search_attributes(Some(&request)).unwrap();
        assert_eq!(
            ok.calls.lock().unwrap().as_slice(),
            ["put vis CustomInt long"]
        );

        let bad = Arc::new(FakeEs {
            fail_put: true,
            ..Default::default()
        });
        let handler = Handler {
            es: Some(cadence_service_core::es_mapping::EsVisibility {
                client: bad,
                index: "vis".into(),
            }),
            ..Handler::default()
        };
        let err = handler.add_search_attributes(Some(&request)).unwrap_err();
        match err {
            AdminError::Internal(e) => {
                assert!(e.message.starts_with("Failed to update ES mapping, err:"))
            }
            other => panic!("unexpected {other:?}"),
        }
        // Go updates the dynamic config before touching ES, so the key stays whitelisted.
        assert!(handler.search_attributes().contains_key("CustomInt"));
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
    fn add_search_attribute_follows_go_validation_and_updates_valid_attributes_config() {
        let handler = Handler::default();
        let request = |name: &str, token: &str| AddSearchAttributeRequest {
            search_attribute: Some(std::collections::HashMap::from([(
                name.to_string(),
                IndexedValueType::Keyword,
            )])),
            security_token: token.to_string(),
        };
        // Empty attribute set is rejected before anything else.
        assert!(matches!(
            handler.add_search_attributes(Some(&AddSearchAttributeRequest::default())),
            Err(AdminError::BadRequest(e)) if e.message == "SearchAttributes are not provided"
        ));
        // System keys are reserved.
        assert!(matches!(
            handler.add_search_attributes(Some(&request("WorkflowID", ""))),
            Err(AdminError::BadRequest(e)) if e.message == "Key [WorkflowID] is reserved by system"
        ));
        handler
            .add_search_attributes(Some(&request("CustomKw", "")))
            .unwrap();
        // The accepted key lands in frontend.validSearchAttributes (Go UpdateValue)
        // alongside the default indexed keys, and GetSearchAttributes reads it back.
        let stored = handler
            .get_dynamic_config("frontend.validSearchAttributes", &[], false)
            .unwrap()
            .value
            .unwrap();
        let stored: std::collections::HashMap<String, i32> =
            serde_json::from_slice(&stored.data).unwrap();
        assert_eq!(stored["CustomKw"], IndexedValueType::Keyword.value());
        assert!(stored.contains_key("DomainID"));
        assert_eq!(
            handler.search_attributes()["CustomKw"],
            IndexedValueType::Keyword
        );
        // Admin protection requires the configured token.
        let protect = |name: &str, json: &str| {
            handler
                .update_dynamic_config(
                    name,
                    vec![DynamicConfigValue {
                        value: Some(cadence_api::types::DataBlob {
                            encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                            data: json.as_bytes().to_vec(),
                        }),
                        filters: vec![],
                    }],
                    false,
                )
                .unwrap();
        };
        protect("history.enableAdminProtection", "true");
        protect("history.adminOperationToken", "\"secret\"");
        assert!(matches!(
            handler.add_search_attributes(Some(&request("Other", "wrong"))),
            Err(AdminError::BadRequest(e)) if e.message == "No permission to do this operation."
        ));
        handler
            .add_search_attributes(Some(&request("Other", "secret")))
            .unwrap();
    }

    #[test]
    fn app_state_constructor_shares_dynamic_config_and_search_attributes() {
        let state = cadence_service_core::handlers::AppState::default();
        let handler = Handler::from_app_state(&state);
        handler
            .update_dynamic_config(
                "worker.enableScheduler",
                vec![DynamicConfigValue {
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: b"true".to_vec(),
                    }),
                    filters: vec![],
                }],
                false,
            )
            .unwrap();
        assert!(state.scheduler_enabled_for_domain("payments"));

        handler
            .add_search_attributes(Some(&AddSearchAttributeRequest {
                search_attribute: Some(std::collections::HashMap::from([(
                    "CustomShared".to_string(),
                    IndexedValueType::Keyword,
                )])),
                security_token: String::new(),
            }))
            .unwrap();
        assert_eq!(
            state
                .admin
                .custom_search_attributes
                .lock()
                .unwrap()
                .get("CustomShared"),
            Some(&IndexedValueType::Keyword)
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
