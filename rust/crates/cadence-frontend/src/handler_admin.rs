//! Frontend Admin handler primitives ported from
//! `go/service/frontend/admin/handler.go`.

use std::{collections::BTreeMap, sync::{Arc, Mutex}};

use cadence_api::{
    admin::{
        AddSearchAttributeRequest, AddSearchAttributeResponse, DynamicConfigEntry,
        DynamicConfigFilter, DynamicConfigValue, GetDynamicConfigResponse,
        RestoreDynamicConfigResponse, UpdateDynamicConfigResponse,
    },
    enums::IndexedValueType,
    errors::{BadRequestError, EntityNotExistsError, InternalServiceError},
};
use cadence_service_core::admin::DynamicConfigOverlay;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    BadRequest(BadRequestError),
    EntityNotExists(EntityNotExistsError),
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
            .ok_or_else(|| AdminError::BadRequest(BadRequestError::new("SearchAttributes are not provided")))?;
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
        let value = overlay.get(operational, config_name, filters).ok_or_else(|| {
            AdminError::EntityNotExists(EntityNotExistsError::new(format!(
                "Dynamic config {config_name:?} does not have a value for the given filters."
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

    pub fn list_dynamic_config(&self, config_name: &str, operational: bool) -> Vec<DynamicConfigEntry> {
        self.dynamic_config
            .lock()
            .expect("admin lock")
            .list(operational, config_name)
    }

    #[must_use]
    pub fn search_attributes(&self) -> BTreeMap<String, IndexedValueType> {
        self.custom_search_attributes.lock().expect("admin lock").clone()
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
        handler.update_dynamic_config("worker.enabled", values.clone(), false).unwrap();
        assert!(handler.get_dynamic_config("worker.enabled", &[], true).is_err());
        assert_eq!(handler.get_dynamic_config("worker.enabled", &[], false).unwrap().value.unwrap().data, b"true");
        handler.restore_dynamic_config("worker.enabled", &[], false).unwrap();
        assert!(handler.get_dynamic_config("worker.enabled", &[], false).is_err());
    }

    #[test]
    fn search_attribute_conflicts_are_rejected_without_partial_update() {
        let handler = Handler::default();
        let attrs = std::collections::HashMap::from([("CustomOne".to_string(), IndexedValueType::String)]);
        let request = AddSearchAttributeRequest { search_attribute: Some(attrs), security_token: String::new() };
        handler.add_search_attributes(Some(&request)).unwrap();
        let conflicting = AddSearchAttributeRequest {
            search_attribute: Some(std::collections::HashMap::from([("CustomOne".to_string(), IndexedValueType::Int)])),
            security_token: String::new(),
        };
        assert!(matches!(handler.add_search_attributes(Some(&conflicting)), Err(AdminError::BadRequest(_))));
        assert_eq!(handler.search_attributes()["CustomOne"], IndexedValueType::String);
    }
}
