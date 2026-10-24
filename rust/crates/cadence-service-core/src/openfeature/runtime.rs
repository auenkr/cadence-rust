//! Adapter from Matching's key-preserving runtime property reads to the
//! process-wide OpenFeature client.

use std::{collections::BTreeMap, sync::Arc};

use cadence_persistence::dynamic_config::RuntimePropertyProvider;
use serde_json::Value;

use super::client::OpenFeatureClient;

#[derive(Debug, Default)]
pub struct OpenFeatureRuntimeProvider {
    client: OpenFeatureClient,
    cluster_name: Option<String>,
}

impl OpenFeatureRuntimeProvider {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { client: OpenFeatureClient::new(), cluster_name: None })
    }

    pub fn with_cluster_name(cluster_name: impl Into<String>) -> Arc<Self> {
        let cluster_name = cluster_name.into();
        Arc::new(Self { client: OpenFeatureClient::new(), cluster_name: (!cluster_name.is_empty()).then_some(cluster_name) })
    }
}

impl RuntimePropertyProvider for OpenFeatureRuntimeProvider {
    fn evaluate(
        &self,
        key: &str,
        default: Value,
        filters: &BTreeMap<String, Value>,
    ) -> Result<Value, String> {
        let mut context = filters.clone();
        if let Some(cluster_name) = &self.cluster_name {
            context.entry("clusterName".to_owned()).or_insert_with(|| Value::String(cluster_name.clone()));
        }
        match &default {
            Value::Bool(default) => {
                let (value, error) = self.client.get_bool_value(key, *default, &context);
                error.map_or_else(|| Ok(Value::Bool(value)), |error| Err(error.to_string()))
            }
            Value::String(default) => {
                let (value, error) = self.client.get_string_value(key, default, &context);
                error.map_or_else(|| Ok(Value::String(value)), |error| Err(error.to_string()))
            }
            Value::Number(default) if default.as_i64().is_some() => {
                let (value, error) = self.client.get_int_value(key, default.as_i64().unwrap(), &context);
                error.map_or_else(|| Ok(Value::from(value)), |error| Err(error.to_string()))
            }
            Value::Number(default) => {
                let (value, error) = self.client.get_float_value(key, default.as_f64().unwrap_or_default(), &context);
                error.map_or_else(|| Ok(serde_json::Number::from_f64(value).map(Value::Number).unwrap_or(Value::Null)), |error| Err(error.to_string()))
            }
            Value::Array(default) => {
                let (value, error) = self.client.get_list_value(key, default.clone(), &context);
                error.map_or_else(|| Ok(Value::Array(value)), |error| Err(error.to_string()))
            }
            Value::Object(default) => {
                let (value, error) = self.client.get_map_value(key, default.clone(), &context);
                error.map_or_else(|| Ok(Value::Object(value)), |error| Err(error.to_string()))
            }
            Value::Null => return Err("null has no OpenFeature property type".into()),
        }
    }
}
