//! Go `openfeatureclient.openFeatureClient` as a History `DynamicClient`.
//!
//! Go's typed collection getters call the per-type OpenFeature evaluation
//! (`GetIntValue`, `GetBoolValue`, ...); the Rust collection passes the
//! requested [`DynKind`]. A missing flag (Go `NotFoundError`) or any
//! evaluation error yields `None`, so the key's default applies exactly like
//! Go's `Collection` getters. The collection's cluster-name filter is added to
//! every evaluation context (Go `dynamicproperties.ClusterNameFilter`).

use std::collections::BTreeMap;

use cadence_service_core::openfeature::client::OpenFeatureClient;
use serde_json::Value;

use crate::config_collection::{DynKind, DynamicClient, Filters};

pub struct OpenFeatureDynamicClient {
    client: OpenFeatureClient,
    cluster_name: String,
}

impl OpenFeatureDynamicClient {
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self { client: OpenFeatureClient::new(), cluster_name: cluster_name.into() }
    }

    /// Go `toEvalContext` (filter names are `Filter.String()`).
    fn context(&self, f: &Filters) -> BTreeMap<String, Value> {
        let mut ctx = BTreeMap::new();
        if let Some(v) = &f.domain_name {
            ctx.insert("domainName".to_string(), Value::from(v.clone()));
        }
        if let Some(v) = &f.domain_id {
            ctx.insert("domainID".to_string(), Value::from(v.clone()));
        }
        if let Some(v) = &f.task_list {
            ctx.insert("taskListName".to_string(), Value::from(v.clone()));
        }
        if let Some(v) = f.shard_id {
            ctx.insert("shardID".to_string(), Value::from(v));
        }
        if let Some(v) = &f.workflow_id {
            ctx.insert("workflowID".to_string(), Value::from(v.clone()));
        }
        if !self.cluster_name.is_empty() {
            ctx.entry("clusterName".to_string()).or_insert_with(|| Value::from(self.cluster_name.clone()));
        }
        ctx
    }
}

impl DynamicClient for OpenFeatureDynamicClient {
    fn get_value(&self, key: &str, filters: &Filters) -> Option<Value> {
        let (v, err) = self.client.get_value_with_filters(key, Value::Null, &self.context(filters));
        err.is_none().then_some(v).filter(|v| !v.is_null())
    }

    fn get_typed(&self, key: &str, filters: &Filters, kind: DynKind) -> Option<Value> {
        let ctx = self.context(filters);
        match kind {
            DynKind::Int => {
                let (v, e) = self.client.get_int_value(key, 0, &ctx);
                e.is_none().then(|| Value::from(v))
            }
            DynKind::Float => {
                let (v, e) = self.client.get_float_value(key, 0.0, &ctx);
                e.is_none().then(|| serde_json::Number::from_f64(v).map(Value::Number)).flatten()
            }
            DynKind::Bool => {
                let (v, e) = self.client.get_bool_value(key, false, &ctx);
                e.is_none().then_some(Value::Bool(v))
            }
            DynKind::String => {
                let (v, e) = self.client.get_string_value(key, "", &ctx);
                e.is_none().then_some(Value::String(v))
            }
            DynKind::Duration => {
                let (v, e) = self.client.get_duration_value(key, 0, &ctx);
                // Number = seconds in `FromDynamic for Duration`.
                e.is_none().then(|| Value::from(v as f64 / 1e9))
            }
            DynKind::Map => {
                let (v, e) = self.client.get_map_value(key, Default::default(), &ctx);
                e.is_none().then_some(Value::Object(v))
            }
            DynKind::List => {
                let (v, e) = self.client.get_list_value(key, Vec::new(), &ctx);
                e.is_none().then_some(Value::Array(v))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_uses_go_filter_names_and_cluster() {
        let c = OpenFeatureDynamicClient::new("cluster0");
        let ctx = c.context(&Filters {
            domain_name: Some("d".into()),
            domain_id: Some("id".into()),
            shard_id: Some(3),
            task_list: Some("tl".into()),
            workflow_id: Some("wf".into()),
        });
        let keys: Vec<_> = ctx.keys().map(String::as_str).collect();
        assert_eq!(keys, ["clusterName", "domainID", "domainName", "shardID", "taskListName", "workflowID"]);
        assert_eq!(ctx["shardID"], Value::from(3));
    }

    #[test]
    fn no_provider_registered_returns_default_via_zero_value_none_for_untyped() {
        // Go: with no provider the typed details return the default and no error; the untyped
        // object lookup returns the Null default, which maps to "not set".
        let c = OpenFeatureDynamicClient::new("");
        assert_eq!(c.get_value("history.someKey", &Filters::default()), None);
        assert!(c.context(&Filters::default()).is_empty());
    }
}
