//! Persistence-backed dynamic config `Client` (Go
//! `common/dynamicconfig/configstore`): serves values from the latest
//! `config_entries` snapshot polled through `ConfigStoreClient`.
use std::sync::Arc;
use std::time::Duration;

use cadence_api::admin::{DynamicConfigFilter, DynamicConfigValue};
use cadence_api::enums::EncodingType;
use cadence_persistence::config_store::ConfigStoreClient;
use serde_json::Value;

use crate::config_collection::{DynamicClient, Filters};

pub struct PersistentDynamicClient {
    store: Arc<ConfigStoreClient>,
}

impl PersistentDynamicClient {
    pub fn new(store: Arc<ConfigStoreClient>) -> Arc<Self> {
        Arc::new(Self { store })
    }

    /// Go polls the datastore every `PollInterval`; failures keep the old snapshot.
    pub fn spawn_poller(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let store = self.store.clone();
        let interval: Duration = store.poll_interval();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(interval);
            loop {
                t.tick().await;
                let s = store.clone();
                match tokio::task::spawn_blocking(move || s.update()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!("dynamic config store update failed: {e}"),
                    Err(e) => tracing::warn!("dynamic config store update join: {e}"),
                }
            }
        })
    }
}

fn decode(blob: &cadence_api::types::DataBlob) -> Option<Value> {
    if blob.encoding_type != Some(EncodingType::JSON) {
        return None;
    }
    serde_json::from_slice(&blob.data).ok()
}

fn filter_matches(f: &DynamicConfigFilter, filters: &Filters) -> bool {
    let Some(want) = f.value.as_ref().and_then(decode) else { return false };
    let s = |o: &Option<String>| o.as_deref().is_some_and(|v| want.as_str() == Some(v));
    match f.name.to_lowercase().as_str() {
        "domainname" => s(&filters.domain_name),
        "domainid" => s(&filters.domain_id),
        "tasklistname" => s(&filters.task_list),
        "workflowid" => s(&filters.workflow_id),
        "shardid" => filters.shard_id.is_some_and(|v| want.as_i64() == Some(v)),
        _ => false,
    }
}

/// Go `matchFilters`: every filter must match; most filters wins.
fn pick<'a>(values: &'a [DynamicConfigValue], filters: &Filters) -> Option<&'a DynamicConfigValue> {
    values
        .iter()
        .filter(|v| v.filters.iter().all(|f| filter_matches(f, filters)))
        .max_by_key(|v| v.filters.len())
}

impl DynamicClient for PersistentDynamicClient {
    fn get_value(&self, key: &str, filters: &Filters) -> Option<Value> {
        let snap = self.store.snapshot()?;
        let entry = snap
            .entries?
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case(key))?;
        pick(&entry.values, filters)?.value.as_ref().and_then(decode)
    }
}
