//! File-based dynamic config `Client` (go/common/dynamicconfig/configstore + file client):
//! YAML `key: [{value, constraints}]`, case-insensitive keys, the entry with the most
//! matching constraints wins, periodic reload from disk.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::Value;

use crate::config_collection::{DynamicClient, Filters};

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Value,
    pub constraints: HashMap<String, Value>,
}

/// `dynamicconfig.Client` backed by a YAML file; `None` path = no overrides (defaults only).
pub struct FileDynamicClient {
    path: Option<PathBuf>,
    entries: RwLock<HashMap<String, Vec<Entry>>>,
}

impl FileDynamicClient {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self { path: None, entries: RwLock::new(HashMap::new()) })
    }

    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        let c = Arc::new(Self { path: Some(path), entries: RwLock::new(HashMap::new()) });
        c.reload()?;
        Ok(c)
    }

    pub fn parse(text: &str) -> anyhow::Result<HashMap<String, Vec<Entry>>> {
        let doc: serde_yaml::Value = serde_yaml::from_str(text)?;
        let mut out: HashMap<String, Vec<Entry>> = HashMap::new();
        let serde_yaml::Value::Mapping(map) = doc else {
            return Ok(out);
        };
        for (k, v) in map {
            let Some(key) = k.as_str() else { continue };
            let list = v.as_sequence().cloned().unwrap_or_default();
            let mut entries = Vec::new();
            for item in list {
                let item: Value = serde_json::to_value(&item)?;
                let value = item.get("value").cloned().unwrap_or(Value::Null);
                let constraints = item
                    .get("constraints")
                    .and_then(|c| c.as_object())
                    .map(|o| o.iter().map(|(a, b)| (a.to_lowercase(), b.clone())).collect())
                    .unwrap_or_default();
                entries.push(Entry { value, constraints });
            }
            out.insert(key.to_lowercase(), entries);
        }
        Ok(out)
    }

    /// Re-read the file; on error the previous snapshot is kept (Go logs and keeps old values).
    pub fn reload(&self) -> anyhow::Result<()> {
        let Some(path) = &self.path else { return Ok(()) };
        let text = std::fs::read_to_string(path)?;
        *self.entries.write().unwrap() = Self::parse(&text)?;
        Ok(())
    }

    /// Poll the file every `interval` (Go default 10s `pollInterval`).
    pub fn spawn_reloader(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(interval);
            loop {
                t.tick().await;
                if let Err(e) = me.reload() {
                    tracing::warn!("dynamic config reload failed: {e}");
                }
            }
        })
    }
}

fn constraint_matches(name: &str, want: &Value, f: &Filters) -> bool {
    let s = |o: &Option<String>| o.as_deref().is_some_and(|v| want.as_str() == Some(v));
    match name {
        "domainname" => s(&f.domain_name),
        "domainid" => s(&f.domain_id),
        "tasklistname" => s(&f.task_list),
        "workflowid" => s(&f.workflow_id),
        "shardid" => f.shard_id.is_some_and(|v| want.as_i64() == Some(v)),
        _ => false,
    }
}

impl DynamicClient for FileDynamicClient {
    fn get_value(&self, key: &str, filters: &Filters) -> Option<Value> {
        let map = self.entries.read().unwrap();
        let entries = map.get(&key.to_lowercase())?;
        entries
            .iter()
            .filter(|e| e.constraints.iter().all(|(n, w)| constraint_matches(n, w, filters)))
            .max_by_key(|e| e.constraints.len())
            .map(|e| e.value.clone())
    }
}

/// Go `configStoreClient` (common/dynamicconfig/configstore) read path: polls the
/// persisted snapshot and resolves a key with Go `getValueWithFilters` semantics.
pub struct ConfigStoreDynamicClient {
    client: Arc<cadence_persistence::config_store::ConfigStoreClient>,
}

impl ConfigStoreDynamicClient {
    pub fn new(client: Arc<cadence_persistence::config_store::ConfigStoreClient>) -> Arc<Self> {
        Arc::new(Self { client })
    }

    /// Go `pollForDynamicConfig`: refresh the snapshot every poll interval.
    pub fn spawn_poller(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(&self.client);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(client.poll_interval());
            loop {
                t.tick().await;
                let c = Arc::clone(&client);
                match tokio::task::spawn_blocking(move || c.update()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!("config store dynamic config update failed: {e}"),
                    Err(e) => tracing::warn!("config store dynamic config update panicked: {e}"),
                }
            }
        })
    }
}

fn blob_json(blob: &Option<cadence_api::types::DataBlob>) -> Option<Value> {
    serde_json::from_slice(&blob.as_ref()?.data).ok()
}

fn filter_value(f: &Filters, name: &str) -> Option<Value> {
    let s = |o: &Option<String>| o.as_ref().map(|v| Value::String(v.clone()));
    match name.to_lowercase().as_str() {
        "domainname" => s(&f.domain_name),
        "domainid" => s(&f.domain_id),
        "tasklistname" => s(&f.task_list),
        "workflowid" => s(&f.workflow_id),
        "shardid" => f.shard_id.map(Value::from),
        _ => None,
    }
}

impl DynamicClient for ConfigStoreDynamicClient {
    fn get_value(&self, key: &str, filters: &Filters) -> Option<Value> {
        let snapshot = self.client.snapshot()?;
        let entries = snapshot.entries?;
        let entry = entries.iter().find(|e| e.name.eq_ignore_ascii_case(key))?;
        let provided = [
            filters.domain_name.is_some(),
            filters.domain_id.is_some(),
            filters.task_list.is_some(),
            filters.workflow_id.is_some(),
            filters.shard_id.is_some(),
        ]
        .into_iter()
        .filter(|b| *b)
        .count();
        let mut default = None;
        for v in &entry.values {
            if v.filters.is_empty() {
                if let Some(parsed) = blob_json(&v.value) {
                    default = Some(parsed);
                }
                continue;
            }
            if v.filters.len() > provided {
                continue;
            }
            let matched = v.filters.iter().all(|f| {
                match (filter_value(filters, &f.name), blob_json(&f.value)) {
                    (Some(got), Some(want)) => got == want,
                    _ => false,
                }
            });
            if matched {
                return blob_json(&v.value);
            }
        }
        default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn most_specific_constraint_wins() {
        let text = "history.RPS:\n- value: 10\n  constraints: {}\n- value: 20\n  constraints:\n    domainName: foo\n";
        let c = FileDynamicClient { path: None, entries: RwLock::new(FileDynamicClient::parse(text).unwrap()) };
        assert_eq!(c.get_value("history.rps", &Filters::default()), Some(Value::from(10)));
        let f = Filters { domain_name: Some("foo".into()), ..Default::default() };
        assert_eq!(c.get_value("History.RPS", &f), Some(Value::from(20)));
        assert_eq!(c.get_value("missing", &f), None);
    }
}
