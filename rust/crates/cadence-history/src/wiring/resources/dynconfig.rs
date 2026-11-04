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
