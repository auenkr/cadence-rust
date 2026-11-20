//! Global isolation-group config store, ported from Go
//! `resource.createConfigStoreOrDefault` + `configstore.configStoreClient`
//! (config type `GlobalIsolationGroupConfig` = 1) and the
//! `isolationgroupapi` global handlers / mappers.
//!
//! The latest `cluster_config` row of row type 1 is polled every
//! `system.isolationGroupStateRefreshInterval`; updates write version
//! `cached + 1` and, on a version conflict, refetch and retry
//! `system.isolationGroupStateUpdateRetryAttempts` times.

use crate::config_store::{
    decode_snapshot, encode_snapshot, ClientConfig, ConfigRowFetcher, ConfigRowWriter,
    ConfigStoreRow, ConfigWriteError, Snapshot, MIN_POLL_INTERVAL,
};
use cadence_api::admin::{DynamicConfigEntry, DynamicConfigValue};
use cadence_api::enums::{EncodingType, IsolationGroupState};
use cadence_api::types::{DataBlob, IsolationGroupConfiguration, IsolationGroupPartition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Go `persistence.GlobalIsolationGroupConfig`.
pub const CONFIG_TYPE_GLOBAL_ISOLATION_GROUP: i32 = 1;
/// Go `dynamicproperties.DefaultIsolationGroupConfigStoreManagerGlobalMapping` key name.
pub const GLOBAL_MAPPING_KEY: &str = "system.defaultIsolationGroupConfigStoreManagerGlobalMapping";
/// Go `isolationgroupapi` error when no config store could be created.
pub const NOT_SUPPORTED: &str = "global isolation group drain is not supported in this cluster";

/// Go dynamic config defaults for the isolation-group store client
/// (`system.isolationGroupState*`).
pub fn go_isolation_client_config() -> ClientConfig {
    ClientConfig {
        poll_interval: Duration::from_secs(30),
        update_retry_attempts: 2,
        fetch_timeout: Duration::from_secs(30),
        update_timeout: Duration::from_secs(30),
    }
}

/// Go `validateClientConfig`; invalid values fall back to `defaultConfigValues`.
pub fn validated(config: ClientConfig) -> ClientConfig {
    if config.poll_interval < MIN_POLL_INTERVAL
        || config.update_retry_attempts < 0
        || config.fetch_timeout.is_zero()
        || config.update_timeout.is_zero()
    {
        ClientConfig::go_defaults()
    } else {
        config
    }
}

/// Errors surfaced by Get/UpdateGlobalIsolationGroups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationStoreError {
    /// Go `types.BadRequestError`.
    BadRequest(String),
    /// Any other error (Go returns it as an internal error).
    Internal(String),
}

impl std::fmt::Display for IsolationStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(m) | Self::Internal(m) => f.write_str(m),
        }
    }
}

pub struct GlobalIsolationGroupStore {
    fetch: ConfigRowFetcher,
    write: ConfigRowWriter,
    config: ClientConfig,
    values: Mutex<Option<Snapshot>>,
}

impl std::fmt::Debug for GlobalIsolationGroupStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalIsolationGroupStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GlobalIsolationGroupStore {
    /// Go `NewConfigStoreClient`: validates the config and runs the first
    /// `update`; a failing first fetch fails construction.
    pub fn new(
        config: ClientConfig,
        fetch: ConfigRowFetcher,
        write: ConfigRowWriter,
    ) -> Result<Self, String> {
        let store = Self {
            fetch,
            write,
            config: validated(config),
            values: Mutex::new(None),
        };
        store.update()?;
        Ok(store)
    }

    pub fn poll_interval(&self) -> Duration {
        self.config.poll_interval
    }

    /// Go `startUpdate` goroutine: refetch every poll interval for the
    /// process lifetime.
    pub fn start_polling(self: &Arc<Self>) {
        let store = Arc::downgrade(self);
        let interval = self.config.poll_interval;
        let _ = std::thread::Builder::new()
            .name("isolation-group-store".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                let Some(store) = store.upgrade() else { return };
                if let Err(e) = store.update() {
                    eprintln!("Failed to update dynamic config: {e}");
                }
            });
    }

    /// Go `update`.
    pub fn update(&self) -> Result<(), String> {
        let started = Instant::now();
        let result = (self.fetch)(CONFIG_TYPE_GLOBAL_ISOLATION_GROUP);
        if started.elapsed() >= self.config.fetch_timeout {
            return Err("timeout error on fetch".into());
        }
        let row = result.map_err(|e| format!("failed to fetch dynamic config snapshot {e}"))?;
        let Some(row) = row else { return Ok(()) };
        let snapshot = decode_snapshot(&row)
            .map_err(|e| format!("failed to fetch dynamic config snapshot {e}"))?;
        *self.values.lock().expect("isolation store lock") = Some(snapshot);
        Ok(())
    }

    pub fn snapshot(&self) -> Option<Snapshot> {
        self.values.lock().expect("isolation store lock").clone()
    }

    /// Go `handlerImpl.GetGlobalState`.
    pub fn get(&self) -> Result<Option<IsolationGroupConfiguration>, IsolationStoreError> {
        let wrap =
            |e: String| IsolationStoreError::Internal(format!("failed to get global isolation groups from datastore: {e}"));
        let list = match self.get_list_value() {
            Ok(list) => list,
            // dc.NotFoundError is a types.EntityNotExistsError.
            Err(ListError::NotFound) => return Ok(None),
            Err(ListError::Other(e)) => return Err(wrap(e)),
        };
        map_dynamic_config_response(list).map_err(wrap)
    }

    /// Go's drain watcher propagates lookup errors to the task router.
    pub(crate) fn get_for_routing(&self) -> Result<Option<IsolationGroupConfiguration>, String> {
        let list = self
            .get_list_value()
            .map_err(|error| match error {
                ListError::NotFound => "global isolation groups not found".to_string(),
                ListError::Other(error) => error,
            })?;
        map_dynamic_config_response(list)
    }

    /// Go `GetListValue(key, nil)` -> `getValueWithFilters`.
    fn get_list_value(&self) -> Result<Option<Vec<serde_json::Value>>, ListError> {
        let Some(snapshot) = self.snapshot() else {
            // Nothing loaded: the (nil) default list without error.
            return Ok(None);
        };
        let mut found: Option<serde_json::Value> = None;
        if let Some(entry) = snapshot
            .entries
            .iter()
            .flatten()
            .rev()
            .find(|e| e.name == GLOBAL_MAPPING_KEY)
        {
            for value in &entry.values {
                if value.filters.is_empty() {
                    if let Ok(v) = convert_from_data_blob(value.value.as_ref()) {
                        found = Some(v);
                    }
                    continue;
                }
                // A filtered value matches nil request filters only when it
                // has no filters, which was handled above.
            }
        }
        match found {
            None => Err(ListError::NotFound),
            Some(serde_json::Value::Array(list)) => Ok(Some(list)),
            Some(_) => Err(ListError::Other("value type is not list".into())),
        }
    }

    /// Go `handlerImpl.UpdateGlobalState` -> `UpdateValue` -> `updateValue`.
    pub fn set(
        &self,
        groups: Option<IsolationGroupConfiguration>,
    ) -> Result<(), IsolationStoreError> {
        let values = map_update_request(groups);
        self.update_value(values, self.config.update_retry_attempts)
            .map_err(IsolationStoreError::Internal)
    }

    fn update_value(&self, values: Vec<DynamicConfigValue>, retry_attempts: i32) -> Result<(), String> {
        for v in &values {
            // validateKeyDataBlobPair for a ListKey: any JSON decodes, then a
            // list type is required.
            match convert_from_data_blob(v.value.as_ref())? {
                serde_json::Value::Array(_) => {}
                _ => {
                    return Err(
                        "key value pair mismatch, key type: dynamicproperties.ListKey, value type: <non-list>".into(),
                    )
                }
            }
        }
        let current = self.snapshot().unwrap_or(Snapshot {
            entries: Some(Vec::new()),
            ..Snapshot::default()
        });
        let existing: Vec<DynamicConfigEntry> = current.entries.clone().unwrap_or_default();
        let exists = existing.iter().any(|e| e.name == GLOBAL_MAPPING_KEY);
        let mut new_entries = Vec::with_capacity(existing.len() + 1);
        if values.is_empty() {
            new_entries.extend(existing.into_iter().filter(|e| e.name != GLOBAL_MAPPING_KEY));
        } else {
            if !exists {
                new_entries.push(DynamicConfigEntry {
                    name: GLOBAL_MAPPING_KEY.into(),
                    values: values.clone(),
                });
            }
            for e in existing {
                if e.name == GLOBAL_MAPPING_KEY {
                    new_entries.push(DynamicConfigEntry {
                        name: GLOBAL_MAPPING_KEY.into(),
                        values: values.clone(),
                    });
                } else {
                    new_entries.push(e);
                }
            }
        }
        let row = ConfigStoreRow {
            version: current.version + 1,
            data: encode_snapshot(current.schema_version, &new_entries),
            encoding: "thriftrw".into(),
        };
        let started = Instant::now();
        let result = (self.write)(CONFIG_TYPE_GLOBAL_ISOLATION_GROUP, &row);
        if started.elapsed() >= self.config.update_timeout {
            return Err("timeout error on update".into());
        }
        match result {
            Ok(()) => Ok(()),
            Err(ConfigWriteError::ConditionFailed(_)) if retry_attempts > 0 => {
                self.update()?;
                self.update_value(values, retry_attempts - 1)
            }
            Err(_) if retry_attempts == 0 => Err("ran out of retry attempts on update".into()),
            Err(ConfigWriteError::ConditionFailed(e)) | Err(ConfigWriteError::Other(e)) => Err(e),
        }
    }
}

enum ListError {
    NotFound,
    Other(String),
}

/// Go `convertFromDataBlob`: only JSON blobs are supported.
fn convert_from_data_blob(blob: Option<&DataBlob>) -> Result<serde_json::Value, String> {
    match blob {
        Some(b) if matches!(b.encoding_type, Some(EncodingType::JSON)) => {
            serde_json::from_slice(&b.data).map_err(|e| e.to_string())
        }
        _ => Err("unsupported blob encoding".into()),
    }
}

fn state_to_i64(state: Option<IsolationGroupState>) -> i64 {
    state.map_or(0, |s| s as i64)
}

fn state_from_f64(v: f64) -> Option<IsolationGroupState> {
    match v as i32 {
        0 => Some(IsolationGroupState::Invalid),
        1 => Some(IsolationGroupState::Healthy),
        2 => Some(IsolationGroupState::Drained),
        _ => None,
    }
}

/// Go `types.IsolationGroupConfiguration` is a map keyed by name; the wire
/// form (`ToPartitionList` / `FromIsolationGroupConfig`) is sorted by name.
fn dedupe_sorted(partitions: impl IntoIterator<Item = IsolationGroupPartition>) -> Vec<IsolationGroupPartition> {
    let mut map = BTreeMap::new();
    for p in partitions {
        map.insert(p.name.clone(), p);
    }
    map.into_values().collect()
}

/// Go `MapUpdateGlobalIsolationGroupsRequest`.
pub fn map_update_request(groups: Option<IsolationGroupConfiguration>) -> Vec<DynamicConfigValue> {
    let list: Vec<serde_json::Value> = dedupe_sorted(groups.map(|g| g.isolation_groups).unwrap_or_default())
        .into_iter()
        .map(|p| serde_json::json!({"Name": p.name, "State": state_to_i64(p.state)}))
        .collect();
    vec![DynamicConfigValue {
        value: Some(DataBlob {
            encoding_type: Some(EncodingType::JSON),
            data: serde_json::to_vec(&list).expect("json"),
        }),
        filters: Vec::new(),
    }]
}

/// Go `MapDynamicConfigResponse`.
pub fn map_dynamic_config_response(
    list: Option<Vec<serde_json::Value>>,
) -> Result<Option<IsolationGroupConfiguration>, String> {
    let Some(list) = list else { return Ok(None) };
    let mut out = Vec::with_capacity(list.len());
    for v in &list {
        let err = || format!("failed parse a dynamic config entry, {v}, (got {v})");
        let obj = v.as_object().ok_or_else(err)?;
        let (Some(n), Some(s)) = (obj.get("Name"), obj.get("State")) else {
            return Err(err());
        };
        let (Some(name), Some(state)) = (n.as_str(), s.as_f64()) else {
            return Err(err());
        };
        out.push(IsolationGroupPartition {
            name: name.to_owned(),
            state: state_from_f64(state),
        });
    }
    Ok(Some(IsolationGroupConfiguration {
        isolation_groups: dedupe_sorted(out),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::{sqlite_fetch_latest, sqlite_insert_config};
    use std::sync::Mutex as StdMutex;

    fn db() -> Arc<StdMutex<rusqlite::Connection>> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE cluster_config (row_type INT NOT NULL, version BIGINT NOT NULL, timestamp DATETIME NOT NULL, data MEDIUMBLOB NOT NULL, data_encoding VARCHAR(16) NOT NULL, PRIMARY KEY (row_type, version));",
        )
        .unwrap();
        Arc::new(StdMutex::new(conn))
    }

    fn store(db: &Arc<StdMutex<rusqlite::Connection>>) -> GlobalIsolationGroupStore {
        let (f, w) = (db.clone(), db.clone());
        GlobalIsolationGroupStore::new(
            go_isolation_client_config(),
            Arc::new(move |t| sqlite_fetch_latest(&f.lock().unwrap(), t)),
            Arc::new(move |t, r| sqlite_insert_config(&w.lock().unwrap(), t, r)),
        )
        .unwrap()
    }

    fn cfg(parts: &[(&str, IsolationGroupState)]) -> Option<IsolationGroupConfiguration> {
        Some(IsolationGroupConfiguration {
            isolation_groups: parts
                .iter()
                .map(|(n, s)| IsolationGroupPartition {
                    name: (*n).into(),
                    state: Some(*s),
                })
                .collect(),
        })
    }

    #[test]
    fn empty_store_returns_nil_and_update_persists_shared_across_clients() {
        let db = db();
        let a = store(&db);
        let b = store(&db);
        assert_eq!(a.get(), Ok(None));
        a.set(cfg(&[("zone-b", IsolationGroupState::Drained), ("zone-a", IsolationGroupState::Healthy)]))
            .unwrap();
        let want = cfg(&[("zone-a", IsolationGroupState::Healthy), ("zone-b", IsolationGroupState::Drained)]);
        // a has not refetched yet (Go only refreshes on poll).
        assert_eq!(a.get(), Ok(None));
        a.update().unwrap();
        assert_eq!(a.get(), Ok(want.clone()));
        // A second client (another host) sees it after its poll.
        b.update().unwrap();
        assert_eq!(b.get(), Ok(want));
        let row = sqlite_fetch_latest(&db.lock().unwrap(), 1).unwrap().unwrap();
        assert_eq!(row.version, 1);
        assert!(sqlite_fetch_latest(&db.lock().unwrap(), 0).unwrap().is_none());
        let snap = decode_snapshot(&row).unwrap();
        let entry = &snap.entries.unwrap()[0];
        assert_eq!(entry.name, GLOBAL_MAPPING_KEY);
        assert_eq!(
            entry.values[0].value.as_ref().unwrap().data,
            br#"[{"Name":"zone-a","State":1},{"Name":"zone-b","State":2}]"#.to_vec()
        );
    }

    #[test]
    fn stale_client_retries_on_version_conflict() {
        let db = db();
        let a = store(&db);
        let b = store(&db);
        a.set(cfg(&[("x", IsolationGroupState::Drained)])).unwrap();
        // b still caches version 0 -> conflict on version 1, refetch, write 2.
        b.set(cfg(&[("y", IsolationGroupState::Healthy)])).unwrap();
        assert_eq!(sqlite_fetch_latest(&db.lock().unwrap(), 1).unwrap().unwrap().version, 2);
        a.update().unwrap();
        assert_eq!(a.get(), Ok(cfg(&[("y", IsolationGroupState::Healthy)])));
    }

    #[test]
    fn nil_update_stores_empty_list_and_reads_back_empty_config() {
        let db = db();
        let a = store(&db);
        a.set(None).unwrap();
        a.update().unwrap();
        assert_eq!(a.get(), Ok(Some(IsolationGroupConfiguration::default())));
    }

    #[test]
    fn mapper_errors_and_invalid_client_config() {
        assert!(map_dynamic_config_response(Some(vec![serde_json::json!("x")])).is_err());
        assert!(map_dynamic_config_response(Some(vec![serde_json::json!({"Name": "a"})])).is_err());
        let bad = ClientConfig {
            poll_interval: Duration::from_secs(1),
            ..go_isolation_client_config()
        };
        assert_eq!(validated(bad), ClientConfig::go_defaults());
        assert_eq!(validated(go_isolation_client_config()), go_isolation_client_config());
    }
}
