//! Config-store dynamic config client, ported from Go
//! `common/dynamicconfig/configstore/config_store_client.go`.
//!
//! The latest `cluster_config` row (row type `DynamicConfig` = 0) holds a
//! ThriftRW `config.DynamicConfigBlob`. SQL stores keep `-version` and select
//! `ORDER BY version LIMIT 1`; Cassandra keeps `version` as a descending
//! clustering key. A fetch replaces the whole cached snapshot, a failed fetch
//! keeps the previous one.

use cadence_api::admin::{DynamicConfigEntry, DynamicConfigFilter, DynamicConfigValue};
use cadence_api::enums::EncodingType;
use cadence_api::types::DataBlob;
use cadence_thrift::protocol::Reader;
use cadence_thrift::value::{read_struct, Struct, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Go `persistence.DynamicConfig` config type.
pub const CONFIG_TYPE_DYNAMIC_CONFIG: i32 = 0;
/// Go `configStoreMinPollInterval`.
pub const MIN_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Go `persistence.InternalConfigStoreEntry` after the SQL plugin has
/// restored the version sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigStoreRow {
    pub version: i64,
    pub data: Vec<u8>,
    pub encoding: String,
}

/// Reads the latest row for a row type; `Ok(None)` is Go's not-found case.
pub type ConfigRowFetcher =
    Arc<dyn Fn(i32) -> Result<Option<ConfigStoreRow>, String> + Send + Sync>;

/// Go `csc.ClientConfig`, durations in nanoseconds like `time.Duration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub poll_interval: Duration,
    pub update_retry_attempts: i32,
    pub fetch_timeout: Duration,
    pub update_timeout: Duration,
}

impl ClientConfig {
    /// Go `defaultConfigValues` (FetchTimeout/UpdateTimeout are 2ns).
    pub fn go_defaults() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            update_retry_attempts: 1,
            fetch_timeout: Duration::from_nanos(2),
            update_timeout: Duration::from_nanos(2),
        }
    }
}

/// Go `cacheEntry`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Snapshot {
    pub version: i64,
    pub schema_version: i64,
    /// `None` is Go's nil map (blob without `entries`).
    pub entries: Option<Vec<DynamicConfigEntry>>,
}

pub struct ConfigStoreClient {
    fetch: ConfigRowFetcher,
    fetch_timeout: Duration,
    poll_interval: Duration,
    values: Mutex<Option<Snapshot>>,
}

impl ConfigStoreClient {
    /// Go `NewConfigStoreClient` after the datastore was opened: runs the
    /// first `update` and fails when it fails.
    pub fn new(config: &ClientConfig, fetch: ConfigRowFetcher) -> Result<Self, String> {
        let client = Self {
            fetch,
            fetch_timeout: config.fetch_timeout,
            poll_interval: config.poll_interval,
            values: Mutex::new(None),
        };
        client.update()?;
        Ok(client)
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Go `update`: fetch, and replace the cache when a snapshot exists.
    pub fn update(&self) -> Result<(), String> {
        let started = Instant::now();
        let result = (self.fetch)(CONFIG_TYPE_DYNAMIC_CONFIG);
        // Go checks `ctx.Done()` after the call, so a call that outlived
        // FetchTimeout is a timeout even when it returned data.
        if started.elapsed() >= self.fetch_timeout {
            return Err("timeout error on fetch".into());
        }
        let row = result.map_err(|e| format!("failed to fetch dynamic config snapshot {e}"))?;
        let Some(row) = row else { return Ok(()) };
        let snapshot = decode_snapshot(&row)
            .map_err(|e| format!("failed to fetch dynamic config snapshot {e}"))?;
        *self.values.lock().expect("config store lock") = Some(snapshot);
        Ok(())
    }

    /// `None` until a row was loaded (Go `values.Load() == nil`).
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.values.lock().expect("config store lock").clone()
    }
}

/// Go `DeserializeDynamicConfigBlob` + `FetchDynamicConfig`.
pub fn decode_snapshot(row: &ConfigStoreRow) -> Result<Snapshot, String> {
    if row.data.is_empty() {
        return Ok(Snapshot {
            version: row.version,
            ..Snapshot::default()
        });
    }
    let thrift = match row.encoding.as_str() {
        "thriftrw" => decode_struct(&row.data)?,
        "thriftrw_snappy" => {
            let raw = snap::raw::Decoder::new()
                .decompress_vec(&row.data)
                .map_err(|e| format!("DeserializeBatchEvents encoding: \"thriftrw_snappy\", error: {e}"))?;
            decode_struct(&raw)?
        }
        "json" | "" | "unknown" => {
            return Err(format!(
                "config store blob encoding {:?} is not supported by the Rust port (Go writes thriftrw)",
                row.encoding
            ))
        }
        other => return Err(format!("unknown encoding type: {other}")),
    };
    Ok(blob_from_thrift(&thrift, row.version))
}

fn decode_struct(data: &[u8]) -> Result<Struct, String> {
    read_struct(&mut Reader::new(data))
        .map_err(|e| format!("DeserializeBatchEvents encoding: \"thriftrw\", error: {e}"))
}

// Generated Go decoders skip a known field ID with the wrong wire type.
fn text(s: &Struct, id: i16) -> Option<String> {
    match s.get(id) {
        Some(Value::Bytes(b)) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

fn list(s: &Struct, id: i16) -> Option<&Vec<Value>> {
    match s.get(id) {
        Some(Value::List(l)) => Some(l),
        _ => None,
    }
}

fn data_blob(s: &Struct, id: i16) -> Option<DataBlob> {
    let Some(Value::Struct(b)) = s.get(id) else { return None };
    let encoding_type = match b.get(10) {
        Some(Value::I32(0)) => Some(EncodingType::ThriftRW),
        Some(Value::I32(1)) => Some(EncodingType::JSON),
        _ => None,
    };
    let data = match b.get(20) {
        Some(Value::Bytes(d)) => d.clone(),
        _ => Vec::new(),
    };
    Some(DataBlob { encoding_type, data })
}

fn blob_from_thrift(blob: &Struct, version: i64) -> Snapshot {
    let schema_version = match blob.get(10) {
        Some(Value::I64(v)) => *v,
        _ => 0,
    };
    let entries = list(blob, 20).map(|entries| {
        entries
            .iter()
            .filter_map(|e| match e {
                Value::Struct(e) => Some(e),
                _ => None,
            })
            .map(|e| DynamicConfigEntry {
                name: text(e, 10).unwrap_or_default(),
                values: list(e, 20)
                    .map(|vals| {
                        vals.iter()
                            .filter_map(|v| match v {
                                Value::Struct(v) => Some(v),
                                _ => None,
                            })
                            .map(|v| DynamicConfigValue {
                                value: data_blob(v, 10),
                                filters: list(v, 20)
                                    .map(|fs| {
                                        fs.iter()
                                            .filter_map(|f| match f {
                                                Value::Struct(f) => Some(f),
                                                _ => None,
                                            })
                                            .map(|f| DynamicConfigFilter {
                                                name: text(f, 10).unwrap_or_default(),
                                                value: data_blob(f, 20),
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect()
    });
    Snapshot {
        version,
        schema_version,
        entries,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_thrift::protocol::{TType, Writer};

    fn encode_blob(entries: &[(&str, &[(&str, Option<(&str, &str)>)])]) -> Vec<u8> {
        let mut w = Writer::new();
        w.write_field_begin(TType::I64, 10);
        w.write_i64(7);
        w.write_field_begin(TType::List, 20);
        w.write_list_begin(TType::Struct, entries.len());
        for (name, values) in entries {
            w.write_field_begin(TType::String, 10);
            w.write_string(name);
            w.write_field_begin(TType::List, 20);
            w.write_list_begin(TType::Struct, values.len());
            for (json, filter) in *values {
                w.write_field_begin(TType::Struct, 10);
                w.write_field_begin(TType::I32, 10);
                w.write_i32(1);
                w.write_field_begin(TType::String, 20);
                w.write_bytes(json.as_bytes());
                w.write_field_stop();
                if let Some((fname, fjson)) = filter {
                    w.write_field_begin(TType::List, 20);
                    w.write_list_begin(TType::Struct, 1);
                    w.write_field_begin(TType::String, 10);
                    w.write_string(fname);
                    w.write_field_begin(TType::Struct, 20);
                    w.write_field_begin(TType::I32, 10);
                    w.write_i32(1);
                    w.write_field_begin(TType::String, 20);
                    w.write_bytes(fjson.as_bytes());
                    w.write_field_stop();
                    w.write_field_stop();
                }
                w.write_field_stop();
            }
            w.write_field_stop();
        }
        w.write_field_stop();
        w.into_bytes()
    }

    fn row(version: i64, data: Vec<u8>, encoding: &str) -> ConfigStoreRow {
        ConfigStoreRow { version, data, encoding: encoding.into() }
    }

    #[test]
    fn decodes_thriftrw_blob_with_filters_in_order() {
        let data = encode_blob(&[(
            "matching.k",
            &[("1", None), ("2", Some(("domainName", "\"d\"")))],
        )]);
        let snap = decode_snapshot(&row(5, data, "thriftrw")).unwrap();
        assert_eq!((snap.version, snap.schema_version), (5, 7));
        let entries = snap.entries.unwrap();
        assert_eq!(entries[0].name, "matching.k");
        assert_eq!(entries[0].values.len(), 2);
        assert_eq!(entries[0].values[0].value.as_ref().unwrap().data, b"1");
        assert_eq!(entries[0].values[1].filters[0].name, "domainName");
        assert_eq!(
            entries[0].values[1].value.as_ref().unwrap().encoding_type,
            Some(EncodingType::JSON)
        );
    }

    #[test]
    fn empty_data_is_empty_blob_and_unknown_encoding_errors() {
        let snap = decode_snapshot(&row(3, vec![], "thriftrw")).unwrap();
        assert_eq!(snap, Snapshot { version: 3, schema_version: 0, entries: None });
        assert!(decode_snapshot(&row(3, vec![1], "bogus")).is_err());
    }

    #[test]
    fn snappy_blob_decodes() {
        let raw = encode_blob(&[("a", &[("true", None)])]);
        let packed = snap::raw::Encoder::new().compress_vec(&raw).unwrap();
        let snap = decode_snapshot(&row(1, packed, "thriftrw_snappy")).unwrap();
        assert_eq!(snap.entries.unwrap()[0].name, "a");
    }

    fn client_config(timeout: Duration) -> ClientConfig {
        ClientConfig { fetch_timeout: timeout, ..ClientConfig::go_defaults() }
    }

    #[test]
    fn update_replaces_snapshot_and_keeps_it_on_failure_or_missing_row() {
        let rows = Arc::new(Mutex::new(vec![
            Ok(Some(row(1, encode_blob(&[("a", &[("1", None)])]), "thriftrw"))),
            Ok(None),
            Err("boom".to_string()),
            Ok(Some(row(2, encode_blob(&[("b", &[("1", None)])]), "thriftrw"))),
        ]));
        let source = rows.clone();
        let fetch: ConfigRowFetcher = Arc::new(move |_| source.lock().unwrap().remove(0));
        let client = ConfigStoreClient::new(&client_config(Duration::from_secs(5)), fetch).unwrap();
        assert_eq!(client.snapshot().unwrap().version, 1);
        client.update().unwrap(); // not found keeps the cache
        assert_eq!(client.snapshot().unwrap().version, 1);
        let err = client.update().unwrap_err();
        assert!(err.starts_with("failed to fetch dynamic config snapshot"), "{err}");
        assert_eq!(client.snapshot().unwrap().version, 1);
        client.update().unwrap();
        let snap = client.snapshot().unwrap();
        assert_eq!(snap.version, 2);
        assert_eq!(snap.entries.unwrap()[0].name, "b");
    }

    #[test]
    fn no_row_at_startup_leaves_no_snapshot_and_first_error_fails_creation() {
        let ok: ConfigRowFetcher = Arc::new(|_| Ok(None));
        let client = ConfigStoreClient::new(&client_config(Duration::from_secs(5)), ok).unwrap();
        assert!(client.snapshot().is_none());
        let bad: ConfigRowFetcher = Arc::new(|_| Err("down".into()));
        assert!(ConfigStoreClient::new(&client_config(Duration::from_secs(5)), bad).is_err());
    }

    #[test]
    fn go_default_two_nanosecond_fetch_timeout_always_times_out() {
        let fetch: ConfigRowFetcher = Arc::new(|_| Ok(None));
        let err = ConfigStoreClient::new(&ClientConfig::go_defaults(), fetch).err().unwrap();
        assert_eq!(err, "timeout error on fetch");
    }

    #[test]
    fn sqlite_fetcher_returns_latest_version_with_sign_restored() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE cluster_config (row_type INT NOT NULL, version BIGINT NOT NULL, timestamp DATETIME NOT NULL, data MEDIUMBLOB NOT NULL, data_encoding VARCHAR(16) NOT NULL, PRIMARY KEY (row_type, version));
             INSERT INTO cluster_config VALUES (0, -1, '2020-01-01', x'00', 'thriftrw');
             INSERT INTO cluster_config VALUES (0, -3, '2020-01-02', x'01', 'thriftrw');
             INSERT INTO cluster_config VALUES (0, -2, '2020-01-02', x'02', 'thriftrw');
             INSERT INTO cluster_config VALUES (1, -9, '2020-01-02', x'03', 'thriftrw');",
        )
        .unwrap();
        let latest = sqlite_fetch_latest(&conn, 0).unwrap().unwrap();
        assert_eq!((latest.version, latest.data), (3, vec![1]));
        assert!(sqlite_fetch_latest(&conn, 5).unwrap().is_none());
    }
}

/// SQL plugins (`SelectLatestConfig`): smallest stored (negated) version wins.
pub fn sqlite_fetch_latest(
    conn: &rusqlite::Connection,
    row_type: i32,
) -> Result<Option<ConfigStoreRow>, String> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        "SELECT version, data, data_encoding FROM cluster_config WHERE row_type = ? ORDER BY version LIMIT 1",
        [row_type],
        |r| {
            Ok(ConfigStoreRow {
                version: -r.get::<_, i64>(0)?,
                data: r.get(1)?,
                encoding: r.get(2)?,
            })
        },
    )
    .optional()
    .map_err(|e| e.to_string())
}
