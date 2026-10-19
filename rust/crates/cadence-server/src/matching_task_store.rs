//! Select Matching's queue persistence from Go's configured default store.
//!
//! This adapter intentionally supports the existing single-database SQLite
//! task tables only. It opens Cadence's shared schema and never creates a
//! parallel production database or installs a partial schema.
use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
use std::{sync::Arc, time::Duration};

const GO_DEFAULT_BUSY_TIMEOUT_MS: u64 = 60_000;
const GO_DEFAULT_JOURNAL_MODE: &str = "wal";
const GO_DEFAULT_TIME_FORMAT: &str = "2006-01-02T15:04:05.000000000Z07:00";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteTaskStoreConfig {
    pub database_name: String,
    /// SQLite file URI modes. `ro` is rejected because Matching writes tasks.
    pub mode: String,
    pub busy_timeout: Duration,
    pub journal_mode: String,
}

impl SqliteTaskStoreConfig {
    pub fn from_default_store(
        persistence: &crate::config::PersistenceConfig,
    ) -> anyhow::Result<Self> {
        let name = persistence.default_store.trim();
        if name.is_empty() {
            anyhow::bail!("Matching task persistence requires persistence.defaultStore");
        }
        let datastore = persistence.datastores.get(name).ok_or_else(|| {
            anyhow::anyhow!(
                "Matching defaultStore {name:?} is not present in persistence.datastores"
            )
        })?;
        let sql = datastore.sql.as_ref().ok_or_else(|| {
            if datastore.nosql.is_some() {
                anyhow::anyhow!("Matching task persistence does not support NoSQL defaultStore {name:?}; supported backend is SQL sqlite")
            } else {
                anyhow::anyhow!("Matching defaultStore {name:?} has no supported SQL configuration")
            }
        })?;
        if datastore.nosql.is_some() {
            anyhow::bail!("Matching defaultStore {name:?} configures both SQL and NoSQL backends");
        }
        if sql.plugin_name != "sqlite" {
            anyhow::bail!("Matching task persistence does not support SQL plugin {:?}; supported plugin is sqlite", sql.plugin_name);
        }
        if sql.n_shards < 0 || sql.n_shards > 1 || sql.use_multiple_databases {
            anyhow::bail!("Matching task persistence supports only a single SQL database (nShards=1, useMultipleDatabases=false)");
        }
        let database_name = &sql.database_name;
        if database_name.trim().is_empty() || database_name == ":memory:" {
            anyhow::bail!("Matching task persistence requires a file-backed SQLite databaseName");
        }
        // Go composes `file:<databaseName>?<attributes>` without escaping the
        // name. A literal '?' is already the URI query separator; percent
        // escapes continue through SQLite URI decoding.
        if database_name.contains('?') {
            anyhow::bail!("Matching SQLite databaseName cannot contain URI query attributes");
        }

        // The Go SQLite DSN omits mode by default; SQLite then uses rwc.
        let mut mode = "rwc".to_owned();
        let mut busy_timeout_ms = GO_DEFAULT_BUSY_TIMEOUT_MS;
        let mut journal_mode = GO_DEFAULT_JOURNAL_MODE.to_owned();
        let mut time_format = GO_DEFAULT_TIME_FORMAT.to_owned();
        let mut seen = std::collections::HashSet::new();
        for (raw_key, raw_value) in &sql.connect_attributes {
            let key = raw_key.trim().to_ascii_lowercase();
            let value = raw_value.trim();
            if !seen.insert(key.clone()) {
                anyhow::bail!("duplicate SQLite connectAttribute after key normalization: {key:?}");
            }
            match key.as_str() {
                // Go trims URI attribute values but leaves their case to the
                // SQLite URI parser; don't reinterpret a value it may reject.
                "mode" => mode = value.to_owned(),
                "_pragma.busy_timeout" => {
                    busy_timeout_ms = value.parse::<u64>().map_err(|_| {
                        anyhow::anyhow!("invalid SQLite _pragma.busy_timeout {value:?}")
                    })?;
                }
                "_pragma.journal_mode" => journal_mode = value.to_ascii_lowercase(),
                "_timefmt" => time_format = value.to_owned(),
                _ => anyhow::bail!(
                    "Matching SQLite task persistence does not support connectAttribute {key:?}"
                ),
            }
        }
        if !matches!(mode.as_str(), "rw" | "rwc") {
            anyhow::bail!(
                "Matching task persistence requires SQLite mode=rw or mode=rwc, got {mode:?}"
            );
        }
        if busy_timeout_ms > i32::MAX as u64 {
            anyhow::bail!("SQLite _pragma.busy_timeout exceeds the supported millisecond range");
        }
        if !matches!(
            journal_mode.as_str(),
            "delete" | "truncate" | "persist" | "memory" | "wal" | "off"
        ) {
            anyhow::bail!("unsupported SQLite _pragma.journal_mode {journal_mode:?}");
        }
        // Matching's task tables contain Thrift-encoded nanosecond timestamps,
        // not SQL DATETIME values. Still reject custom Go time formats rather
        // than silently ignoring a connection option that can affect other
        // Cadence SQL operations on the same connection pool.
        if time_format != GO_DEFAULT_TIME_FORMAT {
            anyhow::bail!(
                "Matching SQLite task persistence supports only Go's fixed-nanosecond _timefmt"
            );
        }
        Ok(Self {
            database_name: database_name.to_owned(),
            mode,
            busy_timeout: Duration::from_millis(busy_timeout_ms),
            journal_mode,
        })
    }

    pub fn open_store(&self) -> anyhow::Result<Arc<SqliteTaskStore>> {
        let store = SqliteTaskStore::open_with_go_sqlite_options(
            &self.database_name,
            BlobEncoding::ThriftRWSnappy,
            &self.mode,
            self.busy_timeout,
            &self.journal_mode,
        )?;
        Ok(Arc::new(store))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingTaskStoreSource<'a> {
    ConfiguredDefaultStore,
    EmbeddedDataDir(&'a str),
    InMemory,
}

/// Production config always selects the configured shared database. The
/// private `--data-dir/matching.db` remains an embedded-development fallback.
pub fn select_source<'a>(
    embedded_development: bool,
    data_dir: Option<&'a str>,
) -> MatchingTaskStoreSource<'a> {
    if !embedded_development {
        MatchingTaskStoreSource::ConfiguredDefaultStore
    } else if let Some(directory) = data_dir {
        MatchingTaskStoreSource::EmbeddedDataDir(directory)
    } else {
        MatchingTaskStoreSource::InMemory
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatastoreConfig, SqlConfig};
    use std::collections::HashMap;

    fn persistence(sql: Option<SqlConfig>) -> crate::config::PersistenceConfig {
        crate::config::PersistenceConfig {
            default_store: "primary".into(),
            datastores: HashMap::from([(
                "primary".into(),
                DatastoreConfig {
                    sql,
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    #[test]
    fn configured_default_store_keeps_go_sqlite_writer_options() {
        let config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: "shared/cadence.sqlite".into(),
            connect_attributes: HashMap::from([
                ("_pragma.busy_timeout".into(), "2400".into()),
                ("_pragma.journal_mode".into(), "WAL".into()),
                ("_timefmt".into(), GO_DEFAULT_TIME_FORMAT.into()),
                ("mode".into(), "rw".into()),
            ]),
            ..Default::default()
        }));
        let resolved = SqliteTaskStoreConfig::from_default_store(&config).unwrap();
        assert_eq!(resolved.database_name, "shared/cadence.sqlite");
        assert_eq!(resolved.mode, "rw");
        assert_eq!(resolved.busy_timeout, Duration::from_millis(2400));
        assert_eq!(resolved.journal_mode, "wal");
    }

    #[test]
    fn configured_default_store_applies_go_sqlite_defaults() {
        let config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: "cadence.sqlite".into(),
            // Go's SQLite driver builds its DSN from databaseName and ignores
            // connectAddr for this plugin.
            connect_addr: ":memory:".into(),
            ..Default::default()
        }));
        let resolved = SqliteTaskStoreConfig::from_default_store(&config).unwrap();
        assert_eq!(resolved.mode, "rwc");
        assert_eq!(resolved.busy_timeout, Duration::from_millis(60_000));
        assert_eq!(resolved.journal_mode, "wal");
    }

    #[test]
    fn rejects_unimplemented_persistence_shapes_and_attributes() {
        assert!(SqliteTaskStoreConfig::from_default_store(
            &crate::config::PersistenceConfig::default()
        )
        .unwrap_err()
        .to_string()
        .contains("requires persistence.defaultStore"));

        let missing_datastore = crate::config::PersistenceConfig {
            default_store: "missing".into(),
            ..Default::default()
        };
        assert!(
            SqliteTaskStoreConfig::from_default_store(&missing_datastore)
                .unwrap_err()
                .to_string()
                .contains("not present in persistence.datastores")
        );

        let mut config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: "cadence.sqlite".into(),
            ..Default::default()
        }));
        config
            .datastores
            .get_mut("primary")
            .unwrap()
            .sql
            .as_mut()
            .unwrap()
            .n_shards = 2;
        assert!(SqliteTaskStoreConfig::from_default_store(&config)
            .unwrap_err()
            .to_string()
            .contains("single SQL database"));

        let config = persistence(Some(SqlConfig {
            plugin_name: "postgres".into(),
            database_name: "cadence".into(),
            ..Default::default()
        }));
        assert!(SqliteTaskStoreConfig::from_default_store(&config)
            .unwrap_err()
            .to_string()
            .contains("supported plugin is sqlite"));

        let config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: "cadence.sqlite".into(),
            connect_attributes: HashMap::from([("_fk".into(), "true".into())]),
            ..Default::default()
        }));
        assert!(SqliteTaskStoreConfig::from_default_store(&config)
            .unwrap_err()
            .to_string()
            .contains("does not support connectAttribute"));

        let config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: "cadence.sqlite".into(),
            connect_attributes: HashMap::from([("mode".into(), "RW".into())]),
            ..Default::default()
        }));
        assert!(SqliteTaskStoreConfig::from_default_store(&config)
            .unwrap_err()
            .to_string()
            .contains("requires SQLite mode=rw or mode=rwc"));
    }

    #[test]
    fn explicit_production_config_wins_over_data_dir_but_embedded_mode_keeps_it() {
        assert_eq!(
            select_source(false, Some("state")),
            MatchingTaskStoreSource::ConfiguredDefaultStore
        );
        assert_eq!(
            select_source(true, Some("state")),
            MatchingTaskStoreSource::EmbeddedDataDir("state")
        );
        assert_eq!(select_source(true, None), MatchingTaskStoreSource::InMemory);
    }
}
