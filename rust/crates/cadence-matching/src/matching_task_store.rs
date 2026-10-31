//! Select Matching's queue persistence from Go's configured default store.
//!
//! This adapter supports the existing SQLite, MySQL, and PostgreSQL task
//! tables. It opens Cadence's shared schema and never creates a
//! parallel production database or installs a partial schema.
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_mysql::{MySqlTaskStore, MySqlTaskStoreConfig, MySqlTlsConfig},
    matching_postgres::{
        PostgresShardConfig, PostgresTaskStore, PostgresTaskStoreConfig, PostgresTlsConfig,
    },
    matching_sqlite::SqliteTaskStore,
    matching_task_store::MatchingTaskStore,
};
use std::{sync::Arc, time::Duration};

const GO_DEFAULT_BUSY_TIMEOUT_MS: u64 = 60_000;
const GO_DEFAULT_JOURNAL_MODE: &str = "wal";
const GO_DEFAULT_TIME_FORMAT: &str = "2006-01-02T15:04:05.000000000Z07:00";

pub struct ConfiguredMatchingStores {
    pub task_store: Arc<dyn MatchingTaskStore>,
    /// The same backend/pool exposed through the byte-preserving contract.
    pub native_task_store:
        Arc<dyn cadence_persistence::matching_native_task_store::MatchingTaskStoreBytes>,
    pub domain_metadata_source: Option<Arc<dyn crate::matching_domain_cache::DomainMetadataSource>>,
    /// Transactional Go SQL domain/metadata manager when this backend exposes
    /// the existing SQL domain row schema (currently MySQL and PostgreSQL).
    pub domain_persistence:
        Option<Arc<dyn cadence_persistence::matching_domain_metadata::GoSqlDomainPersistence>>,
    /// Explicit per-backend Go domain-ID parse behavior; every production constructor sets it.
    pub domain_id_parse: crate::matching_domain_uuid::DomainIdParse,
}

fn validate_sql_shards(sql: &crate::config::SqlConfig) -> anyhow::Result<()> {
    if sql.use_multiple_databases {
        if !sql.database_name.is_empty()
            || !sql.connect_addr.is_empty()
            || !sql.user.is_empty()
            || !sql.password.is_empty()
        {
            anyhow::bail!("SQL multipleDatabasesConfig requires databaseName, connectAddr, user, and password to be configured per endpoint");
        }
        if sql.n_shards <= 1 || sql.multiple_databases_config.len() != sql.n_shards as usize {
            anyhow::bail!("SQL nShards must be greater than one and equal to the length of multipleDatabasesConfig");
        }
        if sql
            .multiple_databases_config
            .iter()
            .any(|entry| entry.database_name.is_empty() || entry.connect_addr.is_empty())
        {
            anyhow::bail!(
                "SQL multipleDatabasesConfig entries require databaseName and connectAddr"
            );
        }
    } else if !sql.multiple_databases_config.is_empty() {
        anyhow::bail!("SQL multipleDatabasesConfig requires useMultipleDatabases=true");
    }
    Ok(())
}

/// Open Matching's configured default store.
fn cassandra_config(
    nosql: &crate::config::NosqlConfig,
) -> anyhow::Result<cadence_persistence::cassandra::CassandraSessionConfig> {
    let proto_version = if nosql.proto_version == 0 {
        std::env::var("CASSANDRA_PROTO_VERSION")
            .ok()
            .map(|version| version.parse::<i32>())
            .transpose()?
            .unwrap_or(4)
    } else {
        nosql.proto_version
    };
    let tls = nosql.tls.as_ref().filter(|tls| tls.enabled).map(|tls| {
        cadence_persistence::cassandra::CassandraTlsConfig {
            ca_files: tls
                .ca_files
                .iter()
                .chain((!tls.ca_file.is_empty()).then_some(&tls.ca_file))
                .map(std::path::PathBuf::from)
                .collect(),
            cert_file: (!tls.cert_file.is_empty()).then(|| tls.cert_file.clone().into()),
            key_file: (!tls.key_file.is_empty()).then(|| tls.key_file.clone().into()),
            verify_hostname: tls.enable_host_verification,
            server_name: (!tls.server_name.is_empty()).then(|| tls.server_name.clone()),
        }
    });
    let options = cadence_persistence::cassandra::CassandraSessionOptions {
        timeout_nanos: nosql.timeout_nanos,
        connect_timeout_nanos: nosql.connect_timeout_nanos,
        consistency: nosql.consistency.clone(),
        serial_consistency: nosql.serial_consistency.clone(),
        max_conns: nosql.max_conns,
        datacenter: nosql.datacenter.clone(),
        region: nosql.region.clone(),
        tls,
        allowed_authenticators: nosql.allowed_authenticators.clone(),
        proto_version,
        connect_attributes: nosql.connect_attributes.clone(),
        host_selection_policy: nosql.host_selection_policy.clone(),
    };
    cadence_persistence::cassandra::CassandraSessionConfig::from_parts(
        nosql.hosts_list(),
        nosql.port,
        nosql.user.clone(),
        nosql.password.clone(),
        nosql.keyspace.clone(),
    )?
    .with_go_options(options)
}

/// Open the configured default store's semaphore adapter. SQL deliberately
/// returns Go's unsupported implementation; Cassandra uses the same async
/// session primitives as the task/domain adapters.
pub struct ConfiguredSemaphoreStores {
    pub legacy: Arc<dyn cadence_persistence::SemaphorePersistence>,
    pub native: Arc<dyn cadence_persistence::semaphores_native::SemaphorePersistenceBytes>,
}

pub async fn open_configured_semaphore_store(
    persistence: &crate::config::PersistenceConfig,
) -> anyhow::Result<Arc<dyn cadence_persistence::SemaphorePersistence>> {
    Ok(open_configured_semaphore_stores(persistence).await?.legacy)
}

/// Expose both contracts over one configured session, so frontend metadata and
/// native Matching token operations use the same Cassandra keyspace.
pub async fn open_configured_semaphore_stores(
    persistence: &crate::config::PersistenceConfig,
) -> anyhow::Result<ConfiguredSemaphoreStores> {
    let name = persistence.default_store.as_str();
    let datastore = persistence.datastores.get(name).ok_or_else(|| {
        anyhow::anyhow!("Matching defaultStore {name:?} is not present in persistence.datastores")
    })?;
    if let Some(nosql) = datastore.nosql.as_ref() {
        if datastore.sql.is_some() {
            anyhow::bail!("Matching defaultStore {name:?} configures both SQL and NoSQL backends");
        }
        if nosql.plugin_name != "cassandra" {
            anyhow::bail!("Matching NoSQL persistence does not support plugin {:?}; supported plugin is cassandra", nosql.plugin_name);
        }
        let config = cassandra_config(nosql)?;
        let session = Arc::new(config.connect().await?);
        let store =
            Arc::new(cadence_persistence::cassandra::CassandraSemaphorePersistence::new(session));
        return Ok(ConfiguredSemaphoreStores {
            legacy: store.clone(),
            native: store,
        });
    }
    if datastore.sql.is_some() {
        let store = Arc::new(cadence_persistence::SqlUnsupportedSemaphorePersistence);
        return Ok(ConfiguredSemaphoreStores {
            legacy: store.clone(),
            native: store,
        });
    }
    anyhow::bail!("Matching defaultStore {name:?} has neither SQL nor NoSQL configuration")
}

pub fn open_configured_default_store(
    persistence: &crate::config::PersistenceConfig,
    active_cluster_name: &str,
    all_consistency_delete: Arc<dyn Fn() -> bool + Send + Sync>,
    serialization_encoding: Arc<dyn Fn() -> String + Send + Sync>,
) -> anyhow::Result<ConfiguredMatchingStores> {
    let name = persistence.default_store.as_str();
    if name.is_empty() {
        anyhow::bail!("Matching task persistence requires persistence.defaultStore");
    }
    let datastore = persistence.datastores.get(name).ok_or_else(|| {
        anyhow::anyhow!("Matching defaultStore {name:?} is not present in persistence.datastores")
    })?;
    if let Some(nosql) = datastore.nosql.as_ref() {
        if datastore.sql.is_some() {
            anyhow::bail!("Matching defaultStore {name:?} configures both SQL and NoSQL backends");
        }
        if nosql.plugin_name != "cassandra" {
            anyhow::bail!("Matching NoSQL persistence does not support plugin {:?}; supported plugin is cassandra", nosql.plugin_name);
        }
        let config = cassandra_config(nosql)?;
        let driver =
            Arc::new(cadence_persistence::cassandra::CassandraBlockingSession::connect(&config)?);
        let task_store = Arc::new(
            cadence_persistence::matching_cassandra::CassandraMatchingTaskStore::with_all_consistency_delete(
                driver.clone(),
                all_consistency_delete,
            ),
        );
        let domain_metadata =
            cadence_persistence::matching_domain_metadata::CassandraDomainMetadataStore::new(
                driver,
                active_cluster_name,
            );
        let domain_persistence: Arc<
            dyn cadence_persistence::matching_domain_metadata::GoSqlDomainPersistence,
        > = Arc::new(
            cadence_persistence::matching_domain_metadata::CassandraFrontendDomainPersistence::new(
                domain_metadata.clone(),
            ),
        );
        return Ok(ConfiguredMatchingStores {
            domain_id_parse: crate::matching_domain_uuid::DomainIdParse::Lenient,
            native_task_store: task_store.clone(),
            task_store,
            domain_metadata_source: Some(Arc::new(domain_metadata)),
            domain_persistence: Some(domain_persistence),
        });
    }
    let sql = datastore.sql.as_ref().ok_or_else(|| {
        anyhow::anyhow!("Matching defaultStore {name:?} has no supported SQL configuration")
    })?;
    match sql.plugin_name.as_str() {
        "sqlite" => {
            if sql.use_multiple_databases || !sql.multiple_databases_config.is_empty() {
                anyhow::bail!("Matching SQLite task persistence does not support multipleDatabasesConfig");
            }
            let task_store =
                SqliteTaskStoreConfig::from_default_store(persistence)?.open_store()?;
            Ok(ConfiguredMatchingStores {
                domain_id_parse: crate::matching_domain_uuid::DomainIdParse::GoogleUuidPanic,
                native_task_store: task_store.clone(),
                task_store,
                domain_metadata_source: None,
                domain_persistence: None,
            })
        }
        "mysql" => {
            validate_sql_shards(sql)?;
            if !sql.use_multiple_databases && sql.database_name.is_empty() {
                anyhow::bail!("Matching MySQL SQL persistence databaseName cannot be empty");
            }
            if !sql.use_multiple_databases && sql.connect_addr.is_empty() {
                anyhow::bail!("Matching MySQL SQL persistence connectAddr cannot be empty");
            }
            let n_shards = if sql.n_shards == 0 { 1 } else { sql.n_shards };
            if n_shards < 1 {
                anyhow::bail!("Matching MySQL nShards must be positive, got {n_shards}");
            }
            // Go never reads the SQL `encodingType` for task blobs; rows are
            // encoded with dynamic `history.serializationEncoding` instead.
            let encoding = BlobEncoding::ThriftRW;
            let encoding_source = cadence_persistence::matching_blob::EncodingSource::dynamic(
                serialization_encoding.clone(),
            );
            let tls = sql.tls.as_ref().map(|tls| MySqlTlsConfig {
                enabled: tls.enabled,
                ssl_mode: tls.ssl_mode.clone(),
                ca_file: tls.ca_file.clone(),
                cert_file: tls.cert_file.clone(),
                key_file: tls.key_file.clone(),
                enable_host_verification: tls.enable_host_verification,
                require_client_auth: tls.require_client_auth,
            });
            let config = MySqlTaskStoreConfig {
                user: sql.user.clone(),
                password: sql.password.clone(),
                database_name: sql.database_name.clone(),
                connect_protocol: sql.connect_protocol.clone(),
                connect_addr: sql.connect_addr.clone(),
                connect_attributes: sql.connect_attributes.clone(),
                max_conns: sql.max_conns,
                max_idle_conns: sql.max_idle_conns,
                max_conn_lifetime: (sql.max_conn_lifetime_nanos > 0)
                    .then(|| Duration::from_nanos(sql.max_conn_lifetime_nanos as u64)),
                n_shards: n_shards as usize,
                use_multiple_databases: sql.use_multiple_databases,
                multiple_databases_config: sql.multiple_databases_config.iter().map(|entry| cadence_persistence::matching_mysql::MySqlShardConfig {
                    user: entry.user.clone(), password: entry.password.clone(), database_name: entry.database_name.clone(), connect_addr: entry.connect_addr.clone(),
                }).collect(),
                encoding,
                tls,
            };
            let (store, metadata) = MySqlTaskStore::connect_with_domain_metadata(
                config,
                active_cluster_name,
            )?;
            let store = Arc::new(store.with_encoding_source(encoding_source));
            let metadata_source: Arc<dyn crate::matching_domain_cache::DomainMetadataSource> = Arc::new(metadata.clone());
            let domain_persistence: Arc<dyn cadence_persistence::matching_domain_metadata::GoSqlDomainPersistence> = Arc::new(metadata);
            Ok(ConfiguredMatchingStores {
                domain_id_parse: crate::matching_domain_uuid::DomainIdParse::GoogleUuidPanic,
                native_task_store: store.clone(),
                task_store: store,
                domain_metadata_source: Some(metadata_source),
                domain_persistence: Some(domain_persistence),
            })
        }
        "postgres" => {
            validate_sql_shards(sql)?;
            if !sql.use_multiple_databases && sql.database_name.is_empty() {
                anyhow::bail!("Matching PostgreSQL SQL persistence databaseName cannot be empty");
            }
            if !sql.use_multiple_databases && sql.connect_addr.is_empty() {
                anyhow::bail!("Matching PostgreSQL SQL persistence connectAddr cannot be empty");
            }
            let n_shards = if sql.n_shards == 0 { 1 } else { sql.n_shards };
            if n_shards < 1 {
                anyhow::bail!("Matching PostgreSQL nShards must be positive, got {n_shards}");
            }
            // Go never reads the SQL `encodingType` for task blobs; rows are
            // encoded with dynamic `history.serializationEncoding` instead.
            let encoding = BlobEncoding::ThriftRW;
            let encoding_source = cadence_persistence::matching_blob::EncodingSource::dynamic(
                serialization_encoding.clone(),
            );
            let tls = sql.tls.as_ref().map(|tls| PostgresTlsConfig {
                enabled: tls.enabled,
                ssl_mode: tls.ssl_mode.clone(),
                ca_file: tls.ca_file.clone(),
                cert_file: tls.cert_file.clone(),
                key_file: tls.key_file.clone(),
            });
            let config = PostgresTaskStoreConfig {
                shards: if sql.use_multiple_databases { sql.multiple_databases_config.iter().map(|entry| PostgresShardConfig {
                    connect_addr: entry.connect_addr.clone(), user: entry.user.clone(), password: entry.password.clone(), database_name: entry.database_name.clone(), connect_attributes: sql.connect_attributes.clone(), tls: tls.clone(),
                }).collect() } else { vec![PostgresShardConfig {
                    connect_addr: sql.connect_addr.clone(),
                    user: sql.user.clone(),
                    password: sql.password.clone(),
                    database_name: sql.database_name.clone(),
                    connect_attributes: sql.connect_attributes.clone(),
                    tls,
                }] },
                n_shards: n_shards as usize,
                use_multiple_databases: sql.use_multiple_databases,
                max_conns: sql.max_conns,
                max_idle_conns: sql.max_idle_conns,
                max_conn_lifetime: (sql.max_conn_lifetime_nanos > 0)
                    .then(|| Duration::from_nanos(sql.max_conn_lifetime_nanos as u64)),
                encoding,
            };
            let (store, metadata) = PostgresTaskStore::connect_with_domain_metadata(
                config,
                active_cluster_name,
            )?;
            let store = Arc::new(store.with_encoding_source(encoding_source));
            let metadata_source: Arc<dyn crate::matching_domain_cache::DomainMetadataSource> = Arc::new(metadata.clone());
            let domain_persistence: Arc<dyn cadence_persistence::matching_domain_metadata::GoSqlDomainPersistence> = Arc::new(metadata);
            Ok(ConfiguredMatchingStores {
                domain_id_parse: crate::matching_domain_uuid::DomainIdParse::GoogleUuidPanic,
                native_task_store: store.clone(),
                task_store: store,
                domain_metadata_source: Some(metadata_source),
                domain_persistence: Some(domain_persistence),
            })
        }
        other => anyhow::bail!(
            "Matching task persistence does not support SQL plugin {other:?}; supported plugins are sqlite, mysql, and postgres"
        ),
    }
}

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
        // Go indexes persistence.datastores with the configured name exactly;
        // whitespace is part of the key and must not be normalized here.
        let name = persistence.default_store.as_str();
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

    fn nosql_from_yaml(body: &str) -> crate::config::NosqlConfig {
        let yaml = format!("persistence:\n  datastores:\n    c:\n      nosql:\n        pluginName: cassandra\n        hosts: \"h1,h2\"\n        keyspace: cadence\n{body}");
        crate::config::Config::from_yaml(&yaml)
            .unwrap()
            .persistence
            .datastores["c"]
            .nosql
            .clone()
            .unwrap()
    }

    #[test]
    fn cassandra_config_maps_go_options() {
        let nosql = nosql_from_yaml("        user: u\n        password: p\n        port: 9142\n        datacenter: dc1\n        consistency: quorum\n        serialConsistency: serial\n        maxConns: 5\n        timeout: 3s\n        connectTimeout: 1s\n        tls:\n          enabled: true\n          caFile: /ca.pem\n          caFiles: [/a.pem]\n          certFile: /c.pem\n          keyFile: /k.pem\n          enableHostVerification: true\n          serverName: cass.internal\n");
        let cfg = cassandra_config(&nosql).unwrap();
        assert_eq!(cfg.hosts, vec!["h1", "h2"]);
        assert_eq!(cfg.port, 9142);
        assert_eq!(cfg.datacenter, "dc1");
        assert_eq!(cfg.max_connections_per_host.unwrap().get(), 5);
        assert_eq!(cfg.request_timeout, std::time::Duration::from_secs(3));
        assert_eq!(cfg.connect_timeout, std::time::Duration::from_secs(1));
        let tls = cfg.tls.unwrap();
        assert_eq!(tls.ca_files.len(), 2);
        assert!(tls.verify_hostname);
        assert_eq!(tls.server_name.as_deref(), Some("cass.internal"));
    }

    #[test]
    fn cassandra_config_ignores_disabled_tls_and_rejects_unsupported_proto() {
        let nosql =
            nosql_from_yaml("        tls:\n          enabled: false\n          caFile: /ca.pem\n");
        assert!(cassandra_config(&nosql).unwrap().tls.is_none());
        let nosql = nosql_from_yaml("        protoVersion: 3\n");
        let error = cassandra_config(&nosql).unwrap_err().to_string();
        assert!(error.contains("protoVersion 3"), "{error}");
    }

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
    fn configured_sqlite_exposes_native_and_legacy_views_of_one_store() {
        use cadence_common::go_string::GoString;
        use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
        use cadence_persistence::matching_task_store::TaskListKey;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");
        drop(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let config = persistence(Some(SqlConfig {
            plugin_name: "sqlite".into(),
            database_name: path.to_str().unwrap().into(),
            ..Default::default()
        }));
        let stores = open_configured_default_store(
            &config,
            "active",
            Arc::new(|| false),
            Arc::new(|| "thriftrw".into()),
        )
        .unwrap();
        let legacy_key = TaskListKey {
            domain_id: uuid::Uuid::from_u128(1),
            name: "shared".into(),
            task_type: 0,
        };
        let lease = stores.task_store.lease(&legacy_key, 0, 0, 1).unwrap();
        let key = TaskListKeyBytes::from(legacy_key.clone());
        assert_eq!(
            stores
                .native_task_store
                .get_task_list(&key)
                .unwrap()
                .range_id,
            lease.range_id
        );
        let renewed = stores
            .native_task_store
            .lease(&key, 0, lease.range_id, 2)
            .unwrap();
        assert_eq!(
            stores
                .task_store
                .get_task_list(&legacy_key)
                .unwrap()
                .range_id,
            renewed.range_id
        );
        let raw_key = TaskListKeyBytes {
            name: GoString::from(&b"q\xff"[..]),
            ..key
        };
        stores.native_task_store.lease(&raw_key, 0, 0, 3).unwrap();
        assert_eq!(
            stores
                .native_task_store
                .get_task_list(&raw_key)
                .unwrap()
                .range_id,
            1
        );
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
    fn configured_sql_shards_validate_go_endpoint_count_and_endpoint_fields() {
        let mut sql = SqlConfig {
            plugin_name: "mysql".into(),
            use_multiple_databases: true,
            n_shards: 2,
            multiple_databases_config: vec![
                crate::config::MultipleDatabasesConfigEntry {
                    database_name: "cadence0".into(),
                    connect_addr: "db0:3306".into(),
                    ..Default::default()
                },
                crate::config::MultipleDatabasesConfigEntry {
                    database_name: "cadence1".into(),
                    connect_addr: "db1:3306".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(validate_sql_shards(&sql).is_ok());
        sql.multiple_databases_config.pop();
        assert!(validate_sql_shards(&sql)
            .unwrap_err()
            .to_string()
            .contains("equal to the length"));
        sql.multiple_databases_config
            .push(crate::config::MultipleDatabasesConfigEntry::default());
        assert!(validate_sql_shards(&sql)
            .unwrap_err()
            .to_string()
            .contains("require databaseName and connectAddr"));
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

        let padded_name = crate::config::PersistenceConfig {
            default_store: " primary ".into(),
            ..persistence(Some(SqlConfig {
                plugin_name: "sqlite".into(),
                database_name: "cadence.sqlite".into(),
                ..Default::default()
            }))
        };
        assert!(SqliteTaskStoreConfig::from_default_store(&padded_name)
            .unwrap_err()
            .to_string()
            .contains("defaultStore \" primary \" is not present"));

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
