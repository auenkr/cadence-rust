//! Builds the durable visibility manager from `persistence.visibilityStore`.

use cadence_persistence::{
    matching_mysql::{MySqlShardConfig, MySqlTaskStoreConfig, MySqlTlsConfig},
    matching_postgres::{PostgresShardConfig, PostgresTlsConfig},
    visibility_store::{CassandraVisibilityStore, SqlVisibilityStore, VisibilityManager},
};
use std::{sync::Arc, time::Duration};

/// Returns Ok(None) when no basic (non-Elasticsearch) visibility store is
/// configured, leaving the in-memory fallback in place.
pub fn open_configured_visibility_store(
    persistence: &crate::config::PersistenceConfig,
    sort_closed_by_close_time: bool,
) -> anyhow::Result<Option<Arc<dyn VisibilityManager>>> {
    if persistence.visibility_store.is_empty() {
        return Ok(None);
    }
    let datastore = persistence
        .datastore_for(crate::config::PersistenceStoreRole::Visibility)
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow::anyhow!("visibilityStore has no datastore"))?;
    if let Some(nosql) = datastore.nosql.as_ref() {
        if nosql.plugin_name != "cassandra" {
            anyhow::bail!(
                "visibility NoSQL plugin {:?} is not supported; supported plugin is cassandra",
                nosql.plugin_name
            );
        }
        let config = crate::matching_task_store::cassandra_config(nosql)?;
        let driver = Arc::new(cadence_persistence::cassandra::CassandraBlockingSession::connect(
            &config,
        )?);
        return Ok(Some(Arc::new(CassandraVisibilityStore::new(
            driver,
            sort_closed_by_close_time,
        ))));
    }
    let Some(sql) = datastore.sql.as_ref() else {
        return Ok(None);
    };
    let lifetime = (sql.max_conn_lifetime_nanos > 0)
        .then(|| Duration::from_nanos(sql.max_conn_lifetime_nanos as u64));
    let store: Arc<dyn VisibilityManager> = match sql.plugin_name.as_str() {
        "sqlite" => Arc::new(SqlVisibilityStore::sqlite(&sql.database_name)?),
        "mysql" => Arc::new(SqlVisibilityStore::mysql(&MySqlTaskStoreConfig {
            user: sql.user.clone(),
            password: sql.password.clone(),
            database_name: sql.database_name.clone(),
            connect_protocol: sql.connect_protocol.clone(),
            connect_addr: sql.connect_addr.clone(),
            connect_attributes: sql.connect_attributes.clone(),
            max_conns: sql.max_conns,
            max_idle_conns: sql.max_idle_conns,
            max_conn_lifetime: lifetime,
            n_shards: 1,
            use_multiple_databases: sql.use_multiple_databases,
            multiple_databases_config: sql
                .multiple_databases_config
                .iter()
                .map(|e| MySqlShardConfig {
                    user: e.user.clone(),
                    password: e.password.clone(),
                    database_name: e.database_name.clone(),
                    connect_addr: e.connect_addr.clone(),
                })
                .collect(),
            encoding: cadence_persistence::matching_blob::BlobEncoding::ThriftRW,
            tls: sql.tls.as_ref().map(|tls| MySqlTlsConfig {
                enabled: tls.enabled,
                ssl_mode: tls.ssl_mode.clone(),
                ca_file: tls.ca_file.clone(),
                cert_file: tls.cert_file.clone(),
                key_file: tls.key_file.clone(),
                enable_host_verification: tls.enable_host_verification,
                require_client_auth: tls.require_client_auth,
            }),
        })?),
        "postgres" => Arc::new(SqlVisibilityStore::postgres(
            &PostgresShardConfig {
                connect_addr: sql.connect_addr.clone(),
                user: sql.user.clone(),
                password: sql.password.clone(),
                database_name: sql.database_name.clone(),
                connect_attributes: sql.connect_attributes.clone(),
                tls: sql.tls.as_ref().map(|tls| PostgresTlsConfig {
                    enabled: tls.enabled,
                    ssl_mode: tls.ssl_mode.clone(),
                    ca_file: tls.ca_file.clone(),
                    cert_file: tls.cert_file.clone(),
                    key_file: tls.key_file.clone(),
                }),
            },
            sql.max_conns,
            sql.max_idle_conns,
            lifetime,
        )?),
        other => anyhow::bail!("visibility SQL plugin {other:?} is not supported"),
    };
    Ok(Some(store))
}
