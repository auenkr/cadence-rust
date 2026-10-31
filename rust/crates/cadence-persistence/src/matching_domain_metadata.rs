//! Read-only view of Go SQL domain metadata for Matching.
//!
//! Cadence's Go `sqlDomainStore` puts a `sqlblobs.DomainInfo` ThriftRW payload
//! in `domains.data`; the table columns alone do not contain Matching's domain
//! configuration. This reader keeps that persistence contract and never
//! writes or migrates the database.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

use cadence_api::enums::{ArchivalStatus, DomainStatus};
use cadence_api::types::{
    ActiveClusters, AsyncWorkflowConfiguration, BadBinaries, DomainConfiguration, DomainInfo,
    DomainReplicationConfiguration, IsolationGroupConfiguration,
};
use cadence_thrift::protocol::{Reader, TType};
use cadence_thrift::value::{read_struct, read_value, Struct, Value};
use cadence_thrift::FromThrift;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use thiserror::Error;

const GO_DEFAULT_SHARD_ID: i64 = 54321;
const ENCODING_THRIFT_RW: &str = "thriftrw";
const ENCODING_THRIFT_RW_SNAPPY: &str = "thriftrw_snappy";
const GO_ZERO_TIME_NANOS: i64 = -6_795_364_578_871_345_152;

/// A nested Go persistence `DataBlob`. The exact bytes and encoding survive
/// this read boundary so the Matching cache adapter can decode them later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoDomainBlob {
    pub data: Vec<u8>,
    pub encoding: String,
}

/// Domain state read from the Go SQLite schema with fields needed by Matching.
/// `notification_version` and nested blobs are intentionally outside the
/// existing `DomainRecord`, which predates durable domain metadata reads.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchingDomainMetadata {
    pub info: DomainInfo,
    pub configuration: DomainConfiguration,
    pub replication: DomainReplicationConfiguration,
    pub is_global_domain: bool,
    pub archival_bucket: String,
    pub status_raw: i32,
    pub archival_status_raw: i16,
    pub history_archival_status_raw: i16,
    pub visibility_archival_status_raw: i16,
    pub config_version: i64,
    pub notification_version: i64,
    pub failover_notification_version: i64,
    pub failover_version: i64,
    pub previous_failover_version: i64,
    /// Go's `zeroTimeNanos` sentinel is normalized to `None`.
    pub failover_end_time_nanos: Option<i64>,
    pub last_updated_time_nanos: i64,
    pub bad_binaries: Option<GoDomainBlob>,
    pub isolation_groups: Option<GoDomainBlob>,
    pub async_workflow_config: Option<GoDomainBlob>,
    pub active_clusters_config: Option<GoDomainBlob>,
    pub decoded_bad_binaries: Option<BadBinaries>,
    pub decoded_isolation_groups: Option<IsolationGroupConfiguration>,
    pub decoded_async_workflow_config: Option<AsyncWorkflowConfiguration>,
    /// Deferred typed-conversion failure. The raw SQL store returns opaque
    /// blobs unchanged; Matching cache conversion reports this like Go's
    /// MetadataManager when it materializes DomainConfig/ReplicationConfig.
    pub config_decode_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum MatchingDomainMetadataError {
    #[error("GetDomain operation failed.  Both ID and Name specified in request.")]
    BothIdAndName,
    #[error("GetDomain operation failed.  Both ID and Name are empty.")]
    EmptyIdentity,
    #[error("invalid domain UUID: {0}")]
    InvalidUuid(String),
    #[error("Domain {0} does not exist.")]
    NotFound(String),
    #[error("invalid Go domain metadata: {0}")]
    InvalidBlob(String),
    #[error("unsupported Go domain blob encoding: {0}")]
    UnsupportedEncoding(String),
    #[error("Go domain metadata database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Go MySQL domain metadata database error: {0}")]
    MySql(#[from] mysql::Error),
    #[error("Go MySQL async domain metadata database error: {0}")]
    MySqlAsync(#[from] mysql_async::Error),
    #[error("Go MySQL domain metadata connection pool error: {0}")]
    MySqlPool(String),
    #[error("Go PostgreSQL domain metadata database error: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("Go Cassandra domain metadata error: {0}")]
    Cassandra(String),
    #[error("domain page size must be positive")]
    InvalidPageSize,
    #[error("domain metadata persistence operation timed out")]
    OperationTimedOut,
    #[error("invalid SQLite domain metadata reader configuration: {0}")]
    InvalidSqliteConfiguration(String),
    #[error("invalid MySQL domain metadata reader configuration: {0}")]
    InvalidMySqlConfiguration(String),
    #[error("invalid PostgreSQL domain metadata reader configuration: {0}")]
    InvalidPostgresConfiguration(String),
}

/// Read-only access to the Go `domains` and `domain_metadata` tables.
///
/// Go's SQL backend stores UUIDs as 16-byte BLOBs, uses shard 54321 for domain
/// rows, and pages by `id > token ORDER BY id LIMIT page_size`.
pub struct MatchingDomainMetadataStore {
    connection: Mutex<Connection>,
    active_cluster_name: String,
    busy_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchingDomainPage {
    pub domains: Vec<MatchingDomainMetadata>,
    /// Go returns the last row's raw UUID whenever rows.len() >= page_size.
    pub next_page_token: Option<Vec<u8>>,
}

/// Go SQL domain and singleton metadata persistence operations. Matching's
/// cache and Frontend's domain manager share the same Go schema; keeping this
/// contract beside the wire-compatible metadata type lets both services use
/// one backend implementation without inventing another table layout.
pub trait GoSqlDomainPersistence: Send + Sync {
    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError>;

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError>;

    fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError>;

    fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError>;

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError>;

    fn list_domains(
        &self,
        page_size: usize,
        page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError>;
}

/// Convert Cassandra's nested domain/config/replication UDTs through the same
/// Go DomainInfo field interpretation used by SQL domain metadata.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_go_cassandra_domain_metadata(
    info: crate::cassandra::CassandraDomainUdt,
    config: crate::cassandra::CassandraDomainConfigUdt,
    replication: crate::cassandra::CassandraDomainReplicationConfigUdt,
    is_global: bool,
    config_version: i64,
    failover_version: i64,
    failover_notification_version: i64,
    previous_failover_version: i64,
    failover_end_time: i64,
    last_updated_time: i64,
    notification_version: i64,
    current_cluster: &str,
) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
    use cadence_thrift::value::{Struct, Value};
    let mut thrift = Struct::default();
    let mut put = |id, value| thrift.fields.push((id, value));
    let text = |value: String| Value::Bytes(value.into_bytes());
    put(10, text(info.name.clone()));
    put(12, text(info.description.unwrap_or_default()));
    put(14, text(info.owner_email.unwrap_or_default()));
    put(16, Value::I32(info.status));
    if let Some(data) = info.data {
        put(
            38,
            Value::Map(
                data.into_iter()
                    .map(|(key, value)| (text(key), text(value)))
                    .collect(),
            ),
        );
    }
    put(18, Value::I16(config.retention as i16));
    put(20, Value::Bool(config.emit_metric));
    put(22, text(config.archival_bucket.unwrap_or_default()));
    if let Some(status) = config.archival_status {
        put(24, Value::I16(status as i16));
    }
    if let Some(status) = config.history_archival_status {
        put(42, Value::I16(status as i16));
    }
    put(44, text(config.history_archival_uri.unwrap_or_default()));
    if let Some(status) = config.visibility_archival_status {
        put(46, Value::I16(status as i16));
    }
    put(48, text(config.visibility_archival_uri.unwrap_or_default()));
    if let Some(data) = config.bad_binaries {
        put(39, Value::Bytes(data));
    }
    if let Some(encoding) = config.bad_binaries_encoding {
        put(40, text(String::from_utf8_lossy(&encoding).into_owned()));
    }
    if let Some(data) = config.isolation_groups {
        put(56, Value::Bytes(data));
    }
    if let Some(encoding) = config.isolation_groups_encoding {
        put(58, text(encoding));
    }
    if let Some(data) = config.async_workflow_config {
        put(60, Value::Bytes(data));
    }
    if let Some(encoding) = config.async_workflow_config_encoding {
        put(62, text(encoding));
    }
    if let Some(active) = replication.active_cluster_name {
        put(34, text(active));
    }
    if let Some(clusters) = replication.clusters {
        put(
            36,
            Value::List(
                clusters
                    .into_iter()
                    .filter_map(|cluster| cluster.cluster_name)
                    .map(text)
                    .collect(),
            ),
        );
    }
    if let Some(data) = replication.active_clusters_config {
        put(64, Value::Bytes(data));
    }
    if let Some(encoding) = replication.active_clusters_config_encoding {
        put(66, text(encoding));
    }
    put(26, Value::I64(config_version));
    put(28, Value::I64(notification_version));
    put(30, Value::I64(failover_notification_version));
    put(32, Value::I64(failover_version));
    put(50, Value::I64(failover_end_time));
    put(52, Value::I64(previous_failover_version));
    put(54, Value::I64(last_updated_time));
    let uuid = info.id.to_string();
    let row = SqlDomainRow {
        id: info.id.as_bytes().to_vec(),
        name: info.name,
        is_global,
        data: Vec::new(),
        data_encoding: String::new(),
    };
    let mut domain = thrift_to_metadata(thrift, uuid, row, current_cluster)?;
    domain.is_global_domain = is_global;
    Ok(domain)
}

/// Read-only Matching view over Go's Cassandra `domains_by_name_v2` tables.
#[derive(Clone)]
pub struct CassandraDomainMetadataStore {
    driver: std::sync::Arc<crate::cassandra::CassandraBlockingSession>,
    current_cluster: String,
}

/// Read/write Frontend domain persistence for Go's Cassandra schema. The Go
/// plugin writes the UUID lookup and name-index rows separately, with a
/// conditional batch that advances the shared notification version. This
/// adapter retains those row shapes and compare-and-set boundaries.
#[derive(Clone)]
pub struct CassandraFrontendDomainPersistence {
    metadata: CassandraDomainMetadataStore,
}

impl CassandraFrontendDomainPersistence {
    #[must_use]
    pub fn new(metadata: CassandraDomainMetadataStore) -> Self {
        Self { metadata }
    }

    fn cql_error(error: impl std::fmt::Display) -> MatchingDomainMetadataError {
        MatchingDomainMetadataError::Cassandra(error.to_string())
    }

    fn domain_udts(
        metadata: &MatchingDomainMetadata,
    ) -> (
        crate::cassandra::CassandraDomainUdt,
        crate::cassandra::CassandraDomainConfigUdt,
        crate::cassandra::CassandraDomainReplicationConfigUdt,
    ) {
        use crate::cassandra::{
            CassandraClusterReplicationConfigUdt, CassandraDomainConfigUdt,
            CassandraDomainReplicationConfigUdt, CassandraDomainUdt,
        };
        let info = CassandraDomainUdt {
            id: uuid::Uuid::parse_str(&metadata.info.uuid).unwrap_or_default(),
            name: metadata.info.name.clone(),
            status: metadata.status_raw,
            description: Some(metadata.info.description.clone()),
            data: Some(metadata.info.data.clone()),
            owner_email: Some(metadata.info.owner_email.clone()),
        };
        let config = CassandraDomainConfigUdt {
            retention: metadata.configuration.workflow_execution_retention_period_in_days,
            emit_metric: metadata.configuration.emit_metric,
            archival_bucket: Some(metadata.archival_bucket.clone()),
            archival_status: Some(i32::from(metadata.archival_status_raw)),
            history_archival_status: Some(i32::from(metadata.history_archival_status_raw)),
            history_archival_uri: Some(metadata.configuration.history_archival_uri.clone()),
            visibility_archival_status: Some(i32::from(
                metadata.visibility_archival_status_raw,
            )),
            visibility_archival_uri: Some(
                metadata.configuration.visibility_archival_uri.clone(),
            ),
            bad_binaries: metadata.bad_binaries.as_ref().map(|blob| blob.data.clone()),
            bad_binaries_encoding: metadata
                .bad_binaries
                .as_ref()
                .map(|blob| blob.encoding.as_bytes().to_vec()),
            isolation_groups: metadata
                .isolation_groups
                .as_ref()
                .map(|blob| blob.data.clone()),
            isolation_groups_encoding: metadata
                .isolation_groups
                .as_ref()
                .map(|blob| blob.encoding.clone()),
            async_workflow_config: metadata
                .async_workflow_config
                .as_ref()
                .map(|blob| blob.data.clone()),
            async_workflow_config_encoding: metadata
                .async_workflow_config
                .as_ref()
                .map(|blob| blob.encoding.clone()),
        };
        let replication = CassandraDomainReplicationConfigUdt {
            active_cluster_name: Some(metadata.replication.active_cluster_name.clone()),
            clusters: Some(
                metadata
                    .replication
                    .clusters
                    .iter()
                    .map(|cluster| CassandraClusterReplicationConfigUdt {
                        cluster_name: Some(cluster.cluster_name.clone()),
                    })
                    .collect(),
            ),
            active_clusters_config: metadata
                .active_clusters_config
                .as_ref()
                .map(|blob| blob.data.clone()),
            active_clusters_config_encoding: metadata
                .active_clusters_config
                .as_ref()
                .map(|blob| blob.encoding.clone()),
        };
        (info, config, replication)
    }

    fn applied(result: scylla::response::query_result::QueryResult) -> Result<bool, MatchingDomainMetadataError> {
        let rows = result
            .into_rows_result()
            .map_err(Self::cql_error)?;
        let mut columns = rows
            .rows::<(bool,)>()
            .map_err(Self::cql_error)?;
        columns
            .next()
            .transpose()
            .map_err(Self::cql_error)?
            .map(|(applied,)| applied)
            .ok_or_else(|| Self::cql_error("Cassandra LWT response did not include [applied]"))
    }

    fn executor(&self) -> crate::matching_cassandra_exec::ScyllaExecutor {
        crate::matching_cassandra_exec::ScyllaExecutor::new(self.metadata.driver.clone())
    }
}

impl GoSqlDomainPersistence for CassandraFrontendDomainPersistence {
    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError> {
        use crate::matching_cassandra_exec::{bound, CqlExecutor, CqlStatement};
        use chrono::Utc;
        let id = uuid::Uuid::parse_str(&metadata.info.uuid)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let driver = &self.metadata.driver;
        let session = driver.session();
        let id_applied = driver
            .run(session.query_unpaged(
                "INSERT INTO domains (id, domain, created_time) VALUES (?, {name: ?}, ?) IF NOT EXISTS",
                (id, metadata.info.name.as_str(), Utc::now()),
            ))
            .map_err(Self::cql_error)?;
        if !Self::applied(id_applied)? {
            return Err(MatchingDomainMetadataError::InvalidBlob(format!(
                "CreateDomain operation failed because of UUID collision: {id}"
            )));
        }

        let notification_version = self.metadata.metadata_version()?;
        let (info, config, replication) = Self::domain_udts(metadata);
        let (history_status, visibility_status) = (
            i32::from(metadata.history_archival_status_raw),
            i32::from(metadata.visibility_archival_status_raw),
        );
        let name = metadata.info.name.clone();
        let insert = CqlStatement::new(
            "INSERT INTO domains_by_name_v2 (domains_partition, name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
        );
        let update_metadata = CqlStatement::new(
            "UPDATE domains_by_name_v2 SET notification_version = ? WHERE domains_partition = ? AND name = ? IF notification_version = ?",
        );
        let end_time = metadata.failover_end_time_nanos.unwrap_or_default();
        let executor = self.executor();
        let outcome = executor
            .batch_lwt(
                &[insert, update_metadata],
                vec![
                    vec![
                        bound(0_i32),
                        bound(name.clone()),
                        bound(info),
                        bound(config),
                        bound(replication),
                        bound(metadata.is_global_domain),
                        bound(metadata.config_version),
                        bound(metadata.failover_version),
                        bound(metadata.failover_notification_version),
                        bound(metadata.previous_failover_version),
                        bound(end_time),
                        bound(metadata.last_updated_time_nanos),
                        bound(notification_version),
                        bound(Utc::now()),
                    ],
                    vec![
                        bound(if notification_version > 0 {
                            notification_version + 1
                        } else {
                            1
                        }),
                        bound(0_i32),
                        bound("cadence-domain-metadata".to_string()),
                        bound((notification_version > 0).then_some(notification_version)),
                    ],
                ],
            )
            .map_err(Self::cql_error)?;
        if !outcome.applied {
            let _ = executor.exec(
                &CqlStatement::new("DELETE FROM domains WHERE id = ?"),
                vec![bound(id)],
            );
            return Err(MatchingDomainMetadataError::InvalidBlob(format!(
                "Domain {} already exists or domain metadata changed",
                metadata.info.name
            )));
        }
        Ok(notification_version)
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        use crate::matching_cassandra_exec::{bound, CqlExecutor, CqlStatement};
        let (info, config, replication) = Self::domain_udts(metadata);
        let executor = self.executor();
        let outcome = executor
            .batch_lwt(
                &[
                    CqlStatement::new(
                        "UPDATE domains_by_name_v2 SET domain = ?, config = ?, replication_config = ?, is_global_domain = ?, config_version = ?, failover_version = ?, failover_notification_version = ?, previous_failover_version = ?, failover_end_time = ?, last_updated_time = ?, notification_version = ? WHERE domains_partition = ? AND name = ?",
                    ),
                    CqlStatement::new(
                        "UPDATE domains_by_name_v2 SET notification_version = ? WHERE domains_partition = ? AND name = ? IF notification_version = ?",
                    ),
                ],
                vec![
                    vec![
                        bound(info),
                        bound(config),
                        bound(replication),
                        bound(metadata.is_global_domain),
                        bound(metadata.config_version),
                        bound(metadata.failover_version),
                        bound(metadata.failover_notification_version),
                        bound(metadata.previous_failover_version),
                        bound(metadata.failover_end_time_nanos.unwrap_or_default()),
                        bound(metadata.last_updated_time_nanos),
                        bound(expected_notification_version),
                        bound(0_i32),
                        bound(metadata.info.name.clone()),
                    ],
                    vec![
                        bound(if expected_notification_version > 0 {
                            expected_notification_version + 1
                        } else {
                            1
                        }),
                        bound(0_i32),
                        bound("cadence-domain-metadata".to_string()),
                        bound((expected_notification_version > 0)
                            .then_some(expected_notification_version)),
                    ],
                ],
            )
            .map_err(Self::cql_error)?;
        if outcome.applied {
            Ok(())
        } else {
            Err(MatchingDomainMetadataError::InvalidBlob(format!(
                "domain metadata notification version changed: expected {expected_notification_version}"
            )))
        }
    }

    fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError> {
        use crate::matching_cassandra_exec::{bound, CqlExecutor, CqlStatement};
        let metadata = self.metadata.get_domain(None, Some(domain_id))?;
        let id = uuid::Uuid::parse_str(domain_id)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let executor = self.executor();
        executor
            .exec(
                &CqlStatement::new("DELETE FROM domains WHERE id = ?"),
                vec![bound(id)],
            )
            .map_err(Self::cql_error)?;
        executor
            .exec(
                &CqlStatement::new(
                    "DELETE FROM domains_by_name_v2 WHERE domains_partition = ? AND name = ?",
                ),
                vec![bound(0_i32), bound(metadata.info.name)],
            )
            .map_err(Self::cql_error)
    }

    fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        self.metadata.metadata_version()
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        self.metadata.get_domain(name, id)
    }

    fn list_domains(
        &self,
        page_size: usize,
        page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        self.metadata.list_domains(page_size, page_token)
    }
}

impl CassandraDomainMetadataStore {
    pub fn new(
        driver: std::sync::Arc<crate::cassandra::CassandraBlockingSession>,
        current_cluster: impl Into<String>,
    ) -> Self {
        Self {
            driver,
            current_cluster: current_cluster.into(),
        }
    }

    fn get_by_name(
        &self,
        name: &str,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        use crate::cassandra::{
            CassandraDomainConfigUdt, CassandraDomainReplicationConfigUdt, CassandraNullableDomainUdt,
        };
        let session = self.driver.session();
        let result = self.driver.run(session.query_unpaged(
            "SELECT domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version FROM domains_by_name_v2 WHERE domains_partition=? AND name=?",
            (0_i32, name),
        )).map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        let rows = result
            .into_rows_result()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        let row = rows
            .maybe_first_row::<(
                CassandraNullableDomainUdt,
                Option<CassandraDomainConfigUdt>,
                Option<CassandraDomainReplicationConfigUdt>,
                Option<bool>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
            )>()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?
            .ok_or_else(|| MatchingDomainMetadataError::NotFound(name.to_string()))?;
        let domain = row
            .0
            .into_domain()
            .ok_or_else(|| MatchingDomainMetadataError::NotFound(name.to_string()))?;
        // gocql reads NULL columns as zero values; the seeded cadence-system row has several.
        decode_go_cassandra_domain_metadata(
            domain,
            row.1.unwrap_or_default(),
            row.2.unwrap_or_default(),
            row.3.unwrap_or_default(),
            row.4.unwrap_or_default(),
            row.5.unwrap_or_default(),
            row.6.unwrap_or_default(),
            row.7.unwrap_or_default(),
            row.8.unwrap_or_default(),
            row.9.unwrap_or_default(),
            row.10.unwrap_or_default(),
            &self.current_cluster,
        )
    }

    pub fn metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        let session = self.driver.session();
        let result = self.driver.run(session.query_unpaged(
            "SELECT notification_version FROM domains_by_name_v2 WHERE domains_partition=? AND name=?",
            (0_i32, "cadence-domain-metadata"),
        )).map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        let rows = result
            .into_rows_result()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        Ok(rows
            .maybe_first_row::<(i64,)>()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?
            .map(|(version,)| version)
            .unwrap_or(0))
    }

    pub fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        match (name, id) {
            (Some(_), Some(_)) => Err(MatchingDomainMetadataError::BothIdAndName),
            (None, None) => Err(MatchingDomainMetadataError::EmptyIdentity),
            (Some(""), None) => Err(MatchingDomainMetadataError::EmptyIdentity),
            (Some(name), None) => self.get_by_name(name),
            (None, Some("")) => Err(MatchingDomainMetadataError::EmptyIdentity),
            (None, Some(id)) => {
                let id = uuid::Uuid::parse_str(id)
                    .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
                let session = self.driver.session();
                let result = self
                    .driver
                    .run(session.query_unpaged("SELECT domain FROM domains WHERE id=?", (id,)))
                    .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
                let rows = result
                    .into_rows_result()
                    .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
                let row = rows
                    .maybe_first_row::<(crate::cassandra::CassandraDomainUdt,)>()
                    .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?
                    .ok_or_else(|| MatchingDomainMetadataError::NotFound(id.to_string()))?;
                self.get_by_name(&row.0.name)
            }
        }
    }

    pub fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        use crate::cassandra::{
            CassandraDomainConfigUdt, CassandraDomainReplicationConfigUdt, CassandraNullableDomainUdt,
        };
        use scylla::{response::PagingState, statement::Statement};
        if page_size == 0 {
            return Err(MatchingDomainMetadataError::InvalidPageSize);
        }
        let page_size =
            i32::try_from(page_size).map_err(|_| MatchingDomainMetadataError::InvalidPageSize)?;
        let session = self.driver.session();
        let mut query = Statement::new("SELECT name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version FROM domains_by_name_v2 WHERE domains_partition=?");
        query.set_page_size(page_size);
        let paging_state = next_page_token
            .map(|token| PagingState::new_from_raw_bytes(token.to_vec()))
            .unwrap_or_else(PagingState::start);
        let (result, paging_response) = self
            .driver
            .run(session.query_single_page(query, (0_i32,), paging_state))
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        let rows = result
            .into_rows_result()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
        let mut domains = Vec::new();
        for row in rows
            .rows::<(
                String,
                Option<CassandraNullableDomainUdt>,
                Option<CassandraDomainConfigUdt>,
                Option<CassandraDomainReplicationConfigUdt>,
                Option<bool>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
            )>()
            .map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?
        {
            let (
                name,
                domain,
                config,
                replication,
                global,
                config_v,
                failover_v,
                failover_notify_v,
                previous_v,
                end,
                updated,
                notify,
            ) = row.map_err(|error| MatchingDomainMetadataError::Cassandra(error.to_string()))?;
            // The metadata record has a NULL domain UDT; Go skips it before decoding.
            let Some(domain) = domain
                .filter(|_| name != "cadence-domain-metadata")
                .and_then(CassandraNullableDomainUdt::into_domain)
            else {
                continue;
            };
            domains.push(decode_go_cassandra_domain_metadata(
                domain,
                config.unwrap_or_default(),
                replication.unwrap_or_default(),
                global.unwrap_or_default(),
                config_v.unwrap_or_default(),
                failover_v.unwrap_or_default(),
                failover_notify_v.unwrap_or_default(),
                previous_v.unwrap_or_default(),
                end.unwrap_or_default(),
                updated.unwrap_or_default(),
                notify.unwrap_or_default(),
                &self.current_cluster,
            )?);
        }
        let next_page_token = match paging_response.into_paging_control_flow() {
            std::ops::ControlFlow::Continue(state) => {
                state.as_bytes_slice().map(|bytes| bytes.to_vec())
            }
            std::ops::ControlFlow::Break(()) => None,
        };
        Ok(MatchingDomainPage {
            domains,
            next_page_token,
        })
    }
}

/// Bounds both SQLite lock waiting and virtual-machine execution for one
/// persistence call. Go supplies a fresh three-second context to each call.
struct SqliteOperationDeadline<'a>(&'a Connection);

fn sqlite_operation_error(
    error: rusqlite::Error,
    deadline: Instant,
) -> MatchingDomainMetadataError {
    let expired_sql_lock = matches!(
        &error,
        rusqlite::Error::SqliteFailure(failure, _)
            if Instant::now() >= deadline
                && matches!(
                    failure.code,
                    rusqlite::ErrorCode::DatabaseBusy
                        | rusqlite::ErrorCode::DatabaseLocked
                        | rusqlite::ErrorCode::OperationInterrupted
                )
    );
    if expired_sql_lock {
        MatchingDomainMetadataError::OperationTimedOut
    } else {
        MatchingDomainMetadataError::Sqlite(error)
    }
}

impl<'a> SqliteOperationDeadline<'a> {
    fn new(
        connection: &'a Connection,
        deadline: Instant,
        configured_busy_timeout: Duration,
    ) -> Result<Self, rusqlite::Error> {
        connection.busy_timeout(
            deadline
                .saturating_duration_since(Instant::now())
                .min(configured_busy_timeout),
        )?;
        connection.progress_handler(1_000, Some(move || Instant::now() >= deadline));
        Ok(Self(connection))
    }
}

impl Drop for SqliteOperationDeadline<'_> {
    fn drop(&mut self) {
        self.0.progress_handler(0, None::<fn() -> bool>);
    }
}

impl MatchingDomainMetadataStore {
    fn lock_connection(
        &self,
        timeout: Duration,
    ) -> Result<(MutexGuard<'_, Connection>, Instant), MatchingDomainMetadataError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.connection.try_lock() {
                Ok(connection) => {
                    if Instant::now() >= deadline {
                        return Err(MatchingDomainMetadataError::OperationTimedOut);
                    }
                    return Ok((connection, deadline));
                }
                Err(TryLockError::Poisoned(error)) => {
                    if Instant::now() >= deadline {
                        return Err(MatchingDomainMetadataError::OperationTimedOut);
                    }
                    return Ok((error.into_inner(), deadline));
                }
                Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                    return Err(MatchingDomainMetadataError::OperationTimedOut);
                }
                Err(TryLockError::WouldBlock) => {
                    std::thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(1)),
                    );
                }
            }
        }
    }

    /// Open an existing Cadence SQLite file without write access.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        active_cluster_name: impl Into<String>,
    ) -> Result<Self, MatchingDomainMetadataError> {
        Self::open_read_only_with_options(path, active_cluster_name, "ro", Duration::from_secs(60))
    }

    /// Open a Go-configured SQLite database while preserving URI mode and
    /// busy-timeout behavior. The reader always remains read-only; `rw` and
    /// `rwc` match Go's existence/create modes only insofar as this reader
    /// requires an already-existing database and never creates one.
    pub fn open_read_only_with_options(
        path: impl AsRef<Path>,
        active_cluster_name: impl Into<String>,
        mode: &str,
        busy_timeout: Duration,
    ) -> Result<Self, MatchingDomainMetadataError> {
        match mode {
            "ro" | "rw" | "rwc" => {}
            "memory" => {
                return Err(MatchingDomainMetadataError::InvalidSqliteConfiguration(
                    "mode=memory is not supported by the Matching domain reader".into(),
                ));
            }
            _ => {
                return Err(MatchingDomainMetadataError::InvalidSqliteConfiguration(
                    format!("unsupported URI mode {mode:?}"),
                ));
            }
        };
        let path = path.as_ref().to_string_lossy();
        // Go config's `rw` and `rwc` describe the read/write pool. This
        // adapter is deliberately read-only, while preserving the same
        // existing-file lookup behavior for all three file-backed modes.
        let uri = format!("file:{path}?mode=ro");
        let connection = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        Ok(Self::from_connection_with_busy_timeout(
            connection,
            active_cluster_name,
            busy_timeout,
        ))
    }

    /// Wrap an existing read-only connection. Primarily useful to callers that
    /// manage connection lifecycle outside this metadata reader.
    pub fn from_connection(connection: Connection, active_cluster_name: impl Into<String>) -> Self {
        Self::from_connection_with_busy_timeout(
            connection,
            active_cluster_name,
            Duration::from_secs(60),
        )
    }

    fn from_connection_with_busy_timeout(
        connection: Connection,
        active_cluster_name: impl Into<String>,
        busy_timeout: Duration,
    ) -> Self {
        Self {
            connection: Mutex::new(connection),
            active_cluster_name: active_cluster_name.into(),
            busy_timeout,
        }
    }

    pub fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        self.get_metadata_version_with_timeout(Duration::from_secs(3))
    }

    /// Read the cluster-wide domain notification version with a bounded SQLite
    /// lock wait. Matching uses Go's three-second persistence-call deadline.
    pub fn get_metadata_version_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<i64, MatchingDomainMetadataError> {
        let (connection, deadline) = self.lock_connection(timeout)?;
        let _deadline = SqliteOperationDeadline::new(&connection, deadline, self.busy_timeout)?;
        connection
            .query_row(
                "SELECT notification_version FROM domain_metadata",
                [],
                |row| row.get(0),
            )
            .map_err(|error| sqlite_operation_error(error, deadline))
    }

    /// Read by exactly one of name or canonical UUID, matching Go request
    /// validation and not-found wording.
    pub fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        self.get_domain_with_timeout(name, id, Duration::from_secs(3))
    }

    /// Cache-miss read-through using the remaining Go persistence-call budget.
    pub fn get_domain_with_timeout(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        match (
            name.filter(|value| !value.is_empty()),
            id.filter(|value| !value.is_empty()),
        ) {
            (Some(_), Some(_)) => Err(MatchingDomainMetadataError::BothIdAndName),
            (None, None) | (Some(""), None) | (None, Some("")) => {
                Err(MatchingDomainMetadataError::EmptyIdentity)
            }
            (Some(name), None) => self.read_one("name", name, name, timeout),
            (None, Some(id)) => {
                let uuid = uuid::Uuid::parse_str(id)
                    .map_err(|_| MatchingDomainMetadataError::InvalidUuid(id.to_string()))?;
                self.read_one("id", uuid.as_bytes(), id, timeout)
            }
        }
    }

    /// List in Go's raw UUID byte order. Tokens are raw 16-byte UUIDs; a full
    /// page returns a token even when it happens to be the final page.
    pub fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        self.list_domains_with_timeout(page_size, next_page_token, Duration::from_secs(3))
    }

    /// Scan one cache page with the remaining Go refresh-context budget.
    pub fn list_domains_with_timeout(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        if page_size == 0 {
            return Err(MatchingDomainMetadataError::InvalidPageSize);
        }
        let (connection, deadline) = self.lock_connection(timeout)?;
        let _deadline = SqliteOperationDeadline::new(&connection, deadline, self.busy_timeout)?;
        let mut rows = if let Some(token) = next_page_token {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, is_global, data, data_encoding FROM domains \
                 WHERE shard_id = ?1 AND id > ?2 ORDER BY id LIMIT ?3",
                )
                .map_err(|error| sqlite_operation_error(error, deadline))?;
            let mapped = statement
                .query_map(
                    params![GO_DEFAULT_SHARD_ID, token, page_size as i64],
                    row_from_sqlite,
                )
                .map_err(|error| sqlite_operation_error(error, deadline))?;
            let mut collected = Vec::with_capacity(page_size);
            for row in mapped {
                collected.push(row.map_err(|error| sqlite_operation_error(error, deadline))?);
            }
            collected
        } else {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, is_global, data, data_encoding FROM domains \
                 WHERE shard_id = ?1 ORDER BY id LIMIT ?2",
                )
                .map_err(|error| sqlite_operation_error(error, deadline))?;
            let mapped = statement
                .query_map(
                    params![GO_DEFAULT_SHARD_ID, page_size as i64],
                    row_from_sqlite,
                )
                .map_err(|error| sqlite_operation_error(error, deadline))?;
            let mut collected = Vec::with_capacity(page_size);
            for row in mapped {
                collected.push(row.map_err(|error| sqlite_operation_error(error, deadline))?);
            }
            collected
        };
        let has_next_token = rows.len() >= page_size;
        let next_page_token = if has_next_token {
            rows.last().map(|row| row.id.clone())
        } else {
            None
        };
        let domains = rows
            .drain(..)
            .map(|row| self.decode_row(row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MatchingDomainPage {
            domains,
            next_page_token,
        })
    }

    fn read_one(
        &self,
        selector: &str,
        value: impl rusqlite::ToSql,
        identity: &str,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        // `selector` is chosen only from this module's two literals.
        let query = format!(
            "SELECT id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ?1 AND {selector} = ?2"
        );
        let (connection, deadline) = self.lock_connection(timeout)?;
        let _deadline = SqliteOperationDeadline::new(&connection, deadline, self.busy_timeout)?;
        let row = connection
            .query_row(&query, params![GO_DEFAULT_SHARD_ID, value], row_from_sqlite)
            .optional()
            .map_err(|error| sqlite_operation_error(error, deadline))?
            .ok_or_else(|| MatchingDomainMetadataError::NotFound(identity.to_string()))?;
        self.decode_row(row)
    }

    fn decode_row(
        &self,
        row: SqlDomainRow,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        decode_go_sql_domain_metadata(
            row.id,
            row.name,
            row.is_global,
            row.data,
            row.data_encoding,
            &self.active_cluster_name,
        )
    }
}

/// Shared conversion used by SQLite and MySQL readers so both expose the same
/// Go UUID, ThriftRW, blob encoding, and current-cluster semantics.
pub(crate) fn decode_go_sql_domain_metadata(
    id: Vec<u8>,
    name: String,
    is_global: bool,
    data: Vec<u8>,
    data_encoding: String,
    current_cluster: &str,
) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
    let uuid = uuid::Uuid::from_slice(&id)
        .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()))?;
    let thrift = decode_domain_info_blob(&data, &data_encoding)?;
    thrift_to_metadata(
        thrift,
        uuid.to_string(),
        SqlDomainRow {
            id,
            name,
            is_global,
            data,
            data_encoding,
        },
        current_cluster,
    )
}

/// Encode the SQL `domains.data` payload written by Go's `sqlDomainStore`.
/// This is the flattened `sqlblobs.DomainInfo` ThriftRW struct; unlike nested
/// persistence DataBlobs, it has no `0x59` version prefix.
pub fn encode_go_sql_domain_metadata(
    metadata: &MatchingDomainMetadata,
) -> Result<Vec<u8>, MatchingDomainMetadataError> {
    use cadence_thrift::protocol::Writer;
    use cadence_thrift::value::write_struct;

    let mut thrift = Struct::default();
    let mut put = |id, value| thrift.fields.push((id, value));
    let text = |value: &str| Value::Bytes(value.as_bytes().to_vec());
    put(10, text(&metadata.info.name));
    put(12, text(&metadata.info.description));
    put(14, text(&metadata.info.owner_email));
    put(16, Value::I32(metadata.status_raw));
    put(
        18,
        Value::I16(metadata.configuration.workflow_execution_retention_period_in_days as i16),
    );
    put(20, Value::Bool(metadata.configuration.emit_metric));
    put(22, text(&metadata.archival_bucket));
    put(24, Value::I16(metadata.archival_status_raw));
    put(26, Value::I64(metadata.config_version));
    put(28, Value::I64(metadata.notification_version));
    put(30, Value::I64(metadata.failover_notification_version));
    put(32, Value::I64(metadata.failover_version));
    put(34, text(&metadata.replication.active_cluster_name));
    put(
        36,
        Value::List(
            metadata
                .replication
                .clusters
                .iter()
                .map(|cluster| text(&cluster.cluster_name))
                .collect(),
        ),
    );
    put(
        38,
        Value::Map(
            metadata
                .info
                .data
                .as_ref()
                .into_iter()
                .flat_map(|data| data.iter())
                .map(|(key, value)| (text(key), text(value)))
                .collect(),
        ),
    );
    put(44, text(&metadata.configuration.history_archival_uri));
    put(42, Value::I16(metadata.history_archival_status_raw));
    put(48, text(&metadata.configuration.visibility_archival_uri));
    put(46, Value::I16(metadata.visibility_archival_status_raw));
    put(
        50,
        Value::I64(
            metadata
                .failover_end_time_nanos
                .unwrap_or(GO_ZERO_TIME_NANOS),
        ),
    );
    put(52, Value::I64(metadata.previous_failover_version));
    put(54, Value::I64(metadata.last_updated_time_nanos));
    if let Some(blob) = &metadata.bad_binaries {
        put(39, Value::Bytes(blob.data.clone()));
        put(40, text(&blob.encoding));
    }
    if let Some(blob) = &metadata.isolation_groups {
        put(56, Value::Bytes(blob.data.clone()));
        put(58, text(&blob.encoding));
    }
    if let Some(blob) = &metadata.async_workflow_config {
        put(60, Value::Bytes(blob.data.clone()));
        put(62, text(&blob.encoding));
    }
    if let Some(blob) = &metadata.active_clusters_config {
        put(64, Value::Bytes(blob.data.clone()));
        put(66, text(&blob.encoding));
    }

    let mut writer = Writer::new();
    write_struct(&mut writer, &thrift);
    Ok(writer.into_bytes())
}

struct SqlDomainRow {
    id: Vec<u8>,
    name: String,
    is_global: bool,
    data: Vec<u8>,
    data_encoding: String,
}

fn row_from_sqlite(row: &rusqlite::Row<'_>) -> rusqlite::Result<SqlDomainRow> {
    Ok(SqlDomainRow {
        id: row.get(0)?,
        name: row.get(1)?,
        is_global: row.get(2)?,
        data: row.get(3)?,
        data_encoding: row.get(4)?,
    })
}

fn decode_domain_info_blob(
    data: &[u8],
    encoding: &str,
) -> Result<Struct, MatchingDomainMetadataError> {
    let decoded = match encoding {
        ENCODING_THRIFT_RW => data.to_vec(),
        ENCODING_THRIFT_RW_SNAPPY => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()))?,
        other => {
            return Err(MatchingDomainMetadataError::UnsupportedEncoding(
                other.to_string(),
            ));
        }
    };
    let mut reader = Reader::new(&decoded);
    decode_go_domain_info(&mut reader)
        .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()))
}

/// Generated Go `sqlblobs.DomainInfo.Decode` switches on both field ID and
/// wire type. A known field with the wrong wire type is skipped, and repeated
/// correctly-typed fields overwrite earlier values. Apply that behavior here
/// without changing the shared Thrift value model used by other records.
fn decode_go_domain_info(reader: &mut Reader<'_>) -> cadence_thrift::protocol::Result<Struct> {
    let mut fields: Vec<(i16, Value)> = Vec::new();
    while let Some((wire_type, id)) = reader.read_field_begin()? {
        let Some(expected_type) = domain_info_wire_type(id) else {
            reader.skip(wire_type)?;
            continue;
        };
        if wire_type != expected_type {
            reader.skip(wire_type)?;
            continue;
        }
        let value = match id {
            36 => read_domain_clusters(reader)?,
            38 => read_domain_data(reader)?,
            _ => Some(read_value(reader, wire_type)?),
        };
        if let Some(index) = fields.iter().position(|(field_id, _)| *field_id == id) {
            if let Some(value) = value {
                fields[index].1 = value;
            } else {
                fields.remove(index);
            }
        } else if let Some(value) = value {
            fields.push((id, value));
        }
    }
    Ok(Struct { fields })
}

fn read_domain_clusters(
    reader: &mut Reader<'_>,
) -> cadence_thrift::protocol::Result<Option<Value>> {
    let (element_type, length) = reader.read_list_begin()?;
    if element_type != TType::String {
        for _ in 0..length {
            reader.skip(element_type)?;
        }
        return Ok(None);
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        values.push(Value::Bytes(reader.read_string()?.into_bytes()));
    }
    Ok(Some(Value::List(values)))
}

fn read_domain_data(reader: &mut Reader<'_>) -> cadence_thrift::protocol::Result<Option<Value>> {
    let (key_type, value_type, length) = reader.read_map_begin()?;
    if key_type != TType::String || value_type != TType::String {
        for _ in 0..length {
            reader.skip(key_type)?;
            reader.skip(value_type)?;
        }
        return Ok(None);
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let key = Value::Bytes(reader.read_string()?.into_bytes());
        let value = Value::Bytes(reader.read_string()?.into_bytes());
        values.push((key, value));
    }
    Ok(Some(Value::Map(values)))
}

fn domain_info_wire_type(id: i16) -> Option<TType> {
    use TType::{Bool, List, Map, String, I16, I32, I64};
    match id {
        10 | 12 | 14 | 22 | 34 | 40 | 44 | 48 | 58 | 62 | 66 => Some(String),
        16 => Some(I32),
        18 | 24 | 42 | 46 => Some(I16),
        20 => Some(Bool),
        26 | 28 | 30 | 32 | 50 | 52 | 54 => Some(I64),
        36 => Some(List),
        38 => Some(Map),
        39 | 56 | 60 | 64 => Some(String),
        _ => None,
    }
}

fn thrift_to_metadata(
    thrift: Struct,
    uuid: String,
    row: SqlDomainRow,
    current_cluster: &str,
) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
    let mut config_decode_errors = Vec::new();
    let data_map = thrift.opt_map_str::<String>(38).map_err(protocol_error)?;
    let clusters = thrift
        .opt_list::<String>(36)
        .map_err(protocol_error)?
        .unwrap_or_default();
    let active_clusters_config = blob(&thrift, 64, 66)?;
    let active_clusters = decode_optional_go_blob::<ActiveClusters>(
        active_clusters_config.as_ref(),
        &mut config_decode_errors,
    );
    // Go injects current cluster for non-active-active domains only. The
    // presence of field 64, including an empty byte string, suppresses it.
    let active_cluster_name = match thrift
        .opt_string(34)
        .map_err(protocol_error)?
        .unwrap_or_default()
    {
        stored if active_clusters_config.is_some() => stored,
        stored if stored.is_empty() => current_cluster.to_string(),
        stored => stored,
    };
    let clusters = if clusters.is_empty() {
        vec![cadence_api::types::ClusterReplicationConfiguration {
            cluster_name: current_cluster.to_string(),
        }]
    } else {
        clusters
            .into_iter()
            .map(
                |cluster_name| cadence_api::types::ClusterReplicationConfiguration { cluster_name },
            )
            .collect()
    };

    let status_raw = thrift
        .opt_i32(16)
        .map_err(protocol_error)?
        .unwrap_or_default();
    let archival_status_raw = optional_i16(&thrift, 24)?;
    let history_archival_status_raw = optional_i16(&thrift, 42)?;
    let visibility_archival_status_raw = optional_i16(&thrift, 46)?;
    let info = DomainInfo {
        name: row.name,
        status: Some(DomainStatus::from_i32(status_raw).unwrap_or(DomainStatus::Registered)),
        description: thrift
            .opt_string(12)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        owner_email: thrift
            .opt_string(14)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        data: data_map,
        uuid,
    };
    let bad_binaries = blob(&thrift, 39, 40)?;
    let isolation_groups = blob(&thrift, 56, 58)?;
    let async_workflow_config = blob(&thrift, 60, 62)?;
    let decoded_bad_binaries =
        decode_optional_go_blob::<BadBinaries>(bad_binaries.as_ref(), &mut config_decode_errors);
    let decoded_isolation_groups = decode_optional_go_blob::<IsolationGroupConfiguration>(
        isolation_groups.as_ref(),
        &mut config_decode_errors,
    );
    let decoded_async_workflow_config = decode_optional_go_blob::<AsyncWorkflowConfiguration>(
        async_workflow_config.as_ref(),
        &mut config_decode_errors,
    );
    let configuration = DomainConfiguration {
        workflow_execution_retention_period_in_days: i32::from(optional_i16(&thrift, 18)?),
        emit_metric: thrift
            .opt_bool(20)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        history_archival_status: Some(
            ArchivalStatus::from_i32(i32::from(history_archival_status_raw))
                .unwrap_or(ArchivalStatus::Disabled),
        ),
        history_archival_uri: thrift
            .opt_string(44)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        visibility_archival_status: Some(
            ArchivalStatus::from_i32(i32::from(visibility_archival_status_raw))
                .unwrap_or(ArchivalStatus::Disabled),
        ),
        visibility_archival_uri: thrift
            .opt_string(48)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        bad_binaries: decoded_bad_binaries.clone(),
    };
    let replication = DomainReplicationConfiguration {
        active_cluster_name,
        clusters,
        // The encoded bytes are also retained in `active_clusters_config`.
        active_clusters,
    };
    let failover_end_time_nanos = thrift
        .opt_i64(50)
        .map_err(protocol_error)?
        .filter(|timestamp| *timestamp != GO_ZERO_TIME_NANOS);
    Ok(MatchingDomainMetadata {
        info,
        configuration,
        replication,
        is_global_domain: row.is_global,
        archival_bucket: thrift
            .opt_string(22)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        status_raw,
        archival_status_raw,
        history_archival_status_raw,
        visibility_archival_status_raw,
        config_version: thrift
            .opt_i64(26)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        notification_version: thrift
            .opt_i64(28)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        failover_notification_version: thrift
            .opt_i64(30)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        failover_version: thrift
            .opt_i64(32)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        previous_failover_version: thrift
            .opt_i64(52)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        failover_end_time_nanos,
        last_updated_time_nanos: thrift
            .opt_i64(54)
            .map_err(protocol_error)?
            .unwrap_or_default(),
        bad_binaries,
        isolation_groups,
        async_workflow_config,
        active_clusters_config,
        decoded_bad_binaries,
        decoded_isolation_groups,
        decoded_async_workflow_config,
        config_decode_error: (!config_decode_errors.is_empty())
            .then(|| config_decode_errors.join("; ")),
    })
}

fn decode_optional_go_blob<T>(blob: Option<&GoDomainBlob>, errors: &mut Vec<String>) -> Option<T>
where
    T: FromThrift + serde::de::DeserializeOwned,
{
    blob.and_then(|blob| match decode_go_blob(blob) {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(error.to_string());
            None
        }
    })
}

fn decode_go_blob<T>(blob: &GoDomainBlob) -> Result<T, MatchingDomainMetadataError>
where
    T: FromThrift + serde::de::DeserializeOwned,
{
    let encoding = match blob.encoding.as_str() {
        "thriftrw" => Some(false),
        "thriftrw_snappy" => Some(true),
        // The Go serializer uses JSON for JSON, empty, and unknown encodings.
        "json" | "" | "unknown" => None,
        other => {
            return Err(MatchingDomainMetadataError::UnsupportedEncoding(
                other.to_string(),
            ));
        }
    };
    let Some(snappy) = encoding else {
        return serde_json::from_slice(&blob.data)
            .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()));
    };
    let bytes = if snappy {
        snap::raw::Decoder::new()
            .decompress_vec(&blob.data)
            .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()))?
    } else {
        blob.data.clone()
    };
    // Nested persistence DataBlobs use common/codec.ThriftRWEncoder, which
    // prefixes the Thrift payload with the version byte 0x59. The enclosing
    // domains row is serialized directly with thriftrw and has no such byte.
    let Some((&version, payload)) = bytes.split_first() else {
        return Err(MatchingDomainMetadataError::InvalidBlob(
            "missing ThriftRW encoding version".into(),
        ));
    };
    if version != 0x59 {
        return Err(MatchingDomainMetadataError::InvalidBlob(format!(
            "invalid ThriftRW encoding version: {version:#04x}"
        )));
    }
    let mut reader = Reader::new(payload);
    let value = T::from_thrift(&read_struct(&mut reader).map_err(protocol_error)?)
        .map_err(protocol_error)?;
    if reader.remaining() != 0 {
        return Err(MatchingDomainMetadataError::InvalidBlob(
            "trailing Go ThriftRW blob bytes".into(),
        ));
    }
    Ok(value)
}

fn blob(
    thrift: &Struct,
    data_field: i16,
    encoding_field: i16,
) -> Result<Option<GoDomainBlob>, MatchingDomainMetadataError> {
    let data = thrift.opt_bytes(data_field).map_err(protocol_error)?;
    let encoding = thrift
        .opt_string(encoding_field)
        .map_err(protocol_error)?
        .unwrap_or_default();
    Ok(data.map(|data| GoDomainBlob { data, encoding }))
}

fn optional_i16(thrift: &Struct, field: i16) -> Result<i16, MatchingDomainMetadataError> {
    match thrift.get(field) {
        None => Ok(0),
        Some(Value::I16(value)) => Ok(*value),
        Some(value) => Err(MatchingDomainMetadataError::InvalidBlob(format!(
            "field {field}: expected i16, got {value:?}"
        ))),
    }
}

fn protocol_error(error: cadence_thrift::protocol::Error) -> MatchingDomainMetadataError {
    MatchingDomainMetadataError::InvalidBlob(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_thrift::protocol::Writer;
    use cadence_thrift::value::write_struct;

    #[test]
    fn go_sql_domain_metadata_encoder_round_trips_the_domain_row() {
        let uuid = uuid::Uuid::from_u128(17);
        let metadata = MatchingDomainMetadata {
            info: DomainInfo {
                name: "sql-domain".into(),
                status: Some(DomainStatus::Registered),
                description: "description".into(),
                owner_email: "owner@example.test".into(),
                data: Some([("key".into(), "value".into())].into()),
                uuid: uuid.to_string(),
            },
            configuration: DomainConfiguration {
                workflow_execution_retention_period_in_days: 11,
                emit_metric: true,
                history_archival_status: Some(ArchivalStatus::Enabled),
                history_archival_uri: "history://archive".into(),
                visibility_archival_status: Some(ArchivalStatus::Disabled),
                visibility_archival_uri: "visibility://archive".into(),
                bad_binaries: None,
            },
            replication: DomainReplicationConfiguration {
                active_cluster_name: "cluster-a".into(),
                clusters: vec![cadence_api::types::ClusterReplicationConfiguration {
                    cluster_name: "cluster-a".into(),
                }],
                active_clusters: None,
            },
            is_global_domain: true,
            archival_bucket: "bucket".into(),
            status_raw: 0,
            archival_status_raw: 0,
            history_archival_status_raw: 1,
            visibility_archival_status_raw: 0,
            config_version: 2,
            notification_version: 3,
            failover_notification_version: 4,
            failover_version: 5,
            previous_failover_version: 4,
            failover_end_time_nanos: None,
            last_updated_time_nanos: 6,
            bad_binaries: None,
            isolation_groups: None,
            async_workflow_config: None,
            active_clusters_config: None,
            decoded_bad_binaries: None,
            decoded_isolation_groups: None,
            decoded_async_workflow_config: None,
            config_decode_error: None,
        };
        let data = encode_go_sql_domain_metadata(&metadata).unwrap();
        let decoded = decode_go_sql_domain_metadata(
            uuid.as_bytes().to_vec(),
            metadata.info.name.clone(),
            metadata.is_global_domain,
            data,
            ENCODING_THRIFT_RW.into(),
            "cluster-a",
        )
        .unwrap();
        assert_eq!(decoded.info, metadata.info);
        assert_eq!(decoded.configuration, metadata.configuration);
        assert_eq!(decoded.replication, metadata.replication);
        assert_eq!(decoded.archival_bucket, metadata.archival_bucket);
        assert_eq!(decoded.config_version, metadata.config_version);
        assert_eq!(decoded.notification_version, metadata.notification_version);
        assert_eq!(decoded.failover_notification_version, metadata.failover_notification_version);
        assert_eq!(decoded.failover_version, metadata.failover_version);
        assert_eq!(decoded.previous_failover_version, metadata.previous_failover_version);
        assert_eq!(decoded.last_updated_time_nanos, metadata.last_updated_time_nanos);
    }

    #[test]
    fn cassandra_domain_udts_follow_go_domain_metadata_conversion() {
        let domain = decode_go_cassandra_domain_metadata(
            crate::cassandra::CassandraDomainUdt {
                id: uuid::Uuid::from_u128(7),
                name: "cassandra-domain".into(),
                status: 1,
                description: Some("description".into()),
                data: Some([("key".to_string(), "value".to_string())].into()),
                owner_email: Some("owner@example.test".into()),
            },
            crate::cassandra::CassandraDomainConfigUdt {
                retention: 9,
                emit_metric: true,
                ..Default::default()
            },
            crate::cassandra::CassandraDomainReplicationConfigUdt {
                active_cluster_name: None,
                clusters: Some(vec![]),
                active_clusters_config: None,
                active_clusters_config_encoding: None,
            },
            true,
            11,
            12,
            13,
            14,
            GO_ZERO_TIME_NANOS,
            15,
            16,
            "active",
        )
        .unwrap();
        assert_eq!(domain.info.name, "cassandra-domain");
        assert_eq!(domain.info.status, Some(DomainStatus::Deprecated));
        assert_eq!(domain.info.description, "description");
        assert_eq!(domain.info.owner_email, "owner@example.test");
        assert_eq!(
            domain
                .configuration
                .workflow_execution_retention_period_in_days,
            9
        );
        assert!(domain.configuration.emit_metric);
        assert_eq!(domain.replication.active_cluster_name, "active");
        assert_eq!(domain.replication.clusters[0].cluster_name, "active");
        assert_eq!(domain.config_version, 11);
        assert_eq!(domain.notification_version, 16);
        assert_eq!(domain.last_updated_time_nanos, 15);
        assert!(domain.failover_end_time_nanos.is_none());
    }

    fn thrift_blob(fields: Vec<(i16, Value)>) -> Vec<u8> {
        let mut writer = Writer::new();
        write_struct(&mut writer, &Struct { fields });
        writer.into_bytes()
    }

    fn go_codec_thrift_blob(fields: Vec<(i16, Value)>) -> Vec<u8> {
        let mut bytes = vec![0x59];
        bytes.extend_from_slice(&thrift_blob(fields));
        bytes
    }

    fn insert_domain(connection: &Connection, id: uuid::Uuid, name: &str, info: Vec<u8>) {
        connection.execute(
            "INSERT INTO domains (shard_id, id, name, is_global, data, data_encoding) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![GO_DEFAULT_SHARD_ID, id.as_bytes(), name, false, info, ENCODING_THRIFT_RW],
        ).unwrap();
    }

    #[test]
    fn read_only_uri_modes_and_configured_busy_timeout_are_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("domains.sqlite");
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch(
            "CREATE TABLE domains (shard_id INTEGER, id BLOB, name TEXT, is_global BOOLEAN, data BLOB, data_encoding TEXT);
             CREATE TABLE domain_metadata (id INTEGER, notification_version INTEGER);
             INSERT INTO domain_metadata VALUES (1, 91);",
        ).unwrap();
        drop(writer);

        for mode in ["ro", "rw", "rwc"] {
            let store = MatchingDomainMetadataStore::open_read_only_with_options(
                &path,
                "cluster-a",
                mode,
                Duration::from_millis(17),
            )
            .unwrap();
            assert_eq!(store.busy_timeout, Duration::from_millis(17));
            assert_eq!(store.get_metadata_version().unwrap(), 91);
        }
        let lock = Connection::open(&path).unwrap();
        lock.execute_batch("BEGIN EXCLUSIVE;").unwrap();
        let store = MatchingDomainMetadataStore::open_read_only_with_options(
            &path,
            "cluster-a",
            "ro",
            Duration::from_millis(17),
        )
        .unwrap();
        let started = Instant::now();
        assert!(matches!(
            store.get_metadata_version(),
            Err(MatchingDomainMetadataError::Sqlite(
                rusqlite::Error::SqliteFailure(failure, _)
            )) if failure.code == rusqlite::ErrorCode::DatabaseBusy
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
        lock.execute_batch("ROLLBACK;").unwrap();
        assert!(matches!(
            MatchingDomainMetadataStore::open_read_only_with_options(
                &path,
                "cluster-a",
                "memory",
                Duration::from_secs(1),
            ),
            Err(MatchingDomainMetadataError::InvalidSqliteConfiguration(_))
        ));
    }

    #[test]
    fn reads_go_domain_metadata_and_pages_by_raw_uuid() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(
            "CREATE TABLE domains (shard_id INTEGER, id BLOB, name TEXT, is_global BOOLEAN, data BLOB, data_encoding TEXT);
             CREATE TABLE domain_metadata (id INTEGER, notification_version INTEGER);
             INSERT INTO domain_metadata VALUES (1, 91);",
        ).unwrap();
        let low = uuid::Uuid::from_bytes([1; 16]);
        let high = uuid::Uuid::from_bytes([2; 16]);
        let fields = vec![
            (16, Value::I32(2)),
            (18, Value::I16(7)),
            (20, Value::Bool(true)),
            (28, Value::I64(31)),
            (30, Value::I64(47)),
            (32, Value::I64(53)),
            (34, Value::Bytes(Vec::new())),
            (
                38,
                Value::Map(vec![(
                    Value::Bytes(b"k".to_vec()),
                    Value::Bytes(b"v".to_vec()),
                )]),
            ),
            (
                56,
                Value::Bytes({
                    let mut data = go_codec_thrift_blob(Vec::new());
                    data.push(0xff);
                    data
                }),
            ),
            (58, Value::Bytes(b"thriftrw".to_vec())),
        ];
        insert_domain(&connection, high, "second", thrift_blob(fields.clone()));
        insert_domain(&connection, low, "first", thrift_blob(fields));
        let store = MatchingDomainMetadataStore::from_connection(connection, "cluster-a");

        assert_eq!(store.get_metadata_version().unwrap(), 91);
        let first_page = store.list_domains(1, None).unwrap();
        assert_eq!(first_page.domains[0].info.name, "first");
        assert_eq!(first_page.next_page_token, Some(low.as_bytes().to_vec()));
        let second_page = store
            .list_domains(1, first_page.next_page_token.as_deref())
            .unwrap();
        let domain = &second_page.domains[0];
        assert_eq!(domain.info.uuid, high.to_string());
        assert_eq!(
            domain
                .configuration
                .workflow_execution_retention_period_in_days,
            7
        );
        assert!(domain.configuration.emit_metric);
        assert_eq!(domain.notification_version, 31);
        assert_eq!(domain.failover_notification_version, 47);
        assert_eq!(domain.failover_version, 53);
        assert_eq!(domain.replication.active_cluster_name, "cluster-a");
        assert_eq!(domain.replication.clusters[0].cluster_name, "cluster-a");
        assert!(domain
            .isolation_groups
            .as_ref()
            .unwrap()
            .data
            .starts_with(&[0x59]));
        assert!(domain.decoded_isolation_groups.is_none());
        assert!(domain
            .config_decode_error
            .as_deref()
            .unwrap()
            .contains("trailing Go ThriftRW blob bytes"));
        assert!(domain.decoded_isolation_groups.is_none());
    }

    #[test]
    fn sqlite_lock_wait_past_deadline_maps_to_operation_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("domain-lock.sqlite");
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "CREATE TABLE domain_metadata (id INTEGER, notification_version INTEGER);
                 INSERT INTO domain_metadata VALUES (1, 91);",
            )
            .unwrap();
        let store = MatchingDomainMetadataStore::open_read_only(&path, "cluster-a").unwrap();
        writer.execute_batch("BEGIN EXCLUSIVE;").unwrap();

        let result = store.get_metadata_version_with_timeout(Duration::from_millis(20));
        assert!(
            matches!(
                result,
                Err(MatchingDomainMetadataError::OperationTimedOut)
                    | Err(MatchingDomainMetadataError::Sqlite(
                        rusqlite::Error::SqliteFailure(_, _)
                    ))
            ),
            "{result:?}"
        );

        let before_deadline = Instant::now() + Duration::from_secs(1);
        let immediate_busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        );
        assert!(matches!(
            sqlite_operation_error(immediate_busy, before_deadline),
            MatchingDomainMetadataError::Sqlite(_)
        ));
        writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn active_active_blob_suppresses_current_cluster_default() {
        let active_config = go_codec_thrift_blob(Vec::new());
        let metadata = thrift_to_metadata(
            Struct {
                fields: vec![
                    (34, Value::Bytes(Vec::new())),
                    (36, Value::List(vec![Value::Bytes(b"cluster-b".to_vec())])),
                    (64, Value::Bytes(active_config.clone())),
                    (66, Value::Bytes(b"thriftrw".to_vec())),
                ],
            },
            uuid::Uuid::from_bytes([3; 16]).to_string(),
            SqlDomainRow {
                id: uuid::Uuid::from_bytes([3; 16]).as_bytes().to_vec(),
                name: "aa".into(),
                is_global: true,
                data: vec![],
                data_encoding: ENCODING_THRIFT_RW.into(),
            },
            "cluster-a",
        )
        .unwrap();
        assert_eq!(metadata.replication.active_cluster_name, "");
        assert_eq!(metadata.replication.clusters[0].cluster_name, "cluster-b");
        assert_eq!(metadata.active_clusters_config.unwrap().data, active_config);
    }

    #[test]
    fn malformed_page_token_is_passed_to_sqlite_without_length_validation() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE domains (shard_id INTEGER, id BLOB, name TEXT, is_global BOOLEAN, data BLOB, data_encoding TEXT);").unwrap();
        let store = MatchingDomainMetadataStore::from_connection(connection, "cluster-a");
        assert!(store
            .list_domains(10, Some(b"opaque token"))
            .unwrap()
            .domains
            .is_empty());
    }

    #[test]
    fn wrong_type_known_field_is_skipped_like_go_decoder() {
        let info = thrift_blob(vec![(28, Value::Bytes(b"wrong type".to_vec()))]);
        let decoded = decode_domain_info_blob(&info, ENCODING_THRIFT_RW).unwrap();
        assert_eq!(decoded.opt_i64(28).unwrap(), None);
    }

    #[test]
    fn duplicate_fields_use_last_correctly_typed_value() {
        let info = thrift_blob(vec![
            (28, Value::I64(5)),
            (28, Value::Bytes(b"wrong type is skipped".to_vec())),
            (28, Value::I64(7)),
        ]);
        let decoded = decode_domain_info_blob(&info, ENCODING_THRIFT_RW).unwrap();
        assert_eq!(decoded.opt_i64(28).unwrap(), Some(7));
    }

    #[test]
    fn trailing_bytes_after_struct_match_go_thrift_reader_behavior() {
        let mut info = thrift_blob(Vec::new());
        info.extend_from_slice(b"ignored tail");
        assert!(decode_domain_info_blob(&info, ENCODING_THRIFT_RW).is_ok());
    }

    #[test]
    fn nested_thriftrw_blob_requires_and_strips_go_codec_version() {
        let mut data = vec![0x59];
        data.extend_from_slice(&thrift_blob(Vec::new()));
        let decoded: BadBinaries = decode_go_blob(&GoDomainBlob {
            data,
            encoding: "thriftrw".into(),
        })
        .unwrap();
        assert!(decoded.binaries.is_none());

        for (data, expected) in [
            (Vec::new(), "missing ThriftRW encoding version"),
            (vec![0x58, 0], "invalid ThriftRW encoding version: 0x58"),
        ] {
            let error = decode_go_blob::<BadBinaries>(&GoDomainBlob {
                data,
                encoding: "thriftrw".into(),
            })
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn decodes_nested_blobs_emitted_by_go_payload_serializer() {
        // Captured from Go common/persistence.NewPayloadSerializer with
        // EncodingTypeThriftRW for zero-value config structures.
        let go_blobs: [(&str, &[u8]); 3] = [
            (
                "thriftrw",
                &[
                    0x59, 0x0d, 0x00, 0x0a, 0x0b, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00,
                ],
            ),
            ("thriftrw", &[0x59, 0x00]),
            (
                "thriftrw",
                &[
                    0x59, 0x02, 0x00, 0x0a, 0x00, 0x0b, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x0b,
                    0x00, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00,
                ],
            ),
        ];
        let bad_binaries: BadBinaries = decode_go_blob(&GoDomainBlob {
            data: go_blobs[0].1.to_vec(),
            encoding: go_blobs[0].0.into(),
        })
        .unwrap();
        assert_eq!(
            bad_binaries.binaries,
            Some(std::collections::HashMap::new())
        );
        let isolation_groups: IsolationGroupConfiguration = decode_go_blob(&GoDomainBlob {
            data: go_blobs[1].1.to_vec(),
            encoding: go_blobs[1].0.into(),
        })
        .unwrap();
        assert!(isolation_groups.isolation_groups.is_empty());
        let async_config: AsyncWorkflowConfiguration = decode_go_blob(&GoDomainBlob {
            data: go_blobs[2].1.to_vec(),
            encoding: go_blobs[2].0.into(),
        })
        .unwrap();
        assert!(!async_config.enabled);
    }

    #[test]
    fn wrong_collection_element_types_are_skipped_like_go_decoder() {
        let mut writer = Writer::new();
        writer.write_field_begin(TType::List, 36);
        writer.write_list_begin(TType::I32, 1);
        writer.write_i32(42);
        writer.write_field_begin(TType::Map, 38);
        writer.write_map_begin(TType::I32, TType::String, 1);
        writer.write_i32(7);
        writer.write_string("value");
        writer.write_field_stop();
        let decoded = decode_domain_info_blob(&writer.into_bytes(), ENCODING_THRIFT_RW).unwrap();
        assert!(decoded.get(36).is_none());
        assert!(decoded.get(38).is_none());
    }

    #[test]
    fn wrong_collection_duplicate_clears_previous_collection() {
        let mut writer = Writer::new();
        writer.write_field_begin(TType::Map, 38);
        writer.write_map_begin(TType::String, TType::String, 0);
        writer.write_field_begin(TType::Map, 38);
        writer.write_map_begin(TType::String, TType::I32, 1);
        writer.write_string("key");
        writer.write_i32(1);
        writer.write_field_stop();
        let decoded = decode_domain_info_blob(&writer.into_bytes(), ENCODING_THRIFT_RW).unwrap();
        assert!(decoded.get(38).is_none());
    }

    #[test]
    #[ignore = "requires CADENCE_MATCHING_SQLITE_DB produced by the Go SQLite domain-store oracle"]
    fn go_domain_store_database_interop() {
        let path = std::env::var("CADENCE_MATCHING_SQLITE_DB").expect("oracle DB path");
        let store = MatchingDomainMetadataStore::open_read_only(path, "cluster0").unwrap();
        assert!(store.get_metadata_version().unwrap() >= 2);
        let standard = store.get_domain(Some("metadata-standard"), None).unwrap();
        assert_eq!(standard.info.uuid, "00000000-0000-0000-0000-000000000001");
        assert_eq!(standard.info.description, "Go oracle domain");
        assert_eq!(standard.info.owner_email, "cadence@example.test");
        assert!(standard.info.data.is_none());
        assert_eq!(
            standard
                .configuration
                .workflow_execution_retention_period_in_days,
            7
        );
        assert!(standard.configuration.emit_metric);
        assert_eq!(standard.notification_version, 101);
        assert_eq!(standard.replication.clusters.len(), 1);
        assert_eq!(standard.replication.clusters[0].cluster_name, "cluster0");
        assert_eq!(standard.config_version, 19);
        assert_eq!(standard.failover_version, 23);
        assert_eq!(standard.failover_notification_version, 29);
        assert_eq!(standard.previous_failover_version, 17);
        assert_eq!(
            standard.failover_end_time_nanos,
            Some(1_700_000_000_987_654_321)
        );
        assert_eq!(standard.last_updated_time_nanos, 1_700_000_000_999_999_999);
        assert_eq!(
            standard.isolation_groups.as_ref().unwrap().data,
            [0, 255, 7]
        );
        assert_eq!(
            standard.async_workflow_config.as_ref().unwrap().data,
            [9, 8, 7]
        );
        assert!(standard.config_decode_error.is_some());

        for id in [
            "00000000000000000000000000000001",
            "{00000000-0000-0000-0000-000000000001}",
            "urn:uuid:00000000-0000-0000-0000-000000000001",
        ] {
            assert_eq!(
                store.get_domain(None, Some(id)).unwrap().info.name,
                "metadata-standard"
            );
        }
        assert!(matches!(
            store.get_domain(None, Some("invalid-uuid")),
            Err(MatchingDomainMetadataError::InvalidUuid(_))
        ));

        let active_active = store
            .get_domain(Some("metadata-active-active"), None)
            .unwrap();
        assert!(active_active.is_global_domain);
        assert_eq!(active_active.replication.active_cluster_name, "");
        assert_eq!(active_active.replication.clusters.len(), 2);
        assert_eq!(
            active_active.active_clusters_config.as_ref().unwrap().data,
            [0x80, 0]
        );

        let first_page = store.list_domains(1, None).unwrap();
        assert_eq!(first_page.domains.len(), 1);
        assert_eq!(
            first_page.next_page_token.as_deref(),
            Some(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1][..])
        );
        let second_page = store
            .list_domains(1, first_page.next_page_token.as_deref())
            .unwrap();
        assert_eq!(second_page.domains[0].info.name, "metadata-active-active");
        assert!(matches!(
            store.get_domain(Some("missing-domain"), None),
            Err(MatchingDomainMetadataError::NotFound(name)) if name == "missing-domain"
        ));
    }
}
