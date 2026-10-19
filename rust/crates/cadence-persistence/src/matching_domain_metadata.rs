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
use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};
use cadence_thrift::protocol::{Reader, TType};
use cadence_thrift::value::{read_value, Struct, Value};
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    #[error("domain page size must be positive")]
    InvalidPageSize,
    #[error("domain metadata persistence operation timed out")]
    OperationTimedOut,
}

/// Read-only access to the Go `domains` and `domain_metadata` tables.
///
/// Go's SQL backend stores UUIDs as 16-byte BLOBs, uses shard 54321 for domain
/// rows, and pages by `id > token ORDER BY id LIMIT page_size`.
pub struct MatchingDomainMetadataStore {
    connection: Mutex<Connection>,
    active_cluster_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingDomainPage {
    pub domains: Vec<MatchingDomainMetadata>,
    /// Go returns the last row's raw UUID whenever rows.len() >= page_size.
    pub next_page_token: Option<Vec<u8>>,
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
    fn new(connection: &'a Connection, deadline: Instant) -> Result<Self, rusqlite::Error> {
        connection.busy_timeout(deadline.saturating_duration_since(Instant::now()))?;
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
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Ok(Self::from_connection(connection, active_cluster_name))
    }

    /// Wrap an existing read-only connection. Primarily useful to callers that
    /// manage connection lifecycle outside this metadata reader.
    pub fn from_connection(connection: Connection, active_cluster_name: impl Into<String>) -> Self {
        Self {
            connection: Mutex::new(connection),
            active_cluster_name: active_cluster_name.into(),
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
        let _deadline = SqliteOperationDeadline::new(&connection, deadline)?;
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
        let _deadline = SqliteOperationDeadline::new(&connection, deadline)?;
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
        let _deadline = SqliteOperationDeadline::new(&connection, deadline)?;
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
        let uuid = uuid::Uuid::from_slice(&row.id)
            .map_err(|error| MatchingDomainMetadataError::InvalidBlob(error.to_string()))?;
        let thrift = decode_domain_info_blob(&row.data, &row.data_encoding)?;
        thrift_to_metadata(thrift, uuid.to_string(), row, &self.active_cluster_name)
    }
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
    let data_map = thrift.opt_map_str::<String>(38).map_err(protocol_error)?;
    let clusters = thrift
        .opt_list::<String>(36)
        .map_err(protocol_error)?
        .unwrap_or_default();
    let active_clusters_config = blob(&thrift, 64, 66)?;
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
        ..DomainConfiguration::default()
    };
    let replication = DomainReplicationConfiguration {
        active_cluster_name,
        clusters,
        // The encoded bytes are also retained in `active_clusters_config`.
        active_clusters: None,
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
        bad_binaries: blob(&thrift, 39, 40)?,
        isolation_groups: blob(&thrift, 56, 58)?,
        async_workflow_config: blob(&thrift, 60, 62)?,
        active_clusters_config,
    })
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

    fn thrift_blob(fields: Vec<(i16, Value)>) -> Vec<u8> {
        let mut writer = Writer::new();
        write_struct(&mut writer, &Struct { fields });
        writer.into_bytes()
    }

    fn insert_domain(connection: &Connection, id: uuid::Uuid, name: &str, info: Vec<u8>) {
        connection.execute(
            "INSERT INTO domains (shard_id, id, name, is_global, data, data_encoding) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![GO_DEFAULT_SHARD_ID, id.as_bytes(), name, false, info, ENCODING_THRIFT_RW],
        ).unwrap();
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
            (56, Value::Bytes(vec![0, 255, 7])),
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
        assert_eq!(domain.isolation_groups.as_ref().unwrap().data, [0, 255, 7]);
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
        assert!(matches!(
            result,
            Err(MatchingDomainMetadataError::OperationTimedOut)
        ));

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
        let active_config = vec![0x80, 0];
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
