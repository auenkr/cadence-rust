//! Matching task-store adapter for Cadence's existing MySQL v8 schema.
//!
//! This module never creates or migrates tables. Its sync connection pool
//! mirrors Go database/sql's max-open, max-idle, and connection-lifetime
//! behavior; a checked-out connection is only recycled after its transaction
//! has ended. Production config assembly is a separate server integration.

use crate::{
    matching_blob::{self as blob, BlobEncoding, TaskListData},
    matching_sqlite::StorageError,
    matching_task_store::{
        AllocatedTask, BackendErrorKind, MatchingTaskStore, Result, TaskListKey, TaskListRecord,
    },
};
use mysql::{
    consts::CapabilityFlags, prelude::Queryable, ClientIdentity, Conn, Error as MySqlError,
    IsolationLevel, Opts, OptsBuilder, SslOpts, Transaction, TxOpts,
};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

const DEFAULT_MAX_IDLE_CONNS: usize = 2;

#[derive(Debug, Clone)]
pub struct MySqlTlsConfig {
    pub enabled: bool,
    pub ssl_mode: String,
    pub ca_file: String,
    pub cert_file: String,
    pub key_file: String,
    pub enable_host_verification: bool,
    pub require_client_auth: bool,
}

/// The subset of Cadence SQL connection config accepted by this adapter.
/// Unknown MySQL connect attributes fail closed instead of being discarded.
#[derive(Debug, Clone)]
pub struct MySqlTaskStoreConfig {
    pub user: String,
    pub password: String,
    pub database_name: String,
    pub connect_protocol: String,
    pub connect_addr: String,
    pub connect_attributes: std::collections::HashMap<String, String>,
    /// Cadence Go's `maxConns`: nonpositive means unlimited.
    pub max_conns: i64,
    /// Cadence only calls SetMaxIdleConns for values greater than zero;
    /// otherwise database/sql retains its default of two idle connections.
    pub max_idle_conns: i64,
    /// Go expires an aged connection when returned to the pool, never during an
    /// active transaction. `None`/zero means no lifetime limit.
    pub max_conn_lifetime: Option<Duration>,
    /// Go nShards selects the database; it is not a task-list shard count.
    pub n_shards: usize,
    /// Multiple DBs require the separate multi-shard adapter/selector slice.
    pub use_multiple_databases: bool,
    pub encoding: BlobEncoding,
    pub tls: Option<MySqlTlsConfig>,
}

struct IdleConnection {
    connection: Conn,
    created_at: Instant,
}
#[derive(Default)]
struct PoolState {
    idle: Vec<IdleConnection>,
    open: usize,
}

pub(crate) struct SyncConnectionPool {
    options: Opts,
    max_open: usize,
    max_idle: usize,
    max_lifetime: Option<Duration>,
    state: Mutex<PoolState>,
    available: Condvar,
}

impl SyncConnectionPool {
    pub(crate) fn checkout(self: &Arc<Self>) -> Result<ConnectionLease> {
        loop {
            let mut state = self.state.lock().expect("MySQL Matching pool lock");
            while let Some(idle) = state.idle.pop() {
                let expired = self
                    .max_lifetime
                    .is_some_and(|lifetime| idle.created_at.elapsed() >= lifetime);
                if expired {
                    state.open -= 1;
                    self.available.notify_one();
                    continue;
                }
                let mut connection = idle.connection;
                drop(state);
                if connection.ping().is_ok() {
                    return Ok(ConnectionLease {
                        pool: self.clone(),
                        connection: Some(connection),
                        created_at: idle.created_at,
                    });
                }
                let mut state = self.state.lock().expect("MySQL Matching pool lock");
                state.open -= 1;
                self.available.notify_one();
                drop(connection);
                continue;
            }
            if state.open < self.max_open {
                // Reserve the slot before connecting so simultaneous callers
                // cannot exceed Go's max-open setting.
                state.open += 1;
                drop(state);
                return match Conn::new(self.options.clone()) {
                    Ok(connection) => Ok(ConnectionLease {
                        pool: self.clone(),
                        connection: Some(connection),
                        created_at: Instant::now(),
                    }),
                    Err(error) => {
                        let mut state = self.state.lock().expect("MySQL Matching pool lock");
                        state.open -= 1;
                        self.available.notify_one();
                        Err(normalize_mysql_error(error))
                    }
                };
            }
            state = self
                .available
                .wait(state)
                .expect("MySQL Matching pool lock");
            drop(state);
        }
    }
}

pub(crate) struct ConnectionLease {
    pool: Arc<SyncConnectionPool>,
    connection: Option<Conn>,
    created_at: Instant,
}
impl Deref for ConnectionLease {
    type Target = Conn;
    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("connection lease is active")
    }
}
impl DerefMut for ConnectionLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("connection lease is active")
    }
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        let mut state = self.pool.state.lock().expect("MySQL Matching pool lock");
        let expired = self
            .pool
            .max_lifetime
            .is_some_and(|lifetime| self.created_at.elapsed() >= lifetime);
        if !expired && state.idle.len() < self.pool.max_idle {
            state.idle.push(IdleConnection {
                connection,
                created_at: self.created_at,
            });
        } else {
            state.open -= 1;
        }
        self.pool.available.notify_one();
    }
}

pub struct MySqlTaskStore {
    pool: Arc<SyncConnectionPool>,
    n_shards: usize,
    encoding: BlobEncoding,
    isolation: IsolationLevel,
}

impl MySqlTaskStore {
    pub fn connect(config: MySqlTaskStoreConfig) -> Result<Self> {
        if config.use_multiple_databases {
            return Err(StorageError::BackendConfiguration(
                "MySQL Matching multipleDatabasesConfig is not supported by this single-database adapter".into(),
            ));
        }
        if config.n_shards == 0 || config.n_shards > i32::MAX as usize {
            return Err(StorageError::BackendConfiguration(format!(
                "MySQL Matching nShards must be in 1..={}, got {}",
                i32::MAX,
                config.n_shards
            )));
        }
        let pool = Self::create_mysql_pool(&config)?;
        let store = Self {
            pool,
            n_shards: config.n_shards,
            encoding: config.encoding,
            isolation: configured_isolation(&config.connect_attributes)?,
        };
        store.validate_schema()?;
        Ok(store)
    }

    pub(crate) fn create_mysql_pool(
        config: &MySqlTaskStoreConfig,
    ) -> Result<Arc<SyncConnectionPool>> {
        if config.use_multiple_databases {
            return Err(StorageError::BackendConfiguration(
                "MySQL Matching multipleDatabasesConfig is not supported by this single-database adapter".into(),
            ));
        }
        let max_idle = if config.max_idle_conns > 0 {
            usize::try_from(config.max_idle_conns).unwrap_or(usize::MAX)
        } else {
            DEFAULT_MAX_IDLE_CONNS
        };
        let max_open = if config.max_conns <= 0 {
            usize::MAX
        } else {
            usize::try_from(config.max_conns).unwrap_or(usize::MAX)
        };
        let max_idle = max_idle.min(max_open);
        let options = mysql_options(config)?;
        Ok(Arc::new(SyncConnectionPool {
            options,
            max_open,
            max_idle,
            max_lifetime: config.max_conn_lifetime.filter(|value| !value.is_zero()),
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }))
    }

    fn database_shard(&self, key: &TaskListKey) -> i32 {
        // Go hashes the canonical UUID string plus underscore plus task-list
        // name. Task rows have no shard_id column; this value only chooses the
        // configured physical DB and is stored in task_lists.shard_id.
        let input = format!("{}_{}", key.domain_id, key.name);
        (farmhash::hash32(input.as_bytes()) % self.n_shards as u32) as i32
    }

    fn validate_schema(&self) -> Result<()> {
        let mut connection = self.pool.checkout()?;
        connection
            .exec_drop(
                "SELECT shard_id, domain_id, name, task_type, range_id, data, data_encoding FROM task_lists LIMIT 0",
                (),
            )
            .map_err(normalize_mysql_error)?;
        connection
            .exec_drop(
                "SELECT domain_id, task_list_name, task_type, task_id, data, data_encoding FROM tasks LIMIT 0",
                (),
            )
            .map_err(normalize_mysql_error)?;
        for (table, expected) in [
            (
                "task_lists",
                vec!["shard_id", "domain_id", "name", "task_type"],
            ),
            (
                "tasks",
                vec!["domain_id", "task_list_name", "task_type", "task_id"],
            ),
        ] {
            let columns: Vec<String> = connection
                .exec(
                    "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position",
                    (table,),
                )
                .map_err(normalize_mysql_error)?;
            if columns.len() != expected.len()
                || columns
                    .iter()
                    .zip(expected.iter())
                    .any(|(actual, expected)| actual.as_str() != *expected)
            {
                return Err(StorageError::BackendSchema(format!(
                    "{table} primary key is {columns:?}, expected {expected:?}"
                )));
            }
        }
        Ok(())
    }

    fn read_task_list_row(
        &self,
        connection: &mut ConnectionLease,
        key: &TaskListKey,
    ) -> Result<Option<(i64, Vec<u8>, String)>> {
        let shard_id = self.database_shard(key);
        connection
            .exec_first(
                "SELECT range_id, data, data_encoding FROM task_lists WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?",
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(normalize_mysql_error)
    }

    fn decode_task_list_row(row: (i64, Vec<u8>, String)) -> Result<TaskListRecord> {
        let (range_id, bytes, encoding) = row;
        let encoded = BlobEncoding::parse(&encoding)?.decode(&bytes)?;
        Ok(TaskListRecord {
            range_id,
            data: blob::decode_task_list(&encoded)?,
        })
    }

    fn read_task_list(
        &self,
        connection: &mut ConnectionLease,
        key: &TaskListKey,
    ) -> Result<Option<TaskListRecord>> {
        let row = self.read_task_list_row(connection, key)?;
        row.map(|(range_id, bytes, encoding)| {
            Self::decode_task_list_row((range_id, bytes, encoding))
        })
        .transpose()
    }

    fn lock_and_check(
        transaction: &mut Transaction<'_>,
        shard_id: i32,
        key: &TaskListKey,
        expected_range: i64,
    ) -> Result<()> {
        let range: Option<i64> = transaction
            .exec_first(
                "SELECT range_id FROM task_lists WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ? FOR UPDATE",
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(normalize_mysql_error)?;
        match range {
            Some(range) if range == expected_range => Ok(()),
            Some(range) => Err(StorageError::ConditionFailed(format!(
                "Task list range ID was {range} when it was should have been {expected_range}"
            ))),
            None => Err(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            )),
        }
    }

    fn encode_task_list(&self, data: &TaskListData) -> Result<(Vec<u8>, &'static str)> {
        Ok((
            self.encoding.encode(&blob::encode_task_list(data))?,
            self.encoding.as_str(),
        ))
    }
}

impl MatchingTaskStore for MySqlTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        let mut connection = self.pool.checkout()?;
        self.read_task_list(&mut connection, key)?
            .ok_or(StorageError::NotFound)
    }

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord> {
        let shard_id = self.database_shard(key);
        let mut connection = self.pool.checkout()?;
        let mut current = match self.read_task_list_row(&mut connection, key)? {
            Some((range_id, bytes, encoding)) => {
                if previous_range > 0 && previous_range != range_id {
                    // Go checks the requested range before decoding persisted
                    // Thrift data; preserve that error precedence.
                    return Err(StorageError::ConditionFailed(format!(
                        "leaseTaskList:renew failed:taskList:{}, taskListType:{}, haveRangeID:0, gotRangeID:{}",
                        key.name, key.task_type, range_id
                    )));
                }
                Self::decode_task_list_row((range_id, bytes, encoding))?
            }
            None => {
                let data = TaskListData {
                    kind,
                    last_updated_nanos: now_nanos,
                    ..Default::default()
                };
                let (bytes, encoding) = self.encode_task_list(&data)?;
                // Match Go's select-then-insert creation race: the insert is
                // outside the lease transaction. A duplicate insert is a
                // backend error, not a stale-range condition failure.
                connection
                    .exec_drop(
                        "INSERT INTO task_lists (shard_id, domain_id, name, task_type, range_id, data, data_encoding) VALUES (?, ?, ?, ?, 0, ?, ?)",
                        (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, bytes, encoding),
                    )
                    .map_err(normalize_mysql_error)?;
                TaskListRecord { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            // The missing-row path inserts range zero before this check, just
            // as Go does, and leaves the inserted row behind on mismatch.
            return Err(StorageError::ConditionFailed(format!(
                "leaseTaskList:renew failed:taskList:{}, taskListType:{}, haveRangeID:0, gotRangeID:{}",
                key.name, key.task_type, current.range_id
            )));
        }
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(normalize_mysql_error)?;
        Self::lock_and_check(&mut transaction, shard_id, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        let (bytes, encoding) = self.encode_task_list(&current.data)?;
        transaction
            .exec_drop(
                "UPDATE task_lists SET range_id = ?, data = ?, data_encoding = ? WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?",
                (current.range_id, bytes, encoding, shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(normalize_mysql_error)?;
        if transaction.affected_rows() != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "lease update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            });
        }
        transaction.commit().map_err(normalize_mysql_error)?;
        // Go reports the requested kind but does not overwrite stored kind.
        current.data.kind = kind;
        Ok(current)
    }

    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        mut data: TaskListData,
        now_nanos: i64,
    ) -> Result<()> {
        data.last_updated_nanos = now_nanos;
        data.expiry_nanos = if matches!(data.kind, 1 | 2) {
            now_nanos.wrapping_add(86_400_000_000_000)
        } else {
            0
        };
        let shard_id = self.database_shard(key);
        let mut connection = self.pool.checkout()?;
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(normalize_mysql_error)?;
        Self::lock_and_check(&mut transaction, shard_id, key, range)?;
        let (bytes, encoding) = self.encode_task_list(&data)?;
        transaction
            .exec_drop(
                "UPDATE task_lists SET range_id = ?, data = ?, data_encoding = ? WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?",
                (range, bytes, encoding, shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(normalize_mysql_error)?;
        if transaction.affected_rows() != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "task-list update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            });
        }
        transaction.commit().map_err(normalize_mysql_error)
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()> {
        let shard_id = self.database_shard(key);
        let mut connection = self.pool.checkout()?;
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(normalize_mysql_error)?;
        for (domain, task) in tasks {
            let bytes = self.encoding.encode(&blob::encode_task(&task.data))?;
            transaction
                .exec_drop(
                    "INSERT INTO tasks (domain_id, task_list_name, task_type, task_id, data, data_encoding) VALUES (?, ?, ?, ?, ?, ?)",
                    (domain.as_bytes().as_slice(), &key.name, key.task_type, task.task_id, bytes, self.encoding.as_str()),
                )
                .map_err(normalize_mysql_error)?;
        }
        Self::lock_and_check(&mut transaction, shard_id, key, range)?;
        transaction.commit().map_err(normalize_mysql_error)
    }

    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        let mut connection = self.pool.checkout()?;
        let rows: Vec<(i64, Vec<u8>, String)> = connection
            .exec(
                "SELECT task_id, data, data_encoding FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? AND task_id <= ? ORDER BY task_id LIMIT ?",
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, read_level, max_read_level, batch_size),
            )
            .map_err(normalize_mysql_error)?;
        rows.into_iter()
            .map(|(task_id, bytes, encoding)| {
                let encoded = BlobEncoding::parse(&encoding)?.decode(&bytes)?;
                Ok(AllocatedTask {
                    task_id,
                    data: blob::decode_task(&encoded)?,
                })
            })
            .collect()
    }

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        let mut connection = self.pool.checkout()?;
        let size: Option<i64> = connection
            .exec_first(
                "SELECT COUNT(*) FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ?",
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, ack_level),
            )
            .map_err(normalize_mysql_error)?;
        size.ok_or_else(|| StorageError::Backend {
            kind: BackendErrorKind::Permanent,
            message: "COUNT(*) returned no row".into(),
        })
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        let mut connection = self.pool.checkout()?;
        connection
            .exec_drop(
                "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id = ?",
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, task_id),
            )
            .map_err(normalize_mysql_error)
    }

    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize> {
        if limit == 0 {
            return Err(StorageError::MissingLimit);
        }
        let mut connection = self.pool.checkout()?;
        connection
            .exec_drop(
                "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id <= ? ORDER BY domain_id, task_list_name, task_type, task_id LIMIT ?",
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, inclusive_task_id, limit),
            )
            .map_err(normalize_mysql_error)?;
        usize::try_from(connection.affected_rows()).map_err(|error| StorageError::Backend {
            kind: BackendErrorKind::Permanent,
            message: format!("MySQL affected-row count did not fit usize: {error}"),
        })
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let shard_id = self.database_shard(key);
        let mut connection = self.pool.checkout()?;
        connection
            .exec_drop(
                "DELETE FROM task_lists WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ? AND range_id = ?",
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, range),
            )
            .map_err(normalize_mysql_error)?;
        let affected = connection.affected_rows();
        if affected != 1 {
            return Err(StorageError::DeleteCount(affected as usize));
        }
        Ok(())
    }
}

pub(crate) fn mysql_options(config: &MySqlTaskStoreConfig) -> Result<Opts> {
    let mut builder = OptsBuilder::default()
        .user(Some(config.user.clone()))
        .pass(Some(config.password.clone()))
        .db_name(Some(config.database_name.clone()))
        // Go forces clientFoundRows=true for every MySQL connection.
        .additional_capabilities(CapabilityFlags::CLIENT_FOUND_ROWS);
    let protocol = if config.connect_protocol.is_empty() {
        "tcp"
    } else {
        config.connect_protocol.as_str()
    };
    match protocol {
        "tcp" => {
            if config.connect_addr.is_empty() {
                builder = builder.ip_or_hostname(Some("127.0.0.1"));
            } else {
                let (host, port) = split_host_port(&config.connect_addr)?;
                builder = builder.ip_or_hostname(Some(host)).tcp_port(port);
            }
        }
        "unix" => builder = builder.socket(Some(config.connect_addr.clone())),
        other => {
            return Err(StorageError::BackendConfiguration(format!(
                "unsupported MySQL connectProtocol {other:?}"
            )))
        }
    }

    let mut connect_timeout = None;
    let mut read_timeout = None;
    let mut write_timeout = None;
    let mut tls_mode = None;
    for (key, value) in &config.connect_attributes {
        match key.to_ascii_lowercase().as_str() {
            "transaction_isolation" | "tx_isolation" => {
                let _ = parse_isolation(value)?;
            }
            "timeout" => connect_timeout = Some(parse_go_duration(value)?),
            "readtimeout" => read_timeout = Some(parse_go_duration(value)?),
            "writetimeout" => write_timeout = Some(parse_go_duration(value)?),
            "tls" => tls_mode = Some(value.to_ascii_lowercase()),
            // Go forces these attributes true in the MySQL DSN. parseTime has
            // no impact here because this task store reads only binary blobs;
            // `clientFoundRows` is mapped to the client capability above.
            "parsetime" | "clientfoundrows" | "multistatements" => {}
            unsupported => {
                return Err(StorageError::BackendConfiguration(format!(
                    "unsupported MySQL connect attribute {unsupported:?}; refusing to ignore it"
                )))
            }
        }
    }
    builder = builder
        .tcp_connect_timeout(connect_timeout.filter(|value: &Duration| !value.is_zero()))
        .read_timeout(read_timeout.filter(|value: &Duration| !value.is_zero()))
        .write_timeout(write_timeout.filter(|value: &Duration| !value.is_zero()));

    let tls_mode = tls_mode.unwrap_or_else(|| {
        if let Some(tls) = config.tls.as_ref().filter(|tls| tls.enabled) {
            if tls.ssl_mode.is_empty() {
                "tls-custom".into()
            } else {
                tls.ssl_mode.to_ascii_lowercase()
            }
        } else {
            "false".into()
        }
    });
    if tls_mode != "false" {
        let tls_config = config.tls.as_ref();
        let mut ssl = SslOpts::default();
        match tls_mode.as_str() {
            "true" | "required" | "tls-custom" => {}
            "preferred" => {
                return Err(StorageError::BackendConfiguration(
                    "MySQL tls=preferred fallback semantics are unsupported; use required TLS"
                        .into(),
                ))
            }
            "skip-verify" => {
                ssl = ssl
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true)
            }
            other => {
                return Err(StorageError::BackendConfiguration(format!(
                    "unsupported MySQL tls connect attribute {other:?}"
                )))
            }
        }
        if tls_mode == "tls-custom" {
            if let Some(tls) = tls_config.filter(|tls| tls.enabled) {
                if !tls.ca_file.is_empty() {
                    ssl = ssl.with_root_cert_path(Some(std::path::PathBuf::from(&tls.ca_file)));
                }
                // Go loads a client identity only when both paths are set;
                // either path alone is ignored.
                if !tls.cert_file.is_empty() && !tls.key_file.is_empty() {
                    ssl = ssl.with_client_identity(Some(ClientIdentity::new(
                        tls.cert_file.clone(),
                        tls.key_file.clone(),
                    )));
                }
                if !tls.enable_host_verification {
                    ssl = ssl
                        .with_danger_skip_domain_validation(true)
                        .with_danger_accept_invalid_certs(true);
                }
                // Go's MySQL plugin uses connectAddr as SNI and does not apply
                // TLS.ServerName or RequireClientAuth on its client config.
            }
        } else if tls_mode == "skip-verify" {
            ssl = ssl
                .with_danger_skip_domain_validation(true)
                .with_danger_accept_invalid_certs(true);
        }
        builder = builder.ssl_opts(Some(ssl));
    }

    // Isolation is set per transaction below, matching the Go MySQL DSN's
    // transaction_isolation session setting without relying on server default.
    Ok(Opts::from(builder))
}

fn configured_isolation(
    attributes: &std::collections::HashMap<String, String>,
) -> Result<IsolationLevel> {
    let value = attributes
        .iter()
        .find(|(key, _)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "transaction_isolation" | "tx_isolation"
            )
        })
        .map(|(_, value)| value.as_str())
        .unwrap_or("READ-COMMITTED");
    parse_isolation(value)
}

fn parse_isolation(value: &str) -> Result<IsolationLevel> {
    match value.to_ascii_uppercase().replace('_', "-").as_str() {
        "READ-UNCOMMITTED" => Ok(IsolationLevel::ReadUncommitted),
        "READ-COMMITTED" => Ok(IsolationLevel::ReadCommitted),
        "REPEATABLE-READ" => Ok(IsolationLevel::RepeatableRead),
        "SERIALIZABLE" => Ok(IsolationLevel::Serializable),
        other => Err(StorageError::BackendConfiguration(format!(
            "unsupported MySQL transaction isolation {other:?}"
        ))),
    }
}

fn split_host_port(address: &str) -> Result<(String, u16)> {
    if let Some(rest) = address.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return Err(StorageError::BackendConfiguration(format!(
                "invalid MySQL TCP connectAddr {address:?}"
            )));
        };
        let port = port.parse::<u16>().map_err(|_| {
            StorageError::BackendConfiguration(format!("invalid MySQL TCP port in {address:?}"))
        })?;
        return Ok((host.to_owned(), port));
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return Err(StorageError::BackendConfiguration(format!(
            "MySQL TCP connectAddr must contain host:port: {address:?}"
        )));
    };
    if host.is_empty() {
        return Err(StorageError::BackendConfiguration(format!(
            "MySQL TCP connectAddr has an empty host: {address:?}"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        StorageError::BackendConfiguration(format!("invalid MySQL TCP port in {address:?}"))
    })?;
    Ok((host.to_owned(), port))
}

fn parse_go_duration(value: &str) -> Result<Duration> {
    let (number, unit) = value.trim().split_at(
        value
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(value.len()),
    );
    let amount = number.parse::<f64>().map_err(|_| {
        StorageError::BackendConfiguration(format!("invalid MySQL timeout duration {value:?}"))
    })?;
    let multiplier = match unit {
        "ns" => 1e-9,
        "us" | "µs" => 1e-6,
        "ms" => 1e-3,
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        _ => {
            return Err(StorageError::BackendConfiguration(format!(
                "unsupported MySQL timeout duration unit in {value:?}"
            )))
        }
    };
    let seconds = amount * multiplier;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(StorageError::BackendConfiguration(format!(
            "invalid MySQL timeout duration {value:?}"
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn normalize_mysql_error(error: MySqlError) -> StorageError {
    let (kind, message) = match error {
        MySqlError::MySqlError(error) => {
            let code = error.code;
            // Go's mysql plugin classification in mysql/db.go.
            let kind = if matches!(code, 1159 | 1161 | 1205 | 1613 | 3024 | 3133 | 3699) {
                BackendErrorKind::Timeout
            } else if matches!(code, 1040 | 1203 | 1637 | 3634) {
                BackendErrorKind::Throttled
            } else {
                // Go converts all remaining SQL operation errors to
                // InternalServiceError, which persistence treats as transient.
                BackendErrorKind::Internal
            };
            (kind, error.to_string())
        }
        MySqlError::DriverError(mysql::DriverError::Timeout) => (
            BackendErrorKind::Timeout,
            "MySQL operation timed out".into(),
        ),
        MySqlError::DriverError(mysql::DriverError::ConnectTimeout) => (
            BackendErrorKind::Timeout,
            "MySQL connection timed out".into(),
        ),
        other @ (MySqlError::IoError(_) | MySqlError::DriverError(_)) => {
            (BackendErrorKind::Internal, other.to_string())
        }
        other => (BackendErrorKind::Internal, other.to_string()),
    };
    StorageError::Backend { kind, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_matching_schema_contract_matches_go_v8_primary_keys() {
        let schema = include_str!("../../../../go/schema/mysql/v8/cadence/schema.sql");
        let task_lists = schema
            .split_once("CREATE TABLE task_lists(")
            .unwrap()
            .1
            .split_once(");")
            .unwrap()
            .0;
        assert!(task_lists.contains("PRIMARY KEY (shard_id, domain_id, name, task_type)"));
        let tasks = schema
            .split_once("CREATE TABLE tasks(")
            .unwrap()
            .1
            .split_once(");")
            .unwrap()
            .0;
        assert!(tasks.contains("PRIMARY KEY (domain_id, task_list_name, task_type, task_id)"));
        assert!(!tasks.contains("shard_id"));
    }

    #[test]
    fn farmhash_32_uses_go_compatible_fingerprint() {
        // The Go and Rust implementations both use FarmHash32; this published
        // vector guards against accidentally switching hash families.
        assert_eq!(farmhash::hash32(b"hello world"), 430_397_466);
    }

    #[test]
    fn mysql_driver_errors_keep_go_retry_categories() {
        let error = |code| {
            MySqlError::MySqlError(mysql::MySqlError {
                state: "HY000".into(),
                message: "oracle".into(),
                code,
            })
        };
        assert!(matches!(
            normalize_mysql_error(error(1205)),
            StorageError::Backend {
                kind: BackendErrorKind::Timeout,
                ..
            }
        ));
        assert!(matches!(
            normalize_mysql_error(error(1040)),
            StorageError::Backend {
                kind: BackendErrorKind::Throttled,
                ..
            }
        ));
        assert!(matches!(
            normalize_mysql_error(error(1146)),
            StorageError::Backend {
                kind: BackendErrorKind::Internal,
                ..
            }
        ));
    }

    #[test]
    fn unsupported_mysql_connect_attributes_fail_closed() {
        let mut config = MySqlTaskStoreConfig {
            user: "u".into(),
            password: "p".into(),
            database_name: "cadence".into(),
            connect_protocol: "tcp".into(),
            connect_addr: "localhost:3306".into(),
            connect_attributes: std::collections::HashMap::new(),
            max_conns: 0,
            max_idle_conns: 0,
            max_conn_lifetime: None,
            n_shards: 1,
            use_multiple_databases: false,
            encoding: BlobEncoding::ThriftRW,
            tls: None,
        };
        config
            .connect_attributes
            .insert("unknownDriverOption".into(), "value".into());
        assert!(matches!(
            mysql_options(&config),
            Err(StorageError::BackendConfiguration(_))
        ));
    }
}
