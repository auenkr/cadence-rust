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
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[path = "matching_mysql_native.rs"]
mod native;

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

#[derive(Debug, Clone)]
pub struct MySqlShardConfig {
    pub user: String,
    pub password: String,
    pub database_name: String,
    pub connect_addr: String,
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
    /// Ordered endpoint entries from Go multipleDatabasesConfig.
    pub multiple_databases_config: Vec<MySqlShardConfig>,
    pub encoding: BlobEncoding,
    pub tls: Option<MySqlTlsConfig>,
}

fn validate_mysql_shards(config: &MySqlTaskStoreConfig) -> Result<()> {
    if config.use_multiple_databases {
        if config.n_shards <= 1 || config.multiple_databases_config.len() != config.n_shards {
            return Err(StorageError::BackendConfiguration(format!("MySQL nShards must be greater than one and equal multipleDatabasesConfig length (nShards={}, entries={})", config.n_shards, config.multiple_databases_config.len())));
        }
        if config
            .multiple_databases_config
            .iter()
            .any(|entry| entry.database_name.is_empty() || entry.connect_addr.is_empty())
        {
            return Err(StorageError::BackendConfiguration(
                "MySQL multipleDatabasesConfig entries require databaseName and connectAddr".into(),
            ));
        }
    } else if !config.multiple_databases_config.is_empty() {
        return Err(StorageError::BackendConfiguration(
            "MySQL multipleDatabasesConfig requires useMultipleDatabases=true".into(),
        ));
    }
    Ok(())
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
    async_options: mysql_async::Opts,
    query_runtime: QueryRuntimeOwner,
    max_open: usize,
    max_idle: usize,
    max_lifetime: Option<Duration>,
    state: Mutex<PoolState>,
    available: Condvar,
}

/// Owns the asynchronous query runtime from a non-worker thread. Timed query
/// tasks retain the pool while they clean up, so the final pool reference can
/// be dropped by a Tokio worker. Dropping `Runtime` there would self-join (and
/// panic); the owner thread accepts a shutdown signal and drops it safely.
struct QueryRuntimeOwner {
    handle: tokio::runtime::Handle,
    shutdown: Option<mpsc::Sender<()>>,
    owner_thread: Option<thread::JoinHandle<()>>,
    #[cfg(test)]
    stopped: Arc<AtomicBool>,
}

impl QueryRuntimeOwner {
    fn new() -> std::result::Result<Self, String> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        #[cfg(test)]
        let stopped = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let thread_stopped = stopped.clone();
        let owner_thread = thread::Builder::new()
            .name("cadence-mysql-domain-runtime-owner".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("cadence-mysql-domain-query")
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => {
                        let _ = ready_tx.send(Ok(runtime.handle().clone()));
                        let _ = shutdown_rx.recv();
                        // This thread is outside every Tokio runtime context.
                        drop(runtime);
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                    }
                }
                #[cfg(test)]
                thread_stopped.store(true, Ordering::Release);
            })
            .map_err(|error| {
                format!("could not start MySQL domain-query runtime owner: {error}")
            })?;
        let handle = match ready_rx.recv() {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => {
                let _ = owner_thread.join();
                return Err(format!(
                    "could not start MySQL domain-query runtime: {error}"
                ));
            }
            Err(error) => {
                let _ = owner_thread.join();
                return Err(format!(
                    "MySQL domain-query runtime owner stopped during startup: {error}"
                ));
            }
        };
        Ok(Self {
            handle,
            shutdown: Some(shutdown_tx),
            owner_thread: Some(owner_thread),
            #[cfg(test)]
            stopped,
        })
    }

    fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}

impl Drop for QueryRuntimeOwner {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(owner_thread) = self.owner_thread.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            let _ = owner_thread.join();
        }
        // In an async context the JoinHandle is detached. The owner thread
        // still receives shutdown, drops Runtime, and exits without self-join.
    }
}

#[derive(Debug)]
pub(crate) enum TimedQueryError {
    Timeout,
    MySql(mysql_async::Error),
    Pool(String),
}

impl SyncConnectionPool {
    pub(crate) fn checkout(self: &Arc<Self>) -> Result<ConnectionLease> {
        'checkout: loop {
            let mut state = self.state.lock().expect("MySQL Matching pool lock");
            if let Some(idle) = state.idle.pop() {
                let expired = self
                    .max_lifetime
                    .is_some_and(|lifetime| idle.created_at.elapsed() >= lifetime);
                if expired {
                    state.open -= 1;
                    self.available.notify_one();
                    drop(state);
                    drop(idle);
                    continue 'checkout;
                }
                return Ok(ConnectionLease {
                    pool: self.clone(),
                    connection: Some(idle.connection),
                    created_at: idle.created_at,
                    discard: Arc::new(AtomicBool::new(false)),
                });
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
                        discard: Arc::new(AtomicBool::new(false)),
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

    /// Run a read-only operation on a dedicated connection with a wall-clock
    /// deadline. A timeout cancels only this connection's active statement;
    /// it cannot disturb another operation borrowed from the shared pool.
    /// `operation` must await the driver's protocol future directly; inserting
    /// unrelated awaits could leave a pending result for Conn::drop to drain.
    pub(crate) fn query_until<T, F>(
        self: &Arc<Self>,
        deadline: Instant,
        operation: F,
    ) -> std::result::Result<T, TimedQueryError>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(
                &'a mut mysql_async::Conn,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = std::result::Result<T, mysql_async::Error>>
                        + Send
                        + 'a,
                >,
            > + Send
            + 'static,
    {
        if !self.reserve_until(deadline) {
            return Err(TimedQueryError::Timeout);
        }
        let (result_tx, result_rx) = mpsc::channel();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.release_reserved();
            return Err(TimedQueryError::Timeout);
        }
        let slot = ReservedConnectionSlot(self.clone());
        let options = self.async_options.clone();
        let task = self.query_runtime.spawn(async move {
            let _slot = slot;
            let result = tokio::time::timeout(remaining, async move {
                let mut connection = mysql_async::Conn::new(options).await?;
                let result = operation(&mut connection).await;
                let disconnect = connection.disconnect().await;
                match result {
                    Ok(value) => {
                        disconnect?;
                        Ok(value)
                    }
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(|_| TimedQueryError::Timeout)
            .and_then(|result| result.map_err(TimedQueryError::MySql));
            // A successful result means both protocol cleanup and the shared
            // max-open reservation have completed.
            drop(_slot);
            let _ = result_tx.send(result);
        });

        let remaining = deadline.saturating_duration_since(Instant::now());
        match result_rx.recv_timeout(remaining) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(TimedQueryError::Pool(
                "timed MySQL query worker stopped without a result".into(),
            )),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                task.abort();
                Err(TimedQueryError::Timeout)
            }
        }
    }

    fn reserve_until(&self, deadline: Instant) -> bool {
        let mut state = self.state.lock().expect("MySQL Matching pool lock");
        loop {
            if state.open < self.max_open {
                state.open += 1;
                return true;
            }
            // Async deadline connections cannot reuse sync-driver sockets and
            // are never pooled; retire an idle slot so the shared max-open
            // limit still includes this dedicated connection.
            if let Some(idle) = state.idle.pop() {
                state.open -= 1;
                self.available.notify_one();
                drop(state);
                // Conn::drop sends COM_QUIT synchronously. Retire stale idle
                // sockets off the deadline-bearing caller thread.
                let _ = thread::Builder::new()
                    .name("cadence-mysql-idle-retire".into())
                    .spawn(move || drop(idle));
                state = self.state.lock().expect("MySQL Matching pool lock");
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timeout) = self
                .available
                .wait_timeout(state, remaining)
                .expect("MySQL Matching pool lock");
            state = next;
            if timeout.timed_out() && state.open >= self.max_open && state.idle.is_empty() {
                return false;
            }
        }
    }

    fn release_reserved(&self) {
        let mut state = self.state.lock().expect("MySQL Matching pool lock");
        state.open -= 1;
        self.available.notify_one();
    }
}

struct ReservedConnectionSlot(Arc<SyncConnectionPool>);
impl Drop for ReservedConnectionSlot {
    fn drop(&mut self) {
        self.0.release_reserved();
    }
}

pub(crate) struct ConnectionLease {
    pool: Arc<SyncConnectionPool>,
    connection: Option<Conn>,
    created_at: Instant,
    /// Connectivity failures poison this lease. The Arc lets a transaction
    /// mark it before `Transaction::drop` performs its implicit rollback.
    discard: Arc<AtomicBool>,
}
pub(crate) fn normalize_mysql_error_on_lease(
    error: MySqlError,
    discard: &AtomicBool,
) -> StorageError {
    if error.is_connectivity_error() {
        discard.store(true, Ordering::Release);
    }
    normalize_mysql_error(error)
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
        let discard = self.discard.load(Ordering::Acquire);
        if !discard && !expired && state.idle.len() < self.pool.max_idle {
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

// Statements below mirror go/common/persistence/sql/sqlplugin/mysql/task.go
// (consts at lines 32-104); `matching_mysql_sql_matches_go_task_go` pins them.
const SELECT_TASK_LIST: &str = "SELECT domain_id, range_id, name, task_type, data, data_encoding FROM task_lists WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?";
const LOCK_TASK_LIST: &str = "SELECT range_id FROM task_lists WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ? FOR UPDATE";
const INSERT_TASK_LIST: &str = "INSERT INTO task_lists(shard_id, domain_id, name, task_type, range_id, data, data_encoding) VALUES (?, ?, ?, ?, ?, ?, ?)";
const UPDATE_TASK_LIST: &str = "UPDATE task_lists SET range_id = ?, data = ?, data_encoding = ? WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?";
const DELETE_TASK_LIST: &str = "DELETE FROM task_lists WHERE shard_id=? AND domain_id=? AND name=? AND task_type=? AND range_id=?";
const INSERT_TASK: &str = "INSERT INTO tasks(domain_id, task_list_name, task_type, task_id, data, data_encoding) VALUES(?, ?, ?, ?, ?, ?)";
const SELECT_TASKS: &str = "SELECT task_id, data, data_encoding FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? AND task_id <= ? ORDER BY task_id LIMIT ?";
const COUNT_TASKS: &str = "SELECT count(1) as count FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ?";
const DELETE_TASK: &str =
    "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id = ?";
const RANGE_DELETE_TASKS: &str = "DELETE FROM tasks WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id <= ? ORDER BY domain_id,task_list_name,task_type,task_id LIMIT ?";

pub struct MySqlTaskStore {
    pools: Vec<Arc<SyncConnectionPool>>,
    n_shards: usize,
    encoding: BlobEncoding,
    encoding_source: Option<crate::matching_blob::EncodingSource>,
    isolation: IsolationLevel,
}

impl MySqlTaskStore {
    /// Encode new rows with Go's dynamic `history.serializationEncoding`
    /// instead of the (Go-ignored) SQL `encodingType` setting.
    pub fn with_encoding_source(mut self, source: crate::matching_blob::EncodingSource) -> Self {
        self.encoding_source = Some(source);
        self
    }

    fn write_encoding(&self) -> Result<BlobEncoding> {
        match &self.encoding_source {
            Some(source) => Ok(source.current()?),
            None => Ok(self.encoding),
        }
    }

    fn pool(&self, key: &TaskListKey) -> &Arc<SyncConnectionPool> {
        let index = mysql_database_index(self.database_shard(key) as usize, self.pools.len());
        &self.pools[index]
    }

    pub fn connect(config: MySqlTaskStoreConfig) -> Result<Self> {
        validate_mysql_shards(&config)?;
        if config.n_shards == 0 || config.n_shards > i32::MAX as usize {
            return Err(StorageError::BackendConfiguration(format!(
                "MySQL Matching nShards must be in 1..={}, got {}",
                i32::MAX,
                config.n_shards
            )));
        }
        let pools = Self::create_mysql_pools(&config)?;
        Self::connect_with_pools(config, pools)
    }

    /// Construct the task adapter and metadata reader over one shared pool,
    /// keeping their combined connections within Go's configured maxConns.
    pub fn connect_with_domain_metadata(
        config: MySqlTaskStoreConfig,
        active_cluster_name: impl Into<String>,
    ) -> Result<(Self, crate::matching_domain_mysql::MySqlDomainMetadataStore)> {
        validate_mysql_shards(&config)?;
        if config.n_shards == 0 || config.n_shards > i32::MAX as usize {
            return Err(StorageError::BackendConfiguration(format!(
                "MySQL Matching nShards must be in 1..={}, got {}",
                i32::MAX,
                config.n_shards
            )));
        }
        let pools = Self::create_mysql_pools(&config)?;
        let store = Self::connect_with_pools(config, pools.clone())?;
        let metadata = crate::matching_domain_mysql::MySqlDomainMetadataStore::from_shared_pool(
            pools[0].clone(),
            active_cluster_name,
        );
        Ok((store, metadata))
    }

    fn connect_with_pools(
        config: MySqlTaskStoreConfig,
        pools: Vec<Arc<SyncConnectionPool>>,
    ) -> Result<Self> {
        let store = Self {
            pools,
            n_shards: config.n_shards,
            encoding: config.encoding,
            encoding_source: None,
            isolation: configured_isolation(&config.connect_attributes)?,
        };
        store.validate_schema()?;
        Ok(store)
    }

    pub(crate) fn create_mysql_pool(
        config: &MySqlTaskStoreConfig,
    ) -> Result<Arc<SyncConnectionPool>> {
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
        let async_options = mysql_async_options(config)?;
        let query_runtime = QueryRuntimeOwner::new().map_err(StorageError::BackendConfiguration)?;
        Ok(Arc::new(SyncConnectionPool {
            options,
            async_options,
            query_runtime,
            max_open,
            max_idle,
            max_lifetime: config.max_conn_lifetime.filter(|value| !value.is_zero()),
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }))
    }

    fn create_mysql_pools(config: &MySqlTaskStoreConfig) -> Result<Vec<Arc<SyncConnectionPool>>> {
        validate_mysql_shards(config)?;
        if !config.use_multiple_databases {
            return Ok(vec![Self::create_mysql_pool(config)?]);
        }
        let mut pools = Vec::with_capacity(config.multiple_databases_config.len());
        for (index, endpoint) in config.multiple_databases_config.iter().enumerate() {
            let mut shard = config.clone();
            shard.user = endpoint.user.clone();
            shard.password = endpoint.password.clone();
            shard.database_name = endpoint.database_name.clone();
            shard.connect_addr = endpoint.connect_addr.clone();
            shard.use_multiple_databases = false;
            shard.multiple_databases_config.clear();
            pools.push(Self::create_mysql_pool(&shard).map_err(|error| {
                StorageError::BackendConfiguration(format!(
                    "failed connecting MySQL multipleDatabasesConfig endpoint {index}: {error}"
                ))
            })?);
        }
        Ok(pools)
    }

    fn database_shard(&self, key: &TaskListKey) -> i32 {
        // Task rows have no shard_id column; this value only chooses the
        // configured physical DB and is stored in task_lists.shard_id.
        crate::matching_task_store::go_db_shard_id(&key.domain_id, &key.name, self.n_shards)
    }

    fn validate_schema(&self) -> Result<()> {
        for pool in &self.pools {
            let mut connection = pool.checkout()?;
            let discard = connection.discard.clone();
            connection
            .exec_drop(
                "SELECT shard_id, domain_id, name, task_type, range_id, data, data_encoding FROM task_lists LIMIT 0",
                (),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
            connection
            .exec_drop(
                "SELECT domain_id, task_list_name, task_type, task_id, data, data_encoding FROM tasks LIMIT 0",
                (),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
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
                .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
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
        }
        Ok(())
    }

    fn read_task_list_row(
        &self,
        connection: &mut ConnectionLease,
        key: &TaskListKey,
    ) -> Result<Option<(i64, Vec<u8>, String)>> {
        let shard_id = self.database_shard(key);
        let discard = connection.discard.clone();
        // Columns: domain_id, range_id, name, task_type, data, data_encoding.
        type Row = (Vec<u8>, i64, String, i32, Vec<u8>, String);
        let row: Option<Row> = connection
            .exec_first(
                SELECT_TASK_LIST,
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        // Columns: domain_id, range_id, name, task_type, data, data_encoding.
        Ok(row.map(|(_, range_id, _, _, data, encoding)| (range_id, data, encoding)))
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
        discard: &AtomicBool,
        shard_id: i32,
        key: &TaskListKey,
        expected_range: i64,
    ) -> Result<()> {
        let range: Option<i64> = transaction
            .exec_first(
                LOCK_TASK_LIST,
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, discard))?;
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
        let encoding = self.write_encoding()?;
        Ok((
            encoding.encode(&blob::encode_task_list(data))?,
            encoding.as_str(),
        ))
    }
}

fn mysql_database_index(logical_shard: usize, endpoint_count: usize) -> usize {
    if endpoint_count == 1 {
        0
    } else {
        logical_shard
    }
}

impl MatchingTaskStore for MySqlTaskStore {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        // Go sqlplugin/mysql SelectLatestConfig on the default shard.
        let mut lease = self.pools[0].checkout().map_err(|e| e.to_string())?;
        let row: Option<(i64, Vec<u8>, String)> = lease
            .exec_first(
                "SELECT version, data, data_encoding FROM cluster_config WHERE row_type = ? ORDER BY version LIMIT 1;",
                (row_type,),
            )
            .map_err(|e| e.to_string())?;
        Ok(row.map(|(version, data, encoding)| crate::config_store::ConfigStoreRow {
            version: -version,
            data,
            encoding,
        }))
    }

    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        let mut connection = self.pool(key).checkout()?;
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
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
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
                        INSERT_TASK_LIST,
                        (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, 0_i64, bytes, encoding),
                    )
                    .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
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
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        // Any early return below drops the transaction and asks mysql 26 to
        // roll it back. Since Drop intentionally hides rollback errors, never
        // recycle this connection unless commit succeeds.
        discard.store(true, Ordering::Release);
        Self::lock_and_check(&mut transaction, &discard, shard_id, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        let (bytes, encoding) = self.encode_task_list(&current.data)?;
        transaction
            .exec_drop(
                UPDATE_TASK_LIST,
                (current.range_id, bytes, encoding, shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        if transaction.affected_rows() != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "lease update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            });
        }
        transaction
            .commit()
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        discard.store(false, Ordering::Release);
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
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        discard.store(true, Ordering::Release);
        Self::lock_and_check(&mut transaction, &discard, shard_id, key, range)?;
        let (bytes, encoding) = self.encode_task_list(&data)?;
        transaction
            .exec_drop(
                UPDATE_TASK_LIST,
                (range, bytes, encoding, shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        if transaction.affected_rows() != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!(
                    "task-list update affected {} rows instead of 1",
                    transaction.affected_rows()
                ),
            });
        }
        transaction
            .commit()
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        discard.store(false, Ordering::Release);
        Ok(())
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()> {
        let shard_id = self.database_shard(key);
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        let mut transaction = connection
            .start_transaction(TxOpts::default().set_isolation_level(Some(self.isolation)))
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        discard.store(true, Ordering::Release);
        // Go encodes every row in the request before opening the transaction;
        // an unsupported encoding therefore fails before any statement runs.
        let encoding = self.write_encoding()?;
        for (domain, task) in tasks {
            // Go stamps CreatedTimestamp with the CreateTasks request time.
            let mut data = task.data.for_sql().map_err(StorageError::MalformedSqlMatchingRunId)?;
            data.created_nanos = now_nanos;
            let bytes = encoding.encode(&blob::encode_task(&data).map_err(StorageError::BackendConfiguration)?)?;
            transaction
                .exec_drop(
                    INSERT_TASK,
                    (domain.as_bytes().as_slice(), &key.name, key.task_type, task.task_id, bytes, encoding.as_str()),
                )
                .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        }
        Self::lock_and_check(&mut transaction, &discard, shard_id, key, range)?;
        transaction
            .commit()
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        discard.store(false, Ordering::Release);
        Ok(())
    }

    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        let rows: Vec<(i64, Vec<u8>, String)> = connection
            .exec(
                SELECT_TASKS,
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, read_level, max_read_level, batch_size),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
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
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        let size: Option<i64> = connection
            .exec_first(
                COUNT_TASKS,
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, ack_level),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        size.ok_or_else(|| StorageError::Backend {
            kind: BackendErrorKind::Permanent,
            message: "COUNT(*) returned no row".into(),
        })
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                DELETE_TASK,
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, task_id),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))
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
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                RANGE_DELETE_TASKS,
                (key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, inclusive_task_id, limit),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        usize::try_from(connection.affected_rows()).map_err(|error| StorageError::Backend {
            kind: BackendErrorKind::Permanent,
            message: format!("MySQL affected-row count did not fit usize: {error}"),
        })
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let shard_id = self.database_shard(key);
        let mut connection = self.pool(key).checkout()?;
        let discard = connection.discard.clone();
        connection
            .exec_drop(
                DELETE_TASK_LIST,
                (shard_id, key.domain_id.as_bytes().as_slice(), &key.name, key.task_type, range),
            )
            .map_err(|error| normalize_mysql_error_on_lease(error, &discard))?;
        let affected = connection.affected_rows();
        if affected != 1 {
            return Err(StorageError::DeleteCount(affected as usize));
        }
        Ok(())
    }
}

impl std::fmt::Debug for MySqlTaskStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MySqlTaskStore").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl crate::SemaphorePersistence for MySqlTaskStore {
    async fn create_semaphore(
        &self,
        domain: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> std::result::Result<crate::SemaphoreMetadata, crate::SemaphoreError> {
        crate::SqlUnsupportedSemaphorePersistence
            .create_semaphore(domain, name, size, bucket_size)
            .await
    }

    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> std::result::Result<crate::SemaphoreMetadata, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence
            .get_semaphore_metadata(domain_id, name)
            .await
    }

    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> std::result::Result<crate::SemaphoreBucketPage, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence
            .scan_semaphore_bucket(domain_id, name, bucket, page_size, next_page_token)
            .await
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> std::result::Result<String, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence
            .get_semaphore_ownership_by_token(domain_id, name, bucket, token_id)
            .await
    }

    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> std::result::Result<crate::GrantOutcome, crate::AcquireError> {
        crate::SqlUnsupportedSemaphorePersistence
            .grant_semaphore_token(domain_id, name, bucket, token_id, owner_id)
            .await
    }
}

pub(crate) fn mysql_options(config: &MySqlTaskStoreConfig) -> Result<Opts> {
    validate_mysql_tls_config(config)?;
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
            )));
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
            "tls" if !value.is_empty() => tls_mode = Some(value.to_ascii_lowercase()),
            "tls" => {}
            // Go forces these attributes true in the MySQL DSN. parseTime has
            // no impact here because this task store reads only binary blobs;
            // `clientFoundRows` is mapped to the client capability above.
            "parsetime" | "clientfoundrows" | "multistatements" => {}
            unsupported => {
                return Err(StorageError::BackendConfiguration(format!(
                    "unsupported MySQL connect attribute {unsupported:?}; refusing to ignore it"
                )));
            }
        }
    }
    builder = builder
        .tcp_connect_timeout(connect_timeout.and_then(GoTimeout::connect_timeout))
        .read_timeout(read_timeout.and_then(GoTimeout::socket_timeout))
        .write_timeout(write_timeout.and_then(GoTimeout::socket_timeout));

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
                ));
            }
            "skip-verify" => {
                ssl = ssl
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true)
            }
            other => {
                return Err(StorageError::BackendConfiguration(format!(
                    "unsupported MySQL tls connect attribute {other:?}"
                )));
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
                        std::path::PathBuf::from(&tls.cert_file),
                        std::path::PathBuf::from(&tls.key_file),
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

/// Build options for the non-pooled async connection used by deadline-bound
/// domain-cache reads. mysql_async has no socket read/write timeout options;
/// the caller enforces its monotonic deadline by dropping this connection's
/// in-flight future.
fn mysql_async_options(config: &MySqlTaskStoreConfig) -> Result<mysql_async::Opts> {
    validate_mysql_tls_config(config)?;
    let mut builder = mysql_async::OptsBuilder::default()
        .user(Some(config.user.clone()))
        .pass(Some(config.password.clone()))
        .db_name(Some(config.database_name.clone()))
        .client_found_rows(true);
    let mut connect_timeout = None;
    let mut read_timeout = None;
    let mut write_timeout = None;
    for (key, value) in &config.connect_attributes {
        match key.to_ascii_lowercase().as_str() {
            "timeout" => connect_timeout = Some(parse_go_duration(value)?),
            "readtimeout" => read_timeout = Some(parse_go_duration(value)?),
            "writetimeout" => write_timeout = Some(parse_go_duration(value)?),
            _ => {}
        }
    }
    builder = builder
        .tcp_connect_timeout(connect_timeout.and_then(GoTimeout::connect_timeout))
        .read_timeout(read_timeout.and_then(GoTimeout::socket_timeout))
        .write_timeout(write_timeout.and_then(GoTimeout::socket_timeout));
    let protocol = if config.connect_protocol.is_empty() {
        "tcp"
    } else {
        config.connect_protocol.as_str()
    };
    match protocol {
        "tcp" => {
            if config.connect_addr.is_empty() {
                builder = builder.ip_or_hostname("127.0.0.1");
            } else {
                let (host, port) = split_host_port(&config.connect_addr)?;
                builder = builder.ip_or_hostname(host).tcp_port(port);
            }
        }
        "unix" => builder = builder.socket(Some(config.connect_addr.clone())),
        other => {
            return Err(StorageError::BackendConfiguration(format!(
                "unsupported MySQL connectProtocol {other:?}"
            )));
        }
    }

    let tls_mode = config
        .connect_attributes
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("tls"))
        .map(|(_, value)| value.to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
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
        let mut ssl = mysql_async::SslOpts::default();
        match tls_mode.as_str() {
            "true" | "required" | "tls-custom" => {}
            "preferred" => {
                return Err(StorageError::BackendConfiguration(
                    "MySQL tls=preferred fallback semantics are unsupported; use required TLS"
                        .into(),
                ));
            }
            "skip-verify" => {
                ssl = ssl
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true);
            }
            other => {
                return Err(StorageError::BackendConfiguration(format!(
                    "unsupported MySQL tls connect attribute {other:?}"
                )));
            }
        }
        if tls_mode == "tls-custom" {
            if let Some(tls) = tls_config.filter(|tls| tls.enabled) {
                if !tls.ca_file.is_empty() {
                    ssl = ssl.with_root_certs(vec![std::path::PathBuf::from(&tls.ca_file).into()]);
                }
                if !tls.cert_file.is_empty() && !tls.key_file.is_empty() {
                    ssl = ssl.with_client_identity(Some(mysql_async::ClientIdentity::new(
                        std::path::PathBuf::from(&tls.cert_file).into(),
                        std::path::PathBuf::from(&tls.key_file).into(),
                    )));
                }
                if !tls.enable_host_verification {
                    ssl = ssl
                        .with_danger_skip_domain_validation(true)
                        .with_danger_accept_invalid_certs(true);
                }
            }
        }
        builder = builder.ssl_opts(Some(ssl));
    }
    Ok(mysql_async::Opts::from(builder))
}

fn validate_mysql_tls_config(config: &MySqlTaskStoreConfig) -> Result<()> {
    let Some(tls) = config.tls.as_ref().filter(|tls| tls.enabled) else {
        if config.connect_attributes.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case("tls") && value.eq_ignore_ascii_case("tls-custom")
        }) {
            return Err(StorageError::BackendConfiguration(
                "MySQL tls-custom requires an enabled SQL TLS configuration".into(),
            ));
        }
        return Ok(());
    };
    let protocol = if config.connect_protocol.is_empty() {
        "tcp"
    } else {
        config.connect_protocol.as_str()
    };
    if protocol != "tcp" {
        return Err(StorageError::BackendConfiguration(
            "Go MySQL TLS configuration requires TCP connectProtocol".into(),
        ));
    }
    let address = config.connect_addr.as_str();
    let Some((host, port)) = address.rsplit_once(':') else {
        return Err(StorageError::BackendConfiguration(format!(
            "Go MySQL TLS configuration requires connectAddr host:port, got {address:?}"
        )));
    };
    if host.is_empty() || port.is_empty() || port.parse::<u16>().is_err() {
        return Err(StorageError::BackendConfiguration(format!(
            "Go MySQL TLS configuration requires connectAddr host:port, got {address:?}"
        )));
    }
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return Err(StorageError::BackendConfiguration(format!(
            "Go MySQL TLS configuration requires bracketed IPv6 connectAddr, got {address:?}"
        )));
    }
    if !tls.ca_file.is_empty() {
        std::fs::read(&tls.ca_file).map_err(|error| {
            StorageError::BackendConfiguration(format!(
                "could not read MySQL CA file {:?}: {error}",
                tls.ca_file
            ))
        })?;
    }
    Ok(())
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
        let Some((host, suffix)) = rest.split_once(']') else {
            return Err(StorageError::BackendConfiguration(format!(
                "invalid MySQL TCP connectAddr {address:?}"
            )));
        };
        if host.is_empty() || host.contains('[') || host.contains(']') {
            return Err(StorageError::BackendConfiguration(format!(
                "invalid MySQL TCP connectAddr {address:?}"
            )));
        }
        let port = match suffix {
            "" => 3306,
            suffix if suffix.starts_with(':') => suffix[1..].parse::<u16>().map_err(|_| {
                StorageError::BackendConfiguration(format!("invalid MySQL TCP port in {address:?}"))
            })?,
            _ => {
                return Err(StorageError::BackendConfiguration(format!(
                    "invalid MySQL TCP connectAddr {address:?}"
                )));
            }
        };
        return Ok((host.to_owned(), port));
    }
    if !address.contains(':') {
        if address.is_empty() || address.contains('[') || address.contains(']') {
            return Err(StorageError::BackendConfiguration(format!(
                "invalid MySQL TCP connectAddr {address:?}"
            )));
        }
        return Ok((address.to_owned(), 3306));
    }
    if address.bytes().filter(|byte| *byte == b':').count() > 1 {
        // Go's ensureHavePort treats an unbracketed IPv6 literal as a host
        // with no parseable port and applies 3306.
        return Ok((address.to_owned(), 3306));
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        unreachable!("checked for a colon")
    };
    if host.is_empty() || host.contains('[') || host.contains(']') {
        return Err(StorageError::BackendConfiguration(format!(
            "invalid MySQL TCP connectAddr {address:?}"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        StorageError::BackendConfiguration(format!("invalid MySQL TCP port in {address:?}"))
    })?;
    Ok((host.to_owned(), port))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GoTimeout {
    negative: bool,
    magnitude: Duration,
}

impl GoTimeout {
    fn connect_timeout(self) -> Option<Duration> {
        (!self.negative && !self.magnitude.is_zero()).then_some(self.magnitude)
    }

    fn socket_timeout(self) -> Option<Duration> {
        if self.magnitude.is_zero() {
            None
        } else if self.negative {
            // Go turns a negative absolute deadline into immediate I/O expiry.
            // mysql 26 only accepts relative socket timeouts, so one nanosecond
            // is the closest supported behavior.
            Some(Duration::from_nanos(1))
        } else {
            Some(self.magnitude)
        }
    }
}

fn parse_go_duration(value: &str) -> Result<GoTimeout> {
    // Match Go time.ParseDuration's signed sequence of decimal components.
    // Keep the total in nanoseconds so conversion cannot panic or silently
    // saturate at Duration's range boundary.
    let input = value;
    let invalid =
        || StorageError::BackendConfiguration(format!("invalid MySQL timeout duration {value:?}"));
    if input.is_empty() {
        return Err(invalid());
    }
    let (negative, rest) = match input.as_bytes()[0] {
        b'+' => (false, &input[1..]),
        b'-' => (true, &input[1..]),
        _ => (false, input),
    };
    if rest == "0" {
        return Ok(GoTimeout {
            negative: false,
            magnitude: Duration::ZERO,
        });
    }
    let bytes = rest.as_bytes();
    let mut index = 0;
    let mut total_nanos = 0u128;
    let mut components = 0;
    while index < bytes.len() {
        let number_start = index;
        let mut integer_digits = 0;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
            integer_digits += 1;
        }
        let mut fractional_start = index;
        let mut fractional_digits = 0;
        if index < bytes.len() && bytes[index] == b'.' {
            index += 1;
            fractional_start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
                fractional_digits += 1;
            }
        }
        if integer_digits + fractional_digits == 0 {
            return Err(invalid());
        }
        let integer = if integer_digits == 0 {
            0
        } else {
            rest[number_start..number_start + integer_digits]
                .parse::<u128>()
                .map_err(|_| invalid())?
        };
        let mut unit_end = index;
        while unit_end < bytes.len()
            && (bytes[unit_end].is_ascii_alphabetic() || bytes[unit_end] >= 0x80)
        {
            unit_end += 1;
        }
        let unit = &rest[index..unit_end];
        let unit_nanos = match unit {
            "ns" => 1u128,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(invalid()),
        };
        let whole_nanos = integer.checked_mul(unit_nanos).ok_or_else(invalid)?;
        let fractional_nanos = if fractional_digits == 0 {
            0
        } else {
            // Go uses float64 for the fractional component and truncates the
            // resulting nanoseconds. This also naturally accepts arbitrarily
            // long fractional tails, which contribute no extra precision.
            let fractional_text = &rest[fractional_start..index];
            let retained = &fractional_text[..fractional_text.len().min(18)];
            let fraction = retained.parse::<f64>().map_err(|_| invalid())?;
            let scale = 10_f64.powi(retained.len() as i32);
            let nanos = fraction * (unit_nanos as f64 / scale);
            if !nanos.is_finite() || nanos < 0.0 || nanos > i64::MAX as f64 {
                return Err(invalid());
            }
            nanos as u128
        };
        total_nanos = total_nanos
            .checked_add(whole_nanos)
            .and_then(|n| n.checked_add(fractional_nanos))
            .ok_or_else(invalid)?;
        let maximum = if negative {
            (i64::MAX as u128) + 1
        } else {
            i64::MAX as u128
        };
        if total_nanos > maximum {
            return Err(invalid());
        }
        index = unit_end;
        components += 1;
    }
    if components == 0 {
        return Err(invalid());
    }
    let seconds = u64::try_from(total_nanos / 1_000_000_000).map_err(|_| invalid())?;
    let nanos = (total_nanos % 1_000_000_000) as u32;
    Ok(GoTimeout {
        negative: negative && total_nanos != 0,
        magnitude: Duration::new(seconds, nanos),
    })
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
    fn mysql_physical_database_follows_go_logical_shard_index() {
        assert_eq!(mysql_database_index(0, 2), 0);
        assert_eq!(mysql_database_index(1, 2), 1);
        assert_eq!(mysql_database_index(3, 1), 0);
    }

    fn timed_mysql_test_config(max_conns: i64) -> MySqlTaskStoreConfig {
        MySqlTaskStoreConfig {
            user: std::env::var("CADENCE_MYSQL_USER").unwrap_or_else(|_| "root".into()),
            password: std::env::var("CADENCE_MYSQL_PASSWORD").unwrap_or_else(|_| "cadence".into()),
            database_name: std::env::var("CADENCE_MYSQL_DATABASE")
                .unwrap_or_else(|_| "cadence".into()),
            connect_protocol: "tcp".into(),
            connect_addr: std::env::var("CADENCE_MYSQL_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:33307".into()),
            connect_attributes: Default::default(),
            max_conns,
            max_idle_conns: 2,
            max_conn_lifetime: None,
            n_shards: 1,
            use_multiple_databases: false,
            multiple_databases_config: Vec::new(),
            encoding: BlobEncoding::ThriftRW,
            tls: None,
        }
    }

    #[test]
    fn mysql_matching_schema_contract_matches_go_v8_primary_keys() {
        let schema = include_str!("../../../../go/schema/mysql/v8/cadence/schema.sql");
        let task_lists = schema
            .split_once("CREATE TABLE task_lists (")
            .unwrap()
            .1
            .split_once(");")
            .unwrap()
            .0;
        assert!(task_lists.contains("PRIMARY KEY (shard_id, domain_id, name, task_type)"));
        let tasks = schema
            .split_once("CREATE TABLE tasks (")
            .unwrap()
            .1
            .split_once(");")
            .unwrap()
            .0;
        assert!(tasks.contains("PRIMARY KEY (domain_id, task_list_name, task_type, task_id)"));
        assert!(!tasks.contains("shard_id"));
    }

    #[test]
    fn matching_mysql_sql_matches_go_task_go() {
        use crate::matching_task_store::go_sql_pin::{assert_in_go, assert_named};
        let go = include_str!("../../../../go/common/persistence/sql/sqlplugin/mysql/task.go");
        // Positional statements: verbatim in Go (task.go:62-104).
        for statement in [
            SELECT_TASK_LIST,
            LOCK_TASK_LIST,
            DELETE_TASK_LIST,
            SELECT_TASKS,
            COUNT_TASKS,
            DELETE_TASK,
            RANGE_DELETE_TASKS,
        ] {
            assert_in_go(go, statement);
        }
        // Named statements (task.go:32-47, 89-91); Go binds struct fields in
        // declaration order, which is the positional order used here.
        assert_named(
            go,
            "INTO task_lists(shard_id, domain_id, name, task_type, range_id, data, data_encoding) VALUES (:shard_id, :domain_id, :name, :task_type, :range_id, :data, :data_encoding)",
            INSERT_TASK_LIST.trim_start_matches("INSERT "),
            false,
        );
        assert_named(
            go,
            "tasks(domain_id, task_list_name, task_type, task_id, data, data_encoding) VALUES(:domain_id, :task_list_name, :task_type, :task_id, :data, :data_encoding)",
            INSERT_TASK.trim_start_matches("INSERT INTO "),
            false,
        );
        assert_named(
            go,
            "UPDATE task_lists SET range_id = :range_id, data = :data, data_encoding = :data_encoding WHERE shard_id = :shard_id AND domain_id = :domain_id AND name = :name AND task_type = :task_type",
            UPDATE_TASK_LIST,
            false,
        );
    }

    #[test]
    fn farmhash_32_uses_go_compatible_fingerprint() {
        // The Go and Rust implementations both use FarmHash32; this published
        // vector guards against accidentally switching hash families.
        assert_eq!(farmhash::hash32(b"hello world"), 430_397_466);
    }

    #[test]
    fn go_tcp_connect_addresses_default_missing_port_to_3306() {
        assert_eq!(
            split_host_port("db.example").unwrap(),
            ("db.example".into(), 3306)
        );
        assert_eq!(
            split_host_port("2001:db8::1").unwrap(),
            ("2001:db8::1".into(), 3306)
        );
        assert_eq!(
            split_host_port("[2001:db8::1]").unwrap(),
            ("2001:db8::1".into(), 3306)
        );
        assert_eq!(
            split_host_port("[2001:db8::1]:3307").unwrap(),
            ("2001:db8::1".into(), 3307)
        );
        assert!(split_host_port("[2001:db8::1]junk").is_err());
        assert!(split_host_port("db.example:not-a-port").is_err());
    }

    #[test]
    fn mysql_timeout_duration_matches_go_composite_and_boundary_forms() {
        let positive = |magnitude| GoTimeout {
            negative: false,
            magnitude,
        };
        let cases = [
            ("0", positive(Duration::ZERO)),
            ("+1.5h", positive(Duration::from_secs(5_400))),
            ("1h30m", positive(Duration::from_secs(5_400))),
            ("1.000000001s", positive(Duration::new(1, 1))),
            ("500µs", positive(Duration::from_micros(500))),
            ("500μs", positive(Duration::from_micros(500))),
            ("1ns", positive(Duration::from_nanos(1))),
            ("0.0000000001s", positive(Duration::ZERO)),
            (
                "9223372036.854775807s",
                positive(Duration::new(9_223_372_036, 854_775_807)),
            ),
            (
                "0.00000000000000000000000000000000000000000000000001s",
                positive(Duration::ZERO),
            ),
            ("-0s", positive(Duration::ZERO)),
            (
                "-1s",
                GoTimeout {
                    negative: true,
                    magnitude: Duration::from_secs(1),
                },
            ),
        ];
        for (source, expected) in cases {
            assert_eq!(parse_go_duration(source).unwrap(), expected, "{source}");
        }
        for source in ["", "1", " 1s", "1s2", "1fortnight", "9223372036.854775808s"] {
            assert!(parse_go_duration(source).is_err(), "{source}");
        }
        assert_eq!(parse_go_duration("-1s").unwrap().connect_timeout(), None);
        assert_eq!(
            parse_go_duration("-1s").unwrap().socket_timeout(),
            Some(Duration::from_nanos(1))
        );
    }

    #[test]
    fn mysql_async_options_apply_go_socket_timeout_attributes() {
        let mut config = timed_mysql_test_config(1);
        config.connect_attributes = [
            ("TIMEOUT".into(), "2s".into()),
            ("readTimeout".into(), "100ms".into()),
            ("writeTimeout".into(), "250ms".into()),
        ]
        .into_iter()
        .collect();
        let opts = mysql_async_options(&config).unwrap();
        assert_eq!(opts.tcp_connect_timeout(), Some(Duration::from_secs(2)));
        assert_eq!(opts.read_timeout(), Some(Duration::from_millis(100)));
        assert_eq!(opts.write_timeout(), Some(Duration::from_millis(250)));

        config.connect_attributes = [
            ("timeout".into(), "0".into()),
            ("readTimeout".into(), "0".into()),
            ("writeTimeout".into(), "0".into()),
        ]
        .into_iter()
        .collect();
        let opts = mysql_async_options(&config).unwrap();
        assert_eq!(opts.tcp_connect_timeout(), None);
        assert_eq!(opts.read_timeout(), None);
        assert_eq!(opts.write_timeout(), None);
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
    fn connectivity_errors_poison_only_their_connection_lease() {
        let first = AtomicBool::new(false);
        let other = AtomicBool::new(false);
        let network_error = MySqlError::IoError(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "server gone",
        ));
        normalize_mysql_error_on_lease(network_error, &first);
        assert!(first.load(Ordering::Acquire));
        assert!(!other.load(Ordering::Acquire));

        let server_error = MySqlError::MySqlError(mysql::MySqlError {
            state: "HY000".into(),
            message: "ordinary SQL failure".into(),
            code: 1146,
        });
        normalize_mysql_error_on_lease(server_error, &other);
        assert!(!other.load(Ordering::Acquire));
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_cancels_only_its_dedicated_connection() {
        use mysql_async::prelude::Queryable as _;

        let config = timed_mysql_test_config(2);
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let mut unrelated = pool.checkout().unwrap();
        let mut transaction = unrelated.start_transaction(TxOpts::default()).unwrap();
        let started = Instant::now();
        let result = pool.query_until(Instant::now() + Duration::from_millis(250), |connection| {
            Box::pin(async move { connection.query_first::<u8, _>("SELECT SLEEP(10)").await })
        });
        assert!(matches!(result, Err(TimedQueryError::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while pool.state.lock().unwrap().open != 1 {
            assert!(
                Instant::now() < cleanup_deadline,
                "timed query slot was not released after cancellation"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(pool.state.lock().unwrap().open <= 2);
        let value: Option<u8> = transaction.query_first("SELECT 7").unwrap();
        assert_eq!(value, Some(7));
        transaction.commit().unwrap();
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_releases_single_pool_slot_after_cancellation() {
        use mysql_async::prelude::Queryable as _;

        let mut config = timed_mysql_test_config(1);
        config.max_idle_conns = 0;
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let result = pool.query_until(Instant::now() + Duration::from_millis(250), |connection| {
            Box::pin(async move { connection.query_first::<u8, _>("SELECT SLEEP(10)").await })
        });
        assert!(matches!(result, Err(TimedQueryError::Timeout)));

        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while pool.state.lock().unwrap().open != 0 {
            assert!(Instant::now() < cleanup_deadline, "timed query slot leaked");
            thread::sleep(Duration::from_millis(10));
        }
        let value = pool
            .query_until(Instant::now() + Duration::from_secs(2), |connection| {
                Box::pin(async move { connection.query_first::<u8, _>("SELECT 7").await })
            })
            .unwrap();
        assert_eq!(value, Some(7));
        assert_eq!(pool.state.lock().unwrap().open, 0);
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_cancels_while_draining_a_slow_result_set() {
        use mysql_async::prelude::Queryable as _;

        let config = timed_mysql_test_config(1);
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let result = pool.query_until(Instant::now() + Duration::from_millis(250), |connection| {
            Box::pin(async move { connection.query::<u8, _>(
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) SELECT SLEEP(0.1) FROM seq",
            ).await })
        });
        assert!(matches!(result, Err(TimedQueryError::Timeout)));
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while pool.state.lock().unwrap().open != 0 {
            assert!(
                Instant::now() < cleanup_deadline,
                "slow-result query slot leaked"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let value = pool
            .query_until(Instant::now() + Duration::from_secs(2), |connection| {
                Box::pin(async move { connection.query_first::<u8, _>("SELECT 9").await })
            })
            .unwrap();
        assert_eq!(value, Some(9));
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_bounds_connect_failure_and_releases_slot() {
        let mut config = timed_mysql_test_config(1);
        config.connect_addr = "127.0.0.1:1".into();
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let started = Instant::now();
        let result = pool.query_until(Instant::now() + Duration::from_millis(250), |_| {
            Box::pin(async { Ok::<(), mysql_async::Error>(()) })
        });
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(
            result,
            Err(TimedQueryError::MySql(_)) | Err(TimedQueryError::Timeout)
        ));
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while pool.state.lock().unwrap().open != 0 {
            assert!(
                Instant::now() < cleanup_deadline,
                "failed connect leaked its slot"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn timed_domain_query_cancels_a_stalled_mysql_handshake() {
        use std::io::Read;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let peer_thread = thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            let _ = accepted_tx.send(());
            peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut byte = [0u8; 1];
            peer.read(&mut byte)
        });
        let mut config = timed_mysql_test_config(1);
        config.connect_addr = address;
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let runtime_stopped = pool.query_runtime.stopped.clone();
        let weak_pool = Arc::downgrade(&pool);
        let started = Instant::now();
        let query_pool = pool.clone();
        let query_thread = thread::spawn(move || {
            query_pool.query_until(Instant::now() + Duration::from_millis(250), |_| {
                Box::pin(async { Ok::<(), mysql_async::Error>(()) })
            })
        });
        accepted_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timed query never reached the stalled handshake");
        let result = query_thread.join().unwrap();
        assert!(matches!(result, Err(TimedQueryError::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        // The worker's cancelled task may still own the last pool reference.
        // Dropping the store-side reference immediately exercises that path.
        drop(pool);
        match peer_thread.join().unwrap() {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => panic!("client did not close stalled handshake: {result:?}"),
        }
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while !runtime_stopped.load(Ordering::Acquire) {
            assert!(
                Instant::now() < cleanup_deadline,
                "query runtime did not shut down after its final pool reference dropped"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            weak_pool.upgrade().is_none(),
            "timed query retained its pool"
        );
    }

    #[test]
    fn query_runtime_can_be_dropped_from_a_tokio_context() {
        let pool = MySqlTaskStore::create_mysql_pool(&timed_mysql_test_config(1)).unwrap();
        let runtime_stopped = pool.query_runtime.stopped.clone();
        let weak_pool = Arc::downgrade(&pool);
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        caller_runtime.block_on(async move {
            drop(pool);
        });

        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while !runtime_stopped.load(Ordering::Acquire) {
            assert!(
                Instant::now() < cleanup_deadline,
                "query runtime owner did not finish shutdown after async-context drop"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(weak_pool.upgrade().is_none(), "runtime retained the pool");
    }

    #[test]
    fn query_runtime_final_owner_can_drop_on_its_worker() {
        let pool = MySqlTaskStore::create_mysql_pool(&timed_mysql_test_config(1)).unwrap();
        let runtime_stopped = pool.query_runtime.stopped.clone();
        let weak_pool = Arc::downgrade(&pool);
        let handle = pool.query_runtime.handle.clone();
        let worker_owner = pool.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (drop_tx, drop_rx) = mpsc::channel();
        handle.spawn(async move {
            let _ = ready_tx.send(());
            let _ = drop_rx.recv();
            // The caller drops its reference before releasing this task, so
            // QueryRuntimeOwner::drop runs on its own Tokio worker thread.
            drop(worker_owner);
        });

        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("runtime worker did not start the final-owner test");
        drop(pool);
        drop_tx.send(()).unwrap();

        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while !runtime_stopped.load(Ordering::Acquire) {
            assert!(
                Instant::now() < cleanup_deadline,
                "query runtime owner did not finish worker-context shutdown"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(weak_pool.upgrade().is_none(), "runtime retained the pool");
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_completes_before_deadline_and_disconnects() {
        use mysql_async::prelude::Queryable as _;

        let pool = MySqlTaskStore::create_mysql_pool(&timed_mysql_test_config(1)).unwrap();
        let value = pool
            .query_until(Instant::now() + Duration::from_secs(2), |connection| {
                Box::pin(async move {
                    connection
                        .query_first::<u8, _>("SELECT IF(SLEEP(0.05) = 0, 11, 12)")
                        .await
                })
            })
            .unwrap();
        assert_eq!(value, Some(11));
        let follow_up = pool
            .query_until(Instant::now() + Duration::from_secs(2), |connection| {
                Box::pin(async move { connection.query_first::<u8, _>("SELECT 13").await })
            })
            .unwrap();
        assert_eq!(follow_up, Some(13));
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_resets_read_timeout_for_each_socket_operation() {
        use mysql_async::prelude::Queryable as _;

        let mut config = timed_mysql_test_config(1);
        config.connect_attributes = [("readTimeout".into(), "200ms".into())]
            .into_iter()
            .collect();
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let started = Instant::now();
        let values = pool
            .query_until(Instant::now() + Duration::from_secs(3), |connection| {
                Box::pin(async move {
                    let first = connection
                        .query_first::<u8, _>("SELECT IF(SLEEP(0.12) = 0, 1, 0)")
                        .await?;
                    let second = connection
                        .query_first::<u8, _>("SELECT IF(SLEEP(0.12) = 0, 2, 0)")
                        .await?;
                    Ok((first, second))
                })
            })
            .unwrap();
        assert_eq!(values, (Some(1), Some(2)));
        assert!(
            started.elapsed() > Duration::from_millis(200),
            "combined operation completed before the configured per-read timeout"
        );
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server"]
    fn timed_domain_query_bounds_a_stalled_socket_read() {
        use mysql_async::prelude::Queryable as _;

        let mut config = timed_mysql_test_config(1);
        config.connect_attributes = [("readTimeout".into(), "150ms".into())]
            .into_iter()
            .collect();
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let started = Instant::now();
        let result = pool.query_until(Instant::now() + Duration::from_secs(2), |connection| {
            Box::pin(async move { connection.query_first::<u8, _>("SELECT SLEEP(5)").await })
        });
        assert!(matches!(result, Err(TimedQueryError::MySql(_))));
        assert!(started.elapsed() < Duration::from_secs(1));
        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while pool.state.lock().unwrap().open != 0 {
            assert!(
                Instant::now() < cleanup_deadline,
                "socket read timeout did not release its dedicated pool slot"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    #[ignore = "requires the opt-in MySQL interoperability server with TLS enabled"]
    fn mysql_async_tls_uses_go_socket_timeout_options() {
        use mysql_async::prelude::Queryable as _;

        let mut config = timed_mysql_test_config(1);
        config.connect_attributes = [
            ("tls".into(), "skip-verify".into()),
            ("timeout".into(), "3s".into()),
            ("readTimeout".into(), "3s".into()),
            ("writeTimeout".into(), "3s".into()),
        ]
        .into_iter()
        .collect();
        let pool = MySqlTaskStore::create_mysql_pool(&config).unwrap();
        let status: Option<(String, String)> = pool
            .query_until(Instant::now() + Duration::from_secs(5), |connection| {
                Box::pin(async move {
                    connection
                        .exec_first("SHOW STATUS LIKE 'Ssl_cipher'", ())
                        .await
                })
            })
            .unwrap();
        assert!(status.is_some_and(|(_, cipher)| !cipher.is_empty()));
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
            multiple_databases_config: Vec::new(),
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
