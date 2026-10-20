//! Matching task-store adapter for Cadence's existing PostgreSQL schema.
//!
//! This adapter never creates or migrates schema. It supports plain PostgreSQL
//! transport (`sslmode=disable`) and one or more configured physical database
//! shards. TLS modes are rejected until PostgreSQL TLS roots/client identity
//! can be mapped exactly to Go's lib/pq behavior.

use crate::{
    matching_blob::{self as blob, BlobEncoding, TaskListData},
    matching_sqlite::StorageError,
    matching_task_store::{
        AllocatedTask, BackendErrorKind, MatchingTaskStore, Result, TaskListKey, TaskListRecord,
    },
};
use postgres::{config::SslMode, types::ToSql, Client, Config, NoTls};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

const DEFAULT_MAX_IDLE_CONNS: usize = 2;

/// One PostgreSQL endpoint from Go's `MultipleDatabasesConfig` list.
#[derive(Debug, Clone)]
pub struct PostgresShardConfig {
    /// Go's `ConnectAddr` in host:port form.
    pub connect_addr: String,
    pub user: String,
    pub password: String,
    /// Empty database names are rejected by Go's task plugin.
    pub database_name: String,
    /// Go `ConnectAttributes`; this initial adapter accepts `sslmode=disable`,
    /// `connect_timeout` (seconds), `application_name`, and `options`.
    pub connect_attributes: std::collections::HashMap<String, String>,
}

/// PostgreSQL Matching task-store settings shared by each database shard.
#[derive(Debug, Clone)]
pub struct PostgresTaskStoreConfig {
    /// One endpoint for the singleton SQL driver or exactly `n_shards`
    /// endpoints for Go's multiple-databases driver.
    pub shards: Vec<PostgresShardConfig>,
    /// Go `NumShards`, used for the task-list row hash even with one DB.
    pub n_shards: usize,
    /// Go `UseMultipleDatabases` controls physical database routing. When
    /// false, every logical shard shares `shards[0]` but retains its row key.
    pub use_multiple_databases: bool,
    /// Go `maxConns`: nonpositive means unlimited.
    pub max_conns: i64,
    /// Go only sets a positive max-idle value; otherwise database/sql defaults
    /// to two idle connections.
    pub max_idle_conns: i64,
    /// Go expires connections when returned to the pool, never during use.
    pub max_conn_lifetime: Option<Duration>,
    pub encoding: BlobEncoding,
}

struct IdleConnection {
    client: Client,
    created_at: Instant,
}

#[derive(Default)]
struct PoolState {
    idle: Vec<IdleConnection>,
    open: usize,
}

struct PostgresConnectionPool {
    options: Config,
    max_open: usize,
    max_idle: usize,
    max_lifetime: Option<Duration>,
    state: Mutex<PoolState>,
    available: Condvar,
}

impl PostgresConnectionPool {
    fn new(
        options: Config,
        max_open: usize,
        max_idle: usize,
        max_lifetime: Option<Duration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            max_open,
            max_idle,
            max_lifetime,
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        })
    }

    fn checkout(self: &Arc<Self>) -> Result<PostgresLease> {
        'checkout: loop {
            let mut state = self.state.lock().expect("PostgreSQL Matching pool lock");
            while let Some(idle) = state.idle.pop() {
                let expired = self
                    .max_lifetime
                    .is_some_and(|lifetime| idle.created_at.elapsed() >= lifetime);
                if expired || idle.client.is_closed() {
                    state.open -= 1;
                    self.available.notify_one();
                    drop(state);
                    drop(idle);
                    continue 'checkout;
                }
                return Ok(PostgresLease {
                    pool: self.clone(),
                    client: Some(idle.client),
                    created_at: idle.created_at,
                    discard: false,
                });
            }
            if state.open < self.max_open {
                state.open += 1;
                drop(state);
                return match self.options.connect(NoTls) {
                    Ok(client) => Ok(PostgresLease {
                        pool: self.clone(),
                        client: Some(client),
                        created_at: Instant::now(),
                        discard: false,
                    }),
                    Err(error) => {
                        let mut state = self.state.lock().expect("PostgreSQL Matching pool lock");
                        state.open -= 1;
                        self.available.notify_one();
                        Err(normalize_postgres_error(error))
                    }
                };
            }
            state = self
                .available
                .wait(state)
                .expect("PostgreSQL Matching pool lock");
            drop(state);
        }
    }
}

struct PostgresLease {
    pool: Arc<PostgresConnectionPool>,
    client: Option<Client>,
    created_at: Instant,
    discard: bool,
}

impl PostgresLease {
    fn poison_if_closed(&mut self, error: &postgres::Error) {
        self.discard |= error.is_closed();
    }
}

impl Deref for PostgresLease {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("PostgreSQL lease is active")
    }
}

impl DerefMut for PostgresLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("PostgreSQL lease is active")
    }
}

impl Drop for PostgresLease {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let mut state = self
            .pool
            .state
            .lock()
            .expect("PostgreSQL Matching pool lock");
        let expired = self
            .pool
            .max_lifetime
            .is_some_and(|lifetime| self.created_at.elapsed() >= lifetime);
        if !self.discard && !client.is_closed() && !expired && state.idle.len() < self.pool.max_idle
        {
            state.idle.push(IdleConnection {
                client,
                created_at: self.created_at,
            });
        } else {
            state.open -= 1;
            drop(state);
            drop(client);
            self.pool.available.notify_one();
        }
    }
}

/// PostgreSQL store over Go's existing cadence task tables.
pub struct PostgresTaskStore {
    pools: Vec<Arc<PostgresConnectionPool>>,
    n_shards: usize,
    use_multiple_databases: bool,
    encoding: BlobEncoding,
}

impl PostgresTaskStore {
    pub fn connect(config: PostgresTaskStoreConfig) -> Result<Self> {
        validate_shard_config(&config)?;
        if config.n_shards > u32::MAX as usize {
            return Err(configuration_error(
                "n_shards exceeds FarmHash routing range".into(),
            ));
        }
        let max_open = if config.max_conns <= 0 {
            usize::MAX
        } else {
            usize::try_from(config.max_conns).unwrap_or(usize::MAX)
        };
        let configured_idle = if config.max_idle_conns > 0 {
            usize::try_from(config.max_idle_conns).unwrap_or(usize::MAX)
        } else {
            DEFAULT_MAX_IDLE_CONNS
        };
        let max_idle = configured_idle.min(max_open);
        let max_lifetime = config.max_conn_lifetime.filter(|value| !value.is_zero());
        let mut pools = Vec::with_capacity(config.shards.len());
        for shard in config.shards {
            let options = postgres_options(&shard)?;
            let pool = PostgresConnectionPool::new(options, max_open, max_idle, max_lifetime);
            let mut client = pool.checkout()?;
            validate_postgres_schema(&mut client)?;
            drop(client);
            pools.push(pool);
        }
        Ok(Self {
            pools,
            n_shards: config.n_shards,
            use_multiple_databases: config.use_multiple_databases,
            encoding: config.encoding,
        })
    }

    fn pool(&self, key: &TaskListKey) -> &Arc<PostgresConnectionPool> {
        &self.pools[database_index(self.shard_id(key) as usize, self.use_multiple_databases)]
    }

    fn shard_id(&self, key: &TaskListKey) -> i32 {
        let input = format!("{}_{}", key.domain_id, key.name);
        (farmhash::hash32(input.as_bytes()) % self.n_shards as u32) as i32
    }

    fn read_task_list_row(
        &self,
        client: &mut PostgresLease,
        key: &TaskListKey,
    ) -> Result<Option<(i64, Vec<u8>, String)>> {
        let shard_id = self.shard_id(key);
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &shard_id,
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
        ];
        match client.query_opt(
            "SELECT range_id, data, data_encoding FROM task_lists WHERE shard_id=$1 AND domain_id=$2 AND name=$3 AND task_type=$4",
            params,
        ) {
            Ok(row) => row
                .map(|row| {
                    Ok((
                        row.try_get(0).map_err(normalize_postgres_error)?,
                        row.try_get(1).map_err(normalize_postgres_error)?,
                        row.try_get(2).map_err(normalize_postgres_error)?,
                    ))
                })
                .transpose(),
            Err(error) => {
                client.poison_if_closed(&error);
                Err(normalize_postgres_error(error))
            }
        }
    }

    fn decode_task_list_row(row: (i64, Vec<u8>, String)) -> Result<TaskListRecord> {
        let (range_id, bytes, encoding) = row;
        let encoding = BlobEncoding::parse(&encoding)?;
        let blob = encoding.decode(&bytes)?;
        Ok(TaskListRecord {
            range_id,
            data: blob::decode_task_list(&blob)?,
        })
    }

    fn encode_task_list(&self, data: &TaskListData) -> Result<(Vec<u8>, &'static str)> {
        let bytes = blob::encode_task_list(data);
        Ok((self.encoding.encode(&bytes)?, self.encoding.as_str()))
    }

    fn lock_and_check(
        &self,
        transaction: &mut postgres::Transaction<'_>,
        key: &TaskListKey,
        expected_range: i64,
    ) -> Result<()> {
        let shard_id = self.shard_id(key);
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &shard_id,
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
        ];
        let actual: Option<i64> = transaction
            .query_opt(
                "SELECT range_id FROM task_lists WHERE shard_id=$1 AND domain_id=$2 AND name=$3 AND task_type=$4 FOR UPDATE",
                params,
            )
            .map_err(normalize_postgres_error)?
            .map(|row| row.get(0));
        let Some(actual) = actual else {
            return Err(StorageError::ConditionFailed(
                "Task list does not exist.".into(),
            ));
        };
        if actual != expected_range {
            return Err(StorageError::ConditionFailed(format!(
                "Task list range ID was {actual} when it was should have been {expected_range}"
            )));
        }
        Ok(())
    }
}

impl MatchingTaskStore for PostgresTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord> {
        let mut client = self.pool(key).checkout()?;
        self.read_task_list_row(&mut client, key)?
            .ok_or(StorageError::NotFound)
            .and_then(Self::decode_task_list_row)
    }

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord> {
        let shard_id = self.shard_id(key);
        let task_type = postgres_task_type(key)?;
        let mut client = self.pool(key).checkout()?;
        let mut current = match self.read_task_list_row(&mut client, key)? {
            Some((range_id, bytes, encoding)) => {
                if previous_range > 0 && previous_range != range_id {
                    return Err(stale_range(key, range_id));
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
                let params: &[&(dyn ToSql + Sync)] = &[
                    &shard_id,
                    &key.domain_id.as_bytes().as_slice(),
                    &key.name,
                    &task_type,
                    &bytes,
                    &encoding,
                ];
                client
                    .execute(
                        "INSERT INTO task_lists (shard_id, domain_id, name, task_type, range_id, data, data_encoding) VALUES ($1,$2,$3,$4,0,$5,$6)",
                        params,
                    )
                    .map_err(|error| {
                        client.poison_if_closed(&error);
                        normalize_postgres_error(error)
                    })?;
                TaskListRecord { range_id: 0, data }
            }
        };
        if previous_range > 0 && previous_range != current.range_id {
            return Err(stale_range(key, current.range_id));
        }
        let mut transaction = client
            .transaction()
            .map_err(|error| normalize_postgres_error(error))?;
        self.lock_and_check(&mut transaction, key, current.range_id)?;
        current.range_id = current.range_id.wrapping_add(1);
        current.data.last_updated_nanos = now_nanos;
        let (bytes, encoding) = self.encode_task_list(&current.data)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &current.range_id,
            &bytes,
            &encoding,
            &shard_id,
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
        ];
        let affected = transaction
            .execute(
                "UPDATE task_lists SET range_id=$1,data=$2,data_encoding=$3 WHERE shard_id=$4 AND domain_id=$5 AND name=$6 AND task_type=$7",
                params,
            )
            .map_err(normalize_postgres_error)?;
        if affected != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("lease update affected {affected} rows instead of 1"),
            });
        }
        transaction.commit().map_err(normalize_postgres_error)?;
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
        let shard_id = self.shard_id(key);
        let task_type = postgres_task_type(key)?;
        let mut client = self.pool(key).checkout()?;
        let mut transaction = client
            .transaction()
            .map_err(|error| normalize_postgres_error(error))?;
        self.lock_and_check(&mut transaction, key, range)?;
        let (bytes, encoding) = self.encode_task_list(&data)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &range,
            &bytes,
            &encoding,
            &shard_id,
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
        ];
        let affected = transaction
            .execute(
                "UPDATE task_lists SET range_id=$1,data=$2,data_encoding=$3 WHERE shard_id=$4 AND domain_id=$5 AND name=$6 AND task_type=$7",
                params,
            )
            .map_err(normalize_postgres_error)?;
        if affected != 1 {
            return Err(StorageError::Backend {
                kind: BackendErrorKind::Permanent,
                message: format!("task-list update affected {affected} rows instead of 1"),
            });
        }
        transaction.commit().map_err(normalize_postgres_error)
    }

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()> {
        let task_type = postgres_task_type(key)?;
        let mut client = self.pool(key).checkout()?;
        let mut transaction = client
            .transaction()
            .map_err(|error| normalize_postgres_error(error))?;
        for (domain, task) in tasks {
            let bytes = self.encoding.encode(&blob::encode_task(&task.data))?;
            let encoding = self.encoding.as_str();
            let params: &[&(dyn ToSql + Sync)] = &[
                &domain.as_bytes().as_slice(),
                &key.name,
                &task_type,
                &task.task_id,
                &bytes,
                &encoding,
            ];
            transaction
                .execute(
                    "INSERT INTO tasks (domain_id, task_list_name, task_type, task_id, data, data_encoding) VALUES ($1,$2,$3,$4,$5,$6)",
                    params,
                )
                .map_err(normalize_postgres_error)?;
        }
        self.lock_and_check(&mut transaction, key, range)?;
        transaction.commit().map_err(normalize_postgres_error)
    }

    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>> {
        let mut client = self.pool(key).checkout()?;
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
            &read_level,
            &max_read_level,
            &batch_size,
        ];
        let rows = client
            .query(
                "SELECT task_id,data,data_encoding FROM tasks WHERE domain_id=$1 AND task_list_name=$2 AND task_type=$3 AND task_id>$4 AND task_id<=$5 ORDER BY task_id LIMIT $6",
                params,
            )
            .map_err(|error| {
                client.poison_if_closed(&error);
                normalize_postgres_error(error)
            })?;
        rows.into_iter()
            .map(|row| {
                let task_id: i64 = row.try_get(0).map_err(normalize_postgres_error)?;
                let bytes: Vec<u8> = row.try_get(1).map_err(normalize_postgres_error)?;
                let label: String = row.try_get(2).map_err(normalize_postgres_error)?;
                let encoding = BlobEncoding::parse(&label)?;
                Ok(AllocatedTask {
                    task_id,
                    data: blob::decode_task(&encoding.decode(&bytes)?)?,
                })
            })
            .collect()
    }

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64> {
        let mut client = self.pool(key).checkout()?;
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
            &ack_level,
        ];
        client
            .query_one(
                "SELECT count(1) FROM tasks WHERE domain_id=$1 AND task_list_name=$2 AND task_type=$3 AND task_id>$4",
                params,
            )
            .and_then(|row| row.try_get(0))
            .map_err(|error| {
                client.poison_if_closed(&error);
                normalize_postgres_error(error)
            })
    }

    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()> {
        let mut client = self.pool(key).checkout()?;
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
            &task_id,
        ];
        client
            .execute(
                "DELETE FROM tasks WHERE domain_id=$1 AND task_list_name=$2 AND task_type=$3 AND task_id=$4",
                params,
            )
            .map(|_| ())
            .map_err(|error| {
                client.poison_if_closed(&error);
                normalize_postgres_error(error)
            })
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
        let mut client = self.pool(key).checkout()?;
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
            &inclusive_task_id,
            &limit,
        ];
        let affected = client
            .execute(
                "DELETE FROM tasks WHERE domain_id=$1 AND task_list_name=$2 AND task_type=$3 AND task_id IN (SELECT task_id FROM tasks WHERE domain_id=$1 AND task_list_name=$2 AND task_type=$3 AND task_id<=$4 ORDER BY domain_id,task_list_name,task_type,task_id LIMIT $5)",
                params,
            )
            .map_err(|error| {
                client.poison_if_closed(&error);
                normalize_postgres_error(error)
            })?;
        usize::try_from(affected).map_err(|error| StorageError::Backend {
            kind: BackendErrorKind::Permanent,
            message: format!("PostgreSQL affected-row count did not fit usize: {error}"),
        })
    }

    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()> {
        let shard_id = self.shard_id(key);
        let mut client = self.pool(key).checkout()?;
        let task_type = postgres_task_type(key)?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &shard_id,
            &key.domain_id.as_bytes().as_slice(),
            &key.name,
            &task_type,
            &range,
        ];
        let affected = client
            .execute(
                "DELETE FROM task_lists WHERE shard_id=$1 AND domain_id=$2 AND name=$3 AND task_type=$4 AND range_id=$5",
                params,
            )
            .map_err(|error| {
                client.poison_if_closed(&error);
                normalize_postgres_error(error)
            })?;
        if affected != 1 {
            return Err(StorageError::DeleteCount(affected as usize));
        }
        Ok(())
    }
}

fn postgres_options(shard: &PostgresShardConfig) -> Result<Config> {
    if shard.database_name.is_empty() {
        return Err(configuration_error(
            "PostgreSQL database_name must not be empty".into(),
        ));
    }
    let (host, port) = split_host_port(&shard.connect_addr)?;
    let mut options = Config::new();
    options
        .ssl_mode(SslMode::Disable)
        .host(&host)
        .port(port)
        .user(&shard.user)
        .password(&shard.password)
        .dbname(&shard.database_name);
    for (key, value) in &shard.connect_attributes {
        match key.to_ascii_lowercase().as_str() {
            "sslmode" if value.eq_ignore_ascii_case("disable") => {}
            "sslmode" => {
                return Err(configuration_error(format!(
                    "PostgreSQL sslmode={value:?} requires a TLS adapter; only disable is currently supported"
                )))
            }
            "connect_timeout" => {
                let seconds = value.parse::<u64>().map_err(|_| {
                    configuration_error(format!(
                        "invalid PostgreSQL connect_timeout {value:?}; expected whole seconds"
                    ))
                })?;
                if seconds > 0 {
                    options.connect_timeout(Duration::from_secs(seconds));
                }
            }
            "application_name" => {
                options.application_name(value);
            }
            "options" => {
                options.options(value);
            }
            other => {
                return Err(configuration_error(format!(
                    "unsupported PostgreSQL connect attribute {other:?}"
                )))
            }
        }
    }
    Ok(options)
}

fn split_host_port(address: &str) -> Result<(String, u16)> {
    let (host, port) = address.rsplit_once(':').ok_or_else(|| {
        configuration_error(format!(
            "invalid PostgreSQL connect address {address:?}; expected host:port"
        ))
    })?;
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() || host.contains(':') && !address.starts_with('[') {
        return Err(configuration_error(format!(
            "invalid PostgreSQL connect address {address:?}; IPv6 hosts must be bracketed"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        configuration_error(format!(
            "invalid PostgreSQL port in connect address {address:?}"
        ))
    })?;
    Ok((host.to_owned(), port))
}

fn validate_postgres_schema(client: &mut PostgresLease) -> Result<()> {
    client
        .prepare("SELECT shard_id,domain_id,name,task_type,range_id,data,data_encoding FROM task_lists LIMIT 0")
        .map_err(|error| {
            client.poison_if_closed(&error);
            StorageError::BackendSchema(format!("PostgreSQL task_lists schema: {error}"))
        })?;
    client
        .prepare("SELECT domain_id,task_list_name,task_type,task_id,data,data_encoding FROM tasks LIMIT 0")
        .map_err(|error| {
            client.poison_if_closed(&error);
            StorageError::BackendSchema(format!("PostgreSQL tasks schema: {error}"))
        })?;
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
        let rows = client
            .query(
                "SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema AND tc.table_name=kcu.table_name WHERE tc.table_schema=current_schema() AND tc.table_name=$1 AND tc.constraint_type='PRIMARY KEY' ORDER BY kcu.ordinal_position",
                &[&table],
            )
            .map_err(|error| {
                client.poison_if_closed(&error);
                StorageError::BackendSchema(format!("reading PostgreSQL {table} primary key: {error}"))
            })?;
        let actual = rows
            .iter()
            .map(|row| row.try_get::<_, String>(0))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| StorageError::BackendSchema(error.to_string()))?;
        if actual.iter().map(String::as_str).collect::<Vec<_>>() != expected {
            return Err(StorageError::BackendSchema(format!(
                "PostgreSQL {table} primary key is {actual:?}, expected {expected:?}"
            )));
        }
    }
    Ok(())
}

fn stale_range(key: &TaskListKey, range_id: i64) -> StorageError {
    StorageError::ConditionFailed(format!(
        "leaseTaskList:renew failed:taskList:{}, taskListType:{}, haveRangeID:0, gotRangeID:{range_id}",
        key.name, key.task_type
    ))
}

fn postgres_task_type(key: &TaskListKey) -> Result<i16> {
    i16::try_from(key.task_type).map_err(|_| {
        configuration_error(format!(
            "PostgreSQL task type {} does not fit the schema SMALLINT column",
            key.task_type
        ))
    })
}

fn configuration_error(message: String) -> StorageError {
    StorageError::BackendConfiguration(message)
}

fn validate_shard_config(config: &PostgresTaskStoreConfig) -> Result<()> {
    if config.n_shards == 0
        || (config.use_multiple_databases
            && (config.n_shards <= 1 || config.shards.len() != config.n_shards))
        || (!config.use_multiple_databases && config.shards.len() != 1)
    {
        return Err(configuration_error(format!(
            "invalid PostgreSQL shard setup: n_shards={}, use_multiple_databases={}, database configs={}",
            config.n_shards,
            config.use_multiple_databases,
            config.shards.len()
        )));
    }
    Ok(())
}

fn database_index(logical_shard: usize, use_multiple_databases: bool) -> usize {
    if use_multiple_databases {
        logical_shard
    } else {
        0
    }
}

fn normalize_postgres_error(error: postgres::Error) -> StorageError {
    let sql_state = error.code().map(|code| code.code()).unwrap_or_default();
    let kind = match sql_state {
        "57014" => BackendErrorKind::Timeout,
        "53300" | "53000" => BackendErrorKind::Throttled,
        _ => BackendErrorKind::Internal,
    };
    StorageError::Backend {
        kind,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(attributes: &[(&str, &str)]) -> PostgresShardConfig {
        PostgresShardConfig {
            connect_addr: "localhost:5432".into(),
            user: "cadence".into(),
            password: "secret".into(),
            database_name: "cadence".into(),
            connect_attributes: attributes
                .iter()
                .map(|(key, value)| ((*key).into(), (*value).into()))
                .collect(),
        }
    }

    #[test]
    fn postgres_address_parser_matches_host_port_requirements() {
        assert_eq!(
            split_host_port("db.example:5432").unwrap(),
            ("db.example".into(), 5432)
        );
        assert_eq!(split_host_port("[::1]:5432").unwrap(), ("::1".into(), 5432));
        assert!(split_host_port("::1:5432").is_err());
        assert!(split_host_port("db.example:nope").is_err());
    }

    #[test]
    fn postgres_connect_attributes_fail_closed_for_unported_tls() {
        assert!(postgres_options(&shard(&[("sslmode", "disable")])).is_ok());
        assert!(postgres_options(&shard(&[("sslmode", "require")])).is_err());
        assert!(postgres_options(&shard(&[("sslrootcert", "/tmp/ca.pem")])).is_err());
        assert!(postgres_options(&shard(&[("mystery", "ignored")])).is_err());
    }

    #[test]
    fn postgres_config_requires_matching_physical_shard_count() {
        let result = PostgresTaskStore::connect(PostgresTaskStoreConfig {
            shards: vec![shard(&[])],
            n_shards: 2,
            use_multiple_databases: true,
            max_conns: 1,
            max_idle_conns: 1,
            max_conn_lifetime: None,
            encoding: BlobEncoding::ThriftRW,
        });
        let error = match result {
            Ok(_) => panic!("mismatched PostgreSQL shard count unexpectedly connected"),
            Err(error) => error,
        };
        assert!(matches!(error, StorageError::BackendConfiguration(_)));
        let single_database = PostgresTaskStoreConfig {
            shards: vec![shard(&[])],
            n_shards: 4,
            use_multiple_databases: false,
            max_conns: 1,
            max_idle_conns: 1,
            max_conn_lifetime: None,
            encoding: BlobEncoding::ThriftRW,
        };
        assert!(validate_shard_config(&single_database).is_ok());
        assert_eq!(database_index(3, false), 0);
        assert_eq!(database_index(3, true), 3);
    }

    #[test]
    fn postgres_schema_matches_go_task_table_keys() {
        let schema = include_str!("../../../../go/schema/postgres/cadence/schema.sql");
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
}
