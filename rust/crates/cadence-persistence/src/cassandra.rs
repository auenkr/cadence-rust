//! Cassandra driver session setup shared by Matching's persistence adapters.
//!
//! The driver is asynchronous. Consumers must await its APIs from async
//! service paths; they must not call a nested runtime or synchronously wait on
//! driver futures from Tokio workers.

use crate::semaphores::{SemaphoreBucketPage, SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE};
use crate::{
    AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, SemaphorePersistence,
    SemaphoreRow,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::{
    client::execution_profile::ExecutionProfile,
    client::{session::Session, session_builder::SessionBuilder},
    frame::types::{Consistency, SerialConsistency},
    statement::batch::{Batch, BatchType},
    DeserializeValue, SerializeValue,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

const TOKEN_ROW: i32 = 1;
const OWNER_ROW: i32 = 2;
const OWNER_TOKEN_SENTINEL: i32 = -1;
const OWNER_NONE_SENTINEL: &str = "0::30000000-0000-f000-f000-000000000000:-1";
const FREE_SENTINEL: &str = "0::30000000-0000-f000-f000-000000000000:-2";
const INSERT_METADATA: &str = "INSERT INTO semaphore_metadata (domain_id, semaphore_name, size, bucket_size, created_time) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS";
const SELECT_METADATA: &str =
    "SELECT size, bucket_size FROM semaphore_metadata WHERE domain_id = ? AND semaphore_name = ?";
const SELECT_BUCKET: &str = "SELECT type, token_id, owner_id, holder, held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ?";
const SELECT_TOKEN: &str = "SELECT holder FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ?";
const SELECT_OWNER: &str = "SELECT held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";
const GRANT_TOKEN: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder IN (?, null)";
const INSERT_OWNER: &str = "INSERT INTO semaphore_tokens (domain_id, semaphore_name, bucket, type, token_id, owner_id, holder, held_token, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const RELEASE_TOKEN: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder = ?";
const DELETE_OWNER: &str = "DELETE FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";

/// Current Go Cassandra `task` UDT from schema.cql. Field names are part of
/// the wire/storage contract and deliberately match the Go UDT exactly.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CassandraTaskUdt {
    pub domain_id: Uuid,
    pub workflow_id: String,
    pub run_id: Option<Uuid>,
    pub schedule_id: i64,
    pub created_time: DateTime<Utc>,
    pub partition_config: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CassandraTaskListPartitionUdt {
    pub isolation_groups: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CassandraTaskListPartitionConfigUdt {
    pub version: i64,
    pub num_read_partitions: i32,
    pub num_write_partitions: i32,
    pub read_partitions: Option<BTreeMap<i32, CassandraTaskListPartitionUdt>>,
    pub write_partitions: Option<BTreeMap<i32, CassandraTaskListPartitionUdt>>,
}

/// Current Go Cassandra `task_list` UDT, including adaptive partition config.
#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CassandraTaskListUdt {
    pub domain_id: Uuid,
    pub name: String,
    #[scylla(rename = "type")]
    pub type_: i32,
    pub ack_level: i64,
    pub kind: i32,
    pub last_updated: DateTime<Utc>,
    pub adaptive_partition_config: Option<CassandraTaskListPartitionConfigUdt>,
}

#[derive(Debug, Clone, DeserializeValue, SerializeValue)]
pub struct CassandraDomainUdt {
    pub id: Uuid,
    pub name: String,
    pub status: i32,
    pub description: Option<String>,
    pub data: Option<HashMap<String, String>>,
    pub owner_email: Option<String>,
}

/// `domain` UDT as stored in rows that may carry NULL fields (Go's seeded
/// `cadence-domain-metadata` record has an all-NULL UDT; gocql reads NULL as zero).
#[derive(Debug, Clone, DeserializeValue)]
pub struct CassandraNullableDomainUdt {
    pub id: Option<Uuid>,
    pub name: Option<String>,
    pub status: Option<i32>,
    pub description: Option<String>,
    pub data: Option<HashMap<String, String>>,
    pub owner_email: Option<String>,
}

impl CassandraNullableDomainUdt {
    /// `None` when the record has no id (the metadata record).
    pub fn into_domain(self) -> Option<CassandraDomainUdt> {
        Some(CassandraDomainUdt {
            id: self.id?,
            name: self.name.unwrap_or_default(),
            status: self.status.unwrap_or_default(),
            description: self.description,
            data: self.data,
            owner_email: self.owner_email,
        })
    }
}

#[derive(Debug, Clone, Default, DeserializeValue, SerializeValue)]
pub struct CassandraDomainConfigUdt {
    pub retention: i32,
    pub emit_metric: bool,
    pub archival_bucket: Option<String>,
    pub archival_status: Option<i32>,
    pub history_archival_status: Option<i32>,
    pub history_archival_uri: Option<String>,
    pub visibility_archival_status: Option<i32>,
    pub visibility_archival_uri: Option<String>,
    pub bad_binaries: Option<Vec<u8>>,
    pub bad_binaries_encoding: Option<Vec<u8>>,
    pub isolation_groups: Option<Vec<u8>>,
    pub isolation_groups_encoding: Option<String>,
    pub async_workflow_config: Option<Vec<u8>>,
    pub async_workflow_config_encoding: Option<String>,
}

#[derive(Debug, Clone, Default, DeserializeValue, SerializeValue)]
pub struct CassandraClusterReplicationConfigUdt {
    pub cluster_name: Option<String>,
}

#[derive(Debug, Clone, Default, DeserializeValue, SerializeValue)]
pub struct CassandraDomainReplicationConfigUdt {
    pub active_cluster_name: Option<String>,
    pub clusters: Option<Vec<CassandraClusterReplicationConfigUdt>>,
    pub active_clusters_config: Option<Vec<u8>>,
    pub active_clusters_config_encoding: Option<String>,
}

/// Go's Cassandra `semaphore_metadata` and `semaphore_tokens` persistence.
/// The session is shared by Matching task/domain/semaphore stores when the
/// configured Cassandra backend is wired by cadence-server.
pub struct CassandraSemaphorePersistence {
    session: Arc<Session>,
}

#[path = "semaphores_cassandra_native.rs"]
mod semaphores_cassandra_native;

impl fmt::Debug for CassandraSemaphorePersistence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraSemaphorePersistence")
            .finish_non_exhaustive()
    }
}

impl CassandraSemaphorePersistence {
    pub fn new(session: Arc<Session>) -> Self {
        Self { session }
    }

    fn domain_id(value: &str) -> Result<Uuid, AcquireError> {
        Uuid::parse_str(value)
            .map_err(|error| AcquireError::Invalid(format!("invalid domainID {value:?}: {error}")))
    }

    fn internal(error: impl fmt::Display) -> AcquireError {
        AcquireError::Internal(error.to_string())
    }

    async fn owner_token(
        &self,
        domain_id: Uuid,
        name: &str,
        bucket: i32,
        owner: &str,
    ) -> Result<i32, AcquireError> {
        let result = self
            .session
            .query_unpaged(
                SELECT_OWNER,
                (
                    domain_id,
                    name,
                    bucket,
                    OWNER_ROW,
                    OWNER_TOKEN_SENTINEL,
                    owner,
                ),
            )
            .await
            .map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        rows.maybe_first_row::<(i32,)>()
            .map_err(Self::internal)?
            .map(|(token,)| token)
            .ok_or_else(|| {
                AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore owner {owner:?} does not hold a token in bucket {bucket}"
                )))
            })
    }

    async fn token_holder(
        &self,
        domain_id: Uuid,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError> {
        let result = self
            .session
            .query_unpaged(SELECT_TOKEN, (domain_id, name, bucket, TOKEN_ROW, token_id))
            .await
            .map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let holder = rows
            .maybe_first_row::<(Option<String>,)>()
            .map_err(Self::internal)?
            .map(|(holder,)| holder)
            .ok_or_else(|| {
                AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore token {token_id} does not exist in bucket {bucket}"
                )))
            })?;
        Ok(holder
            .filter(|holder| holder != FREE_SENTINEL)
            .unwrap_or_default())
    }
}

#[async_trait]
impl SemaphorePersistence for CassandraSemaphorePersistence {
    async fn create_semaphore(
        &self,
        domain_id: &str,
        name: &str,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 {
            return Err(SemaphoreError::Invalid(format!(
                "Size must be positive, got {size}"
            )));
        }
        if !(0..=crate::semaphores::MAX_BUCKET_SIZE).contains(&bucket_size) {
            return Err(SemaphoreError::Invalid(format!(
                "BucketSize must not exceed {}, got {bucket_size}",
                crate::semaphores::MAX_BUCKET_SIZE
            )));
        }
        let domain_id = Uuid::parse_str(domain_id)
            .map_err(|error| SemaphoreError::Invalid(format!("invalid domainID: {error}")))?;
        let bucket_size = if bucket_size == 0 {
            crate::semaphores::DEFAULT_BUCKET_SIZE
        } else {
            bucket_size
        };
        let created_time: DateTime<Utc> = Utc::now();
        let result = self
            .session
            .query_unpaged(
                INSERT_METADATA,
                (domain_id, name, size, bucket_size, created_time),
            )
            .await
            .map_err(|error| SemaphoreError::Internal(error.to_string()))?;
        // Cassandra returns `[applied]` plus the previous metadata on a
        // conditional conflict. Both shapes use the same typed columns.
        let rows = result
            .into_rows_result()
            .map_err(|error| SemaphoreError::Internal(error.to_string()))?;
        let applied = rows
            .maybe_first_row::<(
                bool,
                Option<Uuid>,
                Option<String>,
                Option<i32>,
                Option<i32>,
                Option<DateTime<Utc>>,
            )>()
            .map_err(|error| SemaphoreError::Internal(error.to_string()))?
            .map(|(applied, ..)| applied)
            .unwrap_or(false);
        if !applied {
            return Err(SemaphoreError::Exists(format!(
                "semaphore {name:?} already exists"
            )));
        }
        Ok(SemaphoreMetadata { size, bucket_size })
    }

    async fn get_semaphore_metadata(
        &self,
        domain_id: &str,
        name: &str,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        let result = self
            .session
            .query_unpaged(SELECT_METADATA, (domain_id, name))
            .await
            .map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        rows.maybe_first_row::<(i32, i32)>()
            .map_err(Self::internal)?
            .map(|(size, bucket_size)| SemaphoreMetadata { size, bucket_size })
            .ok_or_else(|| {
                AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                    "semaphore {name:?} does not exist in domain {domain_id}"
                )))
            })
    }

    async fn scan_semaphore_bucket(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<SemaphoreBucketPage, AcquireError> {
        use scylla::{response::PagingState, statement::Statement};
        let domain_id = Self::domain_id(domain_id)?;
        let page_size = i32::try_from(page_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| AcquireError::Invalid(SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE.into()))?;
        let mut statement = Statement::new(SELECT_BUCKET);
        statement.set_page_size(page_size);
        let paging_state = next_page_token
            .map(|token| PagingState::new_from_raw_bytes(token.to_vec()))
            .unwrap_or_else(PagingState::start);
        let (result, paging_response) = self
            .session
            .query_single_page(statement, (domain_id, name, bucket), paging_state)
            .await
            .map_err(Self::internal)?;
        let next_page_token = match paging_response.into_paging_control_flow() {
            std::ops::ControlFlow::Continue(state) => {
                state.as_bytes_slice().map(|bytes| bytes.to_vec())
            }
            std::ops::ControlFlow::Break(()) => None,
        };
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let mut output = Vec::new();
        for row in rows
            .rows::<(i32, i32, Option<String>, Option<String>, Option<i32>)>()
            .map_err(Self::internal)?
        {
            let (row_type, token_id, owner_id, holder, held_token) = row.map_err(Self::internal)?;
            match row_type {
                TOKEN_ROW => output.push(SemaphoreRow::Token {
                    token_id,
                    holder: holder
                        .filter(|holder| holder != FREE_SENTINEL)
                        .unwrap_or_default(),
                }),
                OWNER_ROW => {
                    if let (Some(owner_id), Some(held_token)) = (owner_id, held_token) {
                        output.push(SemaphoreRow::Owner {
                            owner_id,
                            held_token,
                        });
                    }
                }
                _ => output.push(SemaphoreRow::Unknown),
            }
        }
        Ok(SemaphoreBucketPage {
            rows: output,
            next_page_token,
        })
    }

    async fn get_semaphore_ownership_by_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
    ) -> Result<String, AcquireError> {
        self.token_holder(Self::domain_id(domain_id)?, name, bucket, token_id)
            .await
    }

    async fn get_semaphore_ownership_by_owner(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        owner_id: &str,
    ) -> Result<i32, AcquireError> {
        self.owner_token(Self::domain_id(domain_id)?, name, bucket, owner_id)
            .await
    }

    async fn grant_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<GrantOutcome, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        match self.owner_token(domain_id, name, bucket, owner_id).await {
            Ok(existing) => return Ok(GrantOutcome::AlreadyHeld(existing)),
            Err(AcquireError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(GRANT_TOKEN);
        batch.append_statement(INSERT_OWNER);
        let now: DateTime<Utc> = Utc::now();
        self.session
            .batch(
                &batch,
                (
                    (
                        owner_id,
                        now,
                        domain_id,
                        name,
                        bucket,
                        TOKEN_ROW,
                        token_id,
                        OWNER_NONE_SENTINEL,
                        FREE_SENTINEL,
                    ),
                    (
                        domain_id,
                        name,
                        bucket,
                        OWNER_ROW,
                        OWNER_TOKEN_SENTINEL,
                        owner_id,
                        None::<String>,
                        token_id,
                        now,
                    ),
                ),
            )
            .await
            .map_err(Self::internal)?;
        match self.owner_token(domain_id, name, bucket, owner_id).await {
            Ok(held) if held == token_id => Ok(GrantOutcome::Applied),
            Ok(held) => Ok(GrantOutcome::AlreadyHeld(held)),
            Err(AcquireError::NotFound(_)) => {
                match self.token_holder(domain_id, name, bucket, token_id).await {
                    Ok(holder) if !holder.is_empty() => Ok(GrantOutcome::SlotTaken),
                    Ok(_) | Err(AcquireError::NotFound(_)) => Ok(GrantOutcome::SlotTaken),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn release_semaphore_token(
        &self,
        domain_id: &str,
        name: &str,
        bucket: i32,
        token_id: i32,
        owner_id: &str,
    ) -> Result<bool, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        if self.token_holder(domain_id, name, bucket, token_id).await? != owner_id {
            return Ok(false);
        }
        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(RELEASE_TOKEN);
        batch.append_statement(DELETE_OWNER);
        let now: DateTime<Utc> = Utc::now();
        self.session
            .batch(
                &batch,
                (
                    (
                        FREE_SENTINEL,
                        now,
                        domain_id,
                        name,
                        bucket,
                        TOKEN_ROW,
                        token_id,
                        OWNER_NONE_SENTINEL,
                        owner_id,
                    ),
                    (
                        domain_id,
                        name,
                        bucket,
                        OWNER_ROW,
                        OWNER_TOKEN_SENTINEL,
                        owner_id,
                    ),
                ),
            )
            .await
            .map_err(Self::internal)?;
        match self.owner_token(domain_id, name, bucket, owner_id).await {
            Err(AcquireError::NotFound(_)) => Ok(true),
            Ok(_) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CassandraSessionConfig {
    pub hosts: Vec<String>,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub keyspace: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub consistency: Consistency,
    pub serial_consistency: Option<SerialConsistency>,
    pub max_connections_per_host: Option<NonZeroUsize>,
    pub datacenter: String,
    pub region: String,
    pub tls: Option<CassandraTlsConfig>,
    pub allowed_authenticators: Vec<String>,
    pub host_selection_policy: String,
}

#[derive(Debug, Clone, Default)]
pub struct CassandraTlsConfig {
    pub ca_files: Vec<PathBuf>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub verify_hostname: bool,
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CassandraSessionOptions {
    pub timeout_nanos: i64,
    pub connect_timeout_nanos: i64,
    pub consistency: String,
    pub serial_consistency: String,
    pub max_conns: i32,
    pub datacenter: String,
    pub region: String,
    pub tls: Option<CassandraTlsConfig>,
    pub allowed_authenticators: Vec<String>,
    pub proto_version: i32,
    pub connect_attributes: HashMap<String, String>,
    pub host_selection_policy: String,
}

/// Session and dedicated async driver runtime for Matching's synchronous
/// persistence traits. Calls are made from the server's blocking pool using
/// `run`; the runtime is independent of the RPC runtime.
pub struct CassandraBlockingSession {
    session: Option<Arc<Session>>,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    default_consistency: Consistency,
}

impl fmt::Debug for CassandraBlockingSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraBlockingSession")
            .finish_non_exhaustive()
    }
}

impl CassandraBlockingSession {
    /// Build a private multi-thread runtime and connect on it. Invoke this
    /// constructor from a blocking worker during service startup.
    pub fn connect(config: &CassandraSessionConfig) -> anyhow::Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let session = runtime.block_on(config.connect())?;
        Ok(Self {
            session: Some(Arc::new(session)),
            runtime: Some(runtime),
            default_consistency: config.consistency,
        })
    }

    pub(crate) fn session(&self) -> Arc<Session> {
        self.session
            .as_ref()
            .expect("live Cassandra session")
            .clone()
    }

    /// Drive one Scylla operation from a Tokio blocking-pool worker. Calling
    /// this on an async worker is forbidden because it blocks that worker.
    pub fn run<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("live Cassandra runtime")
            .block_on(future)
    }

    pub fn default_consistency(&self) -> Consistency {
        self.default_consistency
    }
}

impl Drop for CassandraBlockingSession {
    fn drop(&mut self) {
        // Drop the driver session before runtime shutdown. Runtime::drop may
        // block, so transfer it to a plain thread even if we're on Tokio.
        drop(self.session.take());
        if let Some(runtime) = self.runtime.take() {
            shutdown_runtime(runtime);
        }
    }
}

fn shutdown_runtime(runtime: Arc<tokio::runtime::Runtime>) {
    let _ = std::thread::Builder::new()
        .name("cadence-cassandra-runtime-shutdown".into())
        .spawn(move || drop(runtime));
}

#[cfg(test)]
mod blocking_session_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn dedicated_runtime_shutdown_does_not_block_rpc_runtime() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            shutdown_runtime(runtime);
            finished_tx.send(()).unwrap();
        });
        tokio::time::timeout(
            Duration::from_millis(200),
            tokio::time::sleep(Duration::from_millis(5)),
        )
        .await
        .expect("runtime shutdown blocked the RPC timer worker");
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown dispatch completes");
    }
}

impl CassandraSessionConfig {
    pub fn from_parts(
        hosts: impl IntoIterator<Item = String>,
        port: i32,
        user: impl Into<String>,
        password: impl Into<String>,
        keyspace: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let mut hosts: Vec<String> = hosts
            .into_iter()
            .filter(|host| !host.trim().is_empty())
            .collect();
        if hosts.is_empty() {
            hosts = std::env::var("CASSANDRA_SEEDS")
                .unwrap_or_else(|_| "127.0.0.1".into())
                .split(',')
                .map(str::trim)
                .filter(|host| !host.is_empty())
                .map(str::to_owned)
                .collect();
        }
        if hosts.is_empty() {
            anyhow::bail!("Cassandra hosts must not be empty");
        }
        let port = if port == 0 {
            std::env::var("CASSANDRA_DB_PORT")
                .ok()
                .map(|port| port.parse::<i32>())
                .transpose()?
                .unwrap_or(9042)
        } else {
            port
        };
        let port = u16::try_from(port)
            .map_err(|_| anyhow::anyhow!("Cassandra port must be between 1 and 65535"))?;
        let keyspace = keyspace.into();
        if keyspace.is_empty() {
            anyhow::bail!("Cassandra keyspace must not be empty");
        }
        Ok(Self {
            hosts,
            port,
            user: user.into(),
            password: password.into(),
            keyspace,
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            consistency: Consistency::LocalQuorum,
            serial_consistency: Some(SerialConsistency::LocalSerial),
            max_connections_per_host: NonZeroUsize::new(2),
            datacenter: String::new(),
            region: String::new(),
            tls: None,
            allowed_authenticators: Vec::new(),
            host_selection_policy: String::new(),
        })
    }

    pub fn with_go_options(mut self, options: CassandraSessionOptions) -> anyhow::Result<Self> {
        self.request_timeout =
            duration_option(options.timeout_nanos, Duration::from_secs(10), "timeout")?;
        self.connect_timeout = duration_option(
            options.connect_timeout_nanos,
            Duration::from_secs(2),
            "connectTimeout",
        )?;
        if !options.consistency.is_empty() {
            self.consistency = parse_consistency(&options.consistency)?;
        }
        if !options.serial_consistency.is_empty() {
            self.serial_consistency = Some(parse_serial_consistency(&options.serial_consistency)?);
        }
        self.max_connections_per_host = if options.max_conns > 0 {
            NonZeroUsize::new(options.max_conns as usize)
        } else {
            NonZeroUsize::new(2)
        };
        self.datacenter = options.datacenter;
        self.region = options.region;
        self.tls = options.tls;
        self.allowed_authenticators = options.allowed_authenticators;
        self.host_selection_policy = options.host_selection_policy;
        if options.proto_version != 0 && options.proto_version != 4 {
            anyhow::bail!("Cassandra protoVersion {} is not supported by the Scylla Rust driver (the driver only speaks CQL protocol v4)", options.proto_version);
        }
        // Go carries connectAttributes in NoSQL config but does not forward them
        // into gocql ClusterConfig, so intentionally leave them unused here too.
        if !matches!(
            self.host_selection_policy.as_str(),
            "" | "tokenaware,roundrobin" | "roundrobin" | "hostpool-epsilon-greedy"
        ) {
            anyhow::bail!(
                "unknown Cassandra hostSelectionPolicy {:?}",
                self.host_selection_policy
            );
        }
        Ok(self)
    }

    /// Connect to all configured contact points and select Cadence's existing
    /// keyspace. This never creates a keyspace or applies schema migrations.
    pub async fn connect(&self) -> anyhow::Result<Session> {
        let nodes: Vec<String> = self
            .hosts
            .iter()
            .map(|host| {
                let host = host.trim();
                if host.starts_with('[') {
                    format!("{host}:{}", self.port)
                } else if host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_ipv6())
                {
                    format!("[{host}]:{}", self.port)
                } else {
                    format!("{host}:{}", self.port)
                }
            })
            .collect();
        let host_selection_policy: Arc<dyn scylla::policies::load_balancing::LoadBalancingPolicy> =
            if self.host_selection_policy == "hostpool-epsilon-greedy" {
                Arc::new(CassandraHostPoolPolicy::default())
            } else {
                Arc::new(CassandraRoundRobinPolicy {
                    token_aware: self.host_selection_policy != "roundrobin",
                    next: AtomicUsize::default(),
                })
            };
        let profile = ExecutionProfile::builder()
            .consistency(self.consistency)
            .serial_consistency(self.serial_consistency)
            .request_timeout(Some(self.request_timeout))
            .load_balancing_policy(host_selection_policy)
            .build()
            .into_handle();
        let mut builder = SessionBuilder::new()
            .known_nodes(nodes)
            .default_execution_profile_handle(profile)
            .connection_timeout(self.connect_timeout)
            .use_keyspace(self.keyspace.clone(), true);
        if let Some(size) = self.max_connections_per_host {
            builder = builder.pool_size(scylla::client::PoolSize::PerHost(size));
        }
        if !self.region.is_empty() {
            builder = builder.host_filter(Arc::new(CassandraRegionHostFilter(self.region.clone())));
        } else if !self.datacenter.is_empty() {
            builder = builder.host_filter(Arc::new(
                scylla::policies::host_filter::DcHostFilter::new(self.datacenter.clone()),
            ));
        }
        if let Some(tls) = &self.tls {
            let client_config = build_rustls_client_config(tls)?;
            let context = match tls.server_name.as_deref().filter(|name| !name.is_empty()) {
                Some(server_name) => scylla::client::session::TlsContext::Rustls023ServerName(
                    client_config,
                    server_name.to_string(),
                ),
                None => client_config.into(),
            };
            builder = builder.tls_context(Some(context));
        }
        if !self.user.is_empty() && !self.password.is_empty() {
            builder = builder.authenticator_provider(Arc::new(GoPasswordAuthenticator {
                username: self.user.clone(),
                password: self.password.clone(),
                allowed: self.allowed_authenticators.clone(),
            }));
        }
        Ok(builder.build().await?)
    }
}

/// gocql round-robin semantics: rotate the first available host per request,
/// then try the remaining available hosts in the same rotation.
#[derive(Debug, Default)]
struct CassandraRoundRobinPolicy {
    token_aware: bool,
    next: AtomicUsize,
}

impl scylla::policies::load_balancing::LoadBalancingPolicy for CassandraRoundRobinPolicy {
    fn pick<'a>(
        &'a self,
        request: &'a scylla::policies::load_balancing::RoutingInfo<'a>,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Option<(scylla::cluster::NodeRef<'a>, Option<scylla::routing::Shard>)> {
        let routed = self.routed_nodes(request, cluster);
        if let Some(node) = routed.first() {
            return Some((*node, None));
        }
        let connected = self.connected_nodes(cluster);
        if connected.is_empty() {
            return None;
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % connected.len();
        Some((connected[start], None))
    }

    fn fallback<'a>(
        &'a self,
        request: &'a scylla::policies::load_balancing::RoutingInfo<'a>,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> scylla::policies::load_balancing::FallbackPlan<'a> {
        let routed = self.routed_nodes(request, cluster);
        let connected = self.connected_nodes(cluster);
        let mut plan: Vec<_> = routed.iter().skip(1).copied().collect();
        let routed_ids: std::collections::HashSet<_> =
            routed.iter().map(|node| node.host_id).collect();
        // gocql's token-aware wrapper asks its round-robin fallback for a
        // cursor only after all available replicas fail.
        let start = if routed.is_empty() {
            self.next.load(Ordering::Relaxed)
        } else {
            self.next.fetch_add(1, Ordering::Relaxed)
        } % connected.len().max(1);
        plan.extend(
            connected
                .iter()
                .cycle()
                .skip(start)
                .take(connected.len())
                .filter(|node| !routed_ids.contains(&node.host_id))
                .copied(),
        );
        Box::new(plan.into_iter().map(|node| (node, None)))
    }

    fn name(&self) -> String {
        if self.token_aware {
            "CadenceTokenAwareRoundRobinHostPolicy".to_string()
        } else {
            "CadenceRoundRobinHostPolicy".to_string()
        }
    }
}

impl CassandraRoundRobinPolicy {
    fn connected_nodes<'a>(
        &self,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Vec<scylla::cluster::NodeRef<'a>> {
        cluster
            .get_nodes_info()
            .iter()
            .filter(|node| node.is_connected())
            .collect()
    }

    fn routed_nodes<'a>(
        &self,
        request: &scylla::policies::load_balancing::RoutingInfo<'_>,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Vec<scylla::cluster::NodeRef<'a>> {
        if !self.token_aware {
            return Vec::new();
        }
        let (Some(token), Some(table)) = (request.token, request.table) else {
            return Vec::new();
        };
        let replicas = cluster.get_token_endpoints(table.ks_name(), table.table_name(), token);
        replicas
            .iter()
            .filter(|(replica, _)| replica.is_connected())
            .filter_map(|(replica, _)| {
                cluster
                    .get_nodes_info()
                    .iter()
                    .find(|node| Arc::ptr_eq(node, replica))
            })
            .collect()
    }
}

const HOSTPOOL_BUCKETS: usize = 120;
const HOSTPOOL_BUCKET_WIDTH: Duration = Duration::from_millis(2500);
const HOSTPOOL_INITIAL_RETRY: Duration = Duration::from_secs(30);
const HOSTPOOL_MAX_RETRY: Duration = Duration::from_secs(900);
const HOSTPOOL_INITIAL_EPSILON: f32 = 0.3;
const HOSTPOOL_EPSILON_DECAY: f32 = 0.9;
const HOSTPOOL_MIN_EPSILON: f32 = 0.01;

trait CassandraClock: Send + Sync + fmt::Debug {
    fn now(&self) -> Duration;
}

#[derive(Debug)]
struct MonotonicCassandraClock(Instant);

impl Default for MonotonicCassandraClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl CassandraClock for MonotonicCassandraClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct HostpoolLatencyBucket {
    count: u64,
    total_nanos: u128,
}

#[derive(Debug)]
struct HostpoolHostState {
    dead: bool,
    retry_at: Duration,
    retry_delay: Duration,
    buckets: Vec<HostpoolLatencyBucket>,
    bucket_index: usize,
    last_rotation: Duration,
}

impl Default for HostpoolHostState {
    fn default() -> Self {
        Self {
            dead: false,
            retry_at: Duration::ZERO,
            retry_delay: HOSTPOOL_INITIAL_RETRY,
            buckets: vec![HostpoolLatencyBucket::default(); HOSTPOOL_BUCKETS],
            bucket_index: 0,
            last_rotation: Duration::ZERO,
        }
    }
}

impl HostpoolHostState {
    fn rotate(&mut self, now: Duration) {
        let width = HOSTPOOL_BUCKET_WIDTH.as_nanos();
        let steps = now.saturating_sub(self.last_rotation).as_nanos() / width;
        if steps == 0 {
            return;
        }
        if steps >= HOSTPOOL_BUCKETS as u128 {
            self.buckets.fill(HostpoolLatencyBucket::default());
            self.bucket_index = (self.bucket_index + (steps % HOSTPOOL_BUCKETS as u128) as usize)
                % HOSTPOOL_BUCKETS;
        } else {
            for _ in 0..steps {
                self.bucket_index = (self.bucket_index + 1) % HOSTPOOL_BUCKETS;
                self.buckets[self.bucket_index] = HostpoolLatencyBucket::default();
            }
        }
        if steps >= HOSTPOOL_BUCKETS as u128 {
            self.last_rotation = now;
        } else {
            self.last_rotation += HOSTPOOL_BUCKET_WIDTH * (steps as u32);
        }
    }

    fn record_latency(&mut self, latency: Duration, now: Duration) {
        self.rotate(now);
        let bucket = &mut self.buckets[self.bucket_index];
        bucket.count = bucket.count.saturating_add(1);
        bucket.total_nanos = bucket.total_nanos.saturating_add(latency.as_nanos());
    }

    /// Mirrors go-hostpool's 120-bucket linearly weighted rolling average.
    fn weighted_average_nanos(&self) -> f64 {
        let mut value = 0.0;
        let mut last_value = 0.0;
        for age in 1..=HOSTPOOL_BUCKETS {
            let index = (self.bucket_index + age) % HOSTPOOL_BUCKETS;
            let bucket = self.buckets[index];
            if bucket.count > 0 {
                last_value = bucket.total_nanos as f64 / bucket.count as f64;
            }
            value += last_value * age as f64 / HOSTPOOL_BUCKETS as f64;
        }
        value
    }

    fn mark_failed(&mut self, now: Duration) {
        if !self.dead {
            self.dead = true;
            self.retry_delay = HOSTPOOL_INITIAL_RETRY;
            self.retry_at = now + self.retry_delay;
        }
    }

    fn retry_after_expiry(&mut self, now: Duration) {
        self.retry_delay = (self.retry_delay * 2).min(HOSTPOOL_MAX_RETRY);
        self.retry_at = now + self.retry_delay;
    }
}

fn decay_hostpool_epsilon(epsilon: &mut f32) {
    *epsilon = (*epsilon * HOSTPOOL_EPSILON_DECAY).max(HOSTPOOL_MIN_EPSILON);
}

#[derive(Debug)]
struct HostpoolState {
    epsilon: f32,
    next_host_index: usize,
    hosts: HashMap<Uuid, HostpoolHostState>,
}

impl Default for HostpoolState {
    fn default() -> Self {
        Self {
            epsilon: HOSTPOOL_INITIAL_EPSILON,
            next_host_index: 0,
            hosts: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct CassandraHostPoolPolicy {
    clock: Arc<dyn CassandraClock>,
    state: Mutex<HostpoolState>,
    picks: Mutex<HashMap<usize, Uuid>>,
}

impl Default for CassandraHostPoolPolicy {
    fn default() -> Self {
        Self::with_clock(Arc::new(MonotonicCassandraClock::default()))
    }
}

impl CassandraHostPoolPolicy {
    fn with_clock(clock: Arc<dyn CassandraClock>) -> Self {
        Self {
            clock,
            state: Mutex::new(HostpoolState::default()),
            picks: Mutex::new(HashMap::new()),
        }
    }

    fn choose<'a>(
        &self,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Option<scylla::cluster::NodeRef<'a>> {
        use rand::Rng;

        let nodes: Vec<_> = cluster
            .get_nodes_info()
            .iter()
            .filter(|node| node.is_connected())
            .collect();
        if nodes.is_empty() {
            return None;
        }
        let now = self.clock.now();
        let mut state = self.state.lock().expect("hostpool policy lock");
        let live_ids: std::collections::HashSet<_> =
            nodes.iter().map(|node| node.host_id).collect();
        state.hosts.retain(|id, _| live_ids.contains(id));
        for node in &nodes {
            state.hosts.entry(node.host_id).or_default().rotate(now);
        }

        if rand::thread_rng().gen::<f32>() < state.epsilon {
            decay_hostpool_epsilon(&mut state.epsilon);
            return hostpool_round_robin(&mut state, &nodes, now);
        }

        let mut candidates = Vec::new();
        let mut total_weight = 0.0;
        for (index, node) in nodes.iter().enumerate() {
            let host = state.hosts.get(&node.host_id).expect("host inserted");
            if host.dead && host.retry_at > now {
                continue;
            }
            let average = host.weighted_average_nanos();
            if average > 0.0 {
                let weight = 1.0 / average;
                total_weight += weight;
                candidates.push((index, weight));
            }
        }
        if candidates.is_empty() || total_weight <= 0.0 {
            return hostpool_round_robin(&mut state, &nodes, now);
        }
        let mut target = rand::thread_rng().gen::<f64>() * total_weight;
        let mut selected = candidates.last().map(|(index, _)| *index)?;
        for (index, weight) in candidates {
            target -= weight;
            if target <= 0.0 {
                selected = index;
                break;
            }
        }
        let host = state.hosts.get_mut(&nodes[selected].host_id)?;
        if host.dead {
            host.retry_after_expiry(now);
        }
        Some(nodes[selected])
    }

    fn record(&self, host_id: Uuid, latency: Duration, failed: bool) {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("hostpool policy lock");
        let host = state.hosts.entry(host_id).or_default();
        host.record_latency(latency, now);
        if failed {
            host.mark_failed(now);
        } else {
            host.dead = false;
        }
    }

    fn request_key(request: &scylla::policies::load_balancing::RoutingInfo<'_>) -> usize {
        request as *const _ as usize
    }
}

fn hostpool_round_robin<'a>(
    state: &mut HostpoolState,
    nodes: &[scylla::cluster::NodeRef<'a>],
    now: Duration,
) -> Option<scylla::cluster::NodeRef<'a>> {
    if nodes.is_empty() {
        return None;
    }
    for offset in 0..nodes.len() {
        let index = (offset + state.next_host_index) % nodes.len();
        let host = state.hosts.entry(nodes[index].host_id).or_default();
        if !host.dead {
            state.next_host_index = (index + 1) % nodes.len();
            return Some(nodes[index]);
        }
        if host.retry_at <= now {
            host.retry_after_expiry(now);
            state.next_host_index = (index + 1) % nodes.len();
            return Some(nodes[index]);
        }
    }
    // go-hostpool is configured with returnUnhealthy=true: when every member
    // is inside backoff, it clears the dead marks and returns the first host.
    for node in nodes {
        state.hosts.entry(node.host_id).or_default().dead = false;
    }
    state.next_host_index = 1 % nodes.len();
    Some(nodes[0])
}

impl scylla::policies::load_balancing::LoadBalancingPolicy for CassandraHostPoolPolicy {
    fn pick<'a>(
        &'a self,
        request: &'a scylla::policies::load_balancing::RoutingInfo<'a>,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Option<(scylla::cluster::NodeRef<'a>, Option<scylla::routing::Shard>)> {
        let node = self.choose(cluster)?;
        self.picks
            .lock()
            .expect("hostpool picks lock")
            .insert(Self::request_key(request), node.host_id);
        Some((node, None))
    }

    fn fallback<'a>(
        &'a self,
        request: &'a scylla::policies::load_balancing::RoutingInfo<'a>,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> scylla::policies::load_balancing::FallbackPlan<'a> {
        self.picks
            .lock()
            .expect("hostpool picks lock")
            .remove(&Self::request_key(request));
        Box::new(std::iter::from_fn(move || {
            self.choose(cluster).map(|node| (node, None))
        }))
    }

    fn on_request_success(
        &self,
        request: &scylla::policies::load_balancing::RoutingInfo<'_>,
        latency: Duration,
        node: scylla::cluster::NodeRef<'_>,
    ) {
        self.picks
            .lock()
            .expect("hostpool picks lock")
            .remove(&Self::request_key(request));
        self.record(node.host_id, latency, false);
    }

    fn on_request_failure(
        &self,
        request: &scylla::policies::load_balancing::RoutingInfo<'_>,
        latency: Duration,
        node: scylla::cluster::NodeRef<'_>,
        _error: &scylla::errors::RequestAttemptError,
    ) {
        self.picks
            .lock()
            .expect("hostpool picks lock")
            .remove(&Self::request_key(request));
        self.record(node.host_id, latency, true);
    }

    fn name(&self) -> String {
        "CadenceGoHostPoolEpsilonGreedy".to_string()
    }
}

fn duration_option(nanos: i64, default: Duration, name: &str) -> anyhow::Result<Duration> {
    if nanos == 0 {
        return Ok(default);
    }
    let nanos = u64::try_from(nanos)
        .map_err(|_| anyhow::anyhow!("Cassandra {name} must not be negative"))?;
    Ok(Duration::from_nanos(nanos))
}

fn parse_consistency(value: &str) -> anyhow::Result<Consistency> {
    match value.to_ascii_uppercase().as_str() {
        "ANY" => Ok(Consistency::Any),
        "ONE" => Ok(Consistency::One),
        "TWO" => Ok(Consistency::Two),
        "THREE" => Ok(Consistency::Three),
        "QUORUM" => Ok(Consistency::Quorum),
        "ALL" => Ok(Consistency::All),
        "LOCAL_QUORUM" => Ok(Consistency::LocalQuorum),
        "EACH_QUORUM" => Ok(Consistency::EachQuorum),
        "SERIAL" => Ok(Consistency::Serial),
        "LOCAL_SERIAL" => Ok(Consistency::LocalSerial),
        "LOCAL_ONE" => Ok(Consistency::LocalOne),
        _ => anyhow::bail!("invalid Cassandra consistency {value:?}"),
    }
}

fn parse_serial_consistency(value: &str) -> anyhow::Result<SerialConsistency> {
    match value.to_ascii_uppercase().as_str() {
        "SERIAL" => Ok(SerialConsistency::Serial),
        "LOCAL_SERIAL" => Ok(SerialConsistency::LocalSerial),
        _ => anyhow::bail!("invalid Cassandra serialConsistency {value:?}"),
    }
}

struct CassandraRegionHostFilter(String);

impl scylla::policies::host_filter::HostFilter for CassandraRegionHostFilter {
    fn accept(&self, peer: &scylla::cluster::metadata::Peer) -> bool {
        peer.datacenter
            .as_deref()
            .and_then(|dc| dc.get(..3))
            .is_some_and(|dc| dc == self.0)
    }
}

fn build_rustls_client_config(
    tls: &CassandraTlsConfig,
) -> anyhow::Result<Arc<rustls::ClientConfig>> {
    if tls.cert_file.is_some() != tls.key_file.is_some() {
        anyhow::bail!("Cassandra TLS client certFile and keyFile must be configured together");
    }
    let builder = if tls.verify_hostname {
        let mut roots = rustls::RootCertStore::empty();
        if tls.ca_files.is_empty() {
            let native = rustls_native_certs::load_native_certs();
            if native.certs.is_empty() {
                anyhow::bail!(
                    "failed to load any native Cassandra TLS trust roots: {:?}",
                    native.errors
                );
            }
            let (accepted, _rejected) = roots.add_parsable_certificates(native.certs);
            if accepted == 0 {
                anyhow::bail!(
                    "the native Cassandra TLS trust store contained no usable certificates"
                );
            }
        } else {
            for path in &tls.ca_files {
                let file = std::fs::File::open(path)?;
                let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(file))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if certs.is_empty() {
                    anyhow::bail!(
                        "Cassandra TLS CA file {} contains no certificates",
                        path.display()
                    );
                }
                for cert in certs {
                    roots.add(cert)?;
                }
            }
        }
        rustls::ClientConfig::builder().with_root_certificates(roots)
    } else {
        let algorithms = rustls::crypto::ring::default_provider().signature_verification_algorithms;
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification { algorithms }))
    };
    let client = match (&tls.cert_file, &tls.key_file) {
        (Some(cert), Some(key)) => {
            let cert_file = std::fs::File::open(cert)?;
            let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let key_file = std::fs::File::open(key)?;
            let private_key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cassandra TLS key file {} contains no private key",
                        key.display()
                    )
                })?;
            builder.with_client_auth_cert(certs, private_key)?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(Arc::new(client))
}

const GO_DEFAULT_AUTHENTICATORS: &[&str] = &[
    "org.apache.cassandra.auth.PasswordAuthenticator",
    "com.instaclustr.cassandra.auth.SharedSecretAuthenticator",
    "com.datastax.bdp.cassandra.auth.DseAuthenticator",
    "io.aiven.cassandra.auth.AivenAuthenticator",
    "com.ericsson.bss.cassandra.ecaudit.auth.AuditPasswordAuthenticator",
    "com.amazon.helenus.auth.HelenusAuthenticator",
    "com.ericsson.bss.cassandra.ecaudit.auth.AuditAuthenticator",
];

#[derive(Debug)]
struct GoPasswordAuthenticator {
    username: String,
    password: String,
    allowed: Vec<String>,
}

#[derive(Debug)]
struct GoPasswordSession;

#[async_trait]
impl scylla::authentication::AuthenticatorSession for GoPasswordSession {
    async fn evaluate_challenge(
        &mut self,
        _token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        Err("challenges are not expected for Cassandra password authentication".into())
    }
    async fn success(&mut self, _token: Option<&[u8]>) -> Result<(), String> {
        Ok(())
    }
}

#[async_trait]
impl scylla::authentication::AuthenticatorProvider for GoPasswordAuthenticator {
    async fn start_authentication_session(
        &self,
        name: &str,
    ) -> Result<
        (
            Option<Vec<u8>>,
            Box<dyn scylla::authentication::AuthenticatorSession>,
        ),
        String,
    > {
        let allowed = if self.allowed.is_empty() {
            GO_DEFAULT_AUTHENTICATORS
                .iter()
                .copied()
                .any(|candidate| candidate == name)
        } else {
            self.allowed.iter().any(|candidate| candidate == name)
        };
        if !allowed {
            return Err(format!("unexpected Cassandra authenticator {name:?}"));
        }
        let mut token = Vec::with_capacity(2 + self.username.len() + self.password.len());
        token.push(0);
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());
        Ok((Some(token), Box::new(GoPasswordSession)))
    }
}

#[cfg(test)]
mod session_config_tests {
    use super::*;

    #[derive(Debug, Default)]
    struct FakeClock(Mutex<Duration>);

    impl CassandraClock for FakeClock {
        fn now(&self) -> Duration {
            *self.0.lock().unwrap()
        }
    }

    impl FakeClock {
        fn advance(&self, amount: Duration) {
            let mut now = self.0.lock().unwrap();
            *now += amount;
        }
    }

    #[test]
    fn cassandra_defaults_and_explicit_options_match_go() {
        let config = CassandraSessionConfig::from_parts(
            ["127.0.0.1".to_string()],
            9042,
            "".to_string(),
            "".to_string(),
            "cadence".to_string(),
        )
        .unwrap();
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.connect_timeout, Duration::from_secs(2));
        assert_eq!(config.consistency, Consistency::LocalQuorum);
        assert_eq!(
            config.serial_consistency,
            Some(SerialConsistency::LocalSerial)
        );
        assert_eq!(config.max_connections_per_host.unwrap().get(), 2);

        let configured = config
            .with_go_options(CassandraSessionOptions {
                timeout_nanos: 3_000_000_000,
                connect_timeout_nanos: 1_000_000_000,
                consistency: "QUORUM".into(),
                serial_consistency: "SERIAL".into(),
                max_conns: 8,
                datacenter: "dc1".into(),
                proto_version: 4,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(configured.request_timeout, Duration::from_secs(3));
        assert_eq!(configured.connect_timeout, Duration::from_secs(1));
        assert_eq!(configured.consistency, Consistency::Quorum);
        assert_eq!(
            configured.serial_consistency,
            Some(SerialConsistency::Serial)
        );
        assert_eq!(configured.max_connections_per_host.unwrap().get(), 8);
        assert_eq!(configured.datacenter, "dc1");
    }

    #[test]
    fn cassandra_rejects_protocol_versions_the_driver_cannot_speak() {
        let config = CassandraSessionConfig::from_parts(
            ["127.0.0.1".to_string()],
            9042,
            "".to_string(),
            "".to_string(),
            "cadence".to_string(),
        )
        .unwrap();
        for version in [1, 2, 3, 5, 6] {
            let error = config
                .clone()
                .with_go_options(CassandraSessionOptions {
                    proto_version: version,
                    ..Default::default()
                })
                .unwrap_err()
                .to_string();
            assert!(error.contains(&format!("protoVersion {version}")));
            assert!(error.contains("only speaks CQL protocol v4"));
        }
        assert!(config
            .with_go_options(CassandraSessionOptions {
                proto_version: 4,
                ..Default::default()
            })
            .is_ok());
    }

    #[test]
    fn configured_authenticator_allowlist_is_preserved() {
        let config = CassandraSessionConfig::from_parts(
            ["127.0.0.1".to_string()],
            9042,
            "cadence".to_string(),
            "secret".to_string(),
            "cadence".to_string(),
        )
        .unwrap();
        let configured = config
            .with_go_options(CassandraSessionOptions {
                allowed_authenticators: vec!["custom".into()],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(configured.allowed_authenticators, vec!["custom"]);
    }

    #[test]
    fn round_robin_and_hostpool_policies_are_supported() {
        let config = CassandraSessionConfig::from_parts(
            ["127.0.0.1".to_string()],
            9042,
            "".to_string(),
            "".to_string(),
            "cadence".to_string(),
        )
        .unwrap();
        assert!(config
            .clone()
            .with_go_options(CassandraSessionOptions {
                host_selection_policy: "roundrobin".into(),
                ..Default::default()
            })
            .is_ok());
        let hostpool = config
            .with_go_options(CassandraSessionOptions {
                host_selection_policy: "hostpool-epsilon-greedy".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hostpool.host_selection_policy, "hostpool-epsilon-greedy");
    }

    #[test]
    fn hostpool_latency_buckets_expire_after_the_rolling_window() {
        let clock = Arc::new(FakeClock::default());
        let mut host = HostpoolHostState::default();
        host.record_latency(Duration::from_millis(40), clock.now());
        assert!(host.weighted_average_nanos() > 0.0);

        clock.advance(HOSTPOOL_BUCKET_WIDTH);
        host.rotate(clock.now());
        assert_eq!(host.bucket_index, 1);
        assert!(host.weighted_average_nanos() > 0.0);

        clock.advance(HOSTPOOL_BUCKET_WIDTH * HOSTPOOL_BUCKETS as u32);
        host.rotate(clock.now());
        assert_eq!(host.weighted_average_nanos(), 0.0);
        assert_eq!(host.bucket_index, 1);
    }

    #[test]
    fn hostpool_failure_backoff_doubles_after_each_expired_probe() {
        let clock = Arc::new(FakeClock::default());
        let mut host = HostpoolHostState::default();
        host.mark_failed(clock.now());
        assert_eq!(host.retry_at, HOSTPOOL_INITIAL_RETRY);
        assert_eq!(host.retry_delay, HOSTPOOL_INITIAL_RETRY);

        clock.advance(HOSTPOOL_INITIAL_RETRY);
        host.retry_after_expiry(clock.now());
        assert_eq!(host.retry_delay, Duration::from_secs(60));
        assert_eq!(host.retry_at, Duration::from_secs(90));

        host.retry_delay = HOSTPOOL_MAX_RETRY;
        host.retry_after_expiry(clock.now());
        assert_eq!(host.retry_delay, HOSTPOOL_MAX_RETRY);
    }
}

#[derive(Debug)]
struct NoCertificateVerification {
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}
