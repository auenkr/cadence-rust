//! Cassandra driver session setup shared by Matching's persistence adapters.
//!
//! The driver is asynchronous. Consumers must await its APIs from async
//! service paths; they must not call a nested runtime or synchronously wait on
//! driver futures from Tokio workers.

use crate::{
    AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, SemaphorePersistence,
    SemaphoreRow,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    statement::batch::{Batch, BatchType},
};
use std::{fmt, sync::Arc};
use uuid::Uuid;

const TOKEN_ROW: i32 = 1;
const OWNER_ROW: i32 = 2;
const OWNER_TOKEN_SENTINEL: i32 = -1;
const OWNER_NONE_SENTINEL: &str = "0::30000000-0000-f000-f000-000000000000:-1";
const FREE_SENTINEL: &str = "0::30000000-0000-f000-f000-000000000000:-2";
const INSERT_METADATA: &str = "INSERT INTO semaphore_metadata (domain_id, semaphore_name, size, bucket_size, created_time) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS";
const SELECT_METADATA: &str = "SELECT size, bucket_size FROM semaphore_metadata WHERE domain_id = ? AND semaphore_name = ?";
const SELECT_BUCKET: &str = "SELECT type, token_id, owner_id, holder, held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ?";
const SELECT_TOKEN: &str = "SELECT holder FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ?";
const SELECT_OWNER: &str = "SELECT held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";
const GRANT_TOKEN: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder IN (?, null)";
const INSERT_OWNER: &str = "INSERT INTO semaphore_tokens (domain_id, semaphore_name, bucket, type, token_id, owner_id, holder, held_token, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const RELEASE_TOKEN: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder = ?";
const DELETE_OWNER: &str = "DELETE FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";

/// Go's Cassandra `semaphore_metadata` and `semaphore_tokens` persistence.
/// The session is shared by Matching task/domain/semaphore stores when the
/// configured Cassandra backend is wired by cadence-server.
pub struct CassandraSemaphorePersistence {
    session: Arc<Session>,
}

impl fmt::Debug for CassandraSemaphorePersistence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraSemaphorePersistence").finish_non_exhaustive()
    }
}

impl CassandraSemaphorePersistence {
    pub fn new(session: Arc<Session>) -> Self { Self { session } }

    fn domain_id(value: &str) -> Result<Uuid, AcquireError> {
        Uuid::parse_str(value).map_err(|error| AcquireError::Invalid(format!("invalid domainID {value:?}: {error}")))
    }

    fn internal(error: impl fmt::Display) -> AcquireError {
        AcquireError::Internal(error.to_string())
    }

    async fn owner_token(&self, domain_id: Uuid, name: &str, bucket: i32, owner: &str) -> Result<i32, AcquireError> {
        let result = self.session.query_unpaged(
            SELECT_OWNER,
            (domain_id, name, bucket, OWNER_ROW, OWNER_TOKEN_SENTINEL, owner),
        ).await.map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        rows.maybe_first_row::<(i32,)>().map_err(Self::internal)?
            .map(|(token,)| token)
            .ok_or_else(|| AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                format!("semaphore owner {owner:?} does not hold a token in bucket {bucket}"),
            )))
    }

    async fn token_holder(&self, domain_id: Uuid, name: &str, bucket: i32, token_id: i32) -> Result<String, AcquireError> {
        let result = self.session.query_unpaged(
            SELECT_TOKEN,
            (domain_id, name, bucket, TOKEN_ROW, token_id),
        ).await.map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let holder = rows.maybe_first_row::<(Option<String>,)>().map_err(Self::internal)?
            .map(|(holder,)| holder)
            .ok_or_else(|| AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                format!("semaphore token {token_id} does not exist in bucket {bucket}"),
            )))?;
        Ok(holder.filter(|holder| holder != FREE_SENTINEL).unwrap_or_default())
    }
}

#[async_trait]
impl SemaphorePersistence for CassandraSemaphorePersistence {
    async fn create_semaphore(&self, domain_id: &str, name: &str, size: i32, bucket_size: i32) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 { return Err(SemaphoreError::Invalid(format!("Size must be positive, got {size}"))); }
        if !(0..=crate::semaphores::MAX_BUCKET_SIZE).contains(&bucket_size) {
            return Err(SemaphoreError::Invalid(format!("BucketSize must not exceed {}, got {bucket_size}", crate::semaphores::MAX_BUCKET_SIZE)));
        }
        let domain_id = Uuid::parse_str(domain_id).map_err(|error| SemaphoreError::Invalid(format!("invalid domainID: {error}")))?;
        let bucket_size = if bucket_size == 0 { crate::semaphores::DEFAULT_BUCKET_SIZE } else { bucket_size };
        let created_time: DateTime<Utc> = Utc::now();
        let result = self.session.query_unpaged(INSERT_METADATA, (domain_id, name, size, bucket_size, created_time))
            .await.map_err(|error| SemaphoreError::Internal(error.to_string()))?;
        // Cassandra returns `[applied]` plus the previous metadata on a
        // conditional conflict. Both shapes use the same typed columns.
        let rows = result.into_rows_result().map_err(|error| SemaphoreError::Internal(error.to_string()))?;
        let applied = rows.maybe_first_row::<(bool, Option<Uuid>, Option<String>, Option<i32>, Option<i32>, Option<DateTime<Utc>>)>()
            .map_err(|error| SemaphoreError::Internal(error.to_string()))?
            .map(|(applied, ..)| applied).unwrap_or(false);
        if !applied { return Err(SemaphoreError::Exists(format!("semaphore {name:?} already exists"))); }
        Ok(SemaphoreMetadata { size, bucket_size })
    }

    async fn get_semaphore_metadata(&self, domain_id: &str, name: &str) -> Result<SemaphoreMetadata, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        let result = self.session.query_unpaged(SELECT_METADATA, (domain_id, name)).await.map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        rows.maybe_first_row::<(i32, i32)>().map_err(Self::internal)?
            .map(|(size, bucket_size)| SemaphoreMetadata { size, bucket_size })
            .ok_or_else(|| AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                format!("semaphore {name:?} does not exist in domain {domain_id}"),
            )))
    }

    async fn scan_semaphore_bucket(&self, domain_id: &str, name: &str, bucket: i32) -> Result<Vec<SemaphoreRow>, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        let result = self.session.query_unpaged(SELECT_BUCKET, (domain_id, name, bucket)).await.map_err(Self::internal)?;
        let rows = result.into_rows_result().map_err(Self::internal)?;
        let mut output = Vec::new();
        for row in rows.rows::<(i32, i32, Option<String>, Option<String>, Option<i32>)>().map_err(Self::internal)? {
            let (row_type, token_id, owner_id, holder, held_token) = row.map_err(Self::internal)?;
            match row_type {
                TOKEN_ROW => output.push(SemaphoreRow::Token {
                    token_id,
                    holder: holder.filter(|holder| holder != FREE_SENTINEL).unwrap_or_default(),
                }),
                OWNER_ROW => if let (Some(owner_id), Some(held_token)) = (owner_id, held_token) {
                    output.push(SemaphoreRow::Owner { owner_id, held_token });
                },
                _ => return Err(AcquireError::Internal(format!("unknown semaphore row type {row_type}"))),
            }
        }
        Ok(output)
    }

    async fn get_semaphore_ownership_by_token(&self, domain_id: &str, name: &str, bucket: i32, token_id: i32) -> Result<String, AcquireError> {
        self.token_holder(Self::domain_id(domain_id)?, name, bucket, token_id).await
    }

    async fn get_semaphore_ownership_by_owner(&self, domain_id: &str, name: &str, bucket: i32, owner_id: &str) -> Result<i32, AcquireError> {
        self.owner_token(Self::domain_id(domain_id)?, name, bucket, owner_id).await
    }

    async fn grant_semaphore_token(&self, domain_id: &str, name: &str, bucket: i32, token_id: i32, owner_id: &str) -> Result<GrantOutcome, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        match self.owner_token(domain_id, name, bucket, owner_id).await {
            Ok(existing) => return Ok(GrantOutcome::AlreadyHeld(existing)),
            Err(AcquireError::NotFound(_)) => {},
            Err(error) => return Err(error),
        }
        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(GRANT_TOKEN);
        batch.append_statement(INSERT_OWNER);
        let now: DateTime<Utc> = Utc::now();
        self.session.batch(&batch, (
            (owner_id, now, domain_id, name, bucket, TOKEN_ROW, token_id, OWNER_NONE_SENTINEL, FREE_SENTINEL),
            (domain_id, name, bucket, OWNER_ROW, OWNER_TOKEN_SENTINEL, owner_id, None::<String>, token_id, now),
        )).await.map_err(Self::internal)?;
        match self.owner_token(domain_id, name, bucket, owner_id).await {
            Ok(held) if held == token_id => Ok(GrantOutcome::Applied),
            Ok(held) => Ok(GrantOutcome::AlreadyHeld(held)),
            Err(AcquireError::NotFound(_)) => match self.token_holder(domain_id, name, bucket, token_id).await {
                Ok(holder) if !holder.is_empty() => Ok(GrantOutcome::SlotTaken),
                Ok(_) | Err(AcquireError::NotFound(_)) => Ok(GrantOutcome::SlotTaken),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    async fn release_semaphore_token(&self, domain_id: &str, name: &str, bucket: i32, token_id: i32, owner_id: &str) -> Result<bool, AcquireError> {
        let domain_id = Self::domain_id(domain_id)?;
        if self.token_holder(domain_id, name, bucket, token_id).await? != owner_id { return Ok(false); }
        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(RELEASE_TOKEN);
        batch.append_statement(DELETE_OWNER);
        let now: DateTime<Utc> = Utc::now();
        self.session.batch(&batch, (
            (FREE_SENTINEL, now, domain_id, name, bucket, TOKEN_ROW, token_id, OWNER_NONE_SENTINEL, owner_id),
            (domain_id, name, bucket, OWNER_ROW, OWNER_TOKEN_SENTINEL, owner_id),
        )).await.map_err(Self::internal)?;
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
}

impl CassandraSessionConfig {
    pub fn from_parts(
        hosts: impl IntoIterator<Item = String>,
        port: i32,
        user: impl Into<String>,
        password: impl Into<String>,
        keyspace: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let hosts: Vec<String> = hosts.into_iter().filter(|host| !host.trim().is_empty()).collect();
        if hosts.is_empty() {
            anyhow::bail!("Cassandra hosts must not be empty");
        }
        let port = if port == 0 { 9042 } else { port };
        let port = u16::try_from(port)
            .map_err(|_| anyhow::anyhow!("Cassandra port must be between 1 and 65535"))?;
        let keyspace = keyspace.into();
        if keyspace.is_empty() {
            anyhow::bail!("Cassandra keyspace must not be empty");
        }
        Ok(Self { hosts, port, user: user.into(), password: password.into(), keyspace })
    }

    /// Connect to all configured contact points and select Cadence's existing
    /// keyspace. This never creates a keyspace or applies schema migrations.
    pub async fn connect(&self) -> anyhow::Result<Session> {
        let nodes: Vec<String> = self.hosts.iter().map(|host| {
            let host = host.trim();
            if host.starts_with('[') {
                format!("{host}:{}", self.port)
            } else if host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_ipv6()) {
                format!("[{host}]:{}", self.port)
            } else {
                format!("{host}:{}", self.port)
            }
        }).collect();
        let mut builder = SessionBuilder::new()
            .known_nodes(nodes)
            .use_keyspace(self.keyspace.clone(), true);
        if !self.user.is_empty() {
            builder = builder.user(self.user.clone(), self.password.clone());
        }
        Ok(builder.build().await?)
    }
}
