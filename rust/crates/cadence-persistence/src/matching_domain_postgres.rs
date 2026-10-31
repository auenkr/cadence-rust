//! Go-compatible PostgreSQL domain and metadata operations used by the
//! Matching cache and Frontend's configured persistence manager.
//!
//! This reader uses the task store's bounded pool, honors Go's default domain
//! shard and pagination order, and never changes the shared schema.

use crate::{
    matching_domain_metadata::{
        decode_go_sql_domain_metadata, MatchingDomainMetadata, MatchingDomainMetadataError,
        MatchingDomainPage,
    },
    matching_postgres::{PostgresConnectionPool, PostgresTaskStore},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const GO_DEFAULT_SHARD_ID: i32 = 54321;
const GO_DOMAIN_REPLICATION_QUEUE_TYPE: i32 = 1;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);
type DomainRow = (Vec<u8>, String, bool, Vec<u8>, String);

#[derive(Clone)]
pub struct PostgresDomainMetadataStore {
    pool: Arc<PostgresConnectionPool>,
    active_cluster_name: String,
}

impl std::fmt::Debug for PostgresDomainMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresDomainMetadataStore")
            .field("active_cluster_name", &self.active_cluster_name)
            .finish_non_exhaustive()
    }
}

impl PostgresDomainMetadataStore {
    pub fn connect(
        config: &crate::matching_postgres::PostgresTaskStoreConfig,
        active_cluster_name: impl Into<String>,
    ) -> Result<Self, MatchingDomainMetadataError> {
        let (_, source) =
            PostgresTaskStore::connect_with_domain_metadata(config.clone(), active_cluster_name)
                .map_err(|error| {
                    MatchingDomainMetadataError::InvalidPostgresConfiguration(error.to_string())
                })?;
        Ok(source)
    }

    /// Append a ThriftRW domain replication task to Go's shared SQL queue.
    /// The row and ack-level reads use the same transaction and lock ordering
    /// as Go's `sqlQueueStore.EnqueueMessage`.
    pub fn enqueue_domain_replication_message(
        &self,
        payload: Vec<u8>,
    ) -> Result<i64, MatchingDomainMetadataError> {
        let started = Instant::now();
        let timeout = DEFAULT_TIMEOUT;
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let last_id = transaction
            .query_opt(
                "SELECT message_id FROM queue WHERE queue_type=$1 ORDER BY message_id DESC LIMIT 1 FOR UPDATE",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE],
            )
            .map_err(domain_error)?
            .map(|row| row.try_get::<_, i64>(0))
            .transpose()
            .map_err(domain_error)?
            .unwrap_or(-1);
        let ack_data = transaction
            .query_opt(
                "SELECT data FROM queue_metadata WHERE queue_type=$1 FOR UPDATE",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE],
            )
            .map_err(domain_error)?
            .map(|row| row.try_get::<_, Vec<u8>>(0))
            .transpose()
            .map_err(domain_error)?;
        let ack_levels: std::collections::HashMap<String, i64> = ack_data
            .map(|data| serde_json::from_slice(&data))
            .transpose()
            .map_err(|error| {
                MatchingDomainMetadataError::InvalidBlob(format!(
                    "invalid domain replication queue ack levels: {error}"
                ))
            })?
            .unwrap_or_default();
        let id = crate::matching_domain_mysql::next_domain_replication_message_id(
            last_id,
            ack_levels.values().copied(),
        );
        transaction
            .execute(
                "INSERT INTO queue (queue_type, message_id, message_payload) VALUES ($1, $2, $3)",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE, &id, &payload],
            )
            .map_err(domain_error)?;
        transaction.commit().map_err(domain_error)?;
        Ok(id)
    }

    /// Read ordered queue rows after `last_message_id`, matching Go's SQL
    /// QueueStore cursor semantics.
    pub fn read_domain_replication_messages(
        &self,
        last_message_id: i64,
        max_count: usize,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationQueueMessage>, MatchingDomainMetadataError> {
        let started = Instant::now();
        let timeout = DEFAULT_TIMEOUT;
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let rows = transaction
            .query(
                "SELECT message_id, message_payload FROM queue WHERE queue_type=$1 AND message_id>$2 ORDER BY message_id ASC LIMIT $3",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE, &last_message_id, &(max_count as i64)],
            )
            .map_err(domain_error)?;
        let messages = rows
            .into_iter()
            .map(|row| {
                Ok(crate::matching_domain_metadata::DomainReplicationQueueMessage {
                    id: row.try_get(0).map_err(domain_error)?,
                    payload: row.try_get(1).map_err(domain_error)?,
                })
            })
            .collect::<Result<Vec<_>, MatchingDomainMetadataError>>()?;
        transaction.commit().map_err(domain_error)?;
        Ok(messages)
    }

    /// Store a consumer's monotonically increasing acknowledgement level in
    /// Go's JSON `queue_metadata` row. Delayed acknowledgements are ignored.
    pub fn update_domain_replication_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        let started = Instant::now();
        let timeout = DEFAULT_TIMEOUT;
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let current = transaction
            .query_opt(
                "SELECT data FROM queue_metadata WHERE queue_type=$1 FOR UPDATE",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE],
            )
            .map_err(domain_error)?
            .map(|row| row.try_get::<_, Vec<u8>>(0))
            .transpose()
            .map_err(domain_error)?;
        let has_current = current.is_some();
        let mut levels: std::collections::HashMap<String, i64> = current
            .map(|data| serde_json::from_slice(&data))
            .transpose()
            .map_err(|error| {
                MatchingDomainMetadataError::InvalidBlob(format!(
                    "invalid domain replication queue ack levels: {error}"
                ))
            })?
            .unwrap_or_default();
        if levels.get(&cluster_name).is_some_and(|old| *old >= message_id) {
            transaction.commit().map_err(domain_error)?;
            return Ok(());
        }
        levels.insert(cluster_name, message_id);
        let data = serde_json::to_vec(&levels).map_err(|error| {
            MatchingDomainMetadataError::InvalidBlob(format!(
                "failed to encode domain replication queue ack levels: {error}"
            ))
        })?;
        if has_current {
            transaction
                .execute(
                    "UPDATE queue_metadata SET data=$1 WHERE queue_type=$2",
                    &[&data, &GO_DOMAIN_REPLICATION_QUEUE_TYPE],
                )
                .map_err(domain_error)?;
        } else {
            transaction
                .execute(
                    "INSERT INTO queue_metadata (queue_type, data) VALUES ($1, $2)",
                    &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE, &data],
                )
                .map_err(domain_error)?;
        }
        transaction.commit().map_err(domain_error)
    }

    pub fn get_domain_replication_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, MatchingDomainMetadataError> {
        let started = Instant::now();
        let timeout = DEFAULT_TIMEOUT;
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let data = transaction
            .query_opt(
                "SELECT data FROM queue_metadata WHERE queue_type=$1",
                &[&GO_DOMAIN_REPLICATION_QUEUE_TYPE],
            )
            .map_err(domain_error)?
            .map(|row| row.try_get::<_, Vec<u8>>(0))
            .transpose()
            .map_err(domain_error)?;
        let levels = data
            .map(|data| serde_json::from_slice(&data))
            .transpose()
            .map_err(|error| {
                MatchingDomainMetadataError::InvalidBlob(format!(
                    "invalid domain replication queue ack levels: {error}"
                ))
            })?
            .unwrap_or_default();
        transaction.commit().map_err(domain_error)?;
        Ok(levels)
    }

    /// Create a Go SQL domain row and advance `domain_metadata` in the same
    /// transaction, matching `sqlDomainStore.CreateDomain`. The flattened
    /// ThriftRW blob is the existing Go schema payload; this never migrates or
    /// creates tables.
    pub fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError> {
        // Go reads metadata before starting the domain transaction. Keeping
        // this read outside the transaction preserves its concurrent-create
        // conflict behavior: the later compare-and-set may reject a stale
        // metadata version and roll back the inserted domain row.
        let notification_version = self.get_metadata_version()?;
        let mut domain = metadata.clone();
        domain.notification_version = notification_version;
        let blob = crate::matching_domain_metadata::encode_go_sql_domain_metadata(&domain)?;
        let uuid = uuid::Uuid::parse_str(&metadata.info.uuid)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(DEFAULT_TIMEOUT)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, DEFAULT_TIMEOUT.saturating_sub(started.elapsed()))?;
        transaction
            .execute(
                "INSERT INTO domains (shard_id, id, name, is_global, data, data_encoding) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &GO_DEFAULT_SHARD_ID,
                    &uuid.as_bytes().as_slice(),
                    &metadata.info.name,
                    &metadata.is_global_domain,
                    &blob,
                    &"thriftrw",
                ],
            )
            .map_err(domain_error)?;
        transaction
            .query_one("SELECT notification_version FROM domain_metadata FOR UPDATE", &[])
            .map_err(domain_error)?;
        let changed = transaction
            .execute(
                "UPDATE domain_metadata SET notification_version=$1 WHERE notification_version=$2",
                &[&(notification_version + 1), &notification_version],
            )
            .map_err(domain_error)?;
        if changed != 1 {
            return Err(MatchingDomainMetadataError::InvalidBlob(
                "Failed to update domain metadata. <>1 rows affected.".into(),
            ));
        }
        transaction.commit().map_err(domain_error)?;
        Ok(notification_version)
    }

    /// Update a Go SQL domain row and its global notification version in one
    /// transaction, matching `sqlDomainStore.UpdateDomain` optimistic
    /// notification-version handling.
    pub fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(DEFAULT_TIMEOUT)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, DEFAULT_TIMEOUT.saturating_sub(started.elapsed()))?;
        let blob = crate::matching_domain_metadata::encode_go_sql_domain_metadata(metadata)?;
        let uuid = uuid::Uuid::parse_str(&metadata.info.uuid)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let changed = transaction
            .execute(
                "UPDATE domains SET name=$1, data=$2, data_encoding=$3 WHERE shard_id=$4 AND id=$5",
                &[
                    &metadata.info.name,
                    &blob,
                    &"thriftrw",
                    &GO_DEFAULT_SHARD_ID,
                    &uuid.as_bytes().as_slice(),
                ],
            )
            .map_err(domain_error)?;
        if changed != 1 {
            return Err(MatchingDomainMetadataError::NotFound(
                metadata.info.uuid.clone(),
            ));
        }
        let row = transaction
            .query_one("SELECT notification_version FROM domain_metadata FOR UPDATE", &[])
            .map_err(domain_error)?;
        let current: i64 = row.try_get(0).map_err(domain_error)?;
        if current != expected_notification_version {
            return Err(MatchingDomainMetadataError::InvalidBlob(format!(
                "domain metadata notification version changed: expected {expected_notification_version}, found {current}"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE domain_metadata SET notification_version=$1 WHERE notification_version=$2",
                &[&(expected_notification_version + 1), &expected_notification_version],
            )
            .map_err(domain_error)?;
        if changed != 1 {
            return Err(MatchingDomainMetadataError::InvalidBlob(
                "Failed to update domain metadata. <>1 rows affected.".into(),
            ));
        }
        transaction.commit().map_err(domain_error)
    }

    /// Delete a Go SQL domain row. Go does not advance `domain_metadata` for
    /// deletion, so this operation deliberately touches only `domains`.
    pub fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError> {
        let uuid = uuid::Uuid::parse_str(domain_id)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(DEFAULT_TIMEOUT)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, DEFAULT_TIMEOUT.saturating_sub(started.elapsed()))?;
        transaction
            .execute(
                "DELETE FROM domains WHERE shard_id=$1 AND id=$2",
                &[&GO_DEFAULT_SHARD_ID, &uuid.as_bytes().as_slice()],
            )
            .map_err(domain_error)?;
        transaction.commit().map_err(domain_error)
    }

    pub(crate) fn from_shared_pool(
        pool: Arc<PostgresConnectionPool>,
        active_cluster_name: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            active_cluster_name: active_cluster_name.into(),
        }
    }

    pub fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        self.get_metadata_version_with_timeout(DEFAULT_TIMEOUT)
    }

    pub fn get_metadata_version_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<i64, MatchingDomainMetadataError> {
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let result = transaction
            .query_opt("SELECT notification_version FROM domain_metadata", &[])
            .map_err(domain_error)?
            .ok_or_else(|| {
                MatchingDomainMetadataError::InvalidBlob(
                    "Go domain_metadata table has no notification-version row".into(),
                )
            })?
            .try_get(0)
            .map_err(domain_error)?;
        transaction.commit().map_err(domain_error)?;
        Ok(result)
    }

    pub fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        self.get_domain_with_timeout(name, id, DEFAULT_TIMEOUT)
    }

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
            (None, None) => Err(MatchingDomainMetadataError::EmptyIdentity),
            (Some(name), None) => self.read_one("name", &name, name, timeout),
            (None, Some(id)) => {
                let uuid = uuid::Uuid::parse_str(id)
                    .map_err(|_| MatchingDomainMetadataError::InvalidUuid(id.to_owned()))?;
                self.read_one("id", &uuid.as_bytes().to_vec(), id, timeout)
            }
        }
    }

    fn read_one(
        &self,
        selector: &str,
        value: &(dyn postgres::types::ToSql + Sync),
        identity: &str,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        let query = match selector {
            "name" => "SELECT id,name,is_global,data,data_encoding FROM domains WHERE shard_id=$1 AND name=$2",
            "id" => "SELECT id,name,is_global,data,data_encoding FROM domains WHERE shard_id=$1 AND id=$2",
            _ => unreachable!("selector is constrained by caller"),
        };
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let row = transaction
            .query_opt(query, &[&GO_DEFAULT_SHARD_ID, value])
            .map_err(domain_error)?;
        transaction.commit().map_err(domain_error)?;
        let row = row.ok_or_else(|| MatchingDomainMetadataError::NotFound(identity.into()))?;
        self.decode_row(read_domain_row(&row)?)
    }

    pub fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        self.list_domains_with_timeout(page_size, next_page_token, DEFAULT_TIMEOUT)
    }

    pub fn list_domains_with_timeout(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        if page_size == 0 {
            return Err(MatchingDomainMetadataError::InvalidPageSize);
        }
        let limit =
            i64::try_from(page_size).map_err(|_| MatchingDomainMetadataError::InvalidPageSize)?;
        let started = Instant::now();
        let mut client = self
            .pool
            .checkout_with_timeout(timeout)
            .map_err(checkout_error)?;
        let mut transaction = client.transaction().map_err(domain_error)?;
        set_statement_timeout(&mut transaction, timeout.saturating_sub(started.elapsed()))?;
        let rows = match next_page_token {
            Some(token) => transaction.query(
                "SELECT id,name,is_global,data,data_encoding FROM domains WHERE shard_id=$1 AND id>$2 ORDER BY id LIMIT $3",
                &[&GO_DEFAULT_SHARD_ID, &token, &limit],
            ),
            None => transaction.query(
                "SELECT id,name,is_global,data,data_encoding FROM domains WHERE shard_id=$1 ORDER BY id LIMIT $2",
                &[&GO_DEFAULT_SHARD_ID, &limit],
            ),
        }
        .map_err(domain_error)?;
        transaction.commit().map_err(domain_error)?;
        let next_page_token = if rows.len() >= page_size {
            rows.last().map(|row| row.get::<_, Vec<u8>>(0))
        } else {
            None
        };
        let domains = rows
            .iter()
            .map(read_domain_row)
            .map(|row| row.and_then(|row| self.decode_row(row)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MatchingDomainPage {
            domains,
            next_page_token,
        })
    }

    fn decode_row(
        &self,
        (id, name, is_global, data, encoding): DomainRow,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        decode_go_sql_domain_metadata(
            id,
            name,
            is_global,
            data,
            encoding,
            &self.active_cluster_name,
        )
    }
}

impl crate::matching_domain_metadata::GoSqlDomainPersistence for PostgresDomainMetadataStore {
    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::create_domain(self, metadata)
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::update_domain(self, metadata, expected_notification_version)
    }

    fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::delete_domain(self, domain_id)
    }

    fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::get_metadata_version(self)
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::get_domain(self, name, id)
    }

    fn list_domains(
        &self,
        page_size: usize,
        page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        PostgresDomainMetadataStore::list_domains(self, page_size, page_token)
    }
}

impl crate::matching_domain_metadata::DomainMetadataPersistence for PostgresDomainMetadataStore {
    fn metadata_version(&self) -> Result<i64, String> {
        self.get_metadata_version().map_err(|error| error.to_string())
    }

    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        _expected_version: i64,
    ) -> Result<(), String> {
        PostgresDomainMetadataStore::create_domain(self, metadata)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
    ) -> Result<(), String> {
        PostgresDomainMetadataStore::update_domain(self, metadata, expected_version)
            .map_err(|error| error.to_string())
    }

    fn delete_domain(&self, id: &str) -> Result<(), String> {
        PostgresDomainMetadataStore::delete_domain(self, id).map_err(|error| error.to_string())
    }

    fn enqueue_domain_replication_message(&self, payload: Vec<u8>) -> Result<i64, String> {
        PostgresDomainMetadataStore::enqueue_domain_replication_message(self, payload)
            .map_err(|error| error.to_string())
    }

    fn read_domain_replication_messages(
        &self,
        last_message_id: i64,
        max_count: usize,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationQueueMessage>, String> {
        PostgresDomainMetadataStore::read_domain_replication_messages(
            self,
            last_message_id,
            max_count,
        )
        .map_err(|error| error.to_string())
    }

    fn get_domain_replication_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        PostgresDomainMetadataStore::get_domain_replication_ack_levels(self)
            .map_err(|error| error.to_string())
    }

    fn update_domain_replication_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        PostgresDomainMetadataStore::update_domain_replication_ack_level(
            self,
            cluster_name,
            message_id,
        )
        .map_err(|error| error.to_string())
    }
}

fn read_domain_row(row: &postgres::Row) -> Result<DomainRow, MatchingDomainMetadataError> {
    Ok((
        row.try_get(0).map_err(domain_error)?,
        row.try_get(1).map_err(domain_error)?,
        row.try_get(2).map_err(domain_error)?,
        row.try_get(3).map_err(domain_error)?,
        row.try_get(4).map_err(domain_error)?,
    ))
}

fn set_statement_timeout(
    transaction: &mut postgres::Transaction<'_>,
    timeout: Duration,
) -> Result<(), MatchingDomainMetadataError> {
    let millis = timeout.as_millis().max(1).min(i32::MAX as u128) as i32;
    transaction
        .query_one(
            "SELECT set_config('statement_timeout',$1,true)",
            &[&format!("{millis}ms")],
        )
        .map(|_| ())
        .map_err(domain_error)
}

fn domain_error(error: impl Into<postgres::Error>) -> MatchingDomainMetadataError {
    let error = error.into();
    if error.code().is_some_and(|code| code.code() == "57014") {
        MatchingDomainMetadataError::OperationTimedOut
    } else {
        MatchingDomainMetadataError::Postgres(error)
    }
}

fn checkout_error(
    error: crate::matching_task_store::MatchingTaskStoreError,
) -> MatchingDomainMetadataError {
    match error {
        crate::matching_task_store::MatchingTaskStoreError::Backend {
            kind: crate::matching_task_store::BackendErrorKind::Timeout,
            ..
        } => MatchingDomainMetadataError::OperationTimedOut,
        other => MatchingDomainMetadataError::InvalidPostgresConfiguration(other.to_string()),
    }
}
