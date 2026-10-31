//! Go-compatible MySQL domain and metadata operations used by the Matching
//! cache and Frontend's configured persistence manager.
//!
//! This uses the same MySQL pool/options as the task-store adapter and reads
//! the Go default domain shard (54321). It never writes or migrates schema.

use crate::matching_domain_metadata::{
    decode_go_sql_domain_metadata, MatchingDomainMetadata, MatchingDomainMetadataError,
    MatchingDomainPage,
};
use crate::matching_mysql::{MySqlTaskStore, MySqlTaskStoreConfig};
use mysql_async::{prelude::Queryable as AsyncQueryable, TxOpts};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const GO_DEFAULT_SHARD_ID: i32 = 54321;
const GO_DOMAIN_PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(3);
type DomainMetadataRow = (Vec<u8>, String, bool, Vec<u8>, String);

#[derive(Clone)]
pub struct MySqlDomainMetadataStore {
    pool: Arc<crate::matching_mysql::SyncConnectionPool>,
    active_cluster_name: String,
}

impl std::fmt::Debug for MySqlDomainMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlDomainMetadataStore")
            .field("active_cluster_name", &self.active_cluster_name)
            .finish_non_exhaustive()
    }
}

impl MySqlDomainMetadataStore {
    /// Open a reader using the same connection options and pool limits as the
    /// MySQL Matching task store. Schema checks are deferred until reads.
    pub fn connect(
        config: &MySqlTaskStoreConfig,
        active_cluster_name: impl Into<String>,
    ) -> Result<Self, MatchingDomainMetadataError> {
        let pool = MySqlTaskStore::create_mysql_pool(config).map_err(|error| {
            MatchingDomainMetadataError::InvalidMySqlConfiguration(error.to_string())
        })?;
        Ok(Self {
            pool,
            active_cluster_name: active_cluster_name.into(),
        })
    }

    /// Create a Go SQL domain row and increment the singleton domain metadata
    /// notification version in the same transaction.
    pub fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError> {
        use mysql_async::prelude::Queryable;
        let mut metadata = metadata.clone();
        // Go reads the metadata version before opening the create transaction;
        // concurrent creates can therefore conflict at the later CAS.
        let notification_version = self.get_metadata_version()?;
        metadata.notification_version = notification_version;
        let data = crate::matching_domain_metadata::encode_go_sql_domain_metadata(&metadata)?;
        let id = uuid::Uuid::parse_str(&metadata.info.uuid)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?;
        let domain_name = metadata.info.name.clone();
        let is_global_domain = metadata.is_global_domain;
        let deadline = Instant::now() + GO_DOMAIN_PERSISTENCE_TIMEOUT;
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    let mut transaction = connection
                        .start_transaction(mysql_async::TxOpts::default())
                        .await?;
                    transaction
                        .exec_drop(
                            "INSERT INTO domains (shard_id, id, name, is_global, data, data_encoding) VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                GO_DEFAULT_SHARD_ID,
                                id.as_bytes().to_vec(),
                                domain_name,
                                is_global_domain,
                                data,
                                "thriftrw",
                            ),
                        )
                        .await?;
                    let current: Option<i64> = transaction
                        .exec_first(
                            "SELECT notification_version FROM domain_metadata FOR UPDATE",
                            (),
                        )
                        .await?;
                    if current.is_none() {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            "Go domain_metadata table has no notification-version row",
                        ))));
                    }
                    transaction
                        .exec_drop(
                            "UPDATE domain_metadata SET notification_version=? WHERE notification_version=?",
                            (notification_version + 1, notification_version),
                        )
                        .await?;
                    if transaction.affected_rows() != 1 {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            "Failed to update domain metadata. <>1 rows affected.",
                        ))));
                    }
                    transaction.commit().await?;
                    Ok(notification_version)
                })
            })
            .map_err(query_error)
    }

    /// Update the Go domain row and conditionally increment the singleton
    /// notification version in the same transaction.
    pub fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        use mysql_async::prelude::Queryable;
        let metadata = metadata.clone();
        let deadline = Instant::now() + GO_DOMAIN_PERSISTENCE_TIMEOUT;
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    let mut transaction = connection
                        .start_transaction(mysql_async::TxOpts::default())
                        .await?;
                    let data = crate::matching_domain_metadata::encode_go_sql_domain_metadata(
                        &metadata,
                    )
                    .map_err(|error| mysql_async::Error::Other(Box::new(error)))?;
                    let id = uuid::Uuid::parse_str(&metadata.info.uuid).map_err(|error| {
                        mysql_async::Error::Other(Box::new(error))
                    })?;
                    transaction
                        .exec_drop(
                            "UPDATE domains SET name=?, data=?, data_encoding=? WHERE shard_id=? AND id=?",
                            (
                                metadata.info.name,
                                data,
                                "thriftrw",
                                GO_DEFAULT_SHARD_ID,
                                id.as_bytes().to_vec(),
                            ),
                        )
                        .await?;
                    if transaction.affected_rows() != 1 {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            "domain update affected <>1 rows",
                        ))));
                    }
                    let current: Option<i64> = transaction
                        .exec_first(
                            "SELECT notification_version FROM domain_metadata FOR UPDATE",
                            (),
                        )
                        .await?;
                    if current != Some(expected_notification_version) {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            format!(
                                "domain metadata notification version changed: expected {expected_notification_version}, found {current:?}"
                            ),
                        ))));
                    }
                    transaction
                        .exec_drop(
                            "UPDATE domain_metadata SET notification_version=? WHERE notification_version=?",
                            (expected_notification_version + 1, expected_notification_version),
                        )
                        .await?;
                    if transaction.affected_rows() != 1 {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            "Failed to update domain metadata. <>1 rows affected.",
                        ))));
                    }
                    transaction.commit().await
                })
            })
            .map_err(query_error)
    }

    /// Delete a Go SQL domain row; Go does not increment domain metadata for
    /// deletion.
    pub fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError> {
        use mysql_async::prelude::Queryable;
        let domain_id = uuid::Uuid::parse_str(domain_id)
            .map_err(|error| MatchingDomainMetadataError::InvalidUuid(error.to_string()))?
            .as_bytes()
            .to_vec();
        let deadline = Instant::now() + GO_DOMAIN_PERSISTENCE_TIMEOUT;
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    let mut transaction = connection
                        .start_transaction(mysql_async::TxOpts::default())
                        .await?;
                    transaction
                        .exec_drop(
                            "DELETE FROM domains WHERE shard_id=? AND id=?",
                            (GO_DEFAULT_SHARD_ID, domain_id),
                        )
                        .await?;
                    transaction.commit().await
                })
            })
            .map_err(query_error)
    }

    pub(crate) fn from_shared_pool(
        pool: Arc<crate::matching_mysql::SyncConnectionPool>,
        active_cluster_name: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            active_cluster_name: active_cluster_name.into(),
        }
    }

    /// Read the cluster-wide notification version from Go's single metadata row.
    pub fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        self.get_metadata_version_with_timeout(GO_DOMAIN_PERSISTENCE_TIMEOUT)
    }

    /// Insert a Go-compatible domain row and advance the shared metadata
    /// notification version in the same transaction. `expected_version` is
    /// the value read before building the domain update, matching Go's
    /// lock-and-compare persistence contract.
    pub fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        self.write_domain(metadata, expected_version, true)
    }

    /// Replace the serialized Go domain row and advance its notification
    /// version atomically.
    pub fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        self.write_domain(metadata, expected_version, false)
    }

    /// Delete a domain by UUID, matching Go's SQL DeleteDomain contract. Go
    /// does not advance `domain_metadata.notification_version` for deletion.
    pub fn delete_domain(&self, id: &str) -> Result<(), MatchingDomainMetadataError> {
        let uuid = uuid::Uuid::parse_str(id)
            .map_err(|_| MatchingDomainMetadataError::InvalidUuid(id.to_owned()))?;
        let deadline = Instant::now() + GO_DOMAIN_PERSISTENCE_TIMEOUT;
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    connection
                        .exec_drop(
                            "DELETE FROM domains WHERE shard_id = ? AND id = ?",
                            (GO_DEFAULT_SHARD_ID, uuid.as_bytes().to_vec()),
                        )
                        .await
                })
            })
            .map_err(query_error)
    }

    fn write_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
        create: bool,
    ) -> Result<(), MatchingDomainMetadataError> {
        let uuid = uuid::Uuid::parse_str(&metadata.info.uuid).map_err(|_| {
            MatchingDomainMetadataError::InvalidUuid(metadata.info.uuid.clone())
        })?;
        let name = metadata.info.name.clone();
        let is_global_domain = metadata.is_global_domain;
        let data = crate::matching_domain_metadata::encode_go_sql_domain_metadata(metadata)?;
        let deadline = Instant::now() + GO_DOMAIN_PERSISTENCE_TIMEOUT;
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    let mut transaction = connection.start_transaction(TxOpts::default()).await?;
                    if create {
                        transaction
                            .exec_drop(
                                "INSERT INTO domains (shard_id, id, name, data, data_encoding, is_global) VALUES (?, ?, ?, ?, 'thriftrw', ?)",
                                (GO_DEFAULT_SHARD_ID, uuid.as_bytes().to_vec(), name, data, is_global_domain),
                            )
                            .await?;
                    } else {
                        transaction
                            .exec_drop(
                                "UPDATE domains SET data = ?, data_encoding = 'thriftrw' WHERE shard_id = ? AND id = ?",
                                (data, GO_DEFAULT_SHARD_ID, uuid.as_bytes().to_vec()),
                            )
                            .await?;
                        if transaction.affected_rows() != 1 {
                            return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                                "domain update affected no rows",
                            ))));
                        }
                    }
                    transaction
                        .exec_drop(
                            "UPDATE domain_metadata SET notification_version = notification_version + 1 WHERE notification_version = ?",
                            (expected_version,),
                        )
                        .await?;
                    if transaction.affected_rows() != 1 {
                        return Err(mysql_async::Error::Other(Box::new(std::io::Error::other(
                            "domain metadata notification version changed",
                        ))));
                    }
                    transaction.commit().await
                })
            })
            .map_err(query_error)
    }

    /// Read metadata with the remaining Go cache refresh budget.
    pub fn get_metadata_version_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<i64, MatchingDomainMetadataError> {
        let deadline = Instant::now() + timeout;
        self.pool
            .query_until(deadline, |connection| {
                Box::pin(async move { connection.exec_first(
                    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ notification_version FROM domain_metadata",
                    (),
                ).await })
            })
            .map_err(query_error)?
            .ok_or_else(|| {
                MatchingDomainMetadataError::InvalidBlob(
                    "Go domain_metadata table has no notification-version row".into(),
                )
            })
    }

    /// Read one domain by exactly one of name or canonical UUID.
    pub fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        self.get_domain_with_timeout(name, id, GO_DOMAIN_PERSISTENCE_TIMEOUT)
    }

    /// Read a cache-miss domain using the caller's remaining persistence budget.
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
            (Some(name), None) => self.read_one(
                "name",
                mysql_async::Value::Bytes(name.as_bytes().to_vec()),
                name,
                timeout,
            ),
            (None, Some(id)) => {
                let uuid = uuid::Uuid::parse_str(id)
                    .map_err(|_| MatchingDomainMetadataError::InvalidUuid(id.to_string()))?;
                self.read_one(
                    "id",
                    mysql_async::Value::Bytes(uuid.as_bytes().to_vec()),
                    id,
                    timeout,
                )
            }
        }
    }

    fn read_one(
        &self,
        selector: &str,
        value: mysql_async::Value,
        identity: &str,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        // Selector is only one of the two literals above.
        let selector = selector.to_owned();
        let deadline = Instant::now() + timeout;
        let row: Option<DomainMetadataRow> = self
            .pool
            .query_until(deadline, move |connection| {
                Box::pin(async move { connection.exec_first(
                    format!("SELECT /*+ MAX_EXECUTION_TIME(3000) */ id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? AND {selector} = ?"),
                    (GO_DEFAULT_SHARD_ID, value),
                ).await })
            })
            .map_err(query_error)?;
        let (id, name, is_global, data, data_encoding) =
            row.ok_or_else(|| MatchingDomainMetadataError::NotFound(identity.to_string()))?;
        self.decode_row(id, name, is_global, data, data_encoding)
    }

    /// Page by Go's raw UUID byte order. Go returns the final UUID as a token
    /// whenever a page is full, even if the token leads to an empty next page.
    pub fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        self.list_domains_with_timeout(page_size, next_page_token, GO_DOMAIN_PERSISTENCE_TIMEOUT)
    }

    /// Read one cache page using the caller's remaining refresh-context budget.
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
        let deadline = Instant::now() + timeout;
        let token = next_page_token.map(|token| token.to_vec());
        let rows: Vec<DomainMetadataRow> = self
            .pool
            .query_until(deadline, move |connection| Box::pin(async move { match token {
                Some(token) => connection.exec(
                    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? AND id > ? ORDER BY id LIMIT ?",
                    (GO_DEFAULT_SHARD_ID, token, limit),
                ).await,
                None => connection.exec(
                    "SELECT /*+ MAX_EXECUTION_TIME(3000) */ id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? ORDER BY id LIMIT ?",
                    (GO_DEFAULT_SHARD_ID, limit),
                ).await,
            }}))
            .map_err(query_error)?;
        let next_page_token = if rows.len() >= page_size {
            rows.last().map(|row| row.0.clone())
        } else {
            None
        };
        let domains = rows
            .into_iter()
            .map(|(id, name, is_global, data, encoding)| {
                self.decode_row(id, name, is_global, data, encoding)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MatchingDomainPage {
            domains,
            next_page_token,
        })
    }

    fn decode_row(
        &self,
        id: Vec<u8>,
        name: String,
        is_global: bool,
        data: Vec<u8>,
        encoding: String,
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

impl crate::matching_domain_metadata::GoSqlDomainPersistence for MySqlDomainMetadataStore {
    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<i64, MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::create_domain(self, metadata)
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_notification_version: i64,
    ) -> Result<(), MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::update_domain(self, metadata, expected_notification_version)
    }

    fn delete_domain(&self, domain_id: &str) -> Result<(), MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::delete_domain(self, domain_id)
    }

    fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::get_metadata_version(self)
    }

    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::get_domain(self, name, id)
    }

    fn list_domains(
        &self,
        page_size: usize,
        page_token: Option<&[u8]>,
    ) -> Result<MatchingDomainPage, MatchingDomainMetadataError> {
        MySqlDomainMetadataStore::list_domains(self, page_size, page_token)
    }
}

impl crate::matching_domain_metadata::DomainMetadataPersistence for MySqlDomainMetadataStore {
    fn metadata_version(&self) -> Result<i64, String> {
        self.get_metadata_version().map_err(|error| error.to_string())
    }

    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        _expected_version: i64,
    ) -> Result<(), String> {
        MySqlDomainMetadataStore::create_domain(self, metadata)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
    ) -> Result<(), String> {
        MySqlDomainMetadataStore::update_domain(self, metadata, expected_version)
            .map_err(|error| error.to_string())
    }

    fn delete_domain(&self, id: &str) -> Result<(), String> {
        MySqlDomainMetadataStore::delete_domain(self, id).map_err(|error| error.to_string())
    }
}

fn query_error(error: crate::matching_mysql::TimedQueryError) -> MatchingDomainMetadataError {
    match error {
        crate::matching_mysql::TimedQueryError::Timeout => {
            MatchingDomainMetadataError::OperationTimedOut
        }
        crate::matching_mysql::TimedQueryError::MySql(error) => {
            MatchingDomainMetadataError::MySqlAsync(error)
        }
        crate::matching_mysql::TimedQueryError::Pool(error) => {
            MatchingDomainMetadataError::MySqlPool(error)
        }
    }
}
