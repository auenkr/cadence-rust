//! Read-only access to Go's MySQL domain metadata for the Matching cache.
//!
//! This uses the same MySQL pool/options as the task-store adapter and reads
//! the Go default domain shard (54321). It never writes or migrates schema.

use crate::matching_domain_metadata::{
    decode_go_sql_domain_metadata, MatchingDomainMetadata, MatchingDomainMetadataError,
    MatchingDomainPage,
};
use crate::matching_mysql::{MySqlTaskStore, MySqlTaskStoreConfig};
use mysql_async::prelude::Queryable as AsyncQueryable;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const GO_DEFAULT_SHARD_ID: i32 = 54321;
const GO_DOMAIN_PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(3);
type DomainMetadataRow = (Vec<u8>, String, bool, Vec<u8>, String);

pub struct MySqlDomainMetadataStore {
    pool: Arc<crate::matching_mysql::SyncConnectionPool>,
    active_cluster_name: String,
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
