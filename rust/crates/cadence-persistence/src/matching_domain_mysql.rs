//! Read-only access to Go's MySQL domain metadata for the Matching cache.
//!
//! This uses the same MySQL pool/options as the task-store adapter and reads
//! the Go default domain shard (54321). It never writes or migrates schema.

use crate::matching_domain_metadata::{
    decode_go_sql_domain_metadata, MatchingDomainMetadata, MatchingDomainMetadataError,
    MatchingDomainPage,
};
use crate::matching_mysql::{MySqlTaskStore, MySqlTaskStoreConfig};
use mysql::prelude::Queryable;
use std::sync::Arc;

const GO_DEFAULT_SHARD_ID: i32 = 54321;

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

    /// Read the cluster-wide notification version from Go's single metadata row.
    pub fn get_metadata_version(&self) -> Result<i64, MatchingDomainMetadataError> {
        let mut connection = self.pool.checkout().map_err(pool_error)?;
        connection
            .exec_first("SELECT notification_version FROM domain_metadata", ())?
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
        match (
            name.filter(|value| !value.is_empty()),
            id.filter(|value| !value.is_empty()),
        ) {
            (Some(_), Some(_)) => Err(MatchingDomainMetadataError::BothIdAndName),
            (None, None) => Err(MatchingDomainMetadataError::EmptyIdentity),
            (Some(name), None) => {
                self.read_one("name", mysql::Value::Bytes(name.as_bytes().to_vec()), name)
            }
            (None, Some(id)) => {
                let uuid = uuid::Uuid::parse_str(id)
                    .map_err(|_| MatchingDomainMetadataError::InvalidUuid(id.to_string()))?;
                self.read_one("id", mysql::Value::Bytes(uuid.as_bytes().to_vec()), id)
            }
        }
    }

    fn read_one(
        &self,
        selector: &str,
        value: mysql::Value,
        identity: &str,
    ) -> Result<MatchingDomainMetadata, MatchingDomainMetadataError> {
        // Selector is only one of the two literals above.
        let query = format!(
            "SELECT id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? AND {selector} = ?"
        );
        let mut connection = self.pool.checkout().map_err(pool_error)?;
        let row: Option<(Vec<u8>, String, bool, Vec<u8>, String)> =
            connection.exec_first(query, (GO_DEFAULT_SHARD_ID, value))?;
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
        if page_size == 0 {
            return Err(MatchingDomainMetadataError::InvalidPageSize);
        }
        let limit =
            i64::try_from(page_size).map_err(|_| MatchingDomainMetadataError::InvalidPageSize)?;
        let mut connection = self.pool.checkout().map_err(pool_error)?;
        let rows: Vec<(Vec<u8>, String, bool, Vec<u8>, String)> = match next_page_token {
            Some(token) => connection.exec(
                "SELECT id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? AND id > ? ORDER BY id LIMIT ?",
                (GO_DEFAULT_SHARD_ID, token, limit),
            )?,
            None => connection.exec(
                "SELECT id, name, is_global, data, data_encoding FROM domains WHERE shard_id = ? ORDER BY id LIMIT ?",
                (GO_DEFAULT_SHARD_ID, limit),
            )?,
        };
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

fn pool_error(error: crate::matching_task_store::StorageError) -> MatchingDomainMetadataError {
    MatchingDomainMetadataError::MySqlPool(error.to_string())
}
