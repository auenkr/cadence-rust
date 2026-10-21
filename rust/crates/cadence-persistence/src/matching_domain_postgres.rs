//! Read-only access to Go's PostgreSQL domain metadata for Matching.
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
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);
type DomainRow = (Vec<u8>, String, bool, Vec<u8>, String);

pub struct PostgresDomainMetadataStore {
    pool: Arc<PostgresConnectionPool>,
    active_cluster_name: String,
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
