//! Go-schema Cassandra domain writes for the Frontend domain manager.

use crate::cassandra::{
    CassandraBlockingSession, CassandraClusterReplicationConfigUdt, CassandraDomainConfigUdt,
    CassandraDomainReplicationConfigUdt, CassandraDomainUdt,
};
use crate::matching_blob::BlobEncoding;
use crate::matching_cassandra_exec::{bound, BoundValues};
use crate::matching_domain_metadata::{DomainMetadataPersistence, MatchingDomainMetadata};
use cadence_thrift::{protocol::Writer, value::write_struct, ToThrift};
use chrono::{DateTime, Utc};
use scylla::statement::{
    batch::{Batch, BatchType},
    Statement,
};
use std::sync::Arc;
use uuid::Uuid;

const DOMAIN_PARTITION: i32 = 0;
const DOMAIN_METADATA_NAME: &str = "cadence-domain-metadata";
const DOMAIN_REPLICATION_QUEUE_TYPE: i32 = 1;
const DOMAIN_REPLICATION_DLQ_TYPE: i32 = -1;
const DOMAIN_QUEUE_METADATA_SELECT: &str =
    "SELECT cluster_ack_level, version FROM queue_metadata WHERE queue_type=?";
const DOMAIN_QUEUE_METADATA_INSERT: &str = "INSERT INTO queue_metadata (queue_type, cluster_ack_level, version, created_time) VALUES (?, ?, ?, ?) IF NOT EXISTS";
const DOMAIN_QUEUE_METADATA_UPDATE: &str = "UPDATE queue_metadata SET cluster_ack_level=?, version=?, last_updated_time=? WHERE queue_type=? IF version=?";
const DOMAIN_QUEUE_LAST_ID: &str =
    "SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1";
const DOMAIN_QUEUE_INSERT: &str = "INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES (?, ?, ?, ?) IF NOT EXISTS";
const DOMAIN_QUEUE_READ: &str =
    "SELECT message_id, message_payload FROM queue WHERE queue_type=? AND message_id>? LIMIT ?";
const DOMAIN_DLQ_READ: &str = "SELECT message_id, message_payload FROM queue WHERE queue_type=? AND message_id>? AND message_id<=? LIMIT ?";
const CREATE_DOMAIN: &str =
    "INSERT INTO domains (id, domain, created_time) VALUES (?, {name: ?}, ?) IF NOT EXISTS";
const CREATE_DOMAIN_V2: &str = "INSERT INTO domains_by_name_v2 (domains_partition, name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const UPDATE_DOMAIN_V2: &str = "UPDATE domains_by_name_v2 SET domain=?, config=?, replication_config=?, config_version=?, failover_version=?, failover_notification_version=?, previous_failover_version=?, failover_end_time=?, last_updated_time=?, notification_version=? WHERE domains_partition=? AND name=?";
const UPDATE_METADATA: &str = "UPDATE domains_by_name_v2 SET notification_version=? WHERE domains_partition=? AND name=? IF notification_version=?";

/// Cassandra domain persistence uses Go's `domains` and `domains_by_name_v2`
/// tables and the same conditional metadata-version fence as Go's CDB.
pub struct CassandraDomainMetadataPersistence {
    driver: Arc<CassandraBlockingSession>,
    serialization_encoding: Arc<dyn Fn() -> String + Send + Sync>,
}

impl std::fmt::Debug for CassandraDomainMetadataPersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraDomainMetadataPersistence")
            .finish_non_exhaustive()
    }
}

impl CassandraDomainMetadataPersistence {
    pub fn new(
        driver: Arc<CassandraBlockingSession>,
        serialization_encoding: Arc<dyn Fn() -> String + Send + Sync>,
    ) -> Self {
        Self {
            driver,
            serialization_encoding,
        }
    }

    /// Append a ThriftRW domain task to Go's shared Cassandra queue schema.
    /// Cassandra uses a conditional row insert; concurrent ID collisions are
    /// surfaced as a condition failure, matching Go's current NoSQL queue
    /// behavior.
    pub fn enqueue_domain_replication_message(&self, payload: Vec<u8>) -> Result<i64, String> {
        use std::collections::HashMap;
        let session = self.driver.session();
        let current_time = Utc::now();
        let ack_result = self
            .driver
            .run(session.query_unpaged(
                DOMAIN_QUEUE_METADATA_SELECT,
                (DOMAIN_REPLICATION_QUEUE_TYPE,),
            ))
            .map_err(|error| error.to_string())?;
        let ack_rows = ack_result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        let mut ack_rows = ack_rows
            .rows::<(HashMap<String, i64>, i64)>()
            .map_err(|error| error.to_string())?;
        let ack_levels = if let Some(row) = ack_rows.next() {
            row.map_err(|error| error.to_string())?.0
        } else {
            let empty = HashMap::<String, i64>::new();
            let result = self
                .driver
                .run(session.query_unpaged(
                    DOMAIN_QUEUE_METADATA_INSERT,
                    (DOMAIN_REPLICATION_QUEUE_TYPE, empty, 0_i64, current_time),
                ))
                .map_err(|error| error.to_string())?;
            let rows = result
                .into_rows_result()
                .map_err(|error| error.to_string())?;
            let applied = rows
                .maybe_first_row::<(bool,)>()
                .map_err(|error| error.to_string())?
                .map(|(value,)| value)
                .ok_or_else(|| "Cassandra queue metadata LWT returned no result row".to_string())?;
            let _ = applied; // another writer may have initialized the row; the read below is authoritative.
            let result = self
                .driver
                .run(session.query_unpaged(
                    DOMAIN_QUEUE_METADATA_SELECT,
                    (DOMAIN_REPLICATION_QUEUE_TYPE,),
                ))
                .map_err(|error| error.to_string())?;
            let rows = result
                .into_rows_result()
                .map_err(|error| error.to_string())?;
            rows.maybe_first_row::<(HashMap<String, i64>, i64)>()
                .map_err(|error| error.to_string())?
                .map(|(levels, _)| levels)
                .ok_or_else(|| "Cassandra queue metadata row was not created".to_string())?
        };
        let last_result = self
            .driver
            .run(session.query_unpaged(DOMAIN_QUEUE_LAST_ID, (DOMAIN_REPLICATION_QUEUE_TYPE,)))
            .map_err(|error| error.to_string())?;
        let rows = last_result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        let last_id = rows
            .maybe_first_row::<(i64,)>()
            .map_err(|error| error.to_string())?
            .map(|(id,)| id)
            .unwrap_or(-1);
        let id = crate::matching_domain_mysql::next_domain_replication_message_id(
            last_id,
            ack_levels.values().copied(),
        );
        let result = self
            .driver
            .run(session.query_unpaged(
                DOMAIN_QUEUE_INSERT,
                (DOMAIN_REPLICATION_QUEUE_TYPE, id, payload, current_time),
            ))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        let applied = rows
            .maybe_first_row::<(bool,)>()
            .map_err(|error| error.to_string())?
            .map(|(value,)| value)
            .ok_or_else(|| "Cassandra queue insert LWT returned no result row".to_string())?;
        if applied {
            Ok(id)
        } else {
            Err(format!(
                "domain replication queue message ID {id} already exists"
            ))
        }
    }

    /// Read ordered domain queue rows after the supplied cursor.
    pub fn read_domain_replication_messages(
        &self,
        last_message_id: i64,
        max_count: i32,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationQueueMessage>, String> {
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                DOMAIN_QUEUE_READ,
                (DOMAIN_REPLICATION_QUEUE_TYPE, last_message_id, max_count),
            ))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        rows.rows::<(i64, Vec<u8>)>()
            .map_err(|error| error.to_string())?
            .map(|row| {
                let (id, payload) = row.map_err(|error| error.to_string())?;
                Ok(crate::matching_domain_metadata::DomainReplicationQueueMessage { id, payload })
            })
            .collect()
    }

    pub fn read_domain_replication_dlq(
        &self,
        first_message_id: i64,
        last_message_id: i64,
        max_count: i32,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationDlqMessage>, String> {
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                DOMAIN_DLQ_READ,
                (
                    DOMAIN_REPLICATION_DLQ_TYPE,
                    first_message_id,
                    last_message_id,
                    max_count,
                ),
            ))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        rows.rows::<(i64, Vec<u8>)>()
            .map_err(|error| error.to_string())?
            .map(|row| {
                let (id, payload) = row.map_err(|error| error.to_string())?;
                Ok(crate::matching_domain_metadata::DomainReplicationDlqMessage { id, payload })
            })
            .collect()
    }

    pub fn delete_domain_replication_dlq_range(
        &self,
        first_message_id: i64,
        last_message_id: i64,
    ) -> Result<(), String> {
        let session = self.driver.session();
        self.driver
            .run(session.query_unpaged(
                "DELETE FROM queue WHERE queue_type=? AND message_id>? AND message_id<=?",
                (
                    DOMAIN_REPLICATION_DLQ_TYPE,
                    first_message_id,
                    last_message_id,
                ),
            ))
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn domain_replication_dlq_size(&self) -> Result<i64, String> {
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged(
                "SELECT COUNT(*) FROM queue WHERE queue_type=?",
                (DOMAIN_REPLICATION_DLQ_TYPE,),
            ))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        rows.maybe_first_row::<(i64,)>()
            .map_err(|error| error.to_string())?
            .map(|(size,)| size)
            .ok_or_else(|| "Cassandra domain replication DLQ count returned no row".to_string())
    }

    /// Persist a monotonic consumer ack using the queue metadata version CAS.
    pub fn update_domain_replication_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        self.update_queue_ack_level(DOMAIN_REPLICATION_QUEUE_TYPE, cluster_name, message_id)
    }

    pub fn update_domain_replication_dlq_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        self.update_queue_ack_level(DOMAIN_REPLICATION_DLQ_TYPE, cluster_name, message_id)
    }

    fn update_queue_ack_level(
        &self,
        queue_type: i32,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        use std::collections::HashMap;
        let session = self.driver.session();
        let select = || {
            self.driver
                .run(session.query_unpaged(DOMAIN_QUEUE_METADATA_SELECT, (queue_type,)))
                .map_err(|error| error.to_string())
        };
        let result = select()?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        let current = rows
            .maybe_first_row::<(HashMap<String, i64>, i64)>()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Cassandra domain replication queue metadata is missing".to_string())?;
        let (mut levels, version) = current;
        if levels
            .get(&cluster_name)
            .is_some_and(|old| *old >= message_id)
        {
            return Ok(());
        }
        levels.insert(cluster_name, message_id);
        let result = self
            .driver
            .run(session.query_unpaged(
                DOMAIN_QUEUE_METADATA_UPDATE,
                (levels, version + 1, Utc::now(), queue_type, version),
            ))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        let applied = rows
            .maybe_first_row::<(bool,)>()
            .map_err(|error| error.to_string())?
            .map(|(value,)| value)
            .ok_or_else(|| "Cassandra queue ack LWT returned no result row".to_string())?;
        if applied {
            Ok(())
        } else {
            Err("concurrent domain replication queue ack update".into())
        }
    }

    pub fn get_domain_replication_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        self.get_queue_ack_levels(DOMAIN_REPLICATION_QUEUE_TYPE)
    }

    pub fn get_domain_replication_dlq_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        self.get_queue_ack_levels(DOMAIN_REPLICATION_DLQ_TYPE)
    }

    fn get_queue_ack_levels(
        &self,
        queue_type: i32,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        use std::collections::HashMap;
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged(DOMAIN_QUEUE_METADATA_SELECT, (queue_type,)))
            .map_err(|error| error.to_string())?;
        let rows = result
            .into_rows_result()
            .map_err(|error| error.to_string())?;
        Ok(rows
            .maybe_first_row::<(HashMap<String, i64>, i64)>()
            .map_err(|error| error.to_string())?
            .map(|(levels, _)| levels)
            .unwrap_or_default())
    }

    fn serialize<T: ToThrift + ?Sized>(
        value: &T,
        encoding: BlobEncoding,
    ) -> Result<Vec<u8>, String> {
        let mut writer = Writer::new();
        write_struct(&mut writer, &value.to_thrift());
        encoding
            .encode(&writer.into_bytes())
            .map_err(|error| error.to_string())
    }

    fn domain_udt(metadata: &MatchingDomainMetadata) -> Result<CassandraDomainUdt, String> {
        Ok(CassandraDomainUdt {
            id: Uuid::parse_str(&metadata.info.uuid).map_err(|error| error.to_string())?,
            name: metadata.info.name.clone(),
            status: metadata.status_raw,
            description: Some(metadata.info.description.clone()),
            data: metadata.info.data.clone(),
            owner_email: Some(metadata.info.owner_email.clone()),
        })
    }

    fn config_udt(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<CassandraDomainConfigUdt, String> {
        if let Some(error) = &metadata.config_decode_error {
            return Err(error.clone());
        }
        let encoding = BlobEncoding::parse(&(self.serialization_encoding)())
            .map_err(|error| error.to_string())?;
        let blob = |value: Option<&dyn ToThrift>, encoding: BlobEncoding| {
            value
                .map(|value| Self::serialize(value, encoding))
                .transpose()
        };
        let bad_binaries = metadata
            .decoded_bad_binaries
            .as_ref()
            .map(|value| value as &dyn ToThrift);
        let isolation_groups = metadata
            .decoded_isolation_groups
            .as_ref()
            .map(|value| value as &dyn ToThrift);
        let async_config = metadata
            .decoded_async_workflow_config
            .as_ref()
            .map(|value| value as &dyn ToThrift);
        Ok(CassandraDomainConfigUdt {
            retention: metadata
                .configuration
                .workflow_execution_retention_period_in_days,
            emit_metric: metadata.configuration.emit_metric,
            archival_bucket: Some(metadata.archival_bucket.clone()),
            archival_status: Some(metadata.archival_status_raw as i32),
            history_archival_status: Some(metadata.history_archival_status_raw as i32),
            history_archival_uri: Some(metadata.configuration.history_archival_uri.clone()),
            visibility_archival_status: Some(metadata.visibility_archival_status_raw as i32),
            visibility_archival_uri: Some(metadata.configuration.visibility_archival_uri.clone()),
            bad_binaries: blob(bad_binaries, encoding)?,
            bad_binaries_encoding: metadata
                .decoded_bad_binaries
                .as_ref()
                .map(|_| encoding.as_str().as_bytes().to_vec()),
            isolation_groups: blob(isolation_groups, encoding)?,
            isolation_groups_encoding: metadata
                .decoded_isolation_groups
                .as_ref()
                .map(|_| encoding.as_str().to_owned()),
            async_workflow_config: blob(async_config, encoding)?,
            async_workflow_config_encoding: metadata
                .decoded_async_workflow_config
                .as_ref()
                .map(|_| encoding.as_str().to_owned()),
        })
    }

    fn replication_udt(
        metadata: &MatchingDomainMetadata,
    ) -> Result<CassandraDomainReplicationConfigUdt, String> {
        let active = metadata.replication.active_clusters.as_ref();
        let encoding = BlobEncoding::ThriftRWSnappy;
        Ok(CassandraDomainReplicationConfigUdt {
            active_cluster_name: Some(metadata.replication.active_cluster_name.clone()),
            clusters: Some(
                metadata
                    .replication
                    .clusters
                    .iter()
                    .map(|cluster| CassandraClusterReplicationConfigUdt {
                        cluster_name: Some(cluster.cluster_name.clone()),
                    })
                    .collect(),
            ),
            active_clusters_config: active
                .map(|value| Self::serialize(value, encoding))
                .transpose()?,
            active_clusters_config_encoding: active.map(|_| encoding.as_str().to_owned()),
        })
    }

    fn bound_domain(metadata: &MatchingDomainMetadata) -> Result<CassandraDomainUdt, String> {
        Self::domain_udt(metadata)
    }

    fn mutation_values(
        &self,
        metadata: &MatchingDomainMetadata,
    ) -> Result<
        (
            CassandraDomainUdt,
            CassandraDomainConfigUdt,
            CassandraDomainReplicationConfigUdt,
        ),
        String,
    > {
        Ok((
            Self::domain_udt(metadata)?,
            self.config_udt(metadata)?,
            Self::replication_udt(metadata)?,
        ))
    }

    fn lwt(&self, query: &str, values: BoundValues) -> Result<bool, String> {
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged(query, &values))
            .map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        rows.maybe_first_row::<(bool,)>()
            .map_err(|e| e.to_string())?
            .map(|(applied,)| applied)
            .ok_or_else(|| "Cassandra LWT returned no result row".to_string())
    }

    fn batch_lwt(&self, queries: &[&str], values: Vec<BoundValues>) -> Result<bool, String> {
        let mut batch = Batch::new(BatchType::Logged);
        for query in queries {
            batch.append_statement(Statement::new(*query));
        }
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.batch(&batch, values))
            .map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        rows.maybe_first_row::<(bool,)>()
            .map_err(|e| e.to_string())?
            .map(|(applied,)| applied)
            .ok_or_else(|| "Cassandra conditional batch returned no result row".to_string())
    }

    fn update_values(
        &self,
        metadata: &MatchingDomainMetadata,
        expected: i64,
    ) -> Result<BoundValues, String> {
        let (domain, config, replication) = self.mutation_values(metadata)?;
        Ok(vec![
            bound(domain),
            bound(config),
            bound(replication),
            bound(metadata.config_version),
            bound(metadata.failover_version),
            bound(metadata.failover_notification_version),
            bound(metadata.previous_failover_version),
            bound(metadata.failover_end_time_nanos.unwrap_or(0)),
            bound(metadata.last_updated_time_nanos),
            bound(expected),
            bound(DOMAIN_PARTITION),
            bound(metadata.info.name.clone()),
        ])
    }

    fn metadata_cas_values(expected: i64) -> BoundValues {
        let current = (expected > 0).then_some(expected);
        let next = if expected > 0 { expected + 1 } else { 1 };
        vec![
            bound(next),
            bound(DOMAIN_PARTITION),
            bound(DOMAIN_METADATA_NAME),
            bound(current),
        ]
    }
}

impl DomainMetadataPersistence for CassandraDomainMetadataPersistence {
    fn metadata_version(&self) -> Result<i64, String> {
        let session = self.driver.session();
        let result = self.driver.run(session.query_unpaged(
            "SELECT notification_version FROM domains_by_name_v2 WHERE domains_partition=? AND name=?",
            (DOMAIN_PARTITION, DOMAIN_METADATA_NAME),
        )).map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        Ok(rows
            .maybe_first_row::<(i64,)>()
            .map_err(|e| e.to_string())?
            .map(|(version,)| version)
            .unwrap_or(0))
    }

    fn create_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        _expected_version: i64,
    ) -> Result<(), String> {
        let domain = Self::bound_domain(metadata)?;
        let id = domain.id;
        if !self.lwt(
            CREATE_DOMAIN,
            vec![bound(id), bound(domain.name.clone()), bound(Utc::now())],
        )? {
            return Err(format!("domain {} already exists", metadata.info.name));
        }
        // Go inserts the legacy UUID row before reading the current singleton
        // version and opening its conditional batch.
        let expected_version = self.metadata_version()?;
        let (domain, config, replication) = self.mutation_values(metadata)?;
        let created = DateTime::<Utc>::from_timestamp_nanos(metadata.last_updated_time_nanos);
        let create_values = vec![
            bound(DOMAIN_PARTITION),
            bound(metadata.info.name.clone()),
            bound(domain),
            bound(config),
            bound(replication),
            bound(metadata.is_global_domain),
            bound(metadata.config_version),
            bound(metadata.failover_version),
            bound(metadata.failover_notification_version),
            bound(metadata.previous_failover_version),
            bound(metadata.failover_end_time_nanos.unwrap_or(0)),
            bound(metadata.last_updated_time_nanos),
            bound(expected_version),
            bound(created),
        ];
        if self.batch_lwt(
            &[CREATE_DOMAIN_V2, UPDATE_METADATA],
            vec![create_values, Self::metadata_cas_values(expected_version)],
        )? {
            Ok(())
        } else {
            let _ = self.delete_domain(&id.to_string());
            Err("domain metadata notification version changed during create".to_string())
        }
    }

    fn update_domain(
        &self,
        metadata: &MatchingDomainMetadata,
        expected_version: i64,
    ) -> Result<(), String> {
        let update = self.update_values(metadata, expected_version)?;
        if self.batch_lwt(
            &[UPDATE_DOMAIN_V2, UPDATE_METADATA],
            vec![update, Self::metadata_cas_values(expected_version)],
        )? {
            Ok(())
        } else {
            Err("domain metadata notification version changed during update".to_string())
        }
    }

    fn delete_domain(&self, id: &str) -> Result<(), String> {
        let id = Uuid::parse_str(id).map_err(|e| e.to_string())?;
        let session = self.driver.session();
        let result = self
            .driver
            .run(session.query_unpaged("SELECT domain FROM domains WHERE id=?", (id,)))
            .map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        let Some((domain,)) = rows
            .maybe_first_row::<(CassandraDomainUdt,)>()
            .map_err(|e| e.to_string())?
        else {
            return Ok(());
        };
        self.driver
            .run(session.query_unpaged(
                "DELETE FROM domains_by_name_v2 WHERE domains_partition=? AND name=?",
                (DOMAIN_PARTITION, domain.name.clone()),
            ))
            .map_err(|e| e.to_string())?;
        self.driver
            .run(session.query_unpaged("DELETE FROM domains WHERE id=?", (id,)))
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn enqueue_domain_replication_message(&self, payload: Vec<u8>) -> Result<i64, String> {
        CassandraDomainMetadataPersistence::enqueue_domain_replication_message(self, payload)
    }

    fn read_domain_replication_messages(
        &self,
        last_message_id: i64,
        max_count: usize,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationQueueMessage>, String> {
        let max_count = i32::try_from(max_count).map_err(|error| error.to_string())?;
        CassandraDomainMetadataPersistence::read_domain_replication_messages(
            self,
            last_message_id,
            max_count,
        )
    }

    fn read_domain_replication_dlq(
        &self,
        first_message_id: i64,
        last_message_id: i64,
        max_count: usize,
    ) -> Result<Vec<crate::matching_domain_metadata::DomainReplicationDlqMessage>, String> {
        let max_count = i32::try_from(max_count).map_err(|error| error.to_string())?;
        CassandraDomainMetadataPersistence::read_domain_replication_dlq(
            self,
            first_message_id,
            last_message_id,
            max_count,
        )
    }

    fn delete_domain_replication_dlq_range(
        &self,
        first_message_id: i64,
        last_message_id: i64,
    ) -> Result<(), String> {
        CassandraDomainMetadataPersistence::delete_domain_replication_dlq_range(
            self,
            first_message_id,
            last_message_id,
        )
    }

    fn domain_replication_dlq_size(&self) -> Result<i64, String> {
        CassandraDomainMetadataPersistence::domain_replication_dlq_size(self)
    }

    fn get_domain_replication_dlq_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        CassandraDomainMetadataPersistence::get_domain_replication_dlq_ack_levels(self)
    }

    fn update_domain_replication_dlq_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        CassandraDomainMetadataPersistence::update_domain_replication_dlq_ack_level(
            self,
            cluster_name,
            message_id,
        )
    }

    fn get_domain_replication_ack_levels(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, String> {
        CassandraDomainMetadataPersistence::get_domain_replication_ack_levels(self)
    }

    fn update_domain_replication_ack_level(
        &self,
        cluster_name: String,
        message_id: i64,
    ) -> Result<(), String> {
        CassandraDomainMetadataPersistence::update_domain_replication_ack_level(
            self,
            cluster_name,
            message_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};

    #[test]
    fn cql_statements_keep_go_table_names_and_metadata_cas() {
        assert!(CREATE_DOMAIN.contains("INSERT INTO domains "));
        assert!(CREATE_DOMAIN_V2.contains("IF NOT EXISTS"));
        assert!(UPDATE_METADATA.contains("IF notification_version=?"));
        let _fixture = (
            DomainInfo::default(),
            DomainConfiguration::default(),
            DomainReplicationConfiguration::default(),
        );
    }
}
