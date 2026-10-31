//! Go-schema Cassandra domain writes for the Frontend domain manager.

use crate::cassandra::{
    CassandraBlockingSession, CassandraClusterReplicationConfigUdt, CassandraDomainConfigUdt,
    CassandraDomainReplicationConfigUdt, CassandraDomainUdt,
};
use crate::matching_cassandra_exec::{bound, BoundValues};
use crate::matching_domain_metadata::{
    DomainMetadataPersistence, MatchingDomainMetadata,
};
use chrono::{DateTime, Utc};
use scylla::statement::batch::{Batch, BatchType};
use std::sync::Arc;
use uuid::Uuid;

const DOMAIN_PARTITION: i32 = 0;
const DOMAIN_METADATA_NAME: &str = "cadence-domain-metadata";
const CREATE_DOMAIN: &str = "INSERT INTO domains (id, domain, created_time) VALUES (?, {name: ?}, ?) IF NOT EXISTS";
const CREATE_DOMAIN_V2: &str = "INSERT INTO domains_by_name_v2 (domains_partition, name, domain, config, replication_config, is_global_domain, config_version, failover_version, failover_notification_version, previous_failover_version, failover_end_time, last_updated_time, notification_version, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const UPDATE_DOMAIN_V2: &str = "UPDATE domains_by_name_v2 SET domain=?, config=?, replication_config=?, config_version=?, failover_version=?, failover_notification_version=?, previous_failover_version=?, failover_end_time=?, last_updated_time=?, notification_version=? WHERE domains_partition=? AND name=?";
const UPDATE_METADATA: &str = "UPDATE domains_by_name_v2 SET notification_version=? WHERE domains_partition=? AND name=? IF notification_version=?";

/// Cassandra domain persistence uses Go's `domains` and `domains_by_name_v2`
/// tables and the same conditional metadata-version fence as Go's CDB.
pub struct CassandraDomainMetadataPersistence {
    driver: Arc<CassandraBlockingSession>,
}

impl std::fmt::Debug for CassandraDomainMetadataPersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraDomainMetadataPersistence").finish_non_exhaustive()
    }
}

impl CassandraDomainMetadataPersistence {
    pub fn new(driver: Arc<CassandraBlockingSession>) -> Self {
        Self { driver }
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

    fn config_udt(metadata: &MatchingDomainMetadata) -> CassandraDomainConfigUdt {
        let blob = |value: &Option<crate::matching_domain_metadata::GoDomainBlob>| {
            value.as_ref().map(|value| value.data.clone())
        };
        let encoding = |value: &Option<crate::matching_domain_metadata::GoDomainBlob>| {
            value.as_ref().map(|value| value.encoding.clone())
        };
        CassandraDomainConfigUdt {
            retention: metadata.configuration.workflow_execution_retention_period_in_days,
            emit_metric: metadata.configuration.emit_metric,
            archival_bucket: Some(metadata.archival_bucket.clone()),
            archival_status: Some(metadata.archival_status_raw as i32),
            history_archival_status: Some(metadata.history_archival_status_raw as i32),
            history_archival_uri: Some(metadata.configuration.history_archival_uri.clone()),
            visibility_archival_status: Some(metadata.visibility_archival_status_raw as i32),
            visibility_archival_uri: Some(metadata.configuration.visibility_archival_uri.clone()),
            bad_binaries: blob(&metadata.bad_binaries),
            bad_binaries_encoding: encoding(&metadata.bad_binaries).map(String::into_bytes),
            isolation_groups: blob(&metadata.isolation_groups),
            isolation_groups_encoding: encoding(&metadata.isolation_groups),
            async_workflow_config: blob(&metadata.async_workflow_config),
            async_workflow_config_encoding: encoding(&metadata.async_workflow_config),
        }
    }

    fn replication_udt(metadata: &MatchingDomainMetadata) -> CassandraDomainReplicationConfigUdt {
        let active = metadata.active_clusters_config.as_ref();
        CassandraDomainReplicationConfigUdt {
            active_cluster_name: Some(metadata.replication.active_cluster_name.clone()),
            clusters: Some(metadata.replication.clusters.iter().map(|cluster| {
                CassandraClusterReplicationConfigUdt { cluster_name: Some(cluster.cluster_name.clone()) }
            }).collect()),
            active_clusters_config: active.map(|value| value.data.clone()),
            active_clusters_config_encoding: active.map(|value| value.encoding.clone()),
        }
    }

    fn bound_domain(metadata: &MatchingDomainMetadata) -> Result<CassandraDomainUdt, String> {
        Self::domain_udt(metadata)
    }

    fn mutation_values(metadata: &MatchingDomainMetadata) -> Result<(CassandraDomainUdt, CassandraDomainConfigUdt, CassandraDomainReplicationConfigUdt), String> {
        Ok((Self::domain_udt(metadata)?, Self::config_udt(metadata), Self::replication_udt(metadata)))
    }

    fn lwt(&self, query: &str, values: BoundValues) -> Result<bool, String> {
        let session = self.driver.session();
        let result = self.driver.run(session.query_unpaged(query, &values)).map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        rows.maybe_first_row::<(bool,)>().map_err(|e| e.to_string())?
            .map(|(applied,)| applied).ok_or_else(|| "Cassandra LWT returned no result row".to_string())
    }

    fn batch_lwt(&self, queries: &[&str], values: Vec<BoundValues>) -> Result<bool, String> {
        let mut batch = Batch::new(BatchType::Logged);
        for query in queries { batch.append_statement(*query); }
        let session = self.driver.session();
        let result = self.driver.run(session.batch(&batch, values)).map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        rows.maybe_first_row::<(bool,)>().map_err(|e| e.to_string())?
            .map(|(applied,)| applied).ok_or_else(|| "Cassandra conditional batch returned no result row".to_string())
    }

    fn update_values(metadata: &MatchingDomainMetadata, expected: i64) -> Result<BoundValues, String> {
        let (domain, config, replication) = Self::mutation_values(metadata)?;
        Ok(vec![
            bound(domain), bound(config), bound(replication), bound(metadata.config_version),
            bound(metadata.failover_version), bound(metadata.failover_notification_version),
            bound(metadata.previous_failover_version), bound(metadata.failover_end_time_nanos.unwrap_or(0)),
            bound(metadata.last_updated_time_nanos), bound(expected), bound(DOMAIN_PARTITION),
            bound(metadata.info.name.clone()),
        ])
    }

    fn metadata_cas_values(expected: i64) -> BoundValues {
        let current = (expected > 0).then_some(expected);
        let next = if expected > 0 { expected + 1 } else { 1 };
        vec![bound(next), bound(DOMAIN_PARTITION), bound(DOMAIN_METADATA_NAME), bound(current)]
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
        Ok(rows.maybe_first_row::<(i64,)>().map_err(|e| e.to_string())?
            .map(|(version,)| version).unwrap_or(0))
    }

    fn create_domain(&self, metadata: &MatchingDomainMetadata, _expected_version: i64) -> Result<(), String> {
        let domain = Self::bound_domain(metadata)?;
        let id = domain.id;
        if !self.lwt(CREATE_DOMAIN, vec![bound(id), bound(domain.name.clone()), bound(Utc::now())])? {
            return Err(format!("domain {} already exists", metadata.info.name));
        }
        // Go inserts the legacy UUID row before reading the current singleton
        // version and opening its conditional batch.
        let expected_version = self.metadata_version()?;
        let (domain, config, replication) = Self::mutation_values(metadata)?;
        let created = DateTime::<Utc>::from_timestamp_nanos(metadata.last_updated_time_nanos);
        let create_values = vec![
            bound(DOMAIN_PARTITION), bound(metadata.info.name.clone()), bound(domain), bound(config),
            bound(replication), bound(metadata.is_global_domain), bound(metadata.config_version),
            bound(metadata.failover_version), bound(metadata.failover_notification_version),
            bound(metadata.previous_failover_version), bound(metadata.failover_end_time_nanos.unwrap_or(0)),
            bound(metadata.last_updated_time_nanos), bound(expected_version), bound(created),
        ];
        if self.batch_lwt(&[CREATE_DOMAIN_V2, UPDATE_METADATA], vec![create_values, Self::metadata_cas_values(expected_version)])? {
            Ok(())
        } else {
            let _ = self.delete_domain(&id.to_string());
            Err("domain metadata notification version changed during create".to_string())
        }
    }

    fn update_domain(&self, metadata: &MatchingDomainMetadata, expected_version: i64) -> Result<(), String> {
        let update = Self::update_values(metadata, expected_version)?;
        if self.batch_lwt(&[UPDATE_DOMAIN_V2, UPDATE_METADATA], vec![update, Self::metadata_cas_values(expected_version)])? {
            Ok(())
        } else {
            Err("domain metadata notification version changed during update".to_string())
        }
    }

    fn delete_domain(&self, id: &str) -> Result<(), String> {
        let id = Uuid::parse_str(id).map_err(|e| e.to_string())?;
        let session = self.driver.session();
        let result = self.driver.run(session.query_unpaged("SELECT domain FROM domains WHERE id=?", (id,))).map_err(|e| e.to_string())?;
        let rows = result.into_rows_result().map_err(|e| e.to_string())?;
        let Some((domain,)) = rows.maybe_first_row::<(CassandraDomainUdt,)>().map_err(|e| e.to_string())? else { return Ok(()) };
        self.driver.run(session.query_unpaged("DELETE FROM domains_by_name_v2 WHERE domains_partition=? AND name=?", (DOMAIN_PARTITION, domain.name.clone()))).map_err(|e| e.to_string())?;
        self.driver.run(session.query_unpaged("DELETE FROM domains WHERE id=?", (id,))).map_err(|e| e.to_string())?;
        Ok(())
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
        let _fixture = (DomainInfo::default(), DomainConfiguration::default(), DomainReplicationConfiguration::default());
    }
}
