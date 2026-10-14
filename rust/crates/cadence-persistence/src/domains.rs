//! In-memory domain store mirroring `go/common/persistence/domain_manager.go`
//! semantics: create-once names and UUIDs, get by name or UUID, update,
//! deprecate, list.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cadence_api::enums::DomainStatus;
use cadence_api::errors::{DomainAlreadyExistsError, EntityNotExistsError};
use cadence_api::service_types::{
    DeprecateDomainRequest, DescribeDomainRequest, DescribeDomainResponse, ListDomainsResponse,
    UpdateDomainRequest,
};
use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DomainRecord {
    pub info: DomainInfo,
    pub configuration: DomainConfiguration,
    pub replication: DomainReplicationConfiguration,
    pub failover_version: i64,
    pub is_global_domain: bool,
}

impl DomainRecord {
    fn to_describe_response(&self) -> DescribeDomainResponse {
        DescribeDomainResponse {
            domain_info: Some(self.info.clone()),
            configuration: Some(self.configuration.clone()),
            replication_configuration: Some(self.replication.clone()),
            failover_version: self.failover_version,
            is_global_domain: self.is_global_domain,
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    by_name: HashMap<String, DomainRecord>,
    by_id: HashMap<String, String>, // uuid -> name
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryDomainStore {
    inner: Arc<RwLock<Inner>>,
}

impl InMemoryDomainStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mirrors Go `CreateDomain`: duplicate name or ID is an error.
    pub fn create(
        &self,
        info: DomainInfo,
        configuration: DomainConfiguration,
        replication: DomainReplicationConfiguration,
        is_global_domain: bool,
    ) -> Result<DescribeDomainResponse, DomainAlreadyExistsError> {
        let mut inner = self.inner.write().expect("domain store lock");
        if inner.by_name.contains_key(&info.name) || inner.by_id.contains_key(&info.uuid) {
            return Err(DomainAlreadyExistsError {
                message: format!("Domain {} already exists.", info.name),
            });
        }
        let record = DomainRecord {
            info: info.clone(),
            configuration,
            replication,
            failover_version: 0,
            is_global_domain,
        };
        let resp = record.to_describe_response();
        inner.by_id.insert(info.uuid.clone(), info.name.clone());
        inner.by_name.insert(info.name, record);
        Ok(resp)
    }

    /// Mirrors Go `GetDomain`: lookup by name or UUID.
    pub fn describe(
        &self,
        req: &DescribeDomainRequest,
    ) -> Result<DescribeDomainResponse, EntityNotExistsError> {
        let inner = self.inner.read().expect("domain store lock");
        if let Some(name) = req.name.as_deref() {
            return inner
                .by_name
                .get(name)
                .map(DomainRecord::to_describe_response)
                .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")));
        }
        if let Some(uuid) = req.uuid.as_deref() {
            let name = inner.by_id.get(uuid).ok_or_else(|| EntityNotExistsError::new(format!("Domain {uuid} does not exist.")))?;
            return inner
                .by_name
                .get(name)
                .map(DomainRecord::to_describe_response)
                .ok_or_else(|| EntityNotExistsError::new(format!("Domain {uuid} does not exist.")));
        }
        Err(EntityNotExistsError::new("Domain not set on request.".to_string()))
    }

    pub fn domain_id(&self, name: &str) -> Result<String, EntityNotExistsError> {
        let inner = self.inner.read().expect("domain store lock");
        inner
            .by_name
            .get(name)
            .map(|r| r.info.uuid.clone())
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))
    }

    /// Mirrors Go `UpdateDomain`: only set fields change. The Thrift
    /// `DomainConfiguration` has scalar retention/emitMetric (no presence),
    /// so nonzero/changed values apply; nested optionals apply when set.
    pub fn update(
        &self,
        req: &UpdateDomainRequest,
    ) -> Result<cadence_api::service_types::UpdateDomainResponse, EntityNotExistsError> {
        let mut inner = self.inner.write().expect("domain store lock");
        let record = inner
            .by_name
            .get_mut(&req.name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {} does not exist.", req.name)))?;
        if let Some(info) = req.updated_info.clone() {
            if let Some(v) = info.description {
                record.info.description = v;
            }
            if let Some(v) = info.owner_email {
                record.info.owner_email = v;
            }
            if let Some(v) = info.data {
                record.info.data = Some(v);
            }
        }
        if let Some(cfg) = req.configuration.clone() {
            // NOTE: Thrift scalars carry no presence; retention applies when
            // nonzero, while emitMetric (no detectable presence) is left
            // untouched to avoid clobbering it with a default false.
            if cfg.workflow_execution_retention_period_in_days != 0 {
                record.configuration.workflow_execution_retention_period_in_days =
                    cfg.workflow_execution_retention_period_in_days;
            }
            if let Some(v) = cfg.bad_binaries {
                record.configuration.bad_binaries = Some(v);
            }
            if let Some(v) = cfg.history_archival_status {
                record.configuration.history_archival_status = Some(v);
            }
            if !cfg.history_archival_uri.is_empty() {
                record.configuration.history_archival_uri = cfg.history_archival_uri;
            }
            if let Some(v) = cfg.visibility_archival_status {
                record.configuration.visibility_archival_status = Some(v);
            }
            if !cfg.visibility_archival_uri.is_empty() {
                record.configuration.visibility_archival_uri = cfg.visibility_archival_uri;
            }
        }
        if let Some(rep) = req.replication_configuration.clone() {
            if !rep.active_cluster_name.is_empty() {
                record.replication.active_cluster_name = rep.active_cluster_name;
            }
            if !rep.clusters.is_empty() {
                record.replication.clusters = rep.clusters;
            }
            if let Some(v) = rep.active_clusters {
                record.replication.active_clusters = Some(v);
            }
        }
        if let Some(checksum) = req.delete_bad_binary.clone() {
            if let Some(binaries) = record.configuration.bad_binaries.as_mut() {
                if let Some(map) = binaries.binaries.as_mut() {
                    map.remove(&checksum);
                }
            }
        }
        Ok(cadence_api::service_types::UpdateDomainResponse {
            domain_info: Some(record.info.clone()),
            configuration: Some(record.configuration.clone()),
            replication_configuration: Some(record.replication.clone()),
            failover_version: record.failover_version,
            is_global_domain: record.is_global_domain,
        })
    }

    /// Mirrors Go `DeprecateDomain`: flips status to deprecated.
    pub fn deprecate(&self, req: &DeprecateDomainRequest) -> Result<(), EntityNotExistsError> {
        let mut inner = self.inner.write().expect("domain store lock");
        let record = inner
            .by_name
            .get_mut(&req.name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {} does not exist.", req.name)))?;
        record.info.status = Some(DomainStatus::Deprecated);
        Ok(())
    }

    /// Mirrors Go `DeleteDomain`: removes name and UUID mappings.
    pub fn delete(&self, name: &str) -> Result<(), EntityNotExistsError> {
        let mut inner = self.inner.write().expect("domain store lock");
        let record = inner
            .by_name
            .remove(name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?;
        inner.by_id.remove(&record.info.uuid);
        Ok(())
    }

    #[must_use]
    pub fn list(&self) -> ListDomainsResponse {
        let inner = self.inner.read().expect("domain store lock");
        let mut domains: Vec<DescribeDomainResponse> = inner
            .by_name
            .values()
            .map(DomainRecord::to_describe_response)
            .collect();
        domains.sort_by(|a, b| {
            a.domain_info
                .as_ref()
                .map(|i| i.name.clone())
                .cmp(&b.domain_info.as_ref().map(|i| i.name.clone()))
        });
        ListDomainsResponse {
            domains,
            next_page_token: Vec::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().expect("domain store lock").by_name.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Persist the store to a JSON file (atomic rename). Used by the
    /// server's `--data-dir` durability mode.
    pub fn save_to_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let inner = self.inner.read().expect("domain store lock");
        let snapshot: Vec<DomainRecord> = inner.by_name.values().cloned().collect();
        drop(inner);
        let path = path.as_ref();
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, data)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    /// Load a store previously written by [`Self::save_to_file`].
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let data = std::fs::read(path.as_ref())?;
        let snapshot: Vec<DomainRecord> = serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let store = Self::new();
        {
            let mut inner = store.inner.write().expect("domain store lock");
            for record in snapshot {
                inner.by_id.insert(record.info.uuid.clone(), record.info.name.clone());
                inner.by_name.insert(record.info.name.clone(), record);
            }
        }
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums::ArchivalStatus;
    use cadence_api::types::ClusterReplicationConfiguration;

    pub(super) fn sample(
        domain: &str,
        uuid: &str,
    ) -> (
        DomainInfo,
        DomainConfiguration,
        DomainReplicationConfiguration,
    ) {
        (
            DomainInfo {
                name: domain.to_string(),
                status: Some(DomainStatus::Registered),
                uuid: uuid.to_string(),
                ..Default::default()
            },
            DomainConfiguration {
                workflow_execution_retention_period_in_days: 7,
                emit_metric: true,
                ..Default::default()
            },
            DomainReplicationConfiguration {
                active_cluster_name: "active".to_string(),
                clusters: vec![ClusterReplicationConfiguration {
                    cluster_name: "active".to_string(),
                }],
                ..Default::default()
            },
        )
    }

    #[test]
    fn create_describe_round_trip() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d1", "uuid-1");
        store.create(info, cfg, rep, false).unwrap();
        let got = store
            .describe(&DescribeDomainRequest {
                name: Some("d1".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(got.domain_info.unwrap().uuid, "uuid-1");
        let by_id = store
            .describe(&DescribeDomainRequest {
                name: None,
                uuid: Some("uuid-1".into()),
            })
            .unwrap();
        assert_eq!(by_id.failover_version, 0);
    }

    #[test]
    fn duplicate_create_fails_like_go() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d1", "uuid-1");
        store
            .create(info.clone(), cfg.clone(), rep.clone(), false)
            .unwrap();
        assert!(store.create(info, cfg, rep, false).is_err());
        assert!(store
            .describe(&DescribeDomainRequest {
                name: Some("missing".into()),
                uuid: None
            })
            .is_err());
    }

    #[test]
    fn update_only_touches_set_fields() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d1", "uuid-1");
        store.create(info, cfg, rep, false).unwrap();
        let updated = store
            .update(&UpdateDomainRequest {
                name: "d1".into(),
                updated_info: Some(cadence_api::service_types::UpdateDomainInfo {
                    description: Some("new".into()),
                    ..Default::default()
                }),
                configuration: Some(DomainConfiguration {
                    history_archival_status: Some(ArchivalStatus::Enabled),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap();
        let info = updated.domain_info.unwrap();
        assert_eq!(info.description, "new");
        assert_eq!(info.name, "d1");
        let cfg = updated.configuration.unwrap();
        assert_eq!(cfg.history_archival_status, Some(ArchivalStatus::Enabled));
        assert_eq!(cfg.workflow_execution_retention_period_in_days, 7);
    }

    #[test]
    fn deprecate_flips_status() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d1", "uuid-1");
        store.create(info, cfg, rep, false).unwrap();
        store
            .deprecate(&DeprecateDomainRequest {
                name: "d1".into(),
                security_token: String::new(),
            })
            .unwrap();
        let got = store
            .describe(&DescribeDomainRequest {
                name: Some("d1".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(
            got.domain_info.unwrap().status,
            Some(DomainStatus::Deprecated)
        );
    }
}

#[cfg(test)]
mod delete_tests {
    use super::*;
    use cadence_api::types::ClusterReplicationConfiguration;

    #[test]
    fn delete_removes_domain() {
        let store = InMemoryDomainStore::new();
        let info = DomainInfo {
            name: "d1".to_string(),
            status: Some(DomainStatus::Registered),
            uuid: "uuid-1".to_string(),
            ..Default::default()
        };
        let cfg = DomainConfiguration::default();
        let rep = DomainReplicationConfiguration {
            active_cluster_name: "active".to_string(),
            clusters: vec![ClusterReplicationConfiguration {
                cluster_name: "active".to_string(),
            }],
            ..Default::default()
        };
        store.create(info, cfg, rep, false).unwrap();
        store.delete("d1").unwrap();
        assert!(store
            .describe(&DescribeDomainRequest {
                name: Some("d1".into()),
                uuid: None,
            })
            .is_err());
        assert!(store.delete("d1").is_err());
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::tests::sample;
    use super::*;

    #[test]
    fn domain_snapshot_round_trip() {
        let path = std::env::temp_dir().join(format!(
            "cadence-rust-test-domains-{}.json",
            std::process::id()
        ));
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d1", "uuid-1");
        store.create(info, cfg, rep, false).unwrap();
        store.save_to_file(&path).unwrap();
        let loaded = InMemoryDomainStore::load_from_file(&path).unwrap();
        std::fs::remove_file(&path).ok();
        let got = loaded
            .describe(&DescribeDomainRequest {
                name: Some("d1".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(got.domain_info.unwrap().uuid, "uuid-1");
    }
}
