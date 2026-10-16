//! In-memory domain store mirroring `go/common/persistence/domain_manager.go`
//! semantics: create-once names and UUIDs, get by name or UUID, update,
//! deprecate, list.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cadence_api::enums::{DomainStatus, FailoverType};
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
    /// Failover audit entries in publication order (Go domain audit logs).
    #[serde(default)]
    pub(crate) failover_audit: Vec<FailoverAuditEntry>,
    #[serde(default)]
    pub config_version: i64,
    #[serde(default)]
    pub last_updated_time_nanos: i64,
    #[serde(default)]
    pub failover_end_time: Option<i64>,
    #[serde(default = "initial_previous_version")]
    pub previous_failover_version: i64,
    /// Domain isolation-group overrides (Go domain isolation state).
    #[serde(default)]
    pub(crate) isolation_groups: Option<cadence_api::types::IsolationGroupConfiguration>,
    /// Domain async-workflow queue configuration overrides.
    #[serde(default)]
    pub(crate) async_workflow_config: Option<cadence_api::types::AsyncWorkflowConfiguration>,
}

fn initial_previous_version() -> i64 { -1 }

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

/// A recorded domain failover, mirroring Go's domain audit log rows for
/// `DomainAuditOperationTypeFailover`. The before/after replication snapshots
/// let `failover_history` compute `FailoverEvent`s exactly like Go's
/// `ToFailoverEvents` (domain-level change plus per-attribute changes).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct FailoverAuditEntry {
    id: String,
    created_nanos: i64,
    failover_type: FailoverType,
    before: DomainReplicationConfiguration,
    before_version: i64,
    after: DomainReplicationConfiguration,
    after_version: i64,
}

#[derive(Debug, Default)]
struct Inner {
    by_name: HashMap<String, DomainRecord>,
    by_id: HashMap<String, String>, // uuid -> name
    /// Cluster-global isolation groups (Go global isolation state).
    /// In-memory only like Go's isolation-state cache; domain records carry
    /// their own overrides into snapshots.
    global_isolation_groups: Option<cadence_api::types::IsolationGroupConfiguration>,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryDomainStore {
    inner: Arc<RwLock<Inner>>,
}

/// Errors for domain failover: unknown domains are `EntityNotExists`,
/// invalid requests are `BadRequest` with Go's exact messages.
#[derive(Debug, thiserror::Error)]
pub enum FailoverError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Busy(String),
}
impl From<crate::domain_update::UpdateError> for FailoverError {
    fn from(error: crate::domain_update::UpdateError) -> Self {
        match error {
            crate::domain_update::UpdateError::Invalid(e) => Self::Invalid(e.message),
            crate::domain_update::UpdateError::Busy(e) => Self::Busy(e.message),
        }
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_nanos() as i64).unwrap_or(0)
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
        self.create_with_version(info, configuration, replication, is_global_domain,
            if is_global_domain { 0 } else { crate::version_history::EMPTY_VERSION })
    }

    pub fn create_with_version(
        &self,
        info: DomainInfo,
        configuration: DomainConfiguration,
        replication: DomainReplicationConfiguration,
        is_global_domain: bool,
        failover_version: i64,
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
            failover_version,
            is_global_domain,
            failover_audit: Vec::new(),
            config_version: 0,
            last_updated_time_nanos: now_nanos(),
            failover_end_time: None,
            previous_failover_version: -1,
            isolation_groups: None,
            async_workflow_config: None,
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
                .ok_or_else(|| {
                    EntityNotExistsError::new(format!("Domain {name} does not exist."))
                });
        }
        if let Some(uuid) = req.uuid.as_deref() {
            let name = inner.by_id.get(uuid).ok_or_else(|| {
                EntityNotExistsError::new(format!("Domain {uuid} does not exist."))
            })?;
            return inner
                .by_name
                .get(name)
                .map(DomainRecord::to_describe_response)
                .ok_or_else(|| {
                    EntityNotExistsError::new(format!("Domain {uuid} does not exist."))
                });
        }
        Err(EntityNotExistsError::new(
            "Domain not set on request.".to_string(),
        ))
    }

    /// Go DomainCacheEntry.IsActiveIn: local domains are always active,
    /// pending failover is passive, and any active-active attribute may own work.
    pub fn is_active_in(&self, uuid: &str, cluster: &str) -> Result<bool, EntityNotExistsError> {
        let inner = self.inner.read().expect("domain store lock");
        let record = inner.by_id.get(uuid).and_then(|name| inner.by_name.get(name))
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {uuid} does not exist.")))?;
        if !record.is_global_domain { return Ok(true); }
        if record.failover_end_time.is_some() { return Ok(false); }
        if record.replication.active_cluster_name == cluster { return Ok(true); }
        Ok(record.replication.active_clusters.as_ref()
            .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
            .is_some_and(|scopes| scopes.values().any(|scope| scope.cluster_attributes.as_ref()
                .is_some_and(|attributes| attributes.values().any(|attribute| attribute.active_cluster_name == cluster)))))
    }

    pub fn domain_id(&self, name: &str) -> Result<String, EntityNotExistsError> {
        let inner = self.inner.read().expect("domain store lock");
        inner
            .by_name
            .get(name)
            .map(|r| r.info.uuid.clone())
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))
    }

    /// Prepare a full candidate under the store lock; publish only after validation.
    pub fn update(
        &self,
        req: &UpdateDomainRequest,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::service_types::UpdateDomainResponse, FailoverError> {
        let mut inner = self.inner.write().expect("domain store lock");
        let current = inner.by_name.get(&req.name).ok_or_else(|| {
            EntityNotExistsError::new(format!("Domain {} does not exist.", req.name))
        })?;
        let prepared = crate::domain_update::prepare(current, req, metadata, now_nanos(), policy)
            .map_err(FailoverError::from)?;
        let mut candidate = prepared.record;
        if prepared.failover && prepared.write {
            candidate.failover_audit.push(FailoverAuditEntry {
                id: uuid::Uuid::new_v4().to_string(),
                created_nanos: candidate.last_updated_time_nanos,
                failover_type: if req.failover_timeout_in_seconds.is_some() {
                    FailoverType::Graceful
                } else {
                    FailoverType::Force
                },
                before: current.replication.clone(),
                before_version: current.failover_version,
                after: candidate.replication.clone(),
                after_version: candidate.failover_version,
            });
        }
        let response = cadence_api::service_types::UpdateDomainResponse {
            domain_info: Some(candidate.info.clone()),
            configuration: Some(candidate.configuration.clone()),
            replication_configuration: Some(candidate.replication.clone()),
            failover_version: candidate.failover_version,
            is_global_domain: candidate.is_global_domain,
        };
        if prepared.write {
            inner.by_name.insert(req.name.clone(), candidate);
        }
        Ok(response)
    }

    /// Errors for domain failover, mirroring Go's domain-handler mapping:
    /// unknown domains are `EntityNotExists`, invalid requests are
    /// `BadRequest` with Go's exact messages.
    pub fn failover(
        &self,
        req: &cadence_api::types::FailoverDomainRequest,
        current_cluster: &str,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::types::FailoverDomainResponse, FailoverError> {
        use cadence_api::enums::FailoverType;
        let mut inner = self.inner.write().expect("domain store lock");
        let record = inner.by_name.get_mut(&req.domain_name).ok_or_else(|| {
            FailoverError::NotFound(EntityNotExistsError::new(format!(
                "Domain {} does not exist.",
                req.domain_name
            )))
        })?;
        // Go `validateDomainFailoverRequest`: only global domains fail over.
        if !record.is_global_domain {
            return Err(FailoverError::Invalid(
                "Local domains cannot perform failovers or change replication configuration"
                    .to_string(),
            ));
        }
        if req.active_clusters.is_some() && record.replication.active_clusters.is_none() {
            return Err(FailoverError::Invalid(
                "FailoverDomain cannot be used to promote a domain to active-active; use UpdateDomain to set cluster attributes".to_string(),
            ));
        }
        if req.domain_active_cluster_name.is_none() && req.active_clusters.is_none() {
            return Err(FailoverError::Invalid(
                "Domain's ActiveClusterName or ActiveClusters must be set to failover the domain"
                    .to_string(),
            ));
        }
        // Destination-cluster check, unless explicitly skipped.
        if !req.skip_destination_cluster_check {
            if let Some(target) = req.domain_active_cluster_name.as_ref() {
                if target != current_cluster {
                    return Err(FailoverError::Invalid(format!(
                        "Failover requests must be sent to the destination cluster: this request targets cluster {target:?} but was received by cluster {current_cluster:?}. Re-run it against the destination cluster's frontend, or set skipDestinationClusterCheck to override."
                    )));
                }
            }
            if let Some(scopes) = req
                .active_clusters
                .as_ref()
                .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
            {
                let mut scopes: Vec<_> = scopes.iter().collect();
                scopes.sort_by_key(|(name, _)| *name);
                for (_, scope) in scopes {
                    let mut attributes: Vec<_> =
                        scope.cluster_attributes.iter().flatten().collect();
                    attributes.sort_by_key(|(name, _)| *name);
                    for (_, info) in attributes {
                        let target = &info.active_cluster_name;
                        if target != current_cluster {
                            return Err(FailoverError::Invalid(format!(
                                "Failover requests must be sent to the destination cluster: this request targets cluster {target:?} but was received by cluster {current_cluster:?}. Re-run it against the destination cluster's frontend, or set skipDestinationClusterCheck to override."
                            )));
                        }
                    }
                }
            }
        }
        let update = UpdateDomainRequest {
            name: req.domain_name.clone(),
            replication_configuration: Some(
                cadence_api::service_types::DomainReplicationConfigurationUpdate {
                    active_cluster_name: req.domain_active_cluster_name.clone(),
                    active_clusters: req.active_clusters.clone(),
                    ..Default::default()
                },
            ),
            failover_timeout_in_seconds: req.failover_timeout_in_seconds,
            ..Default::default()
        };
        let prepared = crate::domain_update::prepare_failover(
            record,
            &update,
            metadata,
            now_nanos(),
            req.reason.as_deref(),
            policy,
        )
        .map_err(FailoverError::from)?;
        let mut candidate = prepared.record;
        candidate.failover_audit.push(FailoverAuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            created_nanos: candidate.last_updated_time_nanos,
            failover_type: if req.failover_timeout_in_seconds.is_some() {
                FailoverType::Graceful
            } else {
                FailoverType::Force
            },
            before: record.replication.clone(),
            before_version: record.failover_version,
            after: candidate.replication.clone(),
            after_version: candidate.failover_version,
        });
        *record = candidate;
        Ok(cadence_api::types::FailoverDomainResponse {
            domain_info: Some(record.info.clone()),
            configuration: Some(record.configuration.clone()),
            replication_configuration: Some(record.replication.clone()),
            failover_version: record.failover_version,
            is_global_domain: record.is_global_domain,
        })
    }

    /// List failover audit events for a domain UUID with cursor paging,
    /// mirroring Go frontend `ListFailoverHistory` (failover operations only;
    /// events without cluster changes are filtered like Go's
    /// `ToFailoverEvents` nil check). Returns `(events, next_page_token)`.
    pub fn failover_history(
        &self,
        domain_id: &str,
        page_size: usize,
        page_token: &[u8],
    ) -> Result<
        (
            Vec<cadence_api::types::FailoverEvent>,
            Vec<u8>,
        ),
        EntityNotExistsError,
    > {
        use cadence_api::types::{ActiveClusterInfo, ClusterFailover, FailoverEvent};
        let inner = self.inner.read().expect("domain store lock");
        let name = inner.by_id.get(domain_id).ok_or_else(|| {
            EntityNotExistsError::new(format!("Domain {domain_id} does not exist."))
        })?;
        let record = inner.by_name.get(name).expect("domain exists");
        let offset: usize = if page_token.is_empty() {
            0
        } else {
            std::str::from_utf8(page_token)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        };
        let mut events = Vec::new();
        // Newest first, like Go's audit-log ordering.
        let mut entries: Vec<&FailoverAuditEntry> = record.failover_audit.iter().collect();
        entries.reverse();
        let page: Vec<&&FailoverAuditEntry> = entries
            .iter()
            .skip(offset)
            .take(page_size.max(1))
            .collect();
        let page_len = page.len();
        for entry in page {
            let mut cluster_failovers = Vec::new();
            if entry.before.active_cluster_name != entry.after.active_cluster_name {
                cluster_failovers.push(ClusterFailover {
                    from_cluster: Some(ActiveClusterInfo {
                        active_cluster_name: entry.before.active_cluster_name.clone(),
                        failover_version: entry.before_version,
                    }),
                    to_cluster: Some(ActiveClusterInfo {
                        active_cluster_name: entry.after.active_cluster_name.clone(),
                        failover_version: entry.after_version,
                    }),
                    cluster_attribute: None,
                });
            }
            // Per-attribute changes in sorted scope/name order, mirroring Go.
            let mut scopes: Vec<&String> = entry
                .after
                .active_clusters
                .as_ref()
                .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
                .map(|m| m.keys().collect())
                .unwrap_or_default();
            scopes.sort();
            for scope in scopes {
                let after_attrs = entry
                    .after
                    .active_clusters
                    .as_ref()
                    .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
                    .and_then(|m| m.get(scope))
                    .and_then(|s| s.cluster_attributes.as_ref());
                let before_attrs = entry
                    .before
                    .active_clusters
                    .as_ref()
                    .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
                    .and_then(|m| m.get(scope))
                    .and_then(|s| s.cluster_attributes.as_ref());
                let mut names: Vec<&String> = after_attrs
                    .map(|m| m.keys().collect())
                    .unwrap_or_default();
                names.sort();
                for name in names {
                    let after_info = after_attrs.and_then(|m| m.get(name));
                    let before_info = before_attrs.and_then(|m| m.get(name));
                    let changed = match (before_info, after_info) {
                        (Some(b), Some(a)) => b.failover_version != a.failover_version,
                        (None, Some(_)) => true,
                        _ => false,
                    };
                    if !changed {
                        continue;
                    }
                    cluster_failovers.push(ClusterFailover {
                        from_cluster: before_info.cloned(),
                        to_cluster: after_info.cloned(),
                        cluster_attribute: Some(cadence_api::types::ClusterAttribute {
                            scope: (*scope).clone(),
                            name: (*name).clone(),
                        }),
                    });
                }
            }
            if cluster_failovers.is_empty() {
                continue;
            }
            events.push(FailoverEvent {
                id: Some(entry.id.clone()),
                created_time: Some(entry.created_nanos),
                failover_type: Some(entry.failover_type),
                cluster_failovers,
            });
        }
        let next_offset = offset + page_len;
        let next_page_token = if next_offset < entries.len() {
            next_offset.to_string().into_bytes()
        } else {
            Vec::new()
        };
        Ok((events, next_page_token))
    }

    /// Read the cluster-global isolation groups, mirroring Go's global
    /// isolation state read.
    pub fn global_isolation_groups(
        &self,
    ) -> Option<cadence_api::types::IsolationGroupConfiguration> {
        self.inner
            .read()
            .expect("domain store lock")
            .global_isolation_groups
            .clone()
    }

    /// Replace the cluster-global isolation groups.
    pub fn update_global_isolation_groups(
        &self,
        groups: Option<cadence_api::types::IsolationGroupConfiguration>,
    ) {
        self.inner
            .write()
            .expect("domain store lock")
            .global_isolation_groups = groups;
    }

    /// Read a domain's isolation-group overrides, mirroring Go's domain
    /// isolation state read.
    pub fn domain_isolation_groups(
        &self,
        name: &str,
    ) -> Result<Option<cadence_api::types::IsolationGroupConfiguration>, EntityNotExistsError>
    {
        Ok(self
            .inner
            .read()
            .expect("domain store lock")
            .by_name
            .get(name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?
            .isolation_groups
            .clone())
    }

    /// Replace a domain's isolation-group overrides.
    pub fn update_domain_isolation_groups(
        &self,
        name: &str,
        groups: Option<cadence_api::types::IsolationGroupConfiguration>,
    ) -> Result<(), EntityNotExistsError> {
        self.inner
            .write()
            .expect("domain store lock")
            .by_name
            .get_mut(name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?
            .isolation_groups = groups;
        Ok(())
    }

    /// Read a domain's async-workflow queue configuration.
    pub fn domain_async_workflow_config(
        &self,
        name: &str,
    ) -> Result<Option<cadence_api::types::AsyncWorkflowConfiguration>, EntityNotExistsError>
    {
        Ok(self
            .inner
            .read()
            .expect("domain store lock")
            .by_name
            .get(name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?
            .async_workflow_config
            .clone())
    }

    /// Replace a domain's async-workflow queue configuration.
    pub fn update_domain_async_workflow_config(
        &self,
        name: &str,
        config: Option<cadence_api::types::AsyncWorkflowConfiguration>,
    ) -> Result<(), EntityNotExistsError> {
        self.inner
            .write()
            .expect("domain store lock")
            .by_name
            .get_mut(name)
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?
            .async_workflow_config = config;
        Ok(())
    }

    /// Mirrors Go `DeprecateDomain`: flips status to deprecated.
    pub fn deprecate(&self, req: &DeprecateDomainRequest) -> Result<(), EntityNotExistsError> {
        let mut inner = self.inner.write().expect("domain store lock");
        let record = inner.by_name.get_mut(&req.name).ok_or_else(|| {
            EntityNotExistsError::new(format!("Domain {} does not exist.", req.name))
        })?;
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

    /// Opaque name cursor keeps pages stable when another domain is inserted
    /// before the previous page's last name.
    pub fn list_page(
        &self,
        request: &cadence_api::service_types::ListDomainsRequest,
    ) -> Result<ListDomainsResponse, cadence_api::errors::BadRequestError> {
        use cadence_api::errors::BadRequestError;
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Cursor {
            name: String,
        }
        if request.page_size < 0 {
            return Err(BadRequestError::new("Page size must be positive."));
        }
        let size = if request.page_size == 0 {
            100
        } else {
            request.page_size as usize
        };
        let mut rows = self.list().domains;
        if !request.next_page_token.is_empty() {
            let cursor: Cursor = serde_json::from_slice(&request.next_page_token)
                .map_err(|_| BadRequestError::new("Invalid NextPageToken."))?;
            rows.retain(|r| r.domain_info.as_ref().is_some_and(|i| i.name > cursor.name));
        }
        let token = if rows.len() > size {
            serde_json::to_vec(&Cursor {
                name: rows[size - 1]
                    .domain_info
                    .as_ref()
                    .expect("stored domain info")
                    .name
                    .clone(),
            })
            .map_err(|_| BadRequestError::new("Invalid NextPageToken."))?
        } else {
            Vec::new()
        };
        rows.truncate(size);
        Ok(ListDomainsResponse {
            domains: rows,
            next_page_token: token,
        })
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
            for mut record in snapshot {
                if !record.is_global_domain {
                    record.failover_version = crate::version_history::EMPTY_VERSION;
                }
                inner
                    .by_id
                    .insert(record.info.uuid.clone(), record.info.name.clone());
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
    fn matching_domain_activity_follows_local_failover_and_attribute_rules() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("d", "id");
        store.create(info, cfg, rep, false).unwrap();
        assert!(store.is_active_in("id", "passive").unwrap());
        {
            let mut inner = store.inner.write().unwrap();
            let record = inner.by_name.get_mut("d").unwrap();
            record.is_global_domain = true;
        }
        assert!(store.is_active_in("id", "active").unwrap());
        assert!(!store.is_active_in("id", "passive").unwrap());
        {
            let mut inner = store.inner.write().unwrap();
            let record = inner.by_name.get_mut("d").unwrap();
            record.replication.active_clusters = Some(serde_json::from_value(serde_json::json!({
                "activeClustersByClusterAttribute": {"region": {"clusterAttributes": {
                    "r": {"activeClusterName": "passive"}
                }}}
            })).unwrap());
        }
        assert!(store.is_active_in("id", "passive").unwrap());
        store.inner.write().unwrap().by_name.get_mut("d").unwrap().failover_end_time = Some(0);
        assert!(!store.is_active_in("id", "active").unwrap());
        assert!(!store.is_active_in("id", "passive").unwrap());
        assert!(store.is_active_in("missing", "active").is_err());
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
        assert_eq!(by_id.failover_version, crate::version_history::EMPTY_VERSION);
    }

    fn global_sample(domain: &str, uuid: &str) -> (
        DomainInfo,
        DomainConfiguration,
        DomainReplicationConfiguration,
    ) {
        let (info, cfg, mut rep) = sample(domain, uuid);
        rep.clusters.push(ClusterReplicationConfiguration {
            cluster_name: "standby".to_string(),
        });
        (info, cfg, rep)
    }

    fn test_policy() -> crate::domain_update::DomainPolicy {
        crate::domain_update::DomainPolicy { cooldown_nanos: 0, ..Default::default() }
    }

    fn cluster_metadata() -> cadence_common::cluster::Metadata {
        use cadence_common::cluster::{Metadata, ClusterInformation};
        Metadata {
            failover_version_increment: 10,
            primary_cluster_name: "active".into(),
            current_cluster_name: "active".into(),
            clusters: std::collections::BTreeMap::from([
                ("active".into(), ClusterInformation { enabled: true, initial_failover_version: 0, ..Default::default() }),
                ("standby".into(), ClusterInformation { enabled: true, initial_failover_version: 1, ..Default::default() }),
            ]),
        }
    }

    fn failover_req(domain: &str, target: &str) -> cadence_api::types::FailoverDomainRequest {
        cadence_api::types::FailoverDomainRequest {
            domain_name: domain.to_string(),
            domain_active_cluster_name: Some(target.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn failover_moves_global_active_cluster_and_audits() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = global_sample("g", "uuid-g");
        store.create(info, cfg, rep, true).unwrap();
        let resp = store.failover(&failover_req("g", "standby"), "standby", &cluster_metadata(), &test_policy()).unwrap();
        assert_eq!(
            resp.replication_configuration.unwrap().active_cluster_name,
            "standby"
        );
        assert_eq!(resp.failover_version, 1);
        let (events, next) = store.failover_history("uuid-g", 50, &[]).unwrap();
        assert_eq!(events.len(), 1);
        assert!(next.is_empty());
        let cf = &events[0].cluster_failovers;
        assert_eq!(cf.len(), 1);
        assert_eq!(
            cf[0].from_cluster.as_ref().unwrap().active_cluster_name,
            "active"
        );
        assert_eq!(
            cf[0].to_cluster.as_ref().unwrap().active_cluster_name,
            "standby"
        );
        // An explicit same-cluster target is accepted without advancing an AP version.
        let repeated = store.failover(&failover_req("g", "standby"), "standby", &cluster_metadata(), &test_policy()).unwrap();
        assert_eq!(repeated.failover_version, 1);
    }

    #[test]
    fn failover_rejects_local_wrong_destination_and_history_pages() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("local", "uuid-local");
        store.create(info, cfg, rep, false).unwrap();
        let err = store.failover(&failover_req("local", "other"), "other", &cluster_metadata(), &test_policy()).unwrap_err();
        assert!(matches!(err, FailoverError::Invalid(_)));
        let err = store
            .failover(&failover_req("missing", "other"), "other", &cluster_metadata(), &test_policy())
            .unwrap_err();
        assert!(matches!(err, FailoverError::NotFound(_)));

        let (info, cfg, rep) = global_sample("g2", "uuid-g2");
        store.create(info, cfg, rep, true).unwrap();
        // Wrong destination cluster without the skip flag.
        let err = store.failover(&failover_req("g2", "standby"), "active", &cluster_metadata(), &test_policy()).unwrap_err();
        assert!(matches!(err, FailoverError::Invalid(_)));
        // Skip flag accepts from any cluster.
        let mut req = failover_req("g2", "standby");
        req.skip_destination_cluster_check = true;
        store.failover(&req, "active", &cluster_metadata(), &test_policy()).unwrap();
        let mut back = failover_req("g2", "active");
        back.skip_destination_cluster_check = true;
        assert_eq!(store.failover(&back, "active", &cluster_metadata(), &test_policy()).unwrap().failover_version, 10);
        // Paging: newest first, one per page.
        let (first, next) = store.failover_history("uuid-g2", 1, &[]).unwrap();
        assert_eq!(first.len(), 1);
        assert!(!next.is_empty());
        let (second, next2) = store.failover_history("uuid-g2", 1, &next).unwrap();
        assert_eq!(second.len(), 1);
        assert!(next2.is_empty());
        assert_ne!(first[0].id, second[0].id);
        // Unknown domain UUIDs are not found.
        assert!(store.failover_history("nope", 10, &[]).is_err());
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
                configuration: Some(cadence_api::service_types::DomainConfigurationUpdate {
                    history_archival_status: Some(ArchivalStatus::Enabled),
                    history_archival_uri: Some("file:///tmp/history".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }, &cluster_metadata(), &test_policy())
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

#[cfg(test)]
mod page_tests {
    use super::*;
    #[test]
    fn name_cursor_is_stable_when_an_earlier_domain_is_inserted() {
        let store = InMemoryDomainStore::new();
        for name in ["bravo", "delta"] {
            let (i, c, r) = super::tests::sample(name, name);
            store.create(i, c, r, false).unwrap();
        }
        let first = store
            .list_page(&cadence_api::service_types::ListDomainsRequest {
                page_size: 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(first.domains[0].domain_info.as_ref().unwrap().name, "bravo");
        let (i, c, r) = super::tests::sample("alpha", "alpha");
        store.create(i, c, r, false).unwrap();
        let second = store
            .list_page(&cadence_api::service_types::ListDomainsRequest {
                page_size: 1,
                next_page_token: first.next_page_token,
            })
            .unwrap();
        assert_eq!(
            second.domains[0].domain_info.as_ref().unwrap().name,
            "delta"
        );
        assert!(second.next_page_token.is_empty());
    }
}
