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
    /// Go's domain notification version is stored in the flattened SQL blob
    /// and is also used by SQL domain metadata transactions. Keep it in the
    /// domain cache so a durable Frontend manager can round-trip it without
    /// inventing a version during cache refresh.
    #[serde(default)]
    pub notification_version: i64,
    #[serde(default)]
    pub failover_notification_version: i64,
    pub is_global_domain: bool,
    #[serde(default)]
    pub archival_bucket: String,
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

fn initial_previous_version() -> i64 {
    -1
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

#[derive(Debug, Default, Clone)]
struct Inner {
    by_name: HashMap<String, DomainRecord>,
    by_id: HashMap<String, String>, // uuid -> name
    /// Cluster-global isolation groups (Go global isolation state).
    /// In-memory only like Go's isolation-state cache; domain records carry
    /// their own overrides into snapshots.
    global_isolation_groups: Option<cadence_api::types::IsolationGroupConfiguration>,
    /// Matching's Go engine catches up to the highest persisted failover
    /// notification on registration, then disconnects long polls only for a
    /// later notification version.
    matching_failover_notification_version: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryDomainStore {
    inner: Arc<RwLock<Inner>>,
    failover_notifications: Arc<std::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<u64>>>>,
}

/// Frontend's domain operations behind the persistence-manager boundary.
/// Production adapters may keep an `InMemoryDomainStore` as the shared domain
/// cache while committing every write through Go's configured domain store.
pub trait FrontendDomainManager: std::fmt::Debug + Send + Sync {
    fn create_with_version(
        &self,
        info: DomainInfo,
        configuration: DomainConfiguration,
        replication: DomainReplicationConfiguration,
        is_global_domain: bool,
        failover_version: i64,
    ) -> Result<DescribeDomainResponse, DomainAlreadyExistsError>;

    fn describe(
        &self,
        request: &DescribeDomainRequest,
    ) -> Result<DescribeDomainResponse, EntityNotExistsError>;

    fn domain_id(&self, name: &str) -> Result<String, EntityNotExistsError>;

    fn is_active_in(&self, uuid: &str, cluster: &str) -> Result<bool, EntityNotExistsError>;

    fn update(
        &self,
        request: &UpdateDomainRequest,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::service_types::UpdateDomainResponse, FailoverError>;

    fn failover(
        &self,
        request: &cadence_api::types::FailoverDomainRequest,
        current_cluster: &str,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::types::FailoverDomainResponse, FailoverError>;

    fn failover_history(
        &self,
        domain_id: &str,
        page_size: usize,
        page_token: &[u8],
    ) -> Result<(Vec<cadence_api::types::FailoverEvent>, Vec<u8>), EntityNotExistsError>;

    fn deprecate(&self, request: &DeprecateDomainRequest) -> Result<(), EntityNotExistsError>;

    fn delete(&self, name: &str) -> Result<(), EntityNotExistsError>;

    fn list_page(
        &self,
        request: &cadence_api::service_types::ListDomainsRequest,
    ) -> Result<ListDomainsResponse, cadence_api::errors::BadRequestError>;

    fn subscribe_failover(&self, uuid: &str) -> tokio::sync::watch::Receiver<u64>;
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_nanos() as i64)
        .unwrap_or(0)
}

impl InMemoryDomainStore {
    /// Publish metadata discovered by Matching only when this consumer has no
    /// record for the UUID. Cache lookups may race a refresh callback, so they
    /// must never replace metadata that the callback has already published.
    pub fn insert_matching_metadata_if_missing(
        &self,
        metadata: &crate::matching_domain_metadata::MatchingDomainMetadata,
    ) {
        let mut inner = self.inner.write().expect("domain store lock");
        if inner.by_id.contains_key(&metadata.info.uuid) {
            return;
        }
        let record = DomainRecord {
            info: metadata.info.clone(),
            configuration: metadata.configuration.clone(),
            replication: metadata.replication.clone(),
            failover_version: metadata.failover_version,
            notification_version: metadata.notification_version,
            failover_notification_version: metadata.failover_notification_version,
            is_global_domain: metadata.is_global_domain,
            archival_bucket: metadata.archival_bucket.clone(),
            failover_audit: Vec::new(),
            config_version: metadata.config_version,
            last_updated_time_nanos: metadata.last_updated_time_nanos,
            failover_end_time: metadata.failover_end_time_nanos,
            previous_failover_version: metadata.previous_failover_version,
            isolation_groups: metadata.decoded_isolation_groups.clone(),
            async_workflow_config: metadata.decoded_async_workflow_config.clone(),
        };
        inner
            .by_id
            .insert(record.info.uuid.clone(), record.info.name.clone());
        inner.by_name.insert(record.info.name.clone(), record);
    }

    /// Merge a complete SQLite-backed Matching metadata refresh into the
    /// domain view used by Matching consumers. Existing failover audit history
    /// remains owned by the frontend domain handler; refreshed persistence
    /// metadata replaces the fields that Go's DomainCache supplies to Matching.
    pub fn sync_matching_metadata(
        &self,
        domains: &[crate::matching_domain_metadata::MatchingDomainMetadata],
    ) {
        let mut notifications = Vec::new();
        let mut inner = self.inner.write().expect("domain store lock");
        // The initial cache catch-up establishes the Go engine's baseline.
        // It must never disconnect polls merely because a domain is loaded.
        let initialize_failover_baseline = inner.matching_failover_notification_version.is_none();
        if initialize_failover_baseline {
            inner.matching_failover_notification_version = Some(
                domains
                    .iter()
                    .map(|metadata| metadata.failover_notification_version)
                    .max()
                    .unwrap_or_default(),
            );
        }
        let failover_baseline = inner
            .matching_failover_notification_version
            .unwrap_or_default();
        for metadata in domains {
            let old_name = inner.by_id.get(&metadata.info.uuid).cloned();
            let old = old_name
                .as_ref()
                .and_then(|name| inner.by_name.get(name))
                .cloned();
            let record = DomainRecord {
                info: metadata.info.clone(),
                configuration: metadata.configuration.clone(),
                replication: metadata.replication.clone(),
                failover_version: metadata.failover_version,
                notification_version: metadata.notification_version,
                failover_notification_version: metadata.failover_notification_version,
                is_global_domain: metadata.is_global_domain,
                archival_bucket: metadata.archival_bucket.clone(),
                failover_audit: old
                    .as_ref()
                    .map(|record| record.failover_audit.clone())
                    .unwrap_or_default(),
                config_version: metadata.config_version,
                last_updated_time_nanos: metadata.last_updated_time_nanos,
                failover_end_time: metadata.failover_end_time_nanos,
                previous_failover_version: metadata.previous_failover_version,
                isolation_groups: metadata.decoded_isolation_groups.clone(),
                async_workflow_config: metadata.decoded_async_workflow_config.clone(),
            };
            if !initialize_failover_baseline
                && record.is_global_domain
                && record.failover_notification_version > failover_baseline
            {
                notifications.push(record.clone());
            }
            inner.matching_failover_notification_version = Some(
                inner
                    .matching_failover_notification_version
                    .unwrap_or_default()
                    .max(record.failover_notification_version),
            );
            if let Some(old_name) = old_name {
                if old_name != record.info.name {
                    inner.by_name.remove(&old_name);
                }
            }
            inner
                .by_id
                .insert(record.info.uuid.clone(), record.info.name.clone());
            inner.by_name.insert(record.info.name.clone(), record);
        }
        drop(inner);
        for record in &notifications {
            self.notify_failover(record);
        }
    }

    /// Per-domain failover generation. Subscribing before a Matching poll
    /// starts prevents a domain change from leaving its old long poll blocked.
    pub fn subscribe_failover(&self, uuid: &str) -> tokio::sync::watch::Receiver<u64> {
        self.failover_notifications
            .lock()
            .expect("domain notification lock")
            .entry(uuid.into())
            .or_insert_with(|| tokio::sync::watch::channel(0).0)
            .subscribe()
    }
    fn notify_failover(&self, record: &DomainRecord) {
        if !record.is_global_domain {
            return;
        }
        let active_active = record.replication.active_clusters.as_ref().is_some_and(|active| {
            active
                    .active_clusters_by_cluster_attribute
                    .as_ref()
                    .is_some_and(|scopes| {
                        scopes.values().any(|scope| {
                            scope
                                .cluster_attributes
                                .as_ref()
                                .is_some_and(|attributes| !attributes.is_empty())
                        })
                    })
        });
        if active_active {
            return;
        }
        if let Some(sender) = self
            .failover_notifications
            .lock()
            .expect("domain notification lock")
            .get(&record.info.uuid)
        {
            sender.send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }
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
        self.create_with_version(
            info,
            configuration,
            replication,
            is_global_domain,
            if is_global_domain {
                0
            } else {
                crate::version_history::EMPTY_VERSION
            },
        )
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
            notification_version: 0,
            failover_notification_version: 0,
            is_global_domain,
            archival_bucket: String::new(),
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
        let record = inner
            .by_id
            .get(uuid)
            .and_then(|name| inner.by_name.get(name))
            .ok_or_else(|| EntityNotExistsError::new(format!("Domain {uuid} does not exist.")))?;
        if !record.is_global_domain {
            return Ok(true);
        }
        if record.failover_end_time.is_some() {
            return Ok(false);
        }
        if record.replication.active_cluster_name == cluster {
            return Ok(true);
        }
        Ok(record
            .replication
            .active_clusters
            .as_ref()
            .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
            .is_some_and(|scopes| {
                scopes.values().any(|scope| {
                    scope.cluster_attributes.as_ref().is_some_and(|attributes| {
                        attributes
                            .values()
                            .any(|attribute| attribute.active_cluster_name == cluster)
                    })
                })
            }))
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
            if prepared.failover {
                self.notify_failover(&candidate);
            }
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
        self.notify_failover(record);
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
    ) -> Result<(Vec<cadence_api::types::FailoverEvent>, Vec<u8>), EntityNotExistsError> {
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
        let page: Vec<&&FailoverAuditEntry> =
            entries.iter().skip(offset).take(page_size.max(1)).collect();
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
                let mut names: Vec<&String> =
                    after_attrs.map(|m| m.keys().collect()).unwrap_or_default();
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
    ) -> Result<Option<cadence_api::types::IsolationGroupConfiguration>, EntityNotExistsError> {
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
    ) -> Result<Option<cadence_api::types::AsyncWorkflowConfiguration>, EntityNotExistsError> {
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

impl FrontendDomainManager for InMemoryDomainStore {
    fn create_with_version(
        &self,
        info: DomainInfo,
        configuration: DomainConfiguration,
        replication: DomainReplicationConfiguration,
        is_global_domain: bool,
        failover_version: i64,
    ) -> Result<DescribeDomainResponse, DomainAlreadyExistsError> {
        InMemoryDomainStore::create_with_version(
            self,
            info,
            configuration,
            replication,
            is_global_domain,
            failover_version,
        )
    }

    fn describe(
        &self,
        request: &DescribeDomainRequest,
    ) -> Result<DescribeDomainResponse, EntityNotExistsError> {
        InMemoryDomainStore::describe(self, request)
    }

    fn domain_id(&self, name: &str) -> Result<String, EntityNotExistsError> {
        InMemoryDomainStore::domain_id(self, name)
    }

    fn is_active_in(&self, uuid: &str, cluster: &str) -> Result<bool, EntityNotExistsError> {
        InMemoryDomainStore::is_active_in(self, uuid, cluster)
    }

    fn update(
        &self,
        request: &UpdateDomainRequest,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::service_types::UpdateDomainResponse, FailoverError> {
        InMemoryDomainStore::update(self, request, metadata, policy)
    }

    fn failover(
        &self,
        request: &cadence_api::types::FailoverDomainRequest,
        current_cluster: &str,
        metadata: &cadence_common::cluster::Metadata,
        policy: &crate::domain_update::DomainPolicy,
    ) -> Result<cadence_api::types::FailoverDomainResponse, FailoverError> {
        InMemoryDomainStore::failover(self, request, current_cluster, metadata, policy)
    }

    fn failover_history(
        &self,
        domain_id: &str,
        page_size: usize,
        page_token: &[u8],
    ) -> Result<(Vec<cadence_api::types::FailoverEvent>, Vec<u8>), EntityNotExistsError> {
        InMemoryDomainStore::failover_history(self, domain_id, page_size, page_token)
    }

    fn deprecate(&self, request: &DeprecateDomainRequest) -> Result<(), EntityNotExistsError> {
        InMemoryDomainStore::deprecate(self, request)
    }

    fn delete(&self, name: &str) -> Result<(), EntityNotExistsError> {
        InMemoryDomainStore::delete(self, name)
    }

    fn list_page(
        &self,
        request: &cadence_api::service_types::ListDomainsRequest,
    ) -> Result<ListDomainsResponse, cadence_api::errors::BadRequestError> {
        InMemoryDomainStore::list_page(self, request)
    }

    fn subscribe_failover(&self, uuid: &str) -> tokio::sync::watch::Receiver<u64> {
        InMemoryDomainStore::subscribe_failover(self, uuid)
    }
}

/// A SQL-backed Frontend domain manager over Cadence's existing Go `domains`
/// and `domain_metadata` tables. The in-memory store remains the shared cache
/// consumed by Matching, while each write is staged on a private snapshot,
/// committed with Go's metadata-version transaction, and only then published
/// to that cache.
///
/// This manager deliberately accepts the persistence contract rather than a
/// database-specific pool. MySQL and PostgreSQL implement that contract using
/// the same transactions and blob codec as Matching's native persistence.
pub struct GoSqlFrontendDomainManager {
    cache: InMemoryDomainStore,
    persistence: std::sync::Arc<dyn crate::matching_domain_metadata::GoSqlDomainPersistence>,
    write_lock: std::sync::Mutex<()>,
}

impl std::fmt::Debug for GoSqlFrontendDomainManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoSqlFrontendDomainManager").finish_non_exhaustive()
    }
}

impl GoSqlFrontendDomainManager {
    pub fn new(
        cache: InMemoryDomainStore,
        persistence: std::sync::Arc<dyn crate::matching_domain_metadata::GoSqlDomainPersistence>,
    ) -> Self {
        Self { cache, persistence, write_lock: std::sync::Mutex::new(()) }
    }

    fn staged_cache(&self) -> InMemoryDomainStore {
        InMemoryDomainStore {
            inner: std::sync::Arc::new(RwLock::new(
                self.cache.inner.read().expect("domain store lock").clone(),
            )),
            failover_notifications: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn record_metadata(
        record: &DomainRecord,
        previous: Option<crate::matching_domain_metadata::MatchingDomainMetadata>,
    ) -> crate::matching_domain_metadata::MatchingDomainMetadata {
        use crate::matching_domain_metadata::MatchingDomainMetadata;
        let mut metadata = previous.unwrap_or_else(|| MatchingDomainMetadata {
            info: record.info.clone(),
            configuration: record.configuration.clone(),
            replication: record.replication.clone(),
            is_global_domain: record.is_global_domain,
            archival_bucket: record.archival_bucket.clone(),
            status_raw: record.info.status.map(|status| status as i32).unwrap_or_default(),
            archival_status_raw: 0,
            history_archival_status_raw: record.configuration.history_archival_status.map(|status| status as i16).unwrap_or_default(),
            visibility_archival_status_raw: record.configuration.visibility_archival_status.map(|status| status as i16).unwrap_or_default(),
            config_version: record.config_version,
            notification_version: record.notification_version,
            failover_notification_version: record.failover_notification_version,
            failover_version: record.failover_version,
            previous_failover_version: record.previous_failover_version,
            failover_end_time_nanos: record.failover_end_time,
            last_updated_time_nanos: record.last_updated_time_nanos,
            bad_binaries: None,
            isolation_groups: None,
            async_workflow_config: None,
            active_clusters_config: None,
            decoded_bad_binaries: None,
            decoded_isolation_groups: record.isolation_groups.clone(),
            decoded_async_workflow_config: record.async_workflow_config.clone(),
            config_decode_error: None,
        });
        metadata.info.clone_from(&record.info);
        metadata.configuration.clone_from(&record.configuration);
        metadata.replication.clone_from(&record.replication);
        metadata.is_global_domain = record.is_global_domain;
        metadata.archival_bucket.clone_from(&record.archival_bucket);
        metadata.status_raw = record.info.status.map(|status| status as i32).unwrap_or_default();
        metadata.history_archival_status_raw = record.configuration.history_archival_status.map(|status| status as i16).unwrap_or_default();
        metadata.visibility_archival_status_raw = record.configuration.visibility_archival_status.map(|status| status as i16).unwrap_or_default();
        metadata.config_version = record.config_version;
        metadata.notification_version = record.notification_version;
        metadata.failover_notification_version = record.failover_notification_version;
        metadata.failover_version = record.failover_version;
        metadata.previous_failover_version = record.previous_failover_version;
        metadata.failover_end_time_nanos = record.failover_end_time;
        metadata.last_updated_time_nanos = record.last_updated_time_nanos;
        metadata.decoded_isolation_groups = record.isolation_groups.clone();
        metadata.decoded_async_workflow_config = record.async_workflow_config.clone();
        metadata
    }

    fn staged_record(staged: &InMemoryDomainStore, uuid: &str) -> Option<DomainRecord> {
        let inner = staged.inner.read().expect("domain store lock");
        let name = inner.by_id.get(uuid)?;
        inner.by_name.get(name).cloned()
    }

    fn publish(&self, staged: &InMemoryDomainStore, uuid: &str) {
        let Some(mut record) = Self::staged_record(staged, uuid) else { return };
        let previous_failover_notification = {
            let mut inner = self.cache.inner.write().expect("domain store lock");
            let old_name = inner.by_id.get(uuid).cloned();
            let old = old_name.as_ref().and_then(|name| inner.by_name.get(name)).cloned();
            if let Some(old) = &old {
                record.failover_audit.clone_from(&old.failover_audit);
            }
            if let Some(old_name) = old_name {
                if old_name != record.info.name {
                    inner.by_name.remove(&old_name);
                }
            }
            inner.by_id.insert(record.info.uuid.clone(), record.info.name.clone());
            inner.by_name.insert(record.info.name.clone(), record.clone());
            inner.matching_failover_notification_version = Some(
                inner.matching_failover_notification_version.unwrap_or_default()
                    .max(record.failover_notification_version),
            );
            old.map(|old| old.failover_notification_version)
        };
        if previous_failover_notification.is_some_and(|previous| {
            record.failover_notification_version > previous
        }) {
            self.cache.notify_failover(&record);
        }
    }

    fn load(&self, name: Option<&str>, id: Option<&str>) -> Result<crate::matching_domain_metadata::MatchingDomainMetadata, crate::matching_domain_metadata::MatchingDomainMetadataError> {
        self.persistence.get_domain(name, id)
    }

    fn commit_update(
        &self,
        metadata: &mut crate::matching_domain_metadata::MatchingDomainMetadata,
    ) -> Result<(), crate::matching_domain_metadata::MatchingDomainMetadataError> {
        let expected = self.persistence.get_metadata_version()?;
        metadata.notification_version = expected;
        self.persistence.update_domain(metadata, expected)
    }

    fn persistence_failover(error: impl std::fmt::Display) -> FailoverError {
        FailoverError::Busy(error.to_string())
    }
}

impl FrontendDomainManager for GoSqlFrontendDomainManager {
    fn create_with_version(
        &self,
        info: DomainInfo,
        configuration: DomainConfiguration,
        replication: DomainReplicationConfiguration,
        is_global_domain: bool,
        failover_version: i64,
    ) -> Result<DescribeDomainResponse, DomainAlreadyExistsError> {
        let _guard = self.write_lock.lock().expect("domain persistence write lock");
        let staged = self.staged_cache();
        let response = staged.create_with_version(
            info, configuration, replication, is_global_domain, failover_version,
        )?;
        let uuid = response.domain_info.as_ref().map(|info| info.uuid.as_str()).unwrap_or_default();
        let record = Self::staged_record(&staged, uuid).expect("created domain record");
        let metadata = Self::record_metadata(&record, None);
        let notification_version = self.persistence.create_domain(&metadata).map_err(|error| DomainAlreadyExistsError {
            message: format!("Domain {} already exists or could not be created: {error}", record.info.name),
        })?;
        let mut persisted = metadata;
        persisted.notification_version = notification_version;
        staged.sync_matching_metadata(&[persisted]);
        self.publish(&staged, uuid);
        Ok(response)
    }

    fn describe(&self, request: &DescribeDomainRequest) -> Result<DescribeDomainResponse, EntityNotExistsError> {
        let metadata = self.load(request.name.as_deref(), request.uuid.as_deref()).map_err(|_| {
            let identity = request.name.as_deref().or(request.uuid.as_deref()).unwrap_or("Domain not set on request.");
            EntityNotExistsError::new(format!("Domain {identity} does not exist."))
        })?;
        self.cache.sync_matching_metadata(std::slice::from_ref(&metadata));
        self.cache.describe(request)
    }

    fn domain_id(&self, name: &str) -> Result<String, EntityNotExistsError> {
        let metadata = self.load(Some(name), None).map_err(|_| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?;
        Ok(metadata.info.uuid)
    }

    fn is_active_in(&self, uuid: &str, cluster: &str) -> Result<bool, EntityNotExistsError> {
        let metadata = self.load(None, Some(uuid)).map_err(|_| EntityNotExistsError::new(format!("Domain {uuid} does not exist.")))?;
        self.cache.sync_matching_metadata(&[metadata]);
        self.cache.is_active_in(uuid, cluster)
    }

    fn update(&self, request: &UpdateDomainRequest, metadata: &cadence_common::cluster::Metadata, policy: &crate::domain_update::DomainPolicy) -> Result<cadence_api::service_types::UpdateDomainResponse, FailoverError> {
        let _guard = self.write_lock.lock().expect("domain persistence write lock");
        let current = self.load(Some(&request.name), None).map_err(Self::persistence_failover)?;
        let staged = self.staged_cache();
        staged.sync_matching_metadata(std::slice::from_ref(&current));
        let result = staged.update(request, metadata, policy)?;
        let record = Self::staged_record(&staged, &current.info.uuid).expect("updated domain record");
        let mut next = Self::record_metadata(&record, Some(current.clone()));
        self.commit_update(&mut next).map_err(Self::persistence_failover)?;
        let persisted = self.persistence.get_domain(None, Some(&current.info.uuid)).map_err(Self::persistence_failover)?;
        staged.sync_matching_metadata(&[persisted]);
        self.publish(&staged, &current.info.uuid);
        Ok(result)
    }

    fn failover(&self, request: &cadence_api::types::FailoverDomainRequest, current_cluster: &str, metadata: &cadence_common::cluster::Metadata, policy: &crate::domain_update::DomainPolicy) -> Result<cadence_api::types::FailoverDomainResponse, FailoverError> {
        let _guard = self.write_lock.lock().expect("domain persistence write lock");
        let current = self.load(Some(&request.domain_name), None).map_err(Self::persistence_failover)?;
        let staged = self.staged_cache();
        staged.sync_matching_metadata(std::slice::from_ref(&current));
        let result = staged.failover(request, current_cluster, metadata, policy)?;
        let record = Self::staged_record(&staged, &current.info.uuid).expect("failed-over domain record");
        let mut next = Self::record_metadata(&record, Some(current.clone()));
        self.commit_update(&mut next).map_err(Self::persistence_failover)?;
        let persisted = self.persistence.get_domain(None, Some(&current.info.uuid)).map_err(Self::persistence_failover)?;
        staged.sync_matching_metadata(&[persisted]);
        self.publish(&staged, &current.info.uuid);
        Ok(result)
    }

    fn failover_history(&self, domain_id: &str, page_size: usize, page_token: &[u8]) -> Result<(Vec<cadence_api::types::FailoverEvent>, Vec<u8>), EntityNotExistsError> {
        self.cache.failover_history(domain_id, page_size, page_token)
    }

    fn deprecate(&self, request: &DeprecateDomainRequest) -> Result<(), EntityNotExistsError> {
        let _guard = self.write_lock.lock().expect("domain persistence write lock");
        let current = self.load(Some(&request.name), None).map_err(|_| EntityNotExistsError::new(format!("Domain {} does not exist.", request.name)))?;
        let staged = self.staged_cache();
        staged.sync_matching_metadata(std::slice::from_ref(&current));
        staged.deprecate(request)?;
        let record = Self::staged_record(&staged, &current.info.uuid).expect("deprecated domain record");
        let mut next = Self::record_metadata(&record, Some(current.clone()));
        self.commit_update(&mut next).map_err(|error| EntityNotExistsError::new(error.to_string()))?;
        let persisted = self.persistence.get_domain(None, Some(&current.info.uuid)).map_err(|error| EntityNotExistsError::new(error.to_string()))?;
        staged.sync_matching_metadata(&[persisted]);
        self.publish(&staged, &current.info.uuid);
        Ok(())
    }

    fn delete(&self, name: &str) -> Result<(), EntityNotExistsError> {
        let _guard = self.write_lock.lock().expect("domain persistence write lock");
        let current = self.load(Some(name), None).map_err(|_| EntityNotExistsError::new(format!("Domain {name} does not exist.")))?;
        self.persistence.delete_domain(&current.info.uuid).map_err(|error| EntityNotExistsError::new(error.to_string()))?;
        self.cache.sync_matching_metadata(std::slice::from_ref(&current));
        self.cache.delete(name)
    }

    fn list_page(&self, request: &cadence_api::service_types::ListDomainsRequest) -> Result<ListDomainsResponse, cadence_api::errors::BadRequestError> {
        if request.page_size < 0 {
            return Err(cadence_api::errors::BadRequestError::new("Page size must be positive."));
        }
        let page_size = if request.page_size <= 0 { 100 } else { request.page_size as usize };
        let page = self.persistence.list_domains(page_size.saturating_add(1), (!request.next_page_token.is_empty()).then_some(request.next_page_token.as_slice())).map_err(|error| cadence_api::errors::BadRequestError::new(error.to_string()))?;
        let mut domains = page.domains;
        let next_page_token = if domains.len() >= page_size {
            let next = uuid::Uuid::parse_str(&domains[page_size - 1].info.uuid)
                .map(|uuid| uuid.as_bytes().to_vec())
                .unwrap_or_else(|_| domains[page_size - 1].info.uuid.as_bytes().to_vec());
            domains.truncate(page_size);
            next
        } else {
            page.next_page_token.unwrap_or_default()
        };
        self.cache.sync_matching_metadata(&domains);
        Ok(ListDomainsResponse {
            domains: domains.into_iter().map(|metadata| DescribeDomainResponse {
                domain_info: Some(metadata.info),
                configuration: Some(metadata.configuration),
                replication_configuration: Some(metadata.replication),
                failover_version: metadata.failover_version,
                is_global_domain: metadata.is_global_domain,
            }).collect(),
            next_page_token,
        })
    }

    fn subscribe_failover(&self, uuid: &str) -> tokio::sync::watch::Receiver<u64> {
        self.cache.subscribe_failover(uuid)
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
    fn matching_metadata_refresh_updates_full_domain_state_and_publishes_failover() {
        let domains = InMemoryDomainStore::new();
        let id = "00000000-0000-0000-0000-000000000077";
        let mut metadata = crate::matching_domain_metadata::MatchingDomainMetadata {
            info: DomainInfo {
                name: "go-seeded".into(),
                uuid: id.into(),
                status: Some(DomainStatus::Registered),
                ..DomainInfo::default()
            },
            configuration: DomainConfiguration {
                workflow_execution_retention_period_in_days: 17,
                ..DomainConfiguration::default()
            },
            replication: DomainReplicationConfiguration {
                active_cluster_name: "active-a".into(),
                clusters: vec![ClusterReplicationConfiguration {
                    cluster_name: "active-a".into(),
                }],
                active_clusters: None,
            },
            is_global_domain: true,
            archival_bucket: "bucket-a".into(),
            status_raw: DomainStatus::Registered as i32,
            archival_status_raw: 0,
            history_archival_status_raw: 0,
            visibility_archival_status_raw: 0,
            config_version: 4,
            notification_version: 6,
            failover_notification_version: 1,
            failover_version: 8,
            previous_failover_version: 7,
            failover_end_time_nanos: None,
            last_updated_time_nanos: 99,
            bad_binaries: None,
            isolation_groups: None,
            async_workflow_config: None,
            active_clusters_config: None,
            decoded_bad_binaries: None,
            decoded_isolation_groups: Some(cadence_api::types::IsolationGroupConfiguration {
                isolation_groups: vec![cadence_api::types::IsolationGroupPartition {
                    name: "drained".into(),
                    state: Some(cadence_api::enums::IsolationGroupState::Drained),
                }],
            }),
            decoded_async_workflow_config: Some(cadence_api::types::AsyncWorkflowConfiguration {
                enabled: true,
                predefined_queue_name: "work-queue".into(),
                ..Default::default()
            }),
            config_decode_error: None,
        };
        let mut failover = domains.subscribe_failover(id);
        domains.sync_matching_metadata(&[metadata.clone()]);
        let stale_discovery = metadata.clone();
        let view = domains
            .describe(&cadence_api::service_types::DescribeDomainRequest {
                name: Some("go-seeded".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(
            domains
                .inner
                .read()
                .unwrap()
                .by_name
                .get("go-seeded")
                .unwrap()
                .notification_version,
            6
        );
        assert_eq!(
            view.configuration
                .unwrap()
                .workflow_execution_retention_period_in_days,
            17
        );
        assert_eq!(view.failover_version, 8);
        assert!(domains.is_active_in(id, "active-a").unwrap());
        assert_eq!(
            domains
                .domain_isolation_groups("go-seeded")
                .unwrap()
                .unwrap()
                .isolation_groups[0]
                .name,
            "drained"
        );
        assert!(
            domains
                .domain_async_workflow_config("go-seeded")
                .unwrap()
                .unwrap()
                .enabled
        );
        assert!(!failover.has_changed().unwrap());

        // An ordinary domain configuration publication has a newer metadata
        // notification but the same failover notification version.
        metadata.notification_version = 7;
        metadata.configuration.workflow_execution_retention_period_in_days = 18;
        domains.sync_matching_metadata(&[metadata.clone()]);
        assert_eq!(
            domains
                .inner
                .read()
                .unwrap()
                .by_name
                .get("go-seeded")
                .unwrap()
                .notification_version,
            7
        );
        assert!(!failover.has_changed().unwrap());

        // Go's IsActiveActive excludes attribute-scoped domains from the
        // blocked-poller release callback even when their failover version
        // advances.
        metadata.notification_version = 8;
        metadata.failover_notification_version = 2;
        metadata.replication.active_clusters = Some(cadence_api::types::ActiveClusters {
            active_clusters_by_region: Some(std::collections::HashMap::from([(
                "legacy-region".into(),
                cadence_api::types::ActiveClusterInfo::default(),
            )])),
            active_clusters_by_cluster_attribute: Some(std::collections::HashMap::from([(
                "region".into(),
                cadence_api::types::ClusterAttributeScope {
                    cluster_attributes: Some(std::collections::HashMap::from([(
                        "attribute".into(),
                        cadence_api::types::ActiveClusterInfo::default(),
                    )])),
                },
            )])),
        });
        domains.sync_matching_metadata(&[metadata.clone()]);
        assert!(!failover.has_changed().unwrap());

        // Legacy region-only metadata is not active-active according to Go's
        // IsActiveActive predicate and must release an eligible failover poll.
        metadata.failover_notification_version = 3;
        metadata.replication.active_clusters = Some(cadence_api::types::ActiveClusters {
            active_clusters_by_region: Some(std::collections::HashMap::from([(
                "legacy-region".into(),
                cadence_api::types::ActiveClusterInfo::default(),
            )])),
            ..Default::default()
        });
        domains.sync_matching_metadata(&[metadata.clone()]);
        assert!(failover.has_changed().unwrap());
        let _ = failover.borrow_and_update();

        // A later eligible failover releases blocked polls, while retaining
        // the domain and task-list manager so subsequent polls can register.
        metadata.notification_version = 9;
        metadata.failover_notification_version = 4;
        metadata.failover_version = 9;
        metadata.previous_failover_version = 8;
        metadata.replication.active_cluster_name = "active-b".into();
        metadata.replication.active_clusters = None;
        domains.sync_matching_metadata(&[metadata]);
        assert!(failover.has_changed().unwrap());
        assert!(!domains.is_active_in(id, "active-a").unwrap());
        assert!(domains.is_active_in(id, "active-b").unwrap());

        // A cold cache lookup can finish after a newer refresh callback. Its
        // stale snapshot must not roll back configuration or failover state.
        domains.insert_matching_metadata_if_missing(&stale_discovery);
        let view = domains
            .describe(&cadence_api::service_types::DescribeDomainRequest {
                name: Some("go-seeded".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(view.failover_version, 9);
        assert_eq!(
            view.configuration
                .unwrap()
                .workflow_execution_retention_period_in_days,
            18
        );
        assert!(domains.is_active_in(id, "active-b").unwrap());

        let mut newly_discovered = stale_discovery;
        newly_discovered.info.uuid = "00000000-0000-0000-0000-000000000078".into();
        newly_discovered.info.name = "cold-discovered".into();
        domains.insert_matching_metadata_if_missing(&newly_discovered);
        assert_eq!(
            domains
                .describe(&cadence_api::service_types::DescribeDomainRequest {
                    name: Some("cold-discovered".into()),
                    uuid: None,
                })
                .unwrap()
                .failover_version,
            newly_discovered.failover_version
        );
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
        assert_eq!(
            by_id.failover_version,
            crate::version_history::EMPTY_VERSION
        );
    }

    fn global_sample(
        domain: &str,
        uuid: &str,
    ) -> (
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
        crate::domain_update::DomainPolicy {
            cooldown_nanos: 0,
            ..Default::default()
        }
    }

    fn cluster_metadata() -> cadence_common::cluster::Metadata {
        use cadence_common::cluster::{ClusterInformation, Metadata};
        Metadata {
            failover_version_increment: 10,
            primary_cluster_name: "active".into(),
            current_cluster_name: "active".into(),
            clusters: std::collections::BTreeMap::from([
                (
                    "active".into(),
                    ClusterInformation {
                        enabled: true,
                        initial_failover_version: 0,
                        ..Default::default()
                    },
                ),
                (
                    "standby".into(),
                    ClusterInformation {
                        enabled: true,
                        initial_failover_version: 1,
                        ..Default::default()
                    },
                ),
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
    fn failover_notifications_track_only_eligible_domain_changes() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = global_sample("g", "uuid-g");
        store.create(info, cfg, rep, true).unwrap();
        let mut watch = store.subscribe_failover("uuid-g");
        let other = store.subscribe_failover("unrelated");
        assert!(!watch.has_changed().unwrap());
        store
            .failover(
                &failover_req("g", "standby"),
                "standby",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap();
        assert!(watch.has_changed().unwrap());
        assert_eq!(*watch.borrow_and_update(), 1);
        assert!(!other.has_changed().unwrap());
        // New subscribers observe the latest generation without spuriously
        // disconnecting a poll that began after the failover.
        assert!(!store.subscribe_failover("uuid-g").has_changed().unwrap());
        let (info, cfg, rep) = sample("local", "local-id");
        store.create(info, cfg, rep, false).unwrap();
        let local = store.subscribe_failover("local-id");
        store.notify_failover(store.inner.read().unwrap().by_name.get("local").unwrap());
        assert!(!local.has_changed().unwrap());
    }

    #[test]
    fn failover_moves_global_active_cluster_and_audits() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = global_sample("g", "uuid-g");
        store.create(info, cfg, rep, true).unwrap();
        let resp = store
            .failover(
                &failover_req("g", "standby"),
                "standby",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap();
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
        let repeated = store
            .failover(
                &failover_req("g", "standby"),
                "standby",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap();
        assert_eq!(repeated.failover_version, 1);
    }

    #[test]
    fn failover_rejects_local_wrong_destination_and_history_pages() {
        let store = InMemoryDomainStore::new();
        let (info, cfg, rep) = sample("local", "uuid-local");
        store.create(info, cfg, rep, false).unwrap();
        let err = store
            .failover(
                &failover_req("local", "other"),
                "other",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap_err();
        assert!(matches!(err, FailoverError::Invalid(_)));
        let err = store
            .failover(
                &failover_req("missing", "other"),
                "other",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap_err();
        assert!(matches!(err, FailoverError::NotFound(_)));

        let (info, cfg, rep) = global_sample("g2", "uuid-g2");
        store.create(info, cfg, rep, true).unwrap();
        // Wrong destination cluster without the skip flag.
        let err = store
            .failover(
                &failover_req("g2", "standby"),
                "active",
                &cluster_metadata(),
                &test_policy(),
            )
            .unwrap_err();
        assert!(matches!(err, FailoverError::Invalid(_)));
        // Skip flag accepts from any cluster.
        let mut req = failover_req("g2", "standby");
        req.skip_destination_cluster_check = true;
        store
            .failover(&req, "active", &cluster_metadata(), &test_policy())
            .unwrap();
        let mut back = failover_req("g2", "active");
        back.skip_destination_cluster_check = true;
        assert_eq!(
            store
                .failover(&back, "active", &cluster_metadata(), &test_policy())
                .unwrap()
                .failover_version,
            10
        );
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

#[cfg(test)]
mod go_sql_frontend_manager_tests {
    use super::*;

    #[test]
    fn sqlite_domain_manager_survives_cache_restart() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE domains (shard_id INTEGER, id BLOB, name TEXT, is_global BOOLEAN, data BLOB, data_encoding TEXT, PRIMARY KEY (shard_id, id));
                 CREATE TABLE domain_metadata (id INTEGER PRIMARY KEY, notification_version INTEGER);
                 INSERT INTO domain_metadata VALUES (1, 7);",
            )
            .unwrap();
        let persistence = std::sync::Arc::new(
            crate::matching_domain_metadata::MatchingDomainMetadataStore::from_connection(
                connection,
                "cluster-a",
            ),
        );
        let manager = GoSqlFrontendDomainManager::new(
            InMemoryDomainStore::new(),
            persistence.clone(),
        );
        let domain_id = uuid::Uuid::from_u128(0x1234).to_string();
        let (mut info, config, replication) = super::tests::sample("durable", &domain_id);
        info.uuid = domain_id.clone();

        manager
            .create_with_version(info, config, replication, false, 1)
            .unwrap();
        assert_eq!(persistence.get_metadata_version().unwrap(), 8);

        // A fresh Frontend cache must read the Go-format row from SQL.
        let restarted = GoSqlFrontendDomainManager::new(
            InMemoryDomainStore::new(),
            persistence.clone(),
        );
        let described = restarted
            .describe(&DescribeDomainRequest {
                name: Some("durable".into()),
                uuid: None,
            })
            .unwrap();
        assert_eq!(described.domain_info.unwrap().uuid, domain_id);
        restarted.delete("durable").unwrap();
        assert!(persistence.get_domain(Some("durable"), None).is_err());
    }
}
