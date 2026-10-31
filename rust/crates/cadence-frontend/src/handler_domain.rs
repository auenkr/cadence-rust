//! Domain API handler behavior ported from Go's `common/domain` package.

use std::collections::{BTreeMap, BTreeSet};

use cadence_api::{
    enums::DomainStatus,
    errors::{BadRequestError, DomainAlreadyExistsError, EntityNotExistsError, ServiceBusyError},
    service_types::{
        DeprecateDomainRequest, DescribeDomainRequest, DescribeDomainResponse, ListDomainsRequest,
        ListDomainsResponse, UpdateDomainRequest, UpdateDomainResponse,
    },
    types::{
        ClusterReplicationConfiguration, DomainConfiguration, DomainInfo,
        DomainReplicationConfiguration, RegisterDomainRequest,
    },
};

#[derive(Debug, Default)]
pub struct Handler;

impl Handler {
    /// Create a domain in the shared persistence state. The Go handler checks
    /// for an existing name before validating the name, defaults replication
    /// to the active cluster, then validates attributes before persistence.
    pub fn register_domain(
        state: &cadence_service_core::handlers::AppState,
        request: Option<&RegisterDomainRequest>,
    ) -> Result<DescribeDomainResponse, DomainHandlerError> {
        let request = request.ok_or_else(|| {
            DomainHandlerError::BadRequest(cadence_api::validation::err_request_not_set())
        })?;
        if !state.cluster_metadata.is_primary_cluster() && request.is_global_domain {
            return Err(DomainHandlerError::BadRequest(bad(
                "Cluster is not primary cluster, cannot do domain registration or domain update.",
            )));
        }
        if state
            .domain_manager
            .describe(&DescribeDomainRequest {
                name: Some(request.name.clone()),
                uuid: None,
            })
            .is_ok()
        {
            return Err(DomainHandlerError::AlreadyExists(
                DomainAlreadyExistsError::new("Domain already exists."),
            ));
        }
        validate_domain_name(&request.name).map_err(DomainHandlerError::BadRequest)?;
        if request.name.len() > state.limits.ids.domain_max {
            return Err(DomainHandlerError::BadRequest(bad(
                "Domain length exceeds limit.",
            )));
        }

        let active_cluster_name = if request.active_cluster_name.is_empty() {
            state.current_cluster_name.clone()
        } else {
            request.active_cluster_name.clone()
        };
        let clusters = if request.clusters.is_empty() {
            vec![ClusterReplicationConfiguration {
                cluster_name: active_cluster_name.clone(),
            }]
        } else {
            request.clusters.clone()
        };
        let replication = DomainReplicationConfiguration {
            active_cluster_name: active_cluster_name.clone(),
            clusters,
            active_clusters: request.active_clusters.clone(),
        };
        let enabled_clusters = state
            .cluster_metadata
            .enabled_clusters()
            .map(|(name, _)| (name.clone(), ()))
            .collect::<BTreeMap<_, _>>();
        validate_replication_config(
            &replication,
            request.is_global_domain,
            &state.current_cluster_name,
            &enabled_clusters,
        )
        .map_err(DomainHandlerError::BadRequest)?;

        let configuration = DomainConfiguration {
            workflow_execution_retention_period_in_days: request
                .workflow_execution_retention_period_in_days,
            emit_metric: request.emit_metric.unwrap_or(true),
            history_archival_status: request.history_archival_status,
            history_archival_uri: request.history_archival_uri.clone(),
            visibility_archival_status: request.visibility_archival_status,
            visibility_archival_uri: request.visibility_archival_uri.clone(),
            ..Default::default()
        };
        let policy = state.domain_policy(&request.name);
        validate_domain_config(&configuration, policy.min_retention_days)
            .map_err(DomainHandlerError::BadRequest)?;

        let failover_version = if request.is_global_domain {
            state
                .cluster_metadata
                .next_failover_version(&active_cluster_name, 0, false)
                .map_err(|message| DomainHandlerError::BadRequest(bad(message)))?
        } else {
            cadence_persistence::version_history::EMPTY_VERSION
        };
        let info = DomainInfo {
            name: request.name.clone(),
            status: Some(DomainStatus::Registered),
            description: request.description.clone(),
            owner_email: request.owner_email.clone(),
            data: request.data.clone(),
            uuid: uuid::Uuid::now_v7().to_string(),
        };
        let response = state
            .domain_manager
            .create_with_version(
                info.clone(),
                configuration,
                replication,
                request.is_global_domain,
                failover_version,
            )
            .map_err(DomainHandlerError::from)?;
        state.executions.register_domain(&request.name, &info.uuid);
        Ok(response)
    }

    pub fn describe_domain(
        state: &cadence_service_core::handlers::AppState,
        request: Option<&DescribeDomainRequest>,
    ) -> Result<DescribeDomainResponse, DomainHandlerError> {
        let request = request.ok_or_else(|| {
            DomainHandlerError::BadRequest(cadence_api::validation::err_request_not_set())
        })?;
        if request.name.is_none() && request.uuid.is_none() {
            return Err(DomainHandlerError::BadRequest(
                cadence_api::validation::err_domain_not_set(),
            ));
        }
        state
            .domain_manager
            .describe(request)
            .map_err(DomainHandlerError::NotFound)
    }

    pub fn list_domains(
        state: &cadence_service_core::handlers::AppState,
        request: Option<&ListDomainsRequest>,
    ) -> Result<ListDomainsResponse, DomainHandlerError> {
        let request = request.ok_or_else(|| {
            DomainHandlerError::BadRequest(cadence_api::validation::err_request_not_set())
        })?;
        state
            .domain_manager
            .list_page(request)
            .map_err(DomainHandlerError::BadRequest)
    }

    pub fn update_domain(
        state: &cadence_service_core::handlers::AppState,
        request: Option<&UpdateDomainRequest>,
    ) -> Result<UpdateDomainResponse, DomainHandlerError> {
        let request = request.ok_or_else(|| {
            DomainHandlerError::BadRequest(cadence_api::validation::err_request_not_set())
        })?;
        if request.name.is_empty() {
            return Err(DomainHandlerError::BadRequest(
                cadence_api::validation::err_domain_not_set(),
            ));
        }
        state
            .domain_manager
            .update(
                request,
                &state.cluster_metadata,
                &state.domain_policy(&request.name),
            )
            .map_err(DomainHandlerError::from)
    }

    pub fn deprecate_domain(
        state: &cadence_service_core::handlers::AppState,
        request: Option<&DeprecateDomainRequest>,
    ) -> Result<(), DomainHandlerError> {
        let request = request.ok_or_else(|| {
            DomainHandlerError::BadRequest(cadence_api::validation::err_request_not_set())
        })?;
        if request.name.is_empty() {
            return Err(DomainHandlerError::BadRequest(
                cadence_api::validation::err_domain_not_set(),
            ));
        }
        let current = state
            .domain_manager
            .describe(&DescribeDomainRequest {
                name: Some(request.name.clone()),
                uuid: None,
            })
            .map_err(DomainHandlerError::NotFound)?;
        if current.is_global_domain && !state.cluster_metadata.is_primary_cluster() {
            return Err(DomainHandlerError::BadRequest(bad(
                "Cluster is not primary cluster, cannot do domain registration or domain update.",
            )));
        }
        state
            .domain_manager
            .deprecate(request)
            .map_err(DomainHandlerError::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainHandlerError {
    BadRequest(BadRequestError),
    AlreadyExists(DomainAlreadyExistsError),
    NotFound(EntityNotExistsError),
    Busy(ServiceBusyError),
    Internal(String),
}

impl From<cadence_persistence::FailoverError> for DomainHandlerError {
    fn from(error: cadence_persistence::FailoverError) -> Self {
        match error {
            cadence_persistence::FailoverError::NotFound(error) => Self::NotFound(error),
            cadence_persistence::FailoverError::Invalid(message) => Self::BadRequest(bad(message)),
            cadence_persistence::FailoverError::Busy(message) => {
                Self::Busy(ServiceBusyError::new(message))
            }
            cadence_persistence::FailoverError::Persistence(message) => Self::Internal(message),
        }
    }
}

impl From<cadence_persistence::FrontendDomainError> for DomainHandlerError {
    fn from(error: cadence_persistence::FrontendDomainError) -> Self {
        match error {
            cadence_persistence::FrontendDomainError::AlreadyExists(error) => {
                Self::AlreadyExists(error)
            }
            cadence_persistence::FrontendDomainError::NotFound(error) => Self::NotFound(error),
            cadence_persistence::FrontendDomainError::Invalid(message) => {
                Self::BadRequest(bad(message))
            }
            cadence_persistence::FrontendDomainError::Busy(message) => {
                Self::Busy(ServiceBusyError::new(message))
            }
            cadence_persistence::FrontendDomainError::Persistence(message) => {
                Self::Internal(message)
            }
        }
    }
}

// Domain validation and archival transitions from Go's common/domain package.

use cadence_api::{enums::ArchivalStatus, types::ActiveClusters};

fn bad(message: impl Into<String>) -> BadRequestError {
    BadRequestError::new(message)
}

pub fn validate_domain_name(name: &str) -> Result<(), BadRequestError> {
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        Ok(())
    } else {
        Err(bad(
            "Domain name can only include alphanumeric and dash characters.",
        ))
    }
}

/// Mirrors `AttrValidatorImpl.validateDomainConfig`.
pub fn validate_domain_config(
    config: &DomainConfiguration,
    min_retention_days: i32,
) -> Result<(), BadRequestError> {
    if config.workflow_execution_retention_period_in_days < min_retention_days {
        return Err(bad("A valid retention period is not set on request."));
    }
    if (config.history_archival_status == Some(ArchivalStatus::Enabled)
        && config.history_archival_uri.is_empty())
        || (config.visibility_archival_status == Some(ArchivalStatus::Enabled)
            && config.visibility_archival_uri.is_empty())
    {
        return Err(bad("Invalid to enable archival without specifying a uri."));
    }
    Ok(())
}

/// Validate replication configuration using Go's enabled-cluster set.
pub fn validate_replication_config(
    config: &DomainReplicationConfiguration,
    is_global_domain: bool,
    current_cluster: &str,
    enabled_clusters: &BTreeMap<String, ()>,
) -> Result<(), BadRequestError> {
    if !is_global_domain {
        validate_cluster(current_cluster, enabled_clusters)?;
        for cluster in &config.clusters {
            validate_cluster(&cluster.cluster_name, enabled_clusters)?;
        }
        if config.active_cluster_name != current_cluster {
            return Err(bad("Invalid local domain active cluster"));
        }
        if config.clusters.len() != 1
            || config.clusters[0].cluster_name != config.active_cluster_name
        {
            return Err(bad("Invalid local domain clusters"));
        }
        return Ok(());
    }
    if config.active_cluster_name.is_empty() {
        return Err(bad("ActiveClusterName is required for all global domains."));
    }
    for cluster in &config.clusters {
        validate_cluster(&cluster.cluster_name, enabled_clusters)?;
    }
    validate_cluster(&config.active_cluster_name, enabled_clusters)?;
    let names: BTreeSet<_> = config
        .clusters
        .iter()
        .map(|c| c.cluster_name.as_str())
        .collect();
    if !names.contains(config.active_cluster_name.as_str()) {
        return Err(bad("Active cluster is not contained in all clusters."));
    }
    validate_active_clusters(config.active_clusters.as_ref(), &names, enabled_clusters)
}

fn validate_cluster(name: &str, enabled: &BTreeMap<String, ()>) -> Result<(), BadRequestError> {
    if enabled.contains_key(name) {
        Ok(())
    } else {
        Err(bad(format!("Invalid cluster name: {name}")))
    }
}

fn validate_active_clusters(
    active: Option<&ActiveClusters>,
    domain_clusters: &BTreeSet<&str>,
    enabled: &BTreeMap<String, ()>,
) -> Result<(), BadRequestError> {
    let Some(scopes) = active.and_then(|a| a.active_clusters_by_cluster_attribute.as_ref()) else {
        return Ok(());
    };
    for scope in scopes.values() {
        for attr in scope
            .cluster_attributes
            .as_ref()
            .into_iter()
            .flat_map(|m| m.values())
        {
            validate_cluster(&attr.active_cluster_name, enabled).map_err(|_| {
                bad(format!(
                    "Invalid active cluster name: {}",
                    attr.active_cluster_name
                ))
            })?;
            if !domain_clusters.contains(attr.active_cluster_name.as_str()) {
                return Err(bad("Active cluster is not contained in all clusters."));
            }
            if attr.failover_version < 0 {
                return Err(bad(format!(
                    "invalid failover version: {}",
                    attr.failover_version
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_clusters_not_removed(
    old: &[String],
    new: &[String],
) -> Result<(), BadRequestError> {
    let new: BTreeSet<_> = new.iter().map(String::as_str).collect();
    if old.iter().any(|name| !new.contains(name.as_str())) {
        Err(bad(
            "Cannot remove existing replicated clusters from a domain.",
        ))
    } else {
        Ok(())
    }
}

/// The Go `UpdateDomainRequest.IsAFailoverRequest` predicate. A timeout is a
/// failover only when it is positive; the presence of an active cluster or a
/// non-empty active-active attribute scope also selects the failover path.
#[must_use]
pub fn is_failover_request(request: &UpdateDomainRequest) -> bool {
    request
        .replication_configuration
        .as_ref()
        .is_some_and(|replication| {
            replication.active_cluster_name.is_some()
                || replication
                    .active_clusters
                    .as_ref()
                    .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
                    .is_some_and(|scopes| !scopes.is_empty())
        })
        || request
            .failover_timeout_in_seconds
            .is_some_and(|timeout| timeout > 0)
}

/// Reject mixed configuration and failover requests before the failover path
/// can silently discard configuration fields. This is the guard in Go's
/// `handlerImpl.UpdateDomain`.
pub fn validate_update_request(request: &UpdateDomainRequest) -> Result<(), BadRequestError> {
    if request.is_config_update() && is_failover_request(request) {
        return Err(bad(
            "Cannot set active cluster to current cluster when other parameters are set.",
        ));
    }
    Ok(())
}

/// Local domains cannot alter replication configuration or request any kind
/// of failover. Go checks presence of these fields, including a zero or
/// negative timeout, independent of `IsAFailoverRequest`.
pub fn validate_local_domain_update(request: &UpdateDomainRequest) -> Result<(), BadRequestError> {
    let has_replication_update =
        request
            .replication_configuration
            .as_ref()
            .is_some_and(|replication| {
                replication.active_cluster_name.is_some()
                    || replication.active_clusters.is_some()
                    || replication.clusters.is_some()
            });
    if has_replication_update || request.failover_timeout_in_seconds.is_some() {
        return Err(bad(
            "Local domains cannot perform failovers or change replication configuration",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalState {
    pub status: ArchivalStatus,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalEvent {
    pub default_uri: String,
    pub uri: String,
    pub status: Option<ArchivalStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchivalTransitionError {
    InvalidState,
    InvalidEvent,
    CannotHandleStateChange,
    ImmutableUri,
    InvalidUri(BadRequestError),
}

impl std::fmt::Display for ArchivalTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState => f.write_str("Encountered illegal state: archival is enabled but URI is not set (should be impossible)"),
            Self::InvalidEvent => f.write_str("Encountered illegal event: default URI is not set (should be impossible)"),
            Self::CannotHandleStateChange => f.write_str("Encountered current state and event that cannot be handled (should be impossible)"),
            Self::ImmutableUri => f.write_str("Cannot update existing archival URI"),
            Self::InvalidUri(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for ArchivalTransitionError {}

impl ArchivalState {
    #[must_use]
    pub fn never_enabled() -> Self {
        Self {
            status: ArchivalStatus::Disabled,
            uri: String::new(),
        }
    }

    /// Apply Go's state table, then invoke the matching archiver's URI validator.
    pub fn next_state<F>(
        &self,
        event: &ArchivalEvent,
        validate_uri: F,
    ) -> Result<(Self, bool), ArchivalTransitionError>
    where
        F: FnOnce(&str) -> Result<(), BadRequestError>,
    {
        if self.status == ArchivalStatus::Enabled && self.uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidState);
        }
        if event.default_uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidEvent);
        }
        if !self.uri.is_empty() && !event.uri.is_empty() && self.uri != event.uri {
            return Err(ArchivalTransitionError::ImmutableUri);
        }

        let has_uri = !self.uri.is_empty();
        let event_uri = !event.uri.is_empty();
        let (next, changed) = match (self.status, has_uri, event.status, event_uri) {
            (ArchivalStatus::Enabled, true, Some(ArchivalStatus::Disabled), _) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: self.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, true, Some(ArchivalStatus::Enabled), _) => (
                Self {
                    status: ArchivalStatus::Enabled,
                    uri: self.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Enabled), _) => (
                Self {
                    status: ArchivalStatus::Enabled,
                    uri: if event_uri {
                        event.uri.clone()
                    } else {
                        event.default_uri.clone()
                    },
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), true) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: event.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), false)
            | (ArchivalStatus::Disabled, false, None, false) => (self.clone(), false),
            (ArchivalStatus::Disabled, false, None, true) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: event.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Enabled, true, _, _) | (ArchivalStatus::Disabled, true, _, _) => {
                (self.clone(), false)
            }
            _ => return Err(ArchivalTransitionError::CannotHandleStateChange),
        };
        if next.status == ArchivalStatus::Enabled && next.uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidState);
        }
        if !next.uri.is_empty() {
            validate_uri(&next.uri).map_err(ArchivalTransitionError::InvalidUri)?;
        }
        Ok((next, changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(status: Option<ArchivalStatus>, uri: &str) -> ArchivalEvent {
        ArchivalEvent {
            default_uri: "file:///default".into(),
            uri: uri.into(),
            status,
        }
    }

    #[test]
    fn domain_crud_methods_use_shared_store_and_preserve_go_error_order() {
        let state = cadence_service_core::handlers::AppState::default();
        let request = RegisterDomainRequest {
            name: "payments-prod".into(),
            workflow_execution_retention_period_in_days: 1,
            ..Default::default()
        };
        let created = Handler::register_domain(&state, Some(&request)).unwrap();
        assert_eq!(
            created.domain_info.as_ref().unwrap().status,
            Some(DomainStatus::Registered)
        );
        assert_eq!(
            Handler::describe_domain(
                &state,
                Some(&DescribeDomainRequest {
                    name: Some(request.name.clone()),
                    uuid: None,
                }),
            )
            .unwrap(),
            created
        );
        assert_eq!(
            Handler::register_domain(&state, Some(&request)).unwrap_err(),
            DomainHandlerError::AlreadyExists(DomainAlreadyExistsError::new(
                "Domain already exists."
            ))
        );
        assert_eq!(
            Handler::list_domains(&state, Some(&ListDomainsRequest::default()))
                .unwrap()
                .domains
                .len(),
            1
        );
        Handler::deprecate_domain(
            &state,
            Some(&DeprecateDomainRequest {
                name: request.name,
                ..Default::default()
            }),
        )
        .unwrap();
    }

    #[test]
    fn global_domain_registration_requires_primary_cluster_before_name_validation() {
        let state = cadence_service_core::handlers::AppState {
            current_cluster_name: "cluster1".into(),
            cluster_metadata: cadence_common::cluster::Metadata {
                current_cluster_name: "cluster1".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let request = RegisterDomainRequest {
            is_global_domain: true,
            name: "bad_name".into(),
            ..Default::default()
        };
        assert_eq!(
            Handler::register_domain(&state, Some(&request)).unwrap_err(),
            DomainHandlerError::BadRequest(bad(
                "Cluster is not primary cluster, cannot do domain registration or domain update.",
            ))
        );
    }

    #[test]
    fn validates_domain_name_and_retention() {
        assert!(validate_domain_name("workflow-prod").is_ok());
        assert_eq!(
            validate_domain_name("bad_name").unwrap_err().message,
            "Domain name can only include alphanumeric and dash characters."
        );
        assert_eq!(
            validate_domain_config(&DomainConfiguration::default(), 1)
                .unwrap_err()
                .message,
            "A valid retention period is not set on request."
        );
    }

    #[test]
    fn archival_enable_disable_preserves_immutable_uri() {
        let validate = |_: &str| Ok(());
        let (enabled, changed) = ArchivalState::never_enabled()
            .next_state(&event(Some(ArchivalStatus::Enabled), ""), validate)
            .unwrap();
        assert!(changed);
        assert_eq!(enabled.uri, "file:///default");
        let (disabled, changed) = enabled
            .next_state(&event(Some(ArchivalStatus::Disabled), ""), validate)
            .unwrap();
        assert!(changed);
        assert_eq!(disabled.uri, "file:///default");
        assert!(matches!(
            disabled.next_state(
                &event(Some(ArchivalStatus::Enabled), "file:///other"),
                validate
            ),
            Err(ArchivalTransitionError::ImmutableUri)
        ));
    }

    #[test]
    fn validates_unchanged_archival_uri() {
        let state = ArchivalState {
            status: ArchivalStatus::Disabled,
            uri: "file:///old".into(),
        };
        let error = state
            .next_state(&event(None, ""), |_| {
                Err(BadRequestError::new("invalid URI"))
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "invalid URI");
    }

    #[test]
    fn archival_table_preserves_noop_and_accepts_uri_only_event() {
        let initial = ArchivalState::never_enabled();
        let (same, changed) = initial
            .next_state(&event(Some(ArchivalStatus::Disabled), ""), |_| Ok(()))
            .unwrap();
        assert_eq!(same, initial);
        assert!(!changed);
        let (configured, changed) = initial
            .next_state(&event(None, "file:///custom"), |_| Ok(()))
            .unwrap();
        assert_eq!(configured.status, ArchivalStatus::Disabled);
        assert_eq!(configured.uri, "file:///custom");
        assert!(changed);
    }

    #[test]
    fn validates_global_cluster_membership_and_active_attribute_versions() {
        let mut active_by_attribute = std::collections::HashMap::new();
        active_by_attribute.insert(
            "region".to_string(),
            cadence_api::types::ClusterAttributeScope {
                cluster_attributes: Some(std::collections::HashMap::from([(
                    "us-east".to_string(),
                    cadence_api::types::ActiveClusterInfo {
                        active_cluster_name: "cluster-b".into(),
                        failover_version: -1,
                    },
                )])),
            },
        );
        let config = DomainReplicationConfiguration {
            active_cluster_name: "cluster-a".into(),
            clusters: vec![
                cadence_api::types::ClusterReplicationConfiguration {
                    cluster_name: "cluster-a".into(),
                },
                cadence_api::types::ClusterReplicationConfiguration {
                    cluster_name: "cluster-b".into(),
                },
            ],
            active_clusters: Some(ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: Some(active_by_attribute),
            }),
        };
        let enabled = BTreeMap::from([("cluster-a".into(), ()), ("cluster-b".into(), ())]);
        assert_eq!(
            validate_replication_config(&config, true, "cluster-a", &enabled)
                .unwrap_err()
                .message,
            "invalid failover version: -1"
        );
    }

    #[test]
    fn update_classification_and_local_domain_guards_match_go_presence_rules() {
        use cadence_api::service_types::{
            DomainReplicationConfigurationUpdate, UpdateDomainInfo, UpdateDomainRequest,
        };

        let zero_timeout = UpdateDomainRequest {
            failover_timeout_in_seconds: Some(0),
            ..Default::default()
        };
        assert!(!is_failover_request(&zero_timeout));
        assert!(validate_local_domain_update(&zero_timeout).is_err());

        let active_cluster_only = UpdateDomainRequest {
            replication_configuration: Some(DomainReplicationConfigurationUpdate {
                active_cluster_name: Some("cluster-a".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(is_failover_request(&active_cluster_only));
        assert_eq!(
            validate_local_domain_update(&active_cluster_only)
                .unwrap_err()
                .message,
            "Local domains cannot perform failovers or change replication configuration"
        );

        let mixed = UpdateDomainRequest {
            updated_info: Some(UpdateDomainInfo {
                description: Some("new description".into()),
                ..Default::default()
            }),
            replication_configuration: active_cluster_only.replication_configuration,
            ..Default::default()
        };
        assert_eq!(
            validate_update_request(&mixed).unwrap_err().message,
            "Cannot set active cluster to current cluster when other parameters are set."
        );

        let negative_timeout = UpdateDomainRequest {
            failover_timeout_in_seconds: Some(-1),
            ..Default::default()
        };
        assert!(!is_failover_request(&negative_timeout));
        assert!(validate_local_domain_update(&negative_timeout).is_err());
    }
}

// Domain validation and archival transitions from Go's common/domain package.

use std::collections::{BTreeMap, BTreeSet};

use cadence_api::{
    enums::ArchivalStatus,
    errors::BadRequestError,
    service_types::UpdateDomainRequest,
    types::{ActiveClusters, DomainConfiguration, DomainReplicationConfiguration},
};

fn bad(message: impl Into<String>) -> BadRequestError {
    BadRequestError::new(message)
}

pub fn validate_domain_name(name: &str) -> Result<(), BadRequestError> {
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        Ok(())
    } else {
        Err(bad(
            "Domain name can only include alphanumeric and dash characters.",
        ))
    }
}

/// Mirrors `AttrValidatorImpl.validateDomainConfig`.
pub fn validate_domain_config(
    config: &DomainConfiguration,
    min_retention_days: i32,
) -> Result<(), BadRequestError> {
    if config.workflow_execution_retention_period_in_days < min_retention_days {
        return Err(bad("A valid retention period is not set on request."));
    }
    if (config.history_archival_status == Some(ArchivalStatus::Enabled)
        && config.history_archival_uri.is_empty())
        || (config.visibility_archival_status == Some(ArchivalStatus::Enabled)
            && config.visibility_archival_uri.is_empty())
    {
        return Err(bad("Invalid to enable archival without specifying a uri."));
    }
    Ok(())
}

/// Validate replication configuration using Go's enabled-cluster set.
pub fn validate_replication_config(
    config: &DomainReplicationConfiguration,
    is_global_domain: bool,
    current_cluster: &str,
    enabled_clusters: &BTreeMap<String, ()>,
) -> Result<(), BadRequestError> {
    if !is_global_domain {
        validate_cluster(current_cluster, enabled_clusters)?;
        for cluster in &config.clusters {
            validate_cluster(&cluster.cluster_name, enabled_clusters)?;
        }
        if config.active_cluster_name != current_cluster {
            return Err(bad("Invalid local domain active cluster"));
        }
        if config.clusters.len() != 1
            || config.clusters[0].cluster_name != config.active_cluster_name
        {
            return Err(bad("Invalid local domain clusters"));
        }
        return Ok(());
    }
    if config.active_cluster_name.is_empty() {
        return Err(bad("ActiveClusterName is required for all global domains."));
    }
    for cluster in &config.clusters {
        validate_cluster(&cluster.cluster_name, enabled_clusters)?;
    }
    validate_cluster(&config.active_cluster_name, enabled_clusters)?;
    let names: BTreeSet<_> = config
        .clusters
        .iter()
        .map(|c| c.cluster_name.as_str())
        .collect();
    if !names.contains(config.active_cluster_name.as_str()) {
        return Err(bad("Active cluster is not contained in all clusters."));
    }
    validate_active_clusters(config.active_clusters.as_ref(), &names, enabled_clusters)
}

fn validate_cluster(name: &str, enabled: &BTreeMap<String, ()>) -> Result<(), BadRequestError> {
    if enabled.contains_key(name) {
        Ok(())
    } else {
        Err(bad(format!("Invalid cluster name: {name}")))
    }
}

fn validate_active_clusters(
    active: Option<&ActiveClusters>,
    domain_clusters: &BTreeSet<&str>,
    enabled: &BTreeMap<String, ()>,
) -> Result<(), BadRequestError> {
    let Some(scopes) = active.and_then(|a| a.active_clusters_by_cluster_attribute.as_ref()) else {
        return Ok(());
    };
    for scope in scopes.values() {
        for attr in scope
            .cluster_attributes
            .as_ref()
            .into_iter()
            .flat_map(|m| m.values())
        {
            validate_cluster(&attr.active_cluster_name, enabled).map_err(|_| {
                bad(format!(
                    "Invalid active cluster name: {}",
                    attr.active_cluster_name
                ))
            })?;
            if !domain_clusters.contains(attr.active_cluster_name.as_str()) {
                return Err(bad("Active cluster is not contained in all clusters."));
            }
            if attr.failover_version < 0 {
                return Err(bad(format!(
                    "invalid failover version: {}",
                    attr.failover_version
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_clusters_not_removed(
    old: &[String],
    new: &[String],
) -> Result<(), BadRequestError> {
    let new: BTreeSet<_> = new.iter().map(String::as_str).collect();
    if old.iter().any(|name| !new.contains(name.as_str())) {
        Err(bad(
            "Cannot remove existing replicated clusters from a domain.",
        ))
    } else {
        Ok(())
    }
}

/// The Go `UpdateDomainRequest.IsAFailoverRequest` predicate. A timeout is a
/// failover only when it is positive; the presence of an active cluster or a
/// non-empty active-active attribute scope also selects the failover path.
#[must_use]
pub fn is_failover_request(request: &UpdateDomainRequest) -> bool {
    request
        .replication_configuration
        .as_ref()
        .is_some_and(|replication| {
            replication.active_cluster_name.is_some()
                || replication
                    .active_clusters
                    .as_ref()
                    .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
                    .is_some_and(|scopes| !scopes.is_empty())
        })
        || request
            .failover_timeout_in_seconds
            .is_some_and(|timeout| timeout > 0)
}

/// Reject mixed configuration and failover requests before the failover path
/// can silently discard configuration fields. This is the guard in Go's
/// `handlerImpl.UpdateDomain`.
pub fn validate_update_request(request: &UpdateDomainRequest) -> Result<(), BadRequestError> {
    if request.is_config_update() && is_failover_request(request) {
        return Err(bad(
            "Cannot set active cluster to current cluster when other parameters are set.",
        ));
    }
    Ok(())
}

/// Local domains cannot alter replication configuration or request any kind
/// of failover. Go checks presence of these fields, including a zero or
/// negative timeout, independent of `IsAFailoverRequest`.
pub fn validate_local_domain_update(request: &UpdateDomainRequest) -> Result<(), BadRequestError> {
    let has_replication_update =
        request
            .replication_configuration
            .as_ref()
            .is_some_and(|replication| {
                replication.active_cluster_name.is_some()
                    || replication.active_clusters.is_some()
                    || replication.clusters.is_some()
            });
    if has_replication_update || request.failover_timeout_in_seconds.is_some() {
        return Err(bad(
            "Local domains cannot perform failovers or change replication configuration",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalState {
    pub status: ArchivalStatus,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalEvent {
    pub default_uri: String,
    pub uri: String,
    pub status: Option<ArchivalStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchivalTransitionError {
    InvalidState,
    InvalidEvent,
    CannotHandleStateChange,
    ImmutableUri,
    InvalidUri(BadRequestError),
}

impl std::fmt::Display for ArchivalTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState => f.write_str("Encountered illegal state: archival is enabled but URI is not set (should be impossible)"),
            Self::InvalidEvent => f.write_str("Encountered illegal event: default URI is not set (should be impossible)"),
            Self::CannotHandleStateChange => f.write_str("Encountered current state and event that cannot be handled (should be impossible)"),
            Self::ImmutableUri => f.write_str("Cannot update existing archival URI"),
            Self::InvalidUri(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for ArchivalTransitionError {}

impl ArchivalState {
    #[must_use]
    pub fn never_enabled() -> Self {
        Self {
            status: ArchivalStatus::Disabled,
            uri: String::new(),
        }
    }

    /// Apply Go's state table, then invoke the matching archiver's URI validator.
    pub fn next_state<F>(
        &self,
        event: &ArchivalEvent,
        validate_uri: F,
    ) -> Result<(Self, bool), ArchivalTransitionError>
    where
        F: FnOnce(&str) -> Result<(), BadRequestError>,
    {
        if self.status == ArchivalStatus::Enabled && self.uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidState);
        }
        if event.default_uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidEvent);
        }
        if !self.uri.is_empty() && !event.uri.is_empty() && self.uri != event.uri {
            return Err(ArchivalTransitionError::ImmutableUri);
        }

        let has_uri = !self.uri.is_empty();
        let event_uri = !event.uri.is_empty();
        let (next, changed) = match (self.status, has_uri, event.status, event_uri) {
            (ArchivalStatus::Enabled, true, Some(ArchivalStatus::Disabled), _) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: self.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, true, Some(ArchivalStatus::Enabled), _) => (
                Self {
                    status: ArchivalStatus::Enabled,
                    uri: self.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Enabled), _) => (
                Self {
                    status: ArchivalStatus::Enabled,
                    uri: if event_uri {
                        event.uri.clone()
                    } else {
                        event.default_uri.clone()
                    },
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), true) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: event.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), false)
            | (ArchivalStatus::Disabled, false, None, false) => (self.clone(), false),
            (ArchivalStatus::Disabled, false, None, true) => (
                Self {
                    status: ArchivalStatus::Disabled,
                    uri: event.uri.clone(),
                },
                true,
            ),
            (ArchivalStatus::Enabled, true, _, _) | (ArchivalStatus::Disabled, true, _, _) => {
                (self.clone(), false)
            }
            _ => return Err(ArchivalTransitionError::CannotHandleStateChange),
        };
        if next.status == ArchivalStatus::Enabled && next.uri.is_empty() {
            return Err(ArchivalTransitionError::InvalidState);
        }
        if !next.uri.is_empty() {
            validate_uri(&next.uri).map_err(ArchivalTransitionError::InvalidUri)?;
        }
        Ok((next, changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(status: Option<ArchivalStatus>, uri: &str) -> ArchivalEvent {
        ArchivalEvent {
            default_uri: "file:///default".into(),
            uri: uri.into(),
            status,
        }
    }

    #[test]
    fn validates_domain_name_and_retention() {
        assert!(validate_domain_name("workflow-prod").is_ok());
        assert_eq!(
            validate_domain_name("bad_name").unwrap_err().message,
            "Domain name can only include alphanumeric and dash characters."
        );
        assert_eq!(
            validate_domain_config(&DomainConfiguration::default(), 1)
                .unwrap_err()
                .message,
            "A valid retention period is not set on request."
        );
    }

    #[test]
    fn archival_enable_disable_preserves_immutable_uri() {
        let validate = |_: &str| Ok(());
        let (enabled, changed) = ArchivalState::never_enabled()
            .next_state(&event(Some(ArchivalStatus::Enabled), ""), validate)
            .unwrap();
        assert!(changed);
        assert_eq!(enabled.uri, "file:///default");
        let (disabled, changed) = enabled
            .next_state(&event(Some(ArchivalStatus::Disabled), ""), validate)
            .unwrap();
        assert!(changed);
        assert_eq!(disabled.uri, "file:///default");
        assert!(matches!(
            disabled.next_state(
                &event(Some(ArchivalStatus::Enabled), "file:///other"),
                validate
            ),
            Err(ArchivalTransitionError::ImmutableUri)
        ));
    }

    #[test]
    fn validates_unchanged_archival_uri() {
        let state = ArchivalState {
            status: ArchivalStatus::Disabled,
            uri: "file:///old".into(),
        };
        let error = state
            .next_state(&event(None, ""), |_| {
                Err(BadRequestError::new("invalid URI"))
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "invalid URI");
    }

    #[test]
    fn archival_table_preserves_noop_and_accepts_uri_only_event() {
        let initial = ArchivalState::never_enabled();
        let (same, changed) = initial
            .next_state(&event(Some(ArchivalStatus::Disabled), ""), |_| Ok(()))
            .unwrap();
        assert_eq!(same, initial);
        assert!(!changed);
        let (configured, changed) = initial
            .next_state(&event(None, "file:///custom"), |_| Ok(()))
            .unwrap();
        assert_eq!(configured.status, ArchivalStatus::Disabled);
        assert_eq!(configured.uri, "file:///custom");
        assert!(changed);
    }

    #[test]
    fn validates_global_cluster_membership_and_active_attribute_versions() {
        let mut active_by_attribute = std::collections::HashMap::new();
        active_by_attribute.insert(
            "region".to_string(),
            cadence_api::types::ClusterAttributeScope {
                cluster_attributes: Some(std::collections::HashMap::from([(
                    "us-east".to_string(),
                    cadence_api::types::ActiveClusterInfo {
                        active_cluster_name: "cluster-b".into(),
                        failover_version: -1,
                    },
                )])),
            },
        );
        let config = DomainReplicationConfiguration {
            active_cluster_name: "cluster-a".into(),
            clusters: vec![
                cadence_api::types::ClusterReplicationConfiguration {
                    cluster_name: "cluster-a".into(),
                },
                cadence_api::types::ClusterReplicationConfiguration {
                    cluster_name: "cluster-b".into(),
                },
            ],
            active_clusters: Some(ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: Some(active_by_attribute),
            }),
        };
        let enabled = BTreeMap::from([("cluster-a".into(), ()), ("cluster-b".into(), ())]);
        assert_eq!(
            validate_replication_config(&config, true, "cluster-a", &enabled)
                .unwrap_err()
                .message,
            "invalid failover version: -1"
        );
    }

    #[test]
    fn update_classification_and_local_domain_guards_match_go_presence_rules() {
        use cadence_api::service_types::{
            DomainReplicationConfigurationUpdate, UpdateDomainInfo, UpdateDomainRequest,
        };

        let zero_timeout = UpdateDomainRequest {
            failover_timeout_in_seconds: Some(0),
            ..Default::default()
        };
        assert!(!is_failover_request(&zero_timeout));
        assert!(validate_local_domain_update(&zero_timeout).is_err());

        let active_cluster_only = UpdateDomainRequest {
            replication_configuration: Some(DomainReplicationConfigurationUpdate {
                active_cluster_name: Some("cluster-a".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(is_failover_request(&active_cluster_only));
        assert_eq!(
            validate_local_domain_update(&active_cluster_only)
                .unwrap_err()
                .message,
            "Local domains cannot perform failovers or change replication configuration"
        );

        let mixed = UpdateDomainRequest {
            updated_info: Some(UpdateDomainInfo {
                description: Some("new description".into()),
                ..Default::default()
            }),
            replication_configuration: active_cluster_only.replication_configuration,
            ..Default::default()
        };
        assert_eq!(
            validate_update_request(&mixed).unwrap_err().message,
            "Cannot set active cluster to current cluster when other parameters are set."
        );

        let negative_timeout = UpdateDomainRequest {
            failover_timeout_in_seconds: Some(-1),
            ..Default::default()
        };
        assert!(!is_failover_request(&negative_timeout));
        assert!(validate_local_domain_update(&negative_timeout).is_err());
    }
}
