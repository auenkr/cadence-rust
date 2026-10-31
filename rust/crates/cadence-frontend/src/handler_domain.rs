//! Domain API handler boundary shared by Workflow and Admin APIs.

use cadence_api::errors::InternalServiceError;

#[derive(Debug, Default)]
pub struct Handler;

impl Handler {
    pub fn dispatch(&self, operation: &str) -> Result<(), InternalServiceError> {
        Err(crate::handler_workflow::unimplemented_operation(operation))
    }
}

// Domain validation and archival transitions from Go's common/domain package.

use std::collections::{BTreeMap, BTreeSet};

use cadence_api::{
    enums::ArchivalStatus,
    errors::BadRequestError,
    types::{ActiveClusters, DomainConfiguration, DomainReplicationConfiguration},
};

fn bad(message: impl Into<String>) -> BadRequestError {
    BadRequestError::new(message)
}

pub fn validate_domain_name(name: &str) -> Result<(), BadRequestError> {
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        Ok(())
    } else {
        Err(bad("Domain name can only include alphanumeric and dash characters."))
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
    let names: BTreeSet<_> = config.clusters.iter().map(|c| c.cluster_name.as_str()).collect();
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
        for attr in scope.cluster_attributes.as_ref().into_iter().flat_map(|m| m.values()) {
            validate_cluster(&attr.active_cluster_name, enabled).map_err(|_| {
                bad(format!("Invalid active cluster name: {}", attr.active_cluster_name))
            })?;
            if !domain_clusters.contains(attr.active_cluster_name.as_str()) {
                return Err(bad("Active cluster is not contained in all clusters."));
            }
            if attr.failover_version < 0 {
                return Err(bad(format!("invalid failover version: {}", attr.failover_version)));
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
        Err(bad("Cannot remove existing replicated clusters from a domain."))
    } else {
        Ok(())
    }
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
        Self { status: ArchivalStatus::Disabled, uri: String::new() }
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
            (ArchivalStatus::Enabled, true, Some(ArchivalStatus::Disabled), _) =>
                (Self { status: ArchivalStatus::Disabled, uri: self.uri.clone() }, true),
            (ArchivalStatus::Disabled, true, Some(ArchivalStatus::Enabled), _) =>
                (Self { status: ArchivalStatus::Enabled, uri: self.uri.clone() }, true),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Enabled), _) => (Self {
                status: ArchivalStatus::Enabled,
                uri: if event_uri { event.uri.clone() } else { event.default_uri.clone() },
            }, true),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), true) =>
                (Self { status: ArchivalStatus::Disabled, uri: event.uri.clone() }, true),
            (ArchivalStatus::Disabled, false, Some(ArchivalStatus::Disabled), false)
            | (ArchivalStatus::Disabled, false, None, false) => (self.clone(), false),
            (ArchivalStatus::Disabled, false, None, true) =>
                (Self { status: ArchivalStatus::Disabled, uri: event.uri.clone() }, true),
            (ArchivalStatus::Enabled, true, _, _) | (ArchivalStatus::Disabled, true, _, _) =>
                (self.clone(), false),
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
        ArchivalEvent { default_uri: "file:///default".into(), uri: uri.into(), status }
    }

    #[test]
    fn validates_domain_name_and_retention() {
        assert!(validate_domain_name("workflow-prod").is_ok());
        assert_eq!(validate_domain_name("bad_name").unwrap_err().message,
            "Domain name can only include alphanumeric and dash characters.");
        assert_eq!(validate_domain_config(&DomainConfiguration::default(), 1).unwrap_err().message,
            "A valid retention period is not set on request.");
    }

    #[test]
    fn archival_enable_disable_preserves_immutable_uri() {
        let validate = |_: &str| Ok(());
        let (enabled, changed) = ArchivalState::never_enabled()
            .next_state(&event(Some(ArchivalStatus::Enabled), ""), validate).unwrap();
        assert!(changed);
        assert_eq!(enabled.uri, "file:///default");
        let (disabled, changed) = enabled
            .next_state(&event(Some(ArchivalStatus::Disabled), ""), validate).unwrap();
        assert!(changed);
        assert_eq!(disabled.uri, "file:///default");
        assert!(matches!(disabled.next_state(&event(Some(ArchivalStatus::Enabled), "file:///other"), validate),
            Err(ArchivalTransitionError::ImmutableUri)));
    }

    #[test]
    fn validates_unchanged_archival_uri() {
        let state = ArchivalState { status: ArchivalStatus::Disabled, uri: "file:///old".into() };
        let error = state.next_state(&event(None, ""), |_| Err(BadRequestError::new("invalid URI"))).unwrap_err();
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
                cadence_api::types::ClusterReplicationConfiguration { cluster_name: "cluster-a".into() },
                cadence_api::types::ClusterReplicationConfiguration { cluster_name: "cluster-b".into() },
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
