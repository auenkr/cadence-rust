//! Domain helpers ported from `go/common/types/shared.go`:
//! `IsActiveActive`, `GetAllClusters`, `GetFailoverVersionForAttribute`,
//! `IsAConfigUpdateRequest`, `DeepCopy` semantics.

use crate::errors::ClusterAttributeNotFoundError;
use crate::types::*;

impl DomainReplicationConfiguration {
    /// Mirrors Go `IsActiveActive`.
    #[must_use]
    pub fn is_active_active(&self) -> bool {
        match &self.active_clusters {
            None => false,
            Some(ac) => match &ac.active_clusters_by_cluster_attribute {
                None => false,
                Some(scopes) => !scopes.is_empty(),
            },
        }
    }
}

impl ActiveClusters {
    /// Mirrors Go `GetAllClusters`: deduplicated, sorted attribute names.
    #[must_use]
    pub fn get_all_clusters(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .active_clusters_by_cluster_attribute
            .as_ref()
            .map(|scopes| {
                scopes
                    .values()
                    .flat_map(|scope| {
                        scope
                            .cluster_attributes
                            .as_ref()
                            .map(|m| m.keys().cloned().collect::<Vec<_>>())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out.dedup();
        out
    }

    /// Mirrors Go `GetFailoverVersionForAttribute`.
    pub fn get_failover_version_for_attribute(
        &self,
        scope_type: &str,
        attribute_name: &str,
    ) -> Result<i64, ClusterAttributeNotFoundError> {
        let not_found = || ClusterAttributeNotFoundError {
            scope_type: scope_type.to_string(),
            attribute_name: attribute_name.to_string(),
        };
        let scopes = self.active_clusters_by_cluster_attribute.as_ref().ok_or_else(not_found)?;
        let scope = scopes.get(scope_type).ok_or_else(not_found)?;
        let attrs = scope.cluster_attributes.as_ref().ok_or_else(not_found)?;
        attrs
            .get(attribute_name)
            .map(|a| a.failover_version)
            .ok_or_else(not_found)
    }

    /// Deep copy (Clone already deep-copies; explicit for API parity with Go).
    #[must_use]
    pub fn deep_copy(&self) -> Self {
        self.clone()
    }
}

impl DataBlob {
    /// Byte-size estimate mirroring Go `ByteSize` (struct + payload).
    #[must_use]
    pub fn byte_size(&self) -> u64 {
        (std::mem::size_of::<Self>() + self.data.len()) as u64
    }
}

/// Mirrors Go `(v *UpdateDomainRequest) IsAConfigUpdateRequest`.
///
/// The Go source checks Description/OwnerEmail/Data/EmitMetric/retention/
/// BadBinaries/archival fields/DeleteBadBinary; failover-only fields
/// (ActiveClusterName, Clusters) do not count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateDomainRequest {
    pub description: Option<String>,
    pub owner_email: Option<String>,
    pub data: Option<std::collections::HashMap<String, String>>,
    pub emit_metric: Option<bool>,
    pub workflow_execution_retention_period_in_days: Option<i32>,
    pub bad_binaries: Option<BadBinaries>,
    pub history_archival_status: Option<crate::enums::ArchivalStatus>,
    pub history_archival_uri: Option<String>,
    pub visibility_archival_status: Option<crate::enums::ArchivalStatus>,
    pub visibility_archival_uri: Option<String>,
    pub delete_bad_binary: Option<String>,
    // Failover-only fields (do not count as config updates):
    pub active_cluster_name: Option<String>,
}

impl UpdateDomainRequest {
    #[must_use]
    pub fn is_config_update_request(&self) -> bool {
        self.description.is_some()
            || self.owner_email.is_some()
            || self.data.is_some()
            || self.emit_metric.is_some()
            || self.workflow_execution_retention_period_in_days.is_some()
            || self.bad_binaries.is_some()
            || self.history_archival_status.is_some()
            || self.history_archival_uri.is_some()
            || self.visibility_archival_status.is_some()
            || self.visibility_archival_uri.is_some()
            || self.delete_bad_binary.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_active_active_matches_go_table() {
        assert!(!DomainReplicationConfiguration::default().is_active_active());
        let cfg = DomainReplicationConfiguration {
            active_clusters: Some(ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: None,
            }),
            ..Default::default()
        };
        assert!(!cfg.is_active_active());
        let cfg = DomainReplicationConfiguration {
            active_clusters: Some(ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: Some(Default::default()),
            }),
            ..Default::default()
        };
        assert!(!cfg.is_active_active());
    }

    #[test]
    fn get_all_clusters_sorted_dedup() {
        let ac = ActiveClusters {
            active_clusters_by_region: None,
            active_clusters_by_cluster_attribute: Some(
                [(
                    "region".to_string(),
                    ClusterAttributeScope {
                        cluster_attributes: Some(
                            ["us-west-1", "us-east-1", "eu-west-1"]
                                .into_iter()
                                .map(|n| (n.to_string(), ActiveClusterInfo::default()))
                                .collect(),
                        ),
                    },
                )]
                .into_iter()
                .collect(),
            ),
        };
        assert_eq!(
            ac.get_all_clusters(),
            vec!["eu-west-1", "us-east-1", "us-west-1"]
        );
    }

    #[test]
    fn failover_version_lookup_and_missing() {
        let ac = ActiveClusters {
            active_clusters_by_region: None,
            active_clusters_by_cluster_attribute: Some(
                [(
                    "region".to_string(),
                    ClusterAttributeScope {
                        cluster_attributes: Some(
                            [(
                                "us-east-1".to_string(),
                                ActiveClusterInfo {
                                    active_cluster_name: "cluster1".into(),
                                    failover_version: 100,
                                },
                            )]
                            .into_iter()
                            .collect(),
                        ),
                    },
                )]
                .into_iter()
                .collect(),
            ),
        };
        assert_eq!(
            ac.get_failover_version_for_attribute("region", "us-east-1")
                .unwrap(),
            100
        );
        assert!(ac
            .get_failover_version_for_attribute("region", "missing")
            .is_err());
        assert!(ActiveClusters::default()
            .get_failover_version_for_attribute("r", "a")
            .is_err());
    }

    #[test]
    fn config_update_request_matrix() {
        assert!(!UpdateDomainRequest::default().is_config_update_request());
        assert!(!UpdateDomainRequest {
            active_cluster_name: Some("cluster-b".into()),
            ..Default::default()
        }
        .is_config_update_request());
        assert!(UpdateDomainRequest {
            description: Some("new desc".into()),
            ..Default::default()
        }
        .is_config_update_request());
        assert!(UpdateDomainRequest {
            data: Some([("k".to_string(), "v".to_string())].into_iter().collect()),
            ..Default::default()
        }
        .is_config_update_request());
    }
}
