//! The domain fields failover and rebalance planning read (Go
//! `types.DescribeDomainResponse` subset).

use std::collections::BTreeMap;

use crate::failovermanager::shared::{
    failover_timeout_seconds, is_domain_failover_managed_by_cadence,
};

/// Go `constants.DomainDataKeyForPreferredCluster`.
pub const DOMAIN_DATA_KEY_FOR_PREFERRED_CLUSTER: &str = "PreferredCluster";
/// Go `constants.DomainDataKeyForClusterAttributePreferences`.
pub const DOMAIN_DATA_KEY_FOR_CLUSTER_ATTRIBUTE_PREFERENCES: &str = "ClusterAttributePreferences";

/// Go `types.DomainStatus` values that planning distinguishes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DomainStatus {
    #[default]
    Registered,
    Deprecated,
    Deleted,
}

/// One scope:name cluster attribute, as used by the Go `types.ClusterAttribute`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterAttribute {
    pub scope: String,
    pub name: String,
}

/// Domain state read by failover and rebalance planning.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DomainView {
    pub name: String,
    pub status: DomainStatus,
    pub is_global: bool,
    pub data: BTreeMap<String, String>,
    pub active_cluster_name: String,
    /// Names of the clusters in the domain's replication configuration.
    pub clusters: Vec<String>,
    /// Scope → attribute name → active cluster. `None` means the domain has no
    /// active-active configuration.
    pub active_clusters: Option<BTreeMap<String, BTreeMap<String, String>>>,
}

impl DomainView {
    /// Go `isDomainFailoverManagedByCadence`.
    pub fn managed_by_cadence(&self) -> bool {
        is_domain_failover_managed_by_cadence(&self.data)
    }

    /// Go `getPreferredClusterName`. Empty when unset.
    pub fn preferred_cluster(&self) -> &str {
        self.data_value(DOMAIN_DATA_KEY_FOR_PREFERRED_CLUSTER)
    }

    /// Go `getFailoverTimeoutSeconds` lookup from domain data.
    pub fn failover_timeout_seconds(&self) -> i32 {
        failover_timeout_seconds(self.data_value(
            crate::failovermanager::shared::DOMAIN_DATA_KEY_FOR_FAILOVER_TIMEOUT_SECONDS,
        ))
    }

    /// Go `isPreferredClusterInClusterListForDomain`.
    pub fn has_cluster(&self, cluster: &str) -> bool {
        self.clusters.iter().any(|name| name == cluster)
    }

    /// Active cluster of a scope:name attribute, or `None` when the attribute is
    /// not configured (Go `GetActiveClusterByClusterAttribute` errors).
    pub fn attribute_active_cluster(&self, scope: &str, name: &str) -> Option<&str> {
        if scope.is_empty() || name.is_empty() {
            return None;
        }
        self.active_clusters
            .as_ref()?
            .get(scope)?
            .get(name)
            .map(String::as_str)
    }

    /// Raw domain-data value, or `""` when absent (Go map lookup on a nil map).
    pub fn data_value(&self, key: &str) -> &str {
        self.data.get(key).map_or("", String::as_str)
    }
}
