//! V2 failover planning from Go `failovermanager/failover_workflow_v2.go`.
//!
//! The workflow body, the domain listing activity and the batch execution need
//! the SDK and frontend clients and are not ported yet. The parameter defaults
//! and validation, and the per-domain preference planner, are ported here.

use crate::failovermanager::domain::{ClusterAttribute, DomainView};
use crate::failovermanager::shared::{ClusterAttributePreference, DomainFailoverPreferences};

/// Go `defaultBatchSizeV2`.
pub const DEFAULT_BATCH_SIZE_V2: i64 = 20;
/// Go `defaultWaitBetweenBatchSecondsV2`.
pub const DEFAULT_WAIT_BETWEEN_BATCH_SECONDS_V2: i64 = 30;
/// Go `errMsgV2SourceClusterEmpty`.
pub const ERR_SOURCE_CLUSTER_EMPTY: &str = "sourceClusters is empty";
/// Go `errMsgV2TargetClusterEmpty`.
pub const ERR_TARGET_CLUSTER_EMPTY: &str = "targetCluster is empty";
/// Go `errMsgV2SameCluster`.
pub const ERR_SAME_CLUSTER: &str = "targetCluster is also listed as a sourceCluster";

/// Go `FailoverV2Params`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FailoverV2Params {
    pub source_clusters: Vec<String>,
    pub target_cluster: String,
    pub batch_size: i64,
    pub wait_between_batch_seconds: i64,
    pub domains: Vec<String>,
    pub cluster_attributes: Vec<ClusterAttribute>,
    pub skip_destination_cluster_check: bool,
}

impl FailoverV2Params {
    /// Go `validateFailoverV2Params`. Fills non-positive batch settings with the
    /// defaults first, then checks the cluster lists.
    pub fn validate(&mut self) -> Result<(), &'static str> {
        if self.batch_size <= 0 {
            self.batch_size = DEFAULT_BATCH_SIZE_V2;
        }
        if self.wait_between_batch_seconds <= 0 {
            self.wait_between_batch_seconds = DEFAULT_WAIT_BETWEEN_BATCH_SECONDS_V2;
        }
        if self.source_clusters.is_empty() {
            return Err(ERR_SOURCE_CLUSTER_EMPTY);
        }
        if self.target_cluster.is_empty() {
            return Err(ERR_TARGET_CLUSTER_EMPTY);
        }
        for source in &self.source_clusters {
            if source.is_empty() {
                return Err(ERR_SOURCE_CLUSTER_EMPTY);
            }
            if *source == self.target_cluster {
                return Err(ERR_SAME_CLUSTER);
            }
        }
        Ok(())
    }
}

/// Go `DomainSnapshot`: the pre-failover state needed to restore a domain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DomainSnapshot {
    pub domain_name: String,
    /// Empty when the domain-level active cluster did not move.
    pub previous_active_cluster: String,
    pub previous_cluster_attributes: Vec<ClusterAttributePreference>,
}

/// Go `failoverPreferencesForDomain`. Moves the domain-level active cluster
/// only when it sits on a source cluster. Moves each filtered attribute that
/// sits on a source cluster. Returns `None` when neither changes, which means
/// the domain is skipped.
pub fn failover_preferences_for_domain(
    domain: &DomainView,
    source_clusters: &[String],
    target_cluster: &str,
    cluster_attribute_filter: &[ClusterAttribute],
) -> Option<(DomainFailoverPreferences, DomainSnapshot)> {
    let mut prefs = DomainFailoverPreferences {
        domain_name: domain.name.clone(),
        ..Default::default()
    };
    let mut snapshot = DomainSnapshot {
        domain_name: domain.name.clone(),
        ..Default::default()
    };

    if source_clusters.contains(&domain.active_cluster_name) {
        prefs.target_cluster = target_cluster.to_owned();
        snapshot.previous_active_cluster = domain.active_cluster_name.clone();
    }

    if !cluster_attribute_filter.is_empty() {
        if let Some(scopes) = &domain.active_clusters {
            for (scope, attributes) in scopes {
                for (name, active_cluster) in attributes {
                    let attribute = ClusterAttribute {
                        scope: scope.clone(),
                        name: name.clone(),
                    };
                    if !cluster_attribute_filter.contains(&attribute)
                        || !source_clusters.contains(active_cluster)
                    {
                        continue;
                    }
                    prefs.cluster_attribute_updates.push(ClusterAttributePreference {
                        scope: scope.clone(),
                        name: name.clone(),
                        preferred_cluster: target_cluster.to_owned(),
                    });
                    snapshot.previous_cluster_attributes.push(ClusterAttributePreference {
                        scope: scope.clone(),
                        name: name.clone(),
                        preferred_cluster: active_cluster.clone(),
                    });
                }
            }
        }
    }

    if prefs.target_cluster.is_empty() && prefs.cluster_attribute_updates.is_empty() {
        return None;
    }
    prefs.failover_timeout_seconds = domain.failover_timeout_seconds();
    Some((prefs, snapshot))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::failovermanager::shared::DOMAIN_DATA_KEY_FOR_FAILOVER_TIMEOUT_SECONDS;

    fn attribute(scope: &str, name: &str) -> ClusterAttribute {
        ClusterAttribute {
            scope: scope.into(),
            name: name.into(),
        }
    }

    /// The domain-level active cluster is `x`, which is not a source, so only
    /// the attribute `cluster:c0` (active on `attribute_active`) can move.
    fn domain_with_attributes(attribute_active: &str) -> DomainView {
        DomainView {
            name: "d".into(),
            is_global: true,
            active_cluster_name: "x".into(),
            active_clusters: Some(BTreeMap::from([(
                "cluster".to_owned(),
                BTreeMap::from([("c0".to_owned(), attribute_active.to_owned())]),
            )])),
            ..Default::default()
        }
    }

    #[test]
    fn defaults_and_validation_follow_go_order() {
        let mut params = FailoverV2Params {
            source_clusters: vec!["a".into()],
            target_cluster: "b".into(),
            ..Default::default()
        };
        params.validate().unwrap();
        assert_eq!(params.batch_size, 20);
        assert_eq!(params.wait_between_batch_seconds, 30);

        let mut empty_sources = FailoverV2Params {
            target_cluster: "b".into(),
            ..Default::default()
        };
        assert_eq!(empty_sources.validate(), Err(ERR_SOURCE_CLUSTER_EMPTY));

        let mut no_target = FailoverV2Params {
            source_clusters: vec!["a".into()],
            ..Default::default()
        };
        assert_eq!(no_target.validate(), Err(ERR_TARGET_CLUSTER_EMPTY));

        let mut blank_source = FailoverV2Params {
            source_clusters: vec!["".into()],
            target_cluster: "b".into(),
            ..Default::default()
        };
        assert_eq!(blank_source.validate(), Err(ERR_SOURCE_CLUSTER_EMPTY));

        let mut same = FailoverV2Params {
            source_clusters: vec!["a".into(), "b".into()],
            target_cluster: "b".into(),
            ..Default::default()
        };
        assert_eq!(same.validate(), Err(ERR_SAME_CLUSTER));
    }

    #[test]
    fn moves_domain_level_cluster_only_from_a_source() {
        let domain = DomainView {
            name: "d".into(),
            active_cluster_name: "a".into(),
            ..Default::default()
        };
        let (prefs, snapshot) =
            failover_preferences_for_domain(&domain, &["a".into()], "b", &[]).unwrap();
        assert_eq!(prefs.target_cluster, "b");
        assert_eq!(snapshot.previous_active_cluster, "a");
        assert!(failover_preferences_for_domain(&domain, &["x".into()], "b", &[]).is_none());
    }

    #[test]
    fn moves_filtered_attributes_and_records_previous_clusters() {
        let domain = domain_with_attributes("a");
        let (prefs, snapshot) = failover_preferences_for_domain(
            &domain,
            &["a".into()],
            "b",
            &[attribute("cluster", "c0")],
        )
        .unwrap();
        assert_eq!(prefs.target_cluster, "");
        assert_eq!(
            prefs.cluster_attribute_updates,
            vec![ClusterAttributePreference {
                scope: "cluster".into(),
                name: "c0".into(),
                preferred_cluster: "b".into(),
            }]
        );
        assert_eq!(snapshot.previous_cluster_attributes[0].preferred_cluster, "a");
        assert!(snapshot.previous_active_cluster.is_empty());

        // An attribute outside the filter is left alone.
        assert!(failover_preferences_for_domain(
            &domain,
            &["a".into()],
            "b",
            &[attribute("region", "us")],
        )
        .is_none());
    }

    #[test]
    fn timeout_comes_from_domain_data_and_defaults_to_force() {
        let mut domain = DomainView {
            name: "d".into(),
            active_cluster_name: "a".into(),
            ..Default::default()
        };
        domain
            .data
            .insert(DOMAIN_DATA_KEY_FOR_FAILOVER_TIMEOUT_SECONDS.into(), "300".into());
        let (prefs, _) = failover_preferences_for_domain(&domain, &["a".into()], "b", &[]).unwrap();
        assert_eq!(prefs.failover_timeout_seconds, 300);

        domain.data.clear();
        let (prefs, _) = failover_preferences_for_domain(&domain, &["a".into()], "b", &[]).unwrap();
        assert_eq!(prefs.failover_timeout_seconds, 0);
    }
}
