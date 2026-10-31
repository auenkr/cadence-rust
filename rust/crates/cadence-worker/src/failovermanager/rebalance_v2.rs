//! V2 rebalance planning from Go `failovermanager/rebalance_workflow_v2.go` and
//! the rebalance helpers in `rebalance_workflow.go`.
//!
//! The workflow body and the domain listing activity need the SDK and frontend
//! clients and are not ported yet. The per-domain planner and its helpers are
//! ported here.

use std::collections::BTreeMap;

use crate::failovermanager::domain::{
    DomainStatus, DomainView, DOMAIN_DATA_KEY_FOR_CLUSTER_ATTRIBUTE_PREFERENCES,
};
use crate::failovermanager::shared::{
    is_eligible_for_failover, ClusterAttributePreference, DomainFailoverPreferences,
};

/// Go `errors.New("domain is not eligible for rebalance")`.
pub const ERR_NOT_ELIGIBLE: &str = "domain is not eligible for rebalance";
/// Go `errNoRebalanceRequiredV2`.
pub const ERR_NO_REBALANCE_REQUIRED: &str = "no rebalance required";

/// Why a domain produces no rebalance preference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebalanceSkip {
    NotEligible,
    NoRebalanceRequired,
}

/// Go `getClusterAttributePreferences`'s decoding step. An empty value and JSON
/// `null` both give no preferences. Malformed JSON is an error.
pub fn cluster_attribute_preferences(
    raw: &str,
) -> Result<Vec<ClusterAttributePreference>, serde_json::Error> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_str::<Option<Vec<ClusterAttributePreference>>>(raw)?.unwrap_or_default())
}

/// Go `shouldAllowRebalance`: a managed, global, registered domain whose
/// preferred cluster differs from its active cluster and is one of its clusters.
pub fn should_allow_rebalance(domain: &DomainView) -> bool {
    let preferred = domain.preferred_cluster();
    domain.managed_by_cadence()
        && domain.is_global
        && domain.status == DomainStatus::Registered
        && !preferred.is_empty()
        && preferred != domain.active_cluster_name
        && domain.has_cluster(preferred)
}

/// Go `getDomainsByCluster`: domain names grouped by preferred cluster, in
/// input order within each group.
pub fn domains_by_cluster(entries: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (domain_name, preferred_cluster) in entries {
        grouped
            .entry(preferred_cluster.clone())
            .or_default()
            .push(domain_name.clone());
    }
    grouped
}

/// Go `clusterAttributeUpdatesNeeded`. Returns the preferences whose live active
/// cluster differs. Attributes the domain does not configure are skipped.
pub fn cluster_attribute_updates_needed(
    domain: &DomainView,
    preferences: &[ClusterAttributePreference],
) -> Vec<ClusterAttributePreference> {
    if domain.active_clusters.is_none() {
        return Vec::new();
    }
    preferences
        .iter()
        .filter(|pref| {
            domain
                .attribute_active_cluster(&pref.scope, &pref.name)
                .is_some_and(|active| active != pref.preferred_cluster)
        })
        .cloned()
        .collect()
}

/// Go `rebalancePreferencesForDomain`. Returns the domain-level target and the
/// attribute updates. A malformed `ClusterAttributePreferences` value is ignored,
/// as Go ignores its decode error.
pub fn rebalance_preferences_for_domain(
    domain: &DomainView,
) -> Result<DomainFailoverPreferences, RebalanceSkip> {
    if !is_eligible_for_failover(Some(domain)) {
        return Err(RebalanceSkip::NotEligible);
    }

    let mut prefs = DomainFailoverPreferences {
        domain_name: domain.name.clone(),
        ..Default::default()
    };
    let preferred = domain.preferred_cluster();
    if !preferred.is_empty()
        && preferred != domain.active_cluster_name
        && domain.has_cluster(preferred)
    {
        prefs.target_cluster = preferred.to_owned();
    }

    let raw = domain.data_value(DOMAIN_DATA_KEY_FOR_CLUSTER_ATTRIBUTE_PREFERENCES);
    let attribute_preferences = cluster_attribute_preferences(raw).unwrap_or_default();
    prefs.cluster_attribute_updates =
        cluster_attribute_updates_needed(domain, &attribute_preferences);

    if prefs.target_cluster.is_empty() && prefs.cluster_attribute_updates.is_empty() {
        return Err(RebalanceSkip::NoRebalanceRequired);
    }
    prefs.failover_timeout_seconds = domain.failover_timeout_seconds();
    Ok(prefs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failovermanager::domain::DOMAIN_DATA_KEY_FOR_PREFERRED_CLUSTER;
    use crate::failovermanager::shared::DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER;

    fn managed_domain(active: &str, preferred: &str) -> DomainView {
        DomainView {
            name: "d".into(),
            status: DomainStatus::Registered,
            is_global: true,
            data: BTreeMap::from([
                (DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER.into(), "true".into()),
                (DOMAIN_DATA_KEY_FOR_PREFERRED_CLUSTER.into(), preferred.into()),
            ]),
            active_cluster_name: active.into(),
            clusters: vec!["a".into(), "b".into()],
            active_clusters: None,
        }
    }

    #[test]
    fn attribute_preferences_decode_go_json_and_treat_null_as_empty() {
        assert!(cluster_attribute_preferences("").unwrap().is_empty());
        assert!(cluster_attribute_preferences("null").unwrap().is_empty());
        let parsed = cluster_attribute_preferences(
            r#"[{"scope":"cluster","name":"c0","preferredCluster":"b"}]"#,
        )
        .unwrap();
        assert_eq!(parsed[0].preferred_cluster, "b");
        assert!(cluster_attribute_preferences("{").is_err());
    }

    #[test]
    fn allow_rebalance_requires_every_condition() {
        assert!(should_allow_rebalance(&managed_domain("a", "b")));
        assert!(!should_allow_rebalance(&managed_domain("a", "a")));
        assert!(!should_allow_rebalance(&managed_domain("a", "")));
        assert!(!should_allow_rebalance(&managed_domain("a", "z")));
        let mut deleted = managed_domain("a", "b");
        deleted.status = DomainStatus::Deleted;
        assert!(!should_allow_rebalance(&deleted));
        let mut local = managed_domain("a", "b");
        local.is_global = false;
        assert!(!should_allow_rebalance(&local));
    }

    #[test]
    fn groups_domains_by_preferred_cluster() {
        let grouped = domains_by_cluster(&[
            ("d1".into(), "a".into()),
            ("d2".into(), "b".into()),
            ("d3".into(), "a".into()),
        ]);
        assert_eq!(grouped["a"], vec!["d1", "d3"]);
        assert_eq!(grouped["b"], vec!["d2"]);
    }

    #[test]
    fn domain_level_rebalance_targets_the_preferred_cluster() {
        let prefs = rebalance_preferences_for_domain(&managed_domain("a", "b")).unwrap();
        assert_eq!(prefs.target_cluster, "b");
        assert!(prefs.cluster_attribute_updates.is_empty());
    }

    #[test]
    fn rebalance_skips_ineligible_and_already_balanced_domains() {
        assert_eq!(
            rebalance_preferences_for_domain(&DomainView::default()),
            Err(RebalanceSkip::NotEligible)
        );
        assert_eq!(
            rebalance_preferences_for_domain(&managed_domain("b", "b")),
            Err(RebalanceSkip::NoRebalanceRequired)
        );
    }

    #[test]
    fn attribute_updates_only_for_configured_attributes_that_differ() {
        let mut domain = managed_domain("a", "a");
        domain.active_clusters = Some(BTreeMap::from([(
            "cluster".to_owned(),
            BTreeMap::from([("c0".to_owned(), "a".to_owned())]),
        )]));
        domain.data.insert(
            DOMAIN_DATA_KEY_FOR_CLUSTER_ATTRIBUTE_PREFERENCES.into(),
            r#"[{"scope":"cluster","name":"c0","preferredCluster":"b"},{"scope":"cluster","name":"missing","preferredCluster":"b"}]"#
                .into(),
        );
        let prefs = rebalance_preferences_for_domain(&domain).unwrap();
        assert_eq!(prefs.target_cluster, "");
        assert_eq!(
            prefs.cluster_attribute_updates,
            vec![ClusterAttributePreference {
                scope: "cluster".into(),
                name: "c0".into(),
                preferred_cluster: "b".into(),
            }]
        );
    }

    #[test]
    fn malformed_attribute_preferences_are_ignored() {
        let mut domain = managed_domain("a", "b");
        domain
            .data
            .insert(DOMAIN_DATA_KEY_FOR_CLUSTER_ATTRIBUTE_PREFERENCES.into(), "{".into());
        let prefs = rebalance_preferences_for_domain(&domain).unwrap();
        assert_eq!(prefs.target_cluster, "b");
        assert!(prefs.cluster_attribute_updates.is_empty());
    }
}
