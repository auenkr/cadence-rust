//! Shared failover/rebalance helpers from Go `failovermanager/shared.go`.
//!
//! The batch loop, pause handling and eligibility checks are ported as pure
//! functions. The Go activity body that calls `FailoverDomain` and the
//! poller warnings need the frontend client and are not ported yet.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::failovermanager::domain::{DomainStatus, DomainView};

/// Go `constants.DomainDataKeyForManagedFailover`.
pub const DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER: &str = "IsManagedByCadence";
/// Go `constants.DomainDataKeyForFailoverTimeoutSeconds`.
pub const DOMAIN_DATA_KEY_FOR_FAILOVER_TIMEOUT_SECONDS: &str = "FailoverTimeoutInSeconds";

/// Go encodes nil slices as `null`. Decoding treats `null` as an empty list.
fn null_as_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Stored form of one scope:name cluster-attribute preference (Go JSON tags).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterAttributePreference {
    pub scope: String,
    pub name: String,
    #[serde(rename = "preferredCluster")]
    pub preferred_cluster: String,
}

/// Failover instruction for one domain. Go declares no JSON tags, so the
/// names below are Go's exported field names.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DomainFailoverPreferences {
    #[serde(rename = "DomainName")]
    pub domain_name: String,
    #[serde(rename = "TargetCluster")]
    pub target_cluster: String,
    #[serde(rename = "ClusterAttributeUpdates", deserialize_with = "null_as_empty")]
    pub cluster_attribute_updates: Vec<ClusterAttributePreference>,
    /// `> 0` requests graceful failover with this timeout; `0` requests force failover.
    #[serde(rename = "FailoverTimeoutSeconds")]
    pub failover_timeout_seconds: i32,
}

/// Arguments of the shared `FailoverActivityV2`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FailoverActivityV2Params {
    #[serde(rename = "DomainPreferences", deserialize_with = "null_as_empty")]
    pub domain_preferences: Vec<DomainFailoverPreferences>,
    #[serde(rename = "SkipDestinationClusterCheck")]
    pub skip_destination_cluster_check: bool,
}

/// A domain that failed over.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DomainFailoverSuccess {
    #[serde(rename = "DomainName")]
    pub domain_name: String,
}

/// A domain that did not fail over, with the error text (a string so it
/// survives JSON across the activity boundary).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DomainFailoverFailure {
    #[serde(rename = "DomainName")]
    pub domain_name: String,
    #[serde(rename = "Error")]
    pub error: String,
}

/// Result of the shared `FailoverActivityV2`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FailoverActivityV2Result {
    #[serde(rename = "SuccessDomains", deserialize_with = "null_as_empty")]
    pub success_domains: Vec<DomainFailoverSuccess>,
    #[serde(rename = "FailedDomains", deserialize_with = "null_as_empty")]
    pub failed_domains: Vec<DomainFailoverFailure>,
}

/// Distinct, non-empty destination clusters a domain moves onto, sorted (Go
/// `clustersTouchedByPreferences`).
pub fn clusters_touched_by_preferences(prefs: &DomainFailoverPreferences) -> Vec<String> {
    let mut clusters: Vec<String> = Vec::new();
    if !prefs.target_cluster.is_empty() {
        clusters.push(prefs.target_cluster.clone());
    }
    for update in &prefs.cluster_attribute_updates {
        if update.preferred_cluster.is_empty() || clusters.contains(&update.preferred_cluster) {
            continue;
        }
        clusters.push(update.preferred_cluster.clone());
    }
    clusters.sort();
    clusters
}

/// Active-cluster attributes grouped as scope → name → cluster (Go
/// `buildActiveClustersFromUpdates`). A later update to the same scope:name wins.
pub fn build_active_clusters_from_updates(
    updates: &[ClusterAttributePreference],
) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut scopes: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for update in updates {
        scopes
            .entry(update.scope.clone())
            .or_default()
            .insert(update.name.clone(), update.preferred_cluster.clone());
    }
    scopes
}

/// Go `isDomainFailoverManagedByCadence`: the data flag is `true` after trim and lowercase.
pub fn is_domain_failover_managed_by_cadence(data: &BTreeMap<String, String>) -> bool {
    data.get(DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER)
        .is_some_and(|value| value.trim().to_lowercase() == "true")
}

/// Go `isEligibleForFailover`. A missing domain is never eligible.
pub fn is_eligible_for_failover(domain: Option<&DomainView>) -> bool {
    match domain {
        None => false,
        Some(domain) if domain.status != DomainStatus::Registered || !domain.is_global => false,
        Some(domain) => domain.managed_by_cadence(),
    }
}

/// Go `processInBatches`. Splits `prefs` into `batch_size` chunks (a non-positive
/// size means one batch), runs `on_batch_start` and `execute` for each, and
/// calls `wait` between batches but not after the last one.
pub fn process_in_batches<T, S, F>(
    prefs: &[T],
    batch_size: i64,
    mut on_batch_start: impl FnMut(),
    mut execute: impl FnMut(&[T]) -> (Vec<S>, Vec<F>),
    mut wait: impl FnMut(),
) -> (Vec<S>, Vec<F>) {
    if prefs.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let size = if batch_size <= 0 {
        prefs.len()
    } else {
        batch_size as usize
    };
    let batches: Vec<&[T]> = prefs.chunks(size).collect();
    let last = batches.len() - 1;
    let mut success = Vec::new();
    let mut failed = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        on_batch_start();
        let (batch_success, batch_failed) = execute(batch);
        success.extend(batch_success);
        failed.extend(batch_failed);
        if index != last {
            wait();
        }
    }
    (success, failed)
}

/// Go `failuresFromBatch`: marks every domain in the batch failed with `error`.
pub fn failures_from_batch(
    prefs: &[DomainFailoverPreferences],
    error: &str,
) -> Vec<DomainFailoverFailure> {
    prefs
        .iter()
        .map(|pref| DomainFailoverFailure {
            domain_name: pref.domain_name.clone(),
            error: error.to_owned(),
        })
        .collect()
}

/// Go `successDomainNames`.
pub fn success_domain_names(successes: &[DomainFailoverSuccess]) -> Vec<String> {
    successes.iter().map(|s| s.domain_name.clone()).collect()
}

/// Go `failedDomainNames`.
pub fn failed_domain_names(failures: &[DomainFailoverFailure]) -> Vec<String> {
    failures.iter().map(|f| f.domain_name.clone()).collect()
}

/// Go `getFailoverTimeoutSeconds` after the domain-data lookup. Missing, empty,
/// non-numeric, non-positive and overflowing values all give `0`, which means
/// force failover. Go logs a warning for the invalid cases; this port does not.
pub fn failover_timeout_seconds(raw: &str) -> i32 {
    match raw.parse::<i64>() {
        Ok(value) if value > 0 && value <= i64::from(i32::MAX) => value as i32,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pref(
        domain: &str,
        target: &str,
        updates: &[(&str, &str, &str)],
    ) -> DomainFailoverPreferences {
        DomainFailoverPreferences {
            domain_name: domain.into(),
            target_cluster: target.into(),
            cluster_attribute_updates: updates
                .iter()
                .map(|(scope, name, cluster)| ClusterAttributePreference {
                    scope: (*scope).into(),
                    name: (*name).into(),
                    preferred_cluster: (*cluster).into(),
                })
                .collect(),
            failover_timeout_seconds: 0,
        }
    }

    fn view(managed: &str, is_global: bool, status_bad: bool) -> DomainView {
        DomainView {
            status: if status_bad {
                DomainStatus::Deprecated
            } else {
                DomainStatus::Registered
            },
            is_global,
            data: BTreeMap::from([(DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER.into(), managed.into())]),
            ..Default::default()
        }
    }

    #[test]
    fn preserves_go_domain_data_keys() {
        assert_eq!(DOMAIN_DATA_KEY_FOR_MANAGED_FAILOVER, "IsManagedByCadence");
        assert_eq!(
            DOMAIN_DATA_KEY_FOR_FAILOVER_TIMEOUT_SECONDS,
            "FailoverTimeoutInSeconds"
        );
    }

    #[test]
    fn eligibility_matches_go_table() {
        assert!(!is_eligible_for_failover(None));
        assert!(!is_eligible_for_failover(Some(
            &DomainView::default()
        )));
        assert!(is_eligible_for_failover(Some(&view("true", true, false))));
        assert!(is_eligible_for_failover(Some(&view(" TRUE ", true, false))));
        assert!(!is_eligible_for_failover(Some(&view("false", true, false))));
        assert!(!is_eligible_for_failover(Some(&view("true", false, false))));
        assert!(!is_eligible_for_failover(Some(&view("true", true, true))));
    }

    #[test]
    fn touched_clusters_are_distinct_sorted_and_skip_empty() {
        assert_eq!(
            clusters_touched_by_preferences(&pref("d", "cluster1", &[])),
            vec!["cluster1"]
        );
        assert_eq!(
            clusters_touched_by_preferences(&pref(
                "d",
                "",
                &[
                    ("cluster", "cluster0", "cluster1"),
                    ("region", "us-west", "cluster2")
                ]
            )),
            vec!["cluster1", "cluster2"]
        );
        assert_eq!(
            clusters_touched_by_preferences(&pref(
                "d",
                "cluster1",
                &[
                    ("cluster", "cluster0", "cluster1"),
                    ("cluster", "cluster2", "cluster2")
                ]
            )),
            vec!["cluster1", "cluster2"]
        );
        assert!(
            clusters_touched_by_preferences(&pref("d", "", &[("cluster", "c0", "")])).is_empty()
        );
    }

    #[test]
    fn active_clusters_group_by_scope_and_name() {
        let updates = [
            ClusterAttributePreference {
                scope: "cluster".into(),
                name: "c0".into(),
                preferred_cluster: "a".into(),
            },
            ClusterAttributePreference {
                scope: "region".into(),
                name: "us-west".into(),
                preferred_cluster: "b".into(),
            },
            ClusterAttributePreference {
                scope: "cluster".into(),
                name: "c0".into(),
                preferred_cluster: "c".into(),
            },
        ];
        let grouped = build_active_clusters_from_updates(&updates);
        assert_eq!(grouped["cluster"]["c0"], "c");
        assert_eq!(grouped["region"]["us-west"], "b");
        assert_eq!(grouped.len(), 2);
    }

    #[test]
    fn batches_split_in_order_and_wait_between_only() {
        let prefs = vec![
            pref("a", "", &[]),
            pref("b", "", &[]),
            pref("c", "", &[]),
            pref("d", "", &[]),
            pref("e", "", &[]),
        ];
        let mut sizes = Vec::new();
        let mut waits = 0;
        let (success, failed) = process_in_batches::<_, DomainFailoverSuccess, DomainFailoverFailure>(
            &prefs,
            2,
            || {},
            |batch| {
                sizes.push(batch.len());
                (
                    batch
                        .iter()
                        .map(|p| DomainFailoverSuccess {
                            domain_name: p.domain_name.clone(),
                        })
                        .collect(),
                    Vec::new(),
                )
            },
            || waits += 1,
        );
        assert_eq!(sizes, vec![2, 2, 1]);
        assert_eq!(waits, 2);
        assert_eq!(
            success_domain_names(&success),
            vec!["a", "b", "c", "d", "e"]
        );
        assert!(failed.is_empty());
    }

    #[test]
    fn non_positive_batch_size_is_one_batch_and_empty_input_runs_nothing() {
        let prefs = vec![pref("a", "", &[]), pref("b", "", &[])];
        let mut calls = 0;
        process_in_batches::<_, (), ()>(
            &prefs,
            0,
            || {},
            |_| {
                calls += 1;
                (Vec::new(), Vec::new())
            },
            || panic!("no wait for a single batch"),
        );
        assert_eq!(calls, 1);

        process_in_batches::<DomainFailoverPreferences, (), ()>(
            &[],
            2,
            || panic!("no batch"),
            |_| panic!("no batch"),
            || {},
        );
    }

    #[test]
    fn failures_and_name_helpers_mirror_go() {
        let prefs = vec![pref("a", "", &[]), pref("b", "", &[])];
        let failures = failures_from_batch(&prefs, "boom");
        assert_eq!(
            failures,
            vec![
                DomainFailoverFailure {
                    domain_name: "a".into(),
                    error: "boom".into()
                },
                DomainFailoverFailure {
                    domain_name: "b".into(),
                    error: "boom".into()
                },
            ]
        );
        assert!(failures_from_batch(&[], "boom").is_empty());
        assert_eq!(failed_domain_names(&failures), vec!["a", "b"]);
        assert!(success_domain_names(&[]).is_empty());
    }

    #[test]
    fn timeout_parsing_matches_go_table() {
        assert_eq!(failover_timeout_seconds(""), 0);
        assert_eq!(failover_timeout_seconds("0"), 0);
        assert_eq!(failover_timeout_seconds("-5"), 0);
        assert_eq!(failover_timeout_seconds("abc"), 0);
        assert_eq!(failover_timeout_seconds("300"), 300);
        assert_eq!(failover_timeout_seconds("1"), 1);
        assert_eq!(failover_timeout_seconds("2147483648"), 0);
        assert_eq!(failover_timeout_seconds("2147483647"), i32::MAX);
    }

    #[test]
    fn decodes_go_null_slices_and_keeps_go_field_names() {
        let json = r#"{"DomainPreferences":null,"SkipDestinationClusterCheck":true}"#;
        let params: FailoverActivityV2Params = serde_json::from_str(json).unwrap();
        assert!(params.domain_preferences.is_empty());
        assert!(params.skip_destination_cluster_check);

        let pref = DomainFailoverPreferences {
            domain_name: "d".into(),
            target_cluster: "c".into(),
            cluster_attribute_updates: vec![ClusterAttributePreference {
                scope: "s".into(),
                name: "n".into(),
                preferred_cluster: "p".into(),
            }],
            failover_timeout_seconds: 30,
        };
        assert_eq!(
            serde_json::to_string(&pref).unwrap(),
            r#"{"DomainName":"d","TargetCluster":"c","ClusterAttributeUpdates":[{"scope":"s","name":"n","preferredCluster":"p"}],"FailoverTimeoutSeconds":30}"#
        );
    }
}
