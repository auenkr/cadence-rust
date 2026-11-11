//! Ports of engineimpl/register_domain_failover_callback_test.go (pure / fake-level cases).
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::register_domain_failover_callback::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;

fn entry(id: &str, nv: i64) -> FailoverDomainEntry {
    FailoverDomainEntry { id: id.into(), name: id.into(), notification_version: nv, ..Default::default() }
}

fn engine(current: &str) -> HistoryEngine {
    let mut e = nop_engine();
    e.current_cluster_name = current.to_string();
    e.cluster_metadata = Arc::new(FixedClusterMetadata(current.to_string()));
    e
}

#[test]
fn catch_up_entries_sorts_and_filters() {
    let out = catch_up_entries(vec![entry("c", 9), entry("a", 3), entry("b", 5), entry("old", 1)], 3);
    assert_eq!(out.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(), vec!["a", "b", "c"]);
}

#[test]
fn catch_up_entries_empty_when_all_older() {
    assert!(catch_up_entries(vec![entry("a", 1)], 2).is_empty());
}

#[test]
fn is_active_in_cases() {
    let mut d = entry("d", 1);
    assert!(d.is_active_in("x"), "local domain is always active");
    d.is_global_domain = true;
    d.active_cluster_name = "a".into();
    assert!(d.is_active_in("a"));
    assert!(!d.is_active_in("b"));
    d.is_pending_active = true;
    assert!(!d.is_active_in("a"));
}

#[test]
fn active_cluster_attributes_in_returns_matching_pairs() {
    let mut d = entry("d", 1);
    assert!(active_cluster_attributes_in(&d, "a").is_empty());
    let mut scope = BTreeMap::new();
    scope.insert("city".to_string(), BTreeMap::from([("nyc".to_string(), "a".to_string()), ("sf".to_string(), "b".to_string())]));
    d.active_cluster_scopes = Some(scope);
    assert!(d.is_active_active());
    assert_eq!(active_cluster_attributes_in(&d, "a"), vec![("city".to_string(), "nyc".to_string())]);
    assert!(d.is_active_in("b"));
}

#[test]
fn graceful_failover_marker_generated_for_active_to_passive() {
    let e = engine("a");
    let mut d = entry("d", 5);
    d.is_global_domain = true;
    d.active_cluster_name = "b".into();
    d.failover_notification_version = 10;
    d.failover_version = 7;
    d.previous_failover_version = 3;
    let tasks = e.generate_graceful_failover_tasks_for_domain_update_callback(10, &[d.clone()]);
    assert_eq!(tasks, vec![FailoverMarkerTask { version: 7, domain_id: "d".into() }]);
    // older failover notification version: no marker
    assert!(e.generate_graceful_failover_tasks_for_domain_update_callback(11, &[d.clone()]).is_empty());
    // initial previous failover version: no marker
    d.previous_failover_version = INITIAL_PREVIOUS_FAILOVER_VERSION;
    assert!(e.generate_graceful_failover_tasks_for_domain_update_callback(10, &[d]).is_empty());
}

#[test]
fn graceful_failover_skips_active_active_and_still_active_domains() {
    let e = engine("a");
    let mut aa = entry("aa", 1);
    aa.is_global_domain = true;
    aa.active_cluster_scopes = Some(BTreeMap::from([("s".to_string(), BTreeMap::from([("n".to_string(), "b".to_string())]))]));
    aa.failover_notification_version = 10;
    aa.previous_failover_version = 2;
    let mut still = entry("still", 1);
    still.is_global_domain = true;
    still.active_cluster_name = "a".into();
    still.failover_notification_version = 10;
    still.previous_failover_version = 2;
    assert!(e.generate_graceful_failover_tasks_for_domain_update_callback(0, &[aa, still]).is_empty());
}

#[test]
fn failover_predicate_runs_action_only_for_active_global_domains() {
    let e = engine("a");
    let mut d = entry("d", 1);
    d.is_global_domain = true;
    d.active_cluster_name = "a".into();
    d.failover_notification_version = 5;
    let mut hit = 0;
    e.failover_predicate(5, &d, || hit += 1);
    assert_eq!(hit, 1);
    e.failover_predicate(6, &d, || hit += 1);
    assert_eq!(hit, 1, "older failover notification version");
    d.active_cluster_name = "b".into();
    e.failover_predicate(5, &d, || hit += 1);
    assert_eq!(hit, 1, "other cluster is active");
    d.active_cluster_name = "a".into();
    d.is_global_domain = false;
    e.failover_predicate(5, &d, || hit += 1);
    assert_eq!(hit, 1, "local domains are ignored");
}
