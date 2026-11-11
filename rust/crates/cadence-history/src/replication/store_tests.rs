//! Port of `go/service/history/replication/task_store_test.go` (child module of
//! `task_store` so the private cluster caches can be inspected like Go's `ts.clusters`).

use super::super::deps::*;
use super::super::test_support::*;
use super::*;
use std::sync::Arc;

fn store(domains: FakeDomains, hydrator: FakeHydrator) -> TaskStore {
    TaskStore::new(
        &TestConfig::default(),
        vec![CLUSTER_A.into(), CLUSTER_B.into()],
        Arc::new(domains),
        Arc::new(NoopMetrics),
        Arc::new(hydrator),
        Arc::new(NoLimit),
        0,
        Arc::new(FixedTime(0)),
    )
}

fn count(ts: &TaskStore, cluster: &str) -> usize {
    ts.clusters[cluster].count()
}

#[tokio::test]
async fn get_error_on_unknown_cluster() {
    let ts = store(FakeDomains::default(), FakeHydrator::default());
    let err = ts.get("unknown cluster", &history_task(11)).await.unwrap_err();
    assert!(matches!(err, ReplicationError::UnknownCluster));
}

#[tokio::test]
async fn get_error_resolving_domain() {
    let ts = store(FakeDomains::default(), FakeHydrator::default());
    let err = ts.get(CLUSTER_A, &history_task(11)).await.unwrap_err();
    assert_eq!(err.to_string(), "resolving domain: domain does not exist");
}

#[tokio::test]
async fn get_skips_task_for_domains_not_belonging_to_polling_cluster() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    assert!(ts.get(CLUSTER_B, &history_task(11)).await.unwrap().is_none());
}

#[tokio::test]
async fn get_returns_cached_replication_task() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.put(Some(Arc::new(hydrated_history(11))));
    let task = ts.get(CLUSTER_A, &history_task(11)).await.unwrap().unwrap();
    assert_eq!(*task, hydrated_history(11));
}

#[tokio::test]
async fn get_returns_non_cached_replication_task_by_hydrating_it() {
    let mut h = FakeHydrator::default();
    h.0.insert(11, Ok(Some(hydrated_history(11))));
    let ts = store(FakeDomains::test_domain(), h);
    let task = ts.get(CLUSTER_A, &history_task(11)).await.unwrap().unwrap();
    assert_eq!(*task, hydrated_history(11));
    // hydrated task is now cached
    assert_eq!(count(&ts, CLUSTER_A), 1);
}

#[tokio::test]
async fn get_fails_to_hydrate_replication_task() {
    let mut h = FakeHydrator::default();
    h.0.insert(11, Err(ReplicationError::Other("error hydrating task".into())));
    let ts = store(FakeDomains::test_domain(), h);
    let err = ts.get(CLUSTER_A, &history_task(11)).await.unwrap_err();
    assert_eq!(err.to_string(), "error hydrating task");
    assert_eq!(count(&ts, CLUSTER_A), 0);
}

#[tokio::test]
async fn get_hydrator_returns_nothing_is_ignored() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    assert!(ts.get(CLUSTER_A, &history_task(11)).await.unwrap().is_none());
}

#[test]
fn put_does_not_store_nil_task() {
    let ts = store(FakeDomains::default(), FakeHydrator::default());
    ts.put(None);
    for c in ts.clusters.values() {
        assert_eq!(c.count(), 0);
    }
}

#[test]
fn put_error_resolving_domain_does_not_store_task() {
    let ts = store(FakeDomains::default(), FakeHydrator::default());
    ts.put(Some(Arc::new(hydrated_history(11))));
    ts.put(Some(Arc::new(hydrated_sync(12))));
    ts.put(Some(Arc::new(hydrated_failover(14))));
    for c in ts.clusters.values() {
        assert_eq!(c.count(), 0);
    }
}

#[test]
fn put_hydrated_task_into_appropriate_cache() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.put(Some(Arc::new(hydrated_history(11))));
    assert_eq!(count(&ts, CLUSTER_A), 1);
    assert_eq!(count(&ts, CLUSTER_B), 0);
}

#[test]
fn put_hydrated_task_without_domain_info_puts_it_to_all_caches() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.put(Some(Arc::new(ReplicationTask { source_task_id: Some(123), ..Default::default() })));
    assert_eq!(count(&ts, CLUSTER_A), 1);
    assert_eq!(count(&ts, CLUSTER_B), 1);
}

#[test]
fn put_all_three_task_kinds() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.put(Some(Arc::new(hydrated_history(11))));
    ts.put(Some(Arc::new(hydrated_sync(12))));
    ts.put(Some(Arc::new(hydrated_failover(14))));
    assert_eq!(count(&ts, CLUSTER_A), 3);
}

#[test]
fn put_will_not_store_acked_task() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.ack(CLUSTER_A, 11).unwrap();
    ts.put(Some(Arc::new(hydrated_history(11))));
    assert_eq!(count(&ts, CLUSTER_A), 0);
}

#[test]
fn ack_removes_tasks_up_to_level() {
    let ts = store(FakeDomains::test_domain(), FakeHydrator::default());
    ts.put(Some(Arc::new(hydrated_history(11))));
    ts.put(Some(Arc::new(hydrated_sync(12))));
    ts.put(Some(Arc::new(hydrated_history(13))));
    ts.ack(CLUSTER_A, 12).unwrap();
    assert_eq!(count(&ts, CLUSTER_A), 1);
}

#[test]
fn ack_error_on_unknown_cluster() {
    let ts = store(FakeDomains::default(), FakeHydrator::default());
    assert!(matches!(ts.ack("unknown cluster", 0), Err(ReplicationError::UnknownCluster)));
}

#[test]
fn full_cache_rejects_without_panicking() {
    let ts = TaskStore::new(
        &TestConfig::default(),
        vec![CLUSTER_A.into()],
        Arc::new(FakeDomains::test_domain()),
        Arc::new(NoopMetrics),
        Arc::new(FakeHydrator::default()),
        Arc::new(NoLimit),
        0,
        Arc::new(FixedTime(0)),
    );
    // cache capacity of TestConfig is 100: the 101st put is dropped
    for id in 1..=101 {
        ts.put(Some(Arc::new(hydrated_history(id))));
    }
    assert_eq!(count(&ts, CLUSTER_A), 100);
}

// BoundedAckCache (common/cache bounded_ack_cache tests)
#[test]
fn bounded_ack_cache_put_get_ack() {
    let c: BoundedAckCache<i32> = BoundedAckCache::new(2, 100);
    c.put(1, 10, 10).unwrap();
    c.put(2, 20, 10).unwrap();
    assert_eq!(c.put(3, 30, 10), Err(AckCacheError::Full));
    assert_eq!(c.get(1), Some(10));
    assert_eq!(c.size(), 20);
    assert_eq!(c.ack(1), (10, 1));
    assert_eq!(c.get(1), None);
    assert_eq!(c.put(1, 10, 10), Err(AckCacheError::AlreadyAcked));
    c.put(3, 30, 10).unwrap();
    // duplicate put is a no-op
    c.put(3, 31, 10).unwrap();
    assert_eq!(c.get(3), Some(30));
}

#[test]
fn bounded_ack_cache_size_limit() {
    let c: BoundedAckCache<i32> = BoundedAckCache::new(10, 25);
    c.put(1, 1, 10).unwrap();
    c.put(2, 2, 10).unwrap();
    assert_eq!(c.put(3, 3, 10), Err(AckCacheError::Full));
    assert_eq!(c.count(), 2);
}
