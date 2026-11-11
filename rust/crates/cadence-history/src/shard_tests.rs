//! Ports of `go/service/history/shard/controller_test.go` against the Rust
//! `ShardController` (single-process ring based ownership + range acquisition).

use crate::shard::*;
use cadence_common::membership::Host;
use std::sync::Arc;
use std::time::Duration;

fn host(name: &str) -> Host {
    Host {
        address: format!("{name}:7934"),
        identity: name.into(),
        grpc_address: format!("{name}:7834"),
    }
}

fn ring() -> Vec<Host> {
    vec![host("a"), host("b")]
}

fn owned_by(owner: &str, members: &[Host], shards: u32) -> Vec<u32> {
    (0..shards)
        .filter(|id| ShardController::desired_owner(*id, members).as_deref() == Some(owner))
        .collect()
}

// TestAcquireShardSuccess
#[test]
fn acquire_shard_success() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(16, "a", store.clone());
    let mine = owned_by("a", &members, 16);
    assert!(!mine.is_empty());
    for id in &mine {
        let info = c.acquire(*id, &members).unwrap().unwrap();
        assert_eq!(info.shard_id, *id);
        assert_eq!(info.owner, "a");
        assert_eq!(info.range_id, 1);
    }
    assert_eq!(c.owned_shards(), mine);
}

// TestAcquireShardsConcurrently
#[test]
fn acquire_shards_concurrently() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = Arc::new(ShardController::new(32, "a", store));
    let mine = owned_by("a", &members, 32);
    let mut handles = vec![];
    for _ in 0..4 {
        let c = c.clone();
        let members = members.clone();
        handles.push(std::thread::spawn(move || {
            for id in 0..32 {
                c.acquire(id, &members).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(c.owned_shards(), mine);
}

// TestAcquireShardLookupFailure: an empty ring has no owner, nothing is opened.
#[test]
fn acquire_shard_lookup_failure() {
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(8, "a", store);
    assert_eq!(c.acquire(1, &[]).unwrap(), None);
    assert!(c.owned_shards().is_empty());
    assert!(matches!(
        c.check_ownership(1, &[]),
        Err(ShardError::ShardOwnershipLost { shard_id: 1, .. })
    ));
}

// TestAcquireShardRenewSuccess: re-acquiring an owned shard keeps the item open.
#[test]
fn acquire_shard_renew_success() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(16, "a", store);
    let id = owned_by("a", &members, 16)[0];
    let first = c.acquire(id, &members).unwrap().unwrap();
    let again = c.acquire(id, &members).unwrap().unwrap();
    assert_eq!(first.range_id, again.range_id);
    assert_eq!(c.owned_shards(), vec![id]);
    c.check_ownership(id, &members).unwrap();
}

// TestAcquireShardRenewLookupFailed: membership shrinks so the shard moves away.
#[test]
fn acquire_shard_renew_lookup_failed() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(16, "a", store);
    let id = owned_by("a", &members, 16)[0];
    c.acquire(id, &members).unwrap().unwrap();
    assert_eq!(c.acquire(id, &members[1..]).unwrap(), None);
    assert!(c.owned_shards().is_empty());
}

// TestHistoryEngineClosed: closing a shard removes it and ownership checks fail.
#[test]
fn history_engine_closed() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(16, "a", store);
    let id = owned_by("a", &members, 16)[0];
    c.acquire(id, &members).unwrap().unwrap();
    c.close(id);
    assert!(c.owned_shards().is_empty());
    assert!(matches!(
        c.check_ownership(id, &members),
        Err(ShardError::ShardOwnershipLost { .. })
    ));
    // Re-acquire after close succeeds again.
    assert!(c.acquire(id, &members).unwrap().is_some());
}

// TestShardControllerClosed: run() closes every owned shard when shut down.
#[tokio::test]
async fn shard_controller_closed() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = Arc::new(ShardController::new(16, "a", store));
    let (_mtx, mrx) = tokio::sync::watch::channel(members.clone());
    let (stx, srx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(c.clone().run(mrx, srx, Duration::from_millis(5)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!c.owned_shards().is_empty());
    stx.send(true).unwrap();
    handle.await.unwrap().unwrap();
    assert!(c.owned_shards().is_empty());
}

// TestGetOrCreateHistoryShardItem_InvalidShardID_Error
#[test]
fn get_or_create_history_shard_item_invalid_shard_id_error() {
    let store = Arc::new(InMemoryShardStore::default());
    let c = ShardController::new(4, "a", store);
    let err = c.acquire(4, &ring()).unwrap_err();
    assert!(matches!(err, ShardError::Store(_)));
    let err = c.acquire(u32::MAX, &ring()).unwrap_err();
    assert!(matches!(err, ShardError::Store(_)));
}

// Membership change wakes reconciliation (acquireShards on ring update).
#[tokio::test]
async fn membership_change_reconciles() {
    let members = ring();
    let store = Arc::new(InMemoryShardStore::default());
    let c = Arc::new(ShardController::new(16, "a", store));
    let (mtx, mrx) = tokio::sync::watch::channel(vec![host("b")]);
    let (stx, srx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(c.clone().run(mrx, srx, Duration::from_secs(3600)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(c.owned_shards().is_empty());
    mtx.send(members.clone()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(c.owned_shards(), owned_by("a", &members, 16));
    stx.send(true).unwrap();
    handle.await.unwrap().unwrap();
}

// Store contract used by acquireShard: create, then fenced update by range id.
#[test]
fn store_fences_updates_by_previous_range_id() {
    let store = InMemoryShardStore::default();
    assert_eq!(store.get_shard(3).unwrap(), None);
    assert!(matches!(
        store.update_shard(3, 1, "a"),
        Err(ShardError::Store(_))
    ));
    let created = store.create_shard(3, "a").unwrap();
    assert_eq!(created.range_id, 1);
    // create is idempotent
    assert_eq!(store.create_shard(3, "b").unwrap().owner, "a");
    let stolen = store.update_shard(3, 1, "b").unwrap();
    assert_eq!((stolen.range_id, stolen.owner.as_str()), (2, "b"));
    assert!(matches!(
        store.update_shard(3, 1, "a"),
        Err(ShardError::ShardOwnershipLost { shard_id: 3, .. })
    ));
}

#[test]
fn workflow_id_to_shard_zero_shards_is_zero() {
    assert_eq!(ShardController::workflow_id_to_shard("x", 0), 0);
    assert!(ShardController::workflow_id_to_shard("some-id", 7) < 7);
}
