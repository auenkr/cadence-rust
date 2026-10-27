//! Go `handler/membership_test.go` rows that were missing in Rust:
//! `TestShutDownNonOwnedSemaphoreManagers`, `...SurfacesALookupFailure` and the
//! `TestGetTaskListManager_OwnerShip` LookupError row for semaphore buckets.
use super::*;
use crate::matching_semaphore::{Identifier, ManagerError};
use cadence_common::membership::Host;
use std::sync::Arc;
use uuid::Uuid;

fn host(n: u32) -> Host {
    Host {
        address: format!("rust-{n}:7935"),
        identity: format!("rust-matching-{n}"),
        grpc_address: format!("rust-{n}:7835"),
    }
}

fn engine_with(membership: &crate::matching_membership::Membership) -> Matching {
    let (_dir, _store, matching, _key) =
        super::durable_tests::fixture_with_history("http://127.0.0.1:1");
    // Keep the tempdir alive for the whole test process; the store is only used for metadata.
    std::mem::forget(_dir);
    let domain = Uuid::from_u128(1).to_string();
    matching
        .frontend
        .state
        .executions
        .register_domain("domain", &domain);
    matching
        .frontend
        .state
        .executions
        .create_semaphore("domain", "sem", 2, 2)
        .unwrap();
    matching.with_membership(membership.clone())
}

fn bucket(n: i32) -> Identifier {
    Identifier::new(&Uuid::from_u128(1).to_string(), "sem", n).unwrap()
}

async fn load(matching: &Matching, id: Identifier) -> Arc<crate::matching_semaphore::Manager> {
    matching
        .semaphores
        .get_or_create_with_idle_ttl(id, 60_000_000_000)
        .await
        .unwrap()
}

// Go `TestShutDownNonOwnedSemaphoreManagers`: the bucket this host still owns stays loaded and
// usable, only the bucket that moved to the other host is stopped and unregistered.
#[tokio::test]
async fn membership_change_keeps_owned_semaphore_bucket_and_stops_moved_one() {
    let membership =
        crate::matching_membership::Membership::new("rust-matching-0", vec![host(0), host(1)]);
    let owned = (0..512)
        .map(bucket)
        .find(|id| membership.owns(&id.ring_key()).unwrap())
        .expect("a bucket owned by this host");
    let moved = (0..512)
        .map(bucket)
        .find(|id| !membership.owns(&id.ring_key()).unwrap())
        .expect("a bucket owned by the other host");
    let matching = engine_with(&membership);
    let owned_manager = load(&matching, owned.clone()).await;
    let moved_manager = load(&matching, moved.clone()).await;

    matching
        .maintain_membership_until(std::future::ready(()))
        .await
        .unwrap();

    assert!(matching.semaphores.by_identifier(&owned).is_some());
    assert!(matching.semaphores.by_identifier(&moved).is_none());
    assert_eq!(matching.semaphores.all_managers().len(), 1);
    // The moved bucket was stopped; the owned bucket still serves requests.
    assert!(matches!(
        moved_manager.acquire("1:w:r1:1", None).await,
        Err(ManagerError::NotReady)
    ));
    assert!(!matches!(
        owned_manager.acquire("1:w:r1:1", None).await,
        Err(ManagerError::NotReady)
    ));
}

// Go `TestShutDownNonOwnedSemaphoreManagersSurfacesALookupFailure`: when the ring cannot answer
// (no hosts), the cleanup is abandoned and no bucket is unloaded.
#[tokio::test]
async fn membership_lookup_failure_unloads_no_semaphore_bucket() {
    let membership = crate::matching_membership::Membership::new("rust-matching-0", vec![]);
    let matching = engine_with(&membership);
    let first = load(&matching, bucket(0)).await;
    let second = load(&matching, bucket(1)).await;

    let result = matching
        .maintain_membership_until(std::future::ready(()))
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(matching.semaphores.all_managers().len(), 2);
    for manager in [first, second] {
        assert!(!matches!(
            manager.acquire("1:w:r1:1", None).await,
            Err(ManagerError::NotReady)
        ));
    }
}

// Go `TestGetTaskListManager_OwnerShip` LookupError row, for a semaphore bucket: a failed ring
// lookup is an Internal error naming the bucket, not a not-owned Aborted. A bucket owned by
// another host is Aborted with the owner and this host named.
#[tokio::test]
async fn semaphore_ownership_check_distinguishes_lookup_error_from_not_owned() {
    let empty = crate::matching_membership::Membership::new("rust-matching-0", vec![]);
    let matching = engine_with(&empty);
    let id = bucket(0);
    let error = matching.check_semaphore_ownership(&id).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains(&format!(
            "failed to look up the owner of semaphore bucket {}",
            id.to_log_string()
        )),
        "{}",
        error.message()
    );

    let other_only = crate::matching_membership::Membership::new("rust-matching-0", vec![host(1)]);
    let matching = engine_with(&other_only);
    let error = matching.check_semaphore_ownership(&id).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(
        error
            .message()
            .contains("OwnedBy: rust-matching-1, Me: rust-matching-0"),
        "{}",
        error.message()
    );
}
