//! Ports of `go/service/history/shard/context_test.go` for the Rust
//! `ShardContext` (public API only).

use crate::shard_context::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Persist {
    calls: Mutex<Vec<(i64, i64)>>,
    fail: Mutex<Option<ShardContextError>>,
}
impl ShardPersistence for Persist {
    fn update_shard(&self, info: &ShardInfoData, prev: i64) -> Result<(), ShardContextError> {
        if let Some(e) = self.fail.lock().unwrap().clone() {
            return Err(e);
        }
        self.calls.lock().unwrap().push((info.range_id, prev));
        Ok(())
    }
}

struct Cl;
impl ClusterInfo for Cl {
    fn current_cluster_name(&self) -> String {
        "a".into()
    }
    fn enabled_cluster_names(&self) -> Vec<String> {
        vec!["a".into(), "b".into()]
    }
}

struct Fixture {
    ctx: Arc<ShardContext>,
    persist: Arc<Persist>,
    now: Arc<AtomicI64>,
    closed: Arc<AtomicUsize>,
}

fn fixture_with(shift: i64, info: ShardInfoData) -> Result<Fixture, ShardContextError> {
    let persist = Arc::new(Persist::default());
    let now = Arc::new(AtomicI64::new(5_000_000_000));
    let closed = Arc::new(AtomicUsize::new(0));
    let (n, c) = (now.clone(), closed.clone());
    let ctx = ShardContext::acquire(
        info,
        "host1",
        4,
        0,
        shift,
        persist.clone(),
        Arc::new(Cl),
        Arc::new(move || n.load(Ordering::SeqCst)),
        Arc::new(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
        }),
    )?;
    Ok(Fixture { ctx, persist, now, closed })
}

fn fixture() -> Fixture {
    fixture_with(
        0,
        ShardInfoData { shard_id: 3, range_id: 4, owner: "host1".into(), ..Default::default() },
    )
    .ok()
    .unwrap()
}

fn wait_closed(f: &Fixture, n: usize) {
    for _ in 0..100 {
        if f.closed.load(Ordering::SeqCst) >= n {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// TestAccessorMethods
#[test]
fn accessor_methods() {
    let f = fixture();
    assert_eq!(f.ctx.shard_id(), 3);
    assert_eq!(f.ctx.range_id(), 5);
    assert_eq!(f.ctx.shard_info_snapshot().shard_id, 3);
    assert!(!f.ctx.previous_shard_owner_was_different());
    let g = fixture_with(0, ShardInfoData { shard_id: 3, range_id: 4, owner: "other".into(), ..Default::default() })
        .ok()
        .unwrap();
    assert!(g.ctx.previous_shard_owner_was_different());
}

// TestTransferQueueState
#[test]
fn transfer_queue_state() {
    let f = fixture();
    let st = f.ctx.get_queue_state(HistoryTaskCategory::Transfer).unwrap();
    assert_eq!(st.exclusive_max_read_level.task_id, 1);
    let update = QueueState::with_slice_mins(TaskKey { task_id: 100, scheduled_time_nano: 0 }, vec![TaskKey { task_id: 50, scheduled_time_nano: 0 }]);
    f.ctx.update_queue_state(HistoryTaskCategory::Transfer, update.clone()).unwrap();
    assert_eq!(f.ctx.get_queue_state(HistoryTaskCategory::Transfer).unwrap(), update);
    // ack level is min(slice mins, max read) - 1 (exclusive -> inclusive)
    assert_eq!(f.ctx.get_queue_ack_level(HistoryTaskCategory::Transfer).task_id, 49);
    let info = f.ctx.shard_info_snapshot();
    assert_eq!(info.cluster_transfer_ack_level.get("a"), Some(&49));
    assert_eq!(info.cluster_transfer_ack_level.get("b"), Some(&49));
}

// TestTimerQueueState
#[test]
fn timer_queue_state() {
    let f = fixture();
    let update = QueueState::with_slice_mins(TaskKey { task_id: 0, scheduled_time_nano: 9_000 }, vec![TaskKey { task_id: 0, scheduled_time_nano: 4_000 }]);
    f.ctx.update_queue_state(HistoryTaskCategory::Timer, update.clone()).unwrap();
    assert_eq!(f.ctx.get_queue_state(HistoryTaskCategory::Timer).unwrap(), update);
    assert_eq!(f.ctx.get_queue_ack_level(HistoryTaskCategory::Timer).scheduled_time_nanos, 4_000);
    assert!(f.ctx.update_queue_state(HistoryTaskCategory::Replication, QueueState::default()).is_err());
    // Go stores the state before switching on the category, so the rejected
    // update is still readable (Rust matches).
    assert!(f.ctx.get_queue_state(HistoryTaskCategory::Replication).is_ok());
}

// TestTransferAckLevel
#[test]
fn transfer_ack_level() {
    let f = fixture();
    f.ctx.update_queue_ack_level(HistoryTaskCategory::Transfer, HistoryTaskKey::immediate(100)).unwrap();
    assert_eq!(f.ctx.get_queue_ack_level(HistoryTaskCategory::Transfer), HistoryTaskKey::immediate(100));
    let st = f.ctx.get_queue_state(HistoryTaskCategory::Transfer).unwrap();
    assert_eq!(st.exclusive_max_read_level.task_id, 101);
}

// TestClusterTransferAckLevel
#[test]
fn cluster_transfer_ack_level() {
    let f = fixture();
    f.ctx
        .update_queue_cluster_ack_level(HistoryTaskCategory::Transfer, "b", HistoryTaskKey::immediate(7))
        .unwrap();
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Transfer, "b").task_id, 7);
    // unknown cluster falls back to the shard ack level
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Transfer, "zzz").task_id, 0);
}

// TestTimerAckLevel
#[test]
fn timer_ack_level() {
    let f = fixture();
    f.ctx.update_queue_ack_level(HistoryTaskCategory::Timer, HistoryTaskKey::scheduled(123, 0)).unwrap();
    assert_eq!(f.ctx.get_queue_ack_level(HistoryTaskCategory::Timer).scheduled_time_nanos, 123);
}

// TestClusterTimerAckLevel
#[test]
fn cluster_timer_ack_level() {
    let f = fixture();
    f.ctx
        .update_queue_cluster_ack_level(HistoryTaskCategory::Timer, "b", HistoryTaskKey::scheduled(55, 0))
        .unwrap();
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, "b").scheduled_time_nanos, 55);
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Timer, "zzz").scheduled_time_nanos, 0);
}

// Replication cluster ack level: new clusters start from -1.
#[test]
fn cluster_replication_ack_level_defaults_to_minus_one() {
    let f = fixture();
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b").task_id, -1);
    f.ctx
        .update_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b", HistoryTaskKey::immediate(12))
        .unwrap();
    assert_eq!(f.ctx.get_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b").task_id, 12);
}

fn level(start: i64, min: i64, cur: i64, max: i64, timer: bool) -> FailoverLevel {
    let k = |v| if timer { HistoryTaskKey::scheduled(v, 0) } else { HistoryTaskKey::immediate(v) };
    FailoverLevel { start_time_nanos: start, min_level: k(min), current_level: k(cur), max_level: k(max) }
}

// TestUpdateTransferFailoverLevel
#[test]
fn update_transfer_failover_level() {
    let f = fixture();
    let l = level(1_000_000_000, 1, 2, 10, false);
    f.ctx.update_failover_level(HistoryTaskCategory::Transfer, "fid", l);
    let all = f.ctx.get_all_failover_levels(HistoryTaskCategory::Transfer);
    assert_eq!(all.get("fid"), Some(&l));
    assert!(f.ctx.get_all_failover_levels(HistoryTaskCategory::Timer).is_empty());
    // latency = now - start
    assert_eq!(f.ctx.delete_failover_level(HistoryTaskCategory::Transfer, "fid"), Some(4_000_000_000));
    assert!(f.ctx.get_all_failover_levels(HistoryTaskCategory::Transfer).is_empty());
    assert_eq!(f.ctx.delete_failover_level(HistoryTaskCategory::Transfer, "fid"), None);
}

// TestUpdateTimerFailoverLevel
#[test]
fn update_timer_failover_level() {
    let f = fixture();
    let l = level(2_000_000_000, 5, 6, 20, true);
    f.ctx.update_failover_level(HistoryTaskCategory::Timer, "fid", l);
    assert_eq!(f.ctx.get_all_failover_levels(HistoryTaskCategory::Timer).get("fid"), Some(&l));
    assert_eq!(f.ctx.delete_failover_level(HistoryTaskCategory::Timer, "fid"), Some(3_000_000_000));
    assert_eq!(f.ctx.delete_failover_level(HistoryTaskCategory::Timer, "missing"), None);
}

// TestDomainNotificationVersion
#[test]
fn domain_notification_version() {
    let f = fixture();
    assert_eq!(f.ctx.get_domain_notification_version(), 0);
    f.ctx.update_domain_notification_version(42).unwrap();
    assert_eq!(f.ctx.get_domain_notification_version(), 42);
    assert_eq!(f.persist.calls.lock().unwrap().len(), 2); // acquire renew + update
}

// TestTimerMaxReadLevel
#[test]
fn timer_max_read_level() {
    let f = fixture_with(
        1_000_000,
        ShardInfoData { shard_id: 3, range_id: 4, owner: "host1".into(), ..Default::default() },
    )
    .ok()
    .unwrap();
    let k = f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, "a");
    assert_eq!(k, HistoryTaskKey::scheduled(5_001_000_000, 0));
    // Never moves backward when the clock does.
    f.now.store(1_000_000_000, Ordering::SeqCst);
    let k = f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, "a");
    assert_eq!(k.scheduled_time_nanos, 5_001_000_000);
    // Remote cluster uses the remote's current time.
    f.ctx.set_current_time("b", 7_000_000_000);
    let kb = f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, "b");
    assert_eq!(kb.scheduled_time_nanos, 7_001_000_000);
}

// TestGenerateTransferTaskID
#[test]
fn generate_transfer_task_id() {
    let f = fixture();
    assert_eq!(f.ctx.generate_task_id().unwrap(), 80);
    assert_eq!(f.ctx.generate_task_id().unwrap(), 81);
}

// TestGenerateTransferTaskIDs
#[test]
fn generate_transfer_task_ids() {
    let f = fixture();
    assert_eq!(f.ctx.generate_task_ids(3).unwrap(), vec![80, 81, 82]);
    assert_eq!(f.ctx.generate_task_ids(0).unwrap(), Vec::<i64>::new());
}

// TestGenerateTransferTaskID_RenewsRange
#[test]
fn generate_transfer_task_id_renews_range() {
    let f = fixture();
    for _ in 0..16 {
        f.ctx.generate_task_id().unwrap();
    }
    assert_eq!(f.ctx.range_id(), 5);
    assert_eq!(f.ctx.generate_task_id().unwrap(), 96);
    assert_eq!(f.ctx.range_id(), 6);
    let calls = f.persist.calls.lock().unwrap().clone();
    assert_eq!(calls, vec![(5, 4), (6, 5)]);
}

// TestRenewRangeLockedRetriesExceeded analogue: a persistence failure that is
// not ownership loss is surfaced and does not close the shard.
#[test]
fn renew_range_failure_is_returned_without_closing() {
    let f = fixture();
    for _ in 0..16 {
        f.ctx.generate_task_id().unwrap();
    }
    *f.persist.fail.lock().unwrap() = Some(ShardContextError::Persistence("boom".into()));
    let err = f.ctx.generate_task_id().unwrap_err();
    assert_eq!(err, ShardContextError::Persistence("boom".into()));
    assert!(!f.ctx.is_closed());
    assert_eq!(f.ctx.range_id(), 5);
}

// Ownership lost on renew closes the shard (stolen).
#[test]
fn renew_range_ownership_lost_closes_shard() {
    let f = fixture();
    for _ in 0..16 {
        f.ctx.generate_task_id().unwrap();
    }
    *f.persist.fail.lock().unwrap() = Some(ShardContextError::ShardOwnershipLost("x".into()));
    assert!(matches!(f.ctx.generate_task_id(), Err(ShardContextError::ShardOwnershipLost(_))));
    assert!(f.ctx.is_closed());
    assert_eq!(f.ctx.range_id(), -1);
    wait_closed(&f, 1);
    assert_eq!(f.closed.load(Ordering::SeqCst), 1);
}

// acquire with stolen shard fails
#[test]
fn acquire_ownership_lost_fails() {
    let persist = Arc::new(Persist::default());
    *persist.fail.lock().unwrap() = Some(ShardContextError::ShardOwnershipLost("x".into()));
    let r = ShardContext::acquire(
        ShardInfoData { shard_id: 1, range_id: 1, ..Default::default() },
        "me",
        4,
        0,
        0,
        persist,
        Arc::new(Cl),
        Arc::new(|| 0),
        Arc::new(|_| {}),
    );
    assert!(r.is_err());
}

// TestUpdateClusterReplicationLevel_Succeeds
#[test]
fn update_cluster_replication_level_succeeds() {
    let f = fixture();
    f.ctx
        .update_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b", HistoryTaskKey::immediate(10))
        .unwrap();
    assert_eq!(f.ctx.shard_info_snapshot().cluster_replication_level.get("b"), Some(&10));
}

// TestUpdateClusterReplicationLevel_FailsWhenUpdateShardFail
#[test]
fn update_cluster_replication_level_fails_when_update_shard_fails() {
    let f = fixture();
    *f.persist.fail.lock().unwrap() = Some(ShardContextError::Persistence("db".into()));
    let err = f
        .ctx
        .update_queue_cluster_ack_level(HistoryTaskCategory::Replication, "b", HistoryTaskKey::immediate(10))
        .unwrap_err();
    assert_eq!(err, ShardContextError::Persistence("db".into()));
}

fn pq(ack: i64) -> ProcessingQueueState {
    ProcessingQueueState { level: 0, ack_level: ack, max_level: i64::MAX, domain_filter_reverse_match: true }
}

// TestGetAndUpdateProcessingQueueStates
#[test]
fn get_and_update_processing_queue_states() {
    let f = fixture();
    assert_eq!(f.ctx.get_transfer_processing_queue_states("b"), vec![pq(0)]);
    f.ctx.update_transfer_processing_queue_states("b", vec![pq(9), pq(5)]).unwrap();
    assert_eq!(f.ctx.get_transfer_processing_queue_states("b"), vec![pq(9), pq(5)]);
    assert_eq!(f.ctx.shard_info_snapshot().cluster_transfer_ack_level.get("b"), Some(&5));
    f.ctx.update_timer_processing_queue_states("b", vec![pq(8), pq(3)]).unwrap();
    assert_eq!(f.ctx.get_timer_processing_queue_states("b"), vec![pq(8), pq(3)]);
    assert_eq!(f.ctx.shard_info_snapshot().cluster_timer_ack_level.get("b"), Some(&3));
    assert!(f.ctx.update_transfer_processing_queue_states("b", vec![]).is_err());
    assert!(f.ctx.update_timer_processing_queue_states("b", vec![]).is_err());
}

fn marker(d: &str, v: i64) -> FailoverMarker {
    FailoverMarker { domain_id: d.into(), failover_version: v, creation_time: 0 }
}

// TestValidateAndUpdateFailoverMarkers (+ FailoverCompleted/DeprecatedDomain/DomainNoLongerExists)
#[test]
fn validate_and_update_failover_markers() {
    let f = fixture();
    f.ctx.add_pending_failover_marker(marker("d1", 1)).unwrap();
    f.ctx.add_pending_failover_marker(marker("d2", 2)).unwrap();
    f.ctx.add_pending_failover_marker(marker("d3", 3)).unwrap();
    // nothing stale: all remain, no extra persistence
    let before = f.persist.calls.lock().unwrap().len();
    let rem = f.ctx.validate_and_update_failover_markers(|_| Ok(false)).unwrap();
    assert_eq!(rem.len(), 3);
    assert_eq!(f.persist.calls.lock().unwrap().len(), before);
    // d2 stale (completed / deprecated / domain gone)
    let rem = f.ctx.validate_and_update_failover_markers(|m| Ok(m.domain_id == "d2")).unwrap();
    assert_eq!(rem, vec![marker("d1", 1), marker("d3", 3)]);
    assert_eq!(f.ctx.shard_info_snapshot().pending_failover_markers, rem);
    assert_eq!(f.persist.calls.lock().unwrap().len(), before + 1);
    // lookup error propagates
    let err = f
        .ctx
        .validate_and_update_failover_markers(|_| Err(ShardContextError::Other("lookup".into())))
        .unwrap_err();
    assert_eq!(err, ShardContextError::Other("lookup".into()));
}

// Go keeps `FailoverMarkerAttributes.CreationTime` on the pending marker so the
// coordinator's graceful-failover latency uses the original creation time.
#[test]
fn failover_marker_creation_time_is_preserved() {
    let f = fixture();
    let m = FailoverMarker { domain_id: "d1".into(), failover_version: 1, creation_time: 12345 };
    f.ctx.add_pending_failover_marker(m.clone()).unwrap();
    let rem = f.ctx.validate_and_update_failover_markers(|_| Ok(false)).unwrap();
    assert_eq!(rem, vec![m.clone()]);
    assert_eq!(rem[0].creation_time, 12345);
    assert_eq!(f.ctx.shard_info_snapshot().pending_failover_markers[0].creation_time, 12345);
}

// TestCloseShard
#[test]
fn close_shard() {
    let f = fixture();
    assert!(f.ctx.closed_error().is_none());
    f.ctx.close_shard(None);
    assert!(f.ctx.is_closed());
    assert!(matches!(f.ctx.closed_error(), Some(ShardContextError::ShardClosed { closed_at_nanos: 5_000_000_000 })));
    assert_eq!(f.ctx.range_id(), -1);
    wait_closed(&f, 1);
    assert_eq!(f.closed.load(Ordering::SeqCst), 1);
}

// TestCloseShard_AlreadyClosed
#[test]
fn close_shard_already_closed() {
    let f = fixture();
    f.ctx.close_shard(None);
    f.now.store(9_000_000_000, Ordering::SeqCst);
    f.ctx.close_shard(None);
    wait_closed(&f, 1);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(f.closed.load(Ordering::SeqCst), 1);
    // first close time wins
    assert!(matches!(f.ctx.closed_error(), Some(ShardContextError::ShardClosed { closed_at_nanos: 5_000_000_000 })));
}

// TestShardClosedGuard: writes after close fail with ShardClosed.
#[test]
fn shard_closed_guard() {
    let f = fixture();
    f.ctx.close_shard(None);
    let is_closed = |r: Result<(), ShardContextError>| matches!(r, Err(ShardContextError::ShardClosed { .. }));
    assert!(is_closed(f.ctx.update_queue_ack_level(HistoryTaskCategory::Transfer, HistoryTaskKey::immediate(1))));
    assert!(is_closed(f.ctx.update_queue_cluster_ack_level(
        HistoryTaskCategory::Timer,
        "b",
        HistoryTaskKey::scheduled(1, 0)
    )));
    assert!(is_closed(f.ctx.update_domain_notification_version(1)));
    assert!(is_closed(f.ctx.update_transfer_processing_queue_states("b", vec![pq(1)])));
    assert!(is_closed(f.ctx.add_pending_failover_marker(marker("d", 1))));
}

// TestAllocateTimerIDsLocked_*
#[test]
fn allocate_timer_ids_no_tasks() {
    let f = fixture();
    let mut tasks: [(i64, i64); 0] = [];
    f.ctx.allocate_timer_ids("a", &mut tasks).unwrap();
}

#[test]
fn allocate_timer_ids_task_timestamp_before_read_cursor_adjusts() {
    let f = fixture();
    // cursor = now = 5s
    f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, "a");
    f.now.store(1_000_000_000, Ordering::SeqCst); // now before cursor
    let mut tasks = [(0i64, 2_000_000_000i64)];
    f.ctx.allocate_timer_ids("a", &mut tasks).unwrap();
    assert_eq!(tasks[0].1, 5_000_000_000 + DB_TIMESTAMP_MIN_PRECISION_NANOS);
    assert_eq!(tasks[0].0, 80);
}

#[test]
fn allocate_timer_ids_task_timestamp_before_now() {
    let f = fixture();
    let mut tasks = [(0i64, 4_000_000_000i64)];
    f.ctx.allocate_timer_ids("a", &mut tasks).unwrap();
    assert_eq!(tasks[0].1, 5_000_000_000 + DB_TIMESTAMP_MIN_PRECISION_NANOS);
}

#[test]
fn allocate_timer_ids_future_timestamp_is_truncated_not_moved() {
    let f = fixture();
    let mut tasks = [(0i64, 9_000_500_000i64)];
    f.ctx.allocate_timer_ids("a", &mut tasks).unwrap();
    assert_eq!(tasks[0].1, 9_000_000_000);
}

#[test]
fn allocate_timer_ids_multiple_tasks_allocates_all_task_ids() {
    let f = fixture();
    let mut tasks = [(0i64, 9_000_000_000i64), (0, 9_100_000_000), (0, 9_200_000_000)];
    f.ctx.allocate_timer_ids("a", &mut tasks).unwrap();
    let ids: Vec<i64> = tasks.iter().map(|t| t.0).collect();
    assert_eq!(ids, vec![80, 81, 82]);
}

#[test]
fn allocate_timer_ids_task_id_generation_failure_returns_error() {
    let f = fixture();
    for _ in 0..16 {
        f.ctx.generate_task_id().unwrap();
    }
    *f.persist.fail.lock().unwrap() = Some(ShardContextError::Persistence("idgen".into()));
    let mut tasks = [(0i64, 9_000_000_000i64)];
    assert_eq!(
        f.ctx.allocate_timer_ids("a", &mut tasks).unwrap_err(),
        ShardContextError::Persistence("idgen".into())
    );
}

// allocateTransferIDsLocked
#[test]
fn allocate_transfer_ids_sets_ids_visibility_and_read_level() {
    let f = fixture();
    let mut tasks = [(0i64, 0i64), (0, 77)];
    let mut max = 0;
    f.ctx.allocate_transfer_ids(&mut tasks, &mut max).unwrap();
    assert_eq!(tasks[0], (80, 5_000_000_000));
    assert_eq!(tasks[1], (81, 77));
    assert_eq!(max, 81);
}

// update_max_read_level only moves forward
#[test]
fn update_max_read_level_only_moves_forward() {
    let f = fixture();
    f.ctx.update_max_read_level(200);
    assert_eq!(
        f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, ""),
        HistoryTaskKey::immediate(200)
    );
    f.ctx.update_max_read_level(100);
    assert_eq!(
        f.ctx.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Transfer, ""),
        HistoryTaskKey::immediate(200)
    );
}

// TestGetCurrentTimeLocked
#[test]
fn get_current_time() {
    let f = fixture();
    assert_eq!(f.ctx.get_current_time("a"), 5_000_000_000);
    assert_eq!(f.ctx.get_current_time("b"), 0);
    f.ctx.set_current_time("b", 100);
    f.ctx.set_current_time("b", 50); // never regresses
    assert_eq!(f.ctx.get_current_time("b"), 100);
}

#[test]
#[should_panic(expected = "Cannot set current time for current cluster")]
fn set_current_time_for_current_cluster_panics() {
    fixture().ctx.set_current_time("a", 1);
}

// TestPrefetchClusterTimesLocked: remote times seeded from cluster timer ack levels.
#[test]
fn remote_cluster_time_seeded_from_ack_levels() {
    let mut cluster_timer = HashMap::new();
    cluster_timer.insert("b".to_string(), 3_000_000_000i64);
    let f = fixture_with(
        0,
        ShardInfoData {
            shard_id: 3,
            range_id: 4,
            owner: "host1".into(),
            timer_ack_level: 1_000_000_000,
            cluster_timer_ack_level: cluster_timer,
            ..Default::default()
        },
    )
    .ok()
    .unwrap();
    assert_eq!(f.ctx.get_current_time("b"), 3_000_000_000);
}

// TestFailoverMarkerSkipReason-adjacent: lookup helper + sharded DLQ writer (TestShardedHistoryTaskDLQWriterAckLevel).
#[test]
fn sharded_dlq_writer_dedups_per_key_and_retries_on_failure() {
    let cache = ShardedDlqAckLevelCache::new();
    let key = |c: i32| DlqAckLevelKey {
        domain_id: "d".into(),
        cluster_attribute_scope: "s".into(),
        cluster_attribute_name: "n".into(),
        task_category_id: c,
    };
    let n = AtomicUsize::new(0);
    // failure is not cached
    assert!(cache.create_if_not_exists(key(1), || Err::<(), _>("x")).is_err());
    for _ in 0..2 {
        cache
            .create_if_not_exists::<()>(key(1), || {
                n.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
    }
    cache
        .create_if_not_exists::<()>(key(2), || {
            n.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    assert_eq!(n.load(Ordering::SeqCst), 2);
}

#[test]
fn history_server_key_by_shard_id_is_rune_string() {
    assert_eq!(history_server_key_by_shard_id(65), "A");
    assert_eq!(history_server_key_by_shard_id(0x1F600), "\u{1F600}");
}
