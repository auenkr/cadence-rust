//! Port of `go/service/history/replication/dynamic_task_batch_sizer_test.go`.

use super::deps::{NoopMetrics, ReplicationError};
use super::dynamic_task_batch_sizer::*;
use super::task_ack_manager::GetTasksResult;
use super::test_support::*;
use cadence_api::admin::ReplicationMessages;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn state(prev: i64, last: i64, tasks: usize, has_more: bool, shrunk: bool) -> GetTasksResult {
    GetTasksResult {
        previous_read_task_id: prev,
        last_read_task_id: last,
        msgs: ReplicationMessages { has_more, ..Default::default() },
        task_infos: (0..tasks as i64).map(unknown_task).collect(),
        is_shrunk: shrunk,
    }
}

#[test]
fn dynamic_task_batch_sizer() {
    // range [0, 100, ..., 1000], start point 500
    let cfg = TestConfig::with_batch(0, 1000, 11);
    let sizer = DynamicTaskBatchSizerImpl::new(0, cfg.clone(), Arc::new(NoopMetrics));
    let err = ReplicationError::Other("error".into());

    // initial batch size
    assert_eq!(sizer.value(), 500);

    // an error occurred 3 times
    for want in [400, 300, 200] {
        sizer.analyse(Some(&err), None);
        assert_eq!(sizer.value(), want);
    }

    // read level changed 2 times and there are tasks in db
    let s = state(0, 10, 10, true, false);
    sizer.analyse(None, Some(&s));
    assert_eq!(sizer.value(), 300);
    sizer.analyse(None, Some(&s));
    assert_eq!(sizer.value(), 400);

    // read level changed, tasks in db, but shrunk
    sizer.analyse(None, Some(&state(0, 10, 10, true, true)));
    assert_eq!(sizer.value(), 300);

    // read level has not changed, replication tasks are returned
    sizer.analyse(None, Some(&state(10, 10, 10, true, false)));
    assert_eq!(sizer.value(), 200);

    // read level has not changed, tasks are not returned
    sizer.analyse(None, Some(&state(10, 10, 0, false, false)));
    assert_eq!(sizer.value(), 200);

    // read level changed 2 times and there are no tasks in db
    let s = state(0, 10, 0, false, false);
    sizer.analyse(None, Some(&s));
    assert_eq!(sizer.value(), 200);
    sizer.analyse(None, Some(&s));
    assert_eq!(sizer.value(), 200);

    // an error occurred 3 times
    for want in [100, 0, 0] {
        sizer.analyse(Some(&err), None);
        assert_eq!(sizer.value(), want);
    }

    // max has been changed to 500: range [0, 50, ..., 500], start point 250
    cfg.max_batch.store(500, Ordering::SeqCst);
    assert_eq!(sizer.value(), 250);

    // shrunk 10 times
    for want in [200, 150, 100, 50, 0, 0, 0, 0, 0, 0] {
        sizer.analyse(None, Some(&state(0, 0, 10, false, true)));
        assert_eq!(sizer.value(), want);
    }

    // read level has changed and there are tasks in db
    sizer.analyse(None, Some(&state(0, 10, 10, true, false)));
    assert_eq!(sizer.value(), 50);

    // read level has changed and there are no more tasks in db
    sizer.analyse(None, Some(&state(0, 10, 5, false, false)));
    assert_eq!(sizer.value(), 50);

    // read level has not changed, tasks returned, previously tasks were returned
    sizer.analyse(None, Some(&state(10, 10, 5, false, false)));
    assert_eq!(sizer.value(), 0);
}

#[test]
fn linear_iterator_degenerate_ranges() {
    // min > max swaps; tiny ranges collapse to [min, max]
    let mut it = LinearIterator::new(10, 0, 5);
    assert_eq!(it.value(), 5);
    let mut it2 = LinearIterator::new(0, 1, 5);
    assert_eq!(it2.next(), 1);
    assert_eq!(it2.previous(), 0);
    assert_eq!(it.next(), 7);
    it.reset();
    assert_eq!(it.value(), 5);
    // range smaller than step count uses unit steps
    let mut it3 = LinearIterator::new(0, 3, 10);
    assert_eq!(it3.value(), 1);
    assert_eq!(it3.next(), 2);
    assert_eq!(it3.next(), 3);
    assert_eq!(it3.next(), 3);
}
