//! Port of `go/service/history/replication/task_reader_test.go`.

use super::deps::ReplicationError;
use super::task_reader::*;
use super::test_support::*;
use std::sync::Arc;

const SHARD: i32 = 7;
const BATCH: usize = 50;

#[tokio::test]
async fn read_replication_tasks_first_read_uses_batch_size() {
    let exec = Arc::new(FakeExec::default());
    let tasks = vec![history_task(50), history_task(51)];
    *exec.read_result.lock().unwrap() = Some(Ok((tasks.clone(), false)));
    let reader = TaskReader::new(SHARD, exec.clone());
    let (got, more) = reader.read(50, 100, BATCH).await.unwrap();
    assert_eq!(got, tasks);
    assert!(!more);
    // inclusive min = readLevel+1, exclusive max = maxReadLevel+1
    assert_eq!(*exec.read_calls.lock().unwrap(), vec![(SHARD, 51, 101, BATCH)]);
}

#[tokio::test]
async fn do_not_hit_persistence_when_no_task_will_be_returned() {
    let exec = Arc::new(FakeExec::default());
    let reader = TaskReader::new(SHARD, exec.clone());
    let (got, more) = reader.read(50, 50, BATCH).await.unwrap();
    assert!(got.is_empty());
    assert!(!more);
    let (got, _) = reader.read(60, 50, BATCH).await.unwrap();
    assert!(got.is_empty());
    assert!(exec.read_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn read_propagates_persistence_error_and_has_more() {
    let exec = Arc::new(FakeExec::default());
    *exec.read_result.lock().unwrap() = Some(Err(ReplicationError::Other("db down".into())));
    let reader = TaskReader::new(SHARD, exec.clone());
    assert_eq!(reader.read(0, 10, BATCH).await.unwrap_err().to_string(), "db down");
    *exec.read_result.lock().unwrap() = Some(Ok((vec![history_task(1)], true)));
    assert!(reader.read(0, 10, BATCH).await.unwrap().1);
}
