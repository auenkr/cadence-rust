//! Port of engineimpl/dlq_operations_test.go (`TestReadDLQMessages_DropsNilTasks`).
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::{admin::*, enums::DLQType, types::ReplicationTask};

use crate::engine::deps::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;

struct FakeDlqHandler {
    tasks: Vec<Option<ReplicationTask>>,
    info: Vec<ReplicationTaskInfo>,
}

#[async_trait]
impl ReplicationDLQHandler for FakeDlqHandler {
    async fn get_message_count(&self, _: bool) -> HResult<std::collections::HashMap<String, i64>> {
        Ok(Default::default())
    }
    async fn read_messages(&self, _: &str, _: i64, _: i32, _: &[u8]) -> HResult<DLQRead> {
        Ok((self.tasks.clone(), self.info.clone(), Vec::new()))
    }
    async fn purge_messages(&self, _: &str, _: i64) -> HResult<()> {
        Ok(())
    }
    async fn merge_messages(&self, _: &str, _: i64, _: i32, _: &[u8]) -> HResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

fn task(id: i64) -> ReplicationTask {
    ReplicationTask { source_task_id: Some(id), ..Default::default() }
}

fn info(id: i64) -> ReplicationTaskInfo {
    ReplicationTaskInfo { task_id: id, ..Default::default() }
}

async fn run_read(tasks: Vec<Option<ReplicationTask>>, infos: Vec<ReplicationTaskInfo>) -> ReadDLQMessagesResponse {
    let mut engine = nop_engine();
    engine.replication_dlq_handler = Arc::new(FakeDlqHandler { tasks, info: infos });
    let request = ReadDLQMessagesRequest { dlq_type: Some(DLQType::Replication), source_cluster: "source".into(), ..Default::default() };
    engine.read_dlq_messages(&request).await.expect("read_dlq_messages")
}

fn ids(tasks: &[ReplicationTask]) -> Vec<Option<i64>> {
    tasks.iter().map(|t| t.source_task_id).collect()
}

#[tokio::test]
async fn read_dlq_messages_drops_nil_tasks_no_tasks() {
    let resp = run_read(vec![], vec![]).await;
    assert!(resp.replication_tasks.is_empty());
    assert!(resp.replication_tasks_info.is_empty());
}

#[tokio::test]
async fn read_dlq_messages_drops_nil_tasks_all_hydrated() {
    let resp = run_read(vec![Some(task(1)), Some(task(3))], vec![info(1), info(3)]).await;
    assert_eq!(ids(&resp.replication_tasks), vec![Some(1), Some(3)]);
    assert_eq!(resp.replication_tasks_info.len(), 2);
}

#[tokio::test]
async fn read_dlq_messages_drops_nil_tasks_nil_task_is_dropped() {
    let resp = run_read(vec![Some(task(1)), None, Some(task(3))], vec![info(1), info(2), info(3)]).await;
    assert_eq!(ids(&resp.replication_tasks), vec![Some(1), Some(3)]);
    // The full set of tasks is still represented by their info entries.
    assert_eq!(resp.replication_tasks_info.iter().map(|i| i.task_id).collect::<Vec<_>>(), vec![1, 2, 3]);
    // The response must be serializable.
    serde_json::to_string(&resp).expect("serialize response");
}

#[tokio::test]
async fn read_dlq_messages_drops_nil_tasks_only_nil_tasks() {
    let resp = run_read(vec![None, None], vec![info(1), info(2)]).await;
    assert!(resp.replication_tasks.is_empty());
    assert_eq!(resp.replication_tasks_info.len(), 2);
}

#[tokio::test]
async fn read_dlq_messages_defaults_dlq_type_to_invalid() {
    let mut engine = nop_engine();
    engine.replication_dlq_handler = Arc::new(FakeDlqHandler { tasks: vec![], info: vec![] });
    let resp = engine.read_dlq_messages(&ReadDLQMessagesRequest::default()).await.unwrap();
    assert_eq!(resp.dlq_type, Some(DLQType::Invalid));
}

#[tokio::test]
async fn count_purge_merge_dlq_delegate_to_handler() {
    let mut engine = nop_engine();
    engine.replication_dlq_handler = Arc::new(FakeDlqHandler { tasks: vec![], info: vec![] });
    assert!(engine.count_dlq_messages(true).await.unwrap().is_empty());
    engine.purge_dlq_messages(&PurgeDLQMessagesRequest::default()).await.unwrap();
    let merged = engine.merge_dlq_messages(&MergeDLQMessagesRequest::default()).await.unwrap();
    assert!(merged.next_page_token.is_empty());
}

#[test]
fn convert_to_replication_task_maps_types() {
    use crate::engine::dlq_operations::convert_to_replication_task;
    for (task_type, ok) in [(0, true), (1, true), (2, true), (9, false)] {
        let info = ReplicationTaskInfo { task_type, ..Default::default() };
        assert_eq!(convert_to_replication_task(&info).is_ok(), ok, "task_type {task_type}");
    }
}
