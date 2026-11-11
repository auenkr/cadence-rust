//! Port of `go/service/history/replication/dlq_handler_test.go`.

use super::deps::*;
use super::dlq_handler::*;
use super::task_executor::TaskExecutor;
use super::test_support::*;
use async_trait::async_trait;
use cadence_api::admin::{
    GetDLQReplicationMessagesRequest, GetReplicationMessagesRequest, GetReplicationMessagesResponse,
    ReplicationTaskInfo,
};
use cadence_api::types::ReplicationTask;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SRC: &str = "test";

#[derive(Default)]
struct FakeAdmin {
    result: Mutex<Option<Result<Vec<ReplicationTask>>>>,
    calls: Mutex<Vec<Vec<ReplicationTaskInfo>>>,
}

#[async_trait]
impl AdminClient for FakeAdmin {
    async fn get_replication_messages(
        &self,
        _req: GetReplicationMessagesRequest,
        _t: Duration,
    ) -> Result<GetReplicationMessagesResponse> {
        unimplemented!()
    }
    async fn get_dlq_replication_messages(
        &self,
        req: GetDLQReplicationMessagesRequest,
    ) -> Result<Vec<ReplicationTask>> {
        self.calls.lock().unwrap().push(req.task_infos);
        self.result.lock().unwrap().clone().unwrap_or(Ok(vec![]))
    }
}

struct NoHistory;
#[async_trait]
impl HistoryClient for NoHistory {
    async fn sync_activity(&self, _r: SyncActivityRequest, _t: Duration) -> Result<()> {
        Ok(())
    }
    async fn replicate_events_v2(&self, _r: ReplicateEventsV2Request, _t: Duration) -> Result<()> {
        Ok(())
    }
}

struct Bean {
    admin: Arc<FakeAdmin>,
}
impl ClientBean for Bean {
    fn history_client(&self) -> Arc<dyn HistoryClient> {
        Arc::new(NoHistory)
    }
    fn remote_admin_client(&self, cluster: &str) -> Result<Arc<dyn AdminClient>> {
        if cluster == SRC {
            Ok(self.admin.clone())
        } else {
            Err(ReplicationError::Other(cluster.to_string()))
        }
    }
    fn remote_frontend_client(&self, c: &str) -> Result<Arc<dyn FrontendClient>> {
        Err(ReplicationError::Other(c.to_string()))
    }
}

struct Shard {
    exec: Arc<FakeExec>,
    bean: Arc<Bean>,
}

#[async_trait]
impl ShardContext for Shard {
    fn shard_id(&self) -> i32 {
        0
    }
    fn current_cluster_name(&self) -> String {
        CLUSTER_A.into()
    }
    fn remote_cluster_names(&self) -> Vec<String> {
        vec![SRC.into()]
    }
    fn time_source(&self) -> Arc<dyn TimeSource> {
        Arc::new(FixedTime(0))
    }
    fn domain_cache(&self) -> Arc<dyn DomainCache> {
        Arc::new(FakeDomains::default())
    }
    fn execution_manager(&self) -> Arc<dyn ExecutionManager> {
        self.exec.clone()
    }
    fn client_bean(&self) -> Arc<dyn ClientBean> {
        self.bean.clone()
    }
    fn cluster_name_for_failover_version(&self, _v: i64) -> Result<String> {
        Ok(CLUSTER_A.into())
    }
    fn get_queue_cluster_ack_level(&self, _c: &str) -> i64 {
        0
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, _c: &str) -> i64 {
        0
    }
    fn update_queue_cluster_ack_level(&self, _c: &str, _a: i64) -> Result<()> {
        Ok(())
    }
    fn adding_pending_failover_marker(
        &self,
        _m: cadence_api::types::FailoverMarkerAttributes,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct FakeExecutor {
    err: Option<ReplicationError>,
    executed: Mutex<Vec<ReplicationTask>>,
}

#[async_trait]
impl TaskExecutor for FakeExecutor {
    async fn execute(&self, t: &ReplicationTask, _force: bool) -> (&'static str, Result<()>) {
        self.executed.lock().unwrap().push(t.clone());
        ("", self.err.clone().map_or(Ok(()), Err))
    }
}

struct Fx {
    exec: Arc<FakeExec>,
    admin: Arc<FakeAdmin>,
    executor: Arc<FakeExecutor>,
    handler: Arc<DlqHandlerImpl>,
}

fn fixture_with(executor: FakeExecutor) -> Fx {
    let exec = Arc::new(FakeExec::default());
    let admin = Arc::new(FakeAdmin::default());
    let shard = Arc::new(Shard { exec: exec.clone(), bean: Arc::new(Bean { admin: admin.clone() }) });
    let executor = Arc::new(executor);
    let mut m: HashMap<String, Arc<dyn TaskExecutor>> = HashMap::new();
    m.insert(SRC.into(), executor.clone());
    let handler = DlqHandlerImpl::new(shard, Arc::new(NoopMetrics), m);
    Fx { exec, admin, executor, handler }
}

fn fixture() -> Fx {
    fixture_with(FakeExecutor::default())
}

fn dlq(id: i64, task: Option<ReplicationTask>) -> ReplicationDlqTask {
    ReplicationDlqTask {
        info: Some(DlqTaskInfo {
            domain_id: "domainID".into(),
            workflow_id: "workflowID".into(),
            run_id: "runID".into(),
            task_id: id,
            ..Default::default()
        }),
        task,
    }
}

fn rt(id: i64) -> ReplicationTask {
    ReplicationTask { source_task_id: Some(id), ..Default::default() }
}

fn dlq_resp(tasks: Vec<ReplicationDlqTask>, token: &[u8]) -> Result<GetReplicationTasksFromDlqResponse> {
    Ok(GetReplicationTasksFromDlqResponse { tasks, next_page_token: token.to_vec() })
}

#[tokio::test]
async fn start_stop_is_idempotent() {
    let f = fixture();
    f.handler.start();
    f.handler.start();
    f.handler.stop();
    f.handler.stop();
    // stop without start is a no-op
    let g = fixture();
    g.handler.stop();
}

#[tokio::test]
async fn get_message_count_fetches_when_missing_and_caches() {
    let f = fixture();
    *f.exec.dlq_size.lock().unwrap() = Some(Ok(3));
    let got = f.handler.get_message_count(false).await.unwrap();
    assert_eq!(got, HashMap::from([(SRC.to_string(), 3)]));
    // cached: size change not visible without force
    *f.exec.dlq_size.lock().unwrap() = Some(Ok(7));
    assert_eq!(f.handler.get_message_count(false).await.unwrap()[SRC], 3);
    assert_eq!(f.handler.get_message_count(true).await.unwrap()[SRC], 7);
}

#[tokio::test]
async fn get_message_count_zero_size_is_omitted() {
    let f = fixture();
    *f.exec.dlq_size.lock().unwrap() = Some(Ok(0));
    assert!(f.handler.get_message_count(true).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_message_count_error_is_returned() {
    let f = fixture();
    *f.exec.dlq_size.lock().unwrap() = Some(Err(ReplicationError::Other("boom".into())));
    let err = f.handler.get_message_count(false).await.unwrap_err();
    assert_eq!(err.to_string(), "boom");
}

#[tokio::test]
async fn read_messages_ok() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(1)]));
    let (tasks, infos, token) = f.handler.read_messages(SRC, 1, 1, vec![]).await.unwrap();
    assert!(token.is_empty());
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].domain_id, "domainID");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].as_ref().unwrap().source_task_id, Some(1));
    let calls = f.exec.dlq_read_calls.lock().unwrap();
    assert_eq!(calls[0], (0, SRC.to_string(), 0, 2, 1, vec![]));
}

#[tokio::test]
async fn read_messages_with_ack_level_ok() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(123, None)], b"token"));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(123)]));
    let (tasks, infos, token) =
        f.handler.read_messages(SRC, 123, 12, b"token".to_vec()).await.unwrap();
    assert_eq!(tasks[0].as_ref().unwrap(), &rt(123));
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].workflow_id, "workflowID");
    assert_eq!(infos[0].run_id, "runID");
    assert_eq!(token, b"token".to_vec());
    assert_eq!(
        f.exec.dlq_read_calls.lock().unwrap()[0],
        (0, SRC.to_string(), 0, 124, 12, b"token".to_vec())
    );
}

#[tokio::test]
async fn read_messages_get_tasks_from_dlq_failed() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() =
        Some(Err(ReplicationError::Other("GetReplicationTasksFromDLQFailed".into())));
    let err = f.handler.read_messages(SRC, 123, 12, b"token".to_vec()).await.unwrap_err();
    assert_eq!(err.to_string(), "GetReplicationTasksFromDLQFailed");
}

#[tokio::test]
async fn read_messages_invalid_cluster() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None)], b""));
    let err = f.handler.read_messages("invalidCluster", 123, 12, b"token".to_vec()).await.unwrap_err();
    assert!(err.to_string().contains("invalidCluster"));
}

#[tokio::test]
async fn read_messages_get_dlq_replication_messages_failed() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None)], b""));
    *f.admin.result.lock().unwrap() =
        Some(Err(ReplicationError::Other("GetDLQReplicationMessagesFailed".into())));
    let err = f.handler.read_messages(SRC, 123, 12, b"token".to_vec()).await.unwrap_err();
    assert_eq!(err.to_string(), "GetDLQReplicationMessagesFailed");
}

#[tokio::test]
async fn read_messages_local_blob_needs_no_remote_call() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(123, Some(rt(123)))], b""));
    let (tasks, infos, _) = f.handler.read_messages(SRC, 123, 12, vec![]).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].as_ref().unwrap().source_task_id, Some(123));
    assert_eq!(infos.len(), 1);
    assert!(f.admin.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn read_messages_corrupt_blob_falls_back_to_cross_cluster() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(123, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(123)]));
    let (tasks, infos, _) = f.handler.read_messages(SRC, 123, 12, vec![]).await.unwrap();
    assert_eq!(tasks[0].as_ref().unwrap().source_task_id, Some(123));
    assert_eq!(infos.len(), 1);
    assert_eq!(f.admin.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn read_messages_mixed_local_and_remote() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() =
        Some(dlq_resp(vec![dlq(1, Some(rt(1))), dlq(2, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(2)]));
    let (tasks, infos, _) = f.handler.read_messages(SRC, 10, 12, vec![]).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].as_ref().unwrap().source_task_id, Some(1));
    assert_eq!(tasks[1].as_ref().unwrap().source_task_id, Some(2));
    assert_eq!(infos.len(), 2);
    // only task 2 is requested remotely
    let calls = f.admin.calls.lock().unwrap();
    assert_eq!(calls[0].len(), 1);
    assert_eq!(calls[0][0].task_id, 2);
}

#[tokio::test]
async fn read_messages_missing_from_remote() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![]));
    let (tasks, infos, _) = f.handler.read_messages(SRC, 10, 12, vec![]).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].is_none());
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].domain_id, "domainID");
}

#[tokio::test]
async fn purge_messages_success_and_error() {
    let f = fixture();
    f.handler.purge_messages(SRC, 1).await.unwrap();
    assert_eq!(f.exec.dlq_delete_calls.lock().unwrap()[0], (0, SRC.to_string(), 0, 2));
    *f.exec.dlq_delete_result.lock().unwrap() = Some(ReplicationError::Other("error".into()));
    assert!(f.handler.purge_messages(SRC, 1).await.is_err());
}

#[tokio::test]
async fn merge_messages_ok() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None), dlq(2, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(1)]));
    let token = f.handler.merge_messages(SRC, 2, 1, vec![]).await.unwrap();
    assert!(token.is_empty());
    assert_eq!(f.executor.executed.lock().unwrap().len(), 1);
    assert_eq!(f.exec.dlq_read_calls.lock().unwrap()[0], (0, SRC.to_string(), 0, 3, 1, vec![]));
    // purged through the highest raw task id (2), even though task 2 was not hydrated
    assert_eq!(f.exec.dlq_delete_calls.lock().unwrap()[0], (0, SRC.to_string(), 0, 3));
}

#[tokio::test]
async fn merge_messages_invalid_cluster() {
    let f = fixture();
    let err = f.handler.merge_messages("invalid", 1, 1, vec![]).await.unwrap_err();
    assert!(matches!(err, ReplicationError::BadRequest(ref m) if m == "Invalid target cluster name."));
}

#[tokio::test]
async fn merge_messages_get_tasks_from_dlq_failed() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() =
        Some(Err(ReplicationError::Other("GetReplicationTasksFromDLQFailed".into())));
    let err = f.handler.merge_messages(SRC, 1, 1, vec![]).await.unwrap_err();
    assert_eq!(err.to_string(), "GetReplicationTasksFromDLQFailed");
}

#[tokio::test]
async fn merge_messages_range_delete_failed() {
    let f = fixture();
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![], b""));
    *f.exec.dlq_delete_result.lock().unwrap() =
        Some(ReplicationError::Other("RangeDeleteReplicationTaskFromDLQFailed".into()));
    let err = f.handler.merge_messages(SRC, 1, 1, vec![]).await.unwrap_err();
    assert_eq!(err.to_string(), "RangeDeleteReplicationTaskFromDLQFailed");
}

#[tokio::test]
async fn merge_messages_execute_failed() {
    let f = fixture_with(FakeExecutor {
        err: Some(ReplicationError::Other("executeFailed".into())),
        ..Default::default()
    });
    *f.exec.dlq_read_result.lock().unwrap() = Some(dlq_resp(vec![dlq(1, None)], b""));
    *f.admin.result.lock().unwrap() = Some(Ok(vec![rt(1)]));
    let err = f.handler.merge_messages(SRC, 2, 1, vec![]).await.unwrap_err();
    assert_eq!(err.to_string(), "executeFailed");
    // nothing purged when execution fails
    assert!(f.exec.dlq_delete_calls.lock().unwrap().is_empty());
}
