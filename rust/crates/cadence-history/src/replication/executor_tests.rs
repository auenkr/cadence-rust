//! Port of `go/service/history/replication/task_executor_test.go`.

use super::deps::*;
use super::task_executor::*;
use super::test_support::*;
use async_trait::async_trait;
use cadence_api::enums::ReplicationTaskType;
use cadence_api::errors::RetryTaskV2Error;
use cadence_api::types::{
    FailoverMarkerAttributes, HistoryTaskV2Attributes, ReplicationTask, SyncActivityTaskAttributes,
    WorkflowExecution,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SOURCE: &str = "source-cluster";
const DOMAIN: &str = "11111111-1111-1111-1111-111111111111";
const RUN: &str = "22222222-2222-2222-2222-222222222222";
const SHARD0_WF: &str = "6d89f939-e6a4-4c26-a0ed-626ce27bcc9c"; // belongs to shard 0 of 2
const SHARD1_WF: &str = "abc"; // belongs to shard 1 of 2

type Resend = (String, String, String, String, Option<i64>, Option<i64>, Option<i64>, Option<i64>);

#[derive(Default)]
struct Calls {
    sync: Mutex<Vec<SyncActivityRequest>>,
    repl: Mutex<Vec<ReplicateEventsV2Request>>,
}

/// Scripted `engine.Engine` / `history.Client`: pops one outcome per call (default Ok).
#[derive(Default)]
struct Target {
    calls: Calls,
    script: Mutex<std::collections::VecDeque<Result<()>>>,
}
impl Target {
    fn next(&self) -> Result<()> {
        self.script.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }
}
struct EngineFake(Arc<Target>);
#[async_trait]
impl Engine for EngineFake {
    async fn sync_shard_status(&self, _r: SyncShardStatusRequest, _t: Duration) -> Result<()> {
        unimplemented!()
    }
    async fn sync_activity(&self, r: SyncActivityRequest, _t: Duration) -> Result<()> {
        self.0.calls.sync.lock().unwrap().push(r);
        self.0.next()
    }
    async fn replicate_events_v2(&self, r: ReplicateEventsV2Request, _t: Duration) -> Result<()> {
        self.0.calls.repl.lock().unwrap().push(r);
        self.0.next()
    }
}
struct ClientFake(Arc<Target>);
#[async_trait]
impl HistoryClient for ClientFake {
    async fn sync_activity(&self, r: SyncActivityRequest, _t: Duration) -> Result<()> {
        self.0.calls.sync.lock().unwrap().push(r);
        self.0.next()
    }
    async fn replicate_events_v2(&self, r: ReplicateEventsV2Request, _t: Duration) -> Result<()> {
        self.0.calls.repl.lock().unwrap().push(r);
        self.0.next()
    }
}
struct BeanFake(Arc<Target>);
impl ClientBean for BeanFake {
    fn history_client(&self) -> Arc<dyn HistoryClient> {
        Arc::new(ClientFake(self.0.clone()))
    }
    fn remote_admin_client(&self, _c: &str) -> Result<Arc<dyn AdminClient>> {
        unimplemented!()
    }
    fn remote_frontend_client(&self, _c: &str) -> Result<Arc<dyn FrontendClient>> {
        unimplemented!()
    }
}

#[derive(Default)]
struct Resender {
    calls: Mutex<Vec<Resend>>,
    result: Mutex<Option<ReplicationError>>,
}
#[async_trait]
impl HistoryResender for Resender {
    async fn send_single_workflow_history(
        &self,
        remote: &str,
        d: &str,
        w: &str,
        r: &str,
        a: Option<i64>,
        b: Option<i64>,
        c: Option<i64>,
        e: Option<i64>,
    ) -> Result<()> {
        self.calls.lock().unwrap().push((remote.into(), d.into(), w.into(), r.into(), a, b, c, e));
        match self.result.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

struct Fixture {
    exec: TaskExecutorImpl,
    engine: Arc<Target>,
    client: Arc<Target>,
    resender: Arc<Resender>,
    shard: Arc<FakeShard>,
}

fn fixture_with(domains: FakeDomains) -> Fixture {
    let engine = Arc::new(Target::default());
    let client = Arc::new(Target::default());
    let resender = Arc::new(Resender::default());
    let mut shard = FakeShard::new();
    shard.id = 0;
    shard.domains = domains;
    shard.bean = Arc::new(BeanFake(client.clone()));
    let shard = Arc::new(shard);
    let exec = TaskExecutorImpl::new(
        SOURCE.into(),
        shard.clone(),
        2,
        Arc::new(shard.domains.clone()),
        resender.clone(),
        Arc::new(EngineFake(engine.clone())),
        Arc::new(NoopMetrics),
    );
    Fixture { exec, engine, client, resender, shard }
}
fn fixture() -> Fixture {
    fixture_with(FakeDomains::test_domain())
}

fn retry_err() -> RetryTaskV2Error {
    RetryTaskV2Error {
        domain_id: "test-domain-id".into(),
        workflow_id: "test-wf-id".into(),
        run_id: "test-run-id".into(),
        start_event_id: Some(11),
        start_event_version: Some(100),
        end_event_id: Some(19),
        end_event_version: Some(102),
        ..Default::default()
    }
}
fn expected_resend() -> Resend {
    (
        SOURCE.into(),
        "test-domain-id".into(),
        "test-wf-id".into(),
        "test-run-id".into(),
        Some(11),
        Some(100),
        Some(19),
        Some(102),
    )
}

fn activity_task(wf: &str) -> (ReplicationTask, SyncActivityRequest) {
    let attrs = SyncActivityTaskAttributes {
        domain_id: Some(DOMAIN.into()),
        workflow_id: Some(wf.into()),
        run_id: Some(RUN.into()),
        ..Default::default()
    };
    (
        ReplicationTask {
            task_type: Some(ReplicationTaskType::SyncActivity),
            sync_activity_task_attributes: Some(attrs.clone()),
            ..Default::default()
        },
        SyncActivityRequest { task: attrs },
    )
}
fn history_task_v2(wf: &str) -> (ReplicationTask, ReplicateEventsV2Request) {
    (
        ReplicationTask {
            task_type: Some(ReplicationTaskType::HistoryV2),
            history_task_v2_attributes: Some(HistoryTaskV2Attributes {
                domain_id: Some(DOMAIN.into()),
                workflow_id: Some(wf.into()),
                run_id: Some(RUN.into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ReplicateEventsV2Request {
            domain_uuid: Some(DOMAIN.into()),
            workflow_execution: WorkflowExecution { workflow_id: wf.into(), run_id: RUN.into() },
            ..Default::default()
        },
    )
}

fn sync_req_eq(a: &SyncActivityRequest, b: &SyncActivityRequest) -> bool {
    a.task == b.task
}
fn repl_req_eq(a: &ReplicateEventsV2Request, b: &ReplicateEventsV2Request) -> bool {
    a.domain_uuid == b.domain_uuid
        && a.workflow_execution == b.workflow_execution
        && a.version_history_items == b.version_history_items
        && a.events == b.events
        && a.new_run_events == b.new_run_events
}

// TestConvertRetryTaskV2Error_OK / _NotOK
#[test]
fn convert_retry_task_v2_error() {
    assert!(to_retry_task_v2_error(&ReplicationError::RetryTaskV2(RetryTaskV2Error::default())).is_some());
    assert!(to_retry_task_v2_error(&ReplicationError::BadRequest(String::new())).is_none());
}

// TestFilterTask, TestFilterTask_Error, TestFilterTask_EnforceApply (through execute with a
// force_apply flag, since filterTask is private)
#[tokio::test]
async fn filter_task() {
    // domain replicates to the current cluster -> task applied
    let f = fixture();
    let (task, _) = activity_task(SHARD0_WF);
    // the test domain id is registered by FakeDomains::test_domain
    let (_, r) = f.exec.execute(&task, false).await;
    assert!(r.is_ok());
    assert_eq!(f.engine.calls.sync.lock().unwrap().len(), 1);

    // domain is not replicated to the current cluster -> filtered, engine untouched
    let mut domains = FakeDomains::default();
    domains.0.insert(
        DOMAIN.into(),
        DomainEntry { name: "d".into(), replication_clusters: vec![CLUSTER_B.into()] },
    );
    let f = fixture_with(domains);
    let (_, r) = f.exec.execute(&task, false).await;
    assert!(r.is_ok());
    assert!(f.engine.calls.sync.lock().unwrap().is_empty());
}

#[tokio::test]
async fn filter_task_error() {
    let f = fixture_with(FakeDomains::default());
    let (task, _) = activity_task(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, false).await;
    assert!(r.is_err());
    assert!(f.engine.calls.sync.lock().unwrap().is_empty());
}

#[tokio::test]
async fn filter_task_enforce_apply() {
    let f = fixture_with(FakeDomains::default());
    let (task, req) = activity_task(SHARD0_WF);
    let (scope, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(scope, SYNC_ACTIVITY_TASK_SCOPE);
    assert!(sync_req_eq(&f.engine.calls.sync.lock().unwrap()[0], &req));
}

// TestProcessTask_SyncActivityReplicationTask_SameShardID
#[tokio::test]
async fn sync_activity_same_shard() {
    let f = fixture();
    let (task, req) = activity_task(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    let calls = f.engine.calls.sync.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(sync_req_eq(&calls[0], &req));
    assert!(f.client.calls.sync.lock().unwrap().is_empty());
}

fn script(t: &Target, v: Vec<Result<()>>) {
    *t.script.lock().unwrap() = v.into();
}

// ..._RetryTaskV2Error_Success
#[tokio::test]
async fn sync_activity_retry_task_v2_error_success() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err())), Ok(())]);
    let (task, _) = activity_task(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(f.engine.calls.sync.lock().unwrap().len(), 2);
    assert_eq!(*f.resender.calls.lock().unwrap(), vec![expected_resend()]);
}

// ..._RetryTaskV2Error_SkipTask
#[tokio::test]
async fn sync_activity_retry_task_v2_error_skip_task() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err()))]);
    *f.resender.result.lock().unwrap() = Some(ReplicationError::SkipTask);
    let (task, _) = activity_task(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(f.engine.calls.sync.lock().unwrap().len(), 1);
    assert_eq!(*f.resender.calls.lock().unwrap(), vec![expected_resend()]);
}

// ..._RetryTaskV2Error_Error
#[tokio::test]
async fn sync_activity_retry_task_v2_error_error() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err()))]);
    *f.resender.result.lock().unwrap() = Some(ReplicationError::Other("some error".into()));
    let (task, _) = activity_task(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    // the original RetryTaskV2 error is returned
    match r.unwrap_err() {
        ReplicationError::RetryTaskV2(e) => assert_eq!(e, retry_err()),
        other => panic!("expected RetryTaskV2, got {other:?}"),
    }
    assert_eq!(f.engine.calls.sync.lock().unwrap().len(), 1);
}

// TestProcessTask_HistoryV2ReplicationTask_SameShardID
#[tokio::test]
async fn history_v2_same_shard() {
    let f = fixture();
    let (task, req) = history_task_v2(SHARD0_WF);
    let (scope, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(scope, HISTORY_REPLICATION_V2_TASK_SCOPE);
    let calls = f.engine.calls.repl.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(repl_req_eq(&calls[0], &req));
}

// ..._RetryTaskErr_Success
#[tokio::test]
async fn history_v2_retry_task_err_success() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err())), Ok(())]);
    let (task, _) = history_task_v2(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(f.engine.calls.repl.lock().unwrap().len(), 2);
    assert_eq!(*f.resender.calls.lock().unwrap(), vec![expected_resend()]);
}

// ..._RetryTaskErr_SkipTask
#[tokio::test]
async fn history_v2_retry_task_err_skip_task() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err()))]);
    *f.resender.result.lock().unwrap() = Some(ReplicationError::SkipTask);
    let (task, _) = history_task_v2(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(f.engine.calls.repl.lock().unwrap().len(), 1);
}

// ..._RetryTaskErr_Error
#[tokio::test]
async fn history_v2_retry_task_err_error() {
    let f = fixture();
    script(&f.engine, vec![Err(ReplicationError::RetryTaskV2(retry_err()))]);
    *f.resender.result.lock().unwrap() = Some(ReplicationError::Other("some error".into()));
    let (task, _) = history_task_v2(SHARD0_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    match r.unwrap_err() {
        ReplicationError::RetryTaskV2(e) => assert_eq!(e, retry_err()),
        other => panic!("expected RetryTaskV2, got {other:?}"),
    }
    assert_eq!(f.engine.calls.repl.lock().unwrap().len(), 1);
}

// TestProcess_HistoryV2ReplicationTask_DifferentShardID
#[tokio::test]
async fn history_v2_different_shard() {
    let f = fixture();
    let (task, req) = history_task_v2(SHARD1_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert!(f.engine.calls.repl.lock().unwrap().is_empty());
    let calls = f.client.calls.repl.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(repl_req_eq(&calls[0], &req));
}

// TestProcess_SyncActivityReplicationTask_DifferentShardID
#[tokio::test]
async fn sync_activity_different_shard() {
    let f = fixture();
    let (task, req) = activity_task(SHARD1_WF);
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert!(f.engine.calls.sync.lock().unwrap().is_empty());
    let calls = f.client.calls.sync.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(sync_req_eq(&calls[0], &req));
}

// TestProcess_FailoverReplicationTask
#[tokio::test]
async fn failover_replication_task() {
    let f = fixture();
    let task = ReplicationTask {
        task_type: Some(ReplicationTaskType::FailoverMarker),
        failover_marker_attributes: Some(FailoverMarkerAttributes {
            domain_id: Some("test-domain-id".into()),
            failover_version: Some(101),
            creation_time: Some(111),
            ..Default::default()
        }),
        creation_time: Some(222),
        ..Default::default()
    };
    let (scope, r) = f.exec.execute(&task, true).await;
    assert!(r.is_ok());
    assert_eq!(scope, FAILOVER_MARKER_SCOPE);
    // creation time is taken from the task, not the attributes
    assert_eq!(
        *f.shard.markers.lock().unwrap(),
        vec![FailoverMarkerAttributes {
            domain_id: Some("test-domain-id".into()),
            failover_version: Some(101),
            creation_time: Some(222),
            ..Default::default()
        }]
    );
}

// TestProcess_FailoverReplicationTask_NilAttributes
#[tokio::test]
async fn failover_replication_task_nil_attributes() {
    let f = fixture();
    let task = ReplicationTask {
        task_type: Some(ReplicationTaskType::FailoverMarker),
        creation_time: Some(222),
        ..Default::default()
    };
    let (_, r) = f.exec.execute(&task, true).await;
    assert!(r.unwrap_err().to_string().contains("empty failover marker attributes"));
    assert!(f.shard.markers.lock().unwrap().is_empty());
}

// TestProcess_UnknownTask
#[tokio::test]
async fn unknown_task() {
    let f = fixture();
    let (scope, r) = f.exec.execute(&ReplicationTask::default(), true).await;
    assert_eq!(scope, REPLICATOR_SCOPE);
    assert!(matches!(r.unwrap_err(), ReplicationError::BadRequest(m) if m == "unknown replication task"));
}
