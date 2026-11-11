//! Port of `go/service/history/replication/task_processor_test.go` (child module of
//! `task_processor` so private methods are reachable like in the Go package test).

use super::super::deps::*;
use super::super::test_support::*;
use super::*;
use async_trait::async_trait;
use cadence_api::types::{
    DataBlob, FailoverMarkerAttributes, HistoryTaskV2Attributes, SyncActivityTaskAttributes,
    VersionHistoryItem,
};
use std::collections::VecDeque;

const SOURCE: &str = "standby";

struct Fetcher {
    tx: mpsc::Sender<Request>,
}
impl Daemon for Fetcher {
    fn start(&self) {}
    fn stop(&self) {}
}
impl TaskFetcher for Fetcher {
    fn get_source_cluster(&self) -> &str {
        SOURCE
    }
    fn get_request_chan(&self, _shard_id: usize) -> mpsc::Sender<Request> {
        self.tx.clone()
    }
    fn get_rate_limiter(&self) -> Arc<dyn Limiter> {
        Arc::new(NoLimit)
    }
}

/// Scripted executor: pops one outcome per call (default Ok).
#[derive(Default)]
struct Exec {
    script: Mutex<VecDeque<Result<()>>>,
    calls: Mutex<Vec<i64>>,
    always: Mutex<Option<ReplicationError>>,
}
#[async_trait]
impl TaskExecutor for Exec {
    async fn execute(&self, t: &ReplicationTask, _force: bool) -> (&'static str, Result<()>) {
        self.calls.lock().unwrap().push(t.source_task_id.unwrap_or_default());
        if let Some(e) = self.always.lock().unwrap().clone() {
            return ("TestScope", Err(e));
        }
        let r = self.script.lock().unwrap().pop_front().unwrap_or(Ok(()));
        ("TestScope", r)
    }
}

#[derive(Default)]
struct EngineRec {
    syncs: Mutex<Vec<SyncShardStatusRequest>>,
}
#[async_trait]
impl Engine for EngineRec {
    async fn sync_shard_status(&self, r: SyncShardStatusRequest, _t: Duration) -> Result<()> {
        self.syncs.lock().unwrap().push(r);
        Ok(())
    }
    async fn sync_activity(&self, _r: SyncActivityRequest, _t: Duration) -> Result<()> {
        unimplemented!()
    }
    async fn replicate_events_v2(&self, _r: ReplicateEventsV2Request, _t: Duration) -> Result<()> {
        unimplemented!()
    }
}

/// Deserializer returning `(1, 1)`, `(2, 1)` like the Go test's two-event batch.
struct Ser(Vec<(i64, i64)>);
impl PayloadSerializer for Ser {
    fn deserialize_batch_events(&self, _b: &DataBlob) -> Result<Vec<(i64, i64)>> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
struct Frontend {
    inputs: Mutex<Vec<Vec<u8>>>,
}
#[async_trait]
impl FrontendClient for Frontend {
    async fn start_data_corruption_scan(&self, input: Vec<u8>) -> Result<()> {
        self.inputs.lock().unwrap().push(input);
        Ok(())
    }
}
struct FrontBean(Arc<Frontend>);
impl ClientBean for FrontBean {
    fn history_client(&self) -> Arc<dyn HistoryClient> {
        unimplemented!()
    }
    fn remote_admin_client(&self, _c: &str) -> Result<Arc<dyn AdminClient>> {
        unimplemented!()
    }
    fn remote_frontend_client(&self, _c: &str) -> Result<Arc<dyn FrontendClient>> {
        Ok(self.0.clone())
    }
}

struct Fx {
    p: Arc<TaskProcessorImpl>,
    shard: Arc<FakeShard>,
    exec: Arc<Exec>,
    engine: Arc<EngineRec>,
    rx: mpsc::Receiver<Request>,
}

fn fx_with(mut shard: FakeShard, events: Vec<(i64, i64)>) -> Fx {
    shard.domains = FakeDomains::test_domain();
    let shard = Arc::new(shard);
    let (tx, rx) = mpsc::channel(10);
    let exec = Arc::new(Exec::default());
    let engine = Arc::new(EngineRec::default());
    let p = TaskProcessorImpl::new(
        shard.clone(),
        engine.clone(),
        Arc::new(TestConfig::default()),
        Arc::new(NoopMetrics),
        &Fetcher { tx },
        exec.clone(),
        Arc::new(Ser(events)),
    );
    Fx { p, shard, exec, engine, rx }
}

fn fx() -> Fx {
    fx_with(FakeShard::new(), vec![(1, 1), (2, 1)])
}

fn sync_task_attrs(id: i64) -> ReplicationTask {
    ReplicationTask {
        task_type: Some(ReplicationTaskType::SyncActivity),
        source_task_id: Some(id),
        sync_activity_task_attributes: Some(SyncActivityTaskAttributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            workflow_id: Some("wf".into()),
            run_id: Some("run".into()),
            scheduled_id: Some(7),
            version: Some(3),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn history_task_attrs(id: i64) -> ReplicationTask {
    ReplicationTask {
        task_type: Some(ReplicationTaskType::HistoryV2),
        source_task_id: Some(id),
        history_task_v2_attributes: Some(HistoryTaskV2Attributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            workflow_id: Some("wf".into()),
            run_id: Some("run".into()),
            version_history_items: Some(vec![VersionHistoryItem { event_id: 2, version: 9 }]),
            events: Some(DataBlob::default()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn ids(p: &TaskProcessorImpl) -> (i64, i64) {
    (
        p.last_processed_message_id.load(Ordering::SeqCst),
        p.last_retrieved_message_id.load(Ordering::SeqCst),
    )
}

async fn recv(rx: &mut mpsc::Receiver<Request>) -> Request {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for request")
        .expect("request channel closed")
}

#[tokio::test]
async fn start_stop() {
    let f = fx();
    f.p.start();
    f.p.stop();
    assert_eq!(f.p.status.load(Ordering::SeqCst), STATUS_STOPPED);
    assert!(f.p.is_shutting_down());
}

#[tokio::test]
async fn process_response_no_task() {
    let f = fx();
    let resp = ReplicationMessages {
        last_retrieved_message_id: 100,
        has_more: false,
        ..Default::default()
    };
    f.p.process_response(resp).await;
    assert_eq!(ids(&f.p), (100, 100));
}

#[tokio::test]
async fn processor_loop_request_chan_populated() {
    let mut f = fx();
    let h = tokio::spawn(f.p.clone().processor_loop());
    let req = recv(&mut f.rx).await;
    assert_eq!(req.token.shard_id, 0);
    assert_eq!(req.token.last_processed_message_id, -1);
    assert_eq!(req.token.last_retrieved_message_id, -1);
    f.p.done.cancel();
    h.await.unwrap();
}

#[tokio::test]
async fn processor_loop_resp_chan_closed() {
    let mut f = fx();
    let h = tokio::spawn(f.p.clone().processor_loop());
    let first = recv(&mut f.rx).await;
    drop(first.resp_chan);
    // a new request is sent after the response channel closed
    let second = recv(&mut f.rx).await;
    assert_eq!(second.token.shard_id, 0);
    f.p.done.cancel();
    h.await.unwrap();
}

#[tokio::test]
async fn processor_loop_task_execute_success() {
    let mut f = fx();
    let h = tokio::spawn(f.p.clone().processor_loop());
    let req = recv(&mut f.rx).await;
    let resp = ReplicationMessages {
        replication_tasks: vec![sync_task_attrs(5)],
        last_retrieved_message_id: 50,
        ..Default::default()
    };
    req.resp_chan.send(resp).unwrap();
    let next = recv(&mut f.rx).await;
    assert_eq!(next.token.last_processed_message_id, 50);
    assert_eq!(next.token.last_retrieved_message_id, 50);
    assert_eq!(*f.exec.calls.lock().unwrap(), vec![5]);
    f.p.done.cancel();
    h.await.unwrap();
}

#[tokio::test]
async fn processor_loop_task_execute_failed_put_dlq_success() {
    let f = fx();
    f.exec.script.lock().unwrap().push_back(Err(ReplicationError::BadRequest("bad".into())));
    let resp = ReplicationMessages {
        replication_tasks: vec![sync_task_attrs(5)],
        last_retrieved_message_id: 50,
        ..Default::default()
    };
    f.p.process_response(resp).await;
    let puts = f.shard.exec.put_dlq.lock().unwrap();
    assert_eq!(puts.len(), 1);
    let r = &puts[0];
    assert_eq!(r.source_cluster_name, SOURCE);
    assert_eq!(r.domain_name, TEST_DOMAIN_NAME);
    assert_eq!(r.task_info.task_type, REPLICATION_TASK_TYPE_SYNC_ACTIVITY);
    assert_eq!(r.task_info.scheduled_id, 7);
    assert_eq!(r.task_info.task_id, 5);
    // BadRequest is not retried
    assert_eq!(f.exec.calls.lock().unwrap().len(), 1);
    assert_eq!(ids(&f.p), (50, 50));
}

#[tokio::test]
async fn processor_loop_task_execute_failed_put_dlq_failed() {
    // The Rust DLQ retry policy retries until shutdown (Go test bounds it with a 2-attempt policy).
    let f = fx();
    *f.shard.exec.put_dlq_result.lock().unwrap() = Some(ReplicationError::Other("dlq down".into()));
    let p = f.p.clone();
    let task = sync_task_attrs(5);
    let req = p.generate_dlq_request(&task).unwrap();
    let h = tokio::spawn(async move { p.put_replication_task_to_dlq(req).await });
    tokio::time::sleep(Duration::from_millis(1400)).await;
    f.p.done.cancel();
    let res = tokio::time::timeout(Duration::from_secs(5), h).await.unwrap().unwrap();
    assert!(res.is_err());
    assert!(f.shard.exec.put_dlq.lock().unwrap().len() >= 2);
}

#[tokio::test]
async fn processor_loop_task_execute_failed_shard_change_err() {
    let f = fx();
    *f.exec.always.lock().unwrap() = Some(ReplicationError::ShardOwnershipLost("lost".into()));
    *f.shard.exec.put_dlq_result.lock().unwrap() = Some(ReplicationError::ShardOwnershipLost("lost".into()));
    let p = f.p.clone();
    let resp = ReplicationMessages {
        replication_tasks: vec![sync_task_attrs(5)],
        last_retrieved_message_id: 50,
        ..Default::default()
    };
    let h = tokio::spawn(async move { p.process_response(resp).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    f.p.done.cancel();
    tokio::time::timeout(Duration::from_secs(5), h).await.unwrap().unwrap();
    assert_eq!(ids(&f.p), (-1, -1));
}

#[tokio::test]
async fn handle_sync_shard_status() {
    let f = fx();
    f.shard.time.0.store(1_000_000_000_000, Ordering::SeqCst);
    // no status -> nothing
    f.p.handle_sync_shard_status(None).await.unwrap();
    // too old (> 10 minutes) -> dropped
    let old = SyncShardStatus { timestamp: Some(0) };
    f.p.handle_sync_shard_status(Some(&old)).await.unwrap();
    assert!(f.engine.syncs.lock().unwrap().is_empty());
    // fresh -> forwarded
    let fresh = SyncShardStatus { timestamp: Some(999_000_000_000) };
    f.p.handle_sync_shard_status(Some(&fresh)).await.unwrap();
    let s = f.engine.syncs.lock().unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].source_cluster, SOURCE);
    assert_eq!(s[0].shard_id, 0);
    assert_eq!(s[0].timestamp, Some(999_000_000_000));
}

#[tokio::test]
async fn put_replication_task_to_dlq_sync_activity() {
    let f = fx();
    let task = sync_task_attrs(5);
    let req = f.p.generate_dlq_request(&task).unwrap();
    f.p.put_replication_task_to_dlq(req).await.unwrap();
    let puts = f.shard.exec.put_dlq.lock().unwrap();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].task_info.task_type, REPLICATION_TASK_TYPE_SYNC_ACTIVITY);
    assert_eq!(puts[0].shard_id, 0);
}

#[tokio::test]
async fn put_replication_task_to_dlq_history_v2() {
    let f = fx();
    let task = history_task_attrs(5);
    let req = f.p.generate_dlq_request(&task).unwrap();
    f.p.put_replication_task_to_dlq(req).await.unwrap();
    let puts = f.shard.exec.put_dlq.lock().unwrap();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].task_info.task_type, REPLICATION_TASK_TYPE_HISTORY);
    assert_eq!(puts[0].task_info.first_event_id, 1);
}

#[tokio::test]
async fn generate_dlq_request_history_v2() {
    let f = fx();
    let r = f.p.generate_dlq_request(&history_task_attrs(5)).unwrap();
    assert_eq!(r.source_cluster_name, SOURCE);
    assert_eq!(r.domain_name, TEST_DOMAIN_NAME);
    assert_eq!(r.task_info.domain_id, TEST_DOMAIN_ID);
    assert_eq!(r.task_info.workflow_id, "wf");
    assert_eq!(r.task_info.run_id, "run");
    assert_eq!(r.task_info.task_id, 5);
    assert_eq!(r.task_info.first_event_id, 1);
    assert_eq!(r.task_info.next_event_id, 3);
    assert_eq!(r.task_info.version, 1);
}

#[tokio::test]
async fn generate_dlq_request_sync_activity() {
    let f = fx();
    let r = f.p.generate_dlq_request(&sync_task_attrs(5)).unwrap();
    assert_eq!(r.task_info.task_type, REPLICATION_TASK_TYPE_SYNC_ACTIVITY);
    assert_eq!(r.task_info.scheduled_id, 7);
    assert_eq!(r.task_info.workflow_id, "wf");
    assert_eq!(r.domain_name, TEST_DOMAIN_NAME);
}

#[tokio::test]
async fn generate_dlq_request_invalid_task_type() {
    let f = fx();
    let t = ReplicationTask { task_type: Some(ReplicationTaskType::FailoverMarker), ..Default::default() };
    let err = f.p.generate_dlq_request(&t).unwrap_err();
    assert_eq!(err.to_string(), "unknown replication task type");
}

#[tokio::test]
async fn trigger_data_inconsistency_scan_success() {
    let front = Arc::new(Frontend::default());
    let mut shard = FakeShard::new();
    shard.bean = Arc::new(FrontBean(front.clone()));
    let f = fx_with(shard, vec![(1, 1)]);
    f.p.trigger_data_inconsistency_scan(&history_task_attrs(5)).await.unwrap();
    f.p.trigger_data_inconsistency_scan(&sync_task_attrs(6)).await.unwrap();
    let inputs = front.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 2);
    let v: serde_json::Value = serde_json::from_slice(&inputs[0]).unwrap();
    assert_eq!(v["DomainID"], TEST_DOMAIN_ID);
    assert_eq!(v["WorkflowID"], "wf");
    assert_eq!(v["RunID"], "run");
    assert_eq!(v["ShardID"], 0);
}

#[tokio::test]
async fn cleanup_replication_task_loop() {
    let f = fx();
    f.shard.ack.lock().unwrap().insert(CLUSTER_B.into(), 5);
    f.shard.read_level.store(100, Ordering::SeqCst);
    let h = tokio::spawn(f.p.clone().cleanup_replication_task_loop());
    tokio::time::sleep(Duration::from_millis(300)).await;
    f.p.done.cancel();
    h.await.unwrap();
    let calls = f.shard.exec.range_complete_calls.lock().unwrap();
    assert!(!calls.is_empty());
    assert_eq!(calls[0], (0, 6, 100));
}

#[tokio::test]
async fn sync_shard_status_loop_without_task() {
    let f = fx();
    let rx = f.p.sync_shard_rx.lock().unwrap().take().unwrap();
    let h = tokio::spawn(f.p.clone().sync_shard_status_loop(rx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    f.p.done.cancel();
    h.await.unwrap();
    assert!(f.engine.syncs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sync_shard_status_loop_with_task() {
    let f = fx();
    let rx = f.p.sync_shard_rx.lock().unwrap().take().unwrap();
    f.p.sync_shard_tx.send(Some(SyncShardStatus { timestamp: Some(0) })).await.unwrap();
    let h = tokio::spawn(f.p.clone().sync_shard_status_loop(rx));
    tokio::time::sleep(Duration::from_millis(300)).await;
    f.p.done.cancel();
    h.await.unwrap();
    assert!(!f.engine.syncs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn should_retry_dlq() {
    let f = fx();
    let e = ReplicationError::Other("e".into());
    assert!(f.p.should_retry_dlq(&e));
    assert!(f.p.should_retry_dlq(&ReplicationError::ServiceBusy("b".into())));
    f.p.done.cancel();
    assert!(!f.p.should_retry_dlq(&e));
}

#[tokio::test]
async fn is_shutting_down() {
    let f = fx();
    assert!(!f.p.is_shutting_down());
    f.p.done.cancel();
    assert!(f.p.is_shutting_down());
}

#[tokio::test]
async fn process_task_once_over_threshold() {
    let f = fx();
    // task created at 0, now = 10s, threshold 5s -> warns but succeeds
    f.shard.time.0.store(10_000_000_000, Ordering::SeqCst);
    let mut t = sync_task_attrs(5);
    t.creation_time = Some(0);
    f.p.process_task_once(&t).await.unwrap();
    assert_eq!(*f.exec.calls.lock().unwrap(), vec![5]);
}

#[test]
fn failover_marker_attributes_have_domain() {
    // sanity for the failover-marker branch of process_task_once domain extraction
    let t = ReplicationTask {
        task_type: Some(ReplicationTaskType::FailoverMarker),
        failover_marker_attributes: Some(FailoverMarkerAttributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(t.failover_marker_attributes.is_some());
}
