//! Port of `go/service/history/replication/task_fetcher_test.go`.
//! Go drives a mocked clock; here real (millisecond) timers are used.

use super::deps::*;
use super::task_fetcher::*;
use super::test_support::*;
use async_trait::async_trait;
use cadence_api::admin::{
    GetDLQReplicationMessagesRequest, GetReplicationMessagesRequest, GetReplicationMessagesResponse,
    ReplicationMessages, ReplicationToken,
};
use cadence_api::types::ReplicationTask;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Default)]
struct ScriptedAdmin {
    script: Mutex<VecDeque<Result<GetReplicationMessagesResponse>>>,
    calls: Mutex<Vec<GetReplicationMessagesRequest>>,
}

#[async_trait]
impl AdminClient for ScriptedAdmin {
    async fn get_replication_messages(
        &self,
        req: GetReplicationMessagesRequest,
        _t: Duration,
    ) -> Result<GetReplicationMessagesResponse> {
        self.calls.lock().unwrap().push(req);
        self.script.lock().unwrap().pop_front().unwrap_or(Ok(GetReplicationMessagesResponse::default()))
    }
    async fn get_dlq_replication_messages(
        &self,
        _r: GetDLQReplicationMessagesRequest,
    ) -> Result<Vec<ReplicationTask>> {
        unimplemented!()
    }
}

fn token(shard: i32, retrieved: i64, processed: i64) -> ReplicationToken {
    ReplicationToken { shard_id: shard, last_retrieved_message_id: retrieved, last_processed_message_id: processed }
}

fn fetcher(admin: Arc<ScriptedAdmin>, cfg: Arc<TestConfig>) -> Arc<TaskFetcherImpl> {
    TaskFetcherImpl::new("standby".into(), "active".into(), cfg, admin, Arc::new(NoopMetrics))
}

fn request(t: ReplicationToken) -> (Request, oneshot::Receiver<ReplicationMessages>) {
    let (tx, rx) = oneshot::channel();
    (Request { token: t, resp_chan: tx }, rx)
}

fn resp(shards: &[i32]) -> GetReplicationMessagesResponse {
    GetReplicationMessagesResponse {
        messages_by_shard: Some(shards.iter().map(|s| (*s, ReplicationMessages::default())).collect()),
    }
}

// TestGetMessages
#[tokio::test]
async fn get_messages() {
    let admin = Arc::new(ScriptedAdmin::default());
    admin.script.lock().unwrap().push_back(Ok(resp(&[0])));
    let f = fetcher(admin.clone(), Arc::new(TestConfig::default()));
    let (req, _rx) = request(token(0, 2, 1));
    let mut by_shard = HashMap::new();
    by_shard.insert(0, req);
    let got = f.get_messages(&by_shard).await.unwrap();
    assert_eq!(got, HashMap::from([(0, ReplicationMessages::default())]));
    assert_eq!(
        admin.calls.lock().unwrap()[0],
        GetReplicationMessagesRequest { tokens: vec![token(0, 2, 1)], cluster_name: "active".into() }
    );
}

// TestFetchAndDistributeTasks
#[tokio::test]
async fn fetch_and_distribute_tasks() {
    let admin = Arc::new(ScriptedAdmin::default());
    admin.script.lock().unwrap().push_back(Ok(resp(&[0])));
    let f = fetcher(admin.clone(), Arc::new(TestConfig::default()));
    let (req, rx) = request(token(0, 2, 1));
    let mut by_shard = HashMap::new();
    by_shard.insert(0, req);
    f.fetch_and_distribute_tasks(&mut by_shard).await.unwrap();
    assert!(by_shard.is_empty());
    assert_eq!(rx.await.unwrap(), ReplicationMessages::default());
    // empty request set never calls the remote
    f.fetch_and_distribute_tasks(&mut HashMap::new()).await.unwrap();
    assert_eq!(admin.calls.lock().unwrap().len(), 1);
}

// TestLifecycle: duplicate request closes the older one; busy and generic errors
// retry with the same requests; success distributes and clears them.
#[tokio::test]
async fn lifecycle() {
    let admin = Arc::new(ScriptedAdmin::default());
    {
        let mut s = admin.script.lock().unwrap();
        s.push_back(Err(ReplicationError::ServiceBusy("busy".into())));
        s.push_back(Err(ReplicationError::Other("internal".into())));
        s.push_back(Ok(resp(&[0, 1])));
    }
    let cfg = Arc::new(TestConfig::default());
    cfg.aggregation_ms.store(300, Ordering::SeqCst);
    let f = fetcher(admin.clone(), cfg);
    f.start();
    let chan = f.get_request_chan(0);

    let (req0, rx0) = request(token(0, 100, 10));
    let (req1, rx1) = request(token(1, 10, 1));
    let (req2, rx2) = request(token(0, 10, 1));
    chan.send(req0).await.ok().unwrap();
    chan.send(req1).await.ok().unwrap();
    chan.send(req2).await.ok().unwrap();
    // the replaced request's channel is closed without a value
    assert!(rx0.await.is_err());

    let m0 = tokio::time::timeout(Duration::from_secs(10), rx1).await.unwrap().unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(10), rx2).await.unwrap().unwrap();
    assert_eq!(m0, ReplicationMessages::default());
    assert_eq!(m2, ReplicationMessages::default());

    let calls = admin.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    for c in calls.iter() {
        let mut t = c.tokens.clone();
        t.sort_by_key(|t| t.shard_id);
        assert_eq!(t, vec![token(0, 10, 1), token(1, 10, 1)]);
        assert_eq!(c.cluster_name, "active");
    }
    drop(calls);
    f.stop();
}

struct Bean(Arc<ScriptedAdmin>);
impl ClientBean for Bean {
    fn history_client(&self) -> Arc<dyn HistoryClient> {
        unimplemented!()
    }
    fn remote_admin_client(&self, cluster: &str) -> Result<Arc<dyn AdminClient>> {
        assert_eq!(cluster, CLUSTER_B);
        Ok(self.0.clone())
    }
    fn remote_frontend_client(&self, _c: &str) -> Result<Arc<dyn FrontendClient>> {
        unimplemented!()
    }
}

// TestTaskFetchers
#[tokio::test]
async fn task_fetchers() {
    let bean = Bean(Arc::new(ScriptedAdmin::default()));
    let fetchers = TaskFetchersImpl::new(
        Arc::new(TestConfig::default()),
        CLUSTER_A,
        vec![CLUSTER_B.into()],
        &bean,
        Arc::new(NoopMetrics),
    )
    .unwrap();
    assert_eq!(fetchers.get_fetchers().len(), 1);
    assert_eq!(fetchers.get_fetchers()[0].get_source_cluster(), CLUSTER_B);
    fetchers.start();
    fetchers.stop();
}

// TestTaskFetcherParallelism
#[tokio::test]
async fn task_fetcher_parallelism() {
    let cfg = Arc::new(TestConfig::default());
    cfg.parallelism.store(4, Ordering::SeqCst);
    let f = fetcher(Arc::new(ScriptedAdmin::default()), cfg);
    let c0 = f.get_request_chan(0);
    let c1 = f.get_request_chan(1);
    assert!(c0.same_channel(&f.get_request_chan(4)));
    assert!(c1.same_channel(&f.get_request_chan(5)));
    assert!(!c0.same_channel(&c1));
    f.start();
    f.stop();
}
