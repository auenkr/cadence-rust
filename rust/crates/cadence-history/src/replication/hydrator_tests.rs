//! Port of `go/service/history/replication/task_hydrator_test.go` (child module of
//! `task_hydrator` so private loaders are reachable).

use super::super::deps::*;
use super::super::test_support::{FakeDomains, TEST_DOMAIN_ID};
use super::*;
use cadence_api::types::{VersionHistory, VersionHistoryItem};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

const WF: &str = "workflow-id";
const RUN: &str = "22222222-2222-2222-2222-222222222222";
const TASK_ID: i64 = 111;
const CREATION: i64 = 333;
const FIRST: i64 = 6;
const NEXT: i64 = 8;
const VERSION: i64 = 456;
const SCHEDULE: i64 = 10;
const STARTED: i64 = 11;

fn blob(d: &[u8]) -> DataBlob {
    DataBlob { data: d.to_vec(), ..Default::default() }
}
fn info() -> TaskInfo {
    TaskInfo {
        task_id: TASK_ID,
        domain_id: TEST_DOMAIN_ID.into(),
        workflow_id: WF.into(),
        run_id: RUN.into(),
        version: VERSION,
        visibility_timestamp_nanos: CREATION,
    }
}
fn vhs() -> VersionHistories {
    VersionHistories {
        current_version_history_index: 0,
        histories: vec![VersionHistory {
            branch_token: vec![97, 98, 99],
            items: vec![VersionHistoryItem { event_id: FIRST, version: VERSION }],
        }],
    }
}
fn activity() -> ActivityInfo {
    ActivityInfo {
        version: VERSION,
        schedule_id: SCHEDULE,
        scheduled_time: 1,
        started_id: STARTED,
        started_time: 2,
        domain_id: TEST_DOMAIN_ID.into(),
        last_heartbeat_updated_time: 3,
        details: vec![100, 101, 102],
        attempt: 42,
        last_failure_reason: "failure-reason".into(),
        last_failure_details: vec![103, 104, 105],
        last_worker_identity: "worker-identity".into(),
        ..Default::default()
    }
}
fn sync_task() -> HistoryTask {
    HistoryTask::SyncActivity { info: info(), scheduled_id: SCHEDULE }
}
fn hist_task(branch: &[u8]) -> HistoryTask {
    HistoryTask::HistoryReplication {
        info: info(),
        first_event_id: FIRST,
        next_event_id: NEXT,
        branch_token: branch.to_vec(),
        new_run_branch_token: vec![94, 95, 96],
    }
}

struct Ms {
    running: bool,
    vhs: Option<VersionHistories>,
    acts: HashMap<i64, ActivityInfo>,
}
impl MutableStateView for Ms {
    fn is_workflow_execution_running(&self) -> bool {
        self.running
    }
    fn get_activity_info(&self, id: i64) -> Option<ActivityInfo> {
        self.acts.get(&id).cloned()
    }
    fn get_version_histories(&self) -> Option<VersionHistories> {
        self.vhs.clone()
    }
}

/// `fakeMutableStateProvider`: `None` = loading fails, `Some(None)` = not found.
struct Msp {
    ms: Option<Option<Arc<Ms>>>,
    released: Arc<AtomicBool>,
}
fn msp(ms: Option<Option<Ms>>) -> Msp {
    Msp { ms: ms.map(|o| o.map(Arc::new)), released: Arc::new(AtomicBool::new(false)) }
}
#[async_trait]
impl MutableStateProvider for Msp {
    async fn get_mutable_state(&self, _: &str, _: &str, _: &str) -> Result<(Arc<dyn MutableStateView>, ReleaseFn)> {
        let flag = self.released.clone();
        let release: ReleaseFn = Box::new(move |_| flag.store(true, Ordering::SeqCst));
        match &self.ms {
            None => Err(ReplicationError::Other("error loading mutable state".into())),
            Some(None) => Err(ReplicationError::EntityNotExists("not found".into())),
            Some(Some(m)) => {
                // Go returns the release func alongside errors too; the success path hands it out.
                let m: Arc<dyn MutableStateView> = m.clone();
                Ok((m, release))
            }
        }
    }
}

/// `fakeHistoryProvider`: blobs keyed by branch token.
struct Hp(Vec<(Vec<u8>, DataBlob)>);
impl Hp {
    fn get(&self, b: &[u8]) -> Result<DataBlob> {
        self.0
            .iter()
            .find(|(k, _)| k == b)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| ReplicationError::Other("failed reading history".into()))
    }
}
#[async_trait]
impl HistoryProvider for Hp {
    async fn get_event_blob(&self, t: &HistoryReplicationTaskRef) -> Result<DataBlob> {
        self.get(&t.branch_token)
    }
    async fn get_next_run_event_blob(&self, t: &HistoryReplicationTaskRef) -> Result<Option<DataBlob>> {
        self.get(&t.new_run_branch_token).map(Some)
    }
}

fn hydrator(msp: Msp, hp: Hp) -> (TaskHydrator, Arc<AtomicBool>) {
    let released = msp.released.clone();
    (TaskHydrator { ms_provider: Arc::new(msp), history: Arc::new(hp) }, released)
}

fn expected_sync() -> ReplicationTask {
    ReplicationTask {
        task_type: Some(ReplicationTaskType::SyncActivity),
        source_task_id: Some(TASK_ID),
        creation_time: Some(CREATION),
        sync_activity_task_attributes: Some(SyncActivityTaskAttributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            workflow_id: Some(WF.into()),
            run_id: Some(RUN.into()),
            version: Some(VERSION),
            scheduled_id: Some(SCHEDULE),
            scheduled_time: Some(1),
            started_id: Some(STARTED),
            started_time: Some(2),
            last_heartbeat_time: Some(3),
            details: Some(vec![100, 101, 102]),
            attempt: Some(42),
            last_failure_reason: Some("failure-reason".into()),
            last_worker_identity: Some("worker-identity".into()),
            last_failure_details: Some(vec![103, 104, 105]),
            version_history: Some(vhs().histories[0].clone()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn expected_history(events: DataBlob, new_run: Option<DataBlob>) -> ReplicationTask {
    ReplicationTask {
        task_type: Some(ReplicationTaskType::HistoryV2),
        source_task_id: Some(TASK_ID),
        creation_time: Some(CREATION),
        history_task_v2_attributes: Some(HistoryTaskV2Attributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            workflow_id: Some(WF.into()),
            run_id: Some(RUN.into()),
            version_history_items: Some(vhs().histories[0].items.clone()),
            events: Some(events),
            new_run_events: new_run,
            ..Default::default()
        }),
        ..Default::default()
    }
}

// TestTaskHydrator_UnknownTask
#[tokio::test]
async fn unknown_task() {
    let ms = Ms { running: false, vhs: None, acts: HashMap::new() };
    let (h, released) = hydrator(msp(Some(Some(ms))), Hp(vec![]));
    let err = h.hydrate(&HistoryTask::Unknown { info: info() }).await.unwrap_err();
    assert!(matches!(err, ReplicationError::BadRequest(ref m) if m == "unknown replication task"));
    assert!(released.load(Ordering::SeqCst));
}

// TestTaskHydrator_HydrateFailoverMarkerTask
#[tokio::test]
async fn failover_marker_task() {
    let (h, _) = hydrator(msp(None), Hp(vec![]));
    let got = h.hydrate(&HistoryTask::FailoverMarker { info: info() }).await.unwrap();
    assert_eq!(
        got,
        Some(ReplicationTask {
            task_type: Some(ReplicationTaskType::FailoverMarker),
            source_task_id: Some(TASK_ID),
            failover_marker_attributes: Some(FailoverMarkerAttributes {
                domain_id: Some(TEST_DOMAIN_ID.into()),
                failover_version: Some(VERSION),
                ..Default::default()
            }),
            creation_time: Some(CREATION),
            ..Default::default()
        })
    );
}

// TestTaskHydrator_HydrateSyncActivityTask
#[tokio::test]
async fn sync_activity_task() {
    let acts = || HashMap::from([(SCHEDULE, activity())]);
    type Case = (&'static str, Option<Option<Ms>>, std::result::Result<Option<ReplicationTask>, &'static str>);
    let cases: Vec<Case> = vec![
        (
            "hydrates sync activity task",
            Some(Some(Ms { running: true, vhs: Some(vhs()), acts: acts() })),
            Ok(Some(expected_sync())),
        ),
        (
            "workflow is not running",
            Some(Some(Ms { running: false, vhs: None, acts: HashMap::new() })),
            Ok(None),
        ),
        (
            "no activity info",
            Some(Some(Ms { running: true, vhs: None, acts: HashMap::new() })),
            Ok(None),
        ),
        (
            "bad version histories",
            Some(Some(Ms { running: true, vhs: Some(VersionHistories::default()), acts: acts() })),
            Err("getting branch index: 0, available branch count: 0"),
        ),
        ("workflow does not exist", Some(None), Ok(None)),
        ("error loading mutable state", None, Err("error loading mutable state")),
    ];
    for (name, ms, want) in cases {
        let (h, released) = hydrator(msp(ms), Hp(vec![]));
        let got = h.hydrate(&sync_task()).await;
        match want {
            Ok(t) => assert_eq!(got.unwrap(), t, "{name}"),
            Err(e) => assert!(got.unwrap_err().to_string().contains(e), "{name}"),
        }
        // the Go fake always reports release; Rust only gets a release func with a loaded state
        let loaded = matches!(name, "hydrates sync activity task" | "workflow is not running" | "no activity info" | "bad version histories");
        assert_eq!(released.load(Ordering::SeqCst), loaded, "{name}");
    }
}

// TestTaskHydrator_HydrateHistoryReplicationTask
#[tokio::test]
async fn history_replication_task() {
    let with_vh = || Some(Some(Ms { running: true, vhs: Some(vhs()), acts: HashMap::new() }));
    let blobs = |v: &[(&[u8], &[u8])]| Hp(v.iter().map(|(b, d)| (b.to_vec(), blob(d))).collect());

    // given branch token
    let (h, _) = hydrator(
        msp(with_vh()),
        blobs(&[(&[91, 92, 93], &[1, 2, 3]), (&[94, 95, 96], &[4, 5, 6])]),
    );
    let got = h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap();
    assert_eq!(got, Some(expected_history(blob(&[1, 2, 3]), Some(blob(&[4, 5, 6])))));

    // branch token from version histories
    let (h, _) = hydrator(
        msp(with_vh()),
        blobs(&[(&[97, 98, 99], &[7, 8, 9]), (&[94, 95, 96], &[4, 5, 6])]),
    );
    let got = h.hydrate(&hist_task(&[])).await.unwrap();
    assert_eq!(got, Some(expected_history(blob(&[7, 8, 9]), Some(blob(&[4, 5, 6])))));

    // no version histories
    let (h, released) = hydrator(msp(Some(Some(Ms { running: true, vhs: None, acts: HashMap::new() }))), blobs(&[]));
    assert_eq!(h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap(), None);
    assert!(released.load(Ordering::SeqCst));

    // bad version histories
    let (h, _) = hydrator(
        msp(Some(Some(Ms { running: true, vhs: Some(VersionHistories::default()), acts: HashMap::new() }))),
        blobs(&[]),
    );
    let err = h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap_err();
    assert!(err.to_string().contains("version histories does not contains given item."));

    // workflow does not exist
    let (h, _) = hydrator(msp(Some(None)), blobs(&[]));
    assert_eq!(h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap(), None);

    // error loading mutable state
    let (h, _) = hydrator(msp(None), blobs(&[]));
    let err = h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap_err();
    assert!(err.to_string().contains("error loading mutable state"));

    // failed reading event blob
    let (h, _) = hydrator(msp(with_vh()), blobs(&[(&[94, 95, 96], &[4, 5, 6])]));
    let err = h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap_err();
    assert!(err.to_string().contains("failed reading history"));

    // failed reading event blob for new run
    let (h, _) = hydrator(msp(with_vh()), blobs(&[(&[91, 92, 93], &[1, 2, 3])]));
    let err = h.hydrate(&hist_task(&[91, 92, 93])).await.unwrap_err();
    assert!(err.to_string().contains("failed reading history"));
}

type ReadCall = (i32, String, Vec<u8>, i64, i64, usize);

struct Hm {
    calls: Mutex<Vec<ReadCall>>,
    resp: Result<Vec<DataBlob>>,
}
#[async_trait]
impl HistoryManager for Hm {
    async fn read_raw_history_branch(
        &self,
        shard_id: i32,
        domain_name: &str,
        branch_token: &[u8],
        min: i64,
        max: i64,
        page: usize,
    ) -> Result<Vec<DataBlob>> {
        self.calls.lock().unwrap().push((shard_id, domain_name.into(), branch_token.to_vec(), min, max, page));
        self.resp.clone()
    }
}

fn loader(hm: Arc<Hm>, domains: FakeDomains) -> HistoryLoader {
    HistoryLoader { shard_id: 0, history: hm, domains: Arc::new(domains) }
}

fn task_ref(branch: &[u8], first: i64, next: i64, new_run: &[u8]) -> HistoryReplicationTaskRef {
    HistoryReplicationTaskRef {
        info: info(),
        first_event_id: first,
        next_event_id: next,
        branch_token: branch.to_vec(),
        new_run_branch_token: new_run.to_vec(),
    }
}

// TestHistoryLoader_GetEventBlob
#[tokio::test]
async fn history_loader_get_event_blob() {
    // loads data blob
    let hm = Arc::new(Hm { calls: Mutex::new(vec![]), resp: Ok(vec![blob(&[1, 2, 3])]) });
    let l = loader(hm.clone(), FakeDomains::test_domain());
    let got = l.get_event_blob(&task_ref(&[91, 92, 93], 10, 11, &[])).await.unwrap();
    assert_eq!(got, blob(&[1, 2, 3]));
    assert_eq!(
        hm.calls.lock().unwrap()[0],
        (0, "test-domain-name".to_string(), vec![91, 92, 93], 10, 11, 2)
    );

    // failed to get domain name
    let hm = Arc::new(Hm { calls: Mutex::new(vec![]), resp: Ok(vec![]) });
    let l = loader(hm.clone(), FakeDomains::default());
    let err = l.get_event_blob(&task_ref(&[], 0, 0, &[])).await.unwrap_err();
    assert!(err.to_string().contains("domain does not exist"));
    assert!(hm.calls.lock().unwrap().is_empty());

    // load failure
    let hm = Arc::new(Hm { calls: Mutex::new(vec![]), resp: Err(ReplicationError::Other("load failure".into())) });
    let l = loader(hm, FakeDomains::test_domain());
    let err = l.get_event_blob(&task_ref(&[], 0, 0, &[])).await.unwrap_err();
    assert!(err.to_string().contains("load failure"));

    // response must contain exactly one blob
    let hm = Arc::new(Hm { calls: Mutex::new(vec![]), resp: Ok(vec![DataBlob::default(), DataBlob::default()]) });
    let l = loader(hm, FakeDomains::test_domain());
    let err = l.get_event_blob(&task_ref(&[], 0, 0, &[])).await.unwrap_err();
    assert!(err.to_string().contains("replication hydrator encountered more than 1 NDC raw event batch"));
}

// TestHistoryLoader_GetNextRunEventBlob
#[tokio::test]
async fn history_loader_get_next_run_event_blob() {
    let hm = Arc::new(Hm { calls: Mutex::new(vec![]), resp: Ok(vec![blob(&[1, 2, 3])]) });
    let l = loader(hm.clone(), FakeDomains::test_domain());
    assert_eq!(l.get_next_run_event_blob(&task_ref(&[], 0, 0, &[])).await.unwrap(), None);
    assert!(hm.calls.lock().unwrap().is_empty());

    let got = l.get_next_run_event_blob(&task_ref(&[], 0, 0, &[94, 95, 96])).await.unwrap();
    assert_eq!(got, Some(blob(&[1, 2, 3])));
    assert_eq!(
        hm.calls.lock().unwrap()[0],
        (0, "test-domain-name".to_string(), vec![94, 95, 96], 1, 2, 2)
    );
}

// TestImmediateTaskHydrator
#[tokio::test]
async fn immediate_task_hydrator() {
    let acts = || HashMap::from([(SCHEDULE, activity())]);
    let imm = |acts, b: Option<DataBlob>, n: Option<DataBlob>| TaskHydrator::new_immediate(true, Some(vhs()), acts, b, n);

    // sync activity happy path
    assert_eq!(imm(acts(), None, None).hydrate(&sync_task()).await.unwrap(), Some(expected_sync()));
    // sync activity missing activity info
    assert_eq!(imm(HashMap::new(), None, None).hydrate(&sync_task()).await.unwrap(), None);
    // history happy path
    let got = imm(HashMap::new(), Some(blob(&[1, 2, 3])), Some(blob(&[4, 5, 6])))
        .hydrate(&hist_task(&[91, 92, 93]))
        .await
        .unwrap();
    assert_eq!(got, Some(expected_history(blob(&[1, 2, 3]), Some(blob(&[4, 5, 6])))));
    // history, no next run
    let got = imm(HashMap::new(), Some(blob(&[1, 2, 3])), None).hydrate(&hist_task(&[91, 92, 93])).await.unwrap();
    assert_eq!(got, Some(expected_history(blob(&[1, 2, 3]), None)));
    // missing data blob
    let err = imm(HashMap::new(), None, None).hydrate(&hist_task(&[91, 92, 93])).await.unwrap_err();
    assert!(err.to_string().contains("history blob not set"));
}
