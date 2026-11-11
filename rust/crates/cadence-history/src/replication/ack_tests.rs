//! Ports of `task_ack_manager_test.go` and `metrics_emitter_test.go`.

use super::deps::*;
use super::metrics_emitter::MetricsEmitterImpl;
use super::task_ack_manager::*;
use super::task_store::TaskStore;
use super::test_support::*;
use cadence_api::admin::ReplicationMessages;
use cadence_api::types::ReplicationTask;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn hyd(id: i64) -> Result<Option<ReplicationTask>> {
    Ok(Some(match id {
        11 => hydrated_history(11),
        12 => hydrated_sync(12),
        13 => hydrated_history(13),
        _ => hydrated_failover(14),
    }))
}

struct Case {
    name: &'static str,
    domains: FakeDomains,
    reader: Option<Vec<HistoryTask>>,
    hydrator: Vec<(i64, Result<Option<ReplicationTask>>)>,
    polling: &'static str,
    last_read: i64,
    batch: usize,
    max_response: usize,
    update_err: bool,
    remote_ack: i64,
    expect: Option<(Vec<ReplicationTask>, i64, bool)>,
    expect_err: Option<&'static str>,
    expect_ack: Option<i64>,
}

fn all_tasks() -> Option<Vec<HistoryTask>> {
    Some(vec![history_task(11), sync_task(12), history_task(13), failover_task(14)])
}

fn size_fn() -> ReplicationMessagesSizeFn {
    Arc::new(|m: &ReplicationMessages| m.replication_tasks.len() * 100)
}

fn case(name: &'static str) -> Case {
    Case {
        name,
        domains: FakeDomains::test_domain(),
        reader: all_tasks(),
        hydrator: vec![(11, hyd(11)), (12, hyd(12)), (13, hyd(13)), (14, hyd(14))],
        polling: CLUSTER_A,
        last_read: 5,
        batch: 10,
        max_response: 4 * 1024 * 1024,
        update_err: false,
        remote_ack: 2,
        expect: None,
        expect_err: None,
        expect_ack: Some(5),
    }
}

#[tokio::test]
async fn task_ack_manager_get_tasks() {
    let recoverable = Err(ReplicationError::EntityNotExists("".into()));
    let non_recoverable = Err(ReplicationError::Other("error hydrating task".into()));
    let cases = vec![
        Case { reader: Some(vec![]), hydrator: vec![], expect: Some((vec![], 200, false)), ..case("main flow - no replication tasks") },
        Case {
            hydrator: vec![(11, hyd(11)), (12, hyd(12)), (13, recoverable), (14, hyd(14))],
            expect: Some((vec![hydrated_history(11), hydrated_sync(12), hydrated_failover(14)], 14, false)),
            ..case("main flow - continues on recoverable error")
        },
        Case {
            hydrator: vec![(11, hyd(11)), (12, hyd(12)), (13, non_recoverable), (14, hyd(14))],
            expect: Some((vec![hydrated_history(11), hydrated_sync(12)], 12, true)),
            ..case("main flow - stops at non recoverable error")
        },
        Case {
            batch: 2,
            expect: Some((vec![hydrated_history(11), hydrated_sync(12)], 12, true)),
            ..case("main flow - stops at second task, batch size is 2")
        },
        Case {
            max_response: 301,
            expect: Some((vec![hydrated_history(11), hydrated_sync(12), hydrated_history(13)], 13, true)),
            ..case("main flow - stops at a message exceeded max response size")
        },
        Case {
            max_response: 0,
            expect_err: Some("replication messages size is too large and cannot be shrunk anymore, shard will be stuck until the message size is reduced or max size is increased"),
            expect_ack: Some(2),
            ..case("main flow - fail at a message exceeded max response size")
        },
        Case {
            reader: Some(vec![history_task(11)]),
            polling: CLUSTER_B,
            expect: Some((vec![], 11, false)),
            ..case("skips tasks for domains non belonging to polling cluster")
        },
        Case {
            reader: Some(vec![history_task(11), sync_task(12)]),
            hydrator: vec![(12, hyd(12))],
            last_read: -1,
            remote_ack: 12,
            expect: Some((vec![hydrated_sync(12)], 12, false)),
            expect_ack: Some(12),
            ..case("uses remote ack level for first fetch (empty task ID)")
        },
        Case {
            reader: None,
            expect_err: Some("error reading replication tasks"),
            expect_ack: None,
            ..case("failed to read replication tasks - return error")
        },
        Case {
            domains: FakeDomains::default(),
            reader: Some(vec![history_task(11)]),
            hydrator: vec![],
            expect: Some((vec![], 5, true)),
            expect_ack: None,
            ..case("failed to get domain - stops")
        },
        Case {
            reader: Some(vec![history_task(11)]),
            update_err: true,
            expect: Some((vec![hydrated_history(11)], 11, false)),
            expect_ack: None,
            ..case("failed to update ack level - no error, return response anyway")
        },
    ];

    for c in cases {
        let shard = Arc::new(FakeShard::new());
        shard.read_level.store(200, Ordering::SeqCst);
        shard.ack.lock().unwrap().insert(CLUSTER_A.into(), c.remote_ack);
        if c.update_err {
            *shard.update_err.lock().unwrap() = Some(ReplicationError::Other("error update ack level".into()));
        }
        let mut h = FakeHydrator::default();
        for (id, r) in c.hydrator {
            h.0.insert(id, r);
        }
        let store = Arc::new(TaskStore::new(
            &TestConfig::default(),
            vec![CLUSTER_A.into(), CLUSTER_B.into()],
            Arc::new(c.domains),
            Arc::new(NoopMetrics),
            Arc::new(h),
            Arc::new(NoLimit),
            0,
            Arc::new(FixedTime(0)),
        ));
        let cfg = TestConfig::default();
        cfg.max_response.store(c.max_response, Ordering::SeqCst);
        let mgr = TaskAckManager::new(
            0,
            shard.clone(),
            Arc::new(NoopMetrics),
            Arc::new(FakeReader(c.reader)),
            store,
            Arc::new(MockTime::default()),
            &cfg,
            size_fn(),
            Arc::new(FixedBatch(c.batch)),
        );
        let got = mgr.get_tasks(c.polling, c.last_read).await;
        match (c.expect_err, c.expect) {
            (Some(e), _) => assert_eq!(got.unwrap_err().to_string(), e, "{}", c.name),
            (None, Some((tasks, last, more))) => {
                let m = got.unwrap_or_else(|e| panic!("{}: {e}", c.name));
                assert_eq!(m.replication_tasks, tasks, "{}", c.name);
                assert_eq!(m.last_retrieved_message_id, last, "{}", c.name);
                assert_eq!(m.has_more, more, "{}", c.name);
            }
            _ => unreachable!(),
        }
        if let Some(a) = c.expect_ack {
            assert_eq!(shard.ack.lock().unwrap()[c.polling], a, "{}", c.name);
        }
    }
}

// ---- metrics_emitter_test.go -------------------------------------------------

fn emitter_shard() -> Arc<FakeShard> {
    let mut s = FakeShard::new();
    s.remotes = vec!["cluster2".into(), "cluster3".into()];
    s.time.0.store(10_000 * 3_600_000_000_000, Ordering::SeqCst);
    let s = Arc::new(s);
    for c in ["cluster2", "cluster3"] {
        s.ack.lock().unwrap().insert(c.into(), 1);
    }
    s
}

fn task_at(id: i64, ago: Duration, now: i64) -> HistoryTask {
    HistoryTask::HistoryReplication {
        info: TaskInfo { task_id: id, visibility_timestamp_nanos: now - ago.as_nanos() as i64, ..Default::default() },
        first_event_id: 0,
        next_event_id: 0,
        branch_token: vec![],
        new_run_branch_token: vec![],
    }
}

#[tokio::test]
async fn metrics_emitter_determines_replication_latency() {
    let shard = emitter_shard();
    let now = shard.time.0.load(Ordering::SeqCst);
    let reader = Arc::new(FakeReader(Some(vec![
        task_at(1, Duration::from_secs(3600), now),
        task_at(2, Duration::from_secs(60), now),
    ])));
    let e = MetricsEmitterImpl::new(1, shard.clone(), reader, Arc::new(NoopMetrics));
    assert_eq!(e.determine_replication_latency("cluster2").await.unwrap(), Duration::from_secs(3600));
    shard.ack.lock().unwrap().insert("cluster2".into(), 2);
    assert_eq!(e.determine_replication_latency("cluster2").await.unwrap(), Duration::from_secs(60));
    shard.ack.lock().unwrap().insert("cluster2".into(), 3);
    assert_eq!(e.determine_replication_latency("cluster2").await.unwrap(), Duration::ZERO);
    assert_eq!(e.determine_replication_latency("cluster3").await.unwrap(), Duration::from_secs(3600));
}

#[tokio::test]
async fn metrics_emitter_start_stop() {
    let shard = emitter_shard();
    let reader = Arc::new(FakeReader(Some(vec![])));
    let e = MetricsEmitterImpl::new(1, shard, reader, Arc::new(NoopMetrics));
    e.start();
    tokio::time::sleep(Duration::from_millis(20)).await;
    e.stop();
    e.stop();
}
