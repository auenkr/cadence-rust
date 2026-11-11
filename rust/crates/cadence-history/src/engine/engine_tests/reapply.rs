//! Ports of engineimpl/reapply_events_test.go style cases (ReapplyEvents).
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::{VersionHistories, VersionHistory, VersionHistoryItem};

use crate::engine::deps::*;
use crate::engine::reapply_events::event_reapplied_id;
use crate::engine::runtime::*;
use crate::engine::test_support::*;

const DOMAIN: &str = "dom-id";

struct Reapplier {
    result: Result<Vec<HistoryEvent>, String>,
    seen: Mutex<Vec<usize>>,
}
#[async_trait]
impl EventsReapplier for Reapplier {
    async fn reapply_events(&self, _: MutableStateRef, events: &[HistoryEvent], _: &str) -> HResult<Vec<HistoryEvent>> {
        self.seen.lock().unwrap().push(events.len());
        self.result.clone().map_err(|m| internal(&m))
    }
}

#[derive(Default)]
struct Resetter(Mutex<Vec<(i64, i64, String, usize, Vec<u8>, i64)>>);
#[async_trait]
impl WorkflowResetter for Resetter {
    async fn reset_workflow(&self, r: ResetWorkflow) -> HResult<()> {
        self.0.lock().unwrap().push((r.rebuild_last_event_id, r.rebuild_last_event_version, r.reason, r.events.len(), r.base_branch_token, r.base_next_event_id));
        Ok(())
    }
}

fn ev(id: i64, version: i64) -> HistoryEvent {
    HistoryEvent { event_id: id, version, ..Default::default() }
}

fn engine(ms: FakeMs, active: &str, domain: DomainCacheEntry) -> (HistoryEngine, Arc<FakeUpdater>) {
    let updater = Arc::new(FakeUpdater::new(Arc::new(ms)));
    let mut e = nop_engine();
    e.updater = updater.clone();
    e.cluster_metadata = Arc::new(FixedClusterMetadata("cluster-a".into()));
    e.active_cluster_manager = Arc::new(FixedActiveCluster(active.into()));
    e.time_source = Arc::new(FixedTime(1));
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert(DOMAIN.into(), Arc::new(domain));
    e.domain_cache = Arc::new(dc);
    (e, updater)
}

fn reapplier(result: Result<Vec<HistoryEvent>, String>) -> Arc<Reapplier> {
    Arc::new(Reapplier { result, seen: Mutex::new(vec![]) })
}

fn dom() -> DomainCacheEntry {
    domain_entry(DOMAIN, "dom")
}

#[tokio::test]
async fn same_version_events_are_skipped() {
    let ms = FakeMs { last_write_version: 5, ..Default::default() };
    let (e, up) = engine(ms, "cluster-a", dom());
    e.reapply_events(DOMAIN, "wf", "run", &[ev(1, 5), ev(2, 5)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, true)));
    let calls = up.calls.lock().unwrap();
    assert_eq!(calls[0].0, DOMAIN);
    assert!(calls[0].1.run_id.is_empty(), "run id must be dropped to target the current run");
    assert!(calls[0].2);
}

#[tokio::test]
async fn duplicated_events_are_skipped() {
    let ms = FakeMs { last_write_version: 1, ..Default::default() };
    ms.duplicated_resources.lock().unwrap().insert(event_reapplied_id("run", 7, 2));
    let (e, up) = engine(ms, "cluster-a", dom());
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, true)));
}

#[tokio::test]
async fn running_workflow_reapplies_and_creates_decision() {
    let (mut e, up) = engine(FakeMs::default(), "cluster-a", dom());
    let r = reapplier(Ok(vec![ev(7, 2)]));
    e.events_reapplier = r.clone();
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2), ev(8, 2)]).await.unwrap();
    assert_eq!(*r.seen.lock().unwrap(), vec![2]);
    assert_eq!(*up.last_action.lock().unwrap(), Some((true, false)));
}

#[tokio::test]
async fn nothing_reapplied_is_noop() {
    let (mut e, up) = engine(FakeMs::default(), "cluster-a", dom());
    e.events_reapplier = reapplier(Ok(vec![]));
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, true)));
}

#[tokio::test]
async fn reapplier_error_becomes_internal_error() {
    let (mut e, _) = engine(FakeMs::default(), "cluster-a", dom());
    e.events_reapplier = reapplier(Err("bad".into()));
    let err = e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap_err();
    assert!(err.to_string().contains("unable to re-apply stale events"), "{err}");
}

#[tokio::test]
async fn cron_workflow_without_decision_does_not_schedule_one() {
    let ms = FakeMs { processed_or_pending_decision: false, ..Default::default() };
    ms.info.lock().unwrap().cron_schedule = "* * * * *".into();
    let (mut e, up) = engine(ms, "cluster-a", dom());
    e.events_reapplier = reapplier(Ok(vec![ev(7, 2)]));
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, false)));
}

#[tokio::test]
async fn closed_workflow_without_decision_cannot_reapply() {
    let ms = FakeMs { previous_started_event_id: EMPTY_EVENT_ID, ..Default::default() };
    *ms.running.lock().unwrap() = false;
    let (mut e, up) = engine(ms, "cluster-a", dom());
    e.workflow_resetter = Arc::new(Resetter::default());
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, true)));
}

#[tokio::test]
async fn closed_workflow_is_reset_with_events() {
    let histories = VersionHistories {
        current_version_history_index: 0,
        histories: vec![VersionHistory { branch_token: b"bt".to_vec(), items: vec![VersionHistoryItem { event_id: 4, version: 1 }, VersionHistoryItem { event_id: 9, version: 3 }] }],
    };
    let ms = FakeMs { previous_started_event_id: 6, next_event_id: 10, version_histories: Some(histories), ..Default::default() };
    *ms.running.lock().unwrap() = false;
    let (mut e, up) = engine(ms, "cluster-a", dom());
    let resetter = Arc::new(Resetter::default());
    e.workflow_resetter = resetter.clone();
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2), ev(8, 2)]).await.unwrap();
    assert_eq!(*up.last_action.lock().unwrap(), Some((false, true)));
    let calls = resetter.0.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], (6, 3, crate::engine::reapply_events::EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON.to_string(), 2, b"bt".to_vec(), 10));
}

#[tokio::test]
async fn passive_pending_active_domain_is_ignored_otherwise_error() {
    let mut pending = dom();
    pending.failover_end_time = Some(1);
    let (e, up) = engine(FakeMs::default(), "cluster-b", pending);
    e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.unwrap();
    assert!(up.calls.lock().unwrap().is_empty());

    let (e, up) = engine(FakeMs::default(), "cluster-b", dom());
    assert!(e.reapply_events(DOMAIN, "wf", "run", &[ev(7, 2)]).await.is_err());
    assert!(up.calls.lock().unwrap().is_empty());
}
