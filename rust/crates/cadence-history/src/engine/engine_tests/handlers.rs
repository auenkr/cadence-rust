//! Ports of engineimpl describe_mutable_state / reset_sticky_tasklist /
//! terminate_workflow_execution / refresh_workflow_tasks tests, driven through fakes.
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::types::*;

use crate::engine::deps::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

fn domain_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn wf() -> WorkflowExecution {
    WorkflowExecution { workflow_id: "test-wf".into(), run_id: "test-run".into() }
}

fn ctx_for(ms: Arc<FakeMs>) -> Arc<FakeCtx> {
    Arc::new(FakeCtx::new(ms))
}

// ---- describe_mutable_state_test.go ----

#[tokio::test]
async fn describe_mutable_state_success() {
    let cached = Arc::new(FakeMs { persistence_json: serde_json::json!({"domain": "cache"}), ..Default::default() });
    let db = Arc::new(FakeMs { persistence_json: serde_json::json!({"domain": "db"}), ..Default::default() });
    let mut cache_ctx = FakeCtx::new(cached.clone());
    cache_ctx.cached = Some(cached.clone());
    let db_ctx = FakeCtx::new(db);
    let mut cache = FakeCache::new(Arc::new(cache_ctx));
    cache.db_ctx = std::sync::Mutex::new(Some(Arc::new(db_ctx)));
    cache.cache_hit = true;
    let cache = Arc::new(cache);
    let mut engine = nop_engine();
    engine.execution_cache = cache.clone();
    let id = domain_uuid();
    let resp = engine.describe_mutable_state(&DescribeMutableStateRequest { domain_uuid: id.clone(), execution: Some(wf()) }).await.unwrap();
    assert_eq!(resp.mutable_state_in_cache, serde_json::json!({"domain": "cache"}).to_string());
    assert_eq!(resp.mutable_state_in_database, serde_json::json!({"domain": "db"}).to_string());
    assert_eq!(*cache.releases.lock().unwrap(), vec![false]);
    assert_eq!(cache.requested.lock().unwrap()[0].0, id);
}

#[tokio::test]
async fn describe_mutable_state_error_unknown_domain() {
    let engine = nop_engine();
    let err = engine.describe_mutable_state(&DescribeMutableStateRequest { domain_uuid: "This is not a uuid".into(), execution: None }).await.unwrap_err();
    assert!(err.to_string().contains("Invalid domain UUID"), "{err}");
}

#[tokio::test]
async fn describe_mutable_state_error_get_and_create_error() {
    let mut engine = nop_engine();
    engine.execution_cache = Arc::new(FakeCache::failing("cache boom"));
    let err = engine.describe_mutable_state(&DescribeMutableStateRequest { domain_uuid: domain_uuid(), execution: Some(wf()) }).await.unwrap_err();
    assert!(err.to_string().contains("cache boom"), "{err}");
}

#[tokio::test]
async fn describe_mutable_state_error_load_from_db_error() {
    let ms = Arc::new(FakeMs::default());
    let mut db_ctx = FakeCtx::new(ms.clone());
    db_ctx.load_err = Some("load boom".into());
    let cache = FakeCache::new(Arc::new(FakeCtx::new(ms)));
    *cache.db_ctx.lock().unwrap() = Some(Arc::new(db_ctx));
    let cache = Arc::new(cache);
    let mut engine = nop_engine();
    engine.execution_cache = cache.clone();
    let err = engine.describe_mutable_state(&DescribeMutableStateRequest { domain_uuid: domain_uuid(), execution: Some(wf()) }).await.unwrap_err();
    assert!(err.to_string().contains("load boom"), "{err}");
    // release function must be called with the error
    assert_eq!(*cache.releases.lock().unwrap(), vec![true]);
}

// ---- reset_sticky_tasklist_test.go ----

fn reset_sticky_engine(ms: Arc<FakeMs>) -> (HistoryEngine, Arc<FakeUpdater>) {
    let updater = Arc::new(FakeUpdater::new(ms));
    let mut engine = nop_engine();
    engine.updater = updater.clone();
    engine.time_source = Arc::new(FixedTime(1));
    (engine, updater)
}

#[tokio::test]
async fn reset_sticky_task_list_invalid_domain() {
    let (engine, _) = reset_sticky_engine(Arc::new(FakeMs::default()));
    let err = engine.reset_sticky_task_list(&HistoryResetStickyTaskListRequest { domain_uuid: String::new(), execution: Some(wf()) }).await.unwrap_err();
    assert_eq!(err.to_string(), "Missing domain UUID.");
}

#[tokio::test]
async fn reset_sticky_task_list_completed_workflow() {
    let ms = Arc::new(FakeMs::default());
    *ms.running.lock().unwrap() = false;
    let (engine, _) = reset_sticky_engine(ms.clone());
    let err = engine.reset_sticky_task_list(&HistoryResetStickyTaskListRequest { domain_uuid: domain_uuid(), execution: Some(wf()) }).await.unwrap_err();
    assert!(err.is_workflow_already_completed(), "{err}");
    assert!(ms.calls().is_empty());
}

#[tokio::test]
async fn reset_sticky_task_list_success() {
    let ms = Arc::new(FakeMs::default());
    let (engine, updater) = reset_sticky_engine(ms.clone());
    let id = domain_uuid();
    engine.reset_sticky_task_list(&HistoryResetStickyTaskListRequest { domain_uuid: id.clone(), execution: Some(wf()) }).await.unwrap();
    assert_eq!(ms.calls(), vec!["clear_stickyness".to_string()]);
    let calls = updater.calls.lock().unwrap();
    assert_eq!(calls[0].0, id);
    assert!(!calls[0].2, "reset sticky must not require the current run");
}

// ---- terminate_workflow_execution_test.go (core cases) ----

struct ActiveEngine {
    engine: HistoryEngine,
    updater: Arc<FakeUpdater>,
}

fn terminate_engine(ms: Arc<FakeMs>, domain_id: &str) -> ActiveEngine {
    let updater = Arc::new(FakeUpdater::new(ms));
    let mut engine = nop_engine();
    engine.updater = updater.clone();
    engine.time_source = Arc::new(FixedTime(1));
    engine.active_cluster_manager = Arc::new(FixedActiveCluster("cluster-a".into()));
    engine.cluster_metadata = Arc::new(FixedClusterMetadata("cluster-a".into()));
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert(domain_id.to_string(), Arc::new(domain_entry(domain_id, "test-domain")));
    engine.domain_cache = Arc::new(dc);
    ActiveEngine { engine, updater }
}

fn terminate_request(domain_id: &str) -> HistoryTerminateWorkflowExecutionRequest {
    HistoryTerminateWorkflowExecutionRequest {
        domain_uuid: domain_id.to_string(),
        terminate_request: Some(TerminateWorkflowExecutionRequest {
            workflow_execution: Some(wf()),
            reason: "because".into(),
            identity: "tester".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn terminate_workflow_execution_success() {
    let id = domain_uuid();
    let ms = Arc::new(FakeMs::default());
    let e = terminate_engine(ms.clone(), &id);
    e.engine.terminate_workflow_execution(&terminate_request(&id)).await.unwrap();
    assert_eq!(ms.calls(), vec!["terminate:10:because:tester".to_string()]);
    assert_eq!(*e.updater.last_action.lock().unwrap(), Some((false, false)));
}

#[tokio::test]
async fn terminate_workflow_execution_already_completed() {
    let id = domain_uuid();
    let ms = Arc::new(FakeMs::default());
    *ms.running.lock().unwrap() = false;
    let e = terminate_engine(ms.clone(), &id);
    let err = e.engine.terminate_workflow_execution(&terminate_request(&id)).await.unwrap_err();
    assert!(err.is_workflow_already_completed(), "{err}");
    assert!(ms.calls().is_empty());
}

#[tokio::test]
async fn terminate_workflow_execution_first_run_id_mismatch() {
    let id = domain_uuid();
    let ms = Arc::new(FakeMs::with_info(WorkflowExecutionInfo { first_execution_run_id: "first".into(), ..Default::default() }));
    let e = terminate_engine(ms.clone(), &id);
    let mut req = terminate_request(&id);
    req.terminate_request.as_mut().unwrap().first_execution_run_id = "other".into();
    let err = e.engine.terminate_workflow_execution(&req).await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err}");
    // the run id is cleared when first_execution_run_id is set
    assert_eq!(e.updater.calls.lock().unwrap()[0].1.run_id, "");
}

#[tokio::test]
async fn terminate_workflow_execution_child_only_parent_mismatch() {
    let id = domain_uuid();
    let ms = Arc::new(FakeMs::with_info(WorkflowExecutionInfo { parent_workflow_id: "p".into(), parent_run_id: "r".into(), ..Default::default() }));
    let e = terminate_engine(ms, &id);
    let mut req = terminate_request(&id);
    req.child_workflow_only = true;
    req.external_workflow_execution = Some(WorkflowExecution { workflow_id: "p".into(), run_id: "different".into() });
    let err = e.engine.terminate_workflow_execution(&req).await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err}");
}

#[tokio::test]
async fn terminate_workflow_execution_domain_not_active() {
    let id = domain_uuid();
    let mut e = terminate_engine(Arc::new(FakeMs::default()), &id);
    e.engine.active_cluster_manager = Arc::new(FixedActiveCluster("cluster-b".into()));
    let err = e.engine.terminate_workflow_execution(&terminate_request(&id)).await.unwrap_err();
    assert!(err.to_string().contains("is a standby cluster"), "{err}");
}

// ---- refresh_workflow_tasks_test.go (engine-level cases) ----

struct FakeRefresher {
    err: Option<String>,
    seen: std::sync::Mutex<Vec<i64>>,
}
#[async_trait]
impl MutableStateTaskRefresher for FakeRefresher {
    async fn refresh_tasks(&self, start_timestamp: i64, _: MutableStateRef) -> HResult<()> {
        self.seen.lock().unwrap().push(start_timestamp);
        match &self.err {
            Some(m) => Err(HistoryError::Other(m.clone())),
            None => Ok(()),
        }
    }
}

fn refresh_engine(domain_id: &str, ctx: Arc<FakeCtx>, refresher: Arc<FakeRefresher>) -> (HistoryEngine, Arc<FakeCache>) {
    let cache = Arc::new(FakeCache::new(ctx));
    let mut engine = nop_engine();
    engine.execution_cache = cache.clone();
    engine.task_refresher = refresher;
    engine.time_source = Arc::new(FixedTime(77));
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert(domain_id.to_string(), Arc::new(domain_entry(domain_id, "test-domain")));
    engine.domain_cache = Arc::new(dc);
    (engine, cache)
}

#[tokio::test]
async fn refresh_workflow_tasks_success() {
    let id = domain_uuid();
    let ms = Arc::new(FakeMs::with_info(WorkflowExecutionInfo { start_timestamp: 555, ..Default::default() }));
    let ctx = ctx_for(ms);
    let refresher = Arc::new(FakeRefresher { err: None, seen: Default::default() });
    let (engine, cache) = refresh_engine(&id, ctx.clone(), refresher.clone());
    engine.refresh_workflow_tasks(&id, &wf()).await.unwrap();
    assert_eq!(*refresher.seen.lock().unwrap(), vec![555]);
    assert_eq!(ctx.calls(), vec!["update_workflow_execution_tasks:77".to_string()]);
    assert_eq!(*cache.releases.lock().unwrap(), vec![false]);
}

#[tokio::test]
async fn refresh_workflow_tasks_failed_to_get_workflow_execution() {
    let id = domain_uuid();
    let ctx = ctx_for(Arc::new(FakeMs::default()));
    let refresher = Arc::new(FakeRefresher { err: None, seen: Default::default() });
    let (mut engine, _) = refresh_engine(&id, ctx, refresher);
    engine.execution_cache = Arc::new(FakeCache::failing("some random error"));
    assert!(engine.refresh_workflow_tasks(&id, &wf()).await.is_err());
}

#[tokio::test]
async fn refresh_workflow_tasks_failed_to_load() {
    let id = domain_uuid();
    let mut ctx = FakeCtx::new(Arc::new(FakeMs::default()));
    ctx.load_err = Some("load failed".into());
    let refresher = Arc::new(FakeRefresher { err: None, seen: Default::default() });
    let (engine, cache) = refresh_engine(&id, Arc::new(ctx), refresher.clone());
    assert!(engine.refresh_workflow_tasks(&id, &wf()).await.is_err());
    assert!(refresher.seen.lock().unwrap().is_empty());
    assert_eq!(*cache.releases.lock().unwrap(), vec![true]);
}

#[tokio::test]
async fn refresh_workflow_tasks_failed_to_refresh() {
    let id = domain_uuid();
    let ctx = ctx_for(Arc::new(FakeMs::default()));
    let refresher = Arc::new(FakeRefresher { err: Some("refresh failed".into()), seen: Default::default() });
    let (engine, _) = refresh_engine(&id, ctx.clone(), refresher);
    assert!(engine.refresh_workflow_tasks(&id, &wf()).await.is_err());
    assert!(ctx.calls().is_empty(), "tasks must not be persisted when refresh fails");
}

#[tokio::test]
async fn refresh_workflow_tasks_failed_to_update() {
    let id = domain_uuid();
    let mut ctx = FakeCtx::new(Arc::new(FakeMs::default()));
    ctx.update_tasks_err = Some("update failed".into());
    let refresher = Arc::new(FakeRefresher { err: None, seen: Default::default() });
    let (engine, cache) = refresh_engine(&id, Arc::new(ctx), refresher);
    let err = engine.refresh_workflow_tasks(&id, &wf()).await.unwrap_err();
    assert!(err.to_string().contains("update failed"));
    assert_eq!(*cache.releases.lock().unwrap(), vec![true]);
}

#[tokio::test]
async fn refresh_workflow_tasks_unknown_domain() {
    let id = domain_uuid();
    let ctx = ctx_for(Arc::new(FakeMs::default()));
    let refresher = Arc::new(FakeRefresher { err: None, seen: Default::default() });
    let (engine, _) = refresh_engine(&id, ctx, refresher);
    let err = engine.refresh_workflow_tasks("missing-domain", &wf()).await.unwrap_err();
    assert!(err.is_entity_not_exists());
}
