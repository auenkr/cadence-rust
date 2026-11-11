//! Ports of engineimpl signal_workflow_execution / remove_signal_mutable_state tests.
use std::sync::Arc;

use cadence_api::types::*;

use crate::engine::deps::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

fn did() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn setup(ms: Arc<FakeMs>, domain_id: &str, max_signals: i32) -> (HistoryEngine, Arc<FakeUpdater>) {
    let updater = Arc::new(FakeUpdater::new(ms));
    let mut engine = nop_engine();
    engine.updater = updater.clone();
    engine.time_source = Arc::new(FixedTime(1));
    engine.active_cluster_manager = Arc::new(FixedActiveCluster("a".into()));
    engine.cluster_metadata = Arc::new(FixedClusterMetadata("a".into()));
    engine.config = Arc::new(TestConfig { max_signals, ..Default::default() });
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert(domain_id.to_string(), Arc::new(domain_entry(domain_id, "dom")));
    engine.domain_cache = Arc::new(dc);
    (engine, updater)
}

fn req(domain_id: &str, request_id: &str) -> HistorySignalWorkflowExecutionRequest {
    HistorySignalWorkflowExecutionRequest {
        domain_uuid: domain_id.to_string(),
        signal_request: Some(SignalWorkflowExecutionRequest {
            workflow_execution: Some(WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() }),
            signal_name: "sig".into(),
            request_id: request_id.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn signal_workflow_execution_success() {
    let d = did();
    let ms = Arc::new(FakeMs::default());
    let (engine, updater) = setup(ms.clone(), &d, 0);
    engine.signal_workflow_execution(&req(&d, "r1")).await.unwrap();
    assert_eq!(ms.calls(), vec!["add_signal_requested:r1".to_string(), "signal:sig".to_string()]);
    assert_eq!(*updater.last_action.lock().unwrap(), Some((true, false)));
}

#[tokio::test]
async fn signal_workflow_execution_duplicate_request_is_noop() {
    let d = did();
    let ms = Arc::new(FakeMs::default());
    ms.signal_requested.lock().unwrap().insert("r1".into());
    let (engine, updater) = setup(ms.clone(), &d, 0);
    engine.signal_workflow_execution(&req(&d, "r1")).await.unwrap();
    assert!(ms.calls().is_empty());
    assert_eq!(*updater.last_action.lock().unwrap(), Some((false, true)));
}

#[tokio::test]
async fn signal_workflow_execution_already_completed() {
    let d = did();
    let ms = Arc::new(FakeMs::default());
    *ms.running.lock().unwrap() = false;
    let (engine, _) = setup(ms, &d, 0);
    let err = engine.signal_workflow_execution(&req(&d, "")).await.unwrap_err();
    assert!(err.is_workflow_already_completed(), "{err}");
}

#[tokio::test]
async fn signal_workflow_execution_limit_exceeded() {
    let d = did();
    let ms = Arc::new(FakeMs::with_info(WorkflowExecutionInfo { signal_count: 2, ..Default::default() }));
    let (engine, _) = setup(ms.clone(), &d, 2);
    let err = engine.signal_workflow_execution(&req(&d, "")).await.unwrap_err();
    assert!(err.to_string().contains("exceeded workflow execution limit for signal events"), "{err}");
    assert!(ms.calls().is_empty());
}

#[tokio::test]
async fn signal_workflow_execution_child_only_parent_mismatch() {
    let d = did();
    let ms = Arc::new(FakeMs::with_info(WorkflowExecutionInfo { parent_workflow_id: "p".into(), parent_run_id: "r".into(), ..Default::default() }));
    let (engine, _) = setup(ms, &d, 0);
    let mut r = req(&d, "");
    r.child_workflow_only = true;
    r.external_workflow_execution = Some(WorkflowExecution { workflow_id: "p".into(), run_id: "x".into() });
    let err = engine.signal_workflow_execution(&r).await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err}");
}

#[tokio::test]
async fn signal_workflow_execution_add_signal_failure_is_internal() {
    let d = did();
    let ms = Arc::new(FakeMs::default());
    *ms.signal_err.lock().unwrap() = Some("boom".into());
    let (engine, _) = setup(ms, &d, 0);
    let err = engine.signal_workflow_execution(&req(&d, "")).await.unwrap_err();
    assert!(err.to_string().contains("Unable to signal workflow execution."), "{err}");
}

#[tokio::test]
async fn signal_workflow_execution_deprecated_domain() {
    let d = did();
    let (mut engine, _) = setup(Arc::new(FakeMs::default()), &d, 0);
    let mut dc = FakeDomainCache::default();
    let mut e = domain_entry(&d, "dom");
    e.info.status = DOMAIN_STATUS_DEPRECATED;
    dc.by_id.insert(d.clone(), Arc::new(e));
    engine.domain_cache = Arc::new(dc);
    let err = engine.signal_workflow_execution(&req(&d, "")).await.unwrap_err();
    assert!(err.to_string().contains("Domain is deprecated."), "{err}");
}

#[tokio::test]
async fn signal_workflow_execution_domain_not_active() {
    let d = did();
    let (mut engine, _) = setup(Arc::new(FakeMs::default()), &d, 0);
    engine.active_cluster_manager = Arc::new(FixedActiveCluster("b".into()));
    let err = engine.signal_workflow_execution(&req(&d, "")).await.unwrap_err();
    assert!(err.to_string().contains("standby cluster"), "{err}");
}

#[tokio::test]
async fn remove_signal_mutable_state_success_and_completed() {
    let d = did();
    let ms = Arc::new(FakeMs::default());
    let (engine, updater) = setup(ms.clone(), &d, 0);
    let r = RemoveSignalMutableStateRequest {
        domain_uuid: d.clone(),
        workflow_execution: Some(WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() }),
        request_id: "rid".into(),
    };
    engine.remove_signal_mutable_state(&r).await.unwrap();
    assert_eq!(ms.calls(), vec!["delete_signal_requested:rid".to_string()]);
    assert!(!updater.calls.lock().unwrap()[0].2);
    *ms.running.lock().unwrap() = false;
    let err = engine.remove_signal_mutable_state(&r).await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err}");
}
