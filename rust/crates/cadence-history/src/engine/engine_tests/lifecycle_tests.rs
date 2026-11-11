//! Ports of the Start/Stop parts of history_engine_test.go / history_engine_test lifecycle cases.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::deps::HistoryTaskCategory;
use crate::engine::lifecycle::EngineLifecycleHost;
use crate::engine::notify_tasks::NotifyTaskInfo;
use crate::engine::register_domain_failover_callback::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;

type Log = Arc<Mutex<Vec<String>>>;

struct Q(Log);
#[async_trait]
impl QueueProcessor for Q {
    fn start(&self) {
        self.0.lock().unwrap().push("queue.start".into());
    }
    async fn stop(&self) {
        self.0.lock().unwrap().push("queue.stop".into());
    }
    async fn handle_action(&self, _: &str, _: QueueAction) -> HResult<Vec<String>> {
        unimplemented!()
    }
    fn notify_new_task(&self, _: &str, _: &NotifyTaskInfo) {}
    async fn failover_domain(&self, _: &HashSet<String>) {}
    async fn lock_task_processing(&self) {}
    fn unlock_task_processing(&self) {}
}

struct Host {
    log: Log,
    graceful: bool,
    dlq_start_err: bool,
}

impl Host {
    fn rec(&self, s: &str) {
        self.log.lock().unwrap().push(s.to_string());
    }
}

#[async_trait]
impl EngineLifecycleHost for Host {
    async fn start_dlq_processor(&self, _: CancellationToken) -> HResult<()> {
        self.rec("dlq.start");
        if self.dlq_start_err {
            Err(internal("dlq boom"))
        } else {
            Ok(())
        }
    }
    async fn stop_dlq_processor(&self, _: CancellationToken) -> HResult<()> {
        self.rec("dlq.stop");
        Ok(())
    }
    fn start_replication_dlq_handler(&self) {
        self.rec("dlqhandler.start");
    }
    fn stop_replication_dlq_handler(&self) {
        self.rec("dlqhandler.stop");
    }
    fn start_replication_metrics_emitter(&self) {
        self.rec("emitter.start");
    }
    fn stop_replication_metrics_emitter(&self) {
        self.rec("emitter.stop");
    }
    fn start_replication_task_processors(&self) {
        self.rec("repl.start");
    }
    fn stop_replication_task_processors(&self) {
        self.rec("repl.stop");
    }
    fn enable_graceful_failover(&self) -> bool {
        self.graceful
    }
    fn start_failover_marker_notifier(&self) {
        self.rec("marker.start");
    }
    fn stop_failover_marker_notifier(&self) {
        self.rec("marker.stop");
    }
}

struct Failover(Log);
#[async_trait]
impl DomainFailoverHost for Failover {
    fn domain_notification_version(&self) -> i64 {
        0
    }
    fn update_domain_notification_version(&self, _: i64) -> HResult<()> {
        Ok(())
    }
    async fn replicate_failover_markers(&self, _: Vec<FailoverMarkerTask>) -> HResult<()> {
        Ok(())
    }
    fn has_dlq_processor(&self) -> bool {
        false
    }
    fn failover_dlq_partitions(&self, _: Vec<FailoverDlqPartition>) {}
    fn register_domain_change_callback(&self, _: &str, _: i64, _: DomainLockFn, _: DomainChangedFn) {
        self.0.lock().unwrap().push("callback.registered".into());
    }
}

fn engine(graceful: bool, dlq_err: bool) -> (Arc<HistoryEngine>, Log) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let mut e = nop_engine();
    e.lifecycle_host = Arc::new(Host { log: log.clone(), graceful, dlq_start_err: dlq_err });
    e.failover_host = Arc::new(Failover(log.clone()));
    e.start_host = Arc::new(NopShard(1));
    e.queue_processors.insert(HistoryTaskCategory::Transfer, Arc::new(Q(log.clone())));
    (Arc::new(e), log)
}

#[tokio::test]
async fn start_runs_components_in_order() {
    let (e, log) = engine(false, false);
    e.start().await;
    assert_eq!(*log.lock().unwrap(), vec!["queue.start", "dlq.start", "dlqhandler.start", "emitter.start", "callback.registered", "repl.start"]);
    assert!(e.lifecycle_cancel.lock().unwrap().is_some());
}

#[tokio::test]
async fn start_with_graceful_failover_starts_marker_notifier() {
    let (e, log) = engine(true, false);
    e.start().await;
    assert_eq!(log.lock().unwrap().last().unwrap(), "marker.start");
}

#[tokio::test]
async fn start_continues_when_dlq_processor_fails() {
    let (e, log) = engine(false, true);
    e.start().await;
    let l = log.lock().unwrap().clone();
    assert!(l.contains(&"repl.start".to_string()), "{l:?}");
}

#[tokio::test]
async fn stop_stops_components_and_cancels_context() {
    let (e, log) = engine(false, false);
    e.start().await;
    let token = e.lifecycle_cancel.lock().unwrap().clone().unwrap();
    log.lock().unwrap().clear();
    e.stop().await;
    assert_eq!(*log.lock().unwrap(), vec!["queue.stop", "dlq.stop", "dlqhandler.stop", "emitter.stop", "repl.stop", "marker.stop"]);
    assert!(token.is_cancelled());
    assert!(e.lifecycle_cancel.lock().unwrap().is_none());
}

#[tokio::test]
async fn stop_without_start_is_safe() {
    let (e, log) = engine(false, false);
    e.stop().await;
    assert!(log.lock().unwrap().contains(&"dlq.stop".to_string()));
}
