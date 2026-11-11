//! Ports of engineimpl/register_domain_failover_callback_test.go (callback-level cases).
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::engine::notify_tasks::NotifyTaskInfo;
use crate::engine::register_domain_failover_callback::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::deps::HResult;

#[derive(Default)]
struct RecQueue {
    log: Arc<Mutex<Vec<String>>>,
    name: &'static str,
}

#[async_trait]
impl QueueProcessor for RecQueue {
    fn start(&self) {}
    async fn stop(&self) {}
    async fn handle_action(&self, _: &str, _: QueueAction) -> HResult<Vec<String>> {
        unimplemented!()
    }
    fn notify_new_task(&self, cluster: &str, info: &NotifyTaskInfo) {
        self.log.lock().unwrap().push(format!("{}:notify:{}:{}", self.name, cluster, info.tasks.len()));
    }
    async fn failover_domain(&self, ids: &HashSet<String>) {
        let mut v: Vec<_> = ids.iter().cloned().collect();
        v.sort();
        self.log.lock().unwrap().push(format!("{}:failover:{}", self.name, v.join(",")));
    }
    async fn lock_task_processing(&self) {
        self.log.lock().unwrap().push(format!("{}:lock", self.name));
    }
    fn unlock_task_processing(&self) {
        self.log.lock().unwrap().push(format!("{}:unlock", self.name));
    }
}

struct RecHost {
    version: i64,
    dlq: bool,
    marker_err: bool,
    new_version: Mutex<Option<i64>>,
    markers: Mutex<Vec<FailoverMarkerTask>>,
    partitions: Mutex<Vec<FailoverDlqPartition>>,
    registered: Mutex<Vec<(String, i64)>>,
}

impl RecHost {
    fn new(version: i64) -> Self {
        Self { version, dlq: true, marker_err: false, new_version: Mutex::new(None), markers: Mutex::new(vec![]), partitions: Mutex::new(vec![]), registered: Mutex::new(vec![]) }
    }
}

#[async_trait]
impl DomainFailoverHost for RecHost {
    fn domain_notification_version(&self) -> i64 {
        self.version
    }
    fn update_domain_notification_version(&self, v: i64) -> HResult<()> {
        *self.new_version.lock().unwrap() = Some(v);
        Ok(())
    }
    async fn replicate_failover_markers(&self, m: Vec<FailoverMarkerTask>) -> HResult<()> {
        if self.marker_err {
            return Err(internal("marker boom"));
        }
        self.markers.lock().unwrap().extend(m);
        Ok(())
    }
    fn has_dlq_processor(&self) -> bool {
        self.dlq
    }
    fn failover_dlq_partitions(&self, p: Vec<FailoverDlqPartition>) {
        self.partitions.lock().unwrap().extend(p);
    }
    fn register_domain_change_callback(&self, name: &str, v: i64, _: DomainLockFn, _: DomainChangedFn) {
        self.registered.lock().unwrap().push((name.to_string(), v));
    }
}

fn setup(host: Arc<RecHost>) -> (HistoryEngine, Arc<Mutex<Vec<String>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut e = nop_engine();
    e.current_cluster_name = "a".into();
    e.cluster_metadata = Arc::new(FixedClusterMetadata("a".into()));
    e.time_source = Arc::new(FixedTime(77));
    e.failover_host = host;
    e.start_host = Arc::new(NopShard(3));
    e.queue_processors.insert(crate::engine::deps::HistoryTaskCategory::Transfer, Arc::new(RecQueue { log: log.clone(), name: "transfer" }));
    e.queue_processors.insert(crate::engine::deps::HistoryTaskCategory::Timer, Arc::new(RecQueue { log: log.clone(), name: "timer" }));
    (e, log)
}

fn global(id: &str, nv: i64, fnv: i64, active: &str) -> FailoverDomainEntry {
    FailoverDomainEntry {
        id: id.into(),
        name: id.into(),
        is_global_domain: true,
        notification_version: nv,
        failover_notification_version: fnv,
        active_cluster_name: active.into(),
        previous_failover_version: INITIAL_PREVIOUS_FAILOVER_VERSION,
        ..Default::default()
    }
}

fn sorted(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    let mut v = log.lock().unwrap().clone();
    v.sort();
    v
}

#[tokio::test]
async fn empty_domains_only_unlocks() {
    let host = Arc::new(RecHost::new(5));
    let (e, log) = setup(host.clone());
    e.domain_change_cb(&[]).await;
    assert_eq!(sorted(&log), vec!["timer:unlock", "transfer:unlock"]);
    assert!(host.new_version.lock().unwrap().is_none());
}

#[tokio::test]
async fn local_domains_are_ignored_but_version_advances() {
    let host = Arc::new(RecHost::new(5));
    let (e, log) = setup(host.clone());
    let local = FailoverDomainEntry { id: "l".into(), notification_version: 9, ..Default::default() };
    e.domain_change_cb(&[local]).await;
    assert_eq!(sorted(&log), vec!["timer:unlock", "transfer:unlock"]);
    assert_eq!(*host.new_version.lock().unwrap(), Some(10));
    assert!(host.partitions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn active_passive_failover_to_current_cluster() {
    let host = Arc::new(RecHost::new(5));
    let (e, log) = setup(host.clone());
    e.domain_change_cb(&[global("d1", 6, 5, "a"), global("d2", 7, 4, "a"), global("d3", 8, 8, "b")]).await;
    let l = sorted(&log);
    assert!(l.contains(&"transfer:failover:d1".to_string()), "{l:?}");
    assert!(l.contains(&"timer:failover:d1".to_string()), "{l:?}");
    assert!(l.contains(&"transfer:notify:a:1".to_string()), "{l:?}");
    assert!(l.contains(&"timer:notify:a:1".to_string()), "{l:?}");
    assert!(l.contains(&"transfer:unlock".to_string()));
    assert_eq!(
        *host.partitions.lock().unwrap(),
        vec![FailoverDlqPartition { domain_id: "d1".into(), cluster_attribute_scope: String::new(), cluster_attribute_name: String::new() }]
    );
    assert_eq!(*host.new_version.lock().unwrap(), Some(9));
}

#[tokio::test]
async fn no_dlq_processor_skips_partitions() {
    let mut h = RecHost::new(5);
    h.dlq = false;
    let host = Arc::new(h);
    let (e, _log) = setup(host.clone());
    e.domain_change_cb(&[global("d1", 6, 5, "a")]).await;
    assert!(host.partitions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn active_active_failover_creates_attribute_partitions_without_failover_queue() {
    let host = Arc::new(RecHost::new(5));
    let (e, log) = setup(host.clone());
    let mut d = global("aa", 6, 6, "");
    d.active_cluster_scopes = Some(BTreeMap::from([("city".to_string(), BTreeMap::from([("nyc".to_string(), "a".to_string()), ("sf".to_string(), "b".to_string())]))]));
    e.domain_change_cb(&[d]).await;
    let l = sorted(&log);
    assert!(!l.iter().any(|s| s.contains(":failover:")), "{l:?}");
    assert!(l.contains(&"transfer:notify:a:1".to_string()));
    assert_eq!(
        *host.partitions.lock().unwrap(),
        vec![FailoverDlqPartition { domain_id: "aa".into(), cluster_attribute_scope: "city".into(), cluster_attribute_name: "nyc".into() }]
    );
}

#[tokio::test]
async fn graceful_failover_marker_replicated() {
    let host = Arc::new(RecHost::new(5));
    let (e, _log) = setup(host.clone());
    let mut d = global("d", 6, 6, "b");
    d.previous_failover_version = 2;
    d.failover_version = 12;
    e.domain_change_cb(&[d]).await;
    assert_eq!(*host.markers.lock().unwrap(), vec![FailoverMarkerTask { version: 12, domain_id: "d".into() }]);
    assert_eq!(*host.new_version.lock().unwrap(), Some(7));
}

#[tokio::test]
async fn marker_replication_failure_keeps_notification_version() {
    let mut h = RecHost::new(5);
    h.marker_err = true;
    let host = Arc::new(h);
    let (e, log) = setup(host.clone());
    let mut d = global("d", 6, 6, "b");
    d.previous_failover_version = 2;
    e.domain_change_cb(&[d]).await;
    assert!(host.new_version.lock().unwrap().is_none());
    assert!(sorted(&log).contains(&"transfer:unlock".to_string()), "unlock must still run");
}

#[tokio::test]
async fn lock_and_unlock_touch_all_processors() {
    let host = Arc::new(RecHost::new(0));
    let (e, log) = setup(host);
    e.lock_task_processing_for_domain_update().await;
    e.unlock_processing_for_domain_update();
    assert_eq!(sorted(&log), vec!["timer:lock", "timer:unlock", "transfer:lock", "transfer:unlock"]);
}

#[test]
fn notify_queues_requires_both_processors() {
    let host = Arc::new(RecHost::new(0));
    let (mut e, log) = setup(host);
    e.queue_processors.remove(&crate::engine::deps::HistoryTaskCategory::Timer);
    e.notify_queues();
    assert_eq!(sorted(&log), vec!["transfer:notify:a:1"]);
    e.queue_processors.clear();
    e.notify_queues();
}

#[test]
fn register_uses_shard_name_and_initial_version() {
    let host = Arc::new(RecHost::new(42));
    let (e, _log) = setup(host.clone());
    Arc::new(e).register_domain_failover_callback();
    let reg = host.registered.lock().unwrap().clone();
    assert_eq!(reg.len(), 1);
    assert_eq!(reg[0].0, crate::engine::workflow_mutations::create_shard_name_from_shard_id(3));
    assert_eq!(reg[0].1, 42);
}
