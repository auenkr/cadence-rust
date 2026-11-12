//! Test harness: the real `HistoryEngine` (real execution cache, workflow updater, decision handler
//! and start host over `EngineMutableState`) on top of an in-memory `GoExecutionStore` /
//! `GoHistoryV2Store`. Used by `real_engine_tests` (ports of history_engine*_test.go flows).
#![cfg(test)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_persistence::history_contract as pc;
use pc::PersistenceError;

use crate::config::Config;
use crate::engine::deps::{DomainCache, DomainCacheEntry, DomainInfo, HResult, HistoryError, TimeSource};
use crate::engine::runtime::{ActiveClusterManager, HistoryEngine};
use crate::engine::test_support::nop_engine;
use crate::shard_context::{ClusterInfo, ShardContext, ShardContextError, ShardInfoData, ShardPersistence};
use crate::wiring::assembly::engine_cache::{EngineCore, EngineExecutionCache, EngineWorkflowUpdater};
use crate::wiring::assembly::engine_decision::EngineDecisionHandler;
use crate::wiring::assembly::engine_start::EngineStartHost;
use crate::wiring::assembly::engine_state::StateParams;

pub const DOMAIN_ID: &str = "d0a1b2c3-0000-4000-8000-000000000001";
pub const DOMAIN_NAME: &str = "test-domain";
pub const CLUSTER: &str = "cluster-a";

type RunKey = (String, String, String);

#[derive(Default)]
pub struct Inner {
    pub runs: HashMap<RunKey, pc::WorkflowSnapshot>,
    pub current: HashMap<(String, String), pc::CurrentExecution>,
    pub transfer: Vec<pc::Task>,
    pub timer: Vec<pc::Task>,
    pub updates: usize,
    pub fail_updates: usize,
    /// Errors returned (FIFO) by the next create/update calls instead of touching state (gomock `.Return(nil, err)`).
    pub create_errors: std::collections::VecDeque<PersistenceError>,
    pub update_errors: std::collections::VecDeque<PersistenceError>,
    /// Recorded create requests: (mode name, previous run id, previous last write version).
    pub create_calls: Vec<(&'static str, String, i64)>,
}

/// In-memory execution store: create/get/update/current-execution plus task capture.
#[derive(Default)]
pub struct MemExecutions {
    pub inner: Mutex<Inner>,
}

fn merge_keyed<K: Clone + Ord>(dst: &mut Vec<pc::KeyedBlob<K>>, upserts: &[pc::KeyedBlob<K>], deletes: &[K]) {
    dst.retain(|x| !deletes.contains(&x.key) && !upserts.iter().any(|u| u.key == x.key));
    dst.extend(upserts.iter().cloned());
    dst.sort_by(|a, b| a.key.cmp(&b.key));
}

fn store_tasks(inner: &mut Inner, tasks: &BTreeMap<pc::HistoryTaskCategory, Vec<pc::Task>>) {
    for (cat, ts) in tasks {
        match cat.category_type {
            1 => inner.transfer.extend(ts.iter().cloned()),
            2 => inner.timer.extend(ts.iter().cloned()),
            _ => {}
        }
    }
}

fn apply_mutation(snap: &mut pc::WorkflowSnapshot, m: &pc::WorkflowMutation) {
    snap.execution_info = m.execution_info.clone();
    snap.version_histories = m.version_histories.clone();
    snap.start_version = m.start_version;
    snap.last_write_version = m.last_write_version;
    merge_keyed(&mut snap.activity_infos, &m.upsert_activity_infos, &m.delete_activity_infos);
    merge_keyed(&mut snap.timer_infos, &m.upsert_timer_infos, &m.delete_timer_infos);
    merge_keyed(&mut snap.child_execution_infos, &m.upsert_child_execution_infos, &m.delete_child_execution_infos);
    merge_keyed(&mut snap.request_cancel_infos, &m.upsert_request_cancel_infos, &m.delete_request_cancel_infos);
    merge_keyed(&mut snap.signal_infos, &m.upsert_signal_infos, &m.delete_signal_infos);
    snap.signal_requested_ids.retain(|x| !m.delete_signal_requested_ids.contains(x));
    for id in &m.upsert_signal_requested_ids {
        if !snap.signal_requested_ids.contains(id) {
            snap.signal_requested_ids.push(id.clone());
        }
    }
    if m.clear_buffered_events {
        snap.buffered_events.clear();
    }
    if let Some(b) = &m.new_buffered_events {
        snap.buffered_events.push(b.clone());
    }
    snap.checksum = m.checksum.clone();
    snap.condition = m.condition;
    snap.workflow_requests.extend(m.workflow_requests.iter().cloned());
}

impl MemExecutions {
    fn set_current(inner: &mut Inner, info: &pc::WorkflowExecutionInfo, start_version: i64, last_write: i64) {
        inner.current.insert(
            (info.domain_id.clone(), info.workflow_id.clone()),
            pc::CurrentExecution {
                run_id: info.run_id.clone(),
                create_request_id: info.create_request_id.clone(),
                state: info.state,
                close_status: info.close_status,
                start_version,
                last_write_version: last_write,
            },
        );
    }
    pub fn snapshot(&self, wf: &str, run: &str) -> Option<pc::WorkflowSnapshot> {
        self.inner.lock().unwrap().runs.get(&(DOMAIN_ID.into(), wf.into(), run.into())).cloned()
    }
    pub fn current_run(&self, wf: &str) -> Option<pc::CurrentExecution> {
        self.inner.lock().unwrap().current.get(&(DOMAIN_ID.to_string(), wf.to_string())).cloned()
    }
    pub fn transfer_types(&self) -> Vec<i32> {
        self.inner.lock().unwrap().transfer.iter().map(|t| t.task_type).collect()
    }
    pub fn timer_types(&self) -> Vec<i32> {
        self.inner.lock().unwrap().timer.iter().map(|t| t.task_type).collect()
    }
}

#[async_trait]
impl pc::GoExecutionStore for MemExecutions {
    async fn create_workflow_execution(&self, r: pc::CreateWorkflowExecutionRequest) -> Result<pc::CurrentExecution, PersistenceError> {
        let mut g = self.inner.lock().unwrap();
        let mode_name = match r.mode {
            pc::CreateWorkflowMode::BrandNew => "BrandNew",
            pc::CreateWorkflowMode::WorkflowIdReuse => "WorkflowIdReuse",
            pc::CreateWorkflowMode::ContinueAsNew => "ContinueAsNew",
            pc::CreateWorkflowMode::Zombie => "Zombie",
        };
        g.create_calls.push((mode_name, r.previous_run_id.clone(), r.previous_last_write_version));
        if let Some(e) = g.create_errors.pop_front() {
            return Err(e);
        }
        let s = r.new_workflow_snapshot;
        let info = s.execution_info.clone();
        let key_cur = (info.domain_id.clone(), info.workflow_id.clone());
        if let Some(cur) = g.current.get(&key_cur).cloned() {
            match r.mode {
                pc::CreateWorkflowMode::BrandNew => {
                    return Err(PersistenceError::WorkflowAlreadyStarted {
                        message: "Workflow execution already running.".into(),
                        start_request_id: cur.create_request_id,
                        run_id: cur.run_id,
                        state: cur.state,
                        close_status: cur.close_status,
                        last_write_version: cur.last_write_version,
                    })
                }
                pc::CreateWorkflowMode::WorkflowIdReuse | pc::CreateWorkflowMode::ContinueAsNew => {
                    if cur.run_id != r.previous_run_id {
                        return Err(PersistenceError::CurrentWorkflowConditionFailed(format!(
                            "current run {} != previous {}",
                            cur.run_id, r.previous_run_id
                        )));
                    }
                }
                pc::CreateWorkflowMode::Zombie => {}
            }
        } else if matches!(r.mode, pc::CreateWorkflowMode::WorkflowIdReuse | pc::CreateWorkflowMode::ContinueAsNew) {
            return Err(PersistenceError::CurrentWorkflowConditionFailed("no current run".into()));
        }
        store_tasks(&mut g, &s.tasks_by_category);
        if !matches!(r.mode, pc::CreateWorkflowMode::Zombie) {
            Self::set_current(&mut g, &info, s.start_version, s.last_write_version);
        }
        let out = pc::CurrentExecution { run_id: info.run_id.clone(), ..Default::default() };
        g.runs.insert((info.domain_id.clone(), info.workflow_id.clone(), info.run_id.clone()), s);
        Ok(out)
    }
    async fn get_workflow_execution(&self, _: i32, d: String, w: String, r: String, _: i64) -> Result<pc::WorkflowSnapshot, PersistenceError> {
        self.inner
            .lock()
            .unwrap()
            .runs
            .get(&(d, w, r))
            .cloned()
            .ok_or_else(|| PersistenceError::EntityNotExists("workflow execution not found".into()))
    }
    async fn update_workflow_execution(&self, r: pc::UpdateWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.update_errors.pop_front() {
            return Err(e);
        }
        if g.fail_updates > 0 {
            g.fail_updates -= 1;
            return Err(PersistenceError::Internal("injected update failure".into()));
        }
        let m = r.update_workflow_mutation;
        let info = &m.execution_info;
        let key = (info.domain_id.clone(), info.workflow_id.clone(), info.run_id.clone());
        let Some(snap) = g.runs.get_mut(&key) else {
            return Err(PersistenceError::EntityNotExists("workflow execution not found".into()));
        };
        // Go's DB condition is the stored next_event_id.
        if snap.execution_info.next_event_id != m.condition {
            return Err(PersistenceError::ConditionFailed(format!("condition {} != {}", snap.execution_info.next_event_id, m.condition)));
        }
        apply_mutation(snap, &m);
        snap.condition = snap.execution_info.next_event_id;
        store_tasks(&mut g, &m.tasks_by_category);
        if matches!(r.mode, pc::UpdateWorkflowMode::UpdateCurrent) {
            Self::set_current(&mut g, info, m.start_version, m.last_write_version);
        }
        if let Some(n) = r.new_workflow_snapshot {
            let ni = n.execution_info.clone();
            store_tasks(&mut g, &n.tasks_by_category);
            Self::set_current(&mut g, &ni, n.start_version, n.last_write_version);
            g.runs.insert((ni.domain_id.clone(), ni.workflow_id.clone(), ni.run_id.clone()), n);
        }
        g.updates += 1;
        Ok(())
    }
    /// Go `sqlExecutionStore.ConflictResolveWorkflowExecution`: validate mode, assert/update the current
    /// record, replace the reset run, apply the optional current mutation and insert the optional new run.
    async fn conflict_resolve_workflow_execution(&self, r: pc::ConflictResolveWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        let mut g = self.inner.lock().unwrap();
        let reset = r.reset_workflow_snapshot;
        let cur_wf = r.current_workflow_mutation;
        let new_wf = r.new_workflow_snapshot;
        let rinfo = reset.execution_info.clone();
        let cur_key = (rinfo.domain_id.clone(), rinfo.workflow_id.clone());
        let current_row = g.current.get(&cur_key).cloned();
        // Go ValidateConflictResolveWorkflowModeState (subset relevant to the mode).
        match r.mode {
            pc::ConflictResolveWorkflowMode::BypassCurrent => {
                if current_row.as_ref().map(|c| c.run_id == rinfo.run_id).unwrap_or(false) {
                    return Err(PersistenceError::CurrentWorkflowConditionFailed(format!(
                        "ConflictResolveWorkflowExecution: reset run {} is the current run",
                        rinfo.run_id
                    )));
                }
            }
            pc::ConflictResolveWorkflowMode::UpdateCurrent => {
                let (info, sv, lw) = match &new_wf {
                    Some(n) => (n.execution_info.clone(), n.start_version, n.last_write_version),
                    None => (rinfo.clone(), reset.start_version, reset.last_write_version),
                };
                let prev_run = cur_wf.as_ref().map(|m| m.execution_info.run_id.clone()).unwrap_or_else(|| rinfo.run_id.clone());
                match &current_row {
                    Some(c) if c.run_id == prev_run => {}
                    _ => {
                        return Err(PersistenceError::CurrentWorkflowConditionFailed(format!(
                            "ConflictResolveWorkflowExecution: current run is not {prev_run}"
                        )))
                    }
                }
                Self::set_current(&mut g, &info, sv, lw);
            }
        }
        let key = (rinfo.domain_id.clone(), rinfo.workflow_id.clone(), rinfo.run_id.clone());
        match g.runs.get(&key) {
            None => return Err(PersistenceError::EntityNotExists("workflow execution not found".into())),
            Some(s) if s.execution_info.next_event_id != reset.condition => {
                return Err(PersistenceError::ConditionFailed(format!("condition {} != {}", s.execution_info.next_event_id, reset.condition)));
            }
            _ => {}
        }
        store_tasks(&mut g, &reset.tasks_by_category);
        let mut stored = reset;
        stored.condition = stored.execution_info.next_event_id;
        g.runs.insert(key, stored);
        if let Some(m) = cur_wf {
            let k = (m.execution_info.domain_id.clone(), m.execution_info.workflow_id.clone(), m.execution_info.run_id.clone());
            let Some(snap) = g.runs.get_mut(&k) else {
                return Err(PersistenceError::EntityNotExists("workflow execution not found".into()));
            };
            if snap.execution_info.next_event_id != m.condition {
                return Err(PersistenceError::ConditionFailed(format!("condition {} != {}", snap.execution_info.next_event_id, m.condition)));
            }
            apply_mutation(snap, &m);
            snap.condition = snap.execution_info.next_event_id;
            store_tasks(&mut g, &m.tasks_by_category);
        }
        if let Some(n) = new_wf {
            let ni = n.execution_info.clone();
            store_tasks(&mut g, &n.tasks_by_category);
            g.runs.insert((ni.domain_id.clone(), ni.workflow_id.clone(), ni.run_id.clone()), n);
        }
        g.updates += 1;
        Ok(())
    }
    async fn delete_workflow_execution(&self, _: i32, d: String, w: String, r: String) -> Result<(), PersistenceError> {
        self.inner.lock().unwrap().runs.remove(&(d, w, r));
        Ok(())
    }
    async fn delete_current_workflow_execution(&self, _: i32, d: String, w: String, r: String) -> Result<(), PersistenceError> {
        let mut g = self.inner.lock().unwrap();
        if g.current.get(&(d.clone(), w.clone())).map(|c| c.run_id == r).unwrap_or(false) {
            g.current.remove(&(d, w));
        }
        Ok(())
    }
    async fn get_current_execution(&self, _: i32, d: String, w: String) -> Result<pc::CurrentExecution, PersistenceError> {
        let g = self.inner.lock().unwrap();
        let mut cur = g
            .current
            .get(&(d.clone(), w.clone()))
            .cloned()
            .ok_or_else(|| PersistenceError::EntityNotExists("current workflow execution not found".into()))?;
        // The current record mirrors the run's state, like the SQL implementation.
        if let Some(s) = g.runs.get(&(d, w, cur.run_id.clone())) {
            cur.state = s.execution_info.state;
            cur.close_status = s.execution_info.close_status;
            cur.last_write_version = s.last_write_version;
        }
        Ok(cur)
    }
    async fn is_workflow_execution_exists(&self, _: i32, d: String, w: String, r: String) -> Result<bool, PersistenceError> {
        Ok(self.inner.lock().unwrap().runs.contains_key(&(d, w, r)))
    }
    async fn list_concrete_executions(&self, _: pc::ExecutionListFilter) -> Result<pc::ExecutionPage, PersistenceError> {
        Ok(Default::default())
    }
    async fn list_current_executions(&self, _: pc::ExecutionListFilter) -> Result<pc::ExecutionPage, PersistenceError> {
        Ok(Default::default())
    }
    async fn create_transfer_tasks(&self, _: i32, _: Vec<pc::TransferTask>) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_transfer_tasks(&self, _: i32, _: i64, _: i64, _: i32) -> Result<Vec<pc::TransferTask>, PersistenceError> { Ok(vec![]) }
    async fn complete_transfer_task(&self, _: i32, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn create_failover_marker_tasks(&self, _: i32, _: Vec<pc::Task>) -> Result<(), PersistenceError> { Ok(()) }
    async fn create_history_tasks(&self, _: pc::CreateHistoryTasksRequest) -> Result<(), PersistenceError> { Ok(()) }
    async fn create_timer_tasks(&self, _: i32, _: Vec<pc::TimerTask>) -> Result<(), PersistenceError> { Ok(()) }
    async fn select_timer_tasks(&self, _: i32, _: i64, _: i64, _: i32) -> Result<Vec<pc::TimerTask>, PersistenceError> { Ok(vec![]) }
    async fn complete_timer_task(&self, _: i32, _: i64, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn select_workflow_timer_tasks(&self, _: pc::SelectWorkflowTimerTasksRequest) -> Result<Vec<pc::HistoryTaskKey>, PersistenceError> { Ok(vec![]) }
    async fn create_replication_tasks(&self, _: i32, _: Vec<pc::ReplicationTask>) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_replication_tasks(&self, _: i32, _: i64, _: i64, _: i32) -> Result<Vec<pc::ReplicationTask>, PersistenceError> { Ok(vec![]) }
    async fn complete_replication_task(&self, _: i32, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_replication_dlq_size(&self, _: String) -> Result<i64, PersistenceError> { Ok(0) }
    async fn delete_replication_task_from_dlq(&self, _: String, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn put_replication_task_to_dlq(&self, _: String, _: pc::ReplicationTask) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_replication_tasks_from_dlq(&self, _: String, _: i64, _: i64, _: i32) -> Result<Vec<pc::ReplicationTask>, PersistenceError> { Ok(vec![]) }
    async fn range_delete_replication_tasks_from_dlq(&self, _: String, _: i64, _: i32) -> Result<u64, PersistenceError> { Ok(0) }
    async fn create_cross_cluster_tasks(&self, _: i32, _: Vec<pc::CrossClusterTask>) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_cross_cluster_tasks(&self, _: i32, _: i64, _: i64, _: i32) -> Result<Vec<pc::CrossClusterTask>, PersistenceError> { Ok(vec![]) }
    async fn complete_cross_cluster_task(&self, _: i32, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn create_replication_tasks_from_dlq(&self, _: String, _: Vec<pc::ReplicationTask>) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_history_tasks(&self, _: pc::TaskRange, _: pc::HistoryTaskCategory) -> Result<Vec<pc::Task>, PersistenceError> { Ok(vec![]) }
    async fn complete_history_task(&self, _: i32, _: pc::HistoryTaskCategory, _: i64) -> Result<(), PersistenceError> { Ok(()) }
    async fn range_complete_history_tasks(&self, _: pc::TaskRange, _: pc::HistoryTaskCategory) -> Result<u64, PersistenceError> { Ok(0) }
    async fn get_replication_dlq_page(&self, _: cadence_persistence::history_task_sql::ReplicationDlqQuery) -> Result<cadence_persistence::history_task_sql::ReplicationDlqPage, PersistenceError> {
        Err(PersistenceError::Internal("unsupported".into()))
    }
    async fn range_delete_replication_dlq_page(&self, _: i32, _: String, _: i64, _: i64, _: i32) -> Result<u64, PersistenceError> { Ok(0) }
    async fn get_history_task_page(&self, _: cadence_persistence::history_task_sql::HistoryTaskQuery) -> Result<cadence_persistence::history_task_sql::HistoryTaskPage, PersistenceError> {
        Err(PersistenceError::Internal("unsupported".into()))
    }
    async fn complete_history_task_batch(&self, _: i32, _: pc::HistoryTaskCategory, _: Vec<(i64, i64)>) -> Result<(), PersistenceError> { Ok(()) }
    async fn range_complete_history_task_page(&self, _: cadence_persistence::history_task_sql::HistoryTaskQuery) -> Result<u64, PersistenceError> { Ok(0) }
    async fn create_failover_markers_with_range(&self, _: i32, _: i64, _: Vec<pc::Task>) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_active_cluster_selection_policy(&self, _: i32, _: String, _: String, _: String) -> Result<Option<pc::DataBlob>, PersistenceError> { Ok(None) }
    async fn delete_active_cluster_selection_policy(&self, _: i32, _: String, _: String, _: String) -> Result<(), PersistenceError> { Ok(()) }
}

/// In-memory history branch store: appended node batches per (tree, branch).
#[derive(Default)]
pub struct MemHistory {
    pub nodes: Mutex<Vec<pc::HistoryBranchRequest>>,
}

#[async_trait]
impl pc::GoHistoryV2Store for MemHistory {
    async fn append_history_nodes(&self, r: pc::HistoryBranchRequest) -> Result<(), PersistenceError> {
        self.nodes.lock().unwrap().push(r);
        Ok(())
    }
    async fn read_history_branch(&self, r: pc::HistoryBranchRequest) -> Result<pc::HistoryBranchPage, PersistenceError> {
        let g = self.nodes.lock().unwrap();
        let nodes = g
            .iter()
            .filter(|n| n.tree_id == r.tree_id && n.branch_id == r.branch_id && n.node_id >= r.min_node_id && (r.max_node_id == 0 || n.node_id < r.max_node_id))
            .map(|n| n.data.clone())
            .collect();
        Ok(pc::HistoryBranchPage { nodes, ..Default::default() })
    }
    async fn fork_history_branch(&self, r: pc::HistoryBranchRequest, _: i64, new_branch_id: String) -> Result<pc::HistoryBranchRequest, PersistenceError> {
        Ok(pc::HistoryBranchRequest { branch_id: new_branch_id, ..r })
    }
    async fn delete_history_branch(&self, _: pc::HistoryBranchRequest) -> Result<(), PersistenceError> { Ok(()) }
    async fn get_history_tree(&self, _: pc::HistoryBranchRequest) -> Result<Vec<pc::HistoryTreeBranch>, PersistenceError> { Ok(vec![]) }
    async fn get_all_history_tree_branches(&self, _: i32, _: i32, _: Vec<u8>) -> Result<pc::HistoryTreePage, PersistenceError> { Ok(Default::default()) }
}

#[derive(Default)]
pub struct TestDomains(pub Arc<AtomicI32>);
impl DomainCache for TestDomains {
    fn get_domain_by_id(&self, id: &str) -> HResult<Arc<DomainCacheEntry>> {
        if id != DOMAIN_ID {
            return Err(HistoryError::internal(format!("domain {id} not found")));
        }
        Ok(Arc::new(DomainCacheEntry { info: DomainInfo { id: DOMAIN_ID.into(), name: DOMAIN_NAME.into(), status: self.0.load(Ordering::SeqCst) }, ..Default::default() }))
    }
    fn get_domain(&self, name: &str) -> HResult<Arc<DomainCacheEntry>> {
        if name != DOMAIN_NAME {
            return Err(HistoryError::internal(format!("domain {name} not found")));
        }
        self.get_domain_by_id(DOMAIN_ID)
    }
    fn get_domain_name(&self, id: &str) -> HResult<String> {
        self.get_domain_by_id(id).map(|d| d.info.name.clone())
    }
    fn unregister_domain_change_callback(&self, _: &str) {}
}

struct ActiveHere;
#[async_trait]
impl ActiveClusterManager for ActiveHere {
    async fn active_cluster_by_workflow(&self, _: &str, _: &str, _: &str) -> HResult<String> {
        Ok(CLUSTER.to_string())
    }
}

struct NoopShardPersistence;
impl ShardPersistence for NoopShardPersistence {
    fn update_shard(&self, _: &ShardInfoData, _: i64) -> Result<(), ShardContextError> { Ok(()) }
}
struct OneCluster;
impl ClusterInfo for OneCluster {
    fn current_cluster_name(&self) -> String { CLUSTER.into() }
    fn enabled_cluster_names(&self) -> Vec<String> { vec![CLUSTER.into()] }
}

pub struct TestClock(pub AtomicI64);
impl TimeSource for TestClock {
    fn now(&self) -> i64 { self.0.load(Ordering::SeqCst) }
}

pub struct Harness {
    pub engine: Arc<HistoryEngine>,
    pub execs: Arc<MemExecutions>,
    pub history: Arc<MemHistory>,
    pub clock: Arc<TestClock>,
    pub cache: Arc<EngineExecutionCache>,
    /// Domain status served by the test domain cache (0 registered, 1 deprecated).
    pub domain_status: Arc<AtomicI32>,
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

impl Harness {
    pub fn new() -> Self {
        let execs = Arc::new(MemExecutions::default());
        let history = Arc::new(MemHistory::default());
        let clock = Arc::new(TestClock(AtomicI64::new(1_700_000_000_000_000_000)));
        let c = clock.clone();
        let shard = ShardContext::acquire(
            ShardInfoData { shard_id: 0, range_id: 1, owner: "host".into(), ..Default::default() },
            "host",
            4,
            0,
            0,
            Arc::new(NoopShardPersistence),
            Arc::new(OneCluster),
            Arc::new(move || c.0.load(Ordering::SeqCst)),
            Arc::new(|_| {}),
        )
        .unwrap_or_else(|_| panic!("shard"));
        let config = Arc::new(Config::new_for_test());
        let domain_status = Arc::new(AtomicI32::new(0));
        let domains: Arc<dyn DomainCache> = Arc::new(TestDomains(domain_status.clone()));
        let core = Arc::new(EngineCore {
            engine_slot: Arc::new(std::sync::OnceLock::new()),
            semaphore_metadata: Arc::new(cadence_persistence::InMemoryExecutionStore::new()),
            strong_idempotency_sanity_check: Arc::new(|_| false),
            advanced_visibility_writing: Arc::new(|| false),
            active_cluster_manager: Arc::new(ActiveHere),
            remote_admin_clients: Arc::new(crate::wiring::resources::replication_clients::RemoteAdminClients::new(Default::default(), "test")),
            shard,
            executions: execs.clone(),
            domains: domains.clone(),
            clock: clock.clone(),
            sticky_ttl_nanos: Arc::new(|_| 10_000_000_000),
            current_cluster: CLUSTER.to_string(),
            history_v2: history.clone(),
            state_params: {
                let nodes: Arc<dyn cadence_persistence::history_reader::HistoryNodeStore> = Arc::new(MemNodeStore(history.clone()));
                Arc::new(move |_| StateParams { retention_days: 1, history_nodes: Some(nodes.clone()), ..StateParams::default() })
            },
        });
        let cache = EngineExecutionCache::new(core.clone());
        let mut engine = nop_engine();
        engine.current_cluster_name = CLUSTER.into();
        engine.cluster_metadata = Arc::new(TestClusterMetadata);
        engine.domain_cache = domains;
        engine.active_cluster_manager = Arc::new(ActiveHere);
        engine.time_source = clock.clone();
        engine.updater = Arc::new(EngineWorkflowUpdater::new(cache.clone(), core.clone()));
        engine.start_host = EngineStartHost::new(core.clone(), config.clone());
        engine.decision_handler = Arc::new(EngineDecisionHandler::new(cache.clone(), core.clone(), config));
        engine.history_event_notifier =
            Arc::new(crate::wiring::assembly::engine_factory::NotifierAdapter::new(crate::events::Notifier::new()));
        engine.execution_cache = cache.clone();
        let engine = Arc::new(engine);
        let _ = core.engine_slot.set(Arc::downgrade(&engine));
        Self { engine, execs, history, clock, cache, domain_status }
    }
}

struct TestClusterMetadata;
impl crate::engine::deps::ClusterMetadata for TestClusterMetadata {
    fn get_current_cluster_name(&self) -> String {
        CLUSTER.into()
    }
    fn cluster_name_for_failover_version(&self, _: i64) -> crate::engine::deps::HResult<String> {
        Ok(CLUSTER.into())
    }
}

/// Row view over `MemHistory` for the sync `read_history_branch` path (mutable-state event reads).
#[derive(Debug)]
struct MemNodeStore(Arc<MemHistory>);
impl std::fmt::Debug for MemHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemHistory")
    }
}
impl cadence_persistence::history_reader::HistoryNodeStore for MemNodeStore {
    fn select_nodes(
        &self,
        _shard_id: i32,
        tree_id: [u8; 16],
        branch_id: [u8; 16],
        min_node: i64,
        max_node: i64,
        limit: i64,
    ) -> Result<Vec<cadence_persistence::history_reader::HistoryNodeRow>, cadence_persistence::history_reader::HistoryReadError> {
        let tree = uuid::Uuid::from_bytes(tree_id).to_string();
        let branch = uuid::Uuid::from_bytes(branch_id).to_string();
        let g = self.0.nodes.lock().unwrap();
        let mut rows: Vec<_> = g
            .iter()
            .filter(|n| n.tree_id == tree && n.branch_id == branch && n.node_id >= min_node && n.node_id < max_node)
            .map(|n| cadence_persistence::history_reader::HistoryNodeRow {
                node_id: n.node_id,
                txn_id: n.transaction_id,
                data: n.data.data.clone(),
                encoding: n.data.encoding.clone(),
            })
            .collect();
        rows.sort_by_key(|r| (r.node_id, r.txn_id));
        rows.truncate(limit.max(0) as usize);
        Ok(rows)
    }
}
