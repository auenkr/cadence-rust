//! group: assembly/engine_close
//!
//! Close transaction of `EngineMutableState` (Go `mutableStateBuilder.
//! CloseTransactionAsMutation` and `CleanupTransaction`): flushes buffered
//! events, serialises the changed state into a `WorkflowMutation` and, after the
//! shard persisted it, resets the per-transaction bookkeeping.

use std::collections::BTreeMap;

use cadence_api::service_types::HistoryEvent;
use cadence_api::types::VersionHistoryItem;
use cadence_persistence::history_contract as pc;
use cadence_persistence::sqlblobs::{activity_info_to_struct, child_execution_info_to_struct, timer_info_to_struct, Encoding};

use crate::engine::deps::{HResult, HistoryError, WORKFLOW_STATE_COMPLETED};
use crate::wiring::assembly::engine_state::EngineMutableState;

/// Result of closing a transaction: the events to append to the history branch,
/// the mutation (without task ids / history size) and the generated tasks.
#[derive(Clone)]
pub(super) struct ClosedTxn {
    pub events: Vec<HistoryEvent>,
    pub branch_token: Vec<u8>,
    pub mutation: pc::WorkflowMutation,
    pub transfer: Vec<pc::TransferTask>,
    pub timer: Vec<pc::TimerTask>,
    /// Go `insertReplicationTasks` (category 3), ids not yet allocated.
    pub replication: Vec<pc::ReplicationTask>,
}

/// `persistence.ReplicationTaskTypeHistory` / `ReplicationTaskTypeSyncActivity`.
pub(super) const REPLICATION_TASK_TYPE_HISTORY: i32 = 0;
pub(super) const REPLICATION_TASK_TYPE_SYNC_ACTIVITY: i32 = 1;

/// Go `mergeContinueAsNewReplicationTasks`: the new run's first-batch replication task is folded
/// into the closing run's history replication task as `NewRunBranchToken`.
pub(super) fn merge_continue_as_new_replication(
    current: &mut [pc::ReplicationTask],
    close_status: i32,
    new_snapshot: &mut Option<pc::WorkflowSnapshot>,
) -> HResult<()> {
    if close_status != crate::engine::deps::WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW || current.is_empty() {
        return Ok(());
    }
    let cat = pc::HistoryTaskCategory { category_type: 1, category_id: 3 };
    let new_token = match new_snapshot.as_mut().and_then(|s| {
        let list = s.tasks_by_category.get(&cat)?;
        (list.len() == 1).then(|| list[0].clone())
    }) {
        Some(task) => {
            let st = Encoding::ThriftRW
                .decode(&task.data.data)
                .map_err(|e| HistoryError::internal(e.to_string()))?;
            let rt = cadence_persistence::sqlblobs::replication_task_from_struct(&st)
                .map_err(|e| HistoryError::internal(e.to_string()))?;
            rt.branch_token
        }
        None => {
            return Err(HistoryError::internal(
                "unable to find replication task from new workflow for continue as new replication",
            ))
        }
    };
    if let Some(s) = new_snapshot.as_mut() {
        s.tasks_by_category.remove(&cat);
    }
    let mut updated = false;
    for t in current.iter_mut().filter(|t| t.task_type == REPLICATION_TASK_TYPE_HISTORY) {
        t.new_run_branch_token = new_token.clone();
        updated = true;
    }
    if !updated {
        return Err(HistoryError::internal(
            "unable to find replication task from current workflow for continue as new replication",
        ));
    }
    Ok(())
}

fn blob(data: Vec<u8>) -> pc::DataBlob {
    pc::DataBlob { encoding: "thriftrw".to_string(), data }
}

/// Go `validateNoEventsAfterWorkflowFinish`: on an active close of a finished workflow the last
/// event must be a workflow-finish event.
fn validate_no_events_after_workflow_finish(active: bool, state: i32, events: &[HistoryEvent]) -> HResult<()> {
    use cadence_api::enums::EventType as E;
    if !active || events.is_empty() || state != WORKFLOW_STATE_COMPLETED {
        return Ok(());
    }
    match events[events.len() - 1].event_type {
        Some(
            E::WorkflowExecutionCompleted
            | E::WorkflowExecutionFailed
            | E::WorkflowExecutionTimedOut
            | E::WorkflowExecutionTerminated
            | E::WorkflowExecutionContinuedAsNew
            | E::WorkflowExecutionCanceled,
        ) => Ok(()),
        _ => Err(HistoryError::internal("error validating last event being workflow finish event")),
    }
}

/// Go `checksum.Checksum` as serialised by `SerializeChecksum(..., EncodingTypeJSON)`:
/// `{"Version":1,"Flavor":1,"Value":"<base64>"}`.
fn checksum_json(sum: &crate::execution::checksum::Checksum) -> Vec<u8> {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut b64 = String::new();
    for c in sum.value.chunks(3) {
        let n = (u32::from(c[0]) << 16) | (u32::from(*c.get(1).unwrap_or(&0)) << 8) | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                b64.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                b64.push('=');
            }
        }
    }
    format!("{{\"Version\":{},\"Flavor\":{},\"Value\":\"{}\"}}", sum.version, sum.flavor, b64).into_bytes()
}

fn enc(s: &cadence_thrift::value::Struct) -> HResult<Vec<u8>> {
    Encoding::ThriftRW.encode(s).map_err(|e| HistoryError::internal(format!("encode mutable state blob: {e}")))
}

impl EngineMutableState {
    /// Go `CloseTransactionAsMutation`. For the passive/tasks-only policy
    /// (`active == false`) no buffered events are flushed and no activity timer
    /// is created.
    pub(super) fn close_transaction(&self, now_nanos: i64, active: bool) -> HResult<ClosedTxn> {
        if active {
            self.close_transaction_with_policy_check()?;
            // Go `closeTransactionHandleBufferedEventsLimit`: too many persisted buffered events
            // force-close the in-flight decision and schedule a new one.
            let (running, over_limit, in_flight) = {
                let g = self.inner.lock().unwrap();
                (
                    g.exec.state != WORKFLOW_STATE_COMPLETED,
                    g.buffered.len() as i64 >= self.params.max_buffered_events_batch,
                    g.in_flight(),
                )
            };
            if running && over_limit && in_flight {
                self.force_close_in_flight_decision()?;
                let pending = self.inner.lock().unwrap().pc_exec.decision_schedule_id != crate::engine::deps::EMPTY_EVENT_ID;
                if !pending {
                    self.add_decision_task_scheduled(false)?;
                }
            }
        }
        let mut g = self.inner.lock().unwrap();
        if active {
            g.flush_buffered();
            // Go `closeTransactionHandleWorkflowReset`: schedule an auto-reset
            // task when a resettable point carries a bad binary checksum.
            if g.exec.state != WORKFLOW_STATE_COMPLETED && g.pc_children.is_empty() {
                let hit = g.exec.auto_reset_points.as_ref().is_some_and(|p| {
                    p.points.iter().any(|pt| {
                        pt.resettable
                            && !pt.expiring_time_nano.is_some_and(|e| e > 0 && now_nanos > e)
                            && self.is_bad_binary(&pt.binary_checksum)
                    })
                });
                if hit {
                    let version = g.current_version;
                    let mut t = g.transfer_base(7, version);
                    t.task_list = g.exec.task_list.clone();
                    g.transfer_tasks.push(t);
                }
            }
            g.create_next_user_timer();
            g.create_next_activity_timer();
        } else if !g.hist.is_empty() {
            return Err(HistoryError::internal("cannot close a tasks-only transaction with new history events"));
        }
        let events = g.hist.clone();
        validate_no_events_after_workflow_finish(active, g.exec.state, &events).inspect_err(|_| {
            tracing::error!(
                domain_id = %g.pc_exec.domain_id, workflow_id = %g.pc_exec.workflow_id, run_id = %g.pc_exec.run_id,
                "encounter case where events appears after workflow finish."
            );
        })?;
        let mut last_write_version = self.snapshot.last_write_version;
        if let (Some(first), Some(last)) = (events.first(), events.last()) {
            g.last_first_event_id = first.event_id;
            last_write_version = last.version;
            let (id, version) = (last.event_id, last.version);
            if let Some(vh) = g.version_histories.as_mut() {
                if let Some(h) = vh.histories.get_mut(vh.current_version_history_index as usize) {
                    match h.items.last_mut() {
                        Some(item) if item.version == version => item.event_id = id,
                        _ => h.items.push(VersionHistoryItem { event_id: id, version }),
                    }
                }
            }
        }
        g.last_updated_nanos = now_nanos;

        // exec (engine view) -> pc_exec (persistence view)
        let (state, close_status, signal_count) = (g.exec.state, g.exec.close_status, g.exec.signal_count);
        let sticky = g.exec.sticky_task_list.clone();
        let sticky_secs = g.exec.sticky_schedule_to_start_timeout;
        let (clv, cfv, cimpl) = (
            g.exec.client_library_version.clone(),
            g.exec.client_feature_version.clone(),
            g.exec.client_impl.clone(),
        );
        let (next, lfe, lpe, hsize) = (g.next_event_id, g.last_first_event_id, g.last_processed_event, g.history_size);
        g.pc_exec.state = state;
        g.pc_exec.close_status = close_status;
        g.pc_exec.signal_count = signal_count;
        g.pc_exec.sticky_task_list = sticky;
        if (g.pc_exec.sticky_schedule_to_start_timeout_nanos / 1_000_000_000) as i32 != sticky_secs {
            g.pc_exec.sticky_schedule_to_start_timeout_nanos = sticky_secs as i64 * 1_000_000_000;
        }
        g.pc_exec.client_library_version = clv;
        g.pc_exec.client_feature_version = cfv;
        g.pc_exec.client_impl = cimpl;
        g.pc_exec.next_event_id = next;
        g.pc_exec.last_first_event_id = lfe;
        g.pc_exec.last_processed_event = lpe;
        g.pc_exec.last_updated_time_unix_nanos = now_nanos;
        g.pc_exec.history_size = hsize;
        g.pc_exec.last_write_version = last_write_version;
        g.mirror_decision();

        let version_histories = match &g.version_histories {
            Some(vh) => {
                Some(blob(Encoding::ThriftRW.encode_versioned(&crate::wiring::server::thrift_types_g2::version_histories_to_thrift(vh)).map_err(|e| HistoryError::internal(format!("encode version histories: {e}")))?))
            }
            None => self.snapshot.version_histories.clone(),
        };

        let mut upsert_activity_infos = Vec::new();
        let mut heartbeats = BTreeMap::new();
        for sid in &g.upsert_acts {
            if let Some(a) = g.pc_acts.get(sid) {
                upsert_activity_infos.push(pc::KeyedBlob { key: *sid, value: blob(enc(&activity_info_to_struct(a))?) });
            }
            if let Some(h) = g.heartbeats.get(sid) {
                heartbeats.insert(*sid, h.clone());
            }
        }
        let mut delete_activity_infos: Vec<i64> = g.deleted_activities.iter().copied().collect();
        delete_activity_infos.sort_unstable();
        let mut upsert_child_execution_infos = Vec::new();
        for iid in &g.upsert_children {
            if let Some(c) = g.pc_children.get(iid) {
                upsert_child_execution_infos
                    .push(pc::KeyedBlob { key: *iid, value: blob(enc(&child_execution_info_to_struct(c))?) });
            }
        }
        let mut upsert_request_cancel_infos = Vec::new();
        for iid in &g.upsert_cancels {
            if let Some(c) = g.pc_cancels.get(iid) {
                upsert_request_cancel_infos.push(pc::KeyedBlob {
                    key: *iid,
                    value: blob(enc(&cadence_persistence::sqlblobs::request_cancel_info_to_struct(c))?),
                });
            }
        }
        let mut upsert_signal_infos = Vec::new();
        for iid in &g.upsert_signals {
            if let Some(s) = g.pc_signals.get(iid) {
                upsert_signal_infos.push(pc::KeyedBlob {
                    key: *iid,
                    value: blob(enc(&cadence_persistence::sqlblobs::signal_info_to_struct(s))?),
                });
            }
        }
        let mut upsert_timer_infos = Vec::new();
        for id in &g.upsert_timers {
            if let Some(t) = g.timers.get(id) {
                upsert_timer_infos.push(pc::KeyedBlob { key: id.clone(), value: blob(enc(&timer_info_to_struct(t))?) });
            }
        }
        let new_buffered_events = if g.update_buffered.is_empty() {
            None
        } else {
            let b = cadence_thrift::history_blob::encode(g.update_buffered.clone());
            Some(blob(b.data))
        };

        // Go `generateChecksum`: probabilistic, computed from the closed state.
        let checksum = if self.params.checksum_gen_probability > 0
            && rand::Rng::gen_range(&mut rand::thread_rng(), 0i64..100i64) < self.params.checksum_gen_probability
        {
            let payload = g.checksum_payload();
            Some(pc::DataBlob { encoding: "json".to_string(), data: checksum_json(&payload.generate()) })
        } else {
            None
        };

        let mutation = pc::WorkflowMutation {
            checksum,
            activity_heartbeats: heartbeats,
            execution_info: g.pc_exec.clone(),
            version_histories,
            start_version: self.snapshot.start_version,
            last_write_version,
            upsert_activity_infos,
            delete_activity_infos,
            upsert_timer_infos,
            delete_timer_infos: g.delete_timers.iter().cloned().collect(),
            upsert_child_execution_infos,
            delete_child_execution_infos: g.deleted_children.iter().copied().collect(),
            upsert_request_cancel_infos,
            upsert_signal_infos,
            upsert_semaphore_infos: g.semaphores.updates.values().map(super::engine_semaphore::to_record).collect(),
            delete_semaphore_infos: g.semaphores.deletes.iter().copied().collect(),
            delete_request_cancel_infos: g.deleted_request_cancels.iter().copied().collect(),
            delete_signal_infos: g.deleted_signals.iter().copied().collect(),
            upsert_signal_requested_ids: g.upsert_signal_requested.iter().cloned().collect(),
            delete_signal_requested_ids: g.delete_signal_requested.iter().cloned().collect(),
            new_buffered_events,
            clear_buffered_events: g.clear_buffered,
            condition: g.db_next_event_id,
            ..Default::default()
        };
        let branch_token = self.branch_token_locked(&g);
        let replication = if active { self.generate_replication_tasks(&g, &events, &branch_token)? } else { Vec::new() };
        Ok(ClosedTxn {
            events,
            branch_token,
            mutation,
            transfer: g.transfer_tasks.clone(),
            timer: g.timer_tasks.clone(),
            replication,
        })
    }

    /// Go `canReplicateEvents`: multi-cluster (global, >1 cluster) domain and
    /// `EnableReplicationTaskGeneration(domainID, workflowID)`.
    fn can_replicate_events(&self, workflow_id: &str) -> bool {
        let Some(policy) = &self.params.close_policy else { return false };
        self.domain.is_global_domain
            && self.domain.clusters.len() > 1
            && (policy.replication_enabled)(&self.domain.info.id, workflow_id)
    }

    /// Go `prepareEventsAndReplicationTasks` (`eventsToReplicationTask` + `syncActivityToReplicationTask`),
    /// active policy only.
    fn generate_replication_tasks(
        &self,
        g: &crate::wiring::assembly::engine_state::Inner,
        events: &[HistoryEvent],
        branch_token: &[u8],
    ) -> HResult<Vec<pc::ReplicationTask>> {
        if !self.can_replicate_events(&g.pc_exec.workflow_id) {
            return Ok(Vec::new());
        }
        let policy = self.params.close_policy.as_ref().expect("checked by can_replicate_events");
        let mut out = Vec::new();
        if let (Some(first), Some(last)) = (events.first(), events.last()) {
            let source = policy.cluster.cluster_name_for_failover_version(first.version)?;
            if policy.cluster.get_current_cluster_name() != source {
                return Err(HistoryError::internal("mutableStateBuilder encounter contradicting version & transaction policy"));
            }
            out.push(pc::ReplicationTask {
                domain_id: g.pc_exec.domain_id.clone(),
                workflow_id: g.pc_exec.workflow_id.clone(),
                run_id: g.pc_exec.run_id.clone(),
                task_type: REPLICATION_TASK_TYPE_HISTORY,
                version: first.version,
                first_event_id: first.event_id,
                next_event_id: last.event_id + 1,
                branch_token: branch_token.to_vec(),
                ..Default::default()
            });
        }
        for sid in &g.sync_activity_tasks {
            if let Some(a) = g.pc_acts.get(sid) {
                out.push(pc::ReplicationTask {
                    domain_id: g.pc_exec.domain_id.clone(),
                    workflow_id: g.pc_exec.workflow_id.clone(),
                    run_id: g.pc_exec.run_id.clone(),
                    task_type: REPLICATION_TASK_TYPE_SYNC_ACTIVITY,
                    version: a.version,
                    scheduled_id: *sid,
                    ..Default::default()
                });
            }
        }
        Ok(out)
    }

    /// Go `closeTransactionWithPolicyCheck`: an active close of a multi-cluster domain whose current
    /// version belongs to another cluster fails with DomainNotActive.
    fn close_transaction_with_policy_check(&self) -> HResult<()> {
        let Some(policy) = &self.params.close_policy else { return Ok(()) };
        // Go `canReplicateEvents` (ReplicationPolicyMultiCluster).
        if !(self.domain.is_global_domain && self.domain.clusters.len() > 1) {
            return Ok(());
        }
        let (workflow_id, version) = {
            let g = self.inner.lock().unwrap();
            (g.pc_exec.workflow_id.clone(), g.current_version)
        };
        if !(policy.replication_enabled)(&self.domain.info.id, &workflow_id) {
            return Ok(());
        }
        let active = policy.cluster.cluster_name_for_failover_version(version)?;
        let current = policy.cluster.get_current_cluster_name();
        if active != current {
            return Err(self.domain.new_domain_not_active_error(&current, &active));
        }
        Ok(())
    }

    pub(super) fn history_size(&self) -> i64 {
        self.inner.lock().unwrap().history_size
    }

    fn branch_token_locked(&self, g: &crate::wiring::assembly::engine_state::Inner) -> Vec<u8> {
        match &g.version_histories {
            Some(vh) => vh
                .histories
                .get(vh.current_version_history_index as usize)
                .map(|h| h.branch_token.clone())
                .unwrap_or_else(|| g.branch_token.clone()),
            None => g.branch_token.clone(),
        }
    }

    /// Go `CleanupTransaction` after the mutation was persisted.
    pub(super) fn finish_transaction(&self, history_size: i64, last_event_task_id: Option<i64>) {
        let mut g = self.inner.lock().unwrap();
        g.history_size = history_size;
        g.pc_exec.history_size = history_size;
        if let Some(id) = last_event_task_id {
            g.pc_exec.last_event_task_id = id;
        }
        g.hist.clear();
        g.transfer_tasks.clear();
        g.timer_tasks.clear();
        g.sync_activity_tasks.clear();
        g.upsert_acts.clear();
        g.upsert_timers.clear();
        g.delete_timers.clear();
        g.deleted_activities.clear();
        g.upsert_children.clear();
        g.deleted_children.clear();
        g.upsert_cancels.clear();
        g.upsert_signals.clear();
        g.deleted_request_cancels.clear();
        g.semaphores.updates.clear();
        g.semaphores.deletes.clear();
        g.deleted_signals.clear();
        g.upsert_signal_requested.clear();
        g.delete_signal_requested.clear();
        g.clear_buffered = false;
        let moved = std::mem::take(&mut g.update_buffered);
        g.buffered.extend(moved);
        g.has_buffered_in_db = !g.buffered.is_empty();
        g.db_next_event_id = g.next_event_id;
        g.dirty = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::deps::{DomainCacheEntry, DomainInfo};
    use crate::wiring::assembly::engine_state::StateParams;
    use cadence_api::enums::EventType;
    use std::sync::Arc;

    /// Running workflow with decision 3 scheduled / 4 started (in flight) and `buffered` buffered events.
    fn state(in_flight: bool, buffered: usize, limit: i64) -> EngineMutableState {
        let domain = DomainCacheEntry {
            info: DomainInfo { id: "domain-id".into(), name: "test-domain".into(), status: 0 },
            ..Default::default()
        };
        let mut snapshot = pc::WorkflowSnapshot::default();
        snapshot.execution_info.domain_id = "domain-id".into();
        snapshot.execution_info.workflow_id = "wf".into();
        snapshot.execution_info.run_id = "run".into();
        snapshot.execution_info.task_list = "wf-tl".into();
        snapshot.execution_info.next_event_id = 5;
        if in_flight {
            snapshot.execution_info.decision_schedule_id = 3;
            snapshot.execution_info.decision_started_id = 4;
        } else {
            snapshot.execution_info.decision_schedule_id = crate::engine::deps::EMPTY_EVENT_ID;
            snapshot.execution_info.decision_started_id = crate::engine::deps::EMPTY_EVENT_ID;
        }
        let params = StateParams { max_buffered_events_batch: limit, ..StateParams::default() };
        let s = EngineMutableState::from_snapshot(snapshot, domain, 0, Arc::new(|| 1_700_000_000_000_000_000))
            .unwrap()
            .with_params(params);
        {
            let mut g = s.inner.lock().unwrap();
            g.mirror_decision();
            for _ in 0..buffered {
                g.buffered.push(HistoryEvent { event_type: Some(EventType::WorkflowExecutionSignaled), ..Default::default() });
            }
        }
        s
    }

    fn types(c: &ClosedTxn) -> Vec<EventType> {
        c.events.iter().filter_map(|e| e.event_type).collect()
    }

    #[test]
    fn events_after_workflow_finish_rejected_only_for_active_completed() {
        let ev = |t| vec![HistoryEvent { event_type: Some(t), ..Default::default() }];
        let done = WORKFLOW_STATE_COMPLETED;
        assert!(validate_no_events_after_workflow_finish(true, done, &ev(EventType::WorkflowExecutionCompleted)).is_ok());
        assert!(validate_no_events_after_workflow_finish(true, done, &ev(EventType::WorkflowExecutionSignaled)).is_err());
        assert!(validate_no_events_after_workflow_finish(false, done, &ev(EventType::WorkflowExecutionSignaled)).is_ok());
        assert!(validate_no_events_after_workflow_finish(true, 1, &ev(EventType::WorkflowExecutionSignaled)).is_ok());
        assert!(validate_no_events_after_workflow_finish(true, done, &[]).is_ok());
    }

    #[test]
    fn checksum_json_matches_go_shape() {
        let sum = crate::execution::checksum::Checksum { version: 1, flavor: 1, value: vec![0xde, 0xad, 0xbe, 0xef] };
        assert_eq!(String::from_utf8(checksum_json(&sum)).unwrap(), r#"{"Version":1,"Flavor":1,"Value":"3q2+7w=="}"#);
    }

    struct FakeCluster;
    impl crate::engine::deps::ClusterMetadata for FakeCluster {
        fn get_current_cluster_name(&self) -> String {
            "cluster-a".into()
        }
        fn cluster_name_for_failover_version(&self, _: i64) -> HResult<String> {
            Ok("cluster-b".into())
        }
    }

    #[test]
    fn active_close_in_standby_version_is_domain_not_active() {
        let mut s = state(false, 0, 3);
        s.domain.is_global_domain = true;
        s.domain.clusters = vec!["cluster-a".into(), "cluster-b".into()];
        s.params.close_policy = Some(crate::wiring::assembly::engine_state::ClosePolicy {
            cluster: Arc::new(FakeCluster),
            replication_enabled: Arc::new(|_, _| true),
        });
        assert!(s.close_transaction(1_700_000_000_000_000_000, true).is_err());
        // passive close and single-cluster domains skip the check
        assert!(s.close_transaction(1_700_000_000_000_000_000, false).is_ok());
        s.domain.clusters = vec!["cluster-a".into()];
        assert!(s.close_transaction(1_700_000_000_000_000_000, true).is_ok());
    }

    struct SameCluster;
    impl crate::engine::deps::ClusterMetadata for SameCluster {
        fn get_current_cluster_name(&self) -> String {
            "cluster-a".into()
        }
        fn cluster_name_for_failover_version(&self, _: i64) -> HResult<String> {
            Ok("cluster-a".into())
        }
    }

    fn global_state(enabled: bool, global: bool) -> EngineMutableState {
        let mut s = state(false, 0, 3);
        s.domain.is_global_domain = global;
        s.domain.clusters = vec!["cluster-a".into(), "cluster-b".into()];
        s.params.close_policy = Some(crate::wiring::assembly::engine_state::ClosePolicy {
            cluster: Arc::new(SameCluster),
            replication_enabled: Arc::new(move |_, _| enabled),
        });
        s
    }

    #[test]
    fn active_close_of_global_domain_emits_history_and_sync_activity_tasks() {
        let s = global_state(true, true);
        s.add_decision_task_scheduled(false).unwrap();
        {
            let mut g = s.inner.lock().unwrap();
            let a = pc::ActivityInfo { version: 7, ..Default::default() };
            g.pc_acts.insert(9, a);
            g.sync_activity_tasks.insert(9);
            g.sync_activity_tasks.insert(10); // not pending: skipped like Go
        }
        let c = s.close_transaction(1_700_000_000_000_000_000, true).unwrap();
        assert_eq!(c.replication.len(), 2);
        let h = &c.replication[0];
        assert_eq!(h.task_type, REPLICATION_TASK_TYPE_HISTORY);
        assert_eq!(h.first_event_id, c.events.first().unwrap().event_id);
        assert_eq!(h.next_event_id, c.events.last().unwrap().event_id + 1);
        let sa = &c.replication[1];
        assert_eq!((sa.task_type, sa.scheduled_id, sa.version), (REPLICATION_TASK_TYPE_SYNC_ACTIVITY, 9, 7));
    }

    #[test]
    fn no_replication_tasks_for_local_domain_disabled_config_or_passive_close() {
        for (enabled, global) in [(true, false), (false, true)] {
            let s = global_state(enabled, global);
            s.add_decision_task_scheduled(false).unwrap();
            assert!(s.close_transaction(1_700_000_000_000_000_000, true).unwrap().replication.is_empty());
        }
        let s = global_state(true, true);
        s.inner.lock().unwrap().sync_activity_tasks.insert(1);
        assert!(s.close_transaction(1_700_000_000_000_000_000, false).unwrap().replication.is_empty());
    }

    #[test]
    fn continue_as_new_merges_new_run_branch_token() {
        let cat = pc::HistoryTaskCategory { category_type: 1, category_id: 3 };
        let new_run = pc::ReplicationTask {
            domain_id: "00000000-0000-0000-0000-000000000001".into(),
            run_id: "00000000-0000-0000-0000-000000000002".into(),
            branch_token: vec![9, 9],
            ..Default::default()
        };
        let st = cadence_persistence::sqlblobs::replication_task_to_struct(&new_run).unwrap();
        let mut snap = pc::WorkflowSnapshot::default();
        snap.tasks_by_category.insert(
            cat.clone(),
            vec![pc::Task { data: blob(Encoding::ThriftRW.encode(&st).unwrap()), ..Default::default() }],
        );
        let mut new_snapshot = Some(snap);
        let mut cur = vec![pc::ReplicationTask { task_type: REPLICATION_TASK_TYPE_HISTORY, ..Default::default() }];
        merge_continue_as_new_replication(&mut cur, crate::engine::deps::WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW, &mut new_snapshot)
            .unwrap();
        assert_eq!(cur[0].new_run_branch_token, vec![9, 9]);
        assert!(!new_snapshot.unwrap().tasks_by_category.contains_key(&cat));
        // no new-run task: error; other close status: untouched
        let mut none = None;
        assert!(merge_continue_as_new_replication(&mut cur, crate::engine::deps::WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW, &mut none).is_err());
        assert!(merge_continue_as_new_replication(&mut cur, 1, &mut none).is_ok());
    }

    #[test]
    fn checksum_generated_when_probability_is_100() {
        let mut s = state(false, 0, 3);
        s.params.checksum_gen_probability = 100;
        let c = s.close_transaction(1_700_000_000_000_000_000, true).unwrap();
        assert_eq!(c.mutation.checksum.unwrap().encoding, "json");
    }

    #[test]
    fn buffered_limit_reached_force_closes_decision_and_reschedules() {
        let s = state(true, 3, 3);
        let c = s.close_transaction(1_700_000_000_000_000_000, true).unwrap();
        let t = types(&c);
        assert_eq!(t.first(), Some(&EventType::DecisionTaskFailed));
        assert!(t.contains(&EventType::DecisionTaskScheduled));
        assert_eq!(t.iter().filter(|e| **e == EventType::WorkflowExecutionSignaled).count(), 3);
    }

    #[test]
    fn buffered_below_limit_leaves_decision_alone() {
        let s = state(true, 2, 3);
        let c = s.close_transaction(1_700_000_000_000_000_000, true).unwrap();
        assert!(!types(&c).contains(&EventType::DecisionTaskFailed));
    }

    #[test]
    fn buffered_limit_without_in_flight_decision_is_noop() {
        let s = state(false, 3, 3);
        let c = s.close_transaction(1_700_000_000_000_000_000, true).unwrap();
        assert!(!types(&c).contains(&EventType::DecisionTaskFailed));
    }
}
