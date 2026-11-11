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

use crate::engine::deps::{HResult, HistoryError};
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
}

fn blob(data: Vec<u8>) -> pc::DataBlob {
    pc::DataBlob { encoding: "thriftrw".to_string(), data }
}

fn enc(s: &cadence_thrift::value::Struct) -> HResult<Vec<u8>> {
    Encoding::ThriftRW.encode(s).map_err(|e| HistoryError::internal(format!("encode mutable state blob: {e}")))
}

impl EngineMutableState {
    /// Go `CloseTransactionAsMutation`. For the passive/tasks-only policy
    /// (`active == false`) no buffered events are flushed and no activity timer
    /// is created.
    pub(super) fn close_transaction(&self, now_nanos: i64, active: bool) -> HResult<ClosedTxn> {
        let mut g = self.inner.lock().unwrap();
        if active {
            g.flush_buffered();
            g.create_next_user_timer();
            g.create_next_activity_timer();
        } else if !g.hist.is_empty() {
            return Err(HistoryError::internal("cannot close a tasks-only transaction with new history events"));
        }
        let events = g.hist.clone();
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
                Some(blob(enc(&crate::wiring::server::thrift_types_g2::version_histories_to_thrift(vh))?))
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

        let mutation = pc::WorkflowMutation {
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
            delete_request_cancel_infos: g.deleted_request_cancels.iter().copied().collect(),
            delete_signal_infos: g.deleted_signals.iter().copied().collect(),
            upsert_signal_requested_ids: g.upsert_signal_requested.iter().cloned().collect(),
            delete_signal_requested_ids: g.delete_signal_requested.iter().cloned().collect(),
            new_buffered_events,
            clear_buffered_events: g.clear_buffered,
            condition: g.db_next_event_id,
            ..Default::default()
        };
        Ok(ClosedTxn {
            events,
            branch_token: self.branch_token_locked(&g),
            mutation,
            transfer: g.transfer_tasks.clone(),
            timer: g.timer_tasks.clone(),
        })
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
        g.upsert_acts.clear();
        g.upsert_timers.clear();
        g.delete_timers.clear();
        g.deleted_activities.clear();
        g.upsert_children.clear();
        g.deleted_children.clear();
        g.upsert_cancels.clear();
        g.upsert_signals.clear();
        g.deleted_request_cancels.clear();
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
