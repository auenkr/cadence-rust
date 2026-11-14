//! group: assembly/engine_failover
//!
//! Go `mutableStateBuilder.startTransactionHandleDecisionFailover`
//! (`execution/mutable_state_builder.go`) for the engine mutable state.

use crate::engine::deps::{HResult, HistoryError, EMPTY_EVENT_ID};
use crate::wiring::assembly::engine_state::{EngineMutableState, EMPTY_VERSION};
use cadence_api::enums::DecisionTaskFailedCause;

use crate::execution::workflow::IDENTITY_HISTORY_SERVICE;
const PASSIVE_BUFFERED: &str = "mutableStateBuilder encounter previous passive workflow with buffered events";

impl EngineMutableState {
    /// Go `startTransactionHandleDecisionFailover`. Returns `true` when the in-flight
    /// decision was failed and a new one scheduled.
    pub fn start_transaction_handle_decision_failover(&self, incoming_task_version: i64) -> HResult<bool> {
        let (running, workflow_id) = {
            let g = self.inner.lock().unwrap();
            (g.exec.is_running(), g.pc_exec.workflow_id.clone())
        };
        if !running || !self.can_replicate_events_pub(&workflow_id) {
            return Ok(false);
        }
        let Some(policy) = &self.params.close_policy else { return Ok(false) };
        let cluster = &policy.cluster;

        let (decision_version, schedule_id, started_id, in_flight, current_version, last_write_version) = {
            let g = self.inner.lock().unwrap();
            (
                g.pc_exec.decision_version,
                g.pc_exec.decision_schedule_id,
                g.pc_exec.decision_started_id,
                g.pc_exec.decision_started_id != EMPTY_EVENT_ID,
                g.current_version,
                g.pc_exec.last_write_version,
            )
        };
        if !in_flight || decision_version >= current_version {
            return Ok(false);
        }
        if last_write_version != decision_version {
            return Err(HistoryError::internal(format!(
                "mutableStateBuilder encounter mismatch version, decision: {decision_version}, last write version {last_write_version}"
            )));
        }
        let last_write_source_cluster = cluster.cluster_name_for_failover_version(last_write_version)?;
        let current_version_cluster = cluster.cluster_name_for_failover_version(current_version)?;
        let current_cluster = cluster.get_current_cluster_name();

        // handle case 5
        let incoming_task_source_cluster = cluster.cluster_name_for_failover_version(incoming_task_version)?;
        if incoming_task_version != EMPTY_VERSION
            && current_version_cluster != current_cluster
            && incoming_task_source_cluster == current_cluster
        {
            return Ok(false);
        }
        let has_buffered = self.inner.lock().unwrap().has_buffered();
        // handle case 4
        if last_write_source_cluster != current_cluster && current_version_cluster != current_cluster {
            if has_buffered {
                return Err(HistoryError::internal(PASSIVE_BUFFERED));
            }
            return Ok(false);
        }
        // handle case 1 & 2
        let mut flush_buffer_version = last_write_version;
        // handle case 3
        if last_write_source_cluster != current_cluster && current_version_cluster == current_cluster {
            if has_buffered {
                return Err(HistoryError::internal(PASSIVE_BUFFERED));
            }
            flush_buffer_version = current_version;
        }
        self.update_current_version(flush_buffer_version, true)?;

        // Go `FailDecision(e, decision, FailoverCloseDecision)`.
        self.add_decision_task_failed(
            schedule_id,
            started_id,
            Some(DecisionTaskFailedCause::FailoverCloseDecision),
            &[],
            IDENTITY_HISTORY_SERVICE,
            "",
        )?;
        self.inner.lock().unwrap().flush_buffered();

        // Go `ScheduleDecision(e)`.
        let pending = self.inner.lock().unwrap().exec.decision_schedule_id != EMPTY_EVENT_ID;
        if !pending {
            self.add_decision_task_scheduled(false)
                .map_err(|_| HistoryError::internal("Failed to add decision scheduled event."))?;
        }
        Ok(true)
    }
}
