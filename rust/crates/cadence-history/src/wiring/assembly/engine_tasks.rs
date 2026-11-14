//! group: assembly/engine_tasks
//!
//! Task generation over the engine mutable state (Go
//! `mutableStateTaskGeneratorImpl` + `timerSequence`). Tasks are collected in the
//! state and get their ids when the close transaction is persisted.

use cadence_persistence::history_contract as pc;

use crate::execution::mutable_state_task_generator::next_decision_timeout;
use crate::wiring::assembly::engine_state::{Inner, StateParams};

const TRANSFER_DECISION_TASK: i32 = 0;
const TRANSFER_ACTIVITY_TASK: i32 = 1;
const TRANSFER_CLOSE_EXECUTION: i32 = 2;
const TRANSFER_CANCEL_EXECUTION: i32 = 3;
const TRANSFER_START_CHILD_EXECUTION: i32 = 4;
const TRANSFER_SIGNAL_EXECUTION: i32 = 5;

const TIMER_DECISION_TIMEOUT: i32 = 0;
const TIMER_ACTIVITY_TIMEOUT: i32 = 1;
const TIMER_DELETE_HISTORY_EVENT: i32 = 4;
const TIMER_ACTIVITY_RETRY: i32 = 5;

/// Timeout types (Go `TimeoutType`).
const TIMEOUT_START_TO_CLOSE: i32 = 0;
const TIMEOUT_SCHEDULE_TO_START: i32 = 1;
const TIMEOUT_SCHEDULE_TO_CLOSE: i32 = 2;
const TIMEOUT_HEARTBEAT: i32 = 3;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

impl Inner {
    pub(super) fn transfer_base(&self, task_type: i32, version: i64) -> pc::TransferTask {
        pc::TransferTask {
            domain_id: self.exec.domain_id.clone(),
            workflow_id: self.exec.workflow_id.clone(),
            run_id: self.exec.run_id.clone(),
            task_type,
            version,
            // Go persistence.TransferTaskTransferTargetRunID: dummy run id for
            // tasks without a target run; the blob requires a valid UUID.
            target_domain_id: self.exec.domain_id.clone(),
            target_run_id: "30000000-0000-f000-f000-000000000002".to_string(),
            ..Default::default()
        }
    }

    pub(super) fn timer_base(&self, task_type: i32, version: i64, visibility_nanos: i64) -> pc::TimerTask {
        pc::TimerTask {
            domain_id: self.exec.domain_id.clone(),
            workflow_id: self.exec.workflow_id.clone(),
            run_id: self.exec.run_id.clone(),
            task_type,
            version,
            visibility_timestamp_unix_nanos: visibility_nanos,
            ..Default::default()
        }
    }

    /// Go `GenerateDecisionScheduleTasks`.
    pub(super) fn gen_decision_schedule_tasks(&mut self, sticky_enabled: bool, params: &StateParams) {
        let tasklist = if sticky_enabled { self.exec.sticky_task_list.clone() } else { self.exec.task_list.clone() };
        let schedule_id = self.pc_exec.decision_schedule_id;
        let version = self.pc_exec.decision_version;
        let mut t = self.transfer_base(TRANSFER_DECISION_TASK, version);
        t.target_domain_id = self.exec.domain_id.clone();
        t.task_list = tasklist;
        t.schedule_id = schedule_id;
        t.original_task_list = self.exec.task_list.clone();
        self.transfer_tasks.push(t);

        let timeout_nanos = if sticky_enabled {
            self.pc_exec.sticky_schedule_to_start_timeout_nanos
        } else if self.pc_exec.decision_attempt < params.normal_decision_s2s_max_attempts {
            params.normal_decision_s2s_timeout_nanos
        } else {
            0
        };
        if timeout_nanos != 0 {
            let mut tt = self.timer_base(
                TIMER_DECISION_TIMEOUT,
                version,
                self.pc_exec.decision_scheduled_time_unix_nanos.saturating_add(timeout_nanos),
            );
            tt.timeout_type = TIMEOUT_SCHEDULE_TO_START;
            tt.event_id = schedule_id;
            tt.schedule_attempt = self.pc_exec.decision_attempt;
            tt.task_list = self.exec.task_list.clone();
            self.timer_tasks.push(tt);
        }
        self.dirty = true;
    }

    /// Go `GenerateDecisionStartTasks`.
    pub(super) fn gen_decision_start_tasks(&mut self, schedule_id: i64) {
        let attempt = self.pc_exec.decision_attempt;
        if attempt > 1 {
            let next = next_decision_timeout(attempt, self.pc_exec.decision_timeout_nanos, |upper| {
                use rand::Rng;
                rand::thread_rng().gen_range(0..upper.max(1))
            });
            self.pc_exec.decision_timeout_nanos = next;
        }
        let version = self.pc_exec.decision_version;
        let mut tt = self.timer_base(
            TIMER_DECISION_TIMEOUT,
            version,
            self.pc_exec.decision_started_time_unix_nanos.saturating_add(self.pc_exec.decision_timeout_nanos),
        );
        tt.timeout_type = TIMEOUT_START_TO_CLOSE;
        tt.event_id = schedule_id;
        tt.schedule_attempt = attempt;
        tt.task_list = self.exec.task_list.clone();
        self.timer_tasks.push(tt);
        self.dirty = true;
    }

    /// Go `GenerateActivityTransferTasks`.
    pub(super) fn gen_activity_transfer_task(&mut self, schedule_id: i64) {
        if let Some(a) = self.pc_acts.get(&schedule_id) {
            let mut t = self.transfer_base(TRANSFER_ACTIVITY_TASK, a.version);
            t.target_domain_id = self.exec.domain_id.clone();
            t.task_list = a.task_list.clone();
            t.schedule_id = schedule_id;
            self.transfer_tasks.push(t);
            self.dirty = true;
        }
    }

    /// Go `GenerateChildWorkflowTasks`.
    pub(super) fn gen_child_workflow_task(&mut self, initiated_id: i64, target_domain_id: &str) {
        if let Some(c) = self.pc_children.get(&initiated_id) {
            let mut t = self.transfer_base(TRANSFER_START_CHILD_EXECUTION, c.version);
            t.target_domain_id = target_domain_id.to_string();
            t.target_workflow_id = c.started_workflow_id.clone();
            t.schedule_id = initiated_id;
            t.task_list = self.exec.task_list.clone();
            self.transfer_tasks.push(t);
            self.dirty = true;
        }
    }

    /// Go `GenerateRequestCancelExternalTasks` / `GenerateSignalExternalTasks`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn gen_external_task(
        &mut self,
        cancel: bool,
        initiated_id: i64,
        version: i64,
        target_domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        child_only: bool,
    ) {
        let ty = if cancel { TRANSFER_CANCEL_EXECUTION } else { TRANSFER_SIGNAL_EXECUTION };
        let mut t = self.transfer_base(ty, version);
        t.target_domain_id = target_domain_id.to_string();
        t.target_workflow_id = workflow_id.to_string();
        // An empty target run id keeps the dummy default (the persisted blob requires a UUID).
        if !run_id.is_empty() {
            t.target_run_id = run_id.to_string();
        }
        t.target_child_workflow_only = child_only;
        t.schedule_id = initiated_id;
        t.task_list = self.exec.task_list.clone();
        self.transfer_tasks.push(t);
        self.dirty = true;
    }

    /// Go `GenerateActivityRetryTasks`.
    pub(super) fn gen_activity_retry_timer(&mut self, schedule_id: i64, visibility_nanos: i64, attempt: i64, version: i64) {
        let mut tt = self.timer_base(TIMER_ACTIVITY_RETRY, version, visibility_nanos);
        tt.event_id = schedule_id;
        tt.schedule_attempt = attempt;
        self.timer_tasks.push(tt);
        self.dirty = true;
    }

    /// Go `GenerateWorkflowCloseTasks`.
    pub(super) fn gen_workflow_close_tasks(&mut self, params: &StateParams, close_nanos: i64, version: i64) {
        let mut t = self.transfer_base(TRANSFER_CLOSE_EXECUTION, version);
        t.target_domain_id = self.exec.domain_id.clone();
        t.visibility_time_unix_nanos = close_nanos;
        t.record_visibility = false;
        self.transfer_tasks.push(t);

        let retention_nanos = params.retention_days as i64 * 24 * 3600 * NANOS_PER_SECOND;
        let mut delete_at = close_nanos.saturating_add(retention_nanos);
        if params.deletion_jitter_range > 1 {
            use rand::Rng;
            let jitter_secs = rand::thread_rng().gen_range(0..params.deletion_jitter_range * 60);
            delete_at = delete_at.saturating_add(jitter_secs * NANOS_PER_SECOND);
        }
        tracing::warn!(retention_days = params.retention_days, close_nanos, delete_at, version, "DIAG engine DeleteHistoryEvent");
        let tt = self.timer_base(TIMER_DELETE_HISTORY_EVENT, version, delete_at);
        self.timer_tasks.push(tt);
        self.dirty = true;
    }

    /// Go `timerSequence.CreateNextActivityTimer`: creates the timer task for the
    /// earliest pending activity timeout whose bit is not yet set.
    pub(super) fn create_next_activity_timer(&mut self) {
        // (timestamp, event id, timeout type, status bit)
        let mut candidates: Vec<(i64, i64, i32, i32)> = Vec::new();
        for (sid, a) in &self.pc_acts {
            let scheduled = a.scheduled_time_unix_nanos;
            let started = a.started_id != crate::engine::deps::EMPTY_EVENT_ID;
            if a.schedule_to_close_timeout_seconds > 0 {
                candidates.push((
                    scheduled + a.schedule_to_close_timeout_seconds as i64 * NANOS_PER_SECOND,
                    *sid,
                    TIMEOUT_SCHEDULE_TO_CLOSE,
                    4,
                ));
            }
            if !started && a.schedule_to_start_timeout_seconds > 0 {
                candidates.push((
                    scheduled + a.schedule_to_start_timeout_seconds as i64 * NANOS_PER_SECOND,
                    *sid,
                    TIMEOUT_SCHEDULE_TO_START,
                    2,
                ));
            }
            if started && a.start_to_close_timeout_seconds > 0 {
                candidates.push((
                    a.started_time_unix_nanos + a.start_to_close_timeout_seconds as i64 * NANOS_PER_SECOND,
                    *sid,
                    TIMEOUT_START_TO_CLOSE,
                    1,
                ));
            }
            if started && a.heartbeat_timeout_seconds > 0 {
                let last_hb = self.heartbeats.get(sid).map(|h| h.updated_time_unix_nanos).unwrap_or(0);
                let base = a.started_time_unix_nanos.max(last_hb);
                candidates.push((base + a.heartbeat_timeout_seconds as i64 * NANOS_PER_SECOND, *sid, TIMEOUT_HEARTBEAT, 8));
            }
        }
        candidates.sort_by_key(|c| (c.0, c.1, c.2));
        let Some(&(ts, sid, timeout_type, bit)) = candidates.first() else { return };
        let Some(a) = self.pc_acts.get_mut(&sid) else { return };
        if a.timer_task_status & bit != 0 {
            return;
        }
        a.timer_task_status |= bit;
        let (version, task_list, attempt) = (a.version, a.task_list.clone(), a.attempt as i64);
        let mut tt = self.timer_base(TIMER_ACTIVITY_TIMEOUT, version, ts);
        tt.timeout_type = timeout_type;
        tt.event_id = sid;
        tt.schedule_attempt = attempt;
        tt.task_list = task_list;
        self.timer_tasks.push(tt);
        self.refresh_activity(sid);
    }
}

const TRANSFER_RECORD_WORKFLOW_STARTED: i32 = 6;
const TIMER_WORKFLOW_TIMEOUT: i32 = 3;
const TIMER_WORKFLOW_BACKOFF: i32 = 6;

/// Go `WorkflowBackoffTimeoutTypeRetry` / `WorkflowBackoffTimeoutTypeCron`.
pub(super) const BACKOFF_TYPE_RETRY: i32 = 0;
pub(super) const BACKOFF_TYPE_CRON: i32 = 1;

impl Inner {
    /// Go `GenerateWorkflowStartTasks` (workflow timeout timer).
    pub(super) fn gen_workflow_start_timer(&mut self, timeout_at_nanos: i64, version: i64) {
        let mut tt = self.timer_base(TIMER_WORKFLOW_TIMEOUT, version, timeout_at_nanos);
        tt.task_list = self.exec.task_list.clone();
        self.timer_tasks.push(tt);
        self.dirty = true;
    }

    /// Go `GenerateRecordWorkflowStartedTasks`.
    pub(super) fn gen_record_workflow_started(&mut self, version: i64) {
        let mut t = self.transfer_base(TRANSFER_RECORD_WORKFLOW_STARTED, version);
        t.target_domain_id = self.exec.domain_id.clone();
        t.task_list = self.exec.task_list.clone();
        self.transfer_tasks.push(t);
        self.dirty = true;
    }

    /// Go `GenerateDelayedDecisionTasks` (workflow backoff timer).
    pub(super) fn gen_delayed_decision_timer(&mut self, at_nanos: i64, backoff_type: i32, version: i64) {
        let mut tt = self.timer_base(TIMER_WORKFLOW_BACKOFF, version, at_nanos);
        tt.timeout_type = backoff_type;
        tt.task_list = self.exec.task_list.clone();
        self.timer_tasks.push(tt);
        self.dirty = true;
    }
}
