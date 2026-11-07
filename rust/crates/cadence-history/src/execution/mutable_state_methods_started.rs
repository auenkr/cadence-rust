//! `mutable_state_builder_methods_started.go` on [`TypedMutableState`].
//!
//! Not wired: task generation (workflow start / record-started / delayed
//! decision tasks), events cache write, thrift blob of
//! `active_cluster_selection_policy` and `auto_reset_points`.

use cadence_api::service_types::HistoryEvent;

use super::mutable_state_builder::{TypedMutableState, EMPTY_EVENT_ID, EMPTY_VERSION};
use super::mutable_state_methods_common::{internal_err, Result, CLOSE_STATUS_NONE, WORKFLOW_STATE_CREATED};
use super::mutable_state_methods_decision::EMPTY_UUID;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

impl TypedMutableState {
    /// Go `ReplicateWorkflowExecutionStartedEvent`.
    pub fn replicate_workflow_execution_started_event(
        &mut self,
        parent_domain_id: Option<&str>,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        request_id: &str,
        event: &HistoryEvent,
    ) -> Result<()> {
        let Some(a) = event.workflow_execution_started_event_attributes.as_ref() else {
            return internal_err("WorkflowExecutionStarted event is missing attributes");
        };
        self.execution_info.create_request_id = request_id.to_string();
        self.execution_info.domain_id = domain_id.to_string();
        self.execution_info.workflow_id = workflow_id.to_string();
        self.execution_info.run_id = run_id.to_string();
        self.execution_info.first_execution_run_id = a.first_execution_run_id.clone().unwrap_or_default();
        self.execution_info.task_list = a.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.execution_info.workflow_type_name =
            a.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.execution_info.workflow_timeout_nanos =
            i64::from(a.execution_start_to_close_timeout_seconds.unwrap_or_default()) * NANOS_PER_SECOND;
        self.execution_info.decision_start_to_close_timeout_nanos =
            i64::from(a.task_start_to_close_timeout_seconds.unwrap_or_default()) * NANOS_PER_SECOND;
        self.execution_info.cron_schedule = a.cron_schedule.clone().unwrap_or_default();
        self.execution_info.attempt = a.attempt;
        if let Some(exp) = a.expiration_timestamp {
            self.execution_info.expiration_time_unix_nanos = exp;
        }
        if let Some(p) = parent_domain_id {
            self.execution_info.parent_domain_id = p.to_string();
        }
        if let Some(pe) = a.parent_workflow_execution.as_ref() {
            self.execution_info.parent_workflow_id = pe.workflow_id.clone();
            self.execution_info.parent_run_id = pe.run_id.clone();
        }
        if let Some(id) = a.parent_initiated_event_id {
            self.execution_info.initiated_id = id;
        }
        self.execution_info.start_time_unix_nanos = event.timestamp.unwrap_or_default();
        self.execution_info.last_updated_time_unix_nanos = event.timestamp.unwrap_or_default();

        if let Some(rp) = a.retry_policy.as_ref() {
            self.execution_info.has_retry_policy = true;
            self.execution_info.initial_interval_nanos =
                i64::from(rp.initial_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.backoff_coefficient = rp.backoff_coefficient;
            self.execution_info.maximum_interval_nanos =
                i64::from(rp.maximum_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.maximum_attempts = rp.maximum_attempts;
            self.execution_info.non_retriable_errors = rp.non_retriable_error_reasons.clone();
            self.execution_info.expiration_interval_nanos =
                i64::from(rp.expiration_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.jitter_coefficient = rp.jitter_coefficient;
        }
        if let Some(m) = a.memo.as_ref().and_then(|m| m.fields.as_ref()) {
            self.execution_info.memo = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        if let Some(sa) = a.search_attributes.as_ref() {
            self.execution_info.search_attributes =
                sa.indexed_fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        if let Some(pc) = a.partition_config.as_ref() {
            self.execution_info.partition_config = pc.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }

        // Go: UpdateWorkflowStateCloseStatus(Created, None)
        self.update_workflow_state_close_status(WORKFLOW_STATE_CREATED, CLOSE_STATUS_NONE)?;
        self.execution_info.last_processed_event = EMPTY_EVENT_ID;
        self.execution_info.decision_version = EMPTY_VERSION;
        self.execution_info.decision_schedule_id = EMPTY_EVENT_ID;
        self.execution_info.decision_started_id = EMPTY_EVENT_ID;
        self.execution_info.decision_request_id = EMPTY_UUID.to_string();
        self.execution_info.decision_timeout_nanos = 0;
        Ok(())
    }
}
