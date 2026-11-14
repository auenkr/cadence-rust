//! `mutable_state_builder_methods_started.go` on [`TypedMutableState`].
//!
//! Differences from Go (the typed state owns no clock, domain entry or task
//! generator): `timeSource.Now()` and domain entry fields come from
//! [`StartedEnv`]; `writeEventToCache` is NOT wired. `SetHistoryTree` and the
//! thriftrw blob of `active_cluster_selection_policy` are wired. Decoded auto
//! reset points are kept in `StartedEnv::auto_reset_points`.

use cadence_api::enums::{ContinueAsNewInitiator, EventType, TaskListKind};
use cadence_api::service_types::{HistoryEvent, WorkflowExecutionStartedEventAttributes};
use cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes;
use cadence_api::types::{ResetPoints, StartWorkflowExecutionRequest, TaskList, WorkflowExecution, WorkflowType};
use cadence_persistence::history_contract::WorkflowRequest;

use super::mutable_state_builder::{TypedMutableState, EMPTY_EVENT_ID, EMPTY_VERSION, FIRST_EVENT_ID};
use super::mutable_state_methods_common::{internal_err, Result, CLOSE_STATUS_NONE, WORKFLOW_STATE_CREATED};
use super::mutable_state_methods_decision::{DecisionEnv, EMPTY_UUID};
use crate::engine::types::{HistoryStartWorkflowExecutionRequest, ParentExecutionInfo};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 3600 * NANOS_PER_SECOND;

/// Inputs Go reads from `timeSource` and `domainEntry`.
#[derive(Debug, Clone, Default)]
pub struct StartedEnv {
    /// Go `timeSource.Now().UnixNano()`.
    pub now_nanos: i64,
    /// Go `domainEntry.GetInfo().ID`.
    pub domain_id: String,
    /// Go `domainEntry.GetInfo().Name`.
    pub domain_name: String,
    /// Go `domainEntry.GetRetentionDays(workflowID)`.
    pub retention_days: i32,
    /// Decoded `executionInfo.AutoResetPoints` (output of replicate).
    pub auto_reset_points: Option<ResetPoints>,
}

/// Go `rolloverAutoResetPointsWithExpiringTime`.
pub fn rollover_auto_reset_points_with_expiring_time(
    reset_points: Option<&ResetPoints>,
    prev_run_id: &str,
    now_nano: i64,
    domain_retention_days: i32,
) -> Option<ResetPoints> {
    let rp = reset_points?;
    let expiring = now_nano + i64::from(domain_retention_days) * NANOS_PER_DAY;
    let mut points = rp.points.clone();
    for p in points.iter_mut() {
        if p.run_id == prev_run_id {
            p.expiring_time_nano = Some(expiring);
        }
    }
    Some(ResetPoints { points })
}

/// Go `HistoryBuilder.AddWorkflowExecutionStartedEvent` attribute assembly.
pub fn build_started_event_attributes(
    start_request: &HistoryStartWorkflowExecutionRequest,
    prev_run_id: &str,
    prev_reset_points: Option<&ResetPoints>,
    has_previous: bool,
    first_run_id: &str,
    original_run_id: &str,
    first_scheduled_time_nanos: i64,
) -> WorkflowExecutionStartedEventAttributes {
    let default_req = StartWorkflowExecutionRequest::default();
    let request = start_request.start_request.as_ref().unwrap_or(&default_req);
    let (prev_run, reset_points) = if has_previous {
        (prev_run_id.to_string(), prev_reset_points.cloned())
    } else {
        (String::new(), None)
    };
    let mut a = WorkflowExecutionStartedEventAttributes {
        workflow_type: request.workflow_type.clone(),
        task_list: request.task_list.clone(),
        header: request.header.clone(),
        input: request.input.clone(),
        execution_start_to_close_timeout_seconds: request.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: request.task_start_to_close_timeout_seconds,
        continued_execution_run_id: prev_run,
        prev_auto_reset_points: reset_points,
        identity: request.identity.clone(),
        retry_policy: request.retry_policy.clone(),
        attempt: start_request.attempt,
        expiration_timestamp: start_request.expiration_timestamp,
        cron_schedule: request.cron_schedule.clone(),
        cron_overlap_policy: request.cron_overlap_policy,
        last_completion_result: start_request.last_completion_result.clone(),
        continued_failure_reason: start_request.continued_failure_reason.clone().unwrap_or_default(),
        continued_failure_details: start_request.continued_failure_details.clone(),
        initiator: start_request.continue_as_new_initiator,
        first_decision_task_backoff_seconds: start_request.first_decision_task_backoff_seconds,
        first_execution_run_id: first_run_id.to_string(),
        // first scheduled time is only necessary for cron workflows.
        first_scheduled_time_nano: if request.cron_schedule.is_empty() {
            None
        } else {
            Some(first_scheduled_time_nanos)
        },
        original_execution_run_id: original_run_id.to_string(),
        memo: request.memo.clone(),
        search_attributes: request.search_attributes.clone(),
        jitter_start_seconds: request.jitter_start_seconds,
        partition_config: if start_request.partition_config.is_empty() {
            None
        } else {
            Some(start_request.partition_config.clone())
        },
        request_id: request.request_id.clone(),
        active_cluster_selection_policy: request.active_cluster_selection_policy.clone(),
        ..Default::default()
    };
    if let Some(p) = start_request.parent_execution_info.as_ref() {
        a.parent_workflow_domain_id = Some(p.domain_uuid.clone());
        a.parent_workflow_domain = Some(p.domain.clone());
        a.parent_workflow_execution = p.execution.clone();
        a.parent_initiated_event_id = Some(p.initiated_id);
    }
    a
}

impl TypedMutableState {
    /// Go `addWorkflowExecutionStartedEventForContinueAsNew`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_workflow_execution_started_event_for_continue_as_new(
        &mut self,
        env: &mut StartedEnv,
        decision_env: &DecisionEnv,
        parent_execution_info: Option<&ParentExecutionInfo>,
        execution: &WorkflowExecution,
        previous: &TypedMutableState,
        previous_auto_reset_points: Option<&ResetPoints>,
        attributes: &ContinueAsNewWorkflowExecutionDecisionAttributes,
        first_run_id: &str,
        first_scheduled_time_nanos: i64,
    ) -> Result<HistoryEvent> {
        let prev = &previous.execution_info;
        let mut tl = TaskList {
            name: prev.task_list.clone(),
            kind: TaskListKind::from_i32(prev.task_list_kind),
            ..Default::default()
        };
        // ContinueAsNew can change the name, not the kind
        if let Some(t) = attributes.task_list.as_ref() {
            tl.name = t.name.clone();
        }

        let workflow_type = match attributes.workflow_type.as_ref() {
            Some(t) => t.name.clone(),
            None => prev.workflow_type_name.clone(),
        };
        let w_type = WorkflowType { name: workflow_type };

        let mut decision_timeout = (prev.decision_start_to_close_timeout_nanos / NANOS_PER_SECOND) as i32;
        if let Some(t) = attributes.task_start_to_close_timeout_seconds {
            decision_timeout = t;
        }

        let create_request = StartWorkflowExecutionRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            domain: env.domain_name.clone(),
            workflow_id: execution.workflow_id.clone(),
            task_list: Some(tl),
            workflow_type: Some(w_type),
            task_start_to_close_timeout_seconds: Some(decision_timeout),
            execution_start_to_close_timeout_seconds: attributes.execution_start_to_close_timeout_seconds,
            input: attributes.input.clone(),
            header: attributes.header.clone(),
            retry_policy: attributes.retry_policy.clone(),
            cron_schedule: attributes.cron_schedule.clone(),
            memo: attributes.memo.clone(),
            search_attributes: attributes.search_attributes.clone(),
            jitter_start_seconds: attributes.jitter_start_seconds,
            cron_overlap_policy: attributes.cron_overlap_policy,
            active_cluster_selection_policy: attributes.active_cluster_selection_policy.clone(),
            ..Default::default()
        };
        let request_id = create_request.request_id.clone();

        let mut req = HistoryStartWorkflowExecutionRequest {
            domain_uuid: env.domain_id.clone(),
            start_request: Some(create_request),
            parent_execution_info: parent_execution_info.cloned(),
            last_completion_result: attributes.last_completion_result.clone(),
            continued_failure_reason: attributes.failure_reason.clone(),
            continued_failure_details: attributes.failure_details.clone(),
            continue_as_new_initiator: attributes.initiator,
            first_decision_task_backoff_seconds: attributes.backoff_start_interval_in_seconds,
            partition_config: prev.partition_config.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            ..Default::default()
        };

        // if ContinueAsNew as Cron or decider, recalculate the expiration timestamp and set attempts to 0
        req.attempt = 0;
        if let Some(rp) = attributes.retry_policy.as_ref() {
            if rp.expiration_interval_in_seconds > 0 {
                // expirationTime calculates from first decision task schedule to the end of the workflow
                let expiration_secs = i64::from(rp.expiration_interval_in_seconds)
                    + i64::from(req.first_decision_task_backoff_seconds.unwrap_or_default());
                req.expiration_timestamp = Some(env.now_nanos + expiration_secs * NANOS_PER_SECOND);
            }
        }
        // if ContinueAsNew as retry use the same expiration timestamp and increment attempts from previous execution state
        if attributes.initiator == Some(ContinueAsNewInitiator::RetryPolicy) {
            req.attempt = prev.attempt + 1;
            if prev.expiration_time_unix_nanos != 0 {
                req.expiration_timestamp = Some(prev.expiration_time_unix_nanos);
            }
        }

        // History event only has domainName so domainID has to be passed in explicitly to update the mutable state
        let parent_domain_id = parent_execution_info.map(|p| p.domain_uuid.clone());

        let attrs = build_started_event_attributes(
            &req,
            &prev.run_id,
            previous_auto_reset_points,
            true,
            first_run_id,
            &execution.run_id,
            first_scheduled_time_nanos,
        );
        let event = self.add_history_event(env.now_nanos, EventType::WorkflowExecutionStarted, |e| {
            e.workflow_execution_started_event_attributes = Some(attrs);
        })?;
        self.replicate_workflow_execution_started_event(
            env,
            parent_domain_id.as_deref(),
            &execution.workflow_id,
            &execution.run_id,
            &request_id,
            &event,
            false,
        )?;

        self.set_history_tree(&execution.run_id)?;

        self.add_first_decision_task_scheduled(decision_env, &event)?;
        Ok(event)
    }

    /// Go `AddWorkflowExecutionStartedEvent`.
    pub fn add_workflow_execution_started_event(
        &mut self,
        env: &mut StartedEnv,
        execution: &WorkflowExecution,
        start_request: &HistoryStartWorkflowExecutionRequest,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let request_id = start_request
            .start_request
            .as_ref()
            .map(|r| r.request_id.clone())
            .unwrap_or_default();
        let event_id = self.get_next_event_id();
        if event_id != FIRST_EVENT_ID {
            return Err(self.create_internal_server_error("add-workflow-started-event"));
        }

        let attrs = build_started_event_attributes(
            start_request,
            "",
            None,
            false,
            &execution.run_id,
            &execution.run_id,
            env.now_nanos,
        );
        let event = self.add_history_event(env.now_nanos, EventType::WorkflowExecutionStarted, |e| {
            e.workflow_execution_started_event_attributes = Some(attrs);
        })?;

        let parent_domain_id = start_request.parent_execution_info.as_ref().map(|p| p.domain_uuid.clone());
        self.replicate_workflow_execution_started_event(
            env,
            parent_domain_id.as_deref(),
            &execution.workflow_id,
            &execution.run_id,
            &request_id,
            &event,
            true,
        )?;
        Ok(event)
    }

    /// Go `SetHistoryTree`: new branch token for `tree_id`, stored through
    /// `SetCurrentBranchToken` (execution info without version histories,
    /// otherwise the current version history).
    pub fn set_history_tree(&mut self, tree_id: &str) -> Result<()> {
        let branch_id = uuid::Uuid::new_v4().to_string();
        let token = cadence_persistence::history_branch::new_token(tree_id, &branch_id);
        self.set_current_branch_token(token)
    }

    /// Go `SetCurrentBranchToken`.
    pub fn set_current_branch_token(&mut self, token: Vec<u8>) -> Result<()> {
        match self.version_histories.as_mut() {
            None => {
                self.execution_info.branch_token = token;
                Ok(())
            }
            Some(h) => {
                let idx = h.current_version_history_index as usize;
                match h.histories.get_mut(idx) {
                    Some(v) => {
                        v.branch_token = token;
                        Ok(())
                    }
                    None => internal_err("version history index out of range"),
                }
            }
        }
    }

    /// Go `ReplicateWorkflowExecutionStartedEvent` (task generation, events
    /// cache write and ACSP blob not wired).
    #[allow(clippy::too_many_arguments)]
    pub fn replicate_workflow_execution_started_event(
        &mut self,
        env: &mut StartedEnv,
        parent_domain_id: Option<&str>,
        workflow_id: &str,
        run_id: &str,
        request_id: &str,
        start_event: &HistoryEvent,
        generate_delayed_decision_tasks: bool,
    ) -> Result<()> {
        let Some(a) = start_event.workflow_execution_started_event_attributes.as_ref() else {
            return internal_err("WorkflowExecutionStarted event is missing attributes");
        };
        let mut request_id = request_id.to_string();
        if !a.request_id.is_empty() {
            // prefer requestID from history event
            request_id = a.request_id.clone();
        }
        self.execution_info.create_request_id = request_id.clone();

        // Go insertWorkflowRequest (type/version not representable yet).
        self.insert_workflow_requests.push(WorkflowRequest {
            request_id,
            version: start_event.version,
            request_type: cadence_persistence::history_contract::WORKFLOW_REQUEST_TYPE_START,
            data: Default::default(),
        });
        self.execution_info.domain_id = env.domain_id.clone();
        self.execution_info.workflow_id = workflow_id.to_string();
        self.execution_info.run_id = run_id.to_string();
        self.execution_info.first_execution_run_id = a.first_execution_run_id.clone();
        if self.execution_info.first_execution_run_id.is_empty() {
            self.execution_info.first_execution_run_id = run_id.to_string();
        }
        self.execution_info.task_list = a.task_list.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.execution_info.task_list_kind =
            a.task_list.as_ref().and_then(|t| t.kind).map(|k| k as i32).unwrap_or_default();
        self.execution_info.workflow_type_name =
            a.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.execution_info.workflow_timeout_nanos =
            i64::from(a.execution_start_to_close_timeout_seconds.unwrap_or_default()) * NANOS_PER_SECOND;
        self.execution_info.decision_start_to_close_timeout_nanos =
            i64::from(a.task_start_to_close_timeout_seconds.unwrap_or_default()) * NANOS_PER_SECOND;
        let ts = start_event.timestamp.unwrap_or_default();
        self.execution_info.start_time_unix_nanos = ts;

        self.update_workflow_state_close_status(WORKFLOW_STATE_CREATED, CLOSE_STATUS_NONE)?;
        self.execution_info.last_processed_event = EMPTY_EVENT_ID;
        self.execution_info.last_first_event_id = start_event.event_id;

        self.execution_info.decision_version = EMPTY_VERSION;
        self.execution_info.decision_schedule_id = EMPTY_EVENT_ID;
        self.execution_info.decision_started_id = EMPTY_EVENT_ID;
        self.execution_info.decision_request_id = EMPTY_UUID.to_string();
        self.execution_info.decision_timeout_nanos = 0;

        self.execution_info.cron_schedule = a.cron_schedule.clone();
        if let Some(p) = a.cron_overlap_policy {
            self.execution_info.cron_overlap_policy = p as i32;
        }

        if let Some(p) = parent_domain_id {
            self.execution_info.parent_domain_id = p.to_string();
        }
        if let Some(pe) = a.parent_workflow_execution.as_ref() {
            self.execution_info.parent_workflow_id = pe.workflow_id.clone();
            self.execution_info.parent_run_id = pe.run_id.clone();
        }
        self.execution_info.initiated_id = a.parent_initiated_event_id.unwrap_or(EMPTY_EVENT_ID);

        self.execution_info.attempt = a.attempt;
        if let Some(exp) = a.expiration_timestamp {
            if exp != 0 {
                self.execution_info.expiration_time_unix_nanos = exp;
            }
        }
        if let Some(rp) = a.retry_policy.as_ref() {
            self.execution_info.has_retry_policy = true;
            self.execution_info.backoff_coefficient = rp.backoff_coefficient;
            self.execution_info.jitter_coefficient = rp.jitter_coefficient;
            self.execution_info.expiration_interval_nanos =
                i64::from(rp.expiration_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.initial_interval_nanos =
                i64::from(rp.initial_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.maximum_attempts = rp.maximum_attempts;
            self.execution_info.maximum_interval_nanos =
                i64::from(rp.maximum_interval_in_seconds) * NANOS_PER_SECOND;
            self.execution_info.non_retriable_errors = rp.non_retriable_error_reasons.clone();
        }

        env.auto_reset_points = rollover_auto_reset_points_with_expiring_time(
            a.prev_auto_reset_points.as_ref(),
            &a.continued_execution_run_id,
            ts,
            env.retention_days,
        );

        if let Some(m) = a.memo.as_ref().and_then(|m| m.fields.as_ref()) {
            self.execution_info.memo = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        if let Some(sa) = a.search_attributes.as_ref().and_then(|s| s.indexed_fields.as_ref()) {
            self.execution_info.search_attributes = sa.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        if let Some(pc) = a.partition_config.as_ref() {
            self.execution_info.partition_config = pc.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
        // Go `executionInfo.ActiveClusterSelectionPolicy = event.ActiveClusterSelectionPolicy`.
        self.execution_info.active_cluster_selection_policy = match a.active_cluster_selection_policy.as_ref() {
            Some(policy) => {
                use cadence_persistence::sqlblobs::Encoding;
                use cadence_thrift::ToThrift;
                let data = Encoding::ThriftRW
                    .encode_versioned(&policy.to_thrift())
                    .map_err(|e| super::mutable_state_builder::TypedMutableStateError::Internal(format!("encode active cluster selection policy: {e}")))?;
                Some(cadence_persistence::history_contract::DataBlob { encoding: "thriftrw".to_string(), data })
            }
            None => None,
        };
        // Not wired: writeEventToCache.
        if !self.suppress_task_generation {
            self.run_task_generator(0, |g| g.generate_workflow_start_tasks(ts, start_event))?;
            self.run_task_generator(0, |g| g.generate_record_workflow_started_tasks(start_event))?;
        }
        if !self.suppress_task_generation && generate_delayed_decision_tasks && a.first_decision_task_backoff_seconds.unwrap_or_default() > 0 {
            self.run_task_generator(0, |g| g.generate_delayed_decision_tasks(start_event))?;
        }
        Ok(())
    }
}
