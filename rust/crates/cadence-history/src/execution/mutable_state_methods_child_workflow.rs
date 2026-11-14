//! `mutable_state_builder_methods_child_workflow.go` on [`TypedMutableState`].
//! Not wired yet: events cache (`GetChildExecutionInitiatedEvent`, `PutEvent`),
//! domain cache lookups (domain name / ID are passed in by the caller) and
//! `taskGenerator.GenerateChildWorkflowTasks`.

use cadence_api::enums::{ChildWorkflowExecutionFailedCause, EventType};
use cadence_api::service_types::{HistoryEvent, WorkflowExecutionTerminatedEventAttributes};
use cadence_api::tasks::{
    ChildWorkflowExecutionCanceledEventAttributes, ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionFailedEventAttributes, ChildWorkflowExecutionStartedEventAttributes,
    ChildWorkflowExecutionTerminatedEventAttributes, ChildWorkflowExecutionTimedOutEventAttributes,
    StartChildWorkflowExecutionDecisionAttributes, StartChildWorkflowExecutionFailedEventAttributes,
    StartChildWorkflowExecutionInitiatedEventAttributes, WorkflowExecutionCanceledEventAttributes,
    WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
    WorkflowExecutionTimedOutEventAttributes,
};
use cadence_api::types::{Header, WorkflowExecution, WorkflowType};

use super::child_workflow::ChildExecutionInfo;
use super::mutable_state_builder::{TypedMutableState, EMPTY_EVENT_ID};
use super::mutable_state_methods_common::{internal_err, Result};

/// Go `ErrMissingChildWorkflowInfo` message.
pub const ERR_MISSING_CHILD_WORKFLOW_INFO: &str = "unable to get child workflow info";

impl TypedMutableState {
    /// Go `GetChildExecutionInfo`.
    pub fn get_child_execution_info(&self, initiated_event_id: i64) -> Option<&ChildExecutionInfo> {
        self.pending_child_execution_info_ids.get(&initiated_event_id)
    }

    /// Go `GetPendingChildExecutionInfos`.
    pub fn get_pending_child_execution_infos(&self) -> &std::collections::BTreeMap<i64, ChildExecutionInfo> {
        &self.pending_child_execution_info_ids
    }

    /// Go `DeletePendingChildExecution` (missing entry is only logged in Go).
    pub fn delete_pending_child_execution(&mut self, initiated_event_id: i64) -> Result<()> {
        self.pending_child_execution_info_ids.remove(&initiated_event_id);
        self.update_child_execution_infos.remove(&initiated_event_id);
        self.delete_child_execution_info_ids.insert(initiated_event_id);
        Ok(())
    }

    /// Go `AddStartChildWorkflowExecutionInitiatedEvent`; `domain_name` is the
    /// parent domain name (`GetDomainEntry().GetInfo().Name`),
    /// `child_domain_id` the result of the domain cache lookup.
    pub fn add_start_child_workflow_execution_initiated_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        create_request_id: &str,
        attributes: &StartChildWorkflowExecutionDecisionAttributes,
        domain_name: &str,
        child_domain_id: &str,
    ) -> Result<(HistoryEvent, ChildExecutionInfo)> {
        self.check_mutability()?;
        // Go hBuilder: empty attributes.Domain falls back to the parent domain.
        let domain = if attributes.domain.is_empty() { domain_name.to_string() } else { attributes.domain.clone() };
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::StartChildWorkflowExecutionInitiated,
            |e| {
                e.start_child_workflow_execution_initiated_event_attributes =
                    Some(StartChildWorkflowExecutionInitiatedEventAttributes {
                        domain,
                        workflow_id: attributes.workflow_id.clone(),
                        workflow_type: attributes.workflow_type.clone(),
                        task_list: attributes.task_list.clone(),
                        input: attributes.input.clone(),
                        execution_start_to_close_timeout_seconds: attributes
                            .execution_start_to_close_timeout_seconds,
                        task_start_to_close_timeout_seconds: attributes.task_start_to_close_timeout_seconds,
                        // Go: GetParentClosePolicy().Ptr() (unset -> ABANDON).
                        parent_close_policy: Some(
                            attributes.parent_close_policy.unwrap_or(cadence_api::enums::ParentClosePolicy::Abandon),
                        ),
                        cron_overlap_policy: attributes.cron_overlap_policy,
                        control: attributes.control.clone(),
                        decision_task_completed_event_id: decision_completed_event_id,
                        workflow_id_reuse_policy: attributes.workflow_id_reuse_policy,
                        header: attributes.header.clone(),
                        retry_policy: attributes.retry_policy.clone(),
                        cron_schedule: attributes.cron_schedule.clone(),
                        memo: attributes.memo.clone(),
                        search_attributes: attributes.search_attributes.clone(),
                        active_cluster_selection_policy: attributes.active_cluster_selection_policy.clone(),
                        ..Default::default()
                    });
            },
        )?;
        let ci = self.replicate_start_child_workflow_execution_initiated_event(
            decision_completed_event_id,
            &event,
            create_request_id,
            child_domain_id,
        )?;
        Ok((event, ci))
    }

    /// Go `ReplicateStartChildWorkflowExecutionInitiatedEvent`. `child_domain_id`
    /// replaces the domain-cache lookup (pass the parent domain ID when the
    /// event domain is empty).
    pub fn replicate_start_child_workflow_execution_initiated_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
        create_request_id: &str,
        child_domain_id: &str,
    ) -> Result<ChildExecutionInfo> {
        let Some(attributes) = event.start_child_workflow_execution_initiated_event_attributes.as_ref() else {
            return internal_err("StartChildWorkflowExecutionInitiated event is missing attributes");
        };
        let ci = ChildExecutionInfo {
            version: event.version,
            initiated_id: event.event_id,
            initiated_event_batch_id: first_event_id,
            started_id: EMPTY_EVENT_ID,
            started_run_id: String::new(),
            started_workflow_id: attributes.workflow_id.clone(),
            create_request_id: create_request_id.to_string(),
            domain_id: child_domain_id.to_string(),
            workflow_type_name: attributes.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
            parent_close_policy: attributes.parent_close_policy,
        };
        self.pending_child_execution_info_ids.insert(ci.initiated_id, ci.clone());
        self.update_child_execution_infos.insert(ci.initiated_id, ci.clone());
        self.run_task_generator(0, |g| g.generate_child_workflow_tasks(event))?;
        Ok(ci)
    }

    /// Go `AddChildWorkflowExecutionStartedEvent`.
    pub fn add_child_workflow_execution_started_event(
        &mut self,
        timestamp_unix_nanos: i64,
        domain: &str,
        execution: &WorkflowExecution,
        workflow_type: &WorkflowType,
        initiated_id: i64,
        header: Option<Header>,
    ) -> Result<HistoryEvent> {
        let op = "add-childworkflow-started-event";
        self.check_mutability()?;
        match self.get_child_execution_info(initiated_id) {
            Some(ci) if ci.started_id == EMPTY_EVENT_ID => {}
            _ => return Err(self.create_internal_server_error(op)),
        }
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ChildWorkflowExecutionStarted, |e| {
            e.child_workflow_execution_started_event_attributes = Some(ChildWorkflowExecutionStartedEventAttributes {
                domain: domain.to_string(),
                initiated_event_id: initiated_id,
                workflow_execution: Some(execution.clone()),
                workflow_type: Some(workflow_type.clone()),
                header,
            });
        })?;
        self.replicate_child_workflow_execution_started_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionStartedEvent`.
    pub fn replicate_child_workflow_execution_started_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.child_workflow_execution_started_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionStarted event is missing attributes");
        };
        let initiated_id = attributes.initiated_event_id;
        let Some(ci) = self.pending_child_execution_info_ids.get_mut(&initiated_id) else {
            return internal_err(ERR_MISSING_CHILD_WORKFLOW_INFO);
        };
        ci.started_id = event.event_id;
        ci.started_run_id =
            attributes.workflow_execution.as_ref().map(|e| e.run_id.clone()).unwrap_or_default();
        let ci = ci.clone();
        self.update_child_execution_infos.insert(initiated_id, ci);
        Ok(())
    }

    /// Go `AddStartChildWorkflowExecutionFailedEvent`.
    pub fn add_start_child_workflow_execution_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        cause: ChildWorkflowExecutionFailedCause,
        initiated_attributes: &StartChildWorkflowExecutionInitiatedEventAttributes,
    ) -> Result<HistoryEvent> {
        let op = "add-childworkflow-initiation-failed-event";
        self.check_mutability()?;
        match self.get_child_execution_info(initiated_id) {
            Some(ci) if ci.started_id == EMPTY_EVENT_ID => {}
            _ => return Err(self.create_internal_server_error(op)),
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::StartChildWorkflowExecutionFailed,
            |e| {
                e.start_child_workflow_execution_failed_event_attributes =
                    Some(StartChildWorkflowExecutionFailedEventAttributes {
                        domain: initiated_attributes.domain.clone(),
                        workflow_id: initiated_attributes.workflow_id.clone(),
                        workflow_type: initiated_attributes.workflow_type.clone(),
                        cause: Some(cause),
                        control: initiated_attributes.control.clone(),
                        initiated_event_id: initiated_id,
                        decision_task_completed_event_id: initiated_attributes.decision_task_completed_event_id,
                    });
            },
        )?;
        self.replicate_start_child_workflow_execution_failed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateStartChildWorkflowExecutionFailedEvent`.
    pub fn replicate_start_child_workflow_execution_failed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.start_child_workflow_execution_failed_event_attributes.as_ref() else {
            return internal_err("StartChildWorkflowExecutionFailed event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }

    /// Common Go prologue of the five child terminal `Add*` methods: checks
    /// mutability and that the child exists and has started; returns
    /// `(initiated_id, started_id, workflow_type)`.
    fn child_terminal_prologue(&self, op: &str, initiated_id: i64) -> Result<(i64, i64, WorkflowType)> {
        self.check_mutability()?;
        match self.get_child_execution_info(initiated_id) {
            Some(ci) if ci.started_id != EMPTY_EVENT_ID => Ok((
                ci.initiated_id,
                ci.started_id,
                WorkflowType { name: ci.workflow_type_name.clone() },
            )),
            _ => Err(self.create_internal_server_error(op)),
        }
    }

    /// Go `AddChildWorkflowExecutionCompletedEvent`; `child_domain_name` is the
    /// result of `GetChildExecutionDomainName`.
    pub fn add_child_workflow_execution_completed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        child_domain_name: &str,
        child_execution: &WorkflowExecution,
        attributes: &WorkflowExecutionCompletedEventAttributes,
    ) -> Result<HistoryEvent> {
        let (init, started, workflow_type) =
            self.child_terminal_prologue("add-childworkflow-completed-event", initiated_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ChildWorkflowExecutionCompleted, |e| {
            e.child_workflow_execution_completed_event_attributes =
                Some(ChildWorkflowExecutionCompletedEventAttributes {
                    result: attributes.result.clone(),
                    domain: child_domain_name.to_string(),
                    workflow_execution: Some(child_execution.clone()),
                    workflow_type: Some(workflow_type),
                    initiated_event_id: init,
                    started_event_id: started,
                });
        })?;
        self.replicate_child_workflow_execution_completed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionCompletedEvent`.
    pub fn replicate_child_workflow_execution_completed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.child_workflow_execution_completed_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionCompleted event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }

    /// Go `AddChildWorkflowExecutionFailedEvent`.
    pub fn add_child_workflow_execution_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        child_domain_name: &str,
        child_execution: &WorkflowExecution,
        attributes: &WorkflowExecutionFailedEventAttributes,
    ) -> Result<HistoryEvent> {
        let (init, started, workflow_type) =
            self.child_terminal_prologue("add-childworkflow-failed-event", initiated_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ChildWorkflowExecutionFailed, |e| {
            e.child_workflow_execution_failed_event_attributes = Some(ChildWorkflowExecutionFailedEventAttributes {
                reason: attributes.reason.clone(),
                details: attributes.details.clone(),
                domain: child_domain_name.to_string(),
                workflow_execution: Some(child_execution.clone()),
                workflow_type: Some(workflow_type),
                initiated_event_id: init,
                started_event_id: started,
            });
        })?;
        self.replicate_child_workflow_execution_failed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionFailedEvent`.
    pub fn replicate_child_workflow_execution_failed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.child_workflow_execution_failed_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionFailed event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }

    /// Go `AddChildWorkflowExecutionCanceledEvent`.
    pub fn add_child_workflow_execution_canceled_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        child_domain_name: &str,
        child_execution: &WorkflowExecution,
        attributes: &WorkflowExecutionCanceledEventAttributes,
    ) -> Result<HistoryEvent> {
        let (init, started, workflow_type) =
            self.child_terminal_prologue("add-childworkflow-canceled-event", initiated_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ChildWorkflowExecutionCanceled, |e| {
            e.child_workflow_execution_canceled_event_attributes =
                Some(ChildWorkflowExecutionCanceledEventAttributes {
                    details: attributes.details.clone(),
                    domain: child_domain_name.to_string(),
                    workflow_execution: Some(child_execution.clone()),
                    workflow_type: Some(workflow_type),
                    initiated_event_id: init,
                    started_event_id: started,
                });
        })?;
        self.replicate_child_workflow_execution_canceled_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionCanceledEvent`.
    pub fn replicate_child_workflow_execution_canceled_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.child_workflow_execution_canceled_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionCanceled event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }

    /// Go `AddChildWorkflowExecutionTerminatedEvent`.
    pub fn add_child_workflow_execution_terminated_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        child_domain_name: &str,
        child_execution: &WorkflowExecution,
        _attributes: &WorkflowExecutionTerminatedEventAttributes,
    ) -> Result<HistoryEvent> {
        let (init, started, workflow_type) =
            self.child_terminal_prologue("add-childworkflow-terminated-event", initiated_id)?;
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::ChildWorkflowExecutionTerminated,
            |e| {
                e.child_workflow_execution_terminated_event_attributes =
                    Some(ChildWorkflowExecutionTerminatedEventAttributes {
                        domain: child_domain_name.to_string(),
                        workflow_execution: Some(child_execution.clone()),
                        workflow_type: Some(workflow_type),
                        initiated_event_id: init,
                        started_event_id: started,
                    });
            },
        )?;
        self.replicate_child_workflow_execution_terminated_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionTerminatedEvent`.
    pub fn replicate_child_workflow_execution_terminated_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.child_workflow_execution_terminated_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionTerminated event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }

    /// Go `AddChildWorkflowExecutionTimedOutEvent`.
    pub fn add_child_workflow_execution_timed_out_event(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        child_domain_name: &str,
        child_execution: &WorkflowExecution,
        attributes: &WorkflowExecutionTimedOutEventAttributes,
    ) -> Result<HistoryEvent> {
        let (init, started, workflow_type) =
            self.child_terminal_prologue("add-childworkflow-timedout-event", initiated_id)?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::ChildWorkflowExecutionTimedOut, |e| {
            e.child_workflow_execution_timed_out_event_attributes =
                Some(ChildWorkflowExecutionTimedOutEventAttributes {
                    timeout_type: attributes.timeout_type,
                    domain: child_domain_name.to_string(),
                    workflow_execution: Some(child_execution.clone()),
                    workflow_type: Some(workflow_type),
                    initiated_event_id: init,
                    started_event_id: started,
                });
        })?;
        self.replicate_child_workflow_execution_timed_out_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateChildWorkflowExecutionTimedOutEvent`.
    pub fn replicate_child_workflow_execution_timed_out_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.child_workflow_execution_timed_out_event_attributes.as_ref() else {
            return internal_err("ChildWorkflowExecutionTimedOut event is missing attributes");
        };
        self.delete_pending_child_execution(a.initiated_event_id)
    }
}

#[cfg(test)]
mod child_initiated_parity_tests {
    use super::*;
    use crate::execution::mutable_state_task_generator::TaskGeneratorDomains;
    use crate::execution::mutable_state_builder::TypedMutableStateError;

    struct Domains;
    impl TaskGeneratorDomains for Domains {
        fn domain_id_by_name(&self, name: &str) -> std::result::Result<String, TypedMutableStateError> { Ok(format!("id-{name}")) }
        fn retention_days(&self, _: &str, _: &str) -> std::result::Result<Option<i32>, TypedMutableStateError> { Ok(Some(3)) }
        fn is_global_domain(&self, _: &str) -> std::result::Result<bool, TypedMutableStateError> { Ok(false) }
    }

    #[test]
    fn initiated_event_defaults_parent_close_policy_to_abandon() {
        let mut s = TypedMutableState::new();
        s.execution_info.state = 1;
        s.execution_info.decision_started_id = 3;
        s.task_gen_env.domains = Some(std::sync::Arc::new(Domains));
        let attrs = StartChildWorkflowExecutionDecisionAttributes::default();
        let (event, _) = s
            .add_start_child_workflow_execution_initiated_event(1, 4, "req", &attrs, "parent", "cid")
            .unwrap();
        let a = event.start_child_workflow_execution_initiated_event_attributes.unwrap();
        assert_eq!(a.parent_close_policy, Some(cadence_api::enums::ParentClosePolicy::Abandon));
        assert_eq!(a.domain, "parent");
    }

    #[test]
    fn initiated_event_carries_active_cluster_selection_policy() {
        let policy = cadence_api::types::ActiveClusterSelectionPolicy {
            cluster_attribute: Some(cadence_api::types::ClusterAttribute { scope: "s".into(), name: "n".into() }),
        };
        let mut s = TypedMutableState::new();
        s.execution_info.state = 1;
        s.execution_info.decision_started_id = 3;
        s.task_gen_env.domains = Some(std::sync::Arc::new(Domains));
        let attrs = StartChildWorkflowExecutionDecisionAttributes {
            active_cluster_selection_policy: Some(policy.clone()),
            ..Default::default()
        };
        let (event, _) = s
            .add_start_child_workflow_execution_initiated_event(1, 4, "req", &attrs, "parent", "cid")
            .unwrap();
        let a = event.start_child_workflow_execution_initiated_event_attributes.unwrap();
        assert_eq!(a.active_cluster_selection_policy, Some(policy));
    }
}
