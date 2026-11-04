//! Validation and reuse policy from engineimpl/start_workflow_execution.go.
//! TODO(link): start transaction orchestration and persistence adapters.
use super::{
    common_util::is_valid_id_length,
    deps::{HResult, HistoryError},
};
use cadence_api::{
    errors::WorkflowExecutionAlreadyStartedError, types::StartWorkflowExecutionRequest,
};

/// TODO(link): engine configuration and metrics adapter. Lengths count UTF-8 bytes, as Go does.
pub trait StartValidationConfig {
    fn max_id_length_warn_limit(&self) -> usize;
    fn domain_name_max_length(&self, domain: &str) -> usize;
    fn workflow_id_max_length(&self, domain: &str) -> usize;
    fn task_list_name_max_length(&self, domain: &str) -> usize;
    fn workflow_type_max_length(&self, domain: &str) -> usize;
    fn id_length_warning(&self, domain: &str, id_type: &str, value: &str);
    fn decision_timeout_overridden(&self, domain: &str);
}

pub fn validate_start_workflow_execution_request(
    request: &StartWorkflowExecutionRequest,
    config: &dyn StartValidationConfig,
) -> HResult<()> {
    if request.request_id.is_empty() {
        return Err(HistoryError::bad_request("Missing request ID."));
    }
    if request
        .execution_start_to_close_timeout_seconds
        .unwrap_or_default()
        <= 0
    {
        return Err(HistoryError::bad_request(
            "Missing or invalid ExecutionStartToCloseTimeoutSeconds.",
        ));
    }
    if request
        .task_start_to_close_timeout_seconds
        .unwrap_or_default()
        <= 0
    {
        return Err(HistoryError::bad_request(
            "Missing or invalid TaskStartToCloseTimeoutSeconds.",
        ));
    }
    let task_list = request
        .task_list
        .as_ref()
        .map(|t| t.name.as_str())
        .unwrap_or_default();
    if task_list.is_empty() {
        return Err(HistoryError::bad_request("Missing Tasklist."));
    }
    let workflow_type = request
        .workflow_type
        .as_ref()
        .map(|t| t.name.as_str())
        .unwrap_or_default();
    if workflow_type.is_empty() {
        return Err(HistoryError::bad_request("Missing WorkflowType."));
    }
    let domain = &request.domain;
    for (value, limit, id_type, message) in [
        (
            domain.as_str(),
            config.domain_name_max_length(domain),
            "domain-name",
            "Domain exceeds length limit.",
        ),
        (
            request.workflow_id.as_str(),
            config.workflow_id_max_length(domain),
            "workflow-id",
            "WorkflowId exceeds length limit.",
        ),
        (
            task_list,
            config.task_list_name_max_length(domain),
            "task-list-name",
            "TaskList exceeds length limit.",
        ),
        (
            workflow_type,
            config.workflow_type_max_length(domain),
            "workflow-type",
            "WorkflowType exceeds length limit.",
        ),
    ] {
        let (valid, warn) = is_valid_id_length(value, config.max_id_length_warn_limit(), limit);
        if warn {
            config.id_length_warning(domain, id_type, value);
        }
        if !valid {
            return Err(HistoryError::bad_request(message));
        }
    }
    cadence_api::validation::validate_retry_policy(request.retry_policy.as_ref())?;
    Ok(())
}

pub fn override_task_start_to_close_timeout_seconds(
    domain: &str,
    request: &mut StartWorkflowExecutionRequest,
    max_decision_start_to_close_seconds: i32,
    metrics: &dyn StartValidationConfig,
) {
    let original = request
        .task_start_to_close_timeout_seconds
        .unwrap_or_default();
    let timeout = original.min(max_decision_start_to_close_seconds).min(
        request
            .execution_start_to_close_timeout_seconds
            .unwrap_or_default(),
    );
    if timeout != original {
        request.task_start_to_close_timeout_seconds = Some(timeout);
        metrics.decision_timeout_overridden(domain);
    }
}

/// The policy is an integer to retain Go's unknown-value error path.
pub fn apply_workflow_id_reuse_policy(
    previous_request_id: &str,
    previous_run_id: &str,
    previous_state: i32,
    previous_close_state: i32,
    workflow_id: &str,
    policy: i32,
) -> HResult<()> {
    let already_started = |message: String| -> HistoryError {
        WorkflowExecutionAlreadyStartedError {
            message,
            start_request_id: previous_request_id.into(),
            run_id: previous_run_id.into(),
        }
        .into()
    };
    match previous_state {
        0 | 1 => return Err(already_started(format!("Workflow execution is already running. WorkflowId: {workflow_id}, RunId: {previous_run_id}."))),
        2 => {},
        5 => return Ok(()), // corrupted executions ignore the reuse policy
        other => return Err(HistoryError::internal(format!("Failed to process workflow, workflow has invalid state: {other}."))),
    }
    match policy {
        0 => {
            if !matches!(previous_close_state, 2 | 3 | 4 | 6) {
                return Err(already_started(format!("Workflow execution already finished successfully. WorkflowId: {workflow_id}, RunId: {previous_run_id}. Workflow ID reuse policy: allow duplicate workflow ID if last run failed.")));
            }
        }
        1 | 3 => {},
        2 => return Err(already_started(format!("Workflow execution already finished. WorkflowId: {workflow_id}, RunId: {previous_run_id}. Workflow ID reuse policy: reject duplicate workflow ID."))),
        _ => return Err(HistoryError::internal("Failed to process start workflow reuse policy.")),
    }
    Ok(())
}

pub fn should_terminate_and_start(policy: i32, state: i32) -> bool {
    policy == 3 && matches!(state, 0 | 1)
}

pub fn get_terminate_if_running_details(new_run_id: &str) -> Vec<u8> {
    format!("New runID: {new_run_id}").into_bytes()
}

use super::{deps::*, runtime::HistoryEngine, start_dependencies::*, types::*};
use cadence_api::{
    errors::ServiceBusyError,
    types::{StartWorkflowExecutionResponse, WorkflowExecution},
    CadenceError,
};
use std::time::Instant;

#[derive(Default)]
pub struct StartArtifacts {
    pub execution: Option<WorkflowExecution>,
    pub blob: Option<PersistedStartBlob>,
}
impl HistoryEngine {
    pub async fn start_workflow_execution(
        &self,
        request: &mut HistoryStartWorkflowExecutionRequest,
        deadline: Option<Instant>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let domain = self.domain_cache.get_domain_by_id(&request.domain_uuid)?;
        let mut artifacts = StartArtifacts::default();
        let result = self
            .start_workflow_helper(request, &domain, None, None, deadline, &mut artifacts)
            .await;
        if let Err(error) = &result {
            self.cleanup_start_failure(&domain, &artifacts, error).await;
        }
        result
    }

    pub async fn start_workflow_helper(
        &self,
        request: &mut HistoryStartWorkflowExecutionRequest,
        domain: &DomainCacheEntry,
        previous: Option<MutableStateRef>,
        signal: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
        deadline: Option<Instant>,
        artifacts: &mut StartArtifacts,
    ) -> HResult<StartWorkflowExecutionResponse> {
        if domain.info.status != DOMAIN_STATUS_REGISTERED {
            return Err(HistoryError::bad_request("Domain is deprecated."));
        }
        let start = request.start_request.get_or_insert_default();
        validate_start_workflow_execution_request(start, &*self.start_backend)?;
        override_task_start_to_close_timeout_seconds(
            &domain.info.name,
            start,
            self.start_backend
                .maximum_decision_timeout(&domain.info.name),
            &*self.start_backend,
        );
        let workflow_id = start.workflow_id.clone();
        let lock = tokio::time::timeout(
            child_lock_timeout(deadline),
            self.execution_cache
                .get_or_create_current_workflow_execution(&domain.info.id, &workflow_id),
        )
        .await;
        let release = match lock {
            Ok(Ok(release)) => release,
            Err(_) | Ok(Err(HistoryError::DeadlineExceeded)) => return Err(ServiceBusyError {
                message: "an outstanding start workflow request is in-progress. Failed to acquire the resource.".into(),
                ..Default::default()
            }.into()),
            Ok(Err(error)) => return Err(error),
        };
        let result = self
            .start_workflow_locked(request, domain, previous, signal, artifacts)
            .await;
        release(result.as_ref().err());
        result
    }

    async fn start_workflow_locked(
        &self,
        request: &HistoryStartWorkflowExecutionRequest,
        domain: &DomainCacheEntry,
        previous: Option<MutableStateRef>,
        signal: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
        artifacts: &mut StartArtifacts,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let start = request
            .start_request
            .as_ref()
            .expect("validated start request");
        let execution = WorkflowExecution {
            workflow_id: start.workflow_id.clone(),
            run_id: uuid::Uuid::new_v4().to_string(),
        };
        let state = self
            .create_start_mutable_state(domain, &execution.run_id, request)
            .await?;
        let reuse = start
            .workflow_id_reuse_policy
            .map(|p| p as i32)
            .unwrap_or_default();
        if let Some(previous) = &previous {
            let version = previous.get_last_write_version()?;
            let info = previous.get_execution_info();
            if version > state.get_current_version() {
                let policy = self
                    .start_backend
                    .workflow_selection_policy(
                        &domain.info.id,
                        &execution.workflow_id,
                        &info.run_id,
                    )
                    .await?;
                if policy == start.active_cluster_selection_policy {
                    return Err(self.new_domain_not_active_error(domain, version));
                }
            }
            apply_workflow_id_reuse_policy(
                &info.create_request_id,
                &info.run_id,
                info.state,
                info.close_status,
                &execution.workflow_id,
                reuse,
            )?;
        } else if self
            .start_backend
            .record_uninitialized_enabled(&domain.info.name)
        {
            if let Err(error) = self
                .start_backend
                .record_uninitialized(
                    domain,
                    &execution,
                    &start.workflow_type.as_ref().expect("validated type").name,
                    self.time_source.now(),
                )
                .await
            {
                tracing::error!(error = %error, "Failed to record uninitialized workflow execution");
            }
        }
        if let Err(error) = self.add_start_events_and_tasks(&*state, &execution, request, signal) {
            if self
                .start_backend
                .record_uninitialized_enabled(&domain.info.name)
            {
                if let Err(error) = self
                    .start_backend
                    .delete_visibility(domain, &execution)
                    .await
                {
                    tracing::error!(error = %error, "Failed to delete uninitialized workflow execution record");
                }
            }
            return Err(error);
        }
        let snapshot = self
            .start_backend
            .close_snapshot(state.clone(), self.time_source.now())
            .await?;
        artifacts.execution = Some(execution.clone());
        let blob = self
            .start_backend
            .persist_start_batch(&domain.info.id, &execution, &snapshot.event_batches[0])
            .await?;
        artifacts.blob = Some(blob.clone());
        let (mode, previous_run, previous_version) = if let Some(previous) = &previous {
            let info = previous.get_execution_info();
            (
                if info.state == WORKFLOW_STATE_CORRUPTED {
                    CreateWorkflowMode::ContinueAsNew
                } else {
                    CreateWorkflowMode::WorkflowIdReuse
                },
                info.run_id,
                previous.get_last_write_version()?,
            )
        } else {
            (CreateWorkflowMode::BrandNew, String::new(), 0)
        };
        let result = self
            .start_backend
            .create_workflow(
                &domain.info.id,
                &execution,
                &snapshot,
                &blob,
                mode,
                &previous_run,
                previous_version,
            )
            .await;
        let already = match result {
            Ok(()) => {
                return Ok(StartWorkflowExecutionResponse {
                    run_id: execution.run_id,
                })
            }
            Err(HistoryError::DuplicateRequest {
                request_type,
                run_id,
            }) => {
                if request_type == WorkflowRequestType::Start
                    || (signal.is_some() && request_type == WorkflowRequestType::Signal)
                {
                    return Ok(StartWorkflowExecutionResponse { run_id });
                }
                tracing::error!(
                    ?request_type,
                    "A bug is detected for idempotency improvement"
                );
                return Err(HistoryError::DuplicateRequest {
                    request_type,
                    run_id,
                });
            }
            Err(HistoryError::PersistenceAlreadyStarted(already)) => already,
            Err(error) => return Err(error),
        };
        if already.start_request_id == start.request_id {
            return Ok(StartWorkflowExecutionResponse {
                run_id: already.run_id,
            });
        }
        if signal.is_some() {
            tracing::warn!(
                workflow_domain_id = domain.info.id,
                workflow_id = execution.workflow_id,
                workflow_run_id = execution.run_id,
                "signal-with-start might have left an orphaned history branch"
            );
            return Err(HistoryError::PersistenceAlreadyStarted(already));
        }
        if state.get_current_version() < already.last_write_version {
            let policy = self
                .start_backend
                .workflow_selection_policy(&domain.info.id, &execution.workflow_id, &already.run_id)
                .await?;
            if policy == start.active_cluster_selection_policy {
                return Err(self.new_domain_not_active_error(domain, already.last_write_version));
            }
        }
        if should_terminate_and_start(reuse, already.state) {
            let old_execution = WorkflowExecution {
                workflow_id: execution.workflow_id.clone(),
                run_id: already.run_id.clone(),
            };
            let (context, release) = self
                .execution_cache
                .get_or_create_workflow_execution(&domain.info.id, &old_execution)
                .await?;
            let result = async {
                let state = context.load_workflow_execution().await?;
                self.terminate_and_start_workflow(context, state, &execution, domain, request, None)
                    .await
            }
            .await;
            release(result.as_ref().err());
            match result {
                Err(error) if error.is_workflow_already_completed() => {
                    tracing::warn!(error = %error, "Workflow completed while trying to terminate, will continue starting workflow");
                }
                other => return other,
            }
        }
        apply_workflow_id_reuse_policy(
            &already.start_request_id,
            &already.run_id,
            already.state,
            already.close_status,
            &execution.workflow_id,
            reuse,
        )?;
        match self
            .start_backend
            .create_workflow(
                &domain.info.id,
                &execution,
                &snapshot,
                &blob,
                CreateWorkflowMode::WorkflowIdReuse,
                &already.run_id,
                already.last_write_version,
            )
            .await
        {
            Ok(()) => Ok(StartWorkflowExecutionResponse {
                run_id: execution.run_id,
            }),
            Err(HistoryError::DuplicateRequest {
                request_type,
                run_id,
            }) if request_type == WorkflowRequestType::Start
                || (signal.is_some() && request_type == WorkflowRequestType::Signal) =>
            {
                Ok(StartWorkflowExecutionResponse { run_id })
            }
            Err(error) => Err(error),
        }
    }

    pub async fn cleanup_start_failure(
        &self,
        domain: &DomainCacheEntry,
        artifacts: &StartArtifacts,
        error: &HistoryError,
    ) {
        let (Some(execution), Some(blob)) = (&artifacts.execution, &artifacts.blob) else {
            return;
        };
        if matches!(error, HistoryError::PersistenceTimeout(_)) {
            tracing::warn!(error = %error, "timeout error detected when creating execution. If the excution was not created successfully this has probably left some orphaned history branch that needs cleaning up");
            self.start_backend.cleanup_metric("halted-timeout");
            return;
        }
        let known = match error {
            HistoryError::CurrentWorkflowConditionFailed(_)
            | HistoryError::ConditionFailed(_)
            | HistoryError::ShardAlreadyExist(_)
            | HistoryError::PersistenceAlreadyStarted(_)
            | HistoryError::PersistenceShardOwnershipLost { .. }
            | HistoryError::DuplicateRequest { .. } => true,
            HistoryError::Cadence(error) => matches!(
                &**error,
                CadenceError::WorkflowAlreadyStarted(_)
                    | CadenceError::ShardOwnershipLost(_)
                    | CadenceError::WorkflowAlreadyCompleted(_)
            ),
            _ => false,
        };
        if !known {
            tracing::warn!(error = %error, "unknown failure detected when creating execution. If the excution was not created successfully this has probably left some orphaned history branch that needs cleaning up");
            self.start_backend.cleanup_metric("unknown");
            return;
        }
        if !self.start_backend.cleanup_enabled(&domain.info.name) {
            tracing::warn!(error = %error, "cleanup of orphaned history branch is disabled, but possible orphaned history branch was detected");
            return;
        }
        if execution.workflow_id.is_empty() || execution.run_id.is_empty() {
            tracing::error!("some parameters are missing in handleCreateWorkflowExecutionFailureCleanup. This is a bug");
            return;
        }
        tracing::debug!("Deleting orphaned history branch during cleanup after identified failure during creation");
        if let Err(error) = self
            .start_backend
            .delete_history_branch(&domain.info.name, &blob.branch_token)
            .await
        {
            tracing::warn!(error = %error, "Failed to cleanup orphaned history branch");
            self.start_backend.cleanup_metric("failure");
            return;
        }
        self.start_backend.cleanup_metric("success");
        if self
            .start_backend
            .record_uninitialized_enabled(&domain.info.name)
        {
            if let Err(error) = self
                .start_backend
                .delete_visibility(domain, execution)
                .await
            {
                tracing::warn!(error = %error, "Failed to delete uninitialized workflow execution record after failed CreateWorkflowExecution");
            }
        }
    }

    pub async fn terminate_and_start_workflow(
        &self,
        context: ExecutionContextRef,
        mut state: MutableStateRef,
        execution: &WorkflowExecution,
        domain: &DomainCacheEntry,
        request: &HistoryStartWorkflowExecutionRequest,
        signal: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let version = state.get_current_version();
        if self
            .cluster_metadata
            .cluster_name_for_failover_version(version)?
            != self.current_cluster_name
        {
            let policy = request
                .start_request
                .as_ref()
                .and_then(|s| s.active_cluster_selection_policy.clone());
            if state.get_execution_info().active_cluster_selection_policy == policy {
                return Err(self.new_domain_not_active_error(domain, version));
            }
            return Err(HistoryError::bad_request("Cannot terminate the existing workflow and start a new workflow because it is active in a different cluster with a different active cluster selection policy."));
        }
        for attempt in 0..5 {
            if !state.is_workflow_execution_running() {
                return Err(super::workflow_mutations::already_completed());
            }
            if let Err(error) = state.terminate_workflow(
                state.get_next_event_id(),
                "TerminateIfRunning Policy",
                &get_terminate_if_running_details(&execution.run_id),
                "history-service",
            ) {
                if matches!(error, HistoryError::StaleState) {
                    context.clear();
                    if attempt != 4 {
                        state = context.load_workflow_execution().await?;
                    }
                    continue;
                }
                return Err(error);
            }
            let new_state = self
                .create_start_mutable_state(domain, &execution.run_id, request)
                .await?;
            self.add_start_events_and_tasks(&*new_state, execution, request, signal)?;
            match context
                .update_workflow_execution_with_new_as_active(
                    self.time_source.now(),
                    &domain.info.id,
                    execution,
                    new_state,
                )
                .await
            {
                Err(error) if is_start_conflict(&error) => {
                    self.start_backend.concurrency_update_failure();
                    continue;
                }
                Err(error) => return Err(error),
                Ok(()) => break,
            }
        }
        // Go returns the new run even when every conditional update conflicts.
        Ok(StartWorkflowExecutionResponse {
            run_id: execution.run_id.clone(),
        })
    }
}
fn is_start_conflict(error: &HistoryError) -> bool {
    matches!(
        error,
        HistoryError::ConditionFailed(_) | HistoryError::CurrentWorkflowConditionFailed(_)
    )
}

impl HistoryEngine {
    pub async fn signal_with_start_workflow_execution(
        &self,
        request: &HistorySignalWithStartWorkflowExecutionRequest,
        deadline: Option<Instant>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let domain = self.domain_cache.get_domain_by_id(&request.domain_uuid)?;
        if domain.info.status != DOMAIN_STATUS_REGISTERED {
            return Err(HistoryError::bad_request("Domain is deprecated."));
        }
        let signal = request
            .signal_with_start_request
            .clone()
            .unwrap_or_default();
        let execution = WorkflowExecution {
            workflow_id: signal.workflow_id.clone(),
            run_id: String::new(),
        };
        let mut previous = None;
        let mut deferred_release = None;
        let result = async {
            match self.execution_cache.get_or_create_workflow_execution(&domain.info.id, &execution).await {
                Ok((context, release)) => {
                    deferred_release = Some(release);
                    let mut exhausted = true;
                    for _ in 0..5 {
                        let state = match context.load_workflow_execution().await {
                            Ok(state) => state,
                            Err(error) if error.is_entity_not_exists() => { exhausted = false; break; }
                            Err(error) => return Err(error),
                        };
                        if state.is_signal_requested(&signal.request_id) {
                            return Ok(StartWorkflowExecutionResponse { run_id: context.get_execution().run_id });
                        }
                        if !state.is_workflow_execution_running() || self.check_for_history_corruptions(&*state).await? {
                            previous = Some(state); exhausted = false; break;
                        }
                        if signal.workflow_id_reuse_policy.is_some_and(|p| p as i32 == 3) {
                            let new_execution = WorkflowExecution { workflow_id: execution.workflow_id.clone(), run_id: uuid::Uuid::new_v4().to_string() };
                            let start = self.get_start_request(&domain.info.id, &signal, request.partition_config.clone())?;
                            match self.terminate_and_start_workflow(context.clone(), state.clone(), &new_execution, &domain, &start, Some(request)).await {
                                Err(error) if error.is_workflow_already_completed() => {},
                                other => return other,
                            }
                        }
                        let info = state.get_execution_info();
                        let max = self.config.maximum_signals_per_execution(&domain.info.name);
                        if max > 0 && info.signal_count >= max {
                            tracing::info!(signal_count = info.signal_count, workflow_id = execution.workflow_id,
                                run_id = execution.run_id, domain_id = domain.info.id, "Execution limit reached for maximum signals");
                            return Err(cadence_api::errors::LimitExceededError { message: "exceeded workflow execution limit for signal events".into() }.into());
                        }
                        if !signal.request_id.is_empty() { state.add_signal_requested(&signal.request_id); }
                        state.add_workflow_execution_signaled(&signal.signal_name, &signal.signal_input, &signal.identity, &signal.request_id)
                            .map_err(|_| HistoryError::internal("Unable to signal workflow execution."))?;
                        if !state.has_pending_decision() && state.has_processed_or_pending_decision() {
                            state.add_decision_task_scheduled_event(false)
                                .map_err(|_| HistoryError::internal("Failed to add decision scheduled event."))?;
                        }
                        tracing::debug!("SignalWithStartWorkflowExecution calling UpdateWorkflowExecutionAsActive for wfID {}", execution.workflow_id);
                        match context.update_workflow_execution_as_active(self.time_source.now()).await {
                            Ok(()) => return Ok(StartWorkflowExecutionResponse { run_id: context.get_execution().run_id }),
                            Err(HistoryError::DuplicateRequest { request_type: WorkflowRequestType::Signal, run_id }) => return Ok(StartWorkflowExecutionResponse { run_id }),
                            Err(error) if is_start_conflict(&error) => continue,
                            Err(error) => return Err(error),
                        }
                    }
                    if exhausted { return Err(HistoryError::MaxAttemptsExceeded); }
                }
                Err(error) if error.is_entity_not_exists() => {},
                Err(error) => return Err(error),
            }
            let mut start = self.get_start_request(&domain.info.id, &signal, request.partition_config.clone())?;
            let mut artifacts = StartArtifacts::default();
            let result = self.start_workflow_helper(&mut start, &domain, previous, Some(request), deadline, &mut artifacts).await;
            if let Err(error) = &result { self.cleanup_start_failure(&domain, &artifacts, error).await; }
            result
        }.await;
        if let Some(release) = deferred_release {
            release(result.as_ref().err());
        }
        result
    }

    pub fn get_start_request(
        &self,
        domain: &str,
        signal: &cadence_api::service_types::SignalWithStartWorkflowExecutionRequest,
        partition: std::collections::HashMap<String, String>,
    ) -> HResult<HistoryStartWorkflowExecutionRequest> {
        self.start_backend.history_start_request(
            domain,
            StartWorkflowExecutionRequest {
                domain: signal.domain.clone(),
                workflow_id: signal.workflow_id.clone(),
                workflow_type: signal.workflow_type.clone(),
                task_list: signal.task_list.clone(),
                input: signal.input.clone(),
                execution_start_to_close_timeout_seconds: signal
                    .execution_start_to_close_timeout_seconds,
                task_start_to_close_timeout_seconds: signal.task_start_to_close_timeout_seconds,
                identity: signal.identity.clone(),
                request_id: signal.request_id.clone(),
                workflow_id_reuse_policy: signal.workflow_id_reuse_policy,
                retry_policy: signal.retry_policy.clone(),
                cron_schedule: signal.cron_schedule.clone(),
                cron_overlap_policy: signal.cron_overlap_policy,
                memo: signal.memo.clone(),
                search_attributes: signal.search_attributes.clone(),
                header: signal.header.clone(),
                delay_start_seconds: signal.delay_start_seconds,
                jitter_start_seconds: signal.jitter_start_seconds,
                first_run_at_timestamp: signal.first_run_at_timestamp,
                active_cluster_selection_policy: signal.active_cluster_selection_policy.clone(),
            },
            partition,
        )
    }
}
