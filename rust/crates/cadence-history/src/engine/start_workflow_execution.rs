//! Port of engineimpl/start_workflow_execution.go (StartWorkflowExecution and
//! SignalWithStartWorkflowExecution).
//!
//! All persistence-touching steps that Go performs through `execution.Context`,
//! `execution.NewMutableStateBuilderWithVersionHistories`, the shard's history
//! manager and the visibility manager are reached through [`WorkflowStartHost`],
//! which the shard factory implements over the real packages.
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::enums::WorkflowIDReusePolicy;
use cadence_api::errors::{BadRequestError, LimitExceededError, ServiceBusyError, WorkflowExecutionAlreadyStartedError};
use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
use cadence_api::types::{ClusterAttribute, StartWorkflowExecutionRequest, StartWorkflowExecutionResponse};
use cadence_api::CadenceError;

use super::notify_tasks::PersistedBlobRef;
use super::{deps::*, runtime::*, types::*, workflow_mutations::already_completed};

/// `contextLockTimeout`
pub const CONTEXT_LOCK_TIMEOUT: Duration = Duration::from_millis(500);
/// `workflow.ConditionalRetryCount`
pub const CONDITIONAL_RETRY_COUNT: usize = 5;
/// `TerminateIfRunningReason`
pub const TERMINATE_IF_RUNNING_REASON: &str = "TerminateIfRunning Policy";
/// `execution.IdentityHistoryService`
pub const IDENTITY_HISTORY_SERVICE: &str = "history-service";

/// `workflow.ErrConcurrentStartRequest`
pub fn err_concurrent_start_request() -> HistoryError {
    ServiceBusyError::new("an outstanding start workflow request is in-progress. Failed to acquire the resource.").into()
}
/// `errClusterAttributeNotFound`
pub fn err_cluster_attribute_not_found() -> HistoryError {
    BadRequestError::new("Cannot start workflow with a cluster attribute that is not found in the domain's metadata.").into()
}
fn err_domain_deprecated() -> HistoryError {
    BadRequestError::new("Domain is deprecated.").into()
}
fn err_signals_limit_exceeded() -> HistoryError {
    LimitExceededError { message: "exceeded workflow execution limit for signal events".into() }.into()
}

/// `getTerminateIfRunningDetails`
pub fn terminate_if_running_details(new_run_id: &str) -> Vec<u8> {
    format!("New runID: {new_run_id}").into_bytes()
}

/// `FailedWorkflowCloseState`
pub(crate) fn is_failed_close_state(close_status: i32) -> bool {
    matches!(
        close_status,
        WORKFLOW_CLOSE_STATUS_FAILED | WORKFLOW_CLOSE_STATUS_CANCELED | WORKFLOW_CLOSE_STATUS_TERMINATED | WORKFLOW_CLOSE_STATUS_TIMED_OUT
    )
}

/// Limits read from `config.Config` by `validateStartWorkflowExecutionRequest` /
/// `overrideTaskStartToCloseTimeoutSeconds` for one domain.
#[derive(Debug, Clone)]
pub struct StartLimits {
    pub max_id_length_warn_limit: usize,
    pub domain_name_max_length: usize,
    pub workflow_id_max_length: usize,
    pub task_list_name_max_length: usize,
    pub workflow_type_max_length: usize,
    pub max_decision_start_to_close_seconds: i32,
}

/// `activecluster.ActiveClusterInfo`
#[derive(Debug, Clone, Default)]
pub struct ActiveClusterInfo {
    pub active_cluster_name: String,
    pub failover_version: i64,
}

/// Snapshot returned by `MutableState.CloseTransactionAsSnapshot` (opaque to the engine).
pub type NewRunSnapshot = Arc<dyn Any + Send + Sync>;

/// Everything `startWorkflowHelper` needs from the shard, execution package,
/// history manager and visibility manager.
#[async_trait]
pub trait WorkflowStartHost: Send + Sync {
    fn shard_id(&self) -> i32;
    fn start_limits(&self, domain: &str) -> StartLimits;
    /// `config.EnableRecordWorkflowExecutionUninitialized(domain)`
    fn enable_record_workflow_execution_uninitialized(&self, domain: &str) -> bool;
    /// `config.EnableCleanupOrphanedHistoryBranchOnWorkflowCreation(domain)`
    fn enable_cleanup_orphaned_history_branch(&self, domain: &str) -> bool;
    /// `e.visibilityMgr != nil`
    fn has_visibility(&self) -> bool;

    /// `ActiveClusterManager.GetActiveClusterInfoByClusterAttribute`; must return
    /// `HistoryError::ClusterAttributeNotFound` for `ClusterAttributeNotFoundError`.
    async fn active_cluster_info_by_cluster_attribute(&self, domain_id: &str, attribute: Option<&ClusterAttribute>) -> HResult<ActiveClusterInfo>;
    /// `ActiveClusterManager.GetActiveClusterSelectionPolicyForWorkflow`
    async fn active_cluster_selection_policy_for_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<Option<cadence_api::types::ActiveClusterSelectionPolicy>>;

    /// `execution.NewMutableStateBuilderWithVersionHistories` + `SetHistoryTree(runID)`
    fn new_mutable_state(&self, domain: &DomainCacheEntry, failover_version: i64, run_id: &str) -> HResult<MutableStateRef>;
    /// `execution.NewContext(domainID, execution, shard, executionManager, logger)`
    fn new_execution_context(&self, domain_id: &str, execution: &WorkflowExecution) -> ExecutionContextRef;
    /// `MutableState.CloseTransactionAsSnapshot(now, TransactionPolicyActive)`
    fn close_transaction_as_snapshot(&self, state: &MutableStateRef, now: i64) -> HResult<NewRunSnapshot>;
    /// `Context.PersistStartWorkflowBatchEvents(ctx, newWorkflowEventsSeq[0])`
    async fn persist_start_workflow_batch_events(&self, context: &ExecutionContextRef, snapshot: &NewRunSnapshot) -> HResult<PersistedBlobRef>;
    /// `Context.CreateWorkflowExecution`
    #[allow(clippy::too_many_arguments)]
    async fn create_workflow_execution(
        &self,
        context: &ExecutionContextRef,
        snapshot: &NewRunSnapshot,
        history_blob: &PersistedBlobRef,
        mode: CreateWorkflowMode,
        prev_run_id: &str,
        prev_last_write_version: i64,
        request_mode: CreateWorkflowRequestMode,
    ) -> HResult<()>;
    /// `Context.UpdateWorkflowExecutionWithNewAsActive`
    async fn update_workflow_execution_with_new_as_active(
        &self,
        running: &ExecutionContextRef,
        now: i64,
        new_context: &ExecutionContextRef,
        new_state: &MutableStateRef,
    ) -> HResult<()>;
    /// `shard.GetHistoryManager().DeleteHistoryBranch`
    async fn delete_history_branch(&self, branch_token: &[u8], domain_name: &str) -> HResult<()>;
    /// `visibilityMgr.RecordWorkflowExecutionUninitialized`
    async fn record_workflow_execution_uninitialized(&self, domain_id: &str, domain: &str, execution: &WorkflowExecution, workflow_type_name: &str, update_timestamp: i64) -> HResult<()>;
    /// `visibilityMgr.DeleteWorkflowExecution`
    async fn delete_visibility_workflow_execution(&self, domain_id: &str, domain: &str, execution: &WorkflowExecution) -> HResult<()>;
}

/// `signalWithStartArg`
struct SignalWithStartArg {
    request: HistorySignalWithStartWorkflowExecutionRequest,
    prev_mutable_state: Option<MutableStateRef>,
}

/// What `startWorkflowHelper` returned besides the response (workflowExecution, historyBlob).
#[derive(Default)]
struct StartTrace {
    execution: Option<WorkflowExecution>,
    blob: Option<PersistedBlobRef>,
}

fn now_wall_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// `persistence.AsDuplicateRequestError` handling shared by both create attempts.
fn duplicate_outcome(err: &HistoryError, is_signal_with_start: bool) -> Option<HResult<StartWorkflowExecutionResponse>> {
    if let HistoryError::DuplicateRequest { request_type, run_id } = err {
        if *request_type == WorkflowRequestType::Start || (is_signal_with_start && *request_type == WorkflowRequestType::Signal) {
            return Some(Ok(StartWorkflowExecutionResponse { run_id: run_id.clone() }));
        }
        tracing::error!(request_type = ?request_type, "A bug is detected for idempotency improvement");
        return Some(Err(HistoryError::DuplicateRequest { request_type: *request_type, run_id: run_id.clone() }));
    }
    None
}

/// `getWorkflowAlreadyStartedError`
fn workflow_already_started_error(msg: &str, create_request_id: &str, workflow_id: &str, run_id: &str) -> HistoryError {
    WorkflowExecutionAlreadyStartedError {
        message: msg.replacen("{}", workflow_id, 1).replacen("{}", run_id, 1),
        start_request_id: create_request_id.to_string(),
        run_id: run_id.to_string(),
    }
    .into()
}

/// `getStartRequest`
pub fn get_start_request(
    domain_id: &str,
    request: &SignalWithStartWorkflowExecutionRequest,
    partition_config: &std::collections::HashMap<String, String>,
) -> HResult<HistoryStartWorkflowExecutionRequest> {
    let req = StartWorkflowExecutionRequest {
        domain: request.domain.clone(),
        workflow_id: request.workflow_id.clone(),
        workflow_type: request.workflow_type.clone(),
        task_list: request.task_list.clone(),
        input: request.input.clone(),
        execution_start_to_close_timeout_seconds: request.execution_start_to_close_timeout_seconds,
        task_start_to_close_timeout_seconds: request.task_start_to_close_timeout_seconds,
        identity: request.identity.clone(),
        request_id: request.request_id.clone(),
        workflow_id_reuse_policy: request.workflow_id_reuse_policy,
        retry_policy: request.retry_policy.clone(),
        cron_schedule: request.cron_schedule.clone(),
        cron_overlap_policy: request.cron_overlap_policy,
        memo: request.memo.clone(),
        search_attributes: request.search_attributes.clone(),
        header: request.header.clone(),
        delay_start_seconds: request.delay_start_seconds,
        jitter_start_seconds: request.jitter_start_seconds,
        first_run_at_timestamp: request.first_run_at_timestamp,
        active_cluster_selection_policy: request.active_cluster_selection_policy.clone(),
    };
    create_history_start_request(domain_id, req, now_wall_nanos(), partition_config)
}

/// `common.CreateHistoryStartWorkflowRequest`
pub fn create_history_start_request(
    domain_id: &str,
    start_request: StartWorkflowExecutionRequest,
    now_nanos: i64,
    partition_config: &std::collections::HashMap<String, String>,
) -> HResult<HistoryStartWorkflowExecutionRequest> {
    use rand::Rng;
    let jitter_seconds = start_request.jitter_start_seconds.unwrap_or(0);
    let jitter = if jitter_seconds > 0 {
        Duration::from_secs(rand::thread_rng().gen_range(0..=jitter_seconds as u64))
    } else {
        Duration::ZERO
    };
    let built = cadence_api::history_request::create_history_start_workflow_request(&start_request, now_nanos, jitter)
        .map_err(HistoryError::bad_request)?;
    Ok(HistoryStartWorkflowExecutionRequest {
        domain_uuid: domain_id.to_string(),
        start_request: Some(start_request),
        partition_config: partition_config.clone(),
        first_decision_task_backoff_seconds: Some(built.first_decision_task_backoff_seconds),
        expiration_timestamp: built.expiration_timestamp,
        ..Default::default()
    })
}

/// `shouldTerminateAndStart`
pub(crate) fn should_terminate_and_start(start_request: &HistoryStartWorkflowExecutionRequest, state: i32) -> bool {
    start_request.start_request.as_ref().and_then(|r| r.workflow_id_reuse_policy) == Some(WorkflowIDReusePolicy::TerminateIfRunning)
        && (state == WORKFLOW_STATE_CREATED || state == WORKFLOW_STATE_RUNNING)
}

/// `applyWorkflowIDReusePolicyHelper`
pub fn apply_workflow_id_reuse_policy_helper(
    prev_start_request_id: &str,
    prev_run_id: &str,
    prev_state: i32,
    prev_close_state: i32,
    execution: &WorkflowExecution,
    policy: WorkflowIDReusePolicy,
) -> HResult<()> {
    match prev_state {
        WORKFLOW_STATE_CREATED | WORKFLOW_STATE_RUNNING => {
            return Err(workflow_already_started_error(
                "Workflow execution is already running. WorkflowId: {}, RunId: {}.",
                prev_start_request_id,
                &execution.workflow_id,
                prev_run_id,
            ));
        }
        WORKFLOW_STATE_COMPLETED => {}
        // ignore workflow ID reuse policy for corrupted workflows, treat as they do not exist
        WORKFLOW_STATE_CORRUPTED => return Ok(()),
        _ => {
            return Err(HistoryError::internal(format!("Failed to process workflow, workflow has invalid state: {prev_state}.")));
        }
    }
    match policy {
        WorkflowIDReusePolicy::AllowDuplicateFailedOnly => {
            if !is_failed_close_state(prev_close_state) {
                return Err(workflow_already_started_error(
                    "Workflow execution already finished successfully. WorkflowId: {}, RunId: {}. Workflow ID reuse policy: allow duplicate workflow ID if last run failed.",
                    prev_start_request_id,
                    &execution.workflow_id,
                    prev_run_id,
                ));
            }
        }
        WorkflowIDReusePolicy::AllowDuplicate | WorkflowIDReusePolicy::TerminateIfRunning => {}
        WorkflowIDReusePolicy::RejectDuplicate => {
            return Err(workflow_already_started_error(
                "Workflow execution already finished. WorkflowId: {}, RunId: {}. Workflow ID reuse policy: reject duplicate workflow ID.",
                prev_start_request_id,
                &execution.workflow_id,
                prev_run_id,
            ));
        }
        #[allow(unreachable_patterns)]
        _ => return Err(HistoryError::internal("Failed to process start workflow reuse policy.")),
    }
    Ok(())
}

/// `applyWorkflowIDReusePolicyForSigWithStart`
pub fn apply_workflow_id_reuse_policy_for_sig_with_start(
    prev: &WorkflowExecutionInfo,
    execution: &WorkflowExecution,
    policy: WorkflowIDReusePolicy,
) -> HResult<()> {
    apply_workflow_id_reuse_policy_helper(&prev.create_request_id, &prev.run_id, prev.state, prev.close_status, execution, policy)
}

impl HistoryEngine {
    /// `StartWorkflowExecution`
    pub async fn start_workflow_execution(&self, request: &HistoryStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse> {
        let domain_entry = self.domain_cache.get_domain_by_id(&request.domain_uuid)?;
        let mut start_request = request.clone();
        let mut trace = StartTrace::default();
        let result = self.start_workflow_helper(&mut start_request, &domain_entry, None, &mut trace).await;
        match result {
            Ok(resp) => Ok(resp),
            Err(err) => {
                self.handle_create_workflow_execution_failure_cleanup(&domain_entry, trace.execution.as_ref(), trace.blob.as_ref(), false, &err).await;
                Err(err)
            }
        }
    }

    /// `startWorkflowHelper`; the lock on the current-execution record is released
    /// with the final result, as Go's deferred `currentRelease(retError)` does.
    async fn start_workflow_helper(
        &self,
        start_request: &mut HistoryStartWorkflowExecutionRequest,
        domain_entry: &Arc<DomainCacheEntry>,
        sig_with_start: Option<SignalWithStartArg>,
        trace: &mut StartTrace,
    ) -> HResult<StartWorkflowExecutionResponse> {
        if domain_entry.info.status != DOMAIN_STATUS_REGISTERED {
            return Err(err_domain_deprecated());
        }
        let request = start_request.start_request.clone().unwrap_or_default();
        self.validate_start_workflow_execution_request(&request)?;
        self.override_task_start_to_close_timeout_seconds(domain_entry, start_request);

        let workflow_id = request.workflow_id.clone();
        let domain_id = domain_entry.info.id.clone();

        // grab the current context as a lock, nothing more; use a smaller timeout to get the lock
        let current_release = match tokio::time::timeout(
            CONTEXT_LOCK_TIMEOUT,
            self.execution_cache.get_or_create_current_workflow_execution(&domain_id, &workflow_id),
        )
        .await
        {
            Err(_) => return Err(err_concurrent_start_request()),
            Ok(Err(HistoryError::DeadlineExceeded)) => return Err(err_concurrent_start_request()),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(release)) => release,
        };
        let mut deferred_releases: Vec<ReleaseFunc> = Vec::new();
        let result = self
            .start_workflow_helper_locked(start_request, domain_entry, sig_with_start, trace, &mut deferred_releases)
            .await;
        let err_ref = result.as_ref().err();
        for release in deferred_releases.iter().rev() {
            release(err_ref);
        }
        current_release(err_ref);
        result
    }

    async fn start_workflow_helper_locked(
        &self,
        start_request: &mut HistoryStartWorkflowExecutionRequest,
        domain_entry: &Arc<DomainCacheEntry>,
        sig_with_start: Option<SignalWithStartArg>,
        trace: &mut StartTrace,
        deferred_releases: &mut Vec<ReleaseFunc>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let host = self.start_host.clone();
        let request = start_request.start_request.clone().unwrap_or_default();
        let workflow_id = request.workflow_id.clone();
        let domain_id = domain_entry.info.id.clone();
        let domain = domain_entry.info.name.clone();

        let workflow_execution = WorkflowExecution { workflow_id: workflow_id.clone(), run_id: uuid::Uuid::new_v4().to_string() };
        let cur_state = self.create_mutable_state(domain_entry, &workflow_execution.run_id, start_request).await?;

        // preprocess for signalWithStart
        let is_signal_with_start = sig_with_start.is_some();
        let (prev_state, signal_with_start_request) = match sig_with_start {
            Some(arg) => (arg.prev_mutable_state, Some(arg.request)),
            None => (None, None),
        };
        let enable_uninitialized = host.enable_record_workflow_execution_uninitialized(&domain) && host.has_visibility();
        if let Some(prev) = &prev_state {
            let prev_last_write_version = prev.get_last_write_version()?;
            if prev_last_write_version > cur_state.get_current_version() {
                let policy = host
                    .active_cluster_selection_policy_for_workflow(&domain_id, &workflow_id, &prev.get_execution_info().run_id)
                    .await?;
                if policy == request.active_cluster_selection_policy {
                    return Err(self.new_domain_not_active_error(domain_entry, prev_last_write_version));
                }
            }
            apply_workflow_id_reuse_policy_for_sig_with_start(
                &prev.get_execution_info(),
                &workflow_execution,
                request.workflow_id_reuse_policy.unwrap_or(WorkflowIDReusePolicy::AllowDuplicateFailedOnly),
            )?;
        } else if enable_uninitialized {
            let wf_type = request.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default();
            if let Err(e) = host
                .record_workflow_execution_uninitialized(&domain_id, &domain, &workflow_execution, &wf_type, self.time_source.now())
                .await
            {
                tracing::error!(error = %e, "Failed to record uninitialized workflow execution");
            }
        }
        trace.execution = Some(workflow_execution.clone());
        if let Err(err) = self
            .add_start_events_and_tasks(&cur_state, &workflow_execution, start_request, signal_with_start_request.as_ref())
        {
            if host.enable_record_workflow_execution_uninitialized(&domain) && host.has_visibility() {
                // delete the uninitialized workflow execution record since it failed to start the workflow
                if let Err(e) = host.delete_visibility_workflow_execution(&domain_id, &domain, &workflow_execution).await {
                    tracing::error!(error = %e, "Failed to delete uninitialized workflow execution record");
                }
            }
            return Err(err);
        }
        let wf_context = host.new_execution_context(&domain_id, &workflow_execution);

        let new_workflow = host.close_transaction_as_snapshot(&cur_state, self.time_source.now())?;
        let blob = host.persist_start_workflow_batch_events(&wf_context, &new_workflow).await?;
        trace.blob = Some(blob.clone());

        // create as brand new
        let mut create_mode = CreateWorkflowMode::BrandNew;
        let mut prev_run_id = String::new();
        let mut prev_last_write_version = 0i64;
        // overwrite in case of signalWithStart
        if let Some(prev) = &prev_state {
            create_mode = CreateWorkflowMode::WorkflowIdReuse;
            let info = prev.get_execution_info();
            // For corrupted workflows use ContinueAsNew mode.
            if info.state == WORKFLOW_STATE_CORRUPTED {
                create_mode = CreateWorkflowMode::ContinueAsNew;
            }
            prev_run_id = info.run_id.clone();
            prev_last_write_version = prev.get_last_write_version()?;
        }
        let created = host
            .create_workflow_execution(&wf_context, &new_workflow, &blob, create_mode, &prev_run_id, prev_last_write_version, CreateWorkflowRequestMode::New)
            .await;
        let created = match created {
            Err(e) => {
                if let Some(outcome) = duplicate_outcome(&e, is_signal_with_start) {
                    return outcome;
                }
                Err(e)
            }
            ok => ok,
        };
        // handle already started error
        let created = match created {
            Err(HistoryError::PersistenceAlreadyStarted(t)) => {
                let t = *t;
                let original = HistoryError::PersistenceAlreadyStarted(Box::new(t.clone()));
                if t.start_request_id == request.request_id {
                    return Ok(StartWorkflowExecutionResponse { run_id: t.run_id });
                }
                if is_signal_with_start {
                    tracing::warn!(
                        domain_id = %domain_id,
                        workflow_id = %workflow_execution.workflow_id,
                        run_id = %workflow_execution.run_id,
                        "signal-with-start might have left an orphaned history branch"
                    );
                    return Err(original);
                }
                if cur_state.get_current_version() < t.last_write_version {
                    let policy = host
                        .active_cluster_selection_policy_for_workflow(&domain_id, &workflow_id, &t.run_id)
                        .await?;
                    if policy == request.active_cluster_selection_policy {
                        return Err(self.new_domain_not_active_error(domain_entry, t.last_write_version));
                    }
                }
                let prev_run_id = t.run_id.clone();
                if should_terminate_and_start(start_request, t.state) {
                    let (running_ctx, release) = self
                        .execution_cache
                        .get_or_create_workflow_execution(&domain_id, &WorkflowExecution { workflow_id: workflow_id.clone(), run_id: prev_run_id.clone() })
                        .await?;
                    // workflow.LoadOnce
                    let running_state = match running_ctx.load_workflow_execution().await {
                        Ok(s) => s,
                        Err(e) => {
                            release(Some(&e));
                            return Err(e);
                        }
                    };
                    deferred_releases.push(release);
                    match self
                        .terminate_and_start_workflow(&running_ctx, running_state, &workflow_execution, domain_entry, &domain_id, Some(start_request.clone()), None)
                        .await
                    {
                        // By the time we try to terminate the workflow, it was already terminated;
                        // continue as if we didn't need to terminate it in the first place.
                        Err(e) if e.is_workflow_already_completed() => {
                            tracing::warn!(error = %e, "Workflow completed while trying to terminate, will continue starting workflow");
                        }
                        other => return other,
                    }
                }
                apply_workflow_id_reuse_policy_helper(
                    &t.start_request_id,
                    &prev_run_id,
                    t.state,
                    t.close_status,
                    &workflow_execution,
                    request.workflow_id_reuse_policy.unwrap_or(WorkflowIDReusePolicy::AllowDuplicateFailedOnly),
                )?;
                // create as ID reuse
                let second = host
                    .create_workflow_execution(
                        &wf_context,
                        &new_workflow,
                        &blob,
                        CreateWorkflowMode::WorkflowIdReuse,
                        &prev_run_id,
                        t.last_write_version,
                        CreateWorkflowRequestMode::New,
                    )
                    .await;
                if let Err(e) = &second {
                    if let Some(outcome) = duplicate_outcome(e, is_signal_with_start) {
                        return outcome;
                    }
                }
                second
            }
            other => other,
        };
        created?;
        Ok(StartWorkflowExecutionResponse { run_id: workflow_execution.run_id })
    }

    /// `handleCreateWorkflowExecutionFailureCleanup`
    async fn handle_create_workflow_execution_failure_cleanup(
        &self,
        domain_entry: &DomainCacheEntry,
        workflow_execution: Option<&WorkflowExecution>,
        history_blob: Option<&PersistedBlobRef>,
        is_signal_with_start: bool,
        err: &HistoryError,
    ) {
        let (Some(workflow_execution), Some(history_blob)) = (workflow_execution, history_blob) else {
            // expected behaviour, errors caused by validation will not have a workflow execution
            return;
        };
        let host = &self.start_host;
        let domain_id = &domain_entry.info.id;
        let domain = &domain_entry.info.name;
        // we cannot know if the workflow has succeeded, so the safest thing to do is nothing.
        if err.is_timeout() {
            tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id, error = %err,
                "timeout error detected when creating execution. If the excution was not created successfully this has probably left some orphaned history branch that needs cleaning up");
            return;
        }
        let is_known_failure_requiring_cleanup = match err {
            HistoryError::Cadence(e) => matches!(
                &**e,
                CadenceError::WorkflowAlreadyStarted(_) | CadenceError::ShardOwnershipLost(_) | CadenceError::WorkflowAlreadyCompleted(_)
            ),
            HistoryError::CurrentWorkflowConditionFailed(_)
            | HistoryError::ConditionFailed(_)
            | HistoryError::ShardAlreadyExist(_)
            | HistoryError::PersistenceAlreadyStarted(_)
            | HistoryError::PersistenceShardOwnershipLost(_)
            | HistoryError::DuplicateRequest { .. } => true,
            _ => false,
        };
        if !is_known_failure_requiring_cleanup {
            tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id, error = %err,
                "unknown failure detected when creating execution. If the excution was not created successfully this has probably left some orphaned history branch that needs cleaning up");
            return;
        }
        if !host.enable_cleanup_orphaned_history_branch(domain) {
            tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id,
                is_signal_with_start, error = %err,
                "cleanup of orphaned history branch is disabled, but possible orphaned history branch was detected");
            return;
        }
        if workflow_execution.workflow_id.is_empty() || workflow_execution.run_id.is_empty() {
            tracing::error!(domain_id = %domain_id, error = %err, "some parameters are missing in handleCreateWorkflowExecutionFailureCleanup. This is a bug");
            return;
        }
        tracing::debug!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id, is_signal_with_start, error = %err,
            "Deleting orphaned history branch during cleanup after identified failure during creation");
        if let Err(cleanup_err) = host.delete_history_branch(&history_blob.branch_token, domain).await {
            tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id, error = %cleanup_err,
                "Failed to cleanup orphaned history branch");
            return;
        }
        if host.enable_record_workflow_execution_uninitialized(domain) && host.has_visibility() {
            if let Err(e) = host.delete_visibility_workflow_execution(domain_id, domain, workflow_execution).await {
                tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_execution.workflow_id, run_id = %workflow_execution.run_id, error = %e,
                    "Failed to delete uninitialized workflow execution record after failed CreateWorkflowExecution");
            }
        }
    }

    /// `SignalWithStartWorkflowExecution`
    pub async fn signal_with_start_workflow_execution(&self, request: &HistorySignalWithStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse> {
        let domain_entry = self.domain_cache.get_domain_by_id(&request.domain_uuid)?;
        if domain_entry.info.status != DOMAIN_STATUS_REGISTERED {
            return Err(err_domain_deprecated());
        }
        let mut held_release: Option<ReleaseFunc> = None;
        let result = self.signal_with_start_inner(request, &domain_entry, &mut held_release).await;
        if let Some(release) = held_release {
            release(result.as_ref().err());
        }
        result
    }

    async fn signal_with_start_inner(
        &self,
        request: &HistorySignalWithStartWorkflowExecutionRequest,
        domain_entry: &Arc<DomainCacheEntry>,
        held_release: &mut Option<ReleaseFunc>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let domain_id = domain_entry.info.id.clone();
        let s_request = request.signal_with_start_request.clone().unwrap_or_default();
        let mut workflow_execution = WorkflowExecution { workflow_id: s_request.workflow_id.clone(), run_id: String::new() };

        let mut prev_mutable_state: Option<MutableStateRef> = None;
        let mut attempt = 0usize;

        match self.execution_cache.get_or_create_workflow_execution(&domain_id, &workflow_execution).await {
            Ok((wf_context, release)) => {
                *held_release = Some(release);
                // Just_Signal_Loop
                while attempt < CONDITIONAL_RETRY_COUNT {
                    // workflow not exist, will create workflow then signal
                    let mutable_state = match wf_context.load_workflow_execution().await {
                        Ok(ms) => ms,
                        Err(e) if e.is_entity_not_exists() => break,
                        Err(e) => return Err(e),
                    };
                    if mutable_state.is_signal_requested(&s_request.request_id) {
                        return Ok(StartWorkflowExecutionResponse { run_id: wf_context.get_execution().run_id });
                    }
                    // workflow exist but not running, will restart workflow then signal
                    if !mutable_state.is_workflow_execution_running() {
                        prev_mutable_state = Some(mutable_state);
                        break;
                    }
                    // workflow exists but history is corrupted, will restart workflow then signal
                    if self.check_for_history_corruptions(&*mutable_state).await? {
                        prev_mutable_state = Some(mutable_state);
                        break;
                    }
                    // workflow is running, if policy is TerminateIfRunning, terminate current run then signalWithStart
                    if s_request.workflow_id_reuse_policy == Some(WorkflowIDReusePolicy::TerminateIfRunning) {
                        workflow_execution.run_id = uuid::Uuid::new_v4().to_string();
                        let term = self
                            .terminate_and_start_workflow(&wf_context, mutable_state.clone(), &workflow_execution, domain_entry, &domain_id, None, Some(request))
                            .await;
                        // By the time we try to terminate the workflow, it was already terminated
                        // So continue as if we didn't need to terminate it in the first place
                        match term {
                            Err(e) if e.is_workflow_already_completed() => {}
                            other => return other,
                        }
                    }

                    let execution_info = mutable_state.get_execution_info();
                    let max_allowed_signals = self.config.maximum_signals_per_execution(&domain_entry.info.name);
                    if max_allowed_signals > 0 && execution_info.signal_count >= max_allowed_signals {
                        tracing::info!(signal_count = execution_info.signal_count, workflow_id = %workflow_execution.workflow_id,
                            run_id = %workflow_execution.run_id, domain_id = %domain_id, "Execution limit reached for maximum signals");
                        return Err(err_signals_limit_exceeded());
                    }
                    if !s_request.request_id.is_empty() {
                        mutable_state.add_signal_requested(&s_request.request_id);
                    }
                    if mutable_state
                        .add_workflow_execution_signaled(&s_request.signal_name, &s_request.signal_input, &s_request.identity, &s_request.request_id)
                        .is_err()
                    {
                        return Err(HistoryError::internal("Unable to signal workflow execution."));
                    }
                    // Create a transfer task to schedule a decision task.
                    // Do not schedule if the workflow hasn't processed its first decision yet
                    // (e.g. waiting for DelayStart or Cron timer)
                    if !mutable_state.has_pending_decision()
                        && mutable_state.has_processed_or_pending_decision()
                        && mutable_state.add_decision_task_scheduled_event(false).is_err()
                    {
                        return Err(HistoryError::internal("Failed to add decision scheduled event."));
                    }
                    // We apply the update to execution using optimistic concurrency. If it fails due to a conflict then reload
                    // the history and try the operation again.
                    tracing::debug!(workflow_id = %workflow_execution.workflow_id, "SignalWithStartWorkflowExecution calling UpdateWorkflowExecutionAsActive");
                    match wf_context.update_workflow_execution_as_active(self.time_source.now()).await {
                        Ok(()) => return Ok(StartWorkflowExecutionResponse { run_id: wf_context.get_execution().run_id }),
                        Err(HistoryError::DuplicateRequest { request_type, run_id }) => {
                            if request_type == WorkflowRequestType::Signal {
                                return Ok(StartWorkflowExecutionResponse { run_id });
                            }
                            tracing::error!(request_type = ?request_type, "A bug is detected for idempotency improvement");
                            return Err(HistoryError::DuplicateRequest { request_type, run_id });
                        }
                        Err(e) if e.is_conflict_error() => {
                            attempt += 1;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                if attempt == CONDITIONAL_RETRY_COUNT {
                    return Err(HistoryError::MaxAttemptsExceeded);
                }
            }
            Err(e) => {
                if !e.is_entity_not_exists() {
                    return Err(e);
                }
                // workflow not exist, will create workflow then signal
            }
        }

        // Start workflow and signal
        let mut start_request = get_start_request(&domain_id, &s_request, &request.partition_config)?;
        let arg = SignalWithStartArg { request: request.clone(), prev_mutable_state };
        let mut trace = StartTrace::default();
        match self.start_workflow_helper(&mut start_request, domain_entry, Some(arg), &mut trace).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                self.handle_create_workflow_execution_failure_cleanup(domain_entry, trace.execution.as_ref(), trace.blob.as_ref(), true, &err).await;
                Err(err)
            }
        }
    }

    /// `validateStartWorkflowExecutionRequest`
    pub fn validate_start_workflow_execution_request(&self, request: &StartWorkflowExecutionRequest) -> HResult<()> {
        if request.request_id.is_empty() {
            return Err(HistoryError::bad_request("Missing request ID."));
        }
        if !matches!(request.execution_start_to_close_timeout_seconds, Some(v) if v > 0) {
            return Err(HistoryError::bad_request("Missing or invalid ExecutionStartToCloseTimeoutSeconds."));
        }
        if !matches!(request.task_start_to_close_timeout_seconds, Some(v) if v > 0) {
            return Err(HistoryError::bad_request("Missing or invalid TaskStartToCloseTimeoutSeconds."));
        }
        let task_list_name = request.task_list.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        if task_list_name.is_empty() {
            return Err(HistoryError::bad_request("Missing Tasklist."));
        }
        let workflow_type_name = request.workflow_type.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        if workflow_type_name.is_empty() {
            return Err(HistoryError::bad_request("Missing WorkflowType."));
        }
        let limits = self.start_host.start_limits(&request.domain);
        let check = |id: &str, error_limit: usize, id_type: &str, msg: &str| -> HResult<()> {
            let (valid, exceeded_warn) = super::common_util::is_valid_id_length(id, limits.max_id_length_warn_limit, error_limit);
            if exceeded_warn {
                tracing::warn!(domain = %request.domain, id_type, "ID length exceeds warn limit");
            }
            if valid { Ok(()) } else { Err(HistoryError::bad_request(msg)) }
        };
        check(&request.domain, limits.domain_name_max_length, "domain-name", "Domain exceeds length limit.")?;
        check(&request.workflow_id, limits.workflow_id_max_length, "workflow-id", "WorkflowId exceeds length limit.")?;
        check(task_list_name, limits.task_list_name_max_length, "task-list-name", "TaskList exceeds length limit.")?;
        check(workflow_type_name, limits.workflow_type_max_length, "workflow-type", "WorkflowType exceeds length limit.")?;
        cadence_api::validation::validate_retry_policy(request.retry_policy.as_ref())?;
        Ok(())
    }

    /// `overrideTaskStartToCloseTimeoutSeconds`
    fn override_task_start_to_close_timeout_seconds(&self, domain_entry: &DomainCacheEntry, start_request: &mut HistoryStartWorkflowExecutionRequest) {
        let Some(request) = start_request.start_request.as_mut() else { return };
        let limits = self.start_host.start_limits(&domain_entry.info.name);
        let current = request.task_start_to_close_timeout_seconds.unwrap_or(0);
        let mut secs = current.min(limits.max_decision_start_to_close_seconds);
        secs = secs.min(request.execution_start_to_close_timeout_seconds.unwrap_or(0));
        if secs != current {
            request.task_start_to_close_timeout_seconds = Some(secs);
        }
    }

    /// `terminateAndStartWorkflow`: terminate the running workflow then start a new run in one transaction.
    #[allow(clippy::too_many_arguments)]
    async fn terminate_and_start_workflow(
        &self,
        running_ctx: &ExecutionContextRef,
        mut running_state: MutableStateRef,
        workflow_execution: &WorkflowExecution,
        domain_entry: &Arc<DomainCacheEntry>,
        domain_id: &str,
        start_request: Option<HistoryStartWorkflowExecutionRequest>,
        signal_with_start_request: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
    ) -> HResult<StartWorkflowExecutionResponse> {
        let start_request = match signal_with_start_request {
            Some(sws) => get_start_request(domain_id, &sws.signal_with_start_request.clone().unwrap_or_default(), &sws.partition_config)?,
            None => start_request.unwrap_or_default(),
        };
        let active_cluster = self.cluster_metadata.cluster_name_for_failover_version(running_state.get_current_version())?;
        if active_cluster != self.current_cluster_name {
            let running_policy = running_state.get_execution_info().active_cluster_selection_policy;
            let new_policy = start_request.start_request.as_ref().and_then(|r| r.active_cluster_selection_policy.clone());
            if running_policy == new_policy {
                return Err(self.new_domain_not_active_error(domain_entry, running_state.get_current_version()));
            }
            // TODO(active-active): short-term fix, see Go comment.
            return Err(HistoryError::bad_request(
                "Cannot terminate the existing workflow and start a new workflow because it is active in a different cluster with a different active cluster selection policy.",
            ));
        }
        for attempt in 0..CONDITIONAL_RETRY_COUNT {
            if !running_state.is_workflow_execution_running() {
                return Err(already_completed());
            }
            if let Err(err) = running_state.terminate_workflow(
                running_state.get_next_event_id(),
                TERMINATE_IF_RUNNING_REASON,
                &terminate_if_running_details(&workflow_execution.run_id),
                IDENTITY_HISTORY_SERVICE,
            ) {
                if matches!(err, HistoryError::StaleState) {
                    // Handler detected that cached workflow mutable could potentially be stale; reload.
                    running_ctx.clear();
                    if attempt != CONDITIONAL_RETRY_COUNT - 1 {
                        running_state = running_ctx.load_workflow_execution().await?;
                    }
                    continue;
                }
                return Err(err);
            }

            // new mutable state
            let new_state = self.create_mutable_state(domain_entry, &workflow_execution.run_id, &start_request).await?;
            self.add_start_events_and_tasks(&new_state, workflow_execution, &start_request, signal_with_start_request)?;
            let new_context = self.start_host.new_execution_context(domain_id, workflow_execution);
            match self
                .start_host
                .update_workflow_execution_with_new_as_active(running_ctx, self.time_source.now(), &new_context, &new_state)
                .await
            {
                Ok(()) => break,
                Err(e) if e.is_conflict_error() => {
                    tracing::debug!("concurrency update failure while terminating and starting workflow");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(StartWorkflowExecutionResponse { run_id: workflow_execution.run_id.clone() })
    }

    /// `addStartEventsAndTasks`
    fn add_start_events_and_tasks(
        &self,
        mutable_state: &MutableStateRef,
        workflow_execution: &WorkflowExecution,
        start_request: &HistoryStartWorkflowExecutionRequest,
        signal_with_start_request: Option<&HistorySignalWithStartWorkflowExecutionRequest>,
    ) -> HResult<()> {
        // Add WF start event
        let start_event = mutable_state
            .add_workflow_execution_started_event(workflow_execution, start_request)
            .map_err(|_| HistoryError::internal("Failed to add workflow execution started event."))?;
        if let Some(sws) = signal_with_start_request {
            // Add signal event
            let s_request = sws.signal_with_start_request.clone().unwrap_or_default();
            if !s_request.request_id.is_empty() {
                mutable_state.add_signal_requested(&s_request.request_id);
            }
            mutable_state
                .add_workflow_execution_signaled(&s_request.signal_name, &s_request.signal_input, &s_request.identity, &s_request.request_id)
                .map_err(|_| HistoryError::internal("Failed to add workflow execution signaled event."))?;
        }
        // Generate first decision task event if not child WF and no first decision task backoff
        self.generate_first_decision_task(mutable_state, start_request.parent_execution_info.is_some(), &start_event)
    }

    /// `generateFirstDecisionTask`
    fn generate_first_decision_task(&self, mutable_state: &MutableStateRef, has_parent: bool, start_event: &HistoryEvent) -> HResult<()> {
        if !has_parent {
            // DecisionTask is only created when it is not a Child Workflow and no backoff is needed
            mutable_state.add_first_decision_task_scheduled(start_event)?;
        }
        Ok(())
    }

    /// `createMutableState`
    async fn create_mutable_state(
        &self,
        domain_entry: &DomainCacheEntry,
        run_id: &str,
        start_request: &HistoryStartWorkflowExecutionRequest,
    ) -> HResult<MutableStateRef> {
        let attribute = start_request
            .start_request
            .as_ref()
            .and_then(|r| r.active_cluster_selection_policy.as_ref())
            .and_then(|p| p.cluster_attribute.as_ref());
        let info = match self.start_host.active_cluster_info_by_cluster_attribute(&domain_entry.info.id, attribute).await {
            Ok(info) => info,
            Err(HistoryError::ClusterAttributeNotFound(msg)) => {
                tracing::warn!(error = %msg, "Failed to get active cluster info by cluster attribute, falling back to domain-level active cluster info");
                return Err(err_cluster_attribute_not_found());
            }
            Err(e) => return Err(e),
        };
        if info.active_cluster_name != self.current_cluster_name {
            return Err(self.new_domain_not_active_error(domain_entry, info.failover_version));
        }
        self.start_host.new_mutable_state(domain_entry, info.failover_version, run_id)
    }
}
