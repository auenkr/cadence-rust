//! Port of `go/service/history/task/task_util.go`.

use std::sync::Arc;

use super::deps::*;
use super::deps_exec::*;

pub fn err_active_cluster_selection_policy_not_found() -> TaskError {
    TaskError::ActiveClusterSelectionPolicyNotFound
}
pub fn err_domain_entry_not_found() -> TaskError {
    TaskError::DomainEntryNotFound
}

/// `InitializeLoggerForTask`
pub fn initialize_logger_for_task(shard_id: i32, task: &dyn PersistenceTask, logger: &Logger) -> Logger {
    logger.with_tags(vec![
        tag::shard_id(shard_id),
        tag::task_id(task.get_task_id()),
        tag::task_visibility_timestamp(task.get_visibility_timestamp().unix_nano()),
        tag::failover_version(task.get_version()),
        tag::task_type(task.get_task_type()),
        tag::workflow_domain_id(task.get_domain_id()),
        tag::workflow_id(task.get_workflow_id()),
        tag::workflow_run_id(task.get_run_id()),
    ])
}

/// `GetTransferTaskMetricsScope`
pub fn get_transfer_task_metrics_scope(task_type: i32, is_active: bool) -> ScopeIdx {
    use metrics::*;
    let pick = |a: ScopeIdx, s: ScopeIdx| if is_active { a } else { s };
    match task_type {
        ptask_type::TRANSFER_TASK_TYPE_ACTIVITY_TASK => {
            pick(TRANSFER_ACTIVE_TASK_ACTIVITY_SCOPE, TRANSFER_STANDBY_TASK_ACTIVITY_SCOPE)
        }
        ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK => {
            pick(TRANSFER_ACTIVE_TASK_DECISION_SCOPE, TRANSFER_STANDBY_TASK_DECISION_SCOPE)
        }
        ptask_type::TRANSFER_TASK_TYPE_CLOSE_EXECUTION => pick(
            TRANSFER_ACTIVE_TASK_CLOSE_EXECUTION_SCOPE,
            TRANSFER_STANDBY_TASK_CLOSE_EXECUTION_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_CANCEL_EXECUTION => pick(
            TRANSFER_ACTIVE_TASK_CANCEL_EXECUTION_SCOPE,
            TRANSFER_STANDBY_TASK_CANCEL_EXECUTION_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_SIGNAL_EXECUTION => pick(
            TRANSFER_ACTIVE_TASK_SIGNAL_EXECUTION_SCOPE,
            TRANSFER_STANDBY_TASK_SIGNAL_EXECUTION_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_START_CHILD_EXECUTION => pick(
            TRANSFER_ACTIVE_TASK_START_CHILD_EXECUTION_SCOPE,
            TRANSFER_STANDBY_TASK_START_CHILD_EXECUTION_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_RECORD_WORKFLOW_STARTED => pick(
            TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_STARTED_SCOPE,
            TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_STARTED_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_RESET_WORKFLOW => {
            pick(TRANSFER_ACTIVE_TASK_RESET_WORKFLOW_SCOPE, TRANSFER_STANDBY_TASK_RESET_WORKFLOW_SCOPE)
        }
        ptask_type::TRANSFER_TASK_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES => pick(
            TRANSFER_ACTIVE_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE,
            TRANSFER_STANDBY_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_RECORD_WORKFLOW_CLOSED => pick(
            TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_CLOSED_SCOPE,
            TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_CLOSED_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_RECORD_CHILD_EXECUTION_COMPLETED => pick(
            TRANSFER_ACTIVE_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE,
            TRANSFER_STANDBY_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE,
        ),
        ptask_type::TRANSFER_TASK_TYPE_APPLY_PARENT_CLOSE_POLICY => pick(
            TRANSFER_ACTIVE_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE,
            TRANSFER_STANDBY_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE,
        ),
        _ => pick(TRANSFER_ACTIVE_QUEUE_PROCESSOR_SCOPE, TRANSFER_STANDBY_QUEUE_PROCESSOR_SCOPE),
    }
}

/// `GetTimerTaskMetricScope`
pub fn get_timer_task_metric_scope(task_type: i32, is_active: bool) -> ScopeIdx {
    use metrics::*;
    let pick = |a: ScopeIdx, s: ScopeIdx| if is_active { a } else { s };
    match task_type {
        ptask_type::TASK_TYPE_DECISION_TIMEOUT => {
            pick(TIMER_ACTIVE_TASK_DECISION_TIMEOUT_SCOPE, TIMER_STANDBY_TASK_DECISION_TIMEOUT_SCOPE)
        }
        ptask_type::TASK_TYPE_ACTIVITY_TIMEOUT => {
            pick(TIMER_ACTIVE_TASK_ACTIVITY_TIMEOUT_SCOPE, TIMER_STANDBY_TASK_ACTIVITY_TIMEOUT_SCOPE)
        }
        ptask_type::TASK_TYPE_USER_TIMER => {
            pick(TIMER_ACTIVE_TASK_USER_TIMER_SCOPE, TIMER_STANDBY_TASK_USER_TIMER_SCOPE)
        }
        ptask_type::TASK_TYPE_WORKFLOW_TIMEOUT => {
            pick(TIMER_ACTIVE_TASK_WORKFLOW_TIMEOUT_SCOPE, TIMER_STANDBY_TASK_WORKFLOW_TIMEOUT_SCOPE)
        }
        ptask_type::TASK_TYPE_DELETE_HISTORY_EVENT => pick(
            TIMER_ACTIVE_TASK_DELETE_HISTORY_EVENT_SCOPE,
            TIMER_STANDBY_TASK_DELETE_HISTORY_EVENT_SCOPE,
        ),
        ptask_type::TASK_TYPE_ACTIVITY_RETRY_TIMER => pick(
            TIMER_ACTIVE_TASK_ACTIVITY_RETRY_TIMER_SCOPE,
            TIMER_STANDBY_TASK_ACTIVITY_RETRY_TIMER_SCOPE,
        ),
        ptask_type::TASK_TYPE_WORKFLOW_BACKOFF_TIMER => pick(
            TIMER_ACTIVE_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE,
            TIMER_STANDBY_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE,
        ),
        _ => pick(TIMER_ACTIVE_QUEUE_PROCESSOR_SCOPE, TIMER_STANDBY_QUEUE_PROCESSOR_SCOPE),
    }
}

/// `verifyTaskVersion`: returns true if failover version check is successful.
pub fn verify_task_version(
    shard: &Arc<dyn ShardContext>,
    logger: &Logger,
    domain_id: &str,
    version: i64,
    task_version: i64,
    task: &dyn PersistenceTask,
) -> Result<bool> {
    let domain_entry = match shard.get_domain_cache().get_domain_by_id(domain_id) {
        Ok(e) => e,
        Err(e) => {
            logger.debug(&format!("Cannot find domainID: {}, err: {:?}.", domain_id, e), &[]);
            return Err(e);
        }
    };
    if !domain_entry.is_global_domain() {
        logger.debug(&format!("DomainID: {} is not active, task: {:?} version check pass", domain_id, task), &[]);
        return Ok(true);
    } else if version != task_version {
        logger.debug(&format!(
            "DomainID: {} is active, task: {:?} version != target version: {}.",
            domain_id, task, version
        ), &[]);
        return Ok(false);
    }
    logger.debug(&format!(
        "DomainID: {} is active, task: {:?} version == target version: {}.",
        domain_id, task, version
    ), &[]);
    Ok(true)
}

/// `loadMutableState`: load mutable state; if next event ID <= task event ID, attempt a refresh;
/// if still stale, returns `Ok(None)`.
pub async fn load_mutable_state(
    wf_context: &dyn ExecutionContext,
    task: &dyn PersistenceTask,
    metrics_scope: &Arc<dyn MetricsScope>,
    logger: &Logger,
    event_id: i64,
) -> Result<Option<Arc<dyn MutableState>>> {
    let mut ms = match wf_context.load_workflow_execution().await {
        Ok(ms) => ms,
        Err(TaskError::EntityNotExists(_)) => {
            // duplicate processing of the task, execution already completed.
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let execution_info = ms.get_execution_info();

    let is_decision_retry = (task.get_task_type() == ptask_type::TASK_TYPE_DECISION_TIMEOUT
        || task.get_task_type() == ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK)
        && execution_info.decision_schedule_id == event_id
        && execution_info.decision_attempt > 0;

    if event_id >= ms.get_next_event_id() && !is_decision_retry {
        metrics_scope.inc_counter(metrics::STALE_MUTABLE_STATE_COUNTER);
        wf_context.clear();

        ms = wf_context.load_workflow_execution().await?;
        // after refresh, still mutable state's next event ID <= task ID
        if event_id >= ms.get_next_event_id() {
            let domain_name = ms.get_domain_entry().info.name.clone();
            metrics_scope
                .tagged(vec![domain_tag(&domain_name)])
                .inc_counter(metrics::DATA_INCONSISTENT_COUNTER);
            logger.error(
                "Task Processor: task event ID >= MS NextEventID, skip.",
                &[
                    tag::workflow_domain_name(&domain_name),
                    tag::workflow_domain_id(task.get_domain_id()),
                    tag::workflow_id(task.get_workflow_id()),
                    tag::workflow_run_id(task.get_run_id()),
                    tag::task_type(task.get_task_type()),
                    tag::task_id(task.get_task_id()),
                    tag::workflow_event_id(event_id),
                    tag::workflow_next_event_id(ms.get_next_event_id()),
                ],
            );
            return Ok(None);
        }
    }
    Ok(Some(ms))
}

/// `timeoutWorkflow`
pub fn timeout_workflow(ms: &dyn MutableState, event_batch_first_event_id: i64) -> Result<()> {
    if let Some(decision) = ms.get_in_flight_decision() {
        fail_decision(ms, &decision, DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION)?;
    }
    ms.add_timeout_workflow_event(event_batch_first_event_id)
}

/// `retryWorkflow`
pub async fn retry_workflow(
    ms: &dyn MutableState,
    event_batch_first_event_id: i64,
    parent_domain_name: &str,
    attrs: &ContinueAsNewWorkflowExecutionDecisionAttributes,
) -> Result<Arc<dyn MutableState>> {
    if let Some(decision) = ms.get_in_flight_decision() {
        fail_decision(ms, &decision, DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION)?;
    }
    ms.add_continue_as_new_event(event_batch_first_event_id, EMPTY_EVENT_ID, parent_domain_name, attrs)
        .await
}

/// `getWorkflowExecution`
pub fn get_workflow_execution(task_info: &dyn PersistenceTask) -> WorkflowIdentifier {
    WorkflowIdentifier {
        domain_id: task_info.get_domain_id().to_string(),
        workflow_id: task_info.get_workflow_id().to_string(),
        run_id: task_info.get_run_id().to_string(),
    }
}

/// `shouldPushToMatching`
pub async fn should_push_to_matching(shard: &Arc<dyn ShardContext>, task_info: &dyn PersistenceTask) -> Result<bool> {
    let domain_entry = shard.get_domain_cache().get_domain_by_id(task_info.get_domain_id())?;
    if !domain_entry.replication_config.is_active_active() {
        // Preserve the behavior of active-standby and local domains. Always push to matching
        return Ok(true);
    }
    // For active-active domains, only push to matching if the workflow is active in current cluster
    let info = shard
        .get_active_cluster_manager()
        .get_active_cluster_info_by_workflow(
            task_info.get_domain_id(),
            task_info.get_workflow_id(),
            task_info.get_run_id(),
        )
        .await?;
    if info.active_cluster_name != shard.get_cluster_metadata().get_current_cluster_name() {
        return Ok(false);
    }
    Ok(true)
}

/// `getClusterAttributesForTask`
pub async fn get_cluster_attributes_for_task(
    shard: &Arc<dyn ShardContext>,
    task_info: &dyn PersistenceTask,
) -> Result<Option<ClusterAttribute>> {
    let domain_entry = match shard.get_domain_cache().get_domain_by_id(task_info.get_domain_id()) {
        Ok(e) => e,
        Err(e) => return Err(TaskError::Joined(vec![err_domain_entry_not_found(), e])),
    };
    if !domain_entry.replication_config.is_active_active() {
        return Ok(None);
    }
    let policy = shard
        .get_active_cluster_manager()
        .get_active_cluster_selection_policy_for_workflow(
            task_info.get_domain_id(),
            task_info.get_workflow_id(),
            task_info.get_run_id(),
        )
        .await;
    match policy {
        Ok(Some(p)) => Ok(p.cluster_attribute),
        Ok(None) => Err(TaskError::Joined(vec![err_active_cluster_selection_policy_not_found()])),
        Err(e) => Err(TaskError::Joined(vec![err_active_cluster_selection_policy_not_found(), e])),
    }
}
