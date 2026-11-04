//! Port of `go/service/history/task/standby_task_util.go`.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::deps::*;
use super::deps_exec::*;
use super::history_task::{err_task_discarded, new_redispatch_error};
use super::task_util::*;
use crate::taskdlq;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Go `interface{}` post-action info; `None` == nil.
pub type PostActionInfo = Option<Arc<dyn Any + Send + Sync>>;

/// `standbyActionFn`
pub type StandbyActionFn = Arc<
    dyn for<'a> Fn(&'a dyn ExecutionContext, &'a Arc<dyn MutableState>) -> BoxFut<'a, Result<PostActionInfo>>
        + Send
        + Sync,
>;
/// `standbyPostActionFn`
pub type StandbyPostActionFn =
    Arc<dyn for<'a> Fn(&'a dyn PersistenceTask, PostActionInfo, &'a Logger) -> BoxFut<'a, Result<()>> + Send + Sync>;
/// `standbyCurrentTimeFn`
pub type StandbyCurrentTimeFn = Arc<dyn Fn(&dyn PersistenceTask) -> Result<Time> + Send + Sync>;

/// `TaskDLQWriter`
#[async_trait]
pub trait TaskDLQWriter: Send + Sync {
    async fn create_history_dlq_task(&self, request: CreateHistoryDLQTaskRequest) -> Result<()>;
    async fn create_history_dlq_ack_level_if_not_exists(&self, request: CreateHistoryDLQAckLevelRequest) -> Result<()>;
}

/// `errDomainBecomesActive`
pub fn err_domain_becomes_active() -> TaskError {
    TaskError::DomainBecomesActive
}

fn task_tags_basic(t: &dyn PersistenceTask) -> Vec<Tag> {
    vec![
        tag::workflow_id(t.get_workflow_id()),
        tag::workflow_run_id(t.get_run_id()),
        tag::workflow_domain_id(t.get_domain_id()),
        tag::task_id(t.get_task_id()),
        tag::task_type(t.get_task_type()),
        tag::failover_version(t.get_version()),
        tag::timestamp(t.get_visibility_timestamp()),
    ]
}

/// `standbyTaskPostActionNoOp`
pub fn standby_task_post_action_no_op() -> StandbyPostActionFn {
    Arc::new(|task_info, post_action_info, logger| {
        Box::pin(async move {
            if post_action_info.is_none() {
                return Ok(());
            }
            logger.debug_tags(
                "standbyTaskPostActionNoOp return redispatch error so task processing logic will retry",
                &task_tags_basic(task_info),
            );
            // Go: fmt.Sprintf("post action is %T", postActionInfo)
            Err(new_redispatch_error("post action is Some(info)"))
        })
    })
}

/// `standbyTaskPostActionTaskDiscarded`
pub fn standby_task_post_action_task_discarded() -> StandbyPostActionFn {
    Arc::new(|task, post_action_info, logger| {
        Box::pin(async move { discard_impl(task, post_action_info, logger) })
    })
}

fn discard_impl(task: &dyn PersistenceTask, info: PostActionInfo, logger: &Logger) -> Result<()> {
    if info.is_none() {
        return Ok(());
    }
    logger.error(
        "Discarding standby task due to task being pending for too long.",
        &task_tags_basic(task),
    );
    Err(err_task_discarded())
}

/// `standbyTaskPostActionWriteToDLQ`
pub fn standby_task_post_action_write_to_dlq(
    writer: Arc<dyn TaskDLQWriter>,
    shard: Arc<dyn ShardContext>,
    enabled: StringPropertyFnWithDomainFilter,
) -> StandbyPostActionFn {
    let shard_id = shard.get_shard_id();
    Arc::new(move |task, post_action_info, logger| {
        let writer = writer.clone();
        let shard = shard.clone();
        let enabled = enabled.clone();
        Box::pin(async move {
            if post_action_info.is_none() {
                return Ok(());
            }
            let cluster_attribute = match get_cluster_attributes_for_task(&shard, task).await {
                Ok(a) => a,
                Err(e) => {
                    if e.is(&err_active_cluster_selection_policy_not_found()) {
                        logger.warn("Active cluster selection policy not found. Defaulting to default scope and name.");
                        None
                    } else {
                        return Err(e);
                    }
                }
            };
            let cluster_attribute = cluster_attribute.unwrap_or(ClusterAttribute {
                scope: taskdlq::DEFAULT_CLUSTER_ATTRIBUTE_SCOPE.to_string(),
                name: taskdlq::DEFAULT_CLUSTER_ATTRIBUTE_NAME.to_string(),
            });

            let domain_id = task.get_domain_id().to_string();
            let domain_name = match shard.get_domain_cache().get_domain_name(&domain_id) {
                Ok(n) => n,
                Err(e) => {
                    logger.debug_tags(
                        "Failed to get domain name from domain cache. Defaulting to domain ID.",
                        &[tag::workflow_domain_id(&domain_id), tag::error(&e)],
                    );
                    domain_id.clone()
                }
            };
            let mode = enabled(&domain_name);

            let task_tags = vec![
                tag::workflow_id(task.get_workflow_id()),
                tag::workflow_run_id(task.get_run_id()),
                tag::workflow_domain_id(&domain_id),
                tag::workflow_domain_name(&domain_name),
                tag::task_id(task.get_task_id()),
                tag::task_type(task.get_task_type()),
                tag::failover_version(task.get_version()),
                tag::timestamp(task.get_visibility_timestamp()),
            ];

            let dlq_req = CreateHistoryDLQTaskRequest {
                shard_id,
                domain_id: domain_id.clone(),
                domain_name,
                cluster_attribute_scope: cluster_attribute.scope.clone(),
                cluster_attribute_name: cluster_attribute.name.clone(),
                // TODO(link): Task must be an owned Arc; callers hand an Arc-backed task
                task: task.clone_arc(),
            };
            let ack_req = CreateHistoryDLQAckLevelRequest {
                shard_id,
                domain_id,
                cluster_attribute_scope: cluster_attribute.scope,
                cluster_attribute_name: cluster_attribute.name,
                task_category: task.get_task_category(),
            };

            match mode.as_str() {
                HISTORY_TASK_DLQ_MODE_ENABLED => {
                    logger.warn_tags("Writing standby task to DLQ due to task being pending for too long.", &task_tags);
                    writer.create_history_dlq_task(dlq_req).await?;
                    writer.create_history_dlq_ack_level_if_not_exists(ack_req).await
                }
                HISTORY_TASK_DLQ_MODE_SHADOW => {
                    logger.warn_tags(
                        "Writing standby task to DLQ in shadow mode; task will be discarded.",
                        &task_tags,
                    );
                    if let Err(e) = writer.create_history_dlq_task(dlq_req).await {
                        logger.warn_tags(
                            "Failed to write standby task to DLQ in shadow mode. Will discard the task.",
                            &[tag::error(&e)],
                        );
                    } else if let Err(e) = writer.create_history_dlq_ack_level_if_not_exists(ack_req).await {
                        logger.warn_tags(
                            "Failed to seed DLQ ack level in shadow mode. Will discard the task.",
                            &[tag::error(&e)],
                        );
                    }
                    discard_impl(task, post_action_info, logger)
                }
                _ => {
                    logger.warn_tags("DLQ not enabled for domain; discarding standby task.", &task_tags);
                    discard_impl(task, post_action_info, logger)
                }
            }
        })
    })
}

#[derive(Clone, Debug, Default)]
pub struct HistoryResendInfo {
    pub last_event_id: Option<i64>,
    pub last_event_version: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub struct PushActivityToMatchingInfo {
    pub activity_schedule_to_start_timeout: i32,
    pub tasklist: TaskList,
    pub partition_config: std::collections::HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct PushDecisionToMatchingInfo {
    pub decision_schedule_to_start_timeout: i32,
    pub tasklist: TaskList,
    pub partition_config: std::collections::HashMap<String, String>,
}

pub fn new_push_activity_to_matching_info(
    activity_schedule_to_start_timeout: i32,
    tasklist: TaskList,
    partition_config: std::collections::HashMap<String, String>,
) -> PushActivityToMatchingInfo {
    PushActivityToMatchingInfo { activity_schedule_to_start_timeout, tasklist, partition_config }
}

pub fn new_push_decision_to_matching_info(
    decision_schedule_to_start_timeout: i32,
    tasklist: TaskList,
    partition_config: std::collections::HashMap<String, String>,
) -> PushDecisionToMatchingInfo {
    PushDecisionToMatchingInfo { decision_schedule_to_start_timeout, tasklist, partition_config }
}

/// `getHistoryResendInfo`
pub fn get_history_resend_info(ms: &(dyn MutableStateVersionHistories)) -> Result<HistoryResendInfo> {
    let version_histories = ms.get_version_histories().ok_or(TaskError::MissingVersionHistories)?;
    let current_branch = version_histories.get_current_version_history()?;
    let last_item = current_branch.get_last_item()?;
    Ok(HistoryResendInfo {
        last_event_id: Some(last_item.event_id),
        last_event_version: Some(last_item.version),
    })
}

/// `getStandbyPostActionFn`
pub fn get_standby_post_action_fn(
    logger: &Logger,
    task_info: &dyn PersistenceTask,
    standby_now: &StandbyCurrentTimeFn,
    standby_task_missing_events_resend_delay: Duration,
    standby_task_missing_events_discard_delay: Duration,
    fetch_history_standby_post_action_fn: StandbyPostActionFn,
    discard_task_standby_post_action_fn: StandbyPostActionFn,
) -> StandbyPostActionFn {
    let task_time = task_info.get_visibility_timestamp();
    let resend_time = task_time.add(standby_task_missing_events_resend_delay);
    let discard_time = task_time.add(standby_task_missing_events_discard_delay);

    let mut tags = vec![
        tag::workflow_id(task_info.get_workflow_id()),
        tag::workflow_run_id(task_info.get_run_id()),
        tag::workflow_domain_id(task_info.get_domain_id()),
        tag::task_id(task_info.get_task_id()),
        tag::task_type(task_info.get_task_type()),
        tag::timestamp(task_info.get_visibility_timestamp()),
    ];

    let now = match standby_now(task_info) {
        Ok(n) => n,
        Err(e) => {
            tags.push(tag::error(&e));
            logger.error(
                "getStandbyPostActionFn error getting current time, fallback to standbyTaskPostActionNoOp",
                &tags,
            );
            return standby_task_post_action_no_op();
        }
    };

    if now.before(resend_time) {
        logger.debug_tags("getStandbyPostActionFn returning standbyTaskPostActionNoOp because now < task start time + StandbyTaskMissingEventsResendDelay", &tags);
        return standby_task_post_action_no_op();
    }
    if now.before(discard_time) {
        logger.debug_tags("getStandbyPostActionFn returning fetchHistoryStandbyPostActionFn because task start time + StandbyTaskMissingEventsResendDelay <= now < task start time + StandbyTaskMissingEventsResendDelay", &tags);
        return fetch_history_standby_post_action_fn;
    }
    logger.debug_tags("getStandbyPostActionFn returning discardTaskStandbyPostActionFn because task start time + StandbyTaskMissingEventsResendDelay <= now", &tags);
    discard_task_standby_post_action_fn
}

/// `getRemoteClusterName`
pub async fn get_remote_cluster_name(
    current_cluster: &str,
    active_cluster_mgr: &dyn ActiveClusterManager,
    task_info: &dyn PersistenceTask,
) -> Result<String> {
    let info = active_cluster_mgr
        .get_active_cluster_info_by_workflow(task_info.get_domain_id(), task_info.get_workflow_id(), task_info.get_run_id())
        .await?;
    if info.active_cluster_name == current_cluster {
        return Err(err_domain_becomes_active());
    }
    Ok(info.active_cluster_name)
}
