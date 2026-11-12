//! Assembly of the per-shard queue processors consumed by the engine
//! (`engine::runtime::QueueProcessor`).
//!
//! Implemented here: the adapter from `queue::interface::Processor` (transfer /
//! timer, active + standby) to the engine's `QueueProcessor`, and
//! `build_queue_processors`, which packages already-constructed processors.
//!
//! Documented gaps (memory/history-port-wiring-20261105.md, Unit D2/D3):
//! * Constructing `TransferQueueProcessor` / queuev2 factories needs a
//!   `queue::shard_deps::QueueShardContext` and `queuev2::deps::ShardContext`
//!   adapter over the real `ShardContext`, real task executors, a visibility
//!   writer and an archival client. The visibility writer and archival client
//!   have no real implementation yet, so the caller must supply the processors
//!   through `QueueDeps::processors`.
//! * The Go timer queue processor is not ported in `queue/`; only the transfer
//!   processor exists.
//! * The engine notifies with replication-shaped tasks; the adapter converts
//!   that into an empty `NotifyTaskInfo` (wake-up only).
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::engine::deps::{HResult, HistoryError, HistoryTaskCategory};
use crate::engine::runtime::{QueueAction, QueueProcessor};
use crate::queue::action::Action;
use crate::queue::deps::NotifyTaskInfo;
use crate::queue::interface::Processor;

use super::queue_exec::{build_timer_queue_processor, build_transfer_queue_processor, CategoryExecutors, QueueExecCommon};

/// What the environment supplies for the queue processors of one shard.
pub struct QueueDeps {
    /// Already-built processors for each category (transfer, timer). Building them
    /// requires the visibility writer / archival client (see module docs).
    pub processors: HashMap<HistoryTaskCategory, Arc<dyn Processor>>,
    /// When set, the transfer / timer processors missing from `processors` are constructed
    /// from these shared deps and executors.
    pub exec: Option<QueueExec>,
}

/// Inputs for constructing the queue v1 processors of one shard.
pub struct QueueExec {
    pub common: QueueExecCommon,
    pub transfer: CategoryExecutors,
    pub timer: CategoryExecutors,
}

/// Adapts `queue::interface::Processor` to `engine::runtime::QueueProcessor`.
pub struct EngineQueueProcessor {
    inner: Arc<dyn Processor>,
    category: HistoryTaskCategory,
}

impl EngineQueueProcessor {
    pub fn new(inner: Arc<dyn Processor>, category: HistoryTaskCategory) -> Arc<Self> {
        Arc::new(Self { inner, category })
    }
}

/// `persistence.Task` view of one notified task for the queue processors.
#[derive(Debug)]
struct NotifiedPersistenceTask {
    domain_id: String,
    workflow_id: String,
    run_id: String,
    task: crate::engine::notify_tasks::NotifiedTask,
    category: crate::queue::deps::HistoryTaskCategory,
}

impl crate::queue::deps::PersistenceTask for NotifiedPersistenceTask {
    fn get_domain_id(&self) -> String {
        self.domain_id.clone()
    }
    fn get_workflow_id(&self) -> String {
        self.workflow_id.clone()
    }
    fn get_run_id(&self) -> String {
        self.run_id.clone()
    }
    fn get_task_id(&self) -> i64 {
        self.task.task_id
    }
    fn get_task_type(&self) -> i32 {
        self.task.task_type as i32
    }
    fn get_task_category(&self) -> crate::queue::deps::HistoryTaskCategory {
        self.category
    }
    fn get_version(&self) -> i64 {
        self.task.version
    }
    fn get_visibility_timestamp(&self) -> crate::queue::deps::Time {
        crate::queue::deps::Time(self.task.visibility_timestamp)
    }
    fn is_corrupted(&self) -> bool {
        false
    }
}

/// Converts the engine's `hcommon.NotifyTaskInfo` into the queue package's view.
fn to_queue_notify_info(
    info: &crate::engine::notify_tasks::NotifyTaskInfo,
    category: HistoryTaskCategory,
) -> NotifyTaskInfo {
    let queue_category = match category {
        HistoryTaskCategory::Timer => crate::queue::deps::HistoryTaskCategory::Timer,
        _ => crate::queue::deps::HistoryTaskCategory::Transfer,
    };
    let e = &info.execution_info;
    NotifyTaskInfo {
        execution_info: Some(crate::queue::deps::WorkflowExecutionInfo {
            domain_id: e.domain_id.clone(),
            workflow_id: e.workflow_id.clone(),
            run_id: e.run_id.clone(),
        }),
        tasks: info
            .tasks
            .iter()
            .map(|t| {
                Arc::new(NotifiedPersistenceTask {
                    domain_id: e.domain_id.clone(),
                    workflow_id: e.workflow_id.clone(),
                    run_id: e.run_id.clone(),
                    task: t.clone(),
                    category: queue_category,
                }) as Arc<dyn crate::queue::deps::PersistenceTask>
            })
            .collect(),
        persistence_error: info.persistence_error,
        ..NotifyTaskInfo::default()
    }
}

#[async_trait]
impl QueueProcessor for EngineQueueProcessor {
    fn start(&self) {
        crate::queue::deps::Daemon::start(self.inner.clone());
    }

    async fn stop(&self) {
        crate::queue::deps::Daemon::stop(&*self.inner).await;
    }

    async fn handle_action(&self, cluster: &str, action: QueueAction) -> HResult<Vec<String>> {
        match action {
            QueueAction::Reset => {
                self.inner
                    .handle_action(CancellationToken::new(), cluster, Action::new_reset_action())
                    .await
                    .map_err(|e| HistoryError::Other(format!("{e:?}")))?;
                Ok(Vec::new())
            }
            QueueAction::GetState => {
                let res = self
                    .inner
                    .handle_action(CancellationToken::new(), cluster, Action::new_get_state_action())
                    .await
                    .map_err(|e| HistoryError::Other(format!("{e:?}")))?;
                Ok(res
                    .get_state_action_result
                    .map(|r| r.states.iter().map(super::queue_state_string::processing_queue_state_string).collect())
                    .unwrap_or_default())
            }
        }
    }

    fn notify_new_task(&self, cluster: &str, info: &crate::engine::notify_tasks::NotifyTaskInfo) {
        self.inner.notify_new_task(cluster, &to_queue_notify_info(info, self.category));
    }

    async fn failover_domain(&self, domain_ids: &std::collections::HashSet<String>) {
        self.inner.failover_domain(domain_ids.clone()).await;
    }

    async fn lock_task_processing(&self) {
        self.inner.lock_task_processing().await;
    }

    fn unlock_task_processing(&self) {
        self.inner.unlock_task_processing();
    }
}

/// `engine` side map of queue processors per task category.
pub fn build_queue_processors(deps: QueueDeps) -> HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>> {
    let mut processors = deps.processors;
    if let Some(exec) = deps.exec {
        // Go `NewTransferQueueProcessor` / `NewTimerQueueProcessor` (queue v1) constructed for real.
        processors
            .entry(HistoryTaskCategory::Transfer)
            .or_insert_with(|| build_transfer_queue_processor(&exec.common, exec.transfer) as Arc<dyn Processor>);
        processors
            .entry(HistoryTaskCategory::Timer)
            .or_insert_with(|| build_timer_queue_processor(&exec.common, exec.timer) as Arc<dyn Processor>);
    }
    processors
        .into_iter()
        .map(|(category, p)| (category, EngineQueueProcessor::new(p, category) as Arc<dyn QueueProcessor>))
        .collect()
}
