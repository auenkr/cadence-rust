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
use crate::engine::runtime::{QueueAction, QueueProcessor, Task};
use crate::queue::action::Action;
use crate::queue::deps::NotifyTaskInfo;
use crate::queue::interface::Processor;

/// What the environment supplies for the queue processors of one shard.
pub struct QueueDeps {
    /// Already-built processors for each category (transfer, timer). Building them
    /// requires the visibility writer / archival client (see module docs).
    pub processors: HashMap<HistoryTaskCategory, Arc<dyn Processor>>,
}

/// Adapts `queue::interface::Processor` to `engine::runtime::QueueProcessor`.
pub struct EngineQueueProcessor {
    inner: Arc<dyn Processor>,
}

impl EngineQueueProcessor {
    pub fn new(inner: Arc<dyn Processor>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl QueueProcessor for EngineQueueProcessor {
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
                    .map(|r| r.states.iter().map(|s| format!("{s:?}")).collect())
                    .unwrap_or_default())
            }
        }
    }

    fn notify_new_task(&self, cluster: &str, _tasks: &[Task]) {
        self.inner.notify_new_task(cluster, &NotifyTaskInfo::default());
    }
}

/// `engine` side map of queue processors per task category.
pub fn build_queue_processors(deps: QueueDeps) -> HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>> {
    deps.processors
        .into_iter()
        .map(|(category, p)| (category, EngineQueueProcessor::new(p) as Arc<dyn QueueProcessor>))
        .collect()
}
