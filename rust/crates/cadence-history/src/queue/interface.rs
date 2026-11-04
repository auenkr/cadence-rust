//! Port of `go/service/history/queue/interface.go` (the `Processor` interface; the other
//! interfaces there are the concrete structs/traits in processing_queue*.rs, split_policy.rs).
use std::collections::HashSet;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::action::{Action, ActionResult};
use super::deps::{Daemon, NotifyTaskInfo, QueueError};

/// Processor is the interface for task queue processor
#[async_trait]
pub trait Processor: Daemon {
    async fn failover_domain(&self, domain_ids: HashSet<String>);
    fn notify_new_task(&self, cluster_name: &str, info: &NotifyTaskInfo);
    async fn handle_action(
        &self,
        ctx: CancellationToken,
        cluster_name: &str,
        action: Action,
    ) -> Result<ActionResult, QueueError>;
    async fn lock_task_processing(&self);
    fn unlock_task_processing(&self);
}
