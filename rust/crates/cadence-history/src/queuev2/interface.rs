//! Port of queuev2/interface.go

use std::collections::HashSet;

use async_trait::async_trait;

use super::deps::{Action, ActionResult, DepError, HistoryTaskCategory, NotifyTaskInfo};

/// Go `common.Daemon`.
#[async_trait]
pub trait Daemon: Send + Sync {
    async fn start(&self);
    async fn stop(&self);
}

#[async_trait]
pub trait Queue: Daemon {
    fn category(&self) -> HistoryTaskCategory;
    fn notify_new_task(&self, cluster_name: &str, info: &NotifyTaskInfo);
    fn failover_domain(&self, domain_ids: &HashSet<String>);
    async fn handle_action(
        &self,
        cluster_name: &str,
        action: &Action,
    ) -> Result<Option<ActionResult>, DepError>;
    fn lock_task_processing(&self);
    fn unlock_task_processing(&self);
}
