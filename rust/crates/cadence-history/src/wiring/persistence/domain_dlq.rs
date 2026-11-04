//! SQL-backed failover `DomainManager` and task `TaskDLQWriter`.
use std::sync::Arc;

use async_trait::async_trait;
use cadence_persistence::domains::FrontendDomainManager;

use crate::failover::DomainManager;
use crate::task::deps::{Result as TaskResult, TaskError};
use crate::task::deps_exec::{CreateHistoryDLQAckLevelRequest, CreateHistoryDLQTaskRequest};
use crate::task::standby_task_util::TaskDLQWriter;

/// `domain.CleanPendingActiveState` over the persistence domain manager.
pub struct SqlFailoverDomainManager {
    manager: Arc<dyn FrontendDomainManager>,
}

impl SqlFailoverDomainManager {
    pub fn new(manager: Arc<dyn FrontendDomainManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl DomainManager for SqlFailoverDomainManager {
    async fn clean_pending_active_state(&self, domain_id: &str, failover_version: i64) -> Result<bool, String> {
        let m = self.manager.clone();
        let id = domain_id.to_string();
        tokio::task::spawn_blocking(move || m.clean_pending_active(&id, failover_version).map_err(|e| format!("{e:?}")))
            .await
            .map_err(|e| e.to_string())?
    }
}

/// Go only implements the history task DLQ for NoSQL; SQL returns not-supported.
#[derive(Default)]
pub struct SqlTaskDlqWriter;

#[async_trait]
impl TaskDLQWriter for SqlTaskDlqWriter {
    async fn create_history_dlq_task(&self, _r: CreateHistoryDLQTaskRequest) -> TaskResult<()> {
        Err(TaskError::Other("history task DLQ is not supported by the SQL persistence plugin".into()))
    }
    async fn create_history_dlq_ack_level_if_not_exists(&self, _r: CreateHistoryDLQAckLevelRequest) -> TaskResult<()> {
        Err(TaskError::Other("history task DLQ is not supported by the SQL persistence plugin".into()))
    }
}
