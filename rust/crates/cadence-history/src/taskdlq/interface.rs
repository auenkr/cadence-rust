//! Port of `go/service/history/taskdlq/interface.go`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::task::deps::{PersistenceTask, TaskError};

/// TaskReinjector re-injects tasks read from the DLQ back into the shard's task queues.
#[async_trait]
pub trait TaskReinjector: Send + Sync {
    async fn reinject_history_tasks(
        &self,
        ctx: &CancellationToken,
        tasks: Vec<Arc<dyn PersistenceTask>>,
    ) -> Result<(), TaskError>;
}
