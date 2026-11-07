//! Real per-shard `QueueFactory` (Go `queue.NewTransferQueueFactory` /
//! `NewTimerQueueFactory` as used by `historyEngineFactory.CreateEngine`).
//!
//! The factory builds the transfer and timer processors with
//! `queues::build_queue_processors` from a `QueueExec` (shared deps plus the
//! active/standby executors of each category). Supplying that `QueueExec`
//! is delegated to a `QueueExecProvider`, because the executors need
//! implementations of the task dependency traits (`TransferActiveDependencies`,
//! `TransferStandbyDependencies`, `TimerActiveDependencies`,
//! `TimerStandbyDependencies`, `CleanupDependencies`) over the real
//! execution cache, history/matching clients, visibility manager and archiver.
//! Those implementations do not exist yet; see
//! memory/history-gap-queuefactory-20261107.md.
use std::collections::HashMap;
use std::sync::Arc;

use super::env::{QueueFactory, ShardBuildCtx};
use super::queues::{build_queue_processors, QueueDeps, QueueExec};
use crate::engine::deps::HistoryTaskCategory;
use crate::engine::runtime::QueueProcessor;

/// Builds the queue v1 inputs for one shard.
pub trait QueueExecProvider: Send + Sync {
    /// `Ok(None)` means the executors are not available for this shard.
    fn queue_exec(&self, ctx: &ShardBuildCtx) -> anyhow::Result<Option<QueueExec>>;
}

/// Provider used until the executor dependency adapters exist.
pub struct MissingExecutorsProvider;

impl QueueExecProvider for MissingExecutorsProvider {
    fn queue_exec(&self, _ctx: &ShardBuildCtx) -> anyhow::Result<Option<QueueExec>> {
        // TODO(link): implement the *Dependencies traits of task::{transfer,timer}_{active,standby}
        // executors (execution cache, history client, visibility manager, archiver) and return a QueueExec.
        Ok(None)
    }
}

/// `QueueFactory` over a `QueueExecProvider`.
pub struct ExecQueueFactory {
    provider: Arc<dyn QueueExecProvider>,
}

impl ExecQueueFactory {
    pub fn new(provider: Arc<dyn QueueExecProvider>) -> Self {
        Self { provider }
    }
}

impl QueueFactory for ExecQueueFactory {
    fn build_queue_processors(
        &self,
        ctx: &ShardBuildCtx,
    ) -> anyhow::Result<HashMap<HistoryTaskCategory, Arc<dyn QueueProcessor>>> {
        match self.provider.queue_exec(ctx)? {
            Some(exec) => Ok(build_queue_processors(QueueDeps { processors: HashMap::new(), exec: Some(exec) })),
            None => {
                tracing::error!(
                    shard_id = ctx.shard.shard_id(),
                    "history queue executors are not wired: transfer/timer tasks will NOT be processed"
                );
                Ok(HashMap::new())
            }
        }
    }
}
