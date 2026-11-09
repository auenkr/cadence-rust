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

/// Explicit startup failure until the full per-shard executor assembly exists.
/// Go always builds transfer and timer processors; silently starting without them
/// would accept work while never processing it.
pub struct UnassembledExecutorsProvider;

impl QueueExecProvider for UnassembledExecutorsProvider {
    fn queue_exec(&self, ctx: &ShardBuildCtx) -> anyhow::Result<Option<QueueExec>> {
        anyhow::bail!("History shard {} cannot start: runtime queue executor assembly is incomplete (active/standby executors, queue shard context, and task allocator are not assembled)", ctx.shard.shard_id())
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
            None => anyhow::bail!("History queue provider returned no processors for shard {}", ctx.shard.shard_id()),
        }
    }
}
