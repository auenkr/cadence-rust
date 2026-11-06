//! Port of `go/service/history/queue/factory.go`.
//!
//! Rust-forced difference (no logic change): Go's factories hold `task.Processor`, archival client,
//! workflow-ID cache and call `NewTransferQueueProcessor` / `NewTimerQueueProcessor` with the
//! execution cache and invariant passed to `CreateQueue`. Executors, resetter etc. are built by
//! the caller, so each factory holds a `build` closure that performs that wiring for one shard
//! (the closure receives the shard, exactly the `CreateQueue` argument that varies per call).
use std::sync::Arc;

use super::deps::HistoryTaskCategory;
use super::interface::Processor;
use super::shard_deps::QueueShardContext;

/// Go `queue.Factory`.
pub trait Factory: Send + Sync {
    fn category(&self) -> HistoryTaskCategory;
    fn create_queue(&self, shard: Arc<dyn QueueShardContext>) -> Arc<dyn Processor>;
}

pub type QueueBuilder = Arc<dyn Fn(Arc<dyn QueueShardContext>) -> Arc<dyn Processor> + Send + Sync>;

/// Go `transferQueueFactory`.
pub struct TransferQueueFactory {
    build: QueueBuilder,
}

/// Go `timerQueueFactory`.
pub struct TimerQueueFactory {
    build: QueueBuilder,
}

/// Go `NewTransferQueueFactory`.
pub fn new_transfer_queue_factory(build: QueueBuilder) -> Arc<dyn Factory> {
    Arc::new(TransferQueueFactory { build })
}

/// Go `NewTimerQueueFactory`.
pub fn new_timer_queue_factory(build: QueueBuilder) -> Arc<dyn Factory> {
    Arc::new(TimerQueueFactory { build })
}

impl Factory for TransferQueueFactory {
    fn category(&self) -> HistoryTaskCategory {
        HistoryTaskCategory::Transfer
    }

    fn create_queue(&self, shard: Arc<dyn QueueShardContext>) -> Arc<dyn Processor> {
        (self.build)(shard)
    }
}

impl Factory for TimerQueueFactory {
    fn category(&self) -> HistoryTaskCategory {
        HistoryTaskCategory::Timer
    }

    fn create_queue(&self, shard: Arc<dyn QueueShardContext>) -> Arc<dyn Processor> {
        (self.build)(shard)
    }
}
