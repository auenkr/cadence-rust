//! Port of `go/service/history/task/rate_limited_processor.go`.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::deps::*;
use super::interface::*;
use super::task_rate_limiter::RateLimiter;

pub struct RateLimitedProcessor {
    base_processor: Arc<dyn Processor>,
    rate_limiter: Arc<dyn RateLimiter>,
    cancel_ctx: CancellationToken,
    status: AtomicI32,
}

/// `NewRateLimitedProcessor`
pub fn new_rate_limited_processor(
    base_processor: Arc<dyn Processor>,
    rate_limiter: Arc<dyn RateLimiter>,
) -> Arc<dyn Processor> {
    Arc::new(RateLimitedProcessor {
        base_processor,
        rate_limiter,
        cancel_ctx: CancellationToken::new(),
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
    })
}

#[async_trait]
impl Daemon for RateLimitedProcessor {
    async fn start(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_INITIALIZED, DAEMON_STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.base_processor.start().await;
    }

    async fn stop(&self) {
        if self
            .status
            .compare_exchange(DAEMON_STATUS_STARTED, DAEMON_STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.cancel_ctx.cancel();
        self.base_processor.stop().await;
    }
}

#[async_trait]
impl Processor for RateLimitedProcessor {
    async fn submit(&self, t: Arc<dyn Task>) -> Result<()> {
        self.rate_limiter.wait(&self.cancel_ctx, t.as_ref()).await?;
        self.base_processor.submit(t).await
    }

    async fn try_submit(&self, t: Arc<dyn Task>) -> Result<bool> {
        if !self.rate_limiter.allow(t.as_ref()) {
            return Ok(false);
        }
        self.base_processor.try_submit(t).await
    }
}
