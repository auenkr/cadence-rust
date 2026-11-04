//! Port of `go/service/history/taskdlq/host_limiter.go`.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::task::deps::TaskError;

/// HostLimiter bounds the number of concurrent DLQ processing runs on the host.
/// A limit <= 0 disables limiting (Go: nil semaphore channel).
#[derive(Clone, Default)]
pub struct HostLimiter {
    sem: Option<Arc<Semaphore>>,
}

/// Permit returned by `acquire`; dropping it is Go's `Release()`.
pub struct HostLimiterPermit(#[allow(dead_code)] Option<OwnedSemaphorePermit>);

pub fn new_host_limiter(limit: i32) -> HostLimiter {
    HostLimiter { sem: if limit > 0 { Some(Arc::new(Semaphore::new(limit as usize))) } else { None } }
}

impl HostLimiter {
    /// `Acquire`: blocks until a slot is available or ctx is cancelled.
    pub async fn acquire(&self, ctx: &CancellationToken) -> Result<HostLimiterPermit, TaskError> {
        match &self.sem {
            None => {
                if ctx.is_cancelled() {
                    Err(TaskError::ContextCanceled)
                } else {
                    Ok(HostLimiterPermit(None))
                }
            }
            Some(s) => tokio::select! {
                p = s.clone().acquire_owned() => Ok(HostLimiterPermit(p.ok())),
                _ = ctx.cancelled() => Err(TaskError::ContextCanceled),
            },
        }
    }
}
