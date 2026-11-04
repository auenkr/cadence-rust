//! Port of `go/service/history/queue/constants.go`.
use std::time::Duration;

pub const DEFAULT_PROCESSING_QUEUE_LEVEL: i32 = 0;
pub const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
