//! Port of `go/service/history/task/constants.go`.

use std::time::Duration;

pub const LOAD_DOMAIN_ENTRY_FOR_TASK_RETRY_DELAY: Duration = Duration::from_millis(100);

pub const ACTIVE_TASK_RESUBMIT_MAX_ATTEMPTS: i32 = 10;

pub const DEFAULT_TASK_EVENT_LOGGER_SIZE: usize = 100;

pub const STICKY_TASK_MAX_RETRY_COUNT: i32 = 100;

/// `noPriority` is the value returned if no priority is ever assigned to the task.
pub const NO_PRIORITY: i32 = -1;
