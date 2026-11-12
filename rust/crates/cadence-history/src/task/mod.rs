#![allow(clippy::unnecessary_min_or_max)]
//! History task processing, ported from go/service/history/task.
#![allow(clippy::absurd_extreme_comparisons)]
pub mod constants;
pub mod deps;
pub mod deps_exec;
pub mod event_logger;
pub mod executor_wrapper;
pub mod history_task;
pub mod interface;
pub mod priority_assigner;
pub mod rate_limited_processor;
pub mod redispatcher;
pub mod rescheduler;
pub mod standby_task_util;
pub mod task_rate_limiter;
pub mod task_util;
pub mod hierarchical_wrr;
pub mod parallel_processor;
pub mod processor;

pub mod timer_task_executor_base;
pub mod transfer_task_executor_base;
pub mod timer_active_task_executor;
pub mod timer_standby_task_executor;
pub mod transfer_standby_task_executor;
pub mod transfer_active_task_executor;

#[cfg(test)]
mod timer_active_task_executor_tests;
