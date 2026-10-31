//! Task-list scavenger decision logic from Go `scanner/tasklist/handler.go`.

use std::time::{Duration, SystemTime};

pub const SCANNER_TASK_LIST_PREFIX: &str = "cadence-sys-tl-scanner";
pub const TASK_LIST_GRACE_PERIOD: Duration = Duration::from_secs(48 * 60 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskListInfo {
    pub name: String,
    pub last_updated: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskInfo {
    pub task_id: i64,
    /// `None` represents Go's zero `time.Time`, which is not a positive expiry.
    pub expiry: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchDecision {
    /// The list is safe for now or the first task encountered is not expired.
    Done { processed: usize },
    /// Every task in this persistence batch is expired. Complete through the
    /// final task ID; optionally check idle-list deletion after completion.
    CompleteExpired {
        through_task_id: i64,
        limit: usize,
        processed: usize,
        try_delete_task_list: bool,
    },
    /// The per-job fairness cap was reached; let the executor reschedule.
    Defer { processed: usize },
    /// No tasks remain and an idle task list may be conditionally deleted by
    /// range ID. Prefix protection is already applied by this decision.
    DeleteIdleTaskList,
}

/// Decide one iteration of Go's task-list cleanup loop. The caller performs
/// persistence operations and repeats with the next batch when requested.
/// Tasks are expected in increasing task-ID order as returned by Go `GetTasks`.
pub fn decide_batch(
    info: &TaskListInfo,
    tasks: &[TaskInfo],
    batch_size: usize,
    processed_before: usize,
    max_tasks_per_job: usize,
    now: SystemTime,
) -> BatchDecision {
    if processed_before >= max_tasks_per_job {
        return BatchDecision::Defer {
            processed: processed_before,
        };
    }
    if tasks.is_empty() {
        return if task_list_is_idle_and_deletable(info, now) {
            BatchDecision::DeleteIdleTaskList
        } else {
            BatchDecision::Done { processed: 0 }
        };
    }

    let mut processed = 0;
    for task in tasks {
        processed += 1;
        if !is_expired(task, now) {
            return BatchDecision::Done { processed };
        }
    }

    let processed_after_batch = processed_before.saturating_add(processed);
    let task_list_is_exhausted = tasks.len() < batch_size;
    if !task_list_is_exhausted && processed_after_batch >= max_tasks_per_job {
        return BatchDecision::Defer {
            processed: processed_after_batch,
        };
    }

    BatchDecision::CompleteExpired {
        through_task_id: tasks.last().expect("nonempty batch").task_id,
        limit: tasks.len(),
        processed: processed_after_batch,
        try_delete_task_list: task_list_is_exhausted,
    }
}

pub fn is_expired(task: &TaskInfo, now: SystemTime) -> bool {
    let Some(expiry) = task.expiry else {
        return false;
    };
    expiry > SystemTime::UNIX_EPOCH && now > expiry
}

pub fn task_list_is_idle_and_deletable(info: &TaskListInfo, now: SystemTime) -> bool {
    if info.name.starts_with(SCANNER_TASK_LIST_PREFIX) {
        return false;
    }
    now.duration_since(info.last_updated)
        .is_ok_and(|idle| idle >= TASK_LIST_GRACE_PERIOD)
}

#[cfg(test)]
mod tests {
    use super::{BatchDecision, TaskInfo, TaskListInfo};
    use std::time::{Duration, SystemTime};

    fn list(name: &str, age: Duration) -> TaskListInfo {
        TaskListInfo {
            name: name.to_owned(),
            last_updated: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000) - age,
        }
    }

    #[test]
    fn only_expired_prefixes_are_deleted_and_small_final_batch_checks_idle_list() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let expired = TaskInfo {
            task_id: 8,
            expiry: Some(now - Duration::from_secs(1)),
        };
        let decision = super::decide_batch(
            &list("user-tasklist", Duration::from_secs(49 * 60 * 60)),
            &[expired.clone(), TaskInfo {
                task_id: 9,
                expiry: Some(now + Duration::from_secs(1)),
            }],
            16,
            0,
            256,
            now,
        );
        assert_eq!(decision, BatchDecision::Done { processed: 2 });

        let decision = super::decide_batch(
            &list("user-tasklist", Duration::from_secs(49 * 60 * 60)),
            &[expired],
            16,
            0,
            256,
            now,
        );
        assert_eq!(
            decision,
            BatchDecision::CompleteExpired {
                through_task_id: 8,
                limit: 1,
                processed: 1,
                try_delete_task_list: true,
            }
        );
    }

    #[test]
    fn fairness_cap_and_scanner_prefix_protection_match_go() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let tasks: Vec<_> = (1..=16)
            .map(|task_id| TaskInfo {
                task_id,
                expiry: Some(now - Duration::from_secs(1)),
            })
            .collect();
        assert_eq!(
            super::decide_batch(&list("user-tasklist", Duration::ZERO), &tasks, 16, 250, 256, now),
            BatchDecision::Defer { processed: 266 }
        );
        assert!(!super::task_list_is_idle_and_deletable(
            &list("cadence-sys-tl-scanner-0", Duration::from_secs(72 * 60 * 60)),
            now
        ));
        assert!(super::task_list_is_idle_and_deletable(
            &list("user-tasklist", Duration::from_secs(48 * 60 * 60)),
            now
        ));
    }
}
