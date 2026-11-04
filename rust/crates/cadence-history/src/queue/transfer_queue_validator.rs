//! Port of `go/service/history/queue/transfer_queue_validator.go`.
//! `validatePendingTasks` read `processor.processingQueueCollections` directly; since the
//! collections are owned by the pump under a lock, `ack_tasks` receives them as a slice.
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use super::deps::{
    DurationFn, MetricsScope, NotifyTaskInfo, PersistenceTask, TaskKey, TaskRef, Time, WorkflowExecutionInfo,
};
use super::constants::DEFAULT_PROCESSING_QUEUE_LEVEL;
use super::processing_queue::min_task_key;
use super::processing_queue_collection::ProcessingQueueCollection;
use super::util::MAXIMUM_TRANSFER_TASK_KEY;
use std::sync::Arc;

pub const DEFAULT_MAX_PENDING_TASKS_SIZE: usize = 5000;

struct PendingTaskInfo {
    execution_info: WorkflowExecutionInfo,
    task: Arc<dyn PersistenceTask>,
    potential_false_positive: bool,
}

struct Inner {
    pending_task_infos: HashMap<i64, PendingTaskInfo>,
    max_read_levels: HashMap<i32, TaskKey>,
    min_read_task_id: i64,
    last_validate_time: Time,
}

pub struct TransferQueueValidator {
    now: Arc<dyn Fn() -> Time + Send + Sync>,
    metrics_scope: Arc<dyn MetricsScope>,
    validation_interval: DurationFn,
    inner: Mutex<Inner>,
}

impl TransferQueueValidator {
    /// Go `newTransferQueueValidator` (`now` = `processor.shard.GetTimeSource().Now`).
    pub fn new(
        now: Arc<dyn Fn() -> Time + Send + Sync>,
        validation_interval: DurationFn,
        metrics_scope: Arc<dyn MetricsScope>,
    ) -> Self {
        let t = now();
        TransferQueueValidator {
            now,
            metrics_scope,
            validation_interval,
            inner: Mutex::new(Inner {
                pending_task_infos: HashMap::new(),
                max_read_levels: HashMap::new(),
                min_read_task_id: 0,
                last_validate_time: t,
            }),
        }
    }

    pub fn add_tasks(&self, info: &NotifyTaskInfo) {
        let exec = info.execution_info.clone().unwrap_or_default();
        let mut v = self.inner.lock().unwrap();

        let mut num_to_add = info.tasks.len();
        if num_to_add + v.pending_task_infos.len() > DEFAULT_MAX_PENDING_TASKS_SIZE {
            num_to_add = DEFAULT_MAX_PENDING_TASKS_SIZE - v.pending_task_infos.len();
            let mut dump = String::new();
            let dropped = &info.tasks[num_to_add..];
            for t in dropped {
                dump.push_str(&format!("{:?}\n", t));
            }
            tracing::warn!(
                domain_id = %exec.domain_id,
                workflow_id = %exec.workflow_id,
                run_id = %exec.run_id,
                key = "dropped-transfer-tasks",
                value = %dump,
                "Too many pending transfer tasks, dropping new tasks"
            );
            self.metrics_scope.add_counter("QueueValidatorDropTaskCounter", dropped.len() as i64);
        }

        for t in &info.tasks[..num_to_add] {
            // It is possible that a task is acked before it is added to the validator
            // In that case, the lost task could be a potential false positive case
            let fp = info.persistence_error || t.get_task_id() <= v.min_read_task_id;
            v.pending_task_infos.insert(
                t.get_task_id(),
                PendingTaskInfo { execution_info: exec.clone(), task: t.clone(), potential_false_positive: fp },
            );
        }
    }

    pub fn ack_tasks(
        &self,
        queue_level: i32,
        read_level: TaskKey,
        max_read_level: TaskKey,
        loaded_tasks: &HashMap<TaskKey, TaskRef>,
        collections: &[ProcessingQueueCollection],
    ) {
        let mut v = self.inner.lock().unwrap();

        for t in loaded_tasks.values() {
            // loadedTasks may contain tasks not in pending_task_infos (retries / two processors per queue)
            v.pending_task_infos.remove(&t.get_task_id());
        }

        if queue_level == DEFAULT_PROCESSING_QUEUE_LEVEL {
            if let Some(expected) = v.max_read_levels.get(&queue_level) {
                if expected.less(&read_level) {
                    tracing::error!("Transfer queue processor load request is not continuous");
                    self.metrics_scope.inc_counter("QueueValidatorInvalidLoadCounter");
                }
            }
        }
        v.max_read_levels.insert(queue_level, max_read_level);

        let interval: Duration = (self.validation_interval)();
        let now = (self.now)();
        if now.after(v.last_validate_time.add(interval)) {
            self.validate_pending_tasks(&mut v, collections);
            v.last_validate_time = (self.now)();
        }
    }

    fn validate_pending_tasks(&self, v: &mut Inner, collections: &[ProcessingQueueCollection]) {
        self.metrics_scope.inc_counter("QueueValidatorValidationCounter");

        let mut min_read_level = MAXIMUM_TRANSFER_TASK_KEY;
        for c in collections {
            if let Some(q) = c.active_queue() {
                min_read_level = min_task_key(min_read_level, q.state().read_level());
            }
        }

        // all pending tasks with taskID <= minReadLevel will never be loaded; log, emit metrics, delete.
        // NOTE: may contain false positives (see Go comment): check persistence errors first.
        let min_read_task_id = match min_read_level {
            TaskKey::Transfer { task_id } => task_id,
            other => panic!("expected transfer task key, got {:?}", other),
        };
        let lost: Vec<i64> = v.pending_task_infos.keys().copied().filter(|id| *id <= min_read_task_id).collect();
        for id in lost {
            let info = v.pending_task_infos.remove(&id).unwrap();
            tracing::error!(
                task_id = id,
                task_visibility_timestamp = info.task.get_visibility_timestamp().unix_nano(),
                failover_version = info.task.get_version(),
                task_type = info.task.get_task_type(),
                domain_id = %info.execution_info.domain_id,
                workflow_id = %info.execution_info.workflow_id,
                run_id = %info.execution_info.run_id,
                potential_false_positive = info.potential_false_positive,
                "Failed to load transfer task"
            );
            self.metrics_scope.inc_counter("QueueValidatorLostTaskCounter");
        }
        v.min_read_task_id = min_read_task_id;
    }
}

#[allow(dead_code)]
fn _unused(_: Option<TaskRef>) {}
