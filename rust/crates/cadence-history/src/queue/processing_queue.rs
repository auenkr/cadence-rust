//! Port of `go/service/history/queue/processing_queue.go`.
//! Go's `ProcessingQueue` interface is collapsed into the single `ProcessingQueue` struct.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::deps::{TaskKey, TaskRef, TaskState};
use super::processing_queue_state::ProcessingQueueState;
use super::split_policy::ProcessingQueueSplitPolicy;

pub const ERR_TASK_NOT_FOUND: &str = "task not found";

static NEXT_QUEUE_ID: AtomicU64 = AtomicU64::new(1);

pub struct ProcessingQueue {
    /// Replaces Go pointer identity.
    pub id: u64,
    pub(crate) state: ProcessingQueueState,
    pub(crate) outstanding_tasks: HashMap<TaskKey, TaskRef>,
}

impl ProcessingQueue {
    /// Go `NewProcessingQueue`.
    pub fn new(state: ProcessingQueueState) -> ProcessingQueue {
        Self::new_with_tasks(state, None)
    }

    /// Go `newProcessingQueue`.
    pub(crate) fn new_with_tasks(
        state: ProcessingQueueState,
        outstanding_tasks: Option<HashMap<TaskKey, TaskRef>>,
    ) -> ProcessingQueue {
        if state.read_level.less(&state.ack_level) {
            tracing::error!(
                "ack level larger than readlevel when creating processing queue: ack level: {:?}, read level: {:?}",
                state.ack_level,
                state.read_level
            );
            panic!("ack level larger than readlevel when creating processing queue");
        }
        ProcessingQueue {
            id: NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed),
            state,
            outstanding_tasks: outstanding_tasks.unwrap_or_default(),
        }
    }

    pub fn state(&self) -> &ProcessingQueueState {
        &self.state
    }

    /// Go `Split`; consumes the queue and returns the resulting queues.
    pub fn split(self, policy: &dyn ProcessingQueueSplitPolicy) -> Vec<ProcessingQueue> {
        let new_states = policy.evaluate(&self);
        if new_states.is_empty() {
            return vec![self];
        }
        split_processing_queue(vec![self], new_states)
    }

    /// Go `Merge`; consumes both queues.
    pub fn merge(self, other: ProcessingQueue) -> Vec<ProcessingQueue> {
        let (mut q1, mut q2) = (self, other);

        if q1.state.level != q2.state.level {
            let msg = "Processing queue encountered a queue from different level during merge";
            tracing::error!(
                "{}: current queue level: {}, incoming queue level: {}",
                msg,
                q1.state.level,
                q2.state.level
            );
            panic!("{}", msg);
        }

        if !q1.state.ack_level.less(&q2.state.max_level) || !q2.state.ack_level.less(&q1.state.max_level) {
            return vec![q1, q2];
        }

        let mut new_states: Vec<ProcessingQueueState> = Vec::new();
        if !task_key_equals(&q1.state.ack_level, &q2.state.ack_level) {
            if q2.state.ack_level.less(&q1.state.ack_level) {
                std::mem::swap(&mut q1, &mut q2);
            }
            new_states.push(ProcessingQueueState::new_full(
                q1.state.level,
                q1.state.ack_level,
                min_task_key(q1.state.read_level, q2.state.ack_level),
                q2.state.ack_level,
                q1.state.domain_filter.copy(),
            ));
        }

        if !task_key_equals(&q1.state.max_level, &q2.state.max_level) {
            if q1.state.max_level.less(&q2.state.max_level) {
                std::mem::swap(&mut q1, &mut q2);
            }
            new_states.push(ProcessingQueueState::new_full(
                q1.state.level,
                q2.state.max_level,
                max_task_key(q1.state.read_level, q2.state.max_level),
                q1.state.max_level,
                q1.state.domain_filter.copy(),
            ));
        }

        let overlapping_ack = max_task_key(q1.state.ack_level, q2.state.ack_level);
        new_states.push(ProcessingQueueState::new_full(
            q1.state.level,
            overlapping_ack,
            max_task_key(min_task_key(q1.state.read_level, q2.state.read_level), overlapping_ack),
            min_task_key(q1.state.max_level, q2.state.max_level),
            q1.state.domain_filter.merge(&q2.state.domain_filter),
        ));

        for s in &new_states {
            if s.read_level.less(&s.ack_level) || s.max_level.less(&s.read_level) {
                tracing::error!(
                    "invalid processing queue merge result: q1: {:?}, q2: {:?}, merge result: {:?}",
                    q1.state,
                    q2.state,
                    new_states
                );
                panic!("invalid processing queue merge result");
            }
        }

        split_processing_queue(vec![q1, q2], new_states)
    }

    /// Go `AddTasks`.
    pub fn add_tasks(&mut self, tasks: HashMap<TaskKey, TaskRef>, new_read_level: TaskKey) {
        if new_read_level.less(&self.state.read_level) {
            tracing::error!(
                "processing queue read level moved backward: current read level: {:?}, new read level: {:?}",
                self.state.read_level,
                new_read_level
            );
            panic!("processing queue read level moved backward");
        }

        for (key, task) in tasks {
            if self.outstanding_tasks.contains_key(&key) {
                tracing::debug!(
                    "Skipping task: {:?}. DomainID: {}, WorkflowID: {}, RunID: {}, Type: {}",
                    key,
                    task.get_domain_id(),
                    task.get_workflow_id(),
                    task.get_run_id(),
                    task.get_task_type()
                );
                continue;
            }

            if !task_belongs_to_process_queue(&self.state, &key, task.as_ref()) {
                tracing::error!(
                    "Processing queue encountered a task doesn't belong to its scope: processing queue state: {:?}, key: {:?}",
                    self.state,
                    key
                );
                continue;
            }

            self.outstanding_tasks.insert(key, task);
        }

        self.state.read_level = new_read_level;
    }

    /// Go `GetTask`.
    pub fn get_task(&self, key: &TaskKey) -> Result<TaskRef, &'static str> {
        self.outstanding_tasks.get(key).cloned().ok_or(ERR_TASK_NOT_FOUND)
    }

    /// Go `GetTasks`.
    pub fn get_tasks(&self) -> Vec<TaskRef> {
        self.outstanding_tasks.values().cloned().collect()
    }

    /// Go `UpdateAckLevel`: returns the new ack level and the number of pending tasks.
    pub fn update_ack_level(&mut self) -> (TaskKey, usize) {
        let mut keys: Vec<TaskKey> = self.outstanding_tasks.keys().copied().collect();
        keys.sort_by(|a, b| {
            if a.less(b) {
                std::cmp::Ordering::Less
            } else if b.less(a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        // Go: `for idx, key = range keys {...}` leaves idx at the break position, or at the
        // last index if the loop ran to completion (0 for empty keys).
        let mut idx: usize = 0;
        for (i, key) in keys.iter().enumerate() {
            idx = i;
            if self.state.read_level.less(key) {
                break;
            }
            if self.outstanding_tasks[key].state() != TaskState::Acked {
                break;
            }
            self.state.ack_level = *key;
            self.outstanding_tasks.remove(key);
        }

        while idx + 1 < keys.len() && keys[idx].less(&self.state.read_level) {
            let a = self.outstanding_tasks.get(&keys[idx]).map(|t| t.state());
            let b = self.outstanding_tasks.get(&keys[idx + 1]).map(|t| t.state());
            if a == Some(TaskState::Acked) && b == Some(TaskState::Acked) {
                self.outstanding_tasks.remove(&keys[idx]);
            }
            idx += 1;
        }

        if self.outstanding_tasks.is_empty() {
            self.state.ack_level = self.state.read_level;
        }

        if let TaskKey::Timer { visibility_timestamp, .. } = self.state.ack_level {
            self.state.ack_level = TaskKey::Timer { visibility_timestamp, task_id: 0 };
        }

        if self.state.read_level.less(&self.state.ack_level) {
            tracing::error!("ack level moved beyond read level: processing queue state: {:?}", self.state);
            panic!("ack level moved beyond read level");
        }

        (self.state.ack_level, self.outstanding_tasks.len())
    }
}

/// Go `splitProcessingQueue`. On inconsistency the original queues are returned unchanged.
pub(crate) fn split_processing_queue(
    queues: Vec<ProcessingQueue>,
    new_states: Vec<ProcessingQueueState>,
) -> Vec<ProcessingQueue> {
    let mut new_tasks: Vec<HashMap<TaskKey, TaskRef>> = (0..new_states.len()).map(|_| HashMap::new()).collect();

    let mut failed = false;
    'outer: for queue in &queues {
        for (key, task) in &queue.outstanding_tasks {
            let mut placed = false;
            for (i, state) in new_states.iter().enumerate() {
                if task_belongs_to_process_queue(state, key, task.as_ref()) {
                    new_tasks[i].insert(*key, task.clone());
                    placed = true;
                    break;
                }
            }
            if !placed {
                let current: Vec<&ProcessingQueueState> = queues.iter().map(|q| &q.state).collect();
                tracing::error!(
                    "Processing queue encountered an error during split or merge. current queue state: {:?}, new queue state: {:?}",
                    current,
                    new_states
                );
                failed = true;
                break 'outer;
            }
        }
    }
    if failed {
        return queues;
    }

    new_states
        .into_iter()
        .zip(new_tasks)
        .map(|(state, tasks)| ProcessingQueue::new_with_tasks(state, Some(tasks)))
        .collect()
}

/// Go `taskBelongsToProcessQueue`.
pub fn task_belongs_to_process_queue(state: &ProcessingQueueState, key: &TaskKey, task: &dyn super::deps::Task) -> bool {
    state.domain_filter.filter(&task.get_domain_id()) && state.ack_level.less(key) && !state.max_level.less(key)
}

pub fn task_key_equals(a: &TaskKey, b: &TaskKey) -> bool {
    !a.less(b) && !b.less(a)
}

pub fn min_task_key(a: TaskKey, b: TaskKey) -> TaskKey {
    if a.less(&b) {
        a
    } else {
        b
    }
}

pub fn max_task_key(a: TaskKey, b: TaskKey) -> TaskKey {
    if a.less(&b) {
        b
    } else {
        a
    }
}
