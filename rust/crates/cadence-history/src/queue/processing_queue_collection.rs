//! Port of `go/service/history/queue/processing_queue_collection.go`.
//! `activeQueue` (a Go pointer) is tracked by queue id.
use std::collections::HashMap;

use super::deps::{TaskKey, TaskRef};
use super::processing_queue::{min_task_key, task_key_equals, ProcessingQueue, ERR_TASK_NOT_FOUND};
use super::processing_queue_state::ProcessingQueueState;
use super::split_policy::ProcessingQueueSplitPolicy;

pub struct ProcessingQueueCollection {
    level: i32,
    queues: Vec<ProcessingQueue>,
    active_queue: Option<u64>,
}

impl ProcessingQueueCollection {
    /// Go `NewProcessingQueueCollection`.
    pub fn new(level: i32, mut queues: Vec<ProcessingQueue>) -> Self {
        sort_processing_queue(&mut queues);
        let mut c = ProcessingQueueCollection { level, queues, active_queue: None };
        c.reset_active_queue();
        c
    }

    /// Go `newProcessingQueueCollections`.
    pub fn new_collections(states: Vec<ProcessingQueueState>) -> Vec<ProcessingQueueCollection> {
        let mut map: HashMap<i32, Vec<ProcessingQueue>> = HashMap::new();
        for s in states {
            map.entry(s.level).or_default().push(ProcessingQueue::new(s));
        }
        map.into_iter().map(|(level, queues)| Self::new(level, queues)).collect()
    }

    pub fn level(&self) -> i32 {
        self.level
    }

    pub fn queues(&self) -> &[ProcessingQueue] {
        &self.queues
    }

    pub fn active_queue(&self) -> Option<&ProcessingQueue> {
        let id = self.active_queue?;
        self.queues.iter().find(|q| q.id == id)
    }

    pub fn add_tasks(&mut self, tasks: HashMap<TaskKey, TaskRef>, new_read_level: TaskKey) {
        let id = self.active_queue.expect("AddTasks called without an active queue");
        let q = self.queues.iter_mut().find(|q| q.id == id).expect("active queue missing");
        q.add_tasks(tasks, new_read_level);
        if task_key_equals(&q.state().read_level, &q.state().max_level) {
            self.reset_active_queue();
        }
    }

    pub fn get_task(&self, key: &TaskKey) -> Result<TaskRef, &'static str> {
        for q in &self.queues {
            if let Ok(t) = q.get_task(key) {
                return Ok(t);
            }
        }
        Err(ERR_TASK_NOT_FOUND)
    }

    pub fn get_tasks(&self) -> Vec<TaskRef> {
        self.queues.iter().flat_map(|q| q.get_tasks()).collect()
    }

    /// Returns min of all new ack levels (None if no queue remains) and total pending tasks.
    pub fn update_ack_levels(&mut self) -> (Option<TaskKey>, usize) {
        if self.queues.is_empty() {
            return (None, 0);
        }
        let mut remaining = Vec::with_capacity(self.queues.len());
        let mut total = 0;
        let mut min_ack: Option<TaskKey> = None;
        for mut q in std::mem::take(&mut self.queues) {
            let (ack, pending) = q.update_ack_level();
            if task_key_equals(&ack, &q.state().max_level) {
                continue;
            }
            total += pending;
            min_ack = Some(match min_ack {
                None => ack,
                Some(m) => min_task_key(m, ack),
            });
            remaining.push(q);
        }
        self.queues = remaining;
        (min_ack, total)
    }

    /// Returns queues that moved to another level.
    pub fn split(&mut self, policy: &dyn ProcessingQueueSplitPolicy) -> Vec<ProcessingQueue> {
        if self.queues.is_empty() {
            return Vec::new();
        }
        let mut new_queues = Vec::with_capacity(self.queues.len());
        let mut next_level = Vec::new();
        for q in std::mem::take(&mut self.queues) {
            let mut split = q.split(policy);
            sort_processing_queue(&mut split);
            for sq in split {
                if sq.state().level != self.level {
                    next_level.push(sq);
                } else {
                    new_queues.push(sq);
                }
            }
        }
        self.queues = new_queues;
        self.reset_active_queue();
        next_level
    }

    pub fn merge(&mut self, mut incoming: Vec<ProcessingQueue>) {
        sort_processing_queue(&mut incoming);
        let mut current: Vec<Option<ProcessingQueue>> =
            std::mem::take(&mut self.queues).into_iter().map(Some).collect();
        let mut incoming: Vec<Option<ProcessingQueue>> = incoming.into_iter().map(Some).collect();

        let mut new_queues: Vec<ProcessingQueue> = Vec::new();
        let (mut ci, mut ii) = (0usize, 0usize);
        while ii < incoming.len() && ci < current.len() {
            let cq = current[ci].take().unwrap();
            let iq = incoming[ii].take().unwrap();
            let mut merged = cq.merge(iq);
            sort_processing_queue(&mut merged);
            let last = merged.pop().expect("merge returned no queues");
            new_queues.extend(merged);

            let overlap_current = ci + 1 != current.len()
                && current[ci + 1].as_ref().unwrap().state().ack_level.less(&last.state().max_level);
            let overlap_incoming = ii + 1 != incoming.len()
                && incoming[ii + 1].as_ref().unwrap().state().ack_level.less(&last.state().max_level);

            if !overlap_current && !overlap_incoming {
                new_queues.push(last);
                ii += 1;
                ci += 1;
            } else if overlap_current {
                incoming[ii] = Some(last);
                ci += 1;
            } else {
                current[ci] = Some(last);
                ii += 1;
            }
        }
        new_queues.extend(incoming.into_iter().skip(ii).flatten());
        new_queues.extend(current.into_iter().skip(ci).flatten());
        self.queues = new_queues;

        for i in 0..self.queues.len().saturating_sub(1) {
            if self.queues[i + 1].state().ack_level.less(&self.queues[i].state().max_level) {
                let mut msg = String::new();
                for q in &self.queues {
                    msg += &format!("{:?} ", q.state());
                }
                panic!("invalid processing queue merge result: {}", msg);
            }
        }
        self.reset_active_queue();
    }

    pub(crate) fn queues_mut(&mut self) -> &mut Vec<ProcessingQueue> {
        &mut self.queues
    }

    fn reset_active_queue(&mut self) {
        self.active_queue = self
            .queues
            .iter()
            .find(|q| !task_key_equals(&q.state().read_level, &q.state().max_level))
            .map(|q| q.id);
    }
}

/// Go `sortProcessingQueue`: by level, then ack level.
pub fn sort_processing_queue(queues: &mut [ProcessingQueue]) {
    queues.sort_by(|a, b| {
        if a.state().level == b.state().level {
            if a.state().ack_level.less(&b.state().ack_level) {
                std::cmp::Ordering::Less
            } else if b.state().ack_level.less(&a.state().ack_level) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        } else {
            a.state().level.cmp(&b.state().level)
        }
    });
}
