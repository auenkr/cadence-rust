//! Port of queuev2/pending_task_tracker.go

use std::collections::HashMap;

use super::deps::{HTask, HistoryTaskKey, TaskState};

pub trait PendingTaskTracker: Send {
    fn add_task(&mut self, t: HTask);
    fn prune_acked_tasks(&mut self) -> usize;
    fn get_minimum_task_key(&self) -> (HistoryTaskKey, bool);
    fn get_tasks(&self) -> &HashMap<HistoryTaskKey, HTask>;
    fn get_pending_task_count(&self) -> usize;
    fn get_per_domain_pending_task_count(&self) -> &HashMap<String, i64>;
    fn clear(&mut self);
}

pub struct PendingTaskTrackerImpl {
    task_map: HashMap<HistoryTaskKey, HTask>,
    task_count_per_domain: HashMap<String, i64>,
    min_task_key: HistoryTaskKey,
}

pub fn new_pending_task_tracker() -> Box<dyn PendingTaskTracker> {
    Box::new(PendingTaskTrackerImpl::new())
}

impl PendingTaskTrackerImpl {
    pub fn new() -> Self {
        Self {
            task_map: HashMap::new(),
            task_count_per_domain: HashMap::new(),
            min_task_key: HistoryTaskKey::maximum(),
        }
    }
}

impl Default for PendingTaskTrackerImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingTaskTracker for PendingTaskTrackerImpl {
    fn add_task(&mut self, t: HTask) {
        let key = t.get_task_key();
        if self.task_map.is_empty() || self.min_task_key.compare(&key) > 0 {
            self.min_task_key = key;
        }
        *self
            .task_count_per_domain
            .entry(t.get_domain_id())
            .or_insert(0) += 1;
        self.task_map.insert(key, t);
    }

    fn get_minimum_task_key(&self) -> (HistoryTaskKey, bool) {
        if self.task_map.is_empty() {
            return (HistoryTaskKey::maximum(), false);
        }
        (self.min_task_key, true)
    }

    fn get_tasks(&self) -> &HashMap<HistoryTaskKey, HTask> {
        &self.task_map
    }

    fn prune_acked_tasks(&mut self) -> usize {
        let mut pruned = 0;
        let mut min_key = HistoryTaskKey::maximum();
        let acked: Vec<HistoryTaskKey> = self
            .task_map
            .iter()
            .filter(|(_, t)| t.state() == TaskState::Acked)
            .map(|(k, _)| *k)
            .collect();
        for k in acked {
            if let Some(t) = self.task_map.remove(&k) {
                *self
                    .task_count_per_domain
                    .entry(t.get_domain_id())
                    .or_insert(0) -= 1;
                pruned += 1;
            }
        }
        for k in self.task_map.keys() {
            if k.compare(&min_key) < 0 {
                min_key = *k;
            }
        }
        self.min_task_key = min_key;
        pruned
    }

    fn get_pending_task_count(&self) -> usize {
        self.task_map.len()
    }

    fn get_per_domain_pending_task_count(&self) -> &HashMap<String, i64> {
        &self.task_count_per_domain
    }

    fn clear(&mut self) {
        for t in self.task_map.values() {
            t.cancel();
        }
        self.task_map.clear();
        self.task_count_per_domain.clear();
        self.min_task_key = HistoryTaskKey::maximum();
    }
}
