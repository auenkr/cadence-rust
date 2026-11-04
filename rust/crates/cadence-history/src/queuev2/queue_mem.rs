//! Port of queuev2/queue_mem.go

use super::deps::*;
use super::predicate::Predicate;

/// Go `InMemQueue`.
#[allow(clippy::len_without_is_empty)]
pub trait InMemQueue: Send {
    fn put_tasks(&mut self, tasks: &[PTask]);
    fn look_ahead(&self, min_task_key: HistoryTaskKey) -> Option<PTask>;
    fn get_tasks(
        &self,
        inclusive_min_task_key: HistoryTaskKey,
        exclusive_max_task_key: HistoryTaskKey,
        predicate: &Predicate,
        page_size: i64,
    ) -> (Vec<PTask>, HistoryTaskKey);
    fn l_trim(&mut self, new_inclusive_min_task_key: HistoryTaskKey);
    fn r_trim_by_size(&mut self, max_size: i64) -> (HistoryTaskKey, bool);
    fn l_trim_by_size(&mut self, max_size: i64) -> (HistoryTaskKey, bool);
    fn clear(&mut self);
    fn len(&self) -> usize;
}

#[derive(Default)]
pub struct InMemQueueImpl {
    tasks: Vec<PTask>,
}

pub fn new_in_mem_queue() -> InMemQueueImpl {
    InMemQueueImpl::default()
}

impl InMemQueueImpl {
    fn put_task(&mut self, task: PTask) {
        let key = task.get_task_key();
        let n = self.tasks.len();

        if n == 0 || self.tasks[n - 1].get_task_key().compare(&key) < 0 {
            self.tasks.push(task);
            return;
        }

        let pos = self.find_task_position(key);
        if pos < n && self.tasks[pos].get_task_key().compare(&key) == 0 {
            return; // duplicate
        }
        self.tasks.insert(pos, task);
    }

    fn find_task_position(&self, task_key: HistoryTaskKey) -> usize {
        self.tasks
            .partition_point(|t| t.get_task_key().compare(&task_key) < 0)
    }
}

impl InMemQueue for InMemQueueImpl {
    fn put_tasks(&mut self, tasks: &[PTask]) {
        for t in tasks {
            self.put_task(t.clone());
        }
    }

    fn look_ahead(&self, min_task_key: HistoryTaskKey) -> Option<PTask> {
        let pos = self.find_task_position(min_task_key);
        self.tasks.get(pos).cloned()
    }

    fn get_tasks(
        &self,
        inclusive_min_task_key: HistoryTaskKey,
        exclusive_max_task_key: HistoryTaskKey,
        predicate: &Predicate,
        page_size: i64,
    ) -> (Vec<PTask>, HistoryTaskKey) {
        if page_size <= 0 {
            return (Vec::new(), inclusive_min_task_key);
        }
        let page_size = page_size as usize;
        let start = self.find_task_position(inclusive_min_task_key);

        let mut tasks: Vec<PTask> = Vec::new();
        for t in &self.tasks[start..] {
            if t.get_task_key().compare(&exclusive_max_task_key) >= 0 {
                break;
            }
            if !predicate.check(t.as_ref()) {
                continue;
            }
            tasks.push(t.clone());
            if tasks.len() > page_size {
                let next = tasks[page_size - 1].get_task_key().next();
                tasks.truncate(page_size);
                return (tasks, next);
            }
        }
        (tasks, exclusive_max_task_key)
    }

    fn l_trim(&mut self, new_inclusive_min_task_key: HistoryTaskKey) {
        let pos = self.find_task_position(new_inclusive_min_task_key);
        if pos == 0 {
            return;
        }
        self.tasks = self.tasks.split_off(pos);
    }

    fn r_trim_by_size(&mut self, max_size: i64) -> (HistoryTaskKey, bool) {
        if self.tasks.is_empty() {
            return (HistoryTaskKey::minimum(), false);
        }
        if max_size <= 0 {
            self.tasks.clear();
            return (HistoryTaskKey::minimum(), true);
        }
        let trimmed = self.tasks.len() as i64 > max_size;
        if trimmed {
            self.tasks.truncate(max_size as usize);
        }
        (self.tasks[self.tasks.len() - 1].get_task_key().next(), trimmed)
    }

    fn l_trim_by_size(&mut self, max_size: i64) -> (HistoryTaskKey, bool) {
        if self.tasks.is_empty() {
            return (HistoryTaskKey::minimum(), false);
        }
        if max_size <= 0 {
            self.tasks.clear();
            return (HistoryTaskKey::minimum(), true);
        }
        if self.tasks.len() as i64 <= max_size {
            return (HistoryTaskKey::minimum(), false);
        }
        let drop = self.tasks.len() - max_size as usize;
        self.tasks = self.tasks.split_off(drop);
        (self.tasks[0].get_task_key(), true)
    }

    fn clear(&mut self) {
        self.tasks.clear();
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }
}
