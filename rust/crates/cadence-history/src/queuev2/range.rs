//! Port of queuev2/range.go

use super::deps::HistoryTaskKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    pub inclusive_min_task_key: HistoryTaskKey,
    pub exclusive_max_task_key: HistoryTaskKey,
}

impl Range {
    pub fn is_empty(&self) -> bool {
        self.inclusive_min_task_key
            .compare(&self.exclusive_max_task_key)
            >= 0
    }
    pub fn contains(&self, k: HistoryTaskKey) -> bool {
        k.compare(&self.inclusive_min_task_key) >= 0 && k.compare(&self.exclusive_max_task_key) < 0
    }
    pub fn contains_range(&self, o: Range) -> bool {
        self.inclusive_min_task_key
            .compare(&o.inclusive_min_task_key)
            <= 0
            && self
                .exclusive_max_task_key
                .compare(&o.exclusive_max_task_key)
                >= 0
    }
    pub fn can_merge(&self, o: Range) -> bool {
        self.inclusive_min_task_key
            .compare(&o.exclusive_max_task_key)
            <= 0
            && self
                .exclusive_max_task_key
                .compare(&o.inclusive_min_task_key)
                >= 0
    }
    pub fn can_split_by_task_key(&self, k: HistoryTaskKey) -> bool {
        k.compare(&self.inclusive_min_task_key) > 0 && k.compare(&self.exclusive_max_task_key) < 0
    }
}
