//! Port of queuev2/queue_state.go

use std::collections::BTreeMap;

use super::deps::{HistoryTaskKey, PersistenceTask};
use super::predicate::Predicate;
use super::predicate_operation::{and, not};
use super::range::Range;

#[derive(Clone, Debug)]
pub struct QueueState {
    pub virtual_queue_states: BTreeMap<i64, Vec<VirtualSliceState>>,
    pub exclusive_max_read_level: HistoryTaskKey,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VirtualSliceState {
    pub range: Range,
    pub predicate: Predicate,
}

impl VirtualSliceState {
    pub fn is_empty(&self) -> bool {
        self.range.is_empty() || self.predicate.is_empty()
    }

    pub fn contains<T: PersistenceTask + ?Sized>(&self, task: &T) -> bool {
        self.range.contains(task.get_task_key()) && self.predicate.check(task)
    }

    pub fn try_split_by_task_key(
        &self,
        k: HistoryTaskKey,
    ) -> Option<(VirtualSliceState, VirtualSliceState)> {
        if !self.range.can_split_by_task_key(k) {
            return None;
        }
        Some((
            VirtualSliceState {
                range: Range {
                    inclusive_min_task_key: self.range.inclusive_min_task_key,
                    exclusive_max_task_key: k,
                },
                predicate: self.predicate.clone(),
            },
            VirtualSliceState {
                range: Range {
                    inclusive_min_task_key: k,
                    exclusive_max_task_key: self.range.exclusive_max_task_key,
                },
                predicate: self.predicate.clone(),
            },
        ))
    }

    pub fn try_split_by_predicate(
        &self,
        predicate: &Predicate,
    ) -> Option<(VirtualSliceState, VirtualSliceState)> {
        if predicate.equals(&Predicate::Universal)
            || predicate.equals(&Predicate::Empty)
            || predicate.equals(&self.predicate)
        {
            return None;
        }
        Some((
            VirtualSliceState {
                range: self.range,
                predicate: and(&self.predicate, predicate),
            },
            VirtualSliceState {
                range: self.range,
                predicate: and(&self.predicate, &not(predicate)),
            },
        ))
    }
}
