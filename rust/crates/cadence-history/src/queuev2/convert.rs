//! Port of queuev2/convert.go

use std::collections::BTreeMap;

use super::deps::*;
use super::predicate::Predicate;
use super::queue_state::{QueueState, VirtualSliceState};
use super::range::Range;

pub fn from_persistence_queue_state(state: &TypesQueueState) -> QueueState {
    let mut m = BTreeMap::new();
    for (k, v) in &state.virtual_queue_states {
        m.insert(*k, from_persistence_virtual_queue_state(v));
    }
    QueueState {
        virtual_queue_states: m,
        exclusive_max_read_level: from_persistence_task_key(&state.exclusive_max_read_level),
    }
}

pub fn to_persistence_queue_state(state: &QueueState) -> TypesQueueState {
    let mut m = BTreeMap::new();
    for (k, v) in &state.virtual_queue_states {
        m.insert(*k, to_persistence_virtual_queue_state(v));
    }
    TypesQueueState {
        virtual_queue_states: m,
        exclusive_max_read_level: to_persistence_task_key(state.exclusive_max_read_level),
    }
}

pub fn from_persistence_virtual_queue_state(s: &TypesVirtualQueueState) -> Vec<VirtualSliceState> {
    s.virtual_slice_states
        .iter()
        .map(from_persistence_virtual_slice_state)
        .collect()
}

pub fn to_persistence_virtual_queue_state(s: &[VirtualSliceState]) -> TypesVirtualQueueState {
    TypesVirtualQueueState {
        virtual_slice_states: s.iter().map(to_persistence_virtual_slice_state).collect(),
    }
}

pub fn from_persistence_virtual_slice_state(s: &TypesVirtualSliceState) -> VirtualSliceState {
    VirtualSliceState {
        range: from_persistence_task_range(&s.task_range),
        predicate: from_persistence_predicate(s.predicate.as_ref()),
    }
}

pub fn to_persistence_virtual_slice_state(s: &VirtualSliceState) -> TypesVirtualSliceState {
    TypesVirtualSliceState {
        task_range: to_persistence_task_range(s.range),
        predicate: Some(to_persistence_predicate(&s.predicate)),
    }
}

pub fn from_persistence_task_range(r: &TypesTaskRange) -> Range {
    Range {
        inclusive_min_task_key: from_persistence_task_key(&r.inclusive_min),
        exclusive_max_task_key: from_persistence_task_key(&r.exclusive_max),
    }
}

pub fn to_persistence_task_range(r: Range) -> TypesTaskRange {
    TypesTaskRange {
        inclusive_min: to_persistence_task_key(r.inclusive_min_task_key),
        exclusive_max: to_persistence_task_key(r.exclusive_max_task_key),
    }
}

pub fn from_persistence_task_key(k: &TypesTaskKey) -> HistoryTaskKey {
    HistoryTaskKey::new(unix_nano_time(k.scheduled_time_nano), k.task_id)
}

pub fn to_persistence_task_key(k: HistoryTaskKey) -> TypesTaskKey {
    TypesTaskKey {
        task_id: k.get_task_id(),
        scheduled_time_nano: time_unix_nano(&k.get_scheduled_time()),
    }
}

pub fn from_persistence_predicate(p: Option<&TypesPredicate>) -> Predicate {
    match p {
        None | Some(TypesPredicate::Universal) => Predicate::new_universal(),
        Some(TypesPredicate::Empty) => Predicate::new_empty(),
        Some(TypesPredicate::DomainId {
            domain_ids,
            is_exclusive,
        }) => Predicate::new_domain_id(domain_ids, is_exclusive.unwrap_or(false)),
    }
}

pub fn to_persistence_predicate(p: &Predicate) -> TypesPredicate {
    match p {
        Predicate::Universal => TypesPredicate::Universal,
        Predicate::Empty => TypesPredicate::Empty,
        Predicate::DomainId {
            domain_ids,
            is_exclusive,
        } => TypesPredicate::DomainId {
            // BTreeSet iterates sorted, matching Go's slices.Sort.
            domain_ids: domain_ids.iter().cloned().collect(),
            is_exclusive: Some(*is_exclusive),
        },
    }
}
