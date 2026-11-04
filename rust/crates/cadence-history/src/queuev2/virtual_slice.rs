//! Port of queuev2/virtual_slice.go
//!
//! Go's `virtualSliceImpl` has no internal locking (the owning queue serialises
//! access). Here the mutable parts live behind a std `Mutex` which is never held
//! across an `.await`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::deps::*;
use super::pending_task_tracker::{new_pending_task_tracker, PendingTaskTracker};
use super::predicate::Predicate;
use super::predicate_operation::or;
use super::queue_reader::{GetTaskProgress, GetTaskRequest, QueueReader};
use super::queue_state::VirtualSliceState;
use super::range::Range;

#[derive(Clone, Debug, Default)]
pub struct PendingTaskStats {
    pub pending_task_count_per_domain: HashMap<String, i64>,
}

#[async_trait]
pub trait VirtualSlice: Send + Sync {
    fn get_state(&self) -> VirtualSliceState;
    fn is_empty(&self) -> bool;
    async fn get_tasks(&self, page_size: usize) -> Result<Vec<HTask>, DepError>;
    fn has_more_tasks(&self) -> bool;
    fn update_and_get_state(&self) -> VirtualSliceState;
    fn get_pending_task_count(&self) -> usize;
    fn get_read_level(&self) -> HistoryTaskKey;
    fn clear(&self);
    fn pending_task_stats(&self) -> PendingTaskStats;

    fn try_split_by_task_key(
        &self,
        k: HistoryTaskKey,
    ) -> Option<(Arc<dyn VirtualSlice>, Arc<dyn VirtualSlice>)>;
    fn try_split_by_predicate(
        &self,
        p: &Predicate,
    ) -> Option<(Arc<dyn VirtualSlice>, Arc<dyn VirtualSlice>)>;
    fn try_merge_with_virtual_slice(
        &self,
        other: &Arc<dyn VirtualSlice>,
    ) -> Option<Vec<Arc<dyn VirtualSlice>>>;

    fn as_any(&self) -> &dyn Any;
}

/// Identity of a slice (Go uses the interface pointer as a map key).
pub fn slice_key(s: &Arc<dyn VirtualSlice>) -> usize {
    Arc::as_ptr(s) as *const () as usize
}

struct Inner {
    state: VirtualSliceState,
    pending_task_tracker: Box<dyn PendingTaskTracker>,
    progress: Vec<GetTaskProgress>,
}

pub struct VirtualSliceImpl {
    task_initializer: TaskInitializer,
    queue_reader: Arc<dyn QueueReader>,
    logger: Arc<dyn Logger>,
    inner: Mutex<Inner>,
}

/// Cloned view of a slice used by split / merge.
struct Parts {
    state: VirtualSliceState,
    tasks: Vec<(HistoryTaskKey, HTask)>,
    progress: Vec<GetTaskProgress>,
}

fn initial_progress(state: &VirtualSliceState) -> Vec<GetTaskProgress> {
    vec![GetTaskProgress {
        range: state.range,
        next_page_token: None,
        next_task_key: state.range.inclusive_min_task_key,
    }]
}

fn prog(min: HistoryTaskKey, max: HistoryTaskKey, next: HistoryTaskKey) -> GetTaskProgress {
    GetTaskProgress {
        range: Range {
            inclusive_min_task_key: min,
            exclusive_max_task_key: max,
        },
        next_page_token: None,
        next_task_key: next,
    }
}

pub fn new_virtual_slice(
    state: VirtualSliceState,
    task_initializer: TaskInitializer,
    queue_reader: Arc<dyn QueueReader>,
    pending_task_tracker: Box<dyn PendingTaskTracker>,
    logger: Arc<dyn Logger>,
) -> Arc<dyn VirtualSlice> {
    let progress = initial_progress(&state);
    Arc::new(VirtualSliceImpl {
        task_initializer,
        queue_reader,
        logger,
        inner: Mutex::new(Inner {
            state,
            pending_task_tracker,
            progress,
        }),
    })
}

impl VirtualSliceImpl {
    fn build(
        &self,
        state: VirtualSliceState,
        tasks: impl IntoIterator<Item = HTask>,
        progress: Vec<GetTaskProgress>,
    ) -> VirtualSliceImpl {
        let mut tracker = new_pending_task_tracker();
        for t in tasks {
            tracker.add_task(t);
        }
        VirtualSliceImpl {
            task_initializer: self.task_initializer.clone(),
            queue_reader: self.queue_reader.clone(),
            logger: self.logger.clone(),
            inner: Mutex::new(Inner {
                state,
                pending_task_tracker: tracker,
                progress,
            }),
        }
    }

    fn parts(&self) -> Parts {
        let inner = self.inner.lock().unwrap();
        Parts {
            state: inner.state.clone(),
            tasks: inner
                .pending_task_tracker
                .get_tasks()
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            progress: inner.progress.clone(),
        }
    }

    fn split_by_task_key(&self, task_key: HistoryTaskKey) -> Option<(VirtualSliceImpl, VirtualSliceImpl)> {
        let p = self.parts();
        let (left_state, right_state) = p.state.try_split_by_task_key(task_key)?;

        let mut left_tasks = Vec::new();
        let mut right_tasks = Vec::new();
        for (k, t) in &p.tasks {
            if left_state.range.contains(*k) {
                left_tasks.push(t.clone());
            } else {
                right_tasks.push(t.clone());
            }
        }

        let mut left_progress = Vec::new();
        let mut right_progress = Vec::new();
        for progress in &p.progress {
            if left_state.range.contains_range(progress.range) {
                left_progress.push(progress.clone());
                continue;
            }
            if right_state.range.contains_range(progress.range) {
                right_progress.push(progress.clone());
                continue;
            }
            if left_state.range.contains(progress.next_task_key) {
                left_progress.push(prog(
                    progress.next_task_key,
                    left_state.range.exclusive_max_task_key,
                    progress.next_task_key,
                ));
                right_progress.push(prog(
                    right_state.range.inclusive_min_task_key,
                    progress.range.exclusive_max_task_key,
                    right_state.range.inclusive_min_task_key,
                ));
            } else {
                right_progress.push(prog(
                    progress.next_task_key,
                    progress.range.exclusive_max_task_key,
                    progress.next_task_key,
                ));
            }
        }

        Some((
            self.build(left_state, left_tasks, left_progress),
            self.build(right_state, right_tasks, right_progress),
        ))
    }

    fn merge_by_range(left: &VirtualSliceImpl, right: &VirtualSliceImpl) -> VirtualSliceImpl {
        let (mut l, mut r) = (left.parts(), right.parts());
        let mut left = left;
        if l.state.range.inclusive_min_task_key.compare(&r.state.range.inclusive_min_task_key) > 0 {
            std::mem::swap(&mut l, &mut r);
            left = right;
        }
        let merged_state = VirtualSliceState {
            range: Range {
                inclusive_min_task_key: l.state.range.inclusive_min_task_key,
                exclusive_max_task_key: max_history_task_key(
                    l.state.range.exclusive_max_task_key,
                    r.state.range.exclusive_max_task_key,
                ),
            },
            predicate: l.state.predicate.clone(),
        };
        let tasks = l.tasks.into_iter().chain(r.tasks).map(|(_, t)| t);
        let merged_progress = merge_get_task_progress_with_same_predicate(&l.progress, &r.progress);
        left.build(merged_state, tasks, merged_progress)
    }

    fn merge_by_predicate(this: &VirtualSliceImpl, that: &VirtualSliceImpl) -> VirtualSliceImpl {
        let (t, o) = (this.parts(), that.parts());
        let merged_state = VirtualSliceState {
            range: t.state.range, // this and that have the same range
            predicate: or(&t.state.predicate, &o.state.predicate),
        };
        let merged_progress = merge_get_task_progress_with_different_predicate(&t.progress, &o.progress);
        let tasks = t.tasks.into_iter().chain(o.tasks).map(|(_, t)| t);
        this.build(merged_state, tasks, merged_progress)
    }

    fn merge_with_different_predicate(
        this: &VirtualSliceImpl,
        that: &VirtualSliceImpl,
    ) -> Option<Vec<VirtualSliceImpl>> {
        let (mut this, mut that) = (this, that);
        let this_range = this.get_state().range;
        let that_range = that.get_state().range;
        let compare_inclusive_min = this_range
            .inclusive_min_task_key
            .compare(&that_range.inclusive_min_task_key);
        if compare_inclusive_min > 0
            || (compare_inclusive_min == 0
                && this_range
                    .exclusive_max_task_key
                    .compare(&that_range.exclusive_max_task_key)
                    > 0)
        {
            std::mem::swap(&mut this, &mut that);
        }
        let this_range = this.get_state().range;
        let that_range = that.get_state().range;
        if compare_inclusive_min == 0 {
            return match that.split_by_task_key(this_range.exclusive_max_task_key) {
                None => Some(vec![Self::merge_by_predicate(this, that)]),
                Some((that_left, that_right)) => Some(vec![
                    Self::merge_by_predicate(this, &that_left),
                    that_right,
                ]),
            };
        }
        let (this_left, this_right) = this.split_by_task_key(that_range.inclusive_min_task_key)?;
        let mut merged = Vec::with_capacity(3);
        merged.push(this_left);
        match that.split_by_task_key(this_range.exclusive_max_task_key) {
            None => match this_right.split_by_task_key(that_range.exclusive_max_task_key) {
                None => merged.push(Self::merge_by_predicate(&this_right, that)),
                Some((trl, trr)) => {
                    merged.push(Self::merge_by_predicate(&trl, that));
                    merged.push(trr);
                }
            },
            Some((that_left, that_right)) => {
                merged.push(Self::merge_by_predicate(&this_right, &that_left));
                merged.push(that_right);
            }
        }
        Some(merged)
    }
}

#[async_trait]
impl VirtualSlice for VirtualSliceImpl {
    fn get_state(&self) -> VirtualSliceState {
        self.inner.lock().unwrap().state.clone()
    }

    fn get_pending_task_count(&self) -> usize {
        self.inner.lock().unwrap().pending_task_tracker.get_pending_task_count()
    }

    fn get_read_level(&self) -> HistoryTaskKey {
        let inner = self.inner.lock().unwrap();
        if let Some(p) = inner.progress.first() {
            return p.next_task_key;
        }
        inner.state.range.exclusive_max_task_key
    }

    fn is_empty(&self) -> bool {
        self.get_state().is_empty() && !self.has_more_tasks()
    }

    fn clear(&self) {
        self.update_and_get_state();
        let mut inner = self.inner.lock().unwrap();
        inner.pending_task_tracker.clear();
        inner.progress = initial_progress(&inner.state);
    }

    async fn get_tasks(&self, page_size: usize) -> Result<Vec<HTask>, DepError> {
        let mut tasks: Vec<HTask> = Vec::with_capacity(page_size);
        while tasks.len() < page_size {
            let (progress0, predicate) = {
                let inner = self.inner.lock().unwrap();
                match inner.progress.first() {
                    None => break,
                    Some(p) => (p.clone(), inner.state.predicate.clone()),
                }
            };
            let resp = match self
                .queue_reader
                .get_task(&GetTaskRequest {
                    progress: progress0,
                    predicate,
                    page_size: page_size - tasks.len(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    if !tasks.is_empty() {
                        return Ok(tasks);
                    }
                    return Err(e);
                }
            };

            let mut inner = self.inner.lock().unwrap();
            for t in resp.tasks {
                let task = (self.task_initializer)(t);
                tasks.push(task.clone());
                inner.pending_task_tracker.add_task(task);
            }
            if inner.progress.is_empty() {
                break;
            }
            if resp
                .progress
                .next_task_key
                .compare(&inner.progress[0].range.exclusive_max_task_key)
                < 0
            {
                inner.progress[0] = resp.progress;
            } else {
                inner.progress.remove(0);
            }
        }
        Ok(tasks)
    }

    fn has_more_tasks(&self) -> bool {
        !self.inner.lock().unwrap().progress.is_empty()
    }

    fn pending_task_stats(&self) -> PendingTaskStats {
        PendingTaskStats {
            pending_task_count_per_domain: self
                .inner
                .lock()
                .unwrap()
                .pending_task_tracker
                .get_per_domain_pending_task_count()
                .clone(),
        }
    }

    fn update_and_get_state(&self) -> VirtualSliceState {
        let mut inner = self.inner.lock().unwrap();
        let pruned_count = inner.pending_task_tracker.prune_acked_tasks();
        if self.logger.debug_on() {
            let next = inner
                .progress
                .first()
                .map(|p| p.next_task_key)
                .unwrap_or(inner.state.range.exclusive_max_task_key);
            self.logger.debug(&format!(
                "pruned acked tasks counter={} inclusiveMinTaskKey={:?} exclusiveMaxTaskKey={:?} nextTaskKey={:?}",
                pruned_count,
                inner.state.range.inclusive_min_task_key,
                inner.state.range.exclusive_max_task_key,
                next
            ));
        }
        let (min_pending, ok) = inner.pending_task_tracker.get_minimum_task_key();
        let first_next = inner.progress.first().map(|p| p.next_task_key);
        let new_min = match (ok, first_next) {
            // no pending tasks, and there are more tasks to read
            (false, Some(n)) => n,
            // no pending tasks, and no more tasks to read
            (false, None) => inner.state.range.exclusive_max_task_key,
            // pending tasks, and more tasks to read
            (true, Some(n)) => min_history_task_key(min_pending, n),
            // pending tasks, and no more tasks to read
            (true, None) => min_pending,
        };
        inner.state.range.inclusive_min_task_key = new_min;
        inner.state.clone()
    }

    fn try_split_by_task_key(
        &self,
        k: HistoryTaskKey,
    ) -> Option<(Arc<dyn VirtualSlice>, Arc<dyn VirtualSlice>)> {
        let (l, r) = self.split_by_task_key(k)?;
        Some((Arc::new(l), Arc::new(r)))
    }

    fn try_split_by_predicate(
        &self,
        predicate: &Predicate,
    ) -> Option<(Arc<dyn VirtualSlice>, Arc<dyn VirtualSlice>)> {
        let p = self.parts();
        let (pass_state, fail_state) = p.state.try_split_by_predicate(predicate)?;
        let mut pass_tasks = Vec::new();
        let mut fail_tasks = Vec::new();
        for (_, t) in &p.tasks {
            if pass_state.predicate.check(t.as_ref()) {
                pass_tasks.push(t.clone());
            } else {
                fail_tasks.push(t.clone());
            }
        }
        let pass = self.build(pass_state, pass_tasks, p.progress.clone());
        let fail = self.build(fail_state, fail_tasks, p.progress);
        Some((Arc::new(pass), Arc::new(fail)))
    }

    fn try_merge_with_virtual_slice(
        &self,
        other: &Arc<dyn VirtualSlice>,
    ) -> Option<Vec<Arc<dyn VirtualSlice>>> {
        let other_impl = other.as_any().downcast_ref::<VirtualSliceImpl>()?;
        if std::ptr::eq(self as *const Self as *const (), Arc::as_ptr(other) as *const ()) {
            return None;
        }
        let (my_state, other_state) = (self.get_state(), other.get_state());
        if !my_state.range.can_merge(other_state.range) {
            return None;
        }
        if my_state.predicate.equals(&other_state.predicate) {
            return Some(vec![Arc::new(Self::merge_by_range(self, other_impl))]);
        }
        let merged = Self::merge_with_different_predicate(self, other_impl)?;
        Some(
            merged
                .into_iter()
                .map(|s| Arc::new(s) as Arc<dyn VirtualSlice>)
                .collect(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn merge_get_task_progress_with_same_predicate(
    left: &[GetTaskProgress],
    right: &[GetTaskProgress],
) -> Vec<GetTaskProgress> {
    let mut merged = Vec::new();
    let (mut li, mut ri) = (0, 0);
    while li < left.len() && ri < right.len() {
        if left[li]
            .range
            .inclusive_min_task_key
            .compare(&right[ri].range.inclusive_min_task_key)
            <= 0
        {
            append_or_merge(&mut merged, &left[li], merge_progress_with_same_predicate);
            li += 1;
        } else {
            append_or_merge(&mut merged, &right[ri], merge_progress_with_same_predicate);
            ri += 1;
        }
    }
    for p in &left[li..] {
        append_or_merge(&mut merged, p, merge_progress_with_same_predicate);
    }
    for p in &right[ri..] {
        append_or_merge(&mut merged, p, merge_progress_with_same_predicate);
    }
    merged
}

fn append_or_merge(
    merged: &mut Vec<GetTaskProgress>,
    progress: &GetTaskProgress,
    f: fn(&GetTaskProgress, &GetTaskProgress) -> Vec<GetTaskProgress>,
) {
    match merged.pop() {
        None => merged.push(progress.clone()),
        Some(last) => merged.extend(f(&last, progress)),
    }
}

fn merge_progress_with_same_predicate(
    left: &GetTaskProgress,
    right: &GetTaskProgress,
) -> Vec<GetTaskProgress> {
    let (mut left, mut right) = (left, right);
    if left.range.inclusive_min_task_key.compare(&right.range.inclusive_min_task_key) > 0 {
        std::mem::swap(&mut left, &mut right);
    }
    if left.next_task_key.compare(&right.range.inclusive_min_task_key) < 0 {
        return vec![
            prog(
                left.next_task_key,
                min_history_task_key(left.range.exclusive_max_task_key, right.range.inclusive_min_task_key),
                left.next_task_key,
            ),
            prog(
                right.next_task_key,
                max_history_task_key(left.range.exclusive_max_task_key, right.range.exclusive_max_task_key),
                max_history_task_key(left.next_task_key, right.next_task_key),
            ),
        ];
    }
    let m = max_history_task_key(left.next_task_key, right.next_task_key);
    vec![prog(
        m,
        max_history_task_key(left.range.exclusive_max_task_key, right.range.exclusive_max_task_key),
        m,
    )]
}

fn merge_get_task_progress_with_different_predicate(
    left: &[GetTaskProgress],
    right: &[GetTaskProgress],
) -> Vec<GetTaskProgress> {
    let mut merged = Vec::new();
    let (mut li, mut ri) = (0, 0);
    while li < left.len() && ri < right.len() {
        if left[li].next_task_key.compare(&right[ri].next_task_key) <= 0 {
            append_or_merge(&mut merged, &left[li], merge_progress_with_different_predicate);
            li += 1;
        } else {
            append_or_merge(&mut merged, &right[ri], merge_progress_with_different_predicate);
            ri += 1;
        }
    }
    for p in &left[li..] {
        append_or_merge(&mut merged, p, merge_progress_with_different_predicate);
    }
    for p in &right[ri..] {
        append_or_merge(&mut merged, p, merge_progress_with_different_predicate);
    }
    merged
}

fn merge_progress_with_different_predicate(
    left: &GetTaskProgress,
    right: &GetTaskProgress,
) -> Vec<GetTaskProgress> {
    let (mut left, mut right) = (left, right);
    if left.next_task_key.compare(&right.next_task_key) > 0 {
        std::mem::swap(&mut left, &mut right);
    }
    if left.range.exclusive_max_task_key.compare(&right.next_task_key) < 0 {
        return vec![
            prog(left.next_task_key, left.range.exclusive_max_task_key, left.next_task_key),
            prog(right.next_task_key, right.range.exclusive_max_task_key, right.next_task_key),
        ];
    }
    vec![prog(
        left.next_task_key,
        max_history_task_key(left.range.exclusive_max_task_key, right.range.exclusive_max_task_key),
        left.next_task_key,
    )]
}
