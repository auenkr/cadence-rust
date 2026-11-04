//! Port of `task_dynamic_batch_sizer.go` and `common/rangeiter`.
use super::deps::{ReplicationConfig, ReplicationMetrics, ReplicationError};
use super::task_ack_manager::GetTasksResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// `rangeiter.LinearIterator[int]`.
#[derive(Debug, Clone)]
pub struct LinearIterator {
    cur: usize,
    steps: Vec<i32>,
}

impl LinearIterator {
    pub fn new(mut min: i32, mut max: i32, step_count: usize) -> Self {
        if min > max {
            std::mem::swap(&mut min, &mut max);
        }
        let total_range = (max - min) as i64;
        if step_count <= 2 || total_range <= 1 {
            return Self { cur: 0, steps: vec![min, max] };
        }
        if total_range < step_count as i64 {
            let n = total_range as usize;
            let mut steps = vec![0; n + 1];
            steps[0] = min;
            steps[n] = max;
            for (i, s) in steps.iter_mut().enumerate().take(n).skip(1) {
                *s = min + i as i32;
            }
            return Self { cur: (steps.len() - 1) / 2, steps };
        }
        let step_size = total_range as f64 / (step_count - 1) as f64;
        let mut steps: Vec<i32> = (0..step_count).map(|i| min + (i as f64 * step_size) as i32).collect();
        steps[step_count - 1] = max;
        Self { cur: (steps.len() - 1) / 2, steps }
    }
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> i32 {
        if self.cur < self.steps.len() - 1 {
            self.cur += 1;
        }
        self.steps[self.cur]
    }
    pub fn previous(&mut self) -> i32 {
        if self.cur > 0 {
            self.cur -= 1;
        }
        self.steps[self.cur]
    }
    pub fn value(&self) -> i32 {
        self.steps[self.cur]
    }
    pub fn reset(&mut self) {
        self.cur = (self.steps.len() - 1) / 2;
    }
}

type IntFn = Box<dyn Fn() -> i32 + Send + Sync>;
type CountFn = Box<dyn Fn() -> usize + Send + Sync>;

/// `rangeiter.DynamicConfigLinearIterator[int]`.
pub struct DynamicConfigLinearIterator {
    inner: Mutex<(LinearIterator, (i32, i32, usize))>,
    min_fn: IntFn,
    max_fn: IntFn,
    step_count_fn: CountFn,
}

impl DynamicConfigLinearIterator {
    pub fn new(min_fn: IntFn, max_fn: IntFn, step_count_fn: CountFn) -> Self {
        let spec = (min_fn(), max_fn(), step_count_fn());
        tracing::info!(?spec, "Dynamic config linear iterator initialized");
        Self {
            inner: Mutex::new((LinearIterator::new(spec.0, spec.1, spec.2), spec)),
            min_fn,
            max_fn,
            step_count_fn,
        }
    }
    fn with<R>(&self, f: impl FnOnce(&mut LinearIterator) -> R) -> R {
        let mut g = self.inner.lock().unwrap();
        let spec = ((self.min_fn)(), (self.max_fn)(), (self.step_count_fn)());
        if spec != g.1 {
            tracing::info!(?spec, "Dynamic config linear iterator spec has changed, refreshing iterator, applying new spec");
            g.1 = spec;
            g.0 = LinearIterator::new(spec.0, spec.1, spec.2);
        }
        f(&mut g.0)
    }
    pub fn next(&self) -> i32 { self.with(|i| i.next()) }
    pub fn previous(&self) -> i32 { self.with(|i| i.previous()) }
    pub fn value(&self) -> i32 { self.with(|i| i.value()) }
    pub fn reset(&self) { self.with(|i| i.reset()) }
}

/// `DynamicTaskBatchSizer`.
pub trait DynamicTaskBatchSizer: Send + Sync {
    fn analyse(&self, err: Option<&ReplicationError>, state: Option<&GetTasksResult>);
    fn value(&self) -> usize;
}

pub struct DynamicTaskBatchSizerImpl {
    is_fetched_tasks: AtomicBool,
    iter: DynamicConfigLinearIterator,
    shard_id: i32,
    metrics: Arc<dyn ReplicationMetrics>,
}

impl DynamicTaskBatchSizerImpl {
    /// `NewDynamicTaskBatchSizer`.
    pub fn new(shard_id: i32, config: Arc<dyn ReplicationConfig>, metrics: Arc<dyn ReplicationMetrics>) -> Self {
        let (c1, c2, c3) = (config.clone(), config.clone(), config);
        Self {
            is_fetched_tasks: AtomicBool::new(false),
            shard_id,
            metrics,
            iter: DynamicConfigLinearIterator::new(
                Box::new(move || c1.replicator_processor_min_task_batch_size(shard_id)),
                Box::new(move || c2.replicator_processor_max_task_batch_size(shard_id)),
                Box::new(move || c3.replicator_processor_batch_size_step_count(shard_id)),
            ),
        }
    }

    fn decrease(&self, reason: &str) {
        let old = self.iter.value();
        let new = self.iter.previous();
        if old != new {
            self.emit_metric(reason, "decrease");
        }
        tracing::debug!(reason, new, "Decrease task batch size");
    }

    fn increase(&self, reason: &str) {
        let old = self.iter.value();
        let new = self.iter.next();
        if old != new {
            self.emit_metric(reason, "increase");
        }
        tracing::debug!(reason, new, "Increase task batch size");
    }

    fn emit_metric(&self, reason: &str, decision: &str) {
        let shard = self.shard_id.to_string();
        self.metrics.inc_counter(
            "ReplicatorQueueProcessor",
            "ReplicationDynamicTaskBatchSizerDecision",
            &[("instance", &shard), ("reason", reason), ("decision", decision)],
        );
    }
}

impl DynamicTaskBatchSizer for DynamicTaskBatchSizerImpl {
    fn analyse(&self, err: Option<&ReplicationError>, state: Option<&GetTasksResult>) {
        // Go dereferences `state` only in branches reached when err == nil.
        if err.is_some() {
            self.decrease("error");
        } else if let Some(state) = state {
            if state.is_shrunk {
                self.decrease("shrunk");
            } else if state.previous_read_task_id == state.last_read_task_id
                && !state.task_infos.is_empty()
                && self.is_fetched_tasks.load(Ordering::SeqCst)
            {
                self.decrease("possible_stuck");
            } else if state.msgs.has_more {
                self.increase("more_tasks");
            }
        }
        match state {
            None => self.is_fetched_tasks.store(false, Ordering::SeqCst),
            Some(s) => self.is_fetched_tasks.store(!s.task_infos.is_empty(), Ordering::SeqCst),
        }
    }

    fn value(&self) -> usize {
        self.iter.value().max(0) as usize
    }
}
