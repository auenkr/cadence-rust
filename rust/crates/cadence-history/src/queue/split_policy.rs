//! Port of `go/service/history/queue/split_policy.go`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rand::Rng;

use super::deps::{BoolDomainIdFn, MetricsScope, TaskKey, TaskState};
use super::domain_filter::DomainFilter;
use super::processing_queue::{max_task_key, task_key_equals, ProcessingQueue};
use super::processing_queue_state::ProcessingQueueState;

pub const POLICY_TYPE_PENDING_TASK: i32 = 1;
pub const POLICY_TYPE_STUCK_TASK: i32 = 2;
pub const POLICY_TYPE_SELECTED_DOMAIN: i32 = 3;
pub const POLICY_TYPE_RANDOM: i32 = 4;

/// Go `lookAheadFunc`.
pub type LookAheadFunc = Arc<dyn Fn(TaskKey, &str) -> TaskKey + Send + Sync>;

pub trait ProcessingQueueSplitPolicy: Send + Sync {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState>;
}

pub struct PendingTaskSplitPolicy {
    pub pending_task_threshold: HashMap<i32, i64>,
    pub enabled_by_domain_id: BoolDomainIdFn,
    pub max_new_queue_level: i32,
    pub look_ahead_func: Option<LookAheadFunc>,
    pub metrics_scope: Arc<dyn MetricsScope>,
}

pub struct StuckTaskSplitPolicy {
    pub attempt_threshold: HashMap<i32, i64>,
    pub enabled_by_domain_id: BoolDomainIdFn,
    pub max_new_queue_level: i32,
    pub metrics_scope: Arc<dyn MetricsScope>,
}

pub struct SelectedDomainSplitPolicy {
    pub domain_ids: HashSet<String>,
    pub new_queue_level: i32,
    pub metrics_scope: Arc<dyn MetricsScope>,
}

pub struct RandomSplitPolicy {
    pub split_probability: f64,
    pub enabled_by_domain_id: BoolDomainIdFn,
    pub max_new_queue_level: i32,
    pub look_ahead_func: Option<LookAheadFunc>,
    pub metrics_scope: Arc<dyn MetricsScope>,
}

pub struct AggregatedSplitPolicy {
    pub policies: Vec<Box<dyn ProcessingQueueSplitPolicy>>,
}

impl PendingTaskSplitPolicy {
    pub fn new(
        pending_task_threshold: HashMap<i32, i64>,
        enabled_by_domain_id: BoolDomainIdFn,
        look_ahead_func: Option<LookAheadFunc>,
        max_new_queue_level: i32,
        metrics_scope: Arc<dyn MetricsScope>,
    ) -> Self {
        Self { pending_task_threshold, enabled_by_domain_id, max_new_queue_level, look_ahead_func, metrics_scope }
    }
}

impl StuckTaskSplitPolicy {
    pub fn new(
        attempt_threshold: HashMap<i32, i64>,
        enabled_by_domain_id: BoolDomainIdFn,
        max_new_queue_level: i32,
        metrics_scope: Arc<dyn MetricsScope>,
    ) -> Self {
        Self { attempt_threshold, enabled_by_domain_id, max_new_queue_level, metrics_scope }
    }
}

impl SelectedDomainSplitPolicy {
    pub fn new(domain_ids: HashSet<String>, new_queue_level: i32, metrics_scope: Arc<dyn MetricsScope>) -> Self {
        Self { domain_ids, new_queue_level, metrics_scope }
    }
}

impl RandomSplitPolicy {
    pub fn new(
        split_probability: f64,
        enabled_by_domain_id: BoolDomainIdFn,
        max_new_queue_level: i32,
        look_ahead_func: Option<LookAheadFunc>,
        metrics_scope: Arc<dyn MetricsScope>,
    ) -> Self {
        Self { split_probability, enabled_by_domain_id, max_new_queue_level, look_ahead_func, metrics_scope }
    }
}

impl AggregatedSplitPolicy {
    pub fn new(policies: Vec<Box<dyn ProcessingQueueSplitPolicy>>) -> Self {
        Self { policies }
    }
}

fn log_split(new_level: i32, prev_level: i32, domains: &HashSet<String>, policy_type: i32) {
    tracing::info!(
        queue_level = new_level,
        previous_queue_level = prev_level,
        domain_ids = ?domains,
        split_policy_type = policy_type,
        "Split processing queue"
    );
}

impl ProcessingQueueSplitPolicy for PendingTaskSplitPolicy {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState> {
        if queue.state.level == self.max_new_queue_level {
            return Vec::new();
        }
        let Some(&threshold) = self.pending_task_threshold.get(&queue.state.level) else {
            return Vec::new();
        };

        let mut pending_per_domain: HashMap<String, i64> = HashMap::new();
        for task in queue.outstanding_tasks.values() {
            if task.state() != TaskState::Acked {
                *pending_per_domain.entry(task.get_domain_id()).or_insert(0) += 1;
            }
        }

        let mut to_split = HashSet::new();
        for (domain_id, pending) in pending_per_domain {
            if pending > threshold && (self.enabled_by_domain_id)(&domain_id) {
                to_split.insert(domain_id);
            }
        }
        if to_split.is_empty() {
            return Vec::new();
        }

        let new_level = queue.state.level + 1;
        log_split(new_level, queue.state.level, &to_split, POLICY_TYPE_PENDING_TASK);
        self.metrics_scope.inc_counter("ProcessingQueuePendingTaskSplitCounter");

        split_queue_helper(queue, to_split, new_level, self.look_ahead_func.as_ref())
    }
}

impl ProcessingQueueSplitPolicy for StuckTaskSplitPolicy {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState> {
        if queue.state.level == self.max_new_queue_level {
            return Vec::new();
        }
        let Some(&threshold) = self.attempt_threshold.get(&queue.state.level) else {
            return Vec::new();
        };

        let mut to_split = HashSet::new();
        for task in queue.outstanding_tasks.values() {
            let domain_id = task.get_domain_id();
            if (task.get_attempt() as i64) > threshold && (self.enabled_by_domain_id)(&domain_id) {
                to_split.insert(domain_id);
            }
        }
        if to_split.is_empty() {
            return Vec::new();
        }

        let new_level = queue.state.level + 1;
        log_split(new_level, queue.state.level, &to_split, POLICY_TYPE_STUCK_TASK);
        self.metrics_scope.inc_counter("ProcessingQueueStuckTaskSplitCounter");

        split_queue_helper(queue, to_split, new_level, None)
    }
}

impl ProcessingQueueSplitPolicy for SelectedDomainSplitPolicy {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState> {
        let state = queue.state();
        let filter = state.domain_filter();
        if !self.domain_ids.iter().any(|d| filter.filter(d)) {
            return Vec::new();
        }

        log_split(state.level, self.new_queue_level, &self.domain_ids, POLICY_TYPE_SELECTED_DOMAIN);
        self.metrics_scope.inc_counter("ProcessingQueueSelectedDomainSplitCounter");

        vec![
            ProcessingQueueState::new_full(
                state.level,
                state.ack_level,
                state.read_level,
                state.max_level,
                filter.exclude(&self.domain_ids),
            ),
            ProcessingQueueState::new_full(
                self.new_queue_level,
                state.ack_level,
                state.read_level,
                state.max_level,
                DomainFilter::new(self.domain_ids.clone(), false).copy(),
            ),
        ]
    }
}

impl ProcessingQueueSplitPolicy for RandomSplitPolicy {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState> {
        if queue.state.level == self.max_new_queue_level {
            return Vec::new();
        }
        let domain_ids: HashSet<String> = queue.outstanding_tasks.values().map(|t| t.get_domain_id()).collect();
        if domain_ids.is_empty() {
            return Vec::new();
        }

        let mut to_split = HashSet::new();
        for d in domain_ids {
            if !(self.enabled_by_domain_id)(&d) {
                continue;
            }
            if !should_split(self.split_probability) {
                continue;
            }
            to_split.insert(d);
        }
        if to_split.is_empty() {
            return Vec::new();
        }

        let new_level = queue.state.level + 1;
        log_split(new_level, queue.state.level, &to_split, POLICY_TYPE_RANDOM);
        self.metrics_scope.inc_counter("ProcessingQueueRandomSplitCounter");

        split_queue_helper(queue, to_split, new_level, self.look_ahead_func.as_ref())
    }
}

impl ProcessingQueueSplitPolicy for AggregatedSplitPolicy {
    fn evaluate(&self, queue: &ProcessingQueue) -> Vec<ProcessingQueueState> {
        for p in &self.policies {
            let s = p.evaluate(queue);
            if !s.is_empty() {
                return s;
            }
        }
        Vec::new()
    }
}

/// Go `splitQueueHelper`.
fn split_queue_helper(
    queue: &ProcessingQueue,
    domain_to_split: HashSet<String>,
    new_queue_level: i32,
    look_ahead: Option<&LookAheadFunc>,
) -> Vec<ProcessingQueueState> {
    let st = &queue.state;
    let mut new_max = st.read_level;
    if let Some(f) = look_ahead {
        for d in &domain_to_split {
            new_max = max_task_key(new_max, f(st.read_level, d));
        }
    }
    if st.max_level.less(&new_max) {
        new_max = st.max_level;
    }

    let mut states = vec![ProcessingQueueState::new_full(
        new_queue_level,
        st.ack_level,
        st.read_level,
        new_max,
        DomainFilter::new(domain_to_split.clone(), false),
    )];

    let excluded = st.domain_filter.exclude(&domain_to_split);
    if excluded.reverse_match || !excluded.domain_ids.is_empty() {
        states.push(ProcessingQueueState::new_full(st.level, st.ack_level, st.read_level, new_max, excluded));
    }

    if !task_key_equals(&new_max, &st.max_level) {
        states.push(ProcessingQueueState::new_full(
            st.level,
            new_max,
            new_max,
            st.max_level,
            st.domain_filter.copy(),
        ));
    }

    for s in &states {
        if s.read_level.less(&s.ack_level) || s.max_level.less(&s.read_level) {
            panic!(
                "invalid processing queue split result: {:?}, state before split: {:?}, newMaxLevel: {:?}",
                s, st, new_max
            );
        }
    }
    states
}

/// Go `shouldSplit`.
fn should_split(probability: f64) -> bool {
    if probability <= 0.0 {
        return false;
    }
    if probability >= 1.0 {
        return true;
    }
    rand::thread_rng().gen_range(0..((1.0 / probability) as i64)) == 0
}
