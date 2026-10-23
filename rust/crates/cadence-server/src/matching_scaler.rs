//! Go `tasklist/adaptive_scaler.go` and `tasklist/isolation_balancer.go`.
//! The scaler runs on root normal partitions when both
//! `matching.enableAdaptiveScaler` and
//! `matching.enableGetNumberOfPartitionsFromCache` are set (both default
//! off). Each tick it derives total QPS, sustains overload/underload,
//! rebuilds the dense write map, drains trailing empty read-only partitions,
//! optionally rebalances isolation groups, and publishes through the same
//! versioned `update_partition_config` path as manual updates.
use std::collections::HashMap;

/// One partition id to the isolation groups assigned to it, matching the
/// `StoredPartitionConfig` read/write maps.
pub type PartitionMap = HashMap<i32, Vec<String>>;

/// Go `clock.Sustain` over wall nanos: true once the condition has held
/// continuously for the duration. Durations are reread per check so live
/// config changes apply like Go's duration funcs.
#[derive(Default)]
pub struct Sustain {
    started: Option<i64>,
}
impl Sustain {
    pub fn reset(&mut self) {
        self.started = None;
    }
    pub fn check(&mut self, now: i64, value: bool, duration_nanos: i64) -> bool {
        if value {
            if self.started.is_none() {
                self.started = Some(now);
            }
            if now.saturating_sub(self.started.unwrap_or(now)) >= duration_nanos {
                return true;
            }
        } else {
            self.reset();
        }
        false
    }
    pub fn check_and_reset(&mut self, now: i64, value: bool, duration_nanos: i64) -> bool {
        if self.check(now, value, duration_nanos) {
            self.started = Some(now);
            return true;
        }
        false
    }
}

/// Live scaler configuration snapshot, reread every tick. Defaults are Go's
/// `dynamicproperties` defaults: 200 upscale RPS, 0.75 downscale factor,
/// 60s/120s partition sustains, 15s interval, 2 groups per partition, 60s
/// group sustains.
#[derive(Clone)]
pub struct ScalerConfig {
    pub enabled: bool,
    pub cache_enabled: bool,
    pub update_interval_nanos: i64,
    pub upscale_rps: f64,
    pub downscale_factor: f64,
    pub upscale_sustain_nanos: i64,
    pub downscale_sustain_nanos: i64,
    pub min_write_partitions: i64,
    pub isolation_assignment: bool,
    pub empty_check: bool,
    pub group_upscale_sustain_nanos: i64,
    pub group_downscale_sustain_nanos: i64,
    pub group_has_pollers_sustain_nanos: i64,
    pub group_no_pollers_sustain_nanos: i64,
    pub groups_per_partition: i64,
    pub poller_window_nanos: i64,
    pub isolation_groups: Vec<String>,
}

/// Per-partition describe data used for scaling decisions.
#[derive(Clone, Default)]
pub struct PartitionMetrics {
    pub qps: f64,
    pub read_only: bool,
    pub empty: bool,
}
/// Aggregate describe data across the read partitions.
#[derive(Default)]
pub struct AggregateMetrics {
    pub total_qps: f64,
    pub qps_by_group: HashMap<String, f64>,
    pub has_pollers_by_group: HashMap<String, bool>,
    pub by_partition: HashMap<i32, PartitionMetrics>,
    pub isolation_enabled: bool,
}

/// Go `getNumberOfPartitions`: ceil(qps/upscale), at least 1, at least the
/// configured minimum.
pub fn number_of_partitions(qps: f64, upscale_rps: f64, min_write: i64) -> i64 {
    let min_write = min_write.max(1);
    if upscale_rps <= 0.0 {
        return min_write;
    }
    ((qps / upscale_rps).ceil() as i64).max(min_write)
}

/// Go `calculateWritePartitionCount` without scope gauges.
pub fn write_partition_count(
    qps: f64,
    current: i64,
    config: &ScalerConfig,
    over: &mut Sustain,
    under: &mut Sustain,
    now: i64,
) -> i64 {
    let upscale_threshold = current as f64 * config.upscale_rps;
    let downscale_threshold = (current - 1).max(0) as f64 * config.upscale_rps * config.downscale_factor;
    let min_write = config.min_write_partitions.max(1);
    let mut result = current.max(min_write);
    if over.check_and_reset(now, qps > upscale_threshold, config.upscale_sustain_nanos) {
        result = number_of_partitions(qps, config.upscale_rps, min_write);
    }
    if under.check_and_reset(now, qps < downscale_threshold, config.downscale_sustain_nanos) {
        result = number_of_partitions(qps, config.upscale_rps, min_write);
    }
    result
}

/// Go `adjustWritePartitions`: dense `0..target`, preserving entries.
pub fn adjust_write_partitions(
    current: &PartitionMap,
    target: usize,
) -> (PartitionMap, bool) {
    if current.len() == target {
        return (current.clone(), false);
    }
    let mut result = HashMap::with_capacity(target);
    for id in 0..target as i32 {
        result.insert(id, current.get(&id).cloned().unwrap_or_default());
    }
    (result, true)
}

/// Go `adjustReadPartitions`: old reads union new writes, then drain trailing
/// read-only empty partitions from the top. `describe` covers ids missing
/// from the tick metrics; a failed describe stops the drain like Go's break.
pub fn adjust_read_partitions(
    metrics: &AggregateMetrics,
    old_read: &PartitionMap,
    new_write: &PartitionMap,
    mut describe: impl FnMut(i32) -> Option<PartitionMetrics>,
) -> (PartitionMap, bool) {
    let mut result = old_read.clone();
    let mut changed = false;
    for (id, groups) in new_write {
        if result.get(id) != Some(groups) {
            changed = true;
        }
        result.insert(*id, groups.clone());
    }
    let mut id = result.len() as i32 - 1;
    while id >= new_write.len() as i32 {
        let entry = match metrics.by_partition.get(&id) {
            Some(entry) => entry.clone(),
            None => match describe(id) {
                Some(entry) => entry,
                None => break,
            },
        };
        if entry.read_only && entry.empty {
            changed = true;
            result.remove(&id);
        } else {
            break;
        }
        id -= 1;
    }
    (result, changed)
}

struct GroupState {
    under_load: Sustain,
    over_load: Sustain,
    no_pollers: Sustain,
    has_pollers: Sustain,
    assignable: bool,
}
impl GroupState {
    fn new(assignable: bool) -> Self {
        Self {
            under_load: Sustain::default(),
            over_load: Sustain::default(),
            no_pollers: Sustain::default(),
            has_pollers: Sustain::default(),
            assignable,
        }
    }
}

/// Go `isolationBalancer`: assigns isolation groups across write partitions.
#[derive(Default)]
pub struct IsolationBalancer {
    groups: HashMap<String, GroupState>,
}
impl IsolationBalancer {
    fn assignable_groups(&self) -> HashMap<String, bool> {
        self.groups
            .iter()
            .map(|(group, state)| (group.clone(), state.assignable))
            .collect()
    }
    pub fn reset(&mut self) {
        self.groups.clear();
    }
    fn refresh(&mut self, metrics: &AggregateMetrics, write: &PartitionMap) {
        let mut live = HashMap::new();
        for groups in write.values() {
            for group in groups {
                live.insert(group.clone(), true);
                self.groups
                    .entry(group.clone())
                    .or_insert_with(|| GroupState::new(true));
            }
        }
        for group in metrics.has_pollers_by_group.keys() {
            live.insert(group.clone(), false);
            self.groups
                .entry(group.clone())
                .or_insert_with(|| GroupState::new(false));
        }
        self.groups.retain(|group, _| live.contains_key(group));
    }
    fn partitions_per_group(&self, write: &PartitionMap) -> HashMap<String, i64> {
        let mut counts = HashMap::new();
        for groups in write.values() {
            for group in groups {
                *counts.entry(group.clone()).or_insert(0) += 1;
            }
        }
        counts
    }
    fn min_groups_per_partition(&self, config: &ScalerConfig, assignable: i64) -> i64 {
        config.groups_per_partition.min(assignable)
    }
    fn min_partitions_per_group(
        &self,
        config: &ScalerConfig,
        write_count: i64,
        assignable: i64,
    ) -> i64 {
        if assignable == 0 {
            return 0;
        }
        ((write_count * self.min_groups_per_partition(config, assignable)) as f64 / assignable as f64).ceil() as i64
    }
    /// Go `adjustWritePartitions` on the balancer. Record sustain inputs even
    /// with a single partition so a later upscale reacts immediately.
    pub fn adjust_write_partitions(
        &mut self,
        config: &ScalerConfig,
        now: i64,
        metrics: &AggregateMetrics,
        write: &PartitionMap,
    ) -> (PartitionMap, bool) {
        self.refresh(metrics, write);
        let counts = self.partitions_per_group(write);
        let goal = self.partition_goal(config, now, metrics, &counts, write.len() as i64);
        if write.len() == 1 {
            if !write.get(&0).is_none_or(|groups| groups.is_empty()) {
                self.reset();
                return (HashMap::from([(0, Vec::new())]), true);
            }
            return (write.clone(), false);
        }
        self.apply_changes(config, metrics, &counts, &goal, write)
    }
    fn partition_goal(
        &mut self,
        config: &ScalerConfig,
        now: i64,
        metrics: &AggregateMetrics,
        counts: &HashMap<String, i64>,
        write_count: i64,
    ) -> HashMap<String, i64> {
        let mut assignable = 0;
        for (group, state) in self.groups.iter_mut() {
            let has = metrics.has_pollers_by_group.get(group).copied().unwrap_or(false);
            let sustained_pollers =
                state.has_pollers.check_and_reset(now, has, config.group_has_pollers_sustain_nanos);
            let sustained_no =
                state.no_pollers.check_and_reset(now, !has, config.group_no_pollers_sustain_nanos);
            if state.assignable {
                if sustained_no {
                    state.assignable = false;
                } else {
                    assignable += 1;
                }
            } else if sustained_pollers {
                state.assignable = true;
                state.over_load.reset();
                state.under_load.reset();
                assignable += 1;
            }
        }
        let min_partitions = self.min_partitions_per_group(config, write_count, assignable);
        let mut goal = HashMap::new();
        // Per-group goals are independent, so iteration order is irrelevant.
        for (group, state) in self.groups.iter_mut() {
            if !state.assignable {
                goal.insert(group.clone(), 0);
                continue;
            }
            let current = counts.get(group).copied().unwrap_or(0);
            let group_qps = metrics.qps_by_group.get(group).copied().unwrap_or(0.0);
            let upscale_rps = config.upscale_rps / config.groups_per_partition.max(1) as f64;
            let upscale_threshold = current.max(1) as f64 * upscale_rps;
            let downscale_threshold =
                (current - 1).max(0) as f64 * upscale_rps * config.downscale_factor;
            let sustained_over = state.over_load.check(now, group_qps > upscale_threshold, config.group_upscale_sustain_nanos);
            let sustained_under = state.under_load.check(now, group_qps < downscale_threshold, config.group_downscale_sustain_nanos);
            let ideal = number_of_partitions(group_qps, upscale_rps, 1);
            let target = ideal.clamp(min_partitions, write_count.max(min_partitions));
            if (sustained_over || sustained_under || current == 0) && target != current {
                state.over_load.reset();
                state.under_load.reset();
                goal.insert(group.clone(), target);
            } else {
                goal.insert(group.clone(), current.max(min_partitions));
            }
        }
        goal
    }
    fn apply_changes(
        &self,
        config: &ScalerConfig,
        metrics: &AggregateMetrics,
        counts: &HashMap<String, i64>,
        goal: &HashMap<String, i64>,
        write: &PartitionMap,
    ) -> (PartitionMap, bool) {
        let mut sizes = HashMap::new();
        let mut to_add: Vec<String> = Vec::new();
        let mut to_remove: Vec<String> = Vec::new();
        let mut assignable = 0;
        for (group, state) in self.groups.iter() {
            if state.assignable {
                assignable += 1;
            }
            let change = goal.get(group).copied().unwrap_or(0) - counts.get(group).copied().unwrap_or(0);
            let goal_partitions = goal.get(group).copied().unwrap_or(0);
            if goal_partitions > 0 {
                sizes.insert(
                    group.clone(),
                    metrics.qps_by_group.get(group).copied().unwrap_or(0.0) / goal_partitions as f64,
                );
            }
            for _ in 0..change {
                to_add.push(group.clone());
            }
            for _ in change..0 {
                to_remove.push(group.clone());
            }
        }
        let mut by_partition: HashMap<i32, std::collections::HashSet<String>> = write
            .iter()
            .map(|(id, groups)| (*id, groups.iter().cloned().collect()))
            .collect();
        // Lengths are captured before the greedy passes drain the lists;
        // Go inspects the original slice headers after its non-mutating loops.
        let removed = !to_remove.is_empty();
        let added = !to_add.is_empty();
        remove_groups(&mut to_remove, &sizes, &mut by_partition);
        add_groups(&mut to_add, &sizes, &mut by_partition);
        let minimum = self.min_groups_per_partition(config, assignable);
        let moved = ensure_minimum_groups(minimum, &sizes, &mut by_partition);
        let result = by_partition
            .into_iter()
            .map(|(id, set)| {
                let mut groups: Vec<String> = set.into_iter().collect();
                groups.sort();
                (id, groups)
            })
            .collect();
        (result, moved || removed || added)
    }
}

fn partition_size(sizes: &HashMap<String, f64>, set: &std::collections::HashSet<String>) -> f64 {
    set.iter().map(|group| sizes.get(group).copied().unwrap_or(0.0)).sum()
}
fn find_max_with(
    group: &str,
    sizes: &HashMap<String, f64>,
    by_partition: &HashMap<i32, std::collections::HashSet<String>>,
) -> Option<i32> {
    let mut best: Option<(f64, i32)> = None;
    // Iterate in partition-id order for deterministic decisions like Go.
    let mut ids: Vec<i32> = by_partition.keys().copied().collect();
    ids.sort();
    for id in ids {
        let set = &by_partition[&id];
        if set.contains(group) {
            let size = partition_size(sizes, set);
            if best.is_none_or(|(best_size, _)| size > best_size) {
                best = Some((size, id));
            }
        }
    }
    best.map(|(_, id)| id)
}
fn find_min_without(
    group: &str,
    sizes: &HashMap<String, f64>,
    by_partition: &HashMap<i32, std::collections::HashSet<String>>,
) -> Option<i32> {
    let mut best: Option<(f64, i32)> = None;
    let mut ids: Vec<i32> = by_partition.keys().copied().collect();
    ids.sort();
    for id in ids {
        let set = &by_partition[&id];
        if !set.contains(group) {
            let size = partition_size(sizes, set);
            if best.is_none_or(|(best_size, _)| size < best_size) {
                best = Some((size, id));
            }
        }
    }
    best.map(|(_, id)| id)
}
fn remove_groups(
    to_remove: &mut Vec<String>,
    sizes: &HashMap<String, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<String>>,
) {
    to_remove.sort_by(|a, b| {
        sizes
            .get(b)
            .copied()
            .unwrap_or(0.0)
            .partial_cmp(&sizes.get(a).copied().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for group in to_remove.drain(..) {
        if let Some(id) = find_max_with(&group, sizes, by_partition) {
            by_partition.get_mut(&id).map(|set| set.remove(&group));
        }
    }
}
fn add_groups(
    to_add: &mut Vec<String>,
    sizes: &HashMap<String, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<String>>,
) {
    to_add.sort_by(|a, b| {
        sizes
            .get(b)
            .copied()
            .unwrap_or(0.0)
            .partial_cmp(&sizes.get(a).copied().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for group in to_add.drain(..) {
        if let Some(id) = find_min_without(&group, sizes, by_partition) {
            by_partition.get_mut(&id).map(|set| set.insert(group));
        }
    }
}
fn ensure_minimum_groups(
    minimum: i64,
    sizes: &HashMap<String, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<String>>,
) -> bool {
    let mut changed = false;
    loop {
        let mut ids: Vec<i32> = by_partition.keys().copied().collect();
        ids.sort_by(|a, b| {
            let (la, lb) = (by_partition[a].len(), by_partition[b].len());
            la.cmp(&lb).then_with(|| {
                partition_size(sizes, &by_partition[a])
                    .partial_cmp(&partition_size(sizes, &by_partition[b]))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        if ids.len() < 2 {
            return changed;
        }
        let (lo, hi) = (ids[0], ids[ids.len() - 1]);
        if by_partition[&lo].len() >= minimum as usize
            || by_partition[&hi].len() <= minimum as usize
        {
            return changed;
        }
        changed = true;
        let needed = minimum as usize - by_partition[&lo].len();
        let available = by_partition[&hi].len() - minimum as usize;
        move_groups(needed.min(available), hi, lo, sizes, by_partition);
    }
}
fn move_groups(
    mut amount: usize,
    from: i32,
    to: i32,
    sizes: &HashMap<String, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<String>>,
) {
    while amount > 0 {
        amount -= 1;
        let from_size = partition_size(sizes, &by_partition[&from]);
        let to_size = partition_size(sizes, &by_partition[&to]);
        let mut best: Option<(f64, String)> = None;
        // Deterministic candidate order.
        let mut candidates: Vec<String> = by_partition[&from].iter().cloned().collect();
        candidates.sort();
        for group in candidates {
            if by_partition[&to].contains(&group) {
                continue;
            }
            let size = sizes.get(&group).copied().unwrap_or(0.0);
            let diff = (from_size - to_size - 2.0 * size).abs();
            if best.as_ref().is_none_or(|(best_diff, _)| diff < *best_diff) {
                best = Some((diff, group));
            }
        }
        let Some((_, group)) = best else { return };
        let size = sizes.get(&group).copied().unwrap_or(0.0);
        by_partition.get_mut(&from).map(|set| set.remove(&group));
        by_partition.get_mut(&to).map(|set| set.insert(group));
        let _ = (from_size - size, to_size + size);
    }
}

/// Daemon state for one root partition: sustained-load clocks and the
/// isolation balancer, mirroring `adaptiveScalerImpl`.
#[derive(Default)]
pub struct Scaler {
    over: Sustain,
    under: Sustain,
    balancer: IsolationBalancer,
    telemetry: Option<(crate::matching_metrics::MatchingMetrics, crate::matching_metrics::PollMetricContext)>,
}
impl Scaler {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_metrics(
        mut self,
        metrics: crate::matching_metrics::MatchingMetrics,
        context: crate::matching_metrics::PollMetricContext,
    ) -> Self {
        self.telemetry = Some((metrics, context));
        self
    }
    /// One Go `run()` tick over already-collected metrics. Returns the new
    /// read/write maps when anything changed. A metrics-collection failure is
    /// the caller's cue to reset, like Go's error branch.
    pub fn reset(&mut self) {
        self.over.reset();
        self.under.reset();
        self.balancer.reset();
    }
    pub fn tick(
        &mut self,
        config: &ScalerConfig,
        now: i64,
        current_read: &PartitionMap,
        current_write: &PartitionMap,
        metrics: &AggregateMetrics,
        describe: impl FnMut(i32) -> Option<PartitionMetrics>,
    ) -> Option<(PartitionMap, PartitionMap)> {
        let current_write_count = current_write.len();
        if let Some((metrics_sink, context)) = &self.telemetry {
            metrics_sink.scaler_tick(
                context,
                metrics.total_qps,
                current_write_count,
                config.upscale_rps,
                config.downscale_factor,
            );
        }
        let target = write_partition_count(
            metrics.total_qps,
            current_write.len() as i64,
            config,
            &mut self.over,
            &mut self.under,
            now,
        );
        if target as usize > current_write_count {
            if let Some((metrics_sink, context)) = &self.telemetry {
                metrics_sink.partition_scale_change(context, true);
            }
        } else if (target as usize) < current_write_count {
            if let Some((metrics_sink, context)) = &self.telemetry {
                metrics_sink.partition_scale_change(context, false);
            }
        }
        let (mut write, write_changed) =
            adjust_write_partitions(current_write, target.max(0) as usize);
        let mut isolation_changed = false;
        let assignable_before = self.balancer.assignable_groups();
        let counts_before = group_partition_counts(&write);
        if metrics.isolation_enabled {
            (write, isolation_changed) = self
                .balancer
                .adjust_write_partitions(config, now, metrics, &write);
        } else {
            self.balancer.reset();
        }
        let (read, read_changed) =
            adjust_read_partitions(metrics, current_read, &write, describe);
        if let Some((metrics_sink, context)) = &self.telemetry {
            if isolation_changed {
                metrics_sink.isolation_rebalance(context);
            }
            let assignable_after = self.balancer.assignable_groups();
            let counts_after = group_partition_counts(&write);
            let mut groups: Vec<String> = metrics
                .has_pollers_by_group
                .keys()
                .chain(assignable_before.keys())
                .chain(assignable_after.keys())
                .chain(counts_before.keys())
                .chain(counts_after.keys())
                .cloned()
                .collect();
            groups.sort();
            groups.dedup();
            for group in groups {
                let before = assignable_before.get(&group).copied().unwrap_or(false);
                let after = assignable_after.get(&group).copied().unwrap_or(false);
                if !before && after {
                    metrics_sink.isolation_group_counter(
                        crate::matching_metrics::ISOLATION_GROUP_STARTED_POLLING,
                        context,
                        &group,
                    );
                } else if before && !after {
                    metrics_sink.isolation_group_counter(
                        crate::matching_metrics::ISOLATION_GROUP_STOPPED_POLLING,
                        context,
                        &group,
                    );
                }
                let old_count = counts_before.get(&group).copied().unwrap_or(0);
                let new_count = counts_after.get(&group).copied().unwrap_or(0);
                if new_count > old_count {
                    metrics_sink.isolation_group_counter(
                        crate::matching_metrics::ISOLATION_GROUP_UPSCALE,
                        context,
                        &group,
                    );
                } else if new_count < old_count {
                    metrics_sink.isolation_group_counter(
                        crate::matching_metrics::ISOLATION_GROUP_DOWNSCALE,
                        context,
                        &group,
                    );
                }
                metrics_sink.isolation_group_partitions(context, &group, new_count);
            }
            if read_changed && read.len() < current_read.len() {
                metrics_sink.partition_drained(context);
            }
        }
        if write_changed || isolation_changed || read_changed {
            Some((read, write))
        } else {
            None
        }
    }
}

fn group_partition_counts(write: &PartitionMap) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    for groups in write.values() {
        for group in groups {
            *counts.entry(group.clone()).or_insert(0) += 1;
        }
    }
    counts
}

impl Default for ScalerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cache_enabled: false,
            update_interval_nanos: 15_000_000_000,
            upscale_rps: 200.0,
            downscale_factor: 0.75,
            upscale_sustain_nanos: 60_000_000_000,
            downscale_sustain_nanos: 120_000_000_000,
            min_write_partitions: 1,
            isolation_assignment: false,
            empty_check: false,
            group_upscale_sustain_nanos: 60_000_000_000,
            group_downscale_sustain_nanos: 120_000_000_000,
            group_has_pollers_sustain_nanos: 60_000_000_000,
            group_no_pollers_sustain_nanos: 60_000_000_000,
            groups_per_partition: 2,
            poller_window_nanos: 2_000_000_000,
            isolation_groups: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MIN: i64 = 60_000_000_000;
    fn config() -> ScalerConfig {
        ScalerConfig::default()
    }
    #[test]
    fn matching_scaler_sustain_holds_for_the_full_duration() {
        let mut sustain = Sustain::default();
        assert!(!sustain.check(0, true, MIN));
        assert!(!sustain.check(MIN - 1, true, MIN));
        assert!(sustain.check(MIN, true, MIN));
        assert!(sustain.check(MIN + 1, true, MIN));
        assert!(!sustain.check(MIN + 2, false, MIN));
        assert!(!sustain.check(MIN + 3, true, MIN));
        let mut rearmed = Sustain::default();
        assert!(!rearmed.check_and_reset(0, true, MIN));
        assert!(rearmed.check_and_reset(MIN, true, MIN));
        assert!(!rearmed.check_and_reset(MIN + 1, true, MIN));
        assert!(rearmed.check_and_reset(2 * MIN, true, MIN));
    }
    #[test]
    fn matching_scaler_write_count_uses_sustained_thresholds() {
        let config = config();
        let (mut over, mut under) = (Sustain::default(), Sustain::default());
        // 250 > 1*200 upscale threshold, but not yet sustained.
        assert_eq!(write_partition_count(250.0, 1, &config, &mut over, &mut under, 0), 1);
        assert_eq!(write_partition_count(250.0, 1, &config, &mut over, &mut under, MIN), 2);
        // 100 < (2-1)*200*0.75 downscale threshold, sustained after 2 minutes.
        let (mut over, mut under) = (Sustain::default(), Sustain::default());
        assert_eq!(write_partition_count(100.0, 2, &config, &mut over, &mut under, 0), 2);
        assert_eq!(write_partition_count(100.0, 2, &config, &mut over, &mut under, 2 * MIN), 1);
        // Between thresholds never moves.
        let (mut over, mut under) = (Sustain::default(), Sustain::default());
        assert_eq!(write_partition_count(175.0, 1, &config, &mut over, &mut under, 10 * MIN), 1);
        // Minimum write partitions always win.
        assert_eq!(number_of_partitions(0.0, 200.0, 3), 3);
        assert_eq!(number_of_partitions(401.0, 200.0, 1), 3);
    }
    #[test]
    fn matching_scaler_write_map_rebuilds_dense_preserving_groups() {
        let current = HashMap::from([(0, vec!["a".to_string()]), (2, vec!["b".to_string()])]);
        let (map, changed) = adjust_write_partitions(&current, 3);
        assert!(changed);
        assert_eq!(map[&0], vec!["a".to_string()]);
        assert_eq!(map[&1], Vec::<String>::new());
        assert_eq!(map[&2], vec!["b".to_string()]);
        let (_, changed) = adjust_write_partitions(&map, 3);
        assert!(!changed);
    }
    #[test]
    fn matching_scaler_read_drain_stops_at_first_live_partition() {
        let old = HashMap::from([(0, vec![]), (1, vec![]), (2, vec![])]);
        let new = HashMap::from([(0, vec![])]);
        let metrics = AggregateMetrics {
            by_partition: HashMap::from([
                (2, PartitionMetrics { qps: 0.0, read_only: true, empty: true }),
                (1, PartitionMetrics { qps: 5.0, read_only: true, empty: false }),
            ]),
            ..AggregateMetrics::default()
        };
        let (map, changed) = adjust_read_partitions(&metrics, &old, &new, |_| None);
        assert!(changed);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&0) && map.contains_key(&1));
        // A missing describe stops the drain like Go's break.
        let metrics = AggregateMetrics::default();
        let (map, changed) = adjust_read_partitions(&metrics, &old, &new, |_| None);
        assert!(!changed);
        assert_eq!(map.len(), 3);
    }
    fn immediate_config() -> ScalerConfig {
        ScalerConfig {
            group_upscale_sustain_nanos: 0,
            group_downscale_sustain_nanos: 0,
            group_has_pollers_sustain_nanos: 0,
            group_no_pollers_sustain_nanos: 0,
            ..ScalerConfig::default()
        }
    }
    fn group_metrics(groups: &[&str], qps: f64) -> AggregateMetrics {
        AggregateMetrics {
            qps_by_group: groups.iter().map(|group| (group.to_string(), qps)).collect(),
            has_pollers_by_group: groups.iter().map(|group| (group.to_string(), true)).collect(),
            isolation_enabled: true,
            ..AggregateMetrics::default()
        }
    }
    #[test]
    fn matching_scaler_single_partition_with_groups_resets_assignments() {
        let mut balancer = IsolationBalancer::default();
        let write = HashMap::from([(0, vec!["a".to_string()])]);
        let (map, changed) =
            balancer.adjust_write_partitions(&immediate_config(), 0, &group_metrics(&["a"], 10.0), &write);
        assert!(changed);
        assert_eq!(map, HashMap::from([(0, Vec::new())]));
    }
    #[test]
    fn matching_scaler_new_group_spreads_across_partitions() {
        let mut balancer = IsolationBalancer::default();
        let write = HashMap::from([(0, Vec::new()), (1, Vec::new())]);
        let (map, changed) =
            balancer.adjust_write_partitions(&immediate_config(), 0, &group_metrics(&["a"], 10.0), &write);
        // One group over 2 partitions: min 2 per group, goal 2, added to both.
        assert!(changed);
        assert_eq!(map, HashMap::from([(0, vec!["a".to_string()]), (1, vec!["a".to_string()])]));
        // Steady state afterwards: no further moves.
        let (same, changed) =
            balancer.adjust_write_partitions(&immediate_config(), 1, &group_metrics(&["a"], 10.0), &map);
        assert!(!changed);
        assert_eq!(same, map);
    }
    #[test]
    fn matching_scaler_group_without_sustained_pollers_gets_no_partitions() {
        let mut balancer = IsolationBalancer::default();
        let write = HashMap::from([(0, Vec::new()), (1, Vec::new())]);
        let (map, changed) =
            balancer.adjust_write_partitions(&config(), 0, &group_metrics(&["a"], 10.0), &write);
        assert!(!changed);
        assert_eq!(map, write);
    }
}
