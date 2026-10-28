//! Byte-preserving adaptive scaler for native Matching managers.
use crate::{
    matching_dispatch_native::NativeFuture,
    matching_partition_native::NativeStoredPartitionConfig,
    matching_registry_native::{NativeQueueRegistry, NativeScalerHost, NativeScalerStarter},
    matching_scaler::{number_of_partitions, write_partition_count, ScalerConfig, Sustain},
};
use cadence_common::{go_string::GoString, task_list::TaskListNameBytes};
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::Status;

pub type PartitionMap = HashMap<i32, Vec<GoString>>;
#[derive(Clone, Default)]
pub struct PartitionMetrics {
    pub qps: f64,
    pub read_only: bool,
    pub empty: bool,
}
#[derive(Default)]
pub struct AggregateMetrics {
    pub total_qps: f64,
    pub qps_by_group: HashMap<GoString, f64>,
    pub has_pollers_by_group: HashMap<GoString, bool>,
    pub by_partition: HashMap<i32, PartitionMetrics>,
    pub isolation_enabled: bool,
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
    groups: HashMap<GoString, GroupState>,
}
impl IsolationBalancer {
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
    fn partitions_per_group(&self, write: &PartitionMap) -> HashMap<GoString, i64> {
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
        ((write_count * self.min_groups_per_partition(config, assignable)) as f64
            / assignable as f64)
            .ceil() as i64
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
        counts: &HashMap<GoString, i64>,
        write_count: i64,
    ) -> HashMap<GoString, i64> {
        let mut assignable = 0;
        for (group, state) in self.groups.iter_mut() {
            let has = metrics
                .has_pollers_by_group
                .get(group)
                .copied()
                .unwrap_or(false);
            let sustained_pollers =
                state
                    .has_pollers
                    .check_and_reset(now, has, config.group_has_pollers_sustain_nanos);
            let sustained_no =
                state
                    .no_pollers
                    .check_and_reset(now, !has, config.group_no_pollers_sustain_nanos);
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
            let sustained_over = state.over_load.check(
                now,
                group_qps > upscale_threshold,
                config.group_upscale_sustain_nanos,
            );
            let sustained_under = state.under_load.check(
                now,
                group_qps < downscale_threshold,
                config.group_downscale_sustain_nanos,
            );
            let ideal = number_of_partitions(group_qps, upscale_rps, 1);
            let target = ideal.max(min_partitions).min(write_count);
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
        counts: &HashMap<GoString, i64>,
        goal: &HashMap<GoString, i64>,
        write: &PartitionMap,
    ) -> (PartitionMap, bool) {
        let mut sizes = HashMap::new();
        let mut to_add: Vec<GoString> = Vec::new();
        let mut to_remove: Vec<GoString> = Vec::new();
        let mut assignable = 0;
        for (group, state) in self.groups.iter() {
            if state.assignable {
                assignable += 1;
            }
            let change =
                goal.get(group).copied().unwrap_or(0) - counts.get(group).copied().unwrap_or(0);
            let goal_partitions = goal.get(group).copied().unwrap_or(0);
            if goal_partitions > 0 {
                sizes.insert(
                    group.clone(),
                    metrics.qps_by_group.get(group).copied().unwrap_or(0.0)
                        / goal_partitions as f64,
                );
            }
            for _ in 0..change {
                to_add.push(group.clone());
            }
            for _ in change..0 {
                to_remove.push(group.clone());
            }
        }
        let mut by_partition: HashMap<i32, std::collections::HashSet<GoString>> = write
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
                let mut groups: Vec<GoString> = set.into_iter().collect();
                groups.sort();
                (id, groups)
            })
            .collect();
        (result, moved || removed || added)
    }
}

fn partition_size(
    sizes: &HashMap<GoString, f64>,
    set: &std::collections::HashSet<GoString>,
) -> f64 {
    set.iter()
        .map(|group| sizes.get(group).copied().unwrap_or(0.0))
        .sum()
}
fn find_max_with(
    group: &GoString,
    sizes: &HashMap<GoString, f64>,
    by_partition: &HashMap<i32, std::collections::HashSet<GoString>>,
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
    group: &GoString,
    sizes: &HashMap<GoString, f64>,
    by_partition: &HashMap<i32, std::collections::HashSet<GoString>>,
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
    to_remove: &mut Vec<GoString>,
    sizes: &HashMap<GoString, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<GoString>>,
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
            if let Some(set) = by_partition.get_mut(&id) {
                set.remove(&group);
            }
        }
    }
}
fn add_groups(
    to_add: &mut Vec<GoString>,
    sizes: &HashMap<GoString, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<GoString>>,
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
            if let Some(set) = by_partition.get_mut(&id) {
                set.insert(group);
            }
        }
    }
}
fn ensure_minimum_groups(
    minimum: i64,
    sizes: &HashMap<GoString, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<GoString>>,
) -> bool {
    // Go isolation_balancer.go ensureMinimumGroupsPerPartition: sort once, then
    // a single two-pointer pass (lo/hi advance past satisfied/exhausted ends).
    let mut changed = false;
    let mut ids: Vec<i32> = by_partition.keys().copied().collect();
    ids.sort_by(|a, b| {
        let (la, lb) = (by_partition[a].len(), by_partition[b].len());
        la.cmp(&lb).then_with(|| {
            partition_size(sizes, &by_partition[a])
                .partial_cmp(&partition_size(sizes, &by_partition[b]))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    if ids.is_empty() {
        return changed;
    }
    let (mut lo, mut hi) = (0usize, ids.len() - 1);
    while lo < hi {
        let (lo_id, hi_id) = (ids[lo], ids[hi]);
        if by_partition[&lo_id].len() >= minimum as usize {
            lo += 1;
            continue;
        }
        if by_partition[&hi_id].len() <= minimum as usize {
            hi -= 1;
            continue;
        }
        changed = true;
        let needed = minimum as usize - by_partition[&lo_id].len();
        let available = by_partition[&hi_id].len() - minimum as usize;
        let before = by_partition[&lo_id].len();
        move_groups(needed.min(available), hi_id, lo_id, sizes, by_partition);
        if by_partition[&lo_id].len() == before {
            // Go would spin forever when no movable group exists; stop instead.
            return changed;
        }
    }
    changed
}
fn move_groups(
    mut amount: usize,
    from: i32,
    to: i32,
    sizes: &HashMap<GoString, f64>,
    by_partition: &mut HashMap<i32, std::collections::HashSet<GoString>>,
) {
    while amount > 0 {
        amount -= 1;
        let from_size = partition_size(sizes, &by_partition[&from]);
        let to_size = partition_size(sizes, &by_partition[&to]);
        let mut best: Option<(f64, GoString)> = None;
        // Deterministic candidate order.
        let mut candidates: Vec<GoString> = by_partition[&from].iter().cloned().collect();
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
        if let Some(set) = by_partition.get_mut(&from) {
            set.remove(&group);
        }
        if let Some(set) = by_partition.get_mut(&to) {
            set.insert(group);
        }
    }
}

/// Live settings and isolation group names; group bytes are never decoded.
#[derive(Clone)]
pub struct NativeScalerConfig {
    pub settings: ScalerConfig,
    pub isolation_groups: Vec<GoString>,
}
pub type ConfigProvider =
    Arc<dyn Fn(&GoString, &GoString, i32) -> NativeScalerConfig + Send + Sync>;
/// Remote describe must route to the partition owner and return its raw group metrics.
#[derive(Default)]
pub struct DescribedPartition {
    pub qps: f64,
    pub empty: bool,
    pub has_write: bool,
    pub groups: HashMap<GoString, (f64, bool)>,
}
pub type DescribePartition = Arc<
    dyn Fn(
            NativeQueueRegistry,
            TaskListKeyBytes,
            i32,
            NativeScalerConfig,
        ) -> NativeFuture<Result<DescribedPartition, Status>>
        + Send
        + Sync,
>;

/// The callback is used for non-root partitions whenever membership is enabled.
pub fn starter(
    provider: ConfigProvider,
    describe_remote: DescribePartition,
) -> NativeScalerStarter {
    starter_with_metrics(provider, describe_remote, Arc::new(|_| {}))
}

/// Observations for wiring the scaler to the native metrics implementation.
#[derive(Clone, Debug)]
pub enum ScalerObservation {
    Calculation {
        qps: f64,
        current_write_count: usize,
        upscale_rps: f64,
        downscale_factor: f64,
        upscale_threshold: f64,
        downscale_threshold: f64,
        read_count: usize,
        write_count: usize,
        read_changed: bool,
        write_changed: bool,
        isolation_changed: bool,
    },
    ScaleChange(bool),
    IsolationRebalance,
    Group {
        group: GoString,
        before_assignable: bool,
        after_assignable: bool,
        before_count: i64,
        after_count: i64,
    },
    PartitionDrained,
    UpdateRequested,
    UpdateFailed,
}
pub type ScalerMetrics = Arc<dyn Fn(ScalerObservation) + Send + Sync>;
pub type ScalerMetricsProvider = Arc<dyn Fn(&TaskListKeyBytes) -> ScalerMetrics + Send + Sync>;

pub fn starter_with_metrics(
    provider: ConfigProvider,
    describe_remote: DescribePartition,
    metrics: ScalerMetrics,
) -> NativeScalerStarter {
    starter_with_metrics_provider(
        provider,
        describe_remote,
        Arc::new(move |_| metrics.clone()),
    )
}

pub fn starter_with_metrics_provider(
    provider: ConfigProvider,
    describe_remote: DescribePartition,
    metrics: ScalerMetricsProvider,
) -> NativeScalerStarter {
    Arc::new(move |host: NativeScalerHost| {
        let scoped_metrics = metrics(&host.root);
        let provider = provider.clone();
        let describe_remote = describe_remote.clone();
        Box::pin(async move { run(host, provider, describe_remote, scoped_metrics).await })
    })
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
fn has_write_partition(config: Option<&NativeStoredPartitionConfig>, id: i32) -> bool {
    config.is_none_or(|config| config.write_partitions.contains_key(&id))
}
async fn describe(
    registry: &NativeQueueRegistry,
    root: &TaskListKeyBytes,
    id: i32,
    config: &NativeScalerConfig,
    time: i64,
    remote: &DescribePartition,
) -> Result<DescribedPartition, Status> {
    if id != 0 && registry.has_membership() {
        return remote(registry.clone(), root.clone(), id, config.clone()).await;
    }
    describe_local(registry, root, id, config, time).await
}

/// Snapshot a partition owned by this process without retaining the registry in a callback.
pub async fn describe_local(
    registry: &NativeQueueRegistry,
    root: &TaskListKeyBytes,
    id: i32,
    config: &NativeScalerConfig,
    time: i64,
) -> Result<DescribedPartition, Status> {
    let name = TaskListNameBytes::parse(root.name.as_bytes()).map_err(|error| {
        crate::matching_native_status::raw_status(
            tonic::Code::InvalidArgument,
            &error,
            tonic::codegen::Bytes::new(),
        )
    })?;
    let key = TaskListKeyBytes {
        name: name.partition_name(id as i64),
        ..root.clone()
    };
    let handle = registry.get(key, 0, time).await?;
    let (_, _, backlog, _, _, empty) = handle.queue.status();
    let counts = handle
        .pollers
        .lock()
        .expect("poller history lock")
        .count_by_group(
            time,
            time.saturating_sub(config.settings.poller_window_nanos),
        );
    let groups = config
        .isolation_groups
        .iter()
        .map(|group| {
            (
                group.clone(),
                (
                    handle.qps.group_qps(group.as_bytes()),
                    counts.get(group).copied().unwrap_or(0) > 0,
                ),
            )
        })
        .collect();
    Ok(DescribedPartition {
        qps: handle.qps.qps(),
        empty: if config.settings.empty_check {
            empty
        } else {
            backlog == 0
        },
        has_write: has_write_partition(handle.partitions.get().as_ref(), id),
        groups,
    })
}
async fn run(
    host: NativeScalerHost,
    provider: ConfigProvider,
    remote: DescribePartition,
    observe: ScalerMetrics,
) {
    let root = host.root.clone();
    let domain = GoString::from(root.domain_id.to_string());
    let mut over = Sustain::default();
    let mut under = Sustain::default();
    let mut balancer = IsolationBalancer::default();
    loop {
        let config = provider(&domain, &root.name, root.task_type);
        tokio::time::sleep(Duration::from_nanos(
            config.settings.update_interval_nanos.max(0) as u64,
        ))
        .await;
        let config = provider(&domain, &root.name, root.task_type);
        let Some(registry) = host.registry() else {
            return;
        };
        // Do not recreate an owner removed while this loop was asleep.
        if !registry.contains_cached(&root) {
            return;
        }
        let time = now();
        let Ok(handle) = registry.get(root.clone(), 0, time).await else {
            return;
        };
        if handle.queue.is_stopped() {
            return;
        }
        if !config.settings.enabled || !config.settings.cache_enabled {
            continue;
        }
        let current = handle
            .partitions
            .get()
            .unwrap_or_else(|| NativeStoredPartitionConfig {
                version: 0,
                read_partitions: HashMap::from([(0, Vec::new())]),
                write_partitions: HashMap::from([(0, Vec::new())]),
            });
        let mut metrics = AggregateMetrics::default();
        if config.settings.isolation_assignment {
            metrics.isolation_enabled = true;
            let mut ids: Vec<_> = current.read_partitions.keys().copied().collect();
            ids.sort();
            let mut failed = false;
            for id in ids {
                let result = crate::matching_panic::capture(
                    "describe_partition",
                    describe(&registry, &root, id, &config, time, &remote),
                )
                .await;
                let partition = match result {
                    Ok(Ok(partition)) => partition,
                    _ => {
                        failed = true;
                        break;
                    }
                };
                metrics.total_qps += partition.qps;
                metrics.by_partition.insert(
                    id,
                    PartitionMetrics {
                        qps: partition.qps,
                        read_only: !partition.has_write,
                        empty: partition.empty,
                    },
                );
                for (group, (qps, pollers)) in partition.groups {
                    *metrics.qps_by_group.entry(group.clone()).or_insert(0.0) += qps;
                    *metrics.has_pollers_by_group.entry(group).or_insert(false) |= pollers;
                }
            }
            if failed {
                over.reset();
                under.reset();
                balancer.reset();
                continue;
            }
        } else {
            metrics.total_qps = handle.qps.qps() * current.write_partitions.len() as f64;
        }
        let target = write_partition_count(
            metrics.total_qps,
            current.write_partitions.len() as i64,
            &config.settings,
            &mut over,
            &mut under,
            time,
        )
        .max(0) as usize;
        let write_changed = target != current.write_partitions.len();
        if write_changed {
            observe(ScalerObservation::ScaleChange(
                target > current.write_partitions.len(),
            ));
        }
        let mut write = if write_changed {
            (0..target as i32)
                .map(|id| {
                    (
                        id,
                        current
                            .write_partitions
                            .get(&id)
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect()
        } else {
            current.write_partitions.clone()
        };
        let before_assignable: HashMap<_, _> = balancer
            .groups
            .iter()
            .map(|(g, s)| (g.clone(), s.assignable))
            .collect();
        let before_counts = balancer.partitions_per_group(&write);
        let isolation_changed = if metrics.isolation_enabled {
            let (next, changed) =
                balancer.adjust_write_partitions(&config.settings, time, &metrics, &write);
            write = next;
            changed
        } else {
            balancer.reset();
            false
        };
        if isolation_changed {
            observe(ScalerObservation::IsolationRebalance);
        }
        let after_counts = balancer.partitions_per_group(&write);
        let mut groups: Vec<_> = metrics
            .has_pollers_by_group
            .keys()
            .chain(before_assignable.keys())
            .chain(balancer.groups.keys())
            .chain(before_counts.keys())
            .chain(after_counts.keys())
            .cloned()
            .collect();
        groups.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        groups.dedup();
        for group in groups {
            observe(ScalerObservation::Group {
                before_assignable: before_assignable.get(&group).copied().unwrap_or(false),
                after_assignable: balancer.groups.get(&group).is_some_and(|s| s.assignable),
                before_count: before_counts.get(&group).copied().unwrap_or(0),
                after_count: after_counts.get(&group).copied().unwrap_or(0),
                group,
            });
        }
        let mut read = current.read_partitions.clone();
        let mut read_changed = false;
        for (id, groups) in &write {
            if !read.contains_key(id) {
                read_changed = true;
            }
            read.insert(*id, groups.clone());
        }
        let mut id = read.len() as i32 - 1;
        while id >= write.len() as i32 {
            let entry = if let Some(entry) = metrics.by_partition.get(&id) {
                Some(entry.clone())
            } else {
                describe(&registry, &root, id, &config, time, &remote)
                    .await
                    .ok()
                    .map(|p| PartitionMetrics {
                        qps: p.qps,
                        read_only: !p.has_write,
                        empty: p.empty,
                    })
            };
            if entry.is_some_and(|p| p.read_only && p.empty) {
                read.remove(&id);
                read_changed = true;
            } else {
                break;
            }
            id -= 1;
        }
        if read_changed && read.len() < current.read_partitions.len() {
            observe(ScalerObservation::PartitionDrained);
        }
        observe(ScalerObservation::Calculation {
            qps: metrics.total_qps,
            current_write_count: current.write_partitions.len(),
            upscale_rps: config.settings.upscale_rps,
            downscale_factor: config.settings.downscale_factor,
            upscale_threshold: current.write_partitions.len() as f64 * config.settings.upscale_rps,
            downscale_threshold: (current.write_partitions.len() as f64 - 1.0)
                * config.settings.upscale_rps
                * config.settings.downscale_factor,
            read_count: read.len(),
            write_count: write.len(),
            read_changed,
            write_changed,
            isolation_changed,
        });
        if write_changed || isolation_changed || read_changed {
            observe(ScalerObservation::UpdateRequested);
            if let Err(error) = registry
                .update_partition_config_as_scaler(
                    root.clone(),
                    handle,
                    NativeStoredPartitionConfig {
                        version: 0,
                        read_partitions: read,
                        write_partitions: write,
                    },
                    time,
                )
                .await
            {
                observe(ScalerObservation::UpdateFailed);
                eprintln!("Matching adaptive scaler failed: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn drain_uses_described_partition_config_and_treats_missing_config_as_writable() {
        let config = NativeStoredPartitionConfig {
            version: 1,
            read_partitions: HashMap::from([(0, vec![]), (1, vec![])]),
            write_partitions: HashMap::from([(0, vec![]), (1, vec![])]),
        };
        // A proposed shrink does not make a partition read-only until its
        // owner reports the updated configuration.
        assert!(has_write_partition(Some(&config), 1));
        assert!(has_write_partition(None, 1));
        assert!(!has_write_partition(Some(&config), 2));
    }
    #[test]
    fn balancing_moves_groups_without_changing_their_bytes() {
        let a = GoString::from(&b"\xff"[..]);
        let b = GoString::from(&b"\xfe"[..]);
        let mut partitions = HashMap::from([
            (0, std::collections::HashSet::new()),
            (1, std::collections::HashSet::from([a.clone(), b.clone()])),
        ]);
        assert!(ensure_minimum_groups(1, &HashMap::new(), &mut partitions));
        assert_eq!(partitions[&0].len(), 1);
        assert_eq!(partitions[&1].len(), 1);
        assert_eq!(
            partitions
                .values()
                .flatten()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            std::collections::HashSet::from([a, b])
        );
    }
    #[test]
    fn sustained_load_scales_write_partitions() {
        let config = ScalerConfig {
            upscale_sustain_nanos: 10,
            ..ScalerConfig::default()
        };
        let (mut over, mut under) = (Sustain::default(), Sustain::default());
        assert_eq!(
            write_partition_count(500.0, 1, &config, &mut over, &mut under, 0),
            1
        );
        assert_eq!(
            write_partition_count(500.0, 1, &config, &mut over, &mut under, 10),
            3
        );
    }
    #[test]
    fn balancer_keeps_invalid_byte_groups_distinct() {
        let config = ScalerConfig {
            group_has_pollers_sustain_nanos: 0,
            groups_per_partition: 1,
            ..ScalerConfig::default()
        };
        let a = GoString::from(&b"\xff"[..]);
        let b = GoString::from(&b"\xfe"[..]);
        let mut metrics = AggregateMetrics::default();
        metrics.has_pollers_by_group.insert(a.clone(), true);
        metrics.has_pollers_by_group.insert(b.clone(), true);
        let mut balancer = IsolationBalancer::default();
        let (write, changed) = balancer.adjust_write_partitions(
            &config,
            0,
            &metrics,
            &HashMap::from([(0, vec![]), (1, vec![])]),
        );
        assert!(changed);
        let groups: std::collections::HashSet<_> = write.values().flatten().cloned().collect();
        assert_eq!(groups, std::collections::HashSet::from([a, b]));
    }
}
