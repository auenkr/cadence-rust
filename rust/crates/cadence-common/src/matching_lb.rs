//! Port of Go `client/matching` partition selection: `PartitionConfigProvider`,
//! the random (`defaultLoadBalancer`), `roundRobinLoadBalancer`,
//! `weightedLoadBalancer`, `isolationLoadBalancer` and `multiLoadBalancer`.
//! Frontend (poll/query) and History (add task) use it to spread calls across
//! task-list partitions exactly as Go's matching client does.

use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Go `constants.ReservedTaskListPrefix`.
pub const RESERVED_TASK_LIST_PREFIX: &str = "/__cadence_sys/";
/// Go `persistence.TaskListTypeDecision` / `TaskListTypeActivity`.
pub const TASK_LIST_TYPE_DECISION: i32 = 0;
pub const TASK_LIST_TYPE_ACTIVITY: i32 = 1;
/// Go `_backlogThreshold`.
pub const BACKLOG_THRESHOLD: i64 = 100;
/// Go `isolationgroup.GroupKey`.
pub const ISOLATION_GROUP_KEY: &str = "isolation-group";
/// Go cache `MaxCount: 3000` for every LB/provider cache.
const CACHE_MAX: usize = 3000;

/// Dynamic config the Go client reads (filtered by domain name, task list, type).
pub trait LbDynamicConfig: Send + Sync {
    /// Go `domainIDToName`; `None` = lookup error.
    fn domain_name(&self, domain_id: &str) -> Option<String>;
    /// `matching.numTasklistReadPartitions` (default 1).
    fn num_read_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64;
    /// `matching.numTasklistWritePartitions` (default 1).
    fn num_write_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64;
    /// `matching.enableGetNumberOfPartitionsFromCache` (default false).
    fn read_from_cache(&self, domain: &str, task_list: &str, task_type: i32) -> bool;
    /// `system.tasklistLoadBalancerStrategy` (default "weighted").
    fn strategy(&self, domain: &str, task_list: &str, task_type: i32) -> String;
    /// `matching.enablePartitionIsolationGroupAssignment` (default false).
    fn isolation_assignment_enabled(&self, domain: &str) -> bool;
}

/// Go `types.TaskListPartitionConfig` (partition id -> isolation groups).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionConfig {
    pub version: i64,
    pub read_partitions: BTreeMap<i32, Vec<String>>,
    pub write_partitions: BTreeMap<i32, Vec<String>>,
}

fn default_config(n_read: i64, n_write: i64) -> PartitionConfig {
    PartitionConfig {
        version: 0,
        read_partitions: (0..n_read.max(0) as i32).map(|i| (i, Vec::new())).collect(),
        write_partitions: (0..n_write.max(0) as i32).map(|i| (i, Vec::new())).collect(),
    }
}

impl PartitionConfig {
    /// Go proto mapper: deprecated counts win when they disagree with the maps.
    pub fn from_parts(
        version: i64,
        num_read: i32,
        read: HashMap<i32, Vec<String>>,
        num_write: i32,
        write: HashMap<i32, Vec<String>>,
    ) -> Self {
        let fix = |n: i32, m: HashMap<i32, Vec<String>>| -> BTreeMap<i32, Vec<String>> {
            crate::task_list::partition_map(n, m).into_iter().collect()
        };
        Self { version, read_partitions: fix(num_read, read), write_partitions: fix(num_write, write) }
    }
}

/// Go `types.LoadBalancerHints`.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoadBalancerHints {
    pub backlog_count: i64,
    pub rate_per_second: f64,
}

/// Request fields the Go LB interfaces read.
#[derive(Clone, Debug, Default)]
pub struct LbRequest<'a> {
    pub domain_id: &'a str,
    pub task_list: &'a str,
    /// true when the task list kind is NORMAL.
    pub kind_normal: bool,
    pub forwarded_from: &'a str,
    /// Write requests: Go `GetPartitionConfig()` (isolation group key).
    pub partition_config: Option<&'a HashMap<String, String>>,
}

type Key = (String, String, i32);

fn key(req: &LbRequest<'_>, task_type: i32) -> Key {
    (req.domain_id.to_owned(), req.task_list.to_owned(), task_type)
}

/// Bounded map standing in for Go's LRU `cache.Cache` (evicts arbitrarily when full).
struct BoundedMap<V>(Mutex<HashMap<Key, V>>);

impl<V: Clone> BoundedMap<V> {
    fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
    fn get(&self, k: &Key) -> Option<V> {
        self.0.lock().unwrap().get(k).cloned()
    }
    fn get_or_insert(&self, k: Key, v: impl FnOnce() -> V) -> V {
        let mut m = self.0.lock().unwrap();
        if !m.contains_key(&k) && m.len() >= CACHE_MAX {
            if let Some(evict) = m.keys().next().cloned() {
                m.remove(&evict);
            }
        }
        m.entry(k).or_insert_with(v).clone()
    }
    fn remove(&self, k: &Key) {
        self.0.lock().unwrap().remove(k);
    }
}

/// Go `partitionConfigProviderImpl`.
pub struct PartitionConfigProvider {
    dc: Arc<dyn LbDynamicConfig>,
    cache: BoundedMap<Arc<RwLock<PartitionConfig>>>,
}

impl PartitionConfigProvider {
    pub fn new(dc: Arc<dyn LbDynamicConfig>) -> Self {
        Self { dc, cache: BoundedMap::new() }
    }

    pub fn number_of_read_partitions(&self, domain_id: &str, tl: &str, normal: bool, tt: i32) -> usize {
        self.partition_config(domain_id, tl, normal, tt).read_partitions.len()
    }

    pub fn number_of_write_partitions(&self, domain_id: &str, tl: &str, normal: bool, tt: i32) -> usize {
        let c = self.partition_config(domain_id, tl, normal, tt);
        c.write_partitions.len().min(c.read_partitions.len())
    }

    pub fn partition_config(&self, domain_id: &str, tl: &str, normal: bool, tt: i32) -> PartitionConfig {
        let Some(domain) = self.dc.domain_name(domain_id) else {
            return default_config(1, 1);
        };
        if !self.dc.read_from_cache(&domain, tl, tt) {
            let n_read = self.dc.num_read_partitions(&domain, tl, tt);
            let n_write = self.dc.num_write_partitions(&domain, tl, tt).min(n_read);
            return default_config(n_read, n_write);
        }
        if !normal {
            return default_config(1, 1);
        }
        match self.cache.get(&(domain_id.to_owned(), tl.to_owned(), tt)) {
            Some(c) => c.read().unwrap().clone(),
            None => default_config(1, 1),
        }
    }

    /// Go `UpdatePartitionConfig`: keep the highest version.
    pub fn update_partition_config(&self, domain_id: &str, tl: &str, normal: bool, tt: i32, config: Option<PartitionConfig>) {
        let Some(config) = config else { return };
        if !normal {
            return;
        }
        let entry = self
            .cache
            .get_or_insert((domain_id.to_owned(), tl.to_owned(), tt), || Arc::new(RwLock::new(config.clone())));
        let mut c = entry.write().unwrap();
        if c.version < config.version {
            *c = config;
        }
    }

    /// Go `InvalidatePartitionCache`.
    pub fn invalidate(&self, domain_id: &str, tl: &str, normal: bool, tt: i32) {
        if normal {
            self.cache.remove(&(domain_id.to_owned(), tl.to_owned(), tt));
        }
    }
}

/// Go `getPartitionTaskListName`.
pub fn partition_task_list_name(root: &str, partition: i64) -> String {
    if partition <= 0 {
        root.to_owned()
    } else {
        format!("{RESERVED_TASK_LIST_PREFIX}{root}/{partition}")
    }
}

/// Go `weightSelector`.
struct WeightSelector {
    weights: Vec<i64>,
    initialized: bool,
    threshold: i64,
}

impl WeightSelector {
    fn new(n: usize, threshold: i64) -> Self {
        Self { weights: vec![-1; n], initialized: false, threshold }
    }

    fn pick_between(&self, partitions: &[i32]) -> i32 {
        if !self.initialized {
            return -1;
        }
        let mut total = 0i64;
        let mut cumulative = Vec::with_capacity(partitions.len());
        let mut enable = false;
        for &p in partitions {
            let Some(&w) = self.weights.get(p as usize).filter(|_| p >= 0) else {
                return -1;
            };
            total += w;
            cumulative.push(total);
            if w > self.threshold {
                enable = true;
            }
        }
        if total <= 0 || !enable {
            return -1;
        }
        let r = rand::thread_rng().gen_range(0..total);
        let idx = cumulative.partition_point(|&c| c <= r);
        partitions[idx]
    }

    fn pick(&self) -> i32 {
        if !self.initialized {
            return -1;
        }
        let mut total = 0i64;
        let mut cumulative = Vec::with_capacity(self.weights.len());
        let mut drain = false;
        for &w in &self.weights {
            total += w;
            cumulative.push(total);
            if w > self.threshold {
                drain = true;
            }
        }
        if total <= 0 || !drain {
            return -1;
        }
        let r = rand::thread_rng().gen_range(0..total);
        cumulative.partition_point(|&c| c <= r) as i32
    }

    fn update(&mut self, n: usize, p: usize, weight: i64) {
        if n > self.weights.len() {
            self.weights.resize(n, -1);
            self.initialized = false;
        } else if n < self.weights.len() {
            self.weights.truncate(n);
        }
        if p < n {
            self.weights[p] = weight;
        }
        if self.weights.iter().all(|&w| w != -1) {
            self.initialized = true;
        }
    }
}

/// Go `calcWeightFromLoadBalancerHints`.
pub fn calc_weight(info: &LoadBalancerHints) -> i64 {
    let mut smoothing = 0;
    if info.rate_per_second > 0.01 {
        smoothing = (info.rate_per_second * 0.01).ceil() as i64;
    }
    info.backlog_count + smoothing
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Strategy {
    Random,
    RoundRobin,
    Weighted,
    Isolation,
}

/// Go `multiLoadBalancer` over the four Go strategies.
pub struct MatchingLoadBalancer {
    provider: Arc<PartitionConfigProvider>,
    dc: Arc<dyn LbDynamicConfig>,
    rr_read: BoundedMap<Arc<AtomicI64>>,
    rr_write: BoundedMap<Arc<AtomicI64>>,
    weights: BoundedMap<Arc<RwLock<WeightSelector>>>,
}

impl MatchingLoadBalancer {
    pub fn new(dc: Arc<dyn LbDynamicConfig>) -> Self {
        Self {
            provider: Arc::new(PartitionConfigProvider::new(dc.clone())),
            dc,
            rr_read: BoundedMap::new(),
            rr_write: BoundedMap::new(),
            weights: BoundedMap::new(),
        }
    }

    pub fn provider(&self) -> &PartitionConfigProvider {
        &self.provider
    }

    fn can_redirect(req: &LbRequest<'_>) -> bool {
        req.forwarded_from.is_empty() && req.kind_normal && !req.task_list.starts_with(RESERVED_TASK_LIST_PREFIX)
    }

    /// `None` = domain lookup failed (Go uses the default random LB).
    fn strategy(&self, req: &LbRequest<'_>, tt: i32) -> Option<Strategy> {
        let domain = self.dc.domain_name(req.domain_id)?;
        Some(match self.dc.strategy(&domain, req.task_list, tt).as_str() {
            "random" => Strategy::Random,
            "round-robin" => Strategy::RoundRobin,
            "weighted" => Strategy::Weighted,
            "isolation" => Strategy::Isolation,
            other => {
                eprintln!("WARN unsupported load balancer strategy value={other}");
                Strategy::Random
            }
        })
    }

    fn n_write(&self, req: &LbRequest<'_>, tt: i32) -> usize {
        self.provider.number_of_write_partitions(req.domain_id, req.task_list, req.kind_normal, tt)
    }

    fn n_read(&self, req: &LbRequest<'_>, tt: i32) -> usize {
        self.provider.number_of_read_partitions(req.domain_id, req.task_list, req.kind_normal, tt)
    }

    fn random(&self, req: &LbRequest<'_>, n: usize) -> String {
        if n <= 1 {
            return req.task_list.to_owned();
        }
        partition_task_list_name(req.task_list, rand::thread_rng().gen_range(0..n) as i64)
    }

    fn round_robin(&self, req: &LbRequest<'_>, tt: i32, n: usize, write: bool) -> String {
        if n <= 1 {
            return req.task_list.to_owned();
        }
        let cache = if write { &self.rr_write } else { &self.rr_read };
        let counter = cache.get_or_insert(key(req, tt), || Arc::new(AtomicI64::new(-1)));
        let v = counter.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        partition_task_list_name(req.task_list, v % n as i64)
    }

    fn weighted_read(&self, req: &LbRequest<'_>, tt: i32) -> String {
        if let Some(w) = self.weights.get(&key(req, tt)) {
            let p = w.read().unwrap().pick();
            if p >= 0 {
                return partition_task_list_name(req.task_list, i64::from(p));
            }
        }
        self.round_robin(req, tt, self.n_read(req, tt), false)
    }

    /// Go `weightedLoadBalancer.PickBetween`.
    fn pick_between(&self, req: &LbRequest<'_>, tt: i32, partitions: &[i32]) -> i32 {
        match partitions.len() {
            0 => return -1,
            1 => return partitions[0],
            _ => {}
        }
        match self.weights.get(&key(req, tt)) {
            Some(w) => w.read().unwrap().pick_between(partitions),
            None => -1,
        }
    }

    pub fn pick_write_partition(&self, tt: i32, req: &LbRequest<'_>) -> String {
        if !Self::can_redirect(req) {
            return req.task_list.to_owned();
        }
        match self.strategy(req, tt) {
            None | Some(Strategy::Random) => self.random(req, self.n_write(req, tt)),
            Some(Strategy::RoundRobin) | Some(Strategy::Weighted) => {
                self.round_robin(req, tt, self.n_write(req, tt), true)
            }
            Some(Strategy::Isolation) => self.isolation_write(req, tt),
        }
    }

    pub fn pick_read_partition(&self, tt: i32, req: &LbRequest<'_>, isolation_group: &str) -> String {
        if !Self::can_redirect(req) {
            return req.task_list.to_owned();
        }
        match self.strategy(req, tt) {
            None | Some(Strategy::Random) => self.random(req, self.n_read(req, tt)),
            Some(Strategy::RoundRobin) => self.round_robin(req, tt, self.n_read(req, tt), false),
            Some(Strategy::Weighted) => self.weighted_read(req, tt),
            Some(Strategy::Isolation) => self.isolation_read(req, tt, isolation_group),
        }
    }

    pub fn update_weight(&self, tt: i32, req: &LbRequest<'_>, partition: &str, info: Option<LoadBalancerHints>) {
        if !Self::can_redirect(req) {
            return;
        }
        if !matches!(self.strategy(req, tt), Some(Strategy::Weighted) | Some(Strategy::Isolation)) {
            return;
        }
        let Some(info) = info else { return };
        let p = if partition != req.task_list {
            // Go strconv.Atoi(path.Base(partition))
            match partition.rsplit('/').next().unwrap_or("").parse::<usize>() {
                Ok(p) => p,
                Err(_) => return,
            }
        } else {
            0
        };
        let k = key(req, tt);
        let n = self.n_read(req, tt);
        if n <= 1 {
            self.weights.remove(&k);
            return;
        }
        let w = self
            .weights
            .get_or_insert(k, || Arc::new(RwLock::new(WeightSelector::new(n, BACKLOG_THRESHOLD))));
        w.write().unwrap().update(n, p, calc_weight(&info));
    }

    fn isolation_enabled(&self, req: &LbRequest<'_>) -> bool {
        self.dc
            .domain_name(req.domain_id)
            .is_some_and(|d| self.dc.isolation_assignment_enabled(&d))
    }

    fn isolation_write(&self, req: &LbRequest<'_>, tt: i32) -> String {
        let fallback = || self.round_robin(req, tt, self.n_write(req, tt), true);
        if !self.isolation_enabled(req) {
            return fallback();
        }
        let group = req
            .partition_config
            .and_then(|c| c.get(ISOLATION_GROUP_KEY))
            .filter(|g| !g.is_empty());
        let Some(group) = group else { return fallback() };
        let config = self.provider.partition_config(req.domain_id, req.task_list, req.kind_normal, tt);
        if config.write_partitions.len() == 1 {
            return req.task_list.to_owned();
        }
        let partitions = partitions_for_group(group, &config.write_partitions);
        if partitions.is_empty() {
            return fallback();
        }
        let p = partitions[rand::thread_rng().gen_range(0..partitions.len())];
        partition_task_list_name(req.task_list, i64::from(p))
    }

    fn isolation_read(&self, req: &LbRequest<'_>, tt: i32, group: &str) -> String {
        if !self.isolation_enabled(req) || group.is_empty() {
            return self.weighted_read(req, tt);
        }
        let config = self.provider.partition_config(req.domain_id, req.task_list, req.kind_normal, tt);
        if config.read_partitions.len() == 1 {
            return req.task_list.to_owned();
        }
        let partitions = partitions_for_group(group, &config.read_partitions);
        if partitions.is_empty() {
            return self.weighted_read(req, tt);
        }
        let mut p = partitions[0];
        if partitions.len() > 1 {
            p = self.pick_between(req, tt, &partitions);
            if p == -1 {
                p = partitions[rand::thread_rng().gen_range(0..partitions.len())];
            }
        }
        partition_task_list_name(req.task_list, i64::from(p))
    }
}

/// Go `getPartitionsForGroup` (iterates ids 0..len).
fn partitions_for_group(group: &str, partitions: &BTreeMap<i32, Vec<String>>) -> Vec<i32> {
    (0..partitions.len() as i32)
        .filter(|id| match partitions.get(id) {
            Some(groups) => groups.is_empty() || groups.iter().any(|g| g == group),
            // Go dereferences a nil partition here; a missing id never accepts.
            None => false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dc {
        read: i64,
        write: i64,
        strategy: &'static str,
        cache: bool,
    }

    impl LbDynamicConfig for Dc {
        fn domain_name(&self, id: &str) -> Option<String> {
            (id != "missing").then(|| "d".to_owned())
        }
        fn num_read_partitions(&self, _: &str, _: &str, _: i32) -> i64 {
            self.read
        }
        fn num_write_partitions(&self, _: &str, _: &str, _: i32) -> i64 {
            self.write
        }
        fn read_from_cache(&self, _: &str, _: &str, _: i32) -> bool {
            self.cache
        }
        fn strategy(&self, _: &str, _: &str, _: i32) -> String {
            self.strategy.to_owned()
        }
        fn isolation_assignment_enabled(&self, _: &str) -> bool {
            false
        }
    }

    fn lb(read: i64, write: i64, strategy: &'static str) -> MatchingLoadBalancer {
        MatchingLoadBalancer::new(Arc::new(Dc { read, write, strategy, cache: false }))
    }

    fn req(tl: &str) -> LbRequest<'_> {
        LbRequest { domain_id: "id", task_list: tl, kind_normal: true, ..Default::default() }
    }

    #[test]
    fn partition_names_match_go() {
        assert_eq!(partition_task_list_name("tl", 0), "tl");
        assert_eq!(partition_task_list_name("tl", 3), "/__cadence_sys/tl/3");
    }

    #[test]
    fn single_partition_returns_root() {
        let lb = lb(1, 1, "weighted");
        assert_eq!(lb.pick_write_partition(0, &req("tl")), "tl");
        assert_eq!(lb.pick_read_partition(0, &req("tl"), ""), "tl");
    }

    #[test]
    fn weighted_write_is_round_robin_from_zero() {
        let lb = lb(4, 4, "weighted");
        let picks: Vec<_> = (0..5).map(|_| lb.pick_write_partition(0, &req("tl"))).collect();
        assert_eq!(picks, ["tl", "/__cadence_sys/tl/1", "/__cadence_sys/tl/2", "/__cadence_sys/tl/3", "tl"]);
    }

    #[test]
    fn read_and_write_counters_are_independent() {
        let lb = lb(4, 4, "round-robin");
        assert_eq!(lb.pick_write_partition(0, &req("tl")), "tl");
        assert_eq!(lb.pick_read_partition(0, &req("tl"), ""), "tl");
        assert_eq!(lb.pick_read_partition(1, &req("tl"), ""), "tl");
        assert_eq!(lb.pick_read_partition(0, &req("tl"), ""), "/__cadence_sys/tl/1");
    }

    #[test]
    fn write_partitions_clamped_to_read() {
        let lb = lb(2, 4, "round-robin");
        let picks: Vec<_> = (0..3).map(|_| lb.pick_write_partition(0, &req("tl"))).collect();
        assert_eq!(picks, ["tl", "/__cadence_sys/tl/1", "tl"]);
    }

    #[test]
    fn no_redirect_for_forwarded_sticky_or_partition() {
        let lb = lb(4, 4, "random");
        let mut r = req("tl");
        r.forwarded_from = "/__cadence_sys/tl/1";
        assert_eq!(lb.pick_write_partition(0, &r), "tl");
        let mut r = req("tl");
        r.kind_normal = false;
        assert_eq!(lb.pick_read_partition(0, &r, ""), "tl");
        assert_eq!(lb.pick_read_partition(0, &req("/__cadence_sys/tl/2"), ""), "/__cadence_sys/tl/2");
    }

    #[test]
    fn random_stays_in_range() {
        let lb = lb(4, 4, "random");
        for _ in 0..100 {
            let p = lb.pick_read_partition(0, &req("tl"), "");
            assert!(p == "tl" || ["1", "2", "3"].iter().any(|i| p == format!("/__cadence_sys/tl/{i}")));
        }
    }

    #[test]
    fn domain_lookup_failure_uses_random() {
        let lb = lb(4, 4, "round-robin");
        let r = LbRequest { domain_id: "missing", task_list: "tl", kind_normal: true, ..Default::default() };
        // default config(1,1) on lookup failure -> root
        assert_eq!(lb.pick_write_partition(0, &r), "tl");
    }

    #[test]
    fn weighted_read_drains_backlogged_partition() {
        let lb = lb(2, 2, "weighted");
        let hints = |b| Some(LoadBalancerHints { backlog_count: b, rate_per_second: 0.0 });
        lb.update_weight(0, &req("tl"), "tl", hints(0));
        // not initialized until all partitions reported -> round robin
        assert_eq!(lb.pick_read_partition(0, &req("tl"), ""), "tl");
        lb.update_weight(0, &req("tl"), "/__cadence_sys/tl/1", hints(1000));
        for _ in 0..20 {
            assert_eq!(lb.pick_read_partition(0, &req("tl"), ""), "/__cadence_sys/tl/1");
        }
        // below threshold -> fallback round robin again
        lb.update_weight(0, &req("tl"), "/__cadence_sys/tl/1", hints(50));
        let a = lb.pick_read_partition(0, &req("tl"), "");
        let b = lb.pick_read_partition(0, &req("tl"), "");
        assert_ne!(a, b);
    }

    #[test]
    fn calc_weight_adds_littles_law_smoothing() {
        assert_eq!(calc_weight(&LoadBalancerHints { backlog_count: 5, rate_per_second: 0.0 }), 5);
        assert_eq!(calc_weight(&LoadBalancerHints { backlog_count: 5, rate_per_second: 250.0 }), 8);
    }

    #[test]
    fn weight_selector_resize() {
        let mut w = WeightSelector::new(2, 0);
        w.update(2, 0, 1);
        w.update(2, 1, 1);
        assert!(w.initialized);
        w.update(3, 0, 1);
        assert!(!w.initialized);
        w.update(1, 5, 9);
        assert_eq!(w.weights, vec![1]);
    }

    #[test]
    fn provider_cache_keeps_highest_version() {
        let p = PartitionConfigProvider::new(Arc::new(Dc { read: 1, write: 1, strategy: "weighted", cache: true }));
        assert_eq!(p.number_of_read_partitions("id", "tl", true, 0), 1);
        let mut c = default_config(4, 3);
        c.version = 2;
        p.update_partition_config("id", "tl", true, 0, Some(c.clone()));
        assert_eq!(p.number_of_read_partitions("id", "tl", true, 0), 4);
        assert_eq!(p.number_of_write_partitions("id", "tl", true, 0), 3);
        let mut old = default_config(2, 2);
        old.version = 1;
        p.update_partition_config("id", "tl", true, 0, Some(old));
        assert_eq!(p.number_of_read_partitions("id", "tl", true, 0), 4);
        p.invalidate("id", "tl", true, 0);
        assert_eq!(p.number_of_read_partitions("id", "tl", true, 0), 1);
    }
}
