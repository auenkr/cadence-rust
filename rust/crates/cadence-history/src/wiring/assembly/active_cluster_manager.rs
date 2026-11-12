//! Port of Go `common/activecluster/manager.go` (`managerImpl`): active cluster
//! lookup by cluster attribute / workflow, the persisted selection-policy read
//! and the 10s / 1000 entry selection-policy cache.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cadence_api::types::{ActiveClusterInfo, ActiveClusterSelectionPolicy, ClusterAttribute};
use cadence_persistence::history_contract::{CurrentExecution, DataBlob, GoExecutionStore, PersistenceError};
use cadence_persistence::history_reader::{decode_active_cluster_selection_policy, is_workflow_running};

use crate::engine::deps::{DomainCacheEntry, HResult, HistoryError};

/// Go `workflowPolicyCacheTTL`.
pub const WORKFLOW_POLICY_CACHE_TTL: Duration = Duration::from_secs(10);
/// Go `workflowPolicyCacheMaxCount`.
pub const WORKFLOW_POLICY_CACHE_MAX_COUNT: usize = 1000;

/// Go `DomainIDToDomainFn`, returning the entry plus `ReplicationConfig.IsActiveActive()`.
pub type DomainIdToDomainFn = Arc<dyn Fn(&str) -> HResult<(Arc<DomainCacheEntry>, bool)> + Send + Sync>;

/// The two `persistence.ExecutionManager` calls the manager uses.
#[async_trait]
pub trait PolicySource: Send + Sync {
    async fn get_current_execution(&self, shard_id: i32, domain_id: &str, workflow_id: &str) -> Result<CurrentExecution, PersistenceError>;
    async fn get_active_cluster_selection_policy(&self, shard_id: i32, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<Option<DataBlob>, PersistenceError>;
}

#[async_trait]
impl PolicySource for Arc<dyn GoExecutionStore> {
    async fn get_current_execution(&self, shard_id: i32, domain_id: &str, workflow_id: &str) -> Result<CurrentExecution, PersistenceError> {
        GoExecutionStore::get_current_execution(&**self, shard_id, domain_id.to_owned(), workflow_id.to_owned()).await
    }
    async fn get_active_cluster_selection_policy(&self, shard_id: i32, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<Option<DataBlob>, PersistenceError> {
        GoExecutionStore::get_active_cluster_selection_policy(&**self, shard_id, domain_id.to_owned(), workflow_id.to_owned(), run_id.to_owned()).await
    }
}

/// Port of Go `cache.lru` as used by the manager (`ActivelyEvict: true`, `Pin: false`, count based,
/// `MaxCount` 1000, TTL 10s). The recency list is ordered by access: `Get` and `Put` move the
/// entry to the front; expired entries are dropped from the back while they are expired
/// (`evictExpiredItems`); after an insert the oldest-accessed entries go while `len > maxCount`.
/// An entry is expired when `now > createTime + ttl`; `Put` on an existing key refreshes `createTime`.
#[derive(Default)]
pub struct PolicyCache {
    inner: Mutex<LruInner>,
}

#[derive(Default)]
struct LruInner {
    entries: HashMap<String, CacheEntry>,
    /// access sequence -> key; the largest sequence is the list front, the smallest the back.
    by_access: BTreeMap<u64, String>,
    seq: u64,
}

struct CacheEntry {
    policy: ActiveClusterSelectionPolicy,
    created: Instant,
    seq: u64,
}

impl LruInner {
    fn expired(created: Instant, now: Instant) -> bool {
        now > created + WORKFLOW_POLICY_CACHE_TTL
    }

    fn remove(&mut self, key: &str) {
        if let Some(e) = self.entries.remove(key) {
            self.by_access.remove(&e.seq);
        }
    }

    /// Go `evictExpiredItems`: stop at the first non-expired entry from the back.
    fn evict_expired(&mut self, now: Instant) {
        while let Some((_, key)) = self.by_access.iter().next().map(|(s, k)| (*s, k.clone())) {
            let created = self.entries[&key].created;
            if !Self::expired(created, now) {
                break;
            }
            self.remove(&key);
        }
    }

    fn move_to_front(&mut self, key: &str) {
        self.seq += 1;
        let seq = self.seq;
        if let Some(e) = self.entries.get_mut(key) {
            self.by_access.remove(&e.seq);
            e.seq = seq;
            self.by_access.insert(seq, key.to_owned());
        }
    }
}

impl PolicyCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, LruInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `lru.Get`.
    pub fn get(&self, key: &str, now: Instant) -> Option<ActiveClusterSelectionPolicy> {
        let mut c = self.lock();
        c.evict_expired(now);
        let created = c.entries.get(key)?.created;
        if LruInner::expired(created, now) {
            c.remove(key);
            return None;
        }
        c.move_to_front(key);
        c.entries.get(key).map(|e| e.policy.clone())
    }

    /// Go `lru.Put`.
    pub fn put(&self, key: String, policy: ActiveClusterSelectionPolicy, now: Instant) {
        let mut c = self.lock();
        c.evict_expired(now);
        if let Some(created) = c.entries.get(&key).map(|e| e.created) {
            if LruInner::expired(created, now) {
                c.remove(&key);
            } else {
                if let Some(e) = c.entries.get_mut(&key) {
                    e.policy = policy;
                    e.created = now;
                }
                c.move_to_front(&key);
                return;
            }
        }
        c.seq += 1;
        let seq = c.seq;
        c.by_access.insert(seq, key.clone());
        c.entries.insert(key, CacheEntry { policy, created: now, seq });
        while c.entries.len() > WORKFLOW_POLICY_CACHE_MAX_COUNT {
            let Some(back) = c.by_access.values().next().cloned() else { break };
            c.remove(&back);
        }
    }

    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Go `getClusterSelectionPolicy` errors: `NotExists` is `types.EntityNotExistsError`.
#[derive(Debug)]
enum PolicyError {
    NotExists(String),
    Other(HistoryError),
}

impl PolicyError {
    fn into_history(self) -> HistoryError {
        match self {
            PolicyError::NotExists(m) => HistoryError::entity_not_exists(m),
            PolicyError::Other(e) => e,
        }
    }
}

fn persistence_error(e: PersistenceError) -> PolicyError {
    match e {
        PersistenceError::EntityNotExists(m) => PolicyError::NotExists(m),
        other => PolicyError::Other(HistoryError::internal(other.to_string())),
    }
}

/// Go `activecluster.managerImpl`.
pub struct ActiveClusterManagerImpl {
    domain_id_to_domain: DomainIdToDomainFn,
    executions: Arc<dyn PolicySource>,
    num_shards: i32,
    cache: PolicyCache,
}

impl ActiveClusterManagerImpl {
    /// Go `activecluster.NewManager`.
    pub fn new(domain_id_to_domain: DomainIdToDomainFn, executions: Arc<dyn PolicySource>, num_shards: i32) -> Self {
        Self { domain_id_to_domain, executions, num_shards: num_shards.max(1), cache: PolicyCache::default() }
    }

    fn shard_of(&self, workflow_id: &str) -> i32 {
        crate::shard::ShardController::workflow_id_to_shard(workflow_id, self.num_shards as u32) as i32
    }

    async fn cluster_selection_policy(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> Result<ActiveClusterSelectionPolicy, PolicyError> {
        let shard_id = self.shard_of(workflow_id);
        let mut run_id = run_id.to_owned();
        if run_id.is_empty() {
            let current = self.executions.get_current_execution(shard_id, domain_id, workflow_id).await.map_err(persistence_error)?;
            run_id = current.run_id;
        }
        let key = format!("{domain_id}:{workflow_id}:{run_id}");
        if let Some(policy) = self.cache.get(&key, Instant::now()) {
            return Ok(policy);
        }
        let blob = self
            .executions
            .get_active_cluster_selection_policy(shard_id, domain_id, workflow_id, &run_id)
            .await
            .map_err(persistence_error)?;
        let Some(blob) = blob else {
            return Err(PolicyError::NotExists("active cluster selection policy not found".into()));
        };
        let decoded = decode_active_cluster_selection_policy(&blob.data, &blob.encoding)
            .map_err(|e| PolicyError::Other(HistoryError::internal(e.to_string())))?;
        let policy = ActiveClusterSelectionPolicy {
            cluster_attribute: decoded.cluster_attribute.map(|(scope, name)| ClusterAttribute { scope, name }),
        };
        self.cache.put(key, policy.clone(), Instant::now());
        Ok(policy)
    }

    fn lookup_domain(&self, domain_id: &str) -> HResult<(Arc<DomainCacheEntry>, bool)> {
        (self.domain_id_to_domain)(domain_id)
    }

    fn by_attribute(entry: &DomainCacheEntry, domain_id: &str, attribute: Option<&ClusterAttribute>) -> HResult<ActiveClusterInfo> {
        match entry.get_active_cluster_info_by_cluster_attribute(attribute) {
            Some((active_cluster_name, failover_version)) => Ok(ActiveClusterInfo { active_cluster_name, failover_version }),
            None => Err(HistoryError::ClusterAttributeNotFound(crate::engine::deps::cluster_attribute_not_found_message(domain_id, attribute))),
        }
    }

    /// Go `GetActiveClusterInfoByClusterAttribute`.
    pub fn get_active_cluster_info_by_cluster_attribute(&self, domain_id: &str, attribute: Option<&ClusterAttribute>) -> HResult<ActiveClusterInfo> {
        let (entry, _) = self.lookup_domain(domain_id)?;
        Self::by_attribute(&entry, domain_id, attribute)
    }

    /// Go `GetActiveClusterInfoByWorkflow`.
    pub async fn get_active_cluster_info_by_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<ActiveClusterInfo> {
        let (entry, active_active) = self.lookup_domain(domain_id)?;
        if !active_active {
            // Not an active-active domain: ActiveClusterName / FailoverVersion of the domain entry.
            let name = entry.active_clusters.iter().find(|(a, _)| a.is_none()).map(|(_, c)| c.clone()).unwrap_or_default();
            return Ok(ActiveClusterInfo { active_cluster_name: name, failover_version: entry.failover_version });
        }
        let policy = match self.cluster_selection_policy(domain_id, workflow_id, run_id).await {
            Ok(p) => p,
            Err(PolicyError::NotExists(_)) => ActiveClusterSelectionPolicy::default(),
            Err(e) => return Err(e.into_history()),
        };
        Self::by_attribute(&entry, domain_id, policy.cluster_attribute.as_ref())
    }

    /// Go `GetActiveClusterSelectionPolicyForWorkflow`.
    pub async fn get_active_cluster_selection_policy_for_workflow(&self, domain_id: &str, workflow_id: &str, run_id: &str) -> HResult<Option<ActiveClusterSelectionPolicy>> {
        let (_, active_active) = self.lookup_domain(domain_id)?;
        if !active_active {
            return Ok(None);
        }
        match self.cluster_selection_policy(domain_id, workflow_id, run_id).await {
            Ok(p) => Ok(Some(p)),
            Err(PolicyError::NotExists(_)) => Ok(None),
            Err(e) => Err(e.into_history()),
        }
    }

    /// Go `GetActiveClusterSelectionPolicyForCurrentWorkflow`: `(policy, running)`.
    pub async fn get_active_cluster_selection_policy_for_current_workflow(&self, domain_id: &str, workflow_id: &str) -> HResult<(Option<ActiveClusterSelectionPolicy>, bool)> {
        let (_, active_active) = self.lookup_domain(domain_id)?;
        if !active_active {
            return Ok((None, false));
        }
        let shard_id = self.shard_of(workflow_id);
        let current = self
            .executions
            .get_current_execution(shard_id, domain_id, workflow_id)
            .await
            .map_err(|e| persistence_error(e).into_history())?;
        if is_workflow_running(current.state) {
            let policy = self.cluster_selection_policy(domain_id, workflow_id, &current.run_id).await.map_err(PolicyError::into_history)?;
            return Ok((Some(policy), true));
        }
        Ok((None, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn attr(scope: &str, name: &str) -> ClusterAttribute {
        ClusterAttribute { scope: scope.into(), name: name.into() }
    }

    fn entry() -> DomainCacheEntry {
        DomainCacheEntry {
            active_clusters: vec![(None, "c0".into()), (Some(attr("region", "us")), "c1".into()), (Some(attr("region", "eu")), "c2".into())],
            failover_version: 7,
            active_cluster_versions: vec![(attr("region", "us"), 11), (attr("region", "eu"), 12)],
            ..Default::default()
        }
    }

    /// thriftrw encoding of `ActiveClusterSelectionPolicy{ClusterAttribute{scope,name}}`.
    fn blob(scope: &str, name: &str) -> DataBlob {
        let mut b = vec![0x0c, 0x00, 0x01]; // field 1 struct
        for (id, s) in [(1i16, scope), (2, name)] {
            b.push(0x0b);
            b.extend_from_slice(&id.to_be_bytes());
            b.extend_from_slice(&(s.len() as i32).to_be_bytes());
            b.extend_from_slice(s.as_bytes());
        }
        b.push(0); // end attribute
        b.push(0); // end policy
        DataBlob { encoding: "thriftrw".into(), data: b }
    }

    struct Fake {
        current: Result<CurrentExecution, PersistenceError>,
        policy: Result<Option<DataBlob>, PersistenceError>,
        policy_calls: AtomicUsize,
    }

    impl Fake {
        fn new(current: Result<CurrentExecution, PersistenceError>, policy: Result<Option<DataBlob>, PersistenceError>) -> Arc<Self> {
            Arc::new(Self { current, policy, policy_calls: AtomicUsize::new(0) })
        }
    }

    #[async_trait]
    impl PolicySource for Fake {
        async fn get_current_execution(&self, _: i32, _: &str, _: &str) -> Result<CurrentExecution, PersistenceError> {
            self.current.clone()
        }
        async fn get_active_cluster_selection_policy(&self, _: i32, _: &str, _: &str, _: &str) -> Result<Option<DataBlob>, PersistenceError> {
            self.policy_calls.fetch_add(1, Ordering::SeqCst);
            self.policy.clone()
        }
    }

    fn manager(active_active: bool, fake: Arc<Fake>) -> ActiveClusterManagerImpl {
        let e = Arc::new(entry());
        ActiveClusterManagerImpl::new(Arc::new(move |_| Ok((e.clone(), active_active))), fake, 4)
    }

    fn running() -> Result<CurrentExecution, PersistenceError> {
        Ok(CurrentExecution { run_id: "run-1".into(), state: 1, ..Default::default() })
    }

    #[test]
    fn info_by_cluster_attribute_table() {
        let m = manager(true, Fake::new(running(), Ok(None)));
        let got = m.get_active_cluster_info_by_cluster_attribute("d", None).unwrap();
        assert_eq!((got.active_cluster_name.as_str(), got.failover_version), ("c0", 7));
        let got = m.get_active_cluster_info_by_cluster_attribute("d", Some(&attr("region", "us"))).unwrap();
        assert_eq!((got.active_cluster_name.as_str(), got.failover_version), ("c1", 11));
        assert!(matches!(m.get_active_cluster_info_by_cluster_attribute("d", Some(&attr("region", "asia"))), Err(HistoryError::ClusterAttributeNotFound(_))));
    }

    #[tokio::test]
    async fn by_workflow_not_active_active_returns_domain_cluster() {
        let m = manager(false, Fake::new(running(), Ok(Some(blob("region", "us")))));
        let got = m.get_active_cluster_info_by_workflow("d", "wf", "run-1").await.unwrap();
        assert_eq!((got.active_cluster_name.as_str(), got.failover_version), ("c0", 7));
    }

    #[tokio::test]
    async fn by_workflow_uses_persisted_policy() {
        let m = manager(true, Fake::new(running(), Ok(Some(blob("region", "eu")))));
        let got = m.get_active_cluster_info_by_workflow("d", "wf", "run-1").await.unwrap();
        assert_eq!((got.active_cluster_name.as_str(), got.failover_version), ("c2", 12));
    }

    #[tokio::test]
    async fn by_workflow_resolves_empty_run_id_through_current_execution() {
        let fake = Fake::new(running(), Ok(Some(blob("region", "us"))));
        let m = manager(true, fake);
        let got = m.get_active_cluster_info_by_workflow("d", "wf", "").await.unwrap();
        assert_eq!(got.active_cluster_name, "c1");
    }

    #[tokio::test]
    async fn by_workflow_missing_policy_or_execution_falls_back_to_domain_level() {
        let m = manager(true, Fake::new(running(), Ok(None)));
        assert_eq!(m.get_active_cluster_info_by_workflow("d", "wf", "run-1").await.unwrap().active_cluster_name, "c0");
        let m = manager(true, Fake::new(Err(PersistenceError::EntityNotExists("x".into())), Ok(None)));
        assert_eq!(m.get_active_cluster_info_by_workflow("d", "wf", "").await.unwrap().active_cluster_name, "c0");
    }

    #[tokio::test]
    async fn by_workflow_persistence_error_and_unknown_attribute_fail() {
        let m = manager(true, Fake::new(running(), Err(PersistenceError::InvalidArgument("boom".into()))));
        assert!(m.get_active_cluster_info_by_workflow("d", "wf", "run-1").await.is_err());
        let m = manager(true, Fake::new(running(), Ok(Some(blob("region", "asia")))));
        assert!(matches!(m.get_active_cluster_info_by_workflow("d", "wf", "run-1").await, Err(HistoryError::ClusterAttributeNotFound(_))));
    }

    #[tokio::test]
    async fn policy_for_workflow_table() {
        let m = manager(false, Fake::new(running(), Ok(Some(blob("region", "us")))));
        assert!(m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-1").await.unwrap().is_none());
        let m = manager(true, Fake::new(running(), Ok(None)));
        assert!(m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-1").await.unwrap().is_none());
        let m = manager(true, Fake::new(running(), Ok(Some(blob("region", "us")))));
        let p = m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-1").await.unwrap().unwrap();
        assert_eq!(p.cluster_attribute, Some(attr("region", "us")));
        let m = manager(true, Fake::new(running(), Err(PersistenceError::InvalidArgument("boom".into()))));
        assert!(m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-1").await.is_err());
    }

    #[tokio::test]
    async fn policy_is_cached_per_run() {
        let fake = Fake::new(running(), Ok(Some(blob("region", "us"))));
        let m = manager(true, fake.clone());
        for _ in 0..3 {
            m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-1").await.unwrap();
        }
        assert_eq!(fake.policy_calls.load(Ordering::SeqCst), 1);
        m.get_active_cluster_selection_policy_for_workflow("d", "wf", "run-2").await.unwrap();
        assert_eq!(fake.policy_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn current_workflow_policy_table() {
        let m = manager(false, Fake::new(running(), Ok(Some(blob("region", "us")))));
        assert_eq!(m.get_active_cluster_selection_policy_for_current_workflow("d", "wf").await.unwrap(), (None, false));
        let m = manager(true, Fake::new(running(), Ok(Some(blob("region", "us")))));
        let (p, is_running) = m.get_active_cluster_selection_policy_for_current_workflow("d", "wf").await.unwrap();
        assert!(is_running);
        assert_eq!(p.unwrap().cluster_attribute, Some(attr("region", "us")));
        let closed = Ok(CurrentExecution { run_id: "r".into(), state: 2, ..Default::default() });
        let m = manager(true, Fake::new(closed, Ok(Some(blob("region", "us")))));
        assert_eq!(m.get_active_cluster_selection_policy_for_current_workflow("d", "wf").await.unwrap(), (None, false));
        let m = manager(true, Fake::new(Err(PersistenceError::EntityNotExists("x".into())), Ok(None)));
        assert!(m.get_active_cluster_selection_policy_for_current_workflow("d", "wf").await.unwrap_err().is_entity_not_exists());
        let m = manager(true, Fake::new(running(), Ok(None)));
        assert!(m.get_active_cluster_selection_policy_for_current_workflow("d", "wf").await.unwrap_err().is_entity_not_exists());
    }

    #[test]
    fn cache_ttl_and_capacity() {
        let c = PolicyCache::default();
        let t0 = Instant::now();
        c.put("a".into(), ActiveClusterSelectionPolicy::default(), t0);
        assert!(c.get("a", t0 + Duration::from_secs(9)).is_some());
        assert!(c.get("a", t0 + Duration::from_secs(11)).is_none());
        for i in 0..WORKFLOW_POLICY_CACHE_MAX_COUNT + 5 {
            c.put(format!("k{i}"), ActiveClusterSelectionPolicy::default(), t0);
        }
        assert_eq!(c.len(), WORKFLOW_POLICY_CACHE_MAX_COUNT);
    }

    /// Go lru: Get moves to front; overflow evicts the least recently accessed; Put refreshes TTL.
    #[test]
    fn cache_evicts_in_access_order_like_go() {
        let c = PolicyCache::default();
        let t0 = Instant::now();
        for i in 0..WORKFLOW_POLICY_CACHE_MAX_COUNT {
            c.put(format!("k{i}"), ActiveClusterSelectionPolicy::default(), t0);
        }
        assert!(c.get("k0", t0).is_some()); // k0 now most recent, k1 is the back
        c.put("new".into(), ActiveClusterSelectionPolicy::default(), t0);
        assert!(c.get("k1", t0).is_none());
        assert!(c.get("k0", t0).is_some());
        // Put on an existing key refreshes createTime.
        c.put("k0".into(), ActiveClusterSelectionPolicy::default(), t0 + Duration::from_secs(8));
        assert!(c.get("k0", t0 + Duration::from_secs(15)).is_some());
        assert!(c.get("k2", t0 + Duration::from_secs(15)).is_none());
        // Strictly after ttl: exactly ttl is still alive.
        c.put("edge".into(), ActiveClusterSelectionPolicy::default(), t0);
        assert!(c.get("edge", t0 + WORKFLOW_POLICY_CACHE_TTL).is_some());
    }

    #[test]
    fn cluster_attribute_not_found_message_matches_go() {
        use crate::engine::deps::cluster_attribute_not_found_message as m;
        assert_eq!(m("d", Some(&attr("region", "us"))), "could not find cluster attribute &{region us} in the domain d's active cluster config");
        assert_eq!(m("d", None), "could not find cluster attribute %!s(*types.ClusterAttribute=<nil>) in the domain d's active cluster config");
    }
}
