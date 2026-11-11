//! Shared mocks for the replication unit tests (ports of the Go `_test.go`
//! fixtures, e.g. `config.Config` with function fields).

#![allow(dead_code, clippy::type_complexity)]

use super::deps::*;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `config.Config` stand-in; the batch-size knobs are mutable at runtime.
pub struct TestConfig {
    pub min_batch: AtomicI32,
    pub max_batch: AtomicI32,
    pub step_count: AtomicUsize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            min_batch: AtomicI32::new(10),
            max_batch: AtomicI32::new(100),
            step_count: AtomicUsize::new(5),
        }
    }
}

impl TestConfig {
    pub fn with_batch(min: i32, max: i32, steps: usize) -> Arc<Self> {
        Arc::new(Self {
            min_batch: AtomicI32::new(min),
            max_batch: AtomicI32::new(max),
            step_count: AtomicUsize::new(steps),
        })
    }
}

const MS: Duration = Duration::from_millis(1);

impl ReplicationConfig for TestConfig {
    fn number_of_shards(&self) -> usize {
        4
    }
    fn max_response_size(&self) -> usize {
        1 << 20
    }
    fn replication_task_processor_error_retry_wait(&self, _: i32) -> Duration {
        MS
    }
    fn replication_task_processor_error_retry_max_attempts(&self, _: i32) -> usize {
        2
    }
    fn replication_task_processor_error_second_retry_wait(&self, _: i32) -> Duration {
        MS
    }
    fn replication_task_processor_error_second_retry_max_wait(&self, _: i32) -> Duration {
        MS * 5
    }
    fn replication_task_processor_error_second_retry_expiration(&self, _: i32) -> Duration {
        MS * 20
    }
    fn replication_task_processor_no_task_retry_wait(&self, _: i32) -> Duration {
        MS
    }
    fn replication_task_processor_cleanup_interval(&self, _: i32) -> Duration {
        MS * 10
    }
    fn replication_task_processor_cleanup_jitter_coefficient(&self, _: i32) -> f64 {
        0.0
    }
    fn replication_task_processor_start_wait(&self, _: i32) -> Duration {
        Duration::ZERO
    }
    fn replication_task_processor_start_wait_jitter_coefficient(&self, _: i32) -> f64 {
        0.0
    }
    fn replication_task_processor_latency_log_threshold(&self) -> Duration {
        Duration::from_secs(5)
    }
    fn replication_task_processor_shard_qps(&self) -> f64 {
        1000.0
    }
    fn replication_task_processor_host_qps(&self) -> f64 {
        1000.0
    }
    fn replicator_task_delete_batch_size(&self) -> usize {
        100
    }
    fn shard_sync_min_interval(&self) -> Duration {
        MS
    }
    fn shard_sync_timer_jitter_coefficient(&self) -> f64 {
        0.0
    }
    fn replication_task_fetcher_parallelism(&self) -> usize {
        1
    }
    fn replication_task_fetcher_aggregation_interval(&self) -> Duration {
        MS
    }
    fn replication_task_fetcher_timer_jitter_coefficient(&self) -> f64 {
        0.0
    }
    fn replication_task_fetcher_error_retry_wait(&self) -> Duration {
        MS
    }
    fn replication_task_fetcher_service_busy_wait(&self) -> Duration {
        MS
    }
    fn replicator_cache_capacity(&self) -> usize {
        100
    }
    fn replicator_cache_max_size(&self) -> usize {
        1 << 20
    }
    fn replicator_read_task_max_retry_count(&self) -> usize {
        3
    }
    fn replication_task_generation_qps(&self) -> f64 {
        1000.0
    }
    fn replicator_processor_min_task_batch_size(&self, _: i32) -> i32 {
        self.min_batch.load(Ordering::SeqCst)
    }
    fn replicator_processor_max_task_batch_size(&self, _: i32) -> i32 {
        self.max_batch.load(Ordering::SeqCst)
    }
    fn replicator_processor_batch_size_step_count(&self, _: i32) -> usize {
        self.step_count.load(Ordering::SeqCst)
    }
}

pub fn unknown_task(id: i64) -> HistoryTask {
    HistoryTask::Unknown { info: TaskInfo { task_id: id, ..Default::default() } }
}

// ---- shared fixtures (task_ack_manager_test.go vars) ----------------------

use async_trait::async_trait;
use cadence_api::types::{
    FailoverMarkerAttributes, HistoryTaskV2Attributes, ReplicationTask, SyncActivityTaskAttributes,
};
use std::collections::HashMap;

pub const TEST_DOMAIN_ID: &str = "11111111-1111-1111-1111-111111111111";
pub const TEST_DOMAIN_NAME: &str = "test-domain-name";
pub const CLUSTER_A: &str = "cluster-A";
pub const CLUSTER_B: &str = "cluster-B";
pub const CLUSTER_C: &str = "cluster-C";

pub fn task_info(id: i64) -> TaskInfo {
    TaskInfo { task_id: id, domain_id: TEST_DOMAIN_ID.into(), ..Default::default() }
}

/// `testTask11` / `testTask13` (HistoryReplicationTask).
pub fn history_task(id: i64) -> HistoryTask {
    HistoryTask::HistoryReplication {
        info: task_info(id),
        first_event_id: 0,
        next_event_id: 0,
        branch_token: vec![],
        new_run_branch_token: vec![],
    }
}
/// `testTask12` (SyncActivityTask).
pub fn sync_task(id: i64) -> HistoryTask {
    HistoryTask::SyncActivity { info: task_info(id), scheduled_id: 0 }
}
/// `testTask14` (FailoverMarkerTask).
pub fn failover_task(id: i64) -> HistoryTask {
    HistoryTask::FailoverMarker { info: task_info(id) }
}

pub fn hydrated_history(id: i64) -> ReplicationTask {
    ReplicationTask {
        source_task_id: Some(id),
        history_task_v2_attributes: Some(HistoryTaskV2Attributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}
pub fn hydrated_sync(id: i64) -> ReplicationTask {
    ReplicationTask {
        source_task_id: Some(id),
        sync_activity_task_attributes: Some(SyncActivityTaskAttributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}
pub fn hydrated_failover(id: i64) -> ReplicationTask {
    ReplicationTask {
        source_task_id: Some(id),
        failover_marker_attributes: Some(FailoverMarkerAttributes {
            domain_id: Some(TEST_DOMAIN_ID.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `fakeDomainCache`.
#[derive(Default, Clone)]
pub struct FakeDomains(pub HashMap<String, DomainEntry>);

impl FakeDomains {
    /// `{testDomainID: testDomain}` where testDomain replicates to cluster A only.
    pub fn test_domain() -> Self {
        let mut m = HashMap::new();
        m.insert(
            TEST_DOMAIN_ID.to_string(),
            DomainEntry { name: TEST_DOMAIN_NAME.into(), replication_clusters: vec![CLUSTER_A.into()] },
        );
        Self(m)
    }
}
impl DomainCache for FakeDomains {
    fn get_domain_by_id(&self, id: &str) -> Result<DomainEntry> {
        self.0
            .get(id)
            .cloned()
            .ok_or_else(|| ReplicationError::Other("domain does not exist".into()))
    }
    fn get_domain_name(&self, id: &str) -> Result<String> {
        self.get_domain_by_id(id).map(|d| d.name)
    }
}

/// `fakeTaskHydrator`: task id -> outcome; unknown ids hydrate to `None`.
#[derive(Default)]
pub struct FakeHydrator(pub HashMap<i64, Result<Option<ReplicationTask>>>);

#[async_trait]
impl super::task_store::TaskHydratorApi for FakeHydrator {
    async fn hydrate(&self, task: &HistoryTask) -> Result<Option<ReplicationTask>> {
        self.0.get(&task.task_id()).cloned().unwrap_or(Ok(None))
    }
}

pub struct NoLimit;
#[async_trait]
impl Limiter for NoLimit {
    async fn wait(&self) {}
}

pub struct FixedTime(pub i64);
impl TimeSource for FixedTime {
    fn now_nanos(&self) -> i64 {
        self.0
    }
}

/// Recording `persistence.MockExecutionManager`.
#[derive(Default)]
pub struct FakeExec {
    pub read_calls: std::sync::Mutex<Vec<(i32, i64, i64, usize)>>,
    pub read_result: std::sync::Mutex<Option<Result<(Vec<HistoryTask>, bool)>>>,
    pub range_complete_calls: std::sync::Mutex<Vec<(i32, i64, usize)>>,
    pub range_complete_result: std::sync::Mutex<Option<Result<usize>>>,
    pub put_dlq: std::sync::Mutex<Vec<PutReplicationTaskToDlqRequest>>,
    pub put_dlq_result: std::sync::Mutex<Option<ReplicationError>>,
    pub dlq_size: std::sync::Mutex<Option<Result<i64>>>,
    pub dlq_read_calls: std::sync::Mutex<Vec<(i32, String, i64, i64, usize, Vec<u8>)>>,
    pub dlq_read_result: std::sync::Mutex<Option<Result<GetReplicationTasksFromDlqResponse>>>,
    pub dlq_delete_calls: std::sync::Mutex<Vec<(i32, String, i64, i64)>>,
    pub dlq_delete_result: std::sync::Mutex<Option<ReplicationError>>,
}

#[async_trait]
impl ExecutionManager for FakeExec {
    async fn get_replication_tasks(
        &self,
        shard_id: i32,
        min: i64,
        max: i64,
        page_size: usize,
    ) -> Result<(Vec<HistoryTask>, bool)> {
        self.read_calls.lock().unwrap().push((shard_id, min, max, page_size));
        self.read_result.lock().unwrap().clone().unwrap_or(Ok((vec![], false)))
    }
    async fn range_complete_replication_task(&self, shard_id: i32, max: i64, page_size: usize) -> Result<usize> {
        self.range_complete_calls.lock().unwrap().push((shard_id, max, page_size));
        self.range_complete_result.lock().unwrap().clone().unwrap_or(Ok(0))
    }
    async fn put_replication_task_to_dlq(&self, req: PutReplicationTaskToDlqRequest) -> Result<()> {
        self.put_dlq.lock().unwrap().push(req);
        match self.put_dlq_result.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    async fn get_replication_dlq_size(&self, _shard_id: i32, _source: &str) -> Result<i64> {
        self.dlq_size.lock().unwrap().clone().unwrap_or(Ok(0))
    }
    async fn get_replication_tasks_from_dlq(
        &self,
        shard_id: i32,
        source: &str,
        read_level: i64,
        max_read_level: i64,
        batch_size: usize,
        token: Vec<u8>,
    ) -> Result<GetReplicationTasksFromDlqResponse> {
        self.dlq_read_calls
            .lock()
            .unwrap()
            .push((shard_id, source.to_string(), read_level, max_read_level, batch_size, token));
        self.dlq_read_result
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Ok(GetReplicationTasksFromDlqResponse::default()))
    }
    async fn range_delete_replication_task_from_dlq(
        &self,
        shard_id: i32,
        source: &str,
        begin: i64,
        end: i64,
    ) -> Result<()> {
        self.dlq_delete_calls.lock().unwrap().push((shard_id, source.to_string(), begin, end));
        match self.dlq_delete_result.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
