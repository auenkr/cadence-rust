//! group: queues
//! SQL-backed adapters for the queue / queuev2 task persistence traits
//! (Go `persistence.ExecutionManager.GetHistoryTasks / RangeCompleteHistoryTask`
//! on top of `common/persistence/sql`).
//!
//! Gaps: `queue::shard_deps::QueueShardContext` and `queuev2::deps::ShardContext`
//! are the full shard context (ack levels, range id, queue state, config, domain
//! cache); they belong to the shard-controller assembly, not to the SQL stores.
//! `SqlQueueShardContext` implements the SQL-backed task read portion only.

use std::sync::Arc;

use async_trait::async_trait;
use cadence_persistence::history_contract as pc;
use cadence_persistence::history_contract::GoExecutionStore;
use cadence_persistence::history_task_sql::HistoryTaskQuery;
use cadence_persistence::sqlblobs;

use crate::queue::deps as q;
use crate::queue::shard_deps as qs;
use crate::queuev2::deps as v2;

/// Decoded history task row (transfer or timer) usable by both queue versions.
#[derive(Clone, Debug)]
pub struct SqlHistoryTask {
    category_id: i32,
    domain_id: String,
    workflow_id: String,
    run_id: String,
    task_id: i64,
    task_type: i32,
    version: i64,
    visibility_nanos: i64,
}

impl SqlHistoryTask {
    fn decode(category: pc::HistoryTaskCategory, task: &pc::Task) -> Result<Self, String> {
        let value = sqlblobs::Encoding::parse(&task.data.encoding)
            .and_then(|e| e.decode(&task.data.data))
            .map_err(|e| e.to_string())?;
        match category.category_id {
            1 => {
                let t = sqlblobs::transfer_task_from_struct(&value).map_err(|e| e.to_string())?;
                Ok(Self {
                    category_id: 1,
                    domain_id: t.domain_id,
                    workflow_id: t.workflow_id,
                    run_id: t.run_id,
                    task_id: task.task_id,
                    task_type: t.task_type,
                    version: t.version,
                    visibility_nanos: t.visibility_time_unix_nanos,
                })
            }
            2 => {
                let t = sqlblobs::timer_task_from_struct(&value).map_err(|e| e.to_string())?;
                Ok(Self {
                    category_id: 2,
                    domain_id: t.domain_id,
                    workflow_id: t.workflow_id,
                    run_id: t.run_id,
                    task_id: task.task_id,
                    task_type: t.task_type,
                    version: t.version,
                    visibility_nanos: task.visibility_timestamp_unix_nanos,
                })
            }
            other => Err(format!("unsupported history task category {other}")),
        }
    }
}

impl v2::PersistenceTask for SqlHistoryTask {
    fn get_task_key(&self) -> v2::HistoryTaskKey {
        if self.category_id == 2 {
            v2::HistoryTaskKey::new(v2::unix_nano_time(self.visibility_nanos), self.task_id)
        } else {
            v2::HistoryTaskKey::new_immediate(self.task_id)
        }
    }
    fn get_task_id(&self) -> i64 {
        self.task_id
    }
    fn get_domain_id(&self) -> String {
        self.domain_id.clone()
    }
    fn get_workflow_id(&self) -> String {
        self.workflow_id.clone()
    }
    fn get_run_id(&self) -> String {
        self.run_id.clone()
    }
    fn get_visibility_timestamp(&self) -> v2::Time {
        v2::unix_nano_time(self.visibility_nanos)
    }
}

impl q::PersistenceTask for SqlHistoryTask {
    fn get_domain_id(&self) -> String {
        self.domain_id.clone()
    }
    fn get_workflow_id(&self) -> String {
        self.workflow_id.clone()
    }
    fn get_run_id(&self) -> String {
        self.run_id.clone()
    }
    fn get_task_id(&self) -> i64 {
        self.task_id
    }
    fn get_task_type(&self) -> i32 {
        self.task_type
    }
    fn get_task_category(&self) -> q::HistoryTaskCategory {
        if self.category_id == 2 {
            q::HistoryTaskCategory::Timer
        } else {
            q::HistoryTaskCategory::Transfer
        }
    }
    fn get_version(&self) -> i64 {
        self.version
    }
    fn get_visibility_timestamp(&self) -> q::Time {
        q::Time(self.visibility_nanos)
    }
    fn is_corrupted(&self) -> bool {
        self.domain_id.is_empty() || self.workflow_id.is_empty() || self.run_id.is_empty()
    }
}

fn v2_category(c: v2::HistoryTaskCategory) -> pc::HistoryTaskCategory {
    pc::HistoryTaskCategory { category_type: c.category_type, category_id: c.category_id }
}

fn v1_category(c: q::HistoryTaskCategory) -> pc::HistoryTaskCategory {
    match c {
        q::HistoryTaskCategory::Transfer => pc::HistoryTaskCategory { category_type: 1, category_id: 1 },
        q::HistoryTaskCategory::Timer => pc::HistoryTaskCategory { category_type: 2, category_id: 2 },
    }
}

/// Builds the store query from key bounds (task ids for immediate categories,
/// visibility timestamps for scheduled ones), as Go `sqlExecutionStore` does.
fn query(
    shard_id: i32,
    category: pc::HistoryTaskCategory,
    min: (i64, i64),
    max: (i64, i64),
    page_size: i64,
    token: Vec<u8>,
) -> HistoryTaskQuery {
    HistoryTaskQuery {
        shard_id,
        category,
        min_task_id: min.1,
        max_task_id: max.1,
        min_timestamp_unix_nanos: min.0,
        max_timestamp_unix_nanos: max.0,
        page_size: page_size.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
        next_page_token: token,
    }
}

fn v2_bounds(min: &v2::HistoryTaskKey, max: &v2::HistoryTaskKey) -> ((i64, i64), (i64, i64)) {
    (
        (v2::time_unix_nano(&min.get_scheduled_time()), min.get_task_id()),
        (v2::time_unix_nano(&max.get_scheduled_time()), max.get_task_id()),
    )
}

/// queuev2 `ExecutionManager` over a SQL execution store.
pub struct SqlExecutionManager {
    store: Arc<dyn GoExecutionStore>,
    shard_id: i32,
}

impl SqlExecutionManager {
    pub fn new(store: Arc<dyn GoExecutionStore>, shard_id: i32) -> Self {
        Self { store, shard_id }
    }
}

#[async_trait]
impl v2::ExecutionManager for SqlExecutionManager {
    async fn get_history_tasks(
        &self,
        req: v2::GetHistoryTasksRequest,
    ) -> Result<v2::GetHistoryTasksResponse, v2::DepError> {
        let category = v2_category(req.task_category);
        let (min, max) = v2_bounds(&req.inclusive_min_task_key, &req.exclusive_max_task_key);
        let q = query(
            req.shard_id.unwrap_or(self.shard_id),
            category,
            min,
            max,
            req.page_size as i64,
            req.next_page_token.unwrap_or_default(),
        );
        let page = self.store.get_history_task_page(q).await.map_err(|e| e.to_string())?;
        let tasks = page
            .tasks
            .iter()
            .map(|t| SqlHistoryTask::decode(category, t).map(|t| Arc::new(t) as v2::PTask))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(v2::GetHistoryTasksResponse {
            tasks,
            next_page_token: if page.next_page_token.is_empty() { None } else { Some(page.next_page_token) },
        })
    }

    async fn range_complete_history_task(
        &self,
        req: v2::RangeCompleteHistoryTaskRequest,
    ) -> Result<v2::RangeCompleteHistoryTaskResponse, v2::DepError> {
        let category = v2_category(req.task_category);
        let (min, max) = v2_bounds(&req.inclusive_min_task_key, &req.exclusive_max_task_key);
        let q = query(req.shard_id.unwrap_or(self.shard_id), category, min, max, req.page_size, Vec::new());
        let n = self.store.range_complete_history_task_page(q).await.map_err(|e| e.to_string())?;
        Ok(v2::RangeCompleteHistoryTaskResponse { tasks_completed: n as i64 })
    }
}

/// Queue (v1) shard context task-read portion over a SQL execution store.
/// Holds the few static shard settings the queue package reads.
pub struct SqlQueueShardContext {
    store: Arc<dyn GoExecutionStore>,
    shard_id: i32,
    range_size_bits: u32,
    enable_debug_mode: bool,
}

impl SqlQueueShardContext {
    pub fn new(store: Arc<dyn GoExecutionStore>, shard_id: i32, range_size_bits: u32, enable_debug_mode: bool) -> Self {
        Self { store, shard_id, range_size_bits, enable_debug_mode }
    }

    /// `GetExecutionManager().RangeCompleteHistoryTask` (returns `TasksCompleted`).
    pub async fn range_complete_history_task(
        &self,
        req: qs::RangeCompleteHistoryTaskRequest,
    ) -> Result<i64, q::QueueError> {
        let q = query(
            req.shard_id,
            v1_category(req.task_category),
            (req.inclusive_min_task_key.scheduled_time.unix_nano(), req.inclusive_min_task_key.task_id),
            (req.exclusive_max_task_key.scheduled_time.unix_nano(), req.exclusive_max_task_key.task_id),
            req.page_size,
            Vec::new(),
        );
        let n = self
            .store
            .range_complete_history_task_page(q)
            .await
            .map_err(|e| q::QueueError::Other(e.to_string()))?;
        Ok(n as i64)
    }
}

#[async_trait]
impl q::ShardContext for SqlQueueShardContext {
    fn get_shard_id(&self) -> i32 {
        self.shard_id
    }
    fn now(&self) -> q::Time {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        q::Time(d)
    }
    fn range_size_bits(&self) -> u32 {
        self.range_size_bits
    }
    fn enable_debug_mode(&self) -> bool {
        self.enable_debug_mode
    }
    async fn get_history_tasks(
        &self,
        req: q::GetHistoryTasksRequest,
    ) -> Result<q::GetHistoryTasksResponse, q::QueueError> {
        let category = v1_category(req.task_category);
        let q = query(
            req.shard_id,
            category,
            (req.inclusive_min_task_key.scheduled_time.unix_nano(), req.inclusive_min_task_key.task_id),
            (req.exclusive_max_task_key.scheduled_time.unix_nano(), req.exclusive_max_task_key.task_id),
            req.page_size,
            req.next_page_token,
        );
        let page = self
            .store
            .get_history_task_page(q)
            .await
            .map_err(|e| q::QueueError::Other(e.to_string()))?;
        let tasks = page
            .tasks
            .iter()
            .map(|t| SqlHistoryTask::decode(category, t).map(|t| Arc::new(t) as Arc<dyn q::PersistenceTask>))
            .collect::<Result<Vec<_>, _>>()
            .map_err(q::QueueError::Other)?;
        Ok(q::GetHistoryTasksResponse { tasks, next_page_token: page.next_page_token })
    }
}
