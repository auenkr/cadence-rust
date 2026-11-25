//! Queue v2 adapter tests (piece B): a transfer and a timer task run end to end through
//! `V2QueueEnv` -> `task::TaskImpl` -> executor, and the cached reader serves notified tasks
//! without a DB read, falling back to the DB when the range is evicted or missing.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cadence_persistence::history_contract as pc;

use super::*;
use crate::engine::notify_tasks::NotifiedTask;
use crate::queuev2::deps::QueueEnv;
use crate::queuev2::predicate::Predicate;
use crate::queuev2::queue_mem::new_in_mem_queue;
use crate::queuev2::queue_reader::{
    GetTaskProgress, GetTaskRequest, GetTaskResponse, LookAHeadRequest, LookAHeadResponse, QueueReader,
};
use crate::queuev2::queue_reader_cached_base::{CachedQueueReader, CachedQueueReaderOptions};
use crate::queuev2::queue_reader_cached_immediate::new_cached_immediate_queue_reader_with_options;
use crate::queuev2::range::Range;

const NOW: i64 = 1_700_000_000_000_000_000;

// ------------------------------------------------------------ task package fakes

struct Clock;
impl t::TimeSource for Clock {
    fn now(&self) -> t::Time {
        t::Time(NOW)
    }
}

struct Cfg;
impl t::HistoryConfig for Cfg {
    fn enable_debug_mode(&self) -> bool {
        false
    }
    fn enable_task_info_log_by_domain_id(&self, _: &str) -> bool {
        false
    }
    fn enable_drop_stuck_task_by_domain_id(&self, _: &str) -> bool {
        false
    }
    fn task_process_rps(&self) -> i64 {
        1000
    }
    fn task_critical_retry_count(&self) -> i32 {
        50
    }
    fn number_of_shards(&self) -> i32 {
        1
    }
    fn task_scheduler_global_domain_rps(&self, _: &str) -> i32 {
        1000
    }
    fn task_scheduler_enable_rate_limiter(&self) -> t::BoolPropertyFn {
        Arc::new(|| false)
    }
    fn task_scheduler_enable_rate_limiter_shadow_mode(&self) -> t::BoolPropertyFnWithDomainFilter {
        Arc::new(|_| false)
    }
}

struct TaskShard;
impl t::ShardContext for TaskShard {
    fn get_shard_id(&self) -> i32 {
        1
    }
    fn get_time_source(&self) -> Arc<dyn t::TimeSource> {
        Arc::new(Clock)
    }
    fn get_config(&self) -> Arc<dyn t::HistoryConfig> {
        Arc::new(Cfg)
    }
    fn get_domain_cache(&self) -> Arc<dyn t::DomainCache> {
        unimplemented!()
    }
    fn get_cluster_metadata(&self) -> Arc<dyn t::ClusterMetadata> {
        unimplemented!()
    }
    fn get_active_cluster_manager(&self) -> Arc<dyn t::ActiveClusterManager> {
        unimplemented!()
    }
    fn get_metrics_client(&self) -> Arc<dyn t::MetricsClient> {
        unimplemented!()
    }
    fn get_service(&self) -> Arc<dyn t::Service> {
        unimplemented!()
    }
    fn get_logger(&self) -> t::Logger {
        t::Logger::new()
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, _: t::HistoryTaskCategory, _: &str) -> t::HistoryTaskKey {
        unimplemented!()
    }
    fn as_task_reinjector(&self) -> Arc<dyn crate::taskdlq::interface::TaskReinjector> {
        unimplemented!()
    }
}

/// Executes every submitted task inline and acks it on success (the scheduler worker loop).
struct RunProcessor;
#[async_trait]
impl t::Daemon for RunProcessor {
    async fn start(&self) {}
    async fn stop(&self) {}
}
#[async_trait]
impl ti::Processor for RunProcessor {
    async fn submit(&self, task: Arc<dyn ti::Task>) -> t::Result<()> {
        self.try_submit(task).await.map(|_| ())
    }
    async fn try_submit(&self, task: Arc<dyn ti::Task>) -> t::Result<bool> {
        match task.execute().await {
            Ok(()) => task.ack(),
            Err(e) => task.nack(Some(e)).await,
        }
        Ok(true)
    }
}

/// Records which typed task struct the executor received.
#[derive(Default)]
struct RecExecutor {
    seen: Mutex<Vec<String>>,
}
#[async_trait]
impl ti::Executor for RecExecutor {
    async fn execute(&self, task: Arc<dyn ti::Task>) -> (ti::ExecuteResponse, t::Result<()>) {
        let info = task.get_info();
        let any = info.as_any();
        let seen = if let Some(x) = any.downcast_ref::<t::DecisionTask>() {
            format!("decision:{}:{}:{}", x.task_list, x.schedule_id, x.task_data.task_id)
        } else if let Some(x) = any.downcast_ref::<t::UserTimerTask>() {
            format!("user-timer:{}:{}", x.event_id, x.task_data.task_id)
        } else {
            "unknown".to_string()
        };
        self.seen.lock().unwrap().push(seen);
        (ti::ExecuteResponse { scope: t::noop_scope(), is_active_task: true }, Ok(()))
    }
    fn stop(&self) {}
}

// ------------------------------------------------------------------ queuev2 fakes

struct V2Clock;
impl v2::TimeSource for V2Clock {
    fn now(&self) -> v2::Time {
        v2::unix_nano_time(NOW)
    }
    fn new_timer(&self, _: Duration) -> Arc<dyn v2::Timer> {
        unimplemented!()
    }
    fn after_func(&self, _: Duration, _: Box<dyn FnOnce() + Send>) -> Arc<dyn v2::Timer> {
        unimplemented!()
    }
}

struct Log;
impl v2::Logger for Log {
    fn debug_on(&self) -> bool {
        false
    }
    fn debug(&self, _: &str) {}
    fn info(&self, _: &str) {}
    fn warn(&self, _: &str) {}
    fn error(&self, _: &str) {}
    fn fatal(&self, m: &str) {
        panic!("{m}")
    }
    fn with_tags(&self, _: &[(&str, String)]) -> Arc<dyn v2::Logger> {
        Arc::new(Log)
    }
}

struct Watch;
impl v2::Stopwatch for Watch {
    fn stop(&self) {}
}

struct Scope;
impl v2::MetricsScope for Scope {
    fn inc_counter(&self, _: &str) {}
    fn add_counter(&self, _: &str, _: i64) {}
    fn update_gauge(&self, _: &str, _: f64) {}
    fn record_histogram_duration(&self, _: &str, _: Duration) {}
    fn record_histogram_value(&self, _: &str, _: f64) {}
    fn exponential_histogram(&self, _: &str, _: chrono::Duration) {}
    fn int_exponential_histogram(&self, _: &str, _: i64) {}
    fn start_timer_with_exponential_histogram(&self, _: &str, _: &str) -> Box<dyn v2::Stopwatch> {
        Box::new(Watch)
    }
    fn tagged(&self, _: &[(&str, String)]) -> Arc<dyn v2::MetricsScope> {
        Arc::new(Scope)
    }
}

struct V2Shard;
impl v2::ShardContext for V2Shard {
    fn get_shard_id(&self) -> i32 {
        1
    }
    fn get_range_id(&self) -> i64 {
        7
    }
    fn get_time_source(&self) -> Arc<dyn v2::TimeSource> {
        Arc::new(V2Clock)
    }
    fn get_config(&self) -> Arc<v2::QueueConfig> {
        unimplemented!()
    }
    fn get_logger(&self) -> Arc<dyn v2::Logger> {
        Arc::new(Log)
    }
    fn get_metrics_client(&self) -> Arc<dyn v2::MetricsClient> {
        unimplemented!()
    }
    fn get_execution_manager(&self) -> Arc<dyn v2::ExecutionManager> {
        unimplemented!()
    }
    fn get_queue_state(&self, _: v2::HistoryTaskCategory) -> Result<v2::TypesQueueState, v2::DepError> {
        unimplemented!()
    }
    fn update_queue_state(&self, _: v2::HistoryTaskCategory, _: v2::TypesQueueState) -> Result<(), v2::DepError> {
        unimplemented!()
    }
    fn update_if_needed_and_get_queue_max_read_level(&self, _: v2::HistoryTaskCategory, _: &str) -> v2::HistoryTaskKey {
        unimplemented!()
    }
    fn get_current_cluster_name(&self) -> String {
        "active".into()
    }
}

fn env() -> V2QueueEnv {
    V2QueueEnv {
        task_shard: Arc::new(TaskShard),
        task_processor: Arc::new(RunProcessor),
        logger: t::Logger::new(),
        critical_retry_count: Arc::new(|| 50),
        rescheduler_scope: t::noop_scope(),
    }
}

fn transfer_task(task_id: i64) -> pc::TransferTask {
    pc::TransferTask {
        domain_id: "d".into(),
        workflow_id: "wf".into(),
        run_id: "r".into(),
        task_id,
        task_type: 0,
        task_list: "tl".into(),
        schedule_id: 2,
        version: 1,
        ..Default::default()
    }
}

fn timer_task(task_id: i64) -> pc::TimerTask {
    pc::TimerTask {
        domain_id: "d".into(),
        workflow_id: "wf".into(),
        run_id: "r".into(),
        task_id,
        task_type: 2,
        event_id: 5,
        visibility_timestamp_unix_nanos: NOW,
        version: 1,
        ..Default::default()
    }
}

/// Builds a v2 task through the env exactly as `queuev2` does and submits it to the processor.
fn run_through_env(ptask: v2::PTask, queue_type: v2::QueueType, exec: Arc<RecExecutor>) -> v2::HTask {
    let env = env();
    let shard: Arc<dyn v2::ShardContext> = Arc::new(V2Shard);
    let processor: Arc<dyn v2::Processor> = V2ProcessorAdapter::new(Arc::new(RunProcessor));
    let rescheduler = env.new_rescheduler(&shard, processor.clone(), Arc::new(Log), Arc::new(Scope));
    let executor: Arc<dyn v2::TaskExecutor> = V2ExecutorAdapter::new(exec);
    let task = env.new_history_task(&shard, ptask.clone(), queue_type, executor, processor.clone(), rescheduler, Arc::new(Log));
    assert_eq!(task.get_task_key(), ptask.get_task_key());
    assert_eq!(task.state(), v2::TaskState::Pending);
    assert!(processor.try_submit(task.clone()).unwrap());
    task
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_task_executes_end_to_end() {
    let exec = Arc::new(RecExecutor::default());
    let n = NotifiedTask::from_transfer(&transfer_task(42));
    let ptask = notified_to_ptask(&n).expect("full transfer task");
    let task = run_through_env(ptask, v2::QueueType::Transfer, exec.clone());
    assert_eq!(*exec.seen.lock().unwrap(), vec!["decision:tl:2:42".to_string()]);
    assert_eq!(task.state(), v2::TaskState::Acked);
    assert_eq!(task.get_task_key(), v2::HistoryTaskKey::new_immediate(42));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_task_executes_end_to_end() {
    let exec = Arc::new(RecExecutor::default());
    let n = NotifiedTask::from_timer(&timer_task(43));
    let ptask = notified_to_ptask(&n).expect("full timer task");
    let task = run_through_env(ptask, v2::QueueType::Timer, exec.clone());
    assert_eq!(*exec.seen.lock().unwrap(), vec!["user-timer:5:43".to_string()]);
    assert_eq!(task.state(), v2::TaskState::Acked);
    assert_eq!(task.get_task_key(), v2::HistoryTaskKey::new(v2::unix_nano_time(NOW), 43));
}

// ------------------------------------------------------------------ cached reader

/// The DB reader; counts reads.
#[derive(Default)]
struct DbReader {
    reads: AtomicUsize,
    read: tokio::sync::Notify,
}
#[async_trait]
impl QueueReader for DbReader {
    async fn get_task(&self, req: &GetTaskRequest) -> Result<GetTaskResponse, v2::DepError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read.notify_one();
        Ok(GetTaskResponse {
            tasks: vec![],
            progress: GetTaskProgress {
                range: Range {
                    inclusive_min_task_key: req.progress.range.exclusive_max_task_key,
                    exclusive_max_task_key: req.progress.range.exclusive_max_task_key,
                },
                next_page_token: None,
                next_task_key: req.progress.range.exclusive_max_task_key,
            },
        })
    }
    async fn look_ahead(&self, _: &LookAHeadRequest) -> Result<LookAHeadResponse, v2::DepError> {
        unimplemented!()
    }
}

fn req(min: i64, max: i64) -> GetTaskRequest {
    let range = Range {
        inclusive_min_task_key: v2::HistoryTaskKey::new_immediate(min),
        exclusive_max_task_key: v2::HistoryTaskKey::new_immediate(max),
    };
    GetTaskRequest {
        progress: GetTaskProgress { range, next_page_token: None, next_task_key: range.inclusive_min_task_key },
        predicate: Predicate::new_universal(),
        page_size: 100,
    }
}

#[derive(Default)]
struct CountingLimiter {
    waits: AtomicUsize,
    waited: tokio::sync::Notify,
}

#[async_trait]
impl v2::Limiter for CountingLimiter {
    async fn wait(&self, _: &v2::CancelToken) -> Result<(), v2::DepError> {
        self.waits.fetch_add(1, Ordering::SeqCst);
        self.waited.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn single_slice_notification_reserves_one_load_permit() {
    use crate::queuev2::interface::Daemon;
    use crate::queuev2::monitor::{new_monitor, MonitorOptions};
    use crate::queuev2::queue_state::VirtualSliceState;
    use crate::queuev2::virtual_queue::{new_virtual_queue, VirtualQueue, VirtualQueueOptions};
    use crate::queuev2::virtual_slice::new_virtual_slice;

    let shard: Arc<dyn v2::ShardContext> = Arc::new(V2Shard);
    let processor: Arc<dyn v2::Processor> = V2ProcessorAdapter::new(Arc::new(RunProcessor));
    let logger: Arc<dyn v2::Logger> = Arc::new(Log);
    let scope: Arc<dyn v2::MetricsScope> = Arc::new(Scope);
    let rescheduler = env().new_rescheduler(&shard, processor.clone(), logger.clone(), scope.clone());
    let limiter = Arc::new(CountingLimiter::default());
    let queue = new_virtual_queue(
        processor,
        rescheduler,
        logger.clone(),
        scope,
        shard.get_time_source(),
        limiter.clone(),
        new_monitor(
            v2::HistoryTaskCategory::TRANSFER,
            MonitorOptions {
                enable_pending_task_count_alert: Arc::new(|| false),
                critical_pending_task_count: Arc::new(|| 100),
            },
        ),
        vec![],
        VirtualQueueOptions {
            page_size: Arc::new(|| 100),
            max_pending_tasks_count: Arc::new(|| 100),
            poll_backoff_interval: Arc::new(|| Duration::from_secs(1)),
            poll_backoff_interval_jitter_coefficient: Arc::new(|| 0.0),
        },
    );

    queue.start().await;
    tokio::time::timeout(Duration::from_secs(5), limiter.waited.notified())
        .await
        .expect("startup permit");

    let reader = Arc::new(DbReader::default());
    queue
        .append_slices(vec![new_virtual_slice(
            VirtualSliceState {
                range: req(11, 13).progress.range,
                predicate: Predicate::new_universal(),
            },
            Arc::new(|_| panic!("an empty persistence page has no task to initialize")),
            reader.clone(),
            crate::queuev2::pending_task_tracker::new_pending_task_tracker(),
            logger,
        )])
        .await;
    tokio::time::timeout(Duration::from_secs(5), reader.read.notified())
        .await
        .expect("notified slice was read");
    queue.stop().await;

    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    assert_eq!(limiter.waits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cached_reader_serves_notified_tasks_and_falls_back_to_db() {
    let db = Arc::new(DbReader::default());
    let reader = new_cached_immediate_queue_reader_with_options(
        db.clone(),
        Box::new(new_in_mem_queue()),
        Arc::new(V2Shard),
        Arc::new(V2Clock),
        Arc::new(Log),
        Arc::new(Scope),
        CachedQueueReaderOptions {
            mode: Arc::new(|_| "enabled".to_string()),
            max_size: Arc::new(|| 2),
            shadow_sample_interval: Arc::new(|| Duration::ZERO),
        },
    );
    reader.start();

    // Notifications without a payload (failover callbacks) are not injected.
    let mut no_payload = NotifiedTask::from_transfer(&transfer_task(9));
    no_payload.full = None;
    let notified = vec![
        no_payload,
        NotifiedTask::from_transfer(&transfer_task(10)),
        NotifiedTask::from_transfer(&transfer_task(11)),
        NotifiedTask::from_transfer(&transfer_task(12)),
    ];
    let info = notify_task_info(&notified, false);
    assert_eq!(info.tasks.len(), 3);
    reader.inject(&info.tasks);

    // Cache keeps the newest 2 (max size): range [11, 13) is served without a DB read, and the
    // served tasks still carry the full blob the executors need.
    let resp = reader.get_task(&req(11, 13)).await.unwrap();
    assert_eq!(db.reads.load(Ordering::SeqCst), 0);
    let ids: Vec<i64> = resp.tasks.iter().map(|x| x.get_task_id()).collect();
    assert_eq!(ids, vec![11, 12]);
    let row = resp.tasks[0].as_any().and_then(|a| a.downcast_ref::<SqlHistoryTask>()).unwrap();
    assert_eq!(row.transfer_task().unwrap().task_list, "tl");

    // Task 10 was evicted: a range starting below the cache lower bound reads the DB.
    reader.get_task(&req(10, 13)).await.unwrap();
    assert_eq!(db.reads.load(Ordering::SeqCst), 1);

    // A persistence error notification clears the cache (Go cachedQueue.NotifyNewTask), so the
    // next read goes to the DB too.
    assert!(notify_task_info(&[], true).persistence_error);
    reader.clear();
    reader.get_task(&req(11, 13)).await.unwrap();
    assert_eq!(db.reads.load(Ordering::SeqCst), 2);
    reader.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_gate_fires_at_earliest_update() {
    use v2::TimerGate;
    let gate = V2TimerGate::new(Arc::new(V2Clock));
    assert!(gate.update(v2::unix_nano_time(NOW + 1_000_000_000)));
    assert!(gate.update(v2::unix_nano_time(NOW - 1)));
    assert!(!gate.update(v2::unix_nano_time(NOW + 5)));
    tokio::time::timeout(Duration::from_secs(5), gate.fired()).await.expect("gate fired");
}
