//! Port of `go/service/history/queue/timer_queue_failover_processor.go`.
use std::collections::HashSet;
use std::sync::Arc;

use super::constants::DEFAULT_PROCESSING_QUEUE_LEVEL;
use super::deps::{HistoryTaskCategory, PersistenceTask, QueueError, Time, TaskKey};
use super::domain_filter::DomainFilter;
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{QueueShutdownFn, UpdateMaxReadLevelFn};
use super::processor_options_factory::new_timer_queue_processor_options;
use super::shard_deps::{FailoverLevel, HistoryKey, QueueShardContext, QueueType, TaskFilter};
use super::task_allocator::is_domain_not_registered;
use super::timer_gate::TimerGate;
use super::timer_queue_processor::{
    load_timer_task_initializer, timer_visibility, unexpected_task_err, TimerQueueDeps,
    TASK_TYPE_DELETE_HISTORY_EVENT,
};
use super::timer_queue_processor_base::TimerQueueProcessorBase;
use super::util::new_timer_task_key;

pub type UpdateClusterAckLevelAsync = Arc<dyn Fn(TaskKey) -> super::deps::BoxFut<Result<(), QueueError>> + Send + Sync>;

/// Go `newTimerQueueFailoverProcessor`: returns (updateClusterAckLevelFn, processor).
pub fn new_timer_queue_failover_processor(
    deps: &TimerQueueDeps,
    standby_cluster_name: &str,
    min_level: Time,
    max_level: Time,
    domain_ids: HashSet<String>,
) -> (UpdateClusterAckLevelAsync, Arc<TimerQueueProcessorBase>) {
    let shard = deps.shard.clone();
    let options = Arc::new(new_timer_queue_processor_options(&shard.queue_config(), true, true));

    let current_cluster_name = shard.current_cluster_name();
    let failover_start_time = shard.now();
    let failover_uuid = format!("{:032x}", rand::random::<u128>());
    tracing::info!(
        cluster = %current_cluster_name,
        domain_ids = ?domain_ids,
        failover = %format!("from: {standby_cluster_name}"),
        "creating timer failover processor"
    );

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let domain_ids = domain_ids.clone();
        Arc::new(move |timer: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let domain_ids = domain_ids.clone();
            Box::pin(async move {
                if timer.get_task_category() != HistoryTaskCategory::Timer {
                    return Err(unexpected_task_err());
                }
                if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &timer.get_domain_id()) {
                    if timer.get_task_type() == TASK_TYPE_DELETE_HISTORY_EVENT {
                        return Ok(true);
                    }
                    tracing::info!(
                        domain_id = %timer.get_domain_id(),
                        task = ?timer,
                        "Domain is not in registered status, skip task in failover timer queue."
                    );
                    return Ok(false);
                }
                allocator
                    .verify_failover_active_task(
                        &domain_ids,
                        &timer.get_domain_id(),
                        &timer.get_workflow_id(),
                        &timer.get_run_id(),
                    )
                    .await
            })
        })
    };

    let max_read_level_task_key = new_timer_task_key(max_level, 0);
    let update_max_read_level: UpdateMaxReadLevelFn = Arc::new(move || max_read_level_task_key); // this is a const

    let update_cluster_ack_level: UpdateClusterAckLevelAsync = {
        let shard = shard.clone();
        let failover_uuid = failover_uuid.clone();
        let domain_ids = domain_ids.clone();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let failover_uuid = failover_uuid.clone();
            let domain_ids = domain_ids.clone();
            Box::pin(async move {
                let level = FailoverLevel {
                    start_time: failover_start_time,
                    min_level: HistoryKey::new(min_level, 0),
                    current_level: HistoryKey::new(timer_visibility(ack_level), 0),
                    max_level: HistoryKey::new(max_level, 0),
                    domain_ids,
                };
                shard.update_failover_level(HistoryTaskCategory::Timer, &failover_uuid, level).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = {
        let shard = shard.clone();
        let failover_uuid = failover_uuid.clone();
        Arc::new(move || {
            let shard = shard.clone();
            let failover_uuid = failover_uuid.clone();
            Box::pin(async move { shard.delete_failover_level(HistoryTaskCategory::Timer, &failover_uuid).await })
        })
    };

    let processing_queue_states = vec![ProcessingQueueState::new(
        DEFAULT_PROCESSING_QUEUE_LEVEL,
        new_timer_task_key(min_level, 0),
        max_read_level_task_key,
        DomainFilter::new(domain_ids.clone(), false),
    )];

    let now_shard = shard.clone();
    let processor = TimerQueueProcessorBase::new(
        current_cluster_name, // should use current cluster's time when doing domain failover
        shard.clone(),
        processing_queue_states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::ActiveTimer),
        TimerGate::new_local(Arc::new(move || now_shard.now())),
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level.clone(),
        None,
        queue_shutdown,
        load_timer_task_initializer(QueueType::ActiveTimer, deps.active_task_factory.clone(), task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    );
    (update_cluster_ack_level, processor)
}
