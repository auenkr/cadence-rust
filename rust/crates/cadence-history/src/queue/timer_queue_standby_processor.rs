//! Port of `go/service/history/queue/timer_queue_standby_processor.go`.
use std::sync::Arc;

use super::deps::{HistoryTaskCategory, PersistenceTask, QueueError, TaskKey};
use super::processing_queue_state::ProcessingQueueState;
use super::processor_base::{QueueShutdownFn, UpdateClusterAckLevelFn, UpdateMaxReadLevelFn, UpdateProcessingQueueStatesFn};
use super::processor_options_factory::new_timer_queue_processor_options;
use super::shard_deps::{HistoryKey, HistoryTaskFactory, QueueShardContext, QueueType, TaskFilter};
use super::task_allocator::is_domain_not_registered;
use super::timer_gate::TimerGate;
use super::timer_queue_processor::{
    load_timer_processing_queue_states, load_timer_task_initializer, timer_visibility, unexpected_task_err,
    TimerQueueDeps, TASK_TYPE_DELETE_HISTORY_EVENT, TASK_TYPE_WORKFLOW_TIMEOUT,
};
use super::timer_queue_processor_base::TimerQueueProcessorBase;
use super::util::{convert_to_persistence_timer_processing_queue_states, new_timer_task_key};

/// Go `newTimerQueueStandbyProcessor`; also returns the remote timer gate.
pub fn new_timer_queue_standby_processor(
    cluster_name: &str,
    deps: &TimerQueueDeps,
    factory: HistoryTaskFactory,
) -> (Arc<TimerQueueProcessorBase>, Arc<TimerGate>) {
    let shard = deps.shard.clone();
    let options = Arc::new(new_timer_queue_processor_options(&shard.queue_config(), false, false));
    let cluster_name = cluster_name.to_string();

    let task_filter: TaskFilter = {
        let shard = shard.clone();
        let allocator = deps.task_allocator.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |timer: Arc<dyn PersistenceTask>| {
            let shard = shard.clone();
            let allocator = allocator.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                if timer.get_task_category() != HistoryTaskCategory::Timer {
                    return Err(unexpected_task_err());
                }
                if let Ok(true) = is_domain_not_registered(shard.domain_cache().as_ref(), &timer.get_domain_id()) {
                    // Allow deletion tasks for deprecated domains
                    if timer.get_task_type() == TASK_TYPE_DELETE_HISTORY_EVENT {
                        return Ok(true);
                    }
                    tracing::info!(
                        domain_id = %timer.get_domain_id(),
                        task = ?timer,
                        "Domain is not in registered status, skip task in standby timer queue."
                    );
                    return Ok(false);
                }
                if timer.get_task_type() == TASK_TYPE_WORKFLOW_TIMEOUT
                    || timer.get_task_type() == TASK_TYPE_DELETE_HISTORY_EVENT
                {
                    match shard.domain_cache().get_domain_by_id(&timer.get_domain_id()) {
                        Ok(entry) => {
                            if entry.has_replication_cluster(&cluster) {
                                // guarantee the processing of workflow execution history deletion
                                return Ok(true);
                            }
                        }
                        Err(QueueError::EntityNotExists(_)) => {
                            tracing::warn!(
                                domain_id = %timer.get_domain_id(),
                                task = ?timer,
                                "Cannot find domain, default to not process task."
                            );
                            return Ok(false);
                        }
                        Err(e) => {
                            // retry the task if failed to find the domain
                            tracing::warn!(domain_id = %timer.get_domain_id(), "Cannot find domain");
                            return Err(e);
                        }
                    }
                }
                allocator
                    .verify_standby_task(&cluster, &timer.get_domain_id(), &timer.get_workflow_id(), &timer.get_run_id())
                    .await
            })
        })
    };

    let update_max_read_level: UpdateMaxReadLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move || {
            new_timer_task_key(
                shard.update_if_needed_and_get_queue_max_read_level(HistoryTaskCategory::Timer, &cluster).scheduled_time,
                0,
            )
        })
    };

    let update_cluster_ack_level: UpdateClusterAckLevelFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |ack_level: TaskKey| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                shard
                    .update_queue_cluster_ack_level(
                        HistoryTaskCategory::Timer,
                        &cluster,
                        HistoryKey::new(timer_visibility(ack_level), 0),
                    )
                    .await
            })
        })
    };

    let update_processing_queue_states: UpdateProcessingQueueStatesFn = {
        let shard = shard.clone();
        let cluster = cluster_name.clone();
        Arc::new(move |states: Vec<ProcessingQueueState>| {
            let shard = shard.clone();
            let cluster = cluster.clone();
            Box::pin(async move {
                let p_states = convert_to_persistence_timer_processing_queue_states(&states);
                shard.update_timer_processing_queue_states(&cluster, p_states).await
            })
        })
    };

    let queue_shutdown: QueueShutdownFn = Arc::new(|| Box::pin(async { Ok(()) }));

    let remote_timer_gate = TimerGate::new_event(shard.get_current_time(&cluster_name));
    let states = load_timer_processing_queue_states(&cluster_name, shard.as_ref(), &options);

    let base = TimerQueueProcessorBase::new(
        cluster_name,
        shard.clone(),
        states,
        deps.task_processor.clone(),
        (deps.redispatcher_factory)(QueueType::StandbyTimer),
        remote_timer_gate.clone(),
        options.clone(),
        update_max_read_level,
        update_cluster_ack_level,
        Some(update_processing_queue_states),
        queue_shutdown,
        load_timer_task_initializer(QueueType::StandbyTimer, factory, task_filter),
        deps.metrics.clone(),
        (deps.rate_limiter_factory)(options.max_poll_rps.clone()),
    );
    (base, remote_timer_gate)
}
