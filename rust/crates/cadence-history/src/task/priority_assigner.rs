//! Port of `go/service/history/task/priority_assigner.go`.

use std::sync::Arc;

use super::constants::NO_PRIORITY;
use super::deps::*;
use super::deps_exec::*;
use super::interface::*;

const NUM_BITS_PER_LEVEL: u32 = 3;
const HIGH_PRIORITY_CLASS: i32 = 0 << NUM_BITS_PER_LEVEL;
const DEFAULT_PRIORITY_CLASS: i32 = 1 << NUM_BITS_PER_LEVEL;
const LOW_PRIORITY_CLASS: i32 = 2 << NUM_BITS_PER_LEVEL;
const DEFAULT_PRIORITY_SUBCLASS: i32 = 1;

/// `constants.GetTaskPriority`
pub const fn get_task_priority(class: i32, sub_class: i32) -> i32 {
    class | sub_class
}

pub const HIGH_TASK_PRIORITY: i32 = get_task_priority(HIGH_PRIORITY_CLASS, DEFAULT_PRIORITY_SUBCLASS);
pub const DEFAULT_TASK_PRIORITY: i32 = get_task_priority(DEFAULT_PRIORITY_CLASS, DEFAULT_PRIORITY_SUBCLASS);
pub const LOW_TASK_PRIORITY: i32 = get_task_priority(LOW_PRIORITY_CLASS, DEFAULT_PRIORITY_SUBCLASS);

pub struct PriorityAssignerImpl {
    current_cluster_name: String,
    domain_cache: Arc<dyn DomainCache>,
    config: Arc<dyn HistoryConfig>,
    logger: Logger,
    scope: Arc<dyn MetricsScope>,
    rate_limiters: Arc<dyn LimiterCollection>,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
}

/// `NewPriorityAssigner`
pub fn new_priority_assigner(
    current_cluster_name: String,
    domain_cache: Arc<dyn DomainCache>,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
    logger: Logger,
    metric_client: Arc<dyn MetricsClient>,
    config: Arc<dyn HistoryConfig>,
) -> Arc<dyn PriorityAssigner> {
    let c = config.clone();
    Arc::new(PriorityAssignerImpl {
        current_cluster_name,
        domain_cache,
        active_cluster_mgr,
        logger,
        scope: metric_client.scope(metrics::TASK_PRIORITY_ASSIGNER_SCOPE, vec![]),
        rate_limiters: new_simple_dynamic_collection(Arc::new(move |_k: &str| c.task_process_rps())),
        config,
    })
}

impl PriorityAssigner for PriorityAssignerImpl {
    fn assign(&self, queue_task: &dyn Task) -> Result<()> {
        let priority = queue_task.priority();
        if priority != NO_PRIORITY {
            // already assigned: drop to low priority when retried too many times
            if priority != LOW_TASK_PRIORITY && queue_task.get_attempt() > self.config.task_critical_retry_count() {
                queue_task.set_priority(LOW_TASK_PRIORITY);
            }
            return Ok(());
        }

        let queue_type = queue_task.get_queue_type();
        if queue_type == QueueType::Replication {
            queue_task.set_priority(LOW_TASK_PRIORITY);
            return Ok(());
        }

        let is_active_task = queue_type == QueueType::ActiveTimer || queue_type == QueueType::ActiveTransfer;
        let (domain_name, is_active_domain) = self.get_domain_info(
            queue_task.get_domain_id(),
            queue_task.get_workflow_id(),
            queue_task.get_run_id(),
        )?;

        if !is_active_task && !is_active_domain {
            queue_task.set_priority(LOW_TASK_PRIORITY);
            return Ok(());
        }

        if !self.rate_limiters.for_key(&domain_name).allow() {
            queue_task.set_priority(DEFAULT_TASK_PRIORITY);
            let tagged = self.scope.tagged(vec![domain_tag(&domain_name)]);
            match queue_type {
                QueueType::ActiveTransfer | QueueType::StandbyTransfer => {
                    tagged.inc_counter(metrics::TRANSFER_TASK_THROTTLED_COUNTER)
                }
                QueueType::ActiveTimer | QueueType::StandbyTimer => {
                    tagged.inc_counter(metrics::TIMER_TASK_THROTTLED_COUNTER)
                }
                _ => {}
            }
            return Ok(());
        }

        queue_task.set_priority(HIGH_TASK_PRIORITY);
        Ok(())
    }
}

impl PriorityAssignerImpl {
    /// Go's `getDomainInfo` (name, isActiveDomain, err). The active-cluster lookup is async in
    /// Rust; it is driven with `block_in_place` to keep the sync `Assign` signature.
    fn get_domain_info(&self, domain_id: &str, wf_id: &str, r_id: &str) -> Result<(String, bool)> {
        let domain_entry = match self.domain_cache.get_domain_by_id(domain_id) {
            Ok(e) => e,
            Err(e) => {
                if !matches!(e, TaskError::EntityNotExists(_)) {
                    self.logger.warn_tags("Cannot find domain", &[tag::workflow_domain_id(domain_id)]);
                    return Err(e);
                }
                // domain is not found, treat as active
                self.logger.warn_tags("Cannot find domain, treat as active ", &[tag::workflow_domain_id(domain_id)]);
                return Ok((String::new(), true));
            }
        };

        let mgr = self.active_cluster_mgr.clone();
        let (d, w, r) = (domain_id.to_string(), wf_id.to_string(), r_id.to_string());
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { mgr.get_active_cluster_info_by_workflow(&d, &w, &r).await })
        });
        match res {
            Err(e) => {
                self.logger.warn_tags(
                    "Failed to get active cluster info",
                    &[tag::workflow_domain_id(domain_id), tag::error(&e)],
                );
                Ok((String::new(), true))
            }
            Ok(info) => {
                if info.active_cluster_name != self.current_cluster_name {
                    return Ok((domain_entry.info.name.clone(), false));
                }
                Ok((domain_entry.info.name.clone(), true))
            }
        }
    }
}
