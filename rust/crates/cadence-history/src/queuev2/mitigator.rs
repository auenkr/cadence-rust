//! Port of queuev2/mitigator.go

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::alert::{Alert, AlertType};
use super::deps::*;
use super::monitor::Monitor;
use super::predicate::Predicate;
use super::virtual_queue::VirtualQueue;
use super::virtual_queue_manager::VirtualQueueManager;
use super::virtual_slice::{slice_key, VirtualSlice};

const TARGET_LOAD_FACTOR: f64 = 0.8;
const CLEAR_SLICE_THROTTLE_DURATION: Duration = Duration::from_secs(10);

#[async_trait]
pub trait Mitigator: Send + Sync {
    async fn mitigate(&self, alert: Alert);
}

pub struct MitigatorOptions {
    pub max_virtual_queue_count: IntFn,
}

pub struct MitigatorImpl {
    virtual_queue_manager: Arc<dyn VirtualQueueManager>,
    monitor: Arc<dyn Monitor>,
    logger: Arc<dyn Logger>,
    #[allow(dead_code)]
    metrics_scope: Arc<dyn MetricsScope>,
    options: MitigatorOptions,
}

struct PendingTaskStatsAll {
    total_pending_task_count: i64,
    pending_task_count_per_domain: HashMap<String, i64>,
    pending_task_count_per_domain_per_slice: HashMap<usize, HashMap<String, i64>>,
    slices_per_domain: HashMap<String, Vec<Arc<dyn VirtualSlice>>>,
}

pub fn new_mitigator(
    virtual_queue_manager: Arc<dyn VirtualQueueManager>,
    monitor: Arc<dyn Monitor>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    options: MitigatorOptions,
) -> Arc<dyn Mitigator> {
    Arc::new(MitigatorImpl {
        virtual_queue_manager,
        monitor,
        logger,
        metrics_scope,
        options,
    })
}

#[async_trait]
impl Mitigator for MitigatorImpl {
    async fn mitigate(&self, alert: Alert) {
        match alert.alert_type {
            AlertType::QueuePendingTaskCount => self.handle_queue_pending_task_count(&alert).await,
            _ => {
                self.logger.error(&format!(
                    "unknown queue alert type alertType={}",
                    alert.alert_type as i32
                ));
                return;
            }
        }
        self.monitor.resolve_alert(alert.alert_type);
        self.logger.info(&format!(
            "mitigated queue alert alertType={}",
            alert.alert_type as i32
        ));
    }
}

impl MitigatorImpl {
    async fn handle_queue_pending_task_count(&self, alert: &Alert) {
        let virtual_queues = self.virtual_queue_manager.virtual_queues().await;
        for vq in virtual_queues.values() {
            vq.update_and_get_state().await;
        }
        let critical = match &alert.alert_attributes_queue_pending_task_count {
            Some(a) => a.critical_pending_task_count,
            None => return,
        };
        if self.monitor.get_total_pending_task_count() <= critical {
            self.logger
                .debug("mitigating queue alert, skip mitigation because the alert is no longer valid");
            return;
        }
        let stats = self.collect_pending_task_stats().await;

        let target_pending_task_count = (critical as f64 * TARGET_LOAD_FACTOR) as i64;
        if self.logger.debug_on() {
            self.logger.debug(&format!(
                "mitigating queue alert, get task stats alertType={} pending-task-count-per-domain={:?} pending-task-count={} target-task-count={}",
                alert.alert_type as i32,
                stats.pending_task_count_per_domain,
                stats.total_pending_task_count,
                target_pending_task_count
            ));
        }
        let domains_to_clear_per_slice = self.find_domains_to_clear(stats, target_pending_task_count);
        if self.logger.debug_on() {
            for domains in domains_to_clear_per_slice.values() {
                self.logger.debug(&format!(
                    "mitigating queue alert, get domains to clear domains={domains:?}"
                ));
            }
        }

        self.process_queue_splits_and_clear(&virtual_queues, &domains_to_clear_per_slice)
            .await;
        if self.logger.debug_on() {
            let vqs = self.virtual_queue_manager.virtual_queues().await;
            for (queue_id, vq) in vqs {
                self.logger.debug(&format!(
                    "mitigating queue alert, get queue state after mitigation queueID={queue_id} slices={}",
                    vq.get_state().await.len()
                ));
            }
        }
    }

    async fn collect_pending_task_stats(&self) -> PendingTaskStatsAll {
        let mut stats = PendingTaskStatsAll {
            total_pending_task_count: 0,
            pending_task_count_per_domain: HashMap::new(),
            pending_task_count_per_domain_per_slice: HashMap::new(),
            slices_per_domain: HashMap::new(),
        };

        for vq in self.virtual_queue_manager.virtual_queues().await.values() {
            vq.iterate_slices(&mut |slice| {
                let per_domain = slice.pending_task_stats().pending_task_count_per_domain;
                stats
                    .pending_task_count_per_domain_per_slice
                    .insert(slice_key(slice), per_domain.clone());
                for (domain, count) in per_domain {
                    stats.total_pending_task_count += count;
                    *stats
                        .pending_task_count_per_domain
                        .entry(domain.clone())
                        .or_insert(0) += count;
                    stats
                        .slices_per_domain
                        .entry(domain)
                        .or_default()
                        .push(slice.clone());
                }
            })
            .await;
        }

        for slices_list in stats.slices_per_domain.values_mut() {
            // descending by InclusiveMinTaskKey
            slices_list.sort_by(|a, b| {
                b.get_state()
                    .range
                    .inclusive_min_task_key
                    .cmp(&a.get_state().range.inclusive_min_task_key)
            });
        }
        stats
    }

    fn find_domains_to_clear(
        &self,
        mut stats: PendingTaskStatsAll,
        target_count: i64,
    ) -> HashMap<usize, Vec<String>> {
        let mut domains_to_clear: HashMap<usize, Vec<String>> = HashMap::new();

        // Max-priority queue by pending task count (Go: collection.NewPriorityQueue).
        let mut pq: Vec<String> = stats.pending_task_count_per_domain.keys().cloned().collect();

        while stats.total_pending_task_count > target_count && !pq.is_empty() {
            let mut best = 0;
            for i in 1..pq.len() {
                if stats.pending_task_count_per_domain[&pq[i]]
                    > stats.pending_task_count_per_domain[&pq[best]]
                {
                    best = i;
                }
            }
            let domain = pq.swap_remove(best);
            let slices = stats.slices_per_domain.entry(domain.clone()).or_default();
            if slices.is_empty() {
                continue;
            }
            let slice = slices.remove(0);

            let key = slice_key(&slice);
            let task_count = *stats
                .pending_task_count_per_domain_per_slice
                .get(&key)
                .and_then(|m| m.get(&domain))
                .unwrap_or(&0);
            stats.total_pending_task_count -= task_count;
            let c = stats
                .pending_task_count_per_domain
                .entry(domain.clone())
                .or_insert(0);
            *c -= task_count;
            if *c > 0 {
                pq.push(domain.clone());
            }
            domains_to_clear.entry(key).or_default().push(domain);
        }
        domains_to_clear
    }

    async fn process_queue_splits_and_clear(
        &self,
        virtual_queues: &HashMap<i64, Arc<dyn VirtualQueue>>,
        domains_to_clear: &HashMap<usize, Vec<String>>,
    ) {
        let max_queue_id = (self.options.max_virtual_queue_count)() - 1;
        for (queue_id, vq) in virtual_queues {
            let queue_id = *queue_id;
            if queue_id >= max_queue_id {
                let mut cleared = false;
                vq.clear_slices(&mut |slice| {
                    let ok = domains_to_clear.contains_key(&slice_key(slice));
                    cleared = cleared || ok;
                    ok
                })
                .await;
                if cleared {
                    vq.pause(CLEAR_SLICE_THROTTLE_DURATION);
                }
                continue;
            }

            let mut slices_to_move: Vec<Arc<dyn VirtualSlice>> = Vec::new();
            vq.split_slices(&mut |slice| {
                let domains = match domains_to_clear.get(&slice_key(slice)) {
                    Some(d) if !d.is_empty() => d,
                    _ => return (Vec::new(), false),
                };
                let predicate = Predicate::new_domain_id(domains, false);
                match slice.try_split_by_predicate(&predicate) {
                    None => {
                        slice.clear();
                        slices_to_move.push(slice.clone());
                        (Vec::new(), true)
                    }
                    Some((split_slice, remaining_slice)) => {
                        split_slice.clear();
                        slices_to_move.push(split_slice);
                        (vec![remaining_slice], true)
                    }
                }
            })
            .await;

            if !slices_to_move.is_empty() {
                let next_queue = self
                    .virtual_queue_manager
                    .get_or_create_virtual_queue(queue_id + 1)
                    .await;
                next_queue.pause(CLEAR_SLICE_THROTTLE_DURATION);
                next_queue.merge_slices(slices_to_move).await;
            }
        }
    }
}
