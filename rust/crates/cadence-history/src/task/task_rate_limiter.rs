//! Port of `go/service/history/task/task_rate_limiter.go`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::deps::*;
use super::deps_exec::*;
use super::interface::Task;

/// `RateLimiter`
#[async_trait]
pub trait RateLimiter: Send + Sync {
    fn allow(&self, t: &dyn Task) -> bool;
    async fn wait(&self, ctx: &CancellationToken, t: &dyn Task) -> Result<()>;
}

pub struct TaskRateLimiterImpl {
    logger: Logger,
    metrics_scope: Arc<dyn MetricsScope>,
    domain_cache: Arc<dyn DomainCache>,
    limiters: Arc<dyn LimiterCollection>,
    enabled: BoolPropertyFn,
    enable_shadow_mode: BoolPropertyFnWithDomainFilter,
}

/// `NewRateLimiter`
pub fn new_rate_limiter(
    logger: Logger,
    metrics_client: Arc<dyn MetricsClient>,
    domain_cache: Arc<dyn DomainCache>,
    config: Arc<dyn HistoryConfig>,
    controller: Arc<dyn ShardController>,
) -> Arc<dyn RateLimiter> {
    let c = config.clone();
    let rps = move |domain: &str| -> i64 {
        let total_shards = c.number_of_shards() as f64;
        let total_rps = c.task_scheduler_global_domain_rps(domain) as f64;
        let num_shards = controller.num_shards() as f64;
        (total_rps * num_shards / total_shards) as i64
    };
    let limiter_factory = new_simple_dynamic_collection(Arc::new(rps));
    Arc::new(TaskRateLimiterImpl {
        logger,
        metrics_scope: metrics_client.scope(metrics::TASK_SCHEDULER_RATE_LIMITER_SCOPE, vec![]),
        domain_cache,
        enabled: config.task_scheduler_enable_rate_limiter(),
        enable_shadow_mode: config.task_scheduler_enable_rate_limiter_shadow_mode(),
        limiters: limiter_factory,
    })
}

impl TaskRateLimiterImpl {
    fn get_limiter(&self, t: &dyn Task) -> (Arc<dyn Limiter>, Arc<dyn MetricsScope>, bool) {
        let domain_id = t.get_domain_id();
        let domain_name = match self.domain_cache.get_domain_name(domain_id) {
            Ok(n) => n,
            Err(e) => {
                self.logger.warn_tags(
                    "failed to get domain name from domain cache",
                    &[tag::workflow_domain_id(domain_id), tag::error(&e)],
                );
                String::new()
            }
        };
        (
            self.limiters.for_key(&domain_name),
            self.metrics_scope.tagged(vec![domain_tag(&domain_name)]),
            (self.enable_shadow_mode)(&domain_name),
        )
    }
}

#[async_trait]
impl RateLimiter for TaskRateLimiterImpl {
    fn allow(&self, t: &dyn Task) -> bool {
        if !(self.enabled)() {
            return true;
        }
        let (limiter, scope, shadow) = self.get_limiter(t);
        let allow = limiter.allow();
        if allow {
            scope.inc_counter(metrics::TASK_SCHEDULER_ALLOWED_COUNTER_PER_DOMAIN);
            return true;
        }
        scope.inc_counter(metrics::TASK_SCHEDULER_THROTTLED_COUNTER_PER_DOMAIN);
        shadow
    }

    async fn wait(&self, ctx: &CancellationToken, t: &dyn Task) -> Result<()> {
        if !(self.enabled)() {
            return Ok(());
        }
        let (limiter, _, _) = self.get_limiter(t);
        limiter.wait(ctx).await
    }
}
