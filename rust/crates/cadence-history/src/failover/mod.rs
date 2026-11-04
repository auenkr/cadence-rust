//! Port of `go/service/history/failover` (coordinator.go, marker_notifier.go).
//!
//! Dependencies owned by other (not yet merged) modules are expressed as small
//! traits here: [`TimeSource`], [`DomainCache`], [`DomainManager`],
//! [`HistoryClient`], [`FailoverMetrics`] and [`ShardContext`].
pub mod coordinator;
pub mod marker_notifier;

pub use coordinator::{Coordinator, CoordinatorImpl, FailoverError, NewCoordinatorParams};
pub use marker_notifier::{MarkerNotifier, MarkerNotifierImpl};

use async_trait::async_trait;
use cadence_api::types::FailoverMarkerAttributes;
use std::time::Duration;

/// `clock.TimeSource`: wall clock as unix nanoseconds.
pub trait TimeSource: Send + Sync {
    fn now_nanos(&self) -> i64;
}

/// `cache.DomainCache.GetDomainName`.
pub trait DomainCache: Send + Sync {
    fn get_domain_name(&self, domain_id: &str) -> Result<String, String>;
}

/// `domain.CleanPendingActiveState(domainManager, domainID, version, retryPolicy)`.
/// Returns `Ok(true)` when the domain record was updated.
#[async_trait]
pub trait DomainManager: Send + Sync {
    async fn clean_pending_active_state(
        &self,
        domain_id: &str,
        failover_version: i64,
    ) -> Result<bool, String>;
}

/// `history.Client.NotifyFailoverMarkers`.
#[async_trait]
pub trait HistoryClient: Send + Sync {
    async fn notify_failover_markers(
        &self,
        tokens: Vec<FailoverMarkerToken>,
        timeout: Duration,
    ) -> Result<(), String>;
}

/// `types.FailoverMarkerToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverMarkerToken {
    pub shard_ids: Vec<i32>,
    pub failover_marker: FailoverMarkerAttributes,
}

/// `types.GetFailoverInfoResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GetFailoverInfoResponse {
    pub completed_shard_count: i32,
    pub pending_shards: Vec<i32>,
}

/// Metric names used by the failover package (`metrics.FailoverMarkerScope`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverMetric {
    DroppedRegressedDomain,
    CadenceFailures,
    NotificationFailure,
    UpdateShardFailure,
    MarkerCount,
    GracefulFailoverLatency,
    GracefulFailoverLatencyHistogram,
}

pub trait FailoverMetrics: Send + Sync {
    fn inc_counter(&self, _metric: FailoverMetric, _domain: Option<&str>) {}
    fn update_gauge(&self, _metric: FailoverMetric, _domain: Option<&str>, _value: f64) {}
    fn record_timer(&self, _metric: FailoverMetric, _domain: Option<&str>, _d: Duration) {}
}

/// Subset of Go `shard.Context` used by the marker notifier.
pub trait ShardContext: Send + Sync {
    fn shard_id(&self) -> i32;
    fn validate_and_update_failover_markers(
        &self,
    ) -> Result<Vec<FailoverMarkerAttributes>, String>;
}

/// Subset of the history `config.Config` used by failover.
pub trait FailoverConfig: Send + Sync {
    fn number_of_shards(&self) -> usize;
    fn notify_failover_marker_interval(&self) -> Duration;
    fn notify_failover_marker_timer_jitter_coefficient(&self) -> f64;
}
