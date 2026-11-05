//! Assembly of the failover package (go/service/history/failover): coordinator,
//! marker notifier and the shard / history-client adapters they need.
//!
//! Gaps (documented, see memory/history-port-wiring-20261105.md, Unit D2/D3):
//! * `LocalFailoverHistoryClient` delivers `NotifyFailoverMarkers` tokens to the
//!   coordinator of this process only. Go routes tokens through the history
//!   client to the host owning each shard; multi-host routing needs the history
//!   RPC client (not available yet).
//! * `shard_context::FailoverMarker` stores only (domain id, version); the marker
//!   creation time is therefore re-stamped with "now" when markers are read back.
//! * The domain failover watcher (go/common/domain/failover_watcher.go) is part of
//!   the frontend/worker resource set, not history, and is not started here.
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::enums::DomainStatus;
use cadence_api::types::FailoverMarkerAttributes;
use cadence_matching::matching_domain_cache::{DomainCacheError, DomainErrorClass};
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;

use crate::failover::{
    Coordinator, CoordinatorImpl, DomainManager, FailoverConfig, FailoverMarkerToken, FailoverMetrics, HistoryClient,
    MarkerNotifier, MarkerNotifierImpl, NewCoordinatorParams, ShardContext as FailoverShard, TimeSource,
};
use crate::shard_context::{FailoverMarker, ShardContext, ShardContextError};
use crate::wiring::resources::domain_cache::HistoryDomainCache;

/// Go `IsActiveIn`: a local (non-global) domain is active everywhere.
pub fn domain_is_active_in(m: &MatchingDomainMetadata, cluster: &str) -> bool {
    !m.is_global_domain || m.replication.active_cluster_name == cluster
}

/// Go `ValidateAndUpdateFailoverMarkers` / `AddingPendingFailoverMarker` drop rule
/// (shard/context.go): deprecated domain, active here, newer domain failover
/// version, or the failover already completed.
pub fn failover_marker_is_stale(m: &MatchingDomainMetadata, current_cluster: &str, marker_version: i64) -> bool {
    let pending_active = m.failover_end_time_nanos.is_some();
    let completed = !pending_active && m.failover_version >= marker_version;
    matches!(m.info.status, Some(DomainStatus::Deprecated))
        || domain_is_active_in(m, current_cluster)
        || m.failover_version > marker_version
        || completed
}

/// `Ok(true)` when the domain does not exist (marker dropped), as in Go.
pub fn marker_stale_by_cache(
    domains: &HistoryDomainCache,
    current_cluster: &str,
    marker: &FailoverMarker,
) -> Result<bool, ShardContextError> {
    match domains.inner().get_domain_by_id(&marker.domain_id) {
        Ok(m) => Ok(failover_marker_is_stale(&m, current_cluster, marker.failover_version)),
        Err(DomainCacheError::SourceClassified { class: DomainErrorClass::NotFound, .. }) => Ok(true),
        Err(e) => Err(ShardContextError::Other(e.to_string())),
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// `failover.ShardContext` over the real shard context.
pub struct FailoverShardAdapter {
    pub shard: Arc<ShardContext>,
    pub domains: Arc<HistoryDomainCache>,
    pub current_cluster: String,
}

impl FailoverShard for FailoverShardAdapter {
    fn shard_id(&self) -> i32 {
        self.shard.shard_id()
    }
    fn validate_and_update_failover_markers(&self) -> Result<Vec<FailoverMarkerAttributes>, String> {
        let domains = self.domains.clone();
        let cluster = self.current_cluster.clone();
        let remaining = self
            .shard
            .validate_and_update_failover_markers(|m| marker_stale_by_cache(&domains, &cluster, m))
            .map_err(|e| e.to_string())?;
        let now = now_nanos();
        Ok(remaining
            .into_iter()
            .map(|m| FailoverMarkerAttributes {
                domain_id: Some(m.domain_id),
                failover_version: Some(m.failover_version),
                creation_time: Some(now),
            })
            .collect())
    }
}

/// `history.Client.NotifyFailoverMarkers` for a single-host deployment: tokens are
/// handed to this process's coordinator (`ReceiveFailoverMarkers`).
#[derive(Default)]
pub struct LocalFailoverHistoryClient {
    coordinator: OnceLock<Arc<dyn Coordinator>>,
}

impl LocalFailoverHistoryClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    /// Late binding: the coordinator needs the client at construction time.
    pub fn bind(&self, coordinator: Arc<dyn Coordinator>) {
        let _ = self.coordinator.set(coordinator);
    }
}

#[async_trait]
impl HistoryClient for LocalFailoverHistoryClient {
    async fn notify_failover_markers(&self, tokens: Vec<FailoverMarkerToken>, _timeout: Duration) -> Result<(), String> {
        let coordinator = self.coordinator.get().ok_or_else(|| "failover coordinator is not bound".to_string())?;
        for t in tokens {
            coordinator.receive_failover_markers(t.shard_ids, t.failover_marker);
        }
        Ok(())
    }
}

/// What the environment supplies to build the failover components.
pub struct FailoverDeps {
    pub domain_manager: Arc<dyn DomainManager>,
    /// `PersistenceDomainManager` (wiring/resources/failover_domain_manager.rs) or
    /// `SqlFailoverDomainManager` (wiring/persistence/domain_dlq.rs).
    pub time_source: Arc<dyn TimeSource>,
    pub domain_cache: Arc<dyn crate::failover::DomainCache>,
    pub config: Arc<dyn FailoverConfig>,
    /// `MetricsClientImpl`.
    pub metrics: Arc<dyn FailoverMetrics>,
    /// Defaults to [`LocalFailoverHistoryClient`] bound to the coordinator.
    pub history_client: Option<Arc<dyn HistoryClient>>,
}

pub struct FailoverParts {
    pub coordinator: Arc<CoordinatorImpl>,
}

/// Go `failover.NewCoordinator` as built by the history resource.
pub fn build_failover_coordinator(deps: FailoverDeps) -> FailoverParts {
    let local = LocalFailoverHistoryClient::new();
    let history_client: Arc<dyn HistoryClient> = match deps.history_client {
        Some(c) => c,
        None => local.clone(),
    };
    let coordinator = CoordinatorImpl::new(NewCoordinatorParams {
        domain_manager: deps.domain_manager,
        history_client,
        time_source: deps.time_source,
        domain_cache: deps.domain_cache,
        config: deps.config,
        metrics: deps.metrics,
    });
    local.bind(coordinator.clone());
    FailoverParts { coordinator }
}

/// Go `failover.NewMarkerNotifier` for one shard (started by the shard controller
/// together with the engine).
pub fn build_marker_notifier(
    shard: Arc<ShardContext>,
    domains: Arc<HistoryDomainCache>,
    current_cluster: String,
    config: Arc<dyn FailoverConfig>,
    coordinator: Arc<dyn Coordinator>,
    metrics: Arc<dyn FailoverMetrics>,
) -> Arc<dyn MarkerNotifier> {
    let adapter = Arc::new(FailoverShardAdapter { shard, domains, current_cluster });
    MarkerNotifierImpl::new(adapter, config, coordinator, metrics)
}
