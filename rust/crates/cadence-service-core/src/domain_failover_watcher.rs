//! Go `common/domain/failover_watcher.go`: force-completes graceful failovers
//! whose `FailoverEndTime` has passed. Started by the admin handler only when
//! `system.enableGracefulFailover` (default true) is set at startup.

use crate::matching_metrics::{MetricEvent, MetricKind, MetricSink, MetricValue};
use cadence_common::cluster::Metadata;
use cadence_persistence::FrontendDomainManager;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// Go `metrics.GracefulFailoverFailure` metric name.
pub const GRACEFUL_FAILOVER_FAILURES: &str = "graceful_failover_failures";
/// Go `metrics.DomainFailoverScope` operation tag.
pub const DOMAIN_FAILOVER_OPERATION: &str = "DomainFailover";

/// Go `updateDomainRetryInitialInterval/Coefficient/MaxRetry`.
const RETRY_INITIAL: Duration = Duration::from_millis(50);
const RETRY_MAX_ATTEMPTS: u32 = 2;

/// One watcher pass (Go `handleFailoverTimeout` over `GetAllDomain`).
/// Returns the names of domains force-completed.
pub fn run_pass(
    manager: &dyn FrontendDomainManager,
    cluster: &Metadata,
    sink: Option<&Arc<dyn MetricSink>>,
    now_nanos: i64,
) -> Vec<String> {
    let mut completed = Vec::new();
    for d in manager.pending_active_domains() {
        if now_nanos <= d.failover_end_time_nanos {
            continue;
        }
        let mut attempt = 0;
        let result = loop {
            match manager.clean_pending_active(&d.id, d.failover_version) {
                Err(e) if attempt < RETRY_MAX_ATTEMPTS => {
                    attempt += 1;
                    let _ = e;
                    std::thread::sleep(RETRY_INITIAL * 2u32.pow(attempt));
                }
                other => break other,
            }
        };
        match result {
            Err(e) => {
                tracing::error!(domain_id = %d.id, domain = %d.name, error = %e,
                    "Failed to update pending-active domain to active");
            }
            Ok(false) => {}
            Ok(true) => {
                let source = cluster
                    .cluster_name_for_failover_version(d.previous_failover_version)
                    .map(str::to_owned)
                    .unwrap_or_else(|_| "unknown".to_owned());
                tracing::warn!(domain_id = %d.id, domain = %d.name, prev_active_cluster = %source,
                    active_cluster = %d.active_cluster_name,
                    "Graceful failover force-completed by watcher: failover timeout exceeded before all shards reported");
                if let Some(sink) = sink {
                    sink.record(MetricEvent {
                        name: GRACEFUL_FAILOVER_FAILURES,
                        kind: MetricKind::Counter,
                        value: MetricValue::Count(1),
                        tags: BTreeMap::from([
                            ("operation".to_owned(), DOMAIN_FAILOVER_OPERATION.to_owned()),
                            ("domain".to_owned(), d.name.clone()),
                        ]),
                        histogram: None,
                    });
                }
                completed.push(d.name);
            }
        }
    }
    completed
}

/// Spawn the refresh loop; `interval`/`jitter` are re-read each cycle.
pub fn spawn(
    manager: Arc<dyn FrontendDomainManager>,
    cluster: Metadata,
    sink: Option<Arc<dyn MetricSink>>,
    interval: Arc<dyn Fn() -> (Duration, f64) + Send + Sync>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            let (base, coefficient) = interval();
            let wait = cadence_common::jitter::jit_duration(&mut rand::thread_rng(), base, coefficient);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                }
                _ = tokio::time::sleep(wait) => {}
            }
            let (m, c, s) = (manager.clone(), cluster.clone(), sink.clone());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or_default();
            let _ = tokio::task::spawn_blocking(move || run_pass(m.as_ref(), &c, s.as_ref(), now)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::InMemoryDomainStore;

    fn add(store: &InMemoryDomainStore, name: &str, end: i64) {
        let info = cadence_api::types::DomainInfo {
            name: name.to_owned(),
            uuid: name.to_owned(),
            ..Default::default()
        };
        store
            .create_with_version(
                info,
                Default::default(),
                Default::default(),
                true,
                1,
            )
            .expect("create");
        let mut r = store.record_by_name(name).expect("record");
        r.failover_end_time = Some(end);
        store.restore_record(r);
    }

    #[test]
    fn only_expired_domains_are_force_completed() {
        let store = InMemoryDomainStore::default();
        add(&store, "late", 100);
        add(&store, "early", 10_000);
        let done = run_pass(&store, &Metadata::default(), None, 5_000);
        assert_eq!(done, vec!["late"]);
        let pending = store.pending_active_list();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].name, "early");
        assert!(run_pass(&store, &Metadata::default(), None, 5_000).is_empty());
    }
}
