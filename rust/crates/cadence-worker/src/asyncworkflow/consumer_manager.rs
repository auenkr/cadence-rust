//! Consumer reconciliation from Go `asyncworkflow/async_workflow_consumer_manager.go`.
//!
//! Go re-reads every domain on a refresh interval and makes the set of running
//! consumers match the enabled async-workflow queues. The queue provider, the
//! consumer lifecycle and the metrics are not ported yet. This module takes
//! them as closures so the sequencing (one start attempt per domain, shared
//! queues counted once per domain, failed starts skipped, unused consumers
//! stopped) is exact.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// Go `defaultRefreshInterval`.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Go `defaultShutdownTimeout`.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The queue a domain's async-workflow config resolves to, after the queue
/// provider has produced its ID (Go `queue.ID()`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainQueue {
    pub queue_id: String,
    pub enabled: bool,
}

/// Go `refreshConsumers`. `domains` holds one entry per domain, with `None`
/// for a domain that has no async config or whose queue lookup failed. Those
/// domains are skipped, as Go does. `start` creates and starts a consumer for a
/// queue ID; an error skips that domain and leaves the queue unregistered, so a
/// later domain on the same queue retries it. `stop` stops a consumer. Returns
/// the number of active consumers afterwards.
pub fn refresh_consumers<E>(
    domains: &[Option<DomainQueue>],
    active: &mut BTreeSet<String>,
    mut start: impl FnMut(&str) -> Result<(), E>,
    mut stop: impl FnMut(&str),
) -> usize {
    let mut ref_counts: BTreeMap<String, usize> = BTreeMap::new();
    for domain in domains.iter().flatten() {
        if !domain.enabled {
            // Consumers for queues only disabled domains use are stopped below.
            continue;
        }
        if active.contains(&domain.queue_id) {
            *ref_counts.entry(domain.queue_id.clone()).or_default() += 1;
            continue;
        }
        if start(&domain.queue_id).is_err() {
            continue;
        }
        active.insert(domain.queue_id.clone());
        *ref_counts.entry(domain.queue_id.clone()).or_default() += 1;
    }

    let unneeded: Vec<String> = active
        .iter()
        .filter(|queue_id| ref_counts.get(*queue_id).copied().unwrap_or(0) == 0)
        .cloned()
        .collect();
    for queue_id in unneeded {
        stop(&queue_id);
        active.remove(&queue_id);
    }
    active.len()
}

/// Go `stopConsumers`: stops every active consumer. Returns whether any ran,
/// since Go skips the metric update when none were active.
pub fn stop_all(active: &mut BTreeSet<String>, mut stop: impl FnMut(&str)) -> bool {
    if active.is_empty() {
        return false;
    }
    for queue_id in std::mem::take(active) {
        stop(&queue_id);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn domain(queue_id: &str, enabled: bool) -> Option<DomainQueue> {
        Some(DomainQueue {
            queue_id: queue_id.to_owned(),
            enabled,
        })
    }

    #[test]
    fn preserves_go_refresh_and_shutdown_intervals() {
        assert_eq!(DEFAULT_REFRESH_INTERVAL, Duration::from_secs(60));
        assert_eq!(DEFAULT_SHUTDOWN_TIMEOUT, Duration::from_secs(5));
    }

    #[test]
    fn starts_one_consumer_per_enabled_queue_and_shares_it_across_domains() {
        let mut active = BTreeSet::new();
        let mut started = Vec::new();
        let count = refresh_consumers(
            &[domain("q1", true), domain("q1", true), domain("q2", true)],
            &mut active,
            |id| {
                started.push(id.to_owned());
                Ok::<(), ()>(())
            },
            |_| panic!("no consumer is unneeded"),
        );
        assert_eq!(count, 2);
        assert_eq!(started, vec!["q1", "q2"]);
    }

    #[test]
    fn keeps_running_consumers_without_starting_them_again() {
        let mut active = BTreeSet::from(["q1".to_owned()]);
        let count = refresh_consumers(
            &[domain("q1", true)],
            &mut active,
            |_| -> Result<(), ()> { panic!("q1 is already running") },
            |_| panic!("q1 is still needed"),
        );
        assert_eq!(count, 1);
    }

    #[test]
    fn stops_consumers_whose_domains_disabled_or_removed() {
        let mut active = BTreeSet::from(["q1".to_owned(), "q2".to_owned()]);
        let mut stopped = Vec::new();
        let count = refresh_consumers(
            &[domain("q1", false), None],
            &mut active,
            |_| -> Result<(), ()> { panic!("nothing is enabled") },
            |id| stopped.push(id.to_owned()),
        );
        assert_eq!(count, 0);
        assert_eq!(stopped, vec!["q1", "q2"]);
        assert!(active.is_empty());
    }

    #[test]
    fn failed_start_is_skipped_and_retried_by_the_next_domain_on_that_queue() {
        let mut active = BTreeSet::new();
        let mut attempts = 0;
        let count = refresh_consumers(
            &[domain("q1", true), domain("q1", true)],
            &mut active,
            |_| {
                attempts += 1;
                if attempts == 1 {
                    Err("create failed")
                } else {
                    Ok(())
                }
            },
            |_| panic!("no consumer was registered"),
        );
        assert_eq!(attempts, 2);
        assert_eq!(count, 1);
        assert!(active.contains("q1"));
    }

    #[test]
    fn stop_all_reports_whether_anything_was_active() {
        let mut active = BTreeSet::new();
        assert!(!stop_all(&mut active, |_| panic!("nothing to stop")));

        let mut active = BTreeSet::from(["q1".to_owned(), "q2".to_owned()]);
        let mut stopped = Vec::new();
        assert!(stop_all(&mut active, |id| stopped.push(id.to_owned())));
        assert_eq!(stopped, vec!["q1", "q2"]);
        assert!(active.is_empty());
    }
}
