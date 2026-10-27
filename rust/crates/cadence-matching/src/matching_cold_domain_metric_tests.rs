use super::*;
use crate::matching_domain_cache::{
    DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError, MatchingDomainCache,
};
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DOMAIN_ID: &str = "00000000-0000-0000-0000-000000000042";
const DOMAIN_NAME: &str = "created-after-cache-scan";

#[derive(Default)]
struct ManualClock(AtomicU64);

impl crate::matching_domain_cache::DomainCacheClock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::SeqCst))
    }

    fn wait(&self, _duration: Duration, _stopped: &std::sync::atomic::AtomicBool) {}
}

/// Starts empty so the cache's initial scan cannot know the domain. The test
/// publishes it afterward, as when Frontend registers a domain after Matching
/// starts, and records the read-through calls made by the production cache.
#[derive(Default)]
struct LateDomainSource {
    domain: Mutex<Option<MatchingDomainMetadata>>,
    calls: Mutex<Vec<String>>,
}

impl DomainMetadataSource for LateDomainSource {
    fn metadata_version(&self, _timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        // Refresh includes rows strictly below the metadata notification bound.
        Ok(2)
    }

    fn list_domains(
        &self,
        _page_size: usize,
        _next_page_token: Option<&[u8]>,
        _timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        Ok(DomainMetadataPage {
            domains: self.domain.lock().unwrap().clone().into_iter().collect(),
            next_page_token: None,
        })
    }

    fn get_domain(
        &self,
        _name: Option<&str>,
        id: Option<&str>,
        _timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        self.calls
            .lock()
            .unwrap()
            .push(id.unwrap_or_default().to_owned());
        self.domain
            .lock()
            .unwrap()
            .clone()
            .filter(|domain| Some(domain.info.uuid.as_str()) == id)
            .ok_or_else(|| DomainMetadataSourceError::permanent("domain not found"))
    }
}

fn domain_metadata() -> MatchingDomainMetadata {
    MatchingDomainMetadata {
        info: cadence_api::types::DomainInfo {
            name: DOMAIN_NAME.into(),
            uuid: DOMAIN_ID.into(),
            ..Default::default()
        },
        configuration: Default::default(),
        replication: Default::default(),
        is_global_domain: false,
        archival_bucket: String::new(),
        status_raw: 0,
        archival_status_raw: 0,
        history_archival_status_raw: 0,
        visibility_archival_status_raw: 0,
        config_version: 1,
        notification_version: 1,
        failover_notification_version: 1,
        failover_version: 1,
        previous_failover_version: 0,
        failover_end_time_nanos: None,
        last_updated_time_nanos: 0,
        bad_binaries: None,
        isolation_groups: None,
        async_workflow_config: None,
        active_clusters_config: None,
        decoded_bad_binaries: None,
        decoded_isolation_groups: None,
        decoded_async_workflow_config: None,
        config_decode_error: None,
    }
}

#[tokio::test]
async fn add_decision_task_metric_resolves_domain_created_after_cache_scan() {
    use crate::matching_metrics::{RecordingMetricSink, CADENCE_REQUESTS_PER_TASK_LIST};

    let source = Arc::new(LateDomainSource::default());
    let clock = Arc::new(ManualClock::default());
    let cache = Arc::new(MatchingDomainCache::with_clock(source.clone(), clock.clone()));
    cache.refresh_domains().expect("initial empty cache scan");
    assert_eq!(cache.cached_domain_name(DOMAIN_ID), None);

    *source.domain.lock().unwrap() = Some(domain_metadata());
    // A cold lookup immediately after a successful scan is rate limited to
    // avoid duplicate refreshes. Advance beyond that production interval to
    // model a domain registered later while keeping the test deterministic.
    clock.0.store(1, Ordering::SeqCst);
    let sink = Arc::new(RecordingMetricSink::default());
    let matching = Matching::new(crate::handlers::AppState::default())
        .with_metrics(sink.clone())
        .with_domain_cache(cache.clone());
    let request = m::AddDecisionTaskRequest {
        domain_id: DOMAIN_ID.into(),
        ..Default::default()
    };

    // The empty task list is rejected after MatchingApi has resolved the domain
    // and emitted its inbound metric. Calling the real trait method exercises
    // that ordering and the cache resolver installed by with_domain_cache.
    let _ = m::matching_api_server::MatchingApi::add_decision_task(
        &matching,
        Request::new(request),
    )
    .await;

    let event = sink
        .events()
        .into_iter()
        .find(|event| event.name == CADENCE_REQUESTS_PER_TASK_LIST)
        .expect("AddDecisionTask request metric emitted");
    assert_eq!(
        event.tags.get(crate::matching_metrics::DOMAIN_TAG).map(String::as_str),
        Some(DOMAIN_NAME),
        "the metric must use the canonical name learned by cache read-through"
    );
    assert_eq!(&*source.calls.lock().unwrap(), &[DOMAIN_ID]);

}
