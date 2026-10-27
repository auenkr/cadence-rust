use super::*;
use crate::matching_domain_cache::{
    DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError, MatchingDomainCache,
};
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(prost::Message)]
struct DomainLookupStatusDetails {
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

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
    failure: Mutex<Option<DomainMetadataSourceError>>,
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
        name: Option<&str>,
        id: Option<&str>,
        _timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        self.calls.lock().unwrap().push(
            name.map(|name| format!("name:{name}"))
                .or_else(|| id.map(|id| format!("id:{id}")))
                .unwrap_or_default(),
        );
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        self.domain
            .lock()
            .unwrap()
            .clone()
            .filter(|domain| {
                name.map(|name| domain.info.name == name).unwrap_or(true)
                    && id.map(|id| domain.info.uuid == id).unwrap_or(true)
            })
            .ok_or_else(|| {
                DomainMetadataSourceError::permanent("domain not found")
                    .with_class(crate::matching_domain_cache::DomainErrorClass::NotFound)
            })
    }
}

#[tokio::test]
async fn name_lookup_preserves_domain_cache_error_classification() {
    use crate::matching_domain_cache::DomainErrorClass;
    use prost::Message;

    for (error, code, message, detail_type) in [
        (
            DomainMetadataSourceError::permanent("Domain missing")
                .with_class(DomainErrorClass::NotFound),
            tonic::Code::NotFound,
            "Domain missing",
            Some("type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError"),
        ),
        (
            DomainMetadataSourceError::permanent("storage unavailable"),
            tonic::Code::Internal,
            "storage unavailable",
            None,
        ),
    ] {
        let source = Arc::new(LateDomainSource::default());
        *source.failure.lock().unwrap() = Some(error);
        let cache = Arc::new(MatchingDomainCache::with_clock(
            source,
            Arc::new(ManualClock::default()),
        ));
        let matching = Matching::new(crate::handlers::AppState::default())
            .with_domain_cache(cache);
        let status = m::matching_api_server::MatchingApi::list_task_list_partitions(
            &matching,
            Request::new(m::ListTaskListPartitionsRequest {
                domain: DOMAIN_NAME.into(),
                task_list: Some(p::TaskList {
                    name: "queue".into(),
                    ..Default::default()
                }),
            }),
        )
        .await
        .expect_err("cold domain source error must reach the caller");
        assert_eq!(status.code(), code);
        assert_eq!(status.message(), message);
        let details = DomainLookupStatusDetails::decode(status.details()).unwrap();
        assert_eq!(
            details.details.first().map(|detail| detail.type_url.as_str()),
            detail_type,
        );
    }
}

#[tokio::test]
async fn empty_name_lookup_is_bad_request() {
    let matching = Matching::new(crate::handlers::AppState::default())
        .with_domain_cache(Arc::new(MatchingDomainCache::with_clock(
            Arc::new(LateDomainSource::default()),
            Arc::new(ManualClock::default()),
        )));
    let status = m::matching_api_server::MatchingApi::list_task_list_partitions(
        &matching,
        Request::new(m::ListTaskListPartitionsRequest {
            domain: String::new(),
            task_list: Some(p::TaskList {
                name: "queue".into(),
                ..Default::default()
            }),
        }),
    )
    .await
    .expect_err("empty domain name is rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), "Domain name is empty");
}

#[tokio::test]
async fn name_based_listing_handlers_read_through_cold_domain_cache() {
    for handler in ["GetTaskListsByDomain", "ListTaskListPartitions"] {
        let source = Arc::new(LateDomainSource::default());
        let clock = Arc::new(ManualClock::default());
        let cache = Arc::new(MatchingDomainCache::with_clock(
            source.clone(),
            clock.clone(),
        ));
        cache.refresh_domains().expect("initial empty cache scan");
        *source.domain.lock().unwrap() = Some(domain_metadata());
        // Go and Rust suppress another scan within one second of a successful
        // refresh. Model registration after that interval without sleeping.
        clock.0.store(1, Ordering::SeqCst);
        let matching = Matching::new(crate::handlers::AppState::default())
            .with_domain_cache(cache.clone());

        let call = if handler == "GetTaskListsByDomain" {
            let response = m::matching_api_server::MatchingApi::get_task_lists_by_domain(
                &matching,
                Request::new(m::GetTaskListsByDomainRequest {
                    domain: DOMAIN_NAME.into(),
                }),
            )
            .await
            .expect("cold name lookup succeeds")
            .into_inner();
            assert_eq!(response, m::GetTaskListsByDomainResponse::default());
            "name:created-after-cache-scan"
        } else {
            let _response = m::matching_api_server::MatchingApi::list_task_list_partitions(
                &matching,
                Request::new(m::ListTaskListPartitionsRequest {
                    domain: DOMAIN_NAME.into(),
                    task_list: Some(p::TaskList {
                        name: "queue".into(),
                        kind: 0,
                        ..Default::default()
                    }),
                }),
            )
            .await
            .expect("cold name lookup succeeds")
            .into_inner();
            "name:created-after-cache-scan"
        };
        assert_eq!(&*source.calls.lock().unwrap(), &[call]);
        assert_eq!(matching.domain_name(DOMAIN_ID).unwrap(), DOMAIN_NAME);
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
    assert_eq!(&*source.calls.lock().unwrap(), &[format!("id:{DOMAIN_ID}")]);
    assert_eq!(
        matching.domain_name(DOMAIN_ID).expect("cold domain published"),
        DOMAIN_NAME,
        "cold discoveries must reach the domain view used by quotas and workflow handlers"
    );
}

#[tokio::test]
async fn add_decision_task_captures_domain_resolver_panic_from_blocking_task() {
    const PANIC_TEXT: &str = "resolver exploded";

    let mut matching = Matching::new(crate::handlers::AppState::default());
    matching.domain_name_resolver = Some(Arc::new(|_| panic!("{PANIC_TEXT}")));
    let request = m::AddDecisionTaskRequest {
        domain_id: DOMAIN_ID.into(),
        ..Default::default()
    };

    let error = m::matching_api_server::MatchingApi::add_decision_task(
        &matching,
        Request::new(request),
    )
    .await
    .expect_err("resolver panic must be returned as a captured handler error");

    assert_eq!(error.code(), tonic::Code::Unknown);
    assert_eq!(
        error.message(),
        r#"panic object is not error: "resolver exploded""#
    );
    assert!(crate::matching_panic::is_captured_panic(&error));
}

#[tokio::test]
async fn add_decision_task_swallows_ordinary_domain_resolver_failure() {
    let mut matching = Matching::new(crate::handlers::AppState::default());
    matching.domain_name_resolver = Some(Arc::new(|_| None));
    let request = m::AddDecisionTaskRequest {
        domain_id: DOMAIN_ID.into(),
        ..Default::default()
    };

    let error = m::matching_api_server::MatchingApi::add_decision_task(
        &matching,
        Request::new(request),
    )
    .await
    .expect_err("an unknown domain should reach ordinary domain validation");

    assert_eq!(error.code(), tonic::Code::NotFound);
    assert!(!crate::matching_panic::is_captured_panic(&error));
}
