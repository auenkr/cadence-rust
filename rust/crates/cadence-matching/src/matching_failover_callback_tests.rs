//! Exercise the callbacks actually registered by MatchingRuntime, including
//! catch-up and delivery to a blocked registry poll rather than a copied selector.
use super::*;
use crate::matching_domain_cache::{
    DomainCacheClock, DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError,
    MatchingDomainCache,
};
use cadence_api::types::{
    ActiveClusters, ClusterAttributeScope, DomainConfiguration, DomainInfo,
    DomainReplicationConfiguration,
};
use cadence_persistence::{
    matching_blob::BlobEncoding,
    matching_domain_metadata::MatchingDomainMetadata,
    matching_sqlite::{SqliteTaskStore, TaskListKey},
};
use cadence_proto::api as p;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

struct Clock(AtomicU64);
impl DomainCacheClock for Clock {
    fn now(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
    fn wait(&self, _duration: Duration, stopped: &AtomicBool) {
        // Only explicit refreshes advance this fixture. Stop still joins the
        // real cache maintenance thread, without a busy loop.
        while !stopped.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
struct Source(Mutex<MatchingDomainMetadata>);
impl DomainMetadataSource for Source {
    fn metadata_version(&self, _: Duration) -> Result<i64, DomainMetadataSourceError> {
        Ok(self.0.lock().unwrap().notification_version + 1)
    }
    fn list_domains(
        &self,
        _: usize,
        _: Option<&[u8]>,
        _: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        Ok(DomainMetadataPage {
            domains: vec![self.0.lock().unwrap().clone()],
            next_page_token: None,
        })
    }
    fn get_domain(
        &self,
        _: Option<&str>,
        _: Option<&str>,
        _: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        Ok(self.0.lock().unwrap().clone())
    }
}
fn domain() -> MatchingDomainMetadata {
    MatchingDomainMetadata {
        info: DomainInfo {
            name: "callback-domain".into(),
            uuid: uuid::Uuid::from_u128(1).to_string(),
            ..Default::default()
        },
        configuration: DomainConfiguration::default(),
        replication: DomainReplicationConfiguration::default(),
        is_global_domain: true,
        archival_bucket: String::new(),
        status_raw: 0,
        archival_status_raw: 0,
        history_archival_status_raw: 0,
        visibility_archival_status_raw: 0,
        config_version: 1,
        notification_version: 1,
        failover_notification_version: 10,
        failover_version: 10,
        previous_failover_version: -1,
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
async fn registered_failover_callbacks_preserve_catchup_and_release_only_eligible_updates() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(directory.path().join("callback.db"), BlobEncoding::ThriftRW)
            .unwrap(),
    );
    let state = crate::handlers::AppState::default();
    let domains = state.domains.clone();
    let history = crate::matching_history::MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        1024,
    );
    let mut runtime = MatchingRuntime::with_durable_queues(
        state,
        store,
        history,
        NonZeroU32::new(100).unwrap(),
        Duration::from_secs(30),
        Arc::new(|| Duration::ZERO),
    );
    let registry = runtime.registry.as_ref().unwrap().clone();
    let key = TaskListKey {
        domain_id: uuid::Uuid::from_u128(1),
        name: "callback-poll".into(),
        task_type: 0,
    };
    let owner = registry.get(key.clone(), 0, 1).await.unwrap();
    let dispatcher = owner.dispatcher.clone();
    let mut poll = tokio::spawn(async move {
        dispatcher
            .poll_decision(
                p::PollForDecisionTaskRequest::default(),
                Duration::from_secs(30),
            )
            .await
    });
    // Poll before registration so an erroneous catch-up release is observable.
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut poll)
        .await
        .is_err());
    let source = Arc::new(Source(Mutex::new(domain())));
    let clock = Arc::new(Clock(AtomicU64::new(0)));
    let cache = Arc::new(MatchingDomainCache::with_clock(
        source.clone(),
        clock.clone(),
    ));
    MatchingRuntime::start_domain_cache(
        domains,
        cache.clone(),
        Some(registry.clone()),
        Arc::new(Mutex::new(None)),
    )
    .await
    .unwrap();
    runtime.domain_cache = Some(cache.clone());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut poll)
            .await
            .is_err(),
        "registration catch-up must only establish the baseline"
    );

    // Each case reaches the registered changed callback with a newer metadata
    // version, but local, active-active and stale failovers do not disconnect.
    for (global, active_active, version) in
        [(false, false, 11), (true, true, 12), (true, false, 11)]
    {
        {
            let mut record = source.0.lock().unwrap();
            record.notification_version += 1;
            record.is_global_domain = global;
            record.failover_notification_version = version;
            record.replication.active_clusters = active_active.then(|| ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: Some(HashMap::from([(
                    "region".into(),
                    ClusterAttributeScope::default(),
                )])),
            });
        }
        clock.0.fetch_add(1, Ordering::SeqCst);
        cache.refresh_domains().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut poll)
                .await
                .is_err(),
            "filtered change unexpectedly released the poll"
        );
    }
    {
        let mut record = source.0.lock().unwrap();
        record.notification_version += 1;
        record.failover_notification_version = 13;
    }
    clock.0.fetch_add(1, Ordering::SeqCst);
    cache.refresh_domains().unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .expect("registered failover callback did not release the blocked poll")
        .unwrap()
        .unwrap();
    assert!(response.task_token.is_empty());
    let same_owner = registry.get(key, 0, 2).await.unwrap();
    assert!(
        Arc::ptr_eq(&same_owner.queue, &owner.queue),
        "failover releases polls without unloading the manager"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn registered_failover_callback_reaches_native_registry_attached_after_catchup() {
    use cadence_common::go_string::GoString;
    use cadence_persistence::matching_native_task_store::TaskListKeyBytes;

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(directory.path().join("native-callback.db"), BlobEncoding::ThriftRW)
            .unwrap(),
    );
    let history = crate::matching_history::MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        Duration::from_secs(1),
        1024,
    );
    let registry = crate::matching_registry_native::NativeQueueRegistry::new(
        store,
        history,
        NonZeroU32::new(100).unwrap(),
    );
    let key = TaskListKeyBytes {
        domain_id: uuid::Uuid::from_u128(1),
        name: GoString::from(&b"callback-\xff"[..]),
        task_type: 0,
    };
    let owner = registry.get(key.clone(), 0, 1).await.unwrap();
    let dispatcher = owner.dispatcher.clone();
    let mut poll = tokio::spawn(async move {
        dispatcher
            .poll_decision(
                cadence_proto::native::api::PollForDecisionTaskRequest::default(),
                Duration::from_secs(30),
            )
            .await
    });
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut poll)
        .await
        .is_err());
    let source = Arc::new(Source(Mutex::new(domain())));
    let clock = Arc::new(Clock(AtomicU64::new(0)));
    let cache = Arc::new(MatchingDomainCache::with_clock(source.clone(), clock.clone()));
    let slot = Arc::new(Mutex::new(None));
    MatchingRuntime::start_domain_cache(
        crate::handlers::AppState::default().domains,
        cache.clone(),
        None,
        slot.clone(),
    )
    .await
    .unwrap();
    // Production attaches its native registry after the cache starts. The
    // registered closure must consult the shared slot on every notification.
    *slot.lock().unwrap() = Some(registry.clone());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut poll)
        .await
        .is_err());
    for (global, active_active, version) in
        [(false, false, 11), (true, true, 12), (true, false, 11)]
    {
        {
            let mut record = source.0.lock().unwrap();
            record.notification_version += 1;
            record.is_global_domain = global;
            record.failover_notification_version = version;
            record.replication.active_clusters = active_active.then(|| ActiveClusters {
                active_clusters_by_region: None,
                active_clusters_by_cluster_attribute: Some(HashMap::from([(
                    "region".into(),
                    ClusterAttributeScope::default(),
                )])),
            });
        }
        clock.0.fetch_add(1, Ordering::SeqCst);
        cache.refresh_domains().unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut poll)
            .await
            .is_err(), "filtered change released native poll");
    }
    {
        let mut record = source.0.lock().unwrap();
        record.notification_version += 1;
        record.failover_notification_version = 13;
    }
    clock.0.fetch_add(1, Ordering::SeqCst);
    cache.refresh_domains().unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .expect("registered callback did not release native poll")
        .unwrap()
        .unwrap();
    assert!(response.task_token.is_empty());
    let same_owner = registry.get(key, 0, 2).await.unwrap();
    assert!(Arc::ptr_eq(&same_owner.queue, &owner.queue));
    cache.stop();
    registry.shutdown(crate::grpc_matching::matching_now()).await.unwrap();
}
