//! Production owner for Matching's handler, durable queue registry and
//! background maintenance task.
//!
//! Persistence, History and dynamic configuration enter through explicit
//! dependencies. The production domain cache can be attached through the
//! cache-aware constructors; membership discovery/eviction remains a separate
//! adapter, so this owner does not represent complete Go Matching parity.
use crate::{admin::DynamicConfigOverlay, grpc_matching::Matching};
use cadence_persistence::matching_task_store::MatchingTaskStore;
use std::{
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

type ShardProcessLookup = Arc<
    dyn Fn(
            String,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            (),
                            crate::matching_shard_distributor::GetShardProcessError,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

fn shard_task_list_ownership_check(
    membership: crate::matching_membership::Membership,
    percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
    exclude_short_lived: Arc<dyn Fn() -> bool + Send + Sync>,
    draining: tokio::sync::watch::Receiver<bool>,
    lookup: ShardProcessLookup,
) -> crate::grpc_matching::TaskListOwnershipCheck {
    Arc::new(move |name, cached| {
        let membership = membership.clone();
        let percentage = percentage_onboarded.clone();
        let exclude = exclude_short_lived.clone();
        let draining = draining.clone();
        let lookup = lookup.clone();
        Box::pin(async move {
            if *draining.borrow() {
                return Err(membership.task_list_not_owned_by(&name, "not known"));
            }
            // Match Go's signed-to-uint64 conversion, including negative
            // dynamic values that wrap to a large eligible percentage.
            let excluded = crate::matching_shard_routing::task_list_excluded_from_shard_distributor(
                &name,
                percentage() as u64,
                exclude(),
            );
            if excluded {
                if cached {
                    return Ok(());
                }
                return membership.check_task_list(&name);
            }
            match lookup(name.clone()).await {
                Ok(()) => Ok(()),
                Err(crate::matching_shard_distributor::GetShardProcessError::NotFound(_)) => {
                    Err(membership.task_list_not_owned_by(&name, "not known"))
                }
                Err(crate::matching_shard_distributor::GetShardProcessError::Heartbeat(error)) => {
                    // Go wraps non-not-found executor errors as a generic
                    // internal error; do not surface gRPC Unavailable as a
                    // service-busy/YARPC transport class.
                    Err(tonic::Status::internal(format!(
                        "failed to lookup ownership in SD: {error}"
                    )))
                }
            }
        })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), tonic::Status>> + Send>,
            >
    })
}
use tokio::{sync::oneshot, task::JoinHandle};

pub type DrainDuration = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type EvictFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
pub type EvictSelf = Arc<dyn Fn() -> EvictFuture + Send + Sync>;

async fn drain_service<F, Fut>(
    evict_self: &mut Option<EvictSelf>,
    drain_duration: &DrainDuration,
    wait: F,
) -> anyhow::Result<()>
where
    F: FnOnce(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let evict_result = if let Some(evict) = evict_self.take() {
        evict().await
    } else {
        Ok(())
    };
    let drain = drain_duration();
    if !drain.is_zero() {
        wait(drain).await;
    }
    evict_result
}

/// Owns one production Matching handler and all queue lifecycle tasks it
/// starts. shutdown is required on listener success and failure paths.
pub struct MatchingRuntime {
    matching: Matching,
    registry: Option<crate::matching_registry::QueueRegistry>,
    maintenance_stop: Option<oneshot::Sender<()>>,
    maintenance: Option<JoinHandle<Result<(), tonic::Status>>>,
    membership_maintenance_stop: Option<oneshot::Sender<()>>,
    membership_maintenance: Option<JoinHandle<Result<(), tonic::Status>>>,
    domain_cache: Option<Arc<crate::matching_domain_cache::MatchingDomainCache>>,
    dynamic_config_watch: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    shard_distributor_executor: Option<crate::matching_shard_distributor::ShardDistributorExecutor>,
    shard_distributor_spectator: Option<crate::matching_shard_distributor::TonicSpectator>,
    shard_distributor_drain: Option<tokio::sync::watch::Sender<bool>>,
    shard_distributor_peer_cleanup: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    drain_duration: DrainDuration,
    evict_self: Option<EvictSelf>,
    drained: bool,
}

impl MatchingRuntime {
    pub fn in_memory(state: crate::handlers::AppState, drain_duration: DrainDuration) -> Self {
        Self {
            matching: Matching::new(state),
            registry: None,
            maintenance_stop: None,
            maintenance: None,
            membership_maintenance_stop: None,
            membership_maintenance: None,
            domain_cache: None,
            dynamic_config_watch: None,
            shard_distributor_executor: None,
            shard_distributor_spectator: None,
            shard_distributor_drain: None,
            shard_distributor_peer_cleanup: None,
            drain_duration,
            evict_self: None,
            drained: false,
        }
    }

    /// Construct the currently supported durable SQLite runtime. History is
    /// injected so the owner never assumes a loopback address implicitly.
    pub fn with_durable_queues(
        state: crate::handlers::AppState,
        store: Arc<dyn MatchingTaskStore>,
        history: crate::matching_history::MatchingHistoryClient,
        task_id_range: NonZeroU32,
        poll_expiration: Duration,
        drain_duration: DrainDuration,
    ) -> Self {
        let registry = crate::matching_registry::QueueRegistry::new(store, history, task_id_range);
        let matching = Matching::with_durable_queues(state, registry.clone(), poll_expiration);
        let (stop, stopped) = oneshot::channel();
        let maintenance_registry = registry.clone();
        let maintenance = tokio::spawn(async move {
            maintenance_registry
                .maintain_until(Duration::from_secs(10), async {
                    let _ = stopped.await;
                })
                .await
        });
        Self {
            matching,
            registry: Some(registry),
            maintenance_stop: Some(stop),
            maintenance: Some(maintenance),
            membership_maintenance_stop: None,
            membership_maintenance: None,
            domain_cache: None,
            dynamic_config_watch: None,
            shard_distributor_executor: None,
            shard_distributor_spectator: None,
            shard_distributor_drain: None,
            shard_distributor_peer_cleanup: None,
            drain_duration,
            evict_self: None,
            drained: false,
        }
    }

    pub fn matching(&self) -> &Matching {
        &self.matching
    }

    /// Attach shard-distributor ownership checks to both the RPC acquisition
    /// path and registry publication/unload lifecycle. Runtime construction
    /// owns these dependencies so handlers and processors share one policy.
    pub fn with_task_list_ownership(
        mut self,
        check: crate::grpc_matching::TaskListOwnershipCheck,
        policy: crate::matching_registry::QueueOwnershipPolicy,
    ) -> Self {
        self.matching = self.matching.clone().with_task_list_ownership(check);
        if let Some(registry) = &self.registry {
            registry.set_ownership_policy(policy);
        }
        self
    }

    /// Attach discovered shard routing to the same Matching instance exposed
    /// by the owned runtime and its durable queue registry.
    pub fn with_shard_router(
        mut self,
        router: crate::matching_shard_routing::MatchingShardRouter,
    ) -> Self {
        self.matching = self.matching.clone().with_shard_router(router);
        self
    }

    /// Start Go's Matching shard executor and attach its assignment routing to
    /// the same durable registry used by Matching RPCs. A configured manager
    /// requires the production durable registry; silently running with a
    /// no-op executor would split assignments from queue ownership.
    pub async fn with_shard_distributor(
        mut self,
        config: &crate::config::ShardDistributorMatchingConfig,
        channel: tonic::transport::Channel,
        membership: crate::matching_membership::Membership,
        metadata: std::collections::HashMap<String, String>,
        percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
        exclude_short_lived: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> anyhow::Result<Self> {
        if config.namespaces.is_empty() {
            return Ok(self);
        }
        let registry = self.registry.clone().ok_or_else(|| {
            anyhow::anyhow!("shard-distributor Matching requires the durable queue registry")
        })?;
        let enabled = {
            let percentage = percentage_onboarded.clone();
            Arc::new(move || percentage() > 0) as Arc<dyn Fn() -> bool + Send + Sync>
        };
        let rpc = Arc::new(
            crate::matching_shard_distributor::TonicExecutorRpc::from_channel(channel.clone()),
        );
        let factory = Arc::new(
            crate::matching_shard_processor::QueueRegistryShardProcessorFactory::new(
                registry.clone(),
                config.report_ttl_nanos(),
            ),
        );
        let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
        let executor = crate::matching_shard_distributor::ShardDistributorExecutor::new(
            config,
            rpc,
            factory,
            enabled,
            metadata,
            Some(drain_rx),
        )?;
        executor
            .start()
            .await
            .map_err(|error| anyhow::anyhow!("starting shard-distributor executor: {error}"))?;

        let spectator = crate::matching_shard_distributor::TonicSpectator::new(
            channel,
            config.namespaces[0].namespace.clone(),
            Arc::new({
                let percentage = percentage_onboarded.clone();
                move || percentage() > 0
            }),
        );
        if let Err(error) = spectator.start().await {
            executor.shutdown().await;
            return Err(anyhow::anyhow!(
                "starting shard-distributor spectator: {error}"
            ));
        }

        let router = crate::matching_shard_routing::MatchingShardRouter::with_tonic_spectator(
            membership.clone(),
            Some(spectator.clone()),
            percentage_onboarded.clone(),
            exclude_short_lived.clone(),
        )
        .with_peer_ttl(Duration::from_nanos(
            config.effective_peer_ttl_nanos() as u64
        ));
        let peer_cleanup = router.start_peer_cleanup();
        let executor_for_check = executor.clone();
        let lookup: ShardProcessLookup = Arc::new(move |name| {
            let executor = executor_for_check.clone();
            Box::pin(async move { executor.get_shard_process(&name).await.map(|_| ()) })
                as Pin<
                    Box<
                        dyn Future<
                                Output = Result<
                                    (),
                                    crate::matching_shard_distributor::GetShardProcessError,
                                >,
                            > + Send,
                    >,
                >
        });
        let ownership_check = shard_task_list_ownership_check(
            membership.clone(),
            percentage_onboarded.clone(),
            exclude_short_lived.clone(),
            drain_tx.subscribe(),
            lookup,
        );
        let policy = crate::matching_registry::QueueOwnershipPolicy::new(
            Arc::new({
                let percentage = percentage_onboarded.clone();
                let exclude = exclude_short_lived.clone();
                move |name| {
                    !crate::matching_shard_routing::task_list_excluded_from_shard_distributor(
                        name,
                        percentage() as u64,
                        exclude(),
                    )
                }
            }),
            Arc::new({
                let executor = executor.clone();
                move |name| executor.has_local_shard_process(name)
            }),
        );
        self = self
            .with_task_list_ownership(ownership_check, policy)
            .with_shard_router(router);
        self.shard_distributor_executor = Some(executor);
        self.shard_distributor_spectator = Some(spectator);
        self.shard_distributor_drain = Some(drain_tx);
        self.shard_distributor_peer_cleanup = Some(peer_cleanup);
        Ok(self)
    }

    /// Own Go file-based dynamic-config polling for the lifetime of Matching.
    /// The caller loads the initial snapshot synchronously before exposure.
    pub fn with_dynamic_config_file(
        mut self,
        path: std::path::PathBuf,
        poll_interval: Duration,
        initial_load_finished: std::time::SystemTime,
    ) -> Self {
        let overlay = self.matching.frontend.state.admin.dynamic_config.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut last_poll = initial_load_finished;
            let poll_interval = poll_interval.max(Duration::from_secs(5));
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + poll_interval,
                poll_interval,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = interval.tick() => {
                        match std::fs::metadata(&path).and_then(|metadata| metadata.modified()) {
                            Ok(modified) if modified > last_poll => {
                                match crate::admin::DynamicConfigOverlay::from_file(&path) {
                                    Ok(snapshot) => if let Ok(mut current) = overlay.lock() { *current = snapshot; },
                                    Err(error) => eprintln!("warning: reloading dynamic config {:?}: {error}", path),
                                }
                            }
                            Err(error) => eprintln!("warning: checking dynamic config {:?}: {error}", path),
                            Ok(_) => {}
                        }
                        // Go records the wall time at the end of every update
                        // attempt (including an unsuccessful one).
                        last_poll = std::time::SystemTime::now();
                    }
                }
            }
        });
        self.dynamic_config_watch = Some((stop, task));
        self
    }

    /// Attach the service membership ring before this runtime is exposed to
    /// RPC listeners. Both the handler and durable queue registry use this
    /// same snapshot for ownership and forwarding.
    pub fn with_membership(mut self, membership: crate::matching_membership::Membership) -> Self {
        self.matching = self.matching.clone().with_membership(membership);
        let matching = self.matching.clone();
        let (stop, stopped) = oneshot::channel();
        self.membership_maintenance = Some(tokio::spawn(async move {
            matching
                .maintain_membership_until(async {
                    let _ = stopped.await;
                })
                .await
        }));
        self.membership_maintenance_stop = Some(stop);
        self
    }

    /// Start and connect the persistence-backed domain cache before constructing
    /// Matching. Its synchronous initial refresh therefore gates readiness;
    /// subsequent notifications update the same domain view consumed by Matching.
    pub async fn in_memory_with_domain_cache(
        state: crate::handlers::AppState,
        drain_duration: DrainDuration,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<Self> {
        Self::start_domain_cache(state.domains.clone(), cache.clone()).await?;
        let mut runtime = Self::in_memory(state, drain_duration);
        runtime.domain_cache = Some(cache);
        Ok(runtime)
    }

    pub async fn with_durable_queues_and_domain_cache(
        state: crate::handlers::AppState,
        store: Arc<dyn MatchingTaskStore>,
        history: crate::matching_history::MatchingHistoryClient,
        task_id_range: NonZeroU32,
        poll_expiration: Duration,
        drain_duration: DrainDuration,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<Self> {
        Self::start_domain_cache(state.domains.clone(), cache.clone()).await?;
        let mut runtime = Self::with_durable_queues(
            state,
            store,
            history,
            task_id_range,
            poll_expiration,
            drain_duration,
        );
        runtime.domain_cache = Some(cache);
        Ok(runtime)
    }

    async fn start_domain_cache(
        domains: cadence_persistence::InMemoryDomainStore,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<()> {
        crate::matching_blocking::run(move || {
            cache
                .start()
                .map_err(|error| anyhow::anyhow!("starting Matching domain cache: {error}"))?;
            let changed: crate::matching_domain_cache::DomainChangeCallback =
                Arc::new(move |entries| domains.sync_matching_metadata(entries));
            let catch_up: crate::matching_domain_cache::CatchUpCallback =
                Arc::new(|entries, prepare, changed| {
                    prepare();
                    changed(entries);
                });
            cache.register_callback("matching", catch_up, Arc::new(|| {}), changed);
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("joining Matching domain cache startup: {error}"))?
    }

    /// Supply Go's membership eviction action when production membership
    /// lifecycle is wired. It remains absent in the current main assembly.
    pub fn with_membership_evictor(mut self, evict_self: EvictSelf) -> Self {
        self.evict_self = Some(evict_self);
        self
    }

    /// Begin Go's shutdown drain while listeners remain live. The caller
    /// drops listeners only after this future completes.
    pub async fn drain(&mut self) -> anyhow::Result<()> {
        self.drain_with(tokio::time::sleep).await
    }

    async fn drain_with<F, Fut>(&mut self, wait: F) -> anyhow::Result<()>
    where
        F: FnOnce(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        if self.drained {
            return Ok(());
        }
        if let Some(drain) = &self.shard_distributor_drain {
            let _ = drain.send(true);
        }
        let result = drain_service(&mut self.evict_self, &self.drain_duration, wait).await;
        self.drained = true;
        result
    }

    /// Finish shutdown after draining traffic: stop the owner and join
    /// maintenance so final acknowledgements and cleanup complete.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        let drain_result = self.drain().await;
        let dynamic_config_result = if let Some((stop, task)) = self.dynamic_config_watch.take() {
            let _ = stop.send(());
            task.await.map_err(|error| {
                anyhow::anyhow!("joining Matching dynamic-config watcher: {error}")
            })
        } else {
            Ok(())
        };
        if let Some(executor) = self.shard_distributor_executor.take() {
            executor.shutdown().await;
        }
        if let Some(spectator) = self.shard_distributor_spectator.take() {
            spectator.shutdown().await;
        }
        self.shard_distributor_drain.take();
        let peer_cleanup_result = if let Some((stop, task)) =
            self.shard_distributor_peer_cleanup.take()
        {
            let _ = stop.send(());
            task.await
                .map_err(|error| anyhow::anyhow!("joining Matching shard peer cleanup: {error}"))
        } else {
            Ok(())
        };
        if let Some(stop) = self.membership_maintenance_stop.take() {
            let _ = stop.send(());
        }
        if let Some(stop) = self.maintenance_stop.take() {
            let _ = stop.send(());
        }
        let maintenance_result = if let Some(maintenance) = self.maintenance.take() {
            maintenance
                .await
                .map_err(|error| anyhow::anyhow!("joining Matching maintenance: {error}"))
                .and_then(|result| {
                    result
                        .map_err(|error| anyhow::anyhow!("stopping Matching maintenance: {error}"))
                })
        } else if let Some(registry) = &self.registry {
            registry
                .shutdown(crate::grpc_matching::matching_now())
                .await
                .map_err(|error| anyhow::anyhow!("stopping Matching registry: {error}"))
        } else {
            Ok(())
        };
        let membership_result = if let Some(maintenance) = self.membership_maintenance.take() {
            maintenance
                .await
                .map_err(|error| {
                    anyhow::anyhow!("joining Matching membership maintenance: {error}")
                })
                .and_then(|result| {
                    result.map_err(|error| {
                        anyhow::anyhow!("stopping Matching membership maintenance: {error}")
                    })
                })
        } else {
            Ok(())
        };
        if let Some(cache) = self.domain_cache.take() {
            cache.unregister_callback("matching");
            cache.stop();
        }
        maintenance_result?;
        membership_result?;
        peer_cleanup_result?;
        dynamic_config_result?;
        drain_result?;
        Ok(())
    }
}

impl Drop for MatchingRuntime {
    fn drop(&mut self) {
        // Normal shutdown awaits the task. This is a leak guard for abandoned
        // owners, where persistence cannot safely be awaited.
        if let Some(stop) = self.maintenance_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.maintenance.take() {
            task.abort();
        }
        if let Some(stop) = self.membership_maintenance_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.membership_maintenance.take() {
            task.abort();
        }
        if let Some((stop, task)) = self.dynamic_config_watch.take() {
            let _ = stop.send(());
            task.abort();
        }
        if let Some((stop, task)) = self.shard_distributor_peer_cleanup.take() {
            let _ = stop.send(());
            task.abort();
        }
    }
}

/// Read matching.shutdownDrainDuration at call time. Go defines this property
/// without filters and defaults it to zero.
pub fn live_shutdown_drain_duration(overlay: Arc<Mutex<DynamicConfigOverlay>>) -> DrainDuration {
    Arc::new(move || {
        let values = overlay
            .lock()
            .expect("admin dynamic config lock")
            .property_values("matching.shutdownDrainDuration")
            .to_vec();
        let mut selected = None;
        for value in values.iter().filter(|value| value.filters.is_empty()) {
            let Some(blob) = value.value.as_ref() else {
                continue;
            };
            if blob.encoding_type != Some(cadence_api::enums::EncodingType::JSON) {
                continue;
            }
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&blob.data) {
                selected = Some(value);
            }
        }
        let nanos = selected
            .and_then(|value| {
                value
                    .as_str()
                    .and_then(cadence_common::duration::parse_nanos)
            })
            .unwrap_or_default();
        Duration::from_nanos(nanos.max(0) as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_sqlite::SqliteTaskStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn empty_membership() -> crate::matching_membership::Membership {
        crate::matching_membership::Membership::new("local", Vec::new())
    }

    fn installed_ownership_check(
        percentage: i64,
        draining: bool,
        calls: Arc<AtomicUsize>,
        result: Result<(), crate::matching_shard_distributor::GetShardProcessError>,
    ) -> crate::grpc_matching::TaskListOwnershipCheck {
        let (drain_tx, drain_rx) = tokio::sync::watch::channel(draining);
        let _keep_drain_sender_alive = drain_tx;
        shard_task_list_ownership_check(
            empty_membership(),
            Arc::new(move || percentage),
            Arc::new(|| false),
            drain_rx,
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                let result = match &result {
                    Ok(()) => Ok(()),
                    Err(crate::matching_shard_distributor::GetShardProcessError::NotFound(key)) => {
                        Err(
                            crate::matching_shard_distributor::GetShardProcessError::NotFound(
                                key.clone(),
                            ),
                        )
                    }
                    Err(crate::matching_shard_distributor::GetShardProcessError::Heartbeat(
                        error,
                    )) => Err(
                        crate::matching_shard_distributor::GetShardProcessError::Heartbeat(
                            error.clone(),
                        ),
                    ),
                };
                Box::pin(async move { result })
                    as Pin<
                        Box<
                            dyn Future<
                                    Output = Result<
                                        (),
                                        crate::matching_shard_distributor::GetShardProcessError,
                                    >,
                                > + Send,
                        >,
                    >
            }),
        )
    }

    #[tokio::test]
    async fn installed_shard_ownership_check_matches_notfound_shutdown_and_transient_errors() {
        use crate::matching_shard_distributor::GetShardProcessError;

        let calls = Arc::new(AtomicUsize::new(0));
        let not_found = installed_ownership_check(
            100,
            false,
            calls.clone(),
            Err(GetShardProcessError::NotFound("queue".into())),
        );
        assert_eq!(
            not_found("queue".into(), false).await.unwrap_err().code(),
            tonic::Code::Aborted
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let transient = installed_ownership_check(
            100,
            false,
            calls.clone(),
            Err(GetShardProcessError::Heartbeat("rpc unavailable".into())),
        );
        let error = transient("queue".into(), false).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error
            .message()
            .starts_with("failed to lookup ownership in SD:"));

        let shutting_down = installed_ownership_check(100, true, calls.clone(), Ok(()));
        assert_eq!(
            shutting_down("queue".into(), false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "shutdown skips SD lookup");
    }

    #[tokio::test]
    async fn installed_shard_ownership_check_bypasses_ring_for_cached_excluded_and_wraps_negative_percent(
    ) {
        use crate::matching_shard_distributor::GetShardProcessError;

        let calls = Arc::new(AtomicUsize::new(0));
        let cached_excluded = installed_ownership_check(
            0,
            false,
            calls.clone(),
            Err(GetShardProcessError::NotFound("unused".into())),
        );
        assert!(cached_excluded("queue".into(), true).await.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let negative = installed_ownership_check(-1, false, calls.clone(), Ok(()));
        assert!(negative("negative-percentage".into(), false).await.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_drain_provider_reads_live_unfiltered_value() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let provider = live_shutdown_drain_duration(overlay.clone());
        assert_eq!(provider(), Duration::ZERO);
        overlay.lock().unwrap().update(
            false,
            "matching.shutdownDrainDuration",
            vec![
                cadence_api::admin::DynamicConfigValue {
                    filters: vec![cadence_api::admin::DynamicConfigFilter {
                        name: "taskListName".into(),
                        value: Some(cadence_api::types::DataBlob {
                            encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                            data: br#""99ms""#.to_vec(),
                        }),
                    }],
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""99ms""#.to_vec(),
                    }),
                },
                cadence_api::admin::DynamicConfigValue {
                    filters: vec![],
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""25ms""#.to_vec(),
                    }),
                },
            ],
        );
        assert_eq!(provider(), Duration::from_millis(25));
    }

    #[tokio::test]
    async fn shutdown_drain_runs_membership_evictor_before_live_drain_read() {
        let order = Arc::new(AtomicUsize::new(0));
        let evict_order = order.clone();
        let evict: EvictSelf = Arc::new(move || {
            let evict_order = evict_order.clone();
            Box::pin(async move {
                assert_eq!(evict_order.fetch_add(1, Ordering::SeqCst), 0);
                Ok(())
            })
        });
        let drain_order = order.clone();
        let duration: DrainDuration = Arc::new(move || {
            assert_eq!(drain_order.fetch_add(1, Ordering::SeqCst), 1);
            Duration::ZERO
        });
        let mut evictor = Some(evict);
        drain_service(&mut evictor, &duration, |_| std::future::ready(()))
            .await
            .unwrap();
        assert_eq!(order.load(Ordering::SeqCst), 2);
        assert!(evictor.is_none());
    }

    #[tokio::test]
    async fn shutdown_uses_drain_duration_updated_after_runtime_startup() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        overlay.lock().unwrap().update(
            false,
            "matching.shutdownDrainDuration",
            vec![cadence_api::admin::DynamicConfigValue {
                filters: vec![],
                value: Some(cadence_api::types::DataBlob {
                    encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                    data: br#""5ms""#.to_vec(),
                }),
            }],
        );
        let drain_duration = live_shutdown_drain_duration(overlay.clone());
        let runtime =
            MatchingRuntime::in_memory(crate::handlers::AppState::default(), drain_duration);

        // The runtime and provider exist before this shutdown-time change.
        // Go evaluates the property only after its membership eviction.
        let update_overlay = overlay.clone();
        let evict: EvictSelf = Arc::new(move || {
            update_overlay.lock().unwrap().update(
                false,
                "matching.shutdownDrainDuration",
                vec![cadence_api::admin::DynamicConfigValue {
                    filters: vec![],
                    value: Some(cadence_api::types::DataBlob {
                        encoding_type: Some(cadence_api::enums::EncodingType::JSON),
                        data: br#""137ms""#.to_vec(),
                    }),
                }],
            );
            Box::pin(async { Ok(()) })
        });
        let mut runtime = runtime.with_membership_evictor(evict);

        let selected = Arc::new(Mutex::new(None));
        let observed = selected.clone();
        runtime
            .drain_with(move |duration| {
                *observed.lock().unwrap() = Some(duration);
                std::future::ready(())
            })
            .await
            .unwrap();
        assert_eq!(*selected.lock().unwrap(), Some(Duration::from_millis(137)));
    }

    #[tokio::test]
    async fn durable_runtime_joins_maintenance_and_flushes_on_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(
                directory.path().join("matching.db"),
                cadence_persistence::matching_blob::BlobEncoding::ThriftRWSnappy,
            )
            .unwrap(),
        );
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let history = crate::matching_history::MatchingHistoryClient::new(
            channel,
            Duration::from_millis(10),
            1024,
        );
        let mut runtime = MatchingRuntime::with_durable_queues(
            crate::handlers::AppState::default(),
            store,
            history,
            NonZeroU32::new(100).unwrap(),
            Duration::from_secs(1),
            Arc::new(|| Duration::ZERO),
        );
        runtime.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}
