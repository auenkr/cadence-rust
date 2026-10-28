//! Production owner for Matching's handler, durable queue registry and
//! background maintenance task.
//!
//! Persistence, History and dynamic configuration enter through explicit
//! dependencies. The production domain cache can be attached through the
//! cache-aware constructors; membership discovery/eviction remains a separate
//! adapter, so this owner does not represent complete Go Matching parity.
#[cfg(test)]
#[path = "matching_failover_callback_tests.rs"]
mod failover_callback_tests;
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
type NativeMembershipTask = (oneshot::Sender<()>, JoinHandle<Result<(), tonic::Status>>);

fn ensure_queue_registry_unowned(
    legacy: bool,
    native: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(!legacy && !native, "Matching runtime already owns durable queues");
    Ok(())
}

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
    native_registry: Option<crate::matching_registry_native::NativeQueueRegistry>,
    native_host: Option<Arc<crate::matching_service_native::NativeMatchingService<crate::matching_host_native::NativeMatchingHost>>>,
    native_semaphore: Option<crate::matching_semaphore_service_native::NativeSemaphoreService>,
    native_semaphore_maintenance: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    maintenance_stop: Option<oneshot::Sender<()>>,
    maintenance: Option<JoinHandle<Result<(), tonic::Status>>>,
    membership_maintenance_stop: Option<oneshot::Sender<()>>,
    membership_maintenance: Option<JoinHandle<Result<(), tonic::Status>>>,
    domain_cache: Option<Arc<crate::matching_domain_cache::MatchingDomainCache>>,
    native_domain_registry: Arc<Mutex<Option<crate::matching_registry_native::NativeQueueRegistry>>>,
    native_membership_maintenance: Option<NativeMembershipTask>,
    membership: Option<crate::matching_membership::Membership>,
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
    /// Install the production sink on the handler and maintenance registry so
    /// work reached through either retained runtime handle shares one reporter.
    pub fn with_metrics(mut self, sink: Arc<dyn crate::matching_metrics::MetricSink>) -> Self {
        let metrics = crate::matching_metrics::MatchingMetrics::new(sink.clone());
        self.matching = self.matching.clone().with_metrics(sink);
        if let Some(registry) = self.registry.take() {
            self.registry = Some(registry.with_metrics(metrics));
        }
        self
    }
    pub(crate) fn with_fatal_writer(
        mut self,
        fatal: crate::matching_fatal::MatchingFatalSender,
    ) -> Self {
        self.matching = self.matching.clone().with_fatal_writer(fatal.clone());
        if let Some(registry) = self.registry.take() {
            self.registry = Some(registry.with_fatal_writer(fatal));
        }
        self
    }

    pub fn in_memory(state: crate::handlers::AppState, drain_duration: DrainDuration) -> Self {
        Self {
            matching: Matching::new(state),
            registry: None,
            native_registry: None,
            native_host: None,
            native_semaphore: None,
            native_semaphore_maintenance: None,
            maintenance_stop: None,
            maintenance: None,
            membership_maintenance_stop: None,
            membership_maintenance: None,
            domain_cache: None,
            native_domain_registry: Arc::new(Mutex::new(None)),
            native_membership_maintenance: None,
            membership: None,
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
            native_registry: None,
            native_host: None,
            native_semaphore: None,
            native_semaphore_maintenance: None,
            maintenance_stop: Some(stop),
            maintenance: Some(maintenance),
            membership_maintenance_stop: None,
            membership_maintenance: None,
            domain_cache: None,
            native_domain_registry: Arc::new(Mutex::new(None)),
            native_membership_maintenance: None,
            membership: None,
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

    /// Attach the native service and own its maintenance and final writer joins.
    /// Call before listeners start, on a runtime without legacy durable queues.
    pub fn with_native_queues(
        mut self,
        registry: crate::matching_registry_native::NativeQueueRegistry,
        service: Arc<dyn cadence_proto::native::NativeMatchingApi>,
    ) -> anyhow::Result<Self> {
        ensure_queue_registry_unowned(self.registry.is_some(), self.native_registry.is_some())?;
        self.matching = self.matching.clone().with_native_service(service);
        *self
            .native_domain_registry
            .lock()
            .expect("native domain registry lock") = Some(registry.clone());
        let (stop, stopped) = oneshot::channel();
        let maintenance_registry = registry.clone();
        self.maintenance = Some(tokio::spawn(async move {
            maintenance_registry.maintain_until(Duration::from_secs(10), async {
                let _ = stopped.await;
            })
            .await
        }));
        self.maintenance_stop = Some(stop);
        self.native_registry = Some(registry);
        if let Some(membership) = self.membership.clone() {
            self.start_native_membership_maintenance(membership);
        }
        Ok(self)
    }

    /// Attach the native host lifecycle before exposing listeners. The semaphore
    /// uses the same membership stream as queue ownership and is swept even on
    /// an otherwise idle host.
    pub fn with_native_host(
        mut self,
        registry: crate::matching_registry_native::NativeQueueRegistry,
        host: Arc<crate::matching_service_native::NativeMatchingService<crate::matching_host_native::NativeMatchingHost>>,
        semaphore: crate::matching_semaphore_service_native::NativeSemaphoreService,
        membership: crate::matching_membership::Membership,
    ) -> anyhow::Result<Self> {
        self = self.with_native_queues(registry, host.clone())?;
        let mut changes = membership.subscribe();
        let (stop, mut stopped) = oneshot::channel();
        let service = semaphore.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            for error in service.sweep_non_owned() {
                eprintln!("Native Matching semaphore membership cleanup failed: {error}");
            }
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = interval.tick() => service.sweep_idle(crate::grpc_matching::matching_now()),
                    changed = changes.changed() => {
                        if changed.is_err() { break; }
                        for error in service.sweep_non_owned() {
                            eprintln!("Native Matching semaphore membership cleanup failed: {error}");
                        }
                    }
                }
            }
        });
        self.native_host = Some(host);
        self.native_semaphore = Some(semaphore);
        self.native_semaphore_maintenance = Some((stop, task));
        Ok(self)
    }

    /// Open native admission only after the shared listener and membership start.
    pub fn mark_started(&self) {
        if let Some(host) = &self.native_host {
            host.integration.mark_started();
        }
    }

    /// Go `matchingEngineImpl.isShuttingDown`: false until the drain completes.
    pub fn is_shutting_down(&self) -> bool {
        self.matching.is_shutting_down()
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
            Arc::new(move || {
                crate::matching_shard_routing::shard_distributor_onboarded(percentage())
            }) as Arc<dyn Fn() -> bool + Send + Sync>
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
                move || crate::matching_shard_routing::shard_distributor_onboarded(percentage())
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

    /// Go `setupExecutor` with no namespaces: install the no-op executor, which
    /// owns no shards. A task list the percentage admits then has no shard
    /// process, so ownership is refused as not-owned, the same outcome Go's
    /// `ErrShardProcessNotFound` path produces.
    pub fn with_noop_shard_distributor(
        mut self,
        membership: crate::matching_membership::Membership,
        percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
        exclude_short_lived: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        let executor = crate::matching_shard_distributor::NoopShardExecutor;
        let (drain_tx, _) = tokio::sync::watch::channel(false);
        let lookup: ShardProcessLookup = Arc::new(move |name| {
            let result = executor.get_shard_process(&name).map(|_| ());
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
        });
        let ownership_check = shard_task_list_ownership_check(
            membership,
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
            Arc::new(move |name| executor.has_local_shard_process(name)),
        );
        self = self.with_task_list_ownership(ownership_check, policy);
        self.shard_distributor_drain = Some(drain_tx);
        self
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

    /// Own Go config-store polling (`startUpdate`): refresh the cached
    /// snapshot every `pollInterval`; a failed update keeps the old snapshot.
    pub fn with_dynamic_config_store(
        mut self,
        client: std::sync::Arc<cadence_persistence::config_store::ConfigStoreClient>,
    ) -> Self {
        let overlay = self.matching.frontend.state.admin.dynamic_config.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let poll_interval = client.poll_interval();
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + poll_interval,
                poll_interval,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = interval.tick() => {
                        let worker = client.clone();
                        let updated = tokio::task::spawn_blocking(move || {
                            worker.update().map(|()| worker.snapshot())
                        })
                        .await;
                        match updated {
                            Ok(Ok(Some(snapshot))) => if let Ok(mut current) = overlay.lock() {
                                *current = crate::admin::DynamicConfigOverlay::from_config_store(&snapshot);
                            },
                            Ok(Ok(None)) => {}
                            Ok(Err(error)) => eprintln!("error: Failed to update dynamic config: {error}"),
                            Err(error) => eprintln!("error: dynamic config update task: {error}"),
                        }
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
        self.matching = self.matching.clone().with_membership(membership.clone());
        self.membership = Some(membership.clone());
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
        if self.native_registry.is_some() {
            self.start_native_membership_maintenance(membership);
        }
        self
    }

    fn start_native_membership_maintenance(
        &mut self,
        membership: crate::matching_membership::Membership,
    ) {
        if self.native_membership_maintenance.is_none() {
            let registry = self.native_domain_registry.clone();
            let mut changes = membership.subscribe();
            let (native_stop, mut native_stopped) = oneshot::channel();
            let task = tokio::spawn(async move {
                loop {
                    let current = registry.lock().expect("native domain registry lock").clone();
                    if let Some(current) = current {
                        if let Err(error) = current.unload_non_owned(crate::grpc_matching::matching_now()).await {
                            eprintln!("Native Matching membership cleanup failed: {error}");
                        }
                    }
                    tokio::select! {
                        _ = &mut native_stopped => return Ok(()),
                        changed = changes.changed() => {
                            if changed.is_err() { return Ok(()); }
                        }
                    }
                }
            });
            self.native_membership_maintenance = Some((native_stop, task));
        }
    }

    /// Start and connect the persistence-backed domain cache before constructing
    /// Matching. Its synchronous initial refresh therefore gates readiness;
    /// subsequent notifications update the same domain view consumed by Matching.
    pub async fn in_memory_with_domain_cache(
        state: crate::handlers::AppState,
        drain_duration: DrainDuration,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) -> anyhow::Result<Self> {
        let native_registry = Arc::new(Mutex::new(None));
        Self::start_domain_cache(
            state.domains.clone(),
            cache.clone(),
            None,
            native_registry.clone(),
        )
        .await?;
        let mut runtime = Self::in_memory(state, drain_duration);
        runtime.native_domain_registry = native_registry;
        runtime.matching = runtime.matching.clone().with_domain_cache(cache.clone());
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
        let domains = state.domains.clone();
        let mut runtime = Self::with_durable_queues(
            state,
            store,
            history,
            task_id_range,
            poll_expiration,
            drain_duration,
        );
        Self::start_domain_cache(
            domains,
            cache.clone(),
            runtime.registry.clone(),
            runtime.native_domain_registry.clone(),
        )
        .await?;
        runtime.matching = runtime.matching.clone().with_domain_cache(cache.clone());
        runtime.domain_cache = Some(cache);
        Ok(runtime)
    }

    async fn start_domain_cache(
        domains: cadence_persistence::InMemoryDomainStore,
        cache: Arc<crate::matching_domain_cache::MatchingDomainCache>,
        registry: Option<crate::matching_registry::QueueRegistry>,
        native_registry: Arc<Mutex<Option<crate::matching_registry_native::NativeQueueRegistry>>>,
    ) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            cache
                .start()
                .map_err(|error| anyhow::anyhow!("starting Matching domain cache: {error}"))?;
            let notification_version = Arc::new(std::sync::Mutex::new(None::<i64>));
            let prepare: crate::matching_domain_cache::PrepareCallback = Arc::new(|| {});
            let changed: crate::matching_domain_cache::DomainChangeCallback = Arc::new({
                let notification_version = notification_version.clone();
                let runtime = runtime.clone();
                let native_registry = native_registry.clone();
                move |entries| {
                    domains.sync_matching_metadata(entries);
                    let mut version = notification_version
                        .lock()
                        .expect("Matching failover callback version lock");
                    let previous = *version;
                    let current = entries
                        .iter()
                        .map(|entry| entry.failover_notification_version)
                        .max()
                        .unwrap_or_default();
                    // Registration catch-up establishes Go's engine baseline;
                    // it must not disconnect any pollers.
                    if let Some(previous) = previous {
                        if current > previous {
                            let (_, notified) = notified_failover_domains(
                                previous,
                                entries.iter().map(|entry| {
                                    (
                                        entry.info.uuid.clone(),
                                        entry.is_global_domain,
                                        entry.replication.is_active_active(),
                                        entry.failover_notification_version,
                                    )
                                }),
                            );
                            if let Some(registry) = &registry {
                                for domain in &notified {
                                    if let Ok(domain) = uuid::Uuid::parse_str(domain) {
                                        let registry = registry.clone();
                                        runtime.spawn(async move {
                                            registry.release_domain_failover_polls(domain).await;
                                        });
                                    }
                                }
                            }
                            if let Some(registry) = native_registry
                                .lock()
                                .expect("native domain registry lock")
                                .clone()
                            {
                                for domain in notified.iter() {
                                    if let Ok(domain) = uuid::Uuid::parse_str(domain) {
                                        let registry = registry.clone();
                                        runtime.spawn(async move {
                                            registry.release_domain_failover_polls(domain).await;
                                        });
                                    }
                                }
                            }
                        }
                    }
                    *version = Some(previous.map_or(current, |old| old.max(current)));
                }
            });
            let catch_up: crate::matching_domain_cache::CatchUpCallback = Arc::new({
                let notification_version = notification_version.clone();
                move |entries, prepare, changed| {
                    *notification_version
                        .lock()
                        .expect("Matching failover callback version lock") = Some(
                        entries
                            .iter()
                            .map(|entry| entry.failover_notification_version)
                            .max()
                            .unwrap_or_default(),
                    );
                    prepare();
                    changed(entries);
                }
            });
            cache.register_callback("matching", catch_up, prepare, changed);
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
        let result = drain_service(&mut self.evict_self, &self.drain_duration, wait).await;
        // Go closes the engine's shutdown channel (ownership checks answer "not
        // known", the shard-distributor executor stops) only in handler.Stop,
        // i.e. after EvictSelf and the drain sleep.
        self.matching.begin_shutdown();
        if let Some(host) = &self.native_host {
            host.integration.begin_shutdown();
        }
        if let Some(drain) = &self.shard_distributor_drain {
            let _ = drain.send(true);
        }
        self.drained = true;
        // Go's Service.Stop discards EvictSelf's error and never fails shutdown for it.
        if let Err(error) = result {
            eprintln!("evicting Matching from membership failed: {error:#}");
        }
        Ok(())
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
        let native_membership_result = if let Some((stop, task)) = self.native_membership_maintenance.take() {
            let _ = stop.send(());
            task.await
                .map_err(|error| {
                    anyhow::anyhow!("joining native Matching membership maintenance: {error}")
                })
                .and_then(|result| result.map_err(|error| {
                    anyhow::anyhow!("stopping native Matching membership maintenance: {error}")
                }))
        } else { Ok(()) };
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
        } else if let Some(registry) = &self.native_registry {
            registry
                .shutdown(crate::grpc_matching::matching_now())
                .await
                .map_err(|error| anyhow::anyhow!("stopping native Matching registry: {error}"))
        } else {
            Ok(())
        };
        // Registry shutdown stops every owner, but a writer actor may already
        // be inside a blocking SQL append. Join those actors before the
        // launcher samples its fatal-writer channel, so shutdown cannot race
        // a late typed SQL failure notification.
        if let Some(registry) = &self.registry {
            registry.wait_for_writers().await;
        }
        if let Some(registry) = &self.native_registry {
            registry.wait_for_writers().await;
        }
        let native_semaphore_result = if let Some((stop, task)) = self.native_semaphore_maintenance.take() {
            let _ = stop.send(());
            task.await.map_err(|error| anyhow::anyhow!("joining native semaphore maintenance: {error}"))
        } else { Ok(()) };
        if let Some(semaphore) = &self.native_semaphore {
            semaphore.stop_all();
        }
        // Go Stop also stops every semaphore bucket, not only the ones the ring moved away.
        self.matching.stop_semaphores();
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
        native_membership_result?;
        native_semaphore_result?;
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
        if let Some((stop, task)) = self.native_semaphore_maintenance.take() {
            let _ = stop.send(());
            task.abort();
        }
        if let Some(semaphore) = &self.native_semaphore {
            semaphore.stop_all();
        }
        if let Some((stop, task)) = self.native_membership_maintenance.take() {
            let _ = stop.send(());
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
        let config = overlay.lock().expect("admin dynamic config lock");
        let nanos = cadence_persistence::dynamic_config::duration(
            config.property_values("matching.shutdownDrainDuration"),
            None,
            0,
        );
        Duration::from_nanos(nanos.max(0) as u64)
    })
}

/// Go `domainChangeCallback` selection: a global, non-active-active domain whose
/// failover notification version advanced past `previous` disconnects its
/// pollers. Returns the highest version seen (the new baseline) and the UUIDs
/// of the domains that must release their polls.
fn notified_failover_domains(
    previous: i64,
    entries: impl Iterator<Item = (String, bool, bool, i64)>,
) -> (i64, Vec<String>) {
    let mut highest = previous;
    let mut notified = Vec::new();
    for (uuid, is_global, is_active_active, version) in entries {
        highest = highest.max(version);
        if is_global && !is_active_active && version > previous {
            notified.push(uuid);
        }
    }
    (highest, notified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::{
        matching_blob::{BlobEncoding, TaskData, TaskListData},
        matching_sqlite::{AllocatedTask, SqliteTaskStore, TaskListKey, TaskListRecord},
        matching_task_store::{MatchingTaskStore, Result as StoreResult},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Pauses the real store call used by TaskWriter while delegating every
    /// other operation to SQLite. The released append reaches SQLite's normal
    /// malformed-RunID conversion and typed error path.
    struct DelayedAppendStore {
        inner: Arc<SqliteTaskStore>,
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        released: (std::sync::Mutex<bool>, std::sync::Condvar),
    }

    impl MatchingTaskStore for DelayedAppendStore {
        fn get_task_list(&self, key: &TaskListKey) -> StoreResult<TaskListRecord> {
            self.inner.get_task_list(key)
        }
        fn lease(
            &self,
            key: &TaskListKey,
            kind: i16,
            previous_range: i64,
            now_nanos: i64,
        ) -> StoreResult<TaskListRecord> {
            self.inner.lease(key, kind, previous_range, now_nanos)
        }
        fn update_task_list(
            &self,
            key: &TaskListKey,
            range: i64,
            data: TaskListData,
            now_nanos: i64,
        ) -> StoreResult<()> {
            self.inner.update_task_list(key, range, data, now_nanos)
        }
        fn create_tasks_with_domains(
            &self,
            key: &TaskListKey,
            range: i64,
            now_nanos: i64,
            tasks: &[(uuid::Uuid, &AllocatedTask)],
        ) -> StoreResult<()> {
            self.entered
                .send(())
                .expect("test is waiting for delayed append");
            let (lock, changed) = &self.released;
            let mut released = lock.lock().expect("append release lock");
            while !*released {
                released = changed.wait(released).expect("append release lock");
            }
            let _ = now_nanos;
            self.inner.create_tasks_with_domains(
                key,
                range,
                tasks.iter().map(|(domain, task)| (*domain, *task)),
            )
        }
        fn get_tasks(
            &self,
            key: &TaskListKey,
            read_level: i64,
            max_read_level: i64,
            batch_size: i64,
        ) -> StoreResult<Vec<AllocatedTask>> {
            self.inner
                .get_tasks(key, read_level, max_read_level, batch_size)
        }
        fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> StoreResult<i64> {
            self.inner.task_list_size(key, ack_level)
        }
        fn complete_task(&self, key: &TaskListKey, task_id: i64) -> StoreResult<()> {
            self.inner.complete_task(key, task_id)
        }
        fn complete_tasks_less_than(
            &self,
            key: &TaskListKey,
            inclusive_task_id: i64,
            limit: i64,
        ) -> StoreResult<usize> {
            self.inner
                .complete_tasks_less_than(key, inclusive_task_id, limit)
        }
        fn delete_task_list(&self, key: &TaskListKey, range: i64) -> StoreResult<()> {
            self.inner.delete_task_list(key, range)
        }
    }

    // Release a blocked append even when a test assertion unwinds.
    struct AppendReleaseGuard(Arc<DelayedAppendStore>);

    impl Drop for AppendReleaseGuard {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl DelayedAppendStore {
        fn release(&self) {
            let (lock, changed) = &self.released;
            *lock.lock().expect("append release lock") = true;
            changed.notify_all();
        }
    }

    #[test]
    fn failover_callback_selection_matches_go_and_advances_baseline() {
        let (version, notified) = notified_failover_domains(
            10,
            [
                ("global".into(), true, false, 11),
                ("local".into(), false, false, 20),
                ("active-active".into(), true, true, 30),
                ("old".into(), true, false, 10),
            ]
            .into_iter(),
        );
        assert_eq!(version, 30);
        assert_eq!(notified, vec!["global"]);
        let (same, notified) =
            notified_failover_domains(version, [("global".into(), true, false, 11)].into_iter());
        assert_eq!(same, 30);
        assert!(notified.is_empty());
    }

    fn empty_membership() -> crate::matching_membership::Membership {
        crate::matching_membership::Membership::new("local", Vec::new())
    }

    #[test]
    fn durable_queue_ownership_guard_rejects_either_existing_registry() {
        assert!(ensure_queue_registry_unowned(false, false).is_ok());
        assert!(ensure_queue_registry_unowned(true, false).is_err());
        assert!(ensure_queue_registry_unowned(false, true).is_err());
        assert!(ensure_queue_registry_unowned(true, true).is_err());
    }

    #[tokio::test]
    async fn native_registry_maintenance_stops_and_joins_on_shutdown_signal() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(
                directory.path().join("native-maintenance.db"),
                BlobEncoding::ThriftRW,
            )
            .unwrap(),
        );
        let history = crate::matching_history::MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let registry = crate::matching_registry_native::NativeQueueRegistry::new(
            store,
            history,
            NonZeroU32::new(100).unwrap(),
        );
        let owner = tokio::time::timeout(Duration::from_secs(2), registry.get(
            cadence_persistence::matching_native_task_store::TaskListKeyBytes {
                domain_id: uuid::Uuid::from_u128(1),
                name: cadence_common::go_string::GoString::from(&b"shutdown-\xff"[..]),
                task_type: 0,
            }, 0, crate::grpc_matching::matching_now(),
        )).await.unwrap().unwrap();
        let joining_registry = registry.clone();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            registry
                .maintain_until(Duration::from_secs(60), async {
                    let _ = stopped.await;
                })
                .await
        });
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("native maintenance should stop promptly")
            .expect("native maintenance task should join")
            .expect("native registry should shut down cleanly");
        assert!(owner.queue.is_stopped());
        tokio::time::timeout(Duration::from_secs(2), joining_registry.wait_for_writers())
            .await.expect("native writers should join after owner shutdown");
    }

    fn dynamic_config_test_value(runtime: &MatchingRuntime, key: &str) -> i64 {
        let property = runtime
            .matching
            .frontend
            .state
            .admin
            .dynamic_config
            .lock()
            .unwrap()
            .property_values(key);
        let values = property.snapshot_values();
        serde_json::from_slice(&values[0].value.as_ref().expect("dynamic config value").data)
            .unwrap()
    }

    async fn wait_for_dynamic_config(runtime: &MatchingRuntime, key: &str, expected: i64) {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if dynamic_config_test_value(runtime, key) == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("dynamic config did not reload to {expected}"));
    }

    #[tokio::test]
    async fn dynamic_config_file_watcher_reloads_retains_last_good_and_stops_on_shutdown() {
        const KEY: &str = "testGetIntPropertyKey";
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dynamic-config.yaml");
        std::fs::write(&path, format!("{KEY}:\n- value: 1\n")).unwrap();
        let state = crate::handlers::AppState::default();
        *state.admin.dynamic_config.lock().unwrap() =
            DynamicConfigOverlay::from_file(&path).unwrap();
        let mut runtime = MatchingRuntime::in_memory(state, Arc::new(|| Duration::ZERO))
            .with_dynamic_config_file(
                path.clone(),
                Duration::from_secs(5),
                std::time::SystemTime::now(),
            );
        assert_eq!(dynamic_config_test_value(&runtime, KEY), 1);

        // The production minimum remains five seconds; use real elapsed time
        // because the watcher compares filesystem wall-clock modification times.
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(&path, format!("{KEY}:\n- value: 2\n")).unwrap();
        wait_for_dynamic_config(&runtime, KEY, 2).await;

        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(&path, "not: [valid").unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(dynamic_config_test_value(&runtime, KEY), 2);

        std::thread::sleep(Duration::from_millis(20));
        std::fs::remove_file(&path).unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(dynamic_config_test_value(&runtime, KEY), 2);

        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(&path, format!("{KEY}:\n- value: 3\n")).unwrap();
        wait_for_dynamic_config(&runtime, KEY, 3).await;

        runtime.shutdown().await.unwrap();
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(&path, format!("{KEY}:\n- value: 4\n")).unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(dynamic_config_test_value(&runtime, KEY), 3);
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
    async fn drain_flag_flips_after_evict_and_sleep_and_evict_errors_are_ignored() {
        // Go: EvictSelf, sleep(ShutdownDrainDuration), then handler.Stop closes the
        // engine shutdown channel. EvictSelf's error is discarded.
        let duration: DrainDuration = Arc::new(|| Duration::from_millis(5));
        let mut runtime =
            MatchingRuntime::in_memory(crate::handlers::AppState::default(), duration);
        let (tx, rx) = tokio::sync::watch::channel(false);
        runtime.shard_distributor_drain = Some(tx);
        let evict: EvictSelf =
            Arc::new(|| Box::pin(async { Err(anyhow::anyhow!("evict failed")) }));
        let mut runtime = runtime.with_membership_evictor(evict);
        let during = Arc::new(Mutex::new(None));
        let observed = during.clone();
        let rx_during = rx.clone();
        runtime
            .drain_with(move |_| {
                *observed.lock().unwrap() = Some(*rx_during.borrow());
                std::future::ready(())
            })
            .await
            .unwrap();
        assert_eq!(
            *during.lock().unwrap(),
            Some(false),
            "not draining during sleep"
        );
        assert!(*rx.borrow(), "draining once the sleep is over");
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

    #[tokio::test]
    async fn runtime_shutdown_waits_for_real_delayed_append_and_fatal_notification() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite = Arc::new(
            SqliteTaskStore::create(
                directory.path().join("delayed-writer.db"),
                BlobEncoding::ThriftRW,
            )
            .unwrap(),
        );
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let store = Arc::new(DelayedAppendStore {
            inner: sqlite,
            entered: entered_tx,
            released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let _release_on_drop = AppendReleaseGuard(store.clone());
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let history = crate::matching_history::MatchingHistoryClient::new(
            channel,
            Duration::from_millis(10),
            1024,
        );
        let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = MatchingRuntime::with_durable_queues(
            crate::handlers::AppState::default(),
            store.clone(),
            history,
            NonZeroU32::new(100).unwrap(),
            Duration::from_secs(1),
            Arc::new(|| Duration::ZERO),
        )
        .with_fatal_writer(fatal_tx);

        // Load the owner from the runtime's registry so TaskWriter is created
        // through the same automatic tracker registration used in production.
        let key = TaskListKey {
            domain_id: uuid::Uuid::from_u128(9),
            name: "runtime-delayed-fatal".into(),
            task_type: 0,
        };
        let handle = runtime
            .registry
            .as_ref()
            .expect("durable runtime registry")
            .get(key.clone(), 0, 1)
            .await
            .unwrap();
        let append = tokio::spawn(async move {
            handle
                .enqueue(
                    TaskData::default().with_request_run_id("runtime-delayed-malformed-run-id"),
                    key.domain_id,
                    0,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
            .await
            .expect("TaskWriter never entered the delayed persistence operation")
            .expect("delayed persistence sender dropped");

        let mut shutdown = tokio::spawn(async move { runtime.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "runtime shutdown returned while the actual writer append remained in flight"
        );

        store.release();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .expect("runtime shutdown did not finish after releasing persistence")
            .expect("runtime shutdown task panicked")
            .unwrap();
        let append_error = tokio::time::timeout(Duration::from_secs(2), append)
            .await
            .expect("append did not complete after releasing persistence")
            .expect("append task panicked")
            .expect_err("malformed RunID append unexpectedly succeeded");
        assert_eq!(append_error.code(), tonic::Code::Internal);
        assert!(matches!(
            fatal_rx.try_recv().expect("fatal cause must be available when runtime shutdown returns"),
            crate::matching_fatal::MatchingFatal::SqlRunId(
                cadence_persistence::matching_blob::SqlRunIdError::Malformed { raw, .. }
            ) if raw == "runtime-delayed-malformed-run-id"
        ));
    }
    #[tokio::test]
    async fn runtime_reader_invariant_violation_reaches_fatal_channel_before_shutdown_returns() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite = Arc::new(
            SqliteTaskStore::create(
                directory.path().join("reader-fatal.db"),
                BlobEncoding::ThriftRW,
            )
            .unwrap(),
        );
        // Out-of-order rows: Go ackManager.ReadItem rejects 4 after 5.
        let store = Arc::new(crate::matching_fatal::test_support::ScriptedReadStore {
            inner: sqlite,
            script: std::sync::Mutex::new(Some(vec![5, 4])),
        });
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let history = crate::matching_history::MatchingHistoryClient::new(
            channel,
            Duration::from_millis(10),
            1024,
        );
        let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = MatchingRuntime::with_durable_queues(
            crate::handlers::AppState::default(),
            store,
            history,
            NonZeroU32::new(100).unwrap(),
            Duration::from_secs(1),
            Arc::new(|| Duration::ZERO),
        )
        .with_fatal_writer(fatal_tx);
        let key = TaskListKey {
            domain_id: uuid::Uuid::from_u128(11),
            name: "runtime-reader-fatal".into(),
            task_type: 0,
        };
        let handle = runtime
            .registry
            .as_ref()
            .expect("durable runtime registry")
            .get(key.clone(), 0, 1)
            .await
            .unwrap();
        handle
            .enqueue(TaskData::default(), key.domain_id, 0)
            .await
            .unwrap();
        let fatal = tokio::time::timeout(Duration::from_secs(5), fatal_rx.recv())
            .await
            .expect("reader invariant violation never reached the host fatal channel")
            .unwrap();
        assert!(matches!(
            &fatal,
            crate::matching_fatal::MatchingFatal::ReaderInvariant(detail)
                if detail.contains("less than or equal to current read level")
        ));
        runtime.shutdown().await.unwrap();
        assert!(
            fatal_rx.try_recv().is_err(),
            "a single violation reports once"
        );
    }
}
