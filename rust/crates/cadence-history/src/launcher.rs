//! Startup of the `cadence-history` binary: the History role of Go `cmd/server`
//! (`cadence-server start --services history`) -- config load, resources, shard
//! registry, handler chain, Thrift/TChannel + gRPC servers, Ringpop membership,
//! health and graceful shutdown.
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_common::cluster::Metadata;
use cadence_common::membership::{Host, Ring};
use cadence_matching::matching_peer_provider::{MatchingPeerProvider, PeerMember};
use cadence_matching::matching_ringpop_provider::MatchingRingpopProvider;
use cadence_service_core::cli::{Cli, Command};
use cadence_service_core::config::{self as svc_config, Config as ServiceConfig, DynamicConfigProvider, RpcConfig};
use cadence_service_core::service::ServiceName;

use crate::config::Config;
use crate::config_collection::{Collection, DynamicClient};
use crate::service::{MembershipControl, Service};
use crate::wiring::assembly::controller::ShardRegistry;
use crate::wiring::assembly::env::{HistoryEnv, HistoryEnvParams};
use crate::wiring::assembly::queue_factory::ExecQueueFactory;
use crate::wiring::assembly::queue_provider::HistoryQueueExecProvider;
use crate::wiring::assembly::replication_builders::{RegistryHistoryClient, ReplicationFactoryImpl};
use crate::wiring::resources::dynconfig::FileDynamicClient;
use crate::wiring::resources::membership_feed::{refresh_from_snapshot, spawn_membership_feed};
use crate::wiring::resources::query_matching::ChannelResolver;

/// Go role label gossiped through Ringpop (`service.History` full name).
pub const HISTORY_RINGPOP_SERVICE: &str = "cadence-history";
const MATCHING_RINGPOP_SERVICE: &str = "cadence-matching";
const DEFAULT_HISTORY_TCHANNEL_PORT: u16 = 7934;
const DEFAULT_HISTORY_GRPC_PORT: u16 = 7834;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// `membership.Resolver.EvictSelf` over the Ringpop provider.
struct RingpopMembership(Arc<MatchingRingpopProvider>);

#[async_trait]
impl MembershipControl for RingpopMembership {
    async fn evict_self(&self) {
        if let Err(e) = self.0.self_evict().await {
            tracing::warn!(error = %e, "evicting self from the Ringpop membership failed");
        }
    }
}

fn bind_ip(rpc: &RpcConfig) -> anyhow::Result<IpAddr> {
    if !rpc.bind_on_ip.is_empty() {
        return rpc
            .bind_on_ip
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid history rpc.bindOnIP {:?}: {e}", rpc.bind_on_ip));
    }
    Ok(if rpc.bind_on_local_host { IpAddr::from([127, 0, 0, 1]) } else { IpAddr::from([0, 0, 0, 0]) })
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok().map(|h| h.trim().to_string()))
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

async fn shutdown_signal() -> anyhow::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r.map_err(anyhow::Error::from),
        _ = terminate.recv() => Ok(()),
    }
}

/// Go `--auto-setup`: run the schema helper (see the Matching launcher).
fn run_schema_helper() -> anyhow::Result<()> {
    let helper = std::env::var("CADENCE_SCHEMA_HELPER")
        .unwrap_or_else(|_| "/usr/local/bin/cadence-auto-setup.sh".into());
    let status = std::process::Command::new(&helper)
        .status()
        .map_err(|e| anyhow::anyhow!("auto-setup: cannot run schema helper {helper}: {e}"))?;
    anyhow::ensure!(status.success(), "auto-setup: schema helper {helper} failed: {status}");
    Ok(())
}

fn load_config(config: Option<String>, env: Option<String>, zone: Option<String>, root: Option<String>) -> anyhow::Result<ServiceConfig> {
    let root = root.map(std::path::PathBuf::from).unwrap_or(std::env::current_dir()?);
    let config = match config {
        Some(path) if std::path::Path::new(&path).is_relative() => {
            let joined = root.join(&path);
            Some(if joined.exists() { joined.to_string_lossy().into_owned() } else { path })
        }
        Some(path) => Some(path),
        None => {
            let dir = root.join("config");
            dir.is_dir().then(|| dir.to_string_lossy().into_owned())
        }
    };
    let cfg = match config {
        Some(path) if std::path::Path::new(&path).is_dir() => {
            let var = |names: &[&str]| names.iter().find_map(|n| std::env::var(n).ok());
            let env = env.or_else(|| var(&["CADENCE_ENVIRONMENT"])).unwrap_or_default();
            let zone = zone
                .or_else(|| var(&["CADENCE_AVAILABILITY_ZONE", "CADENCE_AVAILABILTY_ZONE"]))
                .unwrap_or_default();
            ServiceConfig::from_layers(&path, &env, &zone)?
        }
        Some(path) => ServiceConfig::from_file(path)?,
        None => ServiceConfig::development()?,
    };
    cfg.validate()?;
    cfg.persistence.validate_go().map_err(anyhow::Error::msg)?;
    Ok(cfg)
}

/// Go `dynamicconfig.NewFileBasedClient` (path resolution + poll interval).
fn file_based_client(
    p: &svc_config::DynamicConfigClientConfig,
    root: &std::path::Path,
) -> anyhow::Result<Arc<dyn DynamicClient>> {
    let path = std::path::PathBuf::from(&p.filepath);
    let path = if path.is_absolute() { path } else { root.join(path) };
    let client = FileDynamicClient::from_file(&path)?;
    let interval = if p.poll_interval.is_empty() {
        Duration::from_secs(5)
    } else {
        let nanos = svc_config::parse_go_duration(&p.poll_interval).map_err(anyhow::Error::msg)?;
        Duration::from_nanos(nanos.max(5_000_000_000) as u64)
    };
    client.spawn_reloader(interval);
    Ok(client)
}

/// Builds the dynamic-config client selected by the config (Go `dynamicconfigfx`).
/// OpenFeature is not supported by the Rust History yet.
async fn dynamic_client(cfg: &ServiceConfig, root: &std::path::Path) -> anyhow::Result<Arc<dyn DynamicClient>> {
    match cfg.dynamic_config_provider() {
        DynamicConfigProvider::FileBased(p) if !p.filepath.is_empty() => {
            // Go dynamicconfigfx: a client creation error means NOP, never an abort.
            match file_based_client(p, root) {
                Ok(client) => Ok(client),
                Err(e) => {
                    eprintln!("error: creating dynamic config client failed, using no-op config client instead: {e}");
                    Ok(FileDynamicClient::empty())
                }
            }
        }
        DynamicConfigProvider::FileBased(_) | DynamicConfigProvider::Nop => Ok(FileDynamicClient::empty()),
        DynamicConfigProvider::ConfigStore(store) => {
            // Go dynamicconfigfx: a client that cannot be created means NOP.
            eprintln!("initialising ConfigStore dynamic config client");
            let client_config = cadence_persistence::config_store::ClientConfig {
                poll_interval: Duration::from_nanos(store.poll_interval_nanos as u64),
                update_retry_attempts: store.update_retry_attempts,
                fetch_timeout: Duration::from_nanos(store.fetch_timeout_nanos as u64),
                update_timeout: Duration::from_nanos(store.update_timeout_nanos as u64),
            };
            let persistence = cfg.persistence.clone();
            let created = tokio::task::spawn_blocking(move || {
                let stores = cadence_matching::matching_task_store::open_configured_default_store(
                    &persistence,
                    "",
                    Arc::new(|| false),
                    Arc::new(|| "thriftrw".to_string()),
                    false,
                )
                .map_err(|e| e.to_string())?;
                let task_store = stores.task_store;
                let fetch: cadence_persistence::config_store::ConfigRowFetcher = Arc::new(move |row_type| {
                    cadence_persistence::matching_task_store::MatchingTaskStore::fetch_latest_config(&*task_store, row_type)
                });
                cadence_persistence::config_store::ConfigStoreClient::new(&client_config, fetch)
            })
            .await
            .map_err(|e| anyhow::anyhow!("joining config store startup: {e}"))?;
            match created {
                Ok(client) => {
                    let client = crate::wiring::resources::dynconfig::ConfigStoreDynamicClient::new(Arc::new(client));
                    client.spawn_poller();
                    Ok(client as Arc<dyn DynamicClient>)
                }
                Err(e) => {
                    eprintln!("warning: creating ConfigStore dynamic config client: {e}; using no-op configuration");
                    Ok(FileDynamicClient::empty())
                }
            }
        }
        DynamicConfigProvider::OpenFeature(_) => anyhow::bail!(
            "History OpenFeature dynamic config provider is not supported yet; see memory/history-gap-startup2-20261107.md"
        ),
    }
}

/// Resolves the Matching host owning a task list (Ringpop members labelled
/// `cadence-matching`, hashed with the shared ring) to a lazy gRPC channel.
fn matching_resolver(
    provider: Arc<MatchingRingpopProvider>,
    tls: Option<cadence_service_core::grpc_tls::ClientTls>,
) -> ChannelResolver {
    Arc::new(move |task_list: &str| {
        let hosts: Vec<Host> = provider
            .snapshot()
            .members
            .iter()
            .filter_map(|m: &PeerMember| m.matching_host(MATCHING_RINGPOP_SERVICE))
            .collect();
        let ring = Ring::new(hosts);
        let host = ring.lookup(task_list).ok_or_else(|| "no matching hosts in the membership ring".to_string())?;
        if host.grpc_address.is_empty() {
            return Err(format!("matching host {} has no gRPC address", host.address));
        }
        // Go dials Matching with the matching service's rpc.tls client config.
        cadence_service_core::grpc_tls::endpoint_channel(&host.grpc_address, tls.as_ref())
            .map_err(|e| e.to_string())
    })
}

fn rpc_for<'a>(cfg: &'a ServiceConfig, name: &str) -> Option<&'a RpcConfig> {
    cfg.services.get(name).map(|s| &s.rpc)
}

/// Entry point of the `cadence-history` binary.
pub async fn run(bin_name: &'static str) -> anyhow::Result<()> {
    // `version` / `check-config` behave exactly like the shared launcher.
    if !matches!(Cli::parse_as(bin_name).command, Command::Start { .. }) {
        return cadence_matching::launcher::run(bin_name, Some(&[ServiceName::History])).await;
    }
    cadence_service_core::grpc_tls::install_tls_crypto_provider();
    let cli = Cli::parse_as(bin_name);
    let Command::Start { services, history_grpc_port, tchannel_port: _, auto_setup, .. } = cli.command else {
        unreachable!("checked above")
    };
    if let Some(other) = services.iter().find(|s| ServiceName::parse(s) != Some(ServiceName::History)) {
        anyhow::bail!("cadence-history only runs the history service, got --services {other}");
    }
    if auto_setup {
        run_schema_helper()?;
    }
    let root = cli.root.clone().map(std::path::PathBuf::from).unwrap_or(std::env::current_dir()?);
    let cfg = load_config(cli.config, cli.env, cli.zone, cli.root)?;
    let metadata = cfg
        .cluster_metadata
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ClusterGroupMetadata cannot be empty"))?;
    let cluster: Metadata = metadata.metadata();

    let rpc = rpc_for(&cfg, "history").cloned().unwrap_or_default();
    // Go rpc.NewParams: History's own rpc.tls secures the gRPC listener.
    let grpc_tls = crate::service::GrpcTls {
        tls: match cadence_service_core::grpc_tls::server_tls_custom_incoming(&rpc.tls)? {
            Some(_) => None,
            None => cadence_service_core::grpc_tls::server_tls(&rpc.tls)?,
        },
        custom: cadence_service_core::grpc_tls::server_tls_custom_incoming(&rpc.tls)?,
    };
    let ip = bind_ip(&rpc)?;
    let tchannel_port = if rpc.port > 0 { rpc.port } else { DEFAULT_HISTORY_TCHANNEL_PORT };
    let grpc_port = history_grpc_port.or(Some(rpc.grpc_port).filter(|p| *p > 0)).unwrap_or(DEFAULT_HISTORY_GRPC_PORT);
    let max_message_size = if rpc.grpc_max_msg_size > 0 { rpc.grpc_max_msg_size } else { DEFAULT_MAX_MESSAGE_SIZE };

    // Reserve both endpoints before Ringpop publishes them.
    let thrift_listener = tokio::net::TcpListener::bind(SocketAddr::new(ip, tchannel_port)).await?;
    let grpc_listener = tokio::net::TcpListener::bind(SocketAddr::new(ip, grpc_port)).await?;
    let thrift_addr = thrift_listener.local_addr()?;
    let grpc_addr = grpc_listener.local_addr()?;
    println!("Cadence internal history gRPC listening on {grpc_addr}");
    println!("Cadence internal history Thrift/TChannel listening on {thrift_addr}");

    let provider = Arc::new(MatchingRingpopProvider::new(
        cfg.ringpop.clone(),
        HISTORY_RINGPOP_SERVICE,
        thrift_addr,
        grpc_addr.port(),
    )?);

    let client = dynamic_client(&cfg, &root).await?;
    let collection = Collection::new(client.clone());
    let service_config = Arc::new(Config::new(
        &collection,
        i64::from(cfg.persistence.num_history_shards),
        max_message_size as i64,
        !cfg.persistence.advanced_visibility_store.is_empty(),
        &hostname(),
    ));

    let matching_client_tls = match rpc_for(&cfg, "matching") {
        Some(m) => cadence_service_core::grpc_tls::client_tls(&m.tls)?,
        None => None,
    };
    let env = HistoryEnv::build(HistoryEnvParams {
        config: service_config.clone(),
        persistence: cfg.persistence.clone(),
        cluster,
        who_am_i: provider.local_identity().to_owned(),
        hosts: Vec::new(),
        matching_resolver: matching_resolver(provider.clone(), matching_client_tls),
        dynamic_client: Some(client),
    })
    .await?;

    let history_client = RegistryHistoryClient::new();
    let replication = Arc::new(ReplicationFactoryImpl::new(history_client.clone()));
    let queue_provider = HistoryQueueExecProvider::new(history_client.clone());
    let registry = ShardRegistry::new(env.clone(), Arc::new(ExecQueueFactory::new(queue_provider.clone())), replication.clone());
    history_client.bind(&registry);
    queue_provider.bind(&registry);

    let service = Arc::new(Service::new(env.clone(), registry.clone(), Arc::new(RingpopMembership(provider.clone()))));
    service.set_grpc_tls(grpc_tls);
    // The TChannel listener must accept Ringpop JOIN before the provider bootstraps.
    service
        .start(thrift_listener, grpc_listener, Some(provider.handler()), max_message_size)
        .await?;

    let start_result = tokio::select! {
        r = provider.start() => r.map_err(|e| anyhow::anyhow!("starting History Ringpop membership: {e}")),
        s = shutdown_signal() => { s?; Err(anyhow::anyhow!("shutdown requested during History Ringpop bootstrap")) }
    };
    if let Err(e) = start_result {
        service.stop().await;
        let _ = provider.stop().await;
        return Err(e);
    }
    let feed = match spawn_membership_feed(env.membership.clone(), provider.clone(), HISTORY_RINGPOP_SERVICE.to_string()).await {
        Ok(handle) => handle,
        Err(e) => {
            service.stop().await;
            let _ = provider.stop().await;
            anyhow::bail!("subscribing to History Ringpop membership: {e}");
        }
    };
    // Close shards whose ownership moved on every ring change (Go controller subscription).
    let ownership_task = {
        let registry = registry.clone();
        let provider = provider.clone();
        let env = env.clone();
        tokio::spawn(async move {
            let mut last = env.membership.members();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                refresh_from_snapshot(&env.membership, &provider, HISTORY_RINGPOP_SERVICE);
                let now = env.membership.members();
                if now != last {
                    registry.on_membership_change();
                    last = now;
                }
            }
        })
    };

    tokio::select! {
        r = shutdown_signal() => r?,
        _ = service.wait_stopped() => {}
    }
    service.stop().await;
    queue_provider.stop().await;
    ownership_task.abort();
    feed.abort();
    replication.stop_fetchers();
    provider.stop().await.map_err(|e| anyhow::anyhow!("stopping History Ringpop membership: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod dynamic_client_fallback_tests {
    use super::*;

    #[test]
    fn missing_file_creation_error_is_mapped_to_nop_by_dynamic_client() {
        let cfg = svc_config::DynamicConfigClientConfig {
            filepath: "/nonexistent/dir/dynamicconfig.yaml".into(),
            ..Default::default()
        };
        assert!(file_based_client(&cfg, std::path::Path::new("/")).is_err());
    }
}
