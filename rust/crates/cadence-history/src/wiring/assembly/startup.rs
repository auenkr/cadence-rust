//! History service process startup (Go `cmd/server` + `service/history` fx wiring).
//!
//! Loads config like `cadence-matching`'s launcher, builds the service
//! environment, serves Thrift + gRPC and shuts down gracefully on SIGTERM/SIGINT.
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use cadence_service_core::cli::{Cli, Command};
use cadence_service_core::config::{Config, RpcConfig};

use super::handler::{EngineRouter, HistoryHandler};
use crate::handler::interface::Handler;
use crate::wiring::server::{HistoryServer, ServerConfig};

/// Everything the running service needs from the environment.
pub struct Environment {
    pub router: Arc<dyn EngineRouter>,
    /// Called once at shutdown after the server stopped (`registry.close_all`).
    pub close_all: Box<dyn Fn() + Send + Sync>,
}

/// TODO(D1): replace by `HistoryEnv::build` (shard registry, persistence, membership ringpop join,
/// matching resolver, domain cache) once unit D1 is merged.
pub fn build_env(_cfg: &Config) -> anyhow::Result<Environment> {
    anyhow::bail!("history environment wiring (HistoryEnv::build) is not merged yet")
}

fn bind_ip(rpc: &RpcConfig) -> anyhow::Result<IpAddr> {
    if rpc.bind_on_local_host && !rpc.bind_on_ip.is_empty() {
        anyhow::bail!("bindOnLocalHost and bindOnIP are mutually exclusive");
    }
    if rpc.bind_on_local_host {
        return Ok(Ipv4Addr::LOCALHOST.into());
    }
    if !rpc.bind_on_ip.is_empty() {
        return rpc
            .bind_on_ip
            .parse()
            .map_err(|_| anyhow::anyhow!("unable to parse bindOnIP value or it is not an IPv4 or IPv6 address: {}", rpc.bind_on_ip));
    }
    Ok(Ipv4Addr::UNSPECIFIED.into())
}

/// `services.history.rpc` -> server config.
pub fn server_config(cfg: &Config) -> anyhow::Result<ServerConfig> {
    let rpc = cfg.services.get("history").map(|s| s.rpc.clone()).unwrap_or_default();
    if rpc.port == 0 || rpc.grpc_port == 0 {
        anyhow::bail!("services.history.rpc port and grpcPort must be set");
    }
    let ip = bind_ip(&rpc)?;
    let mut sc = ServerConfig::new(SocketAddr::new(ip, rpc.port), SocketAddr::new(ip, rpc.grpc_port));
    if rpc.grpc_max_msg_size > 0 {
        sc.max_message_size = rpc.grpc_max_msg_size;
    }
    Ok(sc)
}

fn load_config(cli_config: Option<String>, env: Option<String>, zone: Option<String>, root: Option<String>) -> anyhow::Result<Config> {
    let process_root = root.map(PathBuf::from).unwrap_or(std::env::current_dir()?);
    let config = match cli_config {
        Some(path) if std::path::Path::new(&path).is_relative() => {
            let joined = process_root.join(&path);
            Some(if joined.exists() { joined.to_string_lossy().into_owned() } else { path })
        }
        Some(path) => Some(path),
        None => {
            let default_dir = process_root.join("config");
            default_dir.is_dir().then(|| default_dir.to_string_lossy().into_owned())
        }
    };
    let cfg = match config {
        Some(path) if std::path::Path::new(&path).is_dir() => {
            let var = |names: &[&str]| names.iter().find_map(|n| std::env::var(n).ok());
            let env = env.or_else(|| var(&["CADENCE_ENVIRONMENT"])).unwrap_or_default();
            let zone = zone.or_else(|| var(&["CADENCE_AVAILABILITY_ZONE", "CADENCE_AVAILABILTY_ZONE"])).unwrap_or_default();
            Config::from_layers(&path, &env, &zone)?
        }
        Some(path) => Config::from_file(path)?,
        None => Config::development()?,
    };
    cfg.validate()?;
    cfg.persistence.validate_go().map_err(anyhow::Error::msg)?;
    Ok(cfg)
}

async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

/// Start the history service and block until a termination signal.
pub async fn run(bin_name: &'static str) -> anyhow::Result<()> {
    cadence_service_core::grpc_tls::install_tls_crypto_provider();
    let cli = Cli::parse_as(bin_name);
    let (config, env, zone, root) = (cli.config, cli.env, cli.zone, cli.root);
    match cli.command {
        Command::Start { .. } => {}
        #[allow(unreachable_patterns)]
        _ => anyhow::bail!("unsupported command for {bin_name}"),
    }
    let cfg = load_config(config, env, zone, root)?;
    let environment = build_env(&cfg)?;
    let handler = Arc::new(HistoryHandler::new(environment.router.clone()));
    let server = HistoryServer::start(server_config(&cfg)?, handler.clone() as Arc<dyn Handler>).await?;
    tracing::info!(thrift = %server.thrift_addr, grpc = %server.grpc_addr, "history service started");
    wait_for_signal().await;
    tracing::info!("history service shutting down");
    // handler.prepare_to_stop -> stop listeners -> handler.stop
    server.shutdown().await;
    (environment.close_all)();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_requires_ports() {
        let cfg = Config::development().unwrap();
        // development config may or may not define history ports; either outcome must not panic.
        let _ = server_config(&cfg);
    }

    #[test]
    fn build_env_is_stubbed() {
        let cfg = Config::development().unwrap();
        assert!(build_env(&cfg).is_err());
    }
}
