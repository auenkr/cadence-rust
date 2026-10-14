use cadence_server::{config, handlers, service};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cadence-server",
    version,
    about = "Cadence workflow engine (Rust port)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start one or more services (default: all).
    Start {
        /// Path to the YAML config file (same keys as go/config/*.yaml).
        #[arg(long, default_value = "go/config/development.yaml")]
        config: String,
        /// Services to run. Empty means all.
        #[arg(long, value_delimiter = ',')]
        services: Vec<String>,
        /// HTTP port for the JSON API.
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// Directory for JSON snapshots (domains.json, executions.json).
        /// Enables restart durability; omitted means purely in-memory.
        #[arg(long)]
        data_dir: Option<String>,
    },
    /// Print API compatibility info.
    Version,
    /// Check a config file parses and validates.
    CheckConfig {
        #[arg(long)]
        config: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Start {
            config,
            services,
            port,
            data_dir,
        } => {
            let cfg = config::Config::from_file(&config)?;
            cfg.validate()?;
            let wanted: Vec<service::ServiceName> = if services.is_empty() {
                service::ServiceName::all().to_vec()
            } else {
                services
                    .iter()
                    .map(|s| {
                        service::ServiceName::parse(s)
                            .ok_or_else(|| anyhow::anyhow!("unknown service {s:?}"))
                    })
                    .collect::<Result<_, _>>()?
            };
            let mut registry = service::ServiceRegistry::new();
            for s in &wanted {
                println!("starting {s} (api {})", cadence_api::API_COMPAT_VERSION);
                registry.mark_healthy(*s);
            }
            let mut state = handlers::AppState::default();
            // Optional file durability: load snapshots, then persist every
            // 5 seconds. Crash losses are bounded by the interval.
            let snapshot_paths: Option<(std::path::PathBuf, std::path::PathBuf)> =
                data_dir.as_ref().map(|dir| {
                    let dir = std::path::PathBuf::from(dir);
                    (dir.join("domains.json"), dir.join("executions.json"))
                });
            if let Some((ref domains_path, ref executions_path)) = snapshot_paths {
                if let Some(parent) = domains_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                if domains_path.exists() {
                    match cadence_persistence::InMemoryDomainStore::load_from_file(domains_path) {
                        Ok(loaded) => state.domains = loaded,
                        Err(e) => eprintln!("warning: ignoring {domains_path:?}: {e}"),
                    }
                }
                if executions_path.exists() {
                    match cadence_persistence::InMemoryExecutionStore::load_from_file(
                        executions_path,
                    ) {
                        Ok(loaded) => {
                            state.executions = loaded;
                            // Re-announce restored domains for decision validation.
                            for desc in state.domains.list().domains {
                                if let Some(info) = desc.domain_info {
                                    state.executions.register_domain_name(&info.name);
                                }
                            }
                        }
                        Err(e) => eprintln!("warning: ignoring {executions_path:?}: {e}"),
                    }
                }
            }
            // Background engine: fire due timers and requeue activity
            // retries like Go history's timer queue, plus periodic snapshots.
            let timer_store = state.executions.clone();
            let snapshot_store = state.executions.clone();
            let snapshot_domains = state.domains.clone();
            let snapshot_paths_bg = snapshot_paths.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                let mut ticks: u64 = 0;
                loop {
                    interval.tick().await;
                    ticks += 1;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    timer_store.fire_due_timers(now);
                    timer_store.requeue_due_activities(now);
                    // Snapshot every ~5s when --data-dir is set.
                    if ticks.is_multiple_of(10) {
                        if let Some((ref domains_path, ref executions_path)) = snapshot_paths_bg
                        {
                            if let Err(e) =
                                snapshot_domains.save_to_file(domains_path)
                            {
                                eprintln!("warning: snapshot domains failed: {e}");
                            }
                            if let Err(e) =
                                snapshot_store.save_to_file(executions_path)
                            {
                                eprintln!("warning: snapshot executions failed: {e}");
                            }
                        }
                    }
                }
            });
            let app = handlers::router(state);
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            println!(
                "cadence-server running on {addr}: {}",
                wanted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
            println!("shutting down");
            let _ = registry;
        }
        Command::Version => {
            println!("cadence-server (rust) {}", cadence_api::API_COMPAT_VERSION);
        }
        Command::CheckConfig { config } => {
            let cfg = config::Config::from_file(&config)?;
            cfg.validate()?;
            println!("config OK: {}", config);
        }
    }
    Ok(())
}
