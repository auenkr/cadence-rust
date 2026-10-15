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
        /// YAML config file with Cadence keys; defaults to embedded development settings.
        #[arg(long)]
        config: Option<String>,
        /// Services to run. Empty means all.
        #[arg(long, value_delimiter = ',')]
        services: Vec<String>,
        /// HTTP port for the JSON API.
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// Cadence protobuf gRPC frontend port.
        #[arg(long)]
        grpc_port: Option<u16>,
        /// Native internal history gRPC port (config history.rpc.grpcPort).
        #[arg(long)]
        history_grpc_port: Option<u16>,
        /// Native internal matching gRPC port (config matching.rpc.grpcPort).
        #[arg(long)]
        matching_grpc_port: Option<u16>,
        /// Native TChannel Thrift frontend port (config rpc.port by default).
        #[arg(long)]
        tchannel_port: Option<u16>,
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
            grpc_port,
            history_grpc_port,
            matching_grpc_port,
            tchannel_port,
            data_dir,
        } => {
            let cfg = match config {
                Some(path) => config::Config::from_file(path)?,
                None => config::Config::development()?,
            };
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
            // `worker.enableScheduler` dynamic config is not ported yet; the
            // static file flag or CADENCE_ENABLE_SCHEDULER=1 enables schedules.
            let semaphore_enabled = cfg.enable_distributed_semaphore
                || std::env::var("CADENCE_ENABLE_SEMAPHORE")
                    .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
            let scheduler_enabled = cfg.enable_scheduler
                || std::env::var("CADENCE_ENABLE_SCHEDULER")
                    .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
            let num_history_shards = if cfg.persistence.num_history_shards > 0 {
                cfg.persistence.num_history_shards
            } else {
                4
            };
            let mut state = handlers::AppState {
                scheduler_enabled,
                semaphore_enabled,
                num_history_shards,
                ..Default::default()
            };
            let metadata = cfg.cluster_metadata.as_ref().expect("validated cluster metadata");
            state.current_cluster_name.clone_from(&metadata.current_cluster_name);
            state.cluster_metadata = metadata.metadata();
            let frontend_rpc = cfg.services.get("frontend").map(|s| &s.rpc);
            if let Some(limit) = frontend_rpc
                .map(|r| r.grpc_max_msg_size)
                .filter(|size| *size > 0)
            {
                state.grpc_max_message_size = limit;
            }
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
                        }
                        Err(e) => eprintln!("warning: ignoring {executions_path:?}: {e}"),
                    }
                }
            }
            // UUID namespace registration also upgrades older name-keyed snapshots.
            state.executions.sync_domains(
                &state
                    .domains
                    .list()
                    .domains
                    .into_iter()
                    .filter_map(|desc| desc.domain_info.map(|info| (info.name, info.uuid)))
                    .collect::<Vec<_>>(),
            );
            // Go's timer queue must keep advancing while development snapshots
            // serialize or write large histories. Never block an async RPC worker
            // with snapshot CPU or filesystem work.
            if let Some((domains_path, executions_path)) = snapshot_paths.clone() {
                let snapshot_store = state.executions.clone();
                let snapshot_domains = state.domains.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        interval.tick().await;
                        let store = snapshot_store.clone();
                        let domains = snapshot_domains.clone();
                        let domains_path = domains_path.clone();
                        let executions_path = executions_path.clone();
                        // Await one write so snapshots cannot overlap or race
                        // their atomic temporary-file rename.
                        if let Err(error) = tokio::task::spawn_blocking(move || {
                            if let Err(error) = domains.save_to_file(domains_path) {
                                eprintln!("warning: snapshot domains failed: {error}");
                            }
                            if let Err(error) = store.save_to_file(executions_path) {
                                eprintln!("warning: snapshot executions failed: {error}");
                            }
                        })
                        .await
                        {
                            eprintln!("warning: snapshot task failed: {error}");
                        }
                    }
                });
            }
            let timer_store = state.executions.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    timer_store.expire_due_workflows(now);
                    timer_store.expire_due_decisions(now);
                    timer_store.fire_due_timers(now);
                    timer_store.expire_due_activities(now);
                    timer_store.requeue_due_activities(now);
                    timer_store.fire_due_backoffs(now);
                    // Scheduler worker: fire due schedule actions into target
                    // workflows, mirroring Go's scheduler workflow ticks.
                    timer_store.fire_due_schedules(now.saturating_mul(1_000_000));
                    // Worker-role async-request consumer, mirroring Go's
                    // async-queue consumer draining into frontend Start calls.
                    for request in timer_store.drain_async_requests() {
                        if !timer_store.consume_async_request(&request) {
                            timer_store.requeue_async_request(request);
                        }
                    }
                }
            });
            let grpc_state = state.clone();
            let history_state = state.clone();
            let matching_state = state.clone();
            let tchannel_state = state.clone();
            let run_frontend = wanted.contains(&service::ServiceName::Frontend);
            let app = if run_frontend {
                handlers::router(state)
            } else {
                handlers::health_router()
            };
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
            let grpc_port = grpc_port
                .or_else(|| frontend_rpc.map(|r| r.grpc_port).filter(|p| *p > 0))
                .unwrap_or(7833);
            let grpc_bind = if frontend_rpc.is_some_and(|r| r.bind_on_local_host) {
                [127, 0, 0, 1]
            } else {
                [0, 0, 0, 0]
            };
            let grpc_addr = std::net::SocketAddr::from((grpc_bind, grpc_port));
            let tchannel_port = tchannel_port
                .or_else(|| frontend_rpc.map(|r| r.port).filter(|p| *p > 0))
                .unwrap_or(7933);
            let tchannel_addr = std::net::SocketAddr::from((grpc_bind, tchannel_port));
            if run_frontend {
                println!("Cadence gRPC frontend listening on {grpc_addr}");
                println!("Cadence TChannel frontend listening on {tchannel_addr}");
            }
            let history_rpc = cfg.services.get("history").map(|s| &s.rpc);
            let history_addr = std::net::SocketAddr::from((
                if history_rpc.is_some_and(|r| r.bind_on_local_host) {
                    [127, 0, 0, 1]
                } else {
                    [0, 0, 0, 0]
                },
                history_grpc_port
                    .or_else(|| history_rpc.map(|r| r.grpc_port).filter(|p| *p > 0))
                    .unwrap_or(7834),
            ));
            let run_history = wanted.contains(&service::ServiceName::History);
            if run_history {
                println!("Cadence internal history gRPC listening on {history_addr}");
            }
            let matching_rpc = cfg.services.get("matching").map(|s| &s.rpc);
            let matching_addr = std::net::SocketAddr::from((
                if matching_rpc.is_some_and(|r| r.bind_on_local_host) {
                    [127, 0, 0, 1]
                } else {
                    [0, 0, 0, 0]
                },
                matching_grpc_port
                    .or_else(|| matching_rpc.map(|r| r.grpc_port).filter(|p| *p > 0))
                    .unwrap_or(7835),
            ));
            let run_matching = wanted.contains(&service::ServiceName::Matching);
            if run_matching {
                println!("Cadence internal matching gRPC listening on {matching_addr}");
            }
            tokio::try_join!(
                async {
                    axum::serve(listener, app)
                        .await
                        .map_err(anyhow::Error::from)
                },
                async {
                    if run_frontend {
                        cadence_server::grpc::serve(grpc_addr, grpc_state).await
                    } else {
                        std::future::pending::<anyhow::Result<()>>().await
                    }
                },
                async {
                    if run_frontend {
                        cadence_server::tchannel::serve(tchannel_addr, tchannel_state).await
                    } else {
                        std::future::pending::<anyhow::Result<()>>().await
                    }
                },
                async {
                    if run_history {
                        cadence_server::grpc_internal::serve(history_addr, history_state).await
                    } else {
                        std::future::pending::<anyhow::Result<()>>().await
                    }
                },
                async {
                    if run_matching {
                        cadence_server::grpc_matching::serve(matching_addr, matching_state).await
                    } else {
                        std::future::pending::<anyhow::Result<()>>().await
                    }
                },
            )?;
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
