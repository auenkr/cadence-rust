//! Command line shared by every Cadence service binary.
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cadence-server",
    version,
    about = "Cadence workflow engine (Rust port)"
)]
pub struct Cli {
    /// Root directory of the execution environment (Go `--root`, `CADENCE_ROOT`).
    #[arg(long, short = 'r', global = true, env = "CADENCE_ROOT")]
    pub root: Option<String>,
    /// Config file, or config dir relative to root or absolute (Go `--config`,
    /// `CADENCE_CONFIG_DIR`); without it `<root>/config` is used when present,
    /// else the embedded development settings.
    #[arg(long, short = 'c', global = true, env = "CADENCE_CONFIG_DIR")]
    pub config: Option<String>,
    /// Environment layer (`<env>.yaml`), Go `--env`, `CADENCE_ENVIRONMENT`.
    #[arg(long, short = 'e', global = true, env = "CADENCE_ENVIRONMENT")]
    pub env: Option<String>,
    /// Zone layer (`<env>_<zone>.yaml`), Go `--zone`/`--az`, `CADENCE_AVAILABILITY_ZONE`.
    #[arg(long, visible_alias = "az", global = true, env = "CADENCE_AVAILABILITY_ZONE")]
    pub zone: Option<String>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start one or more services (default: all).
    Start {
        /// Services to run. Empty means all.
        #[arg(long, short = 's', value_delimiter = ',')]
        services: Vec<String>,
        /// HTTP port for the JSON API (default 8080, none for a Matching-only process).
        #[arg(long)]
        port: Option<u16>,
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
        /// Native internal Matching Thrift/TChannel port (matching.rpc.port).
        #[arg(long)]
        matching_tchannel_port: Option<u16>,
        /// Directory for JSON snapshots (domains.json, executions.json).
        /// Enables restart durability; omitted means purely in-memory.
        #[arg(long)]
        data_dir: Option<String>,
    },
    /// Print API compatibility info.
    Version,
    /// Check the file named by `--config` parses and validates.
    CheckConfig,
}

impl Cli {
    /// Parse process arguments with `bin_name` as the displayed binary name.
    pub fn parse_as(bin_name: &'static str) -> Self {
        let matches = Self::command().name(bin_name).get_matches();
        Self::from_arg_matches(&matches).unwrap_or_else(|e| e.exit())
    }
}

/// Entry point of a service binary whose Rust port does not exist yet: parse
/// the same arguments as the Go binary, then fail with a clear message.
pub fn not_migrated(bin_name: &'static str, service: &str) -> anyhow::Result<()> {
    let _ = Cli::parse_as(bin_name);
    anyhow::bail!("service not migrated yet: the Rust {service} service is not implemented; run the Go {service} service instead")
}
