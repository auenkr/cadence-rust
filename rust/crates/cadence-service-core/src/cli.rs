//! Command line shared by every Cadence service binary.
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cadence-server",
    version,
    about = "Cadence workflow engine (Rust port)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start one or more services (default: all).
    Start {
        /// YAML config file with Cadence keys; defaults to embedded development settings.
        #[arg(long)]
        config: Option<String>,
        /// Process root used to resolve relative dynamic-config paths (Go --root semantics).
        #[arg(long)]
        root: Option<String>,
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
    /// Check a config file parses and validates.
    CheckConfig {
        #[arg(long)]
        config: String,
    },
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
