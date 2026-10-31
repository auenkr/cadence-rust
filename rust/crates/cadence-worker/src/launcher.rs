//! CLI and config entry point for the standalone Worker binary.

use cadence_service_core::cli::{Cli, Command};

fn load_config(cli: &Cli) -> anyhow::Result<cadence_service_core::config::Config> {
    let root = cli
        .root
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let config_path = cli.config.as_deref().map(|path| {
        let path = std::path::Path::new(path);
        if path.is_relative() {
            root.join(path)
        } else {
            path.to_path_buf()
        }
    });
    match config_path {
        Some(path) if path.is_dir() => {
            let env = cli.env.as_deref().unwrap_or("development");
            let zone = cli.zone.as_deref().unwrap_or("");
            cadence_service_core::config::Config::from_layers(
                path.to_str().ok_or_else(|| anyhow::anyhow!("config path is not UTF-8"))?,
                env,
                zone,
            )
        }
        Some(path) => cadence_service_core::config::Config::from_file(path),
        None => cadence_service_core::config::Config::development(),
    }
}

pub fn run(bin_name: &'static str) -> anyhow::Result<()> {
    let cli = Cli::parse_as(bin_name);
    match &cli.command {
        Command::Version => {
            println!("cadence-worker {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::CheckConfig => {
            let config = load_config(&cli)?;
            config.validate()?;
            println!("configuration is valid");
            Ok(())
        }
        Command::Start { services, .. } => {
            if !services.is_empty() && !services.iter().any(|service| service == "worker") {
                anyhow::bail!("cadence-worker can only start the worker service")
            }
            let config = load_config(&cli)?;
            config.validate()?;
            crate::service::WorkerService::default().start()
        }
    }
}
