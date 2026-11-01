use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Unset RUST_LOG keeps the default `warn` level, so nothing new is logged unless it is set.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .init();
    cadence_matching::launcher::run_frontend_with_hook(
        "cadence-server",
        None,
        Box::new(cadence_frontend::workflow_backend::build_workflow_api),
    )
    .await
}
