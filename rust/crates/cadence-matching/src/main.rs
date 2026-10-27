use cadence_service_core::service::ServiceName;
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
    match cadence_matching::launcher::run("cadence-matching", Some(&[ServiceName::Matching])).await {
        Err(error) if error.downcast_ref::<cadence_matching::launcher::SqlMatchingWriterFatal>().is_some() => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
        // Go zap Logger.Fatal (task reader invariant) exits with status 1.
        Err(error) if error.downcast_ref::<cadence_matching::launcher::MatchingReaderFatal>().is_some() => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
        result => result,
    }
}
