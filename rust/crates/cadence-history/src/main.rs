#[cfg(feature = "jemalloc-default")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemalloc-default")]
const JEMALLOC_CONFIG: &[u8] = b"background_thread:true,dirty_decay_ms:1000,narenas:2\0";

// tikv-jemalloc-sys prefixes its symbols. Two arenas let the main and runtime
// workers on a one-CPU History host share a small number of pools.
// Deployments can override this via _RJEM_MALLOC_CONF.
#[cfg(feature = "jemalloc-default")]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &u8 = &JEMALLOC_CONFIG[0];

use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // Tokio defaults to available_parallelism, which accounts for Linux CPU
    // quotas, and honors an explicit TOKIO_WORKER_THREADS override.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .init();
    cadence_history::launcher::run("cadence-history").await
}
