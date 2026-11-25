#[cfg(feature = "jemalloc-default")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemalloc-default")]
const JEMALLOC_CONFIG: &[u8] = b"background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:1,tcache_max:4096\0";

// tikv-jemalloc-sys prefixes its symbols. One arena (small tcache, fast decay) lets the main and runtime
// workers on a one-CPU History host share a small number of pools.
// Deployments can override this via _RJEM_MALLOC_CONF.
#[cfg(feature = "jemalloc-default")]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &u8 = &JEMALLOC_CONFIG[0];

mod mem_report;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // Tokio defaults to available_parallelism, which accounts for Linux CPU
    // quotas, and honors an explicit TOKIO_WORKER_THREADS override.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Default is 512 blocking threads kept alive 10s; each retains a stack
        // and allocator cache. Cap and shorten idle life for a small host.
        .max_blocking_threads(64)
        .thread_keep_alive(std::time::Duration::from_secs(2))
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    mem_report::start();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // CADENCE_TRACE_DECISION=1 additionally enables the info-level decision timing trace.
        if cadence_history::decision_trace::enabled() {
            EnvFilter::new("warn,decision_trace=info")
        } else {
            EnvFilter::new("warn")
        }
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .init();
    cadence_history::launcher::run("cadence-history").await
}
