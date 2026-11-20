#[cfg(feature = "jemalloc-default")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// tikv-jemallocator builds with the _rjem_ prefix. Keep the config at a
// C-compatible pointer so jemalloc can read it before the runtime starts.
#[cfg(feature = "jemalloc-default")]
const MALLOC_CONF: &[u8; 53] = b"background_thread:true,narenas:2,dirty_decay_ms:1000\0";

#[cfg(feature = "jemalloc-default")]
#[no_mangle]
pub static _rjem_malloc_conf: &[u8; MALLOC_CONF.len()] = MALLOC_CONF;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .init();
    cadence_history::launcher::run("cadence-history").await
}
