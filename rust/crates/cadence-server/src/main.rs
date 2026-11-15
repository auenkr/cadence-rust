#[cfg(feature = "heap-profile")]
mod heap_profile;

#[cfg(feature = "system-heap-profile")]
mod system_heap_profile;

#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Let jemalloc return unused pages to the OS from its background purger while
// keeping its normal decay intervals and other allocator defaults.
#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
#[no_mangle]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "heap-profile")]
    heap_profile::start();
    #[cfg(feature = "system-heap-profile")]
    system_heap_profile::start();

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
