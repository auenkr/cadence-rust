#[cfg(feature = "heap-profile")]
mod heap_profile;

#[cfg(feature = "system-heap-profile")]
mod system_heap_profile;

#[cfg(all(
    target_os = "linux",
    target_env = "gnu",
    any(feature = "system-allocator", not(feature = "jemalloc-default"))
))]
mod system_allocator;

#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Let jemalloc return unused pages to the OS from its background purger while
// keeping its normal decay intervals and other allocator defaults.
#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
#[no_mangle]
pub static malloc_conf: &[u8] = b"background_thread:true\0";

use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // Apply glibc's arena cap before Tokio creates its worker threads. The
    // default multi-thread runtime settings match #[tokio::main].
    #[cfg(all(
        target_os = "linux",
        target_env = "gnu",
        any(feature = "system-allocator", not(feature = "jemalloc-default"))
    ))]
    let arena_cap_applied = system_allocator::configure();
    #[cfg(not(all(
        target_os = "linux",
        target_env = "gnu",
        any(feature = "system-allocator", not(feature = "jemalloc-default"))
    )))]
    let arena_cap_applied = true;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(arena_cap_applied))
}

async fn run(_arena_cap_applied: bool) -> anyhow::Result<()> {
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

    #[cfg(all(
        target_os = "linux",
        target_env = "gnu",
        any(feature = "system-allocator", not(feature = "jemalloc-default"))
    ))]
    {
        if !_arena_cap_applied {
            tracing::warn!("glibc rejected M_ARENA_MAX=2; periodic malloc_trim remains enabled");
        }
        system_allocator::start_periodic_trim();
    }

    cadence_matching::launcher::run_frontend_with_hook(
        "cadence-server",
        None,
        Box::new(cadence_frontend::workflow_backend::build_workflow_api),
    )
    .await
}
