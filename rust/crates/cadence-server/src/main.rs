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

// tikv-jemalloc-sys builds jemalloc with the `_rjem_` symbol prefix, so the config must be exported as
// `_rjem_malloc_conf` (the unprefixed `malloc_conf` was never read). Same small-footprint settings as
// cadence-history: background purge, two arenas, small tcache, fast dirty-page decay.
// Deployments can override this via _RJEM_MALLOC_CONF.
#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
const JEMALLOC_CONFIG: &[u8] =
    b"background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:2,tcache_max:4096\0";

#[cfg(all(feature = "jemalloc-default", not(feature = "system-allocator")))]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &u8 = &JEMALLOC_CONFIG[0];

use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // mysql's global buffer pool keeps up to 128 buffers of up to 4 MiB each alive for the process lifetime
    // (Go's driver pools nothing). Bound it before the first connection; explicit env values still win.
    for (k, v) in [("RUST_MYSQL_BUFFER_POOL_CAP", "16"), ("RUST_MYSQL_BUFFER_SIZE_CAP", "32768")] {
        if std::env::var_os(k).is_none() {
            std::env::set_var(k, v);
        }
    }
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
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // CADENCE_TRACE_DECISION=1 additionally enables the info-level decision timing trace.
        if std::env::var("CADENCE_TRACE_DECISION").map(|v| v == "1" || v == "true").unwrap_or(false) {
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
