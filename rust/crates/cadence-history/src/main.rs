#[cfg(feature = "jemalloc-default")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemalloc-default")]
#[no_mangle]
pub static malloc_conf: &[u8] = b"background_thread:true\0";

use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .init();

    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    // History uses synchronous SQL. A second scheduler worker can run ready
    // tasks while the first worker hands off its queue to enter blocking SQL.
    if std::env::var_os("TOKIO_WORKER_THREADS").is_none()
        && std::thread::available_parallelism().map_or(1, |count| count.get()) == 1
    {
        runtime.worker_threads(2);
    }
    runtime
        .enable_all()
        .build()?
        .block_on(cadence_history::launcher::run("cadence-history"))
}
