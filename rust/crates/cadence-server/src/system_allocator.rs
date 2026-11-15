//! Small glibc retention controls for the default System allocator.
//!
//! The soak's anonymous mappings are 64 MiB aligned, matching glibc secondary
//! arena heaps. Limiting arenas before Tokio starts avoids per-worker arena
//! proliferation; periodic trimming returns free pages from all arenas.

use std::time::Duration;

const M_ARENA_MAX: libc::c_int = -8;
const ARENA_LIMIT: libc::c_int = 2;

unsafe extern "C" {
    fn mallopt(param: libc::c_int, value: libc::c_int) -> libc::c_int;
    fn malloc_trim(pad: libc::size_t) -> libc::c_int;
}

/// Set the arena cap before Tokio creates worker threads.
pub fn configure() -> bool {
    // SAFETY: mallopt is process-wide and M_ARENA_MAX is a glibc-supported
    // integer option. It is called once before the service runtime starts.
    unsafe { mallopt(M_ARENA_MAX, ARENA_LIMIT) != 0 }
}

/// Ask glibc to return free pages to the OS periodically while the service is
/// active. malloc_trim is thread-safe and scans the process's malloc arenas.
pub fn start_periodic_trim() {
    tokio::spawn(async {
        let interval = Duration::from_secs(60);
        loop {
            tokio::time::sleep(interval).await;
            // SAFETY: malloc_trim(0) is the documented glibc API. It is safe
            // to invoke while other threads allocate or release memory.
            unsafe {
                malloc_trim(0);
            }
        }
    });
}
