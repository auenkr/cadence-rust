//! Diagnostic allocator and snapshots, compiled only for explicitly profiled builds.
use std::{ffi::CString, time::Duration};

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub fn start() {
    let Some(directory) = std::env::var_os("CADENCE_HEAP_PROFILE_DIR") else {
        return;
    };
    std::thread::spawn(move || {
        let directory = std::path::PathBuf::from(directory);
        let mut previous = 0;
        for seconds in [180, 1200, 2400, 4000] {
            std::thread::sleep(Duration::from_secs(seconds - previous));
            previous = seconds;
            let path = directory.join(format!("heap-{seconds:04}.heap"));
            let path = CString::new(path.to_string_lossy().as_bytes()).unwrap();
            // jemalloc consumes the NUL-terminated filename synchronously; it does not retain it.
            let result = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr()) };
            let _ = tikv_jemalloc_ctl::epoch::advance();
            let stats = format!(
                "seconds={seconds} dump={result:?} allocated={:?} active={:?} resident={:?}\n",
                tikv_jemalloc_ctl::stats::allocated::read(),
                tikv_jemalloc_ctl::stats::active::read(),
                tikv_jemalloc_ctl::stats::resident::read(),
            );
            eprint!("{stats}");
            let _ = std::fs::write(directory.join(format!("stats-{seconds:04}.txt")), stats);
        }
    });
}
