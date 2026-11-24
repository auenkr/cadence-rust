//! Opt-in diagnostic (CADENCE_MEM_REPORT=1): every 10s print allocator stats, thread
//! counts by name and process RSS to stderr. No effect when the variable is unset.
use std::{collections::BTreeMap, time::Duration};

pub fn start() {
    if std::env::var_os("CADENCE_MEM_REPORT").is_none() {
        return;
    }
    let _ = std::thread::Builder::new().name("mem-report".into()).spawn(|| loop {
        std::thread::sleep(Duration::from_secs(10));
        eprintln!("{}", report());
        #[cfg(feature = "heap-profile")]
        dump_heap();
    });
}

fn report() -> String {
    let mut out = String::from("MEMREPORT");
    #[cfg(feature = "jemalloc-default")]
    {
        use tikv_jemalloc_ctl::stats;
        let _ = tikv_jemalloc_ctl::epoch::advance();
        out += &format!(
            " allocated={:?} active={:?} resident={:?} metadata={:?} mapped={:?} retained={:?}",
            stats::allocated::read(),
            stats::active::read(),
            stats::resident::read(),
            stats::metadata::read(),
            stats::mapped::read(),
            stats::retained::read()
        );
    }
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if ["Threads:", "VmRSS:", "RssAnon:"].iter().any(|k| line.starts_with(k)) {
                out += &format!(" {}", line.split_whitespace().collect::<Vec<_>>().join("="));
            }
        }
    }
    let mut names: BTreeMap<String, usize> = BTreeMap::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
        for t in dir.flatten() {
            let name = std::fs::read_to_string(t.path().join("comm")).unwrap_or_default();
            *names.entry(name.trim().to_string()).or_default() += 1;
        }
    }
    out += &format!(" threads_by_name={names:?}");
    out
}

/// With CADENCE_HEAP_PROFILE_DIR set (feature heap-profile), write a jemalloc heap profile each report tick,
/// named heap-<seq>-<allocated MiB>.heap so the peak dump can be picked without parsing.
#[cfg(feature = "heap-profile")]
fn dump_heap() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let Some(dir) = std::env::var_os("CADENCE_HEAP_PROFILE_DIR") else { return };
    let _ = std::fs::create_dir_all(&dir);
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let mib = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) >> 20;
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let path = std::path::PathBuf::from(dir).join(format!("heap-{seq:04}-{mib:04}.heap"));
    if let Ok(c) = std::ffi::CString::new(path.to_string_lossy().as_bytes()) {
        // jemalloc copies the NUL-terminated filename synchronously.
        let _ = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", c.as_ptr()) };
    }
}
