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
