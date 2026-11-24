//! Env-gated diagnostics: `CADENCE_HISTORY_TRACE_TRANSFER=1` prints one stderr line per
//! transfer-queue step with wall-clock unix microseconds. Off by default; no behaviour change.

use std::sync::OnceLock;

pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("CADENCE_HISTORY_TRACE_TRANSFER").map(|v| !v.is_empty() && v != "0").unwrap_or(false)
    })
}

pub fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// `TRANSFER_TRACE <step> now_us=<..> <detail>`
pub fn emit(step: &str, detail: std::fmt::Arguments<'_>) {
    eprintln!("TRANSFER_TRACE {step} now_us={} {detail}", now_us());
}
