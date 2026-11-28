//! Diagnostic per-request hop trace (perf-c1-hop-trace). Enabled with `CADENCE_TRACE_DECISION=1`;
//! a single once-initialized bool check when off, and the arguments are not evaluated.
use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var("CADENCE_TRACE_DECISION").is_ok_and(|v| v == "1" || v == "true")
    })
}

pub fn emit(stage: &str, workflow_id: &str, run_id: &str) {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    eprintln!("HOPTRACE svc=frontend epoch_ns={ns} stage={stage} wf={workflow_id} run={run_id}");
}
