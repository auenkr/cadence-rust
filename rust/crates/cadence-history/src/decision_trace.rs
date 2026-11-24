//! Diagnostic timing trace for the decision scheduled -> started path.
//! Enabled with `CADENCE_TRACE_DECISION=1`; a single once-initialized bool check when off.
use std::sync::OnceLock;
use std::time::Instant;

static ENABLED: OnceLock<bool> = OnceLock::new();
static START: OnceLock<Instant> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        let on = std::env::var("CADENCE_TRACE_DECISION").map(|v| v == "1" || v == "true").unwrap_or(false);
        if on {
            let _ = START.set(Instant::now());
        }
        on
    })
}

/// Monotonic milliseconds (fractional) since the first trace use in this process.
pub fn now_ms() -> f64 {
    START.get_or_init(Instant::now).elapsed().as_secs_f64() * 1000.0
}

/// `decision_trace!("stage", workflow_id, run_id, "extra {}", x)`; the arguments are not evaluated when off.
#[macro_export]
macro_rules! decision_trace {
    ($stage:expr, $wf:expr, $run:expr) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE t_ms={:.3} stage={} wf={} run={}", $crate::decision_trace::now_ms(), $stage, $wf, $run);
        }
    };
    ($stage:expr, $wf:expr, $run:expr, $($arg:tt)+) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE t_ms={:.3} stage={} wf={} run={} {}", $crate::decision_trace::now_ms(), $stage, $wf, $run, format!($($arg)+));
        }
    };
}
