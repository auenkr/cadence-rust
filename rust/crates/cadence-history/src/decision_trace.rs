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

/// Wall-clock nanoseconds since the Unix epoch (comparable across service processes).
pub fn epoch_ns() -> i128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as i128).unwrap_or_default()
}

/// `decision_trace!("stage", workflow_id, run_id, "extra {}", x)`; the arguments are not evaluated when off.
#[macro_export]
macro_rules! decision_trace {
    ($stage:expr, $wf:expr, $run:expr) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE epoch_ns={} t_ms={:.3} stage={} wf={} run={}", $crate::decision_trace::epoch_ns(), $crate::decision_trace::now_ms(), $stage, $wf, $run);
        }
    };
    ($stage:expr, $wf:expr, $run:expr, $($arg:tt)+) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE epoch_ns={} t_ms={:.3} stage={} wf={} run={} {}", $crate::decision_trace::epoch_ns(), $crate::decision_trace::now_ms(), $stage, $wf, $run, format!($($arg)+));
        }
    };
}

/// Workflow-independent pump trace: `pump_trace!("stage", "extra {}", x)` (same switch as `decision_trace!`).
#[macro_export]
macro_rules! pump_trace {
    ($stage:expr) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE epoch_ns={} t_ms={:.3} stage={} wf=- run=-", $crate::decision_trace::epoch_ns(), $crate::decision_trace::now_ms(), $stage);
        }
    };
    ($stage:expr, $($arg:tt)+) => {
        if $crate::decision_trace::enabled() {
            tracing::info!(target: "decision_trace", "DTRACE epoch_ns={} t_ms={:.3} stage={} wf=- run=- {}", $crate::decision_trace::epoch_ns(), $crate::decision_trace::now_ms(), $stage, format!($($arg)+));
        }
    };
}
