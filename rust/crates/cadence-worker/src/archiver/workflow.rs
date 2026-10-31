//! Iteration limits from Go `archiver/workflow.go`.

use std::time::Duration;

use crate::archiver::util::max_archival_iteration_timeout;

/// Caps a configured per-iteration time limit at the workflow maximum. Go
/// computes this inside a SideEffect so replays reuse the recorded value.
pub fn iteration_time_limit(configured: Duration) -> Duration {
    configured.min(max_archival_iteration_timeout())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::iteration_time_limit;

    #[test]
    fn keeps_limits_below_the_workflow_maximum() {
        let limit = Duration::from_secs(60);
        assert_eq!(iteration_time_limit(limit), limit);
    }

    #[test]
    fn caps_limits_above_half_the_workflow_timeout() {
        let maximum = Duration::from_secs(15 * 24 * 60 * 60);
        assert_eq!(iteration_time_limit(Duration::from_secs(365 * 24 * 60 * 60)), maximum);
        assert_eq!(iteration_time_limit(maximum), maximum);
    }
}
