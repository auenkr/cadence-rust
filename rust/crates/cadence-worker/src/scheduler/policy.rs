//! Policy and state helpers from Go `scheduler/workflow.go`, `activity.go` and
//! `types.go`.
//!
//! Times are Unix nanoseconds and durations are nanoseconds, so every decision
//! here is deterministic. The workflow body, signal loop and activities need the
//! SDK and are not ported.

/// Go `MaxBufferedFiresSystemLimit`.
pub const MAX_BUFFERED_FIRES_SYSTEM_LIMIT: usize = 1000;
/// Go `MaxConcurrencyLimitSystemLimit`.
pub const MAX_CONCURRENCY_LIMIT_SYSTEM_LIMIT: i32 = 1000;
/// Go `BufferOverflowReasonUserLimit`.
pub const BUFFER_OVERFLOW_REASON_USER_LIMIT: &str = "user_limit";
/// Go `BufferOverflowReasonSystemLimit`.
pub const BUFFER_OVERFLOW_REASON_SYSTEM_LIMIT: &str = "system_limit";
/// Go `defaultCatchUpWindow` (365 days) in nanoseconds.
pub const DEFAULT_CATCH_UP_WINDOW_NANOS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;
/// Go `ScheduleStateActive`.
pub const SCHEDULE_STATE_ACTIVE: &str = "active";
/// Go `ScheduleStatePaused`.
pub const SCHEDULE_STATE_PAUSED: &str = "paused";

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Go `types.ScheduleCatchUpPolicy`. `Invalid` (zero) means no policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CatchUpPolicy {
    #[default]
    Invalid,
    Skip,
    One,
    All,
}

/// Result of `applyMissedRunPolicy`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MissedRunPolicyResult {
    pub to_fire: Vec<i64>,
    pub skipped: i64,
}

/// Go `effectiveBufferLimit`. Values out of range fall back to the system limit.
pub fn effective_buffer_limit(user_limit: i32) -> (usize, &'static str) {
    if user_limit <= 0 || user_limit as usize > MAX_BUFFERED_FIRES_SYSTEM_LIMIT {
        return (
            MAX_BUFFERED_FIRES_SYSTEM_LIMIT,
            BUFFER_OVERFLOW_REASON_SYSTEM_LIMIT,
        );
    }
    (user_limit as usize, BUFFER_OVERFLOW_REASON_USER_LIMIT)
}

/// Go `effectiveConcurrencyLimit`. Clamps to the system ceiling. Called only for
/// a bounded concurrency limit.
pub fn effective_concurrency_limit(user_limit: i32) -> i32 {
    user_limit.min(MAX_CONCURRENCY_LIMIT_SYSTEM_LIMIT)
}

/// Go `ensurePolicyDefaults`: fills the catch-up window for windowed policies
/// when it is unset. Only the first execution calls this.
pub fn ensure_policy_defaults(catch_up_policy: CatchUpPolicy, catch_up_window_nanos: &mut i64) {
    let uses_window = matches!(catch_up_policy, CatchUpPolicy::One | CatchUpPolicy::All);
    if uses_window && *catch_up_window_nanos <= 0 {
        *catch_up_window_nanos = DEFAULT_CATCH_UP_WINDOW_NANOS;
    }
}

/// Go `applyMissedRunPolicy`. A window of zero or less means unlimited. `One`
/// fires only the latest eligible miss. `All` fires every eligible miss. `Skip`,
/// `Invalid` and unknown policies skip everything.
pub fn apply_missed_run_policy(
    policy: CatchUpPolicy,
    window_nanos: i64,
    missed: &[i64],
    now: i64,
) -> MissedRunPolicyResult {
    let eligible: Vec<i64> = missed
        .iter()
        .copied()
        .filter(|t| window_nanos <= 0 || now - t <= window_nanos)
        .collect();
    let out_of_window = (missed.len() - eligible.len()) as i64;

    match policy {
        CatchUpPolicy::One => match eligible.last() {
            None => MissedRunPolicyResult {
                to_fire: Vec::new(),
                skipped: missed.len() as i64,
            },
            Some(latest) => MissedRunPolicyResult {
                to_fire: vec![*latest],
                skipped: out_of_window + (eligible.len() as i64 - 1),
            },
        },
        CatchUpPolicy::All => MissedRunPolicyResult {
            to_fire: eligible,
            skipped: out_of_window,
        },
        CatchUpPolicy::Skip | CatchUpPolicy::Invalid => MissedRunPolicyResult {
            to_fire: Vec::new(),
            skipped: missed.len() as i64,
        },
    }
}

/// Go `scheduleStateFromPaused`.
pub fn schedule_state_from_paused(paused: bool) -> &'static str {
    if paused {
        SCHEDULE_STATE_PAUSED
    } else {
        SCHEDULE_STATE_ACTIVE
    }
}

/// Pause fields of `SchedulerWorkflowState`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PauseState {
    pub paused: bool,
    pub pause_reason: String,
    pub paused_by: String,
    /// `None` is Go's zero time, meaning the schedule is not paused.
    pub paused_at: Option<i64>,
    /// One-shot catch-up override set by an unpause signal.
    pub unpause_catch_up_policy: CatchUpPolicy,
}

/// Go `handlePause`. Returns `false` if already paused.
pub fn handle_pause(state: &mut PauseState, reason: &str, paused_by: &str, now: i64) -> bool {
    if state.paused {
        return false;
    }
    state.paused = true;
    state.pause_reason = reason.to_owned();
    state.paused_by = paused_by.to_owned();
    state.paused_at = Some(now);
    true
}

/// Go `handleUnpause`. Returns `false` if not paused. A non-Invalid catch-up policy
/// on the signal becomes the one-shot override.
pub fn handle_unpause(state: &mut PauseState, catch_up_policy: CatchUpPolicy) -> bool {
    if !state.paused {
        return false;
    }
    state.paused = false;
    state.pause_reason.clear();
    state.paused_by.clear();
    state.paused_at = None;
    if catch_up_policy != CatchUpPolicy::Invalid {
        state.unpause_catch_up_policy = catch_up_policy;
    }
    true
}

/// Go `generateWorkflowID`: `<prefix>-<RFC 3339 UTC scheduled time>`, with the
/// schedule ID as the prefix when none is set.
pub fn generate_workflow_id(prefix: &str, schedule_id: &str, scheduled_time_nanos: i64) -> String {
    let prefix = if prefix.is_empty() {
        schedule_id
    } else {
        prefix
    };
    format!("{prefix}-{}", format_rfc3339_utc(scheduled_time_nanos))
}

/// Formats Unix nanoseconds as Go's `time.RFC3339` in UTC, with whole seconds.
pub fn format_rfc3339_utc(nanos: i64) -> String {
    let secs = nanos.div_euclid(NANOS_PER_SECOND);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// Days since 1970-01-01 to (year, month, day). Proleptic Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3600 * NANOS_PER_SECOND;

    #[test]
    fn buffer_limit_falls_back_to_the_system_limit() {
        assert_eq!(
            effective_buffer_limit(0),
            (1000, BUFFER_OVERFLOW_REASON_SYSTEM_LIMIT)
        );
        assert_eq!(
            effective_buffer_limit(-3),
            (1000, BUFFER_OVERFLOW_REASON_SYSTEM_LIMIT)
        );
        assert_eq!(
            effective_buffer_limit(1001),
            (1000, BUFFER_OVERFLOW_REASON_SYSTEM_LIMIT)
        );
        assert_eq!(
            effective_buffer_limit(5),
            (5, BUFFER_OVERFLOW_REASON_USER_LIMIT)
        );
    }

    #[test]
    fn concurrency_limit_is_clamped_to_the_ceiling() {
        assert_eq!(effective_concurrency_limit(5), 5);
        assert_eq!(effective_concurrency_limit(5000), 1000);
    }

    #[test]
    fn ensure_defaults_sets_window_only_for_windowed_policies() {
        let mut window = 0;
        ensure_policy_defaults(CatchUpPolicy::One, &mut window);
        assert_eq!(window, DEFAULT_CATCH_UP_WINDOW_NANOS);

        let mut kept = 7 * HOUR;
        ensure_policy_defaults(CatchUpPolicy::All, &mut kept);
        assert_eq!(kept, 7 * HOUR);

        let mut skip = 0;
        ensure_policy_defaults(CatchUpPolicy::Skip, &mut skip);
        assert_eq!(skip, 0);
    }

    #[test]
    fn missed_runs_follow_the_policy_and_window() {
        let now = 10 * HOUR;
        let missed = [7 * HOUR, 8 * HOUR, 9 * HOUR];

        let all = apply_missed_run_policy(CatchUpPolicy::All, 2 * HOUR, &missed, now);
        assert_eq!(all.to_fire, vec![8 * HOUR, 9 * HOUR]);
        assert_eq!(all.skipped, 1);

        let one = apply_missed_run_policy(CatchUpPolicy::One, 2 * HOUR, &missed, now);
        assert_eq!(one.to_fire, vec![9 * HOUR]);
        assert_eq!(one.skipped, 2);

        let unlimited = apply_missed_run_policy(CatchUpPolicy::All, 0, &missed, now);
        assert_eq!(unlimited.to_fire.len(), 3);
        assert_eq!(unlimited.skipped, 0);

        let skip = apply_missed_run_policy(CatchUpPolicy::Skip, 0, &missed, now);
        assert!(skip.to_fire.is_empty());
        assert_eq!(skip.skipped, 3);

        let none_in_window = apply_missed_run_policy(CatchUpPolicy::One, HOUR / 2, &missed, now);
        assert!(none_in_window.to_fire.is_empty());
        assert_eq!(none_in_window.skipped, 3);
    }

    #[test]
    fn pause_and_unpause_are_idempotent_and_keep_the_catch_up_override() {
        let mut state = PauseState::default();
        assert!(handle_pause(&mut state, "maint", "ops", 42));
        assert!(!handle_pause(&mut state, "again", "ops", 43));
        assert_eq!(state.pause_reason, "maint");
        assert_eq!(state.paused_at, Some(42));

        assert!(handle_unpause(&mut state, CatchUpPolicy::All));
        assert!(!state.paused);
        assert_eq!(state.paused_at, None);
        assert_eq!(state.unpause_catch_up_policy, CatchUpPolicy::All);
        assert!(!handle_unpause(&mut state, CatchUpPolicy::Skip));
        assert_eq!(state.unpause_catch_up_policy, CatchUpPolicy::All);
    }

    #[test]
    fn unpause_without_policy_keeps_the_existing_override() {
        let mut state = PauseState {
            paused: true,
            unpause_catch_up_policy: CatchUpPolicy::One,
            ..Default::default()
        };
        assert!(handle_unpause(&mut state, CatchUpPolicy::Invalid));
        assert_eq!(state.unpause_catch_up_policy, CatchUpPolicy::One);
    }

    #[test]
    fn state_strings_match_go() {
        assert_eq!(schedule_state_from_paused(true), "paused");
        assert_eq!(schedule_state_from_paused(false), "active");
    }

    #[test]
    fn rfc3339_formats_unix_epoch_and_leap_day() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        // 2026-01-01T00:00:00Z
        assert_eq!(
            format_rfc3339_utc(1_767_225_600 * NANOS_PER_SECOND),
            "2026-01-01T00:00:00Z"
        );
        // 2024-02-29T12:34:56Z
        assert_eq!(
            format_rfc3339_utc(1_709_210_096 * NANOS_PER_SECOND),
            "2024-02-29T12:34:56Z"
        );
    }

    #[test]
    fn workflow_id_uses_prefix_or_schedule_id() {
        assert_eq!(
            generate_workflow_id("", "sched", 0),
            "sched-1970-01-01T00:00:00Z"
        );
        assert_eq!(
            generate_workflow_id("wf", "sched", 0),
            "wf-1970-01-01T00:00:00Z"
        );
    }
}
