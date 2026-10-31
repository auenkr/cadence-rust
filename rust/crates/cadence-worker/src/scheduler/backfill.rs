//! Backfill queue and drain from Go `scheduler/backfill.go` and `types.go`.
//!
//! Times are Unix nanoseconds, so the deterministic decisions need no clock. Cron
//! evaluation (`computeNextRunTime`, `computeMissedFireTimes`) and fire
//! processing (`processScheduleFire`) need the SDK and cron library. The drain
//! takes them as closures so this logic stays identical to Go.

/// Go `maxPendingBackfills`.
pub const MAX_PENDING_BACKFILLS: usize = 10;
/// Go `maxBackfillRunsTotalCount`: the cap for a counted backfill range.
pub const MAX_BACKFILL_RUNS_TOTAL_COUNT: usize = 100_000;
/// Go `BackfillRejectedReasonInvalidRange`.
pub const REASON_INVALID_RANGE: &str = "invalid_range";
/// Go `BackfillRejectedReasonQueueFull`.
pub const REASON_QUEUE_FULL: &str = "queue_full";
/// Go `BackfillRejectedReasonDuplicateID`.
pub const REASON_DUPLICATE_ID: &str = "duplicate_id";

const ONE_SECOND_NANOS: i64 = 1_000_000_000;

/// Go `types.ScheduleOverlapPolicy`. `Invalid` (zero) means inherit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OverlapPolicy {
    #[default]
    Invalid,
    SkipNew,
    Buffer,
    Concurrent,
    CancelPrevious,
    TerminatePrevious,
}

/// Go `TriggerSource` values that matter to overlap resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerSource {
    Schedule,
    Backfill,
}

/// Go `BackfillRequest`. Times are Unix nanoseconds.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackfillRequest {
    pub start_time: i64,
    pub end_time: i64,
    pub overlap_policy: OverlapPolicy,
    pub backfill_id: String,
    pub runs_total: i32,
    pub runs_completed: i32,
    pub runs_total_computed: bool,
}

/// Go `BackfillSignal`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackfillSignal {
    pub start_time: i64,
    pub end_time: i64,
    pub overlap_policy: OverlapPolicy,
    pub backfill_id: String,
}

/// Why `handle_backfill` refused a signal. The value is the Go reason tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackfillRejection {
    InvalidRange,
    QueueFull,
    DuplicateId,
}

impl BackfillRejection {
    /// Go reason tag recorded on the rejected-backfill counter.
    pub fn reason(self) -> &'static str {
        match self {
            Self::InvalidRange => REASON_INVALID_RANGE,
            Self::QueueFull => REASON_QUEUE_FULL,
            Self::DuplicateId => REASON_DUPLICATE_ID,
        }
    }
}

/// Go `handleBackfill`. Validates the signal, then queues it. Overlapping windows
/// are queued too, and their shared fires are deduplicated later.
pub fn handle_backfill(
    pending: &mut Vec<BackfillRequest>,
    signal: &BackfillSignal,
) -> Result<(), BackfillRejection> {
    if signal.end_time <= signal.start_time {
        return Err(BackfillRejection::InvalidRange);
    }
    if pending.len() >= MAX_PENDING_BACKFILLS {
        return Err(BackfillRejection::QueueFull);
    }
    if !signal.backfill_id.is_empty()
        && pending
            .iter()
            .any(|existing| existing.backfill_id == signal.backfill_id)
    {
        return Err(BackfillRejection::DuplicateId);
    }
    pending.push(BackfillRequest {
        start_time: signal.start_time,
        end_time: signal.end_time,
        overlap_policy: signal.overlap_policy,
        backfill_id: signal.backfill_id.clone(),
        ..Default::default()
    });
    Ok(())
}

/// Go `effectiveFireOverlap`. A backfill fire uses its own policy unless that is
/// `Invalid`. Schedule fires always use the schedule policy.
pub fn effective_fire_overlap(
    trigger: TriggerSource,
    backfill_overlap: OverlapPolicy,
    schedule_overlap: OverlapPolicy,
) -> OverlapPolicy {
    if trigger == TriggerSource::Backfill && backfill_overlap != OverlapPolicy::Invalid {
        return backfill_overlap;
    }
    schedule_overlap
}

/// Go `countCronFires`. Counts fires in `(start, end]` up to `limit`, using
/// `next_run_after(t)` (`None` when there is no next run). Returns the count and
/// whether the cap was reached, as Go does.
pub fn count_cron_fires(
    start: i64,
    end: i64,
    limit: usize,
    mut next_run_after: impl FnMut(i64) -> Option<i64>,
) -> (usize, bool) {
    let mut count = 0;
    let mut t = start;
    while count < limit {
        match next_run_after(t) {
            Some(next) if next <= end => {
                count += 1;
                t = next;
            }
            _ => return (count, false),
        }
    }
    (count, true)
}

/// Result of `processBackfills`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrainOutcome {
    /// More backfill work remains. The caller should continue as new.
    pub more_work: bool,
    /// Fires handed to `fire` in this pass.
    pub fired: usize,
}

/// Go `processBackfills` drain. Steps:
/// 1. Fill in `runs_total` for queued backfills that lack it. The count is capped.
/// 2. While the schedule is paused, stop without firing. The queue is kept.
/// 3. Fire each backfill's missed times in order, decrementing `budget`. When the
///    budget runs out, or a window is truncated, the backfill resumes from the
///    last fire.
///
/// `count_total(start, end)` returns the capped run count for `(start, end]`
/// (already adjusted by one second). `missed_fires(start, end)` returns the fire
/// times and whether they were truncated. `fire(time, overlap, backfill_id)`
/// starts or defers one fire.
pub fn process_backfills(
    pending: &mut Vec<BackfillRequest>,
    paused: bool,
    schedule_overlap: OverlapPolicy,
    budget: &mut i64,
    mut count_total: impl FnMut(i64, i64) -> (usize, bool),
    mut missed_fires: impl FnMut(i64, i64) -> (Vec<i64>, bool),
    mut fire: impl FnMut(i64, OverlapPolicy, &str),
) -> DrainOutcome {
    if pending.is_empty() {
        return DrainOutcome::default();
    }

    for backfill in pending.iter_mut().filter(|bf| !bf.runs_total_computed) {
        let (total, truncated) = count_total(
            backfill.start_time - ONE_SECOND_NANOS,
            backfill.end_time,
        );
        backfill.runs_total = if truncated {
            MAX_BACKFILL_RUNS_TOTAL_COUNT as i32
        } else {
            total as i32
        };
        backfill.runs_total_computed = true;
    }

    if paused {
        return DrainOutcome::default();
    }

    let mut fired = 0;
    while !pending.is_empty() {
        let backfill = &mut pending[0];
        let (times, truncated) = missed_fires(
            backfill.start_time - ONE_SECOND_NANOS,
            backfill.end_time,
        );

        for time in &times {
            if *budget <= 0 {
                backfill.start_time = *time;
                return DrainOutcome {
                    more_work: true,
                    fired,
                };
            }
            let overlap = effective_fire_overlap(
                TriggerSource::Backfill,
                backfill.overlap_policy,
                schedule_overlap,
            );
            fire(*time, overlap, &backfill.backfill_id);
            fired += 1;
            backfill.runs_completed += 1;
            *budget -= 1;
        }

        if truncated {
            if let Some(last) = times.last() {
                backfill.start_time = last + ONE_SECOND_NANOS;
            }
            return DrainOutcome {
                more_work: true,
                fired,
            };
        }

        pending.remove(0);
    }

    DrainOutcome {
        more_work: false,
        fired,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(start: i64, end: i64, id: &str) -> BackfillSignal {
        BackfillSignal {
            start_time: start,
            end_time: end,
            backfill_id: id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn preserves_go_limits_and_reason_tags() {
        assert_eq!(MAX_PENDING_BACKFILLS, 10);
        assert_eq!(MAX_BACKFILL_RUNS_TOTAL_COUNT, 100_000);
        assert_eq!(BackfillRejection::InvalidRange.reason(), "invalid_range");
        assert_eq!(BackfillRejection::QueueFull.reason(), "queue_full");
        assert_eq!(BackfillRejection::DuplicateId.reason(), "duplicate_id");
    }

    #[test]
    fn handle_backfill_rejects_empty_or_reversed_ranges() {
        let mut pending = Vec::new();
        assert_eq!(
            handle_backfill(&mut pending, &signal(5, 5, "a")),
            Err(BackfillRejection::InvalidRange)
        );
        assert_eq!(
            handle_backfill(&mut pending, &signal(5, 4, "a")),
            Err(BackfillRejection::InvalidRange)
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn handle_backfill_queues_valid_and_overlapping_ranges() {
        let mut pending = Vec::new();
        handle_backfill(&mut pending, &signal(0, 10, "a")).unwrap();
        handle_backfill(&mut pending, &signal(5, 15, "b")).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[1].backfill_id, "b");
    }

    #[test]
    fn handle_backfill_rejects_a_full_queue() {
        let mut pending: Vec<BackfillRequest> =
            (0..MAX_PENDING_BACKFILLS).map(|_| BackfillRequest::default()).collect();
        assert_eq!(
            handle_backfill(&mut pending, &signal(0, 10, "over-cap")),
            Err(BackfillRejection::QueueFull)
        );
        assert_eq!(pending.len(), MAX_PENDING_BACKFILLS);
    }

    #[test]
    fn handle_backfill_drops_a_duplicate_id_only_when_non_empty() {
        let mut pending = Vec::new();
        handle_backfill(&mut pending, &signal(0, 10, "same")).unwrap();
        assert_eq!(
            handle_backfill(&mut pending, &signal(20, 30, "same")),
            Err(BackfillRejection::DuplicateId)
        );
        handle_backfill(&mut pending, &signal(20, 30, "")).unwrap();
        handle_backfill(&mut pending, &signal(40, 50, "")).unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn backfill_overlap_inherits_only_when_invalid() {
        use OverlapPolicy::*;
        assert_eq!(
            effective_fire_overlap(TriggerSource::Schedule, Concurrent, SkipNew),
            SkipNew
        );
        assert_eq!(
            effective_fire_overlap(TriggerSource::Backfill, Invalid, Buffer),
            Buffer
        );
        assert_eq!(
            effective_fire_overlap(TriggerSource::Backfill, Concurrent, Buffer),
            Concurrent
        );
    }

    #[test]
    fn cron_count_stops_at_end_or_cap() {
        // Fires every 10 units.
        let next = |t: i64| Some(t + 10);
        assert_eq!(count_cron_fires(0, 35, 100, next), (3, false));
        assert_eq!(count_cron_fires(0, 35, 2, next), (2, true));
        assert_eq!(count_cron_fires(0, 35, 100, |_| None), (0, false));
    }

    #[test]
    fn drain_is_a_no_op_when_paused_but_still_fills_runs_total() {
        let mut pending = vec![BackfillRequest {
            start_time: 100,
            end_time: 200,
            ..Default::default()
        }];
        let mut budget = 10;
        let outcome = process_backfills(
            &mut pending,
            true,
            OverlapPolicy::Buffer,
            &mut budget,
            |_, _| (4, false),
            |_, _| panic!("paused schedule must not compute fires"),
            |_, _, _| panic!("paused schedule must not fire"),
        );
        assert_eq!(outcome, DrainOutcome::default());
        assert_eq!(pending[0].runs_total, 4);
        assert!(pending[0].runs_total_computed);
        assert_eq!(budget, 10);
    }

    #[test]
    fn drain_tracks_runs_completed_and_removes_finished_backfills() {
        let mut pending = vec![BackfillRequest {
            start_time: 0,
            end_time: 30,
            backfill_id: "bf".into(),
            overlap_policy: OverlapPolicy::Concurrent,
            ..Default::default()
        }];
        let mut budget = 100;
        let mut fired = Vec::new();
        let outcome = process_backfills(
            &mut pending,
            false,
            OverlapPolicy::Buffer,
            &mut budget,
            |_, _| (3, false),
            |_, _| (vec![10, 20, 30], false),
            |time, overlap, id| fired.push((time, overlap, id.to_owned())),
        );
        assert_eq!(outcome, DrainOutcome { more_work: false, fired: 3 });
        assert!(pending.is_empty());
        assert_eq!(budget, 97);
        assert_eq!(fired[0], (10, OverlapPolicy::Concurrent, "bf".into()));
    }

    #[test]
    fn drain_resumes_from_the_last_fire_when_the_budget_runs_out() {
        let mut pending = vec![BackfillRequest {
            start_time: 0,
            end_time: 100,
            ..Default::default()
        }];
        let mut budget = 2;
        let outcome = process_backfills(
            &mut pending,
            false,
            OverlapPolicy::SkipNew,
            &mut budget,
            |_, _| (0, false),
            |_, _| (vec![10, 20, 30], false),
            |_, _, _| {},
        );
        assert_eq!(outcome, DrainOutcome { more_work: true, fired: 2 });
        assert_eq!(pending[0].start_time, 30);
        assert_eq!(pending[0].runs_completed, 2);
        assert_eq!(budget, 0);
    }

    #[test]
    fn drain_advances_past_a_truncated_scan() {
        let mut pending = vec![BackfillRequest {
            start_time: 0,
            end_time: 100,
            ..Default::default()
        }];
        let mut budget = 100;
        let outcome = process_backfills(
            &mut pending,
            false,
            OverlapPolicy::SkipNew,
            &mut budget,
            |_, _| (0, false),
            |_, _| (vec![10, 20], true),
            |_, _, _| {},
        );
        assert_eq!(outcome, DrainOutcome { more_work: true, fired: 2 });
        assert_eq!(pending[0].start_time, 20 + ONE_SECOND_NANOS);
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn runs_total_is_capped_when_the_count_is_truncated() {
        let mut pending = vec![BackfillRequest {
            start_time: 0,
            end_time: 100,
            ..Default::default()
        }];
        let mut budget = 0;
        process_backfills(
            &mut pending,
            true,
            OverlapPolicy::SkipNew,
            &mut budget,
            |_, _| (MAX_BACKFILL_RUNS_TOTAL_COUNT, true),
            |_, _| panic!("unused"),
            |_, _, _| {},
        );
        assert_eq!(pending[0].runs_total, MAX_BACKFILL_RUNS_TOTAL_COUNT as i32);
    }
}
