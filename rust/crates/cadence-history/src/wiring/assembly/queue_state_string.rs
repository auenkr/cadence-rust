//! Go `processingQueueStateImpl.String()` (`queue/processing_queue_state.go`),
//! returned by `DescribeQueue` and printed by the admin CLI:
//! `&{level: %+v, ackLevel: %+v, readLevel: %+v, maxLevel: %+v, domainFilter: %+v}`.
//! Timestamps are rendered like Go `time.Time.String()` in UTC.

use crate::queue::deps::TaskKey;
use crate::queue::domain_filter::DomainFilter;
use crate::queue::processing_queue_state::ProcessingQueueState;

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go `time.Time.String()` for a UTC instant (`2006-01-02 15:04:05.999999999 +0000 UTC`).
pub fn go_time_string(unix_nanos: i64) -> String {
    let secs = unix_nanos.div_euclid(1_000_000_000);
    let nanos = unix_nanos.rem_euclid(1_000_000_000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let mut s = format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", sod / 3600, sod % 3600 / 60, sod % 60);
    if nanos != 0 {
        let frac = format!("{nanos:09}");
        s.push('.');
        s.push_str(frac.trim_end_matches('0'));
    }
    s.push_str(" +0000 UTC");
    s
}

fn key_string(k: &TaskKey) -> String {
    match k {
        TaskKey::Transfer { task_id } => format!("{{taskID:{task_id}}}"),
        TaskKey::Timer { visibility_timestamp, task_id } => {
            format!("{{visibilityTimestamp:{} taskID:{task_id}}}", go_time_string(visibility_timestamp.unix_nano()))
        }
    }
}

fn filter_string(f: &DomainFilter) -> String {
    let mut ids: Vec<&String> = f.domain_ids.iter().collect();
    ids.sort();
    let ids: Vec<String> = ids.iter().map(|i| format!("{i}:{{}}")).collect();
    format!("{{DomainIDs:map[{}] ReverseMatch:{}}}", ids.join(" "), f.reverse_match)
}

/// Go `ProcessingQueueState.String()`.
pub fn processing_queue_state_string(s: &ProcessingQueueState) -> String {
    format!(
        "&{{level: {}, ackLevel: {}, readLevel: {}, maxLevel: {}, domainFilter: {}}}",
        s.level,
        key_string(&s.ack_level),
        key_string(&s.read_level),
        key_string(&s.max_level),
        filter_string(&s.domain_filter)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::deps::Time;
    use std::collections::HashSet;

    #[test]
    fn transfer_state_matches_go_format() {
        let ids: HashSet<String> = ["b".to_string(), "a".to_string()].into_iter().collect();
        let s = ProcessingQueueState::new(
            1,
            TaskKey::Transfer { task_id: 5 },
            TaskKey::Transfer { task_id: 9 },
            DomainFilter::new(ids, false),
        );
        assert_eq!(
            processing_queue_state_string(&s),
            "&{level: 1, ackLevel: {taskID:5}, readLevel: {taskID:5}, maxLevel: {taskID:9}, domainFilter: {DomainIDs:map[a:{} b:{}] ReverseMatch:false}}"
        );
    }

    #[test]
    fn timer_key_and_time_format() {
        assert_eq!(go_time_string(0), "1970-01-01 00:00:00 +0000 UTC");
        assert_eq!(go_time_string(1_700_000_000_123_000_000), "2023-11-14 22:13:20.123 +0000 UTC");
        let k = TaskKey::Timer { visibility_timestamp: Time(1_000_000_000), task_id: 3 };
        assert_eq!(key_string(&k), "{visibilityTimestamp:1970-01-01 00:00:01 +0000 UTC taskID:3}");
    }
}
