//! Go matching taskListManager.getIsolationGroupForTask policy. Infrastructure
//! resolves drains, poller recency and partition membership before calling it.
pub const MINIMUM_ISOLATION_NANOS: i64 = 50_000_000;

/// Go newInternalTask copies only isolation/original/workflow keys when a
/// group key exists. Do not call this on the persisted original task data.
pub fn dispatch_partition_config(
    config: &Option<std::collections::BTreeMap<String, String>>,
    group: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    let Some(map) = config else { return None };
    let Some(original) = map.get("isolation-group") else {
        return config.clone();
    };
    Some(std::collections::BTreeMap::from([
        ("isolation-group".into(), group.into()),
        (
            "original-isolation-group".into(),
            map.get("original-isolation-group")
                .unwrap_or(original)
                .clone(),
        ),
        (
            "wf-id".into(),
            map.get("wf-id").cloned().unwrap_or_default(),
        ),
    ]))
}

pub struct TaskIsolation<'a> {
    pub enabled: bool,
    pub sticky: bool,
    pub group: &'a str,
    pub known: bool,
    /// Errors reading the drain state leak to the shared channel too.
    pub drained: Result<bool, ()>,
    pub recent_poller: bool,
    /// Missing read partition is different from a partition with no group list.
    pub partition_groups: Option<&'a [String]>,
    pub total_duration: i64,
    pub task_latency: i64,
}

impl TaskIsolation<'_> {
    /// Empty group and zero duration mean no isolation. A selected group with
    /// zero duration means isolation has no age cutoff, not "leak immediately".
    pub fn select(&self) -> (&str, i64) {
        let (group, duration, _) = self.select_with_cause();
        (group, duration)
    }

    /// Preserve Go's leak reason while keeping `select`'s behavior and public
    /// return shape for existing callers.
    pub fn select_with_cause(&self) -> (&str, i64, Option<&'static str>) {
        if !self.enabled
            || self.sticky
            || self.group.is_empty()
        {
            return ("", 0, None);
        }
        if self.drained.is_err() {
            return ("", 0, Some("error"));
        }
        if self.drained == Ok(true) {
            return ("", 0, Some("group_drained"));
        }
        if !self.known {
            return ("", 0, Some("group_unknown"));
        }
        if !self.recent_poller {
            return ("", 0, Some("no_recent_pollers"));
        }
        if !self
            .partition_groups
            .is_some_and(|groups| groups.is_empty() || groups.iter().any(|g| g == self.group))
        {
            return ("", 0, Some("partition_change"));
        }
        if self.total_duration == 0 {
            return (self.group, 0, None);
        }
        if self.task_latency < self.total_duration.saturating_sub(MINIMUM_ISOLATION_NANOS) {
            (
                self.group,
                self.total_duration.saturating_sub(self.task_latency),
                None,
            )
        } else {
            ("", 0, Some("expired"))
        }
    }
}

/// Go taskReader.getDispatchTimeout, including one buffer per configured group
/// plus the shared buffer. Go float-to-duration conversion uses MinInt64 when
/// out of range (including infinity); preserve that before adding the buffer.
pub fn dispatch_timeout(
    rps: f64,
    buffers: usize,
    asynchronous: i64,
    isolation: i64,
    buffer: i64,
) -> i64 {
    let nanos = 1_000_000_000.0 * buffers as f64 / rps;
    let minimum = if !nanos.is_finite() || nanos >= i64::MAX as f64 || nanos < i64::MIN as f64 {
        i64::MIN
    } else {
        nanos as i64
    }
    .wrapping_add(buffer);
    let timeout = if isolation != 0 {
        asynchronous.min(isolation)
    } else {
        asynchronous
    };
    timeout.max(minimum)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_go_isolation_policy_fixtures() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-isolation.json"
        ))
        .unwrap();
        for case in fixtures["selection"].as_array().unwrap() {
            let groups: Option<Vec<String>> =
                serde_json::from_value(case["partition_groups"].clone()).unwrap();
            let policy = TaskIsolation {
                enabled: case["enabled"].as_bool().unwrap(),
                sticky: case["sticky"].as_bool().unwrap(),
                group: case["group"].as_str().unwrap(),
                known: case["known"].as_bool().unwrap(),
                drained: if case["state_error"].as_bool().unwrap() {
                    Err(())
                } else {
                    Ok(case["drained"].as_bool().unwrap())
                },
                recent_poller: case["recent"].as_bool().unwrap(),
                partition_groups: groups.as_deref(),
                total_duration: case["duration"].as_i64().unwrap(),
                task_latency: case["latency"].as_i64().unwrap(),
            };
            let (group, duration) = policy.select();
            assert_eq!(
                serde_json::json!([group, duration]),
                case["expected"],
                "{}",
                case["name"]
            );
        }
        for case in fixtures["timeouts"].as_array().unwrap() {
            assert_eq!(
                dispatch_timeout(
                    case["rps"].as_f64().unwrap(),
                    case["buffers"].as_u64().unwrap() as usize,
                    3_000_000_000,
                    case["isolation"].as_i64().unwrap(),
                    100_000_000
                ),
                case["expected"].as_i64().unwrap()
            );
        }
        for case in fixtures["rewrites"].as_array().unwrap() {
            let input = serde_json::from_value(case["input"].clone()).unwrap();
            let expected = serde_json::from_value(case["expected"].clone()).unwrap();
            assert_eq!(
                dispatch_partition_config(&input, case["group"].as_str().unwrap()),
                expected
            );
        }
    }
    #[test]
    fn selection_preserves_go_leak_causes() {
        let policy = |drained, known, recent_poller, partition_groups, total_duration, task_latency| {
            TaskIsolation {
                enabled: true,
                sticky: false,
                group: "a",
                known,
                drained,
                recent_poller,
                partition_groups,
                total_duration,
                task_latency,
            }
        };
        assert_eq!(policy(Err(()), true, true, Some(&[]), 0, 0).select_with_cause().2, Some("error"));
        assert_eq!(policy(Ok(true), true, true, Some(&[]), 0, 0).select_with_cause().2, Some("group_drained"));
        assert_eq!(policy(Ok(false), false, true, Some(&[]), 0, 0).select_with_cause().2, Some("group_unknown"));
        assert_eq!(policy(Ok(false), true, false, Some(&[]), 0, 0).select_with_cause().2, Some("no_recent_pollers"));
        let wrong_partition = vec!["b".to_owned()];
        assert_eq!(policy(Ok(false), true, true, Some(&wrong_partition), 0, 0).select_with_cause().2, Some("partition_change"));
        assert_eq!(policy(Ok(false), true, true, Some(&[]), 100, 60).select_with_cause().2, Some("expired"));
    }
}
