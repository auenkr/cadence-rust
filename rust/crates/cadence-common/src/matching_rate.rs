//! Report aggregation from Go matching/tasklist/task_list_limiter.go.
//! Token reservation/enforcement is a separate layer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RateSnapshot {
    pub value: f64,
    pub limit: f64,
    pub burst: i64,
    pub last_update: i64,
    pub last_received: i64,
    pub partitions: i64,
}
#[derive(Clone, Debug)]
pub struct TaskListRate {
    value: f64,
    ttl: i64,
    min_burst: i64,
    partitions: i64,
    last_update: i64,
    last_received: i64,
}
impl TaskListRate {
    pub fn new(value: f64, ttl: i64, min_burst: i64, partitions: i64, now: i64) -> Self {
        Self {
            value,
            ttl,
            min_burst,
            partitions,
            last_update: now,
            last_received: now,
        }
    }
    /// Returns whether Go would call SetLimitAndBurst on its backing limiter.
    /// The provider is read only when the Go implementation resnapshots count.
    pub fn report(&mut self, value: f64, now: i64, partitions: impl FnOnce() -> i64) -> bool {
        self.report_with_clock(value, || now, partitions)
    }
    /// Accepted updates acquire a fresh timestamp after the optimistic check,
    /// as Go tryUpdate does after acquiring its update mutex.
    pub fn report_with_clock(
        &mut self,
        value: f64,
        mut clock: impl FnMut() -> i64,
        partitions: impl FnOnce() -> i64,
    ) -> bool {
        let now = clock();
        let elapsed = now.saturating_sub(self.last_update);
        if elapsed < self.ttl {
            // Order is intentional: tiny increases do not refresh receipt time.
            if value > self.value {
                return false;
            }
            if (self.value - value).abs() < 0.001 {
                self.last_received = now;
                return false;
            }
        }
        let now = clock();
        let ttl_elapsed = now.saturating_sub(self.last_update) >= self.ttl;
        let old_receipt = i128::from(self.last_received) <= i128::from(now) - i128::from(self.ttl);
        if value < self.value || (ttl_elapsed && old_receipt) {
            self.last_update = now;
            self.value = value;
            self.last_received = now;
            self.partitions = partitions();
            true
        } else if ttl_elapsed {
            self.last_update = now;
            let next = partitions();
            let changed = self.partitions != next;
            self.partitions = next;
            changed
        } else {
            false
        }
    }
    pub fn snapshot(&self) -> RateSnapshot {
        let limit = self.value / self.partitions as f64;
        // The original-Go arm64 oracle saturates overflow to MaxInt64,
        // matching Rust's cast. Keep this edge covered by the differential.
        let integer = limit.ceil() as i64;
        let burst = if limit == 0.0 {
            0
        } else {
            integer.max(self.min_burst)
        };
        RateSnapshot {
            value: self.value,
            limit,
            burst,
            last_update: self.last_update,
            last_received: self.last_received,
            partitions: self.partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matching_rate_original_go_report_sequences() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-rate.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let mut rate = TaskListRate::new(
                case["initial"].as_f64().unwrap(),
                case["ttl"].as_i64().unwrap(),
                case["min_burst"].as_i64().unwrap(),
                case["partitions"].as_i64().unwrap(),
                0,
            );
            let mut reads = 1;
            for step in case["steps"].as_array().unwrap() {
                rate.report(
                    step["report"].as_f64().unwrap(),
                    step["now"].as_i64().unwrap(),
                    || {
                        reads += 1;
                        step["partitions"].as_i64().unwrap()
                    },
                );
                let got = rate.snapshot();
                assert_eq!(got.value, step["value"].as_f64().unwrap(), "{case}");
                assert_eq!(got.limit, step["limit"].as_f64().unwrap(), "{case}");
                assert_eq!(got.burst, step["burst"].as_i64().unwrap(), "{case}");
                assert_eq!(
                    got.last_update,
                    step["last_update"].as_i64().unwrap(),
                    "{case}"
                );
                assert_eq!(
                    got.last_received,
                    step["last_received"].as_i64().unwrap(),
                    "{case}"
                );
                assert_eq!(
                    got.partitions,
                    step["applied_partitions"].as_i64().unwrap(),
                    "{case}"
                );
                assert_eq!(reads, step["reads"].as_i64().unwrap(), "{case}");
            }
        }
    }
}
