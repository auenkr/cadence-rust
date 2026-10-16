//! Go Matching liveness: inactivity expires at equality, not strictly after TTL.
use std::sync::atomic::{AtomicI64, Ordering};
pub struct Liveness {
    last_event: AtomicI64,
    ttl_nanos: i64,
}
impl Liveness {
    pub fn new(now_nanos: i64, ttl_nanos: i64) -> Self {
        Self {
            last_event: AtomicI64::new(now_nanos),
            ttl_nanos,
        }
    }
    pub fn mark_alive(&self, now_nanos: i64) {
        self.last_event.store(now_nanos, Ordering::Release);
    }
    pub fn is_alive(&self, now_nanos: i64) -> bool {
        now_nanos.wrapping_sub(self.last_event.load(Ordering::Acquire)) < self.ttl_nanos
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_go_liveness_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-liveness.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let mut now = 1_000_000_000i64;
            let live = Liveness::new(now, case["ttl"].as_i64().unwrap());
            for step in case["steps"].as_array().unwrap() {
                now = now.wrapping_add(step["delta"].as_i64().unwrap());
                if step["mark"].as_bool().unwrap_or_default() {
                    live.mark_alive(now);
                }
                assert_eq!(
                    live.is_alive(now),
                    step["alive"].as_bool().unwrap(),
                    "{case} {step}"
                );
            }
        }
    }
}
