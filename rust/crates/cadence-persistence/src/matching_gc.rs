//! Go Matching task_gc.go: one collector at a time, thresholded batch draining.
use std::{
    num::NonZeroU32,
    sync::Mutex,
    time::{Duration, Instant},
};
#[derive(Default)]
struct State {
    ack: i64,
    last_delete: Option<Instant>,
}
#[derive(Default)]
pub struct TaskGarbageCollector {
    state: Mutex<State>,
}
impl TaskGarbageCollector {
    /// A full batch requires another deletion call, even if it emptied storage.
    /// Failed deletion keeps the cursor unchanged but advances the attempt time.
    pub fn run<E>(
        &self,
        ack: i64,
        batch: NonZeroU32,
        max_interval: Duration,
        force: bool,
        now: Instant,
        mut delete: impl FnMut(i64, i64) -> Result<usize, E>,
    ) -> Result<usize, E> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(0),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("Matching GC lock poisoned"),
        };
        let backlog = ack.wrapping_sub(state.ack);
        let due = backlog >= i64::from(batch.get())
            || (backlog > 0
                && (force
                    || state
                        .last_delete
                        .is_none_or(|last| now.saturating_duration_since(last) > max_interval)));
        if !due {
            return Ok(0);
        }
        state.last_delete = Some(now);
        let mut deleted = 0;
        loop {
            let count = delete(ack, i64::from(batch.get()))?;
            deleted += count;
            if count < batch.get() as usize {
                state.ack = ack;
                return Ok(deleted);
            }
        }
    }
    pub fn ack_level(&self) -> i64 {
        self.state.lock().expect("Matching GC lock").ack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matching_gc_original_go_fixtures() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../../../tests/fixtures/matching-gc.json"))
                .unwrap();
        for case in cases.as_array().unwrap() {
            let gc = TaskGarbageCollector::default();
            let now = Instant::now();
            let last = if case["last"].as_bool().unwrap_or_default() {
                let elapsed = case["elapsed_ns"].as_i64().unwrap_or_default();
                Some(if elapsed < 0 {
                    now + Duration::from_nanos(elapsed.unsigned_abs())
                } else {
                    now - Duration::from_nanos((elapsed as u64).max(1))
                })
            } else {
                None
            };
            {
                let mut state = gc.state.lock().unwrap();
                state.ack = case["cursor"].as_i64().unwrap();
                state.last_delete = last;
            }
            let busy = case["busy"]
                .as_bool()
                .unwrap_or_default()
                .then(|| gc.state.lock().unwrap());
            let mut calls = Vec::new();
            let mut position = 0;
            let result = gc.run(
                case["ack"].as_i64().unwrap(),
                NonZeroU32::new(case["batch"].as_u64().unwrap() as u32).unwrap(),
                Duration::from_secs(1),
                case["force"].as_bool().unwrap_or_default(),
                now,
                |ack, limit| {
                    calls.push(vec![ack, limit]);
                    let count = case["results"][position]
                        .as_i64()
                        .expect("unexpected deletion call");
                    position += 1;
                    if count < 0 {
                        Err(())
                    } else {
                        Ok(count as usize)
                    }
                },
            );
            drop(busy);
            let state = gc.state.lock().unwrap();
            assert_eq!(serde_json::json!(calls), case["calls"], "{}", case["name"]);
            assert_eq!(
                state.ack,
                case["expected_cursor"].as_i64().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                state.last_delete != last,
                case["attempted"].as_bool().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                result.is_err(),
                case["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|count| count.as_i64().unwrap() < 0)
            );
        }
    }
    #[test]
    fn matching_gc_time_boundary_is_strict_and_failure_defers_small_retry() {
        let gc = TaskGarbageCollector::default();
        let now = Instant::now();
        gc.state.lock().unwrap().last_delete = Some(now);
        let batch = NonZeroU32::new(100).unwrap();
        let interval = Duration::from_secs(1);
        let result: Result<usize, ()> =
            gc.run(1, batch, interval, false, now + interval, |_, _| {
                panic!("must not delete at equality")
            });
        assert_eq!(result, Ok(0));
        assert_eq!(
            gc.run(
                1,
                batch,
                interval,
                false,
                now + interval + Duration::from_nanos(1),
                |_, _| Err(())
            ),
            Err(())
        );
        assert_eq!(gc.ack_level(), 0);
        let result: Result<usize, ()> = gc.run(
            1,
            batch,
            interval,
            false,
            now + interval + Duration::from_millis(1),
            |_, _| panic!("must not retry small backlog immediately"),
        );
        assert_eq!(result, Ok(0));
        assert_eq!(
            gc.run(
                1,
                batch,
                interval,
                true,
                now + interval + Duration::from_millis(1),
                |_, _| Ok::<_, ()>(1)
            ),
            Ok(1)
        );
        assert_eq!(gc.ack_level(), 1);
    }
}
