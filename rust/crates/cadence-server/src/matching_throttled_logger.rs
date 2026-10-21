//! Shared rate limited error logging for Matching's long poll validation.
use crate::admin::DynamicConfigOverlay;
use cadence_persistence::dynamic_config;
use std::sync::{Arc, Mutex};

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;
type Sink = Arc<dyn Fn(&str) + Send + Sync>;

struct Bucket {
    tokens: f64,
    last: i64,
}

/// One logger is shared by both Matching poll methods, as in Go handler.NewHandler.
pub(crate) struct MatchingThrottledLogger {
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    clock: Clock,
    sink: Sink,
    bucket: Mutex<Bucket>,
}

impl MatchingThrottledLogger {
    pub(crate) fn new(overlay: Arc<Mutex<DynamicConfigOverlay>>) -> Self {
        Self::with_clock_and_sink(
            overlay,
            Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
                    .unwrap_or_default()
            }),
            Arc::new(|message| eprintln!("ERROR {message}")),
        )
    }

    fn with_clock_and_sink(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        clock: Clock,
        sink: Sink,
    ) -> Self {
        let now = clock();
        let initial_tokens = {
            let config = overlay.lock().expect("admin config lock");
            dynamic_config::integer(config.property_values("matching.throttledLogRPS"), None, 20)
                .max(0) as f64
        };
        Self {
            overlay,
            clock,
            sink,
            bucket: Mutex::new(Bucket {
                tokens: initial_tokens,
                last: now,
            }),
        }
    }

    pub(crate) fn error(&self, handler: &str, message: &str) {
        let rate = {
            let config = self.overlay.lock().expect("admin config lock");
            dynamic_config::integer(config.property_values("matching.throttledLogRPS"), None, 20)
                .max(0) as f64
        };
        if rate == 0.0 {
            return;
        }
        let now = (self.clock)();
        let mut bucket = self.bucket.lock().expect("Matching log bucket lock");
        let elapsed = now.saturating_sub(bucket.last).max(0) as f64 / 1_000_000_000.0;
        bucket.tokens = (bucket.tokens + elapsed * rate).min(rate);
        bucket.last = now;
        if bucket.tokens < 1.0 {
            return;
        }
        bucket.tokens -= 1.0;
        drop(bucket);
        (self.sink)(&format!(
            "{message} handler={handler} error=Context timeout is not set."
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    #[test]
    fn activity_and_decision_errors_share_the_dynamic_rate_bucket() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        overlay.lock().unwrap().update(
            false,
            "matching.throttledLogRPS",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    b"1".to_vec(),
                )),
                filters: vec![],
            }],
        );
        let now = Arc::new(AtomicI64::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let logger = MatchingThrottledLogger::with_clock_and_sink(
            overlay,
            {
                let now = now.clone();
                Arc::new(move || now.load(Ordering::SeqCst))
            },
            {
                let calls = calls.clone();
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
            },
        );
        logger.error(
            "PollForActivityTask",
            "Context timeout not set for long poll API.",
        );
        logger.error(
            "PollForDecisionTask",
            "Context timeout not set for long poll API.",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        now.store(1_000_000_000, Ordering::SeqCst);
        logger.error(
            "PollForDecisionTask",
            "Context timeout not set for long poll API.",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
