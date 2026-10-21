//! Shared rate limited error logging for Matching's long poll validation.
use crate::admin::DynamicConfigOverlay;
use cadence_common::rate_limiter::RateLimiter;
use cadence_persistence::dynamic_config;
use std::sync::{Arc, Mutex};

const REFRESH_NANOS: i64 = 5_000_000_000;
type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;
type Sink = Arc<dyn Fn(&str) + Send + Sync>;
type RateSource = Arc<dyn Fn() -> f64 + Send + Sync>;

struct DynamicBucket {
    limiter: RateLimiter,
    last_update: i64,
    source: RateSource,
}

impl DynamicBucket {
    fn burst(rate: f64) -> i64 {
        if rate == 0.0 {
            0
        } else {
            (rate.ceil() as i64).max(1)
        }
    }

    fn new(source: RateSource, now: i64) -> Self {
        let rate = source();
        Self {
            limiter: RateLimiter::new(rate, Self::burst(rate)),
            last_update: now,
            source,
        }
    }

    fn allow(&mut self, now: i64) -> bool {
        // Match Go quotas.NewDynamicRateLimiter: sample config at construction,
        // then refresh on now.After(lastUpdate + TTL - 1ns). SetLimitAndBurst
        // operates at the limiter's recorded time; do not advance it here.
        if now > self.last_update.saturating_add(REFRESH_NANOS - 1) {
            let rate = (self.source)();
            self.limiter.set_limit_and_burst(rate, Self::burst(rate));
            self.last_update = now;
        }
        self.limiter.allow(now)
    }
}

/// One logger is shared by both Matching poll methods, as in Go handler.NewHandler.
pub(crate) struct MatchingThrottledLogger {
    clock: Clock,
    sink: Sink,
    bucket: Mutex<DynamicBucket>,
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
        let source: RateSource = Arc::new(move || {
            let config = overlay.lock().expect("admin config lock");
            dynamic_config::integer(config.property_values("matching.throttledLogRPS"), None, 20)
                as f64
        });
        let now = clock();
        Self {
            clock,
            sink,
            bucket: Mutex::new(DynamicBucket::new(source, now)),
        }
    }

    pub(crate) fn error(&self, handler: &str, message: &str) {
        if self
            .bucket
            .lock()
            .expect("Matching log bucket lock")
            .allow((self.clock)())
        {
            (self.sink)(&format!(
                "{message} handler={handler} error=Context timeout is not set."
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    fn set_rate(overlay: &Arc<Mutex<DynamicConfigOverlay>>, rate: i64) {
        overlay.lock().unwrap().update(
            false,
            "matching.throttledLogRPS",
            vec![cadence_api::admin::DynamicConfigValue {
                value: Some(cadence_api::types::DataBlob::new(
                    cadence_api::enums::EncodingType::JSON,
                    rate.to_string().into_bytes(),
                )),
                filters: vec![],
            }],
        );
    }

    fn logger(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        now: Arc<AtomicI64>,
        calls: Arc<AtomicUsize>,
    ) -> MatchingThrottledLogger {
        MatchingThrottledLogger::with_clock_and_sink(
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
        )
    }

    #[test]
    fn default_rate_starts_with_twenty_tokens_and_is_shared_by_both_polls() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let calls = Arc::new(AtomicUsize::new(0));
        let logger = logger(overlay, Arc::new(AtomicI64::new(0)), calls.clone());
        for i in 0..21 {
            logger.error(
                if i % 2 == 0 {
                    "PollForActivityTask"
                } else {
                    "PollForDecisionTask"
                },
                "Context timeout not set for long poll API.",
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 20);
    }

    #[test]
    fn zero_rate_blocks_initial_logs_and_new_rate_is_sampled_only_after_five_seconds() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set_rate(&overlay, 0);
        let now = Arc::new(AtomicI64::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let logger = logger(overlay.clone(), now.clone(), calls.clone());
        logger.error("PollForActivityTask", "missing deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        set_rate(&overlay, 1);
        now.store(REFRESH_NANOS - 1, Ordering::SeqCst);
        logger.error("PollForDecisionTask", "missing deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        now.store(REFRESH_NANOS, Ordering::SeqCst);
        logger.error("PollForDecisionTask", "missing deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Refresh samples rate=1 at 5s, but the limiter's last recorded time
        // is 1ns before that. It accumulates one token at 5.999999999s.
        now.store(REFRESH_NANOS + 999_999_998, Ordering::SeqCst);
        logger.error("PollForDecisionTask", "missing deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        now.store(REFRESH_NANOS + 999_999_999, Ordering::SeqCst);
        logger.error("PollForDecisionTask", "missing deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
