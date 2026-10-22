//! Go forwarder request-token generations and its dynamic task rate limiter.
use cadence_common::rate_limiter::RateLimiter;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Code, Status};

/// Go `forwarder.handleErr` classification for a parent add/poll failure.
#[derive(Debug)]
pub enum ForwardError {
    SlowDown,
    NoParent,
    TaskListKind,
    InvalidTaskListType,
    Remote(Status),
}

impl ForwardError {
    /// Cadence peers encode `ServiceBusyError` as a ResourceExhausted status
    /// with the same typed detail used by Go's forwarder.
    pub fn classify(status: Status) -> Self {
        let busy_detail = status
            .details()
            .windows(b"ServiceBusyError".len())
            .any(|part| part == b"ServiceBusyError");
        if status.code() == Code::ResourceExhausted && busy_detail {
            Self::SlowDown
        } else {
            Self::Remote(status)
        }
    }
}

pub type Count = Arc<dyn Fn() -> i64 + Send + Sync>;
struct Generation {
    count: i64,
    semaphore: Arc<Semaphore>,
}
/// Go swaps the entire token channel on a limit change. In-flight requests
/// release into their old generation, never inflating the new generation.
pub struct RequestTokens {
    count: Count,
    generation: Mutex<Generation>,
}
impl RequestTokens {
    pub fn new(count: Count) -> Self {
        let value = count().max(0);
        Self { count, generation: Mutex::new(Generation { count: value, semaphore: Arc::new(Semaphore::new(value as usize)) }) }
    }
    fn current(&self) -> Arc<Semaphore> {
        let count = (self.count)().max(0);
        let mut generation = self.generation.lock().expect("forwarder token lock");
        if generation.count != count {
            *generation = Generation { count, semaphore: Arc::new(Semaphore::new(count as usize)) };
        }
        generation.semaphore.clone()
    }
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.current().try_acquire_owned().ok()
    }
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.current().acquire_owned().await.ok()
    }
}
struct RateState {
    limiter: RateLimiter,
    updated: i64,
}
/// Go quotas.NewDynamicRateLimiter: 5-second refresh, ceil(rate) burst,
/// minimum burst 1 except zero rate immediately has zero burst.
pub struct ForwardRate {
    rate: Count,
    state: Mutex<RateState>,
}
impl ForwardRate {
    pub fn new(rate: Count, now: i64) -> Self {
        let value = rate();
        Self { rate, state: Mutex::new(RateState { limiter: RateLimiter::new(value as f64, if value == 0 { 0 } else { value.max(1) }), updated: now }) }
    }
    pub fn allow(&self, now: i64) -> bool {
        let mut state = self.state.lock().expect("forwarder rate lock");
        if now.saturating_sub(state.updated) >= 5_000_000_000 {
            // Match the clock wrapper's update ordering: advance its notion
            // of now before changing the rate/burst.
            state.limiter.tokens(now);
            let rate = (self.rate)();
            state.limiter.set_limit_and_burst(rate as f64, if rate == 0 { 0 } else { rate.max(1) });
            state.updated = now;
        }
        state.limiter.allow(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    fn count(value: i64) -> (Arc<AtomicI64>, Count) {
        let state = Arc::new(AtomicI64::new(value));
        let copy = state.clone();
        (state, Arc::new(move || copy.load(Ordering::SeqCst)))
    }
    #[test]
    fn matching_forward_tokens_replace_generation_without_inflation() {
        let (value, provider) = count(1);
        let tokens = RequestTokens::new(provider);
        let old = tokens.try_acquire().unwrap();
        assert!(tokens.try_acquire().is_none());
        value.store(2, Ordering::SeqCst);
        let first = tokens.try_acquire().unwrap();
        let second = tokens.try_acquire().unwrap();
        drop(old);
        assert!(tokens.try_acquire().is_none());
        value.store(0, Ordering::SeqCst);
        drop((first, second));
        assert!(tokens.try_acquire().is_none());
    }
    #[test]
    fn matching_forward_rate_refreshes_on_five_second_boundary() {
        let (value, provider) = count(2);
        let rate = ForwardRate::new(provider, 0);
        assert!(rate.allow(0));
        assert!(rate.allow(0));
        assert!(!rate.allow(0));
        value.store(0, Ordering::SeqCst);
        assert!(rate.allow(4_999_999_999));
        assert!(!rate.allow(5_000_000_000));
        value.store(1, Ordering::SeqCst);
        assert!(!rate.allow(9_999_999_999));
        assert!(!rate.allow(10_000_000_000));
        assert!(rate.allow(11_000_000_000));
    }

    #[test]
    fn matching_forward_classifies_service_busy_only() {
        let busy = tonic::Status::with_details(
            tonic::Code::ResourceExhausted,
            "busy",
            tonic::codegen::Bytes::from_static(b"uber.cadence.api.v1.ServiceBusyError"),
        );
        assert!(matches!(ForwardError::classify(busy), ForwardError::SlowDown));
        let exhausted = tonic::Status::resource_exhausted("other exhausted error");
        assert!(matches!(ForwardError::classify(exhausted), ForwardError::Remote(_)));
    }
}
