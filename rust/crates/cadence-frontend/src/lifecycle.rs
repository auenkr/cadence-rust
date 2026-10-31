//! Frontend lifecycle state mirrors Go daemon states and health transitions.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

/// Go `frontend.Service.Stop` splits shutdownDrainDuration into the period
/// peers need to observe unhealthy status and a final request-drain window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainPlan {
    pub failure_detection: Duration,
    pub request_drain: Duration,
}

impl DrainPlan {
    #[must_use]
    pub fn from_go_shutdown_drain_duration(duration: Duration) -> Self {
        let request_drain = duration.min(Duration::from_secs(1));
        Self {
            failure_detection: duration.saturating_sub(request_drain),
            request_drain,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum State {
    Initialized = 0,
    Started = 1,
    ShuttingDown = 2,
    Stopped = 3,
}

#[derive(Debug, Default)]
pub struct Lifecycle(AtomicU8);

impl Lifecycle {
    pub fn start(&self) -> bool {
        self.0
            .compare_exchange(0, State::Started as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn begin_shutdown(&self) -> bool {
        self.0
            .compare_exchange(
                1,
                State::ShuttingDown as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn stop(&self) -> bool {
        self.0
            .compare_exchange(2, State::Stopped as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn state(&self) -> State {
        match self.0.load(Ordering::Acquire) {
            1 => State::Started,
            2 => State::ShuttingDown,
            3 => State::Stopped,
            _ => State::Initialized,
        }
    }

    /// Execute Go Frontend's drain sequence. The hooks are supplied by the
    /// service owner so health, API/admin handlers, quota collections and the
    /// base resource stop in the same order as `frontend.Service.Stop`.
    pub async fn shutdown<
        Health,
        HealthFuture,
        StopHandlers,
        StopHandlersFuture,
        StopLimiters,
        StopLimitersFuture,
        StopResource,
        StopResourceFuture,
    >(
        &self,
        shutdown_drain_duration: Duration,
        update_health: Health,
        stop_handlers: StopHandlers,
        stop_limiters: StopLimiters,
        stop_resource: StopResource,
    ) -> bool
    where
        Health: FnOnce() -> HealthFuture,
        HealthFuture: std::future::Future<Output = ()>,
        StopHandlers: FnOnce() -> StopHandlersFuture,
        StopHandlersFuture: std::future::Future<Output = ()>,
        StopLimiters: FnOnce() -> StopLimitersFuture,
        StopLimitersFuture: std::future::Future<Output = ()>,
        StopResource: FnOnce() -> StopResourceFuture,
        StopResourceFuture: std::future::Future<Output = ()>,
    {
        if !self.begin_shutdown() {
            return false;
        }
        let plan = DrainPlan::from_go_shutdown_drain_duration(shutdown_drain_duration);
        update_health().await;
        tokio::time::sleep(plan.failure_detection).await;
        stop_handlers().await;
        stop_limiters().await;
        tokio::time::sleep(plan.request_drain).await;
        stop_resource().await;
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_shutdown_duration_split_matches_service() {
        assert_eq!(
            DrainPlan::from_go_shutdown_drain_duration(Duration::from_millis(500)),
            DrainPlan {
                failure_detection: Duration::ZERO,
                request_drain: Duration::from_millis(500),
            }
        );
        assert_eq!(
            DrainPlan::from_go_shutdown_drain_duration(Duration::from_millis(2500)),
            DrainPlan {
                failure_detection: Duration::from_millis(1500),
                request_drain: Duration::from_secs(1),
            }
        );
    }

    #[tokio::test]
    async fn shutdown_runs_go_hooks_in_order_and_is_idempotent() {
        let lifecycle = Lifecycle::default();
        assert!(lifecycle.start());
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let push = |event: &'static str| {
            let events = events.clone();
            move || {
                let events = events.clone();
                async move {
                    events.lock().unwrap().push(event);
                }
            }
        };
        assert!(
            lifecycle
                .shutdown(
                    Duration::ZERO,
                    push("health"),
                    push("handlers"),
                    push("limiters"),
                    push("resource"),
                )
                .await
        );
        assert_eq!(
            *events.lock().unwrap(),
            ["health", "handlers", "limiters", "resource"]
        );
        assert_eq!(lifecycle.state(), State::Stopped);
        assert!(
            !lifecycle
                .shutdown(
                    Duration::ZERO,
                    || async {},
                    || async {},
                    || async {},
                    || async {},
                )
                .await
        );
    }
}
