//! Go Frontend's ordered health, admission, drain and resource shutdown hooks.

use std::time::Duration;

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

/// Run the same stop-hook order as `frontend.Service.Stop`.
pub async fn shutdown<
    Health,
    HealthFuture,
    Handlers,
    HandlersFuture,
    Limiters,
    LimitersFuture,
    Resource,
    ResourceFuture,
>(
    duration: Duration,
    update_health: Health,
    stop_handlers: Handlers,
    stop_limiters: Limiters,
    stop_resource: Resource,
) where
    Health: FnOnce() -> HealthFuture,
    HealthFuture: std::future::Future<Output = ()>,
    Handlers: FnOnce() -> HandlersFuture,
    HandlersFuture: std::future::Future<Output = ()>,
    Limiters: FnOnce() -> LimitersFuture,
    LimitersFuture: std::future::Future<Output = ()>,
    Resource: FnOnce() -> ResourceFuture,
    ResourceFuture: std::future::Future<Output = ()>,
{
    let plan = DrainPlan::from_go_shutdown_drain_duration(duration);
    update_health().await;
    tokio::time::sleep(plan.failure_detection).await;
    stop_handlers().await;
    stop_limiters().await;
    tokio::time::sleep(plan.request_drain).await;
    stop_resource().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn drain_split_matches_go_service() {
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
    async fn shutdown_runs_go_hooks_in_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook = |event: &'static str| {
            let events = events.clone();
            move || {
                let events = events.clone();
                async move { events.lock().unwrap().push(event) }
            }
        };
        shutdown(
            Duration::ZERO,
            hook("health"),
            hook("handlers"),
            hook("limiters"),
            hook("resource"),
        )
        .await;
        assert_eq!(
            *events.lock().unwrap(),
            ["health", "handlers", "limiters", "resource"]
        );
    }
}
