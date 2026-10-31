//! Frontend error metering and normalization (Go: wrappers/metered).

use cadence_api::errors::{CadenceError, InternalServiceError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Counter {
    Failures,
    BadRequest,
    ServiceBusy,
    EntityNotExists,
    ExecutionAlreadyStarted,
    WorkflowAlreadyCompleted,
    DomainAlreadyExists,
    CancellationAlreadyRequested,
    QueryFailed,
    LimitExceeded,
    ClientVersionNotSupported,
    ContextTimeout,
    ConnectionClosing,
    ServiceUnavailable,
    AuthorizationFailed,
    Unauthorized,
}

impl Counter {
    /// Metric identifier names used by the Go Frontend metered wrapper.
    pub const fn metric_name(self) -> &'static str {
        match self {
            Self::Failures => "cadence-failures",
            Self::BadRequest => "cadence-err-bad-request",
            Self::ServiceBusy => "cadence-err-service-busy",
            Self::EntityNotExists => "cadence-err-entity-not-exists",
            Self::ExecutionAlreadyStarted => "cadence-err-execution-already-started",
            Self::WorkflowAlreadyCompleted => "cadence-err-workflow-execution-already-completed",
            Self::DomainAlreadyExists => "cadence-err-domain-already-exists",
            Self::CancellationAlreadyRequested => "cadence-err-cancellation-already-requested",
            Self::QueryFailed => "cadence-err-query-failed",
            Self::LimitExceeded => "cadence-err-limit-exceeded",
            Self::ClientVersionNotSupported => "cadence-err-client-version-not-supported",
            Self::ContextTimeout => "cadence-err-context-timeout",
            Self::ConnectionClosing => "cadence-err-grpc-connection-closing",
            Self::ServiceUnavailable => "cadence-err-service-unavailable",
            Self::AuthorizationFailed => "cadence-err-authorize-failed",
            Self::Unauthorized => "cadence-err-unauthorized",
        }
    }
}

pub trait Metrics {
    fn increment(&self, counter: Counter);
}

/// Go forwards known typed API errors unchanged. Internal and uncategorized
/// failures become ordinary internal errors so Thrift clients do not count
/// them as user errors.
pub fn normalize_error(error: CadenceError) -> (CadenceError, Counter) {
    match error {
        CadenceError::BadRequest(_)
        | CadenceError::DomainNotActive(_)
        | CadenceError::AccessDenied(_) => (error, Counter::BadRequest),
        CadenceError::ServiceBusy(_) => (error, Counter::ServiceBusy),
        CadenceError::EntityNotExists(_) => (error, Counter::EntityNotExists),
        CadenceError::WorkflowAlreadyStarted(_) => (error, Counter::ExecutionAlreadyStarted),
        CadenceError::ClientVersionNotSupported(_) => {
            (error, Counter::ClientVersionNotSupported)
        }
        CadenceError::LimitExceeded(_) => (error, Counter::LimitExceeded),
        CadenceError::Internal(inner) => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal error, msg: {}",
                inner.message
            ))),
            Counter::Failures,
        ),
        CadenceError::Unknown(message) => (
            CadenceError::Internal(InternalServiceError::new(format!(
                "cadence internal uncategorized error, msg: {message}"
            ))),
            Counter::Failures,
        ),
        other => (other, Counter::Failures),
    }
}

pub fn handle_error(error: CadenceError, metrics: &dyn Metrics) -> CadenceError {
    let (normalized, counter) = normalize_error(error);
    metrics.increment(counter);
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Default)]
    struct RecordingMetrics(RefCell<Vec<Counter>>);
    impl Metrics for RecordingMetrics {
        fn increment(&self, counter: Counter) {
            self.0.borrow_mut().push(counter);
        }
    }

    #[test]
    fn known_service_errors_keep_type_and_use_go_metric() {
        let metrics = RecordingMetrics::default();
        let error = handle_error(
            CadenceError::ServiceBusy(cadence_api::errors::ServiceBusyError::new("busy")),
            &metrics,
        );
        assert_eq!(error.to_string(), "busy");
        assert_eq!(&*metrics.0.borrow(), &[Counter::ServiceBusy]);
        assert_eq!(Counter::ServiceBusy.metric_name(), "cadence-err-service-busy");
    }

    #[test]
    fn internal_error_is_normalized_with_go_message_prefix() {
        let error = normalize_error(CadenceError::Internal(InternalServiceError::new("broken")));
        assert_eq!(error.0.to_string(), "cadence internal error, msg: broken");
        assert_eq!(error.1, Counter::Failures);
    }

    #[test]
    fn uncategorized_error_uses_go_fallback() {
        let (error, counter) = normalize_error(CadenceError::Unknown("oops".into()));
        assert_eq!(error.to_string(), "cadence internal uncategorized error, msg: oops");
        assert_eq!(counter, Counter::Failures);
    }
}
