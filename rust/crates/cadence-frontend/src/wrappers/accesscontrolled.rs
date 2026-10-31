//! Authorization wrapper (Go: wrappers/accesscontrolled).

#![allow(clippy::result_large_err)]

use cadence_api::errors::{AccessDeniedError, CadenceError};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Attributes {
    pub operation: String,
    pub domain: String,
    pub workflow_id: String,
    pub run_id: String,
    pub task_list: String,
    pub identity: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Allow,
    Deny,
}

pub trait Authorizer {
    type Error: std::fmt::Display;

    fn authorize(&self, attributes: &Attributes) -> Result<Decision, Self::Error>;
}

pub trait AuthorizationMetrics {
    fn authorization_latency(&self, elapsed: std::time::Duration);
    fn authorization_failed(&self);
    fn unauthorized(&self);
}

pub fn authorize<A, M>(
    authorizer: &A,
    metrics: &M,
    attributes: &Attributes,
) -> Result<(), CadenceError>
where
    A: Authorizer,
    M: AuthorizationMetrics,
{
    let start = std::time::Instant::now();
    let result = authorizer.authorize(attributes);
    metrics.authorization_latency(start.elapsed());
    match result {
        Err(error) => {
            metrics.authorization_failed();
            Err(CadenceError::Unknown(error.to_string()))
        }
        Ok(Decision::Allow) => Ok(()),
        Ok(Decision::Deny) => {
            metrics.unauthorized();
            Err(CadenceError::AccessDenied(AccessDeniedError::new(
                "Request unauthorized.",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Auth(Decision);
    impl Authorizer for Auth {
        type Error = &'static str;
        fn authorize(&self, _: &Attributes) -> Result<Decision, Self::Error> {
            Ok(self.0)
        }
    }
    #[derive(Default)]
    struct Metrics {
        calls: Cell<u32>,
        denied: Cell<u32>,
    }
    impl AuthorizationMetrics for Metrics {
        fn authorization_latency(&self, _: std::time::Duration) {
            self.calls.set(self.calls.get() + 1);
        }
        fn authorization_failed(&self) {}
        fn unauthorized(&self) {
            self.denied.set(self.denied.get() + 1);
        }
    }

    #[test]
    fn denial_has_go_error_message_and_metrics() {
        let metrics = Metrics::default();
        let err = authorize(&Auth(Decision::Deny), &metrics, &Attributes::default()).unwrap_err();
        assert_eq!(err.to_string(), "Request unauthorized.");
        assert_eq!(metrics.calls.get(), 1);
        assert_eq!(metrics.denied.get(), 1);
    }

    #[test]
    fn allow_preserves_success() {
        let metrics = Metrics::default();
        assert!(authorize(&Auth(Decision::Allow), &metrics, &Attributes::default()).is_ok());
        assert_eq!(metrics.denied.get(), 0);
    }
}
