//! Injectable TChannel JSON boundary for the Ringpop service.
//!
//! This module intentionally contains transport glue only. It does not implement
//! SWIM membership, discovery, or any production Ringpop responder; callers must
//! supply a real handler. The handler is invoked with the TChannel method name,
//! decoded transport headers, and raw JSON request bytes.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

pub type RingpopFuture =
    Pin<Box<dyn Future<Output = Result<RingpopResponse, RingpopError>> + Send>>;
type RingpopHandlerFn =
    dyn Fn(String, Option<HashMap<String, String>>, Vec<u8>) -> RingpopFuture + Send + Sync;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingpopResponse {
    /// `None` serializes as JSON `null`, matching Go's nil response-header map.
    pub headers: Option<HashMap<String, String>>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingpopError {
    /// Application-level failure. The server encodes this as
    /// `{"type":"error","message":"..."}` with the TChannel application-error
    /// flag set.
    Application(String),
    /// Transport/system failure. The server sends a TChannel ERROR frame.
    System(String),
}

#[derive(Clone)]
pub struct RingpopHandler {
    inner: std::sync::Arc<RingpopHandlerFn>,
}

impl RingpopHandler {
    pub fn new<F, Fut>(function: F) -> Self
    where
        F: Fn(String, Option<HashMap<String, String>>, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RingpopResponse, RingpopError>> + Send + 'static,
    {
        Self {
            inner: std::sync::Arc::new(move |method, headers, body| {
                Box::pin(function(method, headers, body))
            }),
        }
    }

    pub(crate) fn call(
        &self,
        method: String,
        headers: Option<HashMap<String, String>>,
        body: Vec<u8>,
    ) -> RingpopFuture {
        (self.inner)(method, headers, body)
    }
}
