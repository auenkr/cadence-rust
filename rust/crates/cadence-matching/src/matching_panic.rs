//! Go `log.CapturePanic`: a panic in a matching handler, the membership loop or a
//! semaphore stop is logged and returned as an error, so it does not unwind through
//! the server task.
//!
//! Go sets the error to `fmt.Errorf("panic object is not error: %#v", v)` for any
//! panic value that is not an error. Rust panic payloads are never Go errors, so the
//! message is always that form.

use std::any::Any;
use std::future::{poll_fn, Future};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::task::Poll;

/// Awaits `future`. A panic while it is polled is logged and returned as `Err(message)`.
/// Typed through the `Result` bound so `?` inside `future` keeps its error type.
pub async fn capture<T, E>(
    operation: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, String> {
    let mut future = pin!(future);
    poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
        Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
        Ok(Poll::Pending) => Poll::Pending,
        Err(payload) => Poll::Ready(Err(panic_message(operation, payload))),
    })
    .await
}

/// Synchronous form of [`capture`], for calls such as `Manager::stop`.
pub fn capture_sync<T>(operation: &'static str, call: impl FnOnce() -> T) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| panic_message(operation, payload))
}

/// Prefix Go's `CapturePanic` gives every captured non-error panic.
pub const CAPTURED_PANIC_PREFIX: &str = "panic object is not error: ";

/// True for the plain Go error a captured panic becomes. Go's Thrift mapper passes a
/// plain error through unchanged, so Thrift and TChannel send it as an unexpected
/// system error, not a declared `InternalServiceError`. Other untyped statuses (for
/// example the fault-injected `Unhandled` error) stay typed `InternalServiceError`.
pub fn is_captured_panic(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unknown
        && status.details().is_empty()
        && is_captured_panic_message(status.message())
}

/// Same check on a message that already crossed the dispatch boundary as a string.
pub fn is_captured_panic_message(message: &str) -> bool {
    message.starts_with(CAPTURED_PANIC_PREFIX)
}

fn panic_message(operation: &str, payload: Box<dyn Any + Send>) -> String {
    let value = if let Some(text) = payload.downcast_ref::<&str>() {
        format!("{text:?}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("{text:?}")
    } else {
        "<non-string panic payload>".to_string()
    };
    let message = format!("panic object is not error: {value}");
    eprintln!("ERROR Panic is captured operation={operation} error={message}");
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn str_panic() -> Result<u8, String> {
        panic!("boom")
    }

    async fn formatted_panic() -> Result<u8, String> {
        panic!("value {}", 7)
    }

    #[tokio::test]
    async fn capture_passes_completed_results_through() {
        assert_eq!(capture("test", async { Ok::<u8, String>(7) }).await, Ok(Ok(7)));
        assert_eq!(
            capture("test", async { Err::<u8, String>("denied".into()) }).await,
            Ok(Err("denied".to_string()))
        );
    }

    #[tokio::test]
    async fn capture_turns_a_str_panic_into_the_go_message() {
        assert_eq!(
            capture("test", str_panic()).await,
            Err(r#"panic object is not error: "boom""#.to_string())
        );
    }

    #[tokio::test]
    async fn capture_turns_a_formatted_panic_into_the_go_message() {
        assert_eq!(
            capture("test", formatted_panic()).await,
            Err(r#"panic object is not error: "value 7""#.to_string())
        );
    }

    #[test]
    fn capture_sync_reports_a_panic_and_passes_values_through() {
        assert_eq!(capture_sync("test", || 3), Ok(3));
        assert_eq!(
            capture_sync::<()>("test", || panic!("stop blew up")),
            Err(r#"panic object is not error: "stop blew up""#.to_string())
        );
    }

    #[test]
    fn capture_sync_reports_a_non_string_payload() {
        assert_eq!(
            capture_sync::<()>("test", || std::panic::panic_any(42_u8)),
            Err("panic object is not error: <non-string panic payload>".to_string())
        );
    }
}
