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

#[path = "go_strconv_isprint.rs"]
mod go_strconv_isprint;

/// Awaits `future`. A panic while it is polled is logged and returned as `Err(message)`.
/// Typed through the `Result` bound so `?` inside `future` keeps its error type.
pub async fn capture<T, E>(
    operation: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, String> {
    let mut future = pin!(future);
    let mut injected = armed_for_test(operation);
    poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| {
            if std::mem::take(&mut injected) {
                panic!("injected panic for test: {operation}");
            }
            future.as_mut().poll(cx)
        })) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(panic_message(operation, payload))),
        }
    })
    .await
}

/// Synchronous form of [`capture`], for calls such as `Manager::stop`.
pub fn capture_sync<T>(operation: &'static str, call: impl FnOnce() -> T) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| panic_message(operation, payload))
}

/// Prefix Go's `CapturePanic` gives every captured non-error panic.
pub const CAPTURED_PANIC_PREFIX: &str = "panic object is not error: ";

/// Go runtime error a nil dereference panic carries; `CapturePanic` hands it back as a plain error.
pub const NIL_DEREFERENCE_MESSAGE: &str =
    "runtime error: invalid memory address or nil pointer dereference";

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
    message.starts_with(CAPTURED_PANIC_PREFIX) || message == NIL_DEREFERENCE_MESSAGE
}

fn panic_message(operation: &str, payload: Box<dyn Any + Send>) -> String {
    let value = if let Some(text) = payload.downcast_ref::<&str>() {
        go_quote(text)
    } else if let Some(text) = payload.downcast_ref::<String>() {
        go_quote(text)
    } else {
        "<non-string panic payload>".to_string()
    };
    let message = format!("panic object is not error: {value}");
    eprintln!("ERROR Panic is captured operation={operation} error={message}");
    message
}

/// Go `%#v` uses `strconv.Quote`; Rust `Debug` spells controls differently (NUL is `\\0`).
/// Go `strconv.IsPrint` means Unicode L/M/N/P/S plus ASCII space: C1 controls,
/// separators, format characters and other non-printing code points must be escaped.
fn go_quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            ch if !go_strconv_isprint::is_print(ch) => {
                let code = ch as u32;
                if code <= 0xff {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code <= 0xffff {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Plain captured-panic status (gRPC Unknown / Thrift 0x05) for a panic Go raises as a string,
/// built without unwinding so the guard has no process side effects.
pub(crate) fn captured_panic_status(operation: &str, text: String) -> tonic::Status {
    tonic::Status::unknown(panic_message(operation, Box::new(text)))
}

#[cfg(test)]
thread_local! {
    /// Operations armed by tests, with the number of captures still to panic. Thread-local so each
    /// `#[tokio::test]` (current-thread runtime) sees only its own arms.
    static ARMED: std::cell::RefCell<Vec<(&'static str, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Test seam: the next `times` captures of `operation` on this thread panic at their first poll,
/// before the wrapped future runs.
#[cfg(test)]
pub(crate) fn arm_panic_for_test(operation: &'static str, times: usize) {
    ARMED.with(|armed| armed.borrow_mut().push((operation, times)));
}

/// Number of arms for `operation` that no capture has consumed yet.
#[cfg(test)]
pub(crate) fn armed_left_for_test(operation: &str) -> usize {
    ARMED.with(|armed| {
        armed
            .borrow()
            .iter()
            .filter(|(name, _)| *name == operation)
            .map(|(_, times)| *times)
            .sum()
    })
}

#[cfg(test)]
fn armed_for_test(operation: &str) -> bool {
    ARMED.with(|armed| {
        let mut armed = armed.borrow_mut();
        match armed
            .iter_mut()
            .find(|(name, times)| *name == operation && *times > 0)
        {
            Some(entry) => {
                entry.1 -= 1;
                true
            }
            None => false,
        }
    })
}

#[cfg(not(test))]
fn armed_for_test(_operation: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn armed_operation_panics_once_then_runs_normally() {
        arm_panic_for_test("armed-op", 1);
        assert_eq!(
            capture("armed-op", async { Ok::<u8, String>(7) }).await,
            Err(r#"panic object is not error: "injected panic for test: armed-op""#.to_string())
        );
        assert_eq!(
            capture("armed-op", async { Ok::<u8, String>(7) }).await,
            Ok(Ok(7))
        );
        assert_eq!(armed_left_for_test("armed-op"), 0);
        assert_eq!(
            capture("other-op", async { Ok::<u8, String>(7) }).await,
            Ok(Ok(7))
        );
    }

    async fn str_panic() -> Result<u8, String> {
        panic!("boom")
    }

    async fn formatted_panic() -> Result<u8, String> {
        panic!("value {}", 7)
    }

    #[tokio::test]
    async fn capture_passes_completed_results_through() {
        assert_eq!(
            capture("test", async { Ok::<u8, String>(7) }).await,
            Ok(Ok(7))
        );
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
    fn go_quote_matches_go_control_and_unicode_escaping() {
        assert_eq!(
            go_quote("\0\x07\x08\x0c\n\r\t\x0b\x7f\u{85}é"),
            r#""\x00\a\b\f\n\r\t\v\x7f\x85é""#
        );
        assert_eq!(
            go_quote("\u{2028}\u{a0}\u{301}\u{200b}\u{ad}"),
            r#""\u2028\xa0́\u200b\xad""#
        );
        assert_eq!(go_quote("\u{378}"), r#""\u0378""#);
        assert_eq!(go_quote("quote: \\\""), r#""quote: \\\"""#);
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
