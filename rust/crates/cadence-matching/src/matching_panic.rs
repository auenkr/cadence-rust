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

#[path = "go_isprint_tables.rs"]
mod go_isprint_tables;

/// Awaits `future`. A panic while it is polled is logged and returned as `Err(message)`.
/// Typed through the `Result` bound so `?` inside `future` keeps its error type.
pub async fn capture<T, E>(
    operation: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, String> {
    let mut future = pin!(future);
    let mut injected = armed_for_test(operation);
    poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| {
        if std::mem::take(&mut injected) {
            panic!("injected panic for test: {operation}");
        }
        future.as_mut().poll(cx)
    })) {
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

/// Go strconv.Quote, matching the pinned Go 1.25.14 implementation.
fn go_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for ch in text.chars() {
        match ch {
            '\u{0007}' => quoted.push_str("\\a"),
            '\u{0008}' => quoted.push_str("\\b"),
            '\u{000c}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{000b}' => quoted.push_str("\\v"),
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            c if !go_is_print(c) => {
                let n = c as u32;
                if n < 0x20 || n == 0x7f {
                    quoted.push_str(&format!("\\x{n:02x}"));
                } else if n < 0x10000 {
                    quoted.push_str(&format!("\\u{n:04x}"));
                } else {
                    quoted.push_str(&format!("\\U{n:08x}"));
                }
            }
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

// Exact Go 1.25.14 strconv.IsPrint lookup. The static data comes from the
// upstream generated tables in go_isprint_tables.rs, not Rust Unicode APIs.
fn go_is_print(ch: char) -> bool {
    use go_isprint_tables::*;
    let n = ch as u32;
    if n <= 0xff {
        return (0x20..=0x7e).contains(&n) || ((0xa1..=0xff).contains(&n) && n != 0xad);
    }
    if n < 0x10000 {
        in_ranges(PRINT16, n) && NOT_PRINT16.binary_search(&(n as u16)).is_err()
    } else {
        in_ranges(PRINT32, n)
            && (n >= 0x20000 || NOT_PRINT32.binary_search(&((n - 0x10000) as u16)).is_err())
    }
}

fn in_ranges<T: Copy + Ord + Into<u32>>(ranges: &[T], value: u32) -> bool {
    let i = ranges.partition_point(|range| (*range).into() < value);
    i < ranges.len() && (i % 2 == 1 || value == ranges[i].into())
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
        match armed.iter_mut().find(|(name, times)| *name == operation && *times > 0) {
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
        assert_eq!(capture("armed-op", async { Ok::<u8, String>(7) }).await, Ok(Ok(7)));
        assert_eq!(armed_left_for_test("armed-op"), 0);
        assert_eq!(capture("other-op", async { Ok::<u8, String>(7) }).await, Ok(Ok(7)));
    }

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

    #[test]
    fn go_quote_preserves_printable_range_starts_and_singletons() {
        assert_eq!(go_quote("\u{037a}\u{09d7}"), "\"\u{037a}\u{09d7}\"");
        assert_eq!(go_quote("\0\u{0085}\u{0378}"), "\"\\x00\\u0085\\u0378\"");
    }

    #[test]
    fn go_12514_panic_quote_oracle_matches() {
        let Ok(path) = std::env::var("MATCHING_GO_PANIC_ORACLE") else {
            return;
        };
        let fixture = std::fs::read_to_string(path).expect("read pinned Go panic quote oracle");
        for line in fixture.lines() {
            let (hex, expected) = line.split_once('\t').expect("oracle row delimiter");
            let scalar = u32::from_str_radix(hex, 16).expect("oracle codepoint");
            let ch = char::from_u32(scalar).expect("valid Go oracle scalar");
            assert_eq!(go_quote(&ch.to_string()), expected, "U+{scalar:04X}");
        }
    }
}
