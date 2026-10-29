# Raw lookup persistence metric regression tests

At master `7828475bc`, added an inspecting source test in `matching_persistence.rs`. It blocks the raw domain lookup after entry, observes exactly one `persistence_requests` event while the source is still active, and checks latency/error events are emitted only after the source returns. The existing raw SQL test also exercises one accepted lookup followed by limiter rejection, asserting each call contributes one request and that the captured panic itself contributes no latency or error event.

Go ordering was inspected in `go/common/persistence/wrappers/metered/base.go`: `call` increments the request counter, starts `time.Now`, invokes the operation, then records duration and error metrics. The raw Rust path records the request before limiter acquisition and starts its `Instant` before acquisition, so rejection time is included. A deterministic assertion of limiter wait duration is not available here because elapsed metrics use `Instant` while the injectable limiter clock only controls token accounting.

Validation: `git diff --check` only. No Cargo, rustfmt, Go, build, runtime tests, or CI dispatch was run, as requested. These tests remain unvalidated until the parent dispatches the next Actions run; run `40806302891` covers only the parent’s current source.
