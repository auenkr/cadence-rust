# Matching native History TChannel task-start (2026-10-28)

Status: implemented, NOT built/tested (no local Cargo; parent owns CI). No production completion claim.

- `matching_history_tchannel_native.rs` (private `#[path]` child `native` of `matching_history_tchannel.rs`):
  byte-preserving request mapping (`GoString` -> `TypedValue::Bytes`) and response decoding for
  RecordDecisionTaskStarted / RecordActivityTaskStarted into `cadence_proto::native` models;
  `TChannelHistoryClient::record_{decision,activity}_task_started_native` reuse `call` (headers/errors).
- `matching_history_native.rs`: early `Unimplemented` guards and `unsupported_tchannel` removed; the
  `HistoryConnection::Tchannel` arm now calls the raw methods inside the existing retry/redirect/deadline/metrics path.
  The old "tchannel raw call is explicitly unsupported" test was removed (no longer true).
- Mapping mirrors the legacy converters (same field ids, task-list kind offset, int seconds -> Duration,
  nanos -> Timestamp), but keeps workflow/run IDs, task list names, identity, checksum, query map keys/types,
  event identity/request ids, activity id/type/domain, retry reasons, header keys as raw bytes.
- Supported event graph: DecisionTaskScheduled(4), DecisionTaskStarted(5), ActivityTaskScheduled(9) with
  retry policy and header map. Other event types are rejected with an error, not dropped.
- Tests added (unit, typed wire): raw request bytes/field ids + codec round trip, decision response with raw query
  map and transient events, activity response with raw header/retry and unknown-event rejection.
- Gap: no scripted TChannel peer test for the full native call yet (only typed-wire tests).
- Legacy `String` path/API unchanged. Parent's clone_on_copy / useless-into fixes untouched.
- Needs CI: clippy + `cargo test -p cadence-matching`.

## Parent integration additions

Added the native TChannel module to the CI formatting list. Corrected retry-policy duration presence: Go FromRetryPolicy wraps each scalar interval (including absent/default zero) in a non-nil duration. Native decoder now does the same.

Extended the existing generated_go_history_server_accepts_native_tchannel_calls ignored oracle (already invoked by matching-dependency-oracles.yml) and go/tools/cadence-rust-history-peer raw mode. The Go peer asserts invalid-byte domain, workflow/run, request ID, poller identity and task-list bytes; Rust asserts raw workflow type, query map key/type/args, activity/header/retry/domain bytes and zero interval presence in the replies. The new live assertions have NOT run. No new ignored-test harness gap was introduced; production registration remains pending.
