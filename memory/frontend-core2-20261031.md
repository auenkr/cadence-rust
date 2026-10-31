# Frontend core continuation — 2026-10-31

Branch: `frontend-core2`, based on `origin/frontend-core` at start.

## Work in this slice

- Replaced the two-method Frontend validator skeleton with all 16 methods in
  `go/service/frontend/api/request_validator.go`, including validation order,
  Go error text, page-size default mutation, visibility filters, admin token,
  required domain data, retention, lockdown, and failover timeout checks.
- Added dynamic overlay reads to validator limits so active property changes
  are observed on each validation call. The API accepts the runtime overlay
  via `RequestValidator::with_dynamic_config`.
- Replaced channel-only History and Matching constructors with generated
  `HistoryApiClient` and `MatchingApiClient` wrappers. Added peer TLS channel
  construction through `cadence-service-core::grpc_tls`, including configured
  mTLS credentials and CA/server-name behavior.
- Corrected `UpdateDomainRequest::is_failover` to match Go's pointer-presence
  rule: active-cluster fields determine failover; the failover timeout alone
  does not. Empty-but-present active cluster data still counts as present.
- Added a Frontend lifecycle shutdown helper with Go's failure-detection and
  one-second request-drain split and hook order (health, handlers, limiters,
  resource), plus timing tests. The current launcher has not wired it into the
  running Frontend process.
- Added per-request gRPC metadata and the Go 30-second default timeout helper
  to both outbound generated clients. Extended dynamic-config string reads to
  support Go's domain-filtered visibility-store selector.
- Added focused validator tests for page mutation/visibility limits, validation
  order, and execution identity checks.

## CI evidence

- Requested package workflow at the first pushed head:
  https://github.com/auenkr/cadence-rust/actions/runs/41174450063
  head `e268cf3c5ed7e1160e5df74a2e663b6f6dd144fd`. It was `in_progress` at the
  last read; that run predates the follow-up dynamic-config changes below.
- A new package CI run is required after the current commit is pushed. Record
  its final conclusion here. Run `41174623555` at
  `39e0220a3bf5d60f7608270357f42b6d09cc750e` was in progress before the latest
  client and lifecycle edits; request a fresh final run after pushing them.
  No Cargo command was run locally.

- Follow-up run https://github.com/auenkr/cadence-rust/actions/runs/41174765836
  validated head `d8a0bafb9a20d4e7660e3a8bc69d2f9bad6210b2` and concluded
  `failure` before Clippy/tests because rustfmt required the new
  `string_domain` signature on one line. This was a formatting-only CI failure;
  corrected in commit `6ecd411348678e5dcd435a465e3699afeb094b9c`.
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41175118404
  validated head `6ecd411348678e5dcd435a465e3699afeb094b9c` and concluded
  `failure` during Clippy compilation: `string_domain` borrowed a string from
  a temporary selected JSON value. Changed the fallback path to return an
  owned string. A new table-like nil-request test now also covers all 16
  validator entry points; both changes need exact-head CI validation.
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41175347808
  validated head `e4f0ec4db8a36284d331312cf33eb11cd7d6574e` and concluded
  `failure` during Clippy: the gRPC TLS builder returned
  `Option<Option<ClientTls>>` when TLS config was optional, and the sticky-task
  request was imported from the wrong API module. Flattened the optional TLS
  result and moved the request import to `service_types`; dispatch package CI
  again after this commit.

## Remaining gaps (this branch is not Frontend complete)

- Wire the shared dynamic overlay into the production Frontend validator
  construction path. The constructor seam is present, but current Frontend
  launcher reuses the Matching launcher and does not yet instantiate a native
  Frontend dependency graph.
- Make dynamic property bindings fully Go-equivalent, including the complete
  default/filter table and warning metrics/logging for max ID lengths.
- History/Matching clients still need Go call-site semantics: membership/ring
  peer discovery and refresh, TChannel variants, per-call routing, auth metadata
  where configured, timeout/retry/error classification, and parity tests.
- Frontend persistence is not wired. `cadence-persistence` currently exposes
  in-memory domain/execution stores and Matching-specific native SQL/Cassandra
  components; production Frontend needs domain, metadata, visibility, history,
  and execution managers matching Go datastore selection.
- Go Frontend service lifecycle is not wired: startup ownership, readiness and
  listener health drain, failure-detection delay, handler stop, ratelimiter
  stop, request drain, and resource shutdown remain open.
- Port the Go request-validator test matrix beyond the focused tests here, then
  run exact-head Clippy, formatting and crate tests on GitHub Actions.
- No mixed original-Go History/Matching service validation, SDK workflows,
  failure/restart/recovery gate, or Frontend parity audit has run for this
  branch.

## Source pointers

- Go: `go/service/frontend/api/request_validator.go`,
  `go/service/frontend/validate/errors.go`, `go/service/frontend/service.go`.
- Rust: `rust/crates/cadence-frontend/src/request_validator.rs`,
  `history_client.rs`, `matching_client.rs`, and
  `rust/crates/cadence-api/src/service_types.rs`.
- Previous skeleton analysis and baseline CI: `frontend-core-20261031.md`.
