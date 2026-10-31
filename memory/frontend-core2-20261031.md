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
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41175558484
  validated head `7338118a6056143f15b16877237a4f75fb664f02` and concluded
  `failure` at the format check. The existing frontend files on this branch
  have broad rustfmt differences, including the client changes and validator
  module. Use CI's `format_frontend_files` input to generate the formatting
  patch artifact, apply that artifact locally, then dispatch package CI again.
- The formatter job in run
  https://github.com/auenkr/cadence-rust/actions/runs/41175836837 generated and
  uploaded the patch; formatting and Clippy passed on its runner, and four
  validator/lifecycle tests passed. The test job found the visibility test
  fixture had not selected an advanced store by domain, so the assertion did
  not match Go behavior. Corrected the fixture to set
  `system.readVisibilityStoreName=es` for domain `d`. Also aligned Matching's
  default outbound timeout to Go's 60 seconds and added its 120-second long-poll
  helper. The timeout call sites and test fixture require fresh CI validation.
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41176185814
  validated head `981e17c690d108d42a2ce2a2eb0f3cba394d9de8` and concluded
  `success`: Frontend formatting, package Clippy and tests all passed. It also
  exercised the full validator nil-request test and the corrected advanced
  visibility selector fixture. Applied the runner's final four-line import
  ordering patch locally; run the package workflow once more on the resulting
  exact head before treating it as the validated checkpoint.
- Exact-head package CI:
  https://github.com/auenkr/cadence-rust/actions/runs/41176534728 validated
  `f3ea32c83fe061effd54089863ad65d81b0b4928` and concluded `success`.
  Formatting, Frontend package Clippy and all five current Frontend unit tests
  passed. The test set includes nil-request coverage for all 16 validator
  methods, but it is still not the full Go test matrix.

## Continuation update — 2026-10-31

- Run https://github.com/auenkr/cadence-rust/actions/runs/41177528727 at
  `ba8d475dee603c6821be483b4a82422dd33d2bfc` concluded `failure` in Clippy
  compilation. The Frontend admission interceptor wrapped generated tonic
  servers before applying their decoding limits; the limit methods then
  resolved against `InterceptedService` and failed. Reordered construction to
  apply each generated server's configured decode limit before wrapping it.
  This correction still needs exact-head CI validation.
- Expanded `request_validator.rs` coverage against Go's tables for the missing
  open/closed/archived visibility cases, all filter conflict shapes, default
  page-size mutation, required domain data, admin-protected operations,
  active-active/passive failover, invalid timeouts, and live dynamic updates.
  The complete Go table has not yet been ported; exact-head CI must confirm this
  test addition compiles and passes.
- No change closes the production persistence gap: current Frontend startup
  opens the default Matching backend and reads domain metadata, but only has a
  read-only metadata source. The durable write-capable domain/metadata,
  visibility, history, and execution managers, including Go SQL/Cassandra
  selection, are still missing.
- Shutdown is wired into Frontend listener lifecycle: readiness flips first,
  `frontend.shutdownDrainDuration` controls failure detection and the final
  one-second request window, then the HTTP/gRPC/TChannel listener tasks are
  torn down. Matching membership/runtime shutdown still follows the shared
  launcher sequence. Stop hooks for Go's handler/admin handler, limiter
  collections, and base resource do not yet exist as separately wired owners;
  compare and close this lifecycle gap before calling it complete.
- Cargo was not run locally. The next package run must be dispatched after
  pushing the current fixes; record its exact head and terminal conclusion.

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

## Continuation checkpoint — 2026-10-31

Changes on the continuation head wire Frontend-only shutdown through the
shared launcher. HTTP health becomes unhealthy first; after the live
`frontend.shutdownDrainDuration` failure-detection interval, the router rejects
new requests, then the launcher waits the final `min(duration, 1s)` drain
window before listener teardown. Other service binaries keep their existing
shutdown path. A Rust test matrix now exercises representative Go validator
error ordering, page-size mutation, task-list/execution identity checks,
visibility filters, admin protection, and domain failover validation, in
addition to nil-request coverage for every validator entry point.

Frontend startup now opens the configured default datastore through Matching's
native persistence adapter (Cassandra, MySQL, or PostgreSQL), retains the
connection/pool, and seeds the existing domain view from the Go-compatible
domain metadata source when available. This is only read/bootstrap support.
The following originally requested persistence managers remain incomplete:
write-capable domain and metadata managers, configured visibility manager,
durable History manager, and durable execution manager; SQL/Cassandra backend
selection for the visibility datastore is also not wired to a manager. The
Frontend still uses in-memory execution state and in-memory domain writes.

CI for this continuation must include `cadence-frontend` Clippy and tests on a
fresh target, plus the Frontend formatting patch for the new launcher and
shared HTTP handler changes. Record exact head and final run conclusion below.
- Previous skeleton analysis and baseline CI: `frontend-core-20261031.md`.
