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
- Run https://github.com/auenkr/cadence-rust/actions/runs/41177893560 at
  `cfb74b0ff5e6a0701dcf7f648207dd5ec3097e4b` concluded `failure` in the
  changed Matching runtime formatting gate, before Clippy. CI identified three
  rustfmt-only hunks in `launcher.rs` and `grpc_matching.rs`; applied them from
  the runner's emitted diff. The run did not validate compilation or tests.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41178156520 at
  `db60040f1fe2b34ffc61ee4598caf34f3496af46` passed formatting and entered
  Clippy, then failed Rust test compilation because new cases used
  `DomainConfiguration` instead of `DomainConfigurationUpdate` and the wrong
  `ActiveClusters` field. Corrected those test fixtures. The run's formatter
  artifact is not a passing test result.
- Added a Go-shaped `RequiredDomainDataKeys` map getter to persistence dynamic
  config, preserving the typed config default when absent and accepting an
  explicit empty-map override. The validator now uses Go map membership and
  formats the missing-key error with Go's map representation. Exact-head CI
  must validate this code and the expanded success matrix.
- Added successful Rust fixtures for every validator method, plus a core
  request-admission test that checks health turns unhealthy before the HTTP
  router rejects new calls. This strengthens the drain contract but does not
  supply the absent global quota-collection lifecycle owners.
- Added exact-error assertions for the Go open/closed visibility time-filter
  matrix, open/closed filter conflicts, and missing-domain precedence. This
  supplements the earlier coverage that asserted only that selected cases
  failed.

## Follow-up continuation

- At `be0432cc7dcffc39469bad1d7e09b6bebc34231d`, CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41178553763 was still
  `failure`: Clippy caught an `and_then(Some(...))` pattern in required-key
  binding and needless struct updates in test fixtures. The required-key
  binding now uses `map`; the two exhaustive request fixtures omit the
  redundant update. Formatting and tests were not reached on that head.
- This follow-up adds exact message checks for all Go open/closed start-time
  filter failures, the conflicting filter cases, and empty-domain validation.
  Run fresh exact-head formatting, Clippy and Frontend tests on Actions before
  treating this update as validated. The first run at `5c2983d36` was dispatched
  as https://github.com/auenkr/cadence-rust/actions/runs/41178727979, but is
  superseded by the Clippy correction in the next commit.
- Persistence remains the largest open item. Frontend opens the configured
  default SQL/Cassandra store through Matching's native adapter and performs a
  read-only domain metadata bootstrap. It still has no write-capable durable
  domain/metadata manager, no configured visibility manager, and no durable
  History or execution managers. Go's advanced/default visibility datastore
  selection is not connected to a manager.
- Shutdown currently flips readiness, waits the configured failure-detection
  interval, rejects new Frontend HTTP/gRPC requests, waits the request-drain
  interval, then tears down Matching runtime/membership where present and
  aborts listeners. The launcher does not own Go-equivalent Frontend workflow
  and admin handler stop hooks or global quota-collection owners, so the full
  `service.go` stop sequence is still incomplete.

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

## Continuation checkpoint — 2026-10-31 (frontend-core2)

- Exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41183986922
  checked `ac5d27af642fe7bd997ac3a0af2f178d9b7f0681` and concluded `failure` in
  Clippy before formatting/tests. The failure is a missing `Duration` import in
  `cadence-frontend/src/lifecycle.rs`; fixed in this checkpoint. Dispatch a
  fresh package run after pushing this commit and record its conclusion.
- Follow-up run https://github.com/auenkr/cadence-rust/actions/runs/41184260635
  checked `61224ce3ce73eeb51de639f04c9ca4a6ecf421bf` and concluded `failure` in
  Frontend formatting: rustfmt sorts the shared lifecycle re-export before
  imports. Applied the runner's one-line ordering patch. The run passed its Go
  oracle gates and Clippy; formatting stopped the test job.
- Validator audit: Go has 16 validation entry points and 15 table-driven test
  groups; Rust now has nil-request coverage for every entry point and expanded
  error, mutation, success, filter, permission, and failover matrices. Do not
  call the test matrix complete until a source-by-source audit confirms every
  Go table row and CI passes the exact head. A literal audit found the Go
  600-second timeout / 300-second maximum example was only represented by a
  smaller equivalent boundary; added an assertion for the exact Go case.
- Persistence remains open: configured Frontend startup currently reuses
  Matching's default-store SQL/Cassandra handles only for Matching tasks and a
  read-only domain-metadata source. `AppState` still owns concrete in-memory
  domain and execution stores. There are no durable Frontend metadata,
  visibility, History, or execution managers, nor Go-compatible selection of
  the advanced visibility datastore. Task snapshots are development-only and
  do not satisfy this requirement.
- Lifecycle remains open: the readiness/failure-detection/admission/drain
  ordering is wired through the shared launcher, and background timer/snapshot
  workers stop after the request window. The Rust launcher has no handler/admin
  lifecycle owners or global quota collection instances to stop, so the
  limiter hook remains empty and complete `service.go` ownership parity is not
  established.
- No Cargo command was run locally. The next exact-head GitHub Actions run must
  validate formatting, Clippy, Frontend tests, and the configured development
  runtime smoke. Continue the implementation after confirming that result.

## Exact-head follow-up — 2026-10-31

- Run https://github.com/auenkr/cadence-rust/actions/runs/41178553763 at
  `be0432cc7dcffc39469bad1d7e09b6bebc34231d` concluded `failure` in Clippy.
  It reported the non-idiomatic `and_then(Some(...))` dynamic-map binding and
  needless struct update warnings in test code.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41178835295 at
  `45e7c95d44070cdf40e98c59e82674d8e1aa004e` concluded `failure` in the
  changed Matching runtime formatting gate, before Clippy/tests. The runner
  reported a rustfmt-only hunk in `cadence-persistence/src/dynamic_config.rs`;
  that hunk is fixed in the next checkpoint. It does not validate Frontend.
- Current uncommitted work adds exact error checks for missing task-list type,
  missing/oversized task lists, reset-sticky domain/execution, and failover
  request shape/lockdown. Its CI validation remains pending.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41179169677 at
  `74930ac159f709255b8ea0d17238463d1d3046ef` concluded `failure` during
  Clippy/test compilation. The only compiler errors were the new matrix's
  incorrect `FailoverDomainRequest` field and moved time-filter fixture; both
  are corrected in the next commit. No Frontend tests ran on that head.
- The branch still has no write-capable durable Frontend domain/metadata,
  visibility, History, or execution manager and no full Go handler/quota stop
  owners. Read-only domain bootstrap and listener drain are not full parity.

## Continued implementation — 2026-10-31

- Run https://github.com/auenkr/cadence-rust/actions/runs/41179407283 at
  `642538a2720d28e7ff5611e690e1fe3208033d95` concluded `failure` in Frontend
  Clippy/test compilation. It identified six `needless_update` warnings in the
  validator test fixtures; removed all six on the next working head. This run
  did not reach unit tests.
- Exact-head validation of that cleanup is pending; dispatch a fresh
  `cadence-frontend` workflow after committing the continued changes.
- Re-audit confirms persistence is still not production-complete: the shared
  Matching configured adapters offer read-only Go domain metadata, Frontend
  domain mutations and execution state remain in-memory, and visibility and
  History have no durable manager. Go SQL/Cassandra selector parity is only
  present for the Matching task store/domain metadata reader, not the five
  requested Frontend managers.
- Re-audit of `go/service/frontend/service.go:319-376` confirms Rust has the
  readiness/failure-detection/request-drain timing but no distinct Frontend
  workflow/admin handler stop hooks, seven global quota collection owners, or
  Go resource stop owner wired to the lifecycle. Those remain required for
  shutdown parity.
- The six Clippy fixture corrections are unvalidated until the exact-head CI
  result is recorded below. Cargo remains prohibited on the development host.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41180118508
  validated `b4fda09b4e53622aa385ffccf57118d07771d578`. Formatting, Clippy,
  and test compilation passed; 11 of 12 Frontend tests passed. The remaining
  required-domain-data expectation used JSON-array spelling while the Go map
  formatter correctly produced `map[tier:true]`; corrected the assertion for
  the next exact-head run.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41180458747
  validated `915aa9c0a4f7deb973e9c5f4beeec8e62b55b181` and failed the
  Frontend formatting check before Clippy or tests. Applied the runner's one
  rustfmt hunk. Also added the remaining closed-workflow TypeFilter/StatusFilter
  conflict pair and explicit missing-domain cases for all domain-scoped
  validator methods; these are awaiting exact-head CI.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41180682856
  validated `fcacf2b5c660fdceed5f66f6f564f40f3c39b026` and failed formatting
  before Clippy/tests. Applied the exact runner diff to one line in the new
  domain-presence matrix. The matrix now also covers wrong tokens for register
  and non-failover update. A new exact-head run is required.
- Added explicit empty task-list validation through the partition-list entry
  point and set the validator matrix's graceful failover limit to the Go test's
  300-second value. These latest test corrections are pushed after run
  `41180907810` was dispatched and still need a new exact-head CI run.
- Canceled run `41180907810` before Rust validation because it targeted the
  earlier test head; dispatched exact-head run
  https://github.com/auenkr/cadence-rust/actions/runs/41181022476 for
  `3d57697610a143e317513f613a8484c7293d7c0e`.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41179758023 at
  `3bc2dcca870ea3875d0915f6769d9b09ae9dcbe4` passed Frontend Clippy but failed
  the crate formatting step. The runner emitted 22 rustfmt hunks in
  `lifecycle.rs` and `request_validator.rs`; applied those exact hunks locally
  and queued a new exact-head package run. Unit tests were skipped.

## Validator matrix checkpoint — 2026-10-31

- Exact-head package CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41181055161 passed on
  `97ee38218fbf77970269d3937eb8e82199787285` (formatting, Clippy, and all
  Frontend tests). Rust parity tests cover all 91 named cases in
  `go/service/frontend/api/request_validator_test.go`, grouped into Rust test
  functions.
- Fresh uncached package run
  https://github.com/auenkr/cadence-rust/actions/runs/41181338783 also passed
  on `97ee38218fbf77970269d3937eb8e82199787285`; CI selected its fresh runner
  target, skipped rust-cache, and passed formatting, Clippy, and Frontend tests.
- This closes the request-validator test-matrix item. Production SQL/Cassandra
  domain, metadata, visibility, History, and execution managers remain absent.
  Frontend stop hooks for separately owned workflow/admin handlers and global
  limiter collections also remain unwired; this package CI run does not validate
  those gaps.

## Continuation verification — 2026-10-31

- Fresh-target Frontend package workflow
  https://github.com/auenkr/cadence-rust/actions/runs/41181848148 completed
  with `success` at head `35e84865e01e5437a7a6afb16055741d531c88ab`.
  This confirms formatting, Clippy and package tests for the pushed branch head;
  no Cargo command was run locally.
- The Go request-validator test matrix remains closed: the previous fresh-target
  run `41181338783` passed all 91 named Go cases ported into Rust tests. No
  validator code changed after that tested commit.
- Lifecycle audit against `go/service/frontend/service.go:319-376`: the Rust
  launcher publishes unhealthy readiness, waits the configured failure
  detection interval, rejects new HTTP/YARPC calls, leaves up to one second for
  drain, then tears down listeners and the configured Matching runtime and
  membership. HTTP and YARPC share one Frontend admission gate, so workflow and
  admin endpoints reject new calls together. However, there are no Frontend
  global quota collection owners/`OnStop` implementations to wire yet. This
  does not close exact shutdown parity.
- Persistence audit remains open and unchanged: `AppState` still exposes
  `InMemoryDomainStore` and `InMemoryExecutionStore`; the configured SQL/
  Cassandra code opened by the launcher is Matching task persistence plus
  read-only Go domain metadata. Production Frontend has no write-capable
  domain/metadata manager, configured visibility manager, durable History
  manager, or durable execution manager selected from the Go datastore config.
  The `data_dir` JSON snapshots are development durability, not those managers.
- No production persistence source or lifecycle owner code changed in this
  continuation. Do not treat this checkpoint as completion: the branch's
  remaining blockers are the five durable Frontend managers with Go SQL/
  Cassandra selection, and the Frontend global quota stop owners. Existing
  branch scope rules also reserve workflow handlers and wrappers for their
  respective owners.
- After pushing this audit entry, the fresh-target package workflow
  https://github.com/auenkr/cadence-rust/actions/runs/41182200811 completed
  with `success` on exact head `def795959ee2b7771c34644a2c6a227fef2748ad`.
  It passed the workflow's Go oracles, Formatting, Clippy, Test, and Frontend
  runtime smoke steps. The audit-only commit that follows does not change Rust
  source.

## Shutdown worker ownership continuation — 2026-10-31

- Added the Go `stopC` equivalent after the Frontend request drain. The shared
  launcher signals its background timer/snapshot workers and joins them before
  listener teardown. The snapshot owner flushes development snapshots once on
  this boundary; the timer worker exits promptly. A unit test verifies the
  shutdown broadcast, and the existing admission test verifies readiness fails
  before requests are rejected.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41183097444 checked
  exact head `6d06117efff74ea22a61b5e4ca9e1237b9806344`; Go oracles passed, but
  the run failed its changed-Matching rustfmt gate on the snapshot interval
  line. Applied the exact one-line formatting correction locally. This run did
  not reach Clippy or tests; dispatch fresh CI on the corrected head.
- The 91-case request-validator matrix remains closed by fresh uncached run
  https://github.com/auenkr/cadence-rust/actions/runs/41181338783 (head
  `97ee38218fbf77970269d3937eb8e82199787285`, success). No validator source
  changed afterward.
- Production persistence remains open. Current configured startup retains the
  Matching SQL/Cassandra task-store connection and reads Go domain metadata,
  while `AppState` still uses `InMemoryDomainStore` and
  `InMemoryExecutionStore`. There are no write-capable Go-compatible domain,
  metadata, visibility, history, or execution manager adapters selected by
  `defaultStore`/visibility-store configuration. Do not count development JSON
  snapshots or Matching task persistence as these Frontend managers.
- The Go service drain order still lacks separately owned Frontend workflow and
  admin handler stop hooks and the seven global quota collection owners from
  `service.go`. The skeleton does not currently construct those objects; full
  parity requires adding their actual owners and lifecycle registration, not
  empty hooks. Background worker shutdown is now covered, but this does not
  close all lifecycle ownership.
- Fresh-target run https://github.com/auenkr/cadence-rust/actions/runs/41183322028
  validated source head `ed6f8294b06d19ffe2bb2fefda99813e0ec58c2b` and concluded
  `success`. Go oracle checks, changed Matching formatting, Frontend formatting,
  Clippy, Frontend tests, and the Go-development-config runtime smoke all passed.
  The run confirms the shutdown worker ownership and formatting correction.

## Shared Frontend drain sequence — continuation

- Extracted the Go `service.go` health/failure-detection/handler-stop/
  limiter-stop/request-drain/resource-stop order into
  `cadence-service-core::frontend_lifecycle` and wired the running launcher
  through it. The public `cadence-frontend::lifecycle::DrainPlan` now reuses
  that shared timing implementation. Added tests for Go's duration split and
  hook order.
- The launcher maps health to readiness failure, handler stop to the shared
  admission gate, and resource stop to signaling/joining the timer and optional
  snapshot workers. The limiter hook is currently empty because the Rust
  Frontend runtime has no global quota collection owners; this remains an
  explicit lifecycle gap, not a completed Go stop-hook port.
- Persistence audit remains unchanged: no production write-capable Frontend
  domain/metadata, visibility, History, or execution manager, and no manager
  set selected from both defaultStore and visibilityStore. Existing Matching
  task persistence and read-only domain metadata code are not substitutes.
- The lifecycle refactor needs exact-head `cadence-frontend` CI, including
  formatting, Clippy, tests, and runtime smoke. Cargo was not run locally.
- Initial shared-drain run https://github.com/auenkr/cadence-rust/actions/runs/41183789672
  checked the previous branch head `074c274ef904b67e1af48183f94a76c407b6433e`
  and failed only the changed-Matching formatting gate. Applied its emitted
  rustfmt hunk to `launcher.rs`; this run did not reach Clippy or tests.
