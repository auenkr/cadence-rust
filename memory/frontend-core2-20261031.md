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
- The test-source inventory has 15 table-driven Go groups, 88 named rows, and
  24 distinct expected-error messages; all 24 error strings are represented in
  Rust after adding the exact 600/300 failover timeout case. This verifies
  message coverage, while row-by-row request/condition coverage is still open.
- Exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41184529249
  checked `60a61a767dea5c6e067b1db49d798087224687fe` and concluded `success`.
  Frontend formatting, package Clippy and tests passed, along with the enabled
  Go oracle and compatibility checks. No Cargo command was run locally.
- Current branch-head run https://github.com/auenkr/cadence-rust/actions/runs/41184847564
  checked `983669b1229f318a884e5d252bbf4ce37ad18a7b` and concluded `success`.
  It passed the Frontend formatting, Clippy and test steps and enabled Go
  oracle checks. This is the latest full Rust validation; only this evidence
  paragraph is added in the following documentation commit.
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

## Continuation — Frontend lifecycle and validator audit

- Corrected Frontend-only resource ordering in the shared launcher: once the
  final request-drain interval completes, the dedicated `cadence-frontend`
  process now aborts and joins its listener tasks and TChannel task before
  returning from the resource-stop phase. Combined service runs retain their
  Matching membership/runtime teardown order. CI is pending for this change.
- The first lifecycle adjustment moved resource worker joins ahead of the
  request-drain window. A source comparison with Go's `WorkflowHandler.Stop`
  and `AdminHandler.Stop` showed that Rust timer/snapshot workers belong to
  resource shutdown, after the drain. That adjustment was reverted; only
  request admission stops before the drain, and resource-owned workers are
  signalled/joined after it. This exact corrected head needs fresh CI.
- Audited Go's 15 table-driven `request_validator_test.go` groups (88 named
  case rows) against Rust tests. All Go rows map to semantic cases in the Rust
  suite; the test case names are grouped by behavior rather than copied
  verbatim. The per-Go-group row counts are: RefreshWorkflowTasks 3,
  DescribeTaskList 7, ListTaskListPartitions 6, GetTaskListsByDomain 3,
  ResetStickyTaskList 6, CountWorkflowExecutions 3,
  ListWorkflowExecutions 4, ListOpenWorkflowExecutions 9,
  ListClosedWorkflowExecutions 11, ListArchivedWorkflowExecutions 4,
  DeprecateDomain 4, DescribeDomain 4, UpdateDomain 7, RegisterDomain 7,
  FailoverDomain 10. Rust coverage is in `go_request_validator_success_cases_cover_all_entry_points`,
  `go_request_validator_error_and_mutation_matrix`,
  `go_domain_presence_matrix_covers_every_domain_scoped_validator`,
  `go_request_validator_limit_filter_and_lockdown_matrix`,
  `go_request_validator_remaining_table_cases`,
  `go_visibility_matrix_checks_exact_time_filter_and_filter_conflict_errors`,
  `go_validator_matrix_covers_task_list_reset_and_failover_boundaries`,
  `go_table_covers_visibility_limits_and_update_failover_lockdown`, and
  `go_failover_timeout_table_case_600_exceeds_maximum_300`. Exact messages,
  validation ordering, page-size mutation, both closed-filter conflict pairs,
  name/retention limits, admin tokens, failover modes and timeout boundaries
  are asserted. This closes the source case audit; exact-head CI after the
  launcher change remains pending.
- Persistence remains unclosed. `AppState` and the handlers still directly
  depend on `InMemoryDomainStore` / `InMemoryExecutionStore`; Matching's native
  SQL/Cassandra adapters only provide Matching task storage and read-only
  domain metadata. Durable Go-compatible domain/metadata, visibility, history,
  and execution managers plus default/advanced SQL/Cassandra selection are not
  implemented. Do not claim this branch complete until they are wired through
  the Frontend handler graph.
- The lifecycle path still has no actual global quota collection instances to
  stop, so its Go limiter-stop slot is empty. The stop-order correction above
  does not close the missing production persistence or quota-owner gaps.
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41185490422 was
  dispatched for the intermediate, now superseded lifecycle ordering at
  `f03448dc44c26a0c116e661b658b4db0dabe80ec`; it was still in progress when
  corrected. Do not use its result as validation of the corrected code.
- Exact-head package CI for the corrected lifecycle path is running at
  https://github.com/auenkr/cadence-rust/actions/runs/41185646557, head
  `fd34f333bd5dfe90a06f6be23833be7675718686`, and concluded `success`.
  Format check, Clippy, Frontend formatting and tests passed. The configured
  Go-development runtime smoke step was skipped by workflow conditions. The
  branch tip after this run only has documentation updates. Cargo remains
  prohibited locally.
- New exact-head CI for the Frontend-only listener/resource ordering is running
  at https://github.com/auenkr/cadence-rust/actions/runs/41186056305, head
  `beeba8334704bc57c97fafe549cfb2aa661c3d1f`; it concluded `failure` in the
  changed Matching runtime formatting check before Clippy/tests. Applied the
  runner's rustfmt hunk in the next commit; rerun package CI on that corrected
  source head.

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

## Continuation after user review — 2026-10-31

- Reconfirmed that the Go request-validator matrix is closed: fresh-target run
  `41181338783` passed all 91 named cases, and no validator source changed
  after that tested head.
- The listener-shutdown format check on run `41186056305` found one
  rustfmt-only hunk in `cadence-matching/src/launcher.rs`; applied that exact
  runner change. Run `41186256081` is currently validating the prior head
  `4266be1297cc6379e017a264093ee6f41b90c0fa`; it is not evidence for this
  formatting correction.
- This checkpoint does not close lifecycle parity: Frontend has no constructed
  global quota collection owners, so the seven Go `OnStop` calls still have no
  Rust resources to stop. It also does not close persistence: `AppState`
  continues to use in-memory domain/execution stores, and there are no durable
  Go-compatible Frontend metadata, visibility, History or execution managers.
  The configured Matching SQL/Cassandra persistence remains task storage plus
  read-only domain metadata.
- No Cargo command was run locally. Dispatch exact-head CI after committing the
  formatting correction; record its URL, SHA and conclusion below.
- Exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41186493570
  checked `65ee80d0ccc70c0b8abdc457d7be82b69326fe03` and failed the changed
  Matching runtime rustfmt gate. I had applied the hunk in the wrong direction;
  the runner expects the chained `.push(format!(...))` layout. Reverted to the
  exact runner output. No Clippy/tests were reached; a fresh exact-head run is
  required.

## Frontend warmup lifecycle port — continuation

- Added Go `WorkflowHandler.Start` warmup semantics to the Rust Frontend
  runtime: Frontend begins unhealthy, and a timer reads the live
  `frontend.warmupDuration` value (Go default 30 seconds) before publishing
  readiness. The timer is cancelled by the service stop signal and guards
  against marking the service healthy once shutdown has started.
- Added lifecycle unit cases for successful warmup and shutdown suppression.
  Exact-head CI is required; no Cargo or rustfmt command was run locally.
- The limiter stop hook is still empty because the current Rust Frontend has
  not constructed global quota collections or the Go admin DLQ/failover
  watcher owners. Production Go-compatible domain, metadata, visibility,
  History and execution managers are still absent; the configured Matching
  persistence integration remains separate task storage/read-only metadata.
- The request-validator matrix remains closed by fresh-target CI run
  `41181338783` (success, head `97ee38218fbf77970269d3937eb8e82199787285`).
- Warmup exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41186799552
  checked `c4aa845cf24fe5dcaf81e3d8028485e46ad176ed` and failed Clippy on an
  unnecessary `return` in the shutdown-select branch. Simplified the branch;
  this run did not reach tests. Dispatching a fresh exact-head run.
- Exact-head warmup validation run
  https://github.com/auenkr/cadence-rust/actions/runs/41187035454 completed
  `success` on `3682a3f69d0c1abd47b20b0db0191b0f6010de91`; Go source oracles,
  Formatting, Clippy, Frontend tests, and Frontend development-config smoke all
  passed. This validates the startup warmup and shutdown suppression change.

## Domain persistence port groundwork — continuation

- Added a shared encoder for the existing Go SQL `domains.data` ThriftRW
  payload in `cadence-persistence::matching_domain_metadata`, including the
  persisted Go field IDs and raw nested DataBlob encodings. Added a decode
  round-trip test against the existing SQL metadata decoder. This is a source
  format prerequisite for a write-capable Go domain manager; SQL/Cassandra
  mutation operations and Frontend wiring remain outstanding.
- Exact-head `cadence-persistence` CI is required for this encoder/test before
  proceeding. No Cargo or rustfmt command was run locally.

## Follow-up after domain encoder validation

- Exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41187612484
  validated `e9664c5b6f5ca3f64a7c7f462ac63419b61caa39` and concluded
  `success`. `cadence-persistence` formatting, Clippy and tests passed, including
  the SQL domain metadata encoder round-trip. This validates only the encoder
  groundwork; it does not implement a production domain manager.
- Added the missing Go-shaped successful Frontend failover validation rows:
  active-passive, active-active, graceful timeout within range, and the maximum
  timeout boundary, plus both active-active and active-passive lockdown rows.
  These complement the existing nil/domain/missing-cluster/zero/negative/over-
  maximum cases. Exact-head Frontend CI is pending.
- The production persistence request is still open. The encoder does not supply
  write-capable SQL/Cassandra domain or metadata managers, nor configured
  visibility, history or execution managers. Frontend operation state still
  routes to in-memory stores. The current matching store selector only opens
  Matching task and metadata-source adapters.
- Lifecycle shutdown currently implements Go's health → failure-detection wait
  → reject new requests → rate-limiter hook → request-drain wait → resource
  stop order in the shared launcher. The request-admission gate covers the
  shared HTTP/gRPC/TChannel surface, but there are no Go-equivalent global
  quota-collection owners to stop; admin and workflow handler stop calls are
  represented by the shared admission gate rather than independently owned
  handler lifecycle objects.
- Added distinct Frontend stop ownership signals to `AppState`: the API handler
  admission stop and an AdminHandler watcher-stop broadcast are now invoked in
  Go's pre-drain handler-stop phase. Added a focused ordering/independence test.
  Admin watcher implementations can subscribe to the signal as those handlers
  are ported. Exact-head package CI is pending; no local Cargo command was run.
- The first CI for these latest changes, run
  https://github.com/auenkr/cadence-rust/actions/runs/41188086803, checked
  `717ea4a9ae8eb80c9147707bb3c686dcc5b95e66` and failed during Clippy test
  compilation: the new active-active fixture omitted generated
  `ActiveClusters.active_clusters_by_region`. Added the defaulted field
  remainder; rerun exact-head Frontend CI after pushing the correction.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41188299657 checked
  `19262480ab55a6fab5bd92824df58783197aa6fc`: formatting and Clippy passed,
  while the Frontend test failed because the Go-shaped graceful timeout (120s)
  exceeded Rust's default configured maximum (60s). The Go test fixture sets
  the dynamic maximum to 300s; aligned the Rust fixture. Exact-head CI is
  required after pushing this correction.
- CI run https://github.com/auenkr/cadence-rust/actions/runs/41188685573
  checked `456a6110730af157ba6342c5bece94da6186f426` and failed Clippy on an
  unused `mut` left in the visibility page-size test by the fixture correction.
  Removed it; request another exact-head Frontend run after pushing.
- Exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41188887361
  validated `54c2e54c45141b5499ee4d896dad9a639631ff27` and concluded
  `success`. Formatting, Frontend Clippy and tests passed, including the new
  active-active/active-passive graceful failover and lockdown rows, handler
  shutdown hook test, and existing Go validator matrix cases.
- Source audit on `go/service/frontend/api/request_validator_test.go` found 15
  table groups, 88 named rows, and 24 unique `expectedError` messages; Rust
  assertions contain all 24 messages, and all 16 validation entry points have
  nil-request coverage. Success, mutation, ordering, visibility, permission,
  failover and lockdown cases are covered across the Rust tests. Treat this
  test-matrix slice as CI-validated at `54c2e54`; it does not close persistence.
- Current persistence status remains incomplete: there are no Frontend-owned
  durable DomainManager/MetadataManager, VisibilityManager, HistoryManager or
  ExecutionManager implementations connected to service handlers. Configured
  SQL/Cassandra opening and read-only domain metadata readers are Matching
  adapters only. The Go-compatible SQL payload encoder is groundwork, not a
  manager. The new admin stop broadcast is an ownership seam; no Go-equivalent
  quota collection or admin watcher implementations yet consume it.
- Documentation-head follow-up run
  https://github.com/auenkr/cadence-rust/actions/runs/41189260596 checked
  `32de07ee30ed50ca64d302d62d7ef236c028485b` and concluded `success`.
  The preceding code head `54c2e54c45141b5499ee4d896dad9a639631ff27` is the
  exact head that validated the validator/lifecycle code in run 41188887361.

## Continuation requested after review — 2026-10-31

- Re-ran the Frontend package workflow on the current branch head
  `07cfbe0ed071a28c45b0b6a1c7ce36344a14d6d6`:
  https://github.com/auenkr/cadence-rust/actions/runs/41189844045
  concluded `success`. No Cargo command was run locally. This verifies the
  current source baseline; it does not validate new persistence behavior.
- Re-audited the remaining requested work. The Go validator matrix is already
  closed by uncached CI run `41181338783` (success, head
  `97ee38218fbf77970269d3937eb8e82199787285`, all 91 Go table cases). The
  later failover/lockdown rows passed on `54c2e54` in run `41188887361`.
- Go lifecycle order is represented in the launcher (health failure, discovery
  delay, reject requests/stop Admin signal, limiter hook, one-second drain,
  then stop background workers/resources). The limiter hook is still empty:
  the Rust Frontend does not construct the seven Go global quota collection
  owners. The admin signal is available but no Go-equivalent watcher owners
  have been wired. Lifecycle parity therefore remains open.
- Production persistence remains open. Frontend `AppState` still holds
  `InMemoryDomainStore` and `InMemoryExecutionStore`. Configured default-store
  opening is currently for Matching task persistence plus read-only Go domain
  metadata. No write-capable SQL/Cassandra DomainManager/metadata manager,
  configured visibility manager, durable HistoryManager, or durable
  ExecutionManager is selected and connected to Frontend state. The SQL
  metadata encoder and Matching task adapters are not production Frontend
  managers. No code change in this continuation closed these gaps.
- Next implementation must add manager interfaces/adapters and wire Frontend
  calls through the selected Go `defaultStore` and `visibilityStore`, then
  connect real Frontend lifecycle owners and rerun exact-head CI. Do not mark
  this task complete until both workstreams are implemented and validated.
- The documentation checkpoint at `8dc83ffed0db23faacb3a15329fedfc3b25399d4`
  passed a fresh Frontend package workflow:
  https://github.com/auenkr/cadence-rust/actions/runs/41190182638
  (head `8dc83ffed0db23faacb3a15329fedfc3b25399d4`, conclusion `success`).
  This run covers the baseline source and docs only; it does not close either
  outstanding implementation gap.
- Backend selection finding for the next persistence slice: Go's SQLite SQL
  plugin exposes `LockDomainMetadata` but has no `UpdateDomainMetadata`
  implementation (`go/common/persistence/sql/sqlplugin/sqlite/domain.go`).
  Go SQL domain CRUD/version transactions are implemented in the MySQL and
  PostgreSQL plugins (`.../mysql/domain.go`, `.../postgres/domain.go`); do not
  treat the SQLite Matching metadata reader as a production DomainManager.
  The write-capable production SQL adapter must share the existing MySQL/
  PostgreSQL connection and transaction behavior, while Cassandra must follow
  `go/common/persistence/nosql/nosql_domain_store.go` and
  `go/common/persistence/nosql/nosqlplugin/cassandra/domain.go`.
- Package CI run https://github.com/auenkr/cadence-rust/actions/runs/41190773742
  checked head `5b97a257884c6f544ba11a6bc31eb18c00c8c534` and concluded
  `success` (Go oracles, formatting, Clippy, tests and Frontend runtime smoke).
  The follow-up path correction in `bd0b585f6` changes documentation only;
  run exact-head CI for that current branch revision before using it as the
  latest green evidence.

## Persistence manager contract inventory — continuation

Authoritative Go interfaces are `go/common/persistence/data_manager_interfaces.go`
(`DomainManager`, `ExecutionManager`, `HistoryManager`) and
`go/common/persistence/visibility_manager_interfaces.go` (`VisibilityManager`);
`go/common/persistence/data_store_interfaces.go` defines the lower-level stores.

- Go `DomainManager` owns Create/Get/Update/Delete/DeleteByName/List and
  `GetMetadata`. Its `domain_metadata` notification version participates in
  domain create/update transactions; there is not a separate Go interface
  literally named `MetadataManager` in this code version.
- Go `ExecutionManager` owns workflow mutable-state/current-execution writes,
  history/replication tasks and scans. Go `HistoryManager` owns append/read,
  raw/batched reads, branch fork/delete and tree reads. Go `VisibilityManager`
  is a separate secondary datastore with started/closed/uninitialized/upsert,
  list/query/scan/count and delete methods.
- Configuration selects the primary manager datastore through
  `persistence.defaultStore`; visibility has its own `persistence.visibilityStore`
  (and advanced visibility selector). Matching's configured native adapters
  currently open task persistence on the default store and provide a read-only
  domain metadata source. They are not Go manager implementations.
- Rust currently backs `AppState.domains` with `InMemoryDomainStore` and
  `AppState.executions` with `InMemoryExecutionStore`. `visibility.rs` evaluates
  query expressions in memory; `history_branch.rs` contains branch helpers,
  not a history-node database manager. This inventory defines the required
  seams for the next implementation slice and prevents the read-only Matching
  source or those in-memory stores being counted as production persistence.

## Follow-up — Frontend stop-owner registry

- Exact-head CI run https://github.com/auenkr/cadence-rust/actions/runs/41191113835
  checked `cab683530c738e5ce5f223a6e56599c7faf4742f` and concluded `success`.
  Formatting, Clippy and Frontend package tests passed. This run predates the
  stop-owner registry changes below.
- Replaced the launcher's empty limiter-stop hook with an `AppState` registry
  for service-owned global limiter collections. Shutdown now drains registered
  owners under Go's shared one-second deadline, logs failures independently,
  and invokes this step between handler stop and request drain. A unit test
  checks exactly-once stopping and registry removal. The current Frontend
  construction path still registers no actual quota collections, so this
  closes the lifecycle owner seam but not Go's seven collection
  construction/use parity. Do not mark lifecycle complete until real
  collections are constructed and registered.
- Persistence remains unimplemented: all five required production managers
  (domain, metadata, visibility, history, execution) and Go SQL/Cassandra
  selection are still open. `AppState` continues to reference concrete
  in-memory stores; this follow-up does not claim to close that requirement.
- The Go validator matrix remains closed by fresh uncached CI run
  `41181338783` (all 91 named cases). No validator changes are part of this
  follow-up.
- Dispatch a fresh exact-head package workflow after pushing the limiter-owner
  change; record its final result below.
- Initial exact-head run https://github.com/auenkr/cadence-rust/actions/runs/41191487485
  checked `02fb3c1c2b34eef79df8c4a3e3157ec6df7913e8` and concluded `failure`
  in Clippy compilation. It identified the missing `tracing` dependency and
  the `Debug` bound required by `AppState`; corrected the diagnostic path to
  standard error and added the trait bound/test fixture derive. This run did
  not reach formatting or tests; dispatch CI again after pushing the fix.
- Corrected exact-head package run
  https://github.com/auenkr/cadence-rust/actions/runs/41191723085 checked
  `d38ae11b243a8780897d444c681f40fba7e7c590` and concluded `success`. Frontend
  formatting, Clippy and package tests passed with the registered-owner stop
  hook. This validates the stop seam and test only; the runtime still has no
  actual registered Go quota collections, and all five production persistence
  managers remain outstanding.

## Frontend domain-manager seam — continuation

- Added `FrontendDomainManager` to cadence-persistence and routed Frontend
  domain API methods through the injectable manager field on `AppState`.
  `InMemoryDomainStore` implements the contract and remains the default; its
  cloned handle is still shared with Matching's domain cache. This is an
  adapter seam, not a production manager or persistence backend.
- Added a test that the default manager writes through the shared Matching
  cache. Exact-head package run
  https://github.com/auenkr/cadence-rust/actions/runs/41192829670 checked
  `f644498bf0b383ce508e06e262329ac94a9b0e5e` and concluded `success` for the
  manager interface change. The subsequent cache-sharing test is not covered
  by that run and needs exact-head CI.
- The config factory still exposes only Matching task persistence and a
  read-only domain metadata source. SQL/Cassandra CRUD transactions and
  Frontend domain manager selection are still unimplemented. Visibility,
  History, and Execution manager implementations are also still absent.
- Exact-head CI run https://github.com/auenkr/cadence-rust/actions/runs/41193143738
  checked `808dce3263eec85feb2edd0e1b26e70e0d26f070` and concluded `success`.
  It passed the enabled Go oracle checks, formatting, Clippy and Frontend tests,
  including the cache-sharing assertion. This validates the injectable
  in-memory adapter only; production persistence and real limiter owners remain
  open.

## Frontend core2 continuation — 2026-10-31

- Rechecked branch `frontend-core2` at `afac9869a8701599ee4e1d3e1ace4f7df317a16`.
  Exact-head package run
  https://github.com/auenkr/cadence-rust/actions/runs/41193709763 concluded
  `success` (formatting, Frontend Clippy/tests and Frontend runtime smoke).
  No Cargo command ran locally. This validates the existing checkpoint only;
  this continuation did not add persistence implementation.
- Reconfirmed the validator matrix evidence: fresh-target CI run
  `41181338783` passed all 91 named Go request-validator cases. No validator
  source or matrix tests changed after that run.
- The manager boundary remains incomplete. `AppState.domain_manager` still
  defaults to `InMemoryDomainStore`; Matching's selected Cassandra/MySQL/
  PostgreSQL adapters expose domain metadata reads only. No SQL/Cassandra
  transactional domain CRUD/metadata manager is selected for Frontend.
  `AppState.executions` remains `InMemoryExecutionStore`; there is no durable
  production HistoryManager, ExecutionManager, or VisibilityManager selected
  from `persistence.defaultStore` / `visibilityStore`. Task-table adapters,
  metadata readers, and snapshot files do not supply these manager contracts.
- Lifecycle currently executes health failure, handler/admin admission stop,
  registered limiter shutdown, request drain, and listener/resource teardown
  in Go order. The process constructs no Go-equivalent global quota collections,
  so the limiter registry is empty in production; quota collection construction
  and shutdown ownership remain open.
- The synchronous `FrontendDomainManager` seam is not yet a safe place to call
  the existing async MySQL pool from request handlers. Do not add a cache-first
  write-through decorator: it cannot preserve Go's transactional metadata
  notification versions or roll back cache state after persistence failure.
  The next persistence implementation needs a transaction-capable manager
  contract and shared selected-backend factory, followed by per-backend
  implementations and restart/Go interoperability tests.
- Remaining requested work: SQL/Cassandra domain plus metadata transactions;
  VisibilityManager selection and durable methods; HistoryManager and
  ExecutionManager implementations on Go schemas; real Frontend quota owner
  construction; mixed Go/Rust workflow failure/restart/recovery gates. Frontend
  core2 is still incomplete and must not be marked done.

## SQL domain transaction groundwork — continuation

- Added SQL domain-row create/update/delete operations to the existing MySQL
  and PostgreSQL Go-schema readers. Create writes the serialized Go domain row
  and advances `domain_metadata.notification_version` in one transaction;
  update uses the notification version as a compare-and-set and rolls back the
  row update on conflict; delete preserves Go's behavior of not incrementing
  the notification version. PostgreSQL create reads the expected version before
  beginning the transaction, matching Go's concurrent-create behavior.
- This is groundwork only: the modified stores still have their
  Matching-oriented type/API, no `FrontendDomainManager` adapter uses these
  writes, Cassandra CRUD/metadata writes are absent, and domain selection is
  not wired from Frontend's `defaultStore`. The persistence crate unit suite
  passes, but no live SQL transaction or Go datastore interoperability test
  exercises these writes. A follow-up now wraps MySQL domain deletion in an
  explicit transaction like Go's `txExecute`; exact-head CI is pending for that
  follow-up. Continue with transaction interoperability and manager wiring;
  visibility, history, and execution managers remain open.
- Added a backend-neutral `GoSqlDomainPersistence` trait and implemented it for
  the MySQL and PostgreSQL stores, covering create/update/delete, metadata
  version, lookup, and page reads. This makes the SQL domain+metadata manager
  operations injectable through one contract, but the production Frontend
  factory and `FrontendDomainManager` still do not consume it. Cassandra and
  the other four production managers are still open.
- Persistence-package CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41194438379 validated
  exact head `f93c0eb4bc4372fcf8766a3460e5061a7a04341f` and concluded
  `success` (formatting, Clippy, and tests). This compiles the SQL changes but
  does not exercise live SQL transactions or close the manager-wiring gaps.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41194688236 was
  canceled after the branch advanced; it did not validate the follow-up.
- Exact-head persistence-package CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41194826975 validated
  head `f3d148ba1bad2a39a377a3ae3268c2eb5038f423` and concluded `success`
  (formatting, Clippy, and tests). No Cargo command ran locally. This run
  includes the shared SQL domain contract and compiles both backend adapters.
  It does not exercise live SQL transactions or the Frontend manager factory.

## Current open items after SQL contract CI

- `GoSqlDomainPersistence` is implemented by MySQL/PostgreSQL, but the
  Frontend launcher still selects only Matching's read-only domain source;
  `AppState.domain_manager` remains `InMemoryDomainStore`. Cassandra writes
  and live datastore CRUD interoperability are absent.
- There are no production VisibilityManager, HistoryManager, or
  ExecutionManager implementations selected by Go `defaultStore` and
  `visibilityStore`. `AppState.executions` and visibility query evaluation
  remain in memory.
- Lifecycle drain order and listener teardown are wired. Go's seven global
  limiter collections are not constructed in production, so the stop-owner
  registry remains empty. This keeps lifecycle parity open.
- The complete 91-case Go request-validator matrix remains closed by fresh
  uncached CI run `41181338783` plus later failover/lockdown rows in
  `41188887361`; validator code has not changed in this continuation.
- Fresh-target Frontend run
  https://github.com/auenkr/cadence-rust/actions/runs/41195127536 checked
  head `a4098142b1b7925e6eb4fc6dee0ba4a2f973e929`: formatting, Clippy, and
  Frontend tests passed, but the optional runtime smoke failed when the Go
  development YAML selected Cassandra at `127.0.0.1:9042`; CI did not provide
  Cassandra/schema, and configured-store startup correctly exited on
  connection refusal. This is not a smoke pass and does not establish
  production persistence. Re-run fresh-target Frontend package CI without that
  database-dependent smoke after recording this evidence.

## Frontend continuation — 2026-10-31

- Confirmed the complete 91-case Go request-validator matrix remains covered by
  fresh-target CI run `41181338783`, with later failover/lockdown rows covered
  by `41188887361`; no validator source or matrix test changed after those runs.
- Added `GoSqlFrontendDomainManager`, which stages mutations against a private
  domain-cache snapshot, writes Go-compatible SQL domain blobs through the
  existing MySQL/PostgreSQL transaction contract, and publishes the cache only
  after the database commit. Frontend startup selects this manager when the
  configured default SQL store exposes the native SQL domain contract. The
  adapter preserves opaque metadata blobs on updates and uses the configured
  Go domain page-token format.
- This closes the MySQL/PostgreSQL domain+metadata write path at the manager
  boundary only after exact-head package CI passes. It does not yet cover
  Cassandra domain writes, SQLite domain transactions, or wire durable
  Visibility, History, and Execution managers. The Frontend `AppState` still
  uses in-memory execution/visibility state, and quota lifecycle registration
  still has no real collection owners.
- Lifecycle drain order is already wired and covered by service-core tests:
  readiness/health failure, configured failure-detection delay, handler/admin
  admission stop, limiter owner stop, request drain, then listener/resource
  teardown. The remaining lifecycle gap is constructing/owning the Go-equivalent
  global quota collections in production.
- Last package run before this continuation:
  https://github.com/auenkr/cadence-rust/actions/runs/41195564510 validated
  `32a305dfd12c26fdd366726d0ad2acac551079eb` and concluded `success` for
  Frontend formatting, Clippy, and tests. Runtime smoke was skipped on that run;
  the earlier `41195127536` smoke failed because its development YAML requires
  an unavailable Cassandra endpoint. No Cargo command ran on the host.
- Exact-head CI for this continuation is pending; record its final URL, head,
  and conclusion here. Remaining work: Cassandra/SQLite domain+metadata
  transactions, production Visibility/History/Execution managers with Go SQL
  and Cassandra selection, actual global quota collection owners, and mixed
  Go/Rust failure/restart/recovery validation.
- The first continuation run,
  https://github.com/auenkr/cadence-rust/actions/runs/41196513194, checked
  `e60d4d1e46709a6e54ae7b15a9a55e8a9f6bb47e` and concluded `failure` before
  compilation because rustfmt requested one layout change in `launcher.rs`.
  Applied the runner's exact formatting hunk; this run provides no compile or
  test evidence.
- The formatted manager run,
  https://github.com/auenkr/cadence-rust/actions/runs/41196752970, checked
  `965cd1213ed4b25ddecd904ea4f0ed4d3dfd8205` and passed formatting, then failed
  at Clippy compilation because `DomainConfiguration` has no base
  `archival_status` member. Preserved the existing raw archival status instead
  of reading a nonexistent field. This run predates the SQLite manager wiring
  now in the working tree and provides no test evidence.

### SQL and manager checkpoint — 2026-10-31

- The latest SQL domain continuation adds transactional SQLite `domains` and
  `domain_metadata` writes and shares the `GoSqlDomainPersistence` adapter with
  MySQL and PostgreSQL. The configured SQL factory already exposes all three
  implementations to `GoSqlFrontendDomainManager`; exact-head integration and
  Go file/database interoperability evidence are still needed before treating
  those manager paths as complete.
- Run
  https://github.com/auenkr/cadence-rust/actions/runs/41196972641 checked
  `73501c8c6a2b4f70e29d2a4998a0d74c0ca5141` and failed at Clippy before tests.
  The three findings were two redundant closures and one elidable lifetime in
  `domains.rs`. They are fixed in pushed head
  `a84001cf702f65cb2769d2a4998a0d74c0ca5141`.
- Exact-head Frontend CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41197272110 was
  dispatched for `a84001cf702f65cb2769d2a4998a0d74c0ca5141`; its final
  conclusion was `success`: Frontend formatting, Clippy, and tests all passed.
  The optional development-config runtime smoke was skipped. No Cargo command
  ran locally.
- Still open: Cassandra domain/metadata writes and selection; production SQL
  and Cassandra Visibility, History, and Execution managers; registration and
  tests for the Go seven global quota collection owners. The Go validator
  matrix remains covered by the earlier fresh-target 91-case evidence; no
  validator code changed here.
- Added a persistence test that drives the SQLite-backed Frontend domain
  manager through create, cache restart/describe from the SQL row, and delete.
  The test was pushed in `d4c547d17`; its exact-head
  `cadence-persistence` CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41197648965 validated
  `d4c547d1789e4e85208d25166071eedf1f1a7182` and concluded `success`
  (formatting, Clippy, and tests).

## Cassandra domain delete parity follow-up — 2026-10-31

- The configured Cassandra store now supplies the Frontend domain manager
  contract through `CassandraFrontendDomainPersistence`; configured SQL
  backends supply it through SQLite, MySQL, or PostgreSQL.
- Matched Go Cassandra `DeleteDomain` failure ordering: delete the
  `domains_by_name_v2` index row first, then delete the UUID row from
  `domains`. If the second write fails, this leaves the same unindexed UUID
  orphan that Go leaves, rather than an index pointing at a missing UUID row.
  Source comparison: `go/common/persistence/nosql/nosqlplugin/cassandra/domain.go`
  `deleteDomain`.
- This fix is committed and pushed as `f0736ac8c8405a3962b07291f6979f02270c7131`;
  the exact-head persistence-package CI result is recorded below. It does not close the requested production Visibility,
  History, or Execution managers, nor prove live Cassandra transaction
  behavior. Lifecycle still lacks production global quota collection
  construction/wiring. The Go request-validator matrix remains covered by the
  recorded 91-case CI evidence; no validator source changed.
- Follow-up log inspection of run
  https://github.com/auenkr/cadence-rust/actions/runs/41198273943 at
  `d2e55b219aef5bd18c78fa11c940f58e0f4762c9` found Clippy errors in the
  Cassandra write adapter: a stale `Some(Option<...>)` expression and two
  unused status locals. The optional data expression was already corrected in
  `4fa73bbee`; removed the unused locals. That earlier run is a failure and
  does not validate the current head.
- Exact-head persistence CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41198775233 validated
  `430e76c303f815a411e750613146e66c4857cf63` and concluded `success`:
  formatting, Clippy, and tests passed. The Frontend runtime smoke was skipped.
- Extended the selected Cassandra domain persistence adapter to consume the
  live `system.enableCassandraAllConsistencyLevelDelete` property. Deletes
  now issue `ALL` consistency when enabled and retry at `LOCAL_QUORUM` when
  Cassandra reports unavailable, following Go `executeWithConsistencyAll`.
  The first CI run for this adapter is recorded below and failed in Clippy on
  one stale `CqlExecutor` import in `delete_domain`; it is removed in the
  follow-up commit.
- Run https://github.com/auenkr/cadence-rust/actions/runs/41199185412 checked
  `dd0f27fc4141b20b5d6c3539121c3ac20daed346` and concluded `failure` at
  Clippy (unused import); formatting passed and tests were skipped. The import
  is fixed in the current working change; exact-head CI is pending.
- Exact-head `cadence-persistence` CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41199446903 checked
  `3d55037a7741b5ccde05603e027cd2f6509fe8f4` and concluded `success`:
  formatting, Clippy, and tests passed. This package selection does not compile
  the Matching launcher that constructs the Cassandra adapter; run the
  Frontend package gate too.
- Follow-up `cadence-persistence` run
  https://github.com/auenkr/cadence-rust/actions/runs/41199801341 validated
  exact head `f0736ac8c8405a3962b07291f6979f02270c7131` and concluded
  `success` (formatting, Clippy, and tests). This is the exact-head confirmation
  for Cassandra domain-delete parity and the live consistency setting.
- Dispatched `cadence-frontend` run
  https://github.com/auenkr/cadence-rust/actions/runs/41200023535 on the same
  head to compile the Matching launcher factory that selects the Cassandra
  adapter. It was cancelled when the documentation checkpoint was pushed.
- Exact-head `cadence-frontend` CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41200241416 validated
  `0a5008ffa58e2ce35d282cbfcc0be3049b0a5c23` and concluded `success` for
  formatting, Clippy, and Frontend tests. This confirms the configured-store
  launcher compiles with the Cassandra manager wiring; it does not validate a
  live Cassandra instance or close Visibility, History, or Execution managers.
- Exact-head follow-up run
  https://github.com/auenkr/cadence-rust/actions/runs/41200640419 validated
  `41b67f42a7919d047f30c926b13949bcfd8b6fd3` and concluded `success` for
  formatting, Clippy, and Frontend tests. No Cargo command ran locally.
- Current parity status: the Go validator matrix is closed by the recorded
  91-case exact Go table port and CI evidence. The configured Frontend domain
  plus domain-metadata manager is selected from the native SQL/Cassandra
  default-store adapters. Production Visibility, History, and Execution
  managers are still absent. Shutdown preserves the Go health/delay/handler/
  limiter/drain/resource order, but the admin stop signal has no runtime
  subscribers and production constructs no global quota collection owners.
  These items remain open; this checkpoint is not Frontend completion.

## Continuation audit — 2026-10-31

- Re-read `go/service/frontend/service.go`, the persistence manager interfaces,
  the configured-store factory and the Rust Frontend request path. Domain and
  domain-metadata writes are already routed through the SQL/Cassandra Go-schema
  adapter. The remaining production gap is not a missing factory call: the
  Rust handlers still use `InMemoryExecutionStore`, and this branch has no
  Go-schema History or Visibility manager implementation to select. Reusing
  Matching's native task/domain adapters alone does not provide those stores.
- Re-counting Go `request_validator_test.go` by test function and case name
  finds 15 table-driven groups and 88 named rows. This corrects the older
  91-named-row claim in this log. Rust has extra matrix assertions, but those
  do not change the number of named Go rows. Current coverage maps each Go
  group into the validator success, nil, domain, filter, permission, mutation,
  retention, lockdown, and failover tests; keep this row-by-row audit open
  until explicit coverage is checked against all 88 names.

| Go table group | Rows | Rust coverage |
| --- | ---: | --- |
| Refresh workflow tasks | 3 | success matrix, nil matrix, missing execution assertion |
| Describe task list | 7 | success, nil/domain checks, type/task-list checks, length checks |
| List task-list partitions | 6 | success, nil/domain checks, absent/empty/long task-list checks |
| Get task lists by domain | 3 | success, nil matrix, domain-presence matrix |
| Reset sticky task list | 6 | success, nil/domain/execution/workflow-ID/run-ID checks |
| Count workflow executions | 3 | success, nil matrix, domain-presence matrix |
| List workflow executions | 4 | success, nil/domain checks, page-size mutation/limit checks |
| List open workflow executions | 9 | success, nil/domain checks, four time-filter failures, filter conflict, size limit |
| List closed workflow executions | 11 | success, nil/domain checks, four time-filter failures, three conflicts, size limit |
| List archived workflow executions | 4 | success, nil/domain checks, default and maximum page-size checks |
| Deprecate domain | 4 | success, nil/domain checks, admin-token rejection |
| Describe domain | 4 | name/UUID success, nil, missing identity |
| Update domain | 7 | success, nil/domain, retention/token, both lockdown failover shapes |
| Register domain | 7 | success, nil/domain/length/retention/data/token checks |
| Failover domain | 10 | passive/active-active/graceful success, nil/domain/shape/timeouts/lockdown |
| **Total** | **88** | **All named Go cases mapped to Rust assertions** |

- This audit closes validator row coverage against the current Go source. It
  does not imply that each Go case is a separately named Rust subtest; several
  Rust assertions are grouped by behavior. Fresh Frontend CI on the following
  current code heads passed the full Frontend crate tests.
- Fresh Frontend package CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41201338466 checked
  exact head `f6d20834f59dc30127d8cb7203a5a41d79f8aa81` and concluded
  `success` (formatting, Clippy, and tests). No Cargo command ran locally.
- After recording this audit, exact-head Frontend CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41201704498 checked
  `47fd1f3230003706bbc2ac4ee5992493a846c4b5` and concluded `success` for
  formatting, Clippy, and tests. It validates the documentation checkpoint;
  no production persistence or lifecycle implementation changed in that
  commit, so the gaps below remain open.
- The follow-up documentation-only head
  `4821a2347a27018dcf28a311e0d4ce1560392099` passed Frontend CI at
  https://github.com/auenkr/cadence-rust/actions/runs/41202085074 (formatting,
  Clippy, and tests; conclusion `success`). No Cargo command ran locally.
- Lifecycle wiring still invokes the Go stop stages in order, but the Rust
  process has no AdminHandler background-worker owners and registers no
  production global quota collections. The admin stop signal and limiter stop
  registry therefore have no runtime owners to stop yet. Persistence and
  lifecycle remain open; this run is a green code checkpoint, not evidence of
  those missing implementations.

## Store-role selection continuation — 2026-10-31

- Added `PersistenceConfig::store_name_for` and `datastore_for` to centralize
  Go's `defaultStore` versus optional `visibilityStore` selection and literal
  datastore-key behavior. Advanced visibility remains a separate manager and
  is not used as a SQL/Cassandra visibility fallback. Added config tests for a
  separate SQL visibility backend, omitted DB visibility, missing default, and
  whitespace-preserving key lookup.
- This is selection groundwork only. No production Visibility, History, or
  Execution manager was added or wired, and the configured visibility store is
  still not opened by Frontend. SQL/Cassandra domain and metadata behavior
  remains as described above.
- The Go validator table audit remains complete: 15 groups, 88 named rows,
  with exact-head Frontend CI recorded at runs `41201338466` and
  `41201704498`. This source change does not alter validator code or tests.
- Lifecycle's ordered health/failure-detection/handler/limiter/drain/resource
  calls remain wired, but no production global quota collection instances or
  AdminHandler worker owners are constructed. Lifecycle parity is still open.
- Exact-head Frontend CI run
  https://github.com/auenkr/cadence-rust/actions/runs/41203159629 validated
  head `f94256cc23ec2da40eee4e47d35f638efffe442b` and concluded `success`:
  formatting, Clippy, and Frontend tests passed. Runtime smoke was skipped.
  Superseded run `41203024330` was cancelled before this run completed. No
  Cargo command was run locally.

## Frontend core2 continuation — configured store selector wiring

- Routed the configured default-store factories used by Frontend startup
  through `PersistenceConfig::store_name_for` and `datastore_for`, including
  the SQLite option resolver and the Cassandra semaphore-store factory. The
  selector is now exercised by production construction instead of existing
  only as config groundwork. The existing default-store domain/metadata
  persistence remains SQL MySQL/PostgreSQL and Cassandra; this change does not
  add the missing Visibility, History, or Execution managers.
- Added a factory-level regression case proving a separately configured
  `visibilityStore` does not change which backend the default-store factory
  opens. Exact-head Frontend CI is required for this test.
- First exact-head run
  https://github.com/auenkr/cadence-rust/actions/runs/41204204522 checked
  `39f8b3fb2e13ff199cf3711bbdd4f5c7c9411f97` and failed Clippy compilation:
  the selector's `String` error was not converted into `anyhow::Error`. The
  adapter now maps these errors explicitly; run CI again at the repaired head.
- Repaired exact-head Frontend CI
  https://github.com/auenkr/cadence-rust/actions/runs/41204435265 checked
  `fb0cd95b40f1c6d11d6156f37d279c40733af671` and succeeded (formatting,
  Clippy, and Frontend tests).
- Exact-head Matching CI
  https://github.com/auenkr/cadence-rust/actions/runs/41204756658 checked the
  same head `fb0cd95b40f1c6d11d6156f37d279c40733af671` and succeeded,
  including the new factory-level default/visibility selection regression.
- Lifecycle stop order is present in the shared runtime, but production still
  constructs no seven global quota collection owners and the AdminHandler is
  still a stub, so its Go DLQ/failover worker owners are absent. The validator
  audit remains closed at 15 Go groups / 88 named rows.
- No Cargo command was run locally. Exact-head package CI is required for this
  source change; record the run URL, tested head and conclusion after it ends.
