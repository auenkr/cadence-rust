# Native Matching acceptance ledger — 2026-10-29

Matching remains incomplete. This is the current acceptance index; earlier v7
and older runtime results are historical. No native mixed lane has passed.
Implementation and CI diagnostics: `matching-native-integration-20261029.md`.
Dispatch commands and exact scenario/artifact requirements:
`matching-acceptance-evidence-readiness-20261027.md` (ten lanes plus supplemental
oracles). No History implementation work until every Matching requirement is
proved.

## Source identity

No final immutable candidate is frozen. Latest selected Matching run
https://github.com/auenkr/cadence-rust/actions/runs/40754945697 targets
`fb049c5515d0c940adc86e038bd3b1ef31b84880` and reached Clippy. It includes
deadline admission, cold-domain publication, typed cache errors, exact UUID
cache keys, live History start timeout, raw isolation-match metrics, native
diagnostic hooks, native failover callback and refresh RPC failure regressions.
Later query/Thrift failure tests are not in that pin. Do not attribute an
earlier result to changed source.

## Required evidence

| Requirement | Current native evidence |
| --- | --- |
| Fresh uncached workspace Clippy, then workspace tests | Pending. Run40753130527 failed at Clippy on missing API trait import (fixed afterward); selected Matching run40754945697 in progress. Earlier run40750262783 Clippy0 but Matching tests940 pass/8 fail/2 ignored, then cancelled. Fixes are not a test pass. |
| PostgreSQL shared Go schema + SDK + domain/GC/standby/partitions/restart/recovery | Not dispatched |
| MySQL probes, shared Go schema + SDK + forwarding/standby/partitions/restart/recovery | Not dispatched |
| Thrift Matching / TChannel History mixed SDK + restart/recovery | Not dispatched |
| Configstore live settings + SDK + restart/recovery | Not dispatched |
| Real shard-distributor / etcd, executor routing and TTL/cleanup + SDK + restart/recovery | Not dispatched |
| Two Rust MySQL nodes, dual Prometheus and cold-domain quota + SDK + drain/crash/idle recovery | Not dispatched; use complete explicit extra_args from dispatch guide |
| Cassandra shared Go schema + task/semaphore interop + SDK + restart/recovery | Not dispatched |
| Two-node adaptive scaling, unchanged full SDK assertions + restart/recovery | Not dispatched; diagnostic single-node and all-Go control cannot substitute |
| Active offer by Go History, asserted persistence bypass + SDK + restart/recovery | Not dispatched |
| Production TLS, live verified listener identity + SDK + restart/recovery | Not dispatched |
| Supplemental actual Go History peer and PostgreSQL TLS dependency oracles | Not dispatched |
| Final source parity audit and ignored-test exclusions reconciled for native runtime | Open; combine historical source audits with native adapters and actual test results |

All lanes must use one final immutable SHA. Verify Actions headSha and retained
commit.txt match; record binary SHA256, fresh build exit, gate/nested exits,
Go Frontend/History/Worker plus Rust-only Matching, actual SDK and lane-specific
results, drain/restart/crash/idle recovery assertions and resource cleanup.
Download small retained artifacts promptly (three-day retention). Review actual
records, not just workflow conclusions or command-line flags. Run all Cargo/Go
validation on standard ubuntu-latest Actions runners in the public repository;
no local validation and no concurrent mixed lanes.

## Bounded source audit checkpoints

- Native service contains all13 Matching gRPC entry points. Native Thrift
  dispatch iterates every pinned MatchingService method in its coverage test.
  These are source/test inventory facts, not a claim that all paths passed.
- Configured launcher assembles byte task/semaphore stores and native host;
  registry owns live routing, executor admission, fatal shutdown and scaler.
  Native deadline tests cover accepted detached startup versus caller expiry.
- Native HTTP Thrift/TChannel real-TCP fixture and native raw-status gRPC
  fixtures cover distinct malformed identifier bytes and deadline/drop seams.
  Mixed SDK gates still must exercise the configured real engine.
- Shared metadata cache preserves Go exact-key lookup. Its original-Go UUID
  oracle passed at b5a989545; raw metric equality happens before label rendering.
- Existing legacy audits (`matching-current-config-reconciliation-20261027.md`,
  `matching-current-ignore-reconciliation-20261027.md`,
  `matching-v7-final-acceptance-ledger-20261027.md`) guide review, but claims
  about grpc_matching.rs alone cannot prove the native path. Newly restored
  native timeout/cache/logging adapters illustrate why this distinction matters.
- Native delivery/add/semaphore info logs retain the current UTF8 domain-filter
  sink boundary and Go-style diagnostic escaping. Non-UTF8 domain IDs cannot
  match this metadata logging API; domain identity/routing never uses rendered
  diagnostics. Review against reachable Go domain-cache behavior when closing
  the audit; do not silently claim unrestricted domain logging equivalence.

## Continuation source review

Run40753130527 advanced to Clippy after its Go prerequisites; no terminal
workspace result yet. Its original-Go UUID artifact records exit0 and source
`54267900c3c9fee3b4fa0b421be6a38bd8d74886`. The runner's two-file formatting
patch has been downloaded and applied. Full downloads are in
`/mnt/storage/service/ci-evidence/matching-native-{format,uuid}-40753130527/`.

The historical ignored-test reconciliation is not sufficient for the native
path. In particular:

- `registered_failover_callbacks_preserve_catchup_and_release_only_eligible_updates`
  exercises the actual registered callback but supplies only the legacy queue
  registry. Added
  `registered_failover_callback_reaches_native_registry_attached_after_catchup`
  to the same module. It starts the production cache callback before attaching
  the native registry to its shared slot, uses an invalid-UTF8 queue name,
  checks catch-up/local/active-active/stale notifications leave a real poll
  blocked, then checks an eligible notification releases it without replacing
  its owner. This new regression is **unvalidated** and requires the next CI
  run, including runner formatting. It is not included in run40753130527.
- Native partition cache tests assert refresh failure categories and unchanged
  cached data, but do not by themselves prove propagation through the public
  native refresh RPC. Keep that direct boundary coverage gap open.
- Native query engine tests cover raw worker results and caller cancellation,
  but do not inject the Go manager dispatch-error case. Keep that specific
  failure assertion open rather than equating it with a deadline/no-poller case.
- `native_transport_compat.rs` is a real TCP HTTP/TChannel QueryWorkflow
  error/deadline fixture. Its stub implements all methods, but its assertions
  do not cover the all-method injected-handler-error/default-response matrix.
  The native Thrift dispatch method inventory is a separate coverage fact.

No local Cargo/Go validation ran. No replacement agents were started. T3 PR
linking still fails with the active-thread-run ownership error; GitHub CLI is
available. Preserve the current run until terminal and review its actual logs
before dispatching the changed source.

Run40753130527 is now terminal **failure**: Clippy exit101, two E0599 errors
from the new poll-deadline logging test missing the generated native API trait
import. Tests never ran. Moved that trait import into the test module scope,
removing the narrower imports. Exact source/target, failure diagnostics and
passing UUID oracle records are retained in
`memory/evidence/matching-native-workspace-40753130527/`; full CI artifacts
remain in `/mnt/storage/service/ci-evidence/matching-native-workspace-40753130527/`.

Added a native public-handler refresh regression using the existing service
fixture: lease the queue, seed a raw-byte partition cache, corrupt its real
SQLite blob after startup, invoke RefreshTaskListPartitionConfig with a live
deadline, restore the blob, then assert exact storage error message/Internal
status, empty typed details, unchanged cache and identical running owner.
This closes a specific missing assertion in source; it is **not a test pass**
until CI executes it. It covers a nonretryable decode failure, not the complete
throttled/timeout taxonomy or cancellation behavior. Next run should validate
Matching first with CI formatting, then the final immutable source still needs
fresh whole-workspace validation and all mixed lanes.

Fresh selected Matching Clippy-then-tests run:
https://github.com/auenkr/cadence-rust/actions/runs/40754945697
at `fb049c551` (verify the full Actions head SHA on review), with clean target,
runner parity formatting and original-Go UUID oracle enabled. Includes the
native registered failover regression and refresh public-handler regression.
No pass is claimed. Preserve this run until terminal; next download formatting
and logs, review failures, then resolve any source/formatting before freezing.

Additional source coverage while run40754945697 remains live (not included in
its pin):

- Native QueryWorkflow handler now has a regression injecting `some error`
  through an actual child-partition dispatch's `NativeQueryParent`. It asserts
  exact public Internal message, parent/raw forwarding fields, one forwarding
  call, returned forwarding permit, and no abandoned local query offer. Go
  `tasklist/matcher.go::OfferQuery` returns a forwarding failure to the handler;
  this exercises that concrete dispatch failure rather than pretending a
  no-poller timeout proves Go's mocked `DispatchQueryTask` failure case.
- Native Thrift dispatcher regression injects InternalServiceError("test")
  through every pinned MatchingService method with a present request. It
  asserts declared error summary, field2 exception/message and absence of
  success field0 for both struct and void methods. Go source is
  `go/service/matching/wrappers/thrift/thrift_handler_test.go::TestThriftHandler`.
  Go's in-process response-plus-error tuple is not a Rust API; its zero response
  is discarded by the Thrift error reply. This test covers that observable
  wire result, not Go object identity or Meta Health.
- Applied run40754945697's formatting artifact to the native failover fixture,
  native TCP transport fixture and refresh RPC test. New query/Thrift test code
  still needs runner formatting on the next validation run.

These are assertion additions pending execution. Exhaustive typed-error
category mappings, Meta Health and the ten mixed acceptance lanes still need
their own reviewed evidence. Preserve the current CI run until terminal.

Supplemental oracle coverage correction: the live native PostgreSQL store
test was ignored and no workflow invoked it. Added it to
`tests/gates/run-matching-dependency-oracles.sh`, after the TLS cases, using
the existing persistence test binary and the same disposable database. The
script seeds the exact original-Go PostgreSQL schema, retains its SHA256 and
seed exit/log, runs the exact module path
`matching_postgres::native::tests::live_native_store_raw_bytes_fencing_and_legacy_cross_read`,
and requires exactly one executed passing test. It retains test and aggregate
oracle exits; failures affect the gate result. The test covers invalid-byte
rejection, valid U+FFFD key separation, range fencing and native/legacy
cross-read. This new gate step is unexecuted; it does not substitute for the
mixed PostgreSQL SDK/recovery lane. Local checks: `bash -n` and
`git diff --check` passed; no local Cargo or Go command ran.
