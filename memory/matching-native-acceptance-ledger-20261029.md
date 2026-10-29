# Native Matching acceptance ledger — 2026-10-29

Matching remains incomplete. This is the current acceptance index; earlier v7
and older runtime results are historical. No native mixed lane has passed.
Implementation and CI diagnostics: `matching-native-integration-20261029.md`.
Dispatch commands and exact scenario/artifact requirements:
`matching-acceptance-evidence-readiness-20261027.md` (ten lanes plus supplemental
oracles). No History implementation work until every Matching requirement is
proved.

## Source identity

No final immutable candidate is frozen. Latest fresh whole-workspace run:
https://github.com/auenkr/cadence-rust/actions/runs/40759246848 at
`c6c8ef1bb49effb2c6b689ad31b23a9e33bf7227` (**success**, reviewed from downloaded artifacts).
Includes quota correction, typed metrics and duration fixes, runner formatting
from run40757715608, and the new all-declared-native-Thrift-error matrix.
Fresh target, sequential Clippy/workspace tests, runner parity formatting and
original-Go UUID oracle enabled. Inspect any formatting artifact before freeze.

Run40757715608 at `a85e585b9a2e396e247459750398dcf09b612cc5` concluded
failure: Clippy passed, Matching 955 passed/2 ignored including live duration
provider, typed metric classifier passed, then service-core 172 passed/one
failed at the already corrected raw-byte quota regression. Compact evidence:
`evidence/matching-native-workspace-40757715608/`.
Run40758347908 at `17523bb3b141e9c5ffea1f23d5ca6c186d19184d` was cancelled
when the changed source superseded it; it passed Clippy but has no completed
test result. Compact partial evidence:
`evidence/matching-native-workspace-40758347908/`.
Earlier workspace run40756335405 failed the same quota regression. Selected
Matching run40754945697 passed at
`fb049c5515d0c940adc86e038bd3b1ef31b84880`; it is not final source evidence.
No native mixed lane passed. Final gates require one frozen immutable source
and same-pin successful whole-workspace validation.

## Required evidence

| Requirement | Current native evidence |
| --- | --- |
| Fresh uncached workspace Clippy, then workspace tests | Run40759246848 passed at c6c8ef1bb (fresh target, Clippy0/test0); subsequent metric corrections need new CI; runs40757715608 and40756335405 failed the raw-byte domain quota regression; run40758347908 was superseded. Selected Matching run40754945697 passed Clippy and enabled tests at fb049c551; this does not establish a whole-workspace pass. |
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

## Selected Matching pass recovered

Run https://github.com/auenkr/cadence-rust/actions/runs/40754945697 completed **success** at `fb049c5515d0c940adc86e038bd3b1ef31b84880`. Downloaded logs prove fresh target `/home/runner/work/_temp/cargo-target-final`, selected cadence-matching Clippy exit0 then tests exit0, including native failover and public refresh failure regressions. Compact evidence: `memory/evidence/matching-native-selected-40754945697/`; complete logs: `/mnt/storage/service/ci-evidence/matching-native-workspace-40754945697/`. This was selected-crate CI, despite the historical local directory name. Advisory whole-workspace formatting exit1 remains reported; changed Matching runtime format exit0. Latest query/Thrift regressions and PostgreSQL oracle invocation postdate this source. No workspace or mixed lane pass is claimed.

## Native error-classification audit correction

Source review of Go `service/matching/handler/context.go::handleErr` and
`context_test.go::TestHandleErrKnowErrors` found a genuine shared metric bug:
Rust searched every encoded status byte for error-name substrings. A typed
QueryFailed error whose message/detail mentioned InternalServiceError selected
the generic failures counter. Native errors with invalid UTF8 outer messages
also omit public details to match grpc-go, so the same classifier lost their
original type even though the private in-process envelope retained it.

Changed the classifier to decode only google.rpc.Status Any fields, compare the
actual first type name exactly, and read the native private envelope before
wire details. It skips the outer message so invalid bytes do not prevent typed
classification. The existing tonic-code fallback remains. The marker constant
is now shared by native status assembly and metrics to avoid drift. Added a
fourteen-category regression (normal and private/native envelopes, misleading
message/payload text, invalid UTF8 outer bytes), malformed-envelope and unknown
suffix cases. The earlier inbound query fixture used a bare ASCII URL as
status details, which is not a google.rpc.Status; it now supplies a real encoded
envelope. These new changes require fresh CI; no test pass is claimed.

Applied the two-file runner formatting patch from run40756335405 for the native
query and all-method Thrift wrapper regressions. That run is still live and
must finish; it predates the metric correction. The final source is not frozen.
No local Cargo, rustfmt or Go command ran; `git diff --check` passed.

Additional configuration audit found and corrected the native/legacy async
reader timeout's wrong integer parser. Go defines a duration property; both
Rust builders now parse duration strings. A direct production-provider test
checks live values, zero, bad-value fallback and byte-exact task filters.
Details: `matching-native-integration-20261029.md` (Async reader duration
correction). This code and the error-metric correction postdate the live
workspace run40756335405 and need fresh validation/formatting. No final pin.

## Native error boundary reconciliation

See `matching-native-error-boundary-audit-20261029.md` for exact observed
native query, refresh, callback and Thrift wrapper passes recovered from
run40756335405. That workspace run still failed later in service-core. Added
an unvalidated native matrix for all ten distinct declared Thrift exception
types across every pinned Matching method (valid/invalid outer messages,
raw payloads and exact exception field IDs). This postdates run40758347908.
Applied run40757715608 formatting patch for metrics/duration provider.
Final immutable candidate remains open; retain current terminal CI evidence
before dispatching the final formatted source.

Continuation: run40759246848 passed; compact evidence retained. New metric rendering and native AddTask counter corrections postdate that source and remain unvalidated. See latest integration checkpoint; final candidate still open. User requested save/push and master sync after validation.


Latest checkpoint: saved native metric corrections are merged into master.
Run40802403212 at7c4e1ee9d failed one new fixture (missing Tokio reactor),
after Clippy0 and Matching957 passed. The fixture attribute is corrected on
master, pending fresh workspace CI. No mixed lane is credited. See
matching-master-sync-20261029.md and matching-native-source-domain-audit-20261029.md.

## Current source-domain continuation

Whole-workspace run40803510954 passed at4d1007f3bdd0a6d667936d53aef6cf6277d98f51:
fresh target, Clippy0/test0 and all four live metric emission comparisons.
Reviewed compact evidence is retained in evidence/matching-native-workspace-40803510954/.
This predates subsequent source-domain corrections and is not a final pin.

Selected run40804502679 atb72844748 failed Clippy on the duplicate/private
tests module. Tests did not run; retained diagnostics are in
evidence/matching-native-selected-40804502679/. The module naming fix is
d9ac938ba. Raw source error correction and successful History dispatch test
are merged into master, including06f4094a8 andeb8b2def8.

Fresh selected validation (clean target, runner formatting, Go UUID oracle):
https://github.com/auenkr/cadence-rust/actions/runs/40804994259
at e03c37b33; conclusion pending. It includes the module fix and merged tests.
The SQL source-parse versus persistence-limiter order remains an identified
parity gap under correction; no source-domain completion is claimed yet.
Separate original-Go source-boundary oracle work is in progress.
All ten native mixed lanes and final same-pin workspace validation remain open.

## Continuation checkpoint — 2026-10-29, 07:55 IST

Integrated Cassandra raw-domain correction `c9d5b8d57` on master. Selected CI [40804994259](https://github.com/auenkr/cadence-rust/actions/runs/40804994259) failed Clippy at source `e03c37b33fa7836d489cd43f82cb9c3e141ba4ae`: sync History regression held a standard MutexGuard across shutdown await; tests did not run. Full failed log retained at `/mnt/storage/service/ci-evidence/matching-native-selected-40804994259/clippy-failed.log`. Corrected guard lexical scope. Runner formatting artifact is retained but cannot apply cleanly to newer source; next dispatch will regenerate it.

Addressed static review findings: raw-domain request counter precedes limiter/source; timing includes limiter; normal completion emits latency/errors without a duplicate request; captured panic leaves request only. Rejected raw lookup records busy/failure and latency after limiter returns. Extended regression checks for counts. Workflow now requires explicit PASS records for manager, SQL, and Cassandra marshal boundary tests. Added the touched persistence/UUID files to CI formatting artifact. All current changes await Actions; no local Cargo, rustfmt, Go or runtime validation. Ten native mixed lanes remain unproven. Previous goal turn produced source and review evidence (progress).

## Integrated follow-up regressions and Cassandra review — 2026-10-29

Merged blocked-source metrics regression `3ff54419003d23bf3052b622858a3cd0a2d482b4` and semaphore concurrent startup/cancellation/failure tests `bac884645463bc50bef5777c2ec0a465ea5a7391`. Corrected the metrics fixture expectation: generic transient source error is Internal, not DBUnavailable. These tests remain unexecuted.

Independent Cassandra review found original accepted ID spelling was lost in missing-domain errors after query canonicalization. Adapter now preserves caller spelling when mapping NotFound while retaining canonical UUID query binds. Strengthened the original gocql oracle to compare actual decoded 16-byte UUID, rather than only byte count. Pure error-mapping coverage alone would not demonstrate Cassandra database execution; the native Cassandra gate remains mandatory.

The raw-source timing review applies equally to the other domain metadata operations, so they now share the same pre-limiter request/timer and post-return result metering helper. This avoids duplicate requests and ensures rejected domain lookups retain Go's overall metric family. No local validation ran.

Actions [40806302891](https://github.com/auenkr/cadence-rust/actions/runs/40806302891), source `7828475bc224f507cbb3769a125638c0edc759d5`, is still live at Clippy as last inspected. Its downloaded source-domain-boundary artifact has exit0 and explicit PASS records for manager, SQL and Cassandra marshal oracles; this pin predates the strengthened byte assertion and latest source/tests. Artifact retained under `/mnt/storage/service/ci-evidence/matching-native-selected-40806302891/source-domain-boundary-oracle`. No final workspace or mixed-lane credit claimed.

## Selected CI terminal result — 40806302891

Run failed Clippy with E0061/E0308: Cassandra raw override called the inherent persistence `get_domain` instead of the source trait method. Corrected with explicit `<Self as DomainMetadataSource>::get_domain`; latest adapter still preserves original caller spelling. Tests were skipped, so no runtime/regression pass credited. Compact evidence is in `memory/evidence/matching-native-selected-40806302891/run.txt`. All three finished worker branches and clean worktrees (raw-domain-error-parity, test-raw-lookup-metrics, matching-semaphore-startup-tests) were verified merged and removed locally/remotely. Previous continuation made source progress; no blocked condition exists.
