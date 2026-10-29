# Native Matching acceptance ledger — 2026-10-29

## Current resume pointer — 2026-10-29

Latest continuation save: `fa82a5b417d793d7e05767b7d5496b1d58150827` was the
clean shared-master checkpoint before a docs-only update. The callback and
ownership details now appear at the top of
`memory/matching-resume-checkpoint-20261029-ci40853270019.md`. In brief:
timeout integration task `34075d70-1b1f-422c-84e2-7cf99a640793`, lifecycle
domain-binding regression task `f5245394-bb12-4e8a-b201-3f3df435dc10`, and CI
evidence scheduler task `3ffb29bd-727d-433e-b088-36ffbab15c89` were last
dispatched and are awaiting callbacks. Their status is not confirmed live or
complete. Run `40853270019` must remain at its last known observation (head
`592a0bb03660479c890680d316b58067f3527810`, Clippy/format passed, tests were
running); do not infer a newer result. Reviewed branch `90aa74112e1c9e4d4c9814f5e1d94a7f6b81176b`
and other post-baseline source is unverified until exact-head CI. No local
validation, Actions inspection/dispatch, or child-status polling occurred for
this checkpoint.

Latest branch-by-branch status and ownership is recorded in
`memory/matching-resume-checkpoint-20261029-ci40853270019.md`. Remote refs
verified: raw-byte/deadline `90aa74112e1c9e4d4c9814f5e1d94a7f6b81176b`,
lifecycle follow-up `ccb6cb11fd72229499d3eeadb55a0cc96647475e`, and timeout
parity `18040515dabb2d6fd79ed5b4eba5c933eee25f1f`. None is integrated or
validated as a combined candidate. Semaphore run 40853270019's last known exact
head remains `592a0bb03660479c890680d316b58067f3527810`; Clippy/format were
successful while tests remained in progress. Do not infer a newer result.
Focused baseline 40842468415 passed at `721e196d31b2cd7876fc09086c8ac4b2a8995ba3`
but predates these changes. Native mixed passes remain zero; frozen candidate
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` remains unchanged.

## Focused semaphore review checkpoint — 2026-10-29

Focused baseline [40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415)
is successful at `721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; scoped formatting
exit 0 and advisory workspace formatting exit 1. This is focused CI, not a
mixed lane pass. The semaphore-reviewed source
`592a0bb03660479c890680d316b58067f3527810` and docs change `283` are pending.

One authoritative observation of [run 40853270019](https://github.com/auenkr/cadence-rust/actions/runs/40853270019)
found exact branch `matching-semaphore-reviewed-592a0bb03`, exact head
`592a0bb03660479c890680d316b58067f3527810`, status `in_progress`; Clippy was
running and formatting/tests were pending. Oracle and scoped-format steps listed
in `matching-native-checkpoint-20261029.md` had succeeded, including upload of
the Matching parity formatting patch. No terminal logs, exits, or artifacts
were available at that time, and the run was not polled or redispatched. Do not
credit semaphore tests until the actual test log/exits are available. Relevant
source test names are recorded in the checkpoint.

Quota scenario integration `530953ac7`, raw-byte work `31ff3eb28`, and reader
work `21a051d36` remain unverified while Terra integrates them. Configured
provider support and JSON unpaired-surrogate handling remain raw-byte caveats
under review; extreme-value caveats remain for reader work. A separate Terra
audit is reviewing the three ack/delete fields. No native mixed lane has passed;
all ten remain required. Frozen `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` is
unchanged.

> Latest state: see `matching-native-checkpoint-20261029.md` and
> `matching-candidate-source-closure-review-20261029.md`. Scheduler-owned run
> `40841353614` was last known live with Test in progress at exact source
> `e84e51a1c5a08cc89df0a04ccf69a77e284bd1c4`; no terminal result is available.
> Formatted source commit `721e196d31b2cd7876fc09086c8ac4b2a8995ba3` has no CI
> credit. No native mixed lane has passed. Frozen ref remains
> `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`.

Matching remains incomplete. This is the current acceptance index; earlier v7
and older runtime results are historical. No native mixed lane has passed.
Implementation and CI diagnostics: `matching-native-integration-20261029.md`.
Dispatch commands and exact scenario/artifact requirements:
`matching-acceptance-evidence-readiness-20261027.md` (ten lanes plus supplemental
oracles). No History implementation work until every Matching requirement is
proved.

## Terminal review of startup-integration candidate run 40840392628

The successful selected-package Actions run
[40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628)
was independently reviewed after completion. Exact expected source head
`d4ee53f00d505246dc0b7fbbe353d7e989a91c49` matches the artifact identity.
Clippy/test exits are 0/0; scoped changed Matching runtime, query, TLS, and
selected formatting exits are all 0. Advisory workspace-wide formatting exit
is 1 for unrelated `rust/crates/cadence-api/src/admin.rs:371`; workflow captures
that result and succeeds. Package test evidence is 973 passed / 2 ignored in
the library harness; 27 retained summaries total 1,083 pass results, no failures,
16 ignored including filtered/repeated invocations. The job restored a Rust
cache and selected `cadence-matching`; it is not fresh uncached workspace CI.

The log contains existing cold-domain/runtime startup regressions (cold-domain
metric handlers, gRPC listener startup/Meta Health, native startup gate and
Thrift startup profiling). The exact-head commit changed no Rust test files.
Do not credit later semaphore startup-timeout, expanded adapter/lock, or two-
thread regressions, nor candidates `25e`, `90cd`, `e84e`, or `8cc`. This result
is neither mixed runtime evidence nor validation of the frozen candidate's
failed PostgreSQL attempt. Compact files are retained at
`memory/evidence/matching-startup-candidate-ci-40840392628/`.

## Source identity

Current frozen candidate: `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`,
remote ref `matching-native-final-20261029` (verified with `git ls-remote`).
Whole-workspace Actions [40807471166](https://github.com/auenkr/cadence-rust/actions/runs/40807471166)
passed at that exact SHA with fresh target, sequential Clippy then workspace
tests, original-Go domain/source oracles enabled, and no formatting mutation.
`clippy.exit=0` and `test.exit=0`; fresh target was
`/home/runner/work/_temp/cargo-target-final`. `fmt.exit=0` and scoped Matching
runtime, TLS and query format checks were 0. The advisory full-workspace format
check recorded `workspace-format.exit=1` for formatting diffs elsewhere; the
workflow intentionally continued, and no formatter mutation occurred. Matching
reports 972 passed and 2 ignored. Four metric comparisons match. Run identity,
actual exit files and compact test summary are retained in
`evidence/matching-native-workspace-40807471166/`; complete downloads are in
`/mnt/storage/service/ci-evidence/matching-native-workspace-40807471166/`. This
credits exact-pin workspace CI only, not mixed-lane acceptance. The selected
predecessor runs failed on compile/test lint issues, now corrected; their compact
evidence is retained below.

Earlier completed whole-workspace pass (historical):
[40803510954](https://github.com/auenkr/cadence-rust/actions/runs/40803510954)
at `4d1007f3bdd0a6d667936d53aef6cf6277d98f51`. It predates source-domain changes
and does not validate this candidate. Earlier results and diagnostics are retained
in the continuation checkpoints and `memory/evidence/`.

PostgreSQL was attempted and failed before SDK execution; the other nine native
mixed lanes remain undispatched. Exact failure evidence and limits are recorded
below and in `memory/matching-native-resume-state-20261029.md`. Final gates must
retain this same candidate SHA (or a coherently revalidated replacement if
fixes are needed).
Evidence-only commits on master may follow the frozen candidate; they do not
change the immutable ref used to build and run the candidate.

## Required evidence

| Requirement | Current native evidence |
| --- | --- |
| Fresh uncached workspace Clippy, then workspace tests | Pass at frozen `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`: run40807471166 success, clean runner target, Clippy0 then workspace tests0. `fmt.exit=0`; scoped format checks0; advisory workspace-wide format check `workspace-format.exit=1` for unrelated formatting diffs. Evidence: `evidence/matching-native-workspace-40807471166/`. |
| PostgreSQL shared Go schema + SDK + domain/GC/standby/partitions/restart/recovery | Run [40808511931](https://github.com/auenkr/cadence-rust/actions/runs/40808511931) failed at frozen SHA `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`. Artifact `/mnt/storage/service/ci-evidence/matching-native-postgres-40808511931/`: binary SHA256 `54771ed458f78f0ea7a5b87678c3f0cd8b9b72e563e378a70bb8d13266d87b57`; build0, gate1, nested run1, lane1. Go Frontend never discovered Rust Matching: probe readiness ended with `Call cancelled` / `Not enough hosts to serve the request`; Rust log confirms gRPC `127.0.0.1:14329` and Thrift/TChannel `127.0.0.1:12973` bound. Probe reports `Rust Matching did not stop` after SIGTERM; shutdown record contains signal/time only. No SDK or recovery scenario ran. Go Frontend/History/Worker were configured and worker readiness was observed; result records no Go Matching. PostgreSQL container id is retained, but the retained cleanup file has only the id and does not prove removal; cleanup must be verified. Runs40808334141 and40808357056 were cancelled and supply no acceptance evidence. |
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

## CI formatting applied — run 40806941617

Downloaded `matching-format-patch` from [40806941617](https://github.com/auenkr/cadence-rust/actions/runs/40806941617), source `fef2b6c3782f8ca0ab95e98d4eb56d66bd09a260`. The exact-source patch passed `git apply --check`, was applied, inspected, and passed `git diff --check`. Ten Matching files are formatted from the runner; no local Cargo or rustfmt ran. The patch is retained at `/mnt/storage/service/ci-evidence/matching-native-format-40806941617/matching-format.patch`. The source run is live at Clippy as last observed. Final workspace run must test the committed formatted source without formatting mutation, then all mixed gates must use that same immutable pin. No final acceptance pass is claimed.

## Workspace candidate preparation

Selected run40806941617 failed solely on `clippy::result_large_err` in the new blocked-source test's thread return. Changed it to return the boolean used by the assertion. No production behavior changed. Tests were skipped, so their outcomes remain unproven. Next dispatch is a whole-workspace fresh-target Clippy-then-test run with original-Go source/domain oracles enabled and no formatter mutation. Candidate validation ref will stay frozen while that run and subsequent serial mixed lanes execute. Acceptance remains pending.

## Frozen candidate oracle evidence and final source reconciliation

Retained all four original-Go oracle artifacts from40807471166 under `memory/evidence/matching-native-workspace-40807471166/go-oracles/`, including content SHA256 inventory. Each artifact source SHA equals frozen `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` and exit0. Source manager, SQL and Cassandra marshal tests have explicit PASS records; Cassandra accepted spellings assert exact decoded UUID bytes. Original cache UUID-spelling, malformed/optional handler payload and raw-string oracles also exited0. This proves those Go assertions only; Rust workspace tests and mixed runtime are separately required.

Integrated bounded final source notes: `matching-entrypoint-source-reconciliation-20261029.md`, `matching-source-cache-persistence-reconciliation-20261029.md`, and `matching-configured-assembly-final-reconciliation-20261029.md`. No new proved production mismatch was found. Notes map named Go behavior to actual native implementation/assertions and explicitly preserve limits: transport error coverage boundaries, configured failure/admission timing, independent semaphore startup timeout, unusual raw-source backend cases, and actual mixed recovery outcomes. These notes do not certify runtime parity. Workspace CI advanced past Clippy to Test at last inspection; terminal result still pending.


## Current handoff reconciliation — 2026-10-29

At review time `master` and `origin/master` both point to
`9a79103bb8cdeb191850452b10b965a02962b3dc`, clean worktree. Latest Actions
include `rust-ci` run [40809425330](https://github.com/auenkr/cadence-rust/actions/runs/40809425330), success at that current head (Clippy/test job). This is newer than the frozen candidate and is not mixed-service evidence; it does not change the pin or complete Matching acceptance.

On current master the native task-list status assertion was added in
`9a79103bb`. Rust CI succeeded at that head, but this review did not confirm its
package/test selection; keep the historical frozen-pin assertion caveat above
distinct from newer-head source state. The configured
semaphore startup fixed-timeout behavior remains unverified in a mixed runtime;
the PostgreSQL readiness failure does not establish a new source defect in it.

## Diagnostic startup instrumentation run — 2026-10-29

The selected fresh-target diagnostic run
[40839466118](https://github.com/auenkr/cadence-rust/actions/runs/40839466118)
was authoritatively observed `in_progress` at `2026-10-29T08:03:46Z`, on
`t3/matching-startup-fix` head
`12c3063ea91e0dc5abd552a79140a14690edb8bd`. This is the diagnostic-only
instrumentation commit; it does not contain a source fix and cannot establish
Matching acceptance. No artifact or terminal conclusion is available yet.
Because it remained live, no additional workspace run was dispatched. When it
is terminal, review its exact logs/exits. If successful, and no intensive CI or
mixed job is running, dispatch whole-workspace `rust-ci.yml` at the same
unchanged head with `clean_target=true` and empty `package`; this prerequisite
must precede PostgreSQL retry. The frozen candidate ref remains at
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` and must not be moved. See
`memory/matching-native-resume-state-20261029.md` for the retained current
checkpoint.

### Terminal result — run 40839466118

The run is terminal `failure`, exact head
`12c3063ea91e0dc5abd552a79140a14690edb8bd`. `Check changed Matching runtime
formatting` failed with exit 1 on a rustfmt diff at `matching_runtime.rs:925`;
workspace formatting exit was also 1. Query and TLS formatting exits were 0.
Clippy and Test were skipped, so there is no Rust build/test conclusion. See
`memory/evidence/matching-startup-fix-40839466118/evidence.md` for the compact
artifact inventory. A clean-target workspace rerun was not dispatched because
the selected run failed; PostgreSQL remains undispatched. Pending commits
`overlay9d3a342d5` (`t3/matching-overlay-lock-order`) and `PG9498ee5e1`
(`t3/fix-postgres-pool-wakeup`) still require separate review and have no test
evidence or proven causal link to the previous startup failure.
