# Frontend Domain, Admin and Schedule port — 2026-10-31

## Branch and current source state

- Work branch: `frontend-domain-admin` (local worktree branch `t3/frontend-domain-admin`).
- Rebased on `origin/frontend-core` at `3b920551e` after it landed.
- Current implementation adds domain validation/archival transition helpers in
  `rust/crates/cadence-frontend/src/handler_domain.rs`, Admin config/search
  attribute state in `handler_admin.rs`, and schedule validation/ID helpers in
  `handler_schedule.rs`.
- Go references: `go/common/domain/handler.go`, `attrValidator.go`,
  `archivalConfigStateMachine.go`, `go/service/frontend/admin/handler.go`, and
  `go/service/frontend/api/handler_schedule.go`.

## Work completed so far

- Domain helpers port Go ASCII domain-name checks, retention and archival URI
  requirements, local/global replication cluster validation, active-active
  cluster name/version checks, cluster-removal rejection, and the archival
  config transition table including immutable URI semantics.
- Admin helpers port the shared dynamic-config normal/operational namespaces,
  filtered get/update/restore/list behavior, and search-attribute reserved-key
  and type-conflict checks.
- Schedule helpers port create-request validation order, feature-gate error,
  scheduler workflow IDs, default list page size and nil-vs-empty backfill
  response behavior. Underlying schedule persistence/engine already exists in
  `cadence-persistence/src/schedules.rs`; Go scheduler workflow interaction is
  still an audit gap.
- These modules have unit cases mirroring Go behavior. No Cargo command has
  been run locally.
- Added Go-compatible `UpdateDomain` classification and guards in
  `handler_domain.rs`: failover is selected by active-cluster presence,
  non-empty active-active attribute scopes, or positive timeout; mixed
  failover/config requests are rejected; local domains reject presence of any
  replication field and any failover timeout, including zero/negative values.
- Completed the previously unterminated domain unit-test module and added
  cases for those request-shape rules. This commit still needs CI verification.
- Schedule `CreateSchedule` now returns Go's distinct missing-action error
  before checking the action's `StartWorkflow` fields. Added an
  `UpdateSchedule` validator for Go's first-error order, empty-update check,
  cron/time-range checks, complete replacement action, policy validation, and
  scheduler-reserved search-attribute keys.
- Added a pure `DescribeCluster` response builder that preserves discovered ring
  and member ordering, reachable-member identities, Go SDK version constraints,
  persistence backend names and the advanced-visibility feature flag. Resolver
  collection/error handling and the RPC adapter still need wiring.

## CI evidence

- Initial domain-only candidate dispatched as run
  [41172959808](https://github.com/auenkr/cadence-rust/actions/runs/41172959808)
  at head `ae651ac85b852f9386b6b84021bfd6b187978bca` before the frontend-core
  rebase and before the Admin/Schedule files were added. Last observed status:
  in progress. This run is stale for current source and does not validate the
  current branch. Dispatch a fresh `cadence-frontend` run after the next push.
- Run [41173158817](https://github.com/auenkr/cadence-rust/actions/runs/41173158817)
  is at head `f3b3611f7da78ab156b6fd14ef7e7026339683c1` and was in progress at
  the last check; it also predates the current domain update guards. It is not
  evidence for the updated source. A fresh run will be dispatched after push.
- Run [41173330575](https://github.com/auenkr/cadence-rust/actions/runs/41173330575)
  targeted `39eaefbd3b9073576df7d650e08ec3190a65ec92`; it was cancelled after
  schedule validation changed the source.
- Fresh package run [41173422606](https://github.com/auenkr/cadence-rust/actions/runs/41173422606)
  targets head `9a920ed2c8ae7b060b83d5b98e022cda0e0e9dfd`. Its conclusion is
  cancelled after Admin source changed.
- Fresh package run [41173547050](https://github.com/auenkr/cadence-rust/actions/runs/41173547050)
  targets head `e6f7d159923b3295e455f0959c7b3af4973aaa2e`. Its conclusion is
  **failure**: Clippy could not resolve the direct `cadence_common` references
  used by cron validation in `handler_schedule.rs`. Go oracle and formatting
  steps passed; Rust formatting and tests were skipped. Added the missing direct
  path dependency to `cadence-frontend/Cargo.toml`; fresh CI is required.
- Run [41173868530](https://github.com/auenkr/cadence-rust/actions/runs/41173868530)
  was dispatched for head `f6c0d65004cab12e9f9e1a3699f3290cad651d90` after
  adding the dependency and lockfile entry. Conclusion pending; inspect with
  `gh run view 41173868530`.

## Known gaps — do not claim service parity

- Domain API methods are not yet wired to persistence/runtime clients. Failover
  and domain replication queue behavior, update cooldown/config state-machine
  integration, global-domain create/update transmission, archival provider
  scheme validation, audit logging, dynamic config filters and history-sourced
  failover progress remain to port or verify.
- Admin `DescribeCluster` must use live membership/persistence data. Go search
  attribute authorization and Elasticsearch/OpenSearch mapping updates remain
  unported. Replication DLQ and queue operations require real persistence and
  service clients; the current `cadence-service-core` single-host stubs return
  empty DLQs/messages or no-op queue resets and are not parity. Raw history
  remains a single-branch implementation and needs Go token/version-history
  oracle coverage.
- Schedule helpers are not yet connected to Frontend RPC handlers. The Go
  scheduler workflow interaction, retry/continued-as-new behavior, lifecycle,
  authorization, metrics/logging, and SDK mixed-service tests remain open.
- The current source has not yet had a completed fresh CI result. Mixed
  Go/Rust failure, restart and recovery validation has not been run.

## Continuation checkpoint

- After the `origin/frontend-core2` + `origin/t3/frontend-workflow-api` +
  `origin/master` integration, package run
  [41207364204](https://github.com/auenkr/cadence-rust/actions/runs/41207364204)
  failed at head `91e51d48b0a19b0e4b42036de75c278d0864d8ca` while compiling
  `cadence-service-core`. The failures were a duplicate `RefreshWorkflowTasks`
  handler left by the merge, two missing `FailoverError::Persistence` arms,
  missing `FromThrift` trait qualification in Admin DLQ decoding, and an
  outdated raw-history blob encoding assignment. These issues are being fixed
  before the next package run; this run is not a passing validation.

- Rebased this branch onto `origin/frontend-core` at `996486568` and pushed
  the rebased branch head `e2f232449aec2a01b0c153f1e8ab2ba439da34c9`.
- CI run [41174245899](https://github.com/auenkr/cadence-rust/actions/runs/41174245899)
  completed `success` at exact head `e2f232449aec2a01b0c153f1e8ab2ba439da34c9`.
  It passed package Clippy, Formatting, and Test; Frontend runtime smoke was
  skipped because this was a package-selected run. The workflow's
  `format_frontend_files` option ran rustfmt on the CI runner and its
  `frontend-format-patch` artifact was applied to the branch.
- Added Admin raw-history request validation (Go validation order, positive
  page size, paired event/version bounds, UUID input forms accepted by
  `google/uuid`) and Elasticsearch search-attribute type mapping with unit
  cases. Commit `e09ab5cebf01a4a29b0b386d816f4f239e4bb1a6` is pushed. CI run
  [41174552823](https://github.com/auenkr/cadence-rust/actions/runs/41174552823)
  targets this head with formatting enabled and is in progress. These new cases
  remain unverified until that run completes.

- Run [41174552823](https://github.com/auenkr/cadence-rust/actions/runs/41174552823)
  completed `success` at `e09ab5cebf01a4a29b0b386d816f4f239e4bb1a6`. It passed
  Frontend package Clippy, Formatting, and Test after CI applied rustfmt in the
  runner; `frontend-format-patch` was applied and committed as
  `809ecc73b`. A clean-target package validation was dispatched at that exact
  source head as run [41174859526](https://github.com/auenkr/cadence-rust/actions/runs/41174859526)
  and completed `success` at exact head
  `809ecc73b6f25b5444272712e534f5a611eefb69`. It passed clean-target Frontend
  package Clippy, Formatting, and Test. The formatter-enabled run's tests also
  cover the new unit cases. This does not validate mixed Go/Rust behavior.

The handlers remain a partial port. Domain methods are not wired to real
persistence/runtime clients; update cooldown/failover transmission and domain
replication queue work are missing. Admin authorization and dynamic-config
backing integration, live membership collection, search-attribute dynamic
config/ES mapping mutation, DLQ/queue RPCs and full raw-history pagination are
missing. Schedule validation helpers are not wired to scheduler workflows;
RPC lifecycle, retries, authorization and operational metrics remain missing.
Do not treat helper tests or this CI run as Frontend parity or move the service
migration forward.

## Continued implementation — after frontend-core2 landed

- Rebased onto `origin/frontend-core2` at `6ecd41134` (which includes the
  generated History/Matching clients, dynamic validator seam and lifecycle
  helpers) and pushed the rebased branch. Rebase retained both core2 and
  domain/admin progress history entries.
- Replaced the Domain handler's unit-only boundary with stateful operations
  over the shared `cadence-service-core::handlers::AppState`: RegisterDomain,
  DescribeDomain, ListDomains, UpdateDomain and DeprecateDomain. Registration
  now performs primary-cluster gating, duplicate-before-name validation,
  enabled-cluster replication validation, dynamic retention checks, v7 UUID
  creation, and domain/execution-store registration. Updates use the existing
  persistence domain-update controller for cooldown/failover classification;
  deprecation checks global-domain primary ownership. Added direct handler
  unit cases for CRUD ordering and global registration gating.
- Added `uuid` as a direct Frontend dependency with v7 enabled; updated the
  workspace lockfile dependency list by hand. No local Cargo command was run.
- This code has not been validated on CI yet. Dispatch a fresh
  `cadence-frontend` run on the pushed rebased source. Do not claim Domain
  parity: update/deprecate transmission and audit, archival provider behavior,
  durable metadata/version locking and replication queue integration still
  require implementation. Admin and Schedule RPC dispatch wiring is also open.

## Continued service-core integration

- The shared YARPC dispatch used by the running Frontend still bypasses the
  new frontend domain handler, so I tightened the live `RegisterDomain` path
  in `cadence-service-core/src/yarpc.rs` to match Go's primary-cluster gate,
  duplicate-before-name validation, domain-name and length checks, enabled
  archival URI requirement, and UUID v7 generation. `DeprecateDomain` now
  checks the existing domain's global flag and primary-cluster ownership before
  mutation. This does not yet add archival-provider scheme checks, domain
  replication queue transmission, or the audit record.
- `GetSearchAttributes` now merges registered custom keys from the shared
  `AppState.admin` map into the default registry for both JSON and YARPC paths;
  this connects the existing AddSearchAttribute state mutation to reads.
- These changes are in progress at the current branch head and need a fresh
  package CI run. The previous run [41175622384](https://github.com/auenkr/cadence-rust/actions/runs/41175622384)
  targets `508ae8ce85a05a9db5c8a6f3d1c909e0a1817a0a` and was still in progress
  at the last check; it does not cover these edits. Remaining Admin and
  Schedule RPC dispatch, real persistence/client behavior, RPC lifecycle,
  mixed-cluster validation, and full Go method coverage remain open.
- Run [41175622384](https://github.com/auenkr/cadence-rust/actions/runs/41175622384)
  ultimately failed at `508ae8ce8` before Frontend checks: Clippy hit an
  existing lifetime error in `cadence-persistence::dynamic_config::string_domain`,
  which returned a borrowed `&str` from an owned selected value. Fixed it by
  owning the string inside `and_then`; this is required to restore the rebased
  branch build. Run [41175834322](https://github.com/auenkr/cadence-rust/actions/runs/41175834322)
  was dispatched at `e4731c654` before this fix and will not validate it.
- Package run [41175929711](https://github.com/auenkr/cadence-rust/actions/runs/41175929711)
  at exact source `2e8a43df447ba3daaf3d28477b58b727d4f446e9` compiled through
  `cadence-service-core`, then failed in the rebased Frontend skeleton before
  tests: `ResetStickyTaskListRequest` was imported from `cadence_api::tasks`
  rather than `service_types`, and both generated gRPC clients wrapped the TLS
  result in `Option<Option<ClientTls>>`. Fixed the import and flattened the
  TLS construction. Dispatch a new CI run for the updated source.
- Added Go's `CheckPermission` token gate to the live Admin AddSearchAttribute
  endpoint: it now reads `history.enableAdminProtection` and
  `history.adminOperationToken` from the same dynamic-config overlay and returns
  Go's exact bad-request message when the token is missing or wrong. External
  authorization middleware (the Go authorizer decision and operation mapping)
  remains unwired in the Rust YARPC/HTTP runtime.
- The raw-history package validator had its own strict UUID parsing and still
  rejected forms accepted by Go `google/uuid.Parse`; updated it to accept
  canonical, compact, braced and `urn:uuid:` spellings and added Go-shape cases.
  This still does not add multi-branch persistence or History-client paging.

## YARPC dynamic-config wiring continuation

- Added Frontend YARPC dispatch for Admin `GetDynamicConfig`,
  `UpdateDynamicConfig`, `RestoreDynamicConfig` and `ListDynamicConfig` at
  `428dacef917e8d00e1e46ce7aa360236ca7d661b`. These use the shared
  `AppState.admin.dynamic_config` overlay so reads observe updates made through
  the JSON Admin endpoints and dynamic properties. Requests validate required
  `ConfigName`; missing get values map to the IDL-declared InternalServiceError.
- Pushed `t3/frontend-domain-admin` to `origin/frontend-domain-admin`.
- Package CI run
  [41177742133](https://github.com/auenkr/cadence-rust/actions/runs/41177742133)
  targets exact head `428dacef917e8d00e1e46ce7aa360236ca7d661b`; latest observed
  status: in progress. No Cargo command was run locally.
- Current continuation gaps still include Admin operational dynamic-config
  RPCs, authorization, AddSearchAttribute Thrift dispatch and ES mapping writes;
  domain durable persistence and replication queue; full multi-branch raw
  history; schedule workflow RPC/lifecycle wiring; and complete method parity.
  Do not claim Frontend parity on this checkpoint.

## Admin YARPC search-attribute and cluster methods

- Added Thrift converters for normal dynamic-config and Admin search-attribute
  messages in `cadence-thrift/src/convert.rs`, then connected YARPC
  `AddSearchAttribute`, `GetDynamicConfig`, `UpdateDynamicConfig`,
  `RestoreDynamicConfig`, and `ListDynamicConfig` to the shared AppState.
  Search-attribute registration enforces the dynamic-config token gate, rejects
  reserved and type-conflicting keys without a partial update, and updates the
  shared `frontend.validSearchAttributes` overlay and registry.
- Added response converters and YARPC dispatch for `DescribeCluster`. It uses
  the existing single-host snapshot builder. Live resolver membership and
  configured persistence backend discovery are still unimplemented.
- Code commits pushed: `81bc89c81` (config/search converters and handlers) and
  `846b1b257` (cluster description dispatch). The earlier run
  [41177742133](https://github.com/auenkr/cadence-rust/actions/runs/41177742133)
  at `428dacef9` was cancelled when follow-up changes were dispatched.
  Run [41177839469](https://github.com/auenkr/cadence-rust/actions/runs/41177839469)
  at `81bc89c81` was in progress when superseded; run
  [41177900129](https://github.com/auenkr/cadence-rust/actions/runs/41177900129)
  targets exact current code `846b1b25767a7496b66c535819bae982df9f963b` and
  was pending at last check. No local Cargo command was run.
- Method parity remains incomplete. Operational dynamic config, live membership,
  Admin DLQ/replication/queue paths, raw-history version-branch paging, Domain
  replication transmission/durable metadata, scheduler workflow interactions,
  lifecycle/retry/metrics and mixed Go/Rust validation remain open.

## Operational dynamic-config dispatch

- Added Rust Admin aliases for Go's operational dynamic-config messages (same
  Thrift field shapes) and reused the shared overlay with its operational
  namespace for `GetOperationalDynamicConfig`,
  `UpdateOperationalDynamicConfig`, `RestoreOperationalDynamicConfig`, and
  `ListOperationalDynamicConfig`. Commit `6e6298dd2` is pushed.
- CI run [41177987741](https://github.com/auenkr/cadence-rust/actions/runs/41177987741)
  targets exact head `6e6298dd2`; status was pending at dispatch. This run
  includes package Clippy, formatting, and tests. No local Cargo command was
  run.

## Admin queue method Thrift dispatch

- Added converters and YARPC routes for `ResetQueue` and `DescribeQueue` at
  `260de4e0c`. Both validate shard range and required task type; queue
  descriptions report current decision/activity/timer/backoff/schedule depths.
  `ResetQueue` still follows the existing single-host no-op behavior because
  the persistence queues do not expose reset operations yet.
- CI run [41178056604](https://github.com/auenkr/cadence-rust/actions/runs/41178056604)
  targets the branch at the time of dispatch (`260de4e0c`) and includes the
  package gate with CI formatting enabled. Pending at this checkpoint. No local
  Cargo command was run.
- Admin queue parity is still partial: reset is not backed by queue state;
  DLQ operations, replication methods and raw-history YARPC paging are still
  not routed. Domain replication queue transmission and scheduler workflow
  interactions remain gaps.

## Raw-history YARPC path

- Added Thrift converters for `GetWorkflowExecutionRawHistoryV2` and
  `VersionHistory`, plus YARPC dispatch using the existing persistence raw
  history token and version-history selection logic. Each call returns one
  persistence page, encodes event batches as ThriftRW, validates continuation
  tokens against the request, and returns empty batches on persistence-level
  missing history as the existing JSON handler does.
- Commit `6b67253f7` is pushed. Package CI with formatting enabled is run
  [41178167224](https://github.com/auenkr/cadence-rust/actions/runs/41178167224),
  targeting `6b67253f7`; pending at dispatch. Earlier run
  [41178056604](https://github.com/auenkr/cadence-rust/actions/runs/41178056604)
  targeted pre-raw-history queue code `260de4e0c`; inspect it for errors, but
  it is superseded for current-source validation. No local Cargo command ran.
- This raw-history route still relies on the current single-branch execution
  persistence and is not multi-branch Go parity. Remaining Admin dispatch
  includes DLQ/replication operations and task maintenance; Domain durability
  and replication queue, scheduler workflow integration, and mixed-cluster
  validation remain open.

## CI failure and converter import fix

- CI run [41178167224](https://github.com/auenkr/cadence-rust/actions/runs/41178167224)
  reached Clippy at head `6b67253f79170dc07d7fe3e315a121a318be16ef` and failed
  in `cadence-thrift` because the new wildcard import of Admin types conflicted
  with the pre-existing `RefreshWorkflowTasksRequest` type. Replaced it with
  explicit Admin imports in commit `ea87244aa` and pushed both
  `frontend-domain-admin` and its T3-prefixed mirror.
- Fresh package run [41178360466](https://github.com/auenkr/cadence-rust/actions/runs/41178360466)
  targets `ea87244aa`; pending at dispatch with CI formatting enabled. The
  latest source is still unverified until this run passes Clippy, Formatting,
  and Test.

- The second CI dispatch ([41178360466](https://github.com/auenkr/cadence-rust/actions/runs/41178360466), exact code head `ea87244aa`) was still in preflight when checked. Extended the workflow's CI rustfmt patch list to include `cadence-thrift/src/convert.rs`, since `cargo fmt --all --check` also validates the new converter code. Workflow commit `08eb6a783` is pushed. The newest formatting-enabled package run is [41178407221](https://github.com/auenkr/cadence-rust/actions/runs/41178407221), which targets `08eb6a783`; no current-source CI result yet.

## Schedule validation parity correction

- Cross-checked the live Thrift schedule handlers against Go's
  `handler_schedule.go`. Go gates only `CreateSchedule` on `worker.enableScheduler`;
  existing schedule operations remain available. Removed the extra Rust feature
  gates from describe/list/delete/pause/unpause/backfill/update. Moved create
  domain resolution until after request validation, added Go's distinct missing
  Action error, and validate UpdateSchedule policies before its spec/action.
- Commit `ebf2b19a3` is pushed. Package run
  [41178498001](https://github.com/auenkr/cadence-rust/actions/runs/41178498001)
  targets this source and includes CI formatting. Pending at dispatch.

## Workflow maintenance Admin RPCs

- Added Thrift converters and YARPC dispatch for `DeleteWorkflow` and
  `RefreshWorkflowTasks` at `c3acb7697`. Delete uses persistence and returns
  Go-compatible not-found/already-completed errors. Refresh resolves the domain,
  validates the execution, and requeues tasks through persistence.
- CI run [41178581328](https://github.com/auenkr/cadence-rust/actions/runs/41178581328)
  targets the current code and runs package clippy/tests with formatter enabled;
  pending at dispatch. No local Cargo command was run.

## Rebase onto core2 and resumed parity audit — 2026-10-31

- Rebased the domain/admin branch onto `origin/frontend-core2` at
  `be0432cc7dcffc39469bad1d7e09b6bebc34231d`, which itself includes
  `origin/frontend-core` at `4fe22ee2a19345553a7ddda0fea244f186ed286`.
  Conflict resolution retained core2's History/Matching clients and request
  validator, and preserved the domain/admin changes and Frontend drain test.
  The branch has not yet been force-pushed after this rebase.
- Existing in-progress package workflow [41178581328](https://github.com/auenkr/cadence-rust/actions/runs/41178581328)
  targets pre-rebase code `c3acb7697158921807b49476640a7eab6b7afcce`;
  run [41178553763](https://github.com/auenkr/cadence-rust/actions/runs/41178553763)
  tests the core2 base `be0432cc7dcffc39469bad1d7e09b6bebc34231d`. Both were
  still in progress at the audit snapshot. Neither validates the rebased head.
- Coverage audit against Go inventories shows that helpers and shared YARPC
  routes exist, but `cadence-frontend` handler modules still expose stub
  `dispatch` methods. Shared `cadence-service-core` routes have partial
  implementations, with DLQ/replication queue behavior still modeled as empty
  single-host responses and schedules stored in the in-memory execution store
  instead of invoking scheduler workflows. Domain metadata remains in-memory
  and updates do not publish domain replication tasks. The full Admin method
  inventory in `go/service/frontend/admin/handler.go` has not been ported.
  No Frontend parity claim is warranted.
- No Cargo command was run locally. Next: force-push the rebased branch, dispatch
  exact-head `cadence-frontend` CI, then implement missing runtime-backed
  methods and queue/scheduler integrations incrementally. Record each exact
  head and CI result here; mixed Go/Rust validation is still outstanding.
- Implemented a live per-domain `worker.enableScheduler` read in shared
  `AppState` and switched both JSON and YARPC schedule gates from the startup
  boolean to the Admin/runtime dynamic-config overlay. Added a focused test
  that sets the property for one domain and confirms another remains disabled.
  Source `git diff --check` passed; Cargo was not run locally. This is a
  targeted behavior correction, not scheduler-workflow integration.
- Exact-head package run [41178701269](https://github.com/auenkr/cadence-rust/actions/runs/41178701269)
  targeted rebased source `29f68d576e47b2a61e68d2ca5c1076c24791c4f0` and was
  still in progress at last inspection. It predates the scheduler-gate fix.
  Run [41178781339](https://github.com/auenkr/cadence-rust/actions/runs/41178781339)
  targets `c22a3555274b6288ca65b2be0b0f70738786e8f1`, but was pending at the
  last inspection. Both require a final conclusion; neither covers the
  additional schedule-validation-order fix below.
- Corrected live YARPC schedule validation order for CreateSchedule and
  UpdateSchedule so an empty domain returns Go's `Domain not set on request.`
  before dynamic feature gating or schedule-ID checks. Dispatch fresh package CI
  after committing this edit.
- Run [41178826780](https://github.com/auenkr/cadence-rust/actions/runs/41178826780)
  at `348639c20477b44b673c0ec030f860a8efca071b` failed before Clippy/tests in
  the existing changed-runtime formatting check: `cadence-persistence`'s
  `dynamic_config.rs` and `cadence-service-core/src/admin.rs` need rustfmt.
  The Go oracles and workspace format step passed. Extended the existing
  CI-only Frontend formatting patch step to include those touched core files;
  dispatch it with `format_frontend_files=true` and apply its uploaded patch.
- The formatting-patch run [41179058460](https://github.com/auenkr/cadence-rust/actions/runs/41179058460)
  at exact source `7f16043ef72126891befb89b8691d97f8e1d9958` completed with
  `failure` in Clippy. After CI formatting was applied from its
  `frontend-format-patch` artifact, Clippy exposed missing Admin type imports in
  the rebase-conflicted `cadence-thrift/src/convert.rs` (`ResetQueue*`,
  `DescribeQueue*`, and raw-history V2 request/response). Added those imports;
  this fixes the reported source errors, pending fresh exact-head CI.
- Mirrored Go `DeprecateDomain`'s metadata update by incrementing the stored
  domain config version and write timestamp as status becomes Deprecated, with
  a version assertion in the persistence test. Domain replication transmission
  and metadata-backed durable storage remain open.
- Lifecycle review corrected the earlier stale gap note: the shared launcher
  already has a dedicated `run_frontend` path with readiness/admission drain
  and shutdown ordering. Frontend-specific retry policy, request metrics,
  complete middleware and remote membership are still not integrated.
- Clippy on run `41179058460` did not reach Frontend validation: the rebased
  Thrift converter imports failed to compile. Added the six missing Admin
  converter imports and applied the uploaded CI formatting patch. Fresh run
  `41179320362` now targets `b5dedb6a4` and is pending; collect its final result.

## Rebased Admin converter repair — 2026-10-31

- CI run [41179320362](https://github.com/auenkr/cadence-rust/actions/runs/41179320362)
  is cancelled at pre-repair head `b5dedb6a40c93508d5ec383f2804f3ed4779a6d1`;
  it does not validate the latest source. The uploaded CI rustfmt patch was
  applied and missing Admin Thrift converter imports were added. Removed the
  now-obsolete Frontend module stub dispatch methods from `handler_admin.rs`
  and `handler_domain.rs`; those modules expose implemented methods instead.
- Latest core2 remote advanced since the previous rebase; rebase this checkpoint
  onto its current fetched head before dispatching exact-head package CI.
  Domain/Admin/Schedule parity gaps remain extensive; no service completion
  claim is made.
- During post-rebase review, repaired `handler_admin::AdminError`'s missing
  `EntityNotExists`/`InternalServiceError` type imports and made Display cover
  all three variants. `git diff --check` passes. This source correction needs a
  fresh package run; Cargo remains CI-only.
- Shared Admin state is now injectable with `Handler::from_app_state`, using
  the live `AppState.admin` dynamic-config overlay and custom search-attribute
  registry. Added a focused test proving a scheduler property mutation is
  observed by the runtime reader and a custom key lands in the shared map.
  This removes the handler's previous isolated state, but Thrift's existing
  service-core adapters still own their direct implementation and must be
  unified with this module. Source whitespace validation (`git diff --check`)
  passes; no local Cargo/rustfmt was run.

## Current continuation status after the core2 rebase

- Rebased onto current `origin/frontend-core2` (`642538a2720d28e7ff5611e690e1fe3208033d95`), force-pushed `frontend-domain-admin`; latest source head is `d39036c477249d4eeeb9762424b3d30071e0d995`.
- Exact-head CI run [41179462360](https://github.com/auenkr/cadence-rust/actions/runs/41179462360) at `85eda7222fc63e579a8399f2e7a5cdf73fe3dfe6` was cancelled after later source changes. Run [41179565921](https://github.com/auenkr/cadence-rust/actions/runs/41179565921) at `f02bf9fb0c066f385679fd6b430342eb015e1010` was cancelled when shared-state changes were pushed. Run [41179614145](https://github.com/auenkr/cadence-rust/actions/runs/41179614145) targets exact current source `d39036c477249d4eeeb9762424b3d30071e0d995`; it remains in progress. Cargo has not been run locally.
- Source inventory compared against Go methods shows the Thrift dispatch still lacks Admin lifecycle/History-client methods: `RemoveTask`, `CloseShard`, shard/history-host description, replication and DLQ methods, `ReapplyEvents`, `ResendReplicationTasks`, cross-cluster task methods, domain isolation/async configuration and task-list partition mutation. Existing `ResetQueue` is currently validation plus empty success, and `DescribeQueue` reports local in-memory queue depths rather than calling History. Domain dispatch lacks `UpdateIsolationGroups` and `UpdateAsyncWorkflowConfiguraton`. Schedule method names are routed, but the storage path remains in-memory and does not start/update scheduler workflows. This is a source inventory, not parity evidence.
- No mixed Go/Rust Frontend validation has been run. Keep all of these gaps open until real History/Matching client wiring, durable persistence and SDK failure/restart/recovery lanes are complete.
- Ported the six Admin isolation-group and async-workflow-configuration Thrift
  routes into the live YARPC dispatcher. They now decode Go field IDs, validate
  empty domain names, read/mutate the shared domain state, and return typed
  Thrift responses. Added matching request/nested response converters and
  converter round-trip coverage. This closes two domain internal APIs and six
  Admin RPCs from the prior inventory; global/domain metadata remains backed by
  the current in-memory domain store, and durability is still open. `git diff
  --check` passes; CI formatting/build/tests are pending and Cargo was not run
  locally.
- CI run [41179614145](https://github.com/auenkr/cadence-rust/actions/runs/41179614145)
  failed at exact head `d39036c477249d4eeeb9762424b3d30071e0d995` in
  `cadence-service-core` before Frontend checks. It exposed a duplicate
  `RefreshWorkflowTasks` dispatcher/helper (admin and workflow signatures
  collided in the shared dispatcher) and raw-history code assigning a
  `DataBlob` where bytes were expected. Removed the conflicting second
  dispatcher registration/helper, keeping the common Thrift request handler,
  and now select the encoded batch bytes explicitly. These fixes and the new
  isolation/async config routes are pending exact-head CI.
- Added the Admin `ReapplyEvents` Thrift path. It validates the request in Go
  order, decodes the ThriftRW history batch, filters signal events, and invokes
  the execution store's signal reapplication mutation with Go-compatible error
  mapping. Added a converter round-trip case. This is single-version-history
  behavior only; replication/version-history correctness and source Go tests
  remain necessary. CI run [41179895028](https://github.com/auenkr/cadence-rust/actions/runs/41179895028)
  at `83171ac866d417a39db3a6fc888f4b4c719e6a5a` was in progress against the
  preceding code and will be superseded by exact-head validation.
- Added Thrift `MaintainCorruptWorkflow` dispatch backed by the shared execution
  mutation and mapped its not-found/already-completed errors, plus request and
  response converters and a Go-shaped request round trip. Current limitation:
  corrupt-run detection/history data are only as complete as the single-host
  execution store. CI run [41179991180](https://github.com/auenkr/cadence-rust/actions/runs/41179991180)
  targets the previous head and must be inspected for cancellation/result;
  dispatch a fresh exact-head package run after this commit.

## Rebase and Domain route parity continuation

## Rebase onto current frontend-core2 and Admin queue validation — 2026-10-31

- Rebased the complete implementation history onto `origin/frontend-core2`
  (`6eae53c5610a05143f7552b78c7e7c4feb8185b5`) and force-pushed it to
  `origin/frontend-domain-admin` at `a9f8a169c2f9f90617c7a5cc2d80b7582500d6a4`.
- Exact-head package run
  [41180994551](https://github.com/auenkr/cadence-rust/actions/runs/41180994551)
  failed in Clippy due to two `needless_update` warnings in the new core2
  request-validator tests, before Frontend formatting/tests. Removed those
  no-op struct updates; this follow-up needs fresh exact-head CI.
- Cross-checked `ResetQueue` and `DescribeQueue` against Go's Admin handler and
  `validate/errors.go`. Corrected the request-type error to `Queue type is not
  set.` and added the required non-empty cluster-name check (`Cluster name is
  not set.`) after the request-type check. `git diff --check` passed. No Cargo
  command was run locally.
- Remaining parity gaps listed above still stand: especially History-client
  Admin RPCs and real DLQ/queue behavior, full membership resolution, durable
  domain persistence/replication, schedule workflow integration and mixed
  Go/Rust restart/recovery evidence. This is not Frontend completion.
- Raw-history continuation: both JSON and Thrift handlers now normalize
  accepted non-canonical UUID spellings only for the persistence lookup and
  preserve the caller's original run ID for pagination tokens. Added a
  `canonical_run_id` helper and extended its Go-spelling unit matrix. This
  changes `cadence-persistence` and `cadence-service-core`, so the package CI
  needs to be rerun after the next push. `git diff --check` passed; Cargo was
  not run locally.
- CI [41181224995](https://github.com/auenkr/cadence-rust/actions/runs/41181224995)
  at `7b84b0be0` failed in the repository formatting gate before tests. The
  failure was the existing `handler_domain.rs` import wrapping after core2
  rebase; corrected it manually. Run [41181380560](https://github.com/auenkr/cadence-rust/actions/runs/41181380560)
  targets `faeff2796` and is the current validation candidate; poll for its
  conclusion and inspect any follow-up failures.
- CI [41181443949](https://github.com/auenkr/cadence-rust/actions/runs/41181443949)
  at `81a705cdc` passed the workspace format check, then failed the changed
  runtime format check on two `admin.rs` lines in the raw-history patch. Applied
  the rustfmt layout reported by CI manually; no Clippy/test stage ran. Dispatch
  a fresh exact-head package run after pushing this repair.

- Rebased the implementation branch onto the then-current
  `origin/frontend-core2` at `3bc2dcca8`; conflict resolution kept the newer
  validator request fields and core2 History/Matching persistence clients.
  Force-pushed the rebased history to `origin/frontend-domain-admin` at
  `91a75125220f6ace1f214e5289b177932c0ce588`.
- Package run [41180174300](https://github.com/auenkr/cadence-rust/actions/runs/41180174300)
  targets that exact head. It is still in progress; its Clippy/Formatting/Test
  conclusions must be recorded after completion. No Cargo command was run
  locally.
- Source audit confirmed the persistence update transition already enforces
  configured domain update cooldowns, global failover version changes, and
  graceful failover state validation. Found and fixed a transport inconsistency:
  JSON `DeprecateDomain` now resolves the domain and rejects global-domain
  mutation from a non-primary cluster, matching the Go handler and the existing
  YARPC path. This correction needs a fresh exact-head package CI run.
- Remaining parity is substantial and remains open: the domain store is still
  in-memory rather than backed by configured persistence; domain changes do
  not publish/consume the Go replication queue; Admin methods still use
  single-host placeholder behavior for DLQ, reset/remove queue, membership, and
  cross-cluster History client calls; raw history remains single branch; and
  schedule persistence does not invoke scheduler workflows. This checkpoint
  does not claim service parity.
- The `frontend-core2` branch advanced again to `b4fda09b4` during review. I
  rebased this implementation branch onto that current head and force-pushed
  `b2078a0ef`. The earlier run [41180174300](https://github.com/auenkr/cadence-rust/actions/runs/41180174300)
  targets pre-rebase `91a751252`; it is stale for current code.
- Added the HTTP `DeprecateDomain` primary-cluster check and aligned JSON
  `RegisterDomain` with Go's global-secondary error precedence and UUIDv7
  generation. Those follow-up edits are not part of `b2078a0ef` and require
  their own exact-head CI run.
- Follow-up Go comparison found that both JSON and Thrift `DeleteDomain` paths
  also skipped the global-domain primary-cluster guard. Added the lookup and
  guard to both transports before deletion. This is another narrow parity fix;
  persistence-backed deletion, replication transmission and audit records are
  still not implemented. It supersedes the prior exact-head CI candidate and
  must be validated at the next pushed head.
- The exact-head package CI run [41180348859](https://github.com/auenkr/cadence-rust/actions/runs/41180348859)
  at `093abbdef55af2ba5659892b9406e58af6c6b66f` failed in Clippy before
  Frontend checks: core2's `Fields` API has `obool`, not `bool`; four new
  Thrift converter calls used the removed method. Replaced them with `obool`.
  The subsequent candidate [41180550214](https://github.com/auenkr/cadence-rust/actions/runs/41180550214)
  at `dc34713bf` predates this fix and is superseded. Fresh CI is required.
- Exact-head CI [41180597552](https://github.com/auenkr/cadence-rust/actions/runs/41180597552)
  at `d5a77d7237736724a07240baae4ba591da37d4a2` passed Go oracle and formatting
  steps, then failed during Clippy with nine `&str`/`String` mismatches in the
  rebased Admin YARPC handlers. Added explicit string conversion at those
  call sites; source-only diff checks pass. Dispatch a fresh package run after
  pushing this repair.

## Continued Admin validation and core2 refresh — 2026-10-31

- Reconfirmed current source still has placeholder behavior in `cadence-service-core/src/admin.rs`: DLQ endpoints return empty results, queue removal/reset is a no-op, membership is synthesized from one host, and several cross-cluster methods do not call clients. Schedule HTTP/YARPC operations persist schedule records but do not yet signal the scheduler workflow. Domain state remains the shared in-memory metadata store.
- The working tree contained an uncommitted Admin validation fix carried from the prior session: `GetReplicationMessages` now matches Go's `Cluster name is not set.` message; `GetDLQReplicationMessages` rejects an empty task-info list; and read/purge/merge DLQ routes reject `DLQType.Invalid` with Go's unsupported-type error. It passes `git diff --check`; exact-head CI has not yet run.
- `origin/frontend-core2` advanced after this branch's previous rebase. Rebase onto its current fetched head before the next CI dispatch. No Cargo command was run locally.
- After the `frontend-core2` refresh, replaced JSON Admin `ResetQueue` and `DescribeQueue` single-host placeholders with calls to the configured History gRPC API. The shared runtime now creates a lazy History channel using `services.history.rpc` host/port and TLS settings. Both routes preserve Go's request validation order, including required type then cluster name, attach Cadence RPC caller/service/encoding metadata and a 30-second request deadline, and return History queue results/errors. `git diff --check` passes. This does not yet wire the equivalent Thrift Admin methods or the remaining History client methods; compile/Clippy/tests are pending CI. No Cargo command was run locally.
- Extended the History-client delegation to the Thrift/YARPC Admin `ResetQueue` and `DescribeQueue` methods, and to JSON `CloseShard` and `RemoveTask`. RemoveTask converts the API's nanosecond visibility timestamp to protobuf seconds/nanos. This replaces further local no-op/synthetic responses. CI run [41182125468](https://github.com/auenkr/cadence-rust/actions/runs/41182125468) targets the earlier queue-delegation head `55f995e1c2046798b2b56afc62f1fa7165c7e9ad`; it was still running at this checkpoint and will be superseded by the new source push. The added Thrift and extra JSON History calls need exact-head CI.
- Added YARPC/Thrift dispatch for `CloseShard` and `RemoveTask`, delegating to the same configured History API and converting visibility timestamps to protobuf time. This closes the Thrift transport gap for four History-owned queue/shard Admin operations. Exact-head CI run [41182184746](https://github.com/auenkr/cadence-rust/actions/runs/41182184746) at `8bbee1b06c58fb50c0ae0d098947ee46c3e6012c` is still in progress; it covers the JSON implementations and initial Thrift queue delegation before the new CloseShard/RemoveTask dispatch. The current diff passes `git diff --check`; run new CI once the active run concludes. No Cargo command was run locally.
- Exact-head run [41182289893](https://github.com/auenkr/cadence-rust/actions/runs/41182289893) at `642ab10719247668e7a3f29c5a1e9fb9a8dde45d` failed in Clippy because the newly dispatched Thrift `CloseShard` and `RemoveTask` requests/responses lacked `FromThrift`/`ToThrift` implementations. Added the four converters in `cadence-thrift/src/convert.rs`, using Go IDL field IDs 10/20/30/40/50 and empty response structs. This directly repairs the reported type errors; fresh exact-head CI is pending.
- Branch topology check: the implementation shares ancestry with `origin/frontend-core2` at `35e84865e01e5437a7a6afb16055741d531c88ab`; the remote `frontend-core2` tip has since moved to docs-only `def795959ee2b7771c34644a2c6a227fef2748ad`. The persistence and History/Matching client changes from the core2 implementation are already in the shared ancestor. No rebase onto the docs-only tip is needed, and replaying the implementation onto that older endpoint would drop current work.
- Package run [41182631208](https://github.com/auenkr/cadence-rust/actions/runs/41182631208) targets `b54eceb594387caf678422a7596da06eca07de1e` and failed at the changed-runtime rustfmt check before Clippy. The converter repair was not type-checked in this run; rustfmt reported layout changes in `cadence-matching/src/launcher.rs` and `cadence-service-core/src/admin.rs`. Dispatching the existing CI formatting-patch mode, then a fresh package run.
- Formatting run [41182837756](https://github.com/auenkr/cadence-rust/actions/runs/41182837756) targets `94192026e12c3211de1f459a731ee1fc9b48095b`; its `Upload Frontend formatting patch` step succeeded. Applied the runner-produced `frontend-format.patch`, which formatted the launcher, shared Admin/YARPC handlers, and Thrift converters. `git diff --check` passes. This artifact was generated on CI; no local Cargo/rustfmt command was run. Dispatch a fresh package run for compile/clippy/tests after committing.
- Fresh package run [41183087418](https://github.com/auenkr/cadence-rust/actions/runs/41183087418) at exact head `133bc41ec6b65c955d2bb045044810917a5659e3` completed with conclusion `success`: Clippy, Formatting and Test all passed for `cadence-frontend` and its dependencies. This validates the shard converter repair. It does not establish Frontend parity; see the outstanding gaps below.

## Continuation handoff after exact-head green — 2026-10-31

- Progress/evidence is committed through `0fdd46cc974d81dce7b65890a290396f332f4152`, pushed to `origin/frontend-domain-admin`.
- Exact-head package CI [41183087418](https://github.com/auenkr/cadence-rust/actions/runs/41183087418) is green for that source lineage: Clippy, Formatting, Test. Run [41182837756](https://github.com/auenkr/cadence-rust/actions/runs/41182837756) was cancelled by same-branch concurrency after the CI formatting artifact had been applied; the artifact is committed. No local Cargo or rustfmt command was run.
- `origin/frontend-core2` currently points to docs-only `def795959`; this branch shares its implementation base at `35e84865e`. The History client and persistence changes are in this branch's existing ancestry. No destructive rebase was done.
- Stop condition is not met. Active blockers: Domain CRUD still uses `InMemoryDomainStore`; SQL/Cassandra domain writes, global-domain replication transmission/queue consumption and audit lifecycle are missing. Admin `DescribeCluster` synthesizes membership, search-attribute mutation does not update a real visibility index, dynamic-config updates only mutate the process overlay, and DLQ/replication/cross-cluster endpoints still lack their History/domain queue clients. Schedule CRUD still goes to the local schedule store instead of starting/signaling/querying the `cadence-scheduler` workflow. Raw history only serves the current single-branch persistence representation. RPC retry/metrics/access-control lifecycle parity and mixed Go/Rust SDK failure/restart/recovery evidence are also open.
- Next implementation priority: build the durable Domain persistence/client path and its replication queue contract against Go `go/common/domain/handler.go` and persistence interfaces; then port remaining Admin History/domain DLQ RPCs, wire scheduler workflow start/signal/query paths, and rerun exact-head package CI after each pushed implementation chunk. Do not claim parity from this green package run.

## Latest core2 refresh and durable snapshot rebinding — 2026-10-31

- Fetched the moving `origin/frontend-core2` branch and rebased this branch onto
  its latest implementation tip `f644498bf0b383ce508e06e262329ac94a9b0e5e`.
  The new `FrontendDomainManager` boundary is now the handler-facing API;
  conflict resolution retained that boundary along with the existing global
  domain primary-cluster guards. The branch is based on the fetched core2 tip.
- Added a snapshot-load fix in `cadence-matching/src/launcher.rs`: when
  `--data-dir` replaces `AppState.domains` with the loaded snapshot, Frontend's
  `domain_manager` now points at the same loaded store. Previously it retained
  the pre-load empty clone, so RPC reads/writes and the periodic snapshot loop
  could act on different domain stores. This repairs the existing embedded file
  durability path; it does not satisfy configured SQL/Cassandra persistence.
- Rebase checkpoint `1c673ed3af4f149dfa2514b26f60919293db3878` was pushed and
  package CI [41192950727](https://github.com/auenkr/cadence-rust/actions/runs/41192950727)
  started there. It was still running when the snapshot rebinding change was
  made and will be superseded by the next exact-head run.
- `git diff --check` passed for the snapshot rebinding change. No local Cargo,
  formatter, or tests were run. Next: push this fix, dispatch package CI, then
  implement a manager that commits against the configured Go SQL/Cassandra
  domain tables. Global-domain replication queue transmission and consumption
  remain open.
- Extended the new `FrontendDomainManager` seam to Thrift/YARPC Domain CRUD,
  update, failover, and failover-history routes. Those methods had still called
  `state.domains` directly, which would bypass any injected persistence-backed
  manager even after JSON paths used the seam. The remaining direct domain-store
  uses in Admin are cache/isolated config operations and require their own
  persistence contracts. `git diff --check` passes; no Cargo command was run.

## Continuation — 2026-10-31

- Exact-head package run [41193208098](https://github.com/auenkr/cadence-rust/actions/runs/41193208098)
  failed at Clippy/type-check on pushed head
  `6eb3514c2b0275b2c9ab6849246b8c788a4533df`. The failure was an unclosed
  `frontend_drain_tests` module delimiter in `cadence-service-core/src/handlers.rs`,
  exposed by the latest core2 rebase. Added the missing brace and pushed
  `87d8dd286fbaeb5608593e4980046dac9b94c422`.
- Exact-head package run [41193391363](https://github.com/auenkr/cadence-rust/actions/runs/41193391363)
  was dispatched for `87d8dd286fbaeb5608593e4980046dac9b94c422`; result pending
  at this save. No Cargo/rustfmt/test command ran locally.
- Audit confirms configured SQL/Cassandra stores currently expose only read-only
  Matching metadata interfaces; the Frontend manager is still the in-memory
  implementation. Durable writes and replication queue production/consumption
  remain primary blockers. Next work is to add transactional persistence and bind
  it at configured Frontend startup before advancing the remaining Admin DLQ/
  History, scheduler and membership gaps.
- Added configured SQL domain write primitives to the existing metadata adapters:
  MySQL and PostgreSQL create/update now encode the Go flattened
  `sqlblobs.DomainInfo` payload and atomically compare-and-advance
  `domain_metadata.notification_version`; delete uses the Go no-notification-
  bump behavior. These are persistence primitives only: the Frontend manager and
  configured startup do not yet call them, and Cassandra writer support is still
  absent. Source commits: `f0d4dd6a3` (MySQL), `247c371ee` (PostgreSQL).
- Run [41193495788](https://github.com/auenkr/cadence-rust/actions/runs/41193495788)
  was cancelled by the next push before validation. Run
  [41193518257](https://github.com/auenkr/cadence-rust/actions/runs/41193518257)
  targets `247c371ee59a033acf0db1b203786c30e9452438` and is still running at
  this save; it includes both SQL primitives. Inspect its Clippy/format/test
  result before relying on either implementation. No Cargo command ran locally.
- Package CI [41193518257](https://github.com/auenkr/cadence-rust/actions/runs/41193518257)
  completed `success` at source head
  `247c371ee59a033acf0db1b203786c30e9452438`; Clippy, Formatting and Test
  passed with the MySQL/PostgreSQL SQL write primitives. This validates only the
  persistence methods, not Frontend use or service parity.
- Added a common `DomainMetadataPersistence` trait implemented by the SQL
  adapters and exposed the configured SQL writer in `ConfiguredMatchingStores`
  and `AppState`; production launcher now carries that handle into Frontend.
  This remains wiring only until `FrontendDomainManager` calls it. Cassandra
  remains without a writer. These additions are in the current unvalidated
  source diff; next exact-head CI is required.
- Package run [41193874841](https://github.com/auenkr/cadence-rust/actions/runs/41193874841)
  failed at Clippy on `fc0143da8302134f366ddfe05028221ff2812940` because the
  configured persistence trait object was held in `AppState` without a `Debug`
  bound. Added explicit backend `Debug` implementations and a `Debug` bound;
  this is fixed in the next pushed source checkpoint.
- Implemented `PersistedFrontendDomainManager`: configured MySQL/PostgreSQL
  create/update/deprecate/failover/delete now use the SQL writer and global
  notification-version fence; mutations roll back their in-memory record on
  persistence errors. Configured startup installs it after loading domain
  metadata, and JSON/YARPC errors map persistence failures to internal errors.
  `DomainRecord` now converts to the Go SQL blob's metadata representation.
  This candidate still lacks a Cassandra writer, durable audit/replication
  queue behavior, race/fault recovery coverage, and exact-head CI. `git diff
  --check` passed; no local Cargo/rustfmt/tests ran.

## Rebase repair and continuation — 2026-10-31

- Resolved the duplicate PostgreSQL SQL domain mutation methods created when
  this branch was rebased onto `origin/frontend-core2`. Restored core2's
  create implementation, which reads the current metadata notification version,
  serializes it into the new row, then locks and compare-and-updates the version
  in the insert transaction. Retained its expected-version update and Go delete
  semantics. Added the `DomainMetadataPersistence` adapter and explicit Debug
  implementation required by the shared runtime trait. No local Cargo command
  was run.
- Prior manager run [41194319599](https://github.com/auenkr/cadence-rust/actions/runs/41194319599)
  failed at pre-rebase head `9d4c10e991079df152582a08e11bd10ae39fc610`;
  it is stale and does not validate the current rebased source.
- Current follow-up must commit and push this repair with force-with-lease (the
  branch was rebased and diverges from its prior remote history), dispatch
  Frontend package CI, and record exact head/run conclusion.
- Active blockers remain: Cassandra durable domain writes; global domain
  replication queue send/consume and failover audit/history; admin
  authorization and persistence/dynamic-config backing; search mapping writes;
  DLQ and queue RPC behavior; complete History-client raw history pagination;
  live membership; scheduler workflow interaction; RPC lifecycle/retries/
  metrics; mixed Go/Rust failure/restart/recovery gates; complete Go method
  audit. No parity claim is made.
- Inspecting failed run [41194319599](https://github.com/auenkr/cadence-rust/actions/runs/41194319599)
  logs corrected the failure report above: this run failed the existing
  Matching launcher rustfmt check before reaching the Frontend code. The earlier
  `Debug` issue belonged to preceding run [41193874841](https://github.com/auenkr/cadence-rust/actions/runs/41193874841).
  Fixed the launcher formatting to the CI formatter's exact output. Run
  [41194562850](https://github.com/auenkr/cadence-rust/actions/runs/41194562850)
  was cancelled after this unrelated formatting fix changed the source; dispatch
  fresh CI at the next pushed head.
- `origin/frontend-core2` advanced from `9bf48bc0c` to `7b5635c96`
  (`fix(persistence): transact MySQL domain deletes`). Rebased the Frontend
  branch onto the new tip and resolved overlapping SQL persistence interfaces
  by retaining core2's `GoSqlDomainPersistence` API and implementations while
  adding the Frontend-facing `DomainMetadataPersistence` adapter for both SQL
  stores. Preserved core2's transaction semantics and PG Debug implementation.
  The package CI run [41194722171](https://github.com/auenkr/cadence-rust/actions/runs/41194722171)
  at `48e7220a675177dd864d5bc6bf49540dcafb12d2` was cancelled when core2
  advanced. Push the rebased source and dispatch exact-head package CI.
- Package run [41194839384](https://github.com/auenkr/cadence-rust/actions/runs/41194839384)
  completed **failure** at exact rebased head
  `3427f41471afc357349bbb6479499375dabbf96a`. Clippy found duplicate legacy
  MySQL writer methods after applying the SQL adapter commit; removed that old
  `write_domain` block and retained core2's newer transactional create/update/
  delete methods. Core2 also changed the in-memory domain create error type;
  adapted the convenience method to propagate it into `FrontendDomainError`.
  No local Cargo command was run. Push this repair and redispatch package CI.

## Continuation checkpoint — 2026-10-31 (post core2 rebase)

- Rebased the Frontend work branch onto the latest `origin/frontend-core2`
  tip `a4098142b1b7925e6eb4fc6dee0ba4a2f973e929`. The rebase applied all 92
  Frontend commits without conflicts and now includes the shared Go SQL domain
  transaction contract. The resulting working branch is currently 93 commits
  ahead and 92 behind the old remote ref; push it with `--force-with-lease`.
- Exact old-head CI run
  [41195083454](https://github.com/auenkr/cadence-rust/actions/runs/41195083454)
  completed success at `fb55f284af1a2ee1f611dab69ab238e03bb53001` (Clippy,
  formatting, tests and Frontend runtime smoke). This does not validate the
  rebased head; dispatch fresh package CI after pushing.
- Persistence gap inspection: MySQL and PostgreSQL implement both SQL domain
  mutation traits. Cassandra metadata has only the reader/cache path and does
  not implement `DomainMetadataPersistence`, so configured Cassandra Frontend
  domain mutations remain a concrete blocker. Next implementation slice should
  add Cassandra Go-schema create/update/delete with Go's LWT/version semantics,
  then select that writer in configured Frontend startup. No cargo was run
  locally.

## Cassandra domain persistence slice — candidate

- Added `CassandraDomainMetadataPersistence` against Go's legacy `domains`
  and `domains_by_name_v2` schema. It maps the existing Go UDT fields, creates
  the legacy UUID row, then uses a logged conditional batch for the v2 domain
  write and singleton notification-version CAS. Updates use the same batch
  fence; deletes remove both Go rows. Configured Matching store construction
  now exposes this writer so Frontend's persisted manager can use Cassandra.
- Added Scylla `SerializeValue` derives for the Go domain UDTs and registered
  the persistence module. The CI formatter allowlist now includes these touched
  files. `git diff --check` passed; no local Cargo/rustfmt/tests were run.
- This is an unverified implementation candidate. The Go Cassandra domain
  persistence tests and real Cassandra create/update/deprecate/restart oracle
  have not been run. Exact-head CI is required; in particular confirm the
  driver accepts the nested UDT bind shapes and conditional logged batch.
- Follow-up review against `go/common/persistence/nosql/nosqlplugin/cassandra/
  domain.go` corrected create ordering: the legacy row is inserted before the
  metadata version is read. The zero-version CAS now binds a null version, as
  Go's `updateMetadataBatch` does, while the v2 domain row retains version zero.

## Cassandra typed blob serialization follow-up — candidate

- Corrected Cassandra domain writes to serialize typed bad-binary, isolation
  group and async-workflow configuration structs as ThriftRW with the live
  `history.serializationEncoding` closure. Active-cluster configuration now
  serializes the typed `ActiveClusters` value using Go's `thriftrw_snappy`
  encoding. This removes accidental copying of JSON-oriented cached blob bytes.
- Passed the existing dynamic serialization-encoding closure into the configured
  Cassandra domain writer. `git diff --check` passes; no Cargo/rustfmt was run
  locally. Exact-head CI and a Go Cassandra blob oracle are still required.
- At edit time branch head was `b8e692f6c9b126e5acfed3ad6d71901ad19f23c5`;
  Frontend CI run [41195683733](https://github.com/auenkr/cadence-rust/actions/runs/41195683733)
  was still running for the preceding head and does not validate this change.
- Pushed source head `3911f668de1d24e4afe87b5af5b4b1693fa65eb8`; exact-head
  Frontend CI [41195924069](https://github.com/auenkr/cadence-rust/actions/runs/41195924069)
  failed Clippy with two Cassandra writer type errors (`ToThrift`'s `Sized`
  generic and byte-encoded bad-binary encoding). Both were repaired. During
  failure review, found the SQL path also encoded nested domain blobs as JSON;
  this continuation changes the shared DomainRecord conversion to Go's typed
  ThriftRW / dynamic encoding (active clusters use `thriftrw_snappy`) and wires
  the same dynamic encoding closure into `PersistedFrontendDomainManager`.
  These repairs are pending push and exact-head CI; no local Cargo command ran.

## Rebase onto latest core2 — 2026-10-31

- Fetched `origin/frontend-core2` at `bce4cbe99` and rebased this branch onto
  that tip. The source commits replayed with two manual conflict resolutions:
  retained core2's SQL persistence contract and existing Frontend persisted
  manager, while preserving the Frontend mutation adapter for Cassandra and
  the cross-backend startup wiring. The rebase completed at
  `0fd27b832a52a7b0ac26673cd20b42d3cf1babc2` and was force-pushed to
  `origin/frontend-domain-admin`.
- Exact-head Frontend package CI was dispatched as
  [41196648311](https://github.com/auenkr/cadence-rust/actions/runs/41196648311).
  It failed before Cargo at `Check changed Matching runtime formatting`; the
  failure was a single formatting difference in the rebased Matching launcher
  call to `GoSqlFrontendDomainManager`. Applied the exact rustfmt diff from the
  CI log; redispatch at the next push. No Cargo command ran locally.
- Current blocker focus remains durable replication queue/audit behavior and
  failover history across restart. The domain manager writes SQL/Cassandra
  domain rows, but replication task publication/consumption is not backed by
  persistence yet. Admin DLQ/history, live membership, scheduler workflow
  wiring, RPC lifecycle/metrics, and mixed Go/Rust failure/restart/recovery
  validation remain open.
- `origin/frontend-core2` advanced again to `965cd1213` while CI was running.
  Rebased the branch onto that tip. The prior CI run [41196886047](https://github.com/auenkr/cadence-rust/actions/runs/41196886047)
  targets pre-second-rebase source `84f03b8e679f02516a72513abda2ceced8a64b2a`
  and is not evidence for the rebased head. Push and dispatch a fresh exact-head
  run before relying on the source.

## Rebase onto current `frontend-core2` and startup repair — 2026-10-31

- `origin/frontend-core2` advanced to `a84001cf7` (including transactional
  SQLite domain mutations and domain-manager Clippy cleanup). Rebasing this
  branch replayed 101 commits. Resolved the SQL persistence integration
  conflicts by retaining the configured SQL/Cassandra mutation adapter and the
  core2 `PersistedFrontendDomainManager` API.
- Exact-head run [41197133481](https://github.com/auenkr/cadence-rust/actions/runs/41197133481)
  failed Clippy because launcher setup still referenced the removed
  `GoSqlFrontendDomainManager` symbol. Removed that obsolete assignment; the
  configured `PersistedFrontendDomainManager` assignment remains the one
  Frontend production path.
- Fixed combined Frontend+Matching mode startup to load durable domain metadata
  into the Frontend's shared domain cache whenever Frontend is enabled. It was
  previously skipped when Matching ran in the same process, leaving the
  persisted manager's cache empty at startup. This also refreshes execution
  domain aliases from the loaded metadata.
- `git diff --check` passes. No local Cargo/rustfmt/tests were run. Push this
  rebased branch and dispatch package CI at its exact resulting head.
- Remaining blockers are unchanged: persistence-backed replication queue and
  durable failover history, the rest of Admin DLQ/raw-history RPCs, scheduler
  workflow RPC wiring, real membership, and exact-head Go/Rust mixed-cluster
  failure/restart/recovery validation.

## Startup cache fix exact-head validation and SQLite domain-writer adapter — 2026-10-31

- Rebased onto `origin/frontend-core2` tip `a84001cf7`, fixed the stale
  `GoSqlFrontendDomainManager` startup reference, and loaded durable domain
  metadata in combined Frontend+Matching mode. Exact-head package CI
  [41197558733](https://github.com/auenkr/cadence-rust/actions/runs/41197558733)
  completed **success** at `c814e54cb1a2eb176901d00adc279a9f146597bd`.
- Core2 then advanced through SQLite domain-transaction restart validation to
  `3019e49c2`. Rebased again onto that current tip; the rebase replayed 102
  Frontend commits without conflicts.
- SQLite's configured Go-schema domain store already supported create/update/
  delete transactions through `GoSqlDomainPersistence`, but Frontend startup
  did not expose it through the current `DomainMetadataPersistence` manager
  contract. Added the adapter and configured SQLite to provide it, retaining
  expected metadata-version checks and the existing transaction semantics.
  Expanded the SQLite transaction test to exercise the new adapter boundary.
- `git diff --check` passes. No local Cargo commands were run. Commit and push
  this adapter slice next, then run package CI at that pushed exact head.

## SQLite expected-version transaction tightening — 2026-10-31

- While package CI [41198020996](https://github.com/auenkr/cadence-rust/actions/runs/41198020996)
  was running for `0d6ee8c68c1940058cf8592d6cb59ffde622cce9`, reviewed the new
  SQLite trait adapter's create path. Calling the older `GoSqlDomainPersistence`
  method would read its own metadata version and could commit a row before the
  adapter noticed that the Frontend manager's supplied expected version was
  stale.
- Factored the SQLite create transaction to accept the expected version and
  check/update the singleton metadata row inside the same transaction. Both
  existing Go SQL calls and the Frontend mutation trait now use that method.
  This preserves rollback on stale writes. `git diff --check` passes; no local
  Cargo command was run. This fix needs its own exact-head package CI run.
- The first SQLite-adapter package run [41198020996](https://github.com/auenkr/cadence-rust/actions/runs/41198020996)
  was cancelled when a follow-up correctness fix was pushed. The exact-head
  replacement [41198107736](https://github.com/auenkr/cadence-rust/actions/runs/41198107736)
  completed **success** at
  `17ece2f9f55711d94a430adb635eec1f4a036d1c` (Frontend package Clippy,
  formatting, tests and smoke checks).

## SQLite stale-create rollback regression — 2026-10-31

- Added a SQLite regression case that passes a stale expected metadata version
  to the new Frontend mutation trait and asserts that the transaction leaves no
  inserted domain row. This directly guards the rollback behavior reviewed in
  the preceding adapter fix. `git diff --check` passes; no Cargo command was run
  locally. The change is pending exact-head package CI.
- The stale-create rollback regression at `d9e224920c536e11ad766e011e553685ad9baa4d`
  passed exact-head Frontend package CI:
  [41198466298](https://github.com/auenkr/cadence-rust/actions/runs/41198466298)
  completed **success** (Clippy, Formatting and Test jobs).

## Persisted domain manager CRUD routing — 2026-10-31

- Confirmed the rebased source already passes `serialization_encoding` to
  `CassandraDomainMetadataPersistence::new`; the checkpoint's suspected missing
  argument was stale and required no code repair.
- Pushed rebased head `b2db05612ae902cb6b0e44d0be63e7a98895c27c` over the old
  remote branch with an explicit lease. Exact-head Frontend package CI run
  [41199029247](https://github.com/auenkr/cadence-rust/actions/runs/41199029247)
  was dispatched; it was in progress when checked and does not validate any
  later source changes.
- Routed `cadence-frontend::handler_domain` register, describe, list, update,
  and deprecate operations through `AppState.domain_manager`. The persisted
  manager now owns these calls and can commit writes through its configured SQL
  or Cassandra adapter; errors map back to the handler's typed error surface.
  This helper still uses the default in-memory implementation in its unit test;
  production transport dispatch was separately confirmed to use `domain_manager`
  in `cadence-service-core`.
- `git diff --check` passed for this uncommitted CRUD routing slice. No local
  Cargo command ran. Next: commit/push this slice and dispatch exact-head package
  CI. Remaining blockers: durable domain replication queue and durable failover
  history/cooldown behavior; Admin DLQ and complete raw-history RPC behavior;
  scheduler workflow integration; live membership; and mixed-cluster
  Go/Rust failure/restart/recovery validation. These are still open.
- Follow-up CI found the rebase ownership issue missed by source inspection:
  run [41199029247](https://github.com/auenkr/cadence-rust/actions/runs/41199029247)
  failed Clippy/type-check at `matching_task_store.rs` because the Cassandra
  driver `Arc` was moved into the metadata store before constructing the new
  domain persistence adapter. The constructor argument itself was present.
  Changed the first use to `driver.clone()`; dispatch fresh CI at the resulting
  commit, because run [41199231977](https://github.com/auenkr/cadence-rust/actions/runs/41199231977)
  was already running against the pre-fix head `6e64bc87014f012d9dcdd06032600913b8739c4a`.
- Corrected source commit `014526620ae4d5983e62148e0df23c707d5ded4f` passed
  exact-head Frontend package CI:
  [41199384065](https://github.com/auenkr/cadence-rust/actions/runs/41199384065)
  completed `success` (Clippy, Formatting, Test and Frontend runtime smoke).
  This validates the configured domain persistence integration and current
  CRUD routing source, but does not close the parity blockers listed above.

## Core2 refresh after domain CRUD CI — 2026-10-31

- `origin/frontend-core2` advanced to `f0736ac8c` and was not an ancestor of the
  domain-admin branch. Rebased all 110 feature commits onto that fetched tip;
  Git reported a clean replay with no conflicts. This rebase changes commit IDs,
  so force-with-lease push and exact-head package CI are required before using
  prior CI as current evidence. The last successful pre-rebase source head was
  `014526620ae4d5983e62148e0df23c707d5ded4f` (run 41199384065).

- Continued onto fetched `origin/frontend-core2` tip `0a5008ffa` after it
  advanced through `f0736ac8c`. The 110 Frontend commits replayed cleanly;
  rebase result before this evidence update: `72bae2b688438b3e797d69696357be62a1648035`.
  Prior CI is stale after the rebase. Push with force-with-lease and dispatch
  exact-head Frontend package CI before relying on this source.
- Current concrete replication gap: persisted Domain CRUD is routed through
  SQL/Cassandra metadata writers, but mutations do not publish Go-compatible
  domain tasks, the service has no queue manager, and Admin `GetDomainReplicationMessages`
  still returns Go's disabled-queue error unconditionally. Existing failover
  audit rows and queue persistence also are not connected to the durable
  manager. I am auditing the Go queue/read contracts before implementing the
  next bounded queue slice; no parity claim is made.
- Exact-head run [41200242121](https://github.com/auenkr/cadence-rust/actions/runs/41200242121)
  completed **success** at `87935eb960ab4312368addd4cca837a27c83f5a5` after
  the core2 rebase (Frontend Clippy, formatting, tests and runtime smoke).
- The queue audit confirmed Go's producer writes ThriftRW-encoded
  `ReplicationTask` payloads into the shared persistence queue and Admin reads
  from the retrieved cursor, consults per-cluster ack levels only for the
  default cursor, and treats ack updates as warning-only. Rust does not yet
  have the compatible replication task codec or a configured queue manager;
  replacing the disabled-queue response with an in-memory queue would not
  satisfy the durable or wire-compatible requirement.
- Started the next queue slice by adding the Go numeric enums and typed domain
  replication task envelope to `cadence-api`, preserving Go JSON field names
  and omitting zero scalar fields. Added a focused field/enum contract test.
  Added the `DomainRecord` to task mapping for create/update/delete operations;
  it carries the persisted domain snapshot and Go config/failover versions,
  including isolation and async-workflow config fields. Added a persistence
  unit case for the mapped versions. This is only the model layer; ThriftRW
  encoding, durable SQL/Cassandra queue operations, publication on domain
  mutations, and Admin queue reads remain open. `git diff --check` passed; no
  local Cargo command ran. Run 41200689933 tests only the API model at
  `6594677941446b80bc8ed38ff14a19896ee6c460`; the expanded task mapping needs
  fresh exact-head CI.
- Added the Thrift converter for the domain replication task envelope using
  Go's field IDs 10-80, including explicitly present zero-valued version
  fields. DomainConfiguration now carries Go's isolation-group (field 60) and
  async-workflow (field 120) settings through its Thrift converter. Added a
  converter test checking the exact field IDs and values. This produces the
  generic Thrift value model; ThriftRW byte encoding, queue persistence and
  producer/consumer wiring remain open. No local Cargo command ran. The
  expanded source needs exact-head package CI.
- Added `DomainRecord::encode_domain_replication_task`, which writes the Go
  domain task envelope through the existing ThriftRW `Writer` path, and a
  decode-side structural assertion for task type, nested attributes and full
  byte consumption. This establishes the queue payload encoder; no Go-produced
  byte-for-byte oracle exists yet. Queue persistence, mutation publication,
  Admin read/ack behavior, and DLQ operations remain open. `git diff --check`
  passed and no Cargo ran locally; this needs fresh exact-head CI.

## Rebase onto frontend-core2 and queue continuation — 2026-10-31

- Fetched `origin/frontend-core2` at `f6d20834f` and rebased the 116 Frontend commits cleanly. Pushed rewritten branch head `478bdd1ec91e37270933613fe61b7937865e7f3d` with force-with-lease.
- Previous run [41201041499](https://github.com/auenkr/cadence-rust/actions/runs/41201041499) was cancelled during the rebase and targeted the stale pre-rebase SHA `c6e5c9110a8d16157b53f145b069536b64dea586`.
- Dispatched exact-head Frontend package CI [41201182327](https://github.com/auenkr/cadence-rust/actions/runs/41201182327), targeting `478bdd1ec91e37270933613fe61b7937865e7f3d`; it was still in progress at this checkpoint. No local Cargo commands were run.
- The task encoder commit contains Go field IDs and ThriftRW structure support. Open blocker: no persistence-backed QueueStore integration. Go's shared queue schema uses `queue` and `queue_metadata`; SQL assigns the next ID as `max(last message ID, all ack levels)+1`, and Cassandra uses the same logical queue type with LWT allocation. Domain mutation publication and Admin queue/DLQ consumers are still not wired.
- Remaining blockers: SQL/PostgreSQL/Cassandra durable replication queue plus producer/consumer/ack semantics; domain cooldown/failover audit persistence; Admin DLQ and full raw-history RPC behavior; scheduler RPC calls to scheduler workflows; runtime membership; parity audit and mixed Go/Rust failure, restart, recovery evidence. No parity claim is made.

## Queue SQL implementation slice and rebase Clippy repair — 2026-10-31

- Exact-head CI [41201182327](https://github.com/auenkr/cadence-rust/actions/runs/41201182327) failed on `478bdd1ec91e37270933613fe61b7937865e7f3d` before Frontend tests. The diagnostic was `DomainConfiguration: Eq` requiring `AsyncWorkflowConfiguration: Eq`, exposed after the rebase while compiling `cadence-api`.
- Added `Eq` to `AsyncWorkflowConfiguration` (all constituent fields already support equality).
- Added the first MySQL durable domain replication queue adapter on the Go shared `queue` table: queue type 1, transactional row/ack lock ordering, ack-aware monotonic ID allocation, ordered cursor reads, and a helper test for Go's `max(lastID, ackLevels)+1` rule. This is persistence support only; it is not yet connected to domain mutation publication, Admin fetch/ack, or DLQ operations, and PostgreSQL/Cassandra adapters remain open.
- `git diff --check` passes. No local Cargo command was run. Queue slice and derive repair need a new exact-head package CI run after commit/push.
- Continued queue implementation with PostgreSQL using the same queue type, transaction locks, ack-aware message ID allocation, shared `queue` rows, and ordered cursor reads. This PostgreSQL addition is pending exact-head package CI; the MySQL/derive CI [41201498839](https://github.com/auenkr/cadence-rust/actions/runs/41201498839) was still in progress at the time of this edit.
- Extended SQL queue adapters with durable ack-level read/update behavior, matching Go's monotonic ack update (ignore delayed values) and `queue_metadata` JSON map. Implemented for both MySQL and PostgreSQL. `git diff --check` passes. Exact-head CI [41201567799](https://github.com/auenkr/cadence-rust/actions/runs/41201567799) for prior head `bb8c8009d2f686e092a8d74381080edf22548c93` remained in progress; the new ack changes require a later exact-head run. Queue RPC and mutation publication wiring remain open.
- Implemented Cassandra queue type 1 operations against Go's shared tables: initialize/read queue metadata, choose an ack-aware message ID, LWT insert, cursor reads, monotonic ack CAS updates, and ack-level reads. The Cassandra adapter follows Go's current condition-failed behavior for concurrent inserts/ack updates. These backend methods are still not connected to the configured Frontend manager or Admin RPCs. No local Cargo was run; current pushed exact-head CI is [41201629295](https://github.com/auenkr/cadence-rust/actions/runs/41201629295) targeting `c85fca6e56644819812da15854ff4eea6812c4b1` and was in progress at this note.
- Added a shared queue persistence contract and bound the MySQL, PostgreSQL, and Cassandra Go-schema adapters to it. `PersistedFrontendDomainManager` now publishes encoded create/update/delete tasks after successful persistence mutations, only for global domains; failover/update and deprecation publish Update tasks. This mirrors Go's post-mutation transmission call and its error propagation. This producer connection is not yet CI-verified or wired to Admin reads. No local Cargo command ran; the latest pushed CI [41201734019](https://github.com/auenkr/cadence-rust/actions/runs/41201734019) targets `995c71ec50b14d0dff671c7027106bfd923d676d` and was still in progress before these edits.
- Source audit of `go/idls/thrift/replicator.thrift` found the new task envelope converter used incorrect wrapper field IDs (it placed sourceTaskId/domainTaskAttributes at 20/30). Corrected them to the Go IDL IDs 11/20 and added ThriftRW decoding for domain replication tasks and an exact field-ID round trip assertion. This correction is necessary for SQL/Cassandra queue interoperability. `git diff --check` passes; CI has not yet validated it.
- Added the Go-shaped `ReplicationMessages`/`GetDomainReplicationMessagesResponse` API model and Thrift converters, then replaced the disabled queue response with durable cursor read, ThriftRW decode, ack-based default cursor selection, and warning-only ack update across JSON and YARPC handler paths. Queue reads currently return `hasMore=false` and use a fixed batch of 100, matching the Go constant; only domain task payloads are modeled. Full DLQ support and exact Go decoder/error coverage remain open. This increment has not been CI-validated; no local Cargo commands were run.

## Rebase to current core2 and raw-history cursor repair — 2026-10-31

- Fetched and rebased the 124 Frontend commits onto current `origin/frontend-core2`
  at `4821a2347a27018dcf28a311e0d4ce1560392099`. The rebase completed without
  conflicts. Force-pushed branch head `6842527a0cdf012a8e8e97bbc9028a1ef9bc6159`.
- Replaced stale pre-rebase run [41202143737](https://github.com/auenkr/cadence-rust/actions/runs/41202143737)
  with [41202217880](https://github.com/auenkr/cadence-rust/actions/runs/41202217880)
  for exact head `6842527a0cdf012a8e8e97bbc9028a1ef9bc6159`; it was in progress
  at the last check.
- That run was cancelled by the later push. Run [41202398549](https://github.com/auenkr/cadence-rust/actions/runs/41202398549)
  targeted `e5dd8a57fc60697babda927d2279550191342470` and failed before Cargo
  at `Check changed Matching runtime formatting`, reporting rustfmt changes in
  `service-core/src/admin.rs` around the domain queue response. Applied the
  CI diff below. No Rust compilation ran in that attempt; redispatch after
  pushing the formatting repair.
- Continued the raw-history pagination audit against Go's `ReadRawHistoryBranch`
  contract. Rust's in-memory execution store treated any malformed internal
  cursor as offset zero, silently replaying the first page. Added validation
  before the read and cases for valid, malformed, negative and non-UTF-8 cursor
  bytes. This preserves valid cursors while reporting `Invalid pagination
  token.` rather than duplicating history. Source change is pending exact-head
  CI; no local Cargo command or test was run.
- Open blockers: durable domain DLQ operations and Admin RPC wiring; raw-history
  parity for all branch/token encodings and Go History-backed reads; schedule
  handlers still mutate Rust schedule persistence rather than scheduler
  workflows; `DescribeCluster` still reports a static single-host membership
  view; full Admin RPC authorization, dynamic-config backing, metrics/retries,
  and mixed Go/Rust failure, restart and recovery validation. No Frontend parity
  claim is made.
- The formatting repair was pushed as `12389c226581dc880168ef95c180c5ffd6a21812`.
  Its CI run [41202688205](https://github.com/auenkr/cadence-rust/actions/runs/41202688205)
  passed formatting but Clippy found three persistence compile errors exposed
  by the core2 rebase: missing `DomainReplicationQueueMessage` import in MySQL,
  missing newly added isolation/async config fields in SQL metadata decoding,
  and ambiguous Cassandra `HashMap` key/value types. All three source repairs
  are now applied and need exact-head CI. The run did not reach Rust tests.
- Exact-head run [41203031658](https://github.com/auenkr/cadence-rust/actions/runs/41203031658)
  targeted `b5273a34e5bba6844ac1b637717ffaeb3d46fcb2`. Formatting passed and
  persistence compiled, but Clippy failed because YARPC's newly wired domain
  replication route lacked `FromThrift` for its request model. Added the Go
  field-ID decoder (10/20/30 from `replicator.thrift`) and a focused converter
  case. This needs another exact-head CI run; the failed run did not reach Rust
  tests.
- Exact-head run [41203276647](https://github.com/auenkr/cadence-rust/actions/runs/41203276647)
  at `f49290722da9a7cd7b1fdc9664ff1a74b4a37f0d` compiled through
  `cadence-service-core`, then failed Clippy because the domain queue read's
  three-arm `Result` match should use `?`. Replaced it with the equivalent
  propagation. The next exact-head run is required; no Rust tests ran.
- Exact-head Frontend package run [41203546921](https://github.com/auenkr/cadence-rust/actions/runs/41203546921)
  completed **success** at `0919ea858351fd6870165459d1892e4d6462e4af` after the
  fixes above. Frontend Clippy, formatting, tests and runtime smoke passed. The
  package selection compiles the changed service-core/thrift dependencies, but
  does not run their own unit-test suites; dispatch focused `cadence-thrift` and
  `cadence-persistence` package runs for the new conversion and cursor tests.
- Thrift package run [41203933467](https://github.com/auenkr/cadence-rust/actions/runs/41203933467)
  failed Clippy test compilation at documentation head
  `f7deb0b709fed8ec6193a1dcd38bbd3a46af6781`. Existing Admin conversion tests
  revealed missing `ToThrift` implementations for eight Admin request types
  (isolation groups, async-workflow configuration, ReapplyEvents and corrupt
  workflow maintenance). Added encoders using the IDL field IDs. Dispatch
  focused Thrift CI for the updated source; that run did not reach tests.
- Follow-up [41204166761](https://github.com/auenkr/cadence-rust/actions/runs/41204166761)
  reached Thrift test compilation at head `3e984033c0ff09ffd1b4dac9d1d1376f0ffaf186`
  and failed Clippy on a now-redundant `..Default::default()` in the domain
  replication task fixture. Removed that struct update; rerun the focused
  package on the new head.

## Rebased durable domain DLQ slice — 2026-10-31

- Rebased the Frontend implementation onto `origin/frontend-core2` at
  `b48503c82adec46b89cdd4091d051d134df33beb`. The rebase completed; where
  early Frontend commits duplicated newer core2 handlers, clients, converters,
  and lifecycle code, retained core2's more complete versions and kept the
  later RPC wiring changes. Existing domain, Admin and schedule handlers remain
  in the merged tree.
- Added domain DLQ persistence operations for MySQL, PostgreSQL and Cassandra,
  with Admin JSON count/read/purge handlers using the configured persistence
  backend. Rebased implementation commit is
  `051f216181e2216288fb622d39f9e8c8e639ca69`.
- `git diff --check` passes. No local Cargo command was run. Fresh exact-head
  Frontend CI is required after push; previous CI results do not validate this
  rebased head.
- Active blockers remain: Go-compatible DLQ ack-level and retry/move semantics,
  task execution for DLQ merge, History Admin client-backed operations, complete
  raw-history branch pagination, schedule workflow orchestration, live
  membership, domain update cooldown and failover audit, Admin backing stores,
  and mixed Go/Rust failure/restart/recovery acceptance. This slice does not
  claim Frontend parity.

## DLQ acknowledgement boundaries — 2026-10-31

- Added dedicated DLQ acknowledgement-level persistence for MySQL,
  PostgreSQL, and Cassandra using Go's `queue_metadata` row for queue type -1.
  Domain DLQ reads now begin at the stored `domainReplication` ack level (or a
  supplied page cursor); purge deletes `(ackLevel, inclusiveEnd]`, then advances
  the ack level while preserving Go's warning-only update failure behavior.
  Default inclusive end now uses Go's `math.MaxInt64 - 1` sentinel. Added
  little-endian token regression coverage.
- Exact-head package CI run
  [41206134407](https://github.com/auenkr/cadence-rust/actions/runs/41206134407)
  is still running against `8a1dd43aed112ff951c9c949180079f0347387c1` and does
  not validate this added ack-level slice. No local Cargo command was run.

## Integration head CI formatting repair — 2026-10-31

- Integration head `bdf92466121dc8d3bdd87e0fa543378d05e25a88` was checked by
  exact-head package run [41206837226](https://github.com/auenkr/cadence-rust/actions/runs/41206837226).
  It failed before Cargo at the changed Matching runtime formatting check:
  `cadence-service-core/src/admin.rs` needed the DLQ ack update call split
  across lines. Applied that formatting-only repair. No cargo ran locally.
- `git diff --check` passed. Dispatch a fresh `cadence-frontend` package run
  for the pushed repair; the earlier run does not validate it.
- Formatting repair commit `7e9439c56a1fef7a67e49c807234ec4badb3480e` was checked by
  exact-head run [41207071832](https://github.com/auenkr/cadence-rust/actions/runs/41207071832).
  Formatting passed, but Clippy compilation failed because a merge left a
  duplicate `GoSqlDomainPersistence for MySqlDomainMetadataStore` impl at lines
  675 and 836. Removed the duplicate; the earlier implementation is retained
  alongside the Frontend `DomainMetadataPersistence` impl. No local Cargo ran.
  This repair needs fresh package CI.
