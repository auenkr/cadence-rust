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
