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
