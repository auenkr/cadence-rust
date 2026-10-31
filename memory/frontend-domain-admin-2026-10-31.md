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
- The new source after head `ae651ac85b852f9386b6b84021bfd6b187978bca` has
  not yet had a fresh CI result. Mixed Go/Rust failure, restart and recovery
  validation has not been run.
