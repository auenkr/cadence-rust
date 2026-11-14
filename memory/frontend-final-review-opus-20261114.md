# Frontend final parity review (Opus 5.5) — 2026-11-14

This is a read-only review of the Rust Frontend (`rust/crates/cadence-frontend` plus the shared `cadence-service-core` dispatch) against `go/service/frontend`. No cargo, go or docker was run.

Base: origin/master `625371ea9`.

Paths: `R:` is `rust/crates/`, `G:` is `go/service/frontend/`.

## Key structural fact

The live binary is `cadence_frontend::run` (`R:cadence-frontend/src/lib.rs:45`). It calls `run_frontend_with_hook(... build_workflow_api)`. Only the methods listed in `R:cadence-service-core/src/workflow_api.rs` go through the ported `WorkflowHandler` and `RequestValidator` in `cadence-frontend`: start, signal, poll, respond, query, visibility, schedules and task lists.

Domain RPCs (Register, Update, Deprecate, Delete, Failover), CreateSemaphore and every AdminService RPC are served by the hand-written handlers in `R:cadence-service-core/src/yarpc.rs` (dispatch at ~2860-2950). Those handlers do **not** call `cadence-frontend/src/request_validator.rs`. As a result, Go-ported domain checks such as lockdown, admin protection and max retention exist in the crate but are dead on the live path. Most findings below come from this split.

## Findings (most severe first)

### HIGH-1: UpdateDomain skips Go validation (max retention, security token, lockdown) — CONFIRMED
- Rust: `R:cadence-service-core/src/yarpc.rs:563-592`. It only checks that the name is non-empty, then calls `domain_manager.update`.
- Go: `G:api/request_validator.go:324-347` (`ValidateUpdateDomainRequest`).
- Differences:
  - Retention above `system.maxRetentionDays` is not rejected. Go returns BadRequest "RetentionDays is invalid." (`G:validate/errors.go:49`).
  - When `history.enableAdminProtection` is on, a non-failover update with a missing or wrong token is not rejected. Go returns "No permission to do this operation.".
  - A failover update during `system.Lockdown` is not rejected. Go returns "Domain is not accepting fail overs at this time due to lockdown.".
- Fix: in `handle_update_domain`, call the existing `RequestValidator::validate_update_domain_request` (`R:cadence-frontend/src/request_validator.rs` ~480-500/626) before `update()`. Alternatively, inject it into service-core through the hook context.

### HIGH-2: FailoverDomain skips max-timeout and lockdown checks — CONFIRMED
- Rust: `R:cadence-service-core/src/yarpc.rs:1608-1636` uses `cadence_api::validation::validate_failover_domain_request` (`R:cadence-api/src/validation.rs:434-457`).
- Go: `G:api/request_validator.go:369-391`.
- Differences:
  - A timeout above `MaxFailoverTimeoutInSeconds` is accepted. Go returns "FailoverTimeoutInSeconds %d exceeds the maximum allowed %d".
  - Lockdown is not checked (`checkFailOverPermission`).
- The correct implementation already exists, unused, in `R:cadence-frontend/src/request_validator.rs:600-633`.
- Fix: route through `RequestValidator::validate_failover_domain_request`.

### HIGH-3: Graceful failover does not check for an ongoing failover across clusters — CONFIRMED
- Rust: neither `handle_update_domain` (563) nor `handle_failover_domain` (1608) has this check. Searching for "Concurrent failover" finds nothing in rust/.
- Go: `G:api/handler.go:3117-3159` (`checkOngoingFailover`), called at `G:api/domain_handlers.go:132` (UpdateDomain) and `:239` (FailoverDomain) when the request is graceful.
- Difference: Go calls DescribeDomain on every enabled cluster and returns:
  - InternalServiceError "Failed to verify failover version from all clusters" if any cluster gives no response;
  - BadRequest "Concurrent failover is not allowed" if the versions differ.
  Rust starts the graceful failover anyway. Go also emits the graceful-failover initiation-failure metric (reason tags `validation` and `ongoing_failover_check`); Rust has no equivalent.
- Fix: port `checkOngoingFailover` using the remote frontend clients (`frontend_remote_grpc.rs`), gated on `isGraceFailoverRequest`. Port the initiation-failure metric too.

### HIGH-4: AdminService.UpdateTaskListPartitionConfig is not served — CONFIRMED
- Rust: missing from the dispatch match in `R:cadence-service-core/src/yarpc.rs` (~2860-2950). No frontend file mentions it, so it falls to `unknown method`. The gRPC side has no mapping either.
- Go: `G:admin/handler.go:1919-1960`. It validates in this order: request, domain → ID, TaskList, Kind ("Task list kind not set."), Kind == Normal, TaskListType, PartitionConfig, write ≤ read, write > 0. Then it calls matching.
- Impact: the CLI command `cadence admin tasklist update-partition` breaks.
- Fix: add a handler with Go's exact validation messages and forward to the Rust matching client (`MatchingUpdateTaskListPartitionConfigRequest`). The matching side already implements it.

### HIGH-5: AdminService.CountDLQMessages is not served — CONFIRMED
- Rust: not dispatched. `HistoryProxy::count_dlq_messages` exists in `R:cadence-frontend/src/handler_admin_history.rs:131` but nothing outside that file references `HistoryProxy`.
- Go: `G:admin/handler.go:1177-1200`. It combines `domainDLQHandler.Count(ForceFetch)` (Domain) with the history `CountDLQMessages` entries (History). On a history error it still returns the domain count together with the error.
- Impact: `cadence admin dlq count` breaks.
- Fix: add a dispatch arm that merges the domain DLQ count with the history count, as Go does.

### MEDIUM-1: RegisterDomain skips max retention, required domain data keys and security token — CONFIRMED
- Rust: `R:cadence-service-core/src/yarpc.rs:281-~330`. It checks the length limit and min retention only.
- Go: `G:api/request_validator.go:285-312`.
- Differences, in Go's order:
  - retention above maxRetentionDays → "RetentionDays is invalid.";
  - `checkRequiredDomainDataKVs` (`system.requiredDomainDataKeys`);
  - `CheckPermission(SecurityToken)`.
- Also, Rust applies the min-retention check ("A valid retention period is not set on request.") here. In Go that check lives in the domain handler, so it runs only after these validator errors. When several rules fail, the error order may differ.
- Fix: call `RequestValidator::validate_register_domain_request` first.

### MEDIUM-2: DeleteDomain and DeprecateDomain skip the CheckPermission security-token check — CONFIRMED
- Rust: `R:cadence-service-core/src/yarpc.rs:192` (delete) and `:594` (deprecate). Searching both for token/permission finds nothing.
- Go: `G:api/request_validator.go:349-367`.
- Difference: with admin protection on, Go rejects with "No permission to do this operation."; Rust deletes or deprecates.
- Fix: as for HIGH-1.

### LOW-1: Admin history proxy turns every History error into InternalServiceError — CONFIRMED (dead code today)
- Rust: `R:cadence-frontend/src/handler_admin_history.rs:27-29` (`internal()`).
- Go: `adh.error(err, scope)` passes typed errors through (EntityNotExists, ShardOwnershipLost, BadRequest).
- Fix: map the tonic status and details to the typed Cadence error before wiring this into HIGH-5. Otherwise, delete the module.

### LOW-2: Ported RequestValidator domain logic is not on the live path — CONFIRMED
- The domain-validation functions in `R:cadence-frontend/src/request_validator.rs` are only exercised by unit tests. Passing tests there give false parity confidence for HIGH-1/2 and MEDIUM-1/2.
- Fix: wire the validator into service-core (for example a `DomainValidator` trait object on `AppState`, set by `build_workflow_api`). Then add mixed-gate cases that hit the domain RPCs with admin protection and lockdown enabled.

## Checked with no finding (string-level sweep)
- I extracted all 150 Go error-message literals in `G:api`, `G:admin` and `G:validate` and grepped rust/ for each.
- The only non-test literals missing from rust/ belong to the findings above:
  - "Concurrent failover…" and "Failed to verify failover…" → HIGH-3;
  - the partition-config messages → HIGH-4.
- "ES related config not found" and "operational dynamic config store is not available…" are backend-specific. Rust always has an overlay store, so behaviour differs only on backends where Go would return BadRequest. PLAUSIBLE, low.
- Visibility, list and task-list validators and schedule messages are present in Rust.
- The ByID activity RPCs and GetClusterInfo are served through `workflow_api.rs`.

## Not covered in depth
- Per-RPC metric tag names and gRPC field mapping were not compared line by line. The metrics_sink, ratelimited and accesscontrolled method tables do include every RPC name.
