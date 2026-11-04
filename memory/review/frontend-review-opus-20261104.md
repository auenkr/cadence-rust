# Frontend review (Opus 5.5, read-only) — master 3ace896eb, 2026-11-04

Scope was limited (weak host, grep/read only). It was a targeted spot check, not an exhaustive audit.

## Findings

### O1 — medium — StartWorkflow/SignalWithStart RequestID UUID check is stricter than Go
- Rust: rust/crates/cadence-api/src/validation.rs:119-130 (`is_valid_uuid`), used at :186
- Go: go/service/frontend/api/handler.go:1706 (`uuid.Parse(startRequest.RequestID)`, google/uuid)
- Difference: google/uuid `Parse` accepts 4 forms: canonical 36-char, `urn:uuid:` + canonical (45), `{canonical}` (38) and 32 hex chars with no dashes. Rust accepts only the canonical 8-4-4-4-12 form.
- Failure: a client (custom SDK, CLI script, proxy) that sends a dash-less or braced request id works on Go but gets `BadRequestError "requestId \"...\" is not a valid UUID"` from Rust.
- Fix: accept the same 4 forms as google/uuid (lengths 32/36/38/45, with the same dash positions and prefix/brace checks), or use the `uuid` crate's `Uuid::try_parse`, which accepts the same set.

## Areas checked and found correct
- All Go `validate/errors.go` BadRequest messages exist verbatim in Rust (frontend request_validator, cadence-api validation, service-core admin for ErrInvalidFilters).
- StartWorkflowExecution validation order and conditions match Go handler.go:1696-1800: domain, workflowId, requestId UUID, workflowType, domain/workflowId length, retry policy, workflowType length, tasklist set/length, timeouts <= 0, delay/jitter < 0, cron schedule parse, jitter vs cron interval computed with zero start/end times, requestId length.
- validateTaskList (nil/empty name, then length) matches Go handler.go:2911.
- Identity / signal / domain length checks use `len <= limit` like Go `common.IsValidIDLength`.
- Admin protection check (history.enableAdminProtection + adminOperationToken) matches Go.
- GetWorkflowExecutionHistory page size defaults to frontend.historyMaxPageSize (domain filter, default 1000) when <= 0.
- ES page-size limit check ("Pagesize is larger than allow N") is gated on advanced visibility and the domain read store.

## Not covered (recommend another pass)
Thrift/gRPC mapper enum/units, domain handler update/failover paths, cluster redirection policies, global ratelimit, ES query translation, archival readers.
