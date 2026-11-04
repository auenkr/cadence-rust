# Frontend review (Opus, part 4) — 2026-11-04, master 3ace896eb

Read-only review. Scope: domain handler, global ratelimiter, ES query translation, archive readers.

## Findings

### O30 — HIGH — Failover never sets FailoverNotificationVersion
- Rust: rust/crates/cadence-persistence/src/domain_update.rs:342-354 (failover branch of `prepare_inner`); no assignment to `failover_notification_version` anywhere outside tests (grep). Persisted via domains.rs:155.
- Go: go/common/domain/handler.go:590 (`intendedDomainState.FailoverNotificationVersion = notificationVersion`), also FailoverDomain path (~1016+).
- Differs: Rust keeps the old value (0 on new domains) on every force/graceful failover.
- Failure: Go History's domain-change callback / graceful-failover dedup keys on `FailoverNotificationVersion >= shard notification version`; with the value stuck, Go History shards do not run failover processing (transfer/timer queue failover, pending-active handling) after a failover issued by the Rust Frontend. Rust matching's own watcher (domains.rs:594) likewise never sees a bump.
- Fix: in the failover branch set `record.failover_notification_version = <current metadata notification_version>` (the version read in `update`/`failover` before persist), matching Go.

### O31 — MEDIUM — RegisterDomain ignores cluster archival defaults and URI validation
- Rust: rust/crates/cadence-frontend/src/handler_domain.rs:86-95 copies request archival status/URI verbatim.
- Go: handler.go:212-250 (`toArchivalRegisterEvent` with `GetDomainDefaultStatus/URI`, `getNextState`, `validateHistory/VisibilityArchivalURI`) only when `ClusterConfiguredForArchival()`.
- Failure: cluster with `domainDefaults.archival.history.status: enabled` + default URI: Go registers domains with archival enabled at the default URI; Rust registers them disabled/empty → no archival. Invalid scheme URIs are accepted by Rust; archival fields set when cluster archival is disabled are persisted (Go drops them).
- Fix: reuse `cadence_common::archival_state::domain_archival_policy()` (already used in update) with a register event.

### O32 — MEDIUM — Active-active register keeps client failover versions
- Rust: handler_domain.rs:68-71 stores `request.active_clusters` as-is; 364 rejects negative versions.
- Go: handler.go:1829-1868 `activeClustersFromRegisterRequest` rewrites every attribute's FailoverVersion to the cluster's `InitialFailoverVersion`, errors "Cluster %v not found. Domain cannot be registered..." for unknown clusters, returns nil for local domains / empty scopes.
- Failure: client sends versions 0 (default) → domain-level attribute versions differ from Go (wrong active cluster resolution across clusters); local domain with active_clusters persists them.
- Fix: port `activeClustersFromRegisterRequest`.

### O33 — LOW — RegisterDomain swallows non-NotFound GetDomain errors
- Rust handler_domain.rs:36-47 treats any describe error as "not exists". Go handler.go:179-189 returns other errors. Persistence/describe faults → Rust proceeds to create. Fix: only continue on NotFound.

### O34 — MEDIUM (PLAUSIBLE) — Archived history read with MaximumPageSize 0 fails
- Rust: archive_readers.rs:283-287 passes raw page size (0 when unset); cloud_archiver.rs:237-241 rejects page_size==0 as invalid get request. No defaulting of `maximumPageSize` found in handler_workflow.rs before the archival branch (1093-1111).
- Go: service/frontend/api/handler.go:1864-1875 defaults to HistoryMaxPageSize and caps before `getArchivedHistory` (1882).
- Failure: Go SDK GetWorkflowHistory (no page size) on an archived run → BadRequest "Get history request is invalid" instead of history.
- Fix: apply Go default/cap before the archival path (verify validate_request doesn't already).

### O35 — MEDIUM — ES search_after value not type-normalised
- Rust: es_query.rs:978-980 `search_after = [token.sort_value, token.tie_breaker]` raw.
- Go: es_visibility_store.go:771-808 `getValueOfSearchAfterInJSON`: Int/Datetime/Bool → int64 with overflow clamp to MinInt64/MaxInt64 (ES returns ±9223372036854776000 for missing field), Double "±Infinity" strings, Keyword nil → null.
- Failure: list sorted by an int/datetime custom attribute where some docs lack it → next page sends 9.223372036854776e18 → ES parse error / wrong paging.
- Fix: port the per-type conversion using attr_types.

### O36 — LOW — ES sort error text differs
- Rust es_query.rs:965 "Not able to sort by IndexedValueTypeString field..."; Go es_visibility_store.go:752 "not able to sort ...". Client-visible message mismatch. Fix: lowercase.

### O37 — LOW — Domain filter added even for empty domain ID
- Rust es_query.rs:879-882 always adds `match_phrase DomainID`; Go addDomainToQuery (es_visibility_store.go ~700) skips when empty. Only affects internal/admin scans with empty domain.

### O38 — LOW — Global ratelimiter primary burst fractional
- Rust global_ratelimit.rs:217-219,233 burst = `rps.max(1.0)` (f64). Go fallback.go:180 `SetBurst(max(1, int(lim)))` truncates. E.g. rps 2.9 → Go burst 2, Rust 2.9 tokens; slightly more permissive bursts. Fix: `(rps as i64).max(1) as f64`.

## Checked and found matching Go
- Update: local-domain guard, mixed failover+config error, cooldown placement (failover before graceful checks; non-failover after validation), graceful failover validations and error strings, PreviousFailoverVersion/FailoverEndTime handling, AA failover-version bump, config_version increments, bad-binary merge/max/delete, archival update via policy, retention/archival validation, primary-cluster check, replication task emission for global domains on create/update/failover (domains.rs:1676), audit writes.
- FailoverDomain request validations and destination-cluster check.
- Global ratelimiter: fallback fuse (MinInt64 start, >9 failures), shadowing order per key mode, gcAfterIdle=5, boostRPS, missing-weight → FailedUpdate, update-interval reset.
- ES: missing StartTime/CloseTime rewrite, ExecutionTime gt 0, addMust nesting, time/CloseStatus value conversion keys, default sort StartTime desc + RunID tie-breaker, one-sort-field rule, page token JSON field names, cleanDSL backticks.
- Archive readers: historyArchived gating (runID required, EntityNotExists only, ReadEnabled), empty URI → errHistoryNotFound, batches concatenation, Archived=true; visibility gating messages, convertToExecutionInfo shape; S3 history paging/highest version/token; GCS highest version selection.
