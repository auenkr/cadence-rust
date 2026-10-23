# Matching open parity gaps (2026-10-24)

Base: `origin/master` @ `e9b8e9a0e` (fetched 2026-10-24). Audit input: `memory/matching-go-parity-audit-v4-20261023.md` (on `origin/matching-parity-audit-v4-20261023`, written against `92668eb2f`). Test map input: `memory/matching-go-test-map-20261023.md` (on `origin/matching-test-inventory-final-20261023`).

Read-only. No code edited, no cargo, nothing run. Every status below is from reading source and evidence files on `origin/master` at the base above. Test names were grepped to exist; tests were NOT executed, so a "FIXED" means the code and a named test exist, not that the test passes.

Statuses: **FIXED** = the audit's failure mode is no longer present in code (test named where one exists). **STILL OPEN** = the audit's gap is present on master. **UNCLEAR** = code moved or evidence is missing, and I could not decide from source alone.

## Corrections to the audit and test map

These do not belong in the table, but a cheap worker must not start from the stale claims:

- **P0-1 is half-fixed.** The forward function now takes a separate source domain (`matching_dispatch.rs:333`, `:405-406`), and the Go-parity test is un-ignored (`matching_dispatch.rs:2327`). The production backlog caller still passes the list domain as the source (`matching_dispatch.rs:498`).
- **P0-2 is fixed in code.** Rust mirrors Go's `ToTaskListKind`, where INVALID maps to nil and reads as Normal (`go/common/types/mapper/proto/api.go:4051-4054`). Only STICKY (2) and EPHEMERAL (3) are rejected (`grpc_matching.rs:3001`, `:3128`).
- **PG-2 exists now.** The test map says `sync_match_non_blocking_poll_with_no_tasks_returns_immediately` is missing. Master has `matching_dispatch.rs:1840 matching_dispatch_non_blocking_poll_with_no_tasks_returns_immediately`.
- **PG-3 is not a gap.** The test map and audit say Go rejects an unknown isolation group. Go's `Poll` falls back to the default task channel (`go/service/matching/tasklist/matcher.go:440-445`). `TestIsolationPollFailure` (`matcher_test.go:673`) errors only because no task arrives before its one-second timeout. Rust matches Go here (`matching_sync.rs:799`).
- **P1-16 is fixed in code.** `forward_query_poll` now selects on the released-pollers notify (`matching_dispatch.rs:909-917`). The audit said the forward leg did not read it.
- **P1-10 is fixed in code.** The legacy frontend-state enumeration is gone. Without a registry the RPC returns an empty response (`grpc_matching.rs:2803`).

## Table

| id | severity | status | evidence (origin/master e9b8e9a0e) | suggested narrow fix task (cheap worker) |
|---|---|---|---|---|
| P0-1 | P0 | STILL OPEN | Forward fn takes `source_domain` and sends it (`matching_dispatch.rs:333, 405-406`); test un-ignored (`:2327`, asserts `:2346-2349`). Production backlog caller passes list domain for both args: `matching_dispatch.rs:498` `forward_backlog_task(task, &self.domain_id, &self.domain_id, ...)`. Root cause: `TaskData` has no source-domain field (`cadence-persistence/src/matching_blob.rs:8-15`). | First check whether the persisted row carries the workflow's domain id (Go task row has `DomainID`). If yes, pass that row value at `matching_dispatch.rs:498` instead of `self.domain_id`, and add a test where row domain != list domain through the reader path. Do not change the blob codec unless the row lacks the domain. |
| P0-2 | P0 | FIXED | Go mapper INVALID→nil (`go/common/types/mapper/proto/api.go:4051-4054`); engine rejects non-Normal (`go/service/matching/handler/engine.go:1257-1259`, `:1292-1293`). Rust rejects only kind 2|3 (`grpc_matching.rs:3001`, `:3128`) with the matching comment. | Test-only follow-up: add a test that raw kind 0 (INVALID) is accepted as Normal and kind 2/3 are rejected, on both update and refresh. The API enum `TaskListKind` was not located by grep in `go/proto/internal/uber/cadence/shared/v1/tasklist.proto`, so the numbers 2/3 rest on `go/proto/persistenceblobs/v1/message.proto:813-815` (STICKY=2) and the Go mapper names. |
| P0-3 | P0 (conditional) | STILL OPEN | ConfigStore is now implemented (`cadence-matching/src/launcher.rs:564-608`). OpenFeature still bails (`launcher.rs:613-614`, "no provider plugin is ported"). | Only if a deployment uses OpenFeature: decide whether to port the Go plugin or keep the explicit bail. Otherwise close as accepted difference and document it. |
| P1-1 | P1 | STILL OPEN | Two-node MySQL on master: `memory/evidence/matching-mixed-gates-20261023/mysql-2node-b.exit` = 1, `mysql-2node-c.exit` = 143. Newer runs on master also fail: `matching-backlog-evidence-20261023/two-node-mysql-with-scenario/gate.exit` = 1; `two-node-mysql-control-rerun/README.md` says the control fails at a rate-limit check ("paced activity dispatch was not rate limited: [83,1387,473]", `tests/matching-service-compat.cjs:1147`). | Diagnose `tests/matching-service-compat.cjs:1147` on two-node MySQL: decide test-harness vs Rust rate-limit bug. Re-run the control lane only (one gate at a time, `work-safe`), record exit and binary sha. |
| P1-2 | P1 | STILL OPEN | No passing gate bound to `e9b8e9a0e` found on master. Newest two-node MySQL evidence fails (see P1-1). Audit's finding (gate-run3 `binary-shas.txt` has no build commit) was not re-checked here. | After P1-1 is resolved: build with `cargo-target-final`, record binary sha and head commit, then re-run each lane (MySQL, PostgreSQL, Cassandra, Thrift, shard-distributor) one at a time. |
| P1-3 | P1 | UNCLEAR | Emit sites found: DB-unavailable class (`cadence-matching/src/matching_persistence_limiter.rs:68-70`, `..._PER_DOMAIN`), BadRequest class (`:43`, `:73`), row/payload size constants (`cadence-service-core/src/matching_metrics.rs:406-407`) with a test (`matching_persistence_limiter.rs:666-704`). Per-domain names for BadRequest not checked. No Go emission/tag comparison exists. | Build a table comparing each Rust metric name, tags and histogram bounds with `go/common/persistence/wrappers/metered/base.go` (lines 103, 125, 141, 163, 305, 321). Test-only; no Go run needed to draft it. |
| P1-4 | P1 | STILL OPEN | Isolation balancer ported and tested (`cadence-matching/src/matching_scaler_go_tests.rs:133-616+`). Adaptive scaler lifecycle is an ignored empty gap: `matching_scaler_go_tests.rs:1059` (no Start/Stop handle). `enableAdaptiveScaler` gate lane not located. | Implement or document a start/stop handle for `Scaler` so the ignored test can assert idempotent start/stop. Then add one scaler gate lane or document it as not exercised. |
| P1-5 | P1 | STILL OPEN | Present and not ignored: slot-taken retry (`matching_semaphore_parity_tests.rs:366`), max attempts (`:390`), already-held (`:407`, `:425`), confirm-read failure (`:496`), failed grant (`:337`). Still ignored: validate params (`:635`), page tokens (`:655`), nil/unknown rows (`:661`), acquire deadline while starting (`:667`), grant stops at deadline (`:691`), unknown write outcome (`:710`), late stop unregister (`:718`). "Start does not block on load" (Go :420) has no test located. | Narrow: add a `deadline`/cancel parameter to `Manager::acquire` and `grant`, then un-ignore `:667` and `:691`. Second task: add an injectable `GrantOutcome::Unknown` for `:710`. |
| P1-6 | P1 | STILL OPEN | Rust `Liveness` exists (`cadence-common/src/liveness.rs:3`) and sweep is driven by `sweep_idle` (`matching_registry.rs:2144`, `matching_semaphore.rs:537`). Tests named `liveness_event_loop_*` (`matching_registry.rs:3071`, `:3092`) call `sweep_idle` directly, not a TTL/2 ticker. | Test-only: add a test that spawns the TTL/2 ticker (or the registry sweep loop) and asserts unload occurs in [TTL, 1.5·TTL]. Or record it as an accepted difference. |
| P1-7 | P1 | UNCLEAR | Counterparts exist: `cadence-common/src/poller.rs:119` `list`, `:129` `count_by_group`. Not checked: whether `GetCountByIsolationGroup` and per-group `ListInfo` are wired to these in the RPC path. | Trace the poller-info RPC (`grpc_matching.rs` poller info) to `poller.rs:119/:129`. If wired, mark FIXED and add a test that counts per group. |
| P1-8 | P1 | STILL OPEN | `list_task_list_partitions` (`grpc_matching.rs:2808`). Tests exist only against Rust (`matching_rpc_tests.rs:852-3766`). No ring-order comparison with Go. | Build a fixture that dumps ring order from a Go node and compares it with the Rust response for the same membership. Needs a mixed gate, so run it alone. |
| P1-9 | P1 | STILL OPEN | Rust-only alias still read: `cadence-matching/src/launcher.rs:705-707` (`"matching.errorInjectionRate"`). Go has no such key. | One-line narrow fix: drop the alias (`launcher.rs:705-707`) and keep `system.persistenceErrorInjectionRate`. Add a unit test that the alias is ignored. |
| P1-10 | P1 | FIXED | Durable path via `queues.describe_by_domain` (`grpc_matching.rs:2754-2803`); `matchingReturnAllTaskListKinds` read at ~`:2783`. Legacy frontend enumeration removed (returns empty at `:2803`). Tests: `matching_rpc_tests.rs:2680` (`matching_rpc_domain_listing_uses_loaded_matching_managers`), `:3525` (`matching_domain_listing_filters_kinds_and_collapses_partition_roots`). Not run. | None needed. Optional: confirm both tests pass in the next gate. |
| P1-11 | P1 | UNCLEAR | Completion-failure counter has a unit test: `matching_standby.rs:203` (`standby_cancelled_checker_counts_one_completion_failure`). No end-to-end standby query-poll forward test found by name. | Add one RPC-level test: standby query-poll forwards to a parent and completes, using the existing `matching_rpc_tests.rs` fixtures. |
| P1-12 | P1 | STILL OPEN | Matcher coverage is still partial. From the test map: 13 PORTED, 11 PARTIAL, 27 CANNOT-PORT, 2 MISSING. The PG-2 test now exists (`matching_dispatch.rs:1840`). PG-3 is not a gap (see Corrections). | Port the next block of Go `matcher_test.go` cases through the RPC fixture, starting with `TestRemoteSyncMatch`-family (11 PARTIAL). Track by test map row, not by count. |
| P1-13 | P1 | STILL OPEN | The only backlog runtime evidence on master is single-node MySQL (`matching-backlog-evidence-20261023/single-node-mysql/gate.exit`). Two-node backlog runs fail (see P1-1). | Blocked by P1-1. Once the two-node control passes, run `--matching-backlog-forward` on two nodes and record `backlog-forward.json`. |
| P1-14 | P1 | UNCLEAR | Not checked in this pass. The audit says the PostgreSQL gate (`matching-pg-mixed-20261023/run2.exit` = 0) predates head and skipped GC and domain-metadata SQL checks. I did not search PostgreSQL evidence on other branches. | Search `memory/evidence` on all `origin/matching-*` branches for a PostgreSQL run after `e9b8e9a0e`. If none, run the PostgreSQL lane with GC and domain checks enabled. |
| P1-15 | P1 | STILL OPEN | gRPC TLS: 8 tests in `cadence-service-core/src/grpc_tls.rs` (audit said 5). HTTP TLS: a handshake test (`launcher.rs:1100`). TChannel TLS: no implementation or test found by grep. | Decide scope: either port TChannel TLS, or record it as a deployment-excluded feature with evidence. Narrow test task: add one TChannel TLS handshake test if it is in scope. |
| P1-16 | P1 | FIXED | Forward leg selects on released pollers (`matching_dispatch.rs:909-917`). `release_blocked_pollers` is called from `matching_registry.rs:571`, `:610`. Tests: `matching_dispatch.rs:2437` (`matching_failover_releases_active_forward_resolver_and_poller_registration`), `:2511` (standby query failover). Callers not compared line-by-line with `go/service/matching/handler/engine.go:1707-1762`. | Optional: compare the registry callers with Go's failover callback and note any difference. |

## Counts

- FIXED: 3 (P0-2, P1-10, P1-16)
- STILL OPEN: 12 (P0-1, P0-3, P1-1, P1-2, P1-4, P1-5, P1-6, P1-8, P1-9, P1-12, P1-13, P1-15)
- UNCLEAR: 4 (P1-3, P1-7, P1-11, P1-14)
- Total: 19 (3 P0 + 16 P1)

## Top open items

1. **P0-1**: backlog forward sends the list domain as source domain (`matching_dispatch.rs:498`).
2. **P1-1**: two-node MySQL lane fails on the rate-limit step (`tests/matching-service-compat.cjs:1147`), and it blocks P1-13.
3. **P1-2**: no passing gate is bound to the current head.
4. **P1-13**: backlog forward at runtime with more than one partition is unproven.
5. **P1-5**: semaphore deadline and unknown-outcome paths are still ignored gap tests.

Next cheapest: P1-9 (one-line alias removal) and P0-1 (one-line caller change, contingent on the row carrying the domain).

## Method notes

- Audit P0/P1 items were checked by grepping named symbols on `origin/master` at `e9b8e9a0e`. Line numbers are from that commit.
- Go citations were read from `origin/master` (`go/service/matching/...`, `go/common/types/mapper/proto/api.go`).
- The evidence directories on master were read as-is. No gate was run.
