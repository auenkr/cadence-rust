# Matching Go/Rust parity audit v3 (2026-10-23)

Base: `origin/master` @ `8212a2db0` (merge-all + crate split). Read-only source audit: no cargo, no build, no gate was run for this document.
Rust paths: `rust/crates/cadence-matching/src` (MS) and `rust/crates/cadence-service-core/src` (CORE). Go paths: `go/service/matching/...`.
Inputs: v2 audit, `matching-subsystem-audit-20261023.md`, `matching-subsystem-audit-2-20261023.md`, `matching-metrics-matrix-20261023.md`, `progress-2026-10-23-0500.md`, `merge-notes-20261023.md`, `crate-split-plan-20261023.md`, and `memory/evidence/*`.

Labels: implemented | partial | different | missing. **Code** = read in source. **Runtime** = only where a real `.exit` file exists under `memory/evidence/` (path cited); prose-only claims (README/summary text with no `.exit`) are called "prose".

## 0. Runtime evidence ground truth (important caveat)

No full `clippy --workspace` / `test --workspace` exit file exists for the current master head. Gates ran on older binaries, before backlog forwarding, query parity, metrics parity, HTTP TLS, rpc parity and the crate split.

| Evidence | Exit file | Code state it covers |
|---|---|---|
| MySQL mixed gate (Rust Matching, Go FE/History/Worker, Go schema), run 5 | `evidence/matching-mysql-mixed-20261023/run5.exit` = 0 (runs 2-4 = 1) | `a34060ddf` (candidate + 2 fixes) |
| MySQL gate with cancel scenario | `evidence/matching-mixed-gates-20261023/mysql-gate.exit` = 0 | frozen PG-run binaries (`binaries.sha256`), ~`47774da67` |
| PostgreSQL mixed gate run 2 | `evidence/matching-pg-mixed-20261023/run2.exit` = 0 (run1 = 1, harness mistake) | `47774da67` |
| Two-node MySQL gate | `mysql-2node-b.exit` = 1, `mysql-2node-c.exit` = 143 (killed), run a no exit file | FAILED / not passing |
| Two-node shutdown fix unit test | `evidence/matching-twonode-20261023/unit-test.exit` = 0 | unit only |
| Candidate clippy + workspace test (876 passed) | prose only: `evidence/matching-candidate-20261023/gate.md` (no `.exit`) | `4f5fb6722` |
| Integration workspace test | `evidence/matching-integration-20261022/test10.exit`, `test11.exit` = 0 | `5006008` |
| Query parity (A1-A4) check/clippy/test | `evidence/matching-query-parity-20261023/{check,clippy,test}.exit` = 0 | pre-split, `-p cadence-server` only |
| Backlog forward | `final/clippy.exit` = 0; `final/test.exit` = **143** (killed, tests never completed) | tests NOT validated |
| Metrics parity | only `check.exit` = 0; `validation.md` says final clippy/test pending | no test exit |
| Crate split | `crate-split-20261023/clippy.exit` = 0, `test-core.exit` = 0, `test-matching.exit` = **101**, `test-retry.exit` = 101 | 2 failures claimed pre-existing (baseline `base-lib.exit` = 101) |
| gRPC TLS, HTTP TLS | prose only (`matching-grpc-tls-20261022/results.md`, `matching-http-tls-20261023/verification.md`); no `.exit` | unit-level |
| Merge-all | `merge-notes-20261023.md` states check+clippy -p cadence-server exit 0 after fix; no exit file; tests not run | |

Two tests fail on master per the crate-split evidence: `matching_reader_local_poller_wins_after_forward_error` and `matching_registry::tests::replacement_can_load_before_old_cleanup_and_keeps_its_lease` (`test-matching.log`). Not re-run here.

## 1. Corrected v2 claims

1. **`enableSyncMatch`, `maxTasklistIdleTime`, `activityTaskSyncMatchWaitTime`**: v2 called these P0 missing consumers. Wrong. Go only reads them to copy into `tasklist.Config` (`config/config.go:192,202,219`; `tasklist/task_list_manager.go:1040,1050-1051,1059-1060`). `grep -rn` of the three names across `go/` (excluding tests, dynamicproperties, config.go and those task_list_manager lines) returns nothing. Rust keeps unused readers with comments saying so (`matching_config.rs:4-5,44-72`). Status: **implemented (no consumer needed)**.
2. **Partition-config kind check** (v2 "different, Sticky passes"): Rust uses `matches!(task_list.kind, 0 | 1)` (`grpc_matching.rs:3033` update; refresh at the similar site ~`3160`). Go requires Normal (`engine.go:1257-1259,1292-1293`). The gRPC kind enum is INVALID/NORMAL/STICKY/EPHEMERAL (`go/common/types/mapper/proto/api.go:4038-4048`), so 0|1 = invalid|normal. I did not read the Rust proto numbering (submodule not initialised in this worktree); assumed from the mapper. Status: **implemented (code), unverified numbering**.
3. **Health readiness** (v2 "no readiness gate"): now present. `Meta::health` awaits `wait_started` (`grpc_matching.rs:3533-3536`; Thrift `thrift_matching.rs:697-698`), released by `mark_started` after membership startup (`launcher.rs:1321`). Go: `handler.go:106-111`. **implemented**.
4. **gRPC TLS** (v2 missing): now `RpcConfig.tls` (`CORE/config.rs:589-592`) plus `CORE/grpc_tls.rs:29,62,106,129`. **implemented (unit only)**.
5. **HTTP TLS options** (v2 "rejects client auth/CA/etc."): now handles modes disabled/permissive/enforced, client auth, CA files; host verification/serverName/sslmode treated as outbound-only (`launcher.rs:141-211`, permissive sniff `:287,396-409`). **implemented (unit only)**.
6. **Semaphore per-domain gate** (v2 process-wide): now reads `system.enableDistributedSemaphore` per domain (`grpc_matching.rs:2940`; Go `engine.go:458`, `config.go:221`), cached-manager path skips admission as Go. `AppState.semaphore_enabled` remains only for the legacy frontend path (`launcher.rs:573`). **implemented**.
7. **CancelOutstandingPoll** (v2 process-local only): now runs `durable_queue` (owner check + manager load) then cancels (`grpc_matching.rs:3212-3268`); poller cancellation callbacks added (rpc-parity source-review). Go `engine.go:1109-1132`. **implemented**.
8. **Subsystem audits 1/2 unknowns**: matcher, pollers, limiter, shard processor, registry, standby, isolation balancer were compared in audit 2 (source only); A1-A4 were then fixed (query-parity). F10/A4 reversed; see audit 2.

## 2. gRPC RPCs (13)

Go handlers `handler/handler.go`: AddActivityTask 146, AddDecisionTask 179, PollForActivityTask 212, PollForDecisionTask 252, QueryWorkflow 293, RespondQueryTaskCompleted 326, CancelOutstandingPoll 354, DescribeTaskList 382, ListTaskListPartitions 411, GetTaskListsByDomain 441, UpdateTaskListPartitionConfig 470, RefreshTaskListPartitionConfig 498, AcquireSemaphore 526. Engine: `handler/engine.go` (lines below).

| RPC | Go engine | Rust (grpc_matching.rs) | Status | Delta since v2 / remaining |
|---|---|---|---|---|
| AddDecisionTask | engine 497 | 2058 | implemented (code) | Sync-match / forward chain; sticky-standby path equivalent per audit 2; backlog forward added. Runtime: MySQL/PG gates (old binaries). |
| AddActivityTask | 595 | 2273 | implemented (code) | as above |
| PollForDecisionTask | 671 | 1622 | implemented (code) | CancelOutstandingPoll/empty-poll semantics fixed (parity-fixes); query-poll forwarding on standby added (A2, unverified end-to-end) |
| PollForActivityTask | 888 | 1855 | implemented (code) | |
| QueryWorkflow | 1014 | 2482 | implemented (code) | A1 (parent ServiceBusy -> keep waiting locally) fixed; no-poller wait-until-deadline fixed. Mixed-node query only exercised in single-Rust-node gates. |
| RespondQueryTaskCompleted | 1092 | 3269 | implemented (code) | |
| CancelOutstandingPoll | 1109 | 3212 | implemented (code) | see §1 |
| DescribeTaskList | 1133 | 3361 | implemented (code) | audit 2: same fields |
| ListTaskListPartitions | 1158 | 2848 | partial | ring-order/cross-node response parity never compared against a Go node |
| GetTaskListsByDomain | 1222 | 2733 | implemented (durable) | `matchingReturnAllTaskListKinds`; legacy in-memory fallback still enumerates frontend state (dev only) |
| UpdateTaskListPartitionConfig | 1239 | 2991 | implemented (code) | kind check fixed; owner-routing/error precedence unverified |
| RefreshTaskListPartitionConfig | 1281 | 3129 | implemented (code) | same |
| AcquireSemaphore | 400 | 2901 | implemented (code) | per-domain; Cassandra-only persistence, SQL returns Go's unsupported (`matching_task_store.rs:104-129`); no semaphore runtime gate |

Thrift: `thrift_matching.rs` dispatch of the 10 IDL methods at `:311,335,361,382,424,448,463,480,503,536`; `Meta::health` at `:697`. The 3 gRPC-only RPCs are absent from Go's Thrift IDL as well. **implemented (code)**; the Go-Thrift client path was exercised by the Go FE/History in the mixed gates (old binaries), not separately validated.

## 3. Membership / shard ownership / lifecycle

| Concern | Go | Rust | Status |
|---|---|---|---|
| Ringpop bootstrap + membership | `cmd/server/cadence/server.go`, `service.go:100-118` | `launcher.rs:887` (MatchingRingpopProvider), `:1178` (with_membership), lifecycle withdraw `:1285,1463-1464` | partial: code complete; two-node gate failed (exit 1/143), churn/restart convergence not proven |
| Ring-owner routing + remote calls | `engine.go:1600-1662` | `matching_membership.rs`, `matching_runtime.rs:34,318,747` | partial |
| Shard-distributor executor/spectator | `engine.go:221-296` | `launcher.rs:1227` -> `matching_runtime.rs`, `matching_shard_distributor.rs`, `matching_shard_processor.rs` | partial: no failure/recovery gate exists |
| Startup | `handler.go:95` | health gate (§1.3) | implemented |
| Shutdown | `service.go:138-152` (evict self, sleep drain, handler stop); Go never fails exit on lease loss | `launcher.rs:1462-1511`, `matching_runtime.rs:538-560`; lost-ownership tolerated (`matching_registry.rs` `tolerate_lost_ownership`, twonode unit test exit 0); SIGTERM with unreachable metrics sink fixed | implemented (code). Benign stderr "Not enough hosts" at shutdown seen in gates (`matching_membership.rs:114`) |
| Domain-failover callbacks | `engine.go:1707-1762` | `grpc_matching.rs` release of blocked polls | partial: only poll release; sticky-cleanup side effects not compared; audit 2 notes `forward_poll` does not read `released` |

## 4. Task-list subsystem (Go `tasklist/`)

Per audit 1/2 (source-level comparisons, no Go tests run): GC, liveness, writer, reader pump, isolation policy, limiter, poller manager, shard processor, registry, standby completer, matcher Offer/Poll/pollOrForward: **implemented/equivalent**. Differences recorded there and still open unless fixed: isolation balancer/scaler metric emission semantics approximate (A7, MIN); balancer tie-break nondeterminism; standby completion-failure counter (A5, MIN); empty-query-poll AutoConfigHint (A6, MIN); forward leg not reading `released` (MIN); 200 ms forwarded-add fallback deadline (A8, MIN). Backlog forward-to-parent (Go `task_reader`/`matcher.MustOffer`): **implemented (code)** at `matching_dispatch.rs:289,456`, `matching_registry.rs:1367`; **tests not validated** (`final/test.exit` = 143) and no runtime evidence.

## 5. Persistence backends

Selector `matching_task_store.rs`: sqlite `:173`, mysql `:184`, postgres `:238`, Cassandra `:153-162`; MySQL/PG multi-database `:223`; SQLite multi-DB rejected.

| Backend | Status | Runtime |
|---|---|---|
| SQLite | implemented (single DB) | dev only; earlier restart evidence `evidence/matching-process-restart-2026-10-18.md` (prose) |
| MySQL | partial | `run5.exit` = 0, `mysql-gate.exit` = 0 (old binaries); `goSchemaOnly` clean per prose |
| PostgreSQL | partial | `run2.exit` = 0; GC/domain SQL checks "not collected", no Go-schema-only check |
| Cassandra | partial (adapter, TTL/protocol audits exist) | **none**; no Cassandra gate evidence under `memory/evidence/` |
| Multi-DB hash routing | partial | no runtime evidence |

## 6. Dynamic config

Rust reads the Matching keys listed in v2 (rates, batching, forwarding, scaler/isolation, semaphore TTL/enable, shard-manager onboarding, drain, partition config). Not-consumed-by-Go: enableSyncMatch, maxTasklistIdleTime, activityTaskSyncMatchWaitTime (§1.1). Still different/missing:

| Key | Go | Rust | Status |
|---|---|---|---|
| `matching.enableTaskInfoLogByDomainID` | consumed only when `EnableDebugMode` (`engine.go:1588-1598`, esp. 1593) | no consumer (grep) | missing (debug logging only) |
| `matching.errorInjectionRate` | persistence client factory (`common/persistence/client/factory.go:197`) | none | missing (test aid) |
| dynamic-config providers | file, others | filebased only; others bail (`launcher.rs:523`) | partial |
| `forwarderMaxChildrenPerNode` | clamp >= 1 | clamped (`grpc_matching.rs:500-506`) | implemented |

## 7. Config / TLS / transports

| Area | Status | Notes |
|---|---|---|
| Static config | partial | Go-shaped subset (`CORE/config.rs`); not full Go model |
| gRPC TLS | implemented (code, unit prose) | no mixed TLS cluster run |
| HTTP TLS | implemented (code, unit prose) | permissive sniffing never interoperated with Go client |
| TChannel/YARPC | partial | exercised by Go services in gates; TLS on TChannel port: not examined (unknown) |

## 8. Metrics

Matrix (`matching-metrics-matrix-20261023.md`): 142 inventory rows; by status string: 92 "name present; emission/tag audit required", 24 MISSING, 15 "implemented name", 9 "Go catalog only", 2 "implemented emission; final validation pending". Task-store, scaler, manager, membership, routing metrics were wired; MISSING covers persistence shard/size/per-host histograms, domain metadata and semaphore metering, retry tagging, rollup/migration. Backlog-forward metrics wired with the backlog branch (untested). Status: **partial**. Runtime: none (metrics parity has no test exit).

## 9. Health / admin

Go: Matching and Meta only. Rust: Meta health on gRPC and Thrift gated on start (**implemented**). Shared HTTP `/health` and `/api/v1/admin/*` routes in CORE are not Matching surfaces (**different**, by design of the all-in-one server). Per-service `cadence-matching` binary exists (crate split); `cadence-matching` bin only checked, not run, per crate-split summary; gate harness switched to it but not run (`matching-gates-per-service-20261023/README.md`).

## 10. Conclusions

### (a) Still missing / different, ranked

1. No validated build of master head: backlog-forward and metrics tests have no passing exit file; `test-matching.exit` = 101 with 2 failing tests.
2. Two-node (multi-Rust-Matching) forwarding / ownership transfer failed (exit 1/143) and was never fixed or re-run.
3. Shard-distributor failure and recovery: untested.
4. Cassandra: no gate at all (and `acceptanceEligible` is tied to the Cassandra gate by design).
5. Backlog forward-to-parent: code only, no runtime.
6. Metrics: 24 MISSING + 92 unaudited rows.
7. Domain-failover callback side effects beyond poll release; `released` not read in forward leg.
8. Dynamic-config providers beyond filebased; `enableTaskInfoLogByDomainID`, `errorInjectionRate`.
9. MIN: scaler/balancer metric semantics, A5/A6/A8, spurious-metric parent path (A3 parent not added), A2 standby forward untested end to end.
10. ListTaskListPartitions/UpdatePartitionConfig cross-node ordering/error precedence.

### (b) Runtime evidence required before declaring Matching complete

All must be run on the final master binary (`cadence-matching`), Go FE/History/Worker, no Go Matching, with `.exit` files:
- clippy + test `--workspace` exit 0 (two failing tests fixed).
- MySQL gate (incl. cancel, query, isolation, heartbeat, SIGTERM and SIGKILL restart), acceptance-eligible definition agreed.
- PostgreSQL gate incl. GC/domain-metadata checks and Go-schema-only check.
- Cassandra gate (shared Go schema).
- Two-node gate: ring convergence, remote forward, kill-one-node recovery, shutdown exit 0.
- Shard-distributor enabled: assignment, executor restart, shard-manager restart, reassignment under load.
- Restart/recovery on every backend (SIGTERM drain, SIGKILL, persisted backlog, GC).
- SDK suite (Go samples, signal, cancel, query, activity timeout/heartbeat, sticky, standby/failover) and Java/other SDK if in scope.
- Backlog forward with >1 partition, standby query-poll forward (A2).
- TLS gRPC/HTTP with a Go client; metrics comparison against Go emit set.

### (c) Unknowns

- Rust proto numbering for task-list kind (assumed from Go mapper).
- Whether master head passes beyond compile+clippy of `cadence-server`.
- TChannel TLS behavior; ListTaskListPartitions ordering vs Go; exact refresh-partition-config line (~3160).
- Whether the 2 failing tests are flaky or real (only "pre-existing" per baseline log).
- `forward_poll`/`released` effect under real failover; Go `isolation_balancer_test.go` not run against Rust.
- Matrix "name present" rows: emission unverified.
