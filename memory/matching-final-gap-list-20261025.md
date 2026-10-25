# Matching final gap list (2026-10-25)

Base `origin/t3/matching-integration-20261025`. Read-only, docs only: no cargo/go/fmt, no CI, no gate. Matching is NOT complete.

Method: I did not redo the existing audits. This list consolidates `matching-open-gaps-20261024-v2.md` (OPEN rows), `matching-final-signoff-review-20261025.md`, `matching-historical-differences-audit-20261025.md`, `matching-cannot-port-review-20261024.md`, `matching-pause-handoff-20261025.md`, plus a fresh directory-level sweep of Go packages that no existing audit names (section C). Any item copied from an older memory file keeps that file's line numbers and is flagged "from memory, not re-verified at this head". Anything I could not locate is marked UNKNOWN. v2 is dated before several fixes (OpenFeature lifecycle code now exists in `launcher.rs:112-118, 557`; `matching.errorInjectionRate` is a correct Go key, `launcher.rs:767`), so v2 rows may be stale.

Risk: H = can change wire/behavior or data under real traffic; M = observable only in edge/failure paths or operations; L = test-seam or architecture only.

## A. Not verified by current-source mixed evidence (highest risk)

- [ ] **H. No mixed-cluster evidence bound to the final production binary.** Backend/transport lanes are pinned at different SHAs than `29bdd2b04`; `git diff 27d3ff5f..29bdd2b04 -- rust` changes `grpc_matching.rs` (~1693 lines), `launcher.rs`, `grpc_tls.rs`. Green lanes are not inherited (signoff review). Go: n/a. Rust: whole crate. Needs a fresh CI run per lane with binary SHA256.
- [ ] **H. Cassandra lane failing.** Assertion `DescribeTaskList must report the reported backing rate: 100000 !== 1` (`tests/matching-service-compat.cjs:1168`, from memory). Unknown whether this is a harness or a Rust bug; diagnosis branches only read code. Go: `go/service/matching/tasklist/task_list_manager.go` (describe/rate path, exact line UNKNOWN). Rust: `matching_registry.rs` describe path, line UNKNOWN.
- [ ] **H. Long-poll expiry (lane 9) failing.** `decision_started_after_long_poll_window_still_returns_task` is ignored and unrun, no Go control run (branch `matching-longpoll-diag-20261024`). Go: `go/service/matching/handler/engine.go` poll path (line UNKNOWN). Rust: `matching_dispatch.rs`.
- [ ] **H. Real-Go-History callback under TLS and under the final binary.** HTTP TLS interop used a Go ring stub with a Rust History fixture, not original Go History (signoff review). TChannel TLS: no implementation or test found (v2 P1-15). Decision needed: in scope or deployment-excluded.
- [ ] **M. Configstore and real shard-distributor mixed lanes**, shard-distributor/configstore evidence older than the candidate (signoff review). Go: `go/service/matching/handler/engine.go:221+, 281`; Rust `matching_shard_distributor.rs:507+`, `matching_shard_routing.rs:308+` (unit-level only).
- [ ] **M. Adaptive scaler under a gate.** Unit lifecycle exists (`matching_scaler.rs:224,233`), no gate lane enables `enableAdaptiveScaler` (v2 P1-4). Go: `go/service/matching/tasklist/adaptive_scaler.go` (also emits `event.Log` at `:190`).
- [ ] **M. PostgreSQL GC and domain-metadata checks.** Lane evidence shows `domain metadata: null`; MySQL `durableGcEvidence:false` for two queues (measurement limit). Go: `go/service/matching/tasklist/task_gc.go`. Rust: `matching_task_store.rs`, `cadence-persistence`.
- [ ] **M. ListTaskListPartitions owner order vs Go hash ring** across a mixed membership. Test `tests/matching_engine_go_tests.rs:271` ignored; a fresh-workspace owner-order oracle (run 40168414544) later covered baseline cases, so v2 P1-8 may be closed. Needs a reviewer to confirm. UNKNOWN.

## B. Ported behavior with known remaining Rust differences (from memory, re-check before use)

- [ ] **M. Semaphore validation and load path (v2 P1-5).** Still ignored: `Manager::new` param/TTL validation (`matching_semaphore_parity_tests.rs:640`), page tokens through scan (`:660`), acquire during load gives up at own deadline (`tests/matching_engine_go_tests_d.rs:518`). Go: `go/service/matching/handler/engine.go:399+`. Rust: `matching_semaphore.rs`.
- [ ] **M. Poller manager API differences.** `poller::Manager::cancel` returns `()` where Go returns bool (`go/service/matching/poller/manager.go:165`; Rust test `tests/matching_partial_upgrade_b.rs:339`); no history-change callback (`:301`).
- [ ] **M. Non-durable Describe status** lacks task-id block / ack manager (`matching_rpc_tests.rs:1015`). Only matters if the non-durable path is reachable in production; UNKNOWN.
- [ ] **M. Membership has no shutdown state**: semaphore buckets accepted while shutting down on that path (`matching_rpc_tests.rs:1496`). Go: `go/service/matching/handler/membership.go`. Also Go membership panic recovery (`membership.go:51-57`): a `catch_unwind` branch `matching-panic-recovery-20261024` exists, merge status UNKNOWN.
- [ ] **M. Handler domain-name lookup order vs throttle/deadline** and malformed-UUID SQL handling: handler domain-name lookup happens before throttle/deadline checks; a SQL malformed-UUID panic differs from a swallowed cache error. Open per pause handoff (branch `t3/matching-nil-domain-review`). Go: `go/service/matching/handler/handler.go:559`. Rust: `grpc_matching.rs` `check_rpc_quota`.
- [ ] **M. Absent Query fields / nil PollRequest parity** (branches `t3/matching-go-error-parity`, `t3/matching-runid-raw-tests`): fixture failed in run 40176622622, later fixed but not CI-run; activity sync-match-to-poller RunID coverage missing. None merged.
- [ ] **M. Query completion (RespondQueryTaskCompleted)** branch `t3/respond-query-task-parity` not merged; `query-format.exit` / `workspace-format.exit` artifacts not recorded.
- [ ] **L. Engine error injection seams**: four `go_handler_*_engine_error_is_internal_service_error` tests ignored (`matching_rpc_tests.rs:4893-4911`); also partition-config update/refresh manager-error seams, failover-callback closures, `err_if_shard_ownership_lost` 8 subtests. Behavior may match; only unproven.
- [ ] **L. Matcher/manager unit coverage** (v2 P1-12): next block of `matcher_test.go` unported, starting with `TestRemoteSyncMatch`.
- [ ] **L. Metrics matrix against `go/common/persistence/wrappers/metered/base.go`** (v2 P1-3); `matching-metrics-emission-comparison-20261024.md` reports 4 matches, remaining rows UNKNOWN.

## C. Go areas that no existing audit names (fresh sweep, directory level only)

I only listed and grepped these; I did not read them in full. Treat each as UNKNOWN-until-audited.

- [ ] **H(if Rust Frontend/History uses it; else M). `go/client/matching` client-side partition selection.** Files: `loadbalancer.go:99,107`, `rr_loadbalancer.go:76,84`, `weighted_loadbalancer.go:149,170,177`, `isolation_loadbalancer.go:41,51,79`, `multi_loadbalancer.go:46,62,82`, `partition_config_provider.go:89`, `peer_resolver.go:48`. No Rust counterpart found: grep for load-balancer/round-robin/weighted in `rust/crates` hits only `grpc_matching.rs`, `matching_registry.rs`, `cadence-persistence/src/cassandra.rs` (unrelated words). Matching's own forwarder calls Go client code paths (partition picks affect forward targets and what Rust Matching sees from Go History/Frontend). Per the service order, Rust Matching only needs server compatibility; the client belongs to Frontend/History, but it is part of "go/client/matching" in scope here and nothing records the decision. Rust location: UNKNOWN / none.
- [ ] **M. `event.Log` simulation events** (`go/service/matching/event/logger.go:47`; call sites `matcher.go:184-414`, `task_list_manager.go:608-609`, `adaptive_scaler.go:190`, `task_reader.go`, `task_writer.go`, `engine.go`). Go uses them for simulation queries; no Rust equivalent found by grep (`event_log`, `EventName`). Only matters for Go simulations; likely deployment-excluded, needs a decision.
- [ ] **M. Go `service.go` lifecycle (155 lines, `Stop` at `:138`)** and `go/service/matching/liveness`: v2 marks liveness fixed; whole-service start/stop ordering vs Rust `launcher.rs` is covered by the restart gate only, not by a line-by-line comparison. UNKNOWN.
- [ ] **M. `go/service/matching/config/config.go` per-field reader table**: `go_new_config_every_field_has_a_rust_reader` still ignored (`tests/matching_partial_upgrade_a.rs:80`). `matching-dyncfg-key-matrix-20261023.md` exists; completeness against current `config.go` unverified at this head.
- [ ] **L. Go wrappers `wrappers/grpc` and `wrappers/thrift`**: covered by thrift error matrix and wire-field matrix memory files; no per-method re-verification at this head.

## D. Decisions still needed (not engineering gaps)

- OpenFeature provider plugin: v2 said bail at `launcher.rs:613-614`; current source has lifecycle code (`launcher.rs:112-118`). Re-check whether the bail still exists. UNKNOWN.
- Accepted-difference sign-off for the 8 proposed rows of v2 section 4b (several now resolved per historical-differences audit).
- Unmerged branches (15 in v2 section 2a, plus the 2025-10-25 `t3/*` branches above) carry fixes and un-ignored tests that are not on the integration base.

## Ranked top 10

1. No mixed evidence on the final binary (A1).
2. Cassandra describe-rate lane failing, cause unknown (A2).
3. Long-poll expiry lane failing (A3).
4. Unmerged request/error-parity, raw RunID, query-completion, nil-domain branches (B).
5. Client-side matching load balancers with no Rust counterpart (C1).
6. Real-Go-History and TChannel TLS coverage (A4).
7. Semaphore validation/page-token/load-deadline (B1).
8. Configstore + real shard-distributor lanes stale (A5).
9. Adaptive scaler never gate-tested (A6).
10. PostgreSQL GC/domain-metadata evidence incomplete (A7).
