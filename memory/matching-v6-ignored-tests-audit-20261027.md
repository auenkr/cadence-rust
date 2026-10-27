# Matching v6 ignored/excluded test audit (2026-10-27)

Candidate: tag matching-acceptance-20261027-v6, SHA 3055d0c6c. Read-only static audit; no tests, Cargo or Go run. Not a completion declaration.
Scope: `#[ignore]` in Rust Matching and its persistence, harness selectors (`--ignored`), and Go Matching `_test.go` coverage.
Inputs: matching-ignored-rpc-audit-20261025.md, matching-ignored-tests-result.md, v5 ledger. The ledger lists the Luna ignored-test audit (b9ca69a1) as "Running"; its output was not recovered, so this report is a fresh audit.

## A. Ignored tests, grouped

### A1. Parity-gap ignores (13 in cadence-matching). Go behavior is not exercised from Rust.
| Rust test | Go source | Classification |
|---|---|---|
| matching_engine_go_tests_b.rs:193 (query dispatch error) | handler/engine_test.go, DispatchQueryTask mock error | Seam gap. Uncertain whether the behavior itself is missing. |
| matching_engine_go_tests_b.rs:495 (UpdateTaskListPartitionConfig error) | engine_test.go | Seam gap. |
| matching_engine_go_tests_c.rs:156 (RefreshTaskListPartitionConfig error) | engine_test.go | Seam gap. |
| matching_engine_go_tests_c.rs:216/226/232 (domainChangeCallback, registerDomainFailoverCallback x2) | handler/engine.go failover callbacks | The logic is in the private closure `start_domain_cache` (matching_runtime.rs). Behavior is untested. |
| matching_engine_go_tests.rs:200 (DescribeTaskList scripted pollers) | engine_test.go | Seam gap. Live poller history is covered by the active long-poll tests. |
| matching_engine_go_tests.rs:271 (ListTaskListPartitions scripted membership Lookup) | engine_test.go | Seam gap. The real hash-ring path is exercised by gates. |
| matching_engine_go_tests.rs:335 (errIfShardOwnershipLost, 8 subtests) | engine_test.go:421 `TestErrIfShardOwnershipLost` | Seam gap. Rust code is `shard_task_list_ownership_check` (closure). The 8 subtests are unverified, and the shard-ownership error path is untested at the unit level. |
| matching_partial_upgrade_a.rs:85 (NewConfig, 66 fields) | config/config_test.go | The Config key table is untested. A default or key-name mismatch would not be caught. |
| matching_partial_upgrade_a.rs:92 (handleErr, 14 typed errors) | handler/handler_test.go | Error mapping is per call site. This needs a wire-level check that every typed error maps to the Go-visible exception. |
| matching_partial_upgrade_e.rs:431 (active AddDecisionTask offer) | Go sync-match path | **Possible real bug.** A waiting poller gets no task, with the History peer set to 127.0.0.1:1. The ignore notes that the cause is unverified. |
| matching_partial_upgrade_e.rs:541 (TestThriftHandler InternalServiceError) | wrappers/thrift/thrift_handler_test.go | Seam gap. Thrift wrapper exception mapping and zero-value defaults are untested. |
| matching_rpc_tests.rs:1808 (non-durable Describe task_id_block/empty) | tasklist/task_list_manager.go:767-805 | Dev-only fallback (`queues: None`), per the 20261025 audit, which I did not re-verify. Production uses durable queues. Not a blocker. |

### A2. Live or mixed-gate ignores (opt-in harness)
- Harnessed:
  - MySQL ×8 in matching_mysql.rs, plus tests/matching_mysql_interop.rs:63, via run-matching-mysql-interop.py.
  - Cassandra via run-matching-cassandra-interop.py.
  - SQLite and domain-metadata via run-matching-storage-compat.py and run-matching-domain-metadata-compat.py.
  - matching_delivery_go_history via the matching-service-compat.cjs (Cargo-reported binary) and run-go-service-baseline.py `--matching-delivery` paths.
- **No harness found** (grep over tests/ and .github/):
  - `postgres_tls_live_oracle` (matching_postgres.rs:1306), env CADENCE_POSTGRES_ADDR. The production-tls-mixed workflow covers MySQL, not Postgres TLS.
  - `generated_go_history_server_accepts_native_tchannel_calls` (matching_history_tchannel.rs:2387), env CADENCE_HISTORY_PEER_BIN. Its wire coverage is indirectly provided by the mixed gates.
- **Stale harness references (confirmed):**
  - tests/matching-history-compat.cjs:44 runs `-p cadence-server --test matching_delivery`.
  - tests/run-shard-distributor-go-interop.sh:8 runs `-p cadence-server --test shard_distributor_go_interop`.
  - rust/crates/cadence-server/tests does not exist. Both tests now live in cadence-matching/tests/. The gates README:262 acknowledges this for matching_delivery.
  - These two scripts would fail "no test target", so any evidence that claims to use them must be re-checked.
  - matching-service-compat.cjs resolves the binary through Cargo, so it is probably unaffected (not verified).

### A3. Obsolete diagnostics
- `go_history_tchannel_wire_oracle_consumes_reviewed_fixture` no longer exists in source (absent at 3055d0c6c). It appears only in old evidence logs. Obsolete.
- v5 ledger row "Ignored-test audit: Running" is superseded by this report.

## B. Go Matching test coverage vs Rust (by file; the Go list has ~23 test funcs plus subtests)
Not mapped one-by-one (time budget). Checked by name/location only:
- Rust has semaphore parity tests (matching_semaphore_parity_tests.rs) and engine_go_tests a–d, partial_upgrade a–e, and longpoll_expiry/forward_poll tests.
- Not confirmed to have Rust counterparts: tasklist/adaptive_scaler_test.go (742 lines), isolation_balancer_test.go (1326), task_completer_test.go, forwarder_test.go, task_reader_test.go, task_list_limiter_test.go, liveness_test.go, handler/membership_test.go (9 funcs), engine_integration_test.go. Many may be covered by crate-internal modules (grep for liveness found only semaphore/dispatch/grpc/rpc_tests hits). A function-level mapping is still needed.

## C. Blockers vs. bounded closure
Confirmed defects:
1. Two stale harness scripts target a nonexistent `cadence-server` test target (A2). Any evidence that depends on them is invalid until the scripts are fixed.
2. The postgres_tls_live_oracle and Go-history-peer tests have no harness and have never run in an automated lane.

Needs investigation before closure:
3. matching_partial_upgrade_e.rs:431 (AddDecisionTask with a waiting poller) may be a real sync-match bug. Disprove it with a fixture that has a reachable History peer.
4. The seam-gap ignores (A1) are not evidence of a bug, but they mean these Go behaviors are unverified from Rust: shard ownership (8 subtests), domain failover callbacks, NewConfig keys, handleErr mapping, Thrift exception mapping.
   - A bounded closure is either a gated mixed-cluster probe for each behavior or a thin test seam.
5. A Go-test-to-Rust-test function-level map for section B is not done.

Do not declare Matching complete on ignores alone: items 1–5 remain open.
