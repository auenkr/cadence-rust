# Matching open parity gaps v2 (2026-10-24)

Supersedes `memory/matching-open-gaps-20261024.md` (base `e9b8e9a0e`).

**Base:** `origin/master` @ `e7285da4f` ("Record merge11 CI runs"), plus the 15 unmerged `origin/matching-*-20261024` branches listed in section 2.
**Method:** read-only. No cargo, no docker, no gate run. Every status below comes from `git grep`/`git show` on the refs named, and from the evidence files on those refs. Test names were grepped, not executed. "FIXED" means the code and a named test exist on that ref, not that the test passes in CI or in a gate.
**Audit input:** `memory/matching-go-parity-audit-v4-20261023.md` (on `origin/matching-parity-audit-v4-20261023`).

Status vocabulary: **FIXED ON MASTER**, **FIXED ON BRANCH `<name>`**, **OPEN**, **ACCEPTED DIFFERENCE** (proposed here; needs sign-off, not yet recorded as accepted).

---

## 1. What changed since v1 (`e9b8e9a0e` to `e7285da4f`)

- **P0-1 is fixed on master.** The backlog forward path now passes the wire source domain (`matching_dispatch.rs:499`, `:510`). Commit `d1baaf94b` records the CI evidence. v1 said the caller still passed the list domain. That was true at `e9b8e9a0e` only.
- **P1-4 is fixed on master** (unit level). `Scaler::start`/`stop` exist (`matching_scaler.rs:224`, `:233`) and the idempotent lifecycle test is un-ignored (`matching_scaler_go_tests.rs:1064`).
- **P1-6 is fixed on master.** The TTL/2 ticker exists (`matching_registry.rs:1466`), and the 1.5×TTL idle-pump test is present (`matching_registry.rs:3170`).
- **P1-11 is fixed on master.** `matching_standby_query_poll_forwards_to_parent_without_claiming_backlog` (`matching_dispatch.rs:2387`) and the completion-failure counter test (`matching_standby.rs:203`) are on master.
- **P1-5 is partly fixed on master.** The caller deadline is honored in acquire and grant (commit `4940e5809`). Three of the audit's tests are un-ignored: `:672` (deadline while starting), `:691` (grant stops at deadline), `:709` (unknown write outcome).
- **P1-7 durable path is wired on master** (`grpc_matching.rs:3460`, `matching_registry.rs:1680`). The non-durable isolation-group gap is un-ignored on `matching-describe-gaps-20261024`.
- **P0-2 and P1-16 are confirmed on master.** Update and refresh both reject only kinds 2|3 (`grpc_matching.rs:3036`, `:3163`). Forward polls select on released pollers (`matching_dispatch.rs:921`).
- **Gate evidence is now bound to older commits than v1 assumed.** Final2 is bound to `4fef00251`, which is 199 commits behind master. Final3 is bound to `bafd65a77`, which is 93 commits behind. No lane has run at current head.
- **Two branch-only "new" markers are artifacts.** `cass-rate-diag`, `cfgstore-retry-diag`, `gate-final3`, and `partial-upgrade-e` are based on older commits. Diffing them against master shows master's later fixes (scaler start/stop, semaphore deadline, unknown outcome) as missing. Those are not new gaps.

---

## 2. Branch inventory

### 2a. Unmerged `origin/matching-*-20261024` (15, `git branch -r --no-merged origin/master`)

| branch | ahead | what it carries |
|---|---|---|
| `matching-rowskip-lateStop-20261024` | 2 | Un-ignores 2 semaphore gap tests (row skip, late stop). Test-only. |
| `matching-describe-gaps-20261024` | 2 | Un-ignores 2 non-durable describe gap tests (poller rate, isolation-group metrics). Fixes P1-7 non-durable path. |
| `matching-lifecycle-gaps-20261024` | 2 | Un-ignores 5 semaphore/engine lifecycle gap tests. |
| `matching-longpoll-diag-20261024` | 2 | Diagnosis for lane 9 (code reading). Adds one new ignored test, `matching_longpoll_expiry_parity.rs`. No production change. |
| `matching-flaky-reader-20261024` | 3 | Makes the reader dispatch test order deterministic. Test-only. Not an audit item. |
| `matching-panic-recovery-20261024` | 1 | Captures handler, membership and semaphore-stop panics (Go CapturePanic parity). Not an audit item. |
| `matching-hash32-short-20261024` | 2 | Go int8 sign extension for 0–4 byte hash32. Not an audit item. |
| `matching-gate-controls-20261024` | 1 | Lane 9 controls C0–C2 (harness-only). |
| `matching-cass-probe-ownercheck-20261024` | 1 | Lane 4 harness fix: owner agreement check around the paced rate probe. Not gate-run. |
| `matching-cass-rate-diag-20261024` | 4 | Lane 4 diagnosis (code reading). Based on gate-final3. |
| `matching-cfgstore-retry-diag-20261024` | 4 | Lane 9 diagnosis. Refutes the "isolation seeded for Rust only" hypothesis. Based on gate-final3. |
| `matching-gate-final3-20261024` | 3 | Lane 4 and lane 9 evidence (both FAILED). Based on `bafd65a77`. |
| `matching-sticky-query-20261024` | 3 | Sticky query parity tests and evidence. |
| `matching-partial-upgrade-e-20261024` | 4 | Ports Go matching rows 33–40 (PARTIAL cases). Adds parity markers. |
| `matching-auto-config-hint-20261024` | 1 | Gate probe for poll AutoConfigHint. |

### 2b. Other unmerged branches (not in the `matching-*-20261024` pattern, 17 total)

Not part of this scan's scope. Listed for completeness: `matching-missing-tests-20261023` (2), `matching-gate-final-20261023` (1), `matching-backlog-forward-v2-20261022` (3), `t3/matching-integration-20261021` (2), `matching-backlog-forward-20261022` (2), `matching-go-parity-fixes-20261022` (2), `audit/matching-verify-20261022` (1), `audit/matching-forward-plan-20261022` (1), `codex/matching-postgres-parity` (6), `audit/matching-matcher-20261021` (1), `audit/matching-metrics-20261021` (1), `audit/matching-go-parity-20261021` (1), `t3/matching-shard-processor-integration` (9), `t3/matching-source-candidate` (2), `t3/matching-sql-shard-routing` (2), `t3/matching-semaphore-durable` (2), `t3/ringpop-wire-contract` (1). Ahead counts are against `origin/master`.

---

## 3. Audit-v4 P0/P1 items

Counts: **FIXED ON MASTER 7 · FIXED ON BRANCH 1 · OPEN 11 · ACCEPTED DIFFERENCE 0** (total 19).

| id | status | evidence (ref) | still needed |
|---|---|---|---|
| P0-1 backlog forward source domain | **FIXED ON MASTER** | `matching_dispatch.rs:499`, `:510`; test `matching_accepted_backlog_second_hop_keeps_source_domain` (`:2610`); commit `d1baaf94b` | Gate run via P1-13 (no two-node backlog run at head yet) |
| P0-2 partition-config kind | **FIXED ON MASTER** | `grpc_matching.rs:3036` (update), `:3163` (refresh); comment at `:3035` (INVALID→Normal, only 2|3 reject) | Test-only: a test that kind 0 is accepted and 2/3 rejected. Not located on master |
| P0-3 OpenFeature dynamic config | **OPEN** (conditional) | Explicit bail `launcher.rs:613-614` ("no provider plugin is ported") | Decision: port the Go plugin, or accept as deployment-excluded and document it |
| P1-1 two-node MySQL gate | **OPEN** | Final2 lane 2 exit 0 (`memory/evidence/matching-gate-final2-20261023/2-mysql-two-node/run.exit`), bound to `4fef00251`. The failing runs cited in v1 (`matching-mixed-gates-20261023` exit 1/143) are older | Re-run two-node MySQL control at head, one gate at a time |
| P1-2 no gate bound to head | **OPEN** | Final2 base `4fef00251` (199 behind master). Final3 base `bafd65a77` (93 behind). `binary-shas.txt` in both | Full re-run of each lane from `cargo-target-final`, with the binary sha recorded |
| P1-3 metrics matrix | **OPEN** | Emit sites exist: `matching_persistence_limiter.rs:43`, `:68-73`; row/payload size `cadence-service-core/src/matching_metrics.rs:406-407`; test `matching_persistence_limiter.rs:666-704` | Comparison table against `go/common/persistence/wrappers/metered/base.go` (lines 103, 125, 141, 163, 305, 321). Test-only; no Go run needed to draft |
| P1-4 adaptive scaler | **FIXED ON MASTER** (unit) | `matching_scaler.rs:224`, `:233`; `matching_scaler_go_tests.rs:1064` (`matching_adaptive_scaler_lifecycle_start_stop_is_idempotent`), un-ignored; commit `aadd8f7e7` | Gate lane that enables `enableAdaptiveScaler` (none exists). Or accept and document |
| P1-5 semaphore failure paths | **OPEN** (partly fixed) | Fixed on master: deadline `:672`, `:691`, unknown outcome `:709`; commit `4940e5809`. Fixed on branch `matching-rowskip-lateStop-20261024`: nil/unknown rows, late stop. Fixed on branch `matching-lifecycle-gaps-20261024`: late stop. Still ignored on master: validate params (`:640`), page tokens (`:660`), acquire during load (`tests/matching_engine_go_tests_d.rs:518`) | Production: validate TTL/params in `Manager::new`; thread page tokens through scan; deadline in acquire during load. Then merge the two branches |
| P1-6 liveness | **FIXED ON MASTER** | TTL/2 ticker `matching_registry.rs:1466`; `liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl` (`:3170`) | None |
| P1-7 poller counts per group | **FIXED ON BRANCH `matching-describe-gaps-20261024`** (durable path on master) | Master: `count_by_group` wired at `grpc_matching.rs:3460` and `matching_registry.rs:1680`. Branch: un-ignores `matching_rpc_non_durable_describe_reports_isolation_group_metrics` (commit `c460d1d5e`) | Merge the branch and let CI run it |
| P1-8 ListTaskListPartitions order | **OPEN** | Ignored: `tests/matching_engine_go_tests.rs:271` (`list_task_list_partitions_success_owner_hosts_match_go_lookups`). No Go-vs-Rust ring-order fixture | Mixed Go/Rust fixture that dumps ring order for the same membership (needs a mixed gate, run alone) |
| P1-9 Rust-only fault-injection alias | **OPEN** | `launcher.rs:706` still reads `"matching.errorInjectionRate"` | One-line removal of the alias, plus a unit test that the alias is ignored |
| P1-10 GetTaskListsByDomain | **FIXED ON MASTER** | Durable path reads `matchingReturnAllTaskListKinds` (`grpc_matching.rs:2820`). Legacy frontend enumeration removed (source memory: returns empty) | Optional: confirm the tests named in v1 (`matching_rpc_tests.rs:2680`, `:3525`) in the next gate. Not re-run here |
| P1-11 standby completion | **FIXED ON MASTER** | `matching_dispatch.rs:2387` (`matching_standby_query_poll_forwards_to_parent_without_claiming_backlog`); `matching_standby.rs:203` (`standby_cancelled_checker_counts_one_completion_failure`). Commits `e15ab7b2a`, `c4dda59da` | None |
| P1-12 matcher and manager unit coverage | **OPEN** | Coverage: `memory/matching-go-test-inventory-20261023.md` (on master). Branch progress: `matching-partial-upgrade-e-20261024` ports rows 33–40 | Port the next block of Go `matcher_test.go` cases (start with `TestRemoteSyncMatch`), tracked by inventory row |
| P1-13 backlog forward at runtime | **OPEN** | Final2 lane 8 (two-node backlog) exit 0, bound to `4fef00251` (`8-backlog-two-node/run.exit`). v1 said no passing two-node backlog run existed; that is corrected here. Not at head, so it does not close the audit item. Blocked by P1-1 | Once P1-1 passes, run backlog forward on two nodes and record `backlog-forward.json` |
| P1-14 PostgreSQL GC and domain checks | **OPEN** | Final2 lane 3 exit 0 (`3-postgres/run.exit`), bound to `4fef00251`. Its `EVIDENCE.md` shows `domain metadata: null` (check not collected) | PostgreSQL lane at head with GC and domain-metadata SQL checks enabled |
| P1-15 TLS | **OPEN** | gRPC TLS: 8 tests (`cadence-service-core/src/grpc_tls.rs`). HTTP TLS: 5 tests (`launcher.rs`). TChannel TLS: no implementation or test found by grep | Scope decision. If in scope: TChannel TLS handshake test. If not: document as deployment-excluded |
| P1-16 domain-failover release | **FIXED ON MASTER** | `matching_dispatch.rs:921-923` (forward leg selects on released pollers); `matching_failover_releases_active_forward_resolver_and_poller_registration` (`:2449`) | Optional: compare registry callers with Go `engine.go:1707-1762` |

---

## 4. Ignored `#[ignore = "parity gap"]` tests on master (41)

Found with `git grep "parity gap" origin/master -- rust/` (41 attributes, 41 test fns). Status counts: **FIXED ON BRANCH 8 · ACCEPTED DIFFERENCE (proposed) 8 · OPEN 25**. No ignored test is fixed on master, since master still carries all 41 attributes.

Accepted-difference rows are proposed only. They need sign-off before they are recorded as accepted. Each one's ignore reason says Rust has no equivalent object, or the Go case cannot be expressed through the public API.

### 4a. Ignored on master, un-ignored on a branch (8 unique)

| test | file | branch | evidence |
|---|---|---|---|
| `matching_rpc_non_durable_describe_reports_poller_rate` | `matching_rpc_tests.rs:956` | `matching-describe-gaps-20261024` | `c460d1d5e` |
| `matching_rpc_non_durable_describe_reports_isolation_group_metrics` | `matching_rpc_tests.rs:983` | `matching-describe-gaps-20261024` | `c460d1d5e` |
| `matching_semaphore_scan_skips_nil_and_unknown_rows` | `matching_semaphore_parity_tests.rs:666` | `matching-rowskip-lateStop-20261024` | `071544885` |
| `matching_semaphore_late_stop_unregisters_only_itself` | `matching_semaphore_parity_tests.rs:741` | `matching-rowskip-lateStop-20261024` and `matching-lifecycle-gaps-20261024` | `071544885`, `a574fe5f3` |
| `is_shutting_down_is_false_until_stop` | `tests/matching_engine_go_tests_b.rs:356` | `matching-lifecycle-gaps-20261024` | `a574fe5f3` |
| `get_or_create_semaphore_manager_starts_unstarted_registered_manager` | `tests/matching_engine_go_tests_d.rs:278` | `matching-lifecycle-gaps-20261024` | `a574fe5f3` |
| `get_or_create_semaphore_manager_refused_while_engine_shuts_down` | `tests/matching_engine_go_tests_d.rs:303` | `matching-lifecycle-gaps-20261024` | `a574fe5f3` |
| `stop_semaphore_managers_stops_every_bucket_on_engine_stop` | `tests/matching_engine_go_tests_d.rs:459` | `matching-lifecycle-gaps-20261024` | `a574fe5f3` |

### 4b. Ignored on master, proposed accepted difference (8)

| test | file | why it is a candidate | still needed |
|---|---|---|---|
| `matching_rpc_semaphore_cancelled_context_is_internal_error` | `matching_rpc_tests.rs:1503` | tonic drops cancelled handler futures; no Go-equivalent context check | Sign-off |
| `get_tasklists_not_owned_returns_only_non_owned_managers` | `tests/matching_engine_go_tests_b.rs:366` | Rust `unload_non_owned` returns `()`. Ownership is already asserted through the cache (`shut_down_tasklists_not_owned_unloads_only_moved_owner`) | Sign-off |
| `refresh_workflow_tasks_canceled_context_is_propagated` | `tests/matching_engine_go_tests_c.rs:350` | No cancellation token in Rust `refresh_workflow_tasks` (same tonic category) | Sign-off |
| `refresh_workflow_tasks_nil_execution_is_forwarded` | `tests/matching_engine_go_tests_c.rs:368` | `AllocatedTask` always carries workflow and run ids; nil cannot be produced through the API | Sign-off |
| `setup_executor_with_empty_config_installs_noop_executor` | `tests/matching_engine_go_tests_c.rs:378` | Rust has no executor object | Sign-off |
| `shard_distributor_onboarded_partial_percentage_is_engine_wide` | `tests/matching_engine_go_tests_c.rs:411` | Rust gates per task list by fingerprint bucket by design | Sign-off |
| `go_handle_err_passes_typed_errors_through_unchanged` | `tests/matching_partial_upgrade_a.rs:87` | No generic `handleErr`; each RPC maps errors at its call site | Sign-off |
| `go_handler_start_calls_engine_start_once_and_stop_calls_engine_stop_once` | `tests/matching_partial_upgrade_a.rs:95` | No Handler object with Start/Stop | Sign-off |

### 4c. Ignored on master, open (25)

| test | file | gap (from the ignore reason) | still needed |
|---|---|---|---|
| `go_get_tasks_batch_size_returns_raw_non_positive_values` | `matching_reader.rs:1118` | No raw `GetTasksBatchSize` accessor; 0/-5 replaced with 1000 | Production accessor, or accept |
| `loaded_owner_reports_mocked_tracker_qps_after_arrivals` | `matching_registry.rs:3462` | Owner QPS tracker is wall-clock EMA with no injection seam | Test seam |
| `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` | `matching_rpc_tests.rs:1015` | Non-durable status has no ack manager | Production |
| `matching_rpc_semaphore_rejects_new_buckets_while_shutting_down` | `matching_rpc_tests.rs:1496` | Membership has no shutdown state | Production |
| `go_handler_describe_task_list_engine_error_is_internal_service_error` | `matching_rpc_tests.rs:4893` | No injectable engine failure | Test seam |
| `go_handler_list_task_list_partitions_engine_error_is_internal_service_error` | `matching_rpc_tests.rs:4899` | No injectable engine failure | Test seam |
| `go_handler_update_task_list_partition_config_engine_error_is_internal_service_error` | `matching_rpc_tests.rs:4905` | No injectable engine failure | Test seam |
| `go_handler_refresh_partition_config_engine_error_is_internal_service_error` | `matching_rpc_tests.rs:4911` | No injectable engine failure | Test seam |
| `matching_semaphore_new_manager_validates_params` | `matching_semaphore_parity_tests.rs:640` | `Manager::new` clamps invalid TTL; P1-5 | Production: validation |
| `matching_semaphore_scan_threads_page_tokens` | `matching_semaphore_parity_tests.rs:660` | Scan returns `Vec`, no page tokens; P1-5 | Production |
| `get_task_lists_by_domain_success_reports_poller_identity` | `tests/matching_engine_go_tests.rs:200` | No public seam to inject poller identities | Test seam |
| `list_task_list_partitions_success_owner_hosts_match_go_lookups` | `tests/matching_engine_go_tests.rs:271` | Owners come from the real hash ring; P1-8 | Mixed Go/Rust fixture |
| `err_if_shard_ownership_lost_go_subtests_not_reproduced` | `tests/matching_engine_go_tests.rs:335` | Ownership check is a private closure; 8 Go subtests | Test seam, or make the check callable from tests |
| `query_workflow_failed_to_dispatch_query_task_is_error` | `tests/matching_engine_go_tests_b.rs:192` | No dispatch-failure seam; non-sticky query waits for deadline | Production and test seam |
| `update_task_list_partition_config_tasklist_manager_error` | `tests/matching_engine_go_tests_b.rs:467` | No seam to fail the durable partition update | Test seam |
| `refresh_task_list_partition_config_manager_error_is_propagated` | `tests/matching_engine_go_tests_c.rs:151` | No seam to fail the registry refresh | Test seam |
| `domain_change_callback_updates_failover_version_and_releases_eligible_pollers` | `tests/matching_engine_go_tests_c.rs:209` | Callback is a private closure; release itself works (P1-16) | Test seam |
| `register_domain_failover_callback_catch_up_without_updates_keeps_version` | `tests/matching_engine_go_tests_c.rs:219` | Catch-up callback not reachable from `tests/` | Test seam |
| `register_domain_failover_callback_catch_up_advances_version_to_highest` | `tests/matching_engine_go_tests_c.rs:225` | Same as above | Test seam |
| `refresh_workflow_tasks_entity_not_exists_returns_ok` | `tests/matching_engine_go_tests_c.rs:315` | Typed wire detail needs crate-private `grpc::detailed_status` | Test seam (`pub(crate)` test helper) |
| `refresh_workflow_tasks_already_completed_is_propagated` | `tests/matching_engine_go_tests_c.rs:344` | Same as above | Test seam |
| `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline` | `tests/matching_engine_go_tests_d.rs:518` | Acquire waits for startup load with no deadline; P1-5 | Production |
| `go_new_config_every_field_has_a_rust_reader` | `tests/matching_partial_upgrade_a.rs:80` | No single Config struct; keys read per module | Test-only key table, or accept |
| `poller_history_callback_fires_on_start_and_end_with_identity` | `tests/matching_partial_upgrade_b.rs:301` | `poller::Manager` has no history-change callback | Production |
| `poller_cancel_reports_whether_the_poll_is_known` | `tests/matching_partial_upgrade_b.rs:339` | `Manager::cancel` returns `()`, Go returns bool | Production (small) |

Total rows in 4a+4b+4c: 8 + 8 + 25 = 41.

### 4d. New parity-gap markers on branches (not on master)

- `matching-longpoll-diag-20261024`: new ignored test in `tests/matching_longpoll_expiry_parity.rs`: `decision_started_after_long_poll_window_still_returns_task`. Not run. The Go control has not been run either.
- `matching-gate-controls-20261024`, `matching-cass-probe-ownercheck-20261024`, `matching-sticky-query-20261024`, `matching-auto-config-hint-20261024`: a new ignored test about a 2s `poll_for_activity_task` after a durable `add_activity_task` persists one row (store count 1). This is the same marker on all four branches.
- `matching-partial-upgrade-e-20261024`: several new markers (DispatchTask active-offer, Go root Start config push, Thrift handler error mock, FileBased oracle reload). Not inspected individually here.

---

## 5. Gate lanes 1–9

Binding column: the commit the binary was built from. "Ancestor distance" is commits between that commit and current master `e7285da4f`.

| lane | scenario | last known exit | evidence path | binding / note |
|---|---|---|---|---|
| 1 | MySQL single | **0** (attempt 1 failed with exit 1) | `memory/evidence/matching-gate-final2-20261023/1-mysql-single/` | Final2, `4fef00251`, 199 behind |
| 2 | MySQL two-node | **0** | `memory/evidence/matching-gate-final2-20261023/2-mysql-two-node/` | Final2, `4fef00251`, 199 behind. Older mixed-run failures in `matching-mixed-gates-20261023` are stale |
| 3 | PostgreSQL | **0** | `memory/evidence/matching-gate-final2-20261023/3-postgres/` | Final2, 199 behind. Domain-metadata check `null` (incomplete) |
| 4 | Cassandra | **1** (final3 attempt 3) | Final3 (branch `matching-gate-final3-20261024`): `memory/evidence/matching-gate-final3-20261024/4-cassandra/` (`run.exit` 1, `NOTES.md`). Final2 also 1: `memory/evidence/matching-gate-final2-20261023/4-cassandra/` | Final3, `bafd65a77`, 93 behind. Failing assertion: `DescribeTaskList must report the reported backing rate: 100000 !== 1` (`tests/matching-service-compat.cjs:1168`) |
| 5 | Thrift + MySQL | **0** | `memory/evidence/matching-gate-final2-20261023/5-thrift-mysql/` | Final2, 199 behind |
| 6 | shard-distributor fixture | **0** | `memory/evidence/matching-gate-final2-20261023/6-sd-fixture/` | Final2, 199 behind |
| 7 | shard-distributor real | **0** | `memory/evidence/matching-gate-final2-20261023/7-sd-real/` | Final2, 199 behind |
| 8 | backlog two-node | **0** | `memory/evidence/matching-gate-final2-20261023/8-backlog-two-node/` | Final2, 199 behind |
| 9 | configstore (MySQL + `--matching-configstore`) | **1** (final2, and final3 again) | Final2: `memory/evidence/matching-gate-final2-20261023/9-configstore/`. Final3 (branch `matching-gate-final3-20261024`): `memory/evidence/matching-gate-final3-20261024/9-configstore/` (`NOTES.md`) | Final3 failure: `workflow-task-retry: timed out` at `tests/go-sdk/mixed_matching.go:84`, after the earlier SDK scenarios passed |

### 5a. Gate evidence for the open failures

**Lane 4 (Cassandra, paced rate).** Evidence: `memory/evidence/matching-cass-rate-diag-20261024/notes.md` (on branch `matching-cass-rate-diag-20261024`, a code-reading diagnosis, no gate). The diagnosis: the describe was answered by a second in-memory owner of the `paced-*` task list. That owner loaded the list fresh with the default limiter (100000). The probe never checks ring agreement. This is classified as a harness/topology precondition, not a Rust semantic gap. Confidence on the mechanism is high; on the trigger it is moderate.
- Fix on branch `matching-cass-probe-ownercheck-20261024` (commit `7458a1e9b`, one commit ahead of master). It adds an owner-agreement check before the poll, before the describe, and on rate mismatch. Checked with `node --check` only. **Not gate-run.**
- Still needed: run lane 4 once, from a clean `CARGO_TARGET_DIR`, and read `paced-owner.json`. Then decide whether the owner-changed message fires.

**Lane 9 (configstore).** Controls: `memory/evidence/matching-gate-controls-20261024/` on branch `matching-gate-controls-20261024` (notes in `memory/evidence/matching-gate-controls-20261024/NOTES.md`):

| control | scenario | run.exit |
|---|---|---|
| C0 | lane 9 minus `--matching-configstore` (file config) | **0** |
| C1 | configstore, 2s long-poll seed removed | **0** |
| C2 | exact lane 9 command (configstore + 2s seed) | **1** |

The 2s `matching.longPollExpirationInterval` seed is the only Rust-visible difference between C1 and C2. Each control is a single run, so intermittency is not excluded.
- Diagnosis: `memory/evidence/matching-longpoll-diag-20261024/notes.md` on branch `matching-longpoll-diag-20261024` (base master `e7285da4f`). Code reading only. Verdict: a real Rust divergence from Go, and a plausible contributor, but not proven as the sole cause. Rust ends the in-flight `RecordDecisionTaskStarted` at the long-poll window; Go does not. The ignored parity test is `decision_started_after_long_poll_window_still_returns_task`. The proposed fix (Rust, not applied) is to bound the long-poll window to the wait for a task only. The Go-control run (2s window, 900ms start delay) has not been done.
- A separate hypothesis, that the seed turns on isolation for Rust only, is refuted by `memory/evidence/matching-cfgstore-retry-diag-20261024/notes.md` (branch `matching-cfgstore-retry-diag-20261024`). The Go nodes read the same keys from their file config.
- Still needed: apply the proposed fix on a branch, un-ignore the parity test, and re-run C2. Then run the Go-history control.

---

## 6. Counts

- **Audit-v4 P0/P1 (19):** FIXED ON MASTER 7 (P0-1, P0-2, P1-4, P1-6, P1-10, P1-11, P1-16) · FIXED ON BRANCH 1 (P1-7) · OPEN 11 (P0-3, P1-1, P1-2, P1-3, P1-5, P1-8, P1-9, P1-12, P1-13, P1-14, P1-15) · ACCEPTED DIFFERENCE 0.
- **Ignored parity-gap tests on master (41):** FIXED ON BRANCH 8 · ACCEPTED DIFFERENCE (proposed) 8 · OPEN 25 · FIXED ON MASTER 0.
- **Gate lanes (9):** exit 0 on lanes 1, 2, 3, 5, 6, 7, 8 (final2, all bound to `4fef00251`). Exit 1 on lanes 4 and 9 (final3 and final2 for lane 9). Nothing has run at current head.

## 7. Next steps (narrow, cheapest first)

1. P1-9: remove the `matching.errorInjectionRate` alias at `launcher.rs:706`, with a unit test. Test-only plus one line.
2. Merge the three branches that un-ignore tests (`matching-describe-gaps-20261024`, `matching-lifecycle-gaps-20261024`, `matching-rowskip-lateStop-20261024`). Overlap is the late-stop test. Fixes 8 ignored tests and closes P1-7's non-durable part.
3. Lane 4: one gate run of `matching-cass-probe-ownercheck-20261024` from a clean `CARGO_TARGET_DIR`.
4. Lane 9: apply the long-poll fix from `matching-longpoll-diag-20261024`, un-ignore the parity test, re-run C2, then run the Go-history control.
5. P1-1 and P1-2: after lanes 4 and 9 pass, re-run every lane at one head, one gate at a time, with the binary sha recorded.
6. Sign off or reject the 8 proposed accepted differences in section 4b. Decide P0-3 (OpenFeature) and P1-15 (TChannel TLS scope).
