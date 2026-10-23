# matching-partial-upgrade-e-20261024 — evidence

Branch `matching-partial-upgrade-e-20261024` (from origin/master `bafd65a77`). Shared map `memory/matching-go-test-map-20261023.md` is NOT edited.

Selection: PARTIAL rows of the per-test table in document order, skipping engine_test, engine_integration_test, matcher_test, forwarder_test, task_completer_test, and membership tests (`handler/membership_test.go`, `go/common/membership/**`). 48 rows remain after the filter; rows 33–40 of that filtered list are below. Row 40 is `dynamicconfig/file_based_*`, not membership.

CI (GitHub Actions `rust-ci.yml`, clippy with `-D warnings` then `cargo test -p <crate>`):

| Crate | Run | Result |
|---|---|---|
| cadence-persistence | https://github.com/auenkr/cadence-rust/actions/runs/40035709390 | success |
| cadence-matching (first) | https://github.com/auenkr/cadence-rust/actions/runs/40035875563 | 1 failure (dispatch case, see below) |
| cadence-matching (diagnostic) | https://github.com/auenkr/cadence-rust/actions/runs/40036277982 | 1 failure, poller got empty response |
| cadence-matching (final) | https://github.com/auenkr/cadence-rust/actions/runs/40036579366 | success |
| cadence-service-core | https://github.com/auenkr/cadence-rust/actions/runs/40036100716 | success |

## Table

| Filtered # | Map line | Go test | Rust test(s) | Old → New | Run URL |
|---|---|---|---|---|---|
| 33 | L261 | tasklist/task_list_manager_test.go `TestTaskListManagerGetTaskBatch` (974) | cadence-persistence `matching_queue::matching_queue_get_task_batch_row_count_after_ten_acks_matches_go` (new, in-file); existing `matching_queue_get_task_batch_bounds_follow_read_range` | PARTIAL (B) → PARTIAL (B). Row counts 1200 → 1190 now asserted; second-manager batch asserted. Manager/mock loop not reproduced. | 40035709390 |
| 34 | L264 | `TestTaskExpiryAndCompletion` (1241) | cadence-persistence `matching_queue::matching_queue_expired_half_fixture_leaves_go_remaining_counts` (new, in-file) | PARTIAL (B) → PARTIAL (B). Expired-half remaining counts 20→10→0 asserted; GetTask/reader path not in the same test. | 40035709390 |
| 35 | L266 | `TestRefreshTaskListPartitionConfig` (1380) | cadence-matching tests: `go_refresh_partition_config_from_request_ignores_older_version`, `go_refresh_partition_config_from_database_replaces_cache` | PARTIAL → PORTED. All four Go cases mapped. Failed-read case: Rust returns NotFound (missing row), Go injects "some error"; cache-unchanged asserted. | 40035875563 (first), final 40036579366 |
| 36 | L268 | `TestRefreshTaskListPartitionConfigConcurrency` (1703), `TestUpdateTaskListPartitionConfigConcurrency` (1720) | `go_refresh_partition_config_concurrency_keeps_highest_version` (100 pushes → 99), `go_update_partition_config_concurrency_advances_once_per_update` (100 updates → 100) | PARTIAL → PORTED | 40036579366 |
| 37 | L269 | `TestManagerStart_RootPartition` (1739), `_NonRootPartition` (1777) | `go_manager_start_root_adopts_persisted_partition_config`, `go_manager_start_non_root_reads_root_partition_config`; ignored `go_manager_start_root_pushes_config_to_child_partitions` | PARTIAL → PARTIAL. Start load ported. Root push to child owners is an ignored parity gap (`matching_registry.rs:1959` is the only RefreshTaskListPartitionConfigRequest sender). | 40036579366 |
| 38 | L270 | `TestDispatchTask` (1822, 8 cases) | ignored `go_dispatch_task_active_offer_without_error_reaches_poller` | PARTIAL → PARTIAL. Ignored: AddDecisionTask returns Ok but the waiting poller gets an empty response at its ~7s wait (`poller_wait_time_in_ms 6995`). Suspected cause unverified: History peer unreachable in fixture. MustOffer-error and standby-completion cases not ported. | 40036277982 (diagnostic), 40036579366 |
| 39 | L280 | `TestThriftHandler` (wrappers/thrift, 10 sub) | `go_thrift_handler_routes_each_tested_method` (routing); ignored `go_thrift_handler_internal_error_maps_to_internal_service_error` | PARTIAL → PARTIAL. Routing for all 10 Go methods asserted. Internal-error and zero-default mapping ignored: no injectable handler failure in the Rust dispatcher. | 40036579366 |
| 40 | L294 | `go/common/dynamicconfig/file_based_oracle_test.go` (4), `file_based_client_test.go` (1) | cadence-service-core `tests/matching_partial_upgrade_e.rs::file_based_oracle_poll_reload_observes_rewritten_file` (ignored) | PARTIAL → PARTIAL. Poll reload is an ignored parity gap: `DynamicConfigOverlay::from_file` is a one-shot snapshot. Other oracle/client subtests not re-ported in this pass. | 40036100716 |

Counts for this slice: 8 rows. PORTED 2 (35, 36). PARTIAL 6 (33, 34, 37, 38, 39, 40). New ignored tests: 4 (`go_manager_start_root_pushes_config_to_child_partitions`, `go_thrift_handler_internal_error_maps_to_internal_service_error`, `go_dispatch_task_active_offer_without_error_reaches_poller`, `file_based_oracle_poll_reload_observes_rewritten_file`).

## Open items

- Row 38 dispatch case: root cause not confirmed. A next step would be to point the fixture at a reachable History stub and see whether the sync match then delivers.
- Row 39 internal-error mapping needs a seam to inject a handler failure into `thrift_matching::dispatch`.
- Row 37 root push needs a two-owner harness.
