# Matching Go unit-test inventory → Rust port (2026-10-23)

Branch: `matching-go-tests-port-20261023` (from origin/master a028331fb).
Evidence: `memory/evidence/matching-go-tests-port-20261023/`.

## Method and limits (read this first)

- Scope: every `func Test*` and every testify suite method under `go/service/matching/**/*_test.go`,
  plus Matching-relevant `go/common` tests (none found that are Matching-specific: the `go/common`
  files that mention "matching" are proto/thrift mappers, persistence, and generic util/stats tests,
  so they are out of scope and listed at the bottom).
- The "Asserts" column is a paraphrase of the Go test name, its subtest names, and selective reads of
  the Go body. It is NOT a line-by-line audit of every assertion in the ~16k lines of Go tests.
- Rust "equivalent" column is by behavior and test name. `COVERED` means an existing Rust test
  asserts the same behavior; `PARTIAL` means Rust covers some of it; `MISSING` means no Rust test
  exists and the reason is given; `NOT-ASSESSED` means I did not read the Go body and have no
  defensible mapping yet.
- Rust test file names are under `rust/crates/cadence-matching/src/` unless a crate is named.
- Ported tests are the only new Rust code in this branch. None of them have run yet at the time this
  file was written; the run status is in the evidence directory.

## Status totals

Counts are per Go test function / suite method (not per t.Run subtest). Totals are filled in at the
end of this file after the final run.

## Go test → Rust mapping

### config/config_test.go

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestNewConfig (45) | every dynamic-config key maps to its Config field (~90 keys) | matching_config.rs::go_defaults_when_unset, configured_values_override_defaults (7 of ~90 helpers) | PARTIAL: Rust has no single Config struct; values are read via helper fns in matching_config.rs and call sites |
| TestGetTasksBatchSizeValidation (211) | raw GetTasksBatchSize pass-through for 1000/1/0/-5, for decision and activity | matching_reader.rs `batch_size`/`read_batch_size` (no test asserts the raw 0/-5 pass-through) | PARTIAL |

### handler/context_test.go

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestHandleErrNil (38) | nil error stays nil | grpc_matching.rs error→Status mapping (no nil case) | NOT-ASSESSED |
| TestHandleErrKnowErrors (48) | known Cadence error types map to their RPC error | grpc.rs:182 `BadRequestError`→invalid_argument; grpc_matching.rs:159 ManagerError mapping | PARTIAL |
| TestHandleErrUncategorizedError (129) | unknown error becomes internal | grpc_matching.rs::handler_go_tests::matching_handler_uncategorized_error_is_internal (semaphore_status) | PORTED (port2 20261024) |

### handler/engine_test.go (top-level functions; t.Run subtests inside)

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestGetTaskListsByDomain (73) | lists task lists per domain across subtests | matching_rpc_tests.rs::matching_rpc_domain_listing_uses_loaded_matching_managers, matching_domain_listing_filters_kinds_and_collapses_partition_roots | COVERED |
| TestListTaskListPartitions (240) | partition listing per task list | matching_rpc_tests.rs::matching_rpc_lists_configured_write_partitions, matching_rpc_scopes_partition_counts_to_task_list_and_type | COVERED |
| TestCancelOutstandingPoll (353) | cancel poll checks ownership | matching_rpc_tests.rs::matching_rpc_cancel_outstanding_poll_checks_task_list_ownership | COVERED |
| TestErrIfShardOwnershipLost (421) | ownership-lost error from shard check | matching_rpc_tests.rs::durable_queue_preserves_typed_not_owned_error_from_shutdown_policy; matching_registry.rs::shard_policy_accepts_local_assignment_and_rechecks_before_publish | PARTIAL |
| TestIsExcludedFromShardDistributor (530) | exclusion rules for shard distributor | matching_shard_routing.rs::excluded_and_missing_spectator_routes_use_live_ring_snapshot, excluded_cached_owner_skips_ring_check_but_new_owner_does_not | PARTIAL |
| TestRespondQueryTaskCompleted (585) | query task completion | matching_query.rs::offer_is_delivered_once_and_completed_by_task_id, completing_an_unknown_task_reports_not_found | COVERED |
| TestQueryWorkflow (637) | query dispatch to poller, timeouts | matching_rpc_tests.rs::matching_rpc_delivers_query_to_decision_poller, matching_query_without_poller_waits_for_deadline (matching_rpc_query_without_poller_waits_for_deadline) | COVERED |
| TestWaitForQueryResult (780) | waiting for query result, failure paths | matching_query.rs::failed_query_reports_the_worker_message, internal_error_fails_the_waiting_query | COVERED |
| TestIsShuttingDown (918) | shutdown flag reporting | matching_rpc_tests.rs::matching_rpc_domain_failover_releases_blocked_polls (indirect); matching_runtime.rs::shutdown_drain_provider_reads_live_unfiltered_value | PARTIAL |
| TestGetTasklistsNotOwned (942) | list only non-owned task lists | matching_registry.rs::matching_membership_guards_creation_and_unloads_moved_owners | PARTIAL |
| TestShutDownTasklistsNotOwned (978) | shut down non-owned task lists | matching_registry.rs::assigned_eligible_owner_unloads_when_local_processor_snapshot_is_lost; registry unload_non_owned | PARTIAL |
| TestUpdateTaskListPartitionConfig (1025) | partition config update (sub-cases) | matching_rpc_tests.rs::matching_rpc_manual_partition_update_rejected_while_scaler_enabled, matching_rpc_native_partition_cache_updates_refreshes_and_recovers | PARTIAL |
| TestRefreshTaskListPartitionConfig (1249) | refresh partition config from persistence | matching_rpc_tests.rs::matching_rpc_partition_update_refreshes_remote_read_owner_and_ignores_write_only_owner; matching_registry.rs::scaled_partition_from_response_reads_owner_describe_like_go | PARTIAL |
| Test_domainChangeCallback (1434) | domain change callback updates managers | matching_rpc_tests.rs::matching_failover_callback_excludes_sticky_activity_and_other_domains, matching_failover_callback_disconnects_sticky_decision_and_keeps_manager | PARTIAL |
| Test_registerDomainFailoverCallback (1595) | callback is registered for failover | matching_rpc_tests.rs::matching_rpc_domain_failover_releases_blocked_polls | PARTIAL |
| TestRefreshWorkflowTasks (1712) | refresh workflow tasks via history | matching_history_tchannel.rs::refresh_workflow_tasks_matches_nested_thrift_request_fixture | PARTIAL |
| TestSetupExecutorWithEmptyConfig (1910) | executor setup with empty config | matching_shard_distributor.rs::pinned_executor_heartbeat_assignment_and_drain_contract | PARTIAL |
| TestShardDistributorOnboarded (1935) | onboarding via shard distributor | matching_shard_distributor.rs::pinned_executor_heartbeat_assignment_and_drain_contract, spectator_waits_for_initial_snapshot_and_returns_cached_owner | PARTIAL |
| TestGetOrCreateSemaphoreManager (2055) | one manager per bucket, create on demand | matching_semaphore.rs::matching_semaphore_registry_shares_one_manager_per_bucket | COVERED |
| TestUnloadSemaphoreManager (2214) | unload one semaphore manager | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets | PARTIAL: idle sweep covered; explicit unload of one manager is not |
| TestStopSemaphoreManagers (2236) | stop all managers on shutdown | matching_semaphore.rs::matching_semaphore_registry_register_lookup_and_unregister (ported, unregister path); no shutdown-all test | PARTIAL |
| TestAcquireSemaphore (2285) | acquire through engine RPC | matching_rpc_tests.rs::matching_rpc_semaphore_acquire_grants_and_reports_no_slot, matching_rpc_sql_semaphore_matches_go_unsupported_backend_error | COVERED |

### handler/engine_integration_test.go (`matchingEngineSuite`, 36 methods, ~1800 lines)

These run the engine against real persistence and real pollers. Rust counterparts are the RPC-level
tests in matching_rpc_tests.rs, matching_delivery.rs and the reader/writer tests.

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestMatchingEngineSuite (107) | suite runner | n/a | n/a |
| TestPollForActivityTasksEmptyResult (214) | empty poll returns empty result | matching_rpc_tests.rs::matching_rpc_empty_polls_reserve_tailroom_and_read_live_expiration | PARTIAL |
| TestPollForDecisionTasksEmptyResult (218) | same for decision | matching_rpc_tests.rs::matching_rpc_in_memory_empty_polls_reserve_tailroom | PARTIAL |
| TestPollForActivityTasksEmptyResultWithShortContext (222) | short deadline empty poll | matching_rpc_tests.rs::matching_long_polls_reject_missing_transport_deadlines_with_bad_request; matching_dispatch.rs::matching_dispatch_empty_long_poll_expires_without_delivery | PARTIAL |
| TestPollForDecisionTasksEmptyResultWithShortContext (229) | same for decision | matching_dispatch.rs::matching_dispatch_empty_long_poll_expires_without_delivery | PARTIAL |
| TestOnlyUnloadMatchingInstance (236) | unload only the matching instance | matching_registry.rs::stale_teardown_cannot_remove_replacement_owner | PARTIAL |
| TestPollForDecisionTasks (281) | decision poll delivery | matching_rpc_tests.rs::matching_rpc_decision_poll_preserves_isolation_group_history | PARTIAL |
| TestQueryWorkflow (438) | query round trip | matching_rpc_tests.rs::matching_rpc_delivers_query_to_decision_poller, matching_rpc_child_query_forwards_to_parent_poller | COVERED |
| TestAddActivityTasks (592) | activity add | matching_rpc_tests.rs::matching_rpc_durable_activity_preserves_source_domain_and_type | PARTIAL |
| TestAddDecisionTasks (596) | decision add | matching_rpc_tests.rs::matching_rpc_durable_add_does_not_require_local_workflow_state | PARTIAL |
| TestAddActivityTasksForwarded (600) | forwarded activity add | matching_rpc_tests.rs::matching_rpc_child_add_sync_matches_parent_without_duplicate_persistence | PARTIAL |
| TestAddDecisionTasksForwarded (604) | forwarded decision add | matching_rpc_tests.rs::matching_rpc_forwarded_add_never_persists_and_reports_remote_failure | PARTIAL |
| TestAddTaskTrafficMetricOracle (611) | add-task traffic metric | matching_persistence_limiter.rs::success_and_condition_failure_use_go_names_and_tags (persistence metric, not add-task) | PARTIAL |
| TestAddAndPollDecisionTasks (709) | add then poll decision | matching_rpc_tests.rs::matching_rpc_child_poll_forwards_to_parent_backlog | PARTIAL |
| TestAddAndPollActivityTasks (713) | add then poll activity | matching_rpc_tests.rs::matching_rpc_cross_host_poll_add_and_cancellation | PARTIAL |
| TestAddAndPollDecisionTasksIsolation (717) | isolation groups, decision | matching_rpc_tests.rs::matching_rpc_isolation_toggle_returns_empty_poll_and_replaces_owner | PARTIAL |
| TestAddAndPollActivityTasksIsolation (721) | isolation groups, activity | matching_reader.rs::matching_reader_isolation_buffers_avoid_cross_group_head_blocking | PARTIAL |
| TestSyncMatchActivityTasks (786) | sync match activity | matching_sync.rs::sync_match_waits_for_history_completion_and_preserves_original_metadata | PARTIAL |
| TestSyncMatchDecisionTasks (790) | sync match decision | matching_sync.rs::sync_match_zero_wait_claims_registered_poller_and_timeouts_withdraw_offers | PARTIAL |
| TestSyncMatchActivityTasksIsolation (794) | sync match isolation | matching_sync.rs::sync_match_does_not_deliver_to_other_or_unknown_groups | PARTIAL |
| TestSyncMatchDecisionTasksIsolation (798) | sync match isolation, decision | matching_sync.rs::sync_match_does_not_deliver_to_other_or_unknown_groups | PARTIAL |
| TestConcurrentAddAndPollActivities (942) | concurrent add/poll, activity | matching_registry.rs::concurrent_load_acquires_one_lease_and_isolates_task_types | PARTIAL |
| TestConcurrentAddAndPollActivitiesWithZeroDispatch (946) | zero dispatch rate concurrency | matching_writer.rs::zero_capacity_handoffs_are_fifo_and_never_overwrite_waiters | PARTIAL |
| TestConcurrentAddAndPollDecisions (950) | concurrent add/poll, decision | matching_registry.rs::concurrent_load_acquires_one_lease_and_isolates_task_types | PARTIAL |
| TestConcurrentAddAndPollActivitiesIsolation (954) | concurrency with isolation | matching_reader.rs::matching_reader_isolation_buffers_avoid_cross_group_head_blocking | PARTIAL |
| TestConcurrentAddAndPollActivitiesWithZeroDispatchIsolation (958) | zero dispatch + isolation | matching_writer.rs::zero_capacity_handoffs_are_fifo_and_never_overwrite_waiters | PARTIAL |
| TestConcurrentAddAndPollDecisionsIsolation (962) | decision + isolation | matching_reader.rs::matching_reader_isolation_timeout_reselects_without_rewriting_storage | PARTIAL |
| TestPollActivityWithExpiredContext (1088) | expired poll context | matching_dispatch.rs::matching_dispatch_empty_long_poll_expires_without_delivery | PARTIAL |
| TestPollDecisionWithExpiredContext (1092) | expired decision poll | matching_dispatch.rs::matching_dispatch_background_timeout_unregisters_wait_before_next_task | PARTIAL |
| TestMultipleEnginesActivitiesRangeStealing (1127) | two engines steal ranges | matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL |
| TestMultipleEnginesDecisionsRangeStealing (1131) | same for decisions | matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL |
| TestAddTaskAfterStartFailure (1218) | add after failed start | matching_registry.rs::slow_startup_does_not_block_loaded_owner_or_shutdown; startup failures in matching_membership_lifecycle.rs | PARTIAL |
| TestUnloadActivityTasklistOnIsolationConfigChange (1264) | unload on isolation change | matching_registry.rs::stop_if_isolation_changed; matching_rpc_tests.rs::matching_rpc_isolation_toggle_stops_owner_before_admitting_add | PARTIAL |
| TestUnloadDecisionTasklistOnIsolationConfigChange (1268) | same for decision | matching_rpc_tests.rs::matching_rpc_isolation_toggle_stops_owner_before_admitting_add | PARTIAL |
| TestDrainActivityBacklogNoPollersIsolationGroup (1326) | drain backlog with no pollers in group | matching_rpc_tests.rs::matching_rpc_isolation_policy_rechecks_drains_and_poller_recency | PARTIAL |
| TestDrainDecisionBacklogNoPollersIsolationGroup (1330) | same for decision | matching_rpc_tests.rs::matching_rpc_isolation_policy_rechecks_drains_and_poller_recency | PARTIAL |
| TestConfigDefaultHostName (1493) | default host name | matching_config.rs::go_defaults_when_unset (no host-name case) | PARTIAL |
| TestMatchingPollMetricOracle (535) | poll metric names and tags | matching_rpc_tests.rs::matching_rpc_reports_arrival_qps_by_isolation_group; metrics tests in matching_history.rs | PARTIAL |

### handler/handler_test.go (`handlerSuite`, 18 methods)

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestHandlerSuite (73) | suite runner | n/a | n/a |
| TestNewHandler (123) | constructor wiring | matching_rpc_tests.rs::matching_grpc_listener_registers_meta_health_and_gates_startup | PARTIAL |
| TestStart (129) | start | matching_rpc_tests.rs::matching_meta_health_waits_for_start_on_both_transports | PARTIAL |
| TestStop (140) | stop | matching_registry.rs::shutdown_persists_ack_and_stops_previously_returned_handles | PARTIAL |
| TestHealth (153) | health check | matching_rpc_tests.rs::matching_meta_health_waits_for_start_on_both_transports | COVERED |
| TestNewHandlerContext (160) | handler context construction | none | NOT-ASSESSED |
| TestAddActivityTask (167) | add activity via handler | matching_rpc_tests.rs::matching_rpc_durable_activity_preserves_source_domain_and_type | PARTIAL |
| TestAddDecisionTask (235) | add decision via handler | matching_rpc_tests.rs::matching_rpc_durable_add_does_not_require_local_workflow_state | PARTIAL |
| TestPollForActivityTask (303) | poll activity via handler | matching_rpc_tests.rs::matching_rpc_cross_host_poll_add_and_cancellation | PARTIAL |
| TestPollForDecisionTask (380) | poll decision via handler | matching_rpc_tests.rs::matching_rpc_decision_poll_preserves_isolation_group_history | PARTIAL |
| TestQueryWorkflow (457) | query via handler | matching_rpc_tests.rs::matching_rpc_delivers_query_to_decision_poller | COVERED |
| TestRespondQueryTaskCompleted (511) | respond to query | matching_query.rs::offer_is_delivered_once_and_completed_by_task_id | COVERED |
| TestCancelOutstandingPoll (554) | cancel poll | matching_rpc_tests.rs::matching_rpc_cancel_outstanding_poll_checks_task_list_ownership | COVERED |
| TestDescribeTaskList (598) | describe | matching_rpc_tests.rs::matching_rpc_durable_describe_tracks_activity_poller_and_queue_status | COVERED |
| TestListTaskListPartitions (656) | list partitions | matching_rpc_tests.rs::matching_rpc_lists_configured_write_partitions | COVERED |
| TestGetTaskListsByDomain (712) | list by domain | matching_rpc_tests.rs::matching_rpc_domain_listing_uses_loaded_matching_managers | COVERED |
| TestDomainName (771) | domain name lookup | none | NOT-ASSESSED |
| TestRefreshTaskListPartitionConfig (804) | refresh partition config | matching_rpc_tests.rs::matching_rpc_partition_update_refreshes_remote_read_owner_and_ignores_write_only_owner | PARTIAL |
| TestUpdateTaskListPartitionConfig (854) | update partition config | matching_rpc_tests.rs::matching_rpc_partition_config_rpcs_reject_non_normal_kinds | PARTIAL |
| TestHandlerAcquireSemaphore (921) | semaphore acquire via handler | matching_rpc_tests.rs::matching_rpc_semaphore_acquire_grants_and_reports_no_slot | COVERED |

### handler/membership_test.go

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestGetTaskListManager_OwnerShip (61) | owner check before creating manager | matching_registry.rs::matching_membership_guards_creation_and_unloads_moved_owners | COVERED |
| TestMembershipSubscriptionRecoversAfterPanic (169) | subscription survives a panic | none (Rust tasks do not use Go panic recovery) | MISSING: architecture difference; no panic path to test |
| TestSubscriptionAndShutdown (189) | subscribe then shut down | matching_membership_lifecycle.rs::shutdown_self_evicts_before_stop_and_clears_placement | PARTIAL |
| TestSubscriptionAndErrorReturned (222) | subscribe error returned | matching_membership_lifecycle.rs::startup_failures_stop_provider_and_start_is_idempotent | PARTIAL |
| TestSubscribeToMembershipChangesQuitsIfSubscribeFails (285) | quit on subscribe failure | matching_membership_lifecycle.rs::startup_failures_stop_provider_and_start_is_idempotent | PARTIAL |
| TestGetTasklistManagerShutdownScenario (325) | shutdown during manager lookup | matching_registry.rs::shutdown_tolerates_lost_task_list_ownership | PARTIAL |
| TestShutDownNonOwnedSemaphoreManagers (374) | shut down managers not owned | matching_registry.rs::matching_membership_guards_creation_and_unloads_moved_owners; matching_semaphore sweep | PARTIAL |
| TestShutDownNonOwnedSemaphoreManagersReportsAPanic (423) | panic reported | none | MISSING: Go panic reporting |
| TestShutDownNonOwnedSemaphoreManagersSurfacesALookupFailure (448) | lookup failure surfaced | matching_semaphore.rs::matching_semaphore_missing_semaphore_is_not_ready_after_scan | PARTIAL |

### liveness/liveness_test.go (`livenessSuite`, 6 methods)

Rust `cadence_common::liveness::Liveness` is a passive timestamp, not a Go daemon with an event loop.
It has no Start/Stop and no shutdown callback. The event-loop cases do NOT map onto the semaphore
registry's idle sweep (a different mechanism), so they are MISSING, not covered.

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestLivenessSuite (48) | suite runner | n/a | n/a |
| TestIsAlive_No (64) | not alive after ttl | cadence-common/src/liveness.rs::original_go_liveness_fixtures | COVERED |
| TestIsAlive_Yes (70) | alive within ttl/2 | cadence-common/src/liveness.rs::original_go_liveness_fixtures | COVERED |
| TestMarkAlive_Noop (76) | markAlive with no time advance keeps timestamp | cadence-common/src/liveness.rs::original_go_liveness_fixtures | COVERED |
| TestMarkAlive_Updated (83) | markAlive advances timestamp | cadence-common/src/liveness.rs::original_go_liveness_fixtures | COVERED |
| TestEventLoop_Noop (89) | event loop keeps a live manager alive | none | MISSING: Rust Liveness has no event loop; semaphore sweep is a different mechanism |
| TestEventLoop_Shutdown (105) | event loop fires shutdown callback at ttl | none | MISSING: Rust Liveness has no shutdown callback; open question whether registry expiry matches Go daemon behavior |

### poller/manager_test.go (`manager`, 7 tests)

Rust has no separate poller manager struct. Poller bookkeeping is in matching_registry.rs and matching_dispatch.rs.

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestManager_HistoryCallback (38) | history callback invoked on poll | matching_rpc_tests.rs::matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition | PARTIAL |
| TestManager_CancelPoll (58) | cancel a poll | matching_rpc_tests.rs::matching_rpc_cancel_without_matching_poll_returns_ok; matching_rpc_duplicate_poller_id_replaces_entry_without_cancelling_first | PARTIAL |
| TestManager_HasPollerFromIsolationGroupAfter (103) | isolation-group poller recency | matching_registry.rs::has_poller_after (indirect); matching_rpc_tests.rs::matching_rpc_isolation_policy_rechecks_drains_and_poller_recency | PARTIAL |
| TestManager_HasPollerAfter (177) | poller recency | matching_registry.rs::has_poller_after (indirect) | PARTIAL |
| TestManager_GetCount (240) | poller count | none directly | NOT-ASSESSED |
| TestManager_ListInfo (257) | poller list info | none directly | NOT-ASSESSED |
| TestManager_GetCountByIsolationGroup (332) | poller count per group | matching_rpc_tests.rs::matching_rpc_reports_arrival_qps_by_isolation_group (indirect) | PARTIAL |

### semaphore/*

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| identifier_test.go: TestNewIdentifier (16) | valid/invalid identifiers, string form | matching_semaphore.rs::matching_semaphore_identifier_matches_go_validation | COVERED |
| identifier_test.go: TestParseRequestOwner (59) | malformed owner → BadRequest | grpc_matching.rs::handler_go_tests::matching_semaphore_parse_request_owner_matches_go | PORTED (port2 20261024) |
| identifier_test.go: TestRingKey (88) | ring key separates buckets | matching_semaphore.rs::matching_semaphore_ring_key_separates_buckets (PORTED) | PORTED |
| identifier_test.go: TestIdentifierIsUsableAsAMapKey (101) | hashable map key | matching_semaphore.rs::matching_semaphore_identifier_is_usable_as_a_map_key (PORTED) | PORTED |
| identifier_test.go: TestLogTags (116) | zap log field tags | none | MISSING: Rust logs via to_log_string; zap tag shape has no Rust analog |
| semaphore_registry_test.go: TestSemaphoreRegistry_RegisterLookupAndUnregister (38) | empty registry, register, lookup, unregister twice | matching_semaphore.rs::matching_semaphore_registry_register_lookup_and_unregister | PORTED |
| semaphore_registry_test.go: TestSemaphoreRegistry_UnregisterIgnoresAStaleManager (62) | stale manager cannot evict replacement | matching_semaphore.rs::matching_semaphore_unregister_ignores_a_stale_manager | PORTED |
| semaphore_registry_test.go: TestSemaphoreRegistry_AllManagersIsASnapshot (85) | AllManagers is a snapshot | matching_semaphore.rs::matching_semaphore_all_managers_is_a_snapshot | PORTED |
| semaphore_registry_test.go: TestSemaphoreRegistry_GetOrCreateBuildsOnlyWhenThereIsNothingHeld (101) | build once when nothing held | matching_semaphore.rs::matching_semaphore_registry_shares_one_manager_per_bucket | COVERED |
| semaphore_registry_test.go: TestSemaphoreRegistry_GetOrCreateHoldsNothingWhenTheBuildFails (129) | failed build leaves no entry | matching_semaphore.rs::matching_semaphore_missing_semaphore_is_not_ready_after_scan | COVERED |
| semaphore_registry_test.go: TestSemaphoreRegistry_GetOrCreateBuildsOnceUnderConcurrentCallers (146) | 32 callers, one manager | matching_semaphore.rs::matching_semaphore_concurrent_callers_share_one_manager | PORTED |
| semaphore_manager_test.go: TestNewManagerValidatesItsParams (136) | constructor validation | Rust Manager::new has no validation error path | MISSING |
| semaphore_manager_test.go: TestStartBuildsTheFreeSetAndTheHeldIndexAcrossPages (180) | paged scan builds free set | matching_semaphore.rs::matching_semaphore_load_rebuilds_index_from_persisted_holds | PARTIAL: no multi-page test |
| semaphore_manager_test.go: TestStartKeepsAClaimedSlotOutOfTheFreeSet (206) | claimed slot excluded | matching_semaphore.rs::matching_semaphore_load_rebuilds_index_from_persisted_holds | COVERED |
| semaphore_manager_test.go: TestStartSkipsRowsItCannotClassify (227) | unclassifiable rows skipped | none | MISSING |
| semaphore_manager_test.go: TestStartLeavesStateUntouchedWhenTheScanFails (254) | failed scan leaves state | matching_semaphore.rs::matching_semaphore_missing_semaphore_is_not_ready_after_scan | PARTIAL |
| semaphore_manager_test.go: TestSecondStartIsANoOp (282) | start twice | matching_semaphore.rs::matching_semaphore_registry_shares_one_manager_per_bucket (second get_or_create restarts, no rebuild) | PARTIAL |
| semaphore_manager_test.go: TestAFailedLoadUnregistersTheManager (295) | failed load removes manager | matching_semaphore.rs::matching_semaphore_missing_semaphore_is_not_ready_after_scan | COVERED |
| semaphore_manager_test.go: TestAcquireRejectsAnEmptyOwnerID (307) | empty owner rejected | none | NOT-ASSESSED |
| semaphore_manager_test.go: TestAcquireBeforeTheBucketIsUsable (320) | acquire before load → not ready | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets (acquire after stop → NotReady) | PARTIAL |
| semaphore_manager_test.go: TestAcquireHonorsItsDeadlineWhileStarting (389) | deadline during start | none | MISSING: needs slow-store injection |
| semaphore_manager_test.go: TestStartDoesNotWaitOnALoadInFlight (420) | start does not block on load | none | MISSING: needs slow-store injection |
| semaphore_manager_test.go: TestAcquireHandsBackWhatTheGrantDecided (454) | acquire returns grant outcome | matching_semaphore.rs::matching_semaphore_grants_until_full_then_reports_no_slot | PARTIAL |
| semaphore_manager_test.go: TestAcquireSurfacesAFailedGrant (496) | failed grant error surfaced | none | MISSING: needs failing-store injection |
| semaphore_manager_test.go: TestGrantOnAFreeSlot (509) | grant one free slot | matching_semaphore.rs::matching_semaphore_grants_until_full_then_reports_no_slot | COVERED |
| semaphore_manager_test.go: TestGrantRetriesADifferentSlotWhenTheWriteSaysTaken (537) | retry on slot-taken | none | MISSING: needs write-conflict injection |
| semaphore_manager_test.go: TestGrantGivesUpAfterMaxAttempts (562) | max attempts | none | MISSING: needs write-conflict injection |
| semaphore_manager_test.go: TestGrantStopsRetryingOnceTheDeadlineHasPassed (577) | deadline stops retry | none | MISSING: needs write-conflict injection |
| semaphore_manager_test.go: TestGrantWhenTheWriteSaysTheOwnerAlreadyHolds (600) | already-held outcome | matching_semaphore.rs::matching_semaphore_grants_until_full_then_reports_no_slot (re-acquire returns same slot) | PARTIAL |
| semaphore_manager_test.go: TestGrantRejectsAnAlreadyHeldWriteWithNoToken (651) | already-held without token rejected | none | MISSING |
| semaphore_manager_test.go: TestGrantReturnsTheSlotWhenTheWriteFails (679) | failed write returns slot to free set | none | MISSING: needs write-failure injection |
| semaphore_manager_test.go: TestGrantRejectsAnUnrecognizedWriteOutcome (699) | unknown outcome rejected | none | MISSING |
| semaphore_manager_test.go: TestGrantWhenTheReverseIndexIsStale (716) | stale reverse index dropped | matching_semaphore.rs::matching_semaphore_confirm_hold_drops_stale_index_entry | COVERED |
| semaphore_manager_test.go: TestGrantSurfacesAConfirmingReadFailure (772) | confirm read failure surfaced | none | MISSING: needs read-failure injection |
| semaphore_manager_test.go: TestDropStaleHold (786) | drop stale hold | matching_semaphore.rs::matching_semaphore_confirm_hold_drops_stale_index_entry | PARTIAL |
| semaphore_manager_test.go: TestUnreserveIsIdempotent (829) | unreserve twice | none directly | NOT-ASSESSED |
| semaphore_manager_test.go: TestFreeSetSurvivesConcurrentReserveAndUnreserve (854) | concurrent reserve/unreserve | none directly (Rust free set behind one lock) | NOT-ASSESSED |
| semaphore_manager_test.go: TestConcurrentGrantsHandOutDistinctSlots (880) | concurrent grants get distinct slots | matching_semaphore.rs::matching_semaphore_grants_until_full_then_reports_no_slot (sequential only) | PARTIAL: no concurrent test |
| semaphore_manager_test.go: TestLifecycleUnderConcurrentGrants (919) | lifecycle under concurrency | none | NOT-ASSESSED |
| semaphore_manager_test.go: TestAnIdleManagerUnloadsItselfAndLeavesTheRegistry (981) | idle unload | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets | COVERED |
| semaphore_manager_test.go: TestAcquireKeepsAManagerLoaded (999) | acquire refreshes liveness | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets (sweep keeps alive bucket) | PARTIAL |
| semaphore_manager_test.go: TestALateStopUnregistersOnlyItself (1028) | late stop unregisters only itself | matching_semaphore.rs::matching_semaphore_unregister_ignores_a_stale_manager | PARTIAL |

### tasklist/

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| identifier_test.go: TestValidTaskListNames (33) | name parsing, root and partition | cadence-common/src/task_list.rs::matching_identifier_go_fixtures (fixture, different crate) | COVERED (cadence-common) |
| identifier_test.go: TestTaskListParentName (71) | parent of partition for degree 1..3 | cadence-common/src/task_list.rs::matching_identifier_go_fixtures (parents for degrees 0,1,2,3,20,i64::MAX) | COVERED (cadence-common) |
| identifier_test.go: TestInvalidTasklistNames (111) | invalid names rejected | cadence-common/src/task_list.rs::matching_identifier_go_fixtures | COVERED (cadence-common) |
| identifier_test.go: TestTaskListIDToString (133) | "[name=...type=...]" string | none | MISSING: Rust Identifier has no Display; no analog |
| isolation_balancer_test.go: TestAdjustWritePartitions (56) | all 29 subtests | matching_scaler_go_tests.rs::matching_isolation_balancer_* (29 tests, identical expectations) | PORTED (CI 81cbcbff0, evidence matching-isolation-scaler-tests-20261023) |
| task_reader_test.go: TestDispatchSingleTaskFromBuffer (57) | dispatch one buffered task | matching_reader.rs::matching_reader_local_head_start_does_not_forward | PARTIAL |
| task_reader_test.go: TestGetDispatchTimeout (244) | dispatch timeout by head | matching_reader.rs::matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads | PARTIAL |
| task_reader_test.go: TestTaskPump (303) | pump reads and buffers | matching_reader.rs::matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows | PARTIAL |
| task_reader_test.go: TestGetTasksPumpConditionFailedUnloadsTaskList (443) | condition failed unloads list | matching_registry.rs::fatal_owner_wakeup_does_not_wait_for_ack_interval | PARTIAL |
| task_reader_test.go: TestTaskReaderBatchSizeValidation (464) | batch size validation | matching_reader.rs (read_batch_size) | PARTIAL |
| adaptive_scaler_test.go: TestAdaptiveScalerLifecycle (87) | start/stop scaler | matching_scaler_go_tests.rs::matching_adaptive_scaler_lifecycle_start_stop_is_idempotent_gap (ignored: no Start/Stop daemon on Scaler) | GAP (not portable as written) |
| adaptive_scaler_test.go: TestAdaptiveScalerRun (100) | all 17 subtests | matching_scaler_go_tests.rs::matching_adaptive_scaler_* (17 tests); production trailing-drain wiring gap matching_adaptive_scaler_production_drain_describes_trailing_partitions (ignored) | PORTED with 1 wiring GAP |
| task_list_registry_test.go: TestTaskListRegistry_RegisterLookupAndUnregister (30) | register, replace, stale unregister, gauge | matching_registry.rs::stale_teardown_cannot_remove_replacement_owner; task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls | PARTIAL |
| task_list_registry_test.go: TestTaskListRegistryManagersGaugeOracle (78) | gauge name/tag and values | matching_registry.rs::task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls | COVERED |
| task_list_registry_test.go: TestTaskListRegistry_Filters (102) | all/by-domain/by-name filters | shard_processor exact-name stop (matching_shard_processor.rs::start_is_noop_and_stop_is_exact_name_across_domain_and_type); matching_registry.rs::describe_by_domain | PARTIAL |
| task_test.go: TestNewInternalTask (36) | internal task construction | matching_sync.rs (InternalTask-like types) | NOT-ASSESSED |
| forwarder_test.go: TestForwarderSuite (54) | suite runner | n/a | n/a |
| forwarder_test.go: TestForwardTaskError (77) | ErrNoParent for a root list; ErrTaskListKind for sticky | matching_dispatch.rs::matching_forward_task_error_is_no_parent_or_task_list_kind | PORTED |
| forwarder_test.go: TestForwardDecisionTask (86) | AddDecisionTask request fields (parent name, kind, domain, execution, schedule, forwardedFrom) | matching_dispatch.rs::matching_forward_decision_task_fields_match_go_forwarder (in-process RecordingParent; port3 audit: was stale MISSING) | PORTED |
| forwarder_test.go: TestForwardActivityTask (110) | AddActivityTask request fields | matching_dispatch.rs::matching_forward_activity_task_fields_match_go_forwarder (port3 audit: was stale MISSING) | PORTED |
| forwarder_test.go: TestForwardTaskRateExceeded (135) | two forwards allowed, third returns ErrForwarderSlowDown | matching_dispatch.rs::matching_forward_task_rate_exceeded_after_two_forwards | PORTED |
| forwarder_test.go: TestForwardQueryTaskError (148) | ForwardQueryTask: sticky list never forwards | matching_query_forward_family_tests.rs::query_forward_family_sticky_list_never_forwards_and_waits_for_local_poller (port3 audit) | PORTED (Rust has no ForwardQueryTask fn; behaviour via RPC) |
| forwarder_test.go: TestForwardQueryTask (159) | query forward reaches parent with parent name | matching_query_forward_family_tests.rs::query_forward_family_forwards_go_forwarder_fields_to_parent (port3 audit) | PORTED |
| forwarder_test.go: TestForwardQueryTaskRateNotEnforced (178) | query forwards are not rate limited | matching_query_forward_family_tests.rs::query_forward_family_repeated_forwards_are_not_rate_limited (port3 audit) | PORTED |
| forwarder_test.go: TestForwardPollError (192) | ForwardPoll: ErrNoParent / ErrTaskListKind | matching_dispatch.rs::matching_forward_poll_without_parent_waits_locally_and_never_forwards (port3, NoParent branch only) | PORTED (partial: Rust has no ForwardPoll error value; ErrTaskListKind not asserted) |
| forwarder_test.go: TestForwardPollForDecision (203) | ForwardPoll decision: poller id, identity, parent name, response routing | matching_dispatch.rs::matching_forward_poll_for_decision_fields_match_go_forwarder (port3 audit) | PORTED |
| forwarder_test.go: TestForwardPollForActivity (231) | ForwardPoll activity: same fields | matching_dispatch.rs::matching_forward_poll_for_activity_fields_match_go_forwarder (port3 audit) | PORTED |
| forwarder_test.go: TestMaxOutstandingConcurrency (259) | 50 concurrent callers: contention yields 50, leaked tokens yield 1 | matching_forward.rs::matching_forward_max_outstanding_concurrency | PORTED |
| forwarder_test.go: TestMaxOutstandingConfigUpdate (311) | token capacity follows the dynamic config after an update | matching_forward.rs::matching_forward_config_update_resizes_generation | PORTED |
| task_list_limiter_test.go: TestTaskListLimiter (15) | limiter rate and burst | matching_limiter.rs::zero_burst_reports_throttled_without_consuming, wait_paces_after_terminal_burst_until_tokens_replenish | PARTIAL |
| task_list_manager_test.go: TestDeliverBufferTasks (127) | deliver buffered tasks | matching_reader.rs::matching_reader_local_poller_wins_after_forward_error | PARTIAL |
| task_list_manager_test.go: TestTaskListString (155) | String() form | none | MISSING: Rust has no Display for TaskListKey |
| task_list_manager_test.go: TestDeliverBufferTasks_NoPollers (164) | deliver with no pollers | matching_reader.rs::matching_reader_without_forward_token_waits_for_local_poller | PARTIAL |
| task_list_manager_test.go: TestReadLevelForAllExpiredTasksInBatch (181) | read level advances past expired batch | matching_reader.rs::matching_reader_drops_row_that_expired_behind_dispatch_head | PARTIAL |
| task_list_manager_test.go: TestTaskListManagerRegistryNotification (272) | registry notified | matching_registry.rs::registration_notification_reschedules_empty_controller | PARTIAL |
| task_list_manager_test.go: TestDescribeTaskList (304) | describe output | matching_rpc_tests.rs::matching_rpc_durable_describe_tracks_activity_poller_and_queue_status | PARTIAL |
| task_list_manager_test.go: TestQueriesPerSecond (457) | QPS reporting | matching_shard_processor.rs::report_has_strict_ttl_and_recomputes_qps_after_original_deadline; matching_rpc_tests.rs::matching_rpc_reports_arrival_qps_by_isolation_group | PARTIAL |
| task_list_manager_test.go: TestCheckIdleTaskList (497) | idle check (idle / poller / adding task) | matching_registry.rs::idle_tick_runs_without_waiting_for_ack_deadline; idle_unload_retains_unacknowledged_tasks_for_recovery | PARTIAL |
| task_list_manager_test.go: TestAddTaskStandby (590) | add task in standby | matching_standby.rs::standby_decision_uses_schedule_order_and_started_state; matching_rpc_tests.rs::matching_rpc_standby_polls_leave_durable_backlog_unclaimed | PARTIAL |
| task_list_manager_test.go: TestRateLimitErrorsFromTasklistDispatch (646) | rate-limit errors from dispatch | matching_dispatch.rs::queue_status_maps_throttling_to_service_busy_and_other_errors_to_internal | PARTIAL |
| task_list_manager_test.go: TestMisconfiguredZoneDoesNotBlock (671) | misconfigured zone does not block | matching_rpc_tests.rs::matching_rpc_isolation_policy_rechecks_drains_and_poller_recency (indirect) | NOT-ASSESSED |
| task_list_manager_test.go: TestGetIsolationGroupForTask (709) | isolation group selection | matching_reader.rs::matching_reader_isolation_enabled_only_with_groups; matching_reader_isolation_buffers_avoid_cross_group_head_blocking | PARTIAL |
| task_list_manager_test.go: TestTaskWriterShutdown (943) | writer shutdown | matching_writer.rs::matching_writer_zero_capacity_times_out_unaccepted_and_keeps_accepted_write_on_stop | PARTIAL |
| task_list_manager_test.go: TestTaskListManagerGetTaskBatch (974) | batch read (sub-cases) | matching_reader.rs::matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows | PARTIAL |
| task_list_manager_test.go: TestTaskListReaderPumpAdvancesAckLevelAfterEmptyReads (1104) | ack level after empty reads | matching_reader.rs::matching_reader_ack_timer_rereads_backlog_without_append_signal | PARTIAL |
| task_list_manager_test.go: TestTaskListManagerGetTaskBatch_ReadBatchDone (1196) | read batch done | matching_reader.rs::matching_reader_forwards_backlog_and_completes_durable_row | PARTIAL |
| task_list_manager_test.go: TestTaskExpiryAndCompletion (1241) | expiry and completion | matching_reader.rs::matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads | PARTIAL |
| task_list_manager_test.go: TestTaskListManagerImpl_HasPollerAfter (1343) | poller recency | matching_registry.rs::has_poller_after (indirect) | PARTIAL |
| task_list_manager_test.go: TestRefreshTaskListPartitionConfig (1380) | refresh partition config | matching_registry.rs::scaled_partition_from_response_reads_owner_describe_like_go; matching_rpc_tests.rs::matching_rpc_native_partition_cache_updates_refreshes_and_recovers | PARTIAL |
| task_list_manager_test.go: TestUpdateTaskListPartitionConfig (1498) | update partition config (sub-cases) | matching_rpc_tests.rs::matching_rpc_native_partition_cache_controls_dispatch_rate_and_drained_admission | PARTIAL |
| task_list_manager_test.go: TestRefreshTaskListPartitionConfigConcurrency (1703) | concurrent refresh | matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL |
| task_list_manager_test.go: TestUpdateTaskListPartitionConfigConcurrency (1720) | concurrent update | matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL |
| task_list_manager_test.go: TestManagerStart_RootPartition (1739) | root partition start | matching_registry.rs::shard_policy_accepts_local_assignment_and_rechecks_before_publish (indirect) | PARTIAL |
| task_list_manager_test.go: TestManagerStart_NonRootPartition (1777) | non-root partition start | matching_rpc_tests.rs::matching_rpc_child_partition_add_falls_back_to_local_persist | PARTIAL |
| task_list_manager_test.go: TestDispatchTask (1822) | dispatch one task | matching_dispatch.rs::matching_dispatch_writer_rewrite_retries_sql_before_acknowledging_original | PARTIAL |
| task_list_manager_test.go: TestGetNumPartitions (1953) | partition count | matching_rpc_tests.rs::matching_rpc_lists_configured_write_partitions | PARTIAL |
| task_list_manager_test.go: TestDisconnectBlockedPollers (1961) | disconnect blocked pollers | matching_registry.rs::release_blocked_pollers_for_domain (via matching_rpc_tests.rs::matching_rpc_domain_failover_releases_blocked_polls) | PARTIAL |
| task_list_manager_test.go: TestTaskListUsesOverrideRPS (2024) | override RPS wins | matching_rpc_tests.rs::matching_rpc_live_rps_override_matches_go_precedence | COVERED |
| task_completer_test.go: TestCompleteTaskIfStarted (68) | complete started task, skip others | matching_standby.rs::standby_activity_requires_started_or_absent_activity; matching_standby_original_go_completion_decisions | COVERED |
| matcher_test.go: TestMatcherSuite (69) | suite runner | n/a | n/a |
| matcher_test.go: TestLocalSyncMatch (101) | waiting poller takes offer; Offer = true | matching_sync.rs::sync_match_local_match_hands_off_to_waiting_poller | PORTED |
| matcher_test.go: TestLocalSyncMatchTimeout (120) | poller finishes after the 100ms offer budget; Offer = false | matching_sync.rs::sync_match_local_timeout_reports_no_match_when_poller_finishes_late | PORTED |
| matcher_test.go: TestIsolationLocalSyncMatch (142) | isolation-group poller matches local offer | matching_sync.rs::sync_match_isolation_group_hands_off_within_its_group | PORTED |
| matcher_test.go: TestRemoteSyncMatch (163) | offer forwarded to root partition, root poller matches | matching_dispatch.rs::matching_remote_sync_match_forwards_to_root_poller (port3 audit: in-process root dispatcher via remote_sync_match_case) | PORTED |
| matcher_test.go: TestIsolationRemoteSyncMatch (167) | same, isolation group dca1 | matching_dispatch.rs::matching_isolation_remote_sync_match_forwards_to_root_poller | PORTED |
| matcher_test.go: TestRemoteSyncMatchBlocking (171) | same, DbBacklog source blocks until poll | matching_dispatch.rs::matching_remote_sync_match_blocking_waits_for_root_poller | PORTED |
| matcher_test.go: TestIsolationRemoteSyncMatchBlocking (175) | same, isolation group, DbBacklog | matching_dispatch.rs::matching_isolation_remote_sync_match_blocking_waits_for_root_poller | PORTED |
| matcher_test.go: testRemoteSyncMatch (179) | shared helper for the four remote sync tests | matching_dispatch.rs::remote_sync_match_case (helper) | PORTED |
| matcher_test.go: TestSyncMatchFailure (244) | remote AddDecisionTask ServiceBusy → no error, no sync match | matching_dispatch.rs::matching_sync_match_failure_reports_busy_without_matching (port3 audit: was stale MISSING; Rust returns SlowDown, caller persists) | PORTED |
| matcher_test.go: TestRateLimitHandling (262) | matcher rate limiter path | matching_limiter.rs::zero_burst_reports_throttled_without_consuming (limiter only) | MISSING: no matcher-level rate-limit fixture |
| matcher_test.go: TestIsolationSyncMatchFailure (278) | isolation-group remote failure path | none | MISSING: remote forward |
| matcher_test.go: TestQueryLocalSyncMatch (298) | local poller takes query; OfferQuery returns nil, no error | matching_query.rs::registered_query_is_offered_exactly_once_to_a_parked_poller | PARTIAL: Rust covers single delivery, not the OfferQuery return contract |
| matcher_test.go: TestQueryRemoteSyncMatch (317) | query forwarded to parent and answered | matching_query_forward_family_tests.rs::query_forward_family_forwards_go_forwarder_fields_to_parent (port3 audit) | PORTED |
| matcher_test.go: TestQueryRemoteSyncMatchError (368) | remote query error surfaced | matching_query_forward_family_tests.rs::query_forward_family_busy_parent_keeps_query_for_local_poller (port3 audit) | PORTED |
| matcher_test.go: TestMustOfferLocalMatch (400) | MustOffer returns nil on a local poll | none | MISSING: no Rust MustOffer; retry loop lives in reader/dispatch |
| matcher_test.go: TestIsolationMustOfferLocalMatch (418) | MustOffer, isolation group | none | MISSING: no Rust MustOffer |
| matcher_test.go: TestMustOfferRemoteMatch (437) | MustOffer via remote parent | none | MISSING: remote forward |
| matcher_test.go: TestMustOfferRemoteRateLimit (505) | MustOffer remote rate limit | none | MISSING: remote forward |
| matcher_test.go: TestIsolationMustOfferRemoteMatch (524) | MustOffer via remote parent, isolation | none | MISSING: remote forward |
| matcher_test.go: TestPollersDisconnectedAfterDisconnectBlockedPollers (592) | DisconnectBlockedPollers unblocks pollers | matching_dispatch.rs::release_blocked_pollers (RPC-level: matching_rpc_tests.rs::matching_rpc_domain_failover_releases_blocked_polls) | MISSING: no matcher-level fixture |
| matcher_test.go: TestPollersConnectedAfterDisconnectBlockedPollersSetCancelContext (606) | pollers after disconnect get a fresh cancel context | none | MISSING: no per-poll cancel context in Rust |
| matcher_test.go: TestRemotePoll (621) | poll forwarded to parent | matching_dispatch.rs::matching_forward_poll_for_decision_fields_match_go_forwarder (in-process parent; port3 audit) | PORTED |
| matcher_test.go: TestRemotePollForQuery (647) | query poll forwarded to parent | matching_dispatch.rs::matching_standby_query_poll_forwards_to_parent_without_claiming_backlog (port3 audit) | PORTED |
| matcher_test.go: TestIsolationPollFailure (673) | isolation poll forward failure | none | MISSING: remote forward |
| matcher_test.go: TestOffer_RateLimited (682) | Offer rejected by rate limiter | matching_limiter.rs::zero_burst_reports_throttled_without_consuming (limiter only) | MISSING: no matcher-level fixture |
| matcher_test.go: TestOffer_NoTimeoutSyncMatchedNoError (694) | Offer without timeout, sync match, no error | matching_sync.rs::sync_match_local_match_hands_off_to_waiting_poller (analog) | MISSING: Rust offer API takes a deadline; no no-timeout variant |
| matcher_test.go: TestOffer_NoTimeoutSyncMatchedError (719) | Offer without timeout, sync match, poller error | matching_sync.rs::sync_match_cancellation_and_history_failure_allow_persistence_fallback (analog) | PARTIAL: failure branch covered, no-timeout branch not |
| matcher_test.go: TestOffer_NoTimeoutAsyncMatchedNoError (746) | Offer without timeout, async match | none | MISSING |
| matcher_test.go: TestOffer_AsyncMatchedNoError (771) | Offer async match, no error | matching_sync.rs::sync_match_waits_for_history_completion_and_preserves_original_metadata (analog) | PARTIAL |
| matcher_test.go: TestOfferOrTimeout_SyncMatchTimedOut (794) | offerOrTimeout: sync match timeout | matching_sync.rs::sync_match_local_timeout_reports_no_match_when_poller_finishes_late (analog) | PARTIAL: no offerOrTimeout API in Rust |
| matcher_test.go: TestOfferOrTimeout_AsyncMatchNotMatched (818) | offerOrTimeout: async, not matched | none | MISSING |
| matcher_test.go: TestOfferQuery_ForwardError (841) | OfferQuery forward error | matching_query_forward_family_tests.rs::query_forward_family_parent_error_is_returned_without_local_wait (port3 audit) | PORTED |
| matcher_test.go: TestOfferQuery_ContextExpired (865) | OfferQuery context expired | matching_query_forward_family_tests.rs::query_forward_family_expired_deadline_never_forwards (port3 audit) | PORTED |
| matcher_test.go: TestMustOffer_ContextExpiredFirstAttempt (882) | MustOffer, context expired before first attempt | none | MISSING: no MustOffer |
| matcher_test.go: TestMustOffer_ContextExpiredAfterFirstAttempt (893) | MustOffer, context expired after first attempt | none | MISSING: no MustOffer |
| matcher_test.go: TestMustOffer_LocalMatchAfterChildCtxExpired (909) | MustOffer, local match after child ctx expired | none | MISSING: no MustOffer |
| matcher_test.go: TestMustOffer_LocalMatchAfterForwardError (937) | MustOffer, local match after forward error | none | MISSING: no MustOffer |
| matcher_test.go: TestMustOffer_ContextExpiredAfterForwardError (971) | MustOffer, context expired after forward error | none | MISSING: no MustOffer |
| matcher_test.go: Test_pollOrForward_PollIsolatedTask (1002) | private pollOrForward, isolated task | none | MISSING: Go private helper with no Rust function |
| matcher_test.go: Test_pollOrForward_PollTask (1026) | private pollOrForward, task | none | MISSING: Go private helper |
| matcher_test.go: Test_pollOrForward_PollQueryTask (1049) | private pollOrForward, query task | none | MISSING: Go private helper |
| matcher_test.go: Test_pollOrForward_ForwardTask (1069) | private pollOrForward, forwards | none | MISSING: Go private helper |
| matcher_test.go: Test_pollOrForward_ForwardTaskThenPoll (1094) | private pollOrForward, forward then poll | none | MISSING: Go private helper |
| matcher_test.go: Test_poll_IsolatedTask (1129) | private poll, isolated task | none | MISSING: Go private helper |
| matcher_test.go: Test_poll_Task (1143) | private poll, task | none | MISSING: Go private helper |
| matcher_test.go: Test_poll_QueryTask (1156) | private poll, query task | none | MISSING: Go private helper |
| matcher_test.go: Test_poll_ContextExpired (1169) | private poll, context expired | none | MISSING: Go private helper |
| matcher_test.go: Test_pollNonBlocking_IsolatedTask (1181) | non-blocking poll, isolated task | none | MISSING: Rust poll is blocking only |
| matcher_test.go: Test_pollNonBlocking_Task (1196) | non-blocking poll, task | none | MISSING: Rust poll is blocking only |
| matcher_test.go: Test_pollNonBlocking_QueryTask (1210) | non-blocking poll, query task | none | MISSING: Rust poll is blocking only |
| matcher_test.go: Test_pollNonBlocking_NoTasks (1224) | non-blocking poll with no tasks returns immediately | none | MISSING: Rust poll is blocking only |
| matcher_test.go: Test_fwdrPollReqTokenC (1235) | nil forwarder yields noop token channel | none | MISSING: Go nil-forwarder path |
| matcher_test.go: Test_LocalSyncMatch_AutoConfigHint (1605) | poll returns AutoConfigHint disabled by default | none | MISSING: Rust poll result has no AutoConfigHint |
| matcher_test.go: TestRatelimitBehavior (1264) | original vs current limiter: cancel returns tokens, known differences | matching_limiter.rs::wait_paces_after_terminal_burst_until_tokens_replenish | MISSING: subtests pin Go-specific limiter cancel behavior |
| shard_processor_test.go: TestNewShardProcessorFailsWithEmptyParams (52) | empty params rejected | matching_shard_processor.rs::shard_processor_factory_rejects_empty_shard_key (PORTED) | PORTED |
| shard_processor_test.go: TestGetShardReport (59) | new processor: zero load, READY | matching_shard_processor.rs::shard_processor_initial_report_is_ready_with_zero_load (PORTED) | PORTED |
| shard_processor_test.go: TestSetShardStatus (68) | set status reflected | matching_shard_processor.rs::shard_processor_set_status_is_reflected_in_report (PORTED) | PORTED |

### wrappers/thrift/thrift_handler_test.go (`TestThriftHandler`, 10 subtests)

| Go test (line) | Asserts | Rust equivalent | Status |
|---|---|---|---|
| TestThriftHandler (38) | thrift wrapper routes each method and maps errors | thrift_matching.rs::matching_inbound_metrics_count_native_and_thrift_dispatch_once_each; matching_thrift_exceptions_fill_required_fields_like_go_mapper; matching_thrift_uses_each_methods_exception_field_ids | PARTIAL: no per-method routing table test |

## Matching-relevant go/common tests

Checked `go/common/**/*_test.go` mentioning matching. None test the Matching service itself. They cover
generic util, stats, persistence Cassandra/SQL, reconciliation invariants, domain replication, membership
hostinfo, proto/thrift mappers for matching types (`types/mapper/proto/matching_test.go`,
`types/mapper/thrift/any_test.go`), and dynamic config. The matching-type mapper tests correspond to Rust
`grpc_convert.rs` and `thrift_matching.rs` tests. Not ported in this pass.

## Parity gaps

Status as of the matcher/forwarder/liveness pass (2026-10-23):

- No parity gap found yet: no ported test has failed, so none is marked #[ignore].
- Open question (not a test failure): Go `Liveness` has a daemon with a shutdown callback at TTL
  (liveness_test.go TestEventLoop_Shutdown). Rust `cadence_common::liveness::Liveness` has no such
  daemon. Whether registry expiry matches Go needs an audit before those two tests can be ported.
- Pass evidence: `memory/evidence/matching-go-tests-port-20261023/` (step1-matcher.md, matcher-cargo-test.*,
  forwarder-cargo-test.*).

Counts for the three files in this pass (per Go test function / suite method):
- matcher_test.go: 54 functions total; 3 PORTED; 51 MISSING (remote forwarding, MustOffer, private poll helpers).
- forwarder_test.go: 13 functions total (suite runner + 12 methods); 4 PORTED; 8 MISSING (remote forward, query forward).
- liveness_test.go: 6 methods; 4 COVERED by the JSON fixture; 2 MISSING (event loop, no Rust daemon).
- task_gc_test.go: does not exist. `go/service/matching/tasklist/task_gc.go` has no test file.

## Remaining work (for the orchestrator to schedule)

- MISSING with a Rust behavior to test (port candidates): TestHandleErrUncategorizedError (needs an
  error-to-status helper test in grpc_matching.rs); semaphore_manager_test.go write-conflict/retry/deadline
  cases (need fault-injecting SemaphorePersistence); TestStartSkipsRowsItCannotClassify; TestUnreserveIsIdempotent;
  TestFreeSetSurvivesConcurrentReserveAndUnreserve; TestLifecycleUnderConcurrentGrants.
- NOT-ASSESSED rows need a Go-body read before a mapping is claimed.
- PARTIAL rows: the matcher, task-list-manager, reader, and engine-integration suites need per-case
  ports. Each Go subtest should map to a named Rust test with the same scenario.
- Architecture-only MISSING (no port): panic-recovery tests, Go zap log tags, TaskListString/Display,
  Liveness event loop (no Go daemon in Rust).

## Port2 pass (2026-10-24, branch matching-tests-port2-20261024)

- Ported: TestHandleErrUncategorizedError, TestHandleErrKnowErrors (semaphore error category mapping), TestParseRequestOwner (3 tests, `matching_handler_go_tests.rs`). Evidence: `memory/evidence/matching-tests-port2-20261024/handler-test.{log,exit}` (exit 0).
- Stale-table note: the semaphore_manager_test.go rows marked MISSING/NOT-ASSESSED above are already ported in `matching_semaphore_parity_tests.rs` (~35 tests: retry, deadline, write-failure, concurrency, late stop). Treat those rows as PORTED.
- Not done this pass: matcher remote-forward / MustOffer / forwarder parent-client tests (need an in-process fake parent client; no Rust MustOffer/ForwardQueryTask API), engine/task-list-manager PARTIAL rows (per-case ports).
- Build note: worktree needed go/idls copied from another worktree (submodule not initialised); not committed.

## Port3 pass (2026-10-24, branch matching-tests-port3-20261024)

- Audit correction: most forwarder_test.go and matcher_test.go remote-forward rows above were marked MISSING
  but already had in-process Rust tests (RecordingParent / BusyParent tonic servers in matching_dispatch.rs
  and matching_query_forward_family_tests.rs, wired via grpc_matching.rs). Those rows are now PORTED.
- New: `matching_dispatch.rs::matching_forward_poll_without_parent_waits_locally_and_never_forwards`
  (Go TestForwardPollError, NoParent branch). ErrTaskListKind for ForwardPoll is not asserted.
- Still MISSING: MustOffer (no Rust fn; reader analogs in matching_reader.rs), TestIsolationSyncMatchFailure,
  TestIsolationPollFailure, TestForwardPollError ErrTaskListKind branch, private pollOrForward/poll helpers.
- Evidence: `memory/evidence/matching-tests-port3-20261024/` (matching-test.log, matching-test.exit).

## Forwarder and matcher coverage re-audit (2026-10-24)

Compared `matcher_test.go` and `forwarder_test.go` against current Rust test bodies before adding
tests. The port3 “still MISSING” list was stale for the two isolation cases: the matcher tests now
contain `matching_sync.rs::sync_match_isolation_group_mismatch_does_not_match` (Go
`TestIsolationSyncMatchFailure`) and `sync_match_invalid_isolation_group_poll_falls_back_to_default_group`
(Go `TestIsolationPollFailure` observable behavior). The latter explicitly proves the Rust fallback
to the default channel; Go's error-on-context-expiry is represented by Rust `None` when stopped or
expired because the Rust poll API returns `Option`, not an error.

Other Go matcher scenarios already asserted in Rust and not duplicated here:

- `TestOffer_RateLimited` / `TestRateLimitHandling`: Rust's matching-level offer path does not own
  the Go matcher limiter; dispatch admission is covered at the dispatcher/limiter boundary. There is
  no equivalent `SyncMatcher::offer` throttling branch to port.
- `TestForwardPollError` `ErrNoParent`: covered by
  `matching_dispatch.rs::matching_forward_poll_without_parent_waits_locally_and_never_forwards`.
  `ErrTaskListKind` is a Go forwarder validation that Rust's poll-forward API does not expose; no
  equivalent error branch exists.
- `TestOffer_NoTimeoutSyncMatchedNoError`, async/no-wait offers, sync timeout withdrawal, and
  disconnected/expired poll behavior are covered by the named tests in `matching_sync.rs`.
- Forwarded task and poll field mapping, remote sync match (including blocking), query forwarding,
  and parent busy/error behavior are covered by in-process `RecordingParent`/`BusyParent` gRPC tests
  in `matching_dispatch.rs` and `matching_query_forward_family_tests.rs`.

No production seam or workflow was changed in this re-audit. Genuine remaining matcher gaps are
architecture-bound: Go `MustOffer` retry-loop cases have no Rust `MustOffer` operation (reader and
dispatcher use separate behavior), private Go `pollOrForward`/`poll` helpers have no corresponding
Rust API, `AutoConfigHint` is absent from Rust poll responses, and Go's per-poll cancel-context
replacement assertion has no distinct Rust context object. No remaining forwarder/matcher test was
added because the remaining listed candidates either already have matching Rust assertions or test
Go-only APIs without a Rust behavior branch.

CI validation for this audit commit: `cadence-matching` via `.github/workflows/rust-ci.yml`,
source SHA `8385f79eef7238d8d3489be80b4d2ff311df41a2`, run
https://github.com/auenkr/cadence-rust/actions/runs/40073697557, conclusion `success`.
The run executes the workflow's configured clippy and package test jobs on GitHub Actions; no local
Cargo command was run.
