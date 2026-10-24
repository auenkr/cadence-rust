# Matching completion audit — 2026-10-24, source ae442535c

This audit records current-source behavior coverage and limits. It is not a
completion sign-off. No Cargo command or mixed runtime gate was run locally.

## Startup partition-config notification

`rust/crates/cadence-matching/src/matching_registry.rs:1526-1553` starts a
background `notify_read_partitions` when a normal root manager loads a
persisted config. The helper at `:1950-2035` resolves each non-root read
partition owner, sends `RefreshTaskListPartitionConfig` over gRPC, captures
panics, and applies the five-second deadline without propagating refresh errors
to the root caller. The old ignored test reason in
`tests/matching_partial_upgrade_e.rs` was stale.

That ignored test is replaced by
`go_manager_start_root_pushes_loaded_config_to_remote_child_without_update_api`.
It persists a root config, starts a real child Matching gRPC service on an
ephemeral in-process listener, selects a partition owned by that child, then
creates a fresh parent registry and loads the root. It waits up to five seconds
for the child's real `RefreshTaskListPartitionConfig` handler to cache the
persisted config. There is no parent Update RPC after root reload. This tests
the production startup and transport path, not the notifier helper alone.
It still requires CI execution; no local formatting, compile, or test command
was used.

## 13 internal RPC coverage map

Primary RPC definition: `go/proto/internal/uber/cadence/matching/v1/service.proto:48-102`.
The named tests below are current Rust behavior tests (mainly
`src/matching_rpc_tests.rs` and `tests/matching_engine_go_tests*.rs`). This map
establishes concrete coverage, not exhaustive Go assertion parity.

| RPC | Concrete current tests | Remaining evidence limit |
| --- | --- | --- |
| `PollForDecisionTask` | `matching_rpc_decision_poll_preserves_isolation_group_history`; `matching_rpc_cross_host_poll_add_and_cancellation`; `matching_rpc_child_poll_forwards_to_parent_backlog`; `matching_rpc_standby_polls_leave_durable_backlog_unclaimed` | Mixed SDK workflow gates cover normal worker polling; task delivery error cases remain in ignored partial-upgrade test below. |
| `PollForActivityTask` | `matching_rpc_cross_host_poll_add_and_cancellation`; `go_handler_poll_for_activity_task_returns_added_task_token` | Current mixed SDK gates cover activity execution on older source heads. |
| `AddDecisionTask` | `matching_rpc_durable_add_does_not_require_local_workflow_state`; `matching_rpc_cross_host_poll_add_and_cancellation`; `go_engine_add_then_poll_decision_tasks_delivers_each_task_and_drains_store` | The focused `MustOffer` ignored case has a suspected fixture issue and must be resolved by a real History-peer test. |
| `AddActivityTask` | `matching_rpc_durable_activity_preserves_source_domain_and_type`; `matching_rpc_cross_host_poll_add_and_cancellation`; `go_engine_add_decision_and_activity_tasks_persist_every_task` | Mixed SDK gates cover activity scheduling/execution. |
| `QueryWorkflow` | `matching_rpc_delivers_query_to_decision_poller`; `matching_rpc_cross_host_query_completes_on_accepting_host`; `matching_rpc_remote_query_sync_match_forwards_to_parent_and_returns_answer`; sticky-query tests | Mixed SDK query coverage is recorded in older MySQL, PostgreSQL, and Thrift evidence. |
| `RespondQueryTaskCompleted` | `respond_query_task_completed_success_delivers_answer_to_waiter`; `respond_query_task_completed_query_task_not_found_is_error`; cross-host query completion tests | Full current-source SDK lane remains outstanding. |
| `CancelOutstandingPoll` | `matching_rpc_durable_cancel_unregisters_waiting_poller`; `matching_rpc_cancel_without_matching_poll_returns_ok`; `matching_rpc_cancel_outstanding_poll_checks_task_list_ownership`; forwarded-poll cancellation tests | The utility `poller::Manager::cancel` return-value ignored test is not the RPC behavior contract. |
| `DescribeTaskList` | `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status`; `matching_rpc_non_durable_describe_lists_pollers_per_task_list_and_type`; live rate and isolation group tests | Ignored non-durable task-id-block/empty case only targets the legacy in-memory object; production Matching uses durable queues. Final supported-source probe evidence still required. |
| `ListTaskListPartitions` | `matching_rpc_lists_configured_write_partitions`; `matching_rpc_scopes_partition_counts_to_task_list_and_type`; `engine_apis_partition_order_owners_and_lookup_errors_match_go` | Same-membership Go/Rust owner ordering after membership changes still lacks a mixed oracle. |
| `GetTaskListsByDomain` | `matching_rpc_domain_listing_uses_loaded_matching_managers`; `matching_domain_listing_filters_kinds_and_collapses_partition_roots`; `get_task_lists_by_domain_success_lists_normal_kinds` | Older mixed gates exercise domain metadata/listing; repeat on integrated source. |
| `UpdateTaskListPartitionConfig` | `update_task_list_partition_config_success`; `matching_rpc_native_partition_cache_updates_refreshes_and_recovers`; `matching_rpc_partition_update_refreshes_remote_read_owner_and_ignores_write_only_owner`; scaler tests | Startup loaded-root push now has the new direct test above; currently unexecuted. |
| `RefreshTaskListPartitionConfig` | `refresh_task_list_partition_config_success_accepts_partitioned_normal_list`; `matching_rpc_native_partition_cache_updates_refreshes_and_recovers`; `go_refresh_partition_config_from_database_replaces_cache`; new startup test above | Injected manager failure tests require no new mock API if production failure can be exercised; assess only if behavior can fail at the real persistence boundary. |
| `AcquireSemaphore` | `matching_rpc_semaphore_acquire_grants_and_reports_no_slot`; `matching_rpc_semaphore_owner_errors_match_go_text`; deadline, disabled-domain, rate-limit, unsupported-backend, and domain-error tests; `acquire_semaphore_free_slot_is_granted_and_named_in_response` | Current focused semaphore tests cover broad behavior; current-source Cassandra acceptance is older than this head. Check ignored validation/pagination and shutdown semantics below. |

## Lifecycle, persistence, transport, config and discovery evidence

| Area | Current concrete tests / evidence | Status and limit |
| --- | --- | --- |
| Poll/add/ack/query lifecycle | `matching_rpc_*` delivery, forwarding, cancellation, standby and query cases; matching reader/writer/scaler/ack unit suites | Broad focused coverage; no current-head mixed acceptance. The ignored active-offer case is unresolved and must be rerun with reachable Go History before calling it a production bug. |
| Durable stores and recovery | SQLite tests in matching RPC/registry suites; MySQL two-node run 40071731050; PostgreSQL run 40084530178; Cassandra run 40079714508 | All mixed runs passed their recorded lane, but each predates this integrated head. They are evidence for scenarios/backend compatibility, not current-source acceptance. |
| gRPC and SDK/service compatibility | Mixed Rust Matching + Go Frontend/History/Worker: MySQL run 40071731050, PostgreSQL 40084530178, Cassandra 40079714508 | SDK workflow, query, two-node ownership, restart/recovery scenarios were exercised; candidate must rerun after integration. |
| Thrift/TChannel and History peer | Mixed Thrift/TChannel run 40085137140, source `bfa303eeb9c0ba6c8b8b9f3f05c8cd98f68b44d9`, binary SHA256 `f7069f236eb797dc7b23362e2730e188418bac4f71c63c2cd87ed5f196af44ac` | Passed with Go Frontend/History/Worker and no Go Matching; older than current code. Go does not offer TChannel TLS; supported gRPC/HTTP TLS remains separately configured. |
| Runtime config/provider | OpenFeature runtime/live-reader integrations are present in tree; configstore run 40086531679 passed source `e0aeae9e8995ffa3e55fca967032c866ec08e0a4` | Mixed configstore/Unleash lifecycle evidence remains unbound to current integrated source. File-based reload ignored test is service-core behavior and must be assessed against the supported provider contract. |
| Discovery and ownership | Membership/ring tests, `matching_rpc_cross_host_poll_add_and_cancellation`, partition-update remote refresh tests; two-node MySQL and Cassandra gates | Startup notification test adds direct root-to-child behavior coverage when it passes CI. Same-membership Go/Rust list-partition owner ordering remains unresolved. Shard-distributor interop is an ignored serialized mixed test and needs its dedicated gate. |
| Shutdown/restart | MySQL two-node evidence includes SIGTERM drain, SIGKILL restart, idle unload and recovery. PostgreSQL, Cassandra and Thrift runs also exercised restart/recovery scenarios | Older commits only; rerun acceptance on one integrated candidate, with binary hash. |
| Metrics | Matching metric unit tests and migration policy code; separate metrics audit/worker notes | Do not claim emitted Go/Rust name/tag/condition parity from unit coverage alone. The live metrics comparison is outstanding. |

Run references and detailed scenario artifacts:

- MySQL: `memory/matching-twonode-20261024.md`, run
  <https://github.com/auenkr/cadence-rust/actions/runs/40071731050>, source
  `95ba26b852ee`, binary SHA256
  `6e7c89d62612e93a90d3fa8497911eb31ef3533eac229d3f4fb55da8fbcab161`.
- Cassandra: `memory/evidence/matching-cassandra-acceptance-20261024/README.md`,
  run <https://github.com/auenkr/cadence-rust/actions/runs/40079714508>, source
  `2432047f861f4ab68735e3b0d91c0a82303fb7d0`, binary SHA256
  `6276cb36698e1b1e7c178cf2e5d058a421dbd26f86c83da66a8fffae84f9fa95`.
- PostgreSQL: `memory/evidence/matching-postgres-acceptance-20261024/README.md`,
  run <https://github.com/auenkr/cadence-rust/actions/runs/40084530178>, source
  `1dd23e8a5d987e9a8bebfa9de00f62665a6fab97`, binary SHA256
  `90bcca0fac50d73beec7d0d1ad3b4904371661df356fc5dc5cacb5fd22875452`.
- Thrift/TChannel: `memory/evidence/matching-thrift-acceptance-20261024/README.md`,
  run <https://github.com/auenkr/cadence-rust/actions/runs/40085137140>.
- Configstore: `memory/evidence/matching-configstore-acceptance-20261024/README.md`,
  run <https://github.com/auenkr/cadence-rust/actions/runs/40086531679>, source
  `e0aeae9e8995ffa3e55fca967032c866ec08e0a4`.

All listed gates are historical relative to `ae442535c1d4ed708daeb38face1ab0e9d84f88a`.
They cannot establish integrated-source parity. The final strict CI workflow
must use a fresh target (`clean_target=true`), run clippy before tests, and
record head SHA and conclusion. Mixed gates must additionally record the
candidate Matching binary SHA256.

## Current ignored tests: stale mock cases versus actual gaps

The old count of 41 ignored tests is obsolete. A scoped scan of the current
Matching crate and `cadence-service-core/tests/matching_partial_upgrade_e.rs`
found 28 `#[ignore]` attributes; this includes mixed oracle/gate skips, not 28
production gaps. The startup notification placeholder has been replaced by an active behavior test and is no longer in this ignored set.

### Behavior gaps or unclosed behavior evidence

- `go_dispatch_task_active_offer_without_error_reaches_poller` in
  `tests/matching_partial_upgrade_e.rs`: it currently fails with an empty poll
  in a fixture whose History target is `127.0.0.1:1`; the test itself says
  `StartWorkflow` may be required. Replace the bad fixture with a live Go
  History peer and retest before assigning a production fix.
- `list_task_list_partitions_success_owner_hosts_match_go_lookups` in
  `tests/matching_engine_go_tests.rs`: mock scripted host ordering differs from
  live ring lookup. This is a genuine parity evidence gap; add same-membership
  Go/Rust oracle coverage.
- `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` is
  limited to legacy non-durable `Matching::new`; production launcher paths use
  durable queues. Do not treat this as a production gap absent a supported
  deployed non-durable source.
- `matching_semaphore_*` ignored validation/page-token cases in
  `src/matching_semaphore_parity_tests.rs`, the shutdown-admission test in
  `src/matching_rpc_tests.rs`, and acquire-during-load in
  `tests/matching_engine_go_tests_d.rs` require a fresh source-level review.
  Existing grant/deadline, lifecycle and Cassandra tests do not automatically
  prove these semantics. Implement only behavior confirmed missing.
- `go_get_tasks_batch_size_returns_raw_non_positive_values` in
  `src/matching_reader.rs` may be a real config accessor discrepancy; establish
  whether any production path observes raw nonpositive values before accepting
  it as harmless.
- `matching_rpc_semaphore_cancelled_context_is_internal_error` and
  `refresh_workflow_tasks_canceled_context_is_propagated` test cancellation
  semantics; do not close them as accepted differences without checking caller
  context behavior against Go.
- File-based dynamic config reload test in
  `cadence-service-core/tests/matching_partial_upgrade_e.rs` describes a
  one-shot overlay versus Go polling. Determine whether Matching's supported
  config source requires live file reload; OpenFeature live updates are a
  separate provider path.

### Mock-only, architecture-shape, or stale claims (not proof of production gaps)

- `go_new_config_every_field_has_a_rust_reader` (`partial_upgrade_a`): Go's
  66-field `Config` aggregation has no Rust counterpart; verify config keys at
  their owning readers, not by adding a duplicate struct.
- `go_handle_err_passes_typed_errors_through_unchanged` and
  `go_handler_start_calls_engine_start_once_and_stop_calls_engine_stop_once`:
  Go wrapper object identity/lifecycle is not Rust's API; RPC status and actual
  launcher lifecycle tests are the relevant contracts.
- `poller_history_callback_fires_on_start_and_end_with_identity` and
  `poller_cancel_reports_whether_the_poll_is_known`: Go callback/return shape
  tests for the poller utility; evaluate RPC outcomes and observable poller
  metrics instead of adding mock-only APIs.
- Ignored injected failures in `matching_engine_go_tests_b.rs`,
  `matching_engine_go_tests_c.rs`, and `matching_rpc_tests.rs` rely on Go
  gomock manager failures. Do not add public failure injection. Exercise a
  real failing persistence/transport boundary if it is an observable supported
  failure path.
- `domainChangeCallback_*` and `registerDomainFailoverCallback_*` call Go
  private methods. Rust folds the behavior into `start_domain_cache`; test
  version convergence and released pollers through runtime behavior.
- `err_if_shard_ownership_lost_go_subtests_not_reproduced` calls Go's private
  engine helper; use shard-distributor/ownership runtime behavior tests.
- `shard_distributor_go_interop` and the two ignored History TChannel cases are
  skipped mixed wire/oracle validations, not test-only discrepancies. They
  need their named CI oracle/gate oracles before their behavior is counted as
  established.
- The ignored `matching_delivery` test requires original Go History. It is a
  real mixed dependency scenario, not a reason to introduce a mock API.

## Immediate remaining work

1. Run the new startup test and the Matching package CI; then fresh uncached
   workspace strict clippy followed by workspace tests. Record Actions run URL,
   head, outcomes and logs in evidence.
2. Resolve the active-offer case with reachable Go History and inspect the
   semaphore/config ignored cases against production paths.
3. Run current-head mixed SDK/MySQL, PostgreSQL, Cassandra, Thrift/TChannel,
   configstore and shard-distributor lanes serially; preserve each binary hash.
4. Complete live Go/Rust emitted metrics comparison and same-membership owner
   ordering oracle. Reconcile failures and open behavior rows before any
   Matching completion claim.
