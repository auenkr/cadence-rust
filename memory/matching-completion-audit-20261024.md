# Matching completion audit — 2026-10-24, source ae442535c

> Current validation status (2026-10-25): this document retains historical
> observations below. The authoritative integrated-source tracker is
> [final candidate evidence](evidence/matching-final-candidate-20261024/README.md),
> pinned to `27d3ff5f60f0604553824420da0cf61026eda468`. Workspace strict clippy
> and tests, PostgreSQL, Thrift and Cassandra lanes passed that candidate.
> MySQL run 40153901033 is running; configstore, shard-distributor and
> active-offer candidate lanes remain pending. Matching is not signed off.
> Owner ordering including membership changes is covered by the executable
> Go/Rust oracle preserved in the candidate workspace evidence. Metrics
> comparison evidence is preserved there too, with the representative-path
> limits in `memory/matching-metrics-emission-comparison-20261024.md`.
> Startup root-to-child and production watcher lifecycle tests are integrated;
> older statements that they are unexecuted or absent are superseded.


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

### Focused completion branch CI

- Matching package run
  <https://github.com/auenkr/cadence-rust/actions/runs/40090630855> passed on
  head `4e88d36b7e10900834fdd13403f83b3d4448f0bf`. The workflow used
  `package=cadence-matching`, `clean_target=true`; strict clippy, formatting,
  and package tests all passed. The new loaded-root startup-to-child test passed
  as part of `matching_partial_upgrade_e.rs`.
- Earlier attempts on `e39cd8d5c` and `c94781a18` failed only in the new test's
  topology fixture: first the root resolved to the child (correct ownership
  rejection), then a test searched placement addresses while Ringpop virtual
  points are keyed by identity. The fixture now varies both identities and
  placement addresses to obtain parent-owned root and child-owned partition;
  this passed run 40090630855. No production code was altered for those test
  failures.
- Fresh workspace validation was dispatched as
  <https://github.com/auenkr/cadence-rust/actions/runs/40090951826> on the same
  code head `4e88d36b7e10900834fdd13403f83b3d4448f0bf`, with
  `clean_target=true`; conclusion `success`. Workspace strict clippy and then
  workspace tests passed on a fresh uncached runner target.

## Current ignored tests: stale mock cases versus actual gaps

The old count of 41 ignored tests is obsolete. A scoped scan of the current
Matching crate and Matching-related service-core test found 24 `#[ignore]`
attributes; this includes mixed oracle/gate skips, not 24 production gaps. The
startup notification placeholder has been replaced by an active behavior test
and is no longer in this ignored set.

### Behavior gaps or unclosed behavior evidence

- `go_dispatch_task_active_offer_without_error_reaches_poller` in
  `tests/matching_partial_upgrade_e.rs`: the ignored fixture still uses an
  unreachable History target (`127.0.0.1:1`) and a fabricated task, so it is
  not evidence of a production delivery failure. A production mixed probe now
  covers the behavior against real Go History (details below). Keep the unit
  test ignored and unchanged; do not attribute its timeout to production.
- `list_task_list_partitions_success_owner_hosts_match_go_lookups` in
  `tests/matching_engine_go_tests.rs`: mock scripted host ordering differs from
  live ring lookup. This is a genuine parity evidence gap; add same-membership
  Go/Rust oracle coverage.
- `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` is
  limited to legacy non-durable `Matching::new`; production launcher paths use
  durable queues. Do not treat this as a production gap absent a supported
  deployed non-durable source.
- Semaphore parameter validation, paging, shutdown admission, and
  acquire-during-load semantics are not ignored in this checkout. Their current
  tests include `matching_semaphore_parity_tests.rs`,
  `matching_rpc_semaphore_rejects_new_buckets_while_shutting_down`, and
  `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline`; keep
  these in the final CI and mixed backend evidence review rather than carrying
  forward stale ignored-test claims.
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

### Active offer mixed evidence — 2026-10-24

The focused active-offer probe was added to
`tests/matching-probe-scenarios.cjs` and exposed as
`--probe-active-offer` by `tests/run-go-service-baseline.py`. It starts a direct
Rust Matching `PollForDecisionTask`, waits until `DescribeTaskList` reports its
unique poller identity, then calls Go Frontend `StartWorkflowExecution` for
that same task list. Go History creates the real workflow decision task and
calls Rust Matching `AddDecisionTask`. The probe checks the returned token and
exact workflow/run IDs, then completes the task through Go Frontend. To rule
out a persisted task being consumed before an after-delivery count, the probe
installs a uniquely named, task-list-scoped temporary MySQL `BEFORE INSERT`
audit trigger while scheduling. It requires zero matching inserts as well as
zero remaining task rows, then removes the trigger and audit table. The ignored
Go-shaped unit test was not edited or enabled.

- Mixed CI run: <https://github.com/auenkr/cadence-rust/actions/runs/40104204435>
- Conclusion: `success`; source/head:
  `c1426544dba42035122ba925a18c1e90534e8f5a`.
- The run used a fresh runner target and built
  `cadence-matching`; binary SHA256:
  `1a369ac12b60dc5d1a5e8dc035b6e1a1e22109bb54cfee1d7a276f780ab9a640`.
- Artifact topology says Go Matching was not started; Go Frontend, History,
  and Worker were running with Rust Matching through production Ringpop. The
  `activeOffer.json` result passed for workflow
  `active-offer-workflow-c5c78e70-ee54-4753-b63e-6b07e7456313`, run
  `57fe82a8-f67f-4c3f-94fd-aa93ee03cbd7`: history-scheduled task delivered,
  task token delivered, `persistedTaskRows: 0`, and
  `persistedTaskInsertAttempts: 0` while the scoped `BEFORE INSERT` trigger was
  active.
- The legacy SDK mixed workflow in the same gate also completed (`probeExitCode:
  0`). The gate's full command and logs are retained in the run artifact.

This closes the active-offer production evidence gap for the supported mixed
topology. It does not convert the mock-based `TestDispatchTask` expectation
into an end-to-end proof of every Go dispatch error branch, nor is it a
Matching completion sign-off; remaining parity rows and current-head backend
gates above are still open.
