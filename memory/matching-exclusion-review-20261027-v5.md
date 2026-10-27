# Matching exclusion review at final acceptance pin v5

Read-only source audit at tag `matching-acceptance-20261027-v5`, commit
`8d7dac38e8d7e5a8114c6da5e0c3cc3afcbc77d0`. Scope was `#[ignore]`,
`todo!`/`unimplemented!`, and explicit unsupported behavior in
`rust/crates/cadence-matching` plus its directly required MySQL interoperability
test. No tests, builds, gates, or runtime were run; no active CI results were
read. This note does not change the existing acceptance checklist or declare
Matching complete.

## Concrete behavior gap

`rust/crates/cadence-matching/src/matching_rpc_tests.rs:1808` ignores
`matching_rpc_non_durable_describe_reports_task_id_block_and_empty`: the
non-durable `DescribeTaskList` response omits `task_id_block` and `empty`.
`grpc_matching.rs:3840-3974` shows this is the non-durable status path; the
durable status calculation is separate. The source contract and prior audit
are recorded in `memory/matching-final-parity-20261024.md:48,77`: Go derives
these values from the task-list ack manager; the Rust non-durable fixture has no
ack manager. This is a concrete discrepancy for that legacy/helper path, but
the cited audit also establishes it is not the configured production Matching
path (launcher/runtime uses durable queues). The ten mixed acceptance lanes in
`memory/matching-acceptance-status-20261027.md` exercise deployed durable
behavior and do not close this non-durable discrepancy. Treat as scoped legacy
path parity debt, not a final acceptance blocker absent a supported caller of
`Matching::new`.

`matching_partial_upgrade_e.rs:431-432`,
`go_dispatch_task_active_offer_without_error_reaches_poller`, is the other
direct behavior claim: the fixture's AddDecisionTask returns successfully but
the waiting poll does not receive the task, while Go's contract hands it to
MustOffer. The ignore reason itself marks the proposed unreachable-History
cause unverified. This is not superseded by unit test scaffolding: it is a
possible real delivery gap. The existing active-offer lane
`.github/workflows/matching-active-offer-gate.yml` and its review map
`memory/matching-acceptance-gate-gap-update-20261026.md` provide the relevant
production mixed-topology coverage pointer (`activeOffer.json`, probe and
Rust logs); only artifact evidence at this exact pin can resolve it. Runs
40483216431 and 40483221504 were explicitly left uninspected per task, so this
review makes no claim about their outcomes.

## Ignored tests and disposition

| Test | Disposition / applicable coverage |
|---|---|
| `matching_partial_upgrade_a.rs::go_new_config_every_field_has_a_rust_reader` | Structural Go `Config` table assertion; Rust reads settings at their owning modules and has no equivalent aggregate `NewConfig`. Not itself a runtime behavior contract. Configuration behavior is covered by the configstore production lane; this test adds no separate acceptance requirement. |
| `matching_partial_upgrade_a.rs::go_handle_err_passes_typed_errors_through_unchanged` | Tests Go's generic internal helper and wrapped error identity. Rust maps errors at RPC boundaries to tonic status; helper identity is not a public API contract. Existing RPC/SDK mixed lanes cover caller-visible behavior; no specific mismatch is established here. |
| `matching_engine_go_tests.rs::get_task_lists_by_domain_success_reports_poller_identity` | Go injects a scripted poller identity; Rust derives poller history from actual long polls. Mock-fixture limitation; no evidence that production reports the wrong live identity. SDK/mixed lanes are indirect coverage only, not a direct identity assertion. |
| `matching_engine_go_tests.rs::list_task_list_partitions_success_owner_hosts_match_go_lookups` | Scripted owner names and injected lookup error cannot be supplied through Rust's ring-backed API. Not proof of wrong ring result. Shard-distributor and two-node lanes exercise real ownership/routing, but do not assert this RPC's exact owner ordering; no concrete mismatch established by this ignored fixture. |
| `matching_engine_go_tests.rs::err_if_shard_ownership_lost_go_subtests_not_reproduced` | Eight private closure cases require Go's mock executor/resolver/shutdown channel. Rust implementation is `matching_runtime.rs::shard_task_list_ownership_check` and is reached through runtime/shard-distributor. Shard-distributor and two-node recovery lanes are relevant production coverage, but do not independently assert every injected subcase. These are test seam omissions, not separately identified production defects. |
| `matching_engine_go_tests_b.rs::query_workflow_failed_to_dispatch_query_task_is_error` | Requires injecting TaskListManager dispatch failure; no public seam. This is a failure branch not directly represented by the ignored mock test. Query dispatch/SDK lanes cover normal dispatch; no mismatch in production failure behavior is evidenced by this test. |
| `matching_engine_go_tests_b.rs::update_task_list_partition_config_tasklist_manager_error` | Requires injected durable update failure. No seam, and no evidence from the marker alone that durable error propagation differs. Persistence mixed lanes exercise real backend behavior, not this synthetic injected failure. |
| `matching_engine_go_tests_c.rs::refresh_task_list_partition_config_manager_error_is_propagated` | Same mock-only failure injection limitation for durable refresh; not a proven behavior mismatch. Relevant backend/runtime lanes do not claim exact synthetic error injection. |
| `matching_engine_go_tests_c.rs::domain_change_callback_updates_failover_version_and_releases_eligible_pollers`; `register_domain_failover_callback_catch_up_without_updates_keeps_version`; `register_domain_failover_callback_catch_up_advances_version_to_highest` | Private Go callback state is implemented inside Rust `matching_runtime.rs::start_domain_cache`; no public seam to inspect counters/callback registration. Lifecycle and domain-change production paths are exercised indirectly by mixed service lanes. No concrete external mismatch identified by these internal assertions. |
| `matching_partial_upgrade_e.rs::go_thrift_handler_internal_error_maps_to_internal_service_error` | Go mock-handler exception mapping cannot be induced through Rust's concrete dispatcher. The Thrift production lane covers actual transport/RPC interoperability, but not this synthetic injected handler failure. Not evidence of a caller-visible mapping defect. |
| `matching_rpc_tests.rs::matching_rpc_non_durable_describe_reports_task_id_block_and_empty` | Concrete but scoped discrepancy described above; not covered by production durable lanes. |
| `shard_distributor_go_interop.rs::rust_executor_and_spectator_interoperate_with_pinned_go_handlers` | Explicit opt-in Go/Rust wire fixture. Matching shard-distributor production lane is the acceptance coverage pointer; this fixture is not required in ordinary Rust CI. Do not infer its exact artifact result from current runs. |
| `matching_delivery.rs::matching_delivery_go_history` | Explicitly requires original Go History. The production mixed gates retain Go History and run SDK/service scenarios; this unit fixture is an alternate focused harness, not an unported implementation. See lane map in `memory/matching-acceptance-evidence-readiness-20261027.md`. |
| `matching_history_tchannel.rs::generated_go_history_server_accepts_native_tchannel_calls` | Explicit Go peer binary opt-in, testing Rust's History client wire calls used by Matching. TChannel/Thrift production mixed lane is the acceptance pointer; not a standalone Matching behavior omission. |
| `cadence-persistence/tests/matching_mysql_interop.rs::mysql_interop_harness` | Opt-in harness needs the Go fixture and ephemeral MySQL 8. MySQL-probes and two-node MySQL acceptance lanes exercise shared Go schema with Rust Matching; this ignored local harness is not missing persistence implementation. |

The bounded scan found no production `todo!` or `unimplemented!` in the
Matching crate. The `unimplemented!` occurrences are bodies of explicitly
ignored mock/seam tests listed above. `matching_engine_go_tests_b.rs` has a test
named `wait_for_query_result_strong_consistency_version_unsupported`; it tests
the intentional unsupported strong-consistency version response, not an
unimplemented implementation. The TChannel source's unsupported event-type
error is an explicit error path, not a TODO marker. No disposition above
supersedes the separate exact-pin runtime evidence requirement.
