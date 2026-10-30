# Matching CI and ignored-test checkpoint — 2026-10-30

## Frozen source and current run

The candidate is remote branch `t3/matching-failure-coverage-20261030142037`
at full SHA `df461a3e046c6a0da1ac440d7be9acebcc12cd48` (verified with
`git ls-remote`). The user reports the only source edits versus its formatting
candidate are the two CI-provided import-order corrections in
`matching_partition_cancellation_tests.rs`.

At the one permitted observation, fresh uncached workspace run
[41030554541](https://github.com/auenkr/cadence-rust/actions/runs/41030554541)
was `in_progress`, workflow `rust-ci`, event `workflow_dispatch`, branch and
head SHA exactly matching the frozen candidate. Job `clippy-test` had checked
out that ref. Its original-Go raw-byte oracle was running; Clippy, formatting,
workspace tests, and final artifacts were still pending. Do not dispatch a
mixed lane until the run reaches terminal success and its uploaded evidence
confirms clean-target selection and exit 0 for Clippy followed by workspace
tests. No second observation or dispatch was made in this checkpoint.

The immediately preceding full-workspace success
[41028655670](https://github.com/auenkr/cadence-rust/actions/runs/41028655670)
at `22c695d65af367a0b49b605877f72ec33fa2beb8` is historical and does not test
the candidate's new tests. Its downloaded `rust-ci-logs/.../rust/test.log`
contains 30 distinct ignored test cases; the inventory and disposition below
are transcribed from those actual test lines. Its aggregate result (1,827
passed, 30 ignored, 57 harnesses) is not candidate evidence.

## Exact 30 ignored tests and disposition

### Missing parity evidence (13)

These are not satisfied by the existing broad SDK/database workflows. Each
requires a targeted Rust fixture or an existing public-path assertion that
actually reaches and checks the named behavior.

1. `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` —
   non-durable `DescribeTaskList` fallback fields. No current workflow asserts
   this synthetic/non-durable path; add a focused RPC test (or document why
   this mode is out of product scope).
2. `err_if_shard_ownership_lost_go_subtests_not_reproduced` — eight Go cases
   for missing/nil shard process, shutdown, and excluded/non-excluded ownership
   changes. `matching-production-validation.yml` lane `shard-distributor`
   tests live routing, but is not evidence for all eight individual branches;
   add a direct test seam or explicit gate assertions for each.
3. `get_task_lists_by_domain_success_reports_poller_identity` — scripted
   poller identity projection. Live SDK polling is only indirect evidence;
   add a direct public RPC assertion with a connected poller.
4. `list_task_list_partitions_success_owner_hosts_match_go_lookups` — scripted
   lookup results and lookup-error fallback. The real shard-distributor lane
   tests production ring routing, not these scripted return/error cases.
5. `query_workflow_failed_to_dispatch_query_task_is_error` — dispatch error
   propagation to caller. No current mixed lane injects a TaskListManager
   dispatch error; add an injectable failure fixture and assert the wire error.
6. `update_task_list_partition_config_tasklist_manager_error` — update
   failure propagation. PostgreSQL's `probe-list-partitions` checks successful
   live partition behavior, not injected manager failure; add a targeted
   failure assertion.
7. `domain_change_callback_updates_failover_version_and_releases_eligible_pollers`
   — callback version change and selective blocked-poller release. Existing
   domain-cache lifecycle/SDK gates do not assert this exact callback outcome;
   add a callback-facing fixture.
8. `refresh_task_list_partition_config_manager_error_is_propagated` — refresh
   failure propagation. PostgreSQL's `probe-list-partitions` is success-path
   only; add an injected-failure assertion.
9. `register_domain_failover_callback_catch_up_advances_version_to_highest` —
   registration-time catch-up maximum version. No mixed workflow asserts this
   exact private callback catch-up; expose a narrow test seam or test the
   production cache-to-registry boundary.
10. `register_domain_failover_callback_catch_up_without_updates_keeps_version`
    — empty catch-up preserves version. Same missing callback fixture as item 9.
11. `go_handle_err_passes_typed_errors_through_unchanged` — the 14 typed Go
    error identities and wrapped forms. No aggregate SDK suite proves this
    table; map each type to its Rust gRPC/Thrift wire status/exception in a
    focused compatibility table.
12. `go_new_config_every_field_has_a_rust_reader` — all 66 Go config keys,
    defaults and readers. Runtime config lanes exercise selected settings only;
    add a key-by-key Go/Rust configuration oracle.
13. `go_thrift_handler_internal_error_maps_to_internal_service_error` — each
    Thrift method's injected internal error and zero-value response. The Thrift
    mixed lane covers normal SDK calls, not handler-wide injected failures;
    add a wrapper test seam and method matrix.

### Dependency-gated tests (17)

These have concrete opt-in infrastructure lanes. Ignoring them in workspace
CI is expected, but their corresponding mixed/oracle evidence is still required.

1. `generated_go_history_server_accepts_native_tchannel_calls` — direct Go
   History peer test using `CADENCE_HISTORY_PEER_BIN`; no dedicated workflow
   currently sets that variable. Mixed Thrift/TChannel SDK lanes cover service
   interop indirectly; add a direct peer lane if this exact generated-server
   assertion remains required.
2. `matching_delivery_go_history` — run
   `tests/run-go-service-baseline.py --matching-delivery` against original Go
   History. Existing baseline/mixed SDK workflow path; ensure it calls the
   current `cadence-matching` test target (older script references to
   `cadence-server` are stale).
3. `rust_executor_and_spectator_interoperate_with_pinned_go_handlers` — pinned
   Go handler wire interop. Map to the serialized shard-distributor Go
   interoperability script/workflow; that test's own ignored reason names the
   Go dependency cache and serialized lane.
4. `matching_cassandra::tests::go_seed_rust_write_restart_go_verify` — run
   `matching-cassandra-mixed-gate.yml` with the Go schema and restart lane.
5. `matching_domain_metadata::tests::go_domain_store_database_interop` — run
   `tests/run-matching-domain-metadata-compat.py` with the Go SQLite seed.
6. `matching_mysql::tests::mysql_async_tls_uses_go_socket_timeout_options` —
   opt-in TLS MySQL interop. Include the MySQL production TLS/mixed lane or a
   focused MySQL TLS wrapper test; ordinary PostgreSQL lane does not cover it.
7. `matching_mysql::tests::timed_domain_query_bounds_a_stalled_socket_read` —
   MySQL interop server required. The production MySQL probes lane is the
   runtime counterpart; check this exact socket-stall case in its emitted
   database evidence or add it to the wrapper.
8. `matching_mysql::tests::timed_domain_query_bounds_connect_failure_and_releases_slot`
   — MySQL connection failure case; same MySQL opt-in wrapper.
9. `matching_mysql::tests::timed_domain_query_cancels_only_its_dedicated_connection`
   — MySQL cancellation isolation; same MySQL opt-in wrapper.
10. `matching_mysql::tests::timed_domain_query_cancels_while_draining_a_slow_result_set`
    — MySQL slow result cancellation; same MySQL opt-in wrapper.
11. `matching_mysql::tests::timed_domain_query_completes_before_deadline_and_disconnects`
    — MySQL normal completion/connection release; same MySQL opt-in wrapper.
12. `matching_mysql::tests::timed_domain_query_releases_single_pool_slot_after_cancellation`
    — MySQL pool-slot release after cancellation; same MySQL opt-in wrapper.
13. `matching_mysql::tests::timed_domain_query_resets_read_timeout_for_each_socket_operation`
    — MySQL per-operation timeout reset; same MySQL opt-in wrapper.
14. `matching_postgres::native::tests::live_native_store_raw_bytes_fencing_and_legacy_cross_read`
    — disposable PostgreSQL live store/oracle test. PostgreSQL mixed production
    validation is the required backend lane; this exact test must be enabled
    by the wrapper or run under a database-focused PostgreSQL oracle.
15. `matching_postgres::tests::postgres_tls_live_oracle` — PostgreSQL TLS
    certificate/connection oracle. Existing `matching-production-validation`
    lane `postgres` does not configure TLS. A separate PostgreSQL TLS oracle is
    needed; `matching-production-tls-mixed.yml` is MySQL-backed and cannot
    substitute.
16. `matching_sqlite::interop_tests::matching_sqlite_go_database_interop` —
    run `tests/run-matching-storage-compat.py` with an original-Go SQLite seed.
17. `mysql_interop_harness` — run
    `tests/run-matching-mysql-interop.py` against ephemeral MySQL 8; the
    production MySQL mixed lane may provide broader service evidence, but the
    harness itself should be enabled or explicitly superseded by retained
    equivalent test output.

The listed 17 are classified by prerequisites, not credited as passed: each
requires its stated runner evidence. In particular, PostgreSQL TLS, Go History
peer, and the individually ignored SQL socket timing cases do not become
covered merely because a general mixed SDK workflow passes.

## Pinned PostgreSQL mixed-gate plan

Once run 41030554541 has terminal success, inspect its downloaded artifact and
require all of the following before dispatch: `headSha` and artifact
`commit.txt` equal `df461a3e046c6a0da1ac440d7be9acebcc12cd48`; fresh target
selection is recorded; `clippy.exit=0`, then `test.exit=0`; workspace test log
shows the new cancellation tests ran (not ignored). If any condition fails,
do not dispatch and report the smallest correction required.

Then run the existing workflow on the immutable branch:

```sh
gh workflow run matching-production-validation.yml \
  --repo auenkr/cadence-rust \
  --ref t3/matching-failure-coverage-20261030142037 \
  -f lane=postgres
```

Before dispatch, verify `git ls-remote origin
refs/heads/t3/matching-failure-coverage-20261030142037` still resolves to the
full SHA above. The workflow uses `actions/checkout@v4` at the event ref,
records `git rev-parse HEAD` before the build and again before the mixed gate,
builds `cadence-matching` in a per-run fresh target, hashes that executable,
and uploads its compact evidence. After the gate, require Actions `headSha`,
both recorded commit identities, build/gate/nested run exits, and binary hash
provenance to agree. Inspect `results.json`, `probe.log`, SDK/restart/recovery
assertions, Go Frontend/History/Worker topology with no Go Matching, and
PostgreSQL container cleanup; workflow success alone is insufficient. Record
the actual run URL, full source SHA, artifact exit values and binary SHA256 in
the follow-up evidence note. Dispatch only one mixed lane at a time.

This checkpoint makes no service-completion claim. Other required native mixed
lanes and untested behavior remain open in
`memory/matching-native-acceptance-ledger-20261029.md`.
