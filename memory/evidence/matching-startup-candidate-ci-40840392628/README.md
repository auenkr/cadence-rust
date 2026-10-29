# Matching startup candidate CI evidence — 40840392628

Authoritative GitHub Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40840392628
Workflow `rust-ci`, branch `t3/matching-startup-integration-review`, exact head
`d4ee53f00d505246dc0b7fbbe353d7e989a91c49`; workflow conclusion `success`.
The run completed 2026-10-29 08:18:16Z. The uploaded `commit.txt` agrees with
that head. Run metadata and raw Rust CI evidence logs/exits are alongside this
note. Downloaded artifacts were inspected from this run only.

The workflow selected `cadence-matching` (`PKG=cadence-matching`). Clippy ran
before `cargo test`; both exit files contain `0`. The library harness reports
975 tests: 973 passed, 0 failed, 2 ignored. All package integration binaries
also passed. Across 27 Cargo test-result summaries in the retained `test.log`,
there are 1,083 pass results, 0 failures, and 16 ignored results; that sum
includes several exact/filtered reruns of library tests and is not a count of
unique tests. Doc tests ran zero tests. No mixed Go/Rust runtime gate was run.

Formatting details: `fmt.exit=0` (the separately scoped Unleash formatting
check); `matching-runtime-format.exit=0`; `query-format.exit=0`; and
`tls-format.exit=0`. The advisory workspace-wide `cargo fmt --all -- --check`
recorded `workspace-format.exit=1`; `format.log` identified a formatting diff
in `rust/crates/cadence-api/src/admin.rs:371`. The workflow intentionally
captured this advisory result and continued. The original large `format.log`
was omitted from this compact record; its exact exit and first reported path
are preserved here. No format patch was applied to source by this evidence
collection.

The artifact target was `/home/runner/work/cadence-rust/cadence-rust/rust/target`;
the Actions log shows a Rust cache restore. This is a successful selected-crate
CI run, not the required fresh uncached workspace gate and not final Matching
acceptance.

The test log contains pre-existing cold-domain/runtime coverage, including
`grpc_matching::cold_domain_metric_tests::*`,
`matching_grpc_listener_registers_meta_health_and_gates_startup`,
`matching_host_native::tests::startup_gate_opens_and_shutdown_wakes_waiters_with_unavailable`,
and `thrift_matching::tests::matching_thrift_nil_request_waits_for_startup_like_go_start_profiling`.
These are exactly the tests appearing in this run's log; this CI head's commit
itself changed no Rust test files. Do not attribute later semaphore startup
timeout, expanded adapter/lock, or two-thread regression cases to this run.

This run validates only the selected Matching package at this exact head. It
does not validate later candidates `25e`, `90cd`, `e84e`, or `8cc`; nor does it
replace frozen candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` or alter its
failed PostgreSQL evidence and the ten required mixed lanes.
