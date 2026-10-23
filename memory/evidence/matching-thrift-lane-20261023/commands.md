# Commands and exit codes (2026-10-23)

Worktree `/mnt/storage/service/github/wt/thriftlane`, branch `matching-thrift-lane-20261023`, base 5ff4d0ac1.

| Command | Exit |
|---|---|
| `python3 -m py_compile tests/run-go-service-baseline.py` | 0 |
| `python3 -B tests/run-go-service-baseline.py --rust-services matching --mysql --matching-transport thrift --dry-run --artifacts-dir /tmp/thriftlane-dry` | 0 (dynamic-config.yaml gets `system.enableGRPCOutbound: false`; rust-dynamic-config.yaml keeps exactly one key) |
| `cargo-safe check -p cadence-matching --all-targets` (first attempt: closure lifetime error) | 101 |
| `cargo-safe check -p cadence-matching --all-targets` (after fix) | 0 |
| `cargo-safe clippy -p cadence-matching --all-targets -- -D warnings` | 0 |
| `cargo-safe test -p cadence-matching` | 101: 397 passed, 1 failed (`grpc_matching::durable_tests::matching_rpc_durable_cancel_unregisters_waiting_poller`, `Elapsed` at matching_rpc_tests.rs:613), see `test.log` |
| `cargo-safe test -p cadence-matching --lib matching_rpc_durable_cancel_unregisters` (rerun) | 0, 1 passed (timing flake under load; not touched by this change) |

New tests pass in the full run: `tchannel::tests::application_error_details_truncate_like_yarpc`,
`thrift_matching::tests::matching_thrift_exceptions_fill_required_fields_like_go_mapper`.

No gate was run. No mixed Go/Rust execution of the thrift lane has happened.
