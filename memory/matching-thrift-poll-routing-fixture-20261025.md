# Matching Thrift poll routing fixture correction (2026-10-25)

- Prior CI: https://github.com/auenkr/cadence-rust/actions/runs/40176622622
- Prior head: `70a7eebb1e75e6b3d50c93077c63cf1bf686d7d9`; conclusion: failure.
- Failure: `matching_partial_upgrade_e.rs:500` in `go_thrift_handler_routes_each_tested_method`: PollForActivityTask surfaced Go's nil-pointer panic because the routing test dispatched an empty Thrift struct, leaving its required inner PollRequest absent.
- Fix: only the PollForActivityTask routing case now sends an inner PollRequest with a domain and task list. This keeps the routing assertion meaningful. Dedicated nil-request behavior tests remain unchanged (`matching_rpc_tests.rs`, `go_activity_poll_absent_request_*`).
- Validation policy: no local Cargo, Go, or formatting commands. `git diff --check` passed locally; CI is dispatched on the public ubuntu runner as requested.
- Follow-up CI URL/head/conclusion: to be recorded after dispatch below.
