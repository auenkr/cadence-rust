# Matching isolation-group observations (2026-10-16)

Continuation after scoped dynamic config commit f4bb9a5d5. In-progress change:

- Durable decision/activity poll adapters now preserve request.isolation_group
  in the existing PollLifecycle/common poller manager.
- DescribeTaskList reads `system.allIsolationGroups` live (the Go cmd/server
  getFromDynamicConfig source, not global admin isolation-state overrides),
  rejects the entire list if any element is not a string, and emits GroupQPS
  and recent poller counts for every configured group, including zero groups.
- Counts use live task-list-info-filtered `matching.taskIsolationPollerWindow`
  (2s default); outstanding pollers remain counted beyond the recent window,
  completed pollers do not. Common manager already ports these Go semantics.
- Tests extend activity poller/Describe and arrival-QPS RPC coverage and add a
  string-list decoding test.

Initial validation passed: 3 persistence tests, 83 server tests and strict
all-target workspace Clippy. Additional live-group enumeration assertions and
a new decision-poller attribution test were added while that build was running;
these require the fresh full runner below. Initial log
`/tmp/opencode/cadence-isolation-tests.log`, exit marker
`/tmp/opencode/cadence-isolation-tests.exit` = 0.

The scoped-config full runner is also running. Workspace checks built the
scoped-config checkpoint; later stages may build the new isolation code, so
this run is not frozen-source evidence for isolation reporting. Run a fresh
full runner once the reporting code and tests are final. A fresh full runner is
queued under tmux cadence-rust:isolation-full after the scoped run ends:
`/tmp/opencode/cadence-isolation-full.log`, marker
`/tmp/opencode/cadence-isolation-full.exit`, artifacts
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-isolation-metrics-full`.

Open: isolation-aware task selection, duration fallback, isolation state,
balancer/partition assignments, forwarding, semaphores, membership and standalone
durable service wiring. Observation support does not complete the isolation gate.

Final frozen-source validation CONFIRMED PASSED: all 29 full-runner stages,
395 workspace tests (84 server lib tests), strict workspace all-target Clippy,
Go SDK TChannel, Go History delivery, and mixed Go services/Rust Matching.
`/tmp/opencode/cadence-isolation-full.exit` = 0; artifacts above include
`results.json` with all exitCode=0. Both decision and activity attribution tests,
live group replacement and completed-poller window exclusion passed.
