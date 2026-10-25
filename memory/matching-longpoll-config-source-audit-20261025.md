# Matching long-poll and config source audit (2026-10-25)

Audited at `6af476941031193317fdd6c44070e9785e6384b9` before this note.
No Cargo, Go, formatting command, CI dispatch, or mixed gate was run.

## Long-poll claim corrected

`rust/crates/cadence-matching/tests/matching_longpoll_expiry_parity.rs` has two
active tests, with no `#[ignore]` attribute:

- `decision_started_after_long_poll_window_still_returns_task` at line 212
- `activity_started_after_long_poll_window_still_returns_task` at line 262

Both park a poller, match at 1.3 seconds into a 2-second long-poll window, and
use a History stand-in whose `Record*TaskStarted` response takes 900ms. The
decision test asserts `started_event_id == 92`; the activity test asserts a
non-empty task token. The module comments explain the production behavior fix:
the long-poll window bounds waiting for a task, while claimed-task History start
and response wait use the caller RPC deadline, matching Go's
`go/service/matching/handler/engine.go:808` and
`go/service/matching/tasklist/task_list_manager.go:708` behavior. Thus the
2026-10-25 reconciliation at line 23 incorrectly called the decision case
ignored and the fix only probable. The current source establishes it is active
and documents the corresponding behavior. Execution remains unverified at
this exact SHA because CI was deliberately not dispatched.

## Config completeness test disposition

`matching_partial_upgrade_a.rs:85-88` ignores
`go_new_config_every_field_has_a_rust_reader` because it requires one Rust
`Config` object corresponding to Go's `NewConfig`. That is an architectural
shape test, not evidence that runtime settings are missing. Go's runtime
settings are the providers wired in `go/service/matching/config/config.go`
`NewConfig`; static values such as `RangeSize`, `HostName`, RPC config,
`TaskDispatchRPS`/TTL, delete interval, and isolation-group callback are not
dynamic-config keys. Rust reads dynamic settings at their use sites instead of
materializing the same struct.

Source cross-checks found current Rust reads for the potentially easy-to-miss
Go settings: `matching.enableGetNumberOfPartitionsFromCache`,
`matching.qpsTrackerInterval`, `matching.readRangeSize`,
`matching.maxTaskDeleteBatchSize`, scaler/isolation settings, task-start
timeout, standby completion, client auto-config, shutdown drain, and short-lived
task-list exclusion. See `matching_dyncfg_defaults_tests.rs:27-80` for the
source/default inventory, `grpc_matching.rs`, `matching_registry.rs`,
`matching_history.rs`, `matching_runtime.rs`, and `launcher.rs` for their live
read sites. Existing configuration behavior tests also set keys at actual
runtime boundaries (for example `matching_rpc_tests.rs:2802-2936`).

No concrete absent Go `NewConfig` runtime setting was established in this
source-only pass, so no production setting was added. The ignored test remains
useful only as a reminder that Rust intentionally has no monolithic Go-shaped
config struct; it must not be cited as an actual runtime config parity gap.
