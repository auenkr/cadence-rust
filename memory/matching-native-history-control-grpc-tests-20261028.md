# Native History control gRPC scripted-peer tests (2026-10-28)

UNVERIFIED: written without local Cargo; needs CI (rust-ci.yml, -p cadence-matching).

Added to `matching_history_native.rs` test module (no production changes):
- `ScriptedControl` tonic service on a real HTTP2 channel (TcpListener, oneshot-shutdown, bounded join) serving GetMutableState, DescribeWorkflowExecution, RefreshWorkflowTasks.
- `scripted_peer_control_calls_keep_raw_bytes_and_single_attempt`: asserts exact raw domain/workflow/run/domain-name bytes (`\xff\xfe` invalid UTF-8), raw responses, exactly one refresh attempt, empty RefreshWorkflowTasksResponse encodes to zero bytes and round-trips. 20s overall timeout.
- `refresh_native_until_cancels_when_caller_gone`: ready caller-gone future yields Code::Cancelled.

Assumptions to check on CI: `GetMutableStateResponse.next_event_id` field exists; `prost` is a dev/normal dependency of cadence-matching; RefreshWorkflowTasksResponse derives PartialEq.
TChannel peer untouched (CI 40616972759).

Parent review: added raw mutable-state and describe response fields/assertions, used a lexical lock scope before awaiting shutdown, and made shutdown timeout abort and join the peer task and fail the test rather than silently detach it. Tests remain unverified until a new CI source pin.
