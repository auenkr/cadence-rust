# Matching forwarded-poll failure fallback: Go to Rust evidence (2026-10-24)

Branch `matching-forward-poll-fallback-20261024` (from origin/master 8d31b6f61). Source: item 4 of
`memory/matching-cannot-port-review-20261024.md` (row 39 `Test_pollOrForward_ForwardTaskThenPoll`,
row 14 `TestForwardPollError`).

Test file: `rust/crates/cadence-matching/tests/matching_forward_poll_fallback.rs`. It uses only the
public Matching surface (`Matching::with_durable_queues`, `with_membership`, `QueueRegistry::new`,
`Membership::new`/`owner`, `AppState::default`). A test-only fake parent fails every forwarded
poll and counts attempts. No production code changed.

Verification: GitHub Actions `rust-ci.yml`, `-f package=cadence-matching` (clippy `--all-targets
-D warnings`, then `cargo test -p cadence-matching`). No local cargo.

| Go test | Rust test | Status | Run URL |
|---|---|---|---|
| `tasklist/matcher_test.go:Test_pollOrForward_ForwardTaskThenPoll` | `forwarded_decision_poll_failure_delivers_later_local_add` (loops over Unavailable, ResourceExhausted) | PASS (ok, 2.01s for the file) | https://github.com/auenkr/cadence-rust/actions/runs/40036246955 |
| `tasklist/forwarder_test.go:TestForwardPollError` (root, `ErrNoParent`) | `root_and_sticky_decision_polls_never_call_parent` (root half) | PASS | https://github.com/auenkr/cadence-rust/actions/runs/40036246955 |
| `tasklist/forwarder_test.go:TestForwardPollError` (sticky, `ErrTaskListKind`) | `root_and_sticky_decision_polls_never_call_parent` (sticky half) | PASS | https://github.com/auenkr/cadence-rust/actions/runs/40036246955 |

Code commit: `67723cca5` (run `40036246955`, dispatched with `package=cadence-matching`). Result: clippy `--all-targets -D warnings` success; `cargo test -p cadence-matching`: `tests/matching_forward_poll_fallback.rs` 2 passed, 0 failed, 0 ignored. No `#[ignore]` parity gaps were needed.

## Mapping notes

- Go `ForwardPoll` returns `ErrNoParent` and `ErrTaskListKind` before any RPC. The RPC analogue
  is "the parent's `poll_for_decision_task` is never called and the poll waits out its deadline
  with an empty response".
- Rust forward failure: `matching_dispatch.rs` `forward_poll` returns false on `Err`, and the poll
  sets `forward_failed` and waits locally. The parent is therefore called at most once per poll,
  matching Go `Times(1)`. The test asserts exactly one parent call.
- The root test uses a ring where `queue` is owned locally, so the poll is not routed to the
  remote host. Otherwise the call would be a routing hop, not a forward.
- Sticky uses the child partition with `kind = 2` and a remote parent, so the only difference is
  sticky-ness.
- The failing-add case: the fake parent also fails `add_decision_task`. The add persists locally
  and the parked poll claims it.
