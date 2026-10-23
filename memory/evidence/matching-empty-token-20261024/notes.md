# Matching empty task token after durable add (matching-empty-token-20261024)

## Question

`go_handler_poll_for_activity_task_returns_added_task_token`
(`rust/crates/cadence-matching/tests/matching_partial_upgrade_b.rs`) was ignored: after a durable
`add_activity_task` persists one row, a 2s `poll_for_activity_task` returned an empty `task_token`.
Go's success case expects a non-empty token. Was this (a) a harness problem or (b) a real Rust
divergence?

## Verdict: (a) harness setup gap in the test

The Rust test wired `MatchingHistoryClient` to `http://127.0.0.1:1`, where nothing listens. Go's
handler test never reaches History at all.

## Evidence

1. Go reference: `go/service/matching/handler/handler_test.go:316-320`. The success case mocks the
   engine: `s.mockEngine.EXPECT().PollForActivityTask(...).Return(&types.MatchingPollForActivityTaskResponse{TaskToken: []byte("task-token")}, nil)`.
   The engine (which calls History's `RecordActivityTaskStarted`) is not exercised.
2. Rust delivery order: `delivery_loop` (`rust/crates/cadence-matching/src/matching_dispatch.rs:1344-1407`)
   claims the task, then calls `deliver`. `deliver` is `history.start_activity_with_deadline`
   (`matching_history.rs:816-846`), which issues `RecordActivityTaskStarted`. A token is only sent
   after that succeeds (`matching_history.rs` builds `TaskToken` from the started response's
   `scheduled_event`, requiring a non-empty `activity_id`).
3. Failure mode with an unreachable History: the transport error has no `RpcStatus` detail, so
   `start_error_kind` returns `Retry` (`matching_history.rs:30-52`). `finish_delivery(Err)` rewrites
   the task and the loop retries until the poll deadline, then `poll_with_lifecycle` returns
   `R::default()` (empty). This matches the reported symptom (store count 1, empty token after 2s).
4. Rust test check-off list (all ruled out):
   - Active domain: test domain is local (`is_global_domain=false`), so `is_active_in` is true.
   - Task-list kind: both add and poll use kind 0 (`task_list()` helper).
   - Forwarded flag: poll has no `forwarded_from`; no forwarder is involved.
   - Schedule-to-start timeout: 3s, and the poll follows the add immediately, so the row has not expired.
   - Poller/reader start: the poll registers its poller and claims through the same `durable_queue`
     the add loaded; no separate reader load is required.
   - Quota: success test uses default dynamic config (the denied-quota tests are separate).

## Change made (test only, no production code)

- Added a test-only `StartedHistory` gRPC peer that overrides `record_activity_task_started`,
  returns a `scheduled_event` with `ActivityTaskScheduledEventAttributes { activity_id: schedule_id }`,
  and counts calls.
- `durable_with_history(state, url)` added; `durable(state)` delegates with the original dead URL,
  so other tests are unchanged.
- The success test now asserts the store holds one task before the poll, asserts `starts == 1`
  after the poll, and asserts a non-empty token. `#[ignore]` removed.

Commit: `d4dd1feb6` on `matching-empty-token-20261024`.

## CI evidence

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40035416424 (`rust-ci.yml`,
  `workflow_dispatch`, `package=cadence-matching`) — conclusion **success**.
- Log line: `test go_handler_poll_for_activity_task_returns_added_task_token ... ok`
- Package result: `test result: ok. 13 passed; 0 failed; 2 ignored` for `matching_partial_upgrade_b`.
  The 2 remaining ignores are the poller history-callback and cancel-bool parity gaps, unchanged.

## Not done / out of scope

- No production code changed. No merge.
- The earlier automatic `push` run for this branch was cancelled by a newer run; only the
  dispatched run is authoritative.
