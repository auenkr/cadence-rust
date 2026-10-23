# Query remote sync match port (Go TestQueryRemoteSyncMatch / TestQueryRemoteSyncMatchError)

Branch: `matching-query-remote-sync-20261024` (from `origin/master`), worktree `/mnt/storage/service/github/wt/qrsm`.
Scope: port the two PARTIAL Go matcher cases to Rust unit tests. No production code changed.

## Go source

- `go/service/matching/tasklist/matcher_test.go:317` `TestQueryRemoteSyncMatch`
- `go/service/matching/tasklist/matcher_test.go:368` `TestQueryRemoteSyncMatchError`
- Go `taskMatcherImpl.OfferQuery` at `go/service/matching/tasklist/matcher.go:281`:
  local `queryTaskC` send first, then `ForwardQueryTask` on a forwarder token;
  `ErrForwarderSlowDown` switches to local-only for the rest of the context;
  a local match returns `nil, nil` (the answer is delivered via the response path).

## Rust production path

The Rust OfferQuery equivalent is the gRPC handler `query_workflow` in
`rust/crates/cadence-matching/src/grpc_matching.rs` (loop around `try_offer`,
`remote_channel_for_task_list`, `MatchingApiClient::query_workflow`, and
`query_forward_slow_down` -> `force_local`). It is not in `matching_dispatch.rs`.

## Where the tests live (deviation from the brief)

The brief said `matching_dispatch.rs`. The production query path is only reachable
through a full `Matching` service, and the fixture that builds one
(`fixture_with_history`, `membership_with_parent_remote`) is private to
`matching_rpc_tests.rs`. Putting the tests in `matching_dispatch.rs` would require
changing that file's visibility, so the tests sit next to the existing child-query
tests in `matching_rpc_tests.rs`.

## Ported tests

| Go case | Rust test | What it asserts |
|---|---|---|
| TestQueryRemoteSyncMatch | `matching_rpc_remote_query_sync_match_forwards_to_parent_and_returns_answer` | no local poller: query forwarded to remote parent; parent answer returned; `forwarded_from` = child list; forwarded task list = parent `queue` |
| TestQueryRemoteSyncMatchError | `matching_rpc_remote_query_sync_match_error_falls_back_to_local_poller` | parent ServiceBusy; local child poller (started after the forward, as Go `ready()`) receives `ping`, answers `answer`; query returns it |

## Mapping notes (not gaps)

- Go `OfferQuery` returns `nil, nil` on a local match; the Rust handler returns the
  answer to the caller directly. Go's gRPC handler then waits on the response path
  (`waitForQueryResult`); the caller-visible result is the same, so the Error test
  asserts the answer rather than `nil`.
- Go's `t.fwdr.PollReqTokenC()` consumption disables forwarded polls in the Error
  test. The Rust remote parent does not implement `poll_for_decision_task`, so a
  forwarded poll would fail there. This is intentional; a failing CI run would show
  it.

## Coverage not in these two tests

- The happy-path Go test also checks the child's forwarded poll (`remotePollResp.Query`)
  delivered through the parent. That leg is covered by
  `matching_rpc_child_query_forwards_to_parent_poller`
  and `matching_standby_query_poll_forwards_to_parent_without_claiming_backlog`,
  not by the new happy-path test.

## Parity gaps

None found by reading the code. No `#[ignore]` added. The result must come from CI.

## CI evidence

- Commit `fc8a16037`, run https://github.com/auenkr/cadence-rust/actions/runs/40033180011 (workflow_dispatch, `package=cadence-matching`): conclusion **success**, job `clippy-test`.
- Log: `grpc_matching::durable_tests::matching_rpc_remote_query_sync_match_forwards_to_parent_and_returns_answer ... ok`
- Log: `grpc_matching::durable_tests::matching_rpc_remote_query_sync_match_error_falls_back_to_local_poller ... ok`
- Crate result: `602 passed; 0 failed; 13 ignored` (main unit-test binary).
