# Matching durable query delivery (2026-10-16)

Closes the "query delivery" item of Matching gate 4 for the **durable** matcher
(`with_durable_queues`). Previously the durable `Matching::query_workflow`
delegated to the in-process Rust frontend, which is wrong when Matching runs in
its own process (mixed topology): the frontend's in-process pollers do not exist
there, so a query would never reach the SDK worker polling Matching.

## Go semantics implemented

- `matcher.queryTaskC` / `OfferQuery`: a query is an ephemeral task offered to a
  decision poller on the **same long poll** as the decision backlog; it never
  touches task storage and is not part of the backlog.
- `lockableQueryTaskMap`: `QueryWorkflow` installs the waiter **before**
  offering, and `RespondQueryTaskCompleted` completes it by task id. A caller
  that gives up (deadline) withdraws the offer.
- `createPollForDecisionTaskResponse` for `task.IsQuery()`:
  - token = `common.QueryTaskToken{DomainID, WorkflowID, RunID, TaskList, TaskID}`
    (JSON serializer, same format the Go frontend's `DeserializeQueryTaskToken`
    reads);
  - `response.Query = queryRequest.Query`;
  - every other field comes from History `GetMutableState`
    (`PreviousStartedEventID`, `NextEventID`, `WorkflowType`,
    `WorkflowExecutionTaskList`, `BranchToken`, `HistorySize`) plus
    `StickyExecutionEnabled`, because the Go frontend loads history for a
    **non-sticky** query task (`createPollForDecisionTaskResponse` case 3).
  - `StartedEventID`/`Attempt` stay zero (no `DecisionTaskStarted` event).
- `waitForQueryResult`: `QueryResultTypeAnswered` -> payload; `Failed` ->
  `QueryFailedError`-equivalent error with the worker message; a poll-side
  failure (`queryResult{internalError}`) fails the waiting call.
- `versionChecker.SupportsStickyQuery` is approximated (known SDK impls or an
  unknown-impl client at feature major >= 1); see limits.

## Files

- `rust/crates/cadence-server/src/matching_query.rs` (new): `QueryChannel`
  (pending offers + waiters + `Notify`), `PendingQuery`, `QueryOutcome`
  (`Worker`/`Internal`), `query_response`, `query_outcome`, sticky check.
- `rust/crates/cadence-server/src/matching_registry.rs`: `queries:
  Arc<QueryChannel>` on `Owner` and `QueueHandle` (one per queue key).
- `rust/crates/cadence-server/src/matching_dispatch.rs`: `PollLifecycle::begin`
  and its guard are now `pub(crate)`; `TaskDispatcher::query_mutable_state`.
- `rust/crates/cadence-server/src/matching_history.rs`:
  `MatchingHistoryClient::mutable_state` (History `GetMutableState`).
- `rust/crates/cadence-server/src/grpc_matching.rs`:
  - `poll_for_decision_task` durable branch now `tokio::select!`s
    (biased) the pending query **before** the durable dispatch; a ready query
    is handed to this poller and the dispatch future is dropped (dropping it
    makes the worker rewrite any claimed task, so racing is safe);
  - `query_workflow` durable branch registers + offers + waits (bounded by
    `poll_expiration`), withdrawing on timeout;
  - `respond_query_task_completed` durable branch completes by task id.
- `rust/crates/cadence-server/src/matching_rpc_tests.rs`: query RPC test with a
  minimal fake History implementing only `GetMutableState` (the generated
  `HistoryApi` trait has default `unimplemented` bodies, so no boilerplate).
- `tests/matching-service-compat.cjs`: mixed scenario `queryDispatch`.

## Validation

- `cargo test -p cadence-server --lib`: **79 passed** (6 in `matching_query`,
  1 new RPC end-to-end query test).
- strict all-target Clippy clean.
- **Mixed PASSED** (`queryDispatch: passed`):
  `/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-query-mixed/probe.log`
  - Go Frontend -> Go History -> **Rust Matching** `QueryWorkflow`;
    Rust Matching delivered the query to a `PollForDecisionTask` long poll
    (query token + history from mutable state), the probe answered with
    `WorkerAPI.RespondQueryTaskCompleted`, and `queryResult.data === 'pong'`
    came back to the caller. The existing Go SDK run in the same probe also
    passed.
- Full 29/30-stage runner: see `matching-query-full/results.json`.

## Remaining limits (do NOT report as complete)

1. Sticky query dispatch: Go tries the sticky task list first, then clears
   stickiness and retries non-sticky. The Rust durable matcher has no sticky
   path; a sticky query is offered on the (normal) key it is told to use.
   `SupportsStickyQuery` is only approximated.
2. `QueryRejectCondition` / closed-workflow rejection is decided by History, not
   Matching, so the Rust matcher does not produce `QueryRejected` (matches Go,
   which also leaves it to History/frontend).
3. Forwarding (`forwarded_from`), partition routing/scaling, isolation groups,
   semaphores, membership/ownership and standalone lifecycle remain open
   Matching gate 4/5 items.
4. The production standalone server still serves matching via
   `Matching::new(state)` (in-memory); the durable matcher is wired only in the
   test/mixed harness. Query delivery therefore only affects the durable path
   so far.
