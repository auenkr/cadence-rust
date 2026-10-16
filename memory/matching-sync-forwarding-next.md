# Matching completion continuation (2026-10-16)

Prior increments: `a9531588d` (sync match), `ca062c861` (local ancestor sync
and poll forwarding). This increment closes the remaining single-node gaps:
forward limiter parity, forwarded-backlog waits, query forwarding, and
standalone durable wiring.

## State

- `matching_forward.rs` (new): Go forwarder token generations
  (`RequestTokens` swaps the whole pool on limit change; in-flight releases
  never inflate the new generation) and `ForwardRate` mirroring
  `quotas.NewDynamicRateLimiter` (5s refresh, ceil burst, zero-rate zero
  burst). Burst uses integer config values so ceil is exact.
- Add walk takes a forward-task permit per ancestor hop (task tokens only for
  the permit; rate checked via `try_forward_task`). Zero tasks/zero rate
  blocks forwarding; live config replaces pools without owner replacement.
- Forwarded root DB-backlog adds (`source == 2`, matching Go
  `TASK_SOURCE_DB_BACKLOG = 2`) wait for pollers through the incoming
  deadline via `offer_forwarded_backlog`, mirroring `OfferOrTimeout`.
- Forwarded polls rebuild the parent request (parent task list + kind +
  identity only), like Go `ForwardPoll`.
- Query forwarding mirrors `OfferQuery`: `QueryChannel::try_offer` succeeds
  only with a decision poller parked in `next` (new waiter counter);
  otherwise the query walks ancestors with a forward-task token (no rate
  limit, like `ForwardQueryTask`). Root with no waiting poller fails fast
  with internal `cannot find parent task list for forwarding` (Go
  `ErrNoParent`); sticky lists never walk. Permits held until resolution.
  The old `delivers_query_to_decision_poller` test now parks the poller
  first; immediate-offer semantics made its old timing flaky.
- Standalone wiring: `grpc_matching::serve_with` serves a prebuilt matcher;
  `main.rs` builds a durable `QueueRegistry` (`matching.db` beside snapshots,
  loopback History client, 10s maintenance pump) when `--data-dir` is set.
  Without `--data-dir` behavior is unchanged (in-memory engine).
- `tests/run-matching-forward-compat.py` (new full-run stage): Go
  `TestRustMatchingForwardRate` boundary check plus Rust `matching_forward`
  unit tests.
- Query cancellation safety: `QueryChannel::Registration` guard withdraws on
  drop (timeout/cancel paths), and `next()` subscribes before taking.

## Verification (this increment)

- 92 focused `matching_` lib tests pass (includes: sync without metadata,
  child poll -> parent backlog, child add -> parent sync, forwarded
  failure/no-persistence, parent waiter cancellation, forward task/rate
  reload, forwarded root backlog late-poller wait, child query -> parent
  poller answer, no-poller query fast-fail, forwarding-disabled reload).
- Strict server all-target Clippy clean.
- Standalone smoke with `--data-dir`: `matching.db` created,
  `service-workflow-compat.cjs` (decision + activity to completion) PASSED,
  restart on the same dir reopens the DB and passes again.
- Full 30+1-stage compatibility run: PENDING — rerun on the committed tree
  and record outcome here before claiming completion.

## Remaining (explicitly out of single-node scope)

- Cross-host forwarding transport, ringpop membership/ownership, multi-node
  task-list ownership. Single node owns all partitions, as in Go dev mode.
- Adaptive read/write partition autoscaling beyond the configured count
  providers and native partition caches.
- Incoming forwarded adds without timeout use a 200ms fallback; Go trusts
  the incoming context. Audit cancellation/deadline parity.
