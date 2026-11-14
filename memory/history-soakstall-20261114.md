# History soak stall: findings (2026-11-14, branch history-soakstall)

## Current result: result long-poll fixed; full soak acceptance remains red

Branch `history-pollms-fix` implements the missing direct-engine history
notification. Fresh CI: Clippy + 890 History tests pass. MySQL/gRPC and
Cassandra/TChannel two-History mixed gates both report `GATE SUMMARY failed: []`.
The 20-minute mysql soak confirms e2e p50=4101ms (Go baseline ~4100ms), but
4794/4800 workflows complete: six restart-window child-completion stalls remain,
plus first-host RSS growth fails the guard. See implementation evidence below.
Do not claim all workflows complete or full soak acceptance.

## Original result before the fix: NOT fixed; narrowed to result long-poll

Rust History + Matching + Frontend + Go Worker, mysql, release build, History log level warn.

| run | head | dur | completed | failed | e2e p50 / p95 / p99 |
|---|---|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/43536936639 (TCP_NODELAY on gRPC servers) | 71b167c89~ | 8m | 1916/1920 | 4 | 24050 / 25140 / 29178 ms |
| https://github.com/auenkr/cadence-rust/actions/runs/43545415725 (after merging master b5d5d358a, failoverEndTime fix) | c0c982cca | 8m | 1920/1920 | 0 | 24102 / 26162 / 31323 ms |
| Go History baseline, https://github.com/auenkr/cadence-rust/actions/runs/43526328282 | - | 6m | 1440/1440 | 0 | p50 4100 ms |

Debug builds with debug History logging saturated the 4-core runner (the original near-total non-completion). Release + warn completes everything, but e2e is a constant ~24s versus Go's ~4s.

## Where the time goes (per-stage timings, run 43544467523 and 43545415725 `soak-driver.log`, `STAGE` lines)
Stages: 1 start, 2 sleep+wake signal, 3 query, 4 done-signal + `GetWorkflow().Get()`.
Stage 4 is 20.01 s for every dumped workflow (e.g. 20013 ms, 20010 ms, 20109 ms), i.e. the 20 s long-poll expiry. Stages 1-3 total ~4 s.

Server-side history of the same workflows (HISTDUMP in `histdump.txt`, run 43542422663): gen0 finishes in ~3.5 s (timer 2 s, activity, child 1 s, continue-as-new); gen1 starts, receives wake, done and completes ~0.7 s later. Decision Scheduled->Started is 5-100 ms. So History, Matching and decision dispatch are healthy. The workflow closes on time; the client just does not learn about it for 20 s.

## Hypothesis
`GetWorkflowExecutionHistory(waitForNewEvent)` -> Frontend -> History `PollMutableState` long-poll is not woken by the close notification and returns only at `long_poll_expiration_interval` (~20 s). Likely sites:
- rust/crates/cadence-history/src/engine/poll_mutable_state.rs `long_poll_for_event_id` (subscribes via NotifierAdapter in wiring/assembly/engine_factory.rs).
- notification producer: execution/context.rs `store.notify_history(state)` -> wiring/assembly/ndc_context_hooks.rs `notify_history` -> EngineCore::notify_history_event -> engine_slot -> engine.notify_new_history_event.
- Check: `Notifier::watch_history_event` uses an mpsc(1) channel and `try_send` (events/mod.rs:59-62); the subscriber key is the (domain, workflow, run) identifier. The client polls with `run_id` empty, so Rust must resolve the run id before subscribing (`execution.run_id = state.execution.run_id` at poll_mutable_state.rs:65, the notification uses the real run id); a continue-as-new chain (gen0 closes -> gen1) is the most likely mismatch (close of gen1 notified under gen1 run id, the poller may be subscribed under gen0's id or an empty id).
- Also compare with Go `engineimpl/poll_mutable_state.go` (`expectedNextEventID` handling) and with the Frontend side `GetWorkflowExecutionHistory` long-poll.

Next step for the next worker: add a unit/integration test for PollMutableState with an empty run id on a continued-as-new chain, and log (at debug, short run) subscribe/notify identifiers; compare against the Go History baseline.

## Ruled out
- Nagle/delayed-ACK: TCP_NODELAY added to History and Matching gRPC listeners (commit "grpc servers ... TCP_NODELAY"); no e2e change. Kept as harmless.
- failoverEndTime encoding (master b5d5d358a): merged, no change on this soak.
- Query "init": occurs on Go too (race with the wake signal), non-discriminating.

## Not done
- 45-minute soaks (mysql/postgres/cassandra) and 45-minute Go baseline.
- 12 history-mixed-gate combos on the final head (earlier 18 runs on 0e46b77c1 were checked by conclusion only, not GATE SUMMARY).
- Driver additions kept: `STAGE` timings and `HISTDUMP` for gen0/child of slow workflows (tests/history-soak/main.go); workflow inputs `go_history`, `history_log`.

## PollMutableState implementation fix (2026-11-14, history-pollms-fix)

Root cause: the live `EngineExecutionContext::persist` path in
`wiring/assembly/engine_cache.rs` directly persists the mutation and notified
transfer/timer queues, but never notified history subscribers. Decision completion,
close, continue-as-new and ordinary engine updates use this path, bypassing the
separate `WorkflowContext`/NDC `notify_history` hook. Thus the server committed the
close while Frontend's close-only `PollMutableState` remained asleep until expiry.

Comparison with Go `engineimpl/poll_mutable_state.go` and `events/notifier.go`:
resolved run-ID subscription, domain/workflow/run keys, post-subscribe recheck,
expected-event-ID comparison and close-status handling already match. Controller
wiring shares the core's engine slot with the engine containing the subscribed
notifier. Go `execution/context.go` notifies after successful update (including
continue-as-new) and conflict resolution, not creation; Rust's NDC paths already
have these hooks. The direct engine update path was the missing producer.

Fix c4d28963d: notify the shard engine after successful persistence and transaction
cleanup with the current run ID, last-first/next/previous-started IDs, workflow
state/close status and version histories. Failed writes do not publish a close.
Regression test uses real in-memory persistence, engine, cache and notifier:
continue-as-new, subscribe a close-only poll (expected ID i64::MAX) with either
explicit or empty/current run ID, then complete the new run. Require response
within 250ms (test poll expiry is 1s), with persisted close state and event IDs.

CI dispatched on c4d28963dcd6c825b62313d2247e57c6430ce4a4 (results pending):
- rust-ci, cadence-history, clean_target=true: https://github.com/auenkr/cadence-rust/actions/runs/43548203627
- mysql soak, 8m, rate=4, two_history=true, release/warn: https://github.com/auenkr/cadence-rust/actions/runs/43548206209
- mysql/grpc mixed, two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43548210090
- cassandra/tchannel mixed, two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43548212487
No Cargo, Go or Docker commands run on the dev host. An automatic rust-check push
run (43548203626) was cancelled to honor the explicitly requested workflow scope.

Validated so far on implementation head c4d28963dcd6c825b62313d2247e57c6430ce4a4:
- rust-ci 43548203627: SUCCESS, fresh clean target, Clippy then 890 tests passed,
  including `poll_mutable_state_wakes_on_close_after_continue_as_new`.
- mysql/grpc mixed 43548210090: SUCCESS, `GATE SUMMARY failed: []`, artifact
  `failedSteps: []`, exit 0, two_history=true.
- cassandra/tchannel mixed 43548212487: SUCCESS, `GATE SUMMARY failed: []`, artifact
  `failedSteps: []`, exit 0, two_history=true.
- MySQL History binary SHA256: b588fef7e8ed539f418f7ce0a2b4cffff519c870e1891b87ab9a0c56593a459b
- Cassandra History binary SHA256: 9478fac203d4cc190e4f7033eee5d519446e146b38949b7dc24c29c2ba3c0bbc
- Durable compact evidence: `memory/evidence/history-pollms-fix-20261114/`.
  Full gate artifacts downloaded and inspected; raw logs remain on Actions for
  their configured three-day retention. Soak 43548206209 is running SDK traffic.

First fixed-code mysql soak 43548206209: workflow latency FIXED, but overall run
FAILURE. p50=4124ms, p95=4440ms, p99=6301ms (baseline Go p50~4100ms, old Rust
p50=24102ms). Completed 1918/1920; two workflows (IDs ending 0000948/0000949)
failed with `query state "init"` after the History restart. Their dumped histories
show child started before restart, wake signal after restart, but no child-completed
event or continue-as-new; these are distinct from the result long-poll wakeup.
The soak also cannot pass restarted-host RSS sampling at 8m: restart at 245s +
300s warm-up exceeds end=456s, yielding zero samples. Do not report this run green.
Evidence retained under `memory/evidence/history-pollms-fix-20261114/soak-8m/`.

Longer reproducible validation dispatched on documentation head
8e2728d87 (same Rust implementation as c4d28963d):
https://github.com/auenkr/cadence-rust/actions/runs/43550611874
Inputs: database=mysql, duration_minutes=20, two_history=true, rate=4,
history_log=warn. The longer duration is necessary for the existing RSS guard:
restart halfway + 300s warm-up + eight 30-second samples. It also checks whether
the first run's two restart-window child-completion stalls reproduce. The first
soak's logs contain service-to-service TCP connection errors during restart;
this is not proof of the exact cause of the missing child-completion events.
Build succeeded and SDK traffic began at 2026-11-14T03:30:03Z. Result pending.


Final longer-soak result: https://github.com/auenkr/cadence-rust/actions/runs/43550611874
- Source head: 8e2728d87 (Rust code identical to implementation head c4d28963d).
- Conclusion: FAILURE. 4794/4800 completed, 6 failed, zero inflight.
- E2E p50=4101ms, p95=4179ms, p99=4214ms, max=10385ms. The original
  constant 20s result-poll delay is gone, including across the restart load.
- Six failures: query state "init", workflow suffixes 0002390/2392/2393/2394/
  2397/2398, all created near restart t=600s. The dumped histories again stop
  before child completion/continue-as-new; these failures happen before Get().
- Restarted host RSS check now has 10 samples and PASSES (237.2->299.3 MiB).
- First History host RSS FAILS (167.9->324.4 MiB, peak343.9): +156.5 MiB,
  just over the harness's +150MiB alternative limit and over its 1.5x limit.
- Durable summaries, steps, failure lines, history dump and source/binary
  identities: `memory/evidence/history-pollms-fix-20261114/soak-20m/`.

Remaining acceptance gap is real: this branch fixes PollMutableState and passes
both requested mixed gates, but cannot be described as a fully passing soak.
Further restart child-completion/recovery and RSS work is needed; no checks were
weakened, workflow failures suppressed, or retries added to hide these findings.
All validation ran on GitHub Actions; no local Cargo, Go or Docker commands.

## Follow-up implementation: history-soak-tail (2026-11-14)

Base origin/master 05fb1e57c; merged origin/history-restartgate without merging
anything to master. Diagnostic soak captures child histories on query failures
(the earlier evidence only captured parent histories). Restart gate now uses
release binaries, matching the soak and avoiding the known debug-build overload.

RSS audit found a real Go parity gap: EngineExecutionCache hard-coded 10,000
entries per shard and evicted all idle entries only at capacity. Go NewCache
uses history.cacheMaxSize (default 512), history.cacheTTL (default one hour),
pinned entries and LRU eviction. Rust has those configuration properties but
ignored them. Follow-up wires both properties, evicts expired idle entries and
the least recently accessed unpinned entry, and refuses insertion when every
entry is pinned. No RSS acceptance threshold changed. Validation pending.

Child close executor comparison: both CloseExecution and the separate
RecordChildExecutionCompleted transfer task notify the parent; connection and
shard errors propagate for retry, while only EntityNotExists/already-completed
are ignored. Added retry regression covering both task types and unchanged
parent/child identifiers across attempts. Exact restart stall cause remains
under investigation; diagnostic run below has no Rust behavior changes.

| run URL | head | scenario | result |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/43554242022 | 76e7cc97c | mysql 20m diagnostic soak, child history dumps | FAILURE: 4797/4800, p50 4124ms; both RSS guards pass |

Fresh-target History Clippy/test run on 98a1e1b00 (cache implementation and child
retry/reload regressions; later real-cache capacity/TTL test added in 521abb079):
https://github.com/auenkr/cadence-rust/actions/runs/43555072334 — SUCCESS, 901 tests.
The repository's automatic rust-check push workflow ran on the first pushes;
it was disabled through gh to keep subsequent validation within the explicitly
requested workflow set. No local Cargo/Go/Docker commands were executed.

Compaction checkpoint: diagnostic run 43554242022 failed with 4797/4800 completed, p50 4124ms; both RSS guards passed. Fresh History CI 43555072334 succeeded (901 tests). Cancellation regression on 56a415667 is running in 43557514557; suspected abandoned cache lease retains uncommitted parent mutations. No claim of root cause confirmation yet.

Cancellation safety audit: Go cache.makeReleaseFunc is invoked by deferred release,
clearing state on an update error/panic before unlocking. Rust's release closure
previously dropped its owned lock on cancellation without clearing state. A parent
update removes the pending child before awaiting persistence; cancellation can
therefore leave an uncommitted removal cached. The retry then returns
EntityNotExists and processCloseExecution acknowledges the notification. Commit
e2167b61a introduces a lease whose Drop clears abandoned state before unlock;
explicit release remains idempotent. Deterministic pre-fix cancellation regression
is pending in run 43557514557 (head 56a415667). This is a demonstrated code-path
gap; attribution of the original live stalls awaits regression/runtime results.
Diagnostic summaries and parent/child histories are retained in
memory/evidence/history-soak-tail-20261114/diagnostic/.

Pre-fix CI 43557514557 concluded FAILURE: 902 tests passed and only canceled_child_completion_is_retried_from_persisted_parent_state failed with EntityNotExists: Pending child execution not found. This confirms uncommitted cached child removal survives cancellation. Patched soak run: https://github.com/auenkr/cadence-rust/actions/runs/43558072143 (e2167b61a), pending.

Patched fresh-target History validation: https://github.com/auenkr/cadence-rust/actions/runs/43558280598, head e2167b61ad242a1b20922f2d6d9473a326107912, SUCCESS: Clippy then 903 tests, including cancellation and real-cache capacity/TTL regressions.

Fresh uncached workspace validation https://github.com/auenkr/cadence-rust/actions/runs/43559207202, head 6a885777a4cfea4764777a4ca0cf341d7c05588a (same Rust as e2167b61a), SUCCESS: workspace Clippy followed by tests. TChannel wiring/server/tchannel.rs handle_call wraps dispatch in timeout(remaining), establishing a production deadline-cancellation path for the regression.
