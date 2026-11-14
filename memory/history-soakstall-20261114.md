# History soak stall: findings (2026-11-14, branch history-soakstall)

## Result: NOT fixed. Root cause narrowed to the result long-poll, not decisions.

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
