# Sticky query forwarding parity (2026-10-24)

Question: does Go forward sticky-task-list queries (memo `memory/matching-cannot-port-review-20261024.md`, row 11 and "Sticky query forwarding (row 11, to verify)"), and does Rust differ?

## Verdict

**No parity gap for sticky query forwarding.** The memo's premise is wrong on the Go side. Go never forwards a sticky query in production, so `ErrTaskListKind` is not an observable behaviour of `QueryWorkflow`. Rust matches Go for all three cases below.

## Why the memo premise is wrong

The memo said Go `matcher.go:804` only short-circuits when `fwdr == nil`, and that for a sticky list "the forwarder exists". Both halves are wrong:

- The forwarder is only constructed when `isFowardingAllowed` holds. That function returns `!taskList.IsRoot() && kind != TaskListKindSticky`:
  - `go/service/matching/tasklist/task_list_manager.go:237-238` (`var fwdr Forwarder; if tlMgr.isFowardingAllowed(...) { fwdr = newForwarder(...) }`)
  - `go/service/matching/tasklist/task_list_manager.go:891-892`
- `newForwarder` has exactly one production call site (`task_list_manager.go:238`). The other constructions are `forwarder_test.go:70` and `matcher_test.go:89`, both with `TaskListKindNormal`.
- With `fwdr == nil`, `OfferQuery` takes `fwdrTokenC := tm.fwdrAddReqTokenC()` (`matcher.go:289`), which returns `noopForwarderTokenC` (`matcher.go:805-806`). `noopForwarderTokenC` is an unbuffered channel that never becomes ready (`forwarder.go:80-81`). So `OfferQuery` only does a local send on `queryTaskC` and waits for `ctx.Done()` (`matcher.go:281-315`, the `ctx.Done` case at `:309`).
- `ErrTaskListKind` (`forwarder.go:118-119`, `:182-183`, `:212-213`) is reached only by `forwarder_test.go:82`, `:154`, `:197`, which force `t.fwdr.taskListKind = types.TaskListKindSticky` on a forwarder built directly. Production never reaches it.

## Go behaviour (file:line)

Engine entry: `go/service/matching/handler/engine.go:1014` `QueryWorkflow`.

| Case | Go path | Observable result |
|---|---|---|
| Sticky query, live sticky poller (polled within 10 s, parked now or not) | `engine.go:1031-1034` check passes (`HasPollerAfter`, window `_stickyPollerUnavailableWindow` `engine.go:67`). `DispatchQueryTask` (`task_list_manager.go:668`) → `OfferQuery` (`matcher.go:281`) → local send to the parked poller (`:283`). | The parked sticky poller receives the query. No forward. |
| Sticky query, no poller in the window | `engine.go:1033-1034` returns `_stickyPollerUnavailableError` (`engine.go:129`): `StickyWorkerUnavailableError`, message `sticky worker is unavailable, please use non-sticky task list.` | Fails fast before any dispatch or forward. |
| Sticky query on a parent-partition name (e.g. `/__cadence_sys/queue/1`, kind sticky) | Same as the first two rows, because `getOrCreateTaskListManager` builds the manager with no forwarder (`task_list_manager.go:237`). `OfferQuery` never reaches `ForwardQueryTask`. | With a recent but not parked sticky poller: blocks on the local queue until the RPC context ends. `OfferQuery` returns `ctx.Err()`; `engine.go` returns it; `handler.go:322-323` `handleErr`; `context.go:137` default maps it to `InternalServiceError`. Never reaches the parent. |

## Rust behaviour (file:line)

- Sticky list detection: `rust/crates/cadence-matching/src/grpc_matching.rs:132-134` (`list.kind == 2`).
- Sticky error: `grpc_matching.rs:138-146` (`sticky_unavailable`, `Code::Unavailable`, detail `StickyWorkerUnavailableError`, same message).
- Pre-check: `grpc_matching.rs:147-157` (`check_sticky_poller`), called from `query_workflow` at `grpc_matching.rs:2581`. The `AddDecisionTask` call site is at `:2170`.
- Entry: `grpc_matching.rs:2517` `query_workflow`.
- Local offer first (`try_offer`), then the no-forward branch: `grpc_matching.rs:2662-2665` `if list.kind == 2 || current.is_root() { force_local = true; continue; }`. The forward code (`:2673` `current.parent(degree)`, `:2683` `try_forward_query`) is unreachable for sticky lists because `kind` is preserved across hops.
- Deadline: `grpc_matching.rs:2647` `Err(_) => Err(Status::internal("context deadline exceeded"))`. This matches Go's `InternalServiceError` mapping.

| Case | Rust result | Match? |
|---|---|---|
| Sticky query, live sticky poller | `try_offer` delivers to the parked poller; no forward | Yes |
| Sticky query, no poller | `check_sticky_poller` returns `Unavailable` with the Go detail before offer or forward | Yes (existing test `matching_rpc_sticky_task_list_requires_a_recent_poller`, `matching_rpc_tests.rs:2796`) |
| Sticky query, parent-partition name | Same guard. With a recent but not parked poller it waits on the child queue until the deadline and returns `Internal`. Never forwards. | Yes |

Rust has no forwarder type and no `ErrTaskListKind`. That error is only reachable in Go through a unit-test override, so it has no RPC analogue.

## New tests

All appended to `rust/crates/cadence-matching/src/matching_rpc_tests.rs` (helpers there are private to that module). None is `#[ignore]`, because no difference was found.

| Test | Go reference | What it pins |
|---|---|---|
| `matching_rpc_sticky_partition_query_reaches_only_the_sticky_poller` | `OfferQuery` local-first; no forwarder (`matcher.go:281-289`, `task_list_manager.go:237`) | Parked sticky poller on the partitioned name gets the query; the parked root poller never finishes |
| `matching_rpc_sticky_partition_query_without_parked_poller_never_forwards` | `engine.go:562`/`:1033` pass; `matcher.go:805-806` nil forwarder; `context.go:137` | Recent but unparked sticky poller: query returns `Internal` at the deadline; root poller never finishes |
| `matching_rpc_sticky_partition_query_without_any_sticky_poller_fails_fast` | `engine.go:1033-1034`, `engine.go:129` | No sticky poller: `Unavailable` with the Go message; root poller never finishes |

CI history on this branch:
- Run `40036528096` (head `da2ce09c3`): clippy passed; Test failed on `matching_rpc_sticky_partition_query_reaches_only_the_sticky_poller` (`Elapsed`). Cause: a delivered query makes the poll fetch mutable state from history (`grpc_matching.rs:1765-1775`); the fixture used the unreachable peer `127.0.0.1:1`, so the poll outlived the 5 s test timeout. Fixed in the next commit by using `start_fake_history` + `fixture_with_history`, as `matching_rpc_delivers_query_to_decision_poller` does. The other two new tests passed in that run.
- Run `40036908869` (head `729d4ab81`): **success** (clippy `-D warnings` and `cargo test -p cadence-matching` green, all three new sticky tests included). https://github.com/auenkr/cadence-rust/actions/runs/40036908869

Mixed Go/Rust evidence: not run here. The Go side of these cases is code-read only. A Go-cluster probe would need the `sticky` query path through `tests/matching-service-compat.cjs`, which is out of scope for this check.

## Method and limits

- Read-only on Go and Rust production code. No production change. The only change is appended tests.
- Cargo not run locally. The tests are verified only by the GitHub Actions run `rust-ci.yml` (package `cadence-matching`, clippy `-D warnings`, then test).
- Not checked: history-side handling of `StickyWorkerUnavailableError` (`go/service/history/engine/engineimpl/query_workflow.go:266`, which falls back to non-sticky). That is a history-service behaviour, outside matching.
- Correction to `memory/matching-cannot-port-review-20261024.md`: the "Sticky query forwarding (row 11, to verify)" item should be marked resolved as no gap. Row 11's `ErrTaskListKind` proposal is only a Go unit-test artefact.
