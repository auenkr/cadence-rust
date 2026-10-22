# Matching audit verification: items 4, 5, 6, 8 (2026-10-22)

Follow-up to `memory/matching-matcher-tasklist-audit-20261021.md` (branch `origin/audit/matching-matcher-20261021`, base `864dcdbe9`). That audit cited some line numbers that are wrong; corrections are marked **[corrected]**.

**Basis.** Go line numbers are from `go/` at `a0953f121` (master). Rust line numbers are from `rust/crates/cadence-server/src/*.rs` at `a0953f121` read via `git show`, not the dirty working tree of the main checkout. Source-level only: nothing was built or executed (no cargo builds, no Go runs).

Status legend: **confirmed** = Go behavior verified from source and the Rust gap is verified from source. **Unverified** = not executed against a live Go host.

---

## Item 4: CancelOutstandingPoll

### Go behavior

Request path:
- `handler/handler.go:354-377` `CancelOutstandingPoll` → `h.engine.CancelOutstandingPoll` (375) → `hCtx.handleErr(err)` (376).
- `handler/engine.go:1109-1131`:
  - builds the identifier (1119),
  - `getOrCreateTaskListManager(hCtx.Context, taskListID, taskListKind)` (1124). This loads or creates the manager. Fast path returns the loaded manager when the shard process exists (`engine.go:315-331`). Otherwise it runs `errIfShardOwnershipLost` (333; returns `TaskListNotOwnedByHostError` for a non-owner, `engine.go:1600-1612`), then loads/creates. A non-owner therefore gets an error, not a silent no-op.
  - `tlMgr.CancelPoller(pollerID)` (1129), then `return nil` (1130). **Returns nil whether or not a poll matched.**
- `tasklist/task_list_manager.go:758-762` `CancelPoller` → `c.pollers.CancelPoll(pollerID)`; logs only on a hit.
- `poller/manager.go:165-171` `CancelPoll` looks up `m.outstanding[pollerID]` and calls the stored `cancel()`. Returns bool.

Registration (what gets cancelled):
- `task_list_manager.go:708` `childCtx, cancel := c.newChildContext(ctx, LongPollExpirationInterval, returnEmptyTaskTimeBudget)`.
- `task_list_manager.go:711-712` isolation group and identity read from ctx; `pollerID := PollerIDFromContext(ctx)` (712).
- `task_list_manager.go:726-731` `StartPoll(pollerID, cancel, ...)` then `defer EndPoll(pollerID)`. The cancel func is the **child** ctx cancel.
- `poller/manager.go:107-131` `StartPoll`:
  - stored in `outstanding` only when `pollerID != ""` (120),
  - **`m.outstanding[pollerID] = ...` (123) overwrites any earlier entry without cancelling it.** A second poll with the same poller ID does not cancel the first.
- `poller/manager.go:133-163` `EndPoll` → `tryRemovePoller` does `delete(m.outstanding, pollerID)` (153) **unconditionally**. If two polls share an ID, the first to finish removes the second's entry.

What the cancelled poll returns (empty success, not an error):
- The matcher's ctx-done branches return `ErrNoTasks`: `tasklist/matcher.go:569-577`, `659-667`, `720-727` (each `case <-ctx.Done()` → `return nil, ErrNoTasks`).
- `task_list_manager.go:693-695` wraps it (`couldn't get task: %w`); the wrap keeps `errors.Is` working.
- `handler/engine.go:719-750` (decision poll): `errors.Is(err, tasklist.ErrNoTasks)` → logs, returns `&MatchingPollForDecisionTaskResponse{PartitionConfig, LoadBalancerHints, AutoConfigHint{EnableAutoConfig, PollerWaitTimeInMs}}` with **nil error** (742-749). The activity poll path (~engine.go:910-960) uses the same pattern; that branch was **not re-read line by line**.
- The engine comment at `engine.go:709-710` documents the intent: "so tasklistMgr can support cancellation of long-poll when frontend calls CancelOutstandingPoll API".

Pollers are keyed per task-list manager (per `pollerID`), so the Go scope is one manager's outstanding polls.

### Rust at a0953f121

- `grpc_matching.rs:2293-2323` `cancel_outstanding_poll`:
  - validates domain (2298) and task-list type (2299-2301) and name (2306),
  - builds `PollKey{domain, task_list, task_type, poller}` (2307-2312),
  - looks up `self.polls` and calls `cancel.notify_one()` (2313-2321),
  - returns OK (2322).
  - **No call to `durable_queue` / owner load**, so no ownership check and no manager load (Go 1124-1129 has both).
- `grpc_matching.rs:1214-1229` `register_poll`: `insert` then **`previous.notify_one()` (1216-1222)**, which cancels the earlier poll. Go does not (`poller/manager.go:123`).
- `grpc_matching.rs:260-270` `PollGuard::drop` removes the key only if `Arc::ptr_eq` matches its own `Notify`. Go's `EndPoll` removes unconditionally (`manager.go:153`). Differs when IDs collide.
- Cancelled polls return **`Status::cancelled("Outstanding poll cancelled.")`**:
  - `grpc_matching.rs:1358` and `1366` in `poll_for_decision_task` (fn starts 1233),
  - `grpc_matching.rs:1510` and `1518` in `poll_for_activity_task` (fn starts 1401).
  Go returns an empty success response.

### Required Rust change

1. In `poll_for_decision_task` (1358, 1366) and `poll_for_activity_task` (1510, 1518), replace the `Status::cancelled` arm with the same empty-response builder used for the normal "no task before the poll deadline" return. It must carry partition config, load-balancer hints and the `AutoConfigHint` (`PollerWaitTimeInMs` = elapsed), matching `engine.go:742-749`. Return `Ok`, not an error.
2. `register_poll` (1216-1222): delete `previous.notify_one()`. A repeat poll ID replaces the map entry and cancels nothing (`manager.go:123`).
3. `PollGuard::drop` (262-268): remove the key unconditionally, mirroring `EndPoll`/`tryRemovePoller` (`manager.go:153`). This preserves the Go quirk (a finishing poll can delete a newer poll's entry). Leave a comment that says it is deliberate parity.
4. `cancel_outstanding_poll` (2293-2323): before the map lookup, route through the same owner/load path as add/poll (`durable_queue(domain, list, type)`) so a non-owner returns the same error as Go's `errIfShardOwnershipLost` (`engine.go:333`), and a not-yet-loaded list is loaded as in Go (`engine.go:1124`). Keep the OK response when no poll matched (Go returns nil regardless, 1129-1130).
5. Update or re-check `matching_rpc_tests.rs:258`, `296` and `373` (cancel tests). They were **not re-read** here and may assert the old `Cancelled` behavior.

Verdict: **confirmed** difference (status code, double-poll cancel, missing owner check). Item 4 in the parent audit stands.

---

## Item 5: QueryWorkflow with no poller on root / sticky

### Go behavior (block vs fail fast)

**Go blocks until the matching RPC context ends, then returns `InternalServiceError`. It does not fail fast at root.** The only fail-fast path is the sticky-worker gate, and it checks recent history, not the current pollers.

1. Handler: `handler/handler.go:300-322`. The user rate limiter (317-319) then `engine.QueryWorkflow` (321) then `hCtx.handleErr(err)` (322).
2. Engine `handler/engine.go:1014-1062`:
   - manager via `getOrCreateTaskListManager` (1026),
   - **sticky gate (1031-1036):** if `taskListKind == Sticky` and `!tlMgr.HasPollerAfter(now - _stickyPollerUnavailableWindow)`, return `_stickyPollerUnavailableError` (a `StickyWorkerUnavailableError`). `_stickyPollerUnavailableWindow = 10 * time.Second` (`engine.go:67`). This is the only fail-fast. It applies to sticky lists only, and it checks whether any poller was seen in the last 10s.
   - registers a result channel (1038-1041),
   - `tlMgr.DispatchQueryTask(hCtx.Context, taskID, queryRequest)` (1043). If it returns an error, the engine returns it (1046-1048).
   - Only when it returns no response and no error (local dispatch) does it call `waitForQueryResult` (1056).
3. `task_list_manager.go:668-676` `DispatchQueryTask` → `c.matcher.OfferQuery(ctx, task)`, after `startWG.Wait()`.
4. `tasklist/matcher.go:281-313` `OfferQuery`:
   - 282-287: non-blocking send to `queryTaskC`, else fall through.
   - 289: `fwdrTokenC := tm.fwdrAddReqTokenC()`.
   - 291-312 loop:
     - 293: `case tm.queryTaskC <- task:` → `<-task.ResponseC` (294), return nil,nil. **The wait on 294 has no ctx case.**
     - 296: `case token := <-fwdrTokenC:` → `tm.fwdr.ForwardQueryTask(ctx, task)` → release → return result, or on `ErrForwarderSlowDown` swap in `noopForwarderTokenC` and continue (302-306), else return error (308).
     - 309-310: `case <-ctx.Done(): return nil, ctx.Err()`. This is a **raw** `context.DeadlineExceeded`/`Canceled` (not wrapped).
5. **Root and sticky have no forwarder.** `task_list_manager.go:236-239` builds the forwarder only if `isFowardingAllowed` (`task_list_manager.go:891-893`: `!IsRoot() && kind != Sticky`). `matcher.go:804-809` `fwdrAddReqTokenC()` returns `noopForwarderTokenC` when `fwdr == nil`. `forwarder.go:80-81` defines that channel as unbuffered and never written, so the `case token` arm never fires.
   - Result: for root and sticky, OfferQuery only proceeds when a poller receives from `queryTaskC`, or when `ctx.Done()` fires.
6. `ErrNoParent` (`forwarder.go:74`, `forwarder.go:188`) is returned only from a forwarder whose `Parent(degree)` is `""`. A forwarder exists only for non-root lists, and `Parent` for a non-root partition is never `""` after the clamp (see item 6). **So `ErrNoParent` is effectively unreachable in Go. It is not a fast-fail for root.**
7. Error path back to the client when ctx expires:
   - No local offer (the OfferQuery path): `engine.go:1046-1048` returns the raw ctx error. `handler.go:322` → `handleErr` → default branch (`handler/context.go:134-137`) → `&types.InternalServiceError{Message: "context deadline exceeded"}` → proto `CodeInternal` (`types/mapper/proto/errors.go:289-291`).
   - Local offer, then the poller is slow: `waitForQueryResult` (`engine.go:1064-1090`) selects on `hCtx.Done()` (1087) and returns `hCtx.Err()` (1088). `handlerContext` embeds `context.Context` (`context.go:38-42`), so this is the same `DeadlineExceeded` → `InternalServiceError`.
8. There is no Go-side timeout on the query path other than the RPC ctx. If the RPC has no deadline, Go blocks indefinitely.

### Rust at a0953f121

`grpc_matching.rs:1819-1955` `query_workflow`:
- Local-route branch only when `self.queues.is_some()` (1832).
- Sticky check via `check_sticky_poller` (1845), matching Go's gate.
- `degree` read from `matching.forwarderMaxChildrenPerNode` with default 20 (1848-1863); see item 6.
- Deadline: `incoming_deadline` else **`now + self.poll_expiration` (1864-1865)**. Go has no such fallback; it waits on the RPC ctx.
- **Fail-fast (1900-1907):** `if list.kind == 2 || current.is_root() || degree <= 0 || now >= deadline { return Err(Status::internal("cannot find parent task list for forwarding")) }`. For a root (normal) list with no waiting poller, this returns immediately. **Go blocks instead.**
- 1909-1914: parent empty → same Internal. This matches the (unreachable) Go `ErrNoParent`.
- 1880: `force_local` → `handle.queries.offer(query)` queues unconditionally. Go's `queryTaskC` is an unbuffered handoff; the query is not stored without a receiver. Different (note for item 5 follow-up; see "Required change" 4).
- Waiting after offer (1886-1894): `timeout_at(deadline, receiver)`; on elapse, **`Status::deadline_exceeded("Query task not completed before the deadline.")` (1891-1893)**. Go gives `Internal` (see above).
- Test `matching_rpc_tests.rs:1368-1400` `matching_rpc_query_without_poller_fails_fast` asserts the fail-fast: a 2s timeout (1373), then `Code::Internal` (1392). It cites "Go `OfferQuery` at the root returns `ErrNoParent`" (1371). **That citation is wrong**; Go blocks (see above).

### Required Rust change

1. Remove the fail-fast for root and sticky (1900-1907 minus the parent-empty case). When this list cannot forward (`kind == 2 || is_root()`) and no poller is waiting, wait on the local query channel until a poller takes it or the deadline elapses. This matches the Go unbuffered handoff in `OfferQuery` (`matcher.go:293`, `309`).
2. On deadline elapse in the wait (1891-1893), return **`Status::internal("context deadline exceeded")`**, not `deadline_exceeded`. Go returns `InternalServiceError{"context deadline exceeded"}` (`context.go:134-137`).
3. Forwarding (non-root, non-sticky): keep. Go's `ForwardQueryTask` has no rate limit (`forwarder.go:177-206`) and Rust matches.
4. Decide with the owner whether to keep `force_local` → `queries.offer` (queues without a receiver). Go does not store a query without a receiver. This is a parity gap, not part of item 5's fail-fast; record it separately.
5. The `poll_expiration` fallback (1864-1865): Go has no fallback. Keep only if the gRPC layer always supplies a deadline. Otherwise record as a known deviation.
6. Rewrite `matching_rpc_query_without_poller_fails_fast` (`matching_rpc_tests.rs:1368-1400`). New expectation: no response before the deadline, then `Code::Internal` at the deadline. The Go citation at line 1371 must be fixed.

Verdict: **confirmed by Go source; the Rust fail-fast is a real divergence.** Remaining uncertainty: the frontend's own query timeout (outside matching) was **not inspected**, so the SDK-visible result is unverified.

---

## Item 6: ForwarderMaxChildrenPerNode clamp

### Go behavior

- Dynamic property: `go/common/dynamicconfig/dynamicproperties/constants.go:4054-4058` `matching.forwarderMaxChildrenPerNode`, filters domain/tasklist/tasktype, **default 20**.
- Config wiring: `go/service/matching/config/config.go:56`, `125`, `214`.
- **Clamp:** `tasklist/task_list_manager.go:1156-1158`:
  ```go
  ForwarderMaxChildrenPerNode: func() int {
      return max(1, cfg.ForwarderMaxChildrenPerNode(domainName, taskListName, taskType))
  },
  ```
  This `ForwarderConfig` is the one passed to `newForwarder` at `task_list_manager.go:238`, so the clamp applies to every `Parent` call in `forwarder.go:122`, `186`, `216`.
  - **[corrected]:** the parent audit cites `task_list_manager.go:1126-1128`. That range is `IsolationGroupsPerPartition`/`QPSTrackerInterval`. The correct line is 1156-1157.
- `tasklist/identifier.go:101-107` `Parent(degree)`: `if tn.IsRoot() || degree == 0 { return "" }`, then `pid := (tn.partition+degree-1)/degree - 1`, `return tn.GetPartition(pid)`.
  - After the clamp `degree ≥ 1`. So `degree == 0` is unreachable from the manager. Raw 0 or any negative becomes 1, which chains each partition to `partition-1`. A non-root partition always has a non-empty parent.
  - Only `IsRoot()` yields `""`.

### Rust at a0953f121

Three read sites, none clamped:
- Add/poll path: `grpc_matching.rs:1077-1125`. `let degree = {…}` (1077), `|| degree <= 0` (1120) disables forwarding, then `name.parent(degree)` (1125).
- Forward-degree provider: `grpc_matching.rs:410-416` (closure feeding `ForwardDegreeProvider`, `matching_registry.rs:110-112`, `172-173`). No `max(1, …)` visible in 410-416.
- Query path: `grpc_matching.rs:1848-1863` (`task_list_integer(values, domain, root, 0, 20)` — the `0` is task type, the `20` is the default). `|| degree <= 0` at 1902 disables forwarding.

### Required Rust change

At all three read sites, clamp to `max(1, raw)` before use (equivalent to `task_list_manager.go:1157`). Then remove the `degree <= 0` disable branches (1120, 1902) because they become unreachable. Check the Rust `TaskListName::parent(degree)` implementation (not re-read here) against `identifier.go:105`: `((p + d - 1) / d) - 1`, with root → `""`.

Verdict: **confirmed.** Edge-config only: in practice only a 0 or negative dynamic-config value triggers it.

---

## Item 8: Error code mapping for shutdown / transient errors

### Go mapping chain

1. **Handler** (`handler/handler.go`): every RPC returns `hCtx.handleErr(err)`, e.g. 175, 208, 248, 285, 322, 350, 376, 407, 437, 556.
2. **`handleErr`** (`handler/context.go:83-138`):
   - Typed errors pass through unchanged. Cases: `InternalServiceError` (91), `BadRequestError` (95), `EntityNotExistsError` (98), `WorkflowExecutionAlreadyStarted` (101), `DomainAlreadyExists` (104), `QueryFailedError` (107), `LimitExceededError` (110), `ServiceBusyError` (113), `DomainNotActiveError` (116), `RemoteSyncMatchedError` (119), `StickyWorkerUnavailableError` (122), `ReadOnlyPartitionError` (125), `TaskListNotOwnedByHostError` (128), `SemaphoreNotOwnedByHostError` (131).
   - **Default (134-137): any other error → `&types.InternalServiceError{Message: err.Error()}`.** This includes `errShutdown`, raw `context.DeadlineExceeded`/`Canceled`, wrapped `fmt.Errorf`, `persistence.ConditionFailedError`, `persistence.TimeoutError`.
   - `errMatchingHostThrottle` = `ServiceBusyError{"Matching host rps exceeded"}` (`handler.go:54`).
3. **Proto wrapper** (`wrappers/grpc/grpc_handler_generated.go:38-40`) → `proto.FromError` (`types/mapper/proto/errors.go:34-94`):
   - `InternalServiceError` → `CodeInternal` (`errors.go:289-291`)
   - `ServiceBusy` → `CodeResourceExhausted` + `ServiceBusyError` detail (400-404)
   - `RemoteSyncMatched` → `CodeUnavailable` + `RemoteSyncMatchedError` detail (406-408)
   - `StickyWorkerUnavailable` → `CodeUnavailable` + `StickyWorkerUnavailableError` detail (410-412)
   - `ReadOnlyPartition` → `CodeAborted` + detail (414-416)
   - `TaskListNotOwnedByHost` → `CodeAborted` + detail (319-325)
   - `BadRequest` → `CodeInvalidArgument` (305-307); `EntityNotExists` → `CodeNotFound` (293-299)
   - Anything unmatched → `CodeUnknown` (93). Unreachable on the matching path because `handleErr` has already converted it.
4. **Thrift wrapper** (`wrappers/thrift/thrift_handler_generated.go:25-26`) passes typed errors through as thrift structs (`mapper/thrift/errors.go`, not re-read).

Go error sources for shutdown and transient cases:
- `errShutdown` (`task_writer.go:82`, plain `errors.New`). Returned at `task_writer.go:158`, `181` (appendTask on stop), `265` (allocTaskIDs failure while stopping), `task_list_manager.go:551` (`AddTask` when `shouldReload`), `813` (`ReleaseBlockedPollers` when stopped). Through `handleErr` → **`CodeInternal`, message "task list shutting down"**.
- Queue full: `task_writer.go:183-184` `createServiceBusyError("task queue is full and timed out waiting to enqueue")` → `ResourceExhausted`.
- Response-wait timeout: `task_writer.go:176-177`, `fmt.Errorf("...%w", tCtx.Err())` → `CodeInternal`.
- Non-sticky `ConditionFailedError`: `task_writer.go:139-143` signals fatal, then returns the raw error (147-148) → `handleErr` default → **`CodeInternal`**.
- Sticky `ConditionFailedError`: `task_writer.go:144-146` → `InternalServiceError{StickyTaskConditionFailedErrorMsg}` → `CodeInternal`.
- SQL persistence (`common/persistence/sql/common.go:117-153` `convertCommonErrors`): passthrough list (122-132, includes `persistence.TimeoutError` at 127) returns as-is → `handleErr` default → Internal. Not-found branch (134-142, not re-read). **Throttling → `ServiceBusyError`** (144-148). Everything else → `InternalServiceError` (150-152).
  - `IsThrottlingError` per dialect: mysql `sqlplugin/mysql/db.go:106-117` (`ER_CON_COUNT_ERROR`, `ER_TOO_MANY_USER_CONNECTIONS`, `ER_TOO_MANY_CONCURRENT_TRXS`, `ER_CLONE_TOO_MANY_CONCURRENT_CLONES`); postgres `sqlplugin/postgres/db.go:90-99` (`ErrTooManyConnections`, `ErrInsufficientResources`); sqlite always false (`sqlite/error_checker.go:95-97`).
- Ctx errors in matching: query wait deadline (`matcher.go:309-310`, `engine.go:1087-1088`), AddTask sync-match ctx (`matcher.go:187-188`, `211`) → all Internal via default.

### Rust at a0953f121

Correct (no change):
- `sticky_unavailable()` `grpc_matching.rs:110-118`: `Unavailable` + `StickyWorkerUnavailableError` detail. Matches `errors.go:410-412`.
- `remote_sync_failed()` `grpc_matching.rs:1042-1048`: `Unavailable` + `RemoteSyncMatchedError` detail. Matches `errors.go:406-408`.
- `full()` `matching_writer.rs:129-…`: `ResourceExhausted` + `ServiceBusyError` detail. Matches `task_writer.go:184`.

Divergent:
- **Shutdown → Unavailable (should be Internal):**
  - `grpc_matching.rs:1578-1580` (`add_decision_task`) and `1717-1719` (`add_activity_task`): `stop_if_isolation_changed()` → `Status::unavailable("task list shutting down")`. Go `task_list_manager.go:549-551` returns `errShutdown` → `CodeInternal`.
  - `matching_writer.rs:126-128` `stopped()` = `Status::unavailable("task list shutting down")`. Used at 332-333, 360, 366, 369-370. Go `task_writer.go:157-159`, `178-181` → `CodeInternal`.
- **Storage / condition errors → Unavailable (should be Internal, or ResourceExhausted for throttling):**
  - `matching_dispatch.rs:237-239` `queue_status(QueueError)` = `Status::unavailable(error.to_string())` for **every** `QueueError`. Callers: 356 (`Ok(Err(error)) => Err(queue_status(error))`) and 801 (`ClaimError::Queue`).
  - `matching_writer.rs:256-261`: non-sticky `ConditionFailed` → `Status::unavailable(error.to_string())`. Go → `CodeInternal`. Sticky (`ConditionFailed` with kind 1) → `Status::internal("StickyTaskConditionFailedError")` matches Go's sticky branch in form. The exact constant value was **not compared**.
- **Deadline → DeadlineExceeded (should be Internal):**
  - `matching_writer.rs:371`: `Status::deadline_exceeded("failed to receive a success response …context deadline exceeded")`. Go wraps the ctx error (`task_writer.go:176-177`) → `CodeInternal`. Tests `matching_writer.rs:664` and `701` assert `DeadlineExceeded`; they must change with the code.
  - `matching_writer.rs:495` builds `Status::deadline_exceeded("context deadline exceeded")` for a retryable-classification test. Keep the `retryable` value; change the code if the status is ever surfaced.
  - `grpc_matching.rs:1891-1893` (query wait): covered by item 5.
- **Poll deadline error (not analysed):** `grpc_matching.rs:1312` `Status::deadline_exceeded("Matching poll deadline exceeded.")` is on the query-state fetch inside poll. No Go equivalent was traced. Go poll ctx expiry returns an **empty success** (`engine.go:719-750`), so any *poll-level* deadline error in Rust is a candidate divergence. Verify before changing.
- `matching_dispatch.rs:668` `Status::unavailable("Matching delivery worker stopped.")`: no Go analogue traced.

### Required Rust change

1. Change `task list shutting down` to `Status::internal(...)` at `grpc_matching.rs:1579`, `1718` and in `matching_writer.rs:126-128` (`stopped()`). Message unchanged.
2. `queue_status` (`matching_dispatch.rs:237-239`): map by variant. `StorageError::Sql` and `ConditionFailed` (non-sticky) and timeouts → `Internal` with the error text. Throttling storage errors → `ResourceExhausted` + `ServiceBusyError` detail, using the same per-dialect throttling rule as Go (mysql and postgres codes above; sqlite never). Keep Unavailable only for paths that Go maps to Unavailable.
3. `matching_writer.rs:256-261`: non-sticky `ConditionFailed` → `Internal`. Keep the sticky branch and its message. The `retryable` flags are program logic — **do not change them** in this item.
4. `matching_writer.rs:371` and the tests at 664 and 701 → `Internal` (message unchanged). Go wraps the ctx error as `CodeInternal`.
5. Query deadline (`grpc_matching.rs:1891-1893`): `Internal("context deadline exceeded")` (see item 5).
6. Audit `grpc_matching.rs:1312` and `matching_dispatch.rs:668` before changing. Go's poll never returns a ctx error; the poll path returns an empty response.
7. Frontend note: Go's frontend mapping of these codes is **outside matching** and was not inspected here. Verify end to end with the SDK before claiming SDK-visible parity.

Verdict: **confirmed** for shutdown (Unavailable vs Internal), condition errors, the writer deadline, and the query deadline. The poll-level deadline (1312) and the `delivery worker stopped` unavailable (matching_dispatch.rs:668) are **unverified** candidates.

---

## Summary of corrections to the 2026-10-21 audit

| Item | Parent audit said | Verified |
|---|---|---|
| G4 | cancelled poll is `Cancelled`; Go empty success | Confirmed. Also: Go overwrites (no cancel) on duplicate poller ID; `EndPoll` deletes unconditionally; Go loads/owner-checks the manager. |
| G5 | Go root/sticky `OfferQuery` waits until ctx | Confirmed. `ErrNoParent` is effectively unreachable in Go. Go ctx-expiry returns `InternalServiceError`, not `DeadlineExceeded`. |
| G6 | `max(1,…)` clamp missing | Confirmed. Go clamp is at `task_list_manager.go:1156-1157` (parent audit said 1126-1128). |
| G8 | Go `errShutdown` plain error; Rust Unavailable | Confirmed Go → `CodeInternal` via `handleErr` default. Persistence errors and ConditionFailed also Internal in Go; Rust maps them to Unavailable. |

## Not verified (no live Go host; not executed)

- Frontend-side query and poll timeout mapping (outside matching).
- Activity poll cancel branch (`engine.go` ~910-960) beyond the pattern.
- `errIfShardOwnershipLost` non-owner error type beyond its signature.
- Rust `TaskListName::parent` implementation and the cancel-test expectations (`matching_rpc_tests.rs:258, 296, 373`).
- Rust `grpc_matching.rs:1312` and `matching_dispatch.rs:668`.
- Whether the sticky-unavailable message constant matches `StickyTaskConditionFailedErrorMsg`.
