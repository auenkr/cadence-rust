# Matching subsystem audit 2: Go vs Rust (2026-10-23)

Base: origin/audit/matching-subsystem-20261023 (0e4e0c606). Read-only: no builds or tests were run.
Follow-up to `memory/matching-subsystem-audit-20261023.md` (call it "audit 1"). It covers the areas audit 1 marked unknown.
Excluded: backlog forward-to-parent (`OfferOrTimeout`, the `source==DbBacklog && forwarded` branch, `MustOffer` forwarding). Another worker owns it.

Paths. Go: `go/service/matching/tasklist/` (matcher.go = M, task_list_manager.go = TLM). Rust: `rust/crates/cadence-server/src/` (grpc_matching.rs = GM, matching_registry.rs = REG, matching_dispatch.rs = DSP, matching_sync.rs = SYNC).

Severity: SDK = SDK-visible, COR = correctness, MIN = minor (metrics or edge cases), UNK = could not verify.

## Architecture note

Go matches with unbuffered channels inside `taskMatcherImpl`.
Rust has no matcher object. It splits the work into three parts:
- `SyncMatcher` (SYNC) is a rendezvous of waiting pollers and waiting offers.
- The `sync_match_chain` in GM does the forward-to-parent loop for AddTask.
- `TaskDispatcher.delivery_loop` (DSP) is the poll side. It takes the local claim, the sync offer, and the forward-poll leg together in one `select!`.

## 1. matcher.go, method by method

| Go (M:line) | Rust | Behavior diff | Sev | Fix |
|---|---|---|---|---|
| `Offer` 150-245. Rate limit for non-forwarded tasks, local wait (`LocalTaskWaitTime`), then a non-blocking send, then forward with a token. | `REG offer_sync_observed` 385-440 (limiter, then `SYNC.offer` 124-201). The forward leg is `GM sync_match_chain` 1338-1567. | **Equivalent.** Same order. The limiter applies only when not forwarded, and ancestors skip it (`GM:1393-1404`). Zero local wait returns false unless a poller is already parked (`SYNC:163`). `ErrTasklistThrottled`, a ctx error, or a consumer error all end with the task persisted. Rust maps all three to `Unmatched` (`REG:428-436`) and persists. The only differences are the extra `Throttled` metric hook and the lack of a final non-blocking retry after local wait (negligible). | none | |
| `Offer` ResponseC wait 170-190 | `SYNC.response` 202-212 | Equivalent. Success is reported only after History's start outcome. A deadline expiry returns false, the task is persisted, and the poller still completes (same Go race, deduplicated by History). | none | |
| `Offer` forwarding: `ErrForwarderSlowDown` counter 230 | `GM:1456-1472` | Equivalent (`RateLimited` gives the metric and a fall back to persist). | none | |
| `getTaskC` 828-834 plus isolation groups | `SYNC.compatible` 87-89, `poll_group` 80-86 | Equivalent. An empty or unknown task group goes to the shared channel, which every poller reads. A known group goes only to pollers of that group. An unknown poller group is treated as "". | none | |
| `OfferQuery` 281-313 | `GM query_workflow` 2451-2677, `matching_query.rs` 77-173 | Same shape: try a local parked poller, else forward with a token (no rate limit), else wait locally. **Divergence:** Go forwards then handles errors like this. `ErrForwarderSlowDown` (the parent returned ServiceBusy) falls back to local-only waiting. Any other error is returned. Rust `GM:2634-2643` returns the remote `result` unconditionally, so a parent ServiceBusy or ResourceExhausted fails the query instead of waiting locally. See finding A1. | COR (SDK-visible on error) | See A1. |
| `MustOffer` 317-432 | Out of scope (backlog and forward). Pull model in `DSP.delivery_loop` plus `matching_reader.rs`. | Audit 1 read it. Not repeated. | | |
| `Poll` 438-485 | `DSP.poll_with_lifecycle` 749-784, `delivery_loop` 823-1077 | Equivalent for the local wait. `localPollWaitTime` gives local tasks a head start (`DSP:875-885`). A parked poll takes a sync offer or a backlog claim. An empty poll returns `R::default()` (`DSP:781`). An unknown poller group is treated as default. | none | |
| `Poll` AutoConfigHint 454-459 | `GM auto_config_hint` 1184-1212 | Equivalent, including `PollerWaitTimeInMs` and the decision-type quirk on activity empties. | none | |
| `Poll` invalid isolation group counter 441-445 | `DSP.delivery_worker` 799-807 | **Differs.** Go counts only when `Poll` was given a group (isolation matcher enabled, non-sticky) and the group is unknown. Rust counts when the group is non-empty and the reader has no such group. With isolation disabled, or on a sticky list, `reader.isolated` is empty, so **every poll that carries a group increments `poller_invalid_isolation_group_per_tl`**. A forwarded poll at the parent also skips the check. See finding A3. | MIN (spurious metric) | See A3. |
| `PollForQuery` 490-506 (standby domain) | `GM:1759-1766` | **Differs.** Go runs a local wait, then **forwards the poll to the parent** (`pollOrForward` on `queryTaskC` only). Rust parks locally with `sleep(expiration)` and has no forward leg. See finding A2. | COR (SDK-visible) | See A2. |
| `pollOrForward` 514-603 | `DSP.forward_poll` 477-606 | Equivalent. While forwarding, the local channels are not read. A forward error falls back to local (`forward_failed`). A parent response, including an empty one, ends the poll. Rust takes the poll-token permit inside the `select!` alongside the local claim, which matches Go's `select`. Note: the forward leg does not read `released` (`DSP:532-535`), but a domain failover releases the parent too (`GM:1774-1777`). | none | |
| `poll` 605-669 and `pollLocalWait` 671-729 and `pollNonBlocking` 731-795 | `delivery_loop` | Equivalent. Only counter and event names differ. | none | |
| `ratelimit` 811-822 | `TaskListLimiter.wait_until` (`matching_limiter.rs:190`) | Equivalent. `ErrCannotWait` becomes `Throttled`. | none | |
| `DisconnectBlockedPollers` / `RefreshCancelContext` 116-119, 508-512 (`ReleaseBlockedPollers` TLM:807-820) | `DSP.release_blocked_pollers` 353-355 (`Notify::notify_waiters`) | Equivalent for polls parked in the claim. Go's `cancelCtx` also cancels a poll blocked in `ForwardPoll`, and a poll that starts between disconnect and refresh. Rust's `Notify` is stateless, so a poll in History start or in the forward leg is not released by this call. Because failover also releases every list for the domain, the effect is small. | MIN | Optional: add `released` to the `forward_poll` select (`DSP:532`). |

## 2. Poller history and CancelPoller

| Go | Rust | Diff | Sev |
|---|---|---|---|
| `poller/manager.go` StartPoll/EndPoll/CancelPoll, `HasPollerAfter`, `HasPollerFromIsolationGroupAfter`, `GetCountByIsolationGroup`, `ListInfo`, TTL 5 min, cap 5000 | `cadence-common/src/poller.rs` 1-132, fixture test `matching-pollers.json` | Equivalent. Same `outstanding` flag, same `mostRecentPollEnd` reset, same per-group counts. StartPoll/EndPoll timing matches: `DSP.begin()` is called per claim iteration and dropped once a task is claimed, as Go's `defer EndPoll` fires when `getTask` returns (`TLM:731`). | none |
| `CancelPoller` TLM:758 and engine.go:1109-1131 | `GM cancel_outstanding_poll` 3176-3230, `register_poll` 1590 | Go cancels the child ctx of `getTask`, which yields an empty poll. Rust uses `Notify::notify_one` on a per-poll guard, with the same empty-poll result. Go returns nil whether or not a poll matched, and so does Rust. Rust's guard is registered for the entire RPC, including History start. Go's is registered only between StartPoll and EndPoll. A cancel during History start drops the start with `Cancelled` and rewrites the task. | MIN |
| `getTask` child ctx 708 (LongPollExpirationInterval, 1s tailroom) | `GM poll_timeout` 1213-1246 | Equivalent. | none |
| `getTask` standby test 738 | `GM:1713-1719` | The standby poll registers its wait guard but never forwards. Same issue as A2. | COR |

## 3. Task list limiter

Go `task_list_limiter.go` (`ReportLimit`, `tryUpdate`, `getLimitAndBurst`) against `cadence-common/src/matching_rate.rs` 21-95 and `matching_limiter.rs` 79-243. Equivalent: precision is 0.001, the optimistic reject path, the TTL re-snapshot, burst = `max(ceil(rps/partitions), minBurst)` with burst 0 when rps is 0. `matching-rate.json` Go fixtures cover it.
- The partition divisor matches. Go uses `numReadPartitionsFn` (from the cache when enabled, else `NumReadPartitions`, clamped to >= 1). Rust uses the same (`REG:1083-1108`). The cache branch is not clamped in either.
- `poll_rate` (`GM:1039-1096`) matches `getTask:714-725`: override > 0, then the worker rate, then `TaskDispatchRPS`. Activity polls carry a worker rate. Decision polls pass nil.
- A forwarded poll resets the rate to `TASK_DISPATCH_RPS` (`DSP:584`). Go's ForwardPoll sends no metadata, so this also matches.
- Gap: the Go limiter updates `RateLimitPerTaskListGauge` in `tryUpdate:103,112`. Rust's `report_limit` returns an `Option` to emit it (`GM:1074`). Not verified that the `else if ttlElapsed` gauge at 112 is emitted when the rate is unchanged. MIN, UNK.

## 4. shard_processor*.go and task_list_registry.go

- `shard_processor.go` against `matching_shard_processor.rs`: equivalent. The Ready initial state, the load staying at its prior value until `reportTime + TTL < now` (strict) and then being recomputed on every call (reportTime is never updated), Stop by exact name, Start as a no-op, and an empty ShardID rejected. Rust's Stop also awaits full queue teardown, which is stricter than Go and acceptable.
- `task_list_registry.go` against `QueueRegistry` (`REG`): the by-domain and by-name indexes and `TaskListManagersGauge` (`REG:1001,1532,1974,2117,2140`). Equivalent. Go overwrites on Register and un-registers only if the manager still matches (`Unregister` compares `mgr`). Rust uses `registry.detach(&key, &owner)` (`REG:2209`) with the same compare. UNK: not traced to every call site.

## 5. Standby add-task and task completer

| Go | Rust | Diff | Sev |
|---|---|---|---|
| `AddTask` standby branch TLM:578-601: a forwarded add returns `errRemoteSyncMatchFailed`. Otherwise `appendTask`, `Signal`, `syncMatch=false`. | `GM:2156-2217` | Equivalent. A standby domain skips `sync_match_chain`, a forwarded add returns `remote_sync_failed` (Unavailable plus the `RemoteSyncMatchedError` detail), and a non-forwarded add enqueues and returns the partition config. | none |
| `DispatchTask` 634-665 (active: MustOffer; standby with `EnableStandbyTaskCompletion && !IsActiveActive`: completer; active-active: MustOffer) | `matching_standby.rs checker` 38-150, `matching_reader.rs:493-513` | Equivalent routing. Active-active and disabled return `Ok(false)`. | none |
| `CompleteTaskIfStarted` 92-170, retry policy 1s/10s/5min | `checker` 57-147 | Equivalent. Fixture-tested `task_started`. A missing execution completes only after 24 h (`created_nanos`), otherwise it retries. A nil `WorkflowExecutionInfo` is an error that retries. | none |
| Counters 128,150,155,163 | `checker` 109-144 | **Differs.** Go counts `StandbyClusterTasksCompletionFailure` for the `errWaitTimeNotReachedForEntityNotExists` outcome. Rust turns that outcome into `Ok(None)`, and the failure counter fires only when the last result is an `Err`. | MIN |
| Reader handling of completer error: `errTaskNotStarted`/`errWait...` retry silently, other errors do `BufferUnknownTaskDispatchError` plus an error log (`task_reader.go:549-575`) | `matching_reader.rs:509-512`: `Err(_)` yields `yield_now` and retries, with no counter and no log | **Differs.** Rust cannot tell "not started" from "unknown". It counts and logs neither case. See A4 (this reverses audit 1 F10). | MIN |
| Completer ctx: Go runs under `cancelCtx` (no timeout) on standby (`newDispatchContext:436-451`) | `checker` + `dispatch_stop.changed()` | Equivalent. Stopping the reader aborts the retrier (`matching_reader.rs:494-497`). | none |

## 6. isolation_balancer.go

Compared `adjustWritePartitions`, `refreshGroups`, `calculatePartitionGoal`, `applyGroupChanges`, `getMinPartitionsPerGroup`, `getMinGroupsPerPartition`, `ensureMinimumGroupsPerPartition`, `movePartitions`, `findMaxWithGroup`, `findMinWithoutGroup` and `Sustain` (`go/common/clock/sustain.go`) against `matching_scaler.rs` 17-523. The math and structure are equivalent (target = `min(max(minPartitions, ideal), writeCount)` against Rust's clamp, which is the same because `minPartitions <= writeCount`).
- `ensureMinimumGroupsPerPartition`: Go sorts once and moves two pointers. Rust re-sorts every iteration and compares the extremes (`matching_scaler.rs:461-491`). Same invariants. Tie-break groups can differ. Go itself is nondeterministic here, because groupState is a map and `SortFunc` is unstable, so only invariants can be fixture-tested. MIN.
- Metrics: Go emits `IsolationGroupStoppedPolling/StartedPolling/Upscale/Downscale/PartitionsGauge` inside the balancer, only for groups in `groupState`. Rust infers them from before/after sets over a wider group union (`matching_scaler.rs:600-655`). Rust's Upscale/Downscale fires on any count change, while Go fires only when a sustained trigger is taken (`target != current`). `PartitionsGauge` is also emitted for groups not in `groupState`. MIN.
- UNK: no property test of the balancer against Go output (Go's own `isolation_balancer_test.go` was not run).

## 7. Audit 1 unknowns 7-10 resolved

| Audit 1 | Result |
|---|---|
| F6 (non-sticky ConditionFailed wire code) | **Resolved.** Go `handlerContext.handleErr` (`handler/context.go:83-139`) turns every unlisted error, including `ConditionFailedError`, into `InternalServiceError{Message: err.Error()}`. Rust returns `Status::internal(error.to_string())` (`matching_writer.rs:262`). The sticky case is also Internal. Equivalent. |
| F4 (writer response-timeout status) | **Resolved.** Go wraps the ctx error, which `handleErr` makes Internal. Rust returns `Status::internal("failed to receive ... context deadline exceeded")` (`matching_writer.rs:374`). A full queue is ServiceBusy in both. Minor: Go's timeout is `min(AppendTaskTimeout, request ctx)`, while Rust uses only the append timeout. |
| F10 (unknown dispatch metric) | **Reversed.** See A4. |
| F1 (ack-interval wiring) | **Resolved.** The reader pump reads the interval live per reset (`matching_reader.rs:61-62,804`), and the registry re-reads it per tick (`REG:2201-2206`, test `maintenance_uses_live_tasklist_ack_interval_provider` at `REG:2795`). Go has a single timer. |
| F8 (scaler metrics) | Partly resolved. The counters exist, but the emission semantics are approximated. See section 6. |

## Findings (SDK-visible and correctness first)

**A1. COR/SDK. Query forward error handling.** Go: `matcher.go:296-306`. A `ServiceBusyError` from the parent (`forwarder.handleErr`, `forwarder.go:298-303`) becomes `ErrForwarderSlowDown`, and the query keeps waiting for a local poller until ctx expires. Any other error is returned. Rust: `grpc_matching.rs:2634-2643` returns the remote result as is, including ServiceBusy and ResourceExhausted.
- Scenario: the parent host is overloaded, the frontend gets ServiceBusy for a query that Go would have served locally.
- Fix: after the remote `query_workflow` call, if the status code is ResourceExhausted with a ServiceBusy detail, set `force_local = true` and `continue` instead of returning. The permit is released as Go does.

**A2. COR/SDK. Standby-domain decision polls never forward.** Go: `getTask` (`task_list_manager.go:738-740`) calls `matcher.PollForQuery`, which runs the local wait and then `pollOrForward` on `queryTaskC`, so a child partition forwards its poll to the parent. Rust: `grpc_matching.rs:1759-1766` just `sleep(expiration)` and consults only the local `queries.next()`.
- Scenario: a standby cluster with more than one partition. `QueryWorkflow` offers locally, then forwards to the parent. A parent that has no direct poller never serves the query, because the children's polls are not forwarded to it. The query waits until its deadline.
- Fix: for `!active`, run the same forward-poll leg as `forward_poll` (`matching_dispatch.rs:477`) restricted to the query channel, or call `forward_poll` with a claim that never yields tasks.
- Severity: only affects standby domains with `numTasklistWritePartitions > 1`.

**A3. MIN. Spurious `poller_invalid_isolation_group_per_tl`** (`DSP:799-807`). Go (`M:440-445`) counts only when the isolation matcher is enabled and the group is unknown. Rust counts for every non-empty group when the reader has no isolated channel for it, which is always the case when isolation is disabled or the list is sticky. A forwarded poll at the parent is never checked. Fix: gate the check on `reader` having isolation enabled, and run it in the parent path as well.

**A4. MIN. Dispatch-error metric is misplaced.** `matching_dispatch.rs:1055-1071` increments `buffer_unknown_task_dispatch_error_per_tl` when History start fails on a poll. Go increments it only in the reader's `dispatchSingleTaskFromBuffer` (`task_reader.go:568`). Rust's reader (`matching_reader.rs:509-512`) counts nothing. Fix: remove the increment from `DSP:1066`. In the reader's `Err(_)` branch, increment the counter and log, except when the error is the final "task not started" (`matching_standby.rs:144`) or the wait-time retry.

**A5. MIN. Standby completion-failure counter** (`matching_standby.rs:134-145`): the Go counter also fires for the "workflow not found and under 24 h" outcome. Rust returns `Ok(None)` for it.

**A6. MIN. Query-poll empty response.** In the Go path where `GetMutableState` fails (`engine.go:784-787`), the empty response has `PartitionConfig` and `LoadBalancerHints` but **no** `AutoConfigHint`. Rust adds one (`GM:1754`). Harmless, but not identical.

**A7. MIN. Isolation balancer and scaler metrics** (section 6): emission semantics approximated, not 1:1.

**A8. MIN. Forwarded add with no gRPC timeout.** Go trusts the context. Rust falls back to a 200 ms deadline (`GM:2146-2148`), which also bounds the wait for History's response. Real Go callers always send timeouts.

## Not verified

- Rate-limit gauge on the unchanged-TTL branch (section 3).
- `QueueRegistry` call sites for Register/Unregister parity (section 4).
- No Go balancer test cases were executed against Rust (section 6).
- `Parent::Dispatcher` is only the library-mode fallback. Production resolves `Parent::Remote` or `Parent::Service` (`REG:2238-2253`), so the dispatcher-only branch does not run the parent's query channel. Not exercised here.

## Verified equivalent (no action)

Offer local wait, rate limit, forwarded skip, sync-match budget (`sync_match_budget`, `SYNC:216-225` against `newChildContext`), isolation compatibility, `OfferQuery` local-first plus forward token without rate limit, poll local wait then forward, empty poll with hints, poller manager, `CancelOutstandingPoll`, limiter report math, shard processor report and stop, standby AddTask and completer decision logic.
