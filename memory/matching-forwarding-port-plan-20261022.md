# Matching forwarding port plan (2026-10-22)

Research-only plan. No code was changed and no cargo build was run.
Base: `master` @ `a0953f121` (committed sources only). The main checkout has
uncommitted Rust edits (`matching_registry.rs`, `matching_runtime.rs`, `lib.rs`,
new `matching_shard_processor.rs`) that overlap the files named here. Rebase
any implementation onto those edits before starting.

Go paths are under `go/service/matching/`. Rust paths are under
`rust/crates/cadence-server/src/`. Line numbers were read from the committed
files on this base.

## 1. Go behavior inventory

### 1.1 Forwarder (`tasklist/forwarder.go`)

| Behavior | Go location |
| --- | --- |
| Struct: scope, cfg, taskListID, kind, client, add/poll token chans, cached limits, `quotas` dynamic limiter | `forwarder.go:40-64` |
| Errors: `ErrNoParent`, `ErrTaskListKind`, `ErrInvalidTaskListType`, `ErrForwarderSlowDown` | `forwarder.go:73-78` |
| Ctor: `rpsFunc` from `ForwarderMaxRatePerSecond`, `NewDynamicRateLimiter`, token pools sized by outstanding limits | `forwarder.go:93-114` (limiter `:108`, pools `:111-112`) |
| `ForwardTask`: sticky refused `:118`; parent name via `Parent(ForwarderMaxChildrenPerNode)` `:122`; empty name -> `ErrNoParent` `:123`; rate limiter `Allow()` else `ErrForwarderSlowDown` `:127` | `forwarder.go:117-129` |
| `ForwardTask` decision: `AddDecisionTask`, `DomainUUID = task.Event.DomainID` (source domain), `TaskList{Name: parent, Kind: fwdr.taskListKind}`, `ForwardedFrom = child name`, `Source`, `PartitionConfig`, `ScheduleID`, `ScheduleToStartTimeoutSeconds` | `forwarder.go:141-153` |
| `ForwardTask` activity: `AddActivityTask`, `DomainUUID = taskListID.GetDomainID()` (target list domain), `SourceDomainUUID = task.Event.DomainID` | `forwarder.go:155-168` |
| `ForwardTask` latency timers (`ForwardTaskLatencyPerTaskList` + histogram) | `forwarder.go:133-138` |
| `ForwardQueryTask`: sticky refused `:182`; `ErrNoParent` `:187`; **no rate limiter**; `QueryWorkflow` with `DomainUUID = Query.Request.DomainUUID`, `TaskList{Name: parent, Kind}`, `QueryRequest`, `ForwardedFrom = child name` | `forwarder.go:177-208` (RPC `:197-205`) |
| `ForwardPoll`: sticky refused `:212`; `ErrNoParent` `:217`; poller ID / identity / isolation group from ctx `:227-229`; `PollForDecisionTask` `:233-245` (`DomainUUID = taskListID.GetDomainID()`, `PollerID`, `PollRequest{TaskList{parent, Kind}, Identity}`, `ForwardedFrom = child name`, `IsolationGroup`) | `forwarder.go:211-271` |
| `ForwardPoll` activity: `PollForActivityTask` `:251-263` | `forwarder.go:250-267` |
| `ForwardPoll` returns started task via `newInternalStartedTask` `:249`, `:267` | `forwarder.go:249,267` |
| Token channels: `AddReqTokenC` `:276`, `PollReqTokenC` `:284`, lazy resize `refreshTokenC` `:289-296` | `forwarder.go:273-296` |
| `handleErr`: `*types.ServiceBusyError` -> `ErrForwarderSlowDown`; all other errors pass through | `forwarder.go:298-303` |
| Token pool: buffered chan of `maxOutstanding` tokens; `release()` puts back | `forwarder.go:305-315` |

Note the child name is always the **forwarding** partition's own name
(`fwdr.taskListID.GetName()`), not the parent's. The receiving side uses it
to recognise a forwarded request.

Loadbalancer: `client/matching/loadbalancer.go:62` says a non-empty
`ForwardedFrom` means no load balancing. The request goes straight to the
named partition (`loadbalancer.go:103,111,117`).

### 1.2 Matcher (`tasklist/matcher.go`)

Offer / sync-match forward:

| Behavior | Go location |
| --- | --- |
| `Offer`: rate limit only for non-forwarded tasks `:152-158` (`SyncThrottlePerTaskListCounter` `:155`) | `matcher.go:150-158` |
| Local wait: `LocalTaskWaitTime` child ctx `:164-166`; wait on `getTaskC(task)` `:168`; with `ResponseC` waits for poller reply `:173-189` | `matcher.go:164-195` |
| Immediate `getTaskC` send `:197`; with `ResponseC` waits `:201-212` | `matcher.go:196-215` |
| **Forward trigger**: `default:` branch when no poller is ready. Non-blocking token check `:219`, `ForwardTask(ctx, task)` `:222`, `token.release()` `:223` | `matcher.go:216-224` |
| Success: `SyncMatchForwardPollLatencyPerTaskList` (+histogram) emitted, return `true, nil` | `matcher.go:224-228` |
| Busy: `SyncMatchForwardTaskThrottleErrorPerTasklist` `:230-232` | `matcher.go:230-232` |
| Backlog root branch: `!isForwardingAllowed() && source == DbBacklog && IsForwarded()` -> `OfferOrTimeout` `:233-240` | `matcher.go:233-240` |
| Otherwise `return false, nil` (no forward error surfaced) | `matcher.go:243` |
| `OfferOrTimeout`: rate limit only if not forwarded `:249-258`; block on `getTaskC`/ctx `:260-275`; no forwarding | `matcher.go:248-276` |
| `OfferQuery`: local non-blocking `queryTaskC` `:282-287`; token chan `:289`; loop select: local send, token -> `ForwardQueryTask` `:297`, release `:298`, success returns resp `:299-300`, `ErrForwarderSlowDown` -> switch to `noopForwarderTokenC` and retry local only `:302-306`, ctx done `:309-310` | `matcher.go:281-313` |

Must-offer (backlog dispatch and any `DispatchTask` caller):

| Behavior | Go location |
| --- | --- |
| `MustOffer`: rate limit first; error -> `"rate limit error dispatching"` `:324-328` | `matcher.go:317-328` |
| Local-first attempt with `localWaitTime` child ctx `:333-336`; send `:337`; `AsyncMatchLocalPollCounter` / latency `:339-344`; ctx done `:345-349` | `matcher.go:330-352` |
| Retry loop, ctx check `:357-361`; local send `:363-371` | `matcher.go:354-371` |
| **Forward leg**: token `:372`; `childCtx` **2s** timeout `:375`; `ForwardTask(childCtx, task)` `:376`; `token.release()` `:377` | `matcher.go:372-377` |
| Failure: `AsyncMatchForwardTaskThrottleErrorPerTasklist` on busy `:379-381`; event + debug log `:382-389`; **race local poll vs childCtx** `:394-409`; local win -> `AsyncMatchLocalPollAfterForwardFailed*` metrics `:398-404`; childCtx expiry -> `attempt++`, `continue forLoop` `:405-408` | `matcher.go:378-409` |
| Forward success: `Task Forwarded` event, `AsyncMatchForwardPollCounterPerTaskList` `:415`, attempt/latency timers `:416-419` | `matcher.go:411-419` |
| **`task.Finish(nil)` after forward success** (deletes the durable row on the child) `:421-424` | `matcher.go:421-425` |
| Ctx done in this loop `:426-429` | `matcher.go:426-429` |

Poll forward:

| Behavior | Go location |
| --- | --- |
| `Poll`: isolation fallback `:440-445`; local `pollNonBlocking` first `:464` | `matcher.go:438-468` |
| Then `pollOrForward` `:483`; `PollForQuery` `:490-506` (query-only, same shape) | `matcher.go:469-506` |
| `pollOrForward` selects over: isolated task, task, query, ctx done (`PollTimeoutPerTaskListCounter`), **poll token** `:578` | `matcher.go:514-603` |
| Forward poll `:588`; success -> `token.release()` `:589`, `PollForwardMatchLatencyPerTaskList` `:590-591`, return task `:598` | `matcher.go:578-599` |
| Failure -> `token.release()` `:600`, then `tm.poll(...)` (local only, no second forward) `:601` | `matcher.go:600-601` |
| Local-only `poll` after failed forward; `PollLocalMatchAfterForwardFailedLatencyPerTaskList` `:617,638` | `matcher.go:605-669` |
| `fwdrPollReqTokenC` / `fwdrAddReqTokenC`: noop channel when `fwdr == nil` `:797-809` | `matcher.go:797-809` |
| `ratelimit` maps `ErrCannotWait` to `ErrTasklistThrottled` `:811-822` | `matcher.go:811-822` |
| `isForwardingAllowed() = fwdr != nil` `:824-826`; `getTaskC` isolation selection `:828-834` | `matcher.go:824-834` |

### 1.3 Task list manager (`tasklist/task_list_manager.go`)

| Behavior | Go location |
| --- | --- |
| Forwarder built only when `isFowardingAllowed` (non-root, non-sticky) | `task_list_manager.go:236-238`, `isFowardingAllowed :891-893` |
| Forwarder config closures (outstanding polls/tasks, rate, `max(1, ChildrenPerNode)`) | `task_list_manager.go:1146-1157` |
| `AddTask`: forwarded add (non-empty `ForwardedFrom`) skips liveness/QPS `:559-568` | `task_list_manager.go:559-568` |
| `AddTask`: `isForwarded := ForwardedFrom != ""` `:586`; standby + forwarded -> `errRemoteSyncMatchFailed` `:590-591` | `task_list_manager.go:585-592` |
| `AddTask`: `trySyncMatch` `:606`; sync success returns `:607-611` | `task_list_manager.go:604-611` |
| `AddTask`: **forwarded and sync-miss -> `errRemoteSyncMatchFailed`, never persisted** `:613-619`; otherwise append + `taskReader.Signal()` `:621-627` | `task_list_manager.go:613-627` |
| `DispatchTask` active side -> `MustOffer` `:641-648`; standby path `:652-663` | `task_list_manager.go:634-664` |
| `DispatchQueryTask` -> `OfferQuery` `:668-676` | `task_list_manager.go:668-676` |
| `trySyncMatch`: `newInternalTask(..., ForwardedFrom, forSyncMatch=true)` `:849`; non-forwarded gets `newChildContext(ctx, maxSyncMatchWaitTime, 1s)` `:855-859`; forwarded trusts ctx `:855-856` | `task_list_manager.go:848-862` |
| `maxSyncMatchWaitTime = 200ms` | `task_list_manager.go:159` |
| `newChildContext` (tailroom clamp) | `task_list_manager.go:870-889` |

### 1.4 Reader and task (`tasklist/task_reader.go`, `tasklist/task.go`)

| Behavior | Go location |
| --- | --- |
| Reader's dispatch function is `tlMgr.DispatchTask` (the only wiring in the tasklist package) | `task_reader.go:142` |
| Buffered backlog task: `newInternalTask(..., TaskSourceDbBacklog, "", false, isolationGroup)` `:500` | `task_reader.go:500` |
| `dispatchTask` call `:504`; success -> `Dispatched Buffered Task`, break retry `:509-512` | `task_reader.go:504-512` |
| `ErrTasklistThrottled` -> retry after `runtime.Gosched()` `:543-547` | `task_reader.go:543-547` |
| `newInternalTask` (ForwardedFrom stored, `ResponseC` for sync) | `task.go:64-99` |
| `IsForwarded() = forwardedFrom != ""` | `task.go:132-134` |
| `newInternalQueryTask` takes `ForwardedFrom` from the request | `task.go:102-113` |

### 1.5 Handler / engine (`handler/handler.go`, `handler/engine.go`)

| Behavior | Go location |
| --- | --- |
| Per-RPC `ForwardedPerTaskListCounter` when `ForwardedFrom != ""` | `handler.go:166-168` (AddActivityTask), `:199`, `:232`, `:272`, `:313` (QueryWorkflow) |
| Engine add/poll traffic counter skips forwarded requests (`CadenceTasklistRequests`) | `engine.go:546-548` (decision), `:629-631` (activity) |
| `AddTask` call passes `ForwardedFrom` `:580`, `:656` | `engine.go:575-584`, `:652-660` |
| Query: sticky refused in engine `:1031`; `DispatchQueryTask` `:1043` | `engine.go:1014-1043` |
| `emitForwardedFromStats` mapping local/remote matches | `engine.go:1553-1571`, called at `:771` and `:958` |

## 2. Rust current state (committed master)

| Go behavior | Rust location | Status |
| --- | --- | --- |
| Forwarder state (token pools, rate, parent degree, child name) | `matching_forward.rs:1-60` (`RequestTokens`, `ForwardRate`); `matching_registry.rs:209-221` (`QueueHandle::try_forward_task`, `try_forward_query`); `matching_dispatch.rs:69-74` (`ForwardState`) | Split across three types. No single `Forwarder`. |
| `ForwardTask` rate check (`ForwarderMaxRatePerSecond`) | `matching_registry.rs:213-216` (`forward_rate.allow`) | Ported. |
| Errors `ErrNoParent`, `ErrTaskListKind`, `ErrForwarderSlowDown` | Only `Status::internal("cannot find parent task list for forwarding")` at `grpc_matching.rs:1905-1913`. No slow-down type. | Partial. |
| Sync-match forward chain (`Offer` default branch) | `grpc_matching.rs:1059-1207` (`sync_match_chain`) | Ported, but the RPC branch and in-process branch both live inline. |
| Forwarded add: no rate limit (`Offer` `:152`) | `matching_registry.rs:243-253` (`try_sync_match_forwarded` -> `offer_sync(limit=false)`) | Ported. |
| Forwarded add sync-miss -> never persisted | `grpc_matching.rs:1646-1650`, `1778-1780` | Ported. |
| Forwarded root backlog waits (`OfferOrTimeout`) | `grpc_matching.rs:1112-1117`; `matching_registry.rs:254-270` (`offer_forwarded_backlog`) | Ported. |
| Forward-to-parent for a **buffered backlog task** (`MustOffer` forward leg, `matcher.go:372-424`) | `matching_reader.rs:426-467` offers only through local `target.offer(...)`. Nothing calls `try_forward_task` on this path (grep: only `grpc_matching.rs:1129`). | **MISSING.** |
| Poll forward (`pollOrForward` token leg) | `matching_dispatch.rs:701-800` (`ClaimError::Forward`, `forward_failed`); `forward_poll` `:415-503`; `ForwardPollRequest` impls `:92-193` | Ported. Local head start `:748-751` mirrors `pollNonBlocking`. |
| Query forward (`OfferQuery`) | `grpc_matching.rs:1826-1941` (`try_offer`, parent walk, `try_forward_query`, `query_workflow` RPC `:1931`) | Ported with one divergence (see 4.6). |
| Forwarded poll: request rebuilt (`ForwardPoll`) | `matching_dispatch.rs:97-111`, `:146-160`; test `:919` | Ported. |
| Forwarded add RPC to parent (`ForwardTask` request fields) | `grpc_matching.rs:1152-1188` (`AddDecisionTaskRequest` / `AddActivityTaskRequest`) | Ported. Fields match Go (see section 4). |
| Remote channel resolution | `membership.remote_channel(name)` `grpc_matching.rs:1135-1136`, `:1920-1921`; `resolve_parent_dispatcher` `matching_registry.rs:1646`; `Parent::{Dispatcher,Service,Remote}` `matching_dispatch.rs:75-79` | Ported. In-process fallback exists. |
| Forwarding metrics | None. Rust has no metrics emission in `matching_*.rs` or `grpc_matching.rs` (grep for `metrics::`, `IncCounter`, `counter!` finds nothing). | **MISSING** (cross-cutting, see section 5). |

Existing Rust tests already covering forwarding (`matching_rpc_tests.rs`):
`:595` forward task limits/reload/rate zero; `:796` forwarded root backlog late
poller; `:887` child poll -> parent backlog; `:909` child add sync match, no
duplicate persistence; `:939` cancelled forwarded poll unregisters waiter;
`:969` forwarding disabled; `:1245` child query -> parent poller; `:1791`
forwarded add never persists; `:2107` local poll wait delays forwarding;
`:2251` parent query and completion partition; `:2289` local parent poll
expiration. Unit tests: `matching_forward.rs:76`, `:91`; `matching_dispatch.rs:919`.

## 3. Port steps (in order)

Each step is independently testable. Do not begin step N+1 before step N's
tests pass on the same branch.

### Step 1: Error taxonomy (no behavior change)

- Add `ForwardError` to `matching_forward.rs`:
  `SlowDown` (Go `ErrForwarderSlowDown`), `NoParent` (`ErrNoParent`),
  `TaskListKind` (`ErrTaskListKind`), `InvalidTaskListType`
  (`ErrInvalidTaskListType`), `Remote(Status)`.
- Add `fn classify(status: Status) -> ForwardError` mirroring
  `forwarder.go:298-303`. Map the parent's busy status to `SlowDown`. Before
  implementing, grep `grpc_matching.rs` for how ServiceBusy is encoded today
  (the port has to match what the Go peer actually sends).
- Replace the bare `Status::internal("cannot find parent task list ...")` at
  `grpc_matching.rs:1905-1913` with `ForwardError::NoParent` converted to the
  same status. The wire text must not change.

### Step 2: Consolidate the forwarder

- New module `matching_forwarder.rs` with
  `pub struct Forwarder { child: TaskListName, kind: TaskListKind, degree: i64, add_tokens: Arc<RequestTokens>, poll_tokens: Arc<RequestTokens>, rate: Arc<ForwardRate>, resolve: Arc<ParentResolver> }`.
  This mirrors `forwarderImpl` (`forwarder.go:40-64`).
- Methods, each with the Go gate order:
  - `forward_task(&self, req) -> Result<Partition, ForwardError>`: sticky -> `TaskListKind`; no parent -> `NoParent`; `rate.allow()` else `SlowDown`; RPC; `classify`.
  - `forward_query(&self, req)`: sticky -> `TaskListKind`; no parent -> `NoParent`; **no rate check**; RPC.
  - `forward_poll(&self, req)`: sticky -> `TaskListKind`; no parent -> `NoParent`; RPC; classify.
- `ForwardState` (`matching_dispatch.rs:69-74`) and the `try_forward_*` methods on `QueueHandle` (`matching_registry.rs:213-221`) delegate to this type. Keep the existing `ForwardState::new` signature working for the registry (`matching_registry.rs:954`) during the refactor, or change both in one commit.
- Config closures stay in `grpc_matching.rs:410-460` (already read live, which matches Go `task_list_manager.go:1146-1157`).

### Step 3: Backlog `MustOffer` forward leg (the missing behavior)

Target: `matching_reader.rs` dispatch loop, around the current
`target.offer(dispatched, ...)` call at `:467`.

Go order, reproduced exactly (`matcher.go:317-425`):

1. Rate limit first (`ratelimit`, `:324`). Keep the existing
   `dispatch_limiter.wait_until` at `matching_reader.rs:448`. It already does
   this.
2. Race a local offer against `LocalTaskWaitTime` (`:334-336`). Use the
   same local-wait source as `sync_wait` (`matching_registry.rs:205`).
3. Loop:
   a. Race local offer vs `try_forward_task` permit (`:372`). Permit is the
      add token; rate is still checked by `try_forward_task` (Go checks it
      inside `ForwardTask`, `forwarder.go:127`).
   b. `ForwardTask` with a **2s** child timeout (`:375`). Use
      `tokio::time::timeout(Duration::from_secs(2), ...)`.
   c. On `Ok`: complete the durable row (Go `task.Finish(nil)`, `:424`). Use
      the same completion the reader already calls for `Attempt::Offered`
      (`matching_reader.rs` ~467-480). Emit no local offer.
   d. On `Err`: race local offer vs the child timeout (`:394-409`). Local
      wins -> success. Timeout -> `attempt += 1` and continue (`:405-408`).
4. Ctx/stop: honor `dispatch_stop` like the existing offer does.

Scope rule: only the `DispatchTask` path (reader -> `MustOffer`) gets this.
The forwarded backlog that arrives at a root is already covered by
`offer_forwarded_backlog` (step 3 does not change it).

Parent request: `ForwardTask` with `source = backlog (2)`,
`forwarded_from = child name`, `partition_config` as on the task, and
`ScheduleToStartTimeoutSeconds` from the task.

### Step 4: Sync-match chain uses the Forwarder

- Replace the inline RPC block at `grpc_matching.rs:1135-1193` with
  `forwarder.forward_task(...)` (decision and activity variants). Keep the
  in-process branch (`durable_queue` hop at `:1194-1204`) as the single-node
  fallback. Go has no in-process hop, so mark it in a comment as the local
  equivalent of `ForwardTask` through the client.
- Keep the Go gate order: `Offer` forwards only if a token is available
  right now (non-blocking `try_forward_task`, `matcher.go:219`). Do not wait.
- On `SlowDown`, keep returning `Ok(None)` with no error to the caller, so the
  child persists (Go `Offer` returns `false, nil`, `matcher.go:230-243`). Only
  `ServiceBusy` increments the throttle metric (step 6).

### Step 5: Poll forward (keep, add error typing)

- `forward_poll` (`matching_dispatch.rs:415-503`) already matches Go. Move the
  RPC body into `Forwarder::forward_poll` with `ForwardError` returned, and
  keep `forward_failed = true` (`matching_dispatch.rs:779-780`) to match Go
  falling to local-only `poll` (`matcher.go:600-601`).
- Keep the local head start `Duration::from_nanos(local_wait)`
  (`matching_dispatch.rs:748-751`). It mirrors `pollNonBlocking`
  (`matcher.go:737-741`).

### Step 6: Query forward: fix the token-exhaustion divergence

- Current Rust: `force_local = true` when no token is available
  (`grpc_matching.rs:1938-1940`).
- Go: `OfferQuery` blocks on the token **and** on local match
  (`matcher.go:289-313`). It only falls back to local-only after
  `ErrForwarderSlowDown` (`:302-306`).
- Fix: when no permit is immediately available, `select!` over
  `handle.queries.registration + offered` and `forward.permits.acquire()`
  until the deadline. Do not use `force_local`.
- Keep the NoParent fast-fail (`grpc_matching.rs:1900-1913`).

### Step 7: Metrics (blocked on a metrics subsystem)

Rust emits no metrics. Do not add a one-off counter. Add a
`MatchingMetrics` trait with a no-op default and call sites only where the
Go call sites are. Emission sites:

| Metric (Go key) | defs.go | Go emission site |
| --- | --- | --- |
| `ForwardedPerTaskListCounter` (`forwarded_per_tl`) | `:4056` | `handler.go:166,199,232,272,313` |
| `SyncMatchForwardTaskThrottleErrorPerTasklist` | `:4059` | `matcher.go:231` |
| `AsyncMatchForwardTaskThrottleErrorPerTasklist` | `:4060` | `matcher.go:380` |
| `ForwardTaskLatencyPerTaskList` (+ `Histogram`) | `:4070-4071` | `forwarder.go:133-138` |
| `ForwardQueryLatencyPerTaskList` (+ `Histogram`) | `:4072-4073` | `forwarder.go:191-196` |
| `ForwardPollLatencyPerTaskList` (+ `Histogram`) | `:4074-4075` | `forwarder.go:221-226` |
| `LocalToLocalMatchPerTaskListCounter` | `:4076` | `engine.go:1553-1571` (via `:771`, `:958`) |
| `LocalToRemoteMatchPerTaskListCounter` | `:4077` | same |
| `RemoteToLocalMatchPerTaskListCounter` | `:4078` | same |
| `RemoteToRemoteMatchPerTaskListCounter` | `:4079` | same |
| `SyncMatchForwardPollLatencyPerTaskList` (+ `Histogram`) | `:4092-4093` | `matcher.go:226-227` |
| `AsyncMatchForwardPollCounterPerTaskList` | `:4099` | `matcher.go:415` |
| `AsyncMatchForwardPollAttemptPerTaskList` | `:4100` | `matcher.go:416`, `:417` (histogram) |
| `AsyncMatchForwardPollLatencyPerTaskList` (+ `Histogram`) | `:4102-4103` | `matcher.go:418-419` |
| `AsyncMatchLocalPollAfterForwardFailedCounterPerTaskList` | `:4104` | `matcher.go:399` |
| `AsyncMatchLocalPollAfterForwardFailedLatencyPerTaskList` | `:4107` | `matcher.go:402-403` |
| `PollForwardMatchLatencyPerTaskList` (+ `Histogram`) | `:4111-4112` | `matcher.go:590-591` |
| `PollLocalMatchAfterForwardFailedLatencyPerTaskList` | `:4113` | `matcher.go:617,638` |

Read the defs.go lines again before wiring names. Only the names in the Go
table are authoritative.

### Step 8: Tests and verification

See section 6. Once steps 1-6 pass, run the existing gates in
`memory/matching-sync-forwarding-next.md` (`matching_` lib tests, forwarding
differential `tests/run-matching-forward-compat.py`, and mixed Go/Rust
matching) before claiming the forwarding slice done.

## 4. RPC client calls and fields

Each row lists the Go call, the Rust call, and the fields that must match on
the wire.

| Path | Go call (forwarder.go) | Rust call | Key fields |
| --- | --- | --- | --- |
| Sync add forward (decision) | `AddDecisionTask` `:141-153` | `add_decision_task` `grpc_matching.rs:1155-1169` | `domain_id = source domain` (`:142`); `task_list = {parent, kind}`; `forwarded_from = child name`; `source`; `partition_config`; `schedule_id`; `schedule_to_start_timeout`; `workflow_execution` |
| Sync add forward (activity) | `AddActivityTask` `:155-168` | `add_activity_task` `grpc_matching.rs:1171-1187` | `domain_id = target list domain` (`:156`); `source_domain_id = source` (`:157`); same as decision |
| Backlog forward (new, step 3) | `ForwardTask` via `AddDecisionTask` / `AddActivityTask` from `MustOffer` (`matcher.go:376`) | new, in `matching_reader.rs` via the `Forwarder` (step 2) | same field set; timeout 2s |
| Query forward | `QueryWorkflow` `:197-205` | `query_workflow` `grpc_matching.rs:1922-1933` | `domain_id = Query.Request.DomainUUID`; `task_list = {parent, kind}`; `request = QueryRequest`; `forwarded_from = child name` |
| Poll forward (decision) | `PollForDecisionTask` `:233-245` | `poll_for_decision_task` via `ForwardPollRequest::forward` `matching_dispatch.rs:112-143` | `domain_id = target list domain` (`:234`); `poller_id`; `request = {task_list parent+kind, identity}`; `forwarded_from = child`; `isolation_group` |
| Poll forward (activity) | `PollForActivityTask` `:251-263` | `poll_for_activity_task` `matching_dispatch.rs:161-192` | same |
| Remote channel selection | `matching.Client` load balancer, skipped for forwarded (`loadbalancer.go:62,103,111`) | `membership.remote_channel(name)` `grpc_matching.rs:1135`, `:1920`; `resolve_parent_dispatcher` `matching_registry.rs:1646` | Name-addressed. No load balancing for forwarded. |

Timeouts: Go's `ForwardTask` uses the caller ctx (Offer) or 2s (MustOffer,
`matcher.go:375`). Rust sets `request.set_timeout(deadline - now)` for the
sync path (`grpc_matching.rs:1153`) and the poll path
(`matching_dispatch.rs:122-126`). Backlog forward needs the 2s bound added.

Metadata: Go passes no metadata beyond the request fields. Keep it that way.

## 5. Structs and signatures to add or change

- `matching_forward.rs`:
  - `pub enum ForwardError { SlowDown, NoParent, TaskListKind, InvalidTaskListType, Remote(Status) }`
  - `pub fn classify(status: Status) -> ForwardError`
- `matching_forwarder.rs` (new):
  - `pub struct Forwarder` (fields in step 2)
  - `pub async fn forward_task(&self, req: AddRequest, ...) -> Result<Option<PartitionConfig>, ForwardError>`
  - `pub async fn forward_query(&self, req: QueryRequest) -> Result<QueryWorkflowResponse, ForwardError>`
  - `pub async fn forward_poll<R>(&self, req: R) -> Result<R::Response, ForwardError>` (generic over the decision/activity poll request)
- `matching_dispatch.rs`:
  - `ForwardState` becomes a thin holder of `Arc<Forwarder>`.
  - `TaskDispatcher` gains a forward leg on its reader offer path (step 3).
- `matching_reader.rs`:
  - `TaskReader` gains `forward: Option<Arc<Forwarder>>` (or a closure). Keep it optional so the non-forwarding root still behaves as today (`matching_rpc_tests.rs:969`).
- `matching_registry.rs`:
  - `QueueHandle::try_forward_task` / `try_forward_query` (`:213-221`) become `Forwarder`-backed.
  - Add `forward_source` check: `ForwardState.source` must equal the child list name (confirm at `matching_registry.rs:954-960` before implementing; it is passed as `forwarded_from` in `forward_poll`, `matching_dispatch.rs:457`).
- `grpc_matching.rs`:
  - `sync_match_chain` (`:1059-1207`) delegates the RPC to `Forwarder`.
  - Query loop (`:1870-1941`) replaces `force_local` with the wait in step 6.
- `metrics`: new `MatchingMetrics` trait (no-op default). Add only once a metrics backend exists. Otherwise leave the call sites out and record them as open.

## 6. Unit tests to write

Go test -> Rust test (new names proposed). Existing Rust tests are noted with
`(exists)`.

Forwarder (Go `forwarder_test.go` suite, `:54`):

1. `forwarder_task_error_maps_busy_to_slow_down` (Go `TestForwardTaskError` `:77`; classify step 1).
2. `forwarder_decision_request_fields` (Go `TestForwardDecisionTask` `:86`): `domain_id` is source; `forwarded_from` is child; kind copied.
3. `forwarder_activity_request_fields` (Go `TestForwardActivityTask` `:110`): `domain_id` is target list; `source_domain_id` is source.
4. `forwarder_task_rate_exceeded_makes_no_rpc` (Go `TestForwardTaskRateExceeded` `:135`).
5. `forwarder_query_task_error` (Go `TestForwardQueryTaskError` `:148`).
6. `forwarder_query_task_request` (Go `TestForwardQueryTask` `:159`).
7. `forwarder_query_not_rate_limited` (Go `TestForwardQueryTaskRateNotEnforced` `:178`): with zero forward rate, a query still forwards.
8. `forwarder_poll_error_falls_back_local` (Go `TestForwardPollError` `:192`).
9. `forwarder_poll_decision_request` (Go `TestForwardPollForDecision` `:203`): poller id, identity, isolation group carried.
10. `forwarder_poll_activity_request` (Go `TestForwardPollForActivity` `:231`).
11. `forwarder_max_outstanding_concurrency` (Go `TestMaxOutstandingConcurrency` `:259`). Pool bound honored. Existing partial coverage: `matching_forward.rs:76`.
12. `forwarder_max_outstanding_config_update` (Go `TestMaxOutstandingConfigUpdate` `:311`). Existing partial: `matching_forward.rs:76`.
13. `forwarder_sticky_never_forwards` (Go `forwarder.go:118,182,212`).
14. `forwarder_no_parent_for_root_and_zero_degree` (Go `identifier.go:101-103`, `forwarder.go:123,187,217`).

Parent naming (Go `identifier.go:95-120`):

15. `parent_name_table` for degree 1, 2, 20: `parent(degree)` equals Go `GetPartition((p+d-1)/d-1)`. Add a table test that uses the same numbers as Go `identifier_test.go`.

Matcher (Go `matcher_test.go` suite, `:298-1235`):

16. `offer_forwards_when_token_ready_and_no_local_poller` (Go `TestOffer_NoTimeoutSyncMatchedNoError` `:694`, `TestOffer_AsyncMatchedNoError` `:771`). Cover the `default` branch `matcher.go:216-224`.
17. `offer_forward_error_returns_false_no_error` (Go `TestOffer_NoTimeoutSyncMatchedError` `:719` analogue). `matcher.go:243`. Child persists.
18. `offer_rate_limited_non_forwarded_only` (Go `TestOffer_RateLimited` `:682`; forwarded skips limiter per `:152`). Exists partially: `matching_rpc_forward_task_limits_reload_and_rate_zero_blocks` (`:595`).
19. `offer_or_timeout_root_forwarded_backlog_waits_for_poller` (Go `TestOfferOrTimeout_SyncMatchTimedOut` `:794`, `TestOfferOrTimeout_AsyncMatchNotMatched` `:818`). Exists: `matching_rpc_forwarded_root_backlog_waits_for_late_poller` (`:796`).
20. `must_offer_remote_match_completes_durable_row` (Go `TestMustOfferRemoteMatch` `:437`; `task.Finish(nil)` `matcher.go:424`). **New.**
21. `must_offer_remote_rate_limit` (Go `TestMustOfferRemoteRateLimit` `:505`). **New.** Rate-limited forward on the reader path, local retry.
22. `must_offer_forward_busy_then_local_match` (Go `TestMustOffer_LocalMatchAfterForwardError` `:937`). **New.** Local wins during child timeout, and no row is completed twice.
23. `must_offer_forward_busy_then_timeout_retries` (Go `TestMustOffer_ContextExpiredAfterForwardError` `:971`). **New.**
24. `must_offer_forward_timeout_is_two_seconds` (`matcher.go:375`). **New.** Assert the 2s bound on the forward RPC.
25. `query_remote_sync_match_returns_parent_response` (Go `TestQueryRemoteSyncMatch` `:317`). Exists: `matching_rpc_child_query_forwards_to_parent_poller` (`:1245`).
26. `query_remote_error_propagates` (Go `TestQueryRemoteSyncMatchError` `:368`).
27. `offer_query_token_exhaustion_waits_for_local_or_token` (Go `OfferQuery` `:289-313`). **New.** Covers step 6, which replaces `force_local`.
28. `offer_query_forward_error_slowdown_falls_back_local_until_deadline` (Go `TestOfferQuery_ForwardError` `:841`, `:302-306`). **New.**
29. `offer_query_context_expired` (Go `TestOfferQuery_ContextExpired` `:865`).
30. `poll_or_forward_forwards_when_no_local_task` (Go `Test_pollOrForward_ForwardTask` `:1069`). Exists: `matching_rpc_child_poll_forwards_to_parent_backlog` (`:887`).
31. `poll_or_forward_forward_then_poll` (Go `Test_pollOrForward_ForwardTaskThenPoll` `:1094`): forward error -> local poll only (`matcher.go:600-601`). Exists partially: `matching_rpc_local_poll_wait_delays_forwarding_and_reloads` (`:2107`).
32. `remote_poll` and `remote_poll_for_query` (Go `TestRemotePoll` `:621`, `TestRemotePollForQuery` `:647`). Exists: `matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition` (`:2251`).
33. `poll_forward_cancel_unregisters_parent_waiter` (Go Test_fwdrPollReqTokenC `:1235`). Exists: `matching_rpc_cancelled_forwarded_poll_unregisters_parent_waiter` (`:939`).

Sync chain / add path (Rust-only, grounded in Go `task_list_manager.go:585-627`):

34. `forwarded_add_never_persists_on_miss` (exists `matching_rpc_forwarded_add_never_persists_and_reports_remote_failure` `:1791`).
35. `child_add_sync_matches_parent_without_duplicate_persistence` (exists `:909`).
36. `sync_chain_rpc_carries_partition_config_and_timeout` (**new**, asserts `grpc_matching.rs:1152-1188` fields and timeout = deadline remaining).
37. `forwarded_add_is_not_rate_limited_at_parent` (`matching_registry.rs:243-253`, Go `matcher.go:152`). **New.**
38. `forwarding_disabled_root_keeps_backlog` (exists `:969`).

Metrics (after step 7 only):

39. One test per emission row in section 3 step 7, asserting the Go key string from `defs.go`.

## 7. Divergences found while reading (for review)

1. **Backlog forward leg is missing** (`matcher.go:372-424` has no Rust equivalent; reader offers only locally at `matching_reader.rs:467`). A child partition with a backlog of unpolled tasks and a parent poller will not drain to the parent. This is the largest functional gap in this area.
2. **Query token exhaustion** (`grpc_matching.rs:1938-1940` `force_local`) differs from Go's blocking select (`matcher.go:289-313`).
3. **Error taxonomy**: Rust has no `SlowDown`. Any RPC error on the sync chain returns `Ok(None)` (`grpc_matching.rs:1189-1191`). This matches Go's end result (`matcher.go:230-243` -> `false, nil`), but the Go metric `SyncMatchForwardTaskThrottleErrorPerTasklist` is not emitted.
4. **In-process hop**: when no remote channel exists, Rust walks the ancestor's `durable_queue` (`grpc_matching.rs:1194-1204`). Go always goes through `ForwardTask` RPC. The in-process path skips the parent's `AddTask` checks other than `check_write_partition`. Confirm that the parent's `isForwarded` branch (`task_list_manager.go:586-619`) has no other effect in the single-process case.
5. **Deadline for forwarded add**: Rust uses `parent_deadline` or `now + 200ms` (`grpc_matching.rs:1605-1608`). Go's `trySyncMatch` trusts the incoming ctx for forwarded (`task_list_manager.go:855-856`). The 200ms fallback is only for a forwarded call with no deadline. Go would have no such bound.
6. **Forwarded poll** `forwarded_from` is `ForwardState.source`. Confirm this equals the child list name (Go `forwarder.go:243`, `:261`). This is not verified in this read.
7. **Metrics**: none emitted in Rust (see step 7).

## 8. Open items

- Verify the ServiceBusy wire encoding before writing `classify` (step 1).
- Verify `ForwardState.source` (matching_registry.rs:954) equals the child name (divergence 6).
- Confirm the metrics subsystem plan. Without it, steps 7 and test 39 stay open.
- Rebase onto the uncommitted main-checkout Rust edits before implementing.
