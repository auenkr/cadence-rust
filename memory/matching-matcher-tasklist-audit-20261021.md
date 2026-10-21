# Matching matcher / task-list-manager source audit (2026-10-21)

Scope: Go `go/service/matching/tasklist/{matcher,task_list_manager,task_reader,task_writer,task_gc,forwarder,task_completer,task_list_limiter,db,adaptive_scaler,isolation_balancer}.go` and `go/service/matching/poller/manager.go`, `go/service/matching/liveness`, `go/common/messaging/ackManager.go`, plus `handler/engine.go` where it drives the manager, compared with `rust/crates/cadence-server/src/matching_*.rs`, `grpc_matching.rs`, and the supporting persistence/common crates.

Base commit: `864dcdbe9` (branch `matching-telemetry-mysql-candidate`).

**Source-level only. Nothing here was executed or tested; "implemented" means the Rust code appears to follow the Go logic, not that it is verified.** Rust does not mirror Go's one-object-per-concern structure: the Go `taskListManagerImpl` is spread over `QueueRegistry`/`Owner`/`QueueHandle` (matching_registry.rs), `TaskDispatcher` (matching_dispatch.rs), `TaskReader`, `TaskWriter`, `SyncMatcher`, `DurableTaskQueue` (cadence-persistence/matching_queue.rs) and the `Matching` gRPC handler (grpc_matching.rs). Status values: implemented / partial / missing / different.

## 1. Headline gaps (ranked)

| # | Area | Status | Summary |
|---|------|--------|---------|
| G1 | Go `MustOffer` backlog forwarding | **missing** | A non-root partition's DB backlog is never forwarded to its parent partition. Rust reader only offers to the local rendezvous (matching_reader.rs:529-587). Go forwards after `LocalTaskWaitTime` and deletes the local row on success (matcher.go:317-432). |
| G2 | Partition-config propagation across hosts | **missing** | Go `notifyPartitionConfig` RPCs `RefreshTaskListPartitionConfig` to every non-root read partition (task_list_manager.go:500-540) on update and on root start (:291-305). Rust only pushes to partition owners loaded in the same process registry (matching_registry.rs:1606-1643, 1357-1381). |
| G3 | Adaptive scaler metric collection | **different** | Go collects per-partition metrics via `DescribeTaskList` RPC (adaptive_scaler.go:331-375). Rust calls `self.get()` on each partition locally (matching_registry.rs:1538); with ring ownership that fails for non-local partitions and resets the scaler (:1540-1543), so scaling cannot work in a multi-host ring. |
| G4 | `CancelOutstandingPoll` | **different** | Go cancels the poll context, so the cancelled poll returns an *empty successful* response (ErrNoTasks, engine.go:722). Rust returns gRPC `Cancelled` (grpc_matching.rs:1725). Rust does not load the task-list manager/ownership check (Go engine.go:1109-1130; Rust grpc_matching.rs:3025-3070) and a second poll with the same poller id cancels the first (`register_poll`, :1533-1541), which Go does not do. |
| G5 | Query with no local poller on root/sticky | **different (unvalidated)** | Go root/sticky have `fwdr == nil`, so `OfferQuery` waits on `queryTaskC` until ctx expires (matcher.go:281-313; `noopForwarderTokenC` blocks forever, forwarder.go:80-81). Rust returns `Internal "cannot find parent task list for forwarding"` immediately (grpc_matching.rs:2467-2491), asserted by Rust test `matching_rpc_query_without_poller_fails_fast` (matching_rpc_tests.rs:1519) claiming it is Go's `ErrNoParent`. Go source only returns `ErrNoParent` when a forwarder exists and parent is empty. No Go oracle evidence found in memory for the fast-fail. |
| G6 | `ForwarderMaxChildrenPerNode` clamp | **different** | Go clamps `max(1, value)` (task_list_manager.go:1126-1128). Rust uses the raw value: `degree <= 0` disables forwarding (grpc_matching.rs:1398, 2471) while Go would forward to `partition-1`. Edge config only. |
| G7 | Periodic ack/GC/idle timers | **different** | Go has per-manager timers inside the reader pump and liveness ticker (task_reader.go:269-291, liveness.go eventLoop). Rust runs them from an external maintenance loop (`flush` matching_registry.rs:1832, `sweep_idle` :1781). The Go pump also re-`Signal()`s every ack interval (task_reader.go:289); the Rust pump has no timed re-wake (matching_reader.rs:787-790 waits only on the append notification). |
| G8 | Error codes for shutdown/unavailable | **unverified** | Go `errShutdown` is a plain `errors.New` (task_writer.go:82); Rust maps it to `Unavailable` (grpc_matching.rs:2032, matching_writer.rs:126-128). Persistence transient errors map to `Unavailable` (matching_dispatch.rs:255-257). Confirm what Go's mapper emits for each. |

## 2. matcher.go

| Go function | Behaviour | Rust evidence | Status / difference |
|---|---|---|---|
| `newTaskMatcher` (matcher.go:81) | `taskC`, per-isolation-group `isolatedTaskC`, `queryTaskC` unbuffered channels | `SyncMatcher::new` matching_sync.rs:70; `QueryChannel` matching_query.rs:~60; reader rendezvous matching_reader.rs:389-396 | partial: Rust has two separate rendezvous (SyncMatcher for sync offers, reader `Rendezvous` for DB backlog) plus a separate query channel; Go has one set of channels shared by all three |
| `DisconnectBlockedPollers` / `RefreshCancelContext` (:117,:508) and `ReleaseBlockedPollers` (task_list_manager.go:807) | Cancel blocked pollers, then install fresh cancel ctx; returns `errShutdown` if stopped | `TaskDispatcher::release_blocked_pollers` matching_dispatch.rs:316 (`Notify::notify_waiters`); `release_blocked_pollers_for_domain` matching_registry.rs:502 | implemented; no `errShutdown` return for stopped managers; waiters that arrive after the notify are not affected (same as Go fresh ctx). Released poll returns empty OK (delivery_loop :877-883) like Go |
| `Offer` (:150) | rate-limit unless forwarded (`ErrTasklistThrottled`, counter), `LocalTaskWaitTime` wait, then non-blocking send, else forward under token+`limiter.Allow`, else for root+backlog+forwarded block to ctx (`OfferOrTimeout`); wait for `ResponseC` result | `QueueHandle::offer_sync_observed` matching_registry.rs:341-387; `SyncMatcher::offer` matching_sync.rs:124-201; chain `sync_match_chain` grpc_matching.rs:1300-1526 | implemented. Differences: (a) Go's post-local-wait attempt is a *non-blocking* send (matcher.go:196-244); Rust has no second attempt but blocks only for `local_wait` which is equivalent; (b) forwarding is an in-process loop/remote AddTask rather than matcher-internal; (c) Throttled counter emitted in chain (:1365-1369) |
| Local wait time | `LocalTaskWaitTime`, default 10ms (constants.go:6185) | `sync_wait` default 10_000_000ns registry.rs:1106-1120 | implemented |
| `OfferOrTimeout` (:248) | Root receiving a forwarded DB-backlog task blocks to ctx; ratelimit only when not forwarded; ctx-expired -> `(false,nil)` | `offer_forwarded_backlog` matching_registry.rs:310-327; used grpc_matching.rs:1386-1395 | implemented (only reached via an incoming forwarded AddTask with `source==DbBacklog`, i.e. from a Go child; Rust children never produce one, see G1) |
| `OfferQuery` (:281) | non-blocking local send, else loop {local send / forward token -> `ForwardQueryTask` / ctx}; on `ErrForwarderSlowDown` stop forwarding, keep waiting locally | `query_workflow` grpc_matching.rs:2340-2577, `try_offer` matching_query.rs:~123, `try_forward_query` registry.rs:267 | partial: no wait-until-ctx loop at root/sticky (G5); forwarded error other than slow-down returned as-is (Go same); slow-down handling = `force_local` offer (:2559) which queues the query unconditionally rather than "wait for a poller", result then waits to `deadline` |
| `MustOffer` (:317) | rate limit (`ErrTasklistThrottled` wrapped), local wait, then loop local / forward (2s child ctx) / ctx; on forward success `task.Finish(nil)` | reader dispatch loop matching_reader.rs:437-645 (rate limit via `wait_until` :552-559, local rendezvous offer :571) | **missing forward leg** (G1). Metrics for `AsyncMatchForward*` and `*AfterForwardFailed*` have no Rust producer |
| `Poll` (:438) | invalid isolation group -> default + counter; propagate cancel ctx; `pollNonBlocking` with `LocalPollWaitTime`, then `pollOrForward`; sets `AutoConfigHint` | `delivery_worker` matching_dispatch.rs:748-770 (invalid-group metric), `delivery_loop` :786-1040, `claim_delivery` :336-357, `poll_group` reader.rs:836; hint grpc_matching.rs:1146 | implemented; Go's random `select` among channels vs Rust FIFO queues and `biased` query-first select (grpc_matching.rs:1662) |
| `PollForQuery` (:490) | standby domains poll only `queryTaskC` | grpc_matching.rs:1659-1707 (`active` false -> sleeps `expiration`, query only) | implemented; sleeps the full expiration rather than returning when ctx cancels... returns empty response, same shape |
| `pollOrForward` / `poll` / `pollLocalWait` / `pollNonBlocking` (:514-795) | local-first, forward poll under poll token, fall back to local after forward failure | `delivery_loop` :830-876 + `forward_poll` :440-569 | implemented. `forward_failed` (:802,:874) mimics fallback to `poll`. Parent lifecycle swap (:544-553) sets rate to 100000 to mimic `ForwardPoll` rebuilding the request. Go `ForwardPoll` sets only `Identity` + task list on the forwarded request, Rust matches (:112-127) |
| Forward poll token | `ForwarderMaxOutstandingPolls` default 1 | `ForwardState.permits` registry.rs:1182-1193 default 1; `RequestTokens` matching_forward.rs:17-35 | implemented (generation swap on limit change same as Go `refreshTokenC`) |
| `ratelimit` (:811) | `ErrCannotWait` -> `ErrTasklistThrottled` | `TaskListLimiter::wait_until` matching_limiter.rs:190-242, `WaitOutcome::Throttled` | implemented |
| `getTaskC` (:828) | isolated task goes to group channel only if group known and non-empty | `SyncMatcher::poll_group/compatible` matching_sync.rs:80-89, reader `isolated.get(&group)` :529-532 | implemented |

## 3. task_list_manager.go

| Go function | Rust evidence | Status / note |
|---|---|---|
| `NewManager` (:164) incl. isolation group list sorted, scope, poller manager, liveness, qps tracker, adaptive scaler only for root+normal, forwarder only for non-root non-sticky | `QueueRegistry::get` matching_registry.rs:871-1440 | implemented. Forwarder = `ForwardState` (:1172-1272, `kind==1 || root` -> none). Scaler spawn :1384-1411 requires `scaler_config` |
| `Start` (:262) non-root reads root `GetTaskListInfo`; error other than EntityNotExists stops manager; root pushes config to partitions | `PartitionCache::load` matching_partition.rs:88-113 (`NotFound` -> None, other error -> `queue.stop` + Unavailable at registry.rs:984-995) | partial: root-start push to other partitions missing across hosts (G2) |
| `Stop` (:318) unregister, stop scaler/qps/liveness/writer/reader, reader persists ack + `RunNow` GC, disconnect pollers | `Owner::finish` registry.rs:88-100, `Owner::stop` :82, queue stop waker :1275-1279 | implemented (final ack `persist_final_ack` even after writer stop, GC `force=true`) |
| `watchForStop` / reader+writer `Fatal` | `DurableTaskQueue::check` stops on `ConditionFailed` (matching_queue.rs:232-237); lease failure stops (:264-267); reader errors `queue.stop()` (matching_reader.rs:689,731) | implemented; Rust treats any admission/reader worker failure as fatal, Go retries DB read errors forever (task_reader.go:240-245; Rust loops with `yield_now`, :682-686) |
| `TaskListPartitionConfig` / `PartitionReadConfig` / `PartitionWriteConfig` (:362-403) | `PartitionCache::get` partition.rs:114; `check_write_partition` grpc_matching.rs:1240-1278 | implemented. Sticky returns nil config (`load` kind != 0 -> default). Metrics `TaskListPartitionConfigNum*Gauge`: `emit_partition_runtime_metrics` grpc_matching.rs:~1090-1140 |
| `LoadBalancerHints` (:405) 10s warm-up, backlog count, QPS | `QueueHandle::load_balancer_hints` registry.rs:446-455 | implemented (Go also waits `startWG`; Rust handle only exists after open) |
| `RefreshTaskListPartitionConfig` (:425) nil -> reload from DB (replace even newer), else only newer version wins | `PartitionCache::refresh` partition.rs:131-143, `push` :122-130, handler grpc_matching.rs:2940 | implemented |
| `UpdateTaskListPartitionConfig`/`updatePartitionConfig` (:454-498) validate, no-op if equal (version ignored) or single/single when nil, `version+1`, throttle-retry persist, on failure stop manager | `PartitionCache::update` partition.rs:146-182; `validate` :58-85 mirrors `validatePartitionConfig` (task_list_manager.go:1006-1028) | implemented. Error type: Go `BadRequestError`, Rust `InvalidArgument` (expected mapping) |
| `notifyPartitionConfig` (:500) | registry.rs:1618-1638 | **missing across hosts** (G2); also Go notifies read ids only, Rust unions read+write (write ⊆ read by validation so same set) |
| `AddTask` (:546) | grpc_matching.rs:2005-2120 (decision), 2151-2309 (activity) | implemented with differences below |
| - `shouldReload` -> Stop + `errShutdown` | `stop_if_isolation_changed` registry.rs:272-279, :2031-2033 | implemented (Unavailable vs Go generic error, G8) |
| - drained partition `ReadOnlyPartitionError` (only when `EnableGetNumberOfPartitionsFromCache`) | `check_write_partition` grpc_matching.rs:1240-1278 | implemented |
| - non-forwarded: mark liveness, qps report by isolation group, `EstimatedAddTaskQPSGauge` | `report_arrival` grpc_matching.rs:98-111 | implemented; gauge `EstimatedAddTaskQPSGauge` not emitted by `report_arrival` (verify) |
| - domain lookup error returned | `is_active_in(...).map_err(not_found)` :2069-2077 | implemented (code mapping unverified) |
| - standby: forwarded -> `errRemoteSyncMatchFailed`; else persist, no sync match | :2102-2113 (forwarded -> `remote_sync_failed` :1283-1290, else `enqueue`) | implemented |
| - sync match then forwarded fail -> `errRemoteSyncMatchFailed`; else persist | same | implemented. `remote_sync_failed` is `Unavailable` + `RemoteSyncMatchedError` detail, matches Go proto mapper |
| - sync-match budget `newChildContext(200ms, tailroom 1s)` for non-forwarded; forwarded uses incoming ctx | `sync_match_budget` matching_sync.rs:216-225; deadline selection grpc_matching.rs:2058-2068 | implemented. Forwarded request without gRPC timeout defaults to 200ms (:2060) rather than "no deadline"; Go would use the RPC ctx as is |
| - persist: `taskWriter.appendTask` then `taskReader.Signal` | `QueueHandle::enqueue` registry.rs:388-402, writer notifies waiters on success (matching_writer.rs:269-271) | implemented |
| `DispatchTask` (:634) active -> `MustOffer`; standby + `EnableStandbyTaskCompletion` + not active-active -> `CompleteTaskIfStarted`; `errDomainIsActive` -> MustOffer | `standby::checker` matching_standby.rs:54-150, used by reader dispatch_check matching_reader.rs:478-506 | implemented. If standby completion is disabled Rust falls through to normal offer (no poller ever polls it for standby). Retry: Go task-completer policy is `CreateTaskCompleterRetryPolicy`; Rust 1s/10s/300s (matching_standby.rs:~66-69) - values not cross-checked against `common.taskCompleter*` constants |
| `DispatchQueryTask` (:668) | query_workflow above | partial (G5) |
| `GetTask`/`getTask` (:682-746) | `poll_for_decision_task` grpc_matching.rs:1552-1767, activity analog ~1800-1980 | implemented: `shouldReload` -> empty response :1608; liveness mark `delivery_loop` :808-810; child timeout min(`LongPollExpirationInterval`, parent-1s) `poll_timeout` :1175-1208; rps = `OverrideTaskListRPS` else worker rate else default, `ReportLimit` `poll_rate` :1001-1070; `StartPoll`/`EndPoll` -> `PollLifecycle::begin`/drop matching_dispatch.rs:32-61; standby -> query only :1653-1707. `BacklogCountHint` on task: check `task.BacklogCountHint` setting in activity/decision responses (not verified) |
| `CancelPoller` (:758) | `cancel_outstanding_poll` | **different** (G4) |
| `DescribeTaskList` (:767) | grpc_matching.rs:3167-3320 | implemented: pollers list, task id block, read/ack/backlog, `Empty`, rate, qps, isolation metrics, partition config. `TaskList.Kind` echoes request rather than manager kind |
| `HasPollerAfter` (:754) | registry.rs:459-464, `Manager::has_after` poller.rs | implemented |
| `trySyncMatch` (:848) | see AddTask | implemented |
| `getIsolationGroupForTask` (:903) | closure `IsolationConfig.select` grpc_matching.rs:767-825 + `TaskIsolation::select_with_cause` (cadence-common/task_isolation.rs) | implemented-by-structure; `IsDrained` error -> `error` leak cause path: Rust turns lookup error into `drained = Err` (:789-800) - not verified to take the Go `error` leak branch; poller window default 2s, isolation duration default 200ms checked against dynamicconfig defaults not re-verified |
| `emitMisconfiguredPartitionMetrics` (:954) | grpc_matching.rs:1090-1140 | implemented |
| `newTaskListConfig` (:1030) | registry `with_*` providers + `ReaderLive`/`WriterLive` | partial: most per-task-list properties are read live; `ForwarderMaxChildrenPerNode` clamp missing (G6); `EnableSyncMatch`, `ActivityTaskSyncMatchWaitTime`, `MaxTasklistIdleTime` exist in Go config but are not read by Go manager either |
| `rangeIDToTaskIDBlock` (:983) | `block` matching_queue.rs:165-171 | implemented |
| Identity/PollerID/IsolationGroup in ctx (:1173-1207) | Rust passes explicit fields (`PollLifecycle.info`) | different mechanism, same data |

## 4. task_reader.go

| Go function | Rust evidence | Status / note |
|---|---|---|
| `newTaskReader` per-group buffer size `batchSize-1`, invalid batch size fallback (:104-150) | matching_reader.rs:409-424, `ReaderLive::batch_size` :41-48 | implemented (batch size 1 => unbuffered rendezvous) |
| `Start/Stop/Signal/Fatal` | `with_dispatch_check_metrics` :375-794; `stop`/`wait_stopped` :848-869; wake = `Notify` | implemented. Stop final ack + GC done by `Owner::finish` instead of reader |
| `getTasksPump` (:226) read level, empty-batch ack advance, notify, ack timer | pump matching_reader.rs:650-792, `load_reader_turn` matching_queue.rs:440-470 (`set_read_level`, ack advance when `ack==initial_read`) | implemented; timed ack persistence + `GetTaskListSize` gauge is in `flush` (registry.rs:1896-1903); no timed re-signal (G7) |
| `getTaskBatch` 10-step scan, `ReadRangeSize` (:328) | `scan_batch` matching_queue.rs:128-160 | implemented; per-query transient retry via `matching_retry::persistence` (:143) |
| `getTaskBatchWithRange` retry policy (:296) | matching_retry.rs:30-41 (50ms..10s, 30s expiry) | implemented for SQL errors; Go `ThrottleRetry` also has a throttling policy for ServiceBusy (not applicable to SQL stores; Rust comment matching_retry.rs:1-2) |
| `isTaskExpired` / `addSingleTaskToBuffer` (:350-387) expired -> `SetReadLevel`, else `ReadItem`, route to isolation buffer | `register_read` matching_queue.rs:471-487; routing reader.rs:736-766 | implemented |
| `persistAckLevel` (:389) + lag gauge | `persist_ack_impl` matching_queue.rs:553-573; lag gauge registry.rs:1899-1902 | implemented |
| `completeTask` (:406) error -> rewrite with retry then ack; rewrite failure -> fatal; ok -> `AckItem` + `taskGC.Run` | `finish_delivery`/`rewrite`/`complete` matching_dispatch.rs:358-429; `rewrite_failed` matching_queue.rs:501-512; `append_rewrite` writer.rs:302-330 | implemented. Rewrite failure => `queue.stop()` (:385-387) like `signalFatal`. Rewritten row has fresh created time and zero ScheduleToStart (matches SQL `CurrentTimeStamp` store behaviour, sql_task_store.go:428-447) |
| `newDispatchContext`, `getDispatchTimeout` (:434-468) | reader.rs:533-547 + `task_isolation::dispatch_timeout` | implemented (deadline only for isolated group or rps>1e-7 and active domain) |
| `dispatchSingleTaskFromBuffer` (:480-578) outcome classes: expired, dispatched, canceled (stop), deadline (isolation timeout metric, reselect group), throttled (`BufferThrottle`, Gosched), standby not-started retry, unknown error retry | reader.rs:455-642 | implemented for Offered/Stopped/Throttled/TimedOut. Group is re-selected every attempt (:516-517). `BufferUnknownTaskDispatchError` emitted in delivery path (matching_dispatch.rs:1018-1034) not reader |
| Dispatch handing to pollers | reader `Rendezvous::offer` :139-173 wait for acceptance; poller `claim` | different structure (pull-based claim in poller) but same ack/rewrite-before-ack ordering |

## 5. task_writer.go / db.go

| Go | Rust | Status / note |
|---|---|---|
| `newTaskWriter`, `appendCh` size `OutstandingTaskAppendsThreshold` | `TaskWriter::new_with_live_config` matching_writer.rs:171-290, capacity 0 uses rendezvous queue | implemented (live capacity sampled once; Go too) |
| `appendTask` (:151) timeouts: `AppendTaskTimeout` for enqueue (-> ServiceBusy "task queue is full and timed out waiting to enqueue") and response wait | `append_inner` :331-373 (`full()` :129-138; deadline error text at :371) | implemented. The response-timeout error text matches Go's wrapped format only approximately |
| `taskWriterLoop`, `getWriteBatch` (`MaxTaskBatchSize` additional) | :201-288, `drain_batch_with` :143-164 | implemented |
| `allocTaskIDs`/`allocTaskIDBlock`/`renewLeaseWithRetry` | `append_serialized` matching_queue.rs:238-308 | implemented. IDs allocated per task rather than per batch up front; lease failure stops queue (:264-268) like `Stop`+`signalFatal` |
| `handleErr` ConditionFailed -> fatal; sticky -> `InternalServiceError("StickyTaskConditionFailedError")` | matching_writer.rs:251-262 | implemented; sticky ConditionFailed treated as *retryable* in Rust `retryable` calc (:251-255) only for the outer rewrite path |
| `maxReadLevel` update after write even on error | matching_queue.rs:296-305 | implemented |
| `taskListDB` mutex around Renew/Update/CreateTasks/UpdateTaskListPartitionConfig, not GetTasks/CompleteTasksLessThan | `database` mutex matching_queue.rs:253,277,534,555; `collect_garbage` unlocked :576-579 | implemented |
| `db.GetTaskListInfo` | `store.get_task_list` partition.rs:106 | implemented |
| `GetTaskListSize` / backlog | `task_list_size` matching_queue.rs:643-646; Go stores result in `db.backlogCount` (unused for hints) | implemented |

## 6. task_gc.go and ack manager

| Go | Rust | Status |
|---|---|---|
| `taskGC.Run/RunNow/tryDeleteNextBatch/checkPrecond` | `TaskGarbageCollector::run_with_batch_provider` matching_gc.rs:20-70; `run_garbage_collection` matching_queue.rs:581-638 | implemented; fixture-tested (not re-run). 1s min interval (`MaxTimeBetweenTaskDeletes`) hard-coded at matching_queue.rs:591-597 (Go cfg default also 1s, not dynamic) |
| `messaging.AckManager` (`ReadItem/AckItem/Set*/GetBacklogCount`) | cadence-common/src/ack_manager.rs:8-80 | implemented; ack loop iterates registered ids only (gap-tolerant) which is equivalent; duplicate-ack warning log not emitted |

## 7. forwarder.go / task_list_limiter.go

| Go | Rust | Status |
|---|---|---|
| `newForwarder`, `ForwardTask` (rate limit `ForwarderMaxRatePerSecond` default 10, `ErrNoParent`, sticky refused), `handleErr` ServiceBusy -> slow down | `try_forward_task_observed` registry.rs:255-264, `ForwardRate` matching_forward.rs:43-66 (5s refresh like `quotas.DynamicRateLimiter` `_ttl`, dynamicratelimiter.go:36), forward in chain grpc_matching.rs:1417-1501 | implemented. Remote ServiceBusy -> Rust ignores any remote error (`result.ok()`, :1498-1500) so a remote ServiceBusy/other error is treated as "not matched" and the sync-match flow falls back to persistence; Go does the same for AddTask (errors ignored except slow-down metric) but increments `SyncMatchForwardTaskThrottleErrorPerTasklist` only for slow-down (Rust: not emitted for remote ServiceBusy) |
| `ForwardQueryTask` (no rate limit) | `try_forward_query` registry.rs:267 | implemented |
| `ForwardPoll` | `forward_poll` matching_dispatch.rs:440-569 | implemented (in-process parent via `Parent::Dispatcher`, else gRPC) |
| `AddReqTokenC/PollReqTokenC` | `RequestTokens` | implemented |
| `newTaskListLimiter`, `ReportLimit`, `tryUpdate`, `getLimitAndBurst` | matching_limiter.rs:91-176, `TaskListRate` (cadence-common/matching_rate.rs) | implemented; TaskDispatchRPS 100000 and TTL 60s are constants (:21-23) like Go config; `min burst` captured once at construction like Go (:126-128) |

## 8. Poller history, liveness, QPS, isolation

| Go | Rust | Status |
|---|---|---|
| `poller.Manager` (TTL 5m, max 5000, outstanding tracking, group counters, `HasPollerAfter`, `GetCountByIsolationGroup`, `ListInfo`) | cadence-common/src/poller.rs:1-130 | implemented; `CancelPoll` is not in this manager (Go `Info.cancel`), see G4 |
| Poller gauge on history update | `PollLifecycle::begin`/drop matching_dispatch.rs:32-61 | implemented |
| `liveness.Liveness` (ttl, check at ttl/2, stop manager) | cadence-common/src/liveness.rs; `sweep_idle` registry.rs:1781-1816 (cadence ttl/2) | implemented via external sweeper (G7) |
| `stats.EmaFixedWindowQPSTracker` | cadence-common/src/qps.rs (referenced registry.rs:1039-1052) | implemented-by-reference, not read in this audit |
| Isolation group drain/leak causes | `TaskIsolation::select_with_cause` | implemented-by-reference, not re-read line by line |
| `isolation_balancer.go` / `adaptive_scaler.go` algorithms | matching_scaler.rs (Sustain, balancer, `Scaler::tick`) | implemented for algorithm; metric collection different (G3). Go `run()` resets sustains and isolation state on metric error (adaptive_scaler.go:155-161), Rust `scaler.reset()` on any `get` error (registry.rs:1540-1543) |

## 9. task_completer.go

| Go | Rust | Status |
|---|---|---|
| `CompleteTaskIfStarted` retry loop, active -> `errDomainIsActive`, EntityNotExists + 24h guard, nil info, started check, metrics | matching_standby.rs:54-150 | implemented; `task_started` :12-34 fixture-tested. Go `isTaskStarted` returns `errTaskTypeNotSupported` for other types, Rust returns `Internal`. Active-active check uses `active_clusters_by_cluster_attribute` non-empty (:99-110) vs Go `IsActiveActive()` - equivalence not verified |

## 10. Other behaviours worth recording

* `Matching` sticky checks (`_stickyPollerUnavailableWindow` 10s): grpc_matching.rs:119-147 for AddDecisionTask/QueryWorkflow, matching Go engine.go:1031-1036. Implemented.
* Go `PollForDecisionTask` loop retries when the started-task is already started (`task.IsStarted`, engine.go:754) - forwarded poll results return through `Parent::Remote` response copy (matching_dispatch.rs:512-531) so already-started responses from a Go parent are forwarded as-is.
* Per-RPC readiness: the reader pump only reads after the task list is opened by an RPC; idle owners are unloaded by sweeper at ttl, not on a per-manager Go goroutine.
* Persistence layer coverage is only checked for SQL semantic equivalence in this audit (CreatedTimestamp = request time, Expiry from ScheduleToStart). Cassandra is out of scope for Rust.

## 11. Suggested follow-ups (source-level triage only)

1. Decide and implement or explicitly retire G1 (child backlog forward to parent via `MustOffer` semantics, 2s forward ctx, `Finish(nil)` deleting local row, plus metrics).
2. Implement outbound `RefreshTaskListPartitionConfig` fan-out (and root-start push) using the existing `remote_channel_for_task_list` helper (G2).
3. Make scaler metric collection use DescribeTaskList on the owning host (G3).
4. Make `CancelOutstandingPoll` return empty success to the cancelled poll and route through manager ownership (G4); drop previous-poll cancel on re-register unless Go oracle shows it.
5. Re-validate G5 against a live Go matching host; if Go blocks until ctx, change the fast-fail and the test at matching_rpc_tests.rs:1519.
6. Add the `max(1, ..)` clamp for forwarder degree (G6).
7. Verify error-code mapping for `errShutdown`/persistence transient errors against Go's mapper (G8).
8. Add timed pump re-signal on ack ticks (G7) if any scenario relies on external DB changes; otherwise note as unreachable (single owner).
