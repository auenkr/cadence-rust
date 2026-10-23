# Matching task-list subsystem: Go vs Rust audit (2026-10-23)

Base: origin/matching-candidate-20261023 (4f5fb6722). Read-only; no builds or tests were run.
Scope excludes backlog forward-to-parent (another worker owns it).

## Coverage (be honest)

| Area | Depth |
|---|---|
| task_gc.go | Full, line-by-line |
| task_reader.go | Full read of Go; Rust pump/dispatch read (`matching_reader.rs:400-812`); `reader_turn` and `scan_batch` internals only checked against fixtures |
| task_writer.go | Full Go; Rust `append_serialized`, `persist_ack_impl` and the writer error mapping |
| liveness | Full |
| task_list_manager.go | AddTask, DispatchTask, GetTask/getTask, DescribeTaskList, getIsolationGroupForTask |
| isolation policy | Full compare against `task_isolation.rs` |
| adaptive_scaler | Partial: only the count and threshold math |
| matcher.go | Only MustOffer read. Offer, OfferOrTimeout, OfferQuery, Poll, pollOrForward, pollLocalWait and ratelimit are **unknown** |
| isolation_balancer.go | Function list only; the Rust counterpart exists (`matching_scaler.rs:182-530`) but is **not compared** |
| poller history, task_completer, task_list_limiter, task_list_registry, shard_processor*.go | **Not audited** (unknown) |
| forwarder.go | Only ForwardTask (not the backlog path) |

## Verified-equivalent items (no action)

| Go | Rust | Notes |
|---|---|---|
| `taskGC.checkPrecond` / `tryDeleteNextBatch` (`task_gc.go:62-99`) | `matching_gc.rs` `run_with_batch_provider` | Same trylock bail-out, same `backlog >= batch`, same strict `>` on the time check, same cursor only advancing when a short batch comes back, same abort on delete error. A Go fixture test exists. |
| `Liveness` (`liveness.go`) | `cadence-common/src/liveness.rs`; sweep at `matching_registry.rs:2072-2100` | Same `now - last < ttl` rule and the same ttl/2 cadence. |
| MarkAlive only on a non-forwarded AddTask, and in GetTask (`task_list_manager.go:561,690`) | `grpc_matching.rs:2137-2140`, `matching_dispatch.rs:845` | Same. |
| `writer.maxReadLevel` advancing even when CreateTasks fails (`task_writer.go:~250`) | `matching_queue.rs:~316` | Same. |
| `getIsolationGroupForTask` order and leak causes (`task_list_manager.go:903-952`) | `task_isolation.rs:55-100` | Same order: error, drained, unknown, no recent poller, partition change, expired. |
| `getDispatchTimeout` (`task_reader.go:~395`) | `task_isolation.rs dispatch_timeout` | Same, including the MinInt64 conversion quirk. |
| `getNumberOfPartitions` | `matching_scaler.rs:92` | Same. |
| `Empty` in DescribeTaskList = `ack == maxReadLevel` | `matching_queue.rs:682` | Same. |
| StickyTaskConditionFailedError mapping (`task_writer.go:handleErr`) | `matching_writer.rs:259` | Same message; the Rust error is `Status::internal`. See F6. |

## Findings (prioritized)

Severity key: SDK = SDK-visible; COR = correctness; MIN = minor; UNK = needs verification.

| # | Sev | Go (file:line) | Rust (file:line) | Difference | Recommended fix |
|---|---|---|---|---|---|
| F1 | COR/UNK | `task_reader.go getTasksPump` (ack timer branch): `persistAckLevel` runs inside the reader pump, and a `ConditionFailedError` calls `signalFatal()` and exits the pump. | Ack persistence is done by the registry maintenance loop (`matching_registry.rs:1983-2046 persist_acks`). The reader pump (`matching_reader.rs:~700`) only re-reads on the ack tick. `matching_queue.rs:243-262` stops the queue and calls the observer on ConditionFailed. | The behavior is equivalent in effect, but the ack-tick interval comes from two places (the reader's `update_ack_interval` and the registry's `with_update_ack_interval`). Whether both read the same live dynamic property is **unknown**. | Add a test that changes `matching.updateAckInterval` at runtime and asserts both timers follow it. |
| F2 | COR | `task_reader.go Stop`: `persistAckLevel`, then `taskGC.RunNow`, then `stopWg.Wait` | `matching_registry.rs:140-141` calls `persist_final_ack` and `run_garbage_collection(true)` | Equivalent. The only difference is that Go's final persist reports ConditionFailed. Rust deliberately does not report it (`matching_queue.rs:243` comment), and Go's `Stop` only logs it too. | No action. |
| F3 | SDK/COR | `task_list_manager.go:AddTask` standby path: persists via `taskWriter.appendTask`, then signals the reader, and returns `syncMatch=false` with the error. A forwarded add to a standby domain returns `errRemoteSyncMatchFailed`. | The standby path lives in `matching_standby.rs`. **Not compared in this pass** (unknown). | Unknown. | Audit `matching_standby.rs` against `task_list_manager.go:578-601` and `task_completer.go`. |
| F4 | MIN | `task_writer.go:appendTask` wraps a timeout as `fmt.Errorf("failed to receive a success response ... %w")`, and a full queue returns `createServiceBusyError(...)`. | `matching_writer_actor.rs:18-19,99-136` maps a full queue to `ServiceBusy` and a response timeout to the equivalent error. | A full-queue timeout maps to the same ServiceBusy. The Go response timeout is a plain error, which the handler turns into Internal. **Not verified** that Rust turns the response timeout into an Internal status rather than a ServiceBusy/Unavailable status. | Check the `WriterError` to `Status` mapping in `matching_writer.rs`. |
| F5 | MIN | `task_writer.go:getWriteBatch` loops `MaxTaskBatchSize` extra times. | `matching_writer_actor.rs:189` takes `batch_size().max(0)` extra items. The default is 100 (`matching_writer.rs:26`). | Same semantics. | No action. |
| F6 | MIN | Sticky ConditionFailed becomes an InternalServiceError with message `StickyTaskConditionFailedError`. For non-sticky lists Go returns the raw ConditionFailedError. | `matching_writer.rs:259` returns `Status::internal("StickyTaskConditionFailedError")` for sticky lists. The non-sticky ConditionFailed gets `Status::internal(error.to_string())`. | **Unknown** whether the Go RPC layer converts the non-sticky ConditionFailedError into the same wire error code. Rust marks the sticky case retryable (`matching_writer.rs:250`); Go's retry behavior for that case is also **unknown** (it lives in the history client). | Check the Go matching handler's error translation for `ConditionFailedError`. |
| F7 | MIN | `registry.append` and `append_in_domain` (legacy paths) | `matching_registry.rs:449-480` map every queue error to `Status::unavailable`. | The Go path returns the raw error. These two functions may be used only by tests; I did not trace callers. | Trace the callers. If they serve RPCs, map errors like `matching_writer.rs` does. |
| F8 | COR | `adaptive_scaler.go:232` `calculateWritePartitionCount` emits gauges and counters (`PartitionUpscale`, `PartitionDownscale`, threshold gauges). | `matching_scaler.rs:101` is documented "without scope gauges". `with_metrics` exists at `:538`. | Metrics parity is **unknown**. Behavior is the same. | Confirm that the upscale and downscale counters are emitted from `tick`. |
| F9 | MIN | `task_reader.go getTaskBatch`: loops up to 10 ranges per turn and sets `isReadBatchDone = readLevel == maxReadLevel`. | `matching_queue.rs:131-175 scan_batch` and `block`, with a fixture test. | Covered by the Go fixtures (`matching_queue_original_go_reader_fixtures`). I did not read `scan_batch`. | Unknown beyond the fixtures. |
| F10 | COR | `task_reader.go dispatchSingleTaskFromBuffer` retries forever on an unknown dispatch error, with `Gosched()` only on throttle. | `matching_reader.rs:~600-640`: `Throttled \| TimedOut` leads to `yield_now` and continue. | Equivalent for throttle and timeout. Go also has distinct handling for `errTaskNotStarted` and `errWaitTimeNotReachedForEntityNotExists`. The Rust standby check treats `Err(_)` as a plain retry. Go's unknown-error branch logs an error and increments `BufferUnknownTaskDispatchError`; Rust logs and counts nothing for `Err`. | Add the `BufferUnknownTaskDispatchError` counter and an error log for the `Err` branch (metrics parity only). |
| F11 | MIN | `task_list_manager.go:DescribeTaskList` | `grpc_matching.rs:3380-3450` | Same fields. Rust reads `system.allIsolationGroups` and the poller window per call, as Go does. | No action. |
| F12 | UNK | `matcher.go` (Poll, pollOrForward, pollLocalWait, pollNonBlocking, Offer, OfferOrTimeout, OfferQuery, ratelimit) | `matching_dispatch.rs`, `matching_blocking.rs`, `matching_sync.rs`, `matching_forward.rs` | **Not audited.** This is the largest remaining risk. It governs empty polls, the local wait time, sync-match timing and the query path. | Do a dedicated pass on `matcher.go` lines 150-280 and 438-800 against `matching_dispatch.rs` and `matching_sync.rs`. |
| F13 | UNK | `poller` package (history, `GetCountByIsolationGroup`, `HasPollerFromIsolationGroupAfter`) | `cadence-common/src/poller.rs`, used at `grpc_matching.rs:3420` | **Not audited** (poller TTL, eviction and `CancelPoll` semantics). | Compare the Go `poller` package with `poller.rs`. |
| F14 | UNK | `task_list_limiter.go` (`ReportLimit`, `tryUpdate`, burst calculation) | `matching_limiter.rs` | **Not audited.** | Compare against the Go limiter tests. |
| F15 | UNK | `shard_processor*.go`, `task_list_registry.go` | `matching_shard_processor.rs`, `matching_registry.rs` | **Not audited.** | Dedicated pass. |
| F16 | UNK | `isolation_balancer.go` (425 lines) | `matching_scaler.rs:182-530` | **Not compared.** The function structure maps one-to-one by name. | Run the Go `isolation_balancer_test.go` cases as fixtures if that has not been done. |

## Top 10 summary

1. F12: matcher.go (Poll, Offer, sync-match timing, empty poll, query) not audited; highest risk.
2. F3: standby add-task and task completer path not compared.
3. F13: poller history semantics not audited.
4. F14: task list limiter not audited.
5. F15: shard processor and registry not audited.
6. F16: isolation balancer not compared.
7. F6: non-sticky ConditionFailed wire error code unknown.
8. F4: writer response-timeout status mapping unverified.
9. F10: missing unknown-dispatch-error metric and log in the Rust reader.
10. F1: ack-interval live-property wiring unverified; F8 scaler metrics parity unverified.

Core GC, writer, liveness, reader and isolation-policy logic matched Go in every area I read.
