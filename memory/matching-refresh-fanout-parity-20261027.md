# Matching partition-config refresh fanout parity (2026-10-27)

## Finding

This source pass found no concrete Go/Rust divergence in partition-config
refresh fanout, per-RPC deadline propagation, or task-list initialization that
explains the diagnostic activity decision stall. No runtime or fixture change
is justified by the evidence so far.

## Source map

- Go `task_list_manager.go:425-449`: a refresh waits for manager startup;
  pushed configs replace only older cache versions, while a nil config reloads
  the root config from persistence. `UpdateTaskListPartitionConfig` likewise
  waits for startup, persists the new config, then waits for notifications.
- Go `task_list_manager.go:500-539`: notification targets the union of old and
  new non-root **read** partitions. It starts one `errgroup` goroutine per
  partition, passes the caller context to every RPC, waits for the group, logs
  errors, and does not return notification failures to the update caller.
- Go `task_list_manager.go:66,291-304`: on root manager startup, initial
  notifications run asynchronously with a five-second context timeout; they
  do not hold up `Start` returning.
- Rust `matching_registry.rs:1968-2030`: the durable update builds the same
  union of old/new non-root read partitions, updates already-loaded sibling
  caches, then awaits `notify_read_partitions`.
- Rust `matching_registry.rs:2032-2121`: one Tokio `JoinSet` task is spawned
  per partition. Each resolves its owner and issues the RPC concurrently,
  forwarding the caller deadline (and enforcing it locally). Each error is
  logged; notification errors do not fail the update RPC.
- Rust `matching_registry.rs` around `get` (starting near line 1010) and
  `grpc_matching.rs:1421-1452`: durable queue lookup is keyed by task list;
  it checks ownership and awaits registry initialization before returning the
  queue handle. The refresh handler then applies the pushed config or reloads
  it from persistence in `grpc_matching.rs:3388-3445`.
- Rust `matching_registry.rs` around lines 1580-1610: root startup schedules
  initial fanout in a detached task with the same five-second deadline.

Thus the update fanout is awaited in both versions, both use per-partition
concurrency and caller cancellation, and both treat per-partition failures as
best-effort. Startup fanout is detached and bounded at five seconds in both.
The Rust handler's queue lookup/initialization is synchronous with the refresh
RPC, matching Go's `getOrCreateTaskListManager` plus `startWG.Wait()` before
refresh. The mechanisms are not byte-for-byte identical, but this inspection
does not show serialization of the 383-partition fanout or a new blocking
startup dependency in Rust.

## Runtime evidence and limits

The immutable diagnostic artifacts for Actions run
`40469125507` are under
`/mnt/storage/service/ci-evidence/40469125507/matching-production-adaptive/`;
the source-side summary is `memory/evidence/matching-adaptive-trace-40469125507/README.md`.
The SDK run's activity-timeout workflow has a schedule-7 task observed at
partition 47 and enqueued, followed by repeated forward attempts at partition
2 without a token trace before the SDK wait expired. Retry-only and all 24
initial load workflows completed; the retry-only workflow completed in
37.769s, and the full suite's workflow-task retry completed in 23.434s.

`rust-server-after-graceful-drain.log` contains many partition refresh RPC
`DeadlineExceeded` warnings against a 383-partition config. Those warnings
establish that some refresh calls exceeded their propagated deadline; they do
not identify whether the cause was RPC transport, owner resolution, per-queue
initialization, or server scheduling. The source shows that root startup
refreshes use the same five-second best-effort timeout in Go and Rust. The
artifacts do not include a Go-vs-Rust per-partition latency/concurrency
comparison, nor a complete owner/cache convergence snapshot, so timeout counts
and partition count cannot prove a regression or causal link to the stalled
decision.

Do not change the 60-second SDK deadline, 24-workflow completion checks, or
routing assertions based on this pass. A follow-up proving a runtime difference
needs traces correlating each refresh's start/end, owner initialization, and
subsequent dispatch for the affected partitions, ideally compared with Go under
the same partition set.

## Validation

Read-only source and artifact inspection only. Per task instructions, no local
Cargo, Go, or formatting commands were run. No focused regression or mixed
gate was dispatched because no code divergence was identified; the parent owns
the active mixed gate.
