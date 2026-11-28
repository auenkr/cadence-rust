# perf-unpaced-c1-gate (2026-11-28): `wait_for_task_writes` vs Go — parity confirmed, no fix

Base: origin/master. Inputs: `perf-unpaced-c1-limiter-20261126.md` (Rust-Go p90 +27.0% [+10.2,+46.0], p95 +0.6% [-3.1,+2.8], 16 runs) and `perf-load-bursts-20261126.md`.

## Line-by-line comparison
Rust:
- `queuev2/queue_base.rs` `process_new_tasks`: for immediate queues, `shard.wait_for_task_writes()` (= lock+drop of `task_write_gate`, `shard_adapters.rs`), then `update_if_needed_and_get_queue_max_read_level`.
- Writers (`engine_start.rs` create, `ndc_context_hooks.rs` update) hold `task_write_gate` from task-ID allocation through the DB write, `update_max_read_level` and notify.
- Timer queue: `update_scheduled_task_max_read_level` uses `try_lock` and returns the current level if a write is in flight (it never waits).

Go (`go/service/history/shard/context.go`):
- `UpdateWorkflowExecution` / `CreateWorkflowExecution` take `s.Lock()` (L774) and hold it through `allocateTaskIDsLocked` (L788-797), `executionManager.UpdateWorkflowExecution` (L814, the DB write) and `updateMaxReadLevelLocked` (L818).
- `processNewTasks` → `UpdateIfNeededAndGetQueueMaxReadLevel` → `getImmediateTaskMaxReadLevel` takes `s.RLock()` (L255), which **blocks while a writer holds the lock, i.e. for the whole in-flight DB write**. `sync.RWMutex` also blocks new readers once a writer is waiting, like the FIFO tokio Mutex.
- Scheduled: `updateScheduledTaskMaxReadLevel` takes `s.Lock()` (L261). That blocks in Go, but Rust's `try_lock` fallback returns sooner and never adds a wait.

## Verdict
Rust waits at the same point and for the same span (until the in-flight write finishes) as Go. There is no extra serialization and no timer granularity: the gate is a mutex with no sleep or poll. The "no Go equivalent" note in perf-load-bursts was wrong; the Go equivalent is the shard RWMutex. **No program change** (any change would break parity and read-level correctness).

Benchmark: not run, because there is no code change to measure (same policy as perf-load-bursts). The before numbers stay as in `perf-unpaced-c1-limiter-20261126.md`: p90 +27.0% [+10.2,+46.0], p95 +0.6% [-3.1,+2.8], Rust `cadence-server` sha256 e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725.

## Also checked, not the ~100 ms plateau source
- Matching `matching_dispatch_native.rs` / `matching_dispatch.rs` delivery loop `sleep(100ms)` fallback: it runs only on the reader-less path. With a reader, `claim` blocks on `reader.poll_group` and the sync matcher, and `wake` is enabled before the claim, so no wakeup is missed.
- `min_timeout.rs` 100 ms sleep: test-only.

## Next suggestion
The constant ~98-100 ms Rust p90/p95 plateau is still unexplained. Trace per-request phase timestamps (frontend start → history transfer load → matching add → poll return → respond) in a c1 run to find which hop holds ~100 ms. Candidates are the SDK/worker poll cadence vs matching long-poll handoff, and the history transfer-queue max-poll/notify path.
