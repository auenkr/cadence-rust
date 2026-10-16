# Matching queue lifecycle continuation (2026-10-16)

Prior verified checkpoint: 0e7dbae0e (327 workspace tests, 23 compatibility stages).
New work: QueueRegistry removes stopped/fenced owners, while preserving healthy
owners and identity-checked stale teardown. A stopped writer is replaced at the
next get without waiting for an ack timer; unload/shutdown notify dispatcher
waiters. This follows the fatal-reader/writer unregister path in Go manager.

maintain_until provides an explicitly owned shutdown future and fixed configured
interval. Each owner's deadline is based on its own last update, initially load
time, and resets AFTER its persistence operation (including transient errors).
Only due owners are updated; one failure does not stop other owners or the pump.
Final shutdown flushes and stops handles. Mixed Rust Matching fixture now runs
the pump with Go's default 60 seconds instead of flushing a stale startup time.

Final verification PASSED:
- /tmp/cadence-maintenance-final.exit = 0: all 20 focused Matching tests and
  strict workspace all-target clippy. Eight registry tests include periodic DB
  ack observation, independent per-owner deadlines/reset, fenced-owner eviction,
  stopped-writer replacement, shutdown handle rejection and blocked poll release.
- /tmp/cadence-maintenance-mixed.exit = 0:
  /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-maintenance-mixed/results.json.
  Actual Go Frontend/History/Worker + Rust Matching, DescribeTaskList status,
  seven existing SDK scenarios and owned maintenance shutdown all passed.
The previous full 327-test/23-stage checkpoint remains separate; this incremental
change was validated with the focused suite, strict workspace lint and live mixed
probe. Do not present it as a newly repeated full-workspace/full-runner result.

Important remaining lifecycle gaps: config is supplied as one fixed interval,
not yet Go per-domain/task-list dynamic properties. Task GC is still the older
single bounded delete on delivery; Go task_gc.go has cursor, time/size thresholds,
try-lock and RunNow loop draining until n < configured batch size. Go reader.Stop
persists ack, runs GC immediately, waits for reader stop; native shutdown does
not yet implement that full GC/controller sequence. Idle unload, membership
ownership validation and in-flight shutdown coordination remain. Do not claim
complete lifecycle parity. Continue these and query/sync matching gates.
