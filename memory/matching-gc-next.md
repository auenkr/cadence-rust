# Matching task garbage collector continuation (2026-10-16)

Prior pushed checkpoint: 0d0952ff8 (per-owner ack pump and stopped-owner cleanup).
New matching_gc::TaskGarbageCollector ports Go task_gc.go: nonblocking single
collector, cursor-based backlog estimate, batch/time thresholds, strict time
boundary, attempt timestamp updated before deletion, repeated full batches until
a short batch, and cursor advancement only after successful drain. Failure after
a full batch leaves the cursor unchanged; small retries observe the cooldown.
Force ignores time but still requires progress. Default batch=100, interval=1s
from original Go config; overrides can be supplied to queue GC API.

Shared collector is owned by DurableTaskQueue. Production dispatcher now calls
thresholded GC after completion/rewrite instead of an unconditional 1000-row
delete. Registry unload, shutdown and stopped-owner replacement force drainage.
Added persist_final_ack: a stopped writer does not suppress the final reader ack
write, but SQL range fencing remains mandatory and stopped handles stay stopped.
Stopped-writer recovery test now checks the persisted ack and removed rows.
Maintenance shutdown test checks physical task deletion too.

Go differential harness: tests/run-matching-gc-compat.py, Go overlay and 14 JSON
cases. Actual original-Go collector ran against existing mock TaskManager. Covers
no progress, first drain, size equality, full-batch looping, small backlog timing,
force, error before/after progress, busy collector, sparse levels, regression,
future timestamps and changed batch size. /tmp/cadence-gc-differential.exit = 0.
Rust adds deterministic strict time equality/+1ns and error cooldown assertions.
Raw collect_garbage(limit) is retained for storage tests; native runtime paths use
the thresholded shared collector.

Full verification PASSED: 333 workspace tests, strict all-target workspace lint,
all 24 compatibility stages. /tmp/cadence-gc-verified.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-gc-verified/results.json.
Includes 14-case actual-Go GC differential, SQL/blob/queue/poller differentials,
standalone SDK and actual Go Frontend/History/Worker + Rust Matching with all
seven expanded SDK scenarios. Strengthened stopped-writer and shutdown row-drain
assertions ran in this frozen-source workspace pass.
The earlier matching-gc-final attempt failed to compile because the shared queue
API was edited while Cargo compiled the server against its earlier library build.
It is superseded by this complete fresh run, not functional failure evidence.

Remaining lifecycle work: dynamic per-task-list GC/ack configuration, strict
writer-stop/unregister ordering vs concurrent RPCs, in-flight shutdown handling,
immediate fatal-owner watchers, idle unload and production membership. Current
registry holds its map lock during cleanup; Go unregisters before remaining
teardown. These are still parity gaps, not a completed service gate. Query
delivery, sync matching/forwarding and remaining transport/controller work follow.
