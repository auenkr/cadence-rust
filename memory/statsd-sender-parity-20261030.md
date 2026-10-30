# Matching StatsD sender parity correction (2026-10-30)

Reference: Go's pinned `cactus/go-statsd-client` `sender_buffered.go` at
`/home/auenkr/go/pkg/mod/github.com/cactus/go-statsd-client/statsd@v0.0.0-20191106001114-12b4e2b38748/sender_buffered.go`.
The Rust change is scoped to `rust/crates/cadence-service-core/src/matching_metrics_statsd.rs`.

The Go sender swaps a full packet while holding its buffer mutex, enqueues it
into a 32-packet bounded FIFO channel, and has a separate goroutine write and
release each packet. A failed UDP write does not put the packet back into the
buffer. Rust now follows that queue and sender-worker model. Existing metric
semantics remain in the caller: timers enqueue directly, and counters/gauges
retain their reporting-interval aggregation. The byte threshold checks and
newline packet bytes are unchanged.

The queue send may block when all 32 slots are occupied, matching Go's
backpressure at the packet swap; the sender never needs the buffer mutex, so it
can continue draining. Explicit Rust `flush()` waits for its queued packet and
returns that write result; when its buffer is empty it uses a FIFO barrier so
prior packets finish before return. Periodic flush remains asynchronous.
Shutdown stops and joins the periodic and reporting workers, moves the final
buffer into the queue, appends `Shutdown`, and joins the sender worker, which
drains all preceding packets. Automatic packet failures are discarded and do
not stop later sends.

Added deterministic source test `sender_drops_failed_packet_and_continues_in_fifo_order`,
which injects a write failure and verifies that a subsequent packet is
attempted successfully in order. The test was not run per instruction. Only
source inspection and `git diff --check` are permitted locally; no Cargo,
runtime, or CI validation was performed. This note does not establish Matching
readiness or broader StatsD transport parity.
