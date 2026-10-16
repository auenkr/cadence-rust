# Native Matching poller state continuation (2026-10-16)

Prior RPC/mixed SDK checkpoint ad9ebf44a is pushed. Added common::poller::Manager
with original-Go identity history, MRU ordering, 5000-entry capacity, strict
five-minute TTL boundary, start/end timestamps, outstanding IDs and isolation
group recency/counts. Caller owns cancellation and synchronization. Go cache
Size does not eagerly expire entries here (ActivelyEvict=false); ListInfo and
group iteration do. Duplicate identity/end ordering is preserved.

Differential harness: tests/run-matching-poller-compat.py and test-only original-Go
poller overlay. Ten reference snapshots passed against both implementations:
/tmp/cadence-poller-differential.exit = 0. Includes empty identities/IDs, end of
unknown ID, cancellation retaining outstanding state, TTL equality/+1ns, capacity.

Registry owners/handles now carry their own poller manager. Native poll handlers
record worker identity and rate, Dispatcher wait-cycle RAII updates history before the History start RPC,
and starts a fresh wait after failed/terminal deliveries. It is not scoped to
the entire RPC: that would falsely keep pollers outstanding during History retry.
Activity metadata rate is retained; actual dispatch throttling remains unported.
Native DescribeTaskList returns these pollers and queue read/ack/backlog/block/
empty fields. Controller-specific QPS/isolation/limiter fields are still partial.
It retains existing embedded partition config metadata conversion for now;
that configuration still needs transfer into the native queue owner.

Added a dispatcher test asserting the poll is no longer outstanding inside
History delivery and that retries create another completed wait cycle. Initial
full 23-stage runner started before this timing correction; do not confuse it
with final verification of this added test.

Focused server test/check: /tmp/cadence-poller-rpc-tests.log and .exit; clippy
/tmp/cadence-poller-rpc-clippy.log. Final corrected dispatcher/RPC checks PASSED: all 16 focused Matching tests and
strict all-target workspace clippy, /tmp/cadence-poller-timing-tests.exit = 0.
Full accumulated 23-stage run PASSED: /tmp/cadence-matching-poller-full.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-poller-final-20261016/results.json.
Its live stages use the corrected wait-cycle implementation. The mixed probe
also asserts native poller history and ID-block status through Go Frontend, and
runs all seven expanded existing SDK scenarios. Final post-correction workspace
rerun PASSED all 327 tests: /tmp/cadence-poller-workspace-final.log;
/tmp/cadence-poller-workspace-final.exit = 0.
Full combined runner now includes matching-poller-differential (23 stages).
Next run must use cached original Go CLI and test-only Fx routing binary as
documented in matching-rpc-next.md; avoid simultaneous heavy links.

Remaining service gates are unchanged: sync matching, forwarding, queries,
configured maintenance/ownership/unload, production domain/config/membership,
rate/isolation/partition scaling, semaphore and internal Thrift transport.
