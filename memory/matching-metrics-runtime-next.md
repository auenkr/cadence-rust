# Matching metrics runtime wiring checkpoint

The Rust Matching metrics sinks now have a production configuration path through
`services.matching.metrics`, matching Go's service-scoped configuration. The
runtime selects one non-noop reporter with Go's M3 → StatsD → Prometheus order.
Go's edge case is preserved: empty StatsD produces a no-op scope, so it can be
followed by Prometheus; M3 plus any StatsD block conflicts even when StatsD has
an empty host. The selected sink is shared by Matching and the outbound History
client. StatsD receives the global reporting interval; its own prefix and packet
flush options remain separate. Exporter ownership retains the Prometheus
listener for graceful shutdown. Shutdown flushes buffered StatsD/M3 output.
Matching, its retained QueueRegistry, and the outbound History client share the
same sink.

Prometheus with an explicit `listenAddress` starts its configured listener. Go
with an empty address registers the handler on `http.DefaultServeMux`; the Go
service's PProf config (`services.matching.pprof.host/port`) serves that mux.
Rust routes address-less Prometheus to the matching PProf host/port when that
port is configured. Without a PProf port, no listener is opened. Rust does not
yet expose Go's `/debug/pprof` handlers, so that part of PProf parity remains
open. Prometheus listener bind errors are reported as warnings, matching Go's
`OnError` callback in `common/config.Metrics.newPrometheusScope`; they do not
fail service startup.

The Go source oracle for poll direction and isolation is
`go/service/matching/handler/engine_integration_test.go`:

```sh
cd go
go test ./service/matching/handler -run '^TestMatchingPollMetricOracle$' -count=1
go test ./common/config -run '^TestMetricsSuite$' -count=1
```

These commands were not run in the source-only lane. Rust formatting and
`git diff --check` passed; Cargo checks and mixed Go/Rust exporter validation
remain for the next build lane.

## Matcher event inventory (source-only checkpoint)

Rust now has event builders and tests for `poller_count_per_tl` and the
`poll_local_match_latency_per_tl`, `poll_forward_match_latency_per_tl`, and
`poll_local_match_after_forward_failed_latency_per_tl` timer/histogram pairs.
The poller count is emitted from poll history start/end only for nonempty
poller identities. It uses historical identity cardinality with five-minute
TTL eviction matching Go `poller/manager.go:GetCount` and LRU `Size` behavior.
Go invokes the gauge callback from history updates in
`poller/manager.go:StartPoll` and `EndPoll`; task-list manager wires that
callback to `task_list_manager.go:213`.

The three latency builders are now connected to dispatcher timing using a
poll-start timestamp carried through forward retries. Go emit sites are
`tasklist/matcher.go:465,494,527,548` for local poll match, `:590` for a
successful forwarded poll, and `:617,638` for local matching after forward
failure; each writes both timer and `_ns` histogram. Rust classifies the
successful task claim or successful direct forward. Remaining Go callsites are
now mapped to concrete gaps:

* `matcher.go:155,256` emits `sync_throttle_count_per_tl`; `:231` emits
  `sync_forward_task_throttle_errors_per_tl`. Event builders and sync-match
  latency pairs are now present, with latency emitted on a successful
  in-process sync match. Rust now distinguishes the sync limiter's throttled
  result from a normal unmatched poller and distinguishes a forward request
  token shortage from forward rate limiting; it records the matching throttle
  counters at those outcomes.
* `matcher.go:339-419` emits async local/forward counters, attempt timer and
  integer histogram pairs, latency timer and duration histogram pairs, and
  forward throttle errors. Rust now covers the task-reader async match latency,
  dispatch timeout and buffer throttle events. Async local handoffs now emit
  local counters and latency pairs; retry cases also emit attempt timer and
  integer histogram pairs through Prometheus, StatsD and M3 using Go's
  `Mid1To16k` bounds. Rust currently has no async task-forwarding outcome in the
  reader dispatch path, so forward and after-forward-failure families remain
  unwired. Local attempt counts currently count throttled/timeout retries,
  while Go increments only after a failed task-forward request.
* `matcher.go:444` emits invalid-isolation-group count.
* `matcher.go:465-639` emits the local/forward/after-forward-failure poll
  timing pairs described above; Rust emits these from the claimed task or
  successful direct forward. `forward_poll_latency_per_tl` and its `_ns`
  histogram now wrap both direct remote forwarding (including RPC errors and
  cancellation) and in-process parent-dispatch forwarding, tagged with the
  child poll's task-list context as Go's forwarder does. Invalid nonempty
  isolation groups are counted at poll entry; empty groups remain valid.
* `forwarder.go:134-137,192-195,222-225` emits ForwardTask, ForwardQuery, and
  ForwardPoll latency timer/histogram pairs around actual calls. Poll forwarding
  was wired in `fab15f723`; task forwarding now records direct and in-process
  parent attempts, and query forwarding records direct and in-process attempts
  through the result/error return. Sync-match task/query paths emit their own
  local/forward timer families.
* `task_writer.go:228` increments lease requests and `:232` increments lease
  failures after a terminal renewal failure. Rust now emits the initial task
  list lease request/failure during owner load and observes subsequent range
  renewals through the persistence queue callback. No separate append/write
  metric callsite was found in Go's task writer.
* `adaptive_scaler.go:238-249,254-255,302` emits estimated QPS and partition
  threshold gauges, scale counters and drained-partition counters. Rust now
  emits those from root scaler ticks. Isolation balance now reports rebalance,
  group start/stop polling, group scale changes and group partition gauges from
  the applied state transitions. These changes are source-only and unvalidated.
* `task_reader.go:231,273,395` updates backlog, task-count, and task-lag
  gauges. The durable queue exposes `levels().2` for backlog, but Rust's
  acknowledgement manager's `levels().2` is the same outstanding unacked item
  count as Go `AckManager.GetBacklogCount`. Rust now emits this gauge at reader
  pump turns and wires a service metrics/context through queue creation.
  Rust now emits task-count from the matching store's existing `task_list_size`
  query and task-lag from `max_read_level - ack_level` on each configured ack
  maintenance update, matching Go's task reader timer and `persistAckLevel`.
* `task_reader.go:365,490` emits expired-task counters; `:503-506` emits async
  match latency timer/histogram; `:537` emits async dispatch timeout; `:544`
  emits buffer throttle; and `:568` emits unknown dispatch error. Rust now
  emits expiry at the dispatch-time expiry branch, async-match timing around
  each limiter/offer attempt, and timeout/throttle counters at the matching
  retry outcomes. Rust also counts expiry during initial reader admission and
  expiry while buffered. Unknown dispatch errors still need classification at
  a corresponding terminal dispatch path.

Metric definitions and rollups are in `go/common/metrics/defs.go` around
3060-3134. Rust source formatting and `git diff --check` pass; no build or
mixed Go/Rust runtime test was run in this lane.

## Remaining runtime callsite audit (unvalidated source)

Rust now also emits Go `ForwardedPerTaskListCounter` (`forwarded_per_tl`) for
nonempty `ForwardedFrom` on AddActivityTask, AddDecisionTask,
PollForActivityTask, PollForDecisionTask, and QueryWorkflow. The shared metric
helper applies Go task-list and domain tags, including the sticky/ephemeral
task-list labels. These events were added to the gRPC Matching handlers after
domain resolution and before their core operation, like Go's handler callsites
in `handler/handler.go:167,200,233,273,314`.

The root/non-sticky partition mismatch gauges in `task_list_manager.go:960+`
(`tasklist_read_write_partition_mismatch` and
`tasklist_poller_partition_mismatch`) and task-list partition config
version/read/write gauges at `:372-380,492` still need Rust callsite wiring.

Terminal unknown dispatch error parity remains unimplemented: Go emits from
`task_reader.go:568`; a source scan of Rust `matching_dispatch.rs` found no
equivalent classified terminal error callback yet. No Cargo or Go build/test
was run for this checkpoint; formatting and `git diff --check` are the only
validation permitted in the occupied build lane.

## Additional source-only metric wiring

The task-list limiter now returns the aggregate rate value when Go
`task_list_limiter.go:103-112` would update `rate_limit_per_tl`: an accepted
lower poller rate or a TTL refresh. Poll handling emits this gauge with the
request's domain/task-list context. The limiter preserves the prior report
behavior; the return value only exposes the Go gauge update point.

Task isolation selection now retains Go's leak cause (`error`, `group_drained`,
`group_unknown`, `no_recent_pollers`, `partition_change`, `expired`) alongside
the same selected group and timeout. Matching reader dispatch and buffer
admission emit `task_isolation_leak_per_tl` with isolation-group and leak-cause
tags. `TaskIsolation::select` remains available with its prior return shape;
`select_with_cause` adds the telemetry outcome without changing selection.

Manual task-list partition config persistence failures now emit
`tasklist_partition_update_failed_per_tl` from both durable registry and
legacy-store update paths. Configuration validation errors remain outside the
metric, matching Go's post-validation `UpdateTaskListPartitionConfig` failure
counter.

These changes remain source-only and unvalidated. Rust selection reason and
limiter gauge tests were added but not run. Remaining known gaps include
partition config count/version gauges and mismatch gauges, standby completion
counters, and unknown dispatch error classification. Rust's current
`TaskReader`/`TaskDispatcher` does not expose the same task-dispatch error
outcome as Go's `task_reader.go:568`, so a counter cannot be added without
porting that outcome rather than mapping unrelated queue or History errors.

## Focused validation commands for the next build lane

Run the Rust tests after taking the Cargo lane:

```sh
cargo test -p cadence-common selection_preserves_go_leak_causes
cargo test -p cadence-server report_limit_returns_go_rate_gauge_updates
cargo test -p cadence-server matching_metrics::matcher_metric_tests
```

Run the Go metric source/exporter oracles after taking the Go lane:

```sh
cd go
go test ./service/matching/handler -run '^TestMatchingPollMetricOracle$' -count=1
go test ./common/config -run '^TestMetricsSuite$' -count=1
```

None of these commands has been run for this source checkpoint.
