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
  `sync_forward_task_throttle_errors_per_tl`.
* `matcher.go:339-419` emits async local/forward counters, attempt timer and
  integer histogram pairs, latency timer and duration histogram pairs, and
  forward throttle errors. Rust `matching_dispatch.rs` has delivery and poll
  outcomes but does not yet emit these matcher/reader attempt families.
* `matcher.go:444` emits invalid-isolation-group count.
* `matcher.go:465-639` emits the local/forward/after-forward-failure poll
  timing pairs described above; Rust builders exist, but dispatch wiring is
  open.
* `task_reader.go:231,273,395` updates backlog, task-count, and task-lag
  gauges. The durable queue exposes `levels().2` for backlog, but Rust's
  `matching_reader.rs` currently has no metrics context or gauge emissions.
* `task_reader.go:365,490` emits expired-task counters; `:503-506` emits async
  match latency timer/histogram; `:537` emits async dispatch timeout; `:544`
  emits buffer throttle; and `:568` emits unknown dispatch error. None are yet
  emitted by Rust's `matching_reader.rs`.

Metric definitions and rollups are in `go/common/metrics/defs.go` around
3060-3134. Rust source formatting and `git diff --check` pass; no build or
mixed Go/Rust runtime test was run in this lane.
