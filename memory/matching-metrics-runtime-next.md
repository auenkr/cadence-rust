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

The three latency builders are not yet connected to dispatcher timing. Their
Go emit sites are `tasklist/matcher.go:465,494,527,548` for local poll match,
`:590` for successful forwarded poll, and `:617,638` for local matching after
forward failure; each writes both timer and `_ns` histogram. Rust's
`TaskDispatcher::delivery_loop` has the decision point where a claimed task can
be classified, but needs a poll-start timestamp that survives forwarding and
is retained through queue waits. Remaining telemetry gaps to audit against
`matcher.go` and `task_reader.go` include matcher timing branches, dispatch
attempts/latency, throttling counters, and queue backlog gauge
(`queue.levels().2`). These remain source-only findings; no build or mixed
Go/Rust runtime test was run in this lane.
