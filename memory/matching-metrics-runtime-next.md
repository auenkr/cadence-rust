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
