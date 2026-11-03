# Frontend wrapper layer: production metrics wiring (2026-11-03)

Branch `frontend-wrappers`, code SHA 325c84b58.
- Added `wrappers/metrics_sink.rs` (`SinkMetrics`): Metered-wrapper `Metrics` seam -> shared `MetricSink` (StatsD/M3/Prometheus from Matching runtime). Emits `cadence_requests`, `cadence_latency` (timer), `cadence_latency_ns` (histogram), typed error counters, tagged `operation` + request tags + `domain`.
- `Metrics::increment_error(op, tags, counter)` added so failures carry operation/domain tags (default delegates to `increment`).
- `MatchingMetricsRuntime::new_for_service` (cadence_service tag = cadence-frontend); `build_workflow_api` builds the sink from `services.frontend.metrics`; empty/absent reporter = noop like Go.
- Unit tests: `wrappers::metrics_sink::tests::*`.
- CI: package cadence-frontend https://github.com/auenkr/cadence-rust/actions/runs/41549557016 success; full https://github.com/auenkr/cadence-rust/actions/runs/41550204507 success (head 325c84b58).

Not done (audit rows unchanged): no mixed emitted-metric name/tag comparison against Go; cluster-redirection (policy+dispatch exist and are unit-tested, but production install needs a domain-cache request resolver for ~40 request shapes, an ActiveClusterManager and a remote Frontend client, none of which exist); config-driven authorizer (only Nop installed; no Go authorizer config/OAuth port); versioncheck already installed in production.
