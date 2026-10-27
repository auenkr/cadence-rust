# Matching cold-domain metric probe — 2026-10-27

## Finding

The regression in `rust/crates/cadence-matching/src/matching_cold_domain_metric_tests.rs`
does cover the handler ordering at the Rust API boundary: it empties the cache,
publishes a domain afterward, invokes the real
`MatchingApi::add_decision_task` trait method, and checks that the
`CADENCE_REQUESTS_PER_TASK_LIST` event uses the canonical name. Its
`LateDomainSource` and `RecordingMetricSink` are test doubles, so it does not
exercise the configured persistence adapter or production exporter.

The mixed production probe already starts Rust Matching with its selected
production exporter (`tests/run-go-service-baseline.py --matching-exporter
prometheus|statsd|m3`), captures emitted output, and talks to its gRPC endpoint
through the probe's real gRPC clients. `tests/matching-service-compat.cjs`
registers an SDK domain through Go Frontend after the cluster has started.
However, the existing exporter validator only requires successful SDK metrics
for that established domain. No existing scenario asserts a newly registered
domain's canonical metric tag on a quota-rejected direct Matching call.

In `rust/crates/cadence-matching/src/grpc_matching.rs`, `add_decision_task`
resolves `metric_domain_name` and calls `matching_request`; the latter encloses
`check_rpc_quota`. The order can therefore be tested via the production
exporter and a `ResourceExhausted` result. The quota rejection counter itself
(`matching_rpc_quota_rejections`) is distinct from the per-task-list request
metric; assert the latter's canonical `domain` tag and separately assert the
RPC is rejected.

## Why no probe change was made

The existing Actions mixed-gate inputs do not choose the exporter (the generic
baseline runner does), and its generated dynamic config has no exact-domain
zero-RPS entry. A non-vacuous test needs the same generated domain name in the
Matching startup dynamic config, the late Frontend registration, the direct
Matching RPC, and the production metric assertion. Changing only the probe
would leave quota rejection unconfigured; changing only config would not prove
that the emitted metric belongs to the rejected RPC. A partial change could
pass by observing unrelated metrics. No mixed gate was dispatched and no local
runtime/build/test was run under this research task.

## Parent execution recipe

Extend an existing production lane (no topology change) as follows:

1. Before starting Rust Matching, choose a per-run domain name and inject an
   exact `matching.domainrps` dynamic-config filter for that name with value
   `0` into the generated `rust-dynamic-config.yaml`. Use the same value in the
   probe, and configure the existing Prometheus exporter for that lane.
2. After Matching is ready, have the probe register that domain through the
   existing Go Frontend `DomainAPI.RegisterDomain`, then fetch its UUID through
   `DescribeDomain`. This ensures the initial Matching cache scan predates the
   registration.
3. Send a direct `MatchingAPI.AddDecisionTask` gRPC request for that UUID with
   a valid task-list name and ordinary request metadata. Assert gRPC
   `RESOURCE_EXHAUSTED` / `ServiceBusyError` from the configured domain quota.
4. In the existing Prometheus scrape capture, require a positive
   `cadence_requests_per_tl` sample with `operation="AddDecisionTask"`,
   `transport="grpc"`, and `domain` equal to the registered canonical name.
   Keep the assertion tied to the probe's unique task list/domain so prior SDK
   traffic cannot satisfy it. Record the rejected status beside the matching
   metric sample in the probe result.
5. Run the existing public-repo `matching-mixed-gate.yml` on `ubuntu-latest`
   with the existing MySQL two-node topology and Prometheus enabled, then retain
   run URL, exact head SHA, conclusion, and Rust binary SHA256. A pass proves
   late registration/read-through, handler metric-before-quota ordering, and
   the real exporter on that lane; it does not independently replace the
   remaining Matching acceptance lanes.

This is an execution recipe, not runtime evidence. The current test and probe
evidence boundaries above remain in force until the extension is implemented
and run on Actions.
