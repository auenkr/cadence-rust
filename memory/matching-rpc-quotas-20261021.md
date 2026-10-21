# Matching user/worker RPC quotas

Source implementation added after audit `359c5bb09`. This was source-only:
no Cargo, Go, build, service, container, or mixed-cluster command was run.

## Go contract ported

Pinned Go source is `go/service/matching/handler/handler.go:54-83,145-550`,
`go/common/quotas/multistageratelimiter.go:73-110`,
`go/common/quotas/dynamicratelimiter.go:39-125`, and
`go/common/dynamicconfig/quotas/fallbackdynamicratelimiter.go:30-49`.

- Host user rate key `matching.rps`, default 1200; host worker key
  `matching.workerrps`, default `UnlimitedRPS` (`math.MaxInt32`). Domain keys
  are `matching.domainrps` and `matching.domainworkerrps`, default 0. A domain
  value greater than zero is used; zero or a negative value falls back to the
  corresponding host value.
- Host and domain buckets are independent for user and worker classes.
  Domain buckets are cached per class/domain for the process lifetime. Dynamic
  values are sampled at bucket creation and refreshed after the Go five second
  limiter TTL, using the same `rate.Limiter` token/burst implementation already
  ported in `cadence-common`.
- Admission reserves the domain token first, then attempts the host token. A
  domain reservation is returned when host admission fails. Calls are never
  queued on these handler quotas.
- Hard reject calls: `AddActivityTask`, `AddDecisionTask`, both poll calls;
  `QueryWorkflow`, `DescribeTaskList`, `ListTaskListPartitions`,
  `GetTaskListsByDomain`, `UpdateTaskListPartitionConfig`, `AcquireSemaphore`.
- Count-only calls still invoke quota admission and consume tokens when allowed,
  but ignore a denial: `RespondQueryTaskCompleted`, `CancelOutstandingPoll`,
  `RefreshTaskListPartitionConfig`. `Health` is not quota-limited.
- A hard rejection returns gRPC `RESOURCE_EXHAUSTED` with typed
  `ServiceBusyError` detail and message `Matching host rps exceeded`. The
  Thrift/TChannel adapter calls the same Matching trait methods, so it receives
  the same decision once; its existing application exception conversion maps
  that status to the declared `ServiceBusyError` exception. No retry delay is
  supplied in Go's error, and none is added here.
- Quota checks resolve UUIDs through `Matching::domain_name`, the same
  cache-backed resolver used by its handlers. A lookup failure falls back to
  the host bucket, matching Go's empty domain name behavior; the normal handler
  can then return its domain lookup error after quota accounting.

Implementation is in `rust/crates/cadence-server/src/matching_quota.rs`,
shared entry guards in `grpc_matching.rs`, and production trait-path regression
in `matching_rpc_tests.rs`. The helper is in the Matching method path (not in
the wire adapters), which prevents gRPC/TChannel double charging.

The Rust server currently has no general tagged metrics registry/exporter.
Hard rejects emit a `QuotaRejection` to the injectable `QuotaMetricSink`,
including method, user/worker class, and resolved domain; count-only denials do
not emit rejection events. `Matching::new_with_quota_metric_sink` exposes the
injection point. The default constructor currently installs a no-op sink
because there is no production metrics registry to bind. Wiring that sink to a
production metrics exporter remains pending service-wide metrics
infrastructure, and must be completed before claiming metric parity.

## Queued validation after telemetry releases the build lane

Run from `/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider`:

```sh
cargo test -p cadence-server matching_quota -- --nocapture
cargo test -p cadence-server matching_rpc_quotas -- --nocapture
cd go && go test ./service/matching/handler -run 'TestHandlerSuite|TestHandlerAcquireSemaphore' -count=1
```

Then build the `cadence-server` binary from this commit and run the prepared
mixed service gate in both outbound modes with explicit quota assertions added
to the driver: Go Frontend/History/Worker, Rust Matching only; configure
`matching.rps=1`, `matching.domainrps=1` for one named domain,
`matching.workerrps=1`, and `matching.domainworkerrps=1` for that domain. Verify
one admitted and then one rejected hard RPC per class, an independent domain
and independent opposite class, each count-only RPC remaining successful after
bucket exhaustion, identical typed ServiceBusy mapping over gRPC and TChannel,
no duplicate charge per adapter invocation, and captured rejection sink events.
Preserve request/response status, typed detail, RPC method, resolved domain,
and sink event in artifacts. Run existing harness entry point:

```sh
python3 tests/run-go-service-baseline.py --rust-matching --matching-restart --history-transport grpc --rust-production-binary /absolute/path/to/cadence-server --artifacts-dir /absolute/path/to/quota-mixed-grpc
python3 tests/run-go-service-baseline.py --rust-matching --matching-restart --history-transport tchannel --rust-production-binary /absolute/path/to/cadence-server --artifacts-dir /absolute/path/to/quota-mixed-tchannel
```

The current harness does not itself generate these quota pressure requests;
add that driver assertion before treating the mixed gate as quota evidence.
Do not treat source regressions or existing SDK/restart evidence as validation
of the live quota behavior.
