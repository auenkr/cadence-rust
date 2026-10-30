# Matching gRPC registration source correction (2026-10-30)

Audit scope: exact source commits `4a5da9fb0b9709e40d1117244c1faf82bc60a27f` and `a47e4032d94134b250ce0670cbd5f791d32b285b`, plus retained gate evidence at `/mnt/storage/service/ci-evidence/matching-postgres-41100032910/`.

## Production registration and runtime availability

In `rust/crates/cadence-matching/src/grpc_matching.rs` at `4a5da9f` (same registration in `a47e4032d`), `serve_with_listener_tls_inner` reads `matching.native_service()` at lines 4173-74. When `Some`, lines 4180-86 register generated `cadence_proto::native_service::matching_api_server::MatchingApiServer<SharedNativeMatching>` inside `RawStatusService`. The `else` branch at lines 4187-91 registers the instrumented legacy `m::matching_api_server::MatchingApiServer<Matching>`.

The production native launcher `matching_launch_native.rs::assemble_configured` constructs the native registry and passes its live slot into assembly at lines 46-83. `MatchingRuntime::with_native_queues` attaches the native API to the `Matching` handle with `.with_native_service(service)` at `matching_runtime.rs:350-359`; `with_native_host` invokes it at lines 383-397. This is the runtime availability path: when this native assembly is used, `native_service()` is populated before the listener serves requests and production gRPC selects `SharedNativeMatching`.

Therefore, `4a5da9f`'s instrumentation in the legacy `Matching::list_task_list_partitions` implementation (`grpc_matching.rs`, method around line 3272) is not instrumentation of the production native gRPC handler when native service is present. The native implementation is `matching_service_native.rs::core_list_task_list_partitions` (around lines 245-265), exposed through the generated native service and adapter. Do not use missing legacy `matching_grpc_transport` observations to conclude the production request never reached Rust or its handler.

## Retained gate evidence: limits of inference

The retained run is GitHub Actions run `41100032910`, attached to `4a5da9f`; evidence is in `/mnt/storage/service/ci-evidence/matching-postgres-41100032910/`. The Rust log contains native `list_task_list_partitions` `engine-complete` evidence, but it is uncorrelated to the final direct readiness request. Its absence of a `matching_grpc_transport` line is not evidence that a request did not reach Rust: the native gRPC production registration bypasses the instrumented legacy `Matching` RPC implementation. The engine-complete record could correspond to a direct RPC or a Thrift request; available evidence does not identify the transport.

Existing retained readiness facts remain as stated in `memory/matching-ci-and-transport-checkpoint-20261030.md`: the final direct gRPC readiness request timed out waiting for LB pick; its TCP connection succeeded; the separate HTTP/2 probe connected but did not observe remote settings before timeout. These facts do not establish a transport defect or identify where that readiness request stopped. Do not speculate about a transport defect from these observations.

No source changes, runtime actions, CI, build, test, or local validation were performed for this documentation audit.
