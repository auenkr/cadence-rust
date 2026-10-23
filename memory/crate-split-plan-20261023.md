# Crate split plan (2026-10-23)

Goal: matching / history / frontend / worker are independent crates + binaries; legacy `cadence-server` is only the all-in-one launcher. Pure code motion, no behavior change.

## Module -> crate
- **cadence-service-core** (no dependency on any service crate): `cli` (new: shared Cli/Command, `not_migrated`), `admin`, `config`, `grpc_tls` (+ `install_tls_crypto_provider`, moved from thrift_matching, re-exported there), `handlers` (AppState), `grpc_shard`, `yarpc`, `service`, `tchannel_json_client`, `ringpop_discovery`, `ringpop_tchannel`, `matching_metrics`, `matching_metrics_{prometheus,statsd,m3,runtime}` (config.rs embeds the exporter config types), `matching_quota` (matching_metrics uses it). `pub(crate)` -> `pub` blanket in these files.
- **cadence-matching** (lib + bin `cadence-matching`): all other `matching_*`, `grpc_matching`, `thrift_matching`, `tchannel`, `grpc`, `grpc_admin`, `grpc_internal`, `grpc_convert`, `grpc_decisions`, `grpc_history`, `ringpop_provider`, `ringpop_runtime`, `launcher` (old main.rs: `run(bin_name, only)`), tests, `matching_delivery`/`shard_distributor_go_interop` integration tests, wire fixtures.
- **cadence-history / -frontend / -worker**: lib (`SERVICE_NAME`) + bin that parses the same CLI and exits non-zero "service not migrated yet".
- **cadence-server**: legacy launcher; `pub use cadence_matching::*; pub use cadence_service_core::*;` keeps `cadence_server::<module>` paths (examples keep working); main calls `launcher::run("cadence-server", None)`.
- `e2e.rs` integration test moved to core (it only uses handlers).

## Cycles that forced placement (known debt)
1. `Matching` embeds `grpc::Frontend` and `grpc_internal::History` (legacy in-process stubs); `grpc_admin` calls `Matching::new` (1 call, `update_task_list_partition_config`). Inherent impls cannot be split across crates, so the legacy Frontend/History/Admin stack lives in cadence-matching until Frontend/History are really ported (then invert with a trait object hook).
2. `tchannel.rs` is one server serving frontend + Matching + Ringpop; `ringpop_provider` tests call `tchannel::serve_ringpop` and `serve_matching_with_ringpop_listener`, so `ringpop_provider`/`ringpop_runtime` stay in cadence-matching. Next step: split `tchannel.rs` into a generic transport (core) and the Matching dispatch, then move Ringpop to core.
3. `config` -> metrics exporter configs -> `matching_metrics` -> `matching_quota`: those moved to core instead of breaking the edge.

## CLI note
Existing Rust CLI is `start --config --root --services ...` (no top-level `--env/--zone`); kept unchanged for all binaries.
