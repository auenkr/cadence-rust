# History startup gaps 2 (2026-11-07, branch history-gap-startup2)

## Ported
- **gRPC TLS** (`rpc.tls`): launcher no longer rejects it. Uses shared `cadence_service_core::grpc_tls` (`server_tls`, `server_tls_custom_incoming`, `server_builder`); the "no identity / empty client CA" cases reuse `cadence_matching::grpc_matching::no_identity_tls_incoming` (made `pub`). `HistoryService::set_grpc_tls(GrpcTls)` feeds `ServerConfig`. Scope matches Go: only the gRPC listener is TLS; TChannel stays plaintext.
- **Dynamic config**: `DynamicConfigProvider::ConfigStore` wired (`ConfigStoreDynamicClient` in `wiring/resources/dynconfig.rs`, backed by `cadence_persistence::config_store::ConfigStoreClient`, polled via spawn_blocking; Go `getValueWithFilters` semantics; open failure falls back to empty/NOP like Go). **Not done**: `OpenFeature` still bails at startup with an explicit error (Rust Frontend does not support it either).
- **Wrapper audit** (Go `service/history/wrappers/{grpc,thrift}`): gaps found and closed:
  - `ResetWorkflowExecution` over gRPC and Thrift (`HistoryResetWorkflowExecutionRequest` Thrift codec, exception table).
  - gRPC `GetReplicationMessages`, `GetDLQReplicationMessages`, `ReadDLQMessages`, `PurgeDLQMessages`, `MergeDLQMessages` (previously UNIMPLEMENTED) using new out-converters in `cadence-service-core/src/replication_convert.rs` (`replication_task_out`, `replication_messages_out`, `replication_token_in`, ...).
- Not examined: History outbound client TLS (`client_tls`) when dialing peers.

## Evidence
- Failed run 42339537370 (head fce833bb4): clippy `items_after_test_module` only; fixed.
- Final run: see bottom (updated after CI).
- Run 42339907186 (head 4c7b56ac1): clippy ok, 2 stale server tests expected Reset UNIMPLEMENTED; updated.
- **Final green**: https://github.com/auenkr/cadence-rust/actions/runs/42340418109 — package=cadence-history, head 3f98ce402, conclusion success (clippy + tests). Only memory file added after this head.
