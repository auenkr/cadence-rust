# Frontend replication Admin methods — bounded implementation report

Date: 2026-11-01
Base: integrated `ace9aefd4` (worktree branch `t3/dfe56271`)

## Scope and source behavior

Task methods only: `GetReplicationMessages` and `GetDLQReplicationMessages`.

Go source: `go/service/frontend/admin/handler.go:920-943` and `:1003-1029`.

- Both reject a nil request with `validate.ErrRequestNotSet` (`Request is nil.`).
- `GetReplicationMessages` rejects empty `clusterName` with `Cluster name is not set.`, then invokes `GetHistoryRawClient().GetReplicationMessages` with the original request.
- `GetDLQReplicationMessages` rejects empty `taskInfos` with `Replication task info is not set.`, then invokes `GetHistoryClient().GetDLQReplicationMessages` with the original request.
- History errors flow through `adh.error`, preserving declared Admin errors.
- Pinned Thrift: `go/idls/thrift/admin.thrift` declarations around lines 102 and 118; History transport procedures are in `go/idls/thrift/history.thrift:947-970`; task structs are `go/idls/thrift/replicator.thrift:142-170`.
- Pinned protobuf Admin wire contract: `go/idls/proto/uber/cadence/admin/v1/service.proto:259-275`; response payload types in `replication.proto` and the full union in `history.proto`.
- History's internal protobuf contract does have these RPCs: `go/proto/internal/uber/cadence/history/v1/service.proto:197-201,637-652`.

## Blocking architecture gap

The integrated runtime’s relevant Admin endpoints are in `rust/crates/cadence-service-core/src/admin.rs:687-708` and currently fabricate empty successful JSON responses. The configured `AppState.history_admin_channel` can reach the internal History protobuf client, but the shared `cadence_api::admin` model only defines the two request types (`rust/crates/cadence-api/src/admin.rs:660-697`); it has no corresponding response/map-by-shard types. `cadence_api::types::ReplicationTask` (`rust/crates/cadence-api/src/types.rs:618-636`) represents only task type, source task ID, and domain attributes, while the pinned task union includes creation time and additional attributes (including history V2) at `go/idls/proto/uber/cadence/admin/v1/replication.proto` and `history.proto`.

The two Thrift procedures are registered in `rust/crates/cadence-thrift/src/service.rs:969-993`, but `cadence-thrift/src/convert.rs` has only domain replication message and partial replication task conversions (around 1452-1515). `rust/crates/cadence-service-core/src/yarpc.rs` has no arms for either exact method; only the unrelated domain method is wired around 724 and 2446. The gRPC Admin implementation at `rust/crates/cadence-matching/src/grpc_admin.rs:840-860` is also an independent single-host transport implementation and currently synthesizes empty responses. The task explicitly assigns this path to `grpc_admin.rs`, but the Active runtime is service-core.

There is also a crate ownership mismatch: the prepared `cadence-frontend/src/handler_admin_history.rs:98-120` and `history_client.rs:360-390` already implement the Go validation and History RPC calls, but those are not the `service-core` Admin handlers named by this task, and their return types are internal History protobuf types rather than the `cadence_api::admin` JSON/Thrift models consumed by the integrated service-core transport.

## What is needed to unblock faithfully

Please confirm the intended transport contract for these two methods in `cadence-service-core`:

1. May the bounded patch extend `cadence-api` with complete `GetReplicationMessagesResponse` / `GetDLQReplicationMessagesResponse` types and the full pinned `ReplicationTask` union (including every history/task attribute), then add matching Thrift conversions and service-core YARPC arms?
2. Should service-core gRPC be wired through the service-core YARPC/JSON handlers, or is `cadence-matching/src/grpc_admin.rs` intended to remain the production Admin gRPC server despite the service-core History channel being configured elsewhere?

Without resolving the response ownership and production gRPC routing, returning only a subset or using the matching single-host empty behavior would violate the no-loss/no-fabricated-success requirement. No code change was made in this slice to avoid introducing an incompatible wire representation.

## Validation and status

No Cargo/Go commands were run, per CI-only validation instructions. No CI run or tested SHA exists because implementation is blocked pending the contract decision. Exact source pointers above are sufficient to resume without repeating the audit.
