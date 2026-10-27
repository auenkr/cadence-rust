# Matching RPC inventory audit — 2026-10-27

Pin: `8d7dac38e8d7e5a8114c6da5e0c3cc3afcbc77d0` (`matching-acceptance-20261027-v5`).
Scope is the Go Matching RPC method inventory and Rust native gRPC/Thrift dispatch only. No Cargo, Go, build, test, format, runtime, gate, or CI-result inspection was performed.

## Source comparison

Go gRPC contract: `go/proto/internal/uber/cadence/matching/v1/service.proto:44-102`.
Go service implementation: `go/service/matching/handler/handler.go:146-552`.
Go Thrift contract: `go/idls/thrift/matching.thrift:142-272`.
Rust gRPC trait dispatch: `rust/crates/cadence-matching/src/grpc_matching.rs:1900-4010`.
Rust Thrift method routing and native calls: `rust/crates/cadence-matching/src/thrift_matching.rs:308-687,718-800`.

| Go Matching method | Rust native gRPC source | Thrift route on both Go and Rust | Existing audit/evidence pointer |
|---|---|---|---|
| `PollForDecisionTask` | `grpc_matching.rs:1902` | Yes; `matching.thrift:147`; Rust `thrift_matching.rs` `invoke` arm | `memory/matching-final-rpc-acceptance-matrix-20261025.md` PollForDecisionTask row; `memory/matching-thrift-native.md` |
| `PollForActivityTask` | `grpc_matching.rs:2172` | Yes; `matching.thrift:159`; Rust `invoke` arm | Same matrix PollForActivityTask row; `memory/matching-thrift-native.md` |
| `AddDecisionTask` | `grpc_matching.rs:2386` | Yes; `matching.thrift:171`; Rust `invoke` arm | Same matrix AddDecisionTask row; `memory/matching-thrift-native.md` |
| `AddActivityTask` | `grpc_matching.rs:2630` | Yes; `matching.thrift:187`; Rust `invoke` arm | Same matrix AddActivityTask row; `memory/matching-thrift-native.md` |
| `QueryWorkflow` | `grpc_matching.rs:2846` | Yes; `matching.thrift:201`; Rust `invoke` arm | Same matrix QueryWorkflow row; `memory/matching-thrift-native.md` |
| `RespondQueryTaskCompleted` | `grpc_matching.rs:3704` | Yes; `matching.thrift:216`; Rust `invoke` arm | Same matrix RespondQueryTaskCompleted row; `memory/matching-thrift-native.md` |
| `CancelOutstandingPoll` | `grpc_matching.rs:3623` | Yes; `matching.thrift:234`; Rust `invoke` arm | Same matrix CancelOutstandingPoll row; `memory/matching-thrift-native.md` |
| `DescribeTaskList` | `grpc_matching.rs:3840` | Yes; `matching.thrift:246`; Rust `invoke` arm | Same matrix DescribeTaskList row; `memory/matching-ignored-rpc-audit-20261025.md` for the documented in-memory constructor limit |
| `ListTaskListPartitions` | `grpc_matching.rs:3220` | Yes; `matching.thrift:269`; Rust `invoke` arm | Same matrix ListTaskListPartitions row; `memory/matching-thrift-native.md` |
| `GetTaskListsByDomain` | `grpc_matching.rs:3158` | Yes; `matching.thrift:258`; Rust `invoke` arm | Same matrix GetTaskListsByDomain row; `memory/matching-thrift-native.md` |
| `UpdateTaskListPartitionConfig` | `grpc_matching.rs:3392` | No Go Thrift procedure; no Rust Thrift arm | Same matrix UpdateTaskListPartitionConfig row; `memory/matching-thrift-native.md` |
| `RefreshTaskListPartitionConfig` | `grpc_matching.rs:3536` | No Go Thrift procedure; no Rust Thrift arm | Same matrix RefreshTaskListPartitionConfig row; `memory/matching-thrift-native.md` |
| `AcquireSemaphore` | `grpc_matching.rs:3278` | No Go Thrift procedure; no Rust Thrift arm | Same matrix AcquireSemaphore row; `memory/matching-thrift-native.md` |

The Go Thrift contract has ten procedures, matching the ten Rust `invoke` arms. Rust resolves the procedure through generated metadata (`dispatch_inner`, `thrift_matching.rs:718-742`), rejects unknown services/procedures, then calls the selected native `Matching` method; it does not proxy to a Go Matching instance. The three gRPC-only methods are also enumerated as Thrift-absent in `memory/matching-final-rpc-acceptance-matrix-20261025.md` and `memory/matching-final-acceptance-audit-20261026.md`.

## Result and boundary

No uncovered method, explicit unimplemented arm, or adapter to Go Matching was found in the pinned API inventory and handler routing. Therefore this audit closes only the RPC inventory/routing coverage question. It does not establish all method branch semantics, error parity, service acceptance, or mixed-cluster gate completion; prior audits retain those boundaries and residuals. Existing focused Thrift evidence includes `matching_thrift_preserves_nanos_and_internal_decision_field_ids`, `matching_thrift_exceptions_fill_required_fields_like_go_mapper`, `matching_thrift_uses_each_methods_exception_field_ids`, and `matching_thrift_has_no_partition_config_procedure` in `rust/crates/cadence-matching/src/thrift_matching.rs` and `matching_rpc_tests.rs`.
