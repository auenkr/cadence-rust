# Frontend outbound gRPC completion (2026-11-23)

Branch `fe-grpc-remaining-methods`, based on `origin/master` at `5a84a8513`. Go source is pinned at `bb4833c865311f2e22eb6dfe73cad52b53dbaf4b`.

## Go transport default

- `go/common/config/config.go:126` defines the per-service gRPC listener port.
- `go/common/dynamicconfig/dynamicproperties/constants.go:4829` defines `system.enableGRPCOutbound` with `DefaultValue: true` at line 4832.
- `go/common/rpc/params.go:76` reads that property and passes it to the History and Matching outbound builders at lines 119–133.
- `go/common/rpc/outbounds.go:225` builds the direct outbound, selecting gRPC when enabled at lines 235–242 and TChannel otherwise.
- `go/client/clientfactory.go:117` and `:148` select the gRPC client and gRPC membership port when the corresponding dispatcher outbound uses gRPC.

## Mappings

- History `PollMutableState`, `RespondDecisionTaskCompleted`, `DescribeWorkflowExecution`, and `RatelimitUpdate`.
- Matching `PollForDecisionTask` and `PollForActivityTask`. The outbound Thrift-envelope mapping in `workflow_matching.rs` now populates ForwardedFrom (field 30) and consumes internal reply fields for gRPC partition config and load-balancer hints at the two existing poll feedback calls. Matching's Thrift wire replies lack these fields. Go behavior is in `go/client/matching/client.go:158` and `:199`, with the mappings in `go/common/types/mapper/proto/matching.go:341`, `:382`, `:464`, `:491`, and `:546`.
- The Go gRPC mapper at `go/common/types/mapper/proto/history.go:512` omits the TChannel version-history item in `PollMutableState`. The response preserves version histories and the internal workflow state. Decision completion preserves the underlying started decision and activity dispatch map. History and Matching wire timestamps retain nanosecond precision, including transient decision events and activity poll replies.
- The shared gRPC error decoder preserves typed `EntityNotExistsError` cluster fields per `go/common/types/mapper/proto/errors.go:113`.
- Tests cover the six new request and response adapters plus StartWorkflowExecution, SignalWithStartWorkflowExecution, ResetWorkflowExecution, QueryWorkflow, and RecordActivityTaskHeartbeat.

## CI

Validated source head `f1aa6ac52f26edfbeb38387d00f511bfa2ec77cf`. The Rust CI runs used fresh, uncached target directories and ran Clippy before tests.

| Run | Scope | Conclusion |
| --- | --- | --- |
| [45211346572](https://github.com/auenkr/cadence-rust/actions/runs/45211346572) | Frontend | Success. 304 tests passed, including the eleven mapping tests. |
| [45211350255](https://github.com/auenkr/cadence-rust/actions/runs/45211350255) | Service-core | Success. 317 tests passed, including the typed error metadata test. |
| [45211354126](https://github.com/auenkr/cadence-rust/actions/runs/45211354126) | Workspace | Success. Clippy, workspace tests, and follow-up parity checks passed. |
| [45211358903](https://github.com/auenkr/cadence-rust/actions/runs/45211358903) | Default gRPC mixed gate, MySQL and Kafka | Success. `GATE SUMMARY failed: []`. |

The mixed gate ran Rust Frontend and Rust Matching with Go History and Worker. The freshly built Rust `cadence-server` SHA256 was `7320216ed87d55c7e87e34af52204469f9db0cdf037cf3bcfc2ae859f948f6d1`.

The Python gate accepts `--rust-outbound tchannel`, but `.github/workflows/frontend-mixed-gate.yml` exposes no dispatch input to pass it through.
