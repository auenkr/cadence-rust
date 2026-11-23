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
- The Go gRPC mapper at `go/common/types/mapper/proto/history.go:512` omits the TChannel version-history item in `PollMutableState`. The response preserves version histories and the internal workflow state. Decision completion preserves the underlying started decision and activity dispatch map. History timestamps use nanoseconds.
- The shared gRPC error decoder preserves typed `EntityNotExistsError` cluster fields per `go/common/types/mapper/proto/errors.go:113`.
- Tests cover the six new request and response adapters plus StartWorkflowExecution, SignalWithStartWorkflowExecution, ResetWorkflowExecution, QueryWorkflow, and RecordActivityTaskHeartbeat.

## CI

Validation pending. The Python gate accepts `--rust-outbound tchannel`, but `.github/workflows/frontend-mixed-gate.yml` exposes no dispatch input to pass it through.
