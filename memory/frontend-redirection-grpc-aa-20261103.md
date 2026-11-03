# Frontend redirection: gRPC remote client + active-active (2026-11-03)

Branch `t3/frontend-redirection-grpc-aa`.

## Implemented
- `cadence-service-core::frontend_remote` picks transport per `clusterGroup[x].rpcTransport` (Go `NewCrossDCOutbounds`): `tchannel` -> TChannel/Thrift, `grpc` -> `frontend_remote_grpc` (tonic WorkflowAPI client, lazily cached channel, TLS from the cluster `tls` block via `grpc_tls::client_tls`, yarpc `rpc-*` metadata + `cadence-forwarding-cluster`). `ClusterInformation.tls` added (config).
- gRPC error mapping: `DomainNotActiveError` detail -> `CadenceError::DomainNotActive` (keeps the policy's second-hop fallback), NotFound/InvalidArgument/other.
- Active-active: `DomainState.active_active` from domain `activeClusters`; `DomainActiveClusterManager` resolves `ClusterAttribute{scope,name}` from the domain entry (Go `GetActiveClusterInfoByClusterAttribute`); start / signal-with-start requests supply `activeClusterSelectionPolicy.clusterAttribute`.
- Gate step `grpc-forward-*` in `tests/gates/run-frontend-two-cluster-gate.py` (cluster B over grpc).

## Gaps (not promoted)
- gRPC forwarding converts only StartWorkflowExecution, SignalWorkflowExecution, TerminateWorkflowExecution, RequestCancelWorkflowExecution. The other 40 redirected APIs return an explicit "not supported over the gRPC remote Frontend transport" error: `cadence_proto::convert` only has server-direction converters (proto request -> API, API response -> proto), and the inverse (including History events) is not ported.
- Active-active per-workflow lookups (`GetActiveClusterSelectionPolicyForWorkflow/CurrentWorkflow`, execution store read of the policy) are not ported: for active-active domains they fail and route to the current cluster. Authorization-provider outbound middleware not ported.

## Evidence
(pending CI)
