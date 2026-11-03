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
- rust-ci full (clippy+workspace tests) on head 7a86792e..a rustfmt fix later: https://github.com/auenkr/cadence-rust/actions/runs/41637083087 success
- two-cluster gate (rust_ref this branch, head 380765b5388145a1f329a8ef23fd708533a2050b): https://github.com/auenkr/cadence-rust/actions/runs/41638396585 success, `GATE SUMMARY failed: []`, steps grpc-forward-start-on-A / lives-on-B / signal / terminate / terminated-on-B all ok (cluster B `rpcTransport: grpc`)
- Rust binary sha256: f571261aa93a36c7f8391d10afe7c62b68e5262627f028d3d1d3494dada2c79e
- Unit tests: `frontend_remote_grpc::tests::*`, `frontend_remote::tests::*`, `redirection_runtime::tests::{active_active_*,cluster_attribute_lookup_*,requested_attribute_*}`
