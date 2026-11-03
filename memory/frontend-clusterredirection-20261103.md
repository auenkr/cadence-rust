# Frontend cluster redirection: production install (2026-11-03)

Branch `frontend-clusterredirection`, code head dd42132f6.

Ported:
- Config: `clusterGroupMetadata.clusterRedirectionPolicy{policy,allDomainApisForwardingTargetCluster}` (legacy `dcRedirectionPolicy` copied in). Policy strings via `ForwardingPolicy::from_name`; unknown = startup error; `noop`/empty = nothing installed (behaviour unchanged).
- `wrappers/redirection_runtime.rs`: `DomainCacheResolver` (36 redirected APIs; domain by name, task-token ops by domain UUID; execution for workflow-keyed ops), `FnRuntimeConfig` (`system.enableDomainNotActiveAutoForwarding`, domain filter, default true), `SinkRedirectMetrics` (`cluster_forwarding_policy_requests`, `cadence_client_*_redirection`), `ForwardingBackend` + `RemoteFrontends` from `clusterGroup` (enabled, remote clusters only).
- `cadence-service-core::frontend_remote` + `workflow_api::encode_client_request/decode_client_response`: TChannel/Thrift client for `WorkflowService::<op>` with `cadence-forwarding-cluster` header; exception mapping (DomainNotActive etc.).
- Tests: `wrappers::redirection_runtime::tests::*`, `frontend_remote::tests::*`.

CI: package cadence-frontend https://github.com/auenkr/cadence-rust/actions/runs/41568231364 success (head before cluster.rs serde fix); full workspace https://github.com/auenkr/cadence-rust/actions/runs/41569433333 success (head dd42132f6).

Gaps (audit rows NOT promoted to ported): no mixed-gate evidence of a real cross-cluster forward (single-cluster only); gRPC remote transport rejected explicitly; active-active domains unsupported (lookup errors fall back to current cluster); strong-consistency header not plumbed (always eventual); per-request domain describe (no cache); authorizer still Nop (config-driven/OAuth not ported); schedule APIs not covered by resolver.
