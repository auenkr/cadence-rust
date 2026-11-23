# Frontend per-member History/Matching gRPC target (branch fe-history-grpc-per-member)

Fixes root cause 2 of gate-sweep-fa85672-20261123.md. Code head: ac0275875d1c2ef3af5d725a50a637428df52e96 (commits 'resolve ... per ring member grpc label' + 'allow too_many_arguments on try_call').

## Go references
go/common/membership (HostInfo / `membership.PortGRPC` label `grpc`), go/client/history peer resolver (resolves owner via ring member grpc port; ShardOwnershipLost owner mapped via the resolver), go/common/rpc. Rust Matching: matching_history_membership.rs `grpc_address(member)`.

## Change
- cadence-frontend grpc_outbound.rs: `resolve_grpc_target(tchannel_addr, member_grpc, fallback_port)`; `try_call` takes the member's grpc address; fallback = configured port only when no label.
- workflow_backend.rs `call_with_timeout`: looks up the member via `ClusterMembership::history_host` / new `matching_host` by address. ShardOwnershipLost redirects re-enter call_with_timeout, so the owner address goes through the same mapping.
- cadence-service-core admin.rs: new `ClusterMembership::matching_host` (default None); cadence-matching ringpop provider implements it.
- Matching outbound had the same single-port bug; fixed the same way.
- Test: `per_member_grpc_ports_and_redirect`.

## Runs (branch fe-history-grpc-per-member, all success)
rust-ci: frontend 45258519715, matching 45258526679, service-core 45258533505 (first frontend run 45257822978 failed clippy too_many_arguments, fixed).
History soak mysql/postgres/cassandra: 45259352928 / 45259363726 / 45259374934.
History restart gate mysql/postgres/cassandra: 45259358576 / 45259367502 / 45259381466.
History mixed gate two-History mysql: 45259387925 (`GATE SUMMARY failed: []`).
Frontend mixed gate mysql: 45259393468 (`GATE SUMMARY failed: []`).
Soak/restart workflows have no GATE SUMMARY line in the job log; their gate step exited 0 (job success).

## Rust binary sha256
- history mixed 45259387925: cadence-server 7cc49d31d9c8a37ac1a7ab8f4b910dff8698dbed59d770e68951511dbe0346ae, cadence-history 8fa60a5e440f5be959022b32a33c1c0e8f8a6fc494da6bc2a748d3c8c973d511
- frontend mixed 45259393468: cadence-server 761da2159f1b3e990262816e53b313cdbf3e02590b1a6fff6005e023ba3f6725
- history soak mysql 45259352928: cadence-server 1e6e2a7a1b8f7b8c9f5c08d35f3914d934c7e3c16b092fdd81e9dd6bab9d7845, cadence-history 8bf74b7488fe9d4afb7906dd9802c848c089f69a75779cf7c0ec9903450db010
