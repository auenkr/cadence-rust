# Thrift candidate-v3 startup failure

https://github.com/auenkr/cadence-rust/actions/runs/40469948997 failed on
`ff3b638b49190f7653c2de3b062232d7ccc57f07`. Build succeeded; probe/gate failed.
The initial RegisterDomain RPC lost its connection because Go Worker exited
fatally during concurrent system-domain registration, before Matching scenarios.

Actual Go server log sequence at2026-10-27T07:57:50:

1. Worker: domain cadence-batcher does not exist, attempting to register domain.
2. Frontend: Register domain succeeded for service-compat-ebacd341-... .
3. Worker: Failed to update domain metadata. <>1 rows affected.
4. Worker fatal: failed to register system domain.

Runtime results confirm goServicesAliveAfterProbe:false. This does not establish
a Rust Matching transport defect. The harness currently waits only for the
Frontend TCP listener before probing; it must also wait for Go Worker's existing
`worker started` readiness log before creating the probe domain. Do not modify Go
production behavior or retry away a crashed service. Repeat the complete lane
after that startup coordination fix; keep existing transport/SDK assertions.

Raw server log remains under
`/mnt/storage/service/ci-evidence/40469948997/matching-production-thrift/gate/run/server.log`.
Compact actual results and source/binary identity are retained here.
