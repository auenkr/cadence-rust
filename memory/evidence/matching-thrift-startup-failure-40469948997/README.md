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

## Harness startup coordination follow-up

`tests/run-go-service-baseline.py` now waits for the original Go Worker's
`"msg":"worker started"` log marker, in addition to the Frontend TCP listener,
when and only when `worker` is included in `go_services`. The existing 90-second
startup deadline and live server process check remain in force; timeout errors
identify which readiness condition was missing. Successful startup records the
required marker and whether it was observed in `results.json` as
`workerReadiness`.

The marker is emitted at the end of `go/service/worker/service.go:Start`, after
`ensureDomainExists(system)` and, when batcher is enabled,
`ensureDomainExists(batcher)`. This coordinates against the demonstrated
domain-metadata version race without retrying or hiding a dead Worker. It does
not create a startup dependency cycle: the compatibility probe starts Rust
Matching after domain registration, while Worker startup/readiness completes
before that probe is launched.

Focused validation: pending `rust-ci.yml` dispatch with `go_sdk_compile=true`;
this compiles Go SDK compatibility code and checks Python syntax on the CI
runner. The full mixed Thrift gate is intentionally left for the parent after
review.
