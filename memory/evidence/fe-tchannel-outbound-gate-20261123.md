# Frontend mixed gate with TChannel outbound (system.enableGRPCOutbound=false)

Branch `fe-tchannel-outbound-gate`, code/gate head `89d0317511da92b76207c604114a63e9c3cd5165` (kafka=false lanes).

Change: workflow input `outbound_transport: grpc|tchannel` on frontend-mixed-gate.yml; `run-frontend-gate.py --rust-outbound tchannel` now also writes `system.enableGRPCOutbound=false` into the Go services' dynamic config (whole cluster); gate steps `outbound-transport-<mode>-configured` (config) and `rust-frontend-outbound-runtime-<mode>` (runtime); the Rust Frontend prints one startup line `Cadence frontend outbound transport to History/Matching: Tchannel|Grpc` (workflow_backend.rs).

## Final runs (all `GATE SUMMARY failed: []`, both new steps ok)
| Lane | Run | Rust binary sha256 (prefix) |
|---|---|---|
| mysql tchannel | https://github.com/auenkr/cadence-rust/actions/runs/45240430451 | 878312b32f9f |
| postgres tchannel | https://github.com/auenkr/cadence-rust/actions/runs/45240437280 | aa485d7d0cec |
| cassandra tchannel | https://github.com/auenkr/cadence-rust/actions/runs/45240443152 | 35d1305422e8 |
| mysql grpc (no regression) | https://github.com/auenkr/cadence-rust/actions/runs/45240450244 | f8710f690764 |
| rust-ci cadence-frontend | https://github.com/auenkr/cadence-rust/actions/runs/45240423514 | success |

## Findings
- First tchannel attempts (45235838820 mysql, 45235844762 postgres, 45235850393 cassandra) FAILED: harness bug of mine, not service code. The tchannel key was added to the shared `dyn`, which the harness appends to again, giving a duplicate `system.enableGRPCOutbound` key; Rust logged "duplicate entry ... using no-op configuration" and many steps failed. Fixed by adding it only to the Go config. Superseded runs 45238021577/45238026170/45238031263 passed (config-only proof).
- No real Rust TChannel bug found. Runtime proof is the new startup log line, asserted by the gate.
- Limit: the line shows the transport selected at startup; it is not a per-call counter. Dynamic runtime switching is only covered by unit tests.
