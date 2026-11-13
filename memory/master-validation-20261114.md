# Master validation — 2026-11-14

Validated GitHub Actions runs at master head `40abefe7af3084a2789cde4ad527c3fca38e6b03` (short SHA `40abefe7a`). The 12 mixed History gates cover MySQL, PostgreSQL, and Cassandra with gRPC/TChannel outbound transport and one/two Rust History hosts. For each gate, the uploaded `service-stack.json` confirms the History host count; each gate log contains the exact `GATE SUMMARY failed: []` line and each run concluded successfully. The Rust CI run also concluded successfully.

| Run ID | URL | Workflow | Inputs | Conclusion | GATE SUMMARY line |
|---:|---|---|---|---|---|
| 43492476990 | https://github.com/auenkr/cadence-rust/actions/runs/43492476990 | rust-ci | workspace (clippy/test) | success | N/A (not a mixed gate) |
| 43492483385 | https://github.com/auenkr/cadence-rust/actions/runs/43492483385 | History mixed gate | MySQL, gRPC, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492489092 | https://github.com/auenkr/cadence-rust/actions/runs/43492489092 | History mixed gate | MySQL, gRPC, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |
| 43492494035 | https://github.com/auenkr/cadence-rust/actions/runs/43492494035 | History mixed gate | MySQL, TChannel, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492500278 | https://github.com/auenkr/cadence-rust/actions/runs/43492500278 | History mixed gate | MySQL, TChannel, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |
| 43492505870 | https://github.com/auenkr/cadence-rust/actions/runs/43492505870 | History mixed gate | PostgreSQL, gRPC, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492511060 | https://github.com/auenkr/cadence-rust/actions/runs/43492511060 | History mixed gate | PostgreSQL, gRPC, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |
| 43492516761 | https://github.com/auenkr/cadence-rust/actions/runs/43492516761 | History mixed gate | PostgreSQL, TChannel, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492522148 | https://github.com/auenkr/cadence-rust/actions/runs/43492522148 | History mixed gate | PostgreSQL, TChannel, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |
| 43492527961 | https://github.com/auenkr/cadence-rust/actions/runs/43492527961 | History mixed gate | Cassandra, gRPC, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492533503 | https://github.com/auenkr/cadence-rust/actions/runs/43492533503 | History mixed gate | Cassandra, gRPC, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |
| 43492539163 | https://github.com/auenkr/cadence-rust/actions/runs/43492539163 | History mixed gate | Cassandra, TChannel, two History hosts (`two_history=true`) | success | `GATE SUMMARY failed: []` |
| 43492544713 | https://github.com/auenkr/cadence-rust/actions/runs/43492544713 | History mixed gate | Cassandra, TChannel, one History host (`two_history=false`) | success | `GATE SUMMARY failed: []` |

No rows failed; no error excerpts apply. Mixed-gate identity and build evidence are available in each run's uploaded artifact (retention 3 days). The Rust CI run performs the repository CI checks independently of the mixed-gate matrix.
