# Frontend + Matching paired benchmark evidence

## Reproduction

- Workflow: [Frontend + Matching paired benchmark](https://github.com/auenkr/cadence-rust/actions/runs/41762497645)
- Conclusion: `success`
- Workflow head: `2e7e5f56d65ccdc2c23afa070c9b6486e4ee73fb`
- Run settings: 5 alternating pairs; 100 measured and 10 warmup workflows per arm and concurrency; concurrency 1, 16, 64; 1024 byte payload; TChannel SDK driver.
- Persistence: MySQL 8.0. The compose project was removed between every arm, giving each Go/Rust arm a fresh database. History and system Worker were Go in both arms. Frontend and Matching were both Go in the Go arm, and both Rust in the Rust arm.
- Runner: `ubuntu26`, Linux x64. Docker compose configuration and runner CPU/memory details are in the run artifact. The database is capped at 1.5 CPU and 1800 MiB; service containers share the same uncapped defaults in both arms.
- Artifact: `frontend-matching-paired-benchmark`, downloadable from the workflow run; includes all 30 workload JSONs, `summary.json`, resolved compose configs, command/log/exit records, resource samples, image IDs, binary hashes and runner details.

## Build/source identities

- Go source tree: `36b5af995abeea69cdca171d449c93c835c84871`
- Rust source tree: `33b78ae30a6cdabbae4e3992032658a9f5761608`
- Benchmark driver source tree: `663946fb68d8465de3571c24ec616e9ab521cff2`
- Go image: `sha256:bba833673e72144cb65a385c82dad8b3a7fa0a736fa0427a8ec8cffe4f6ac682`
- Rust image: `sha256:8e72e3881be54d9e71d9b1b4aa5de882f861e9d9acda410e3791acbf155fcb63`
- Go server binary SHA256: `ec4dc24d0b5982e16d50b78d13ebd959781e365e6dd69cb0a31f950d2c5f2854`
- Rust release server binary SHA256: `84878674bb435f323fa2f0dfed9bbd9614e09de899a2a1ff3cac85e5279e9d62`
- Shared SDK driver SHA256: `d2fffc27a5ea8c81cf730f76855da278129ee608dd351b6e9bb18fa068265720`
- CLI SHA256: `de47d555283ac598401703a7ccb99fa982659b7dabd37dbf79abcd58f41024c4`

The image IDs and hashes above are from the attached artifact. The run built the Rust server with the Dockerfile's Cargo release profile, and verified Frontend/Matching image identities and Go History/Worker identities in every deployment.

## Measured medians

All 30 runs completed with 100 successes and zero failures. Throughput is successful workflows/second; latency is end-to-end successful workflow completion in milliseconds.

| Concurrency | Arm | Throughput | p50 | p95 | p99 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Go | 25.19 | 38.68 | 44.75 | 77.70 |
| 1 | Rust | 17.72 | 47.58 | 149.47 | 151.36 |
| 16 | Go | 67.99 | 221.27 | 334.76 | 370.63 |
| 16 | Rust | 50.72 | 290.77 | 420.48 | 460.64 |
| 64 | Go | 62.68 | 855.60 | 1329.98 | 1378.75 |
| 64 | Rust | 51.71 | 1125.11 | 1594.72 | 1662.93 |

These are descriptive measurements on one hosted runner, not a general performance conclusion. In this run the Rust arm had lower throughput and higher latency at all three concurrency settings. The workload is closed-loop, includes SDK retries, and exercises one echo activity; it is not isolated RPC throughput or an open-loop overload test. The older collector integrated sampled Docker CPU percentages and mislabeled Docker memory usage as RSS. Those resource samples are excluded from the validated comparison reports and do not establish resource improvements. This run used identical uncapped service defaults in both arms; newer runner versions apply identical explicit service caps, so their results must be reported separately.

Raw SDK result JSON, effective configs, source/binary hashes, command/cleanup evidence and reproduced JSON/Markdown/HTML comparison reports are retained in `memory/evidence/frontend-matching-benchmark-initial-41762497645/`. The current comparer independently accepted all 30 SDK outputs and reproduced these medians; resource status is explicitly `not_collected` in those reports. Full service logs remain in the downloaded artifact at `/tmp/cadence-fm-benchmark-41762497645/`.

This benchmark validates that its measured SDK workflows succeeded against both service pairs. It does not replace the broader Frontend compatibility/parity gates or establish service-wide behavioral parity.

## Earlier driver formatting result

The original driver build run [41761570862](https://github.com/auenkr/cadence-rust/actions/runs/41761570862) succeeded at head `d9b4b1df3b4f6b6c20ec3c959933de499e0900a2`. Its `format.diff` was applied to `tests/frontend-gate/benchmark/main.go`; the paired run's `benchmark-format.diff` is empty.
