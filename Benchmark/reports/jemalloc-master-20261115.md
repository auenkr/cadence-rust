# Final-code benchmark: 571fb2a08 versus 77dbaa379

New source: `571fb2a0862bca59bb799f7baae9518d26c0e7b4`. Previous source: `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`.
Same pinned SDK/one-activity workflow, 1 KiB payload, 10ms activity, non-sticky, worker capacity 128. Fixed load 5 starts/s; capacity steps 5/25/100; three alternating fresh-MySQL pairs, 10s warmup and 30s measured per arm. Recovery drops MySQL TCP input for 3s, 10s into measurement. One Go system Worker stays in every configuration.
Standard ubuntu-latest, jobs <=90min. Services each 1 CPU/1400MiB; MySQL 1.5 CPU/1800MiB. SDK generator/worker occupy separate CPUs; services+DB share CPUs2–3. New Alpine/musl binaries enable jemalloc-default in both launchers; previous binaries used musl System. The GNU-only glibc arena cap is not exercised here. Allocator and Matching writer changes are measured together.

## Attempts and outcomes

- matching, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43752799238 — failure; harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`.
  recovery exited 1. Harness failures: {"00-go-failure.json": {"error": "resource collector failed"}} Traceback (most recent call last):
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 178, in <module>
    if __name__=='__main__':main()
                            ~~~~^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 174, in main
    run(['python3',str(ROOT/'report.py'),str(OUT)],'report')
    ~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 33, in run
    raise RuntimeError(f'{name} failed ({status}); see {path}')
RuntimeError: report failed (1); see /home/runner/work/cadence-rust/cadence-rust/Benchmark/evidence/run/recovery-5/report.log

- matching, attempt 2: https://github.com/auenkr/cadence-rust/actions/runs/43758977790 — failure; harness `d227d9ce800b5eed5e8624a1409e570e778c43a8`.
  recovery exited 1. Harness failures: {"00-go-failure.json": {"error": "resource collector failed"}} Traceback (most recent call last):
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 178, in <module>
    if __name__=='__main__':main()
                            ~~~~^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 174, in main
    run(['python3',str(ROOT/'report.py'),str(OUT)],'report')
    ~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 33, in run
    raise RuntimeError(f'{name} failed ({status}); see {path}')
RuntimeError: report failed (1); see /home/runner/work/cadence-rust/cadence-rust/Benchmark/evidence/run/recovery-5/report.log

- frontend, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43752802886 — success; harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`.
- history, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43752806311 — failure; harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`.
  recovery exited 1. 00-go: measured=0; warmup_correct=49; errors={'unexpected task failure: DecisionTaskTimedOut': 1} Harness failures: {"00-go-failure.json": {"error": "00-go-generate failed (1); see /home/runner/work/cadence-rust/cadence-rust/Benchmark/evidence/run/recovery-5/00-go-generate.log"}} Traceback (most recent call last):
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 178, in <module>
    if __name__=='__main__':main()
                            ~~~~^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 174, in main
    run(['python3',str(ROOT/'report.py'),str(OUT)],'report')
    ~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 33, in run
    raise RuntimeError(f'{name} failed ({status}); see {path}')
RuntimeError: report failed (1); see /home/runner/work/cadence-rust/cadence-rust/Benchmark/evidence/run/recovery-5/report.log

- history, attempt 2: https://github.com/auenkr/cadence-rust/actions/runs/43765306279 — success; harness `d227d9ce800b5eed5e8624a1409e570e778c43a8`.
- all, attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43752809662 — failure; harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`.
  recovery exited 1. Harness failures: {"00-go-failure.json": {"error": "resource collector failed"}} Traceback (most recent call last):
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 178, in <module>
    if __name__=='__main__':main()
                            ~~~~^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 174, in main
    run(['python3',str(ROOT/'report.py'),str(OUT)],'report')
    ~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/cadence-rust/cadence-rust/Benchmark/run.py", line 33, in run
    raise RuntimeError(f'{name} failed ({status}); see {path}')
RuntimeError: report failed (1); see /home/runner/work/cadence-rust/cadence-rust/Benchmark/evidence/run/recovery-5/report.log

- all, attempt 2: https://github.com/auenkr/cadence-rust/actions/runs/43772385162 — success; harness `d227d9ce800b5eed5e8624a1409e570e778c43a8`.

## Fixed-load results

Each cell is previous → new. Medians of per-run values; CPU sums Frontend+Matching+History cores. Memory/RSS sum per-container sampled peaks, not simultaneous cluster peaks; summed process RSS can repeat shared pages. Worker/DB excluded. Resources include drain and History audit. All-Go main row uses the All-Rust lane baseline; every paired baseline is shown separately below.

| Configuration | completions/s | p50 ms | p95 ms | p99 ms | CPU cores | memory MiB | RSS MiB | correct | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| All Go | 5.00 → 5.00 | 45.07 → 59.65 | 51.42 → 68.81 | 54.30 → 74.40 | 0.14 → 0.21 | 105.49 → 106.16 | 305.25 → 307.74 | 450 → 450 | 0 → 0 |
| Rust Matching only | 5.00 → 5.00 | 41.00 → 46.19 | 48.94 → 55.32 | 71.95 → 67.29 | 0.12 → 0.15 | 94.63 → 98.03 | 266.41 → 270.56 | 450 → 450 | 0 → 0 |
| Rust Frontend only | 5.00 → 5.00 | 60.32 → 50.98 | 68.52 → 206.04 | 73.39 → 447.73 | 0.17 → 0.14 | 94.92 → 100.75 | 272.29 → 276.45 | 450 → 450 | 0 → 0 |
| Rust History only | 5.00 → 5.00 | 81.29 → 71.48 | 424.14 → 76.04 | 720.03 → 79.54 | 0.15 → 0.18 | 89.28 → 97.30 | 262.75 → 270.46 | 450 → 450 | 0 → 0 |
| All Rust (Go Worker) | 5.00 → 5.00 | 66.31 → 67.48 | 67.96 → 71.45 | 72.02 → 73.51 | 0.11 → 0.12 | 67.70 → 78.32 | 186.36 → 199.60 | 450 → 450 | 0 → 0 |

## Capacity and recovery

Capacity is highest tested rate passing all three repetitions: zero errors, valid generator, completion p99 <=1000ms and at most max(2,1% arrivals) outstanding at the end. It is a short tested bound, not long-term sustainable capacity. An empty capacity means no tested step passed all targets. Recovery measures the END of the first full healthy five-second arrival window after restoration; it cannot be less than 5s. Recovery errors include warmup failures; missing windows are not successes.

| Configuration | passing tested starts/s old → new | recovery repetitions old → new | recovery seconds old → new | recovery errors old → new |
|---|---:|---|---:|---:|
| All Go | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Matching only | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Frontend only | 25 → 5 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust History only | 5 → 25 | 3/3 → 3/3 | 6.00 → 5.00 | 0 → 0 |
| All Rust (Go Worker) | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |

## Paired baselines and stage outcomes

Runner-to-runner variation is visible in the All-Go baselines. Compare Rust with its own Go pair before attributing changes to the migration. Retry results are identified above; first failures are retained rather than silently replaced.

| Lane / source | Go fixed completion p99 ms | Go CPU cores | Go memory / RSS MiB | Go passing starts/s |
|---|---:|---:|---:|---:|
| matching / previous | 91.22 | 0.12 | 105.62 / 306.37 | 25 |
| matching / new | 81.97 | 0.15 | 106.96 / 308.17 | 25 |
| frontend / previous | 73.31 | 0.18 | 107.46 / 307.61 | 25 |
| frontend / new | 217.52 | 0.16 | 106.72 / 306.88 | 5 |
| history / previous | 559.90 | 0.18 | 107.20 / 308.25 | 5 |
| history / new | 77.50 | 0.23 | 108.11 / 310.96 | 25 |
| all / previous | 54.30 | 0.14 | 105.49 / 305.25 | 25 |
| all / new | 74.40 | 0.21 | 106.16 / 307.74 | 25 |

| New lane / arm | 25/s passed all targets | 100/s stage exit | 100/s recorded errors |
|---|---|---:|---:|
| matching / go | True | 124 | 8781 |
| matching / rust | True | 124 | 5960 |
| frontend / go | False | 124 | 8955 |
| frontend / rust | False | 124 | 5982 |
| history / go | True | 124 | 8988 |
| history / rust | True | 124 | 4208 |
| all / go | True | 124 | 8987 |
| all / rust | True | 124 | 5595 |

## Rust binary provenance

Source/harness/tree hashes and actual runtime-image executable SHA256s follow. Exact config is versioned at the harness SHA in Benchmark/compose.yml and run.py; Go config/dynamic defaults/schema come from the pinned source. New manifests record the Rust compiler/target. Historical rust:1-alpine and mysql:8.0 tags can move; these comparisons do not isolate compiler or hosted-runner effects.

- previous matching: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `20aba723e0d0edba2fbe82cba3041c643ccf3cc47f47634aa8a23f073742ffcc  /usr/local/bin/cadence-server`
  - go_binary_sha256: `111981f33e6e3919030c93aa9d9490d61371326d28e30b9f7440e582843f0fd3  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- previous history: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `5bd8d3ededd1974c7c7c7db5d6b350c753e28fa2b1359a371beb921dedd1bdfa  /usr/local/bin/cadence-server`
  - go_binary_sha256: `63d72102f54dd413977253ad707a779979e10554e9382fbd689f54c5849bb944  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- previous all: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `82027497179f426282baf8431e3a8af10ab1246758c9a8443ee2be1860d0cad1  /usr/local/bin/cadence-server`
  - rust_history_binary_sha256: `3901d029b8e799259e1c04073e22b9acfcd61a0355ed66095c4e4b1e466931ca  /usr/local/bin/cadence-server`
  - go_binary_sha256: `4518d3846a464b9c12af92c89e686f339debc237f1f76571f71aafb886dda22f  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- previous frontend: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d0207e990a955df41264cffad5e11818a60e87e7`, Rust tree `0e3c285a65ec413473718763245995a4042cd22c`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `20aba723e0d0edba2fbe82cba3041c643ccf3cc47f47634aa8a23f073742ffcc  /usr/local/bin/cadence-server`
  - go_binary_sha256: `e2310f51806f54fd14d9f301628c3953b81302542b2579940fbd6790032c5d63  /usr/local/bin/cadence-server`
  - driver_sha256: `95fa25cc91e298d0a49c124c1d217322b770896bebca2b4c9685e51e73f2be89`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new matching: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`, Rust tree `f09d282feb38f80d75475e599e425d69779bde45`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `61362e6593274a3b2368f5f171fb98164b8b09f462c0a5552d7f9a79b9142859  /usr/local/bin/cadence-server`
  - go_binary_sha256: `d9cb0bddd718125807a65c6df3bbad65d65b036ba444650000c45aa80d181bdd  /usr/local/bin/cadence-server`
  - driver_sha256: `1580554ddaccf3f2b703a0c910d278669e1f96970d3dd1d54294700977aa337c`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new history: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d227d9ce800b5eed5e8624a1409e570e778c43a8`, Rust tree `f09d282feb38f80d75475e599e425d69779bde45`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `5db7b108a95a3a565d6b362c5cdf66e69fd534fcabde60a73d67b9303a3ef563  /usr/local/bin/cadence-server`
  - go_binary_sha256: `c065aeaca87b0ae2ba55896ae12ade1c985a3f8f4bb599acfdcbb584901afa87  /usr/local/bin/cadence-server`
  - driver_sha256: `b787c40c557cd2da8d069259a632efa1f56f93583c5600e9794441d57036f95b`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new all: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `d227d9ce800b5eed5e8624a1409e570e778c43a8`, Rust tree `f09d282feb38f80d75475e599e425d69779bde45`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `112276f24bb11610fcbd4e0076cbf20f99a713be867bce59a13a0721d4105854  /usr/local/bin/cadence-server`
  - rust_history_binary_sha256: `3b5ba6ac98099463c993ce9107473423192ea3c7a961587d844cd5574e6a6562  /usr/local/bin/cadence-server`
  - go_binary_sha256: `ebb771866ab27d565d4986bf413db1e8b77ae68d4703e5b06931176270dda1d9  /usr/local/bin/cadence-server`
  - driver_sha256: `b787c40c557cd2da8d069259a632efa1f56f93583c5600e9794441d57036f95b`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5
- new frontend: source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`, harness `9a002d774cfc22f9fd2671e773b9292ebf7bc960`, Rust tree `f09d282feb38f80d75475e599e425d69779bde45`, Go tree `60cff4fed2b6295126835a467e2e1de9d98897d1`.
  - rust_binary_sha256: `61362e6593274a3b2368f5f171fb98164b8b09f462c0a5552d7f9a79b9142859  /usr/local/bin/cadence-server`
  - go_binary_sha256: `7d241cd3d8e027211bf012a051360c29708c6ed5f73996d4de26fbc6fa5bc519  /usr/local/bin/cadence-server`
  - driver_sha256: `1580554ddaccf3f2b703a0c910d278669e1f96970d3dd1d54294700977aa337c`
  - sdk_lock_sha256: `63839f10df3718a77bdf8de5cc48e8f654a81fbc0147b218afa4c042db1d1e14`
  - Compiler/target: rustc 1.100.0 (0ef1bc386 2026-11-09); binary: rustc; commit-hash: 0ef1bc386059596dada8fb65b50425aab2ea637a; commit-date: 2026-11-09; host: x86_64-unknown-linux-musl; release: 1.100.0; LLVM version: 23.1.5

## Separate older reference: CI 41767896292

https://github.com/auenkr/cadence-rust/actions/runs/41767896292 — source `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`. Combined Rust Frontend+Matching with Go History/Worker; five closed-loop repetitions, concurrency 1/16/64, 100 warmups and 1000 measured workflows. All 30000 measured workflows succeeded. This is a different protocol and configuration, so its throughput is not an open-loop capacity comparison.

| concurrency | Go/Rust workflows/s | Go/Rust p50 ms | Go/Rust p95 ms | Go/Rust p99 ms |
|---:|---:|---:|---:|---:|
| 1 | 12.77 / 10.78 | 77.63 / 92.24 | 84.94 / 99.84 | 90.34 / 104.10 |
| 16 | 33.57 / 29.82 | 484.45 / 541.93 | 618.96 / 709.16 | 671.86 / 769.26 |
| 64 | 31.13 / 27.55 | 2294.92 / 2623.26 | 2600.80 / 3035.88 | 2884.15 / 3488.81 |

At old concurrency16, FE CPU seconds per 1000 workflows Go/Rust were 4.069/5.008, Matching 2.687/5.045; FE memory MiB 39.25/29.27, Matching 35.66/26.80. Older RSS and recovery are unavailable. CPU seconds per fixed work count differ from average CPU cores.

## Limits

No speed/cost claim from incomplete arms or unequal completion throughput. Short RSS samples do not establish a soak slope or that QPS-bucket retention fixes have removed all growth. Jemalloc and the Matching writer payload-copy optimization changed together; the GNU-only arena cap is outside this musl test; this test does not isolate their individual effects. Other scenario specifications remain unimplemented. Raw logs/ledgers stay ignored; only compact reports/charts are committed.

## Current Rust versus its paired Go baseline

| Lane | Go / Rust p50 ms | Go / Rust p99 ms | Go / Rust CPU cores | Go / Rust memory MiB | Go / Rust max passing starts/s |
|---|---:|---:|---:|---:|---:|
| matching | 47.21 / 46.19 | 81.97 / 67.29 | 0.15 / 0.15 | 106.96 / 98.03 | 25 / 25 |
| frontend | 51.29 / 50.98 | 217.52 / 447.73 | 0.16 / 0.14 | 106.72 / 100.75 | 5 / 5 |
| history | 63.90 / 71.48 | 77.50 / 79.54 | 0.23 / 0.18 | 108.11 / 97.30 | 25 / 25 |
| all | 59.65 / 67.48 | 74.40 / 73.51 | 0.21 / 0.12 | 106.16 / 78.32 | 25 / 25 |

## Earlier 3c2bf3aaa → current 571fb2a08

## Fixed-load results

Each cell is previous → new. Medians of per-run values; CPU sums Frontend+Matching+History cores. Memory/RSS sum per-container sampled peaks, not simultaneous cluster peaks; summed process RSS can repeat shared pages. Worker/DB excluded. Resources include drain and History audit. All-Go main row uses the All-Rust lane baseline; every paired baseline is shown separately below.

| Configuration | completions/s | p50 ms | p95 ms | p99 ms | CPU cores | memory MiB | RSS MiB | correct | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| All Go | 5.00 → 5.00 | 57.96 → 59.65 | 67.98 → 68.81 | 73.13 → 74.40 | 0.22 → 0.21 | 106.91 → 106.16 | 313.93 → 307.74 | 450 → 450 | 0 → 0 |
| Rust Matching only | 5.00 → 5.00 | 49.26 → 46.19 | 56.58 → 55.32 | 59.85 → 67.29 | 0.16 → 0.15 | 98.19 → 98.03 | 276.68 → 270.56 | 450 → 450 | 0 → 0 |
| Rust Frontend only | 5.00 → 5.00 | 62.51 → 50.98 | 72.45 → 206.04 | 78.07 → 447.73 | 0.21 → 0.14 | 97.05 → 100.75 | 272.30 → 276.45 | 450 → 450 | 0 → 0 |
| Rust History only | 5.00 → 5.00 | 122.83 → 71.48 | 654.13 → 76.04 | 931.88 → 79.54 | 0.15 → 0.18 | 90.49 → 97.30 | 267.91 → 270.46 | 450 → 450 | 0 → 0 |
| All Rust (Go Worker) | 5.00 → 5.00 | 72.34 → 67.48 | 75.79 → 71.45 | 78.45 → 73.51 | 0.18 → 0.12 | 67.13 → 78.32 | 188.70 → 199.60 | 450 → 450 | 0 → 0 |

## Capacity and recovery

Capacity is highest tested rate passing all three repetitions: zero errors, valid generator, completion p99 <=1000ms and at most max(2,1% arrivals) outstanding at the end. It is a short tested bound, not long-term sustainable capacity. An empty capacity means no tested step passed all targets. Recovery measures the END of the first full healthy five-second arrival window after restoration; it cannot be less than 5s. Recovery errors include warmup failures; missing windows are not successes.

| Configuration | passing tested starts/s old → new | recovery repetitions old → new | recovery seconds old → new | recovery errors old → new |
|---|---:|---|---:|---:|
| All Go | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Matching only | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust Frontend only | 25 → 5 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |
| Rust History only | none → 25 | 3/3 → 3/3 | 6.00 → 5.00 | 0 → 0 |
| All Rust (Go Worker) | 25 → 25 | 3/3 → 3/3 | 5.00 → 5.00 | 0 → 0 |

## Paired baselines and stage outcomes

Runner-to-runner variation is visible in the All-Go baselines. Compare Rust with its own Go pair before attributing changes to the migration. Retry results are identified above; first failures are retained rather than silently replaced.

| Lane / source | Go fixed completion p99 ms | Go CPU cores | Go memory / RSS MiB | Go passing starts/s |
|---|---:|---:|---:|---:|
| matching / previous | 81.91 | 0.16 | 106.25 / 310.88 | 5 |
| matching / new | 81.97 | 0.15 | 106.96 / 308.17 | 25 |
| frontend / previous | 75.71 | 0.21 | 107.13 / 307.34 | 25 |
| frontend / new | 217.52 | 0.16 | 106.72 / 306.88 | 5 |
| history / previous | 613.35 | 0.18 | 107.62 / 313.03 | 5 |
| history / new | 77.50 | 0.23 | 108.11 / 310.96 | 25 |
| all / previous | 73.13 | 0.22 | 106.91 / 313.93 | 25 |
| all / new | 74.40 | 0.21 | 106.16 / 307.74 | 25 |

| New lane / arm | 25/s passed all targets | 100/s stage exit | 100/s recorded errors |
|---|---|---:|---:|
| matching / go | True | 124 | 8781 |
| matching / rust | True | 124 | 5960 |
| frontend / go | False | 124 | 8955 |
| frontend / rust | False | 124 | 5982 |
| history / go | True | 124 | 8988 |
| history / rust | True | 124 | 4208 |
| all / go | True | 124 | 8987 |
| all / rust | True | 124 | 5595 |


## Matching retry: CI 44321372113 (complete)

- matching, attempt 3: https://github.com/auenkr/cadence-rust/actions/runs/44321372113 — **success**; harness `173f9053797595b3e9b441f54635419e15c7edbb`; source `571fb2a0862bca59bb799f7baae9518d26c0e7b4` (rust tree `f09d282fe`, go tree `60cff4fed`).
- Rust binary SHA256 `61362e6593274a3b2368f5f171fb98164b8b09f462c0a5552d7f9a79b9142859`; Go binary SHA256 `4f01bf34c5d6cd6e891b5a59cc54726518b84b680308867d879b9b68a4dbc1fa`; driver SHA256 `547c5167949b765fc573f25a0bccd6665025f91b92d0910ddf93e1064de2e562`.
- Harness change before this attempt: MySQL observer-probe failures are recorded as sample gaps (`mysql_sample_errors`) instead of aborting the collector, and collector failures now carry the log tail. In this run no probe failed (0 gaps in all six recovery arms), and the earlier `00-go` stall did not recur. That stall is therefore unexplained: it was seen twice and is not reproduced here.

Same protocol as above: three alternating fresh-MySQL pairs; medians of per-run values; CPU sums Frontend+Matching+History.

| Arm | completions/s | p50 ms | p95 ms | p99 ms | CPU cores | memory MiB | RSS MiB | Matching CPU / memory MiB / RSS MiB | correct | errors | passing starts/s | recovery |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Go (paired baseline) | 5.00 | 59.28 | 67.85 | 75.53 | 0.21 | 106.10 | 306.55 | 0.026 / 30.97 / 95.67 | 450 | 0 | 25 | 3/3, 5s, 0 errors |
| Rust Matching only | 5.00 | 61.13 | 70.75 | 78.04 | 0.22 | 100.49 | 277.72 | 0.025 / 23.38 / 59.70 | 450 | 0 | 25 | 3/3, 5s, 0 errors |

100/s failed for both arms (stage exit 124 on the 90-minute-bounded step; Go 8985 vs Rust 2997 recorded errors). These are not completion-throughput results.

**Conclusion:** Rust Matching is a drop-in replacement under this one-activity workload. Correctness, capacity bound (25/s) and recovery (5s, no errors) are equal. At 5 starts/s, end-to-end latency and total cluster CPU are within run-to-run noise (p99 +2.5ms, CPU +0.01 core). The Matching process itself used ~25% less cgroup memory and ~38% less summed RSS. This closes the earlier Matching recovery validation gap.
