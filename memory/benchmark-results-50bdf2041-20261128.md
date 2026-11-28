# Go vs Rust benchmark results (2026-11-28, master 50bdf2041)

This report supersedes `benchmark-results-5f6504e05-20261126.md`. Master `50bdf2041225934e77426c208ae23adf962cc0fc` is the first head that combines all three Cassandra changes: the prepared-statement CachingSession, the nil retry policy, and the text-or-blob column for `bad_binaries_encoding`.

Rust runs Frontend, Matching and History. The Worker service stays in Go. Everything was dispatched with `--ref master` at this head (the benchmark and gate workflows build the ref they are dispatched on). Run IDs are listed in `/mnt/storage/service/tmp/bench50bd.txt` and in the tables below.

| Binary | SHA256 |
|--|--|
| Rust `cadence-server` (runs A–D, history shape) | `037a9c60a0a77aa0fcce6820560ec56381a6c673e684830cc3d7766fc53acaa2` |
| Rust `cadence-history` (same runs) | `f607e98e15e088e382613ec82ec21d786e21f79c64081a4e7a44a3ff63a2c49d` |
| Rust `cadence-server` (activities, signal-timer, child; Intel runners) | `5c9396fbad1fdab32830b759a20ca384f46523c9ca79c3e3fecd5bacacb29ca5` |
| Rust `cadence-history` (same) | `4e1adc8296c1e78b1c5891b4b6ecddbf5ab99dbef9cf4cd26390055ba24a4f4d` |

The source is the same in every run. The second hash pair appears on the three Intel-runner runs, which suggests a per-runner build difference (cache or toolchain image). This was not investigated. In every run the Rust arm and the A/A control arm have identical hashes (`control-same-tree.txt`=1). Go is rebuilt in every run. Go `cadence-server` hashes (first 12 hex): A b9b076e1463d, B 420820d0a2ba, C b240a5fdc5a2, D 24bf196fb818, activities 4f1bb9e0d7bc, signal-timer 4eea7712c377, child 077f270ee671, history e1ee952d6704.

- **Runners:** GitHub-hosted, 4 vCPU, 15 GiB, Ubuntu 26.04.1. The CPU of each run is listed below.
- **Container limits:** each service gets 1 CPU and 1.4 GiB; the database gets 1.5 CPU and 1.8 GiB.
- **Arms:** every run has three arms: Go, Rust, and an A/A control that runs the same Rust binary again in the same job to measure noise.

## 0. Gates on 50bdf2041: 31/31 success, no reruns needed

| Workflow | Inputs | Run | Result |
|--|--|--|--|
| rust-ci.yml | full workspace (clippy, then tests, fresh target) | [46128613896](https://github.com/auenkr/cadence-rust/actions/runs/46128613896) | success |
| matching-mixed-gate.yml | defaults | [46128617137](https://github.com/auenkr/cadence-rust/actions/runs/46128617137) | success |
| matching-cassandra-mixed-gate.yml | defaults | [46128619716](https://github.com/auenkr/cadence-rust/actions/runs/46128619716) | success (failed in the 1ee60ef82 sweep, R1) |
| matching-active-offer-gate.yml | defaults | [46128622528](https://github.com/auenkr/cadence-rust/actions/runs/46128622528) | success |
| matching-dependency-oracles.yml | defaults | [46128625436](https://github.com/auenkr/cadence-rust/actions/runs/46128625436) | success (was R2, stale lock) |
| matching-raw-runid-oracle.yml | format_oracle_files=false | [46128640342](https://github.com/auenkr/cadence-rust/actions/runs/46128640342) | success |
| matching-production-validation.yml | defaults | [46128628395](https://github.com/auenkr/cadence-rust/actions/runs/46128628395) | success |
| matching-production-tls-mixed.yml | defaults | [46128631402](https://github.com/auenkr/cadence-rust/actions/runs/46128631402) | success (was R2) |
| frontend-mixed-gate.yml | mysql / postgres / cassandra | [46128645639](https://github.com/auenkr/cadence-rust/actions/runs/46128645639) / [46128656363](https://github.com/auenkr/cadence-rust/actions/runs/46128656363) / [46128667062](https://github.com/auenkr/cadence-rust/actions/runs/46128667062) | success ×3 |
| history-mixed-gate.yml | mysql / postgres / cassandra | [46128648173](https://github.com/auenkr/cadence-rust/actions/runs/46128648173) / [46128659047](https://github.com/auenkr/cadence-rust/actions/runs/46128659047) / [46128669316](https://github.com/auenkr/cadence-rust/actions/runs/46128669316) | success ×3 |
| history-restartgate.yml | mysql / postgres / cassandra | [46128650687](https://github.com/auenkr/cadence-rust/actions/runs/46128650687) / [46128661644](https://github.com/auenkr/cadence-rust/actions/runs/46128661644) / [46128671592](https://github.com/auenkr/cadence-rust/actions/runs/46128671592) | success ×3 |
| history-soak.yml | mysql / postgres / cassandra | [46128653517](https://github.com/auenkr/cadence-rust/actions/runs/46128653517) / [46128664150](https://github.com/auenkr/cadence-rust/actions/runs/46128664150) / [46128673953](https://github.com/auenkr/cadence-rust/actions/runs/46128673953) | success ×3 |
| history-es-visibility.yml | defaults / es_flavor=opensearch | [46128634396](https://github.com/auenkr/cadence-rust/actions/runs/46128634396) / [46128642903](https://github.com/auenkr/cadence-rust/actions/runs/46128642903) | success ×2 |
| frontend-soak.yml | defaults | [46128637462](https://github.com/auenkr/cadence-rust/actions/runs/46128637462) | success |
| frontend-matching-benchmark.yml | 8 benchmark runs (sections 1 and 3) | 46128676756 … 46128694776 | success ×8 |

That makes 23 gate runs plus 8 benchmark runs. The benchmark runs replace the single matching benchmark entry of the 1ee60ef82 sweep, and `matching-http-tls-interop.yml` was skipped as in that sweep. Every Cassandra lane now passes (R1 resolved), so the three Cassandra changes work together.

## 1. Throughput and latency, closed loop

Echo shape, 1,000 requests, warmup 10, 1 KiB payload. Values are medians over reps (5 reps for SQL, 4 for Cassandra). Change is Go → Rust; a negative latency change means Rust is faster.

| Setup | Run / CPU | Conc | Go wf/s | Rust wf/s | Throughput | p50 | p95 | p99 | A/A wf/s · p95 |
|--|--|--|--|--|--|--|--|--|--|
| MySQL default | [A 46128676756](https://github.com/auenkr/cadence-rust/actions/runs/46128676756) EPYC 9V74 | 1 | 24.3 | 25.8 | +6.1% | −35.2% | +1.9% | −1.3% | +0.3% · +0.1% |
| MySQL default | A | 16 | 59.4 | 63.6 | +7.0% | −17.8% | **+18.8%** | **+25.3%** | +1.5% · +4.5% |
| MySQL default | A | 64 | 27.7 | 63.1 | +127.3% | −59.2% | −53.1% | −52.5% | +10.0% · −19.7% |
| MySQL `transferProcessorMaxPollRPS=1000` | [B 46128680323](https://github.com/auenkr/cadence-rust/actions/runs/46128680323) EPYC 9V45 | 1 | 37.6 | 54.1 | +43.9% | −31.9% | −39.4% | −22.6% | +2.9% · −2.2% |
| MySQL `transferProcessorMaxPollRPS=1000` | B | 16 | 74.3 | 72.4 | −2.5% (noise) | −12.2% | **+35.7%** | **+38.4%** | −16.6% · +2.3% |
| MySQL `transferProcessorMaxPollRPS=1000` | B | 64 | 33.1 | 53.2 | +60.7% | −37.2% | −42.7% | −44.3% | −2.2% · +2.4% |
| PostgreSQL default | [C 46128682679](https://github.com/auenkr/cadence-rust/actions/runs/46128682679) EPYC 9V74 | 1 | 26.0 | 26.8 | +3.1% | −14.4% | −0.9% | −4.7% | −0.8% · +0.1% |
| PostgreSQL default | C | 16 | 88.7 | 123.0 | +38.7% | −30.4% | −30.3% | −25.8% | +0.3% · −0.9% |
| PostgreSQL default | C | 64 | 81.5 | 110.7 | +35.9% | −22.0% | −27.9% | −28.7% | −0.2% · −1.4% |
| Cassandra default (4 reps) | [D 46128685122](https://github.com/auenkr/cadence-rust/actions/runs/46128685122) EPYC 7763 | 1 | 22.0 | 25.0 | +13.5% | −42.2% | **+34.7%** | +0.3% | +0.4% · +0.0% |
| Cassandra default | D | 16 | 59.4 | 66.8 | +12.6% | −10.2% | −13.9% | −27.5% | −4.0% · +4.1% |
| Cassandra default | D | 64 | 67.7 | 79.3 | +17.2% | −25.6% | −6.7% | −1.3% | +4.4% · −0.2% |

There were 0 failed workflows in any cell. The A/A column is control relative to Rust. In B c16 the control arm came in 16.6% below the Rust arm on the same binary, so that cell's throughput and tail changes are within noise. A c64 shows 10% A/A noise. Compared with 5f6504e05, MySQL c16 throughput gains are smaller (+7% here against +50% before). Go was faster in this run (59.4 against 47.2 wf/s) while Rust stayed level (63.6 against 71.1), and these runs used different CPUs, so a regression cannot be claimed from one run. Cassandra c16 confirms the c16 fix: Rust is +7.4 wf/s ahead of Go (the fix evidence had +10.0 wf/s [+7.4, +12.6]).

## 2. Concurrency 1 without pacing: root cause (unchanged)

`memory/evidence/perf-c1-hop-trace-20261128.md` shows that the extra unpaced c1 p90/p95 in Rust (about 94–100 ms against Go's 53–77 ms) is spent in History's transfer-queue load limiter. It is the per-shard `history.transferProcessorMaxPollRPS` limit of 20 rps with a burst of 20, which is Go's own default. Rust's faster loop pushes more decision tasks to the limiter edge, so more of them wait out a full refill interval. The code is not divergent and has no fix. With the limit raised to 1000 (run B), c1 p95 is 39.4% lower than Go. The paced c1 knee results from 5f6504e05 still stand: rates of 10–22 wf/s all pass (`perf-c1-knee*-20261126.md`).

## 3. Workload shapes (MySQL, 3 reps)

The anon-memory and CPU columns are Rust/Go ratios summed over Frontend, Matching and History.

| Shape | Run / CPU | Go wf/s | Rust wf/s | Rust/Go wf/s | p95 change | p99 ratio | Peak anon | CPU-s |
|--|--|--|--|--|--|--|--|--|
| activities (300 req, c16) | [46128687923](https://github.com/auenkr/cadence-rust/actions/runs/46128687923) Xeon 8573C | 11.2 | 16.9 | 1.51× | −35.1% | 0.64 | 0.71 | 0.64 |
| signal-timer (300 req, c16) | [46128690088](https://github.com/auenkr/cadence-rust/actions/runs/46128690088) Xeon 8573C | 13.9 | 14.6 | 1.05× | −8.7% | 0.86 | 0.60 | 0.49 |
| child (300 req, c16) | [46128692370](https://github.com/auenkr/cadence-rust/actions/runs/46128692370) Xeon 8370C | 8.6 | 12.7 | 1.49× | −32.5% | 0.68 | 0.82 | 0.63 |
| history (20 req, warmup 2, c4) | [46128694776](https://github.com/auenkr/cadence-rust/actions/runs/46128694776) EPYC 9V45 | 0.33 | 0.35 | 1.08× | −6.1% | 0.94 | 0.78 | **1.01** |

## 4. Resources (echo runs, per-rep medians)

- **Frontend and Matching memory:** Rust peaks at 14–24 MiB anon per service against Go's 30–49 MiB.
- **History memory:** Rust is 2–18% below Go (for example, PostgreSQL c16 is 86 MiB against Go's 100 MiB). On the history shape it is 56 MiB against Go's 63 MiB, and the Frontend is 65 against 94.
- **CPU-seconds, three services:** Rust uses 7.6–14.2 CPU-s against Go's 16.4–42.7, so the Rust/Go ratio is 0.33–0.77. The exception is the history shape at 1.01 (49.5 against 48.9 CPU-s).

## 5. Recovery

The Cassandra cold_restart loss reported in 5f6504e05 has been fixed. `fix-restart-lost-wf-20261126.md` and `confirm-cold-restart-20261128.md` show 0 lost Rust workflows on Cassandra, MySQL and PostgreSQL. The restart gates pass on all three databases at this head (section 0). The 5f6504e05 recovery timings were not re-measured.

## Where Rust is still worse than Go

1. **Unpaced c1 p95.** On Cassandra it is +34.7%. On MySQL it is +1.9% here (it was +25.6% in 5f6504e05; it varies by runner). The cause is the limiter plateau described in section 2: Go's 20 rps per-shard limit, with no code divergence.
2. **c16 tails on MySQL in this run.** Default: p95 +18.8%, p99 +25.3%. With the poll limit at 1000: p95 +35.7% and p99 +38.4%, but the A/A throughput noise in that cell is 16.6%. Previous reports had c16 MySQL p95 at −31% and −8%, so this needs a repeat on several runners before it can be called a regression.
3. **History-shape CPU.** Rust uses 1.01× Go's CPU. It is at parity, not better, while every other shape and setup uses 23–67% less CPU.

Every other measured metric is equal or better for Rust.

The HTML view of this report is `memory/benchmark-report-20261128.html`.
