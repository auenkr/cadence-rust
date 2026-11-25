# Benchmark coverage: PostgreSQL + long run (2026-11-25)

Branch `perf-coverage`, head `43cbe0c0387f880a93b04049603680fab7114447` (adds optional workflow input `database` = mysql|postgres, default mysql unchanged; `--database` in `paired-run.py`; new `tests/docker-drop-in/docker-compose.split.postgres.yml`, postgres:13, same limits as MySQL). Workflow `frontend-matching-benchmark.yml`, `source_ref=control_ref=master`, `rust_services=frontend,matching,history`. Arms: go, control (same Rust tree/binary as rust => A/A noise), rust. Rust FE+MT+HI vs all-Go.

Runner (both runs): AMD EPYC 7763, 2 cores x 2 threads (nproc 4), 15 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-1020-azure, image ubuntu26 20261116.202.1. Container limits 1 cpu / 1.4 GiB per service, DB 1.5 cpu / 1.8 GiB. Different runners per run (MySQL result with different CPU than the earlier Xeon reports: do not compare absolute numbers with benchmark-final-de38ba36c).

Binary SHA256 (postgres run): see artifact files go-binary.sha256 / rust-binary.sha256 (Rust cadence-server `a3d4752e...b91f`, cadence-history `1280bea9...84b5`; Go cadence-server `ad8a44fb...cdba`).

## 1. PostgreSQL, c=1,16,64, 5 reps — run https://github.com/auenkr/cadence-rust/actions/runs/45593008866 (success, 0 failed workflows)

Medians over 5 reps; latency ms; resources = peak cgroup anon MiB / CPU-seconds per container.

| c | arm | wf/s | p50 | p95 | p99 | FE | MT | HI |
|--|--|--|--|--|--|--|--|--|
| 1 | go | 23.9 | 36.7 | 96.2 | 113.7 | 33/9.5 | 31/5.4 | 72/21.7 |
| 1 | control | 25.0 | 28.9 | 99.1 | 104.5 | 17/4.6 | 19/2.6 | 68/12.1 |
| 1 | rust | 24.9 | 28.9 | 99.5 | 107.0 | 17/4.6 | 18/2.6 | 67/12.0 |
| 16 | go | 55.7 | 289.6 | 396.9 | 432.1 | 37/6.9 | 33/4.4 | 99/16.0 |
| 16 | control | 78.1 | 201.4 | 298.5 | 335.3 | 20/4.3 | 20/3.4 | 87/12.4 |
| 16 | rust | 78.2 | 203.2 | 282.6 | 320.8 | 20/4.4 | 20/3.5 | 88/12.5 |
| 64 | go | 53.5 | 1377.2 | 1641.8 | 1717.6 | 44/6.9 | 34/4.4 | 113/16.6 |
| 64 | control | 68.6 | 902.9 | 1379.8 | 1501.6 | 31/4.7 | 23/3.7 | 102/14.5 |
| 64 | rust | 71.9 | 1012.3 | 1284.7 | 1386.6 | 32/4.4 | 22/3.5 | 97/13.8 |

A/A (control -> rust, wf/s): c1 -0.4%, c16 +0.1%, c64 +4.8% (c64 p50 +12%: noise floor at c64 is larger than at c1/c16).
Go -> Rust (wf/s): c1 +4.2%, c16 +40%, c64 +34%; p99 c1 -5.9%, c16 -25.8%, c64 -19.3%. Rust uses ~half the CPU-seconds in History and FE/MT, and ~half the FE/MT memory; History peak memory similar (-1..-12%).
Conclusion: PostgreSQL shows the same direction as MySQL, with a larger Go->Rust throughput gain at c16/c64; no failures.

## 2. Long run (MySQL, c16, 1 rep) — run https://github.com/auenkr/cadence-rust/actions/runs/45600196671 (success)

There is no duration input; run length is set by `requests` (the driver counts workflows). 12000 requests/arm ≈ 7.5 min go + 3.8 min each Rust arm ≈ 15 min of measured load (~20 min with setup). First attempt with `requests=17000` (run https://github.com/auenkr/cadence-rust/actions/runs/45593014391) FAILED in the Go arm: 16998/17000 ok, 750 s, 22.6 wf/s, p50 338 / p95 780 / p99 13682 ms; 2 workflows hit the driver's 60 s execution timeout (START_TO_CLOSE), driver exits 1 on any failure so the Rust arms never ran. Go-only observation, not a harness fault.

| arm | ok/failed | wf/s | p50 | p95 | p99 | p99 first third -> last third | median first -> last third | FE/MT/HI peak anon MiB | CPU-s FE/HI/MT |
|--|--|--|--|--|--|--|--|--|--|
| go | 12000/0 | 27.7 | 453 | 1346 | 1678 | 387 -> 1837 | 314 -> 913 | 38/34/104 | 78/399/54 |
| control | 12000/0 | 53.7 | 296 | 428 | 527 | 260 -> 585 | 219 -> 374 | 20/24/107 | 45/158/34 |
| rust | 12000/0 | 53.4 | 297 | 432 | 545 | 260 -> 641 | 220 -> 373 | 21/24/107 | 46/160/35 |

(thirds = completion-ordered latency samples in the result JSON.) A/A control vs rust: wf/s -0.5%, p99 +3.4%.
Findings: peak memory is the same as the short runs (no leak; History 104-107 MiB vs 87-99 in short c16). Latency drifts upward over the run in all arms (database grows: Rust median +70%, Go +190%); Go degrades far more (throughput 27.7 vs 47-56 wf/s in short runs, and 60 s timeouts at 17k). Go -> Rust at 12k: +93% wf/s, p99 -67%. Single repetition: treat magnitudes as indicative; the control arm shows run-to-run noise is small on this metric.
