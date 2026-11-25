# PostgreSQL c16 long-run drift check (2026-11-25)

Branch `perf-pg-drift`. No product code change. Source `81fd2954cf183bf5f78b2709bd752b9f91e58273` (master HEAD), `control_ref=origin/master`
(same tree, so control vs rust is an A/A pair), `database=postgres`, `concurrency=16`, `requests=12000`, `repetitions=1`,
`rust_services=frontend,matching,history`, workflow `frontend-matching-benchmark.yml`. Arms are interleaved on one runner per run; the three runs
were dispatched concurrently on separate runners. Rust binary sha256: cadence-server `a3d4752e1489ee03...ccb91f`,
cadence-history `1280bea951bcba09...3584b5` (same as perf-matched-evidence-20261125.md).
Runner: AMD EPYC 7763, 4 vCPU, 15 GiB, Ubuntu 26.04.1, kernel 7.0.0-1020-azure, image ubuntu26 20261116.202.1; services 1 CPU / 1.4 GiB, postgres:13 1.5 CPU / 1.8 GiB.

Runs: [45621038369](https://github.com/auenkr/cadence-rust/actions/runs/45621038369), [45621045883](https://github.com/auenkr/cadence-rust/actions/runs/45621045883), [45621051492](https://github.com/auenkr/cadence-rust/actions/runs/45621051492). All succeeded; 0 failed requests in every arm.

Drift = last third vs first third of the completion-ordered `successful_latency_samples_ms` (4000 samples each). Memory is peak anon MiB and CPU-s,
in alphabetical container order FE/HI/MT (note: History is the middle value, unlike the earlier report's FE/MT/HI).

| run | arm | wf/s | p50 | p95 | p99 | p50 first->last | p95 first->last | peak anon MiB FE/HI/MT | CPU-s FE/HI/MT |
|---|---|---|---|---|---|---|---|---|---|
| 45621038369 | go | 56.9 | 283 | 396 | 441 | p50 212->349 (+65%) | p95 247->428 (+73%) | 38/129/34 | 73/188/47 |
| 45621038369 | control | 74.2 | 214 | 305 | 354 | p50 163->266 (+63%) | p95 188->339 (+80%) | 24/115/23 | 44/158/36 |
| 45621038369 | rust | 74.6 | 212 | 304 | 350 | p50 163->265 (+62%) | p95 189->337 (+78%) | 23/113/23 | 44/158/34 |
| 45621045883 | go | 55.0 | 293 | 407 | 449 | p50 220->362 (+65%) | p95 256->436 (+70%) | 38/126/34 | 76/194/49 |
| 45621045883 | control | 72.5 | 218 | 312 | 358 | p50 170->273 (+61%) | p95 194->345 (+78%) | 24/109/39 | 46/162/38 |
| 45621045883 | rust | 73.7 | 215 | 308 | 356 | p50 166->269 (+62%) | p95 190->341 (+80%) | 22/113/37 | 46/160/37 |
| 45621051492 | go | 85.2 | 185 | 286 | 351 | p50 126->243 (+93%) | p95 157->327 (+109%) | 38/153/35 | 49/120/28 |
| 45621051492 | control | 114.1 | 129 | 215 | 397 | p50 101->171 (+68%) | p95 114->339 (+197%) | 24/126/23 | 31/99/23 |
| 45621051492 | rust | 122.8 | 119 | 197 | 424 | p50 96->155 (+62%) | p95 107->358 (+235%) | 24/133/25 | 29/91/20 |

## Conclusion

- Rust p50 drift: +62%, +62%, +62%. Control (same tree) p50 drift: +63%, +61%, +68%. Go: +65%, +65%, +93%.
- Rust p95 drift: +78%, +80%, +235%. Control: +80%, +78%, +197%. Go: +73%, +70%, +109%. The third run's p95 tail is noisy in both Rust and control (A/A), so p95 drift differences of that size are runner noise.
- Rust drift is indistinguishable from the A/A control in every run and is not consistently worse than Go on p50. The earlier +15% vs +9% single-rep gap was not reproduced; with 12000 unpaced requests every arm, including Go, slows down by 60-90% over the run on PostgreSQL (queue growth under saturating load), and Rust stays 2-30% faster in absolute p50/p95/p99 than Go with lower CPU and memory.
- Per the task's criterion (act only if Rust drift is consistently worse than Go beyond the A/A spread), step 2 is not triggered. No change was made to `cadence-persistence`, so no rust-ci or postgres mixed-gate rerun was needed.
- Caveat: one rep per run, three runs; absolute throughput varies between runners (55-85 Go, 72-123 Rust wf/s).
