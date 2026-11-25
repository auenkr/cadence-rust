# Go vs Rust with the History poll limit raised equally (2026-11-25)

Run: https://github.com/auenkr/cadence-rust/actions/runs/45570459001 (success). Source = control = master 9ce0ba1a1+ (same Rust in both Rust arms). Rust services: frontend, matching, history. 5 reps. Both arms used dynamic config `history.transferProcessorMaxPollRPS: 1000`, so the shared 20/s limit is out of the way. Runner: GitHub-hosted ubuntu-latest, 4 vCPU, about 15 GiB; 1 CPU per service.

Medians (go -> rust):

| c | wf/s | >60 ms | p50 / p95 / p99 ms | History MiB | FE / Matching MiB | CPU s FE/M/H |
|---|---|---|---|---|---|---|
| 1 | 25.3 -> 38.1 | 0.6% -> 0.2% | 38.5/47.1/56.3 -> 25.8/28.5/35.5 | 72.3 -> 81.4 | 32.8/30.4 -> 24.9/19.5 | 6.6/3.8/16.3 -> 3.2/1.6/5.9 |
| 16 | 52.8 -> 70.2 | n/a | 291/444/482 -> 210/425/506 | 95.0 -> 86.0 | 36.5/33.0 -> 28.0/20.9 | 5.2/3.2/13.7 -> 3.1/2.1/7.9 |
| 64 | 26.7 -> 66.1 | n/a | 2418/3687/3820 -> 880/1753/1959 | 105.9 -> 103.3 | 43.4/34.1 -> 33.3/21.7 | 5.5/3.5/35.1 -> 3.1/2.1/8.5 |

With the shared limit removed, the c1 tail gap goes away: Rust is 50% faster with a much lower p95/p99.
Gaps left:
- History memory at c1 is 81.4 vs 72.3 MiB, though Rust does 50% more work in that time.
- c16 p99 is 506 vs 482 ms, about +5%.
