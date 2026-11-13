# Isolated service smoke comparisons

Same pinned source and workload. Three fresh-database pairs per service; 10s warmup / 30s measured at 5 starts/s.

| Service | Go p99 ms | Rust p99 ms | Go CPU cores | Rust CPU cores | Go peak MiB | Rust peak MiB | Correct workflows Go / Rust |
|---|---:|---:|---:|---:|---:|---:|---:|
| History | 80.79 | 20073.00 | 0.1555 | 0.0659 | 40.97 | 26.70 | 450 / 450 |
| Frontend | Failed/incomplete | — | — | — | — | — | — |
| Matching | 76.77 | 86.11 | 0.0274 | 0.0338 | 31.48 | 19.30 | 450 / 450 |

- Short low-load comparisons; no capacity, recovery or production performance claim.
- Each service has its own paired all-Go baseline on its CI runner.
- Reported values are medians of per-run values over three repetitions.
- CPU/memory collection includes completion drain and History audits.
