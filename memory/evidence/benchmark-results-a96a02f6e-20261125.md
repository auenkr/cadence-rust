# Three-arm benchmark a96a02f6e (2026-11-25)

Run: https://github.com/auenkr/cadence-rust/actions/runs/45528490271 (success). Arms: Go, control 39aa8be14, candidate a96a02f6e. Rust services: frontend, matching, history. 5 reps, payload 1024 B, tchannel. rust-ci 45528486706: success.
Runner: GitHub-hosted ubuntu-latest (4 vCPU, 16 GB RAM).

Medians across 5 reps:

| conc | arm | wf/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | go | 25.0 | 25.3 | 100.2 | 102.5 |
| 1 | rust | 26.7 | 16.8 | 100.6 | 103.0 |
| 16 | go | 49.7 | 298.9 | 558.6 | 615.5 |
| 16 | rust | 56.7 | 289.3 | 419.7 | 482.4 |
| 64 | go | 44.2 | 1496.7 | 2167.5 | 2295.3 |
| 64 | rust | 50.7 | 1430.2 | 1776.9 | 1883.6 |

At c1, p95 and p99 are about 100 ms in all three arms, so the floor does not come from Rust. Rust is +0.4 ms over Go there, which is within noise.

CPU s / peak anon MiB (go -> rust):

| conc | frontend | matching | history |
|---|---|---|---|
| 1 | 5.2/32.6 -> 2.6/25.1 | 3.0/30.5 -> 1.4/19.2 | 12.6/72.8 -> 4.9/78.0 |
| 16 | 4.3/36.9 -> 2.6/28.0 | 2.5/33.5 -> 1.7/21.0 | 11.1/91.3 -> 6.9/89.5 |
| 64 | 4.3/43.5 -> 2.6/34.1 | 2.6/34.1 -> 1.8/24.8 | 21.4/107.2 -> 7.7/98.5 |

Open gaps: History memory at c1 (+5.2 MiB); c1 p95/p99 at parity with Go, not better (shared ~100 ms floor).
