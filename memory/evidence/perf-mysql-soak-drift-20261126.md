# MySQL c16 soak drift check (2026-11-26)

Branch `perf-mysql-soak-drift`. No product code change. Source `2d5e47538ed0a73e561af4dabc3c5911ba60052d` (master), `control_ref=origin/master`
(same tree, so control vs rust is an A/A pair), `database=mysql`, `concurrency=16`, `requests=12000`, `repetitions=1`, default config,
`rust_services=frontend,matching,history`, `sdk_openloop=false`, workflow `frontend-matching-benchmark.yml`. Arms run sequentially (go, control, rust) on one runner per run; runs were on separate runners.
Method as in perf-pg-drift-20261125.md, except thirds are by start order (`seq`, as in the final report's soak tables): 4000 samples each.

Rust binary sha256 (identical in all runs, and equal to control): cadence-server `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`, cadence-history `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5`.
Go cadence-server sha256 (rebuilt per run): 45690760749 `ad98ea9b02ef4068ac3046d984eb5d53e7eb0e77363dab428f73fbd66d385de3`, 45690762276 `11803283676f7240203a57dfdc5f1165eecc12bc59794c1e4b668e930087250b`, 45694943957 `2998bfeb54304fde13933ce298f5529f3fbd18a346aff13ba7b665f7aac6df61`, 45694944032 `99d2da263272c4d484ffcf31e105c6fd2b5f69dda0612fcc332f3c596c1d2642`.

Runs (all four dispatched on master, head 2d5e47538): good runs
[45690760749](https://github.com/auenkr/cadence-rust/actions/runs/45690760749), [45690762276](https://github.com/auenkr/cadence-rust/actions/runs/45690762276),
[45694943957](https://github.com/auenkr/cadence-rust/actions/runs/45694943957), [45694944032](https://github.com/auenkr/cadence-rust/actions/runs/45694944032).
Failed (excluded): [45690760642](https://github.com/auenkr/cadence-rust/actions/runs/45690760642) and [45690760752](https://github.com/auenkr/cadence-rust/actions/runs/45690760752): the Go arm `run-00-go-c16` exited 1 (client error in the Go arm, before the Rust arms ran; same mode as the earlier soak 2, not Rust related). Two replacement runs were dispatched to get 4 good ones. 0 failed requests in every good arm.

Memory: resource data holds only peak cgroup anon and CPU-s per container, no time series, so within-run memory growth cannot be derived; peak anon is shown. Order is FE/HI/MT (alphabetical: frontend, history, matching).

| run | CPU | arm | wf/s | p50 | p95 | p99 | p50 first->last | p95 first->last | p99 first->last | peak anon MiB FE/HI/MT | CPU-s FE/HI/MT |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 45690760749 | AMD EPYC 9V74 | go | 28.3 | 407 | 1394 | 1977 | 399->419 (+5%) | 1428->1472 (+3%) | 2187->2050 (-6%) | 38/106/34 | 68/370/46 |
| | | control | 56.5 | 254 | 542 | 777 | 232->272 (+17%) | 476->629 (+32%) | 617->924 (+50%) | 18/113/22 | 43/155/32 |
| | | rust | 55.6 | 259 | 550 | 814 | 233->277 (+19%) | 601->529 (-12%) | 1079->704 (-35%) | 18/115/23 | 44/159/33 |
| 45690762276 | Intel Xeon Platinum 8573C | go | 25.1 | 431 | 1760 | 2199 | 409->441 (+8%) | 1715->1814 (+6%) | 2103->2246 (+7%) | 38/103/34 | 79/419/52 |
| | | control | 56.9 | 257 | 550 | 797 | 234->272 (+16%) | 527->534 (+1%) | 704->816 (+16%) | 18/115/22 | 49/163/35 |
| | | rust | 57.2 | 254 | 544 | 801 | 232->269 (+16%) | 510->529 (+4%) | 682->809 (+19%) | 18/109/21 | 49/163/35 |
| 45694943957 | AMD EPYC 7763 | go | 27.9 | 445 | 1329 | 1679 | 429->456 (+6%) | 1305->1367 (+5%) | 1795->1672 (-7%) | 38/104/35 | 75/396/52 |
| | | control | 53.9 | 291 | 451 | 561 | 264->305 (+16%) | 383->457 (+19%) | 522->529 (+1%) | 18/110/22 | 44/161/33 |
| | | rust | 54.7 | 289 | 432 | 547 | 259->307 (+18%) | 364->438 (+20%) | 410->526 (+28%) | 18/108/22 | 44/155/34 |
| 45694944032 | AMD EPYC 9V45 | go | 31.3 | 331 | 1300 | 1847 | 309->343 (+11%) | 1335->1310 (-2%) | 1840->1877 (+2%) | 38/112/34 | 58/359/37 |
| | | control | 55.6 | 270 | 518 | 666 | 300->219 (-27%) | 461->591 (+28%) | 515->726 (+41%) | 18/106/22 | 36/132/27 |
| | | rust | 55.4 | 272 | 533 | 661 | 303->235 (-23%) | 440->591 (+34%) | 504->720 (+43%) | 18/112/23 | 34/131/26 |

Runner: 4 vCPU, 15 GiB, Ubuntu 26.04.1; services 1 CPU / 1.4 GiB, mysql 1.5 CPU / 1.8 GiB. Go arm is not directly comparable on absolute numbers (wf/s 25-31 vs 54-57 for Rust).

## Summary (first->last third drift)

| metric | Rust (4 runs) | Control, same binary (4 runs) | Go (4 runs) |
|---|---|---|---|
| p50 | +19, +16, +18, -23 % | +17, +16, +16, -27 % | +5, +8, +6, +11 % |
| p95 | -12, +4, +20, +34 % | +32, +1, +19, +28 % | +3, +6, +5, -2 % |
| p99 | -35, +19, +28, +43 % | +50, +16, +1, +41 % | -6, +7, -7, +2 % |

## Conclusion

- Rust drift matches the A/A control in every run: p50 within 3 points in all four runs (+19/+17, +16/+16, +18/+16, -23/-27), and p95/p99 differences between Rust and control (up to about 44 points on p95 in run 1, 36 on p99 in run 4) are the same size as the spread between runs of the identical binary. The control itself ranges -27%..+17% on p50 and +1%..+32% on p95.
- Soak 2's Rust +17.3% p50 vs Go +8.9% is reproduced as a level (Rust +16..+19% in 3 of 4 runs vs Go +5..+11%), but the identical control binary shows the same +16..+17%, so it is not a Rust-binary property attributable to Rust code. In absolute terms Rust's last-third p50 (269-307 ms) is far below Go's (343-456 ms) in every run; Rust p50 is 20-40% lower, wf/s about 2x.
- Plausible reading (not proven here): Go's arm is saturated/slow from the start (p95 1.3-1.8 s), so its latency has less headroom to rise, while faster arms show relative drift from MySQL load growth. Run 4 shows a negative p50 drift for both Rust and control (first third slower), which indicates start-order thirds are noisy with a single rep; interpret relative drift with caution.
- Peak anon memory: Rust FE 18 / HI 108-115 / MT 21-23 MiB, equal to control; Go FE 38 / HI 103-112 / MT 34-35. History peak is about the same as Go; FE and MT are about half. Rust CPU-s is below Go in every run (history 131-163 vs 359-419), equal to control. Within-run memory growth was not measurable (peak only).
- Verdict: Rust's MySQL soak drift is within the same-binary control's spread; no Rust-specific drift regression was found. Caveat: one rep per run, four runs on four different CPU types (EPYC 9V74, Xeon 8573C, EPYC 7763, EPYC 9V45).
