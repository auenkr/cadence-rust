# PostgreSQL c16 tail soak (2026-11-26)

## Predeclared margin (written before any run was dispatched or read)

Rust is declared free of a material PostgreSQL tail penalty iff, for the mean over runs of per-run p99 (whole run, and also the last-third window),
**Rust p99 <= Go p99 * 1.05**, judged on the upper bound of the 95% run-cluster bootstrap CI of the paired relative delta (Rust-Go)/Go
(upper bound <= +5% = pass; point estimate <= +5% but CI upper > +5% = inconclusive; point estimate > +5% = fail).
Done separately for (a) 4 unpaced runs and (b) 4 paced runs (target_rate=70). Rust-control (A/A, same tree) deltas give the noise floor.

Branch `perf-pg-soak-tails`, head `870a62142988a5adcce1557df7c836b3c78696e8`, workflow `frontend-matching-benchmark.yml`
(`database=postgres concurrency=16 requests=12000 repetitions=1 control_ref=origin/master rust_services=frontend,matching,history sdk_openloop=false`, no other inputs changed).
8 runs dispatched in parallel, all succeeded, 0 failed requests in any arm. Arms interleaved go/control/rust per run on one runner; rust = control tree (A/A).
Unpaced runs: 45696011164, 45696034309, 45696053498, 45696078456. Paced runs: 45696022351, 45696044562, 45696066880, 45696089377
(https://github.com/auenkr/cadence-rust/actions/runs/<id>).

Analysis: [perf-pg-soak-tails-analysis.py](perf-pg-soak-tails-analysis.py) over each run's `paired-benchmark/result-00-<arm>-c16-samples.json`.
Windows = thirds (4000 samples each) of samples ordered by completion time (start+latency); window wf/s = 4000 / completion span. Numbers in ms.

## Binaries (identical in all 8 runs)

- Rust cadence-server `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`
- Rust cadence-history `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5`
- Control (same tree) uses the same two hashes. Go binary hash differs per run (rebuilt each run): 63e8e7ee, 5c2db3d8, 7765443c, bbd527a1, 06fea5ea, e1d0d037, b56fec4f, c314e236 (first 8 hex, see `go-binary.sha256` per artifact).
- Runner CPUs were NOT uniform (4 vCPU each): EPYC 7763 x3 (011164, 053498, 066880), EPYC 9V74 x3 (034309, 044562, 089377), Xeon 6975P (022351), Xeon 8370C (078456). Arms within a run share a runner, so paired deltas are fair; absolute numbers across runs are not comparable.

## Results

### Unpaced (closed loop, c16)

| run | arm | wf/s | p50/p95/p99 ms | window 1 p50/p95/p99 @wf/s | window 2 | window 3 | CPU-s FE/HI/MT | failed |
|---|---|---|---|---|---|---|---|---|
| 45696011164 | go | 56.6 | 282/395/445 | 262/363/408 @61.1 | 298/414/457 @53.8 | 290/395/445 @55.5 | 73/189/47 | 0 |
| 45696011164 | control | 73.2 | 217/306/350 | 199/277/314 @80.1 | 224/307/349 @71.6 | 232/321/364 @68.9 | 47/161/37 | 0 |
| 45696011164 | rust | 73.3 | 216/310/353 | 201/285/319 @79.3 | 221/316/357 @71.5 | 228/322/367 @70.1 | 47/161/37 | 0 |
| 45696034309 | go | 80.4 | 197/289/343 | 189/280/329 @84.0 | 200/292/354 @79.2 | 203/292/349 @78.6 | 51/130/29 | 0 |
| 45696034309 | control | 106.5 | 146/216/264 | 137/206/251 @112.6 | 146/214/258 @107.2 | 156/228/274 @100.6 | 33/109/24 | 0 |
| 45696034309 | rust | 107.2 | 145/215/265 | 135/200/246 @114.7 | 145/212/260 @107.5 | 156/227/275 @100.6 | 33/109/24 | 0 |
| 45696053498 | go | 58.4 | 275/381/424 | 263/365/409 @61.3 | 286/394/437 @56.3 | 277/381/421 @57.9 | 69/182/44 | 0 |
| 45696053498 | control | 75.6 | 210/300/339 | 192/275/312 @82.3 | 219/308/346 @73.0 | 220/309/351 @72.5 | 44/155/34 | 0 |
| 45696053498 | rust | 74.7 | 212/306/352 | 193/273/312 @82.0 | 223/314/358 @71.8 | 222/316/362 @71.5 | 45/157/35 | 0 |
| 45696078456 | go | 73.8 | 215/306/342 | 206/295/336 @76.9 | 219/308/345 @72.7 | 221/310/344 @72.1 | 57/142/33 | 0 |
| 45696078456 | control | 101.7 | 155/223/260 | 143/210/242 @109.6 | 154/218/251 @102.6 | 169/233/270 @94.4 | 38/109/26 | 0 |
| 45696078456 | rust | 102.1 | 156/222/259 | 142/202/233 @111.1 | 156/219/251 @102.6 | 169/236/280 @94.2 | 38/109/26 | 0 |

Paired deltas (Rust minus other; mean over 4 runs, 95% run-cluster bootstrap CI of mean per-run relative delta, 20000 resamples):

```
rust-go p99 all: mean -81.6 abs, rel -21.2% CI[-23.6,-18.5] per-run [-20.7, -22.8, -17.1, -24.4]
rust-go p99 last third: mean -68.9 abs, rel -17.9% CI[-20.3,-15.2] per-run [-17.5, -21.3, -14.0, -18.7]
rust-go p99 first third: mean -93.1 abs, rel -25.4% CI[-29.0,-22.6] per-run [-21.7, -25.2, -23.8, -30.8]
rust-go p99 mid third: mean -91.7 abs, rel -23.4% CI[-26.9,-19.9] per-run [-21.9, -26.5, -17.9, -27.3]
rust-go p50 all: mean -60.1 abs, rel -25.1% CI[-27.0,-23.1] per-run [-23.4, -26.6, -22.9, -27.4]
rust-go p95 all: mean -79.5 abs, rel -23.6% CI[-26.6,-20.6] per-run [-21.3, -25.6, -19.8, -27.6]
rust-go wf/s: mean +22.1 abs, rel +32.3% CI[+28.8,+36.2] per-run [29.6, 33.3, 28.1, 38.4]
rust-control p99 all: mean +4.1 abs, rel +1.2% CI[-0.1,+3.0] per-run [1.0, 0.3, 3.9, -0.4]
rust-control p99 last third: mean +5.8 abs, rel +1.9% CI[+0.5,+3.3] per-run [0.6, 0.3, 2.9, 3.7]
rust-control p99 first third: mean -2.5 abs, rel -1.1% CI[-3.0,+0.7] per-run [1.6, -2.0, -0.2, -3.9]
rust-control p99 mid third: mean +5.4 abs, rel +1.6% CI[+0.3,+3.0] per-run [2.1, 0.8, 3.7, -0.2]
rust-control p50 all: mean +0.3 abs, rel +0.1% CI[-0.6,+0.8] per-run [-0.1, -1.0, 0.9, 0.7]
rust-control p95 all: mean +1.6 abs, rel +0.4% CI[-0.7,+1.6] per-run [1.4, -0.7, 1.7, -0.8]
rust-control wf/s: mean +0.1 abs, rel +0.0% CI[-0.8,+0.6] per-run [0.2, 0.7, -1.2, 0.3]
```

### Paced (target_rate=70 wf/s)

| run | arm | wf/s | p50/p95/p99 ms | window 1 p50/p95/p99 @wf/s | window 2 | window 3 | CPU-s FE/HI/MT | failed |
|---|---|---|---|---|---|---|---|---|
| 45696022351 | go | 70.0 | 97/146/173 | 94/132/166 @69.9 | 99/153/178 @70.0 | 98/149/175 @70.0 | 46/112/25 | 0 |
| 45696022351 | control | 70.0 | 87/110/125 | 86/110/124 @69.9 | 87/111/125 @70.0 | 87/110/125 @70.0 | 31/83/15 | 0 |
| 45696022351 | rust | 70.0 | 88/112/121 | 87/111/120 @69.9 | 89/112/127 @70.1 | 88/112/120 @69.9 | 32/86/16 | 0 |
| 45696044562 | go | 54.5 | 237/664/1074 | 214/578/1057 @59.3 | 276/789/1189 @46.8 | 235/536/901 @59.4 | 53/136/32 | 0 |
| 45696044562 | control | 69.1 | 123/356/589 | 113/312/492 @69.8 | 114/279/453 @70.1 | 169/431/751 @67.6 | 41/129/27 | 0 |
| 45696044562 | rust | 70.0 | 130/368/599 | 107/283/456 @69.9 | 166/427/796 @68.9 | 135/367/557 @71.2 | 43/131/28 | 0 |
| 45696066880 | go | 57.0 | 282/391/437 | 264/370/418 @60.4 | 291/400/441 @55.2 | 290/398/440 @55.7 | 72/188/46 | 0 |
| 45696066880 | control | 69.9 | 120/264/319 | 105/178/248 @69.8 | 132/286/332 @69.8 | 139/272/334 @70.0 | 46/158/30 | 0 |
| 45696066880 | rust | 69.9 | 117/280/329 | 99/139/168 @69.9 | 122/279/329 @69.8 | 183/302/346 @70.0 | 46/158/30 | 0 |
| 45696089377 | go | 64.6 | 231/412/590 | 211/424/649 @66.2 | 242/427/566 @62.6 | 237/382/540 @65.1 | 56/143/35 | 0 |
| 45696089377 | control | 70.0 | 101/243/468 | 99/237/350 @69.9 | 106/274/513 @69.8 | 99/219/509 @70.2 | 39/124/23 | 0 |
| 45696089377 | rust | 70.0 | 98/238/460 | 94/199/357 @69.9 | 99/248/475 @70.0 | 99/253/485 @70.1 | 35/117/20 | 0 |

Paired deltas (Rust minus other; mean over 4 runs, 95% run-cluster bootstrap CI of mean per-run relative delta, 20000 resamples):

```
rust-go p99 all: mean -191.1 abs, rel -30.2% CI[-39.4,-23.4] per-run [-29.7, -44.3, -24.7, -22.0]
rust-go p99 last third: mean -137.1 abs, rel -25.3% CI[-34.7,-15.5] per-run [-31.2, -38.2, -21.4, -10.2]
rust-go p99 first third: mean -297.3 abs, rel -47.3% CI[-58.4,-35.0] per-run [-27.7, -56.9, -59.8, -45.0]
rust-go p99 mid third: mean -161.6 abs, rel -25.8% CI[-31.1,-19.2] per-run [-28.8, -33.0, -25.3, -16.0]
rust-go p50 all: mean -103.5 abs, rel -42.6% CI[-58.1,-21.2] per-run [-9.1, -45.1, -58.5, -57.7]
rust-go p95 all: mean -154.1 abs, rel -34.7% CI[-43.4,-26.1] per-run [-23.7, -44.6, -28.5, -42.2]
rust-go wf/s: mean +8.4 abs, rel +14.9% CI[+4.2,+25.5] per-run [0.0, 28.4, 22.6, 8.4]
rust-control p99 all: mean +2.1 abs, rel +0.0% CI[-2.3,+2.4] per-run [-3.0, 1.7, 3.0, -1.6]
rust-control p99 last third: mean -52.7 abs, rel -7.7% CI[-20.4,+1.6] per-run [-3.8, -25.9, 3.7, -4.7]
rust-control p99 first third: mean -28.1 abs, rel -10.2% CI[-24.9,-0.3] per-run [-3.3, -7.3, -32.1, 2.0]
rust-control p99 mid third: mean +76.0 abs, rel +17.1% CI[-5.2,+56.5] per-run [0.9, 75.6, -0.8, -7.3]
rust-control p50 all: mean +0.7 abs, rel +0.5% CI[-2.7,+4.1] per-run [1.3, 6.3, -2.3, -3.1]
rust-control p95 all: mean +5.8 abs, rel +2.1% CI[-0.6,+4.6] per-run [1.2, 3.2, 5.7, -1.9]
rust-control wf/s: mean +0.2 abs, rel +0.3% CI[-0.0,+0.9] per-run [-0.0, 1.2, 0.0, -0.0]
```

## Verdict

Margin (predeclared above): Rust p99 <= Go p99 +5%.

- **Unpaced: PASS.** Rust p99 is -21.2% vs Go (CI -23.6..-18.5%), upper bound far below +5%; also in the last third (-17.9%, CI upper -15.2%) and first/middle thirds. Rust throughput +32% (CI +29..+36%). Rust p99 was lower than Go p99 in 4/4 runs, in every third.
- **Paced (70 wf/s): PASS but only one run is a clean comparison.** Go could not sustain 70 wf/s in 3 of 4 runs (achieved 54.5, 57.0, 64.6; Rust and control held 70.0), so Go was saturated and the Rust-Go delta (-30.2%, CI -39.4..-23.4) is dominated by Go overload, not a like-for-like tail. The one run where Go sustained 70.0 (45696022351, Xeon 6975P): Rust p99 121 vs Go 173 ms, p95 112 vs 146, p50 88 vs 97. Single-run, so no CI; point estimate -30% passes.
- **Rust vs identical-binary control (noise floor):** unpaced p99 +1.2% (CI -0.1..+3.0%); last third +1.9% (CI +0.5..+3.3%, a small but consistent positive in all 4 runs, +0.3..+3.7%, far inside the 5% margin and not Go-relative). Paced p99 all +0.0% (CI -2.3..+2.4%); per-third paced deltas are noisy (mid third +17% CI -5..+57%, last third -7.7%) because two paced runs (044562, 066880) drove runners to a knee where the queue builds in both Rust and control alike, so thirds there reflect runner noise, not binary difference.
- **The earlier outlier did not reproduce:** the 1-in-3 result (Rust p99 424 vs Go 351, control 397) had Rust p99 above Go in none of these 8 runs (4/4 unpaced, 4/4 paced Rust < Go). That outlier was high-variance p95/p99 on one fast runner that hit control equally (A/A), consistent with runner noise.

**Conclusion: no material Rust tail penalty on PostgreSQL at c16 versus Go (margin +5%).** Rust p99 is ~18-25% lower unpaced, and indistinguishable from its same-tree control. Caveats: 4 runs per condition on heterogeneous runners; paced Rust-Go evidence is weak because Go saturates below 70 wf/s on most runners (a lower rate, e.g. 50, would be the clean paced test); window-level control deltas of a few percent in the last third are within noise but not exactly zero.
