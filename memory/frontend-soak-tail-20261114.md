# Frontend soak tail (2026-11-14), branch frontend-soak-tail

## Run 1: master 52848226a, rate 5, 10 min, all 4 lanes
https://github.com/auenkr/cadence-rust/actions/runs/43582334396 (head 52848226a21ba35aea88e10157da31ff2bd7f168). All jobs green; verdict files per lane:
- rust (Rust FE+MT): e2e p50/p95/p99 3052/4055/5780 vs Go 3063/4071/4568 (PASS); start p95 6 vs 14, p99 8 vs 134 (PASS); query p99 30 vs 63; init retries 1490 wf / 2017 attempts vs Go 1517 / 2052 (PASS). 3000/3000, 0 failed, no panics. Only FAIL: fe/mt RSS+fd slopes (fe n/a, mt 60 MB/h; meaningless in 10 min).
- rust-mt (Go FE + Rust MT): all latency PASS; only slope criteria fail (fe n/a).
- rust-fe (Rust FE + Go MT): e2e p99 7690 vs 4568, query p99 198 vs 63, start p95 160 vs 14, p99 401 vs 134 = FAIL. (Combined lane was clean in the same run, so this is noise-sensitive; Frontend is the only lane with a tail.)
Conclusion: the 2026-11-13 gap (e2e p95 14s, start p95 103ms) is gone on master (History/pool fixes). Matching is not the source. Residual: Rust FE tail in rust-fe lane only; slope criteria need a full-length run.
