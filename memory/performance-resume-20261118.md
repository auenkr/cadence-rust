# Performance investigation resumed — 2026-11-18

- Starting source/master: `571fb2a0862bca59bb799f7baae9518d26c0e7b4`; clean checkout, fetched origin. No implementation changes yet.
- Re-read `Benchmark/reports/jemalloc-master-20261115.md` on `origin/t3/matching-benchmark-harness`.
- Correction to the previous status: Frontend lane's **paired Go baseline** also passed only 5 starts/s (previously 25), with p99 217.52 ms; Rust p99 was 447.73 ms. The 74.40 ms Go baseline belongs to the all-Rust lane, not the Frontend pair. Thus Rust tail overhead warrants investigation, but the capacity drop cannot be attributed solely to Rust.
- Historical History 720→80 ms improvement is also confounded by baseline variation: the paired Go History baseline changed 559.90→77.50 ms. These runs do not isolate an allocator speedup.
- Dispatched paired Frontend fixed-load reproduction on public standard GitHub Actions: https://github.com/auenkr/cadence-rust/actions/runs/44247071847 . Source `571fb2a0862bca59bb799f7baae9518d26c0e7b4`; workflow branch `t3/matching-benchmark-harness`; 5 starts/s, 20 s warmup, 90 s measured, three alternating fresh-database pairs. Conclusion pending; collect actual head and binary SHA256 from resulting evidence before claiming validation.
- One read-only Codex gpt-6-luna worker (high reasoning, default/normal service tier) investigates outbound Frontend connection pools and retry paths. No local builds/tests or extra workers.
- Next: inspect paired reproduction, identify request-level evidence, then implement only a justified behavior-preserving optimization and validate on CI.
