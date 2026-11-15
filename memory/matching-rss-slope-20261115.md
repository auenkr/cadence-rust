# Matching RSS slope investigation (2026-11-15)

## Baseline evidence

- Soak: https://github.com/auenkr/cadence-rust/actions/runs/43629533136
- Tested HEAD: `a77999300a831dda6f89df0a7de6124034f754a1`; run built a fresh Rust binary (`518c8c10096529a723d305097ff19a694d542c1bfaa27a0ecb0a979ceb7cd380`).
- Verdict: `RUST OVERALL: FAIL` solely on Matching RSS: 7.281 MB/h vs Go 1.488 MB/h; allowance is Go + 5 MB/h = 6.488 MB/h.
- Rust Matching samples (t seconds, RSS MiB): 0/58.5, 60/70.2, 120/72.3, 180/72.6, 600/73.1, 1140/75.7, 1800/75.3, 2100/76.5, 2164/76.2, 2584/77.6, 3004/78.0, 3605/79.9, 4145/81.5. Full series: `memory/evidence/fe-soak-latency-takeover/run-43629533136-rust-samples.csv`.
- The first 2 minutes include process warmup (58.5 to 72.3 MiB). Thereafter, RSS mostly rises, with short dips, from 72.6 MiB at 180s to 81.5 MiB at 4145s. After the Frontend restart at 2100s, Matching RSS remained in the same range and continued rising. This is not a clear plateau; it is consistent with a modest ongoing accumulation, though a single finite run cannot distinguish retained allocator pages from a live-object leak.
- Matching FD count stayed about 120–124 after startup (124 at t=1140, 121 at t=4145; slope -0.041/h). Threads stayed 24. All 42,000 workflows completed, with zero errors or panics. Thus connection/thread growth is not the explanation.
- Prior measured slopes vary materially: run 43605415284 reported 6.00 MB/h and run 43621670624 reported 8.60 MB/h. The former passed its RSS threshold but was not evidence of a plateau.

## Audit performed

- Rust `HostRateLimiter` stores one limiter per domain indefinitely. Go `handler.go` uses `quotas.NewCollection`, and Go `common/quotas/collection.go` also keeps one limiter per domain indefinitely. Not a Rust-only mismatch.
- Matching queue owner maps, startup gates, query registrations, durable queue, ack manager, poller history and QPS pending buckets were previously audited as bounded/cleaned or fixed; see `memory/frontend-soak-tail-20261114.md` and the prior evidence notes.
- The RSS shape and stable descriptors leave allocator retention/fragmentation plausible, but there is no direct evidence yet proving glibc allocator behavior is the cause. No allocator change was made on this evidence alone.

## Allocator experiment update

The controlled tests and their complete evidence are in [matching-rss-allocator-experiment-20261115.md](matching-rss-allocator-experiment-20261115.md). `MALLOC_ARENA_MAX=2` improved the slope only from 7.28 to 6.85 MB/h, still above allowance; adding `MALLOC_TRIM_THRESHOLD_=131072` measured 8.22 MB/h. Both failed overall RSS, so neither is an acceptable fix and the allocator root cause is not proven.

## Status

No clear Rust-only retained state was identified in this pass. Root cause is unresolved; this branch does not claim the slope is fixed or the service passes. Next useful experiment is a controlled CI soak with allocator behavior recorded (e.g. glibc arena tuning) against the same workload and baseline, followed by confirmation that the intervention reduces the slope without functional changes. Do not treat a threshold pass from one noisy run as proof of no leak.
