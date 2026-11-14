# Matching allocator RSS experiment (2026-11-15)

## Setup

- Parent finding: [matching-rss-slope-20261115.md](matching-rss-slope-20261115.md); baseline is run 43629533136 (Rust Matching 7.281 MB/h, Go 1.488 MB/h, allowed maximum 6.488 MB/h).
- Added optional `matching_malloc_env` to `.github/workflows/frontend-soak.yml`. It defaults to empty and is passed to the Rust Matching child only. The harness accepts only numeric values for `MALLOC_ARENA_MAX` and `MALLOC_TRIM_THRESHOLD_`.
- The soak workflow concurrency key includes the malloc input so both controlled runs can execute concurrently without canceling each other.
- Both runs use default rate and duration (10 workflows/s, 70 minutes) and lanes `go-baseline,rust`.

## Runs

- A: https://github.com/auenkr/cadence-rust/actions/runs/43638986061 — `MALLOC_ARENA_MAX=2`; head `16db2e80baab78f02789b9981c9501eef20859f7`; canceled by the duplicate dispatch below before lane work started.
- B: https://github.com/auenkr/cadence-rust/actions/runs/43638989852 — `MALLOC_ARENA_MAX=2 MALLOC_TRIM_THRESHOLD_=131072`; head `16db2e80baab78f02789b9981c9501eef20859f7`; status pending at dispatch.
- A duplicate arena-only dispatch, https://github.com/auenkr/cadence-rust/actions/runs/43639157816, was canceled by the workflow concurrency group because the earlier arena-only run was already queued. It built no binary and ran no lane. No additional soak was dispatched; only the two original experiments were scheduled.
- The arena-only experiment is now represented by replacement run https://github.com/auenkr/cadence-rust/actions/runs/43639284037 (head `b472a2a920341ce666312d78c34e437297977f3b`); build succeeded and both lanes started at 2026-11-14 20:56 UTC. It overlaps B, which started its lanes at 20:54 UTC. Four workflow dispatch records exist because two A attempts were cancelled before any soak lane began; only A replacement and B have run soak lanes.

## CI

- Targeted Matching CI: https://github.com/auenkr/cadence-rust/actions/runs/43639180095 — head `b472a2a920341ce666312d78c34e437297977f3b`, `cadence-matching` clippy, formatting, tests, and matching gates all SUCCESS.
- Full Rust CI: https://github.com/auenkr/cadence-rust/actions/runs/43639925933 — same head `b472a2a920341ce666312d78c34e437297977f3b`, SUCCESS. Fresh-target clippy, formatting, workspace tests and configured compatibility gates passed.

## Results

Runs A replacement and B are still in progress. Record their conclusions, Matching RSS samples, calculated slopes, Go baseline slopes, and compare verdicts here. If the intervention lowers the Rust slope clearly below 6.488 MB/h, follow with a minimal permanent parity fix and, subject to the three-soak cap, one default soak. Otherwise reject the allocator hypothesis and make no permanent allocator change.
