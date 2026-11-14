# Matching allocator RSS experiment (2026-11-15)

## Setup

- Parent finding: [matching-rss-slope-20261115.md](matching-rss-slope-20261115.md); baseline is run 43629533136 (Rust Matching 7.281 MB/h, Go 1.488 MB/h, allowed maximum 6.488 MB/h).
- Added optional `matching_malloc_env` to `.github/workflows/frontend-soak.yml`. It defaults to empty and is passed to the Rust Matching child only. The harness accepts only numeric values for `MALLOC_ARENA_MAX` and `MALLOC_TRIM_THRESHOLD_`.
- The soak workflow concurrency key includes the malloc input so both controlled runs can execute concurrently without canceling each other.
- Both runs use default rate and duration (10 workflows/s, 70 minutes) and lanes `go-baseline,rust`.

## Runs

- A: https://github.com/auenkr/cadence-rust/actions/runs/43638986061 — `MALLOC_ARENA_MAX=2`; head `16db2e80baab78f02789b9981c9501eef20859f7`; status pending at dispatch.
- B: https://github.com/auenkr/cadence-rust/actions/runs/43638989852 — `MALLOC_ARENA_MAX=2 MALLOC_TRIM_THRESHOLD_=131072`; head `16db2e80baab78f02789b9981c9501eef20859f7`; status pending at dispatch.

## Results

Pending Actions completion. Record each run conclusion, Matching RSS samples, calculated slope, Go baseline slope, and compare verdict here. If either intervention lowers the Rust slope clearly below 6.488 MB/h, follow with a minimal permanent parity fix, full `rust-ci.yml`, and one default soak. Otherwise reject the allocator hypothesis and make no permanent allocator change.
