# Matching allocator RSS experiment (2026-11-15)

## Setup

- Parent finding: [matching-rss-slope-20261115.md](matching-rss-slope-20261115.md); baseline is run 43629533136 (Rust Matching 7.281 MB/h, Go 1.488 MB/h, allowed maximum 6.488 MB/h).
- Added optional `matching_malloc_env` to `.github/workflows/frontend-soak.yml`. It defaults to empty and is passed to the Rust Matching child only. The harness accepts only numeric values for `MALLOC_ARENA_MAX` and `MALLOC_TRIM_THRESHOLD_`.
- The soak workflow concurrency key includes the malloc input so both controlled runs can execute concurrently without canceling each other.
- Both runs use default rate and duration (10 workflows/s, 70 minutes) and lanes `go-baseline,rust`.

## Runs

- A: https://github.com/auenkr/cadence-rust/actions/runs/43638986061 — `MALLOC_ARENA_MAX=2`; head `16db2e80baab78f02789b9981c9501eef20859f7`; canceled by the duplicate dispatch below before lane work started.
- B: https://github.com/auenkr/cadence-rust/actions/runs/43638989852 — `MALLOC_ARENA_MAX=2 MALLOC_TRIM_THRESHOLD_=131072`; head `16db2e80baab78f02789b9981c9501eef20859f7`; workflow SUCCESS, analyzer **RUST OVERALL: FAIL**.
- A duplicate arena-only dispatch, https://github.com/auenkr/cadence-rust/actions/runs/43639157816, was canceled by the workflow concurrency group because the earlier arena-only run was already queued. It built no binary and ran no lane. No additional soak was dispatched; only the two original experiments were scheduled.
- The arena-only experiment is represented by replacement run https://github.com/auenkr/cadence-rust/actions/runs/43639284037 (head `b472a2a920341ce666312d78c34e437297977f3b`); workflow SUCCESS, analyzer **RUST OVERALL: FAIL**. Four workflow dispatch records exist because two A attempts were canceled before any soak lane began; only A replacement and B ran soak lanes. These two valid soaks exhaust the requested maximum.

## CI

- Targeted Matching CI: https://github.com/auenkr/cadence-rust/actions/runs/43639180095 — head `b472a2a920341ce666312d78c34e437297977f3b`, `cadence-matching` clippy, formatting, tests, and matching gates all SUCCESS.
- Full Rust CI: https://github.com/auenkr/cadence-rust/actions/runs/43639925933 — same head `b472a2a920341ce666312d78c34e437297977f3b`, SUCCESS. Fresh-target clippy, formatting, workspace tests and configured compatibility gates passed.

## Results

- Both measured soaks used Rust binary SHA256 `518c8c10096529a723d305097ff19a694d542c1bfaa27a0ecb0a979ceb7cd380` and completed all 42,000 workflows without errors, panics or process deaths.
- Arena-only run 43639284037: `MALLOC_ARENA_MAX=2`; Rust Matching RSS 73.5 MiB at t=1140s and 80.7 MiB at t=4146s, with FDs 120 at both samples and 25 threads. Slope 6.854 MB/h vs Go 1.488 MB/h; allowed maximum 6.488 MB/h. `RUST OVERALL: FAIL` only on Matching RSS. Latencies all passed. Full samples and summary are saved in [run A evidence](evidence/matching-allocator-20261115/run-a/).
- Arena plus trim run 43638989852: `MALLOC_ARENA_MAX=2 MALLOC_TRIM_THRESHOLD_=131072`; Rust Matching RSS 73.5 MiB at t=1140s, 75.8 MiB at t=2100s and 80.6 MiB at t=4145s. FDs 125, 123 and 123 respectively; threads stayed 27. Slope 8.217 MB/h vs Go 1.044 MB/h; allowed maximum 6.044 MB/h. `RUST OVERALL: FAIL` on Matching RSS and start p99 (176ms vs Go 19ms); remaining criteria passed. Full samples and summary are saved in [run B evidence](evidence/matching-allocator-20261115/run-b/).
- Arena-only lowered the slope by about 0.43 MB/h relative to the unmodified 7.281 MB/h baseline, but remained above the Go+5 allowance. Adding the trim threshold increased the measured slope. The allocator hypothesis is rejected for these tested settings: neither produced a clear improvement within the allowance. No permanent allocator change was made, and no third soak was run.
- Full and targeted CI both passed on source head `b472a2a920341ce666312d78c34e437297977f3b`: [targeted Matching run 43639180095](https://github.com/auenkr/cadence-rust/actions/runs/43639180095) and [full rust-ci run 43639925933](https://github.com/auenkr/cadence-rust/actions/runs/43639925933). The final branch head is docs-only commit `72191d021` above that tested source.

## Conclusion

The Matching RSS criterion still fails both controlled allocator runs. The tested allocator hypothesis is rejected, no permanent allocator change was made, and the two-run soak cap is exhausted. Do not claim a fix or parity pass. A future investigation should use allocation profiling or an instrumented live-owner count before another soak or runtime allocator change.
