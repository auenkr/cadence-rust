# Jemalloc master suite — finished

Source: `571fb2a0862bca59bb799f7baae9518d26c0e7b4`; harness: `9a002d774cfc22f9fd2671e773b9292ebf7bc960`.
Same final-master-20261115 protocol. Both musl launchers explicitly enable jemalloc-default. No GNU allocator-cap claim.

- Matching: https://github.com/auenkr/cadence-rust/actions/runs/43752799238
- Frontend: https://github.com/auenkr/cadence-rust/actions/runs/43752802886
- History: https://github.com/auenkr/cadence-rust/actions/runs/43752806311
- All Rust: https://github.com/auenkr/cadence-rust/actions/runs/43752809662

Direct watcher retains compact summaries, diagnoses/retries a failed lane once, then commits comparison report and charts. Raw artifacts remain ignored. Frontend, History retry and All-Rust retry passed. Matching failed two attempts (resource-collector error in Go baseline recovery); retry 44321372113 passed all stages including recovery, so the gap is closed. Final report and charts: jemalloc-master-20261115.md and .html.
