# Matching final validation candidate — 2026-10-26

Candidate branch: `matching-final-candidate`

Candidate before this evidence note: `8a3b8905fc37831e8ba0b00d2d8cb4a45dbc90ba`.

Assembled from master `97fcaa5c0`, `origin/t3/matching-launcher-fatal-test` at `ff91ef4bde65fe86972831f02177e7a6bd862102`, `origin/t3/refresh-engine-error-test` at `cfc7eab79a5a8ab24fe56f2b027e22af86f13be8`, and docs audit `79e941692be292764aebaeeecedb96878cef319a`. Acceptance matrix conflict was resolved preserving the detailed UpdateTaskListPartitionConfig contract audit and the active Refresh fault-injection test evidence. Merges and sources are present in candidate ancestry; `git diff --check` passed and no actual conflict markers remain.

The real-binary SQLite/gRPC fatal regression in `rust/crates/cadence-matching/tests/sql_writer_fatal_process.rs` covers timeout, rejected RPC, and process exit 2. Added `ChildGuard` so every early assertion/timeout path kills and waits for the child process. No broad production changes were made beyond the imported worker patches.

Focused runs checked once: [40222090786](https://github.com/auenkr/cadence-rust/actions/runs/40222090786) (`ff91ef4b...`) and [40222171242](https://github.com/auenkr/cadence-rust/actions/runs/40222171242) (`cfc7eab7...`) remained in progress. At that check, each had passed Go oracle and targeted format checks; the first had also passed Clippy and Formatting and was in Test, and the second was in Clippy. No terminal errors available to incorporate; no polling performed.

Fresh-target whole-workspace workflow dispatched: [40222337578](https://github.com/auenkr/cadence-rust/actions/runs/40222337578), exact head `8a3b8905fc37831e8ba0b00d2d8cb4a45dbc90ba`, `clean_target=true`. It was in progress at record time. This is a validation candidate, not a completion/parity claim. Mixed-cluster gates have not been dispatched per parent serial coordination.
