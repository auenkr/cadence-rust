# Matching CI run 40840392628 — terminal review

This supersedes the initial in-progress status note. The run completed with
workflow/job conclusion `success` at exact head
`d4ee53f00d505246dc0b7fbbe353d7e989a91c49` on branch
`t3/matching-startup-integration-review`. The uploaded commit identity agrees.

The run selected `cadence-matching`: Clippy and tests both exited 0. The
Matching library test harness reports 973 passed, 2 ignored. The 27 result
summaries sum to 1,083 passing results, 0 failures, and 16 ignored (including
filtered/repeated invocations). Scoped changed Matching runtime, query, TLS,
and selected format exits are 0. Advisory workspace-wide formatting exit is 1,
with a diff in unrelated `rust/crates/cadence-api/src/admin.rs:371`; the
workflow captures this and continues. Rust cache was restored, so this was not
a fresh uncached workspace gate.

Exact compact logs and exit files are retained in
`memory/evidence/matching-startup-candidate-ci-40840392628/`. The test log
contains cold-domain metric, gRPC listener startup/Meta Health, native startup
gate, and Thrift startup profiling tests. This head's commit changed no Rust
test files. Do not credit subsequent semaphore startup-timeout, expanded
adapter/lock or two-thread tests, nor candidates `25e`, `90cd`, `e84e`, or `8cc`.
This selected package run does not validate frozen `bf9f3d3`, whose PostgreSQL
mixed attempt failed before SDK execution; all ten mixed lanes remain required.
