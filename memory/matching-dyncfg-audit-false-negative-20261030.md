# Matching dynamic-config reader audit false negative (2026-10-30)

## Source and failure

- Frozen source: `0032e7b3341275c028952daa82ba302d9ede0f02`
  (`origin/t3/matching-failure-coverage-20261030142037`).
- Fresh workspace run: https://github.com/auenkr/cadence-rust/actions/runs/41031374784
- Exact failure: `matching_dyncfg_defaults_tests::reader_defaults_match_go_table`
  panicked at `matching_dyncfg_defaults_tests.rs:23` with
  `matching.numTasklistWritePartitions: no Rust reader found`.
- Run summary: formatting and Clippy passed; 986 passed, 1 failed, 2 ignored.

## Diagnosis

This was a false negative in the test's source scanner, not a dynamic-config
parity failure. `production()` truncated each source file at the first
top-level `#[cfg(test)]`. In `grpc_matching.rs`, that marker gates the
test-only `QueryDispatchFault` alias near the top of the file. Production
partition readers follow it, so the scanner hid them.

Go defines `matching.numTasklistWritePartitions` with default `1` in
`go/common/dynamicconfig/dynamicproperties/constants.go`. The Rust production
readers use fallback `1` in `grpc_matching.rs` and
`matching_control_context_native.rs`; task-list filters are read through the
same dynamic-config helpers. Thus the implementation and intended table value
agree. No fixture, clock, ordering, environment, or concurrency behavior is
involved in this assertion.

## Fix and gate

- Candidate commit: `caa32882f9a3fd1da4450c8b216607c053996893`, based directly
  on the frozen source above.
- `production()` now ends at top-level test module declarations (`#[path] mod`
  or inline `mod`) rather than the first test-only item. This preserves
  production readers after test-only aliases.
- Fresh uncached workspace gate dispatched with empty `package` and
  `clean_target=true`: https://github.com/auenkr/cadence-rust/actions/runs/41033025800
- Run head SHA: `caa32882f9a3fd1da4450c8b216607c053996893`.
- At the recorded status check (2026-10-30 09:23 UTC), the gate was
  `in_progress`; no result is claimed here. The dev host was not used for
  Cargo, Go, formatter, or runtime validation.
