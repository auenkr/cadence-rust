# History sqlblobs contract adapters — 2026-11-04

## Implemented

- Added the typed `ActivityInfo` persistence contract and its sqlblobs `Struct` encoder/decoder in `rust/crates/cadence-persistence/src/history_contract.rs` and `sqlblobs.rs`.
- Mapping follows Go's sqlblobs field IDs. Time values remain Unix nanoseconds and timeout values remain seconds. Added a round-trip unit test covering a timestamp, duration, and retry string list.
- The Rust codec models generated sqlblobs records as `cadence_thrift::Struct`; it does not currently have generated typed sqlblobs record types, so this follows the existing `*_to_struct` / `*_from_struct` adapter pattern.

## CI evidence

- Branch: `history-sqlblobs-adapters`
- Validated head: `06a9fbf2bbc88a45685d62cc938c31e77b7c67e2`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41804173705
- Command: `gh workflow run rust-ci.yml --ref history-sqlblobs-adapters -f package=cadence-persistence`
- Conclusion: `success` (cadence-persistence Clippy and tests)

## Remaining work

- This task is incomplete: adapters and round-trip tests remain for `ChildExecutionInfo`, `SignalInfo`, `RequestCancelInfo`, `TimerInfo`, and `WorkflowExecutionInfo`. The successful CI run validates the committed ActivityInfo change only.

## Follow-up implementation and CI evidence

- Added typed `ChildExecutionInfo`, `SignalInfo`, `RequestCancelInfo`, `TimerInfo`, and distinct `SqlWorkflowExecutionInfo` contracts with `*_to_struct` / `*_from_struct` adapters in `rust/crates/cadence-persistence/src/{history_contract.rs,sqlblobs.rs}`. The field IDs and wire types follow the embedded Go sqlblobs IDL; timestamps remain Unix nanoseconds and duration values remain seconds.
- Added round-trip tests for all five records using non-default values across their fields.
- CI run: https://github.com/auenkr/cadence-rust/actions/runs/41869012362
- Validated head: `2b3676b0fa83e6f684c35f4e7ef32fbd53ed4fdf`; conclusion: `success` (cadence-persistence Clippy and tests).
