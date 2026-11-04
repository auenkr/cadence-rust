# History sqlblobs codec work — 2026-11-04

## CI evidence

- Branch: `history-sqlblobs`
- Validated code head: `e8b0d6be3c046658440cfdc6e3bf09f4704f4964`
- Workflow: [rust-ci run 41798998970](https://github.com/auenkr/cadence-rust/actions/runs/41798998970)
- Command: `gh workflow run rust-ci.yml --ref history-sqlblobs -f package=cadence-persistence -f clean_target=true`
- Conclusion: `success` (fresh runner target; cadence-persistence Clippy followed by crate tests)
- The Go `TestPrintSQLBlobsGoldenHex` oracle ran in CI and printed fixtures for all 13 requested structs. Its artifact is `sqlblobs-go-golden-fixtures` (retention 3 days).

## Implemented

- Added `rust/crates/cadence-persistence/src/sqlblobs.rs`, a raw ThriftRW struct codec and `thriftrw` / `thriftrw_snappy` selection, compression, and decompression. It reuses the Matching Thrift value reader/writer.
- Added Go-generated golden hex fixtures for `ShardInfo`, `WorkflowExecutionInfo`, `ActivityInfo`, `ChildExecutionInfo`, `SignalInfo`, `RequestCancelInfo`, `TimerInfo`, `TransferTaskInfo`, `TimerTaskInfo`, `ReplicationTaskInfo`, `CrossClusterTaskInfo`, `HistoryTreeInfo`, and `HistoryBranchRange`.
- Rust CI tests decode and re-encode all 13 Go fixtures byte-for-byte. The CI test `sqlblobs::tests::all_go_sqlblobs_golden_fixtures_decode_and_reencode_byte_for_byte` passed.
- Added typed conversions to/from `history_contract` for `TransferTask`, `TimerTask`, `ReplicationTask`, `HistoryTreeInfo`, and `HistoryBranchRange`. Added replication task event-store version fields to the typed contract.
- Updated `.github/workflows/rust-ci.yml` push triggers to include `history-*` branches. Added the Go fixture oracle step, run on the Actions runner.
- Matching's existing `TaskInfo` and `TaskListInfo` codecs remain in `matching_blob.rs`; this work did not duplicate them.

## Remaining gaps

- Contract conversions are still missing for `ShardInfo`, `WorkflowExecutionInfo`, `ActivityInfo`, `ChildExecutionInfo`, `SignalInfo`, `RequestCancelInfo`, and `CrossClusterTaskInfo` (the cross-cluster wire payload is the transfer-task shape, but no separate contract adapter has been added).
- The 13-struct golden test currently validates raw ThriftRW field decoding/re-encoding. It does not validate typed contract conversion for every struct, and there are no Go-generated `thriftrw_snappy` golden fixtures yet.
- The codec is not yet wired into a SQL execution-store implementation, so this work alone does not unblock end-to-end SQL execution persistence.
- CI covered `cadence-persistence` only, as requested for this task; no mixed Go/Rust History cluster gate was run.
