# perf-append-borrow (2026-11-24)

## Change
- Added `cadence_thrift::history_blob::encode_slice(&[HistoryEvent])`; `encode(Vec)` now delegates to it (byte-identical).
- Removed clones feeding encoding: `engine_ndc.rs` (`events.to_vec()`), `mutable_state_bridge.rs` (`new_buffered_events.clone()`), `engine_close.rs` (`update_buffered.clone()`), `replication_builders.rs` (`b.events.clone()`).
- Unit test `borrowed_encode_matches_owned_encode` compares owned vs borrowed bytes (3-event batch and empty batch).

## CI (head dc55aec38974db3f4f7c06d1018c24e317f1a53e)
- rust-ci cadence-thrift: https://github.com/auenkr/cadence-rust/actions/runs/45405596129 success
- rust-ci cadence-history (on efdb5bd97, before test-only fix, which touches only cadence-thrift): https://github.com/auenkr/cadence-rust/actions/runs/45404745877 success
- (first cadence-thrift run 45404740804 failed: test used `timestamp: i64` instead of `Option<i64>`; fixed)
- history-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45406278948 success, `GATE SUMMARY failed: []`

Conclusion: pass.
