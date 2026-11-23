# history-p2gaps (2026-11-12)

Branch `history-p2gaps` from origin/master 105718ea8. CI only (no local cargo/go/docker). Per-item DONE/OPEN table: bottom of
`memory/history-parity-audit-20261112-v2.md`.

## Ported (Go -> Rust)
- `closeTransactionWithPolicyCheck`: `wiring/assembly/engine_close.rs`, `ClosePolicy` in `engine_state.rs`, wired in `engine_factory.rs`; test `active_close_in_standby_version_is_domain_not_active`.
- `generateChecksum`: probabilistic, `StateParams.checksum_gen_probability`, JSON blob `{"Version","Flavor","Value"(base64)}` as Go `SerializeChecksum(JSON)`; tests `checksum_json_matches_go_shape`, `checksum_generated_when_probability_is_100`. Verify-on-load and `shouldInvalidateChecksum` still OPEN.
- `validateNoEventsAfterWorkflowFinish`: test `events_after_workflow_finish_rejected_only_for_active_completed`.
- `DeleteTransferTasks/DeleteTimerTasks`, `logDataInconsistency` (log only) in `execution/mutable_state_builder.rs`.
- `failoverMarkerSkipReason` in `wiring/assembly/failover.rs` (+ drop log, test).
- Archiver client metrics (`ArchiverClient` scope; history/visibility request, inline attempt/failure/throttled, send-signal, service-busy) in `archiver_client.rs`, wired in `env.rs`.
- `tests/gates/run-frontend-soak.py`: 100-attempt port bind retry.

## OPEN
- HistoryExecutionCache metrics (Go `execution/cache.go`), task_notifier shadow-mode drop log, ReloadMutableState 1:1 (inlined), DataInconsistentCounter wiring, non-blob JSON tags (not wire-affecting for SDKs), real S3/GCS endpoints (needs credentials).

## CI evidence (head SHA e4eea63608600b4a9843c50bd0dc1073715ed412, origin/master merged; all success)
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/43224263413
- gate mysql/grpc/two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43224268907 (GATE SUMMARY failed: [])
- gate cassandra/tchannel/true: https://github.com/auenkr/cadence-rust/actions/runs/43224274492 (GATE SUMMARY failed: [])
- gate postgres/grpc/false: https://github.com/auenkr/cadence-rust/actions/runs/43224280484 (GATE SUMMARY failed: [])
(Earlier rust-ci on d9c9659ad, same Rust code: run 43221840578 success; a rust-ci failed twice before on clippy/compile, fixed.)

## Post-merge master validation

Master ff1a5f415 (2026-11-12): 43226512773: success 43226521682: success 43226529439: success 43226536476: success 43226543748: success 43226550283: success 43226556923: success 43226564271: success 43226569964: success 43226574961: success 43226581953: success 43226589356: success 43226594566: success 
