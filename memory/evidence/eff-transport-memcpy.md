# eff-transport-memcpy (2026-11-21)
Branch eff-transport-memcpy from master 8929d366a. Change commit (and gated SHA) 49c0655d9a81b0bfcbf8944f826babae2c066b7a.

## memcpy callers (profile run 44814684776, master 8929d36, service=all, 25/s, profile=1, mysql_tmpfs)
memcpy self = 7.59% of all samples (all Rust services merged). No dominant caller; leaf-memcpy parents (% of samples):
tokio task spawn of tchannel futures (JoinSet/new_task over connection closure + handle_call) ~0.85; tokio worker poll loop 0.54;
tchannel handle_call 0.36; ForwardingBackend/Handler futures 0.31; NativeMatchingService 0.27; BTreeMap<String,..> clone_subtree 0.22 (+0.13 task-local);
read_struct 0.22 (thrift Value::Bytes copy) + history_blob::decode 0.18; History engine_state/close_transaction/record_decision_task_started 0.13-0.27 each.
The tchannel frame path itself (FrameReader, response_frames) does not show as a significant caller (frames are <=64KB).

## Change (49c0655)
`cadence-matching/src/tchannel.rs`: `Box::pin` the `handle_call` future (per-call spawn) and `connection` future (per-connection spawn), so
`JoinSet::spawn` memcpys a small future instead of the large inlined state machine. No wire/logic change.
Not changed: response_frames copy into frame payload and thrift Value::Bytes accessors (copy needed by API; no measurable share).

## Validation (all success)
- rust-ci cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/44820001522
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44820013700
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44821245939 (`GATE SUMMARY failed: []`)
- history-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44821251325 (`GATE SUMMARY failed: []`)
- matching-mixed-gate (MySQL): https://github.com/auenkr/cadence-rust/actions/runs/44821263201 (workflow success; no GATE SUMMARY line, run success counts not inspected)

## A/B (service=all, 25/s, 60s, 2 pairs/run, mysql_tmpfs). Rust minus Go avg CPU %, mean of 2 pairs (frontend / history / matching)
| run | code | CPU model | FE | History | Matching |
|---|---|---|---|---|---|
| 44823432754 | before | EPYC 7763 (rejected) | -43.6 | -57.9 | -25.1 |
| 44823445201 | after | EPYC 7763 (rejected) | -44.7 | -56.8 | -22.0 |
| 44823441721 | before | Xeon 6973P-C | -46.3 | -53.0 | -27.8 |
| 44823449558 | before | Xeon 6975P-C | -48.8 | -56.1 | -34.2 |
| 44823437325 | after | EPYC 9V74 | -34.5 | -50.4 | -14.9 |
| 44823456403 | after | EPYC 9V74 | -45.5 | -57.8 | -32.2 |
No valid same-CPU pair: the only matching model is EPYC 7763, which is rejected; the 9V74/Xeon runs have no counterpart. Between-run noise (9V74 vs 9V74: 11 pts FE) exceeds any
expected gain (<1% of samples). Verdict: no CPU improvement can be claimed; change is correct, gated, and removes ~0.85% memcpy of samples.
