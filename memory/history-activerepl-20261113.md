# history-activerepl (2026-11-13)

Goal: Rust History as ACTIVE source cluster (B), Go History standby (A, all Go).

## Harness
- `tests/gates/run-frontend-two-cluster-gate.py --rust-history-side active|standby` (default standby, unchanged); workflow input `rust_history_side` in `frontend-mixed-gate.yml` (use with two_cluster=true rust_history=true). `rust_yaml()` parametrised by cluster (ports, DB, seeds, ringpop name cadence-b); added A_MATCH/A_MATCH_GRPC ports for Go A matching (first run 43434693460 failed: Go matching bound port 1).
- Active flow: Go A (all services), Go B frontend+matching+worker, Rust History on B. Rows: act-start/signal/terminate-on-rust-active-B, act-go-standby-A-received-started/signal/terminated/describe-terminated, act-standby-history-equals-active (event types of `workflow show` equal on A and B).

## Result
- Run 43436024191 (head 5a1bdd4 area, branch history-activerepl): ALL rows pass, `GATE SUMMARY failed: []`. Rust source side (task generation/hydration, GetReplicationMessages, ack/reader) already produces full history for the Go standby: Started, Signaled, Terminated and identical event lists.
- No source-side defect reproduced, so NO Rust logic changed and no new unit tests (nothing fixed).
- Observed anomaly (not a replication failure): Rust history log shows 12 retries `matching add task: missing TTL` (ProcessingFailed attempt 0..11) once near workflow close against Go matching; message string not found in repo Go/Rust sources, origin unidentified, no row affected. Follow-up candidate.
