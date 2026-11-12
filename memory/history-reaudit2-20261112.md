# history-reaudit2 (2026-11-12)
Branch `history-reaudit2` from origin/master f8914f198. CI only.
Done: ClusterAttributeNotFound `%s` text, Go-exact LRU policy cache, archival `attempt` omitempty, buffered-events-limit in engine close, audit v2
(`history-parity-audit-20261112-v2.md`). Open: P1 `startTransactionHandleDecisionFailover` (see audit v2).
Evidence section below is filled from CI runs.

## Evidence (head 2193cc009efcaf8d69a3c5434e3ef3abcfd22053)
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/43203851157 — success
- mysql/grpc/two_history: https://github.com/auenkr/cadence-rust/actions/runs/43203881826 — success, `GATE SUMMARY failed: []`
- cassandra/grpc/two_history: https://github.com/auenkr/cadence-rust/actions/runs/43203887617 — success, `GATE SUMMARY failed: []`
- postgres/tchannel/single: https://github.com/auenkr/cadence-rust/actions/runs/43203892931 — success, `GATE SUMMARY failed: []`

Not done: P1 `startTransactionHandleDecisionFailover` not ported (see audit v2); no unit test added for the buffered-events-limit change.
