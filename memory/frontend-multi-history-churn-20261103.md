# Frontend multi-History failure/recovery — 2026-11-03

Branch `frontend-multi-history-gate` (from master 439300f77). Rust source head validated: `7ed9b495d0d43b46b11bbb90fe3acb87cf7a4054`.

## Changes
- `workflow_backend.rs:history()`: owner is re-resolved from Ringpop membership on every attempt; `ShardOwnershipLost` and `ServiceBusy` (incl. transport failure to a dead host) are retried with linear backoff (250ms*min(n,8), max 20 attempts, 25s budget), mirroring go/client/history redirect+retry. No other Rust bug surfaced in the gate (membership refresh and connection handling worked unchanged).
- Gate: `frontend-mixed-gate.yml -f two_history=true` -> `run-frontend-gate.py --two-history` starts a second Go History host (`config2/`, own ports, same Ringpop) beside Go History+Worker, Rust Matching, Rust Frontend. SDK driver (`FE_CHURN=1`) runs 4 concurrent loops of start+complete echo workflows (random IDs spread over 4 shards) while the harness SIGKILLs History-2, waits 20s, checks History-1 and Rust Frontend alive, restarts History-2 and waits 30s. Existing Rust Frontend SIGKILL/restart step still runs afterwards.

## Evidence
- Mixed gate (mysql, kafka off, two_history) https://github.com/auenkr/cadence-rust/actions/runs/41547120117 — head `7ed9b495d0d43b46b11bbb90fe3acb87cf7a4054` — **success**, 81 steps, 0 failed; `sdk-history-churn-workflows` ok: 766 workflows completed during churn; no stuck requests. Rust binary SHA256 `9b3377d0146bdb9b326bda3f70d1d19b51a8bbabd02fbc1c344394eb6ff7e7ba`.
- Earlier run 41545658721 (head 98c9922d5) was green but the churn phase was not wired into the driver (call-site edit missed); it proves nothing about churn and is superseded.
- rust-ci (clean_target=true) https://github.com/auenkr/cadence-rust/actions/runs/41548118343 — head `7ed9b495d0d43b46b11bbb90fe3acb87cf7a4054` — **success**.
- Reproduce: `gh workflow run frontend-mixed-gate.yml --ref frontend-multi-history-gate -f two_history=true -f kafka=false -f rust_ref=frontend-multi-history-gate`.

## Limits
MySQL lane only; only the non-worker History host is killed (host with Worker not killed); no unit test added for the retry loop; Postgres/Cassandra variants not run.
