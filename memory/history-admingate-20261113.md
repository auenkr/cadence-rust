# History admin gate (branch history-admingate) — 2026-11-13

Gate rows `history-admin-*` (40 per run) drive the Go `cadence admin ...` CLI against the mixed cluster
(Go frontend/matching/worker + Rust History) over grpc and tchannel. Driver: `tests/gates/history_admin_cli.py`,
wired into `tests/gates/run-frontend-gate.py`; probes in `tests/gates/admin_probe.py`.

## Result
Head SHA tested: 611019d209702f4e36ee7417d968813b79f3a7c3. rust-ci: run 43484480001 success.
All 12 history-mixed-gate combos (database mysql|postgres|cassandra x two_history true|false x outbound grpc|tchannel)
logged `GATE SUMMARY failed: []`:
43484486257, 43484491727, 43484496843, 43484503361, 43484508287, 43484513199,
43484518248, 43484527533, 43484532629, 43484537822, 43484543464, 43484549327
(https://github.com/auenkr/cadence-rust/actions/runs/<id>).

## API table
| API | Status | Notes |
|---|---|---|
| DescribeHistoryHost | FIXED | Rust returned partial response; now full Go response |
| DescribeShardDistribution | PASS | |
| CloseShard | PASS | |
| RemoveTask | FIXED | proto task type passed through (double map gave "Invalid task type"); owner redirect |
| ResetQueue / DescribeQueue | FIXED | route by shard owner + ShardOwnershipLost redirect, on both gRPC and tchannel paths |
| DescribeMutableState | PASS | |
| RefreshWorkflowTasks | PASS | |
| GetDLQReplicationMessages | FIXED | routed by workflow-id shard owner |
| ReadDLQMessages / PurgeDLQMessages / MergeDLQMessages | FIXED | routed by shard owner (two-history); merge w/o cluster gives Go "Invalid target cluster name." |
| CountDLQMessages | PASS | tchannel: Go "Feature not supported on TChannel" |
| GetCrossClusterTasks / RespondCrossClusterTasks | FIXED | shard-owner routing |
| ReapplyEvents | PASS | |
| Shard ack-level admin calls | N-A | no Go CLI command exposes them |

Also: ringpop `history_host` resolves by TChannel address too.

## Re-validation after merging origin/master
Head a17df1fecd06c1037c51c526a4921978da5185a9 (master merged, no conflicts): rust-ci 43487824739 success,
rust-check 43487824532 success; all 12 gates `GATE SUMMARY failed: []`:
43487829934, 43487842389, 43487854915, 43487869582, 43487882676, 43487895029,
43487909807, 43487920788, 43487935112, 43487949286, 43487963701, 43487980482.
