# History batch gap audit and replication validation — 2026-11-15

Branch: `history-batch-gap`
Code validation head: `f1e4dd9dc9beb0a5593052558f28f60245bf074b`
Base: `origin/master` at `af113cb0f80e897b13d7bbb3df65f33158467fa3`

## Trigger

Prior evidence in [`repl-flake-p2b-20261115.md`](repl-flake-p2b-20261115.md) described run `43647065597`: the Go Frontend failed to read the Rust History standby's history with `corrupted history event batch, eventID is not continuous`, while the standby mutable state was at event 35 and its history read returned no events. Existing SQL-row evidence was unavailable, so that failure did not establish which persistence operation caused the inconsistency.

## Targeted parity audit

Compared Go `historyManager.go`, `sql_history_store.go`, the Go nDC replication executor/replicator, and the relevant Rust replication, branch, transaction, SQL history-store, and history-reader paths.

- **Concrete write-boundary mismatch fixed.** Go `historyV2ManagerImpl.AppendHistoryNodes` in `go/common/persistence/history_manager.go` rejects an empty batch, nonpositive first event ID, any event whose version differs from the first event, and any event whose ID is not the previous ID plus one. Rust `SqlHistoryManager::append_history_nodes` in `rust/crates/cadence-history/src/wiring/persistence/engine_ndc.rs` previously checked only nonempty/positive first ID before serializing and persisting. A gap or duplicate in a supplied batch could therefore enter Rust `history_node` and later be rejected by a Go reader. Rust now validates continuity and same-version batches at the persistence boundary, with an equivalent InvalidPersistenceRequest category and a unit test for gaps, mixed versions, and valid adjacent events.
- **Replicated task application and event IDs.** Rust and Go both route HistoryV2 attributes and event blobs to the nDC history replicator/state builder, apply the same current-branch events and transaction-manager workflow update, and use version histories for branch selection. Failover markers are processed as domain failover metadata; they do not append a history batch or assign history event IDs. I found no demonstrated Rust-only failover/version event-ID assignment change. The append validation closes the demonstrated possible malformed-batch write path even if an upstream caller supplies a bad split/merged/retried batch.
- **Transaction IDs and SQL rewrite selection.** History node append receives a shard task ID as transaction ID. Rust's task ID generator advances the shard sequence under its lock; Go's path likewise uses shard-generated task IDs. Both Go SQL plugins store the transaction value with the descending SQL ordering behavior; Rust negates IDs on SQL insert and restores the sign on read. Go and Rust SQL readers order by node then transaction, ignore stale lower-transaction rows, and reject node order violations or a duplicate node with a non-stale transaction. No reproducible smaller/equal-transaction mismatch was found in the Rust allocation path.
- **Branch, ancestor range and node de-duplication reads.** Go `historyV2ManagerImpl.readRawHistoryBranch` and Rust `read_history_branch` select the same intersecting ancestor ranges followed by the current branch, cap each physical range at its exclusive end, and carry continuation range, store page token, last node and transaction state across pages. Rust `store_read` mirrors Go's stale-row skip and same-node validation. No wrong-branch/range selection or missing node de-duplication mismatch was found.
- **Limits of root cause.** The validation mismatch is real and could have persisted a malformed batch, but the old failure artifact did not include DB rows or the source AppendHistoryNodes request. Therefore the original incident's root cause is still not proven to be a particular malformed write. This patch rejects invalid batches and adds diagnostics to make any repeat localizable.

## Failure-only MySQL diagnostics

`tests/gates/run-history-repl-gate.py` writes `failure-workflows.json` when a history-equality poll fails, including workflow/run/domain, active/standby cluster and gate step. Before the MySQL container is removed, `run-history-repl-lane.sh` invokes `dump-history-repl-mysql.py`. The resulting `out/gate/mysql-failure-rows.txt` includes workflow execution and current-execution rows, history_node/history_tree rows for the workflow's shard, and replication task/DLQ rows for both cluster databases. Binary payload columns include byte length plus a bounded 4096-byte hex prefix to keep evidence small. The normal success path does not invoke the dumper or alter gate predicates. The existing always-upload artifact step includes this file and retains it for 3 days.

## CI and mixed-cluster runs

All runs below used code head `f1e4dd9dc9beb0a5593052558f28f60245bf074b`.

| Validation | Run | Verdict / binary SHA256 |
|---|---|---|
| `rust-ci.yml`, `package=cadence-history` | https://github.com/auenkr/cadence-rust/actions/runs/43680535140 | success: clippy, test, formatting and PostgreSQL pool wakeup |
| `rust-ci.yml`, full workspace | https://github.com/auenkr/cadence-rust/actions/runs/43681009979 | success: clippy, test, formatting and PostgreSQL pool wakeup |
| MySQL replication, Rust History cluster B, tag `history-batch-gap-1` | https://github.com/auenkr/cadence-rust/actions/runs/43680547990 | success; `GATE SUMMARY failed: []`; `1f5aea6ed089818e3bfa38c3497f68832c9471fbf39f89d2d224c871d4cefe47` |
| MySQL replication, Rust History cluster B, tag `history-batch-gap-2` | https://github.com/auenkr/cadence-rust/actions/runs/43680547529 | success; `GATE SUMMARY failed: []`; `fec0a4193d46c177f2ffd117255df661997b80770479465ba1f1f8540c4f4794` |
| MySQL replication, Rust History cluster B, tag `history-batch-gap-3` | https://github.com/auenkr/cadence-rust/actions/runs/43680547978 | success; `GATE SUMMARY failed: []`; `de96f6cd5b0c3f907b67fbf7f0a219564faacb165698d250bc569eae6bc8651d` |
| MySQL replication, Rust History cluster B, tag `history-batch-gap-4` | https://github.com/auenkr/cadence-rust/actions/runs/43680547968 | success; `GATE SUMMARY failed: []`; `c0bfebf9afd6f13f7c28e90ffd43c270c72bcdc344006b59afc9ed66bcc3f13f` |
| MySQL replication, Rust History cluster B, tag `history-batch-gap-5` | https://github.com/auenkr/cadence-rust/actions/runs/43680548007 | success; `GATE SUMMARY failed: []`; `fa674d19a65e32aebb1279ec7a17f4e9ab958cc5d341195eda473d47cefa1e65` |

The five mixed-cluster reruns did not reproduce the old incident. Successful runs did not produce failure SQL snapshots, as intended. No local Cargo, Go, or Docker commands were run.
