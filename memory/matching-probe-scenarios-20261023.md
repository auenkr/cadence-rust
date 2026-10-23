# Matching gate probe scenarios (added 2026-10-23, branch matching-probe-scenarios-20261023)

Status: **written, not run.** Validated only with `node --check`, `python3 -m py_compile` and
`--dry-run` (exit 0). The first real run may expose probe bugs; treat a failure as a probe
failure until the assertion is confirmed against Go behavior. No Rust production code changed.

Code: `tests/matching-probe-scenarios.cjs` (scenarios), hooked into `tests/matching-service-compat.cjs`
before the SDK run; flags in `tests/run-go-service-baseline.py`. Each flag sets `CADENCE_PROBE_*` for
the probe, defaults off, and adds a key under `probeScenarios` in the probe's final JSON (also
written to `<artifacts>/<key>.json`). All need `--rust-services matching`. Build binaries first and
run one gate at a time (see `tests/gates/README.md`). Add `--rust-production-binary PATH`.

| Flag | JSON key | Extra requirement |
| --- | --- | --- |
| `--probe-pg-gc-domain` | `postgresGcDomain` | `--postgres` |
| `--probe-query-standby` | `queryStandbyForwarding` | none |
| `--probe-list-partitions` | `listTaskListPartitions` | best with `--matching-failure-recovery` (2 nodes); add `--matching-transport thrift` for the Thrift run |
| `--probe-durable-restart` | `durableRestartDrain` | `--matching-failure-recovery` optional (multi-node) |

Command shape (example for all four, PostgreSQL):
`python3 tests/run-go-service-baseline.py --rust-services matching --rust-production-binary $BIN --postgres --matching-failure-recovery --probe-pg-gc-domain --probe-query-standby --probe-list-partitions --probe-durable-restart --artifacts-dir /mnt/storage/service/gate-runs/probe-scenarios --evidence-dir memory/evidence/matching-probe-scenarios-<date>`
Run `--probe-list-partitions` in its own run: it writes `matching.numTasklistWritePartitions=3` /
`ReadPartitions=3` into the Go and Rust dynamic config for the whole run.

## 1. `postgresGcDomain`
Runs the MySQL lane's `verifyProductionMySqlTaskGc` and `observeProductionMySqlDomain`
(`tests/matching-mysql-gate.cjs`) through `postgresQuery`, which rewrites `UNHEX/HEX/is_global` to
PostgreSQL. Starts one workflow on an acked list (decision polled and completed) and one on a never-polled
list. **Pass:** `result: passed`; acked list task rows reach 0, the persisted thriftrw ack level covers the
watermark read before delivery, range id is the live leased range, the pending list keeps >= 1 row;
`domains` has exactly one thriftrw row for the domain and `domain_metadata` versions are >= 1.
Not covered: the activity-side queue, restart queues (the MySQL lane also checks them).

## 2. `queryStandbyForwarding`
Sets a 2/2 native partition config on a decision list. The only poller (`query-standby-root-poller`)
polls the root partition directly. One QueryWorkflow goes straight to Matching for the child partition
(`/__cadence_sys/<list>/1`), then 6 go through Go Frontend (its balancer reaches the child at least once
with high probability, not deterministically). **Pass:** every query is delivered to the root-only poller,
answered, and the answer text matches. Go refs: `matcher.go` PollForQuery/OfferQuery, forwarder.

## 3. `listTaskListPartitions`
Go `engine.go` `ListTaskListPartitions` (`:1158`) returns, per task list type, `getAllPartitions`
(`:1321`) = `[root, /__cadence_sys/<root>/1 .. n-1]` with `n = matching.numTasklistWritePartitions`
(3 here), `OwnerHostName` = ring owner of each key; a partition name resolves to its root.
**Pass:** on every Rust node (gRPC) and via Go Frontend (TChannel/Thrift to Matching when run with
`--matching-transport thrift`), decision and activity lists have exactly these keys in this order, owners
are ring members, all sources return identical responses, and for each partition only the reported owner
answers `DescribeTaskList` (others return typed ABORTED). Go Frontend `DescribeTaskList` must return status.
Limit: the owner is checked for consistency with serving, not recomputed from Go's hash ring.

## 4. `durableRestartDrain`
Sets 3/3 partitions, persists one backlog activity task in each of root, `/1`, `/2` (no poller), waits
for the rows, SIGKILLs and restarts the primary Rust Matching (same data, Go services up), then polls
child 1, child 2, root. **Pass:** each partition returns its own activity id and the activity completes.
With two nodes, partitions owned by the survivor are served by it; rows must not be lost.
