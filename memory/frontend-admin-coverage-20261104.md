# Frontend admin API coverage (2026-11-04)

Branch `t3/frontend-admin-coverage`, gate head 2595ac438ed5384f9caa9924e66338ca889a1fea (evidence file committed after). Only gate files changed; no Rust change was needed.

## Covered (Go CLI `admin` against Rust Frontend, TChannel + gRPC; tests/frontend-gate/api.go adminCLI, tests/gates/run-frontend-gate.py tail)
- DescribeShardDistribution: `admin shard list` asserts "Total Number of Shards" and, with --pagesize 2, "Number of Shards Returned: 2".
- DLQ: CountDLQMessages (gRPC works; TChannel returns Go's "Feature not supported on TChannel", asserted), Read/Purge/Merge for domain and history DLQ (success or typed error, no routing/internal error).
- Queue admin: DescribeQueue (transfer, timer), missing-cluster rejection; ResetQueue timer (run last).
- Shard admin: CloseShard, RemoveTask (run last, disruptive: shards re-acquire).
- DeleteWorkflow (--remote), MaintainCorruptWorkflow (fix_corruption) on non-existent workflow: typed result.
- Dynamic config: listall, operational-get/list.
- Already covered earlier: DescribeCluster, DescribeHistoryHost, refresh-tasks, isolation groups, async-wf-queue, dynamic config CRUD.
- Review of Rust handlers (yarpc.rs thrift arms, admin.rs) against go/service/frontend/admin/handler.go found validation and routing present for all listed APIs; no divergence fixed. Not asserted: stateful DLQ content (no DLQ messages exist in gate), replication admin beyond the two_cluster lane (GetReplicationMessages/GetDomainReplicationMessages are exercised by the replicating postgres two_cluster lane itself), ReapplyEvents/GetCrossClusterTasks (no CLI command).

## Gate lessons
- CloseShard/ResetQueue/RemoveTask on 4 shards break later ratelimiter steps in the same run; they now run at the very end.
- CountDLQMessages is gRPC-only in Go.

## CI evidence (repo auenkr/cadence-rust)
| Run | Gate | Conclusion | Rust SHA256 |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/41709928032 | rust-ci | success | n/a |
| https://github.com/auenkr/cadence-rust/actions/runs/41717328399 | mysql kafka two_history | success | 091e635f5612a34d627505c91771b4f00b1970b40782ed74e5ce2cad94ce65ae |
| https://github.com/auenkr/cadence-rust/actions/runs/41719301582 | postgres two_cluster | success | 193d4e938c94c4d473afcb768a93f1a968040bd1e800c3badc7fc32cd4d04e46 |
| https://github.com/auenkr/cadence-rust/actions/runs/41720780482 | cassandra | FAILURE (global-ratelimiter-global-shadow-local-*) | 0f0d5b8fa64ceac28391ad4fb52fa490a964fcd4bb5a389add0b942161af0ce4 |
| https://github.com/auenkr/cadence-rust/actions/runs/41723108226 | cassandra rerun | FAILURE (same) | 466fea8ad589722c441349d289fd21a8ff33b5457321d67ed105112063321743 |
| https://github.com/auenkr/cadence-rust/actions/runs/41725448591 | cassandra on unmodified master 8affaec33 | FAILURE (same) | 15377ae1fa736fb625cc6b79e5b21fe23ab2f5443f24c5d9f544c345cffcee04 |

## Open: pre-existing Cassandra regression (not caused by this branch)
Cassandra lane fails on master too. Go worker panics in PollForDecisionTask thrift adapter: `timestamp: &types.Timestamp{Seconds: 1793743359616951, Nanos: 616951...} after 10000-01-01` - an event/decision timestamp in nanoseconds went through the ms-based `timestamp()` in cadence-proto/src/convert/mod.rs (or a Rust HistoryEvent still holding ns, like the archive bug in frontend-archival-cloud-20261103.md). Worker dies, later workflow starts fail ("Failed to create workflow"). api-coverage cassandra run 41707320501 passed, so it is intermittent or from a later merge; needs separate investigation (artifact go-history-worker.log).
