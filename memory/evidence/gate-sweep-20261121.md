# Gate sweep 2026-11-21

Branch gate-sweep-20261121 = origin/master f305a51f2a0fd2aa52d1d6bed5ea4f7bba7072a9 (no code changes).
Covers: persistence QPS limiter, shard pump, cluster_config polling, Box::pin, Frontend Arc metadata.
Backends supported by the gate workflows: mysql, postgres, cassandra (no sqlite lane exists).
Pass = `GATE SUMMARY failed: []` / run success. URL base: https://github.com/auenkr/cadence-rust/actions/runs/<id>

| Gate | mysql | postgres | cassandra |
|---|---|---|---|
| frontend-mixed-gate | PASS 44837090010 | PASS 44837099987 | PASS 44837107462 |
| history-mixed-gate | PASS 44837095478 | PASS 44837103560 | PASS 44837111538 |
| history-restartgate (1500s) | PASS 44837119554 | FAIL 44837122955 -> rerun PASS 44842586723 | FAIL 44837128942 -> rerun PASS 44842666256 |
| matching-mixed-gate | PASS 44837132721 | n/a (workflow is MySQL) | PASS 44837136245 (matching-cassandra-mixed-gate) |
| matching-active-offer-gate | PASS 44837140837 | n/a | n/a |

## First-attempt failures (restart gate, postgres and cassandra)
Both showed the identical signature: started=4500 completed=4499, failed=1, only soak-driver-exit,
zero-failed-workflows and all-started-completed failed; every fault scenario, liveness and no-panic step passed.
Postgres failing workflow fe-soak-...-0000322: child workflow failed ~61s after start (history #25
ChildWorkflowExecutionFailed), during the kill/outage window. Rust History logged "no matching hosts in the
membership ring" while a host was down (expected during kill -9 scenarios).
Reruns (one each) passed with zero failures, so classified as a 1-in-4500 fault-window flake. No Rust code changed,
so no rust-ci rerun was needed. Not root-caused further: if it recurs, check child-workflow failure under History
host kill (s1-kill9) in the soak-driver HISTDUMP.
