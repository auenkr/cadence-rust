# Matching absent Query fields (nil request/Query/result)
Go refs: go/service/matching/handler/handler.go QueryWorkflow (L293) and RespondQueryTaskCompleted (L326) use nil-safe getters, panic captured by log.CapturePanic; engine.go QueryWorkflow (L1015), RespondQueryTaskCompleted (L1092), deliverQueryResult (L1100, EntityNotExists "query task not found, or already expired").
Rust: merged origin/t3/matching-absent-query-fields (nil-safe QueryWorkflow, QueryShape, nil-deref message on poll) into t3/matching-integration-20261025; reused existing NIL_DEREFERENCE_MESSAGE.
CI: see below (filled after run).
CI green: https://github.com/auenkr/cadence-rust/actions/runs/40187504085 on head 7e04a5755 (package=cadence-matching, clippy+test).
Caveat: empty default task list cases are rejected by Rust's partition-drain check (Aborted) before reaching the poller; Go parity for that sub-case not established. Absent-Query cases verified (poller gets nil-deref Unknown, caller hits deadline).
