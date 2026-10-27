# Matching integration and diagnostic control

Integrated Go cache oracle and review, Go Worker startup readiness coordination,
and original-Go adaptive control. No production Rust, Go, IDL or configuration
logic was changed by this integration. The Cassandra normalization patch remains
unmerged: it would change Go's raw-cache-key miss behavior for recognized UUID
spellings. Original-Go overlay oracle40470232857 passed atb93b162061cb32b62caecc22673241579edbec30;
actual downloaded log/exit/source proof retained in
`memory/evidence/matching-go-cache-oracle-40470232857/`.

The oracle mocks successful persistence existence and canonical refresh rows;
it proves cache caller behavior, not backend UUID parsing or RPC metric/quota
attribution. The review note was corrected to distinguish the unmerged patch
from current production. A separate canonical cold-cache metric attribution
source trace remains useful because Rust's metric resolver is cache-only.

Startup fix waits for the original Go `worker started` log after system domain
creation, while retaining process checks and the90s startup deadline. This
addresses the sourced Worker fatal registration race in Thrift40469948997.
Worker focused CI40471364062 on2d92ab568 was pending at handoff; combined CI and
the actual mixed repeat still need their own evidence.

Original-Go control uses `lane=adaptive-go-control` in production validation.
Parent review corrected initial fixture issues: create out before writing,
record source and Go binary SHA, run through the capped/cleaned MySQL container
wrapper, permit the diagnostic all-Go MySQL/idle/retry-only options, include
the same isolation flags and15min outer probe limit, preserve the same60s SDK
budget/24completion/routing assertions, and set Go RPC routing metadata.
No synthetic durable-row count is supplied. Result text names original Go
Matching, and acceptanceEligible:false remains explicit.

Reproduce control after pushing integrated source:

```sh
gh workflow run matching-production-validation.yml --ref SOURCE_REF \
  -f lane=adaptive-go-control -f adaptive_retry_only=true
```

Run only one runtime gate at a time. Control uses all four Go services and is
never Rust Matching acceptance. Final combined source must still pass fresh
workspace CI and all required Rust mixed lanes.
