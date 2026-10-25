# Matching checkpoint — 2026-10-25 16:20 Asia/Kolkata

This supplements matching-pause-handoff-20261025.md. The scheduled resume revoked
the old pause. Matching remains incomplete; no future-service work is started.
At this checkpoint the live thread listing showed no running/queued/waiting child
threads. No implementation branches were merged or deleted.

## Saved implementation and evidence

- Request parity: `t3/matching-go-error-parity`, `4d7a37a3c7dcdbfc945bbfe2dffe7205b6a55c96`.
  Run https://github.com/auenkr/cadence-rust/actions/runs/40186297345 is in progress.
  Prior 40185780201 failed: Thrift fixture omitted args field 1. Latest fix wraps
  the valid activity request in that field; production code was unchanged.
- Raw RunID: `t3/matching-activity-runid-regression`,
  `6b8672d2d45fde70e467b92689e21cf948e687ca`.
  Persistence run https://github.com/auenkr/cadence-rust/actions/runs/40185891064
  succeeded at that SHA. Run 40186253466 is in progress at the same SHA; inspect
  its inputs/results before treating it as matching validation.
  Activity and decision sync-match regressions preserve raw strings. Activity mock
  default response/token assertion remains a possible failure. SQL malformed raw
  IDs return permanent persistence errors rather than reproducing Go's inferred
  process crash. Cassandra error mapping still needs parity review.
- Query completion: `t3/respond-query-task-parity`, `78006b797e658d3169828e8cab8f481c3894e2b4`.
  Run 40177106445 succeeded: clippy/tests/query-format/tls-format/fmt exit 0;
  workspace-format exit 1 from broader drift. Evidence note is on
  `t3/verify-query-ci-evidence` at `34a6618d0` in
  memory/respond-query-run-40177106445-evidence.md. Not mixed-validated or merged.
- SQL malformed domain implementation: `t3/matching-sql-domain-uuid-design`,
  `0a52d5b9de2f2c9899ba9712742fae45c4abd748`. Uncompiled, no CI.
  Backend mode is SQL GoogleUuidPanic versus Cassandra Lenient. Entry guard models
  Go's metrics lookup panic before quota/deadlines, while real validation remains
  separate. Review mode wiring, all 11 guarded handlers, parser text/quoting,
  nil-result mapper precedence, and typed real-lookup behavior before integration.
- Absent query fields: `t3/matching-absent-query-fields`, `f01e92f23`.
  Uncompiled, no CI. Independent review found a BLOCKER: query_absent is computed
  from payload.query.is_none(), but Go panics only when QueryRequest itself is nil.
  Compute it from r.request.is_none() BEFORE unwrap_or_default. A present request
  with nil inner Query must produce a normal poll response with query None;
  correct that test. Keep absent-request/list/both tests. Forwarding marker clearing,
  empty-name acceptance and waiter cleanup were source-reviewed as correct.

## Next steps

1. Fix the absent-query blocker with a Claude Sonnet worker using live discovered
   provider/model/options. Check resources and duplicate work first. Follow current
   user parallelism preference subject to available capacity; no local heavy work.
2. Inspect pending focused runs and resolve actual failures. Review/test the domain
   guard and corrected absent-query branch on Actions.
3. Integrate reviewed branches, preserving docs/evidence; rerun focused checks on
   the actual integrated SHA. Do not relabel branch evidence as integrated evidence.
4. Final exact-SHA validation: fresh uncached dedicated runner target, workspace
   clippy then tests, then mixed gates serially with Rust binary SHA256 recorded.
5. Clean only merged/pushed branches after ancestry, worktree and worker checks.

All Cargo/Go/format commands stay on GitHub Actions in the public auenkr/cadence-rust
repository, standard ubuntu runners only. No Cargo/Go/format commands or new CI
dispatches were performed while writing this checkpoint. Scheduler one-time wake
deletion was not rechecked here; verify it before any recurring wake is permitted.
