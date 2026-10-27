# Matching candidate v6 final acceptance ledger

Final ref: `matching-acceptance-20261027-v6`
Final SHA: `3055d0c6c0c8932c14e12714b498b82569cfe0b5`

Matching remains incomplete. V6 removes four redundant test struct updates that
failed v5 Clippy. All v5 results are historical and do not count for v6.
Keep this source pin immutable; any required source change needs a new candidate
and reruns. Subsequent master commits may contain documentation only.

| Requirement | Run / task | Reviewed result |
| --- | --- | --- |
| Fresh uncached workspace Clippy then tests | [40520934239](https://github.com/auenkr/cadence-rust/actions/runs/40520934239), head matches v6 | In progress: Clippy passed, workspace Test active at 14:30 UTC; artifacts not yet reviewed |
| PostgreSQL | Not dispatched | Pending |
| MySQL probes | Not dispatched | Pending |
| Thrift | Not dispatched | Pending |
| Configstore | Not dispatched | Pending |
| Shard distributor | Not dispatched | Pending |
| Two-node MySQL | Not dispatched | Pending |
| Cassandra | Not dispatched | Pending |
| Adaptive two-node | Not dispatched | Pending |
| Active offer | Not dispatched | Pending |
| Production TLS | Not dispatched | Pending |
| RPC native/Thrift audit | memory/matching-v6-rpc-audit-20261027.md | Method inventory closed; four Thrift questions under deeper review |
| Ignored-test/exclusion audit | memory/matching-v6-ignored-tests-audit-20261027.md | Static report complete; two stale harnesses confirmed, other coverage claims need reconciliation |
| Lifecycle/persistence audit | memory/matching-v6-lifecycle-persistence-audit-20261027.md | Shutdown/drain/fencing reviewed; task reader/writer/GC review running |

Use memory/matching-acceptance-evidence-readiness-20261027.md for exact dispatch
commands and evidence requirements. Run mixed lanes serially after workspace
validation; retain exact source identity, binary SHA256, actual scenario/SDK
results, recovery outcomes, topology and cleanup. Do not infer acceptance from
job conclusion alone. No local Cargo/Go validation is authorized.

Automatic cached push CI 40520845748 was cancelled in favor of the explicit
fresh-target v6 workspace run. No mixed lane is running.

Worker orchestration: three Claude Sonnet 5.5 low-effort source auditors started
2026-10-27 approximately 14:25 UTC (19:55 IST), authorized for the next hour by
the user. Use Luna medium/default speed for later work after that window.
Prior v5 Luna RPC audit was interrupted and lifecycle audit failed; replacement
workers were given their thread pointers to recover partial findings. Parent
owns source changes and CI. Workers write separate memory reports only.

## Follow-up source work (14:30 UTC)

V6 is not the final acceptance candidate: two confirmed stale harness package
selectors require changes (`tests/matching-history-compat.cjs` and
`tests/run-shard-distributor-go-interop.sh`, cadence-server -> cadence-matching).
The current v6 CI remains useful diagnostic evidence but no mixed v6 lanes
will be dispatched. Cut v7 after the deeper workers finish and confirmed fixes
are reviewed, then repeat fresh workspace and all mixed lanes at v7.

Three Sonnet low-effort workers now own independent tasks:
- d1dd5841-ba59-44fd-b2b3-c32d926a4a8c: resolve Thrift U1-U4 against generated Go decoders and handlers; fix only confirmed mismatches.
- c1a099a7-2da7-41f9-b316-62978b1d553c: read-only task reader/writer/GC/completion source audit.
- ae49f94c-3d5a-4f71-b78c-0a097413a3e0: repair active-offer ignored test with reachable History fixture and reconcile Go/Rust test map.

These replace the three completed initial audits, whose scope was bounded;
none constitutes full service acceptance. The ignored-test report's claims of
missing harnesses or untested behavior require source reconciliation, not
blind promotion into new completion requirements.
