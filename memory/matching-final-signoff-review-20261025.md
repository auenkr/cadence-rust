# Matching final sign-off review (read-only, candidate 29bdd2b04)

Matching is INCOMPLETE. This review does not mark completion.

## Candidate limits
- Source `29bdd2b04b573309112286c57f1c347f0ba0b60c`. `git diff fade09c5..29bdd2b04 -- rust` is empty: Rust production is byte-identical to the fresh-workspace source `fade09c5`. Only `.github/workflows/rust-ci.yml` (Go DescribeTaskList oracle step), tests/ harness and docs differ.
- Fresh workspace 40168414544 is bound to `fade09c5`, not 29bdd. It does not run the new Go oracle step; that ran in cached run 40171379726 on `5cafc2dc`. No fresh run exists on 29bdd. Production equality justifies reuse for Rust tests, but must be stated as inference.
- HTTP TLS interop 40172574953: success on exactly 29bdd (verified via gh). Peer = original Go YARPC clients; History endpoint = real Go Ringpop member fronting the Rust History gRPC fixture, NOT original Go History. Fixture/standalone evidence, not Go-service mixed acceptance.
- TLS production fix e3c79b30 is in 29bdd (ancestor of fade09c5); fresh 40164484146 covers it.
- Backend/transport/discovery lanes (PostgreSQL, MySQL, Thrift, configstore, shard-distributor, Cassandra, active-offer) have NO run on current production source; older runs pre-date TLS merge and runtime-reader/Unleash/lifecycle changes and must not be inherited.

## Coverage classes
- Source/unit: 13 RPC handlers, lifecycle units, TLS matrix (invalid DER, lone cert/key, missing identity), metrics comparison (4 `match`, fade09c5), owner-order Go oracle, DescribeTaskList oracle (5cafc2dc).
- Fixture: HTTP TLS interop (Go ring stub + Rust History fixture), synthetic Go oracle mock errors (nil outer panic / unknown-domain mapping: explicit limitations, no Rust guard).
- SDK mixed (original Go Frontend/History/Worker + Rust Matching): only the serial lanes below; currently none current-source.

## Concrete untested / open items
1. No mixed lane on final source (7 lanes: postgres, mysql-probes, thrift, configstore, shard-distributor, Cassandra, active-offer). Only "13 RPC + lifecycle + backends + discovery + recovery" evidence with Go History/Frontend is these lanes.
2. Real Go History + HTTP/Thrift TLS never combined; accept as limit unless a lane enables TLS (plan says existing lanes run plaintext gRPC).
3. Audit "genuine gaps" (matching-completion-audit-20261024.md L139-175): `list_task_list_partitions_success_owner_hosts_match_go_lookups` (owner oracle now covers ordering, confirm row closed), `go_get_tasks_batch_size_returns_raw_non_positive_values` (check production observability), cancelled-context propagation tests, file config live reload vs one-shot (decide whether supported config source needs it). Must be explicitly closed/justified.
4. Root-start `notify_read_partitions` push: ignored test is placeholder; mixed probes do not prove startup push (completion-requirements, runtime reader checkpoint).
5. Workspace `fmt --all` advisory exit 1 remains; report, do not call pass.
6. Fresh uncached workspace + Clippy on the final pinned SHA (29bdd or successor) if any further source change; otherwise record the fade09c5 equivalence explicitly.

## Final done conditions
- Pin one `EXPECTED_SHA`; each run's headSha equals it; serial dispatch; artifacts downloaded and inspected; binary SHA256 per mixed lane; cleanup evidence.
- Fresh `clean_target=true` rust-ci (clippy then tests) on that SHA, or documented production-tree equivalence with `git diff` proof.
- All 7 lanes success; labels `preliminary-passed`/`acceptanceEligible:false` kept as emitted and reconciled.
- Audit rows in item 3/4 each closed with a named test or Go-source justification.
- Tracker + audit updated with run URL/SHA/conclusion; then parent decides completion (not this review).
