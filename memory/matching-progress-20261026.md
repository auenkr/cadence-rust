# Matching continuation checkpoint

User reduced the worker limit to **two Luna workers**. Keep all Cargo, Go,
and formatter commands on Actions. Mixed gates remain serial. Matching is
incomplete; do not start the next service.

Master source `a039f209902eac0bb5899ec36d9582eabbb8c87b` includes the typed
SQL RunID error and the active Refresh error regression. Refresh focused run
40222171242 passed on `cfc7eab79a5a8ab24fe56f2b027e22af86f13be8`.
Master integration run https://github.com/auenkr/cadence-rust/actions/runs/40222658215
passed on that exact SHA. It is ordinary integration CI, not the final fresh
candidate acceptance run.

Unmerged combined candidate: `matching-final-candidate`,
`41739ea94d56b00de5eacbb729d513f4abeb6528`; fresh workspace run
https://github.com/auenkr/cadence-rust/actions/runs/40222413373 failed at Test
on the same unregistered-domain process fixture; fresh workspace Clippy passed.
It contains the standalone SQL fatal lifecycle and real-binary regression.

Focused fatal run https://github.com/auenkr/cadence-rust/actions/runs/40222090786
failed on `ff91ef4bde65fe86972831f02177e7a6bd862102`: Clippy passed, preceding
tests passed, but the real child did not exit within 20 seconds after Add
returned an error. A failed Add alone does not prove the SQL writer ran.
The first repair diagnosed an unregistered domain: Add failed before SQL.
Its replacement test launched cadence-server, whose main does not provide
cadence-matching's exit-2 mapping. Luna task
`78b0e46b-9abe-4cf8-a5ce-154d4d771b70` is correcting the fixture to seed a
domain and exercise the actual Matching-only binary. Preserve exit-2/no-success assertions; do not
hide the defect with a larger timeout. Candidate adds child kill/wait cleanup.

Next: inspect repair and focused CI, assemble corrected candidate, run fresh
workspace Clippy then tests, and run the exact-SHA serial mixed acceptance
plan in `matching-final-gate-plan-20261025.md`, recording binary SHA256s.
No current fatal-path passing test or final Matching acceptance is claimed.

## Registry-clone repair, October 26 evening

Fresh run 40223423001 on `62f068cf9a3e45d04ae57d3765a3c13d889804ec`
reached the typed SQL writer error with the actual Matching-only binary and a
seeded domain, but the child still did not exit. This establishes a wiring
failure beyond the earlier fixture problem. `MatchingRuntime::with_fatal_writer`
updated only its registry; the RPC handler retained a pre-installation clone.
Commit `e69c7663392db4e2c356d38d2a4a42a4b35125b8` installs the sender on the
handler registry as well. It is pushed on `t3/sqlite-writer-fatal-exit`, unmerged.

Focused retry https://github.com/auenkr/cadence-rust/actions/runs/40357713048
is pending at this record. This branch also restricts SQL UUID inputs to Go's
exact 32/36/38/45-byte forms; compact or braced URN suffixes must be rejected.
Two Luna workers are independently reviewing the fatal wiring and final mixed
acceptance requirements. No final mixed run has started for this source.

## Confirmed focused pass and remaining work, 22:00 IST

Run https://github.com/auenkr/cadence-rust/actions/runs/40357713048 completed
**success** on exact head `e69c7663392db4e2c356d38d2a4a42a4b35125b8`.
GitHub job metadata confirms Clippy and Test succeeded. This is focused,
cache-enabled CI, not fresh final workspace or mixed-cluster acceptance.

Independent source review found a remaining concurrent-shutdown race: signal
selection may finish teardown before an in-flight writer sends its fatal
notification; a final `try_recv` alone cannot establish writer completion.
A Luna worker is implementing a lifecycle fix and regression. A second Luna
worker is addressing active-offer restart/recovery coverage and auditing the
HTTP/TLS mixed scope. Both use medium reasoning/default speed; limit remains2.

The readiness review's stale shard failure statement must not override run
40218022772: that shard recovery run succeeded on
`a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd`, binary SHA256
`5d9a60123b651e6cdd1fa3197a9955085351536a668daf8eb57085c8663d52e3`.
It must still be repeated against the final candidate after semantic changes.
No current final candidate is pinned; Matching remains incomplete.

### Artifact verification of focused pass

Downloaded `rust-ci-logs` for40357713048; embedded commit matches e69c76633.
`clippy.exit=0`, `test.exit=0`, `fmt.exit=0`. Actual test log confirms722
library tests passed and the real `sql_writer_fatal_process` integration test
passed (1 test). Compact excerpts, original-log hashes and exit files are in
`memory/evidence/matching-fatal-focused-40357713048/`.

Important limitation: `query-format.exit=1`, `tls-format.exit=1`, and
`workspace-format.exit=1` despite green job steps. Touched-file logs identify
formatting in `grpc_matching.rs` and `matching_registry.rs`, including long
fatal-channel types. Obtain/apply an Actions-generated patch after lifecycle
fix; do not claim touched-file formatting acceptance from job status alone.
