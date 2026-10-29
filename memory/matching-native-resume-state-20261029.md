# Matching native continuation state — 2026-10-29

## Latest evidence-only update — 2026-10-29

Current documentation-only master before this edit was
`b0d7e54ab728bc4e95bb6094b87aa9d4388757d4`, clean and synchronized with
`origin/master`. The frozen mixed candidate remains
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; no native mixed lane has passed and
all ten are still required.

The focused baseline [40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415)
passed at `721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; scoped formatting exit 0,
advisory workspace formatting exit 1. The reviewed semaphore source
`592a0bb03660479c890680d316b58067f3527810` and associated docs `283` remain
pending. A single read of [focused run 40853270019](https://github.com/auenkr/cadence-rust/actions/runs/40853270019)
reported expected branch `matching-semaphore-reviewed-592a0bb03` and exact head
`592a0bb03660479c890680d316b58067f3527810`, `in_progress`; Clippy was running,
formatting and tests pending. No terminal logs, exit files, or formatting patch
were available then. No polling or dispatch followed.

Quota scenario integration `530953ac7`, raw-byte work `31ff3eb28`, and reader
work `21a051d36` remain unverified. Raw-byte caveats: configured provider
support and JSON unpaired-surrogate handling are under review. Reader
extreme-value caveats remain under review. Terra is integrating these items;
Terra's separate audit covers the three ack/delete fields. Do not treat these
items as proven. Existing named deadline and caller-disconnect regressions are
listed in `matching-native-checkpoint-20261029.md`; tests are not credited as
executed by run40853270019. Preserve WIP and other docs. No local Cargo, Go,
rustfmt, build, or runtime validation was run.

## Authoritative state

- Review head: `master` = `origin/master` =
  `9a79103bb8cdeb191850452b10b965a02962b3dc`; worktree was clean before these
  documentation edits. Latest rust-ci run [40809425330](https://github.com/auenkr/cadence-rust/actions/runs/40809425330)
  succeeded at that head. This does not validate mixed service behavior.
- Frozen mixed candidate remains ref `matching-native-final-20261029` at
  `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`. Do not move it.
- Whole-workspace CI [40807471166](https://github.com/auenkr/cadence-rust/actions/runs/40807471166)
  succeeded at the frozen SHA. PostgreSQL mixed run
  [40808511931](https://github.com/auenkr/cadence-rust/actions/runs/40808511931)
  failed at that SHA. The other nine mixed lanes remain undispatched; no mixed
  lane has passed on this native candidate.

## PostgreSQL failure evidence

Retained in `/mnt/storage/service/ci-evidence/matching-native-postgres-40808511931/`.
The Actions head and `commit.txt` are the frozen SHA above. The Rust binary SHA256
is `54771ed458f78f0ea7a5b87678c3f0cd8b9b72e563e378a70bb8d13266d87b57`.
Build exited 0; gate, nested harness, and lane exited 1. `results.json` records
topology Go Frontend/History/Worker plus Rust Matching, and no Go Matching;
worker readiness was observed. Rust bound its primary gRPC listener at
`127.0.0.1:14329` and Thrift/TChannel at `127.0.0.1:12973`. Go Frontend never
became able to serve/discover Matching (`Not enough hosts to serve the request`);
the probe's readiness wait ended with `Call cancelled`. The SDK workflow probe
and all recovery/domain/GC/standby/partition scenarios did not run. The probe
then recorded `Rust Matching did not stop` after SIGTERM; the shutdown JSON only
records signal/time, with no clean exit. The Postgres cleanup record contains a
container id, not proof that the container was removed. Treat cleanup as
unverified and confirm before the next gate. Do not claim SDK ran.

Two `dynamic config integer read failed (value was not found); using default`
messages appear during Rust startup. They are observations only; this artifact
does not establish a source bug or show which intended values were missing.

## Native assertion and timeout limits

Current-head Rust CI covers the native task-list status assertion added by
`9a79103bb`. It is newer than the frozen candidate and is not mixed SDK/runtime
evidence. The configured semaphore startup fixed-timeout behavior remains
unverified in the mixed deployment. Neither the status assertion nor that
semaphore path is proven by the failed PostgreSQL readiness attempt. Treat the
older source notes as historical checkpoints, not current defect reports unless
reconfirmed against current source/evidence.

## Remaining mixed-lane dispatch order

Use the exact commands/artifact review requirements in
`matching-acceptance-evidence-readiness-20261027.md`; dispatch serially on one
immutable, fully validated final SHA. PostgreSQL must be rerun after correcting
readiness/shutdown or establishing their causes, then proceed:

1. PostgreSQL
2. MySQL probes
3. Thrift Matching / TChannel History
4. Configstore
5. Real shard distributor / etcd
6. Two-node adaptive scaling, including its explicit `extra_args`
7. Cassandra semaphore/task interop
8. Active offer by Go History with persistence-bypass assertions
9. Production TLS with verified live listener identity
10. Supplemental actual Go History peer and PostgreSQL TLS dependency oracles

The supplemental oracles are required evidence but do not count as mixed lanes.
For every lane verify run head and artifact commit, binary SHA256, build/gate and
nested exits, actual SDK assertions, topology, restart/failure/recovery outcomes
and database/resource cleanup. The frozen source ref remains untouched; if fixes
are needed, create and validate a replacement candidate coherently before
serial mixed runs.

## Diagnostic instrumentation checkpoint — 2026-10-29 (terminal update)

The authoritative terminal read of [run 40839466118](https://github.com/auenkr/cadence-rust/actions/runs/40839466118)
returned workflow `rust-ci`, branch `t3/matching-startup-fix`, head
`12c3063ea91e0dc5abd552a79140a14690edb8bd`, status `completed`, conclusion
`failure` (created `08:03:42Z`, completed `08:06:03Z`). Artifact identity
`rust/commit.txt` matches that exact head. The job `clippy-test` concluded
failure: setup, checkout, Go oracles, and workspace formatting check succeeded;
the targeted `Check changed Matching runtime formatting` step failed; Clippy,
Formatting, and Test were skipped. Exact exit evidence: `matching-runtime-format.exit=1`,
`workspace-format.exit=1`, `query-format.exit=0`, `tls-format.exit=0`.
The focused log reports a formatting-only diff at
`crates/cadence-matching/src/matching_runtime.rs:925`, wrapping the startup
`eprintln!` in multiple lines. The workspace log also contains formatting diffs.
This run provides no Clippy or Rust test result and does not locate the startup
block. Retained artifacts and concise evidence are in
`memory/evidence/matching-startup-fix-40839466118/` and the original downloaded
bundle at `/mnt/storage/service/ci-evidence/matching-startup-fix-40839466118/`.

No workspace retry was dispatched because the selected run failed. No
PostgreSQL dispatch is authorized by this checkpoint. Next step: separately
review the pending fixes, resolve the formatting issue on the chosen immutable
candidate, then dispatch fresh-target workspace CI only after review and exact
head confirmation. The diagnostic branch is not accepted and its run is not a
mixed-service test. The frozen candidate remains remote ref
`origin/matching-native-final-20261029` at
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; do not move it. Fix commits
`overlay9d3a342d5` on `t3/matching-overlay-lock-order` and `PG9498ee5e1` on
`t3/fix-postgres-pool-wakeup` await separate review; neither has been tested
here or proved to cause the prior PostgreSQL startup failure.

The independent source review is retained in
`memory/matching-native-startup-independent-review-20261029.md`. It proves a
cold-domain overlay/limiter recursive-lock defect under specified conditions
and a PostgreSQL pool missing notification on healthy lease return. It does
**not** prove either defect caused failed run40808511931. The startup failure
cause remains unproved until stage logs or equivalent runtime evidence locate
the block. Do not describe the diagnostic instrumentation as an acceptance
pass. The frozen ref `matching-native-final-20261029` remains at
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`.

Two Terra workers are preparing fixes in isolated checkouts on pending fix
branches. They have not been reviewed or merged; keep the diagnostic ref and
head untouched. Reconcile both worker heads/results before choosing a fix
candidate and revalidating it. The owner of this checkpoint is limited to CI
and evidence coordination; no Rust or test source was changed locally.

## Documentation-only continuation — latest checkpoint

See `matching-native-checkpoint-20261029.md` for the latest concrete candidate,
unverified regression branches, unavailable audit-note thread, and ten-lane
acceptance status. It supersedes earlier expectation that the nine pending
lanes could proceed directly after the old frozen PostgreSQL attempt: current
integration candidate `d4ee53f00d505246dc0b7fbbe353d7e989a91c49` is source-reviewed
but has no validation; formatting run `40840392628` is the last known live run.
The old `bf9f3d3` PostgreSQL failure had no SDK execution and no native lane has
passed. Preserve the two-node MySQL lane. Next dependency chain: collect/review
the formatting artifact, finish Terra's two-branch integration review, freeze a
single SHA, pass fresh-target workspace CI, then run all ten mixed lanes
serially and close the parity audit.


## Terminal run review — 2026-10-29

Run [40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628)
completed `success` at exact expected head
`d4ee53f00d505246dc0b7fbbe353d7e989a91c49`; the artifact `commit.txt` agrees.
The run selected `cadence-matching`, not the workspace. Actual exits: Clippy 0,
tests 0, changed Matching runtime format 0, query format 0, TLS format 0, and
`fmt.exit=0`; advisory workspace format check exit 1 reports an unrelated diff
at `rust/crates/cadence-api/src/admin.rs:371`. The run restored the Rust cache,
so it is not a fresh uncached workspace gate. The Matching library suite reports
973 passed, 2 ignored; across 27 result summaries the retained log sums to 1,083
pass results, 0 failures, 16 ignored, including filtered/repeated invocations.
See `memory/evidence/matching-startup-candidate-ci-40840392628/`.

The log includes earlier cold-domain/runtime startup coverage (cold domain
metric tests, gRPC listener startup/Meta Health, native startup gate and Thrift
startup profiling). The run-head commit changed no Rust test files. It does not
validate later semaphore startup-timeout, expanded adapter/lock, or two-thread
regressions, and gives no credit to later candidates `25e`, `90cd`, `e84e`, or
`8cc`. This result does not replace frozen `bf9f3d3`; its PostgreSQL mixed run
failed before SDK workflows. Preserve all ten required mixed lanes, including
two-node MySQL, and the PostgreSQL failure detail above. Matching remains
incomplete; next validation is only after integration/freeze of a coherent
candidate and must be fresh-target workspace CI before serial mixed gates.
