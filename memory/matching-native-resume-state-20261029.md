# Matching native continuation state — 2026-10-29

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

## Diagnostic instrumentation checkpoint — 2026-10-29

One authoritative Actions read at `2026-10-29T08:03:46Z` found
[run 40839466118](https://github.com/auenkr/cadence-rust/actions/runs/40839466118)
still `in_progress`, workflow `rust-ci`, branch `t3/matching-startup-fix`, head
`12c3063ea91e0dc5abd552a79140a14690edb8bd`. Local `git rev-parse` confirmed the
branch still has exactly that head at the time of the check. This branch adds
startup-stage markers only; it is not a fix. The run's result and retained
artifacts are pending. Do not dispatch another Actions job while this selected
run is live. After it becomes terminal, collect exact source identity, exits,
and failing log evidence, reusing any existing download directory. If it
succeeds and no other intensive CI or mixed gate is running, dispatch a fresh
whole-workspace rust-ci at the same unchanged head, `clean_target=true`, with
`package` empty. Do not start PostgreSQL before that workspace prerequisite.

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
