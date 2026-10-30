# Matching PostgreSQL gate 41034380907 diagnosis

## Run identity and retained evidence

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41034380907
- Workflow head: `caa32882f9a3fd1da4450c8b216607c053996893`; candidate branch `t3/postgres-gate-caa32882`.
- Production binary SHA256: `906ea9107ea95cd31bacb1e5ca9e4a9c2b8bf25894e9b44f82544eceacdbcf26`.
- Retained artifact directory: `/mnt/storage/service/ci-evidence/matching-postgres-41034380907/matching-production-postgres/`.
- Exit codes: fresh binary build 0; nested Go baseline `run.exit` 1; PostgreSQL lane 1; gate 1. CI mixed Go/Rust step failed.
- Topology reached: original Go Frontend, History and Worker were started; worker readiness marker was observed; `goMatchingStarted=false`; one Rust production Matching process started with PostgreSQL and production Ringpop configuration.

## First failure and shutdown

The probe failed in `waitForRustReady` before SDK assertions. Go Frontend `DescribeTaskList` readiness ended in `Call cancelled`; direct primary gRPC readiness to `127.0.0.1:10731` timed out after 10.007 seconds (`metadata filters: 0.001s`). The Go Frontend log reports `Not enough hosts to serve the request` for `PollForDecisionTask`.

Rust Matching bound gRPC `127.0.0.1:10731` and TChannel `127.0.0.1:16403`, opened PostgreSQL stores, assembled its native runtime, and began Ringpop membership. Its retained startup trace records inbound PINGs #1–#8 from Go Frontend/History/Worker peers. Each reports `accepted_changes=[]`; the member snapshots list those peers and the local `127.0.0.1:16403` as alive. The trace does not record bootstrap JOIN completion, SWIM start, or opening Matching admission. `Meta.health` waits on the startup gate (`grpc_matching.rs`, `with_start_pending`/`wait_started`), which therefore explains why direct primary health timed out; it does not identify why `join_cluster` failed to return.

The probe sent SIGTERM after the readiness failure. `matching-shutdown.json` records no exit code and `childSignal: SIGKILL` after the 20-second cleanup timeout. The Go services remained alive at probe exit. The lane cleanup file records the PostgreSQL container name, not an SDK/recovery result. No SDK workflow, restart, failure-recovery or recovery assertion ran; no such evidence is credited.

## Comparison with 40808511931

Run https://github.com/auenkr/cadence-rust/actions/runs/40808511931 failed at `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`, binary SHA256 `54771ed458f78f0ea7a5b87678c3f0cd8b9b72e563e378a70bb8d13266d87b57`. Its retained diagnosis likewise reports Go discovery/readiness failure and failed SIGTERM cleanup, but the retained Rust startup log was missing, so it has no PING/JOIN transition evidence. Treating the two runs as the same cause is unsupported.

## Diagnostic source change

At `rust/crates/cadence-matching/src/ringpop_provider.rs`, log the bounded JOIN transition for each existing bootstrap attempt: selected target group before awaiting it, then successes and failures after the attempt completes. This is justified by the 41034380907 trace ending after inbound PINGs with no visible JOIN result. It does not change bootstrap selection, retries, deadlines, readiness or admission. It is diagnostic only; there is no established underlying runtime fix yet.

## Validation state

Fresh uncached CI workspace validation was dispatched with package unset and `clean_target=true`: https://github.com/auenkr/cadence-rust/actions/runs/41035815248 at diagnostic source SHA `5755021dbaaeeb70b87ec9ba3c69bd06065398e4`. Its observed state was `in_progress`; no conclusion is credited yet. Do not dispatch a mixed-cluster gate until that workspace run passes. The evidence above is a readiness/bootstrap failure diagnosis only, not Matching acceptance or SDK/recovery evidence.
