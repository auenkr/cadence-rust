# PostgreSQL Matching gate diagnosis — run 41039044942

Run: https://github.com/auenkr/cadence-rust/actions/runs/41039044942  
Exact source SHA: `3a958668c040b289b495f9b76b2209969db7e2a0`  
Retained artifact: `/mnt/storage/service/ci-evidence/matching-postgres-41039044942/matching-production-postgres/`

## Finding

The first failing stage was `waitForRustReady("before-restart")` in
`tests/matching-service-compat.cjs`, before any SDK workflow, restart, failure
recovery, PostgreSQL GC, standby-query, partition-parity, or durable-restart
assertion ran. The readiness helper first called Go Frontend `DescribeTaskList`
to prove native Ringpop discovery, then called the primary Rust gRPC endpoint's
actual `ListTaskListPartitions` operation (`directPartitionOwner`) with a
10-second deadline. The Go Frontend call timed out at 20 seconds; the direct
primary operation timed out at 10 seconds. `probe.log` reports both errors and
the helper stack at lines 567/578. This was not a health RPC.

## Reconciled evidence

- Rust Matching opened the configured PostgreSQL stores, assembled its native
  runtime, started its Ringpop provider, joined Go History (`127.0.0.1:15105`),
  Frontend (`127.0.0.1:18225`) and Worker (`127.0.0.1:14881`), published its
  membership snapshot, completed initial membership refresh and opened
  admission. Its log then records eight successful Ringpop PINGs. Therefore
  the log ending around “opening admission” in the earlier failure was not
  evidence of an incomplete startup; this run explicitly records the complete
  stages and continued membership traffic.
- JOIN to `127.0.0.1:21716` failed. That is the configured secondary Matching
  endpoint, which this single-node scenario does not start. The three Go peer
  JOINs succeeded and the Rust node was visible in Go's ring. This isolated
  failed peer does not explain the direct RPC timeout.
- Go Frontend, History and Worker all started; the Go worker readiness marker
  was observed. `results.json` confirms there was no Go Matching process.
- The generated config and command agree: PostgreSQL databases `cadence` and
  `cadence_visibility`, host `127.0.0.1:5432`, SSL mode `disable`; Rust gRPC
  primary `127.0.0.1:17142`, Matching Thrift/TChannel/Ringpop `127.0.0.1:19168`,
  History gRPC peer `127.0.0.1:29069`; Go Frontend gRPC `127.0.0.1:17949`.
  The probe's registered `service-compat-…` domain is present in the retained
  domain snapshot. The direct readiness call uses that domain and a generated
  `primary-ready-…` task-list name. No config/port mismatch was found.
- Ringpop logs show Rust Matching (`127.0.0.1:19168`) added to Go rings at
  10:17:02 and removed at 10:17:21, consistent with the probe killing the Rust
  child after readiness timed out. History's own ring removal at 10:20:22 is
  during final Go-service shutdown. These removals are teardown consequences,
  not evidence that peers spontaneously removed a healthy node before the RPC
  probe.
- PostgreSQL's retained container log records successful startup and readiness,
  but PostgreSQL query logging was not enabled, so it cannot establish whether
  the request reached a domain query. The Rust config's store-open marker
  confirms startup connectivity, not request-level persistence success.
- Exit files: fresh Rust binary build `build.exit=0`, baseline probe
  `gate/run/run.exit=1`, PostgreSQL lane `gate/lane.exit=1`, outer gate
  `gate.exit=1`. Artifact SHA256:
  `4b846e47b4fa81457e5ca5df3f532bad9201ccf4d702b5bdcd6e503bbcf640f2`.
  Cleanup ran and `docker rm --force` returned the container name in
  `gate/postgres-container-cleanup.txt`; the container ID/name is retained.

## Corrected prior hypotheses

The first eight PINGs prove Ringpop readiness/membership traffic reached Rust;
the service log ending near admission is not a startup failure. The retained
gate state has no subscriber-loss marker, and the failure is not pipe
saturation. The old discovery hypothesis from run 40808511931 does not explain
this run: here Rust's startup completed and Go peers saw it. The evidence still
does not distinguish a stalled Matching handler/admission, domain-cache or
PostgreSQL read, or partition owner lookup, so this run does not justify a
production behavior change.

## Bounded next-run diagnostics

Added opt-in `CADENCE_MATCHING_LIST_PARTITIONS_TRACE=1` traces, capped at 32
lines per process and excluding domain/task-list identifiers. The PostgreSQL
validation workflow enables them. Stage boundaries cover handler entry and
admission, engine entry, domain-cache hit or PostgreSQL read/refresh, domain
resolution, partition count, and first activity/decision owner lookup. This
should make the next exact-SHA failure discriminating without generic
startup/JOIN logging. Source commit and its fresh workspace CI result should
be appended here after the parent-authorized CI dispatch finishes.
