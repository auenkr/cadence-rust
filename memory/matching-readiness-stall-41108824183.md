# Matching readiness stall evidence — run 41108824183

Artifact root: `/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/matching-production-postgres/`.
The artifact pins source `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`
(`commit.txt`), binary SHA256
`bc45dd2dc600599fdd2f1135a58a918c878bb4f9ca93eaac5692f6a5eece778d`
(`binary.sha256`), build exit `0`, and gate exit `1`. Gate command requested
PostgreSQL mixed validation plus restart, recovery, idle, GC-domain, query
standby, partition, durable-restart, and auto-config-hint scenarios
(`command.txt`). `gate/run/results.json` says `acceptanceEligible:false`,
probe exit `1`, and run exit `1`; the failure happened before SDK scenarios.

## Observed sequence

- `gate/run/rust-matching/rust-server-before-restart.log:45-60` records one
  `ListTaskListPartitions` entering the handler, passing admission and domain
  resolution, completing both owner lookups, and returning `category=ok` in
  776 µs. The same log records Ringpop membership PINGs and all four listed
  members as alive (`:35-43`). This proves that one RPC made it through the
  handler; it does not prove sustained RPC readiness.
- `gate/run/rust-matching/rust-process-evidence-before-restart-direct-partition.json`
  covers the later direct call from 18:58:11.740 to 18:58:21.739 UTC. The
  client channel was `READY` before, during, and after the ten-second RPC, but
  the call timed out. Its TCP probe connected; its HTTP/2 probe connected but
  received no SETTINGS and ended `ETIMEDOUT` after ~2 s. A channel `READY`
  observation therefore did not mean this RPC completed.
- The paired
  `gate/run/rust-matching/rust-process-evidence-before-restart.json` snapshot
  at 18:58:21.739 UTC reports 10 process threads (four Tokio workers plus
  other named threads), all state `S`, all `wchan=futex_do_wait`, and zero CPU
  ticks for every thread over the 500 ms sample. RSS was ~95.5 MB. Embedded
  `ss` output shows the gRPC listener on `127.0.0.1:18097` with `Recv-Q 2` of
  `Send-Q 128`; four connections existed, including two `ESTAB` sockets with
  receive queues 204 and 13 bytes and two `CLOSE-WAIT` sockets with receive
  queues 1 and 51 bytes.
- The final readiness record is
  `gate/run/rust-matching/list-task-list-partitions-readiness-before-restart.json`.
  At 19:00:42.791 UTC the Go Frontend readiness RPC timed out after 20 s. The
  later direct Rust call ended after 10 s with `Waiting for LB pick`; channel
  connectivity stayed `CONNECTING` from the during-RPC sample through the
  post-RPC sample. Its TCP probe still connected immediately, while HTTP/2
  SETTINGS again did not arrive (`ETIMEDOUT`). The Rust server log contains no
  second handler entry after its earlier successful RPC. `gate/run/probe.log`
  confirms the probe stopped at this readiness failure, before SDK scenarios.

## Classification and limit

The snapshots weigh against active CPU starvation at the sampled instant:
no sampled thread consumed CPU during the 500 ms window, and the captured
threads were sleeping rather than runnable. That short sample cannot rule out
starvation before/after the window or a blocked executor worker. Futex waits
alone also cannot distinguish normal Tokio parking/async wait from a worker
stalled before accepting or servicing the HTTP/2 connection. Existing TCP
backlog and unread socket bytes show connection-level activity, but the
`ss` snapshots omit process/fd ownership and do not prove whether the server
accepted each socket or emitted any HTTP/2 bytes. The final `CONNECTING` plus
`Waiting for LB pick`, alongside successful TCP connect but absent SETTINGS,
places the failure before an RPC response and at or below gRPC/HTTP/2 transport
progress; it does not establish a specific listener, executor, lock, or
application cause. Do not infer a production fix from this evidence.

## Smallest next discriminating check

On one rerun, at the first direct-RPC timeout, capture a single timestamped
snapshot of `/proc/<pid>/task/*/{wchan,syscall,stack}` and
`ss -ltnp` plus `ss -tnp` for port 18097, alongside the existing HTTP/2
SETTINGS probe. This should establish whether the listener/accepted socket is
owned and progressing in a server thread or whether all runtime workers are
parked elsewhere. Preserve the same request start/end timestamps. This is a
diagnostic request only, not a proposed code change.

Scope: this note classifies the readiness evidence only. It does not audit
shutdown behavior or claim Matching service acceptance.
