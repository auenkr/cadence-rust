# Matching Ringpop bootstrap/startup source audit (2026-10-30)

## Scope and evidence boundary

Read-only source audit of the Rust bootstrap/JOIN path, its shared TChannel
JSON client and listener, Matching provider startup/shutdown, and pinned
`github.com/uber/ringpop-go v0.10.0` bootstrap semantics. No Cargo, Go,
formatter, server, CI, or PR command was run. The 41035815248 workspace CI
run remains parent-owned and was not polled or changed.

Retained mixed-gate evidence is under
`/mnt/storage/service/ci-evidence/matching-postgres-41034380907/`. Its Rust
startup trace reports PINGs #1–#8, each with `accepted_changes=[]` and a
membership snapshot containing the three Go peers and local Rust address.
The gate's direct gRPC Meta health check timed out after 10.007s, and the Go
Frontend readiness request was cancelled. The harness sent SIGTERM and later
SIGKILL after its 20-second cleanup timeout. The original trace has no JOIN
attempt transition lines, so it does not name the stage at which startup
stopped.

The trace's PING counter is incremented only after `handle_ping_with_full_sync`
decodes the request, acquires node state, and passes `if !node.ready`;
the trace emission follows `apply_changes` (`ringpop_provider.rs:1965-2024` on
the diagnostic source). `MatchingRingpopProvider::start` sets `ready` only
after `join_cluster` returns and `start_swim` succeeds (`matching_ringpop_provider.rs:255-269`).
Therefore those PING lines prove the bootstrap JOIN phase returned and local
Ringpop reached Ready. They are not rejected pre-ready PINGs. They do not prove
Matching admission opened: gRPC Meta health waits on the separate
`Matching::started` watch (`grpc_matching.rs:1085-1098`), which the launcher
marks only after `MatchingMembershipLifecycle::start` returns
(`launcher.rs:1548-1552,1593-1595`).

## JOIN timeout and Go contract comparison

- Rust `join_cluster` checks destroyed, awaits one complete group, counts
  successes, checks the strict `elapsed > max` deadline after the group, and
  retries with the Go-style delay (`ringpop_provider.rs:216-254`). This ordering
  matches pinned Go `join_sender.go`'s `JoinCluster` loop: success is checked
  before the elapsed deadline, which is checked after `JoinGroup`.
- Each Rust group launches all selected targets and drains its `JoinSet`
  (`ringpop_provider.rs:1272-1299`). Every child reaches
  `join_seed_signed`, whose call passes the configured per-request timeout to
  `cadence_service_core::tchannel_json_client::call` (`ringpop_provider.rs:1131-1159`).
- That client wraps the *whole* asynchronous call, including TCP connect,
  TChannel INIT write/read, request frames, response-frame reads, and JSON
  decode, in `tokio::time::timeout(deadline, work)` (`tchannel_json_client.rs:350-480`).
  A silent peer or incomplete framing cannot leave a JOIN child waiting without
  a timeout. The TChannel TTL is also computed from remaining time after INIT
  (`:389-397`).
- Pinned Go `sendJoinRequest` uses a TChannel context with `joinTimeout`, starts
  `json.CallPeer` in a goroutine and selects on result or context cancellation
  (`swim/join_sender.go:409-449`); `JoinGroup` waits for every selected result
  (`:330-407`). Rust's wait-all plus per-call deadline preserves that behavior.
- Rust decodes the complete JOIN response before applying the membership list
  (`ringpop_provider.rs:1160-1175`), matching Go's decode then
  `AddJoinList` ordering. The inbound Rust JOIN response is built while holding
  state synchronously and does not await another task (`:1898-1941`).

The local PostgreSQL gate config uses static seeds
`127.0.0.1:15761` (Go Frontend), `:22652` (Go History), `:23295` (Go Worker),
`:16403` (Rust Matching itself), and `:26478` (secondary Rust Matching port),
with `maxJoinDuration: 30s` in the retained YAML. Rust exact-self exclusion
removes `:16403`; the secondary address is not evidenced as a live listener.
The source defaults zero JOIN options to 1s per request, size 3 and parallelism
2, while this config's 30s max duration is passed through
(`matching_ringpop_provider.rs:248-255`; selection contract in
`memory/ringpop-bootstrap-next.md`). With these five configured addresses,
effective target count is at most four after self exclusion; group capacity is
up to six. Actual selection order is random and the failed pre-diagnostic run
does not record selected targets or per-target JOIN outcomes.

## Await, lock-order, lifecycle and shutdown audit

- The only asynchronous wait in each bootstrap attempt is the bounded
  per-target client call; `join_group`'s unbounded-looking `join_next` loop
  drains a finite set of timeout-wrapped tasks. The retry sleep is also an
  explicit Tokio sleep (`ringpop_provider.rs:251-253`). The launcher races
  lifecycle startup against SIGTERM and listener completion, so dropping the
  startup future drops the `JoinSet` and aborts its children
  (`launcher.rs:1548-1553`; Tokio `JoinSet` drop semantics).
- `MatchingRingpopProvider::start` holds its lifecycle async mutex across
  bootstrap (`matching_ringpop_provider.rs:229-255`), but inbound JOIN/PING use
  the separate synchronous Ringpop state mutex and do not acquire the provider
  lifecycle mutex (`ringpop_provider.rs:1716-1758,1898-2005`). No lock cycle
  between JOIN, inbound PING, state merge and digest construction is visible:
  state locks are acquired synchronously and released before transport awaits.
  PING digest/merge work has no await while holding state.
- After `ready=true`, provider startup only starts owned monitor/healer tasks,
  synchronously sets labels and publishes its snapshot, then returns
  (`matching_ringpop_provider.rs:264-373`). Membership lifecycle then subscribes,
  refreshes once from that snapshot, and starts its worker
  (`matching_membership_lifecycle.rs:34-116`). The launcher marks Matching
  started only after this sequence. Source inspection found no missing timeout
  or lock-order deadlock in these operations, but the failed artifact does not
  identify which of them failed to return.
- Shutdown during the bootstrap `select!` cancels the startup future before
  cleanup (`launcher.rs:1548-1560`). Cleanup currently calls `withdraw()` before
  `stop_after_drain()` even on startup error. Provider self-eviction sends
  concurrent direct PINGs, each using configured `ping_timeout`, and drains its
  JoinSet (`ringpop_provider.rs:720-769`); shutdown then destroys the node
  (`matching_ringpop_provider.rs:420-430`). This is an unusual failure path,
  but for the recorded small membership source does not establish a 20-second
  unbounded wait. Existing evidence has no cleanup-stage trace, so the observed
  SIGTERM-to-SIGKILL interval cannot be attributed to this path from source
  alone.

## Conclusion / next evidence needed

The reported failure is **after Ringpop bootstrap reached Ready but before
Matching's start gate was marked**. The available trace disproves the narrower
hypothesis that JOIN itself was still blocked throughout the health timeout;
it does not establish whether the hold-up is in provider return, membership
subscription/initial refresh, or subsequent launcher startup. Source review
does not prove a bootstrap JOIN timeout omission or an inbound JOIN/PING lock
deadlock, so no production fix is justified yet.

The diagnostic branch's existing JOIN start/completion logs are the right next
artifact to inspect: a missing completion after `ready=true` would contradict
the expected call ordering and warrant a timestamped trace at `set_ready`,
provider return, `subscribe`, initial `refresh`, and `mark_started`. Also record
which SIGTERM cleanup stage is entered/exited before changing shutdown logic.
Do not bypass `wait_started` or expose Matching admission early; Go's
`Node.Bootstrap` sets Ringpop Ready only after `sendJoin` completes
(`swim/node.go:420-456`), while Cadence starts its provider after bootstrap
(`go/common/peerprovider/ringpopprovider/provider.go:128-153`).

No code changes or Rust validation were made in this audit. Required validation
for any later source fix: fresh uncached CI workspace validation first, then a
separate mixed Go/Rust gate whose artifact captures JOIN group outcomes, ready
transition, membership lifecycle stage transitions, Matching gate-open time,
and shutdown stage durations; preserve the built Rust binary SHA256.
