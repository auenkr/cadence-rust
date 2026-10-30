# Matching acceptance blocking-cycle source audit (2026-10-30)

Candidate inspected: `be7016ef2d04dcb00fda946ddb67e3fbf15bae09`
(`origin/t3/matching-acceptance-shutdown-consolidated`). This was a source-only
review of runtime startup, gRPC/TChannel accept and response, Ringpop request
handling, and SIGTERM shutdown, prompted by PostgreSQL run `41094318642` where
TCP connect succeeded but grpc-js stayed `CONNECTING` until LB pick timeout.
No runtime, build, test, CI, or artifact commands were run.

## Finding

No concrete synchronous blocking cycle is proven in these paths. The gRPC
Matching listener is a pre-bound `tokio::net::TcpListener` passed through
Tonic's `TcpListenerStream` (`rust/crates/cadence-matching/src/launcher.rs:1093-1103,1709-1728`;
`rust/crates/cadence-matching/src/grpc_matching.rs:4149-4202`). The binary uses
Tokio's multi-thread `#[tokio::main]` (`rust/crates/cadence-matching/src/main.rs:4-5`).
For the no-identity TLS mode, accept and TLS handshakes are asynchronous; each
handshake is timed and progressed in `FuturesUnordered`
(`grpc_matching.rs:4205-4266`).

The shared Matching TChannel/Ringpop listener also uses Tokio accept and
spawns a task per connection (`tchannel.rs:732-753`); connection frame reads,
serialized writes, and Ringpop response writes use async I/O and a Tokio mutex
(`tchannel.rs:592-633,642-677`). Ringpop state uses short `std::sync::Mutex`
critical sections, but the inspected JOIN/PING paths snapshot/update state
outside network awaits (`ringpop_provider.rs:1131-1176,1181-1235,1242-1264,1313-1345`).
Probe and response work is async (`ringpop_runtime.rs:49-69,75-120`). The relevant
production paths contain no Condvar wait or synchronous socket I/O.

Startup opens configured stores inside `spawn_blocking` before serving
(`launcher.rs:1251-1261`). It starts the shared TChannel listener before
Ringpop bootstrap and races bootstrap against signal/listener completion
(`launcher.rs:1547-1579`); maintenance starts as Tokio tasks. Startup signal
cleanup awaits withdrawal, Matching runtime shutdown, provider teardown, then
aborts/joins the listener (`launcher.rs:1580-1625`). Normal shutdown records
the signal/fatal trigger and sequentially awaits membership withdrawal,
Matching drain, membership teardown, and listener joins
(`launcher.rs:1772-1863`). This is a potentially long async wait chain, not a
source-proven worker-blocking cycle.

One expected wait worth distinguishing from a deadlock: withdrawal spawns and
joins Ringpop self-eviction (`matching_membership_lifecycle.rs:183-200`), which
starts concurrent async PINGs and joins them (`ringpop_provider.rs:720-768`).
If the shutdown trace stops after `provider self_evict begin`, a delayed or
unanswered peer response is plausible. The trace does not prove a runtime
worker is blocked.

## Discriminating process evidence

If reproduced, capture `gdb -p <pid> -ex 'thread apply all bt' -ex detach -ex quit`
while grpc-js is still CONNECTING, plus the `CADENCE_MATCHING_SHUTDOWN_TRACE`
stages for shutdown cases. Evidence for worker starvation would be Tokio worker
stacks inside synchronous `read`/`recv`/`accept`/`connect` or blocked in
`std::sync::Mutex::lock`/`pthread_mutex_lock`, with the lock owner identified
in another stack. A repeated lock wait paired with an owner blocked in sync I/O
would establish the suspected cycle. Tokio workers parked in runtime
`epoll_wait`/scheduler code, while the main task awaits async Tonic/TChannel
I/O or the Ringpop self-evict join, would distinguish ordinary async waiting.
The run symptom alone (successful TCP connect) does not discriminate these
cases.

No production fix is proposed from this audit. Re-open only with matching
process stacks or equivalent runtime evidence; do not infer a blocking defect
from the grpc-js timeout alone.
