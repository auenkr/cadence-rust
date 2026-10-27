# Bounded review: c06aa9b1e domain quota probe

Review of `c06aa9b1e70632e216a2bf868c89b06d0c44fc0e` against prior finding
`7dbb35241`: no source-level blocker found.

`tests/run-go-service-baseline.py:527-533` now sets the exact late-registered
domain's `matching.domainworkerrps` to 1. In
`rust/crates/cadence-service-core/src/matching_quota.rs:89-105,214-284`, a
positive rate gets a ceil(rate) burst (one token here); domain reservation
occurs before host admission, reservation state is serialized by the domain
bucket mutex, and an unsuccessful host admission returns the reservation.
Concurrent calls against a new domain therefore admit at most one initial
request and reject the other at the domain quota, assuming no unrelated host
quota exhaustion. This is the relevant burst/reservation behavior; no runtime
was run in this review.

`tests/matching-service-compat.cjs:525-559` waits until primary Rust readiness,
registers/describes the domain through Frontend, then sends two direct concurrent
gRPC `AddDecisionTask` requests to the primary Rust endpoint with the canonical
domain ID, unique workflows/task lists, and an explicit probe caller. It records
the task list belonging to the actual status-8 response. The Rust RPC resolves
the ID to its canonical name before checking quota and wraps the check in
request metrics (`rust/crates/cadence-matching/src/grpc_matching.rs:1799-1821,
2307-2337`). The Python exporter validator
(`tests/run-go-service-baseline.py:184-207`) requires status 8 and a positive
`cadence_requests_per_tl` sample with the exact probe caller, gRPC transport,
Matching service, domain, `AddDecisionTask` operation, and rejected request's
task list. This binds the asserted RPC outcome to the canonical tagged request
sample. The admitted concurrent request may warm the cache before the rejected
request; as parent requested, the claim is late registration/read-through plus
tagged rejection, not that the rejected request itself was the first cold lookup.

Cleanup/source limitations: `coldClient.close()` is reached after both callback
promises resolve; each has a 20s RPC deadline, so normal RPC errors also resolve
and close. An assertion thrown before that point would bypass that local close,
though the test process's enclosing shutdown owns overall cleanup. This is not a
false-success path. The evidence JSON records both outcomes, but downstream
validation currently checks only status 8 and the rejected-call metric; this is
adequate for the stated probe because it does not claim to prove the other
request succeeded. Registration and readiness are source-inspected only.

No fixture edits, Cargo/Go/build/test/format/runtime commands, or live-run polling
were performed. Live gate `40478120133` was reported running and remains
unobserved by this review; source inspection does not establish its result.
