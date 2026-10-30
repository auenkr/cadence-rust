# Shared native boundary and attempt-time HTTP/2 diagnostics

Base: `a47e4032d94134b250ce0670cbd5f791d32b285b`. Integrated diagnostic sources `e566e025314e2a836798742ed0bc48f4c039e6cd` and `9d0ccb61b408f1409baf4772cb397a45c46ed572` without importing the divergent 4a5da9fb line.

Review fixed duplicate `NativeMatchingApi` implementations in the first source patch: the generated trait implementation now delegates its partition method to one inherent traced helper. Added a 64-call bound and enabled its opt-in flag in production validation. The shared native wrapper can also serve Thrift adapters; its records establish entry/completion at that boundary, not a transport-specific socket identity. Fixed request IDs, timestamps and safe status categories carry no request identifiers or payloads.

The HTTP/2 probe starts alongside the direct gRPC attempt, uses configured CA or insecure mode, destroys its session and clears its two-second timer, and records only connection/settings booleans, error code and elapsed time. RPC callback and assembly timestamps remain distinct. Acceptance criteria and recovery assertions remain unchanged.

Only source review and `git diff --check` were run. No local Cargo, Go, Node, formatting, build, test or runtime validation. This candidate is unvalidated and separate from frozen workspace run https://github.com/auenkr/cadence-rust/actions/runs/41103249766 at a47e4032d. A transient API 502 while reading that run's jobs did not establish termination; the latest successful inspection showed in progress. Do not restart that run on an observation failure. Matching remains incomplete.
