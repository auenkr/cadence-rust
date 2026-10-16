# Matching RPC migration checkpoint (2026-10-16)

Implemented opt-in Matching::with_durable_queues. AddDecisionTask and
AddActivityTask persist workflow references without consulting local execution
state. Poll RPCs use queue registry + cancellation-safe dispatcher + outbound
History RPC client. Existing domain metadata lookup and poll cancellation guard
remain; activity source domain is validated too. Task-list kinds map protobuf
normal/sticky/ephemeral to Go persistence enum values, and absent protobuf
partition maps serialize as nil Go blob containers. The default listener still
uses the embedded backend; there is no production config switch yet.

Three focused RPC tests and strict all-target workspace lint passed:
/tmp/cadence-matching-rpc-verified.exit = 0. Final combined rerun also checks the
empty-map assertion added after that focused checkpoint.

Live test matching_delivery.rs now starts a real Rust Matching protobuf server,
uses the generated client for AddTask/Poll RPCs, then completes returned tokens
through actual Go Frontend and confirms original-Go History error envelopes.

## Actual mixed-service replacement probe

New tests/matching-peer-go_test.go overlays the original Go cadence CLI package.
It uses Fx decoration of membership.PeerProvider to advertise only the Rust
Matching peer; all original Go Frontend/History/Worker handlers and clients run
unchanged. Go Matching is NOT started. Go discovery seed list uses the running
Worker instead of the absent Go Matching peer; otherwise ring bootstrap fails.
This is test-only discovery substitution, not production Rust Ringpop parity.

New tests/matching-service-compat.cjs registers a domain through Go Frontend,
seeds that metadata into the Rust test server, and uses only Go Frontend's public
worker/workflow APIs for an 11-event workflow, activity heartbeat, and completion.
The original Go History adds tasks to Rust Matching and original Go Frontend polls
Rust Matching. No direct Rust library invocation supplies task delivery.

PASSED: /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-20261016-retry/results.json;
/tmp/cadence-mixed-matching-retry.exit = 0.
Reuse the unchanged built Go overlay harness at
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-20261016/cadence-server.
Do not pass this test binary as --go-server-bin (ordinary Go CLI); it uses
--go-mixed-server-bin in the combined runner or --binary with --rust-matching.

Full 22-stage final runner PASSED, with 324 workspace tests and strict lint. Logs:
/tmp/cadence-matching-mixed-full.log and .exit; artifacts:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-final-20261016.
/tmp/cadence-matching-mixed-full.exit = 0. All 22 stages passed.
Expanded SDK probe also PASSED (outer exit 0):
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-sdk-expanded2/results.json.
Seven scenarios: echo/activity, activity retry, child, workflow-run retry,
workflow-task retry, activity timeout and signal. Expected injected activity
failure/workflow panics appear in its SDK log and are followed by success.
The expanded SDK test was added after the full 22-stage run and checked separately.

## Remaining Matching gates

This only demonstrates normal asynchronous decision/activity RPC delivery.
Sync matching is not ported; forwarded AddTask explicitly reports unimplemented
in the opt-in backend. Queries and administrative APIs still use embedded state.
No production domain cache/config selection, periodic ack maintenance, ownership
validation, idle unload, poller history, distributed rate limits/forwarding,
partition/isolation scaling, semaphore behavior or internal Thrift/TChannel
Matching transport is complete. Domain metadata is seeded by the test.
Do not move to History until Matching gates in service-migration-plan.md pass.

## Build lesson

Do not overlap Go/Rust heavy linking. First full runner's last live stage timed
out compiling its test executable; earlier stages passed. Baseline runner now
precompiles matching_delivery before starting services or workflow deadlines.
First mixed probe failed Go ring bootstrap due absent Matching seed, fixed in
harness only. These failed attempts are not positive compatibility evidence.
