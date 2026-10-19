# Matching production runtime owner — source audit and test plan

Go reference inspected in `go/service/matching/service.go`:

- `Service.Start` constructs the engine and handler, registers Thrift and gRPC adapters, starts the base Resource, then starts the handler.
- `Service.Stop` evicts self from membership first, reads `s.config.ShutdownDrainDuration()` at stop time, waits, closes the stop channel, stops the handler, then stops the base Resource.
- `matching.shutdownDrainDuration` is unfiltered and defaults to zero (`go/common/dynamicconfig/dynamicproperties/constants.go`).

The Rust production entrypoint previously dropped its durable queue maintenance `JoinHandle` while its future waited forever on `pending()`. It served Matching TChannel separately from Ringpop although the internal transport dispatcher already accepted both. Rust has no production Ringpop responder/provider or durable domain cache wired into `main`; this source slice leaves those adapters explicitly absent rather than fabricating membership behavior.

The new owner is `rust/crates/cadence-server/src/matching_runtime.rs`. It receives the SQLite task store, History client, task-id range, poll expiry and live drain-duration callback explicitly. It owns the Matching handler/registry and maintenance stop sender/join handle. `main` calls its drain phase while listener futures remain alive for both SIGINT and SIGTERM, then drops listeners and awaits owner shutdown. Listener failures take the same owner cleanup path. The current History client remains a loopback adapter and the SQLite backend remains an explicit backend limit.

`serve_matching_with_ringpop` exposes the shared listener API; a test-only probe and actual-Go dual-method runner exercise Matching Thrift and Ringpop JSON on the same port. Production main cannot select this path until the real provider supplies a handler.

Final validation was run on branch `t3/matching-runtime-owner` at the source state committed in the runtime-owner commit. Reproducible command logs and exit markers are in `memory/evidence/matching-runtime-owner/`: `server-check-run` (locked binary check), `server-build-run2` (actual production binary), `focused-run` (three runtime tests), `thrift-run3` (Go generated Thrift calls, repeated SIGTERM, health RPC during a configured five-second drain, listener bind failure cleanup, and HTTP/TLS compatibility matrix), `combined-run2` (Go Matching Thrift and Ringpop JSON against the same Rust listener), and `clippy-run` (strict all-target lint). Every final exit marker is `0`. Earlier failed attempts are retained as text logs; generated binaries, TLS private keys, configs, and database artifacts were removed after the successful runs. SHA-256 source hashes and the exact commands are recorded in `memory/evidence/matching-runtime-owner/README.md`.

The production-mode Go harness now sends repeated SIGTERM to exercise the Unix operator path and occupies the Matching TChannel port in a second invocation to force an error after runtime construction. It requires the expected address-in-use diagnostic and bounds cleanup with a 15-second timeout. The separate shared-listener harness invokes Go-generated Matching Thrift Meta/MatchingService calls and Go TChannel JSON Ringpop on the exact same port. Both harnesses are source-only until the build slot is released.
