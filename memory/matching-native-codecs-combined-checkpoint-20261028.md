# Native Matching codec integration checkpoint

Matching remains incomplete and production dispatch has not changed.

Routing/RunID diagnostic run40580362543 at7fbe5a4e6 failed Clippy on the new
Cassandra parser's manual even-number check. No tests ran in that attempt.
Parent replaced it with the existing Go-style bit check. Run URL:
https://github.com/auenkr/cadence-rust/actions/runs/40580362543

The codec worker only implemented string primitives; parent completed the
descriptor-driven Message generator, integrated the native tonic facade and
added real native transport/Go fixture tests. See the three updated native
codec/tonic/Thrift notes for exact scope and review corrections. This source
still requires Actions compile/Clippy/test verification. The public protobuf
contract remains unchanged. Native paths do not convert raw identifiers to
the strict String model. The decision Thrift response graph remains partial
and explicitly rejects unsupported graphs; it is not used in production.

Next: fix combined CI findings, complete Thrift graph and remaining methods,
migrate real shared handlers and queue/backend identity to native raw types,
then freeze a new candidate and rerun every required mixed lane and workspace
gate at that pin. Do not credit any old-pin pass as service completion.
No local Cargo, Go, rustfmt or mixed gate ran.
