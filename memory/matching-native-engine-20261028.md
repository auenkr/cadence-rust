# Native Matching engine progress, 2026-10-28

`matching_engine_native.rs` is an unwired foundation, not an RPC engine. It
contains a registry holder and common byte-preserving add-task construction,
with raw-byte unit tests. No Cargo, formatting, CI or runtime validation was
performed. The parent must wire the module before CI can compile or run it.

The four requested add/poll paths remain unimplemented. Do not treat this as
parity. `grpc_matching.rs` contains their actual control flow, while
`matching_handler_native.rs` contains only helpers. `NativeQueueRegistry::get`
does not substitute for the handler ownership check. A complete engine must
provide domain lookup/failover, admission and metrics/config boundaries; retain
raw identifiers; enforce write-partition and sticky checks; implement the
ancestor/local/remote forwarding chain with tokens, deadlines, cancellation,
remote-sync errors and durable fallback; construct poll lifecycle and rates;
and handle decision query/standby and activity paths. The native dispatcher
provides poll and History-start operations, but its use alone does not supply
all of the handler's query, failover, hint and response semantics. Integrate the
remaining nine RPCs and transport/launcher separately. Mixed Go/Rust recovery,
SDK and byte-compatibility evidence remains outstanding.
