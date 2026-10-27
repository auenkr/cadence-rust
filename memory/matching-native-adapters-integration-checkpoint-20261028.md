# Native adapter integration checkpoint

Matching remains incomplete; the native adapters are not production dispatch.

Diagnostic40582378853 at2911f943e failed before generated code compilation:
https://github.com/auenkr/cadence-rust/actions/runs/40582378853
The second tonic pass explicitly repeated `.google.protobuf`, which prost
already maps. Parent removed that duplicate configuration. No runtime pass
is claimed for this attempt. The review's Int64Value concern was withdrawn
after checking prost's explicit Message-for-i64 wrapper implementation.

Completed worker response code now handles the same decision transient-event,
query and auto-hint graph as existing production adapters. Parent added a
direct valid-model comparison to an existing test, canonicalizing field/map
order without changing values. Shared native module alias is now exported.

The remaining eight request adapters are registered. Parent corrected typed
duplicate struct/map selection, bad-element map assignment, metadata wrapper
presence, empty payload conversion and heartbeat Payload type. Parent completed
dispatch timestamps and raw worker version fields and added regression tests.
The only explicitly unsupported graph in those adapters remains the scheduled
HistoryEvent on AddActivityTask. Source and tests are not yet compile-verified.

Next: run combined crate validation, complete the remaining scheduled-event
decoder, then migrate real handler/registry/backend identity. The raw store
and queue path is not yet integrated. Final immutable pin and every required
same-pin gate are still pending. No local Cargo, Go or rustfmt ran.
