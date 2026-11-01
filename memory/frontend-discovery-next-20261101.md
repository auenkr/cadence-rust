# Frontend discovery next slice (source audit, not implemented)

At integration 62775b41c, Frontend launcher starts TChannel with `tchannel::serve` (launcher.rs ~1944); that path supplies no Ringpop handler. `tchannel::handle_call` rejects Ringpop calls when the handler is absent. Existing mixed baseline failures reported Go History bootstrap unable to join Frontend. A new gate is still building; this note does not claim a current runtime result.

The existing MatchingRingpopProvider is already role-parameterized: constructor at matching_ringpop_provider.rs:72 accepts config, service label string, bound listener address, and gRPC port. It constructs advertised address from broadcast config/local IP and actual TChannel port, and publishes both named ports plus `serviceName` from the supplied role. Its start/stop lifecycle includes JOIN, SWIM, membership monitor and healer. Reuse this infrastructure for the current Frontend service rather than porting a second independent implementation or changing completed Matching behavior. Rename/generalize only as needed; retain tests and explicit role labels.

Suggested Luna implementation scope after an active slot frees:

1. Read Go frontend/service.go resource.Start/Stop sequencing and Go membership/resource startup; trace provider readiness versus Frontend warmup/admission and shutdown.
2. Bind Frontend TChannel before publishing its address, create a provider with `cadence-frontend`, and serve workflow/admin RPCs plus Ringpop on the same listener. Existing tchannel.rs serve_listener can accept state, optional Matching handler, optional Ringpop handler; expose an appropriate Frontend entrypoint without installing a Matching handler.
3. Start membership only once the listener can receive JOIN traffic. Keep its task/provider handles in Frontend lifecycle cleanup, handle partial startup failure and leave/destroy ordering. Do not leak provider tasks or overwrite Matching labels when co-running services.
4. Wire current-service History/Matching discovery consumers to real role-filtered snapshots where appropriate. A JOIN responder alone does not prove service discovery/routing parity. Record remaining consumers separately if this becomes a follow-up slice.
5. Add regression coverage for Frontend Ringpop JOIN, correct role/named ports, graceful leave, bootstrap failure cleanup and independent Matching labels. Run all Cargo/Go validation on CI only. Then re-run mixed gate and record Go History successful bootstrap plus Rust binary SHA256. No Go Frontend or Go Matching instances.

Other source-confirmed gap for later ownership audit: launcher builds the HTTP router before installing the WorkflowApi hook into final gRPC/TChannel state clones. Check whether HTTP routes intentionally delegate to shared production operations; do not count a hook configured only in those two clones as evidence that all transports use it.
