# Native Matching entrypoint reconciliation — 2026-10-29

Bounded source review of the named legacy ignored rows at frozen candidate
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`. The review checkout's Go and Rust
sources match that candidate (`git diff --stat <candidate> HEAD -- rust go` is
empty). Assertions below are source facts, not a final-pin test result. Workspace
[40807471166](https://github.com/auenkr/cadence-rust/actions/runs/40807471166)
was reported live without a terminal pass; all ten mixed lanes are pending.
The earlier error-boundary audit records the narrower historical pass at
`58d2887a4a47ec038699ec2c68b317cf7ee6987e`, not a pass at this pin.

| Go behavior → native path → actual assertion | Pending runtime evidence and limit |
| --- | --- |
| `handler/engine_test.go::TestRespondQueryTaskCompleted`, failed dispatch returns `some error` → native query forwarding returns the parent dispatch status through `query_workflow` → `matching_service_native::tests::query_rpc_propagates_dispatch_forward_failure_and_releases_its_permit` asserts Internal, exact message and empty details, forwarded root/from, released permit and no remaining offer. This is a real native forwarding-failure injection, not the identical Go mock-manager shape. | PostgreSQL and MySQL-probes SDK/query lanes exercise production query/standby; they do not by themselves prove this injected failure. Final workspace CI must execute the named assertion. |
| `TestRefreshTaskListPartitionConfig` returns manager refresh failure → public native refresh RPC returns registry/storage error → `matching_service_native::tests::refresh_rpc_returns_storage_failure_without_replacing_native_cache_or_owner` corrupts the real SQLite blob after lease, asserts Internal and its exact storage message, empty details, unchanged cache and same live owner. | PostgreSQL and MySQL-probes partition scenarios exercise configured service interaction; they do not inject this SQLite decode failure. Final workspace CI must execute the assertion. |
| Go domain failover registration/catchup filters notifications and releases eligible polls → production `start_domain_cache` callback consults the shared native-registry slot → `matching_runtime::failover_callback_tests::registered_failover_callback_reaches_native_registry_attached_after_catchup` starts cache before attaching registry, asserts catchup and local/active-active/stale changes leave a real poll blocked, eligible change releases it and owner remains identical. | PostgreSQL domain/standby and two-node MySQL recovery are relevant mixed interactions; their recorded scenarios must be inspected for an actual failover before claiming mixed failover proof. Neither callback test nor lane selection alone proves that outcome. |
| `context_test.go::TestHandleErrKnowErrors` recognizes fourteen typed errors including wrapping → native statuses use typed details/private raw envelope, with handler metric classification before transport → `matching_metrics::tests::handler_error_categories_use_typed_envelopes_before_transport` checks all fourteen against misleading wrapped text and raw marker; `native_raw_status_transport::served_raw_status_normalizes_message_details_and_marker` checks representative actual gRPC wire status/details. | All mixed gRPC SDK lanes, especially PostgreSQL/MySQL-probes, exercise real entrypoints; inspect actual error results. The metrics matrix is not an exhaustive fourteen-type, every-RPC wire payload test, and Go object identity cannot be asserted as Rust status identity. |
| Go Thrift wrapper returns declared error and zero response on handler failure → native `dispatch_native_with_context` / `application_error_bytes` → `thrift_matching::native_dispatch::tests::every_pinned_thrift_matching_wrapper_returns_internal_error_without_success` asserts field 2 InternalServiceError with exact message and no field 0 for every pinned method; `every_declared_matching_exception_preserves_raw_fields_and_wire_id` checks each declared method/exception ID and raw payload with wrapped and invalid-byte outer messages. | Thrift/TChannel mixed lane supplies actual SDK transport and recovery evidence when run; it does not substitute for the injected all-method matrix. The typed matrix exercises the encoder boundary, not every typed error injected through every handler. |
| Go retains source identity for forwarded activity and History start → native engine/dispatch carries `GoString` source into the forwarded request and synchronous History start → `matching_engine_native::tests::activity_forwarding_preserves_raw_fields_and_domains` asserts source alias and raw request fields on forwarding; `forwarded_activity_sync_dispatch_preserves_cached_raw_source` asserts successful poll/History start sees exact uppercase source bytes and no task persisted in either domain. | MySQL-probes forwarding and active-offer Go History/persistence-bypass lanes provide relevant mixed runtime evidence; inspect actual source identity assertions before claiming that exact raw-byte scenario is proved by a lane. The native sync fixture uses a TCP History stub, not a Go History process. |

The old update-failure, poller-description, partition-owner and shard-ownership
rows remain as qualified in `matching-current-ignore-reconciliation-20261027.md`;
legacy `grpc_matching` assertions are not credited as native entrypoint proof.
The native Thrift method inventory is `every_pinned_thrift_matching_method_has_native_dispatch`,
not an assertion that every method passed in a mixed deployment.

## Defects and evidence gaps

No concrete production defect was established in these bounded rows. Final-pin
workspace results and all ten same-pin mixed lanes, including binary SHA256,
SDK, recovery and topology evidence, remain outstanding. In particular, the
mixed lane descriptions do not promise injection of every error or raw-identity
case above; do not infer those exact outcomes from a green lane. The historical
fourteen-type classification and declared-Thrift encoding assertions have
separate boundaries and do not establish exhaustive typed/wrapped transport
behavior across all RPCs. This review does not certify runtime outcomes or
Matching service completion. No local Cargo, formatting, Go, build, test or
runtime command was run, and no CI was dispatched.
