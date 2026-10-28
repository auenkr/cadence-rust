# Native Matching error boundary audit — 2026-10-29

Matching remains incomplete. This bounded audit reconciles the legacy ignored
coverage gaps with native assertions; it does not close the service audit.

Observed Actions evidence: run40756335405 at
`58d2887a4a47ec038699ec2c68b317cf7ee6987e`, Clippy exit0, Matching library
954 passed/2 ignored, workspace later failed service-core raw-byte quota.
Compact retained evidence: `evidence/matching-native-workspace-40756335405/`.
Full downloaded `test.log` resides under
`/mnt/storage/service/ci-evidence/matching-native-workspace-40756335405/cadence-rust/cadence-rust/rust/`.

| Earlier gap | Native assertion / observed status | Limit |
| --- | --- | --- |
| Query dispatch failure | `matching_service_native::tests::query_rpc_propagates_dispatch_forward_failure_and_releases_its_permit`, passed in that run | Actual forwarding dispatch failure; not all manager failure categories |
| Refresh manager error through public RPC | `matching_service_native::tests::refresh_rpc_returns_storage_failure_without_replacing_native_cache_or_owner`, passed | Nonretryable SQLite blob decode; not complete retry/cancellation taxonomy |
| Production failover callbacks | `matching_runtime::failover_callback_tests::registered_failover_callback_reaches_native_registry_attached_after_catchup`, passed | Callback/attachment and eligible poll release; mixed failover still required |
| Thrift all-method handler error/default responses | `thrift_matching::native_dispatch::tests::every_pinned_thrift_matching_wrapper_returns_internal_error_without_success`, passed | Every pinned MatchingService method, InternalServiceError; does not prove all typed payload fields |
| Raw gRPC status/details boundary | `served_raw_status_normalizes_message_details_and_marker`, passed | Three representative status cases over actual TCP; generic envelope implementation, not every type |
| Go handleErr fourteen known errors | New typed metrics matrix in service-core is pending CI | Metrics classify fourteen known types; does not prove every transport payload |

Go `service/matching/handler/context_test.go::TestHandleErrKnowErrors` checks
fourteen types and wrapped identity. Rust has no Go error object identity;
observable assertions must separately cover status, declared Thrift fields,
raw bytes and classification. The pinned Matching Thrift service declares ten
unique exception types, a different set from generic handleErr. Workflow-start,
domain-create, read-only-partition and semaphore errors are not added to Thrift
methods that do not declare them.

Added `every_declared_matching_exception_preserves_raw_fields_and_wire_id` to
native Thrift dispatch. It iterates every declared exception for every pinned
MatchingService method, with valid and invalid outer messages. It asserts the
actual exception field ID and exact message/payload fields, including required
DomainNotActive fields, repeated cluster fields, last scalar occurrence,
ServiceBusy reason, TaskListNotOwned identities, and RemoteSyncMatched field10.
It forces Code::Unknown so typed envelopes, including the private envelope
retained when grpc-go suppresses invalid text, determine classification.
This assertion is unvalidated and postdates run40758347908. It does not replace
mixed Thrift/TChannel SDK and recovery validation or final same-pin workspace CI.

Applied the 9997-byte formatting patch from run40757715608 to the duration
provider and metric classifier. No local Cargo, rustfmt or Go validation ran.
Both workspace jobs were live at inspection; preserve their terminal evidence.
