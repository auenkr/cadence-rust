# Matching native History task-start client — 2026-10-28

Status: implemented, NOT compiled or tested (no local Cargo by rule; parent owns CI).
Not wired into production launch/dispatch.

## Done scope
* `matching_history_peer_resolver.rs`: `workflow_bytes` / `domain_bytes` hash exact bytes (same fingerprint32 path as the `&str` methods).
* `matching_history.rs`: retry / retry_record_started / execute_with_redirect take `workflow_id: &W` (`W: AsRef<[u8]> + ?Sized`), so legacy `&String`/`&str` callers are unchanged and raw callers pass `&[u8]`; initial peer uses `workflow_bytes`. `HistoryConnection::Grpc` now also carries the configured `Channel` (`client_for_peer` returns `(client, channel)`); the struct stores `channel`. Legacy behavior otherwise untouched.
* New child module `matching_history_native.rs` (declared with `#[path]` inside matching_history.rs for private access): `start_decision_native[_with_deadline]` and `start_activity_native[_with_deadline]` taking `&GoString` domain, `&AllocatedTaskBytes`, native `api::Poll*Request`, returning native `matching::Poll*Response`. Same per-attempt timeout, resource retry, redirect and metrics as legacy. gRPC unary uses `tonic::client::Grpc` + `ProstCodec` over the native `history::Record*Started*` messages. RunID uses `TaskDataBytes::run_id_bytes` (malformed raw preserved). Tokens use `TaskTokenBytes` + `JsonTaskTokenSerializer::serialize_bytes`.
* Tests: byte-preserving execution, decision/activity response + token, resolver byte routing, TChannel unsupported, and a scripted tonic History peer (hand-written `tower::Service` with native `ProstCodec`) asserting the decoded request keeps ff/fe bytes and the token matches.

## Remaining gaps / decisions
* TChannel raw calls: needs a byte-valued Thrift History client; the native methods return `Unimplemented` up front when `with_tchannel_outbound` is set (no retries, no UTF-8 fallback).
* Record-started timeout provider is keyed by `&str`: non-UTF-8 domain IDs use the lossy key, i.e. fall to the default like an unknown domain.
* Legacy `execution()` errors on an invalid persisted run-id UUID; native path emits the empty/raw bytes per `run_id_bytes` (as instructed).

## Compile risks (verify in CI)
* `tonic::codec::ProstCodec`, `tonic::codegen::http::uri::PathAndQuery`, `Status::into_http`, `tonic::server::Grpc::unary` generic bounds on tonic 0.12; the native generated `Message` impls must satisfy `Message + Default`.
* Native type/field names assumed from native_model.rs: `n::api::history_event::Attributes::ActivityTaskScheduledEventAttributes` (boxed or not, handled via deref coercion/`.into()`), `n::api::ActivityType`, `WorkflowType`, `PollFor*Request` with `domain`/`identity` GoString fields.
* Generic `W` inference at legacy call sites (`&task.data.workflow_id`, `&execution.workflow_id`, test `&workflow_id`).

## Parent integration corrections

Removed the lossy domain timeout conversion. Added RecordStartedTimeoutBytes and with_record_started_timeout_bytes so native integration can supply a byte-aware domain resolver. Valid UTF-8 domains continue through the existing provider when no native provider is installed; invalid domains use the documented 1s default and never alias a valid U+FFFD domain. Runtime wiring of the native provider remains required for full dynamic-config parity. Added a regression checking exact provider input and no replacement-rune alias.

Native execution construction now rejects malformed persisted UUID bytes with the same Internal message as legacy, while raw request RunID bytes continue to take precedence unchanged. Added a regression for this distinction. The earlier statement about intentionally forwarding malformed persisted UUIDs is superseded.

TChannel raw task-start calls remain explicitly unsupported. Production handlers still use the legacy client path. No compile or runtime pass is claimed.
