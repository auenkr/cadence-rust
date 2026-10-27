# Matching query and error wire review (2026-10-27)

Read-only source review requested alongside
`matching-current-ignore-reconciliation-20261027.md`. No Cargo, Go, rustfmt,
test, CI, or runtime commands were run. This records reachable query behavior
and specific source/assertion evidence; it is not a general Matching parity
sign-off.

## Query dispatch failure

Go `handler/engine.go:1014-1058` installs a result channel, calls
`tlMgr.DispatchQueryTask`, returns its error unchanged when non-nil, and waits
for a local poller only when dispatch returns `(nil, nil)`. The Go test
`handler/engine_test.go:677-690` forces the manager mock to return an arbitrary
`errors.New("some error")`; it only asserts `wantErr`, so that test demonstrates
the engine's error branch, not a production error category or wire status.

Rust's `grpc_matching.rs` `query_workflow` (around 2869-3175) directly offers
to a local query poller or forwards through the partition chain. The local
offer path has no fallible manager dispatch call: it returns the worker outcome
or an RPC deadline, while forwarding can return a parent RPC status. The
`matching_query.rs` channel dispatch/offer APIs are synchronization operations,
not a generic injectable manager-error boundary. The old ignored mock therefore
does not establish missing behavior: an arbitrary manager error is not a
production-emittable category in this Rust path. No concrete source mismatch
was found. The generic-dispatch test-gap claim in the reconciliation note
should be read as harness-only coverage; injecting an impossible arbitrary
manager error is not required for parity.

### Reachable QueryWorkflow outcomes

| Reachable source | Go behavior / `handleErr` effect | Rust transport mapping and active evidence |
|---|---|---|
| Invalid task-list identifier | `tasklist.NewIdentifier` returns untyped error; `handleErr` wraps `InternalServiceError` | `task_list_name` maps parse error to `Status::internal`; Thrift `application_error` defaults to `InternalServiceError`. Source mapping exists; no query-specific assertion identified in this review. |
| Sticky poller absent | Typed `StickyWorkerUnavailableError`, returned as-is | `sticky_unavailable()` returns `Unavailable` with typed `StickyWorkerUnavailableError`; active RPC assertions at `matching_rpc_tests.rs:5005-5010` and `9268+` inspect message/status path. |
| Query worker returns failed completion | Typed `QueryFailedError` containing worker message | `matching_query::query_outcome` produces typed gRPC details; Thrift declared exception is emitted by `application_error`. Active Thrift assertion near `thrift_matching.rs:1721-1735` checks `QueryFailedError`. |
| Unknown worker completion type | Go constructs `InternalServiceError("unknown query completed type")` | Rust query outcome returns Internal; RPC regression at `matching_rpc_tests.rs:4123+` asserts this path. |
| Caller context expires while waiting/offering | Go returns `ctx.Err()`; `handleErr` default branch turns it into `InternalServiceError` | Rust timeout branches return `Status::internal("context deadline exceeded")` (`grpc_matching.rs` around 3000-3053); active timeout assertions are in `matching_rpc_tests.rs` around 4671 and 9196+. |
| Forwarded parent returns an error | Go forwarding path preserves/handles only errors returned by the parent/forwarder, including its explicit retry/throttle policy | Rust forwards the parent `Status` and applies its forwarding handling around 3087-3160. Thrift converts statuses through `application_error`; typed detail is retained only when declared by the method. No claim made here that all possible parent statuses have a QueryWorkflow assertion. |
| Poller channel closes / query result index misses | Go result delivery to an absent task returns `EntityNotExistsError`; in-flight wait channel closure is not a normal engine result | Rust index delivery returns `NotFound`; Thrift maps NotFound to `EntityNotExistsError` only if that method declares it, otherwise Internal. Query's declared failures do not include EntityNotExists, so an internal channel/index anomaly is not falsely treated as a normal query worker error. |
| Arbitrary task-manager dispatch error | Only the test mock emits this arbitrary `errors.New`; the production local query route is offer/forward/wait | No analogous Rust production source. The Rust test-only engine fault seam targets named RPC engine-call failures, not a nonexistent query manager dispatch. This row is intentionally excluded from required production error parity. |

## `handleErr` category scope

Go `handler/context.go:83-143` recognizes fourteen typed classes. This is a
handler-wide error classifier, not a contract that every RPC can emit every
class. Rust does not have a matching `handleErr` function; each production path
constructs a `tonic::Status`, often with typed protobuf details, and the
Thrift adapter maps that status to the method's declared exception. For
QueryWorkflow, only the reachable categories below are relevant:

| Go category | QueryWorkflow reachability | Rust status helper / Thrift conversion | Assertion evidence / disposition |
|---|---|---|---|
| `InternalServiceError` | Yes: untyped identifier errors, internal query outcome, context expiry | `Status::internal`; `application_error` fallback is `InternalServiceError` | Query timeout and unknown-completion assertions exist; no generic assertion for all internal sources. |
| `StickyWorkerUnavailableError` | Yes, sticky-list precheck | `Unavailable` plus typed detail; Thrift declared exception | Active sticky query RPC status/message assertions. |
| `QueryFailedError` | Yes, worker reports failed query | typed `QueryFailedError` detail; Thrift method exception | Active Thrift mapping assertion; Go `TestWaitForQueryResult` asserts typed source error (`engine_test.go:843-862`). |
| `EntityNotExistsError` | Not a normal `QueryWorkflow` result; applicable to `RespondQueryTaskCompleted` for an expired/unknown query ID | gRPC query-result delivery uses NotFound; Thrift exception is method-declaration-sensitive | Do not require QueryWorkflow to declare/emit this category. Respond path is distinct. |
| `BadRequestError` | No normal query engine outcome identified; malformed Thrift request decoding is transport validation | `Status::invalid_argument`, Thrift `BadRequestError` | Decode/transport tests cover malformed inputs; not a query-engine category requirement. |
| `WorkflowExecutionAlreadyStartedError` | No query dispatch/result producer | Typed detail if a service method declares it | Not reachable from this Matching query path. |
| `DomainAlreadyExistsError` | No query dispatch/result producer | No QueryWorkflow producer | Not reachable. |
| `LimitExceededError` | No query engine producer identified; request quota emits service-busy/throttle semantics | General gRPC mappings exist elsewhere; Thrift fallback/status policy is not query-specific | Do not demand a QueryWorkflow assertion absent a producer. |
| `ServiceBusyError` | Possible at an upstream forwarding boundary or quota gate, not the local query result dispatch | `ResourceExhausted` maps to `ServiceBusyError`; typed details may preserve declared exception | Forwarding/throttle assertions are in the query forwarding family/RPC tests; not an arbitrary local manager error. |
| `DomainNotActiveError` | No local query producer identified | Typed status mapping is used by relevant domain operations | Not reachable in this query engine path. |
| `RemoteSyncMatchedError` | No synchronous query result producer | Typed unavailable status mapping is for task match forwarding | Not reachable as a query result category. |
| `ReadOnlyPartitionError` | No query producer identified | Status mapping belongs to partition mutation behavior | Not reachable. |
| `TaskListNotOwnedByHostError` | May arise at request ownership/partition routing boundary depending on deployment path; not from query offer result | Ownership guard returns typed gRPC detail; Thrift recognizes the detail when method exception is declared | Only require it where ownership guard is actually active; no cross-product assertion implied. |
| `SemaphoreNotOwnedByHostError` | No query/semaphore interaction | Semaphore-specific status mapping | Not reachable. |

This table is a reachability map, not a claim that Rust's mappings have been
exhaustively asserted for every service method. In particular, a generic
identity-pass-through `handleErr` test does not translate to a universal Rust
wire test: Go typed pass-through is per incoming Go error, while Rust wire
mapping is per concrete RPC producer and the Thrift method's declared
exceptions.

## Thrift duplicate map keys

`thrift_matching.rs:196-212` decodes a normalized `Value::Map` by iterating
entries and collecting `(String, String)` into `HashMap`. For both Matching
partition-config inputs (the AddDecisionTask field 70 and AddActivityTask field
90, at `thrift_matching.rs:483` and `525`), duplicate keys therefore resolve
last-wins at this downstream conversion.

The earlier `normalize` helper in `thrift_matching.rs` is for generated
Go `FromWire` struct-field semantics: duplicate struct field IDs are replaced
by the later correctly typed occurrence. It does not collapse entries inside
a map. Separately, general `cadence-thrift::Struct::opt_map_str` in
`value.rs:168-190` inserts each decoded map entry into a HashMap, also
last-wins. These are distinct paths; the Matching wrapper uses its
`string_map` conversion, while generic `FromThrift` conversions use
`opt_map_str`. Go's generated map decode followed by Go map assignment has the
same last-wins result. No duplicate-key mismatch was found. Do not change
normalization to preserve a first value or reject duplicates.

## Scope conclusion

This review found no confirmed production mismatch for query dispatch failure
or duplicate map keys. It narrows the former ignored-mock row: the mock proves
an engine unit-test branch only, and its arbitrary error is not evidence of a
reachable Rust production category. The active Rust assertions listed above
cover selected reachable outcomes, not the complete Matching service parity
audit or mixed-cluster validation.
