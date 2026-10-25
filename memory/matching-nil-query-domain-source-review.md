# Matching: absent Query fields, malformed DomainUUID, nil activity PollRequest (source-only, CORRECTED)
Source-derived only; no code/CI run. Paths under go/. Supersedes the first version of this note.
Wire contract for plain panic errors: `memory/matching-go-panic-wire-contracts-20261025.md` on branch t3/matching-query-wire-contract (2f12f009c).

## Corrections to the first version
- google/uuid is **v1.6.0** (go/go.mod:25), not 1.3.0. Pinned source (module cache uuid@v1.6.0/uuid.go:166-172): `MustParse` does
  `panic("uuid: Parse(" + s + "): " + err.Error())` -> a *string* panic; err text from Parse (e.g. `invalid UUID length: 3`; bad chars: `invalid UUID format`). Same shape as 1.3.0.
- Handler ordering (handler.go:212-249, read directly): `defer CapturePanic` -> `h.domainName(request.GetDomainUUID())` -> newHandlerContext -> throttle -> `ValidateLongPollContextTimeoutIsSet` -> engine.
  `domainName` (handler.go:559) swallows *returned* errors (unknown/empty domain => ""), but a *panic* is not an error return. So a malformed non-empty DomainUUID on SQL panics at the very first
  line, BEFORE throttle, deadline check, IsValidContext and the nil `PollRequest` deref. **Malformed domain wins over nil PollRequest**; a well-formed/empty/unknown domain does not (those proceed to the nil deref).
  The first note wrongly implied only ordinary swallowed errors.
- Prior mock "unknown domain -> Internal" is not the contract (ordinary errors are swallowed).

## Malformed DomainUUID: SQL panics, Cassandra does not
Chain: domainName -> cache.GetDomainName -> getDomainByID (common/cache/domainCache.go:625-665) miss -> checkDomainExists (539-548) -> domainManager.GetDomain.
Manager stack (persistence/client/factory.go:250-270): store -> NewDomainManagerImpl -> [errorinjector] -> [ratelimited] -> metered. None of these (nor domainCache.go, persistence/wrappers, domain manager) contains `recover()`
(grep; recover sites in persistence are only visibility_hybrid_manager and sql_execution_store.go:336+ execution paths, not domain).
- SQL (sql/sql_domain_store.go:206-207): `UUIDPtr(MustParseUUID(request.ID))` panics for any non-empty non-UUID string.
- Cassandra/nosql (nosql/nosql_domain_store.go:121-151): ID used as plain string; no parse; any DB/not-found error handled via handleError -> EntityNotExists (swallowed by domainName). **No panic; request continues** (the Cassandra CQL layer binding a non-UUID string to a uuid column is UNVERIFIED and may yield a driver error, which domainName also swallows unless it panics).
So the contract is backend-specific; do not add a global Rust UUID check.
Resulting error (SQL): handler CapturePanic (common/log/panic.go:37-54) -> `fmt.Errorf("panic object is not error: %#v", s)` => `panic object is not error: "uuid: Parse(abc): invalid UUID length: 3"`; escapes handleErr (no typed wrapping).
Wire (from the wire-contract note, same CapturePanic path as nil activity poll): gRPC `Unknown` (proto.FromError fallthrough, mapper/proto/errors.go:34-93) with that message; Thrift/TChannel plain error -> SystemError code `ErrCodeUnexpected` 0x05 (yarpc tchannel handler.go:388-394) same message. Applies to every matching RPC that calls domainName first (polls, AddTask, QueryWorkflow, ...). Not cached (panic before Put).

## Nil activity PollRequest (valid domain)
engine.go:888-940: after IsValidContext and NewIdentifier(name ""), `request.TaskListMetadata` deref -> raw `runtime error: invalid memory address or nil pointer dereference` (no `panic: ` prefix), gRPC Unknown / Thrift 0x05. Order: [malformed SQL domain panic] > throttle > no-deadline BadRequest "Context timeout is not set." > IsValidContext > nil deref.

## QueryWorkflow absent TaskList / QueryRequest
handler.go:293-323, engine.go:1014-1062: nil-safe getters; no entry error; (SQL malformed domain still panics first). Local dispatch creates a query task; the panic occurs later in the *poller's* PollForDecisionTask
(engine.go:1384 `queryRequest.TaskList.Name`, then 1405 `task.Query.Request.QueryRequest.Query`), returned to the poller as a plain nil-deref error (Unknown/0x05, no `panic: ` prefix because handler CapturePanic). No deliverQueryResult -> QueryWorkflow waits (engine.go:1064-1089) until ctx done/DeadlineExceeded. Unverified: whether the dequeued query task is re-dispatched (source says the task is consumed by GetTask; no requeue seen).

## Implementation plan (not done; Rust files must be located by the implementer — paths below are by role, verify names before editing)
1. Domain lookup: add a persistence-backend flag (e.g. `DomainLookupMode::{SqlStrictUuid, Lenient}` set from the configured persistence type) on the matching handler's domain-name resolver. Only `SqlStrictUuid` returns Err(PlainPanicLike("panic object is not error: \"uuid: Parse(<s>): <parse err>\"")) for non-empty non-UUID IDs; empty stays "" and Lenient keeps swallowing. Parse-error text must mimic google/uuid v1.6 Parse (length cases `invalid UUID length: N`; bad-format `invalid UUID format`; urn/braces variants — read uuid@v1.6.0/uuid.go Parse before coding). Call it at the same first-line position in every handler fn that Go calls domainName in (before throttle/deadline).
2. Plain error type: add/choose a non-typed error variant that serializes as gRPC Unknown and Thrift system error 0x05 (reuse the one the nil activity poll uses).
3. Activity nil PollRequest: keep order throttle > deadline > ctx validity > name parse > error; domain panic ahead of all.
4. Query task flag: carry on the in-memory query task a validity marker (e.g. `QueryTaskShape { has_task_list: bool, has_query: bool }` computed at QueryWorkflow entry from raw request, never serialized). QueryWorkflow behaviour unchanged (dispatch + wait). In the decision-poll response builder: if marker invalid return the plain nil-deref error to the poller and do NOT call deliver_query_result; normal queries untouched (marker both true). Do not change routing/forwarding.
5. Tests (oracle/unit): (a) SQL-mode malformed domain with nil PollRequest -> domain panic message, not nil-deref; (b) Lenient mode same input -> nil-deref/continues; (c) empty & well-formed-unknown domain -> nil-deref for nil PollRequest; (d) nil PollRequest + no deadline -> BadRequest first; (e) query absent TaskList with poller -> poller gets nil-deref error, query call still pending until deadline; (f) absent QueryRequest same; (g) normal query unchanged; (h) mapping of plain errors -> gRPC Unknown / Thrift 0x05 for both messages.
Unverified: Cassandra malformed-ID behaviour, HTTP Thrift, exact parse messages beyond length/format, wire captures (use mixed gate later).
