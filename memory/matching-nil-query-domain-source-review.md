# Matching: absent Query fields, malformed DomainUUID, nil activity PollRequest (source-only)
Go pointers (go/service/matching/handler/{handler,engine}.go unless noted). No code run; source-derived only.

## Activity poll with nil PollRequest (verified directly)
handler.go:212-249: first line `h.domainName(request.GetDomainUUID())`, `request.GetPollRequest().GetTaskList()` (nil-safe getters), then throttle, `ValidateLongPollContextTimeoutIsSet`, then engine.
engine.go:888-940: getters nil-safe -> taskListName "". Loop: IsValidContext, `NewIdentifier(domainID,"",Activity)` OK (empty name has no reserved prefix, identifier.go:116-134),
then `request.TaskListMetadata` (~line 912) derefs nil `request` => nil-pointer panic BEFORE getOrCreateTaskListManager/GetTask. Recovered by handler `log.CapturePanic`
(common/log/panic.go:37-54): runtime error is an `error` => returned as plain error (message "runtime error: invalid memory address or nil pointer dereference"), not a typed Cadence error.
Order for Rust: ctx validity -> task list name parse error -> then nil-request error; ctx/timeout/throttle checks first. Empty-name nil-request must not create a manager.

## QueryWorkflow absent TaskList / QueryRequest
handler.go:293-323 + engine.go:1014-1062: all accessors nil-safe. Absent TaskList => name "", kind 0 (Normal); proceeds through getOrCreateTaskListManager and `DispatchQueryTask`
with no validation error. Absent QueryRequest => consistency level getter returns default (non-strong). No Go "Query missing" error exists.
If a poller is present, query task is delivered locally; the panic occurs in the *poller's* PollForDecisionTask goroutine/RPC, in createPollForDecisionTaskResponse (engine.go ~1372-1406):
 - 1384 `queryRequest.TaskList.Name` panics if TaskList nil (first site);
 - 1405 `task.Query.Request.QueryRequest.Query` panics if QueryRequest nil.
That poll RPC's CapturePanic returns a plain nil-deref error to the POLLER; no deliverQueryResult is called, so the QueryWorkflow RPC keeps waiting (waitForQueryResult, engine.go:1064-1089) until its ctx ends (hCtx.Err()). Without poller: waits/times out similarly (sync-match fails -> dispatch behavior as normal). Do not invent a Query error.
Caveat: the polled query task has also been removed from the task path; whether it is retried is unverified.

## Malformed nonempty DomainUUID (SQL persistence)
handler first line `h.domainName(id)` -> domainCache.GetDomainName -> getDomainByID (common/cache/domainCache.go:625-665): cache miss => checkDomainExists (539-548) => domainManager.GetDomain.
sql_domain_store.go:194-212: ID path calls `serialization.MustParseUUID(request.ID)` (serialization/uuid.go:39: "" => nil, else uuid.MustParse) which PANICS with a *string* (`uuid: Parse(<s>): invalid UUID length: N`, google/uuid v1.3.0 uuid.go:160).
No recover in domain store/cache path (persistence recover() sites: visibility_hybrid_manager, sql_execution_store only). Panic propagates to handler's deferred CapturePanic (registered before domainName call):
non-error panic => `fmt.Errorf("panic object is not error: %#v", s)` => PLAIN error, message `panic object is not error: "uuid: Parse(abc): invalid UUID length: 3"`.
Returned from ALL matching RPCs using domainName before any engine work (query, polls, add task...). No throttling/validation precedes it. Not cached (panic before cache Put), so repeats re-hit DB path.
Empty DomainUUID: `domainName` GetDomain => BadRequest "Both ID and Name are empty" => err swallowed, name "". Well-formed unknown ID => EntityNotExists swallowed, name "", request continues (NOT an Internal error; mock unknown-domain Internal output is not the contract).
Cassandra: not traced (string ID bound to query; likely EntityNotExists/other swallowed => request continues). Unverified.

## Recommendation
Oracle (SQL only): malformed nonempty DomainUUID => plain error with the panic-object text above on every RPC at first line; nil activity PollRequest => plain nil-deref error after name parse; query with absent fields => no error at RPC entry.
Rust minimal: in domain-name lookup add UUID parse check for SQL persistence returning that plain error (message shape matches, exact text unverifiable on wire); keep nil-activity-poll error; for query absent fields dispatch normally, make poller-side response construction return an error for the poller without completing the query. Unverified: wire status mapping of plain errors, retry of consumed query task, Cassandra.
