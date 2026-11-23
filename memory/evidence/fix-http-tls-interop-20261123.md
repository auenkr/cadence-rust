# matching-http-tls-interop failure on master (2026-11-23) — partial fix, NOT green

Branch `fix-http-tls-interop`. Failing baseline: run 45148727359 (head 20b6831f3). Last green: run 45147870330 (6af476941).

## Root cause (not the go/ submodule restructure c61212c15)
The overlay/submodule step is fine (Go unit/sqlite steps pass). The regression is the Rust side:
commit 0c90d7c7e (after 6af476941) made `cadence-server` install the production Frontend
(`run_frontend_with_hook`). `tests/run-matching-thrift-compat.py` was written for the old
`cadence-server`, which had an in-memory Frontend/History shim and effectively only Matching:

1. No-config embedded step: server now starts frontend,history,matching,worker,sharddistributor.
   Frontend/History Ringpop providers bootstrap from fixed 7933-7935 (harness moves the ports), so the
   joins never succeed and the Matching TChannel port is never accepted -> "did not start".
2. `TestRustMatchingNativeThrift` calls Frontend RegisterDomain/StartWorkflowExecution/
   RespondDecisionTaskCompleted over TChannel. That needs a real Frontend+History pair, which the old shim provided.
3. Explicit-config step (after fix of 1): the Rust Frontend resolves cadence-history through Ringpop to the
   Go `TestRustMatchingHistoryRingStub` (it only serves ring membership) -> `no handler for service
   "cadence-history" and method "HistoryService::StartWorkflowExecution"` (runs 45154664436).

## Change on this branch (harness only, no Rust change)
`tests/run-matching-thrift-compat.py`: embedded no-config probe now runs `--services matching` and only
asserts the Matching TChannel port opens (the Go workflow test cannot run there any more). Verified: run
45154664436 got past the embedded step and failed at the explicit-config Go test (item 3).

## Remaining work (needs a decision, larger than a harness tweak)
The explicit-config stage must be redesigned around the real Rust Frontend/History, e.g. drop the Go
history ring stub and make Rust History a ring member of the same ring (bootstrapHosts include its
TChannel port, which defaults to 7934 and is not overridable by flags in this harness), or restrict the Go
workflow test to what Matching alone serves. Tried and rejected: adding the Frontend's own address to the
dev ring bootstrap (self is excluded from join targets) and Frontend-only self bootstrap (works for startup
but the Go test then needs a real History).

## Runs (all on branch fix-http-tls-interop)
- 45149854234 (4d7b333, `--services matching`, probe both ports): fail, frontend tchannel port not opened.
- 45150688692 (1aa283e, probe matching port only): fail, RegisterDomain needs Frontend.
- 45151362746 (881d8fc, frontend,matching): fail, Frontend ring join loop.
- 45152507800 (29a6ace, own addr appended): fail, own addr excluded.
- 45153528092 (02242e7, self-only bootstrap): startup OK; fail `cadence-history unavailable`.
- 45154664436 (f6631c0): embedded step PASS; explicit-config step fail (item 3).
