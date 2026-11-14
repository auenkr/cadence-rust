# matching-rssfe (2026-11-14), branch t3/matching-rssfe

## Fixed
- **rust-fe lane (Rust Frontend + Go Matching): 100% query timeouts / "Connection reset" on CancelOutstandingPoll.**
  Root cause: Rust SQL domain encoder (`rust/crates/cadence-persistence/src/matching_domain_metadata.rs`, `encode_go_sql_domain_metadata`) always wrote field 50 failoverEndTime (zero-time sentinel). Go `serialization.unixNanoPtr(nil)` omits it; a stored value reads back non-nil, so `IsDomainPendingActive()` is true, `IsActiveIn` false (go/common/cache/domainCache.go ~904/933), and Go Matching `getTask` calls `PollForQuery` only. Decisions never dispatched. Fix: write field 50 only when `Some`.
  Result: rust-fe soak lane 42000/42000 completed, 0 failed, verdict PASS on all criteria (mt_rss 0.36 MB/h vs go 1.11).
- **Txn size limit**: `.with_transaction_size_limit(...)` wired on history append paths (engine_resetter.rs x3, engine_start.rs, ndc_adapters.rs, ndc_context_hooks.rs).
- **Gate**: run-frontend-gate.py sleeps 2s before async-config update (Go and Rust honour `frontend.failoverCoolDown`; Rust default is 60s unless overlay is seeded, see Open).
- Soak harness diagnostics (`SOAK_GO_DEBUG`, domain row dump, extra logs).

## Cooldown finding
Master commit 1c75a900c added cooldown/primary checks to admin async-config and isolation-group updates. Updating right after domain registration gives "Domain update too frequent." This likely also explains the master cassandra isolation-group gate failure (run 43537738073). Not verified.

## CI evidence (head 3dff42245 for the gate; earlier heads for the rest)
- soak 43536307377 (go-baseline, rust, rust-fe): rust-fe PASS. Rust (FE+MT) lane: RSS PASS (mt_rss 3.48 vs go 1.11, limit go+5; fe 2.30 vs 4.94) but FAIL on 'query "init" after successful signal' (rust workflows=21197 attempts=26635; go 20832/27917). This is the known deferred FE soak item.
- rust-ci: history 43529845316, persistence 43534769590, matching/frontend/history 43536341864, 43536345478, 43536349174: success.
- history-mixed-gate mysql 43536334319, postgres 43536337822, cassandra 43536340202: `GATE SUMMARY failed: []`.
- frontend-mixed-gate 43544072003: FAILED, only `admin-probe-get-dlq-replication-messages-bad-type` (want INTERNAL "unsupported task type", got OK). Earlier post-merge runs 43538363860, 43541269679 also failed the async-config checks, now fixed by the sleep. Master's runs (e.g. 43521611638) pass bad-type, so this is not from my domain-encoder change as far as seen; cause not investigated.

## Open
- Soak (rust) lane query "init" criterion failing; the previously reported mt RSS slope (6.92) now passes in this run (3.48), but no leak was found by audit, so treat it as unconfirmed, possibly noise.
- frontend gate: bad-type DLQ probe returns OK instead of INTERNAL.
- Rust cooldown ignores file-based `frontend.failoverCoolDown` (1s); only the overlay is read in `domain_policy` (handlers.rs ~293). Not verified.
- Txn-size dynamic key: limit wiring uses defaults at some sites; dynamic key (`system.transactionSizeLimit`) not confirmed on all sites.
- FE->Go Matching query timeouts beyond the encoder fix: not otherwise re-tested on the Rust lane.
