# history-auditgaps (2026-11-14), branch `history-auditgaps`

Static re-check of every open gap in `history-parity-audit-20261114.md` against master 803fcab58 (no local cargo/go/docker).

| Gap (audit ref) | Status | Commit / note | Test |
|---|---|---|---|
| P1-1 soak stall | open (owned by history-soak-tail) | skipped | - |
| P1-2 NDC Go->Rust replication | fixed in code (xrfix, goreplfollow `startTransactionHandleDecisionFailover`); gate row for global-domain replication still unit/two-cluster only | history-goreplfollow-20261114.md | existing |
| P1-3 Kafka TLS/SASL | fixed | 00e41e171 (review3fix) | existing |
| P1-4 archive JSON enums/timestamps | fixed | review3fix (`to_go_json_value`, ns timestamps) | existing |
| P2-1 checksum repair on engine path | fixed | history-p2gaps (engine_checksum.rs) | no dedicated unit test (noted there) |
| P2-2 NDC notifier classification | fixed | history-p2gaps (ndc_context_hooks write_failed) | - |
| P2-3 shard Arc cycle | fixed | review3fix (Weak cells) | - |
| P2-4 failover coordinator | fixed/audited | 16920e353 | - |
| P2-5 stubs: `MissingExecutorsProvider` | dead code (defined, never constructed); `UnwiredReplicator` deleted | n/a | - |
| P2-5 strong-idempotency `WorkflowRequest` empty data | open (not verified; needs request type/version plumbing) | - | - |
| P2-6 CadenceError lacks variants / tchannel `fail` set | variants exist in cadence-api as message errors; HistoryError maps CurrentBranchChanged, ShardOwnership, RetryTaskV2 with details (errors.rs); tchannel `fail` is only for admin-style methods whose declared exceptions are BadRequest/Internal/AccessDenied: matches | - | existing |
| **Go `convertError`: TransactionSizeLimitError -> BadRequestError on every path** | **fixed here** (previously only the decision-completed path; other paths mapped to Internal) | this branch, wiring/server/errors.rs | `transaction_size_limit_is_bad_request_like_go_convert_error` |
| P2-7 queuev2 not gate-exercised | open | - | - |
| P2-8 real S3/GCS/authenticated Kafka CI | open (needs credentials) | - | - |
| msbparity "not covered" Go files (mutable_state_builder.go transactions etc.) | open (history-msbparity7 owns typed Load/checksum) | - | - |
| P3 items | open | - | - |

Not examined this round: line-level re-read of engineimpl/start/reset/signal-with-start paths (listed as not covered in history-respondparity).

## CI
Head c127b22ca48b4829df1cb5d4677e0bf9b793400e (code; memory-only commit follows).
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43557145231 success
- history-mixed-gate two_history=true, success and `GATE SUMMARY failed: []`: mysql/grpc https://github.com/auenkr/cadence-rust/actions/runs/43557705518, cassandra/tchannel https://github.com/auenkr/cadence-rust/actions/runs/43557707773
