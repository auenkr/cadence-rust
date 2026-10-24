# Matching partition owner ordering oracle (2026-10-24)

The former `engine_apis_partition_order_owners_and_lookup_errors_match_go` test
derived expected owner addresses from Rust `Membership::owner`, so it did not
independently verify Rust placement. The replacement executable oracle is
`tests/matching-owner-order-oracle/oracle_test.go`, run by
`tests/run-matching-owner-order-oracle.py`. Its Go test overlay compiles in the
production `go/service/matching/handler` package and calls the production
`matchingEngineImpl.ListTaskListPartitions`; its membership path is the actual
`common/membership.NewHashring` plus `common/membership.NewResolver` and
`MultiringResolver.Lookup`.

CI writes Go-produced activity and decision responses for root, child, and empty
task-list requests across baseline, add, remove, identity replacement, address
replacement, and empty-ring snapshots. Rust's Matching RPC test installs each
snapshot in the Rust membership implementation and compares ordered keys and
owner addresses against that output. Empty-ring response comparison checks
that keys survive failed owner lookup with empty owner strings.

Reproduce on GitHub Actions with:

```sh
gh workflow run rust-ci.yml --ref t3/matching-owner-order-oracle \
  -f package=cadence-matching -f clean_target=true
```

The Go oracle runs before clippy and the Matching crate tests. Ordinary Rust
unit-test runs without the `CADENCE_MATCHING_OWNER_ORACLE` environment variable
skip only this cross-language comparison. Validation and evidence: pending
GitHub Actions run.

## Recovery checkpoint

Worker v7 failed on provider usage limit after committing180bb8875. Parent
recovered that implementation, preserved an explicit empty JSON host array
(instead of null) and added the generated Go response artifact. Integrated
sourcea2579ee22 is dispatched to fresh Matching CI:
https://github.com/auenkr/cadence-rust/actions/runs/40104239210 .
Result pending; implementation is not yet validated. A workflow artifact-path
indentation mistake in486e758a7 was corrected bya2579ee22 before dispatch.

Corrected Unleash backoff/identity integratione4d9720a0 passed fresh workspace
run40092051935. Metrics, active-offer and runtime file-watcher workers were
previously active; their own results must be checked before integration.
