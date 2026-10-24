# Executable Go Matching owner ordering comparison

Run: https://github.com/auenkr/cadence-rust/actions/runs/40106750315
Source: e14928004e (see commit.txt for exact SHA). Conclusion: success.
Fresh workspace clippy, tests, strict focused formatting and Go/Rust oracles
passed. Go-generated ordered partition responses are retained in go-owner-order.json.
Rust production ListTaskListPartitions responses matched across baseline, added,
removed, identity-replaced, address-replaced and empty-ring membership snapshots,
for root, child and empty task-list requests and both task types.

Initial run40104792630 forced Rust membership.replace for identity-only metadata
changes while Go production Refresh ignores unchanged address sets. The test now
uses production replace_if_addresses_changed, matching the deployed watcher.
No production ring behavior was changed to make the oracle pass.

The advisory workspace format exit remains nonzero for existing unrelated debt.
This evidence predates the test-only file-watcher merge; final candidate workspace
run40108643448 includes that test as well and remains separate acceptance.
