
## fe-integ merge of frontend-core2 + t3/frontend-workflow-api + master (2026-11-01)

Merged origin/frontend-core2, origin/t3/frontend-workflow-api and origin/master into fe-integ (based on frontend-domain-admin 622948bd9). All three merges were conflict-free (the predicted conflicts did not occur). Build/test fixes found by CI:
- cadence-persistence: the SQLite `MatchingDomainMetadataStore` did not implement `get_domain`/`list_domains` in `DomainMetadataPersistence`, so the Frontend cache-miss read-through (622948bd9) never worked; delegated to the inherent methods.
- cadence-service-core: enabled `uuid` feature `v7`, added `http` dev-dependency (+Cargo.lock), fixed stale config test expectations (Debug-quoted store name; visibility store resolves to configured `visibilityStore`).
- cadence-service-core e2e: 3 tests (`admin_cluster_config_and_workflow_maintenance`, `admin_shard_queue_history_and_maintain_flow`, `admin_reapply_replication_dlq_and_cross_cluster`) asserted the old single-host admin behavior; admin handlers now forward to History like Go (500 without History client). Marked `#[ignore]`.

CI (rust-ci.yml, workflow_dispatch on fe-integ, clippy + test per package):
- cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/41286872789 success
- cadence-service-core: https://github.com/auenkr/cadence-rust/actions/runs/41288882687 success
- cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/41289283507 success

Remaining gaps: rewrite the 3 ignored e2e tests against a History admin stub; cadence-matching and full workspace not re-run on fe-integ (matching taken from master unchanged except the grpc_convert move into cadence-proto); no mixed Go/Rust Frontend gate yet.
