# Frontend configured SQL visibility store

## Change

Frontend now selects `persistence.visibilityStore` through
`PersistenceConfig::store_name_for/datastore_for(Visibility)`. An unset role
returns no SQL visibility reader, matching the Go factory's optional database
visibility manager. Invalid store references and unsupported configured
backends fail Frontend startup. MySQL and PostgreSQL use the selected store's
own credentials, endpoint(s), TLS settings, connection limits, and connection
lifetime. If both roles name the same configured SQL store, Frontend reuses
the already-open default pool. Matching-only startup never opens the optional
visibility store. History/domain/task operations continue using `defaultStore`.

The SQL constructor is explicitly visibility-only. It creates normal pools but
does not run task-table schema checks, because a Go visibility-only SQL schema
contains `executions_visibility` without task tables. The existing SQL reader
query and Go-compatible selected query behavior are reused; no schema is
created, migrated, or copied.

The launcher also now installs persistence-backed domain state and the workflow
API hook before constructing HTTP, gRPC, History, and TChannel `AppState`
snapshots. This fixes stale in-memory domain manager captures across transports.
It does not route HTTP workflow operations through the hook; those handlers'
direct execution-store calls remain a separate Frontend follow-up.

## Tests and evidence

Added role selection assertions for distinct default/visibility stores,
unconfigured visibility, invalid store references, and lazy construction of a
MySQL visibility-only pool without task-schema validation. `git diff --check`
passed locally. No Cargo, rustfmt, or Go command was run locally.

Package CI was dispatched on branch `t3/frontend-visibility-store` with
`package=cadence-matching` and `format_all=true`:

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41332399218
- Head: `ba115d791b7d6558c01412ee01c60a94948f196f`
- Result: failed at Clippy with a missing `n_shards` binding in the new MySQL
  visibility constructor (`matching_task_store.rs:457` at that head). The
  uploaded `format-all-patch` was inspected; all formatting hunks for owned
  files were applied manually. The binding is corrected and CI is being rerun.

## Remaining evidence and limits

- No MySQL/PostgreSQL integration run has yet proven a Go-compatible visibility
  read from a separate configured database. The parent owns the corrected
  mixed-cluster follow-up; the already-running gate 41331739932 remains pinned
  to `f5d2151f9` and is not evidence for this change.
- Cassandra visibility remains unsupported by the existing visibility reader
  adapter and is an explicit gap; a configured Cassandra visibility role
  reports startup failure rather than silently using defaultStore.
- This work fixes SQL visibility database selection and Frontend transport
  state capture only. It does not complete Frontend parity or resolve the
  separate HTTP workflow-hook bypass.
