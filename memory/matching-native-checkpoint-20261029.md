# Matching native checkpoint — 2026-10-29

Documentation-only state captured on shared `master`. Matching remains
incomplete; no native mixed lane has passed. No CI/delegate status was polled,
no validation was run, and nothing was dispatched during this checkpoint.

## Candidate and validation

- Integrated source candidate: `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`
  on `t3/matching-startup-integration-review`. It includes the diagnostics,
  pool, and lock fixes. Source review exists; validation does not.
- Run `40839466118` failed the scoped formatting check at
  `matching_runtime.rs:925`; Clippy and tests were skipped. Formatting artifact
  run `40840392628` is expected to correspond to `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`
  and remains the last known live run. A separate Luna worker is collecting it;
  its result was not retrieved here.
- Adapter regression branch `t3/matching-lock-regressions` at
  `04826c745` covers four engine and three control cold production limiter
  paths. It has no validation evidence.
- Semaphore branch `t3/matching-semaphore-startup-timeout` at
  `112217ac44cb88c25cb35a9a6bb0ea60d9a2f5e2` adds the independent paused-native
  30-second no-deadline regression and maps startup failure from
  `DeadlineExceeded` to Go-compatible `Internal`. It is unverified.
- Terra is independently reviewing/integrating both regression branches. No
  branch was imported or validated as part of this docs-only checkpoint.
- Requested audit note source thread
  `t3/matching-native-gate-coverage1fc484820` returned `thread_not_found`.
  Therefore no audit-note content could be retrieved or credited.
- The old frozen candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`
  remains unchanged. Its native PostgreSQL mixed run failed before any SDK
  workflow; this is not a passed native lane.

## Ten required mixed lanes

All lanes require one coherent validated source SHA, Rust Matching with Go
Frontend/History/Worker and no Go Matching, and reviewed SDK, service interaction,
failure/restart/recovery, topology, exits, cleanup, and Rust binary SHA256.
No lane below has passed on the native candidate.

1. PostgreSQL — failed on old frozen `bf9f3d3`; SDK did not run.
2. MySQL probes.
3. Thrift Matching with TChannel History.
4. Configstore.
5. Real shard distributor / etcd.
6. Two-node MySQL — retain this lane; require dual-node behavior and recovery.
7. Cassandra semaphore/task interoperability.
8. Two-node adaptive scaling.
9. Active offer through Go History.
10. Production Matching TLS.

The supplemental Go History peer and PostgreSQL TLS dependency oracles remain
additional evidence, not substitutes for these ten lanes. Workspace CI must pass
fresh-target Clippy followed by workspace tests on the same frozen candidate
before serial mixed validation. There is no native lane pass to report.

## Dependency chain

1. Collect and review formatting artifact `40840392628` against the exact
   candidate SHA; independently establish whether formatting is resolved.
2. Complete Terra's source review/integration of both regression branches;
   reconcile their exact commits and preserve focused regression coverage.
3. Freeze one integrated candidate. Run fresh-target CI (Clippy then workspace
   tests) and record run URL, head SHA, conclusion, and actual exits.
4. Resolve startup/readiness and shutdown evidence before mixed acceptance.
   Then execute the ten lanes serially on that same SHA, preserving artifact
   identities and binary SHA256 for every lane, followed by supplemental oracles
   and the Matching parity audit.
5. Only after the audit and all required evidence are complete may migration
   proceed from Matching to History.

This checkpoint does not change source, workflows, frozen refs, or lane status.
