# Matching name-based domain cache read-through

## Change

Commit `9bca9b856` (`fix(matching): read through domains by name in listing RPCs`)
adds `Matching::domain_id_by_name` in `grpc_matching.rs`. With a production
`MatchingDomainCache`, it resolves by name on Tokio's blocking pool, publishes
cold metadata through `insert_matching_metadata_if_missing`, and resumes a
worker panic into the existing `matching_panic::capture` boundary. Without the
cache, it retains the existing in-memory domain lookup. Both native
`GetTaskListsByDomain` and `ListTaskListPartitions` now use this helper.

The regression in `matching_cold_domain_metric_tests.rs` starts from an empty
cache scan and uses a separate cold cache for each handler. Its source accepts
name lookups, verifies each first operation looks up the domain by name, and
checks that the discovered UUID/name reach Matching's metadata view.

## Evidence

- `git diff --check` passed before commit.
- No local Cargo, Go, build, test, format, or runtime command was run, per
  repository instructions.
- Public standard-runner Matching CI was dispatched for package
  `cadence-matching`: <https://github.com/auenkr/cadence-rust/actions/runs/40481424583>
- CI result was not polled in this implementation task; conclusion remains
  pending. Head SHA: `9bca9b856`.
- No mixed-cluster gate was dispatched; no Rust binary SHA256 is applicable.
