# Matching combined candidate source review — 2026-10-27

## Decision

Source integration approved for `t3/matching-combined-20261027` at
`c0ecc437ccf91504b1e941947ac16256ba5b21fc`, including the scoped adaptive
probe change from `t3/matching-adaptive-retry` at
`9e42c8a116f1d1ef52a62dd32d5dbf97bdfb458e`. No source blocker found.

## Scope and findings

- Both tips descend from `master` (`fd11426423139263c9b0175b075fb1457c8cd7c3`).
  The adaptive tip is an ancestor in the combined candidate history. Its
  production-code delta against master is empty; its tip change only raises
  the adaptive SDK subprocess timeout from 180s to 240s to leave headroom over
  the SDK's 180s context budget.
- The combined production delta is limited to
  `matching_panic.rs`, `go_strconv_isprint.rs`, and
  `matching_domain_cache.rs`. No `ListTaskListPartitions` production change
  appears. The rejected `ca64` behavior (listing persisted adaptive partitions)
  is absent; Go's static configured partition-count semantics remain intact.
- Cassandra UUID normalization is confined to the Cassandra metadata source:
  it mirrors pinned gocql parsing and canonicalizes IDs for cache lookup and
  insertion. The default normalization keeps SQL stores unchanged. Existing
  `InvalidUuid` BadRequest classification and the SQL-specific error mapping
  remain unchanged in the diff. No strict SQL UUID validation relaxation was
  found.
- The fatal-writer runtime regression remains present in
  `matching_writer.rs::shutdown_waits_for_inflight_writer_fatal_notification`;
  it constructs a writer with the fatal channel/tracker, induces malformed SQL
  run-ID failure, stops the writer, joins the tracker, and asserts the fatal
  notification. Launcher fatal-writer handling and the guard wiring are also
  present at the candidate tip.
- Panic quoting changes match Go string quoting; they do not affect the
  adaptive or UUID behavior under review.

This is a source-only review. No Cargo/Go/rustfmt commands, CI dispatch,
mixed-cluster gates, or master merge were performed. No runtime result is
claimed; parent owns integration and validation.
