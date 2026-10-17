# Native Matching Thrift (2026-10-17)

All ten pinned MatchingService methods and Meta::health now dispatch directly to
Rust Matching via TChannel and YARPC HTTP. No Go runtime dependency. Original Go
generated clients pass real decision/activity History starts, query dispatch and
completion, typed exceptions, cancellation, loaded-manager snapshots, partition
listing, and empty-poll deadline tailroom. Both /matching and /matching/ work;
Axum nested-root trailing-slash behavior previously routed the latter to frontend.

Probe: /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-native-fixed
Two codec unit tests pass (nanosecond precision/transient events and exception IDs).
Full runner includes matching-native-thrift. Workspace tests passed; initial
strict Clippy found a redundant struct update, now removed. Full rerun pending.

Whole Matching remains incomplete. Work continues on membership placement,
ownership guards/unloading, distributed forwarding and production integration.
