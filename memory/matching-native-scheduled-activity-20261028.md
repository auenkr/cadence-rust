# Native scheduled activity dispatch decoder — 2026-10-28

The earlier worker reported that generated type shapes required a local Cargo build. The actual source prerequisite was the missing go/idls submodule. Initialized the existing pinned commit bfbf273b41b9e36ce21e47a49600d22d73e4614c with git submodule update --init; no local Cargo or Go ran.

Added scheduled_activity_event to thrift_matching_native_extra.rs and wired it into AddActivityTask field 80 / dispatch field 10. Traced field IDs in go/.gen/go/shared/shared.go and shapes in go/idls/proto/uber/cadence/api/v1/history.proto and common.proto. Mapper reference: go/common/types/mapper/proto/api.go FromActivityTaskScheduledEventAttributes, FromRetryPolicy, FromHeader, FromHistoryEvent.

The activity-scheduled variant (Go event type 9, Thrift attributes field 130) preserves activity/type/domain/task-list bytes, payloads, all four timeout presences, decision completion ID, retry policy (including raw error reasons and jitter), raw header keys/values, event timestamp/version/task ID. Absent retry intervals become present zero Durations as in Go's scalar types model. Wrong-typed scalar duplicates are skipped; a later malformed string list clears earlier reasons.

Added focused complete-graph and duplicate/presence tests. Unsupported event variants still return an explicit error. Nil event types / missing attributes and full History graph parity are not claimed. Production native handler registration is still outstanding.

CI run 40583973417 at c51012f9e reached clippy and failed on native_wire_edges.rs chunks_exact_to_as_chunks. Fixed as suggested with as_chunks::<2>().0.iter(). Applied the runner-produced matching-format.patch; no local rustfmt ran. Its passing compilation work is not a complete test pass.

The new decoder and native backend integration need the next CI pin; no test pass is claimed here.
