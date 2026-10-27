# Matching native codec source review — 2026-10-28

## Parent correction

The purported confirmed Int64Value defect below is withdrawn: pinned prost
0.13.5 `src/types.rs:191` explicitly implements Message for i64 as the
google.protobuf.Int64Value wrapper, preserving message framing and presence.
Other scalar wrappers have the corresponding prost implementations. Native
GoString intentionally uses a custom StringValue wrapper because the prost
String implementation validates UTF-8. No additional scalar wrapper is needed.
Actual CI40582378853 failed earlier in build configuration because the second
tonic pass repeated the built-in `.google.protobuf` extern mapping. Parent
removed that explicit duplicate. Generated code remains unverified until CI.

Reviewed `build/native_wire.rs`, `build/native_model.rs`, `src/native.rs`,
`src/native_service.rs`, and the native wire/transport tests at HEAD
`2911f943e`. This was source inspection only; no Cargo, Go, rustfmt, CI
dispatch, or source edits were performed. CI run
https://github.com/auenkr/cadence-rust/actions/runs/40582378853 was read once
and was still `in_progress`; this review makes no compile-pass claim.

## Confirmed defect

**`Int64Value` fields are generated with a scalar Rust type but a message
wire codec, so the new `Message` impl cannot compile.**

- `build/native_model.rs:51-62` maps `.google.protobuf.Int64Value` to `i64`.
- `build/native_wire.rs:4-8` special-cases only `StringValue`; line 25 maps
  the remaining message field kinds to `prost::encoding::message`.
- The reachable Matching schema has
  `PollForDecisionTaskResponse.previous_started_event_id` as
  `google.protobuf.Int64Value` (`go/proto/internal/uber/cadence/matching/v1/service.proto:134`).
- The generated `Option<i64>` implementation therefore calls
  `prost::encoding::message::{encode, encoded_len, merge}` with `i64`;
  those helpers require a `prost::Message`. The normal scalar case must also
  preserve wrapper message framing and presence (`Some(0)` is present).

Fix by using a dedicated Int64Value scalar-wrapper codec (and ensure each
other wrapper in `wrapper()` has a matching codec), or by representing and
converting wrapper messages as wrapper `Message` types. Keep `StringValue` on
the byte-preserving codec. Add a compile/runtime case for absent, present-zero,
and present-nonzero wrapper values.

## Scope and unverified points

The inspected scalar string helper routes native scalar strings through raw
bytes; `StringValue` has a distinct wrapper helper. I found no native path here
that invokes UTF-8 validation while encoding/decoding those fields. The tests
cover raw byte scalar/nested/map strings, unknown fields, duplicate scalar and
map keys, a oneof, presence and tonic transport, but do not establish behavior
for all generated shapes.

I did not establish additional defects in map key identity, repeated wrapper
merge, packed/unpacked numerics, recursive boxing, unknown-field skipping,
generated module paths, or the second tonic bridge pass. Their generated
output remains uncompiled in the available evidence, and the visible tests do
not cover every corner. In particular, do not treat those as audited parity
claims pending the running CI result and focused edge coverage.
