# Native Matching protobuf codecs — 2026-10-28

## Parent implementation update

The worker's incomplete state below is superseded in source: parent added
`build/native_wire.rs`, invoked per native message by native_model.rs. It
generates Message encode/merge/length/clear for primitives, raw strings, maps,
packed/unpacked repeated values, nested messages, optionals/wrappers, oneofs
and unknown fields. StringValue has a byte-valued wrapper implementation
using the standard message recursion boundary. Public model is unchanged.
Parent added actual generated-message tests decoding both independent input
and marshal output from the Go oracle, raw output roundtrip, valid public
decoding, wrapper presence, unknown fields, malformed lengths/wire types,
duplicate keys and a HistoryEvent oneof. Combined CI is still required.

## Evidence read

The completed raw-byte Go oracle in `memory/evidence/matching-raw-protobuf-40578298264/go-oracle.json`
shows gogo/YARPC successfully decodes and re-encodes invalid UTF-8 in the
tested domain ID, nested workflow ID and poll identity fields, and map keys and
values. In particular, `ff`, `fe`, `e180ff`, and `c0af` survive protobuf
round trips byte-for-byte. The parent update at the top of
`memory/matching-native-transport-integration-plan-20261028.md` supersedes its
older pending-run statement. Duplicate map keys and malformed-wire oracle
behavior remain unmeasured.

## Work in this task

Added `native::wire::{encode_string, encoded_len_string, merge_string}` in
`rust/crates/cadence-proto/src/native.rs`. These use prost 0.13.5's public
encoding helpers and the native `GoString` storage directly; decode passes
through prost's bytes merge (including length and wire checks) and never
constructs a Rust `String`. A focused test covers raw `ff`, malformed two-byte
UTF-8 `c0af`, and empty payload encode/decode byte equality.

## Incomplete — not an actual native Message codec implementation

The task's main completion condition is **not met**. The generated structs in
`build/native_model.rs` still do not implement `prost::Message`; the raw-string
helpers are not wired into generated native structs or router codecs. No codec
coverage exists yet for generated numeric fields, packed repeated numbers,
nested messages, maps, oneofs, wrappers, optional/default presence, duplicate
map entries, unknown fields, or boxed recursion. No full generated-message
oracle comparison was possible here. This code has not been compiled or tested:
the project requires all Cargo/rustfmt/Go commands to run in CI, and this task
explicitly assigns CI dispatch and commits to the parent.

## Prost 0.13.5 API checked read-only

Inspected the pinned registry source at
`~/.cargo/registry/src/.../prost-0.13.5/src/{message.rs,encoding.rs}`.
`Message` requires `encode_raw`, `merge_field`, `encoded_len`, and `clear`;
`DecodeContext` can be passed to standard nested-message helpers while callers
cannot directly enter its private recursion methods. `encoding::map::{encode,
merge,encoded_len}` accepts custom key/value functions and its bounds support
`GoString` (`Default + Eq + Hash + Ord` for keys; `Default + PartialEq` for
values). `encoding::bytes::merge` is the appropriate primitive for raw string
payloads. The next implementation should generate Message impls from the
descriptor alongside native structs and use custom raw-byte string helpers for
every `GoString`, including map key and value closures.
