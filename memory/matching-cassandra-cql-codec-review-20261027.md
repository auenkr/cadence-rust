# Matching Cassandra CQL error and value codec review — 2026-10-27

## Scope and checkout

Reviewed assigned branch `t3/cassandra-error-value-review` at `494b0061d`
(`Add default v4 baseline to configured protocol probes`). That commit is also
the current tip of `origin/t3/matching-cql-integration`, so the requested merge
had no delta. Kept edits out of the request/response traits and body dispatch,
session, connection, and metadata paths. This is source review only; no Cargo,
Go, formatting, runtime gate or protocol parity claim.

Go contract is pinned in `go/go.mod` to
`github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1`.

## Error-response comparison

The locked Rust dependency is `scylla-cql-core 1.9.0`, Cargo.lock checksum
`72ca9de2eb08d04a9c85002ac179c51353c91152cd2cd58fc3d7cbc1eee8ab16`.
It is currently a crates.io dependency, not an existing vendored crate in this
checkout (`rust/vendor/scylla-1.9.0/Cargo.toml:166` references the registry
package). Its source was inspected at
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/scylla-cql-core-1.9.0/src/frame/response/error.rs`.

For errors relevant to v2/v3 query workloads, the Rust decoder matches pinned
gocql's `frame.go:576-700` field order and wire widths:

* `UNAVAILABLE` 0x1000: consistency i16, required i32, alive i32.
* `WRITE_TIMEOUT` 0x1100: consistency i16, received i32, block-for i32,
  string write-type.
* `READ_TIMEOUT` 0x1200: consistency i16, received i32, block-for i32,
  one-byte data-present.
* `READ_FAILURE` 0x1300: same prefix then i32 num-failures and one-byte
  data-present for protocol <=4. gocql switches to an error map only above v4;
  Rust's scalar count is correct for v2/v3.
* `WRITE_FAILURE` 0x1500: same prefix, i32 num-failures, string write-type
  for protocol <=4; gocql's error-map branch is above v4 only.
* `ALREADY_EXISTS` 0x2400: keyspace and table strings.
* `UNPREPARED` 0x2500: short-bytes statement id.
* `FUNCTION_FAILURE` 0x1400: keyspace, function and string-list argument
  types. The decoder uses the protocol string-list encoding.

The core decoder also maps standard simple codes and retains unrecognized codes
as `DbError::Other(code)`; gocql's old decoder instead panics on an unknown
code (`frame.go:689-700`). This is not a Rust compatibility gap. Existing Rust
unit fixtures in that same `error.rs` cover simple errors, unavailable,
write/read timeouts, failures and function failure; they are source fixtures,
not newly run validation. No independent error-code fix is justified here.

## Value encodings and concrete protocol-version gap

Pinned Go gocql's `marshal.go:1510-1534` `writeCollectionSize` uses an unsigned
16-bit big-endian size when `info.proto <= protoVersion2`, and signed-width
32-bit big-endian size for v3+. It applies that same size function both to
the collection element count and each encoded element length; `marshalList`
at `1550-1570` and `marshalMap` at `1675-1710` demonstrate the repeated use.
Thus v2 list/map collection payloads are:

* v2: u16 count; each item is u16 length + item bytes (map alternates key,
  value); maximum count/item length 65535.
* v3+: i32 count; each item is i32 length + item bytes (map alternates key,
  value).

Rust core `serialize/value.rs:940-976` and `1102-1150` always append an i32
element count; `serialize/writers.rs:145-178` makes each sub-value length an
i32. The corresponding deserializers likewise parse the fixed 4-byte
collection count and element lengths. That is correct for v3+ including the
current fixed-v4 session but is a concrete incompatibility for protocol v2.
The same width branch is needed in list/map result decoding, including
collection-valued cells returned by the server.

Other Matching value payloads do not themselves need protocol-specific data
layouts: `blob` is opaque bytes, `uuid` is 16 network-order bytes, `int` is
signed i32 big-endian, and `timestamp` is signed i64 milliseconds since epoch.
Their outer bound/query cell framing is also version-sensitive at the frame
layer, but is out of scope here (owned by the request/response worker). Maps
and lists are the value-layer exception above.

Go's `UnsetValue` is documented in pinned `frame.go:21-28` as protocol v4 only;
`doc.go:152` agrees. Rust core exposes `Unset`/`MaybeUnset` and writes the
outer-cell sentinel -2 in `serialize/value.rs:383-417` and
`serialize/writers.rs:118-126`. Therefore v2/v3 must reject/never emit this
sentinel; v4+ can emit it. The current Rust Scylla driver path is fixed v4
(as recorded in `memory/matching-cassandra-protocol-parity-20261021.md`), so
this proves only the v4 primitive exists, not v2/v3 end-to-end support.

## Smallest required follow-up

Do not patch the registry cache. To support protocol 2 correctly, vendor the
locked `scylla-cql-core 1.9.0` source into `rust/vendor/scylla-cql-core` and
patch the Cargo dependency to that local crate, then thread selected protocol
version into collection serialization/deserialization (u16 item count and
item lengths at v2, i32 at v3+) and reject UNSET for v2/v3 at outer bound
value serialization. The parent-owned protocol/request path must supply that
version. Keep existing fixed-v4 behavior unchanged. Add byte fixtures for
empty and one-element list/map in v2/v3, plus lengths/count boundary rejection,
and an UNSET v2/v3 rejection + v4 `-2` fixture. Run the Cargo checks only in
Actions per repository instructions. Because this entails a new vendored core
crate and a protocol parameter crossing shared serialization paths, no code
change was made in this narrow review.

## Evidence limitations

No CI run was dispatched because the parent persistence run
`40412875471` was already underway and the task explicitly prohibited
dispatching a competing gate. This note records source-grounded findings only.
