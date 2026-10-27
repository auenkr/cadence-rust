# Matching native transport integration plan — 2026-10-28

## Scope and evidence

Parent evidence update: the downloaded raw-string artifact from run 40578298264
confirms successful marshal, independently constructed inbound unmarshal and
round-trip recovery of the original bytes for every tested domain/workflow ID,
poll identity and partition map key/value, including ff, fe and malformed UTF-8.
The run completed successfully. This supersedes the pending statements below.
Do not use a fallible conversion to the public String model on outbound Go
protobuf calls carrying raw identifiers; that boundary also needs a native codec.
Duplicate-map and other malformed-wire oracle cases have not been measured.

This plan covers only moving production Matching ingress from UTF-8 protobuf
requests to the existing byte-valued native request model. It does not claim
raw-byte parity is already measured or implemented. Workspace HEAD inspected:
`e085bf7f8da048082d397a27f5656a68e558e3cd`.

The focused oracle run [40578298264](https://github.com/auenkr/cadence-rust/actions/runs/40578298264)
was read once. It was **in progress**: the “Run Go raw-byte string oracle” and
“Run original Go malformed DescribeTaskList oracle” steps had succeeded, but
the run had not yet exposed final conclusions/artifacts. Its runtime findings
are therefore pending; do not infer results from the step names.

Pinned source evidence:

* `go/go.mod` pins `go.uber.org/yarpc v1.88.0` and
  `github.com/gogo/protobuf v1.3.2`; `go/go.sum` records their checksums.
* The pinned YARPC `encoding/protobuf/codec_registry.go` `protoCodec.Marshal`
  calls `Buffer.Marshal`; `protoCodec.Unmarshal` calls `proto.Unmarshal`.
  `encoding/protobuf/marshal.go` routes nonempty protobuf bodies through that
  codec. This establishes the code path, not the actual invalid-byte outcome;
  use the oracle artifact for that conclusion.
* `rust/crates/cadence-proto/build/native_model.rs` already generates plain
  Matching structs with `GoString` for string fields, including nested
  reachable API/shared messages and `HashMap<GoString, GoString>` for maps,
  plus fallible conversions to the public `String` protobuf structs. It also
  emits `NativeMatchingApi` with all 13 methods. These types deliberately do
  not implement `prost::Message`.
* The production implementation is still `impl MatchingApi for Matching`
  in `rust/crates/cadence-matching/src/grpc_matching.rs` (starts around line
  1923). Its methods accept `tonic::Request<m::...>` and consume `String`.
  `thrift_matching.rs` still defines UTF-8-only `string`, `task_list`,
  `execution`, and `string_map` decoders and builds `p`/`m` protobuf models;
  it calls `Matching`'s protobuf `MatchingApi` methods. Thus raw bytes are
  discarded/rejected before shared handler logic.
* The repository's pinned prost source is prost 0.13.5. `prost::Message`
  exposes `encode_raw`, `merge_field`, `encoded_len`, `decode`, and
  `encode_to_vec`. `prost::encoding` exposes key/length/varint/bytes helpers;
  its built-in `string::merge` validates UTF-8. A generated adapter can instead
  use byte-valued `GoString` fields and custom `Message` implementations; maps
  need custom map-entry encoding/decoding because the normal generated map
  entry uses `String` for its key/value. Merely implementing `Message` for the
  current `GoString` model is not enough: every nested string and map entry
  must use bytes merge/encode, preserve protobuf presence/default behavior,
  and follow the pinned gogo oracle on duplicate map keys and malformed wire.

## Recommended implementation shape

Keep `go/proto` and the generated public Rust protobuf types unchanged. Make
native requests the shared internal handler model. Put raw-byte protobuf
decode/encode at a **native Matching tonic service boundary** and retain the
existing public protobuf service/model as the unchanged contract surface.
Do not convert native requests through `TryFrom<Native> for protobuf` on the
request path: that conversion intentionally errors on invalid UTF-8.

Generate two things from the same descriptor set in `cadence-proto/build`:

1. The existing plain native data structs/conversions/trait.
2. A dedicated native protobuf wire adapter for the Matching service. It must
   implement protobuf wire behavior for native messages (including nested
   messages, oneofs, repeateds, wrappers and string-valued map entry keys and
   values), and a tonic server/router whose decoded request and encoded
   response types are those native messages. Reuse generated tonic routing
   metadata/method paths, but do not leave a generated `MatchingApiServer`
   decode step typed as the public `String` structs. Tonic's generic codec is
   the extension point: use a Prost-compatible codec over the generated
   native `Message` implementations. Confirm tonic 0.12's precise generic
   server bounds in CI during implementation.

`NativeMatchingApi` is already the handler trait, so implement it for
`grpc_matching::Matching`; port existing method bodies by changing request
and response types to native equivalents, and use `GoString::as_bytes()` or
byte-aware helpers wherever the current logic reaches identifiers, names,
identity, query type, or string maps. Return native response models all the
way to the codec. At any *outbound call to another protobuf service*, convert
at that actual protobuf boundary and preserve the existing explicit
`NativeStringError` (map it to the Go-equivalent status/error location).
Inbound protobuf gogo behavior must first be confirmed by the oracle; do not
insert UTF-8 validation based on the protobuf specification.

The smallest vertical slice is `PollForDecisionTask` and `AddDecisionTask`,
including their nested `TaskList`, `WorkflowExecution`, request identity,
partition-config map, native handler logic, and native reply encoding. It
proves ingress, nested/map wire decode and response transport without changing
the public protobuf descriptor. Do not enable this path as the service's only
router until all 13 trait methods are implemented and the router dispatches
every existing method. Then migrate Thrift ingress to the same native handler
trait, rather than routing Thrift through protobuf `String` models.

## Ordered file changes

1. **Oracle gate before semantic choices** — consume the already-running run's
   completed artifact later (parent owns this). Record per-case results for
   inbound decode, outbound marshal, nested identity/workflowID and map key/
   value, including duplicate keys and error outcome. If the run never
   completes, dispatch is parent-owned; this task does not dispatch or poll.
2. **`rust/crates/cadence-proto/build/native_model.rs`** — extend generator to
   emit native `prost::Message` implementations and the native Matching
   tonic server/service adapter from the descriptor. Implement byte strings
   with `prost::encoding::bytes`-equivalent length-delimited handling, not the
   validating `encoding::string` helper. Generate map-entry messages
   explicitly so raw bytes are accepted as string key/value wire payloads.
   Preserve tags, field ordering policy, unknown-field handling, oneof,
   optional/default semantics and recursion/error behavior; use oracle cases
   to settle gogo-versus-prost differences. Keep public generated structs and
   descriptor untouched. Add generated-wire tests for scalar, nested, map,
   empty/default, duplicate map key, malformed wire and valid public
   round-trip.
3. **`rust/crates/cadence-proto/src/native.rs`** — expose the native service
   server/codec module and focused adapter tests. Preserve `TryFrom` as an
   explicit boundary utility only; do not use it for native request handling.
4. **`rust/crates/cadence-matching/src/grpc_matching.rs`** — implement
   `NativeMatchingApi for Matching`, porting current 13 `MatchingApi`
   implementations with native request/response models. Extract transport-
   neutral method bodies only as needed to avoid duplicating business logic.
   Replace `&str`/`String` handler inputs with byte-preserving `GoString`
   where they identify queues/workflows/identities/maps. Keep validation and
   error precedence at the same post-decode handler point as Go. Ensure
   request metrics wrap exactly once at the service ingress (not once in the
   native router and again in the handler).
5. **`rust/crates/cadence-matching/src/lib.rs` and service registration** —
   locate the current `MatchingApiServer` registration (launcher/main/router)
   and replace its Rust-owned Matching endpoint with the native server for
   all methods. Do not register both on the same method path. Other services'
   outbound clients and the public protobuf contract stay unchanged.
6. **`rust/crates/cadence-matching/src/thrift_matching.rs`** — add raw
   `GoString` getters for required/optional strings and task-list/execution
   nested structs; change `string_map` to `HashMap<GoString, GoString>`;
   produce native Matching requests/responses and invoke `NativeMatchingApi`
   dispatch. Keep `normalize`'s Go `FromWire` duplicate/type behavior.
   Outbound Thrift `Value::Bytes` already supports arbitrary bytes: serialize
   GoString with `as_bytes().to_vec()` instead of UTF-8 conversion. Retain
   existing byte payload handling for workflow/query arguments.
7. **Boundary follow-through in current-service dependencies only** — update
   `matching_registry.rs`, `matching_forward.rs`, `matching_history.rs`,
   `matching_task_store.rs`, and `cadence-persistence/src/{matching_queue.rs,
   matching_task_store.rs,matching_blob.rs,matching_mysql.rs,
   matching_postgres.rs,matching_cassandra.rs}` only where the slice shows
   actual string conversion/keying. Carry raw queue names and map entries to
   the queue key, hash original task-list bytes for shard selection, and keep
   task blob Thrift bytes unchanged. Do not widen into non-Matching services.
8. **Tests/evidence** — Go oracle parity for native prost encode/decode and
   Thrift encode/decode; two-Rust-handler key-collision tests (invalid byte
   sequence must not alias U+FFFD), raw queue/map integration, matching SDK
   regression, and mixed-cluster lane with Rust Matching replacing Go
   Matching. Parent owns CI dispatch and evidence capture. No local Cargo,
   rustfmt, or Go commands.

## Traps that affect the design

* **Map key identity/collisions:** key equality/hash/ordering must stay on raw
  bytes end-to-end. Do not decode to `String`, use replacement characters,
  or stringify bytes for namespaces. Duplicate wire map keys follow oracle
  semantics (normally last key wins); HashMap iteration must not make
  serialized output a compatibility assumption unless Go oracle shows one.
* **Queue and persistence identity:** task-list names participate in registry
  keys, shard hash, SQL/Cassandra lookup and task blobs. UTF-8 conversion in
  any one of those paths collapses/rejects identifiers. Hash exact original
  bytes; do not alter UUID validation rules or backend-specific malformed
  run-ID error behavior while adapting types.
* **Metrics, tokens, and validation:** keep metric labels/attributes aligned
  with Go formatting and cardinality behavior; raw-byte quoting and invalid
  UTF-8 may change current Rust debug/Display output. Task-token JSON is a
  separate Go `encoding/json` boundary and must use the retained Go oracle's
  replacement semantics; protobuf losslessness does not mean JSON accepts a
  new byte encoding. UUID validation, panic/error precedence, sorting/limits,
  and request timers must remain in their current Go-equivalent stage.
* **Transport separation:** raw invalid protobuf bytes can only survive the
  Rust inbound protobuf decoder if that decoder stores byte strings. A
  byte-valued model added after tonic's generated public decoder is too late.
  Conversely, outbound calls to existing public protobuf clients must not
  claim raw string support unless the receiving Go codec oracle confirms it.

## Unresolved questions (must be answered with evidence)

1. What exactly did run 40578298264 measure for each string field on gogo
   marshal and unmarshal, including map duplicate behavior and malformed wire?
2. Does the actual tonic 0.12 generated server permit substituting native
   request/response Message types by a generated second service adapter, or
   should its server routing be emitted by a small custom service generator?
3. Which gogo wire corner cases differ from prost beyond UTF-8 (unknown
   fields, duplicate scalar/map entries, malformed lengths, recursion)? The
   oracle/tests should constrain only differences observable in matching's
   reachable descriptors, not promise full gogo equivalence without evidence.
4. For outbound History RPCs, which raw-byte cases are actually sent across
   protobuf, and what does the matching-specific Go interop lane demonstrate?
   Keep explicit fallible conversion until that boundary is measured.

Completion is not “native models compile”: production grpc ingress must decode
raw bytes into native requests, Thrift must dispatch the same native handler,
queue/persistence identity must remain raw-byte-safe, all 13 methods must be
served, and CI must demonstrate existing SDK plus mixed Go/Rust compatibility.
