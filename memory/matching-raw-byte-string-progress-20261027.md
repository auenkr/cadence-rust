# Matching raw-byte string parity — explicit requirement

The user explicitly selected **Match Go for raw bytes** on 2026-10-27. Invalid
UTF-8 in Thrift strings is a required parity case, not an accepted divergence.
V7 remains immutable as a baseline. Any integrated source correction needs a
new candidate and fresh workspace plus all mixed lanes rerun at that pin.
The running Configstore lane has finished; subsequent v7 dispatches are held.

Implemented foundation (not yet compiled or used by production handlers):
- cadence-common::go_string::GoString owns arbitrary bytes. Equality, ordering,
  hashing and borrowed map lookup use original bytes. UTF-8 conversion is
  explicit and fallible. No surrogate namespace or lossy identifier conversion.
- cadence-api reexports the type for the native wire layer.
- cadence-thrift supports raw scalar getters, raw map keys and FromValue/
  ToValue conversions. Existing UTF-8 getters remain unchanged until callers
  migrate; no decoder-only parity claim is made.
- Tests cover all256 byte values, invalid-byte/replacement-character identity
  separation, embedded NUL, wire reencoding and duplicate raw map keys.
- The CI formatting input includes the four touched files. No local Cargo,
  rustfmt, Go build or tests have run.

Required integration work:
1. Decouple the shared Matching handler model from protobuf String fields.
   Thrift currently converts native API structs to cadence_proto requests
   before invoking MatchingApi; preserving bytes only in the decoder is
   insufficient. Preserve valid protobuf APIs while giving native handlers a
   byte-preserving model. Validate UTF-8 at actual protobuf boundaries.
2. Carry raw task-list names, identities, workflow/run IDs, query names and
   string map entries through queue keys, registry state and History/Matching
   forwarding. UUID validation, panic/error precedence and request metrics
   must occur at Go-equivalent points, after decode.
3. Persistence: TaskListKey.name, go_db_shard_id, matching_blob TaskData and
   partition maps are currently String-based. Hash original bytes; preserve
   original Thrift blob bytes and backend parameter/error semantics. Follow
   Go's distinct SQL/Cassandra malformed-run-ID handling.
4. Outbound native Thrift must reproduce original bytes. Protobuf rejects
   invalid strings at its own boundary. JSON/task-token handling needs Go's
   actual encoding/json semantics, rather than applying lossless bytes as a
   new JSON wire format. Sorting, limits, metrics and error quoting require
   explicit review wherever current code assumes UTF-8.
5. Add Go runtime oracles and end-to-end raw-identity/map/identifier tests.
   Primitive tests alone do not close the requirement. Then validate all
   source, freeze a new pin and repeat workspace and every mixed lane.

Host load remains above6, so no new workers may start under AGENTS.md. Parent
is progressing with narrow source reads and all validation remains CI-only.
