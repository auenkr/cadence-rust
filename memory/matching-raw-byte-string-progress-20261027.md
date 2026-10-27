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

Foundation source: 00ce7a04f. Focused common+Thrift Clippy/test and runner
formatting CI: https://github.com/auenkr/cadence-rust/actions/runs/40546474520.
Conclusion **success**. Downloaded artifact head matches, Clippy/test exits
are0, and the common/Thrift raw-byte tests execute successfully. Compact proof
is memory/evidence/matching-raw-foundation-40546474520/. Runner formatting
patch applied in e7a932f17. This is not production integration evidence.

e7a932f17 adds go_db_shard_id_bytes and makes the existing UTF-8 entry point
delegate to it. The new entry point hashes canonical domain UUID bytes, an
underscore, then the original task-list bytes. A new Go oracle emits FarmHash32,
byte lengths/UTF-8 validity and encoding/json output for ten cases (including
NUL, ff/fe, replacement rune, overlong/truncated encodings and Unicode).
The persistence test compares raw names against that executable Go output
across five shard counts. It runs with the fixture exported by rust-ci; without
the environment fixture it provides no Go-oracle evidence.

Current focused common+Thrift+persistence CI:
https://github.com/auenkr/cadence-rust/actions/runs/40548080744, head e7a932f17;
conclusion pending. Apply its runner formatting before a new candidate.

Integration decision still to implement: native Matching requests must use
byte-valued string fields in a transport-neutral model, with a protobuf adapter
that performs explicit UTF-8 validation at that actual transport boundary.
The present shared handler implementation directly consumes cadence_proto
String-bearing requests, so changing Thrift getters alone cannot solve this.
Prefer explicit typed bytes over a surrogate Unicode encoding or ambient raw
byte lookup table. Public protobuf contracts must stay unchanged.

JSON caution: Rust from_utf8_lossy groups some malformed sequences differently
from Go utf8.DecodeRuneInString/encoding/json (Go consumes one byte per invalid
rune). Use the retained Go oracle when implementing task-token JSON behavior;
do not assume whole-buffer lossy conversion has Go's replacement semantics.
