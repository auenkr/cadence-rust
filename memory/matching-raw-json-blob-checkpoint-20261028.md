# Matching raw JSON, blob and protobuf checkpoint

Matching remains incomplete. Exact raw-byte parity is required.

Focused native-model CI **passed** at e085bf7f8:
https://github.com/auenkr/cadence-rust/actions/runs/40578298264
Compact retained proof: `memory/evidence/matching-raw-protobuf-40578298264/`.
The original Go YARPC/gogo codec successfully marshals, unmarshals independently
constructed input, and round-trips all ten raw-string cases in each of domain
ID, nested workflow ID, nested poll identity and partition map keys/values.
The ff and fe cases stay distinct from each other and valid replacement rune.
UTF-8 rejection at Rust's public protobuf model is therefore NOT Go parity.
The runner Rust and Go formatting patches have been applied.

Three Luna workers returned JSON quoting, additive raw blob models/codecs,
and an implementation plan. Parent review corrected:
- Invalid UTF-8 JSON output must be escaped `\ufffd`; a valid replacement
  rune remains literal. The e180ff case consumes three invalid bytes, not two.
- The raw map test inserted two keys but expected three; it now inserts and
  distinguishes ff, fe and the replacement rune.
- Infallible legacy-to-raw task conversion uses From. The nested fallible
  partition conversion specifies its error type to avoid ambiguous inference.

Parent connected the quoting helper to existing production task/query-token
serialization and added raw-valued token input types. Legacy public method
signatures remain compatible. Token field order, HTML/control escaping and
numeric fields follow the Go struct and encoding/json serializer. Raw bytes
are replaced only at JSON serialization; no lossy identifier lookup is added.
The oracle now invokes the actual original Go token serializers, with Rust
tests comparing whole output bytes. Additional fixtures cover all 256 byte
values, truncated UTF-8 and Unicode line/paragraph separators.

The raw blob codecs are additive and not yet consumed by the stores/handlers.
Production Matching ingress, queue keys and native protobuf/Thrift forwarding
still need migration. No integrated source pin or service completion is claimed.
No local Cargo, rustfmt, Go build/test or mixed gate ran. New source validation
must run on Actions; current changes are unvalidated until that run completes.

Host load rose above 6 again, so replacement workers are deferred under the
resource rule. The 01:55 IST continuation trigger stays enabled.
