# Ringpop JSON request boundary audit — 2026-10-19

Source-only review at main c64587bdf; relay candidate in t3-6f14baa6 remains unintegrated. This note is a parity work item, not passing interoperability evidence.

Pinned Go tchannel-go v1.34.4 json/handler.go Handle allocates a nonnil pointer to the request struct using reflect.New(h.argType.Elem()), then calls ArgReadHelper.ReadJSON on it. arguments.go ReadJSON: exactly zero bytes return nil without decoding; nonempty body uses json.NewDecoder(reader).Decode(data) once. Consequences to verify by actual raw Go transport tests:

- top-level null leaves the preallocated request at its zero values and reaches the Ringpop handler;
- zero-length body also reaches the handler with zero values;
- whitespace-only nonempty body is a decode error;
- a second JSON document/trailing bytes may be ignored after a successful first Decode; use actual fixtures rather than assuming json.Unmarshal whole-document semantics;
- malformed field types remain decode errors before handler counters/logic.

Current Rust generic ringpop_provider::decode uses serde_json::from_slice, which rejects null for struct targets, rejects zero length and checks complete consumption. New relay Rust-only test initially assumed null should be rejected; parent review flagged the unsupported assumption. Worker is tasked to compare actual Go ready/not-ready PING-REQ null/empty/whitespace/trailing documents and repair request decode accordingly. JoinRequest, Ping and PingRequest all implement Default, permitting a shared zero-value mapping when actual evidence confirms it.

Do not confuse this streaming transport boundary with JSON discovery files: Go discovery JSON file uses json.Unmarshal and must validate the entire document before decoding type errors; that behavior was separately ported/tested in6977c326d.

TChannel transport Arg2 JSON headers also use the same Go ReadJSON routine. Rust tchannel.rs currently parses headers with serde_json::from_slice, so empty/trailing header differences remain a separate transport audit after the relay request patch. Similarly audit outbound response decoding/client framing instead of assuming from_value has identical Go null/empty/streaming behavior. This is not a request to replace unrelated transport code inside the current relay patch.
