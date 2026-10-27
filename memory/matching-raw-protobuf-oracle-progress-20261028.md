# Matching raw protobuf oracle and native model checkpoint

Matching remains incomplete. The user requires exact Go raw-byte behavior.

The native model diagnostic at 07cbf2dc3 failed Clippy:
https://github.com/auenkr/cadence-rust/actions/runs/40549877892
The failure was `field_reassign_with_default` in the nested-identity test.
It is corrected by constructing the outer request with its nested request.
The previously uncommitted native Matching trait generator and its 13-method
surface assertion are retained in this change. Nothing has run locally.

The raw-string oracle now measures the actual pinned YARPC protobuf codec,
not a newer protobuf implementation. Source inspection shows generated
Matching imports `go.uber.org/yarpc/encoding/protobuf` (without `/v2`), whose
codec uses gogo Buffer.Marshal and proto.Unmarshal. Do not infer rejection of
invalid UTF-8 from protobuf specifications or Rust's String model.

For each existing raw string case, evidence now records independently built
inbound wire bytes, marshal/unmarshal errors, encoded bytes and recovered
field bytes for domain ID, nested workflow ID, nested poll identity, and
partition map keys/values. Output retains the prior hash/JSON fields so the
persistence oracle stays compatible. The runner retains module versions,
Go version and a Go formatting patch. Runtime conclusions remain pending.

Next: review the oracle, apply runner formatting, then implement the native
transport/handler/persistence integration. A fallible conversion to Rust's
public protobuf structs is a representation constraint, not evidence of
Go-compatible rejection. All source corrections need a new immutable pin and
fresh workspace and mixed-cluster gates before completion.

The existing 01:55 Asia/Kolkata continuation task is enabled. Its next run
returned by T3 is 2026-10-28T20:25:00Z (2026-10-29 01:55 IST).
Scheduling does not bypass provider usage limits.
