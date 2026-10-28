# Native Matching semaphore identifier primitives (2026-10-28)

## Scope

Implemented only the owned, unregistered primitives:

- `rust/crates/cadence-common/src/semaphore_bytes.rs`: byte-preserving `OwnerBytes`, encoder, and parser.
- `rust/crates/cadence-matching/src/matching_semaphore_native.rs`: byte-preserving `IdentifierBytes`, validation, log string, and ring key.

The owner/task explicitly excluded editing either crate's `lib.rs`, existing `semaphore.rs` files, manager/persistence, and registration/integration. The follow-on manager must register and consume these modules. No manager or persistence parity is claimed.

## Source evidence consulted

- `go/common/semaphore/owner.go`: `Owner.String` uses `strconv.Itoa(len(WorkflowID))` (Go string byte length), writes raw workflow/run bytes between `:`, parses the prefix with `strconv.Atoi`, rejects negative and noncanonical prefix spellings, then parses hold ID with `strconv.ParseInt(..., 10, 64)` and rejects noncanonical decimal spelling.
- `go/service/matching/semaphore/identifier.go`: empty domain/name are rejected, negative bucket rejected; `RingKey()` delegates to common semaphore routing.
- `go/common/semaphore/routing.go`: `RingKey` formats raw domain bytes, `_`, raw semaphore-name bytes, `_`, decimal bucket. The related owner bucket selection uses `farm.Fingerprint32` over raw owner-ID bytes.
- `rust/crates/cadence-common/src/go_string.rs`: `GoString` retains arbitrary bytes and exposes explicit UTF-8 validation.

The parser uses byte offsets throughout and does not slice Rust UTF-8 strings. Integer syntax is ASCII decimal with optional sign, then exact canonical-string comparison reproduces Go's rejection of `+`, leading zeroes, and `-0`; signed i64 boundaries are checked.

## Regressions included (not executed)

Inline unit tests cover arbitrary invalid UTF-8 (`ff`, `fe`), valid U+FFFD distinct from invalid bytes, NUL and separator bytes, multibyte byte length, malformed/truncated length boundaries, canonical numeric forms and i64 minimum/overflow, raw semaphore-name bytes, validation, ring-key byte identity, and hash distinction for raw vs replacement-rune keys.

## Validation status

No Cargo/Go/rustfmt/build/test/check command was run, per task instructions. `git diff --check` passed. Unit tests remain unverified. These source-level notes do not claim runtime validation.

Parent registered both modules and formatter entries. Fixed non-ASCII Rust byte literals, a numeric error-wording assertion and owner error quoting. New `GoString::go_quoted_string` follows strconv.Quote using exact Go 1.25 printable tables copied from the existing Matching panic implementation. Invalid bytes escape as `\xNN` instead of being rewritten by JSON quoting or quoted twice by Rust Debug. Worker corrected the signed magnitude parser to accept MinInt64. Tests also cover workflow bytes cut at a non-UTF8 boundary and exact raw-byte error text. Manager/persistence integration remains required. No local compiler or formatter ran.
