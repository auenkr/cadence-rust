# Matching raw-byte JSON quoting — 2026-10-28

Added `GoString::go_json_string()` in `rust/crates/cadence-common/src/go_string.rs`.
It explicitly returns Go `encoding/json` quoted string text for arbitrary input
bytes: invalid UTF-8 consumes one byte per U+FFFD, Go control/quote/backslash
escapes are used, `<>&` and U+2028/U+2029 are HTML-safe escaped, and ordinary
valid Unicode is preserved. `GoString` has no serde serialization semantics.

Tests cover fixed malformed sequences, every byte value, escaping and valid
Unicode, plus `MATCHING_GO_RAW_STRING_ORACLE` rows (`hex`, `jsonText`) when the
CI-provided fixture is configured. This helper is intended for later
task-token JSON integration; no production consumer was changed. Integration
and CI validation remain pending. No local Cargo, rustfmt or Go commands were
run.
