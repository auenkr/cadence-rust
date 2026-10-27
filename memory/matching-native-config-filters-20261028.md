# Matching native dynamic-config task-list filters (2026-10-28)

Implemented additive byte-name typed accessors in
`rust/crates/cadence-persistence/src/dynamic_config.rs`:
`task_list_integer_bytes`, `task_list_boolean_bytes`, `task_list_float_bytes`,
and `task_list_duration_bytes`. Existing `&str` accessors retain their public
signatures and delegate to these raw counterparts. Filter selection remains
conjunctive and ordered: first matching filtered entry wins; the last
unfiltered decodable entry is fallback. Task type spelling/numeric value,
typed value parsing, duration conversion, and defaults are unchanged.

Task-list filter values decode as JSON strings and match by comparing their
UTF-8 bytes with the actual GoString bytes. No lossy conversion is used. Since
the runtime property provider accepts JSON string filters only, invalid UTF-8
names skip runtime evaluation and use the exact snapshot matcher; they are not
converted to replacement runes or treated as unset. Domain stays `Option<&str>`.

Reference: the pinned Go implementation is
`go/common/dynamicconfig/configstore/config_store_client.go`,
`matchFilters` (around lines 585-604): it decodes each filter blob and compares
`filters[filterKey] != requestValue`, requiring every constraint and rejecting
unknown/missing filters. The adjacent file-based `match` in
`go/common/dynamicconfig/file_based_client.go` applies the same conjunctive
subset equality rule. Go string/interface equality compares string bytes, so
Rust compares decoded filter string bytes directly with the GoString task-list
bytes.

Unit coverage was added for the existing valid fixtures, ff/fe versus U+FFFD,
invalid-name fallback/defaults, multi-filter constraints and duration parsing.
No Cargo, Go, or rustfmt command was run per task instructions. The change is
therefore unvalidated by execution; parent owns CI and commit/push.

Integration boundary: current byte-aware accessors are available, but the
Matching consumers still need to pass native queue-name bytes to them. Their
existing `&str` paths remain valid for UTF-8 names. Runtime-property providers
cannot represent arbitrary GoString bytes through the current `Value::String`
filter map; invalid names consequently use snapshot evaluation as described.
No domain-name raw-byte migration or future-service work is included.
