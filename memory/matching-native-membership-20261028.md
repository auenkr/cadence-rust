# Raw Matching membership boundary — 2026-10-28

Added byte-based `Ring::lookup_bytes` / `lookup_n_bytes`; existing UTF-8 entry points delegate to these. Raw membership owner/owns/remote-channel lookups use the same live ring, empty-ring refresh signal and peer cache. `Membership::native_membership` supplies the native registry callbacks from that live membership instance.

Ownership errors keep complete task-list bytes in the native status marker. For valid outer messages the existing shared.v1 typed detail bytes remain unchanged; invalid UTF-8 outer messages omit details as the measured grpc-go path does. Requires RawStatusService registration. This is the ring boundary only; shard-distributor spectator/eligibility/local-processor policy still needs its own raw-byte integration before production adoption.

Added tests for invalid-key/replacement-rune ring separation, raw ownership error bytes, valid typed detail equivalence and membership changes observed by the native callbacks. Source review and `git diff --check` only. CI required; no local Cargo/Go/formatting commands.
