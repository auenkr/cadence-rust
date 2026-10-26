# Focused persistence CI on transport foundation

Run https://github.com/auenkr/cadence-rust/actions/runs/40412875471 succeeded on `e7657429bb43171568eeff10d1b33a8b4c96c541`. Downloaded logs are under `/mnt/storage/service/ci-evidence/40412875471/rust-ci-logs/cadence-rust/cadence-rust/rust`.

Artifact commit and target files identify that SHA and fresh `/home/runner/work/_temp/cargo-target-final`. Clippy, persistence tests and scoped formatting exited 0; advisory workspace formatting exited 1. Persistence unit tests: 295 passed, 0 failed, 12 ignored. MySQL interop was ignored because it requires the external harness. This does not validate later session selection, metadata integration, body codecs, or live protocols 2/3.

Header/format run https://github.com/auenkr/cadence-rust/actions/runs/40413413581 succeeded at `1cc8f2d11fcf832c448a0f93cc2dc2f8e6accf27`. Its runner formatting patch was applied with a conflict resolution preserving the subsequent CQL language-version correction (`3.0.0` for every supported wire version). The session review `e3c1c5112` is not accepted as proof: it incorrectly described the integrated driver as emitting v4-only startup and missed the v2 language-version mapping defect. Pinned gocql `cluster.go:176` defaults language version to 3.0.0; `conn.go:448` sends that independent setting.

Go protocol matrix run https://github.com/auenkr/cadence-rust/actions/runs/40413206359 was dispatched at `494b0061de21609fec8892406887ccebc3df31bb` with Rust comparison disabled. It remains unreviewed; no new acceptance evidence is claimed.
