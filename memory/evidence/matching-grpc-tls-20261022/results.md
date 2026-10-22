# Matching gRPC TLS (branch matching-grpc-tls-v2-20261022)
All with CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared, flock /tmp/cargo.lock, -j2, cwd rust/.
- `cargo check -p cadence-server --all-targets -j2` -> exit 0 (after `git submodule update --init go/idls`)
- `cargo clippy -p cadence-server --all-targets -j2 -- -D warnings` -> exit 0
- `cargo test -p cadence-server --lib grpc_tls -j2` -> exit 0, 5 passed (plaintext, Go yaml names, mTLS CA admission, cert/key pairing, host verification)
Not run: full workspace tests, mixed Go/Rust TLS cluster run, HTTP TLS (pre-existing separate path).
