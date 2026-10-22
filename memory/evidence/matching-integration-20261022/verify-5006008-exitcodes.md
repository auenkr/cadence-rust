# Gate exit codes for 5006008193b08e3d5e51b6f37a28126dcdaf954e

Head: 5006008193b08e3d5e51b6f37a28126dcdaf954e (origin/t3/matching-integration-20261021). No code changes.
Setup note: go/idls submodule had to be initialised in the new worktree (git submodule update --init go/idls); first attempt failed with 'Cadence IDL missing' before that.

```
cd rust && CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared
flock /tmp/cargo.lock cargo clippy -j2 --workspace --all-targets -- -D warnings  -> exit 0
flock /tmp/cargo.lock cargo test -j2 --workspace  -> exit 0
```

Tests: 24 suites, 862 passed, 0 failed, 17 ignored.

## clippy log tail
```
    Checking cadence-common v0.1.0 (/mnt/storage/service/github/wt/verify2/rust/crates/cadence-common)
    Checking cadence-server v0.1.0 (/mnt/storage/service/github/wt/verify2/rust/crates/cadence-server)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 54.90s
```
## test log tail
```

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```
