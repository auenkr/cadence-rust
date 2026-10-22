Command: `flock /tmp/cargo.lock cargo clippy -j2 -p cadence-server --all-targets -- -D warnings`

Source HEAD: `86f896f7782c0613b214e0325713f0c45a4838ab`

Thu Oct 22 05:10:13 PM UTC 2026

Exit code: `101`
    |
221 |     drop(put);
    |     ^^^^^^^^^
    |
note: argument has type `{closure@crates/cadence-persistence/src/matching_domain_metadata.rs:145:19: 145:30}`
   --> crates/cadence-persistence/src/matching_domain_metadata.rs:221:10
    |
221 |     drop(put);
    |          ^^^
    = help: for further information visit https://rust-lang.github.io/rust-clippy/rust-1.98.0/index.html#drop_non_drop
    = note: `-D clippy::drop-non-drop` implied by `-D warnings`
    = help: to override `-D warnings` add `#[allow(clippy::drop_non_drop)]`

error: could not compile `cadence-persistence` (lib) due to 1 previous error
warning: build failed, waiting for other jobs to finish...
