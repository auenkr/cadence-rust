Command: `flock /tmp/cargo.lock cargo check -j2 -p cadence-server`

Source HEAD: `86f896f7782c0613b214e0325713f0c45a4838ab`

Thu Oct 22 05:06:28 PM UTC 2026

Exit code: `101`
    Checking rusqlite v0.32.1
    Checking clap_builder v4.6.7
    Checking hickory-proto v0.24.4
    Checking cadence-thrift v0.1.0 (/mnt/storage/service/github/wt/parityfix/rust/crates/cadence-thrift)
    Checking lru-cache v0.1.2
    Checking rustls-native-certs v0.8.5
    Checking mysql_async v0.37.1 (/mnt/storage/service/github/wt/parityfix/rust/vendor/mysql_async-0.37.1)
    Checking scylla v1.9.0 (/mnt/storage/service/github/wt/parityfix/rust/vendor/scylla-1.9.0)
error: failed to run custom build command for `cadence-proto v0.1.0 (/mnt/storage/service/github/wt/parityfix/rust/crates/cadence-proto)`

Caused by:
  process didn't exit successfully: `/mnt/storage/service/github/wt/parityfix/rust/target/debug/build/cadence-proto-321f1d47fd5b0e99/build-script-build` (exit status: 1)
  --- stderr
  Error: "Cadence IDL missing: run git submodule update --init go/idls"
warning: build failed, waiting for other jobs to finish...
