# Matching runtime delayed-append shutdown regression CI evidence

- Workflow: [rust-ci run 40371677686](https://github.com/auenkr/cadence-rust/actions/runs/40371677686), conclusion `success`.
- Tested head: `10c7e8995aefe717b3d4388a405b07fee3a5b7bd`.
- Inputs: `package=cadence-matching`, `clean_target=true`.
- Fresh target directory: `/home/runner/work/_temp/cargo-target-final`.
- The source includes the implementation commit `ea0a367c95bd13c26e131a8452438d47feaafb56`; the additional tested commit reformats the test and records the first CI result.

## Results

- Changed Matching runtime formatting: passed.
- Clippy: passed.
- Formatting: passed.
- Test: passed. Matching library: 724 passed, 0 failed, 3 ignored.
- New regression: `matching_runtime::tests::runtime_shutdown_waits_for_real_delayed_append_and_fatal_notification ... ok`.
- Existing subprocess regression: `standalone_matching_sqlite_malformed_run_id_exits_process_with_two ... ok`.
- Go oracle steps before Rust validation all passed.

The regression creates a durable runtime with a delegating SQLite store wrapper
that blocks inside the real `create_tasks_with_domains` append. It gets a queue
owner through the runtime registry, exercising the actual TaskWriter's automatic
tracker registration and fatal channel. Runtime shutdown is started while the
store call is blocked and is asserted pending; after releasing the store, SQLite
returns its typed malformed RunID error. Shutdown then returns, after which the
test confirms that exact typed cause is already readable from the fatal channel.

The first attempt, run [40371022510](https://github.com/auenkr/cadence-rust/actions/runs/40371022510),
failed only at rustfmt for the changed runtime source; compilation and tests were
skipped. Its formatting diagnostics were applied before this successful run.
This is a focused Matching crate CI run, not a mixed-cluster gate; no service
binary SHA256 applies. No local Cargo, Go, or rustfmt commands were run.
