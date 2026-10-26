# Matching writer shutdown regression CI evidence

- Workflow: [rust-ci run 40366940209](https://github.com/auenkr/cadence-rust/actions/runs/40366940209), `success` (2026-10-26).
- Branch: `t3/matching-writer-shutdown-regression`.
- Embedded source commit: `c027a0ae66b80586e139a234d1d2f314dfeb5e0f` (`test(matching): bound writer shutdown join`). This is the tested SHA, distinct from any later evidence-note commit.
- Workflow log artifact downloaded to `/mnt/storage/service/ci-evidence/40366940209/rust-ci-logs/cadence-rust/cadence-rust/rust`.
- Fresh target evidence: `target-dir.txt` records `/home/runner/work/_temp/cargo-target-final`; test executable paths in `test.log` use this directory. No service binary artifact was emitted, so there is no binary SHA256 for this focused crate run.

## Commands and outcomes

The completed `clippy-test` job reports Clippy, Formatting, and Test successful. `fmt.exit=0` is the workflow's dedicated formatter gate; `test.exit=0` and `clippy.exit=0`. Advisory formatter logs are not clean: `workspace-format.exit=1`, `query-format.exit=1`, and `tls-format.exit=1`. These exit codes matter even though the overall workflow passed. Query/TLS formatter diagnostics are in `query-format.log` and `tls-format.log`; broad workspace diffs are in `format.log`.

Actual-writer regression result in `test.log`:

```text
test matching_writer::tests::shutdown_waits_for_inflight_writer_fatal_notification ... ok
```

The source test constructs a writer actor with a held in-flight guard, releases its modeled write, calls `writer.stop()`, then awaits `tracker.wait_idle()` under a two-second timeout before asserting that the fatal notification arrived. It does not pause a persistence store while runtime shutdown begins, so the evidence does not establish that stronger scenario.

Subprocess result in `test.log`:

```text
test standalone_matching_sqlite_malformed_run_id_exits_process_with_two ... ok
```

## Formatter residuals

- `format.log` shows two rustfmt diffs in `rust/crates/cadence-matching/src/matching_registry.rs`: line wrapping of the `fatal_writer` sender type near line 217 and `with_fatal_writer` signature near line 787. Both are formatting-only residuals in this changed Matching area.
- `format.log` has no `Diff in .../matching_writer.rs` section; the writer file changed by the tested commit has no reported diff.
- The workflow-level changed Matching runtime formatting step passed, as did the dedicated `fmt` gate. Do not treat that as evidence that all workspace/query/TLS formatting is clean: those advisory passes returned 1 and their uploaded logs contain diffs.

## Original log SHA256

Hashes below identify the downloaded originals; logs remain in the external evidence directory above.

```text
c1f378a7d271387e13083a3ae0c87f2d4ef833a4124843d1a4493e768ac2166d  clippy.log
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  fmt.log
9fd0836b6df6c7b866b2ccd6b1ef18573c5fe640abaf6c93ab312b9e52e3ac51  format.log
012b1402b7078690973d8cfecb5d27273420c2c18c2479bbdb42dc472dddc66a  query-format.log
de5a78fba8a05d0ee7b9e6d509434cb87cdbd2309367cfbf500a6fc65966924d  test.log
b843bdd7bed86ffb4ab44a318859e6559fb40dcf3c7a283f00d95b74f45fced4  tls-format.log
```
