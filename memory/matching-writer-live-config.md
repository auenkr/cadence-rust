# Matching writer live configuration (task 48)

Implemented live sampling for `matching.maxTaskBatchSize` and `matching.appendTaskTimeout` in the existing async `TaskWriter`. The registry installs a domain/task-list/task-type provider from the admin dynamic-config overlay. The writer samples batch size after receiving the initial append and samples append timeout per append attempt, including retries. Values retain signed dynamic-config semantics: negative batch sizes drain no extras, and nonpositive timeouts immediately expire. Defaults are Go's 100 and 5 seconds. `WriterConfig` and `TaskWriter::new` remain available for tests and other default callers. Outstanding append capacity and zero-capacity rendezvous remain intentionally deferred.

Evidence run:

- `cd rust && CARGO_BUILD_JOBS=1 cargo test -p cadence-server matching_writer --lib` — passed (4 tests: Go batch oracle, timeout distinction, cancellation, rewrite retry).
- `cd go && go test ./service/matching/tasklist` — passed.
- `cd rust && CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — pending at time of note; see final report for result.

The requested new dynamic-update/filter-independence integration tests are not yet present. Existing batch test covers the Go signed batch drain behavior; more direct tests of dynamic provider changes and filter independence remain necessary before considering this task fully audited.
