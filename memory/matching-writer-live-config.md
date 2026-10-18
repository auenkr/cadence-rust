# Matching writer live configuration (task 49)

Cherry-picked candidate `c3266b87d81c317f3d2b0d96cfcadc674a5b2fe8` as local commit `c46058356` to wire the matching writer's live configuration provider. The provider resolves the domain name before taking the dynamic-config overlay lock, then reads both task-list-filtered values under that lock. Go defaults are `matching.maxTaskBatchSize = 100` and `matching.appendTaskTimeout = 5s` (`go/common/dynamicconfig/dynamicproperties/constants.go`).

The Go task writer evaluates `MaxTaskBatchSize()` on every `getWriteBatch` loop condition, including the final condition. `drain_batch_with` preserves this sampling cadence and signed values: nonpositive batch sizes admit no additional requests. Append timeout is sampled at each append attempt and covers both enqueue and response wait. A nonpositive timeout on a saturated enqueue deterministically times out; tests avoid asserting a result when Go's send and timeout select arms can both be ready. Capacity/rendezvous behavior remains a separate Matching gap.

Production behavior coverage now includes:

- A gated live `TaskWriter` actor whose config changes before batch collection; five queued tasks persist under the updated one-additional-task limit in three batches, and the provider is called for each Go loop-condition check.
- A saturated enqueue test where a previously accepted request reaches its response deadline, then the next enqueue observes a live update to zero timeout and returns ServiceBusy.
- The actual `Matching` dynamic-config provider, testing domain name, physical task-list name, and task type filters independently, plus updates through the same provider instance.
- The registry owner closure, testing captured domain UUID/task-list/type keys and observing updates without recreating the owner.

Reproducible evidence is under `memory/evidence/matching-writer-live-config/writer49/`:

- `go-writer-oracle.exit` — 0; `tests/run-matching-writer-next-oracle.py` exercises original Go writer methods and compares its trace fixture.
- `go-tasklist-tests.exit` — 0; `cd go && go test ./service/matching/tasklist`.
- `rust-writer-tests.exit` — 0; six `matching_writer` tests, including the new actor batch and saturated-timeout cases.
- `rust-registry-closure-test.exit` — 0; registry key capture/live update test.
- `rust-provider-test.exit` — 0; real Matching provider filter/update test.
- `server-all-target-clippy.exit` — 0; `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings`.
- `server-all-target-clippy-first.exit` — 101 before marking the static batch oracle helper test-only; the strict rerun above passed.

The tuple provider reads both settings at each sampling point. Domain lookup and overlay locking are sequential, with no nested reverse lock order in this path. This work does not complete Matching or address the separate bounded capacity/rendezvous gap.
