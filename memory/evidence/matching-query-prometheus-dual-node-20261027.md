# Matching query Prometheus capture on both failure-recovery nodes (2026-10-27)

## Diagnosis

Mixed gate run `40479417510` (`/mnt/storage/service/ci-evidence/40479417510/matching-mixed-gate`) reached the real SDK query flow: `tests/matching-service-compat.cjs` starts the workflow, schedules and registers a real decision poll, sends `QueryWorkflow`, receives the query task, calls `RespondQueryTaskCompleted`, and asserts the returned answer is `pong` (query fixture around lines 1380-1403). The recorded `probe.log` has `"queryDispatch":"passed"`.

The exporter assertion failed because `run-go-service-baseline.py` configured and scraped only the primary Rust Matching Prometheus listener. The same run enabled two-node failure recovery and `matching-service-compat.cjs` launched the secondary node with the same config, hence the same exporter address. Query task-list ownership is selected through Ringpop and can resolve to the secondary. In the retained scrape the SDK-driven query completion sample with `caller="cadence_frontend"`, the registered domain, gRPC and operation `RespondQueryTaskCompleted` is absent; the validator correctly rejected it. The failure is capture topology, not evidence for weakening the asserted sample.

## Fixture change

For failure-recovery runs with Prometheus enabled, the harness writes a secondary Rust Matching config with a distinct loopback exporter port and supplies its path to the compatibility probe. The secondary process uses that config. The scraper captures nonempty samples from both primary and secondary listeners into the existing compressed timestamped scrape stream. `validate_matching_exporter` remains unchanged and still requires the same positive SDK-driven primary query sample and all other existing metrics. Other exporter modes and non-failure-recovery runs retain their existing configuration.

## Evidence and limits

- Prior gate: `40479417510`; Go SDK query dispatch passed; primary-only scrape missed the expected query sample; overall gate failed at `validate_matching_exporter`.
- Local permitted checks: `python3 -m py_compile tests/run-go-service-baseline.py`, `node --check tests/matching-service-compat.cjs`, and `git diff --check` passed.
- No Rust, Go, build, runtime, or mixed-gate command was run locally. The fixture behavior needs a public Actions mixed-gate rerun to establish the formerly missing scrape assertion. This change does not establish matching-service parity or gate acceptance by itself.
