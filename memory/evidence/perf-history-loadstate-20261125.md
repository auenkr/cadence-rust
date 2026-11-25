# History load-state snapshot, 2026-11-25

Reviewed master at `d7b1662bcb65ba0da2006650840ffe4862fa9823`. Kept the implementation unchanged because the cached snapshot has consumers that require more than write metadata.

Go's `mutableStateBuilder.Load` takes ownership of the loaded maps and execution info and separately tracks the persisted next event ID, state, and buffered-events flag. Rust's `EngineMutableState::from_snapshot` keeps the original `WorkflowSnapshot` as well as the engine's decoded working state. The normal engine write gets its condition from `Inner::db_next_event_id`. Closing a transaction also reads the original start and last-write versions and falls back to the original version-history blob when there is no decoded history. Checksum verification and active and standby queue readers use other parts of the original snapshot.

`EngineExecutionContext::clean_snapshot_for_ndc` clones the original snapshot under the workflow cache lock. `finish_transaction` advances the mutable execution info, history size, and condition and clears the dirty flag while preserving the original snapshot. Consequently even a clean state can contain values different from those delivered by the current NDC checkout. Re-encoding typed maps would also require preserving the original row encodings, opaque fields and buffered-event rows before load-time reordering and deduplication. Re-reading persistence during checkout would add I/O and could return a different revision or fail. None of these substitutions preserves the current checkout behavior as a local memory optimization.

No implementation candidate was produced or dispatched to CI. The following numbers are historical measurements from the event-cache change, averaged over three repetitions using History's `peak_cgroup_anon_bytes` in MiB. That run used control `9ce0ba1a1`. It measured a different change.

| Concurrency | Go | Rust control | Rust event-cache candidate |
| --- | ---: | ---: | ---: |
| 1 | 72.0 | 76.4 | 77.8 |
| 16 | 91.4 | 89.8 | 90.5 |
| 64 | 104.3 | 103.2 | 101.0 |

The earlier benchmark reported candidate/control throughput ratios of 1.018, 1.012, and 1.017. Its successful Actions runs were [crate CI](https://github.com/auenkr/cadence-rust/actions/runs/45540826469), [History mixed gate](https://github.com/auenkr/cadence-rust/actions/runs/45541735032), and [benchmark](https://github.com/auenkr/cadence-rust/actions/runs/45541739794).
