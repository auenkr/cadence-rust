# History third review — 2026-11-13

Review base: `9de5851f25b0dff6c0b43ba59bf11d0886bbde42` (`origin/master`). Branch: `history-review3`, created from that base. Static review only; no Cargo, Go, Docker, builds, tests, or gates were run. Only this report is changed. Failure scenarios below are source-verified, not runtime reproductions. Line numbers refer to the review base.

## Scope

Inspected `git diff <merge>^1 <merge>^2 -- rust/` for each requested merge, comparing production changes and their callers/readers with Go:

| Merge | Commit |
| --- | --- |
| history-failcoord | `cf055bbc9` |
| history-ttl | `f633966ec` |
| history-standbyrepl | `02305d60d` |
| history-review2fix | `a269e055b` |
| history-soakfix | `93181db52` |
| history-s3gate | `67bfba219` |
| history-swap | `3d447401f` |
| history-esvis | `141ecc43d` |

Parent-to-parent diffs also show differences caused by branch ancestry. Findings were checked against the final base above; apparent reversions in those diffs are not counted when the intended fix remains on master. Pure tests/harness changes were excluded.

## 1. P1 — Archived event attributes still contain enums that Go cannot decode

- Rust: `rust/crates/cadence-history/src/wiring/assembly/archiver_wiring.rs:18` (history-s3gate), especially the conversion limited to `eventType` at line 23. Supporting serializers: `rust/crates/cadence-api/src/enums.rs:16` and `rust/crates/cadence-api/src/tasks.rs:2560`.
- Go: `go/common/types/shared.go:383` (`ActivityTaskTimedOutEventAttributes.TimeoutType`), `:8118` / `:8143` (`TimeoutType.UnmarshalText` / `MarshalText`); archive reader `go/common/archiver/s3store/util.go:49`.
- Checked divergence: the added adapter translates only the outer event type. Rust's shared enum serializer emits JSON integers for nested attributes too. Go's enum types implement text marshaling/unmarshaling, so their JSON representation is a string; its `json.Unmarshal` archive reader rejects a JSON number for these fields. The common `go_history_batch` helper does not convert nested enums either.
- Concrete failure: a workflow has an `ActivityTaskTimedOut` event with timeout type StartToClose, later completes, and Rust archives its history. The resulting event has `"eventType":"ActivityTaskTimedOut"` but `"timeoutType":0` inside its attributes. Go expects `"timeoutType":"START_TO_CLOSE"`; fetching that archived history through Go fails decoding the blob. Other enum-bearing attributes, including decision failure causes and task-list kinds, have the same missed conversion. Deleting the live history after archival leaves Go dependent on this incompatible blob.
- Suggested fix: use a complete archive-specific Go JSON serialization layer for history and every nested attribute, including enums. Keep numeric Thrift/wire serialization separate. Validate Go archive decoding with histories containing timeouts, decision failures, child policies, and task-list kinds, rather than only start/complete events.

## 2. P1 — The new visibility producer cannot replace Go with Kafka authentication enabled

- Rust: `rust/crates/cadence-history/src/wiring/assembly/visibility_wiring.rs:30` (history-esvis); `rust/crates/cadence-service-core/src/config.rs:91` and `rust/crates/cadence-history/src/wiring/persistence/es_visibility.rs:219` (client construction).
- Go: `go/common/messaging/kafka/client_impl.go:145` (producer initialization calls `initAuth`), `:173` (TLS setup), `:183` (SASL setup and subsequent SCRAM selection).
- Checked divergence: TLS explicitly aborts Rust History initialization. The new `KafkaConfig` also has no SASL configuration, and the client builder receives only broker addresses; SASL credentials/mechanism from a Go configuration are never applied. Go initializes both TLS and SASL on the producer.
- Concrete failure: replace Go History in an ES/OpenSearch deployment whose existing Kafka configuration has `tls.enabled: true`. Rust exits while building visibility dependencies, even if the current dynamic write mode is DB. Independently, on a SASL-only Kafka listener, Rust discards the SASL configuration and attempts unauthenticated connections; visibility writes fail and their transfer tasks retry instead of producing index messages. Both are supported working configurations of the Go service.
- Suggested fix: port Kafka TLS trust/client-certificate configuration and SASL configuration/mechanism selection into the Rust producer, preserving the existing YAML contract. Add CI compatibility cases against authenticated Kafka, including SCRAM where configured.

## 3. P1 — The shard identity fix introduces a strong-reference cycle for every published shard

- Rust: `rust/crates/cadence-history/src/wiring/assembly/controller.rs:186` (history-review2fix), with `my_cell` captured at line 195 and the entry published through line 163; `rust/crates/cadence-history/src/shard_context.rs:206` retains the callback.
- Go: `go/service/history/shard/controller.go:375` removes the registered shard item; `:272` stops the closing item, and `:539` / `:551` stop and clear its engine.
- Checked divergence: `Cell` is an `Arc<OnceCell<Arc<ShardEntry>>>`. The callback now owns a strong `my_cell`, rather than the former weak shard identity. After publication the ownership graph is `cell -> entry -> shard -> close_callback -> cell`. Removing the cell from the registry and stopping the engine does not break any link in that cycle. `ShardContext::close_shard` clones/invokes its stored callback but does not remove it. Go's garbage collector can reclaim its corresponding cyclic objects once they are unreachable; Rust's `Arc` cannot.
- Concrete failure: normal ownership changes repeatedly acquire and retire shards on a History host. Each retired shard permanently retains its entry, engine, shard state, and objects reachable through those fields, even after all request references and registry references are dropped. Memory accumulates with shard churn and can exhaust the service process. This does not require the narrow initialization race in finding 4.
- Suggested fix: capture a `Weak` reference to the owning cell (or an identity token that does not own the cell/entry), upgrade only while checking registry identity, and ensure stop/removal releases the retired object graph. Verify reclamation after both explicit removal and callback-driven closure using weak-reference lifetime assertions.

## 4. P2 — Closing after the early-close flag check still leaves the initializing engine running

- Rust: `rust/crates/cadence-history/src/wiring/assembly/controller.rs:235` (history-review2fix), callback at line 201 and cell publication at line 163.
- Go: `go/service/history/shard/controller.go:510` (exclusive item lock across engine creation/start), `:539` (stop takes the same lock), and `:272` (the closing item's engine is stopped even when removal finds no current item).
- Checked divergence: `create_entry` checks `closed_early` before returning the entry to `get_or_try_init`. The `OnceCell` is still empty at that check. The callback's `Some(None)` path only sets the flag, with no synchronized handoff to a later stop after publication. Sequentially consistent atomics do not close this interval.
- Concrete failure: engine startup finishes and line 235 reads false; the creation task is then preempted before cell publication. An ownership-loss close callback removes this acquisition's cell, observes it is still empty, sets `closed_early = true`, and returns. Creation resumes and publishes/returns its entry without checking the flag again. The registry no longer contains that entry, yet its engine was never stopped. Reacquisition can start a successor while the old engine's background work remains running. A related missed-stop case occurs if an independent registry removal already removed the initializing cell: the callback's `None` branch does nothing, whereas Go still stops its own closing item.
- Suggested fix: serialize startup/publication and closure with a shared lifecycle state/lock or a handshake that guarantees exactly one eventual stop of the owning engine, including when its cell was already removed or replaced. Preserve the identity check for removing successors, but do not make stopping the old engine conditional on successful registry removal.

## Remaining comparisons and limits

No additional checked findings for the failover-marker creation-time change, Matching TTL, passive replay/history persistence and empty version-history initialization, pending-decision guard/typed missing-history error, or versioned serializer/Cassandra read changes. The Kafka message field IDs, enum discriminants, version preamble, workflow-ID key, and hybrid write-mode loop were compared with `go/idls/thrift/indexer.thrift`, `go/common/persistence/elasticsearch/es_visibility_store.go`, `go/common/messaging/kafka/producer_impl.go`, and `go/common/persistence/visibility_hybrid_manager.go`; no additional concrete wire discrepancy was established there.

Summary: **3 P1, 1 P2, 0 P3**. No implementation fixes were made. This static review does not establish service parity or replace mixed-cluster validation.
