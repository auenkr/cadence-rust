# History advanced-visibility (ES/OpenSearch) writer: 2026-11-13

Branch `history-esvis`, head SHA `084d0148e08c63196cf1a4f8f8364ba6de1d3d85` (evidence commit follows it).

## Ported (Rust `cadence-history`)
- `wiring/persistence/es_visibility.rs`:
  - Go `createVisibilityMessage`, `indexer.Message` (ThriftRW, 0x59 prefix) and `Field` encoding.
  - Kafka key is workflowID, Sarama FNV-1a partitioner, rskafka producer.
  - `EsVisibilityWriter`.
  - `HybridVisibilityWriter`, a port of Go `visibilityHybridManager` write-mode logic (`system.writeVisibilityStoreName`, off=db, fallback to db, aggregated errors).
- `wiring/assembly/visibility_wiring.rs`: builds the managers {db, es|os} from `advancedVisibilityStore` ("es-visibility"/"os-visibility") and `kafka.applications.visibility`.
- `cadence-service-core/src/config.rs`: `kafka` config (clusters/topics/applications), `producer_target` (":9092" default port).
- `queue_deps.rs`: started, closed, uninitialized, upsert and delete visibility tasks all go through the hybrid writer.
- Known gap: Kafka TLS (`kafka.tls.enabled`) is rejected at startup, and pinot is unsupported.
- Go refs: `go/common/persistence/elasticsearch/es_visibility_store.go`, `go/common/persistence/visibility_hybrid_manager.go`, `go/common/persistence/client/factory.go`.

## Bug found by the gate
ES list-closed-by-workflow-ID returned 0. The Rust record carries start/close/execution/scheduled times in ms, while Go sends UnixNano and the frontend range query is in ns. Fix: `nanos()` normalisation in the message builder (same heuristic as `archive_nanos`).
The Go indexer logs "Unregistered field" for ClusterAttributeScope/Name. Go does the same, so it is not a failure.

## Evidence (all on 084d0148, `GATE SUMMARY failed: []`)
- rust-ci cadence-history: 43471866969 success; rust-check 43471866137 success.
- ES gate: elasticsearch 43471872668 (the first of the two, pre-fix failing runs 43467463620/43467469879), opensearch 43471878643. Both pass.
- Mixed gates (mysql/postgres/cassandra x grpc/tchannel x two_history true/false), 12 runs: 43471913674, 43471920155, 43471926180, 43471931212, 43471935830, 43471946444, 43471954427, 43471960880, 43471967066, 43471972044, 43471977918, 43471984646.
