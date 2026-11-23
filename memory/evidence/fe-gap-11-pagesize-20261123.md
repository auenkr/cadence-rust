# Frontend finding 11 — ListDomains page size

Go defaults zero to 100 in `go/common/domain/handler.go` and passes every other signed size to persistence. Rust preserves the signed value through the persistence traits, rate limiter, and stores. Matching's independent cache-source interface keeps its unsigned page size and checks the conversion at its persistence calls.

- MySQL and PostgreSQL require a strictly positive page size in `SelectFromDomain` in their respective `sqlplugin/*/domain.go` files. SQLite embeds the MySQL adapter in `sqlplugin/sqlite/db.go`. A negative size takes the missing-arguments branch before executing SQL. `sql_domain_store.go` and `sql/common.go` turn that error into an internal service error. Rust returns the same class and message from the SQL persistence path.
- Cassandra passes the signed size to `SelectAllDomains` in `nosqlplugin/cassandra/domain.go`. The pinned gocql revision `b225f9b53fa1` writes the native page-size flag only for strictly positive sizes. Cassandra's non-aggregate SELECT executes unpaged in that case and does not apply its paging state. The Rust adapter uses an unpaged query for negative sizes and forwards nonempty opaque state so the native protocol can reject malformed state before execution. A valid state does not resume the unpaged scan. The vendored Scylla driver exposes this query shape because its existing unpaged API supplies an empty state. The standalone in-memory store follows the same unpaged behavior for negatives.
- In the reserved `matching_domain_cassandra.rs` file, the only change is the authorized `ListDomains` page-size signature. Its existing delegation forwards the signed size directly. The Thrift handler already maps persistence errors to internal errors and needed no edit.

Unit regressions cover a negative signed value with an opaque cursor through the persisted Frontend manager and an unbounded result with no cursor in memory. Existing positive-page tests cover continuation.

## CI evidence

Validated implementation commit: `1d308c7d0b57891427d7bda593d8ca652a920b0b`.

| Check | Conclusion | Run |
| --- | --- | --- |
| `rust-ci.yml package=cadence-persistence` | Success, including both negative-size regressions | [45196609084](https://github.com/auenkr/cadence-rust/actions/runs/45196609084) |
| `rust-ci.yml package=cadence-matching` | Success | [45196608958](https://github.com/auenkr/cadence-rust/actions/runs/45196608958) |
| `rust-ci.yml clean_target=true` | Success, with workspace clippy before workspace tests | [45196608735](https://github.com/auenkr/cadence-rust/actions/runs/45196608735) |
| `frontend-mixed-gate.yml` (MySQL) | Success, `GATE SUMMARY failed: []` | [45196608983](https://github.com/auenkr/cadence-rust/actions/runs/45196608983) |
| `frontend-mixed-gate.yml database=cassandra` | Success, `GATE SUMMARY failed: []` | [45196608825](https://github.com/auenkr/cadence-rust/actions/runs/45196608825) |
| `rust-ci.yml package=scylla` | Standalone vendored test targets failed to compile because Cargo did not link upstream test dependencies, including `assert_matches` and `scylla_proxy`. The scoped Persistence run compiled the vendored library and the Cassandra gate exercised it as part of Cadence. | [45196608928](https://github.com/auenkr/cadence-rust/actions/runs/45196608928) |

Both mixed runs built a fresh Rust server. The MySQL binary SHA256 was `19561e763f8879431ce21e5b4aa9829dce95766aee2f3fdb2bbad2167594aff3`. The Cassandra binary SHA256 was `d3a93e5c3c34743ba46b729a3f72a34e661ae10f217de05eae557ecc70733871`. The mixed SDK gate exercises regular ListDomains pagination. The negative-size cases were covered by the Rust unit tests and the Cassandra wire behavior by source inspection.

See the [branch runs](https://github.com/auenkr/cadence-rust/actions?query=branch%3Afe-gap-11-pagesize) for validation on the evidence commit.
