# FE gap #15 (yarpc RegisterDomain) and #12 (raw-history token presence) follow-up

Branch `fe-gap-15-12-followup`, validated head `ad3f7618e`.

- **#15** (`b27f03524`): `handle_register` in `cadence-service-core/src/yarpc.rs` now follows the HTTP order from WP-C1: prepare active-active attributes, check retention, then validate replication config. It reuses `prepare_domain_replication` and `validate_domain_registration_replication`, which are now `pub` in `handlers.rs`. Test: `register_ordering_tests`.
- **#12** (`ad3f7618e`): `handle_raw_history_v2` treats a present Thrift field 80 (`nextPageToken`) with empty bytes as a continuation. Go checks `!= nil`, so this reaches token decoding and returns "unexpected end of JSON input". No `cadence-api` type or wire change. Test: `raw_history_token_presence_is_distinguished`. The live-history path (WP-B, field 40) was checked and is already correct.
- **Not done:** the HTTP/JSON admin path (`admin.rs`) still cannot tell absent from empty, because JSON drops empty bytes before the handler. The Thrift/yarpc path, which the SDK uses, is covered.

## CI (all success, head ad3f7618e)
| Lane | Run | Result |
| --- | --- | --- |
| rust-ci full workspace | [45200861627](https://github.com/auenkr/cadence-rust/actions/runs/45200861627) | success |
| frontend-mixed-gate mysql | [45200878223](https://github.com/auenkr/cadence-rust/actions/runs/45200878223) | `GATE SUMMARY failed: []`, Rust SHA256 62593545a244db9a3408d4d7a6f33ca6c00ebed444d6a6238e0a372f28b6fa0c |
| frontend-mixed-gate postgres | [45200884910](https://github.com/auenkr/cadence-rust/actions/runs/45200884910) | `GATE SUMMARY failed: []`, Rust SHA256 1df935f6d95a915a1638bf4a0585f74d0722f2e00024c644f247e169acd64e27 |
