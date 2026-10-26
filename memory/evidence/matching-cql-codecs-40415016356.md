# Vendored codec unit evidence — 40415016356

Run https://github.com/auenkr/cadence-rust/actions/runs/40415016356 succeeded at `463ce1e97017a95144ada3da8a14eb0eda71b6c7`.

The dedicated vendor workspace patched the actual local scylla-cql/core sources. Fresh target: `/tmp/cql-header-target-40415016356`. Header suite: four passed; collection suite: three passed, covering v2/v3+ list/set/map byte payloads and malformed/overflow inputs. Both recorded exits are 0. This proves the helper tests, not live request/result callsite integration, configured persistence protocol acceptance, or service parity.

Artifacts retained at `/mnt/storage/service/ci-evidence/40415016356/cql-header-foundation`. File SHA256:

- `test.log`: `b783498828c1e4e536361bcb0cf09f80eba4d378361663022a70f8980807c46f`
- `collection-test.log`: `949213f8233cb0e8d5d95c6297e96f21b8b3ab7f815ff69eede21f4a88f1b603`
- `vendor-Cargo.lock`: `f0778edb0f750727dbca2e4df95756fbdaa5c1fd41540f0961f7879e13fa0810`
