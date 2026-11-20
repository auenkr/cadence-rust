# Persisted global isolation groups in Matching and History

Implementation under test is `22ad8f0dcd110a32d1a6ec29686581efad9b9e90` on `parity-isolation-store-svc`, based on `a273c467c6b807a7beff4f15a591844460631429`.

- Go `common/resource/resource_impl.go` constructs the isolation-group store for all services and starts polling in `Impl.Start`. Matching passes its isolation state to task-list routing. The Go History service creates the shared resource and has no History-specific drain lookup.
- Rust Frontend, Matching, and both History launch paths now use `GlobalIsolationGroupStore` against the default datastore. The shared constructor uses the raw persistence adapter, bypassing Matching task-manager fault injection and rate limits as in Go. The standalone History service holds the store in `HistoryEnv` and starts polling before serving.
- Both Rust Matching task-routing selectors read domain and persisted global drains and send lookup failures through the existing isolation error fallback. Unavailable stores follow Go's optional-resource behavior. Embedded deployments continue to use in-memory state.
- The MySQL and PostgreSQL Frontend harnesses write a unique drained group via gRPC, then verify its name and state over fresh gRPC and TChannel connections after the gate's two-second refresh interval.

## Validation

| GitHub Actions run | Result | Rust binary SHA256 |
| --- | --- | --- |
| [Fresh workspace clippy and tests](https://github.com/auenkr/cadence-rust/actions/runs/44676048741) | Success | — |
| [Frontend mixed MySQL](https://github.com/auenkr/cadence-rust/actions/runs/44676048605) | Success, `GATE SUMMARY failed: []` | `955671de1081238be67f344ca65894f8c03ad5e773ee66f04f1610b9ddb44086` |
| [Frontend mixed PostgreSQL](https://github.com/auenkr/cadence-rust/actions/runs/44676048700) | Success, `GATE SUMMARY failed: []` | `822c9db66529a6240936685d0f5a3f59245be9d7079c7b4437c2c167bd3c3b00` |
| [History mixed MySQL](https://github.com/auenkr/cadence-rust/actions/runs/44676048780) | Success, `GATE SUMMARY failed: []` | server `132ac4f8de5d36c271f94a16933dd5766c52cd75671caded05d2ca9cee30ae2e`, History `fe6acf5fa71165719cd9985f389a04efb4f8c6294346df7cdd70415e8a54134c` |
| [History restart MySQL](https://github.com/auenkr/cadence-rust/actions/runs/44676048733) | Pending | Pending |

The restart lane uses its default 1,500-second SDK load duration. The Frontend gate asserts API visibility. It does not independently assert Matching poller routing from this update.
